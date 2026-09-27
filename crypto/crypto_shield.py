#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Crypto Shield (Dynamic Volatility-Based Trailing Stop-Loss)
Enforces trailing stop-losses calculated dynamically from asset volatility.

v1.4 (resilient feeds): live prices route through the shared price_venues
pool (fresh ccxt instance per venue, kucoin -> binance -> bybit,
MACE_PRICE_VENUES-tunable) instead of two long-lived exchange sessions. On
2026-09-27 the long-lived KuCoin session degraded at transport level (empty /
WAF-style responses) while fresh-instance probes priced the same pairs fine -
every BEAT/USDT sweep logged "unavailable on KuCoin" and the Binance failover
raised BadSymbol (BEAT is KuCoin-only), leaving the position blind until a
manual restart. This pass also ships per-position crash isolation,
deterministic connection cleanup, a component_health heartbeat, and
ghost-ticker protection (two-sided quotes required for stop-out pricing).
"""

import os
import sys
import json
import asyncio
import argparse
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
import ccxt
import paho.mqtt.client as mqtt_client

# Base Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

# v1.3: shared modules at repo root - realized round-trip store (honest Kelly)
# and the same taker fee the guardrail virtual ledger now charges. The shield
# settles stop-outs via raw SQL that bypasses guardrail, so it must apply the
# fee itself or exits would be gross while entries (post-v1.3) are fee-inclusive.
# v1.4: price_venues joins the import set - the shield's live prices now come
# from the same multi-venue pool the orchestrator's ledger fills use.
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
import realized_round_trips as rrt
import price_venues as price_pool

TAKER_FEE = float(os.getenv("MACE_TAKER_FEE", "0.001"))

# MQTT Config
MQTT_BROKER_IP = os.getenv("MQTT_BROKER_IP", "192.168.0.110")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "mace/telemetry/crypto_shield"

def push_mqtt_telemetry(payload):
    try:
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        user = os.environ.get("MQTT_USER")
        password = os.environ.get("MQTT_PASSWORD")
        if user and password:
            client.username_pw_set(user, password)
        client.connect(MQTT_BROKER_IP, MQTT_PORT, 60)
        client.publish(MQTT_TOPIC, json.dumps(payload), retain=True)
        client.disconnect()
    except Exception as e:
        logger.error(f"[!] Telemetry update path bottlenecked: {e}")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.crypto_shield")


def _utcnow_naive():
    """v1.4 polish: datetime.utcnow() is deprecated on Python 3.12+; this keeps
    the exact same contract (naive UTC) so strftime/strptime round-trips with
    DB timestamps stay bit-compatible."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_db_connection(db_path=DEFAULT_DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    abs_db_path = os.path.abspath(db_path)
    conn = sqlite3.connect(abs_db_path, timeout=30.0)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn

class CryptoShield:
    def __init__(self):
        # v1.4: the two long-lived exchange sessions are RETIRED. A single
        # persistent ccxt.kucoin session degraded at transport level
        # (empty/WAF-style responses) on 2026-09-27 while fresh-instance
        # probes priced the same pairs fine; the Binance failover then raised
        # BadSymbol for KuCoin-only listings (BEAT/USDT), leaving the position
        # blind until a manual restart. All pricing now goes through the
        # shared price_venues pool: a FRESH ccxt instance per venue per fetch
        # (degradation-immune), kucoin -> binance -> bybit rotation,
        # env-tunable via MACE_PRICE_VENUES.
        self.venues = price_pool.get_venue_list()
        logger.info(f"[*] Autonomous Trailing Defensive Shield Connected. Price pool: {', '.join(self.venues)}")

    def _heartbeat(self, status, detail=""):
        """v1.4: fail-neutral health telemetry (parity with the tradfi news
        guard / tradfi shield). A feed-blind shield is now visible in
        component_health instead of only in journal warnings - the BEAT blind
        windows were discovered by log-reading precisely because nothing else
        would have surfaced them. Never raises, never gates anything."""
        try:
            rrt.write_component_heartbeat("crypto_shield", status, detail)
        except Exception:
            pass

    async def fetch_live_price(self, pair):
        target_pair = pair.replace("_", "/")
        # v1.4: pool routing (fresh instance per venue - immune to the
        # long-lived-session degradation) plus a two-sided-quote requirement:
        # a stop-out must never fire off a ghost ticker (stale last, no live
        # bid/ask). Total pool failure returns (0.0, None) -> None here ->
        # the position is skipped this sweep, same contract as before.
        try:
            price, venue = await price_pool.fetch_live_price_pool(
                target_pair, log=logger.info, two_sided=True)
        except Exception as pool_error:
            logger.error(f"[!] Price pool exception for {target_pair}: {pool_error}")
            return None
        if price and price > 0 and venue is not None:
            return float(price)
        logger.error(f"[!] Ticker lookup failed for {target_pair} across all {len(self.venues)} pool venues")
        return None

    async def run_shield_cycle(self):
        logger.info("[*] Commencing deterministic 15-minute risk trailing sweep...")
        breach_details = []
        positions_telemetry = []
        total_holdings_value = 0.0
        conn = None

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Extract active assets with entry price columns, excluding stablecoins
            cursor.execute("""
                SELECT token, quantity, avg_entry_price 
                FROM portfolio 
                WHERE quantity > 0 
                  AND token NOT IN ('USDT', 'USDC', 'USDE', 'USDS', 'DAI', 'FDUSD', 'TUSD', 'USDP', 'USDD')
            """)
            active_positions = cursor.fetchall()

            # We also query the cash balance (USDT)
            cursor.execute("SELECT quantity FROM portfolio WHERE token = 'USDT'")
            cash_row = cursor.fetchone()
            usdt_balance = float(cash_row["quantity"]) if cash_row else 0.0
            current_usdt_balance = usdt_balance

            if not active_positions:
                logger.info("[*] Sweep complete: No active token holdings found in the matrix database ledger.")
                telemetry_payload = {
                    "timestamp": _utcnow_naive().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "engine": "crypto_shield",
                    "status": "MONITORING_ACTIVE",
                    "usdt_balance": round(usdt_balance, 2),
                    "total_holdings_value": 0.0,
                    "total_portfolio_value": round(usdt_balance, 2),
                    "positions": [],
                    "breaches": []
                }
                push_mqtt_telemetry(telemetry_payload)
                self._heartbeat("healthy", "no active positions")
                return

            for row in active_positions:
                token = row["token"]
                qty = float(row["quantity"])
                cost_basis = float(row["avg_entry_price"])
                pair = f"{token}/USDT"

                if not cost_basis or cost_basis <= 0:
                    continue

                # v1.4: per-position crash isolation (parity with tradfi_shield's
                # per-symbol guard). The old single big try meant one poisoned
                # row aborted every REMAINING position's sweep - a stop-out
                # enforcement gap across the whole book exactly when one symbol
                # needed watching most.
                try:
                    live_price = await self.fetch_live_price(pair)
                    if live_price is None:
                        continue

                    # Query trailing stop-loss metrics from vw_crypto_risk_corridors view
                    cursor.execute("SELECT high_water_mark, loss_limit, stop_floor_price FROM vw_crypto_risk_corridors WHERE symbol = ?", (pair,))
                    hwm_row = cursor.fetchone()
                    if hwm_row:
                        hwm = float(hwm_row["high_water_mark"])
                        loss_limit = float(hwm_row["loss_limit"])
                        calc_floor = float(hwm_row["stop_floor_price"])
                    else:
                        hwm = max(cost_basis, live_price)
                        loss_limit = 0.08
                        calc_floor = hwm * (1 - loss_limit)
                        now_str = _utcnow_naive().strftime('%Y-%m-%dT%H:%M:%SZ')
                        cursor.execute("SELECT asset_id FROM vw_crypto_universe WHERE symbol = ?", (pair,))
                        a_row = cursor.fetchone()
                        if a_row:
                            asset_id = a_row[0]
                            cursor.execute("""
                                INSERT OR IGNORE INTO crypto_hwm (asset_id, symbol, high_water_mark, loss_limit, updated_at)
                                VALUES (?, ?, ?, ?, ?)
                            """, (asset_id, pair, hwm, loss_limit, now_str))
                            conn.commit()

                    # Compute drawdown metrics relative directly to the Peak High Water Mark
                    trailing_drawdown_pct = (live_price - hwm) / hwm
                    floor_price = calc_floor

                    # Dynamic conditional formatting blocks based on asset scale properties
                    if live_price < 0.01:
                        logger.info(f"[*] Position Check: {pair} | Qty: {qty} | Peak HWM: ${hwm:.8f} | Live: ${live_price:.8f} | Floor Target: ${floor_price:.8f} | Drop from Peak: {trailing_drawdown_pct*100:+.2f}%")
                    else:
                        logger.info(f"[*] Position Check: {pair} | Qty: {qty} | Peak HWM: ${hwm:.4f} | Live: ${live_price:.4f} | Floor Target: ${floor_price:.4f} | Drop from Peak: {trailing_drawdown_pct*100:+.2f}%")

                    # Hard Check: Validate if current asset values breach the trailing peak floor
                    if trailing_drawdown_pct <= -loss_limit:
                        logger.warning(f"[!!!] TRAILING STOP-LOSS BREACHED: {pair} dropped {trailing_drawdown_pct*100:.2f}% below peak!")

                        # v1.3: net of taker fee - entries have been fee-inclusive
                        # in the guardrail since v1.3; a gross exit credit would
                        # systematically overstate recovered USDT by 0.1% per stop-out.
                        usdt_recovered = qty * live_price * (1.0 - TAKER_FEE)
                        fee_charged = qty * live_price * TAKER_FEE
                        if live_price < 0.01:
                            logger.warning(f"[!!!] EXECUTION REFLEX: Liquidating {qty} {token} at ${live_price:.8f} -> Recovering ${usdt_recovered:.2f} USDT (net of ${fee_charged:.2f} taker fee)")
                        else:
                            logger.warning(f"[!!!] EXECUTION REFLEX: Liquidating {qty} {token} at ${live_price:.4f} -> Recovering ${usdt_recovered:.2f} USDT (net of ${fee_charged:.2f} taker fee)")

                        # v1.3: register the realized round trip (ledger-exact: entry
                        # basis = ledger avg_entry_price, exit = live net of fee) so
                        # the brains' Kelly stats reflect stop-out outcomes.
                        rrt.record_trip(
                            asset_class="CRYPTO", symbol=pair, qty=qty,
                            entry_price=cost_basis,
                            exit_price=live_price * (1.0 - TAKER_FEE),
                            reason="STOP_LOSS_BREACH", basis="ledger_exact",
                            db_path=DEFAULT_DB_PATH)

                        # Register 24-hour Post-Liquidation Cooldown Lock in trade_cooldowns
                        cursor.execute("SELECT asset_id FROM vw_crypto_universe WHERE symbol = ?", (pair,))
                        a_row = cursor.fetchone()
                        if a_row:
                            asset_id = a_row[0]
                            now_str = _utcnow_naive().strftime("%Y-%m-%dT%H:%M:%SZ")
                            cooldown_until_str = (_utcnow_naive() + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
                            cursor.execute("""
                                INSERT INTO trade_cooldowns (asset_id, symbol, closed_at, reason, cooldown_until)
                                VALUES (?, ?, ?, 'STOP_LOSS_BREACH', ?)
                            """, (asset_id, pair, now_str, cooldown_until_str))

                        # Delete the liquidated position from the portfolio and clear its high-water-mark tracking
                        cursor.execute("DELETE FROM portfolio WHERE token = ?", (token,))
                        cursor.execute("DELETE FROM crypto_hwm WHERE symbol = ?", (pair,))
                        cursor.execute("UPDATE portfolio SET quantity = quantity + ? WHERE token = 'USDT'", (usdt_recovered,))
                        conn.commit()
                        current_usdt_balance += usdt_recovered
                        breach_details.append(f"{pair} stopped out at trailing floor. 24h Cooldown Lock registered.")
                    else:
                        total_holdings_value += qty * live_price
                        positions_telemetry.append({
                            "token": token,
                            "qty": qty,
                            "avg_cost": cost_basis,
                            "live_price": live_price,
                            "hwm": hwm,
                            "loss_limit": loss_limit,
                            "drawdown_pct": round(trailing_drawdown_pct * 100, 2),
                            "value_usdt": round(qty * live_price, 2)
                        })

                except Exception as position_error:
                    logger.error(f"[!] Position check failed for {pair} (isolated; sweep continues): {position_error}")
                    continue

            total_portfolio_value = current_usdt_balance + total_holdings_value
            telemetry_payload = {
                "timestamp": _utcnow_naive().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "engine": "crypto_shield",
                "status": "MONITORING_ACTIVE",
                "usdt_balance": round(current_usdt_balance, 2),
                "total_holdings_value": round(total_holdings_value, 2),
                "total_portfolio_value": round(total_portfolio_value, 2),
                "positions": positions_telemetry,
                "breaches": breach_details
            }
            push_mqtt_telemetry(telemetry_payload)
            self._heartbeat(
                "healthy",
                f"positions={len(active_positions)} breaches={len(breach_details)} "
                f"holdings_usdt={round(total_holdings_value, 2)}")

        except Exception as e:
            logger.error(f"[!] Exception caught inside main Shield trailing execution block: {e}")
            self._heartbeat("degraded", f"sweep exception: {e}")
        finally:
            # v1.4: deterministic cleanup. The old code closed the connection
            # only on its success paths; any mid-sweep exception leaked it.
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

async def main():
    shield = CryptoShield()
    while True:
        await shield.run_shield_cycle()
        if not args.daemon:
            break
        await asyncio.sleep(args.interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="M.A.C.E. Crypto Trailing Shield")
    parser.add_argument("--daemon", action="store_true", default=True, help="Enforces permanent looping")
    parser.add_argument("--interval", type=int, default=900, help="Frequency for evaluation checks in seconds")
    args = parser.parse_args()

    asyncio.run(main())
