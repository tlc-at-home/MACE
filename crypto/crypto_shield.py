#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Crypto Shield (Dynamic Volatility-Based Trailing Stop-Loss)
Enforces trailing stop-losses calculated dynamically from asset volatility.
"""

import os
import sys
import json
import asyncio
import argparse
import logging
import sqlite3
from datetime import datetime
import ccxt
import paho.mqtt.client as mqtt_client

# Base Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

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

def get_db_connection(db_path=DEFAULT_DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    abs_db_path = os.path.abspath(db_path)
    conn = sqlite3.connect(abs_db_path, timeout=30.0)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn

class CryptoShield:
    def __init__(self):
        self.primary_exchange = ccxt.kucoin({'enableRateLimit': True})
        self.fallback_exchange = ccxt.binance({'enableRateLimit': True})
        logger.info("[*] Autonomous Trailing Defensive Shield Connected. Source: KuCoin")

    async def fetch_live_price(self, pair):
            target_pair = pair.replace("_", "/")
            try:
                ticker = await asyncio.to_thread(self.primary_exchange.fetch_ticker, target_pair)
                return float(ticker['last'])
            except (ccxt.BadSymbol, ccxt.NetworkError, ccxt.ExchangeError) as e:
                try:
                    logger.warning(f"[-] Synced feed failed or symbol {target_pair} unavailable on KuCoin ({str(e)}). Failover routing to Binance...")
                    ticker = await asyncio.to_thread(self.fallback_exchange.fetch_ticker, target_pair)
                    return float(ticker['last'])
                except Exception as backup_error:
                    logger.error(f"[!] Critical Error: Ticker lookup failed for {target_pair} across both pools: {backup_error}")
                    return None

    async def run_shield_cycle(self):
        logger.info("[*] Commencing deterministic 15-minute risk trailing sweep...")
        breach_details = []
        positions_telemetry = []
        total_holdings_value = 0.0

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
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "engine": "crypto_shield",
                    "status": "MONITORING_ACTIVE",
                    "usdt_balance": round(usdt_balance, 2),
                    "total_holdings_value": 0.0,
                    "total_portfolio_value": round(usdt_balance, 2),
                    "positions": [],
                    "breaches": []
                }
                push_mqtt_telemetry(telemetry_payload)
                conn.close()
                return

            for row in active_positions:
                token = row["token"]
                qty = float(row["quantity"])
                cost_basis = float(row["avg_entry_price"])
                pair = f"{token}/USDT"

                if not cost_basis or cost_basis <= 0:
                    continue

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
                    now_str = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                    cursor.execute("SELECT asset_id FROM vw_crypto_universe WHERE pair = ?", (pair,))
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

                    usdt_recovered = qty * live_price
                    if live_price < 0.01:
                        logger.warning(f"[!!!] EXECUTION REFLEX: Liquidating {qty} {token} at ${live_price:.8f} -> Recovering ${usdt_recovered:.2f} USDT")
                    else:
                        logger.warning(f"[!!!] EXECUTION REFLEX: Liquidating {qty} {token} at ${live_price:.4f} -> Recovering ${usdt_recovered:.2f} USDT")

                    # Register 24-hour Post-Liquidation Cooldown Lock in trade_cooldowns
                    cursor.execute("SELECT asset_id FROM vw_crypto_universe WHERE pair = ?", (pair,))
                    a_row = cursor.fetchone()
                    if a_row:
                        asset_id = a_row[0]
                        now_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                        cooldown_until_str = (datetime.utcnow() + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
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

            total_portfolio_value = current_usdt_balance + total_holdings_value
            telemetry_payload = {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "engine": "crypto_shield",
                "status": "MONITORING_ACTIVE",
                "usdt_balance": round(current_usdt_balance, 2),
                "total_holdings_value": round(total_holdings_value, 2),
                "total_portfolio_value": round(total_portfolio_value, 2),
                "positions": positions_telemetry,
                "breaches": breach_details
            }
            push_mqtt_telemetry(telemetry_payload)
            conn.close()

        except Exception as e:
            logger.error(f"[!] Exception caught inside main Shield trailing execution block: {e}")

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
