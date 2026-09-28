#!/usr/bin/env python3.11
"""
M.A.C.E. Unified 24-Hour Rolling Volatility Stop & High Water Mark (HWM) State Engine
-----------------------------------------------------------------------------------
Updates portfolio.db every 60 seconds with 24-hour rolling volatility stops and ratcheted HWM.
Integrates with unified asset_universe, equities_hwm, crypto_hwm, and risk corridor views.
"""

import sys
import os
import json
import asyncio
import argparse
import logging
import sqlite3
import numpy as np
from datetime import datetime, timedelta, timezone
import paho.mqtt.client as mqtt_client

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from brokers import get_client_by_name

# v1.4: shared-module imports join (repo root is on sys.path above) - the
# crypto leg gets venue-pool parity and writes a daemon heartbeat.
import price_venues as price_pool
import realized_round_trips as rrt

DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")
MQTT_BROKER_IP = os.getenv("MQTT_BROKER_IP", "192.168.0.110")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "mace/telemetry/hwm_updater"

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.hwm_updater")


def push_mqtt_telemetry(payload):
    try:
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        user = os.environ.get("MQTT_USER")
        password = os.environ.get("MQTT_PASSWORD")
        if user and password:
            client.username_pw_set(user, password)
        client.connect(MQTT_BROKER_IP, MQTT_PORT, 10)
        client.publish(MQTT_TOPIC, json.dumps(payload))
        client.disconnect()
    except Exception as e:
        logger.warning(f"[!] MQTT Telemetry exception: {e}")


def calculate_24h_rolling_volatility_stop(bars, multiplier=5.0, min_bound=0.050, max_bound=0.160):
    """
    Calculates dynamic loss limit based on 24-hour rolling 1-minute log returns.
    Formula: horizon_volatility = std(log_returns) * sqrt(1440) * multiplier

    v1.1 TURNAROUND FIX: defaults widened from 2.5x/3-8% to 5.0x with market-specific
    clamps (equities 5-12%, crypto 6-16%). The original 2.5x daily-vol corridor sat
    INSIDE the noise band of a multi-day momentum hold: Monte Carlo replication of
    this exact exit engine (scripts/mace_stop_autopsy.py, 200 paths x 4 regimes)
    showed 23-33% of round trips stop out on pure noise in mild/chop tapes, capturing
    only +2.65% of a +36.9% strong bull, and the 2x-widened stop strictly dominating
    in every environment. Distance is now Chandelier-equivalent for the holding
    timeframe instead of one-day noise scale.
    """
    try:
        closes = [float(b["c"]) for b in bars if "c" in b and float(b["c"]) > 0]
        if len(closes) < 120:
            return 0.080  # Fallback default 8.0% if insufficient bars exist (<120 mins)

        log_returns = np.diff(np.log(closes))
        sigma_1m = np.std(log_returns)

        # sqrt(1440 bars in 24h) ≈ 37.9473
        sqrt_24h = 37.947331922
        horizon_volatility = sigma_1m * sqrt_24h * multiplier

        return max(min_bound, min(float(horizon_volatility), max_bound))
    except Exception as e:
        logger.warning(f"[!] Exception calculating volatility stop: {e}")
        # v1.4: failure-direction fix. Returning min_bound (5-6%) on a compute
        # exception TIGHTENED the corridor below the documented insufficient-
        # bars fallback (8%) - the wrong direction for a protective stop: a
        # broken calculation must widen the corridor, never narrow it.
        return 0.080


def calculate_daily_volatility_stop(bars, multiplier=5.0, min_bound=0.050, max_bound=0.120, min_daily_bars=10):
    """
    v1.2: equities stop distance from DAILY closes (14-20 calendar day window).

    The intraday 24h/1Min basis starved on weekends and pre-open Mondays
    (IEX returned <120 1-min bars), which pinned every equity corridor at
    exactly the static 8.0% fallback for the whole 48h post-deploy window
    (all 17 held symbols at 8.00%). Daily closes are weekend-immune and
    express the same "5x daily vol" distance the 5-12% clamp band was sized
    for (sigma_1m * sqrt(1440) == sigma_daily), restoring per-symbol scaling.

    Returns None when daily history is insufficient or degenerate, so the
    caller can fall back to the intraday calculation - "no data" stays
    distinguishable from a genuinely computed corridor.
    """
    try:
        closes = [float(b["c"]) for b in bars if "c" in b and float(b["c"]) > 0]
        if len(closes) < min_daily_bars:
            return None

        log_returns = np.diff(np.log(closes))
        sigma_daily = float(np.std(log_returns))
        if sigma_daily <= 0.0 or not np.isfinite(sigma_daily):
            return None

        horizon_volatility = sigma_daily * multiplier
        return max(min_bound, min(horizon_volatility, max_bound))
    except Exception as e:
        logger.warning(f"[!] Exception calculating daily volatility stop: {e}")
        return None


_LAST_VOL_BASIS = {}


def note_vol_basis_change(symbol, vol_basis, loss_limit):
    """
    v1.2: logs stop-distance basis TRANSITIONS only. The updater daemon sweeps
    every 60s; per-sweep logging would add ~25k journal lines/day. Transition-only
    logging keeps the journal quiet while every corridor flip (daily <-> fallback)
    remains visible for dashboard verification.
    """
    try:
        if _LAST_VOL_BASIS.get(symbol) != vol_basis:
            _LAST_VOL_BASIS[symbol] = vol_basis
            logger.info(f"[i] {symbol}: stop-distance basis -> {vol_basis} (corridor {loss_limit * 100:.2f}%)")
    except Exception:
        pass


def get_db_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_or_create_asset_id(conn, symbol, asset_class="TRADFI", broker="alpaca", exchange="SMART", currency="USD"):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT asset_id FROM asset_universe WHERE symbol = ? AND asset_class = ?",
        (symbol, asset_class)
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    # Create record if not found
    cursor.execute("""
        INSERT INTO asset_universe (symbol, asset_class, asset_name, category, broker, exchange, currency)
        VALUES (?, ?, ?, 'EQUITY', ?, ?, ?)
        ON CONFLICT(symbol, broker, exchange) DO UPDATE SET symbol=excluded.symbol
    """, (symbol, asset_class, symbol, broker, exchange, currency))
    conn.commit()

    cursor.execute(
        "SELECT asset_id FROM asset_universe WHERE symbol = ? AND asset_class = ?",
        (symbol, asset_class)
    )
    res = cursor.fetchone()
    return res[0] if res else None


def update_db_hwm(table_name, asset_id, symbol, hwm, loss_limit, last_fill_time=None):
    if not os.path.exists(DB_PATH):
        return
    try:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with get_db_connection() as conn:
            cursor = conn.cursor()
            if table_name == "equities_hwm":
                query = """
                    INSERT INTO equities_hwm (asset_id, symbol, last_fill_time, high_water_mark, loss_limit, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(asset_id) DO UPDATE SET
                        high_water_mark = MAX(equities_hwm.high_water_mark, excluded.high_water_mark),
                        loss_limit = excluded.loss_limit,
                        updated_at = excluded.updated_at;
                """
                cursor.execute(query, (asset_id, symbol, last_fill_time, hwm, loss_limit, now_str))
            else:
                query = """
                    INSERT INTO crypto_hwm (asset_id, symbol, high_water_mark, loss_limit, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(asset_id) DO UPDATE SET
                        high_water_mark = MAX(crypto_hwm.high_water_mark, excluded.high_water_mark),
                        loss_limit = excluded.loss_limit,
                        updated_at = excluded.updated_at;
                """
                cursor.execute(query, (asset_id, symbol, hwm, loss_limit, now_str))
            conn.commit()
    except Exception as e:
        logger.error(f"[!] SQLite HWM upsert exception for {symbol} (asset_id {asset_id}) in {table_name}: {e}")


def get_stored_hwm_map(table_name):
    records = {}
    if not os.path.exists(DB_PATH):
        return records
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            rows = cursor.execute(f"SELECT asset_id, symbol, high_water_mark FROM {table_name}").fetchall()
            for r in rows:
                records[r[1]] = {"asset_id": r[0], "hwm": float(r[2])}
    except Exception as e:
        logger.error(f"[!] SQLite query exception on {table_name}: {e}")
    return records


async def sync_tradfi_positions(alpaca_client):
    positions = await asyncio.to_thread(alpaca_client.get_positions)
    if not positions:
        return []

    stored_map = get_stored_hwm_map("equities_hwm")
    summary = []

    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(hours=24)
    start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    with get_db_connection() as conn:
        for pos in positions:
            symbol = pos["symbol"]
            live_price = float(pos["current_price"])
            avg_entry = float(pos["avg_entry_price"])

            asset_id = get_or_create_asset_id(conn, symbol, asset_class="TRADFI", broker="alpaca")

            # v1.2: PRIMARY vol basis = 20 calendar days of daily bars. The 24h/1Min
            # IEX window starved on weekends and pre-open Mondays (<120 bars), which
            # pinned every equity at the static 8.0% fallback (48h post-deploy: all 17
            # corridors at exactly 8.0%). Daily closes are weekend-immune and match the
            # 5x-daily-vol basis the 5-12% clamp band was sized for.
            vol_basis = "fallback_8pct"
            loss_limit = None
            try:
                daily_start_str = (now_utc - timedelta(days=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
                daily_bars = await asyncio.to_thread(
                    alpaca_client.get_historical_bars,
                    symbol,
                    "1Day",
                    daily_start_str,
                    end_str,
                    "iex"
                )
                loss_limit = calculate_daily_volatility_stop(
                    daily_bars, multiplier=5.0, min_bound=0.050, max_bound=0.120
                )
                if loss_limit is not None:
                    vol_basis = "daily_20d"
            except Exception as e:
                logger.warning(f"[!] Failed fetching daily bars for {symbol}: {e}")

            # Fallback chain: 24h of 1-minute bars (recent listings / daily feed outage)
            if loss_limit is None:
                try:
                    bars = await asyncio.to_thread(
                        alpaca_client.get_historical_bars,
                        symbol,
                        "1Min",
                        start_str,
                        end_str,
                        "iex"
                    )
                except Exception as e:
                    logger.warning(f"[!] Failed fetching bars for {symbol}: {e}")
                    bars = []
                if len(bars) >= 120:
                    vol_basis = "intraday_24h"
                # v1.1: distance doubled (2.5x -> 5.0x daily vol), clamp widened 3-8% -> 5-12%.
                loss_limit = calculate_24h_rolling_volatility_stop(
                    bars, multiplier=5.0, min_bound=0.050, max_bound=0.120
                )

            note_vol_basis_change(symbol, vol_basis, loss_limit)

            prev_info = stored_map.get(symbol, {})
            prev_hwm = prev_info.get("hwm", max(avg_entry, live_price))
            new_hwm = max(prev_hwm, live_price)

            last_fill_dt = await asyncio.to_thread(alpaca_client.get_last_fill_time, symbol)
            last_fill_str = last_fill_dt.strftime("%Y-%m-%dT%H:%M:%SZ") if last_fill_dt else None

            update_db_hwm("equities_hwm", asset_id, symbol, new_hwm, loss_limit, last_fill_str)
            floor_price = new_hwm * (1.0 - loss_limit)

            summary.append({
                "asset_class": "TRADFI",
                "asset_id": asset_id,
                "symbol": symbol,
                "live_price": live_price,
                "hwm": new_hwm,
                "loss_limit_pct": round(loss_limit * 100, 2),
                "floor_price": round(floor_price, 2),
                "vol_basis": vol_basis
            })

    return summary


async def sync_crypto_positions():
    if not os.path.exists(DB_PATH):
        return []

    summary = []
    STABLECOINS = {"USDT", "USDC", "USDE", "USDS", "DAI", "FDUSD", "TUSD", "USDP", "USDD"}

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT token, quantity, avg_entry_price 
                FROM portfolio 
                WHERE quantity > 0 
                  AND token NOT IN ('USDT', 'USDC', 'USDE', 'USDS', 'DAI', 'FDUSD', 'TUSD', 'USDP', 'USDD')
            """)
            active_positions = cursor.fetchall()

        if not active_positions:
            return []

        # v1.4: venue-pool parity (was hardcoded kucoin -> binance; BEAT/USDT
        # is KuCoin-only, so a degraded KuCoin session plus a Binance
        # BadSymbol failover left the ratchet blind for exactly that pair).
        # Fresh async instance per venue per pass - the pattern that kept
        # pricing BEAT while the shield's long-lived session failed - with
        # per-venue isolation and a deterministic close() on every path
        # (fixes the exception-path aiohttp leak: close previously ran only
        # on the happy path at the end of the pass).
        import ccxt.async_support as async_ccxt
        venues = price_pool.get_venue_list()
        exchanges = {}
        try:
            for venue in venues:
                venue_cls = getattr(async_ccxt, venue, None)
                if venue_cls is not None:
                    exchanges[venue] = venue_cls({'enableRateLimit': True})

            stored_map = get_stored_hwm_map("crypto_hwm")

            with get_db_connection() as conn:
                for pos in active_positions:
                    token = pos[0]
                    qty = float(pos[1])
                    avg_entry = float(pos[2])
                    pair = f"{token}/USDT"

                    asset_id = get_or_create_asset_id(conn, pair, asset_class="CRYPTO", broker="kucoin", exchange="BINANCE", currency="USDT")

                    live_price = None
                    bars = []
                    first_error = None
                    served_by = None

                    # Fetch live price & 1m OHLCV bars from the first pool venue
                    # that serves the pair (per-venue isolation: a BadSymbol on
                    # binance no longer masks a working kucoin/bybit read).
                    for venue in venues:
                        exchange = exchanges.get(venue)
                        if exchange is None:
                            continue
                        try:
                            ticker = await exchange.fetch_ticker(pair)
                            live_price = float(ticker['last'])
                            ohlcv = await exchange.fetch_ohlcv(pair, timeframe='1m', limit=1440)
                            bars = [{"c": c[4]} for c in ohlcv]
                            served_by = venue
                            break
                        except Exception as e:
                            if first_error is None:
                                first_error = f"{type(e).__name__}: {e}"
                            continue

                    if served_by is not None and served_by != venues[0]:
                        logger.info(f"[i] {pair} market data served by {served_by} (primary {venues[0]} failed: {first_error})")

                    if live_price is None:
                        if first_error is not None:
                            logger.warning(f"[!] Failed fetching crypto market data for {pair} across {len(venues)} venues (first error: {first_error})")
                        continue

                    # v1.2: track basis for crypto too (1m bars normally return 1440)
                    vol_basis = "crypto_1m" if len(bars) >= 120 else "fallback_8pct"

                    # v1.1: 5.0x daily vol, clamp 6-16% - crypto entries ride 4h momentum
                    # for multi-day horizons; the old 3-8% corridor was a noise-band exit
                    # (34 stop-outs in 2 weeks, incl. WBTC stopped below the August run).
                    loss_limit = calculate_24h_rolling_volatility_stop(
                        bars, multiplier=5.0, min_bound=0.060, max_bound=0.160
                    )
                    note_vol_basis_change(pair, vol_basis, loss_limit)

                    prev_info = stored_map.get(pair, {})
                    prev_hwm = prev_info.get("hwm", max(avg_entry, live_price))
                    new_hwm = max(prev_hwm, live_price)

                    update_db_hwm("crypto_hwm", asset_id, pair, new_hwm, loss_limit)
                    floor_price = new_hwm * (1.0 - loss_limit)

                    summary.append({
                        "asset_class": "CRYPTO",
                        "asset_id": asset_id,
                        "symbol": pair,
                        "live_price": live_price,
                        "hwm": new_hwm,
                        "loss_limit_pct": round(loss_limit * 100, 2),
                        "floor_price": round(floor_price, 2),
                        "vol_basis": vol_basis
                    })

        finally:
            for exchange in exchanges.values():
                try:
                    maybe = exchange.close()
                    if asyncio.iscoroutine(maybe) or asyncio.isfuture(maybe):
                        await maybe
                except Exception:
                    pass

    except Exception as e:
        logger.error(f"[!] Crypto positions HWM sync exception: {e}")

    return summary


async def run_update_sweep():
    logger.info("[*] Executing 1-minute HWM & 24h rolling volatility calibration pass...")
    alpaca_client = get_client_by_name("alpaca")

    tradfi_summary = await sync_tradfi_positions(alpaca_client)
    crypto_summary = await sync_crypto_positions()

    combined_positions = tradfi_summary + crypto_summary
    total_active = len(combined_positions)
    basis_counts = {}
    for p in combined_positions:
        b = p.get("vol_basis", "unknown")
        basis_counts[b] = basis_counts.get(b, 0) + 1
    logger.info(f"[+] HWM state sync complete. Active positions updated: {total_active} (TradFi: {len(tradfi_summary)}, Crypto: {len(crypto_summary)}). Vol basis: {basis_counts}")

    push_mqtt_telemetry({
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "SYNC_COMPLETE",
        "active_positions_count": total_active,
        "positions": combined_positions
    })

    # v1.4: daemon heartbeat - an hwm_updater outage silently degrades every
    # trailing stop to static (ratchet frozen, corridors stale); component_health
    # makes that outage visible to the dashboard instead of discoverable only
    # by reading the journal. Telemetry-only: nothing gates on this row yet.
    try:
        rrt.write_component_heartbeat(
            "hwm_updater", "healthy",
            f"active={total_active} tradfi={len(tradfi_summary)} crypto={len(crypto_summary)}")
    except Exception:
        pass


async def main():
    parser = argparse.ArgumentParser(description="M.A.C.E. 1-Minute HWM Updater")
    parser.add_argument("--daemon", action="store_true", default=False, help="Run as continuous daemon")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval in seconds")
    args = parser.parse_args()

    logger.info(f"[*] Booting M.A.C.E. HWM Updater Daemon (Interval: {args.interval}s)...")
    while True:
        try:
            await run_update_sweep()
        except Exception as e:
            logger.error(f"[!] Exception in HWM update loop: {e}")
            try:
                rrt.write_component_heartbeat("hwm_updater", "degraded", f"sweep exception: {e}")
            except Exception:
                pass

        if not args.daemon:
            break
        await asyncio.sleep(args.interval)


if __name__ == "__main__":
    asyncio.run(main())
