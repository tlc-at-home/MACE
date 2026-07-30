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


def calculate_24h_rolling_volatility_stop(bars, multiplier=2.5, min_bound=0.030, max_bound=0.080):
    """
    Calculates dynamic loss limit based on 24-hour rolling 1-minute log returns.
    Formula: horizon_volatility = std(log_returns) * sqrt(1440) * multiplier
    Clamped between min_bound (3%) and max_bound (8%).
    """
    try:
        closes = [float(b["c"]) for b in bars if "c" in b and float(b["c"]) > 0]
        if len(closes) < 120:
            return 0.040  # Fallback default 4.0% if insufficient bars exist (<120 mins)

        log_returns = np.diff(np.log(closes))
        sigma_1m = np.std(log_returns)

        # sqrt(1440 bars in 24h) ≈ 37.9473
        sqrt_24h = 37.947331922
        horizon_volatility = sigma_1m * sqrt_24h * multiplier

        return max(min_bound, min(float(horizon_volatility), max_bound))
    except Exception as e:
        logger.warning(f"[!] Exception calculating volatility stop: {e}")
        return min_bound


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

            # Request 1,440 1-minute bars for 24h volatility profile (using IEX feed for paper accounts)
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

            loss_limit = calculate_24h_rolling_volatility_stop(
                bars, multiplier=2.5, min_bound=0.030, max_bound=0.080
            )

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
                "floor_price": round(floor_price, 2)
            })

    return summary


async def run_update_sweep():
    logger.info("[*] Executing 1-minute HWM & 24h rolling volatility calibration pass...")
    alpaca_client = get_client_by_name("alpaca")

    tradfi_summary = await sync_tradfi_positions(alpaca_client)

    total_active = len(tradfi_summary)
    logger.info(f"[+] HWM state sync complete. Active positions updated: {total_active}")

    push_mqtt_telemetry({
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "SYNC_COMPLETE",
        "active_positions_count": total_active,
        "positions": tradfi_summary
    })


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

        if not args.daemon:
            break
        await asyncio.sleep(args.interval)


if __name__ == "__main__":
    asyncio.run(main())
