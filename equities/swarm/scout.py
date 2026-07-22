#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 - Equities Swarm Scout Engine (Dynamic Volatility Calibration)
Queries 15-minute candle blocks spanning 30 days, derives asset-specific
Maximum Adverse Excursion / ATR bands, and registers custom loss thresholds to DB.
Refactored to use BrokerClient abstraction layer.
"""

import sys
import os
import json
import sqlite3
import numpy as np
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from brokers import get_client_for_symbol

DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

def calculate_volatility_stop(bars):
    try:
        closes = [float(bar["c"]) for bar in bars]
        if len(closes) < 10:
            return 0.05
        log_returns = np.diff(np.log(closes))
        return max(0.030, min(float(np.std(log_returns) * 3.0), 0.080))
    except Exception:
        return 0.05

def evaluate_position_risk(symbol, client):
    try:
        position_data = client.get_position(symbol)

        # Safe exit if position does not exist (0 position account alignment)
        if not position_data:
            if os.path.exists(DB_PATH):
                with sqlite3.connect(DB_PATH) as conn:
                    conn.cursor().execute("DELETE FROM equities_hwm WHERE symbol = ?", (symbol,))
                    conn.commit()
            return

        live_price = position_data["current_price"]
        now_dt = datetime.now(timezone.utc)
        now_str = now_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        fill_time_dt = client.get_last_fill_time(symbol)
        fill_time_str = fill_time_dt.strftime('%Y-%m-%dT%H:%M:%SZ') if fill_time_dt else now_str

        max_high = live_price
        try:
            bars = client.get_historical_bars(symbol, timeframe="1Min", start=fill_time_str, end=now_str, feed="iex")
            if bars:
                max_high = max([float(bar["h"]) for bar in bars])
        except Exception as e:
            sys.stderr.write(f"[!] 1Min bar fetch drop for {symbol}: {e}\n")

        vol_loss_limit = 0.05
        try:
            start_30d = (now_dt - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
            vol_bars = client.get_historical_bars(symbol, timeframe="15Min", start=start_30d, end=now_str)
            if vol_bars:
                vol_loss_limit = calculate_volatility_stop(vol_bars)
        except Exception as e:
            sys.stderr.write(f"[!] 15Min bar fetch drop for {symbol}: {e}\n")

        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            cursor = conn.cursor()
            cursor.execute("SELECT asset_id FROM vw_tradfi_universe WHERE symbol = ?", (symbol,))
            res = cursor.fetchone()
            if res:
                asset_id = res[0]
                cursor.execute("""
                    INSERT INTO equities_hwm (asset_id, symbol, last_fill_time, high_water_mark, loss_limit, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(asset_id) DO UPDATE SET
                        high_water_mark = MAX(high_water_mark, excluded.high_water_mark, ?),
                        loss_limit = excluded.loss_limit,
                        updated_at = excluded.updated_at;
                """, (asset_id, symbol, fill_time_str, max_high, vol_loss_limit, now_str, live_price))
                conn.commit()

    except Exception as e:
        sys.stderr.write(f"[!] Position risk processing exception for {symbol}: {e}\n")

def fetch_alpha_stream(symbol, client):
    now_utc = datetime.now(timezone.utc)
    start_date = (now_utc - timedelta(days=33)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_date   = (now_utc - timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%SZ')

    try:
        bars = client.get_historical_bars(symbol, timeframe="15Min", start=start_date, end=end_date, feed="sip")
        if not bars:
            return

        payload = {
            "symbol": symbol,
            "timestamps": [bar["t"] for bar in bars],
            "open": [float(bar["o"]) for bar in bars],
            "high": [float(bar["h"]) for bar in bars],
            "low": [float(bar["l"]) for bar in bars],
            "close": [float(bar["c"]) for bar in bars],
            "volume": [int(bar["v"]) for bar in bars]
        }

        print(json.dumps(payload))

    except Exception as e:
        sys.stderr.write(f"[!] Alpha generation exception for {symbol}: {e}\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)

    target_symbol = sys.argv[1].upper()
    client = get_client_for_symbol(target_symbol)

    evaluate_position_risk(target_symbol, client)
    fetch_alpha_stream(target_symbol, client)
