#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 - Equities Swarm Scout Engine (Dynamic Volatility Calibration)
Queries 15-minute candle blocks spanning 30 days, derives asset-specific
Maximum Adverse Excursion / ATR bands, and registers custom loss thresholds to DB.
"""

import sys
import os
import json
import sqlite3
import requests
import numpy as np
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

def get_alpaca_context():
    # Clean environmental values of literal surrounding quotes if present
    api_key = os.environ.get("ALPACA_API_KEY", "").strip("'\"")
    secret_key = os.environ.get("ALPACA_SECRET_KEY", "").strip("'\"")
    is_paper = os.environ.get("ALPACA_PAPER_TRADE", "true").lower() == "true"
    base_url = "https://paper-api.alpaca.markets" if is_paper else "https://api.alpaca.markets"

    if not api_key or not secret_key:
        return None
    return {"headers": {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key}, "base_url": base_url}

def get_max_fill_time(symbol, ctx):
    url = f"{ctx['base_url']}/v2/orders"
    params = {"status": "filled", "limit": 20, "symbols": symbol}
    try:
        response = requests.get(url, headers=ctx["headers"], params=params, timeout=10)
        if response.status_code == 200:
            orders = response.json()
            buy_orders = [o for o in orders if o["side"] == "buy"]
            if buy_orders:
                buy_orders.sort(key=lambda x: x["filled_at"], reverse=True)
                raw_time = buy_orders[0]["filled_at"].split(".")[0].replace("Z", "")
                return datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception as e:
        sys.stderr.write(f"[!] Order history search drop for {symbol}: {e}\n")
    return None

def calculate_volatility_stop(bars):
    try:
        closes = [float(bar["c"]) for bar in bars]
        if len(closes) < 10:
            return 0.05
        log_returns = np.diff(np.log(closes))
        return max(0.030, min(float(np.std(log_returns) * 3.0), 0.080))
    except Exception:
        return 0.05

def evaluate_position_risk(symbol, ctx):
    url = f"{ctx['base_url']}/v2/positions/{symbol}"
    try:
        response = requests.get(url, headers=ctx["headers"], timeout=10)

        # Safe exit if position does not exist (0 position account alignment)
        if response.status_code == 404:
            if os.path.exists(DB_PATH):
                with sqlite3.connect(DB_PATH) as conn:
                    conn.cursor().execute("DELETE FROM equities_hwm WHERE symbol = ?", (symbol,))
                    conn.commit()
            return

        if response.status_code != 200:
            return

        position_data = response.json()
        live_price = float(position_data["current_price"])
        now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        fill_time_dt = get_max_fill_time(symbol, ctx)
        fill_time_str = fill_time_dt.strftime('%Y-%m-%dT%H:%M:%SZ') if fill_time_dt else now_str

        bars_url = "https://data.alpaca.markets/v2/stocks/bars"
        params = {
            "symbols": symbol,
            "timeframe": "1Min",
            "start": fill_time_str,
            "end": now_str,
            "feed": "iex"
        }

        max_high = live_price
        bars_resp = requests.get(bars_url, headers=ctx["headers"], params=params, timeout=10)
        if bars_resp.status_code == 200:
            bars = bars_resp.json().get("bars", {}).get(symbol, [])
            if bars:
                max_high = max([float(bar["h"]) for bar in bars])

        params["timeframe"] = "15Min"
        params["start"] = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%SZ')

        vol_loss_limit = 0.05
        vol_resp = requests.get(bars_url, headers=ctx["headers"], params=params, timeout=10)
        if vol_resp.status_code == 200:
            vol_bars = vol_resp.json().get("bars", {}).get(symbol, [])
            vol_loss_limit = calculate_volatility_stop(vol_bars)

        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO equities_hwm (symbol, last_fill_time, high_water_mark, loss_limit, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    high_water_mark = MAX(high_water_mark, excluded.high_water_mark, ?),
                    loss_limit = excluded.loss_limit,
                    updated_at = excluded.updated_at;
            """, (symbol, fill_time_str, max_high, vol_loss_limit, now_str, live_price))
            conn.commit()

    except Exception as e:
        sys.stderr.write(f"[!] Position risk processing exception for {symbol}: {e}\n")

def fetch_alpha_stream(symbol, ctx):
    url = "https://data.alpaca.markets/v2/stocks/bars"
    # url = "https://data.alpaca.markets/v2/stocks/bars"

    # Isolate a purely timezone-aware UTC clock object first
    now_utc = datetime.now(timezone.utc)
    #start_date = (now_utc - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
    #end_date = now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    # Shift lookback window back to secure a populated trading window on weekends
    start_date = (now_utc - timedelta(days=33)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_date   = (now_utc - timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%SZ')

    params = {
        "symbols": symbol,
        "timeframe": "15Min",
        "start": start_date,
        "end": end_date,
        "feed": "sip"
    }
    try:
        response = requests.get(url, headers=ctx["headers"], params=params, timeout=15)
        if response.status_code != 200:
            sys.stderr.write(f"[-] Data API error {response.status_code} for {symbol}\n")
            return

        res_data = response.json()
        bars = res_data.get("bars", {}).get(symbol, [])
        if not bars:
            return

        # FIXED: Loop mappings correctly aligned to use 'bar' index variable
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
    ctx = get_alpaca_context()

    if not ctx:
        sys.stderr.write(f"[!] Invalid context or keys for ticker context: {target_symbol}\n")
        sys.exit(1)

    evaluate_position_risk(target_symbol, ctx)
    fetch_alpha_stream(target_symbol, ctx)
