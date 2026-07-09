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

DB_PATH = "/home/fedora/MACE/config/portfolio.db"

def get_alpaca_context():
    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")
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
        sys.stderr.write(f"[!] Order retrieval anomaly for {symbol}: {e}\n")
    return None

def calculate_volatility_stop(bars):
    """
    Computes standard deviation of log returns across historical bars
    to create an explicit, personalized asset protection buffer.
    """
    try:
        closes = [float(bar["c"]) for bar in bars]
        if len(closes) < 10:
            return 0.05  # Standard 5% fallback if data depth fails

        log_returns = np.diff(np.log(closes))
        # Measure standard deviation (market noise proxy)
        sample_std = np.std(log_returns)

        # Multiply standard deviation to sit outside standard intra-day noise (3-Sigma rule)
        # Bounded explicitly between a tight 3.0% and an 8.0% high-beta max limit
        computed_limit = float(sample_std * 3.0)
        return max(0.030, min(computed_limit, 0.080))
    except Exception:
        return 0.05

def evaluate_position_risk(symbol, ctx):
    url = f"{ctx['base_url']}/v2/positions/{symbol}"
    try:
        response = requests.get(url, headers=ctx["headers"], timeout=10)

        if response.status_code == 404:
            with sqlite3.connect(DB_PATH) as conn:
                conn.cursor().execute("DELETE FROM equities_hwm WHERE symbol = ?", (symbol,))
                conn.commit()
            return

        if response.status_code != 200:
            return

        position_data = response.json()
        live_price = float(position_data["current_price"])
        now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        # Request 1Min historical bar matrix since order execution time window
        fill_time_dt = get_max_fill_time(symbol, ctx)
        if not fill_time_dt:
            fill_time_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            fill_time_str = fill_time_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        bars_url = "https://data.alpaca.markets/v2/stocks/bars"
        params = {
            "symbols": symbol,
            "timeframe": "1Min",
            "start": fill_time_str,
            "end": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "feed": "iex"
        }

        # Pull 1m historical segments to track trailing high peaks
        max_high = live_price
        bars_resp = requests.get(bars_url, headers=ctx["headers"], params=params, timeout=10)
        if bars_resp.status_code == 200:
            bars = bars_resp.json().get("bars", {}).get(symbol, [])
            if bars:
                max_high = max([float(bar["h"]) for bar in bars])

        # Request separate 15-minute array blocks to execute the volatility math logic
        params["timeframe"] = "15Min"
        params["start"] = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%SZ')

        vol_loss_limit = 0.05
        vol_resp = requests.get(bars_url, headers=ctx["headers"], params=params, timeout=10)
        if vol_resp.status_code == 200:
            vol_bars = vol_resp.json().get("bars", {}).get(symbol, [])
            vol_loss_limit = calculate_volatility_stop(vol_bars)

        # Upsert metrics straight to SQLite relational memory matrix row
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

        sys.stderr.write(f"[*] Dynamic Calibration [{symbol}] Calculated Noise Buffer Stop-Loss: {vol_loss_limit*100:.2f}%\n")

    except Exception as e:
        sys.stderr.write(f"[!] Risk engine tracking crash for {symbol}: {e}\n")

def fetch_alpha_stream(symbol, ctx):
    url = "https://data.alpaca.markets/v2/stocks/bars"
    params = {
        "symbols": symbol,
        "timeframe": "15Min",
        "start": (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "end": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        "feed": "iex"
    }
    try:
        response = requests.get(url, headers=ctx["headers"], params=params, timeout=15)
        if response.status_code == 200:
            bars = response.json().get("bars", {}).get(symbol, [])
            if bars:
                payload = {
                    "symbol": symbol,
                    "timestamps": [b["t"] for bar in bars],
                    "open": [float(b["o"]) for b in bars],
                    "high": [float(b["h"]) for b in bars],
                    "low": [float(b["l"]) for b in bars],
                    "close": [float(b["c"]) for b in bars],
                    "volume": [int(b["v"]) for b in bars]
                }
                print(json.dumps(payload))
    except Exception as e:
        print(json.dumps({"symbol": symbol, "error": str(e)}))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    target_symbol = sys.argv[1].upper()
    ctx = get_alpaca_context()
    if ctx:
        evaluate_position_risk(target_symbol, ctx)
        fetch_alpha_stream(target_symbol, ctx)
