#!/usr/bin/env python3.11
"""
M.A.C.E. Volatility Auditor - Portfolio Maximum Adverse Excursion (MAE)
Analyzes historical 1-minute resolution profiles since your exact entry fill times
to see if your 8% trailing stop-loss is optimally placed or giving up too much alpha.
"""

import os
import sys
import sqlite3
import requests
import numpy as np
from datetime import datetime, timezone

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

def get_context():
    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")
    is_paper = os.environ.get("ALPACA_PAPER_TRADE", "true").lower() == "true"
    base_url = "https://paper-api.alpaca.markets" if is_paper else "https://api.alpaca.markets"
    if not api_key or not secret_key:
        return None
    return {"headers": {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key}, "base_url": base_url}

def audit():
    ctx = get_context()
    if not ctx:
        print("[!] Missing Alpaca context keys in environment.")
        return

    if not os.path.exists(DB_PATH):
        print(f"[!] Database not found at {DB_PATH}")
        return

    # Ingest historical records managed by scout.py via view
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT symbol, last_fill_time, high_water_mark, loss_limit, stop_floor_price FROM vw_equities_risk_corridors")
            rows = cursor.fetchall()
        except sqlite3.OperationalError:
            print("[!] View 'vw_equities_risk_corridors' does not exist yet or is empty.")
            return

    if not rows:
        print("[*] No active equity positions logged in tracking table.")
        return

    print("=" * 90)
    print(f"{'SYMBOL':<8} | {'ENTRY TIME':<20} | {'PEAK HIGH':<10} | {'MAX INTRA-POSITION PULLBACK':<22} | {'SUGGESTED STOP'}")
    print("=" * 90)

    for symbol, last_fill_time, db_hwm, loss_limit, stop_floor_price in rows:
        # Fetch high-resolution bars since fill
        url = "https://data.alpaca.markets/v2/stocks/bars"
        params = {
            "symbols": symbol,
            "timeframe": "1Min",
            "start": last_fill_time,
            "end": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "feed": "iex"
        }

        try:
            resp = requests.get(url, headers=ctx["headers"], params=params, timeout=10)
            if resp.status_code != 200:
                continue

            bars = resp.json().get("bars", {}).get(symbol, [])
            if not bars:
                print(f"{symbol:<8} | No intra-position candles found since fill time.")
                continue

            # Reconstruct the chronological peak and tracking pullbacks
            running_high = float(bars[0]["h"])
            max_drawdown_pct = 0.0
            closes = []

            for bar in bars:
                h = float(bar["h"])
                l = float(bar["l"])
                closes.append(float(bar["c"]))

                if h > running_high:
                    running_high = h

                # Check how low it dropped relative to the running peak high up to that point
                drawdown_from_peak = (running_high - l) / running_high
                if drawdown_from_peak > max_drawdown_pct:
                    max_drawdown_pct = drawdown_from_peak

            # Calculate a rolling Standard Deviation of returns as a proxy for asset noise
            log_returns = np.diff(np.log(closes)) if len(closes) > 1 else [0]
            volatility_envelope_pct = np.std(log_returns) * 100 * np.sqrt(390) if len(log_returns) > 0 else 0.0 # Daily noise proxy

            # Quantitative recommendation ruleset
            max_dd_pct = max_drawdown_pct * 100
            if max_dd_pct < 2.5 and volatility_envelope_pct < 2.0:
                recommended_stop = "3.5% to 4.0% (Low Volatility Blue Chip)"
            elif max_dd_pct < 4.5:
                recommended_stop = "5.0% (Standard Equities Corridor)"
            else:
                recommended_stop = "8.0% (High-Beta Momentum Volatility)"

            print(f"{symbol:<8} | {last_fill_time:<20} | ${db_hwm:<9.2f} | -{max_dd_pct:<21.2f}% | {recommended_stop}")

        except Exception as e:
            print(f"[!] Failed to parse metrics for {symbol}: {e}")

    print("=" * 90)

if __name__ == "__main__":
    audit()
