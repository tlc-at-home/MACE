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

def fetch_alpha_stream(symbol, client):
    now_utc = datetime.now(timezone.utc)
    start_date = (now_utc - timedelta(days=200)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_date   = now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    try:
        bars = client.get_historical_bars(symbol, timeframe="1Day", start=start_date, end=end_date, feed="sip")
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

    fetch_alpha_stream(target_symbol, client)
