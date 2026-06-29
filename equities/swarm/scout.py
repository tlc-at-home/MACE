#!/usr/bin/env python3.11
import sys
import os
import json
import requests
from datetime import datetime, timedelta

def fetch_market_data(symbol):
    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")

    if not api_key or not secret_key:
        print(json.dumps({"symbol": symbol, "error": "Missing Alpaca API keys in environment."}))
        return

    url = "https://data.alpaca.markets/v2/stocks/bars"
    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key}

    # FIX: Reduced from 2007 to 2 years. HMMs don't need 19 years of data, it just clogs the pipes.
    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

    params = {"symbols": symbol, "timeframe": "1Day", "start": start_date, "end": end_date, "feed": "iex"}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code != 200:
            print(json.dumps({"symbol": symbol, "error": f"Alpaca API error: HTTP {response.status_code}"}))
            return

        response_data = response.json()
        if "bars" not in response_data or not response_data["bars"] or symbol not in response_data["bars"]:
            print(json.dumps({"symbol": symbol, "error": f"No bar data returned for {symbol}."}))
            return

        bars = response_data["bars"][symbol]
        close_prices = [bar["c"] for bar in bars]

        print(json.dumps({"symbol": symbol, "prices": close_prices}))

    except Exception as e:
        print(json.dumps({"symbol": symbol, "error": str(e)}))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No asset symbol provided to Scout."}))
        sys.exit(1)
    fetch_market_data(sys.argv[1])
