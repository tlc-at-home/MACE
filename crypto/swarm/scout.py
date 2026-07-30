import sys
import os
import json
import asyncio
import sqlite3
import numpy as np
from datetime import datetime, timezone
import ccxt.async_support as ccxt

async def fetch_market_data(symbol, timeframe='4h', limit=540):
    """
    Pure Data Agent: Connects to KuCoin via CCXT, fetches OHLCV, and outputs a clean JSON payload.
    Looks back exactly 3 months (540 4-hour candles) to provide statistically robust data for the HMM.
    """
    exchange = ccxt.kucoin({
        'enableRateLimit': True,
    })

    try:
        # Fetch OHLCV data (Timestamp, Open, High, Low, Close, Volume)
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        close_prices = [candle[4] for candle in ohlcv]

        payload = {
            "symbol": symbol,
            "prices": close_prices
        }
        print(json.dumps(payload))

    except Exception as e:
        print(json.dumps({"symbol": symbol, "error": str(e)}))
    finally:
        await exchange.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No asset symbol provided to Scout."}))
        sys.exit(1)

    target_symbol = sys.argv[1]
    asyncio.run(fetch_market_data(target_symbol))
