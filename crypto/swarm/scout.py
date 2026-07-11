import sys
import os
import json
import asyncio
import sqlite3
import numpy as np
from datetime import datetime, timezone
import ccxt.async_support as ccxt

async def evaluate_position_risk(symbol, exchange):
    """
    Calculates dynamic trailing stop-loss percentage from volatility of 1h candles over 30 days.
    """
    token = symbol.split("/")[0]
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    db_path = os.path.join(base_dir, "config/portfolio.db")

    try:
        if not os.path.exists(db_path):
            return

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT quantity, avg_entry_price FROM portfolio WHERE token = ?", (token,))
            row = cursor.fetchone()
            if not row or float(row[0]) <= 0:
                cursor.execute("DELETE FROM crypto_hwm WHERE symbol = ?", (symbol,))
                conn.commit()
                return

            quantity = float(row[0])
            avg_entry_price = float(row[1])

        # Fetch current price via exchange
        ticker = await exchange.fetch_ticker(symbol)
        current_price = float(ticker['last'])

        # Fetch 1h candles for the last 30 days (720 candles)
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe='1h', limit=720)
        closes = [candle[4] for candle in ohlcv]

        if len(closes) >= 10:
            log_returns = np.diff(np.log(closes))
            loss_limit = max(0.030, min(float(np.std(log_returns) * 3.0), 0.080))
        else:
            loss_limit = 0.08

        now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT high_water_mark FROM crypto_hwm WHERE symbol = ?", (symbol,))
            hwm_row = cursor.fetchone()
            if hwm_row:
                old_hwm = float(hwm_row[0])
                new_hwm = max(old_hwm, current_price)
            else:
                new_hwm = max(avg_entry_price, current_price)

            cursor.execute("""
                INSERT INTO crypto_hwm (symbol, high_water_mark, loss_limit, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    high_water_mark = MAX(high_water_mark, excluded.high_water_mark),
                    loss_limit = excluded.loss_limit,
                    updated_at = excluded.updated_at;
            """, (symbol, new_hwm, loss_limit, now_str))
            conn.commit()

    except Exception as e:
        sys.stderr.write(f"[!] Position risk processing exception for {symbol}: {e}\n")

async def fetch_market_data(symbol, timeframe='4h', limit=540):
    """
    Pure Data Agent: Connects to KuCoin via CCXT, fetches OHLCV, and outputs a clean JSON payload.
    Looks back exactly 3 months (540 4-hour candles) to provide statistically robust data for the HMM.
    """
    exchange = ccxt.kucoin({
        'enableRateLimit': True,
    })

    try:
        # Evaluate dynamic position risk first
        await evaluate_position_risk(symbol, exchange)

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
