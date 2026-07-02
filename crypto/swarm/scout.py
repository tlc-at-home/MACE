import sys
import json
import asyncio
import ccxt.async_support as ccxt

async def fetch_market_data(symbol, timeframe='4h', limit=540):
    """
    Pure Data Agent: Connects to KuCoin via CCXT, fetches OHLCV, and outputs a clean JSON payload.
    Looks back exactly 3 months (540 4-hour candles) to provide statistically robust data for the HMM.
    """
    # Initialize KuCoin global liquidity access
    # In production, load API keys from your .env if using private execution endpoints
    exchange = ccxt.kucoin({
        'enableRateLimit': True,
    })

    try:
        # Fetch OHLCV data (Timestamp, Open, High, Low, Close, Volume)
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

        # Extract purely the closing prices for the Brain's HMM calculations
        close_prices = [candle[4] for candle in ohlcv]

        # Format the exact JSON payload the Brain expects
        payload = {
            "symbol": symbol,
            "prices": close_prices
        }

        # Print perfectly formatted JSON to standard output (stdout)
        print(json.dumps(payload))

    except Exception as e:
        # If the exchange fails, output a JSON error so the swarm doesn't crash
        print(json.dumps({"symbol": symbol, "error": str(e)}))
    finally:
        await exchange.close()

if __name__ == "__main__":
    # The Scout expects the target asset to be passed as a command-line argument
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No asset symbol provided to Scout."}))
        sys.exit(1)

    target_symbol = sys.argv[1]

    # Run the async dragnet
    asyncio.run(fetch_market_data(target_symbol))
