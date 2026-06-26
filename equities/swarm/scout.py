import sys
import os
import json
import requests
from datetime import datetime

def fetch_market_data(symbol):
    """
    Pure Data Agent: Connects to Alpaca Market Data, fetches historical daily bars since 2007,
    and outputs a clean JSON payload of closing prices to stdout.
    """
    api_key = os.environ.get("ALPACA_API_KEY", "PKPQBCDBNXHU4XUXRDUB7AFIEF")
    secret_key = os.environ.get("ALPACA_SECRET_KEY", "3GZDRF2b1fxKrmvNLp9ZdQRo6rceDw4KFve9W9kYS1R9")
    url = "https://data.alpaca.markets/v2/stocks/bars"
    
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key
    }
    
    # 19-Year Horizon (Captures 2008 Great Financial Crisis)
    start_date = "2007-01-01T00:00:00Z"
    end_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    
    params = {
        "symbols": symbol,
        "timeframe": "1Day",
        "start": start_date,
        "end": end_date,
        "feed": "iex"  # REQUIRED FOR FREE TRADFI DATA
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code != 200:
            print(json.dumps({"symbol": symbol, "error": f"Alpaca API error: HTTP {response.status_code} - {response.text}"}))
            return
            
        response_data = response.json()
        
        if "bars" not in response_data or not response_data["bars"] or symbol not in response_data["bars"]:
            print(json.dumps({"symbol": symbol, "error": f"No bar data returned for symbol {symbol}."}))
            return
            
        bars = response_data["bars"][symbol]
        # Extract purely the closing prices for the Brain's HMM calculations
        close_prices = [bar["c"] for bar in bars]
        
        payload = {
            "symbol": symbol,
            "prices": close_prices
        }
        
        # Print perfectly formatted JSON to standard output (stdout)
        print(json.dumps(payload))
        
    except Exception as e:
        # If the API fails, output a JSON error so the swarm doesn't crash
        print(json.dumps({"symbol": symbol, "error": str(e)}))

if __name__ == "__main__":
    # The Scout expects the target asset to be passed as a command-line argument
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No asset symbol provided to Scout."}))
        sys.exit(1)
        
    target_symbol = sys.argv[1]
    fetch_market_data(target_symbol)
