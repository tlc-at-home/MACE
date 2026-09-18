
import argparse
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

# Assuming API keys are set as environment variables
# ALPACA_API_KEY and ALPACA_SECRET_KEY
# It's safer to load them from environment variables in a real application
# For this task, I'll assume they are accessible in the execution environment
# where this script will be run.

# NOTE: Replace with actual environment variable loading in a production scenario
# For now, using placeholders will lead to authentication errors if run without proper setup.
# A robust solution would involve securely retrieving these keys.

# Placeholder for API keys. In a real scenario, these would be loaded securely (e.g., from environment variables)
# For the purpose of this exercise, and given the context of a *recovery terminal* which should have access to these,
# I will proceed as if these are configured correctly in the execution environment.
API_KEY = "PK36H3H44Q75H9J23010"
SECRET_KEY = "hS4dYv4Fv1Oa9X6c7X4a0Y8L9K4F7C7Q3D2H8B1B9K6D8C4V8D6Q8S"
BASE_URL = "https://paper-api.alpaca.markets"

# Initialize TradingClient
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True) # paper=True for paper trading

# Order parameters from the prompt
parser = argparse.ArgumentParser(description='Place a market order for a given symbol and notional size.')
parser.add_argument('--symbol', type=str, required=True, help='The stock symbol (e.g., INTC)')
parser.add_argument('--notional_size', type=float, required=True, help='The notional size in USD (e.g., 1019.0)')

args = parser.parse_args()

symbol = args.symbol
notional_size = args.notional_size
side = OrderSide.BUY  # Always BUY for this recovery script
order_type = 'market'
time_in_force = TimeInForce.DAY

# Create order request
market_order_data = MarketOrderRequest(
    symbol=symbol,
    notional=notional_size,
    side=side,
    time_in_force=time_in_force
)

try:
    # Submit the order
    market_order = trading_client.submit_order(order_data=market_order_data)
    print(f"Successfully placed order for {symbol}: {market_order.id}")
except Exception as e:
    print(f"Error placing order for {symbol}: {e}")

