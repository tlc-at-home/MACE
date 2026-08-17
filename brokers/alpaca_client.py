import os
import sys
import requests
from datetime import datetime, timezone
from .base import BrokerClient

class AlpacaClient(BrokerClient):
    def __init__(self):
        self.api_key = os.environ.get("ALPACA_API_KEY", "").strip("'\"")
        self.secret_key = os.environ.get("ALPACA_SECRET_KEY", "").strip("'\"")
        self.is_paper = os.environ.get("ALPACA_PAPER_TRADE", "true").lower() == "true"
        self.base_url = "https://paper-api.alpaca.markets" if self.is_paper else "https://api.alpaca.markets"
        self.data_url = "https://data.alpaca.markets"
        self.headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key
        }

    def _check_credentials(self):
        if not self.api_key or not self.secret_key:
            raise ValueError("Alpaca API credentials missing from environment.")

    def get_account(self) -> dict:
        self._check_credentials()
        url = f"{self.base_url}/v2/account"
        resp = requests.get(url, headers=self.headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "equity": float(data.get("equity", 0.0)),
                "cash": float(data.get("cash", 0.0))
            }
        resp.raise_for_status()

    def get_positions(self) -> list[dict]:
        self._check_credentials()
        url = f"{self.base_url}/v2/positions"
        resp = requests.get(url, headers=self.headers, timeout=10)
        if resp.status_code == 200:
            raw_positions = resp.json()
            return [
                {
                    "symbol": p["symbol"],
                    "qty": float(p["qty"]),
                    "current_price": float(p["current_price"]),
                    "avg_entry_price": float(p["avg_entry_price"]),
                    "market_value": float(p["market_value"])
                }
                for p in raw_positions
            ]
        resp.raise_for_status()

    def get_position(self, symbol: str) -> dict | None:
        self._check_credentials()
        url = f"{self.base_url}/v2/positions/{symbol}"
        resp = requests.get(url, headers=self.headers, timeout=10)
        if resp.status_code == 404:
            return None
        if resp.status_code == 200:
            p = resp.json()
            return {
                "symbol": p["symbol"],
                "qty": float(p["qty"]),
                "current_price": float(p["current_price"]),
                "avg_entry_price": float(p["avg_entry_price"]),
                    "market_value": float(p["market_value"])
            }
        resp.raise_for_status()

    def get_historical_bars(self, symbol: str, timeframe: str, start: str, end: str, feed: str = "iex") -> list[dict]:
        self._check_credentials()
        url = f"{self.data_url}/v2/stocks/bars"
        params = {
            "symbols": symbol,
            "timeframe": timeframe,
            "start": start,
            "end": end,
            "feed": feed or "iex"
        }

        resp = requests.get(url, headers=self.headers, params=params, timeout=15)
        if resp.status_code == 403 and params.get("feed") != "iex":
            params["feed"] = "iex"
            resp = requests.get(url, headers=self.headers, params=params, timeout=15)

        if resp.status_code == 200:
            res_data = resp.json()
            bars = res_data.get("bars", {}).get(symbol, [])
            return [
                {
                    "t": bar["t"],
                    "o": float(bar["o"]),
                    "h": float(bar["h"]),
                    "l": float(bar["l"]),
                    "c": float(bar["c"]),
                    "v": int(bar["v"])
                }
                for bar in bars
            ]
        resp.raise_for_status()

    def get_last_fill_time(self, symbol: str) -> datetime | None:
        self._check_credentials()
        url = f"{self.base_url}/v2/orders"
        params = {"status": "filled", "limit": 20, "symbols": symbol}
        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=10)
            if resp.status_code == 200:
                orders = resp.json()
                buy_orders = [o for o in orders if o.get("side") == "buy"]
                if buy_orders:
                    buy_orders.sort(key=lambda x: x["filled_at"], reverse=True)
                    raw_time = buy_orders[0]["filled_at"].split(".")[0].replace("Z", "")
                    return datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception as e:
            sys.stderr.write(f"[!] Order history search drop for {symbol}: {e}\n")
        return None

    def place_order(self, symbol: str, notional: float, side: str, type: str, time_in_force: str) -> dict:
        self._check_credentials()
        url = f"{self.base_url}/v2/orders"
        order_data = {
            "symbol": symbol,
            "notional": notional,
            "side": side,
            "type": type,
            "time_in_force": time_in_force
        }
        resp = requests.post(url, headers=self.headers, json=order_data, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        resp.raise_for_status()

    def close_position(self, symbol: str) -> None:
        self._check_credentials()
        url = f"{self.base_url}/v2/positions/{symbol}"
        resp = requests.delete(url, headers=self.headers, timeout=10)
        if resp.status_code in (200, 204):
            return
        resp.raise_for_status()

    def get_market_hours(self, symbol: str) -> dict:
        self._check_credentials()
        url = f"{self.base_url}/v2/clock"
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                return {"is_open": bool(data.get("is_open", False)), "exchange": "NYSE"}
        except Exception:
            pass
        return {"is_open": True, "exchange": "NYSE"}
