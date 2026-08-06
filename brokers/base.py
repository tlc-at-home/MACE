import os
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

class BrokerClient(ABC):
    @abstractmethod
    def get_account(self) -> dict:
        """Returns account metrics: {"equity": float, "cash": float}"""
        pass

    @abstractmethod
    def get_positions(self) -> list[dict]:
        """Returns list of open positions: [{"symbol": str, "qty": float, "current_price": float, "avg_entry_price": float}]"""
        pass

    @abstractmethod
    def get_position(self, symbol: str) -> dict | None:
        """Returns position details for symbol or None if not held."""
        pass

    @abstractmethod
    def get_historical_bars(self, symbol: str, timeframe: str, start: str, end: str, feed: str = None) -> list[dict]:
        """Returns historical bars: [{"t": str, "o": float, "h": float, "l": float, "c": float, "v": int}]"""
        pass

    @abstractmethod
    def get_last_fill_time(self, symbol: str) -> datetime | None:
        """Returns datetime of the last fill for the given symbol."""
        pass

    @abstractmethod
    def close_position(self, symbol: str) -> None:
        """Closes the position for the given symbol."""
        pass

    @abstractmethod
    def get_market_hours(self, symbol: str) -> dict:
        """Returns market status for symbol: {"is_open": bool, "exchange": str}"""
        pass


_CLIENT_CACHE = {}

def get_client_by_name(name: str) -> BrokerClient:
    broker_name = (name or "alpaca").lower()
    if broker_name not in _CLIENT_CACHE:
        if broker_name == "alpaca":
            from .alpaca_client import AlpacaClient
            _CLIENT_CACHE[broker_name] = AlpacaClient()
        else:
            raise ValueError(f"Unsupported broker: {broker_name}")
    return _CLIENT_CACHE[broker_name]

def get_client_for_symbol(symbol: str, db_path: str = DEFAULT_DB_PATH) -> BrokerClient:
    broker_name = "alpaca"
    if os.path.exists(db_path):
        try:
            with sqlite3.connect(db_path, timeout=10.0) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT broker FROM vw_equities_universe WHERE symbol = ?", (symbol,))
                row = cursor.fetchone()
                if row and row[0]:
                    broker_name = row[0]
        except Exception:
            pass
    return get_client_by_name(broker_name)
