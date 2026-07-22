#!/usr/bin/env python3
import os
import json
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")
TRADFI_JSON_PATH = os.path.join(BASE_DIR, "config/tradfi_universe.json")
CRYPTO_JSON_PATH = os.path.join(BASE_DIR, "config/crypto_universe.json")

def migrate():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Create tradfi_universe table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tradfi_universe (
            symbol TEXT PRIMARY KEY,
            broker TEXT NOT NULL DEFAULT 'alpaca',
            exchange TEXT NOT NULL DEFAULT 'SMART',
            currency TEXT NOT NULL DEFAULT 'USD'
        )
    """)

    # 2. Create crypto_universe table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crypto_universe (
            ticker TEXT PRIMARY KEY,
            asset_name TEXT,
            category TEXT,
            pair TEXT NOT NULL
        )
    """)
    conn.commit()

    # 3. Migrate TradFi Universe
    if os.path.exists(TRADFI_JSON_PATH):
        with open(TRADFI_JSON_PATH, "r") as f:
            tradfi_data = json.load(f)
        for item in tradfi_data:
            if isinstance(item, str):
                cursor.execute("""
                    INSERT OR REPLACE INTO tradfi_universe (symbol, broker, exchange, currency)
                    VALUES (?, 'alpaca', 'SMART', 'USD')
                """, (item,))
            elif isinstance(item, dict):
                cursor.execute("""
                    INSERT OR REPLACE INTO tradfi_universe (symbol, broker, exchange, currency)
                    VALUES (?, ?, ?, ?)
                """, (
                    item.get("symbol"),
                    item.get("broker", "alpaca"),
                    item.get("exchange", "SMART"),
                    item.get("currency", "USD")
                ))
        conn.commit()
        print(f"[+] Migrated TradFi universe entries to DB.")

    # 4. Migrate Crypto Universe
    if os.path.exists(CRYPTO_JSON_PATH):
        with open(CRYPTO_JSON_PATH, "r") as f:
            crypto_data = json.load(f)
        for item in crypto_data:
            if isinstance(item, dict) and "ticker" in item:
                ticker = item["ticker"]
                pair = ticker if "/" in ticker else f"{ticker}/USDT"
                cursor.execute("""
                    INSERT OR REPLACE INTO crypto_universe (ticker, asset_name, category, pair)
                    VALUES (?, ?, ?, ?)
                """, (
                    ticker,
                    item.get("asset_name", ""),
                    item.get("category", ""),
                    pair
                ))
        conn.commit()
        print(f"[+] Migrated Crypto universe entries to DB.")

    conn.close()

    # 5. Delete old JSON files
    if os.path.exists(TRADFI_JSON_PATH):
        os.remove(TRADFI_JSON_PATH)
        print(f"[+] Removed {TRADFI_JSON_PATH}")

    if os.path.exists(CRYPTO_JSON_PATH):
        os.remove(CRYPTO_JSON_PATH)
        print(f"[+] Removed {CRYPTO_JSON_PATH}")

if __name__ == "__main__":
    migrate()
