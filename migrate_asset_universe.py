#!/usr/bin/env python3
import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

def migrate():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    # 1. Primary Universe Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS asset_universe (
            asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            asset_name TEXT,
            category TEXT,
            broker TEXT NOT NULL,
            exchange TEXT NOT NULL,
            currency TEXT NOT NULL,
            UNIQUE(symbol, broker, exchange)
        )
    """)

    # 2. Relational Child Tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trade_cooldowns (
            cooldown_id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            closed_at TEXT NOT NULL,
            reason TEXT NOT NULL,
            cooldown_until TEXT NOT NULL,
            FOREIGN KEY (asset_id) REFERENCES asset_universe(asset_id) ON DELETE CASCADE
        )
    """)

    # 3. Create temp tables for hwm migration if existing hwm tables don't have asset_id
    cursor.execute("PRAGMA table_info(equities_hwm)")
    eq_cols = [r[1] for r in cursor.fetchall()]
    old_eq_rows = []
    if "asset_id" not in eq_cols and "symbol" in eq_cols:
        cursor.execute("SELECT symbol, last_fill_time, high_water_mark, loss_limit, updated_at FROM equities_hwm")
        old_eq_rows = cursor.fetchall()
        cursor.execute("DROP TABLE equities_hwm")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS equities_hwm (
            asset_id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            last_fill_time TEXT,
            high_water_mark REAL NOT NULL,
            loss_limit REAL NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (asset_id) REFERENCES asset_universe(asset_id) ON DELETE CASCADE
        )
    """)

    cursor.execute("PRAGMA table_info(crypto_hwm)")
    cr_cols = [r[1] for r in cursor.fetchall()]
    old_cr_rows = []
    if "asset_id" not in cr_cols and "symbol" in cr_cols:
        cursor.execute("SELECT symbol, high_water_mark, loss_limit, updated_at FROM crypto_hwm")
        old_cr_rows = cursor.fetchall()
        cursor.execute("DROP TABLE crypto_hwm")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crypto_hwm (
            asset_id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            high_water_mark REAL NOT NULL,
            loss_limit REAL NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (asset_id) REFERENCES asset_universe(asset_id) ON DELETE CASCADE
        )
    """)

    # 4. Migrate old tradfi_universe into asset_universe
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tradfi_universe'")
    if cursor.fetchone():
        cursor.execute("SELECT symbol, broker, exchange, currency FROM tradfi_universe")
        for row in cursor.fetchall():
            cursor.execute("""
                INSERT OR IGNORE INTO asset_universe (symbol, asset_class, broker, exchange, currency)
                VALUES (?, 'TRADFI', ?, ?, ?)
            """, (row[0], row[1], row[2], row[3]))
        cursor.execute("DROP TABLE tradfi_universe")
        print("[+] Migrated tradfi_universe table to asset_universe.")

    # 5. Migrate old crypto_universe into asset_universe
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='crypto_universe'")
    if cursor.fetchone():
        cursor.execute("SELECT ticker, asset_name, category, pair FROM crypto_universe")
        for row in cursor.fetchall():
            ticker, asset_name, category, pair = row[0], row[1], row[2], row[3]
            cursor.execute("""
                INSERT OR IGNORE INTO asset_universe (symbol, asset_class, asset_name, category, broker, exchange, currency)
                VALUES (?, 'CRYPTO', ?, ?, 'binance', 'BINANCE', 'USDT')
            """, (pair, asset_name, category))
        cursor.execute("DROP TABLE crypto_universe")
        print("[+] Migrated crypto_universe table to asset_universe.")

    conn.commit()

    # Re-insert HWM rows with matching asset_id
    for row in old_eq_rows:
        sym = row[0]
        cursor.execute("SELECT asset_id FROM asset_universe WHERE symbol = ?", (sym,))
        res = cursor.fetchone()
        if res:
            cursor.execute("""
                INSERT OR REPLACE INTO equities_hwm (asset_id, symbol, last_fill_time, high_water_mark, loss_limit, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (res[0], sym, row[1], row[2], row[3], row[4]))

    for row in old_cr_rows:
        sym = row[0]
        cursor.execute("SELECT asset_id FROM asset_universe WHERE symbol = ?", (sym,))
        res = cursor.fetchone()
        if res:
            cursor.execute("""
                INSERT OR REPLACE INTO crypto_hwm (asset_id, symbol, high_water_mark, loss_limit, updated_at)
                VALUES (?, ?, ?, ?, ?)
            """, (res[0], sym, row[1], row[2], row[3]))

    conn.commit()

    # 6. Optimization Database Views
    cursor.execute("DROP VIEW IF EXISTS vw_tradfi_universe")
    cursor.execute("""
        CREATE VIEW vw_tradfi_universe AS
        SELECT asset_id, symbol, broker, exchange, currency, asset_name, category
        FROM asset_universe WHERE asset_class = 'TRADFI'
    """)

    cursor.execute("DROP VIEW IF EXISTS vw_crypto_universe")
    cursor.execute("""
        CREATE VIEW vw_crypto_universe AS
        SELECT asset_id, symbol, broker, exchange, currency AS pair, asset_name, category
        FROM asset_universe WHERE asset_class = 'CRYPTO'
    """)

    cursor.execute("DROP VIEW IF EXISTS vw_active_cooldowns")
    cursor.execute("""
        CREATE VIEW vw_active_cooldowns AS
        SELECT c.cooldown_id, c.asset_id, c.symbol, a.asset_class, a.broker, a.exchange, c.closed_at, c.reason, c.cooldown_until
        FROM trade_cooldowns c
        JOIN asset_universe a ON c.asset_id = a.asset_id
        WHERE c.cooldown_until > strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
    """)

    cursor.execute("DROP VIEW IF EXISTS vw_equities_risk_corridors")
    cursor.execute("""
        CREATE VIEW vw_equities_risk_corridors AS
        SELECT h.asset_id, h.symbol, a.broker, a.exchange, a.currency, h.last_fill_time, h.high_water_mark, h.loss_limit,
               (h.high_water_mark * (1.0 - h.loss_limit)) AS stop_floor_price, h.updated_at
        FROM equities_hwm h
        JOIN asset_universe a ON h.asset_id = a.asset_id
    """)

    cursor.execute("DROP VIEW IF EXISTS vw_crypto_risk_corridors")
    cursor.execute("""
        CREATE VIEW vw_crypto_risk_corridors AS
        SELECT h.asset_id, h.symbol, a.broker, a.exchange, h.high_water_mark, h.loss_limit,
               (h.high_water_mark * (1.0 - h.loss_limit)) AS stop_floor_price, h.updated_at
        FROM crypto_hwm h
        JOIN asset_universe a ON h.asset_id = a.asset_id
    """)

    conn.commit()
    conn.close()
    print("[+] Created asset_universe schema, foreign keys, and 5 database views.")

if __name__ == "__main__":
    migrate()
