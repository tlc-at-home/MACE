-- ============================================================================
-- M.A.C.E. Quantitative Engine - Complete Database Setup Script
-- System: Local Development Environment
-- Database: /home/tony/dev/MACE-LOCAL/config/portfolio.db
-- Description: Defines all primary tables, indexes, and views for the M.A.C.E. system.
-- ============================================================================

PRAGMA foreign_keys = ON;

-- ----------------------------------------------------------------------------
-- 1. Core Asset Registry & Candidate Tables
-- ----------------------------------------------------------------------------

-- Master Static Asset Directory (TRADFI Equities and CRYPTO Pairs)
CREATE TABLE IF NOT EXISTS asset_universe (
    asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    asset_class TEXT NOT NULL,             -- 'TRADFI' or 'CRYPTO'
    asset_name TEXT,
    category TEXT,
    broker TEXT NOT NULL,
    exchange TEXT NOT NULL,
    currency TEXT NOT NULL,
    UNIQUE(symbol, broker, exchange)
);

-- Dynamic Scouted Whale Trades Table (Political & Smart Money Intelligence)
CREATE TABLE IF NOT EXISTS equities_whale_universe (
    asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,                  -- e.g. 'Finnhub_API', 'HouseStockWatcher_S3'
    politician TEXT,                       -- Filing lawmaker / insider entity
    transaction_type TEXT,                 -- 'BUY', 'OPTION_PURCHASE'
    amount_range TEXT,                     -- Reported size range
    broker TEXT DEFAULT 'ALPACA',
    exchange TEXT DEFAULT 'NASDAQ',
    currency TEXT DEFAULT 'USD',
    asset_name TEXT,
    category TEXT DEFAULT 'WHALE_SCOUT',
    first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 2. Risk Management & Trailing Stop High-Water Mark (HWM) Tables
-- ----------------------------------------------------------------------------

-- Equities High-Water Mark & Trailing Stop Loss Tracker
CREATE TABLE IF NOT EXISTS equities_hwm (
    asset_id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    last_fill_time TEXT,
    high_water_mark REAL NOT NULL,
    loss_limit REAL NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (asset_id) REFERENCES asset_universe(asset_id) ON DELETE CASCADE
);

-- Crypto High-Water Mark & Trailing Stop Loss Tracker
CREATE TABLE IF NOT EXISTS crypto_hwm (
    asset_id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    high_water_mark REAL NOT NULL,
    loss_limit REAL NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (asset_id) REFERENCES asset_universe(asset_id) ON DELETE CASCADE
);

-- Post-Liquidation 24-Hour Risk Lock Cooldowns
CREATE TABLE IF NOT EXISTS trade_cooldowns (
    cooldown_id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    closed_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    cooldown_until TEXT NOT NULL,
    FOREIGN KEY (asset_id) REFERENCES asset_universe(asset_id) ON DELETE CASCADE
);

-- ----------------------------------------------------------------------------
-- 3. Execution, Portfolio & Audit Log Tables
-- ----------------------------------------------------------------------------

-- MCP Agent Execution Audit Log
CREATE TABLE IF NOT EXISTS mcp_execution_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    trade_id INTEGER,
    timestamp TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    arguments TEXT NOT NULL,
    status TEXT NOT NULL,
    result TEXT
);

-- MCP Requested Trade Queue
CREATE TABLE IF NOT EXISTS mcp_requested_trades (
    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    action TEXT NOT NULL,                  -- 'BUY' or 'SELL'
    qty REAL,
    amount_usd REAL,
    status TEXT NOT NULL,                  -- 'PENDING', 'EXECUTED', 'FAILED'
    updated_at TEXT NOT NULL
);

-- Crypto Wallet Balances
CREATE TABLE IF NOT EXISTS wallets (
    blockchain TEXT PRIMARY KEY,
    public_key TEXT NOT NULL,
    gas_balance REAL NOT NULL,
    gas_token TEXT NOT NULL
);

-- On-Chain Portfolio Positions
CREATE TABLE IF NOT EXISTS portfolio (
    blockchain TEXT NOT NULL,
    token TEXT NOT NULL,
    quantity REAL NOT NULL,
    avg_entry_price REAL NOT NULL,
    high_water_mark REAL DEFAULT 0,
    PRIMARY KEY (blockchain, token)
);

-- ----------------------------------------------------------------------------
-- 4. Database Indexes for Query Optimization
-- ----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_asset_universe_class ON asset_universe(asset_class);
CREATE INDEX IF NOT EXISTS idx_equities_whale_symbol ON equities_whale_universe(symbol);
CREATE INDEX IF NOT EXISTS idx_trade_cooldowns_symbol ON trade_cooldowns(symbol);

-- ----------------------------------------------------------------------------
-- 5. Unified System Database Views
-- ----------------------------------------------------------------------------

-- 1) Unified Equities View (Combines Static TradFi assets + Scouted Whale assets)
DROP VIEW IF EXISTS vw_tradfi_universe;
DROP VIEW IF EXISTS vw_equities_universe;

CREATE VIEW vw_equities_universe AS
SELECT 
    asset_id,
    symbol,
    'static' AS source,
    broker,
    exchange,
    currency,
    asset_name,
    category,
    NULL AS politician,
    NULL AS transaction_type
FROM asset_universe
WHERE asset_class = 'TRADFI'
  AND symbol NOT IN (SELECT symbol FROM equities_whale_universe)
UNION ALL
SELECT 
    asset_id,
    symbol,
    source,
    broker,
    exchange,
    currency,
    asset_name,
    category,
    politician,
    transaction_type
FROM equities_whale_universe;

-- 2) Crypto Asset Universe View
DROP VIEW IF EXISTS vw_crypto_universe;

CREATE VIEW vw_crypto_universe AS
SELECT 
    asset_id,
    symbol,
    broker,
    exchange,
    currency AS pair,
    asset_name,
    category
FROM asset_universe 
WHERE asset_class = 'CRYPTO';

-- 3) Active Trade Cooldowns View (Filters locks where cooldown_until > current time)
DROP VIEW IF EXISTS vw_active_cooldowns;

CREATE VIEW vw_active_cooldowns AS
SELECT 
    c.cooldown_id,
    c.asset_id,
    c.symbol,
    a.asset_class,
    a.broker,
    a.exchange,
    c.closed_at,
    c.reason,
    c.cooldown_until
FROM trade_cooldowns c
JOIN asset_universe a ON c.asset_id = a.asset_id
WHERE c.cooldown_until > strftime('%Y-%m-%dT%H:%M:%SZ', 'now');

-- 4) Equities Trailing Risk Corridors View
DROP VIEW IF EXISTS vw_equities_risk_corridors;

CREATE VIEW vw_equities_risk_corridors AS
SELECT 
    h.asset_id,
    h.symbol,
    a.broker,
    a.exchange,
    a.currency,
    h.last_fill_time,
    h.high_water_mark,
    h.loss_limit,
    (h.high_water_mark * (1.0 - h.loss_limit)) AS stop_floor_price,
    h.updated_at
FROM equities_hwm h
JOIN asset_universe a ON h.asset_id = a.asset_id;

-- 5) Crypto Trailing Risk Corridors View
DROP VIEW IF EXISTS vw_crypto_risk_corridors;

CREATE VIEW vw_crypto_risk_corridors AS
SELECT 
    h.asset_id,
    h.symbol,
    a.broker,
    a.exchange,
    h.high_water_mark,
    h.loss_limit,
    (h.high_water_mark * (1.0 - h.loss_limit)) AS stop_floor_price,
    h.updated_at
FROM crypto_hwm h
JOIN asset_universe a ON h.asset_id = a.asset_id;
