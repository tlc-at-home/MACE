# Implementation Plan - Unified Database Schema, Views, Post-Liquidation Cooldown & Routine Profit-Taking

Implement a clean, unified database architecture with database views, full referential integrity, and institutional risk governance across TradFi and Crypto swarms.

## User Review Required

> [!IMPORTANT]
> **Unified `asset_universe` Table & Multi-Broker Symbol Uniqueness**:
> - Primary key is an auto-incrementing unique integer `asset_id`.
> - Composite uniqueness constraint: `UNIQUE(symbol, broker, exchange)` supporting duplicate tickers across different brokers.
> 
> **Database Views (`vw_*`) & Code Integration**:
> - `vw_tradfi_universe`: Used by `equities/swarm/orchestrator.py`, `equities/tradfi_news_guard.py`, and `brokers/base.py`.
> - `vw_crypto_universe`: Used by `crypto/swarm/orchestrator.py`.
> - `vw_active_cooldowns`: Used by `equities/swarm/portfolio_allocator.py` and `crypto/swarm/guardrail.py`.
> - `vw_equities_risk_corridors`: Used by `equities/tradfi_shield.py` and `equities/swarm/check_pullbacks.py`.
> - `vw_crypto_risk_corridors`: Used by `crypto/crypto_shield.py`.
> 
> **Referential Integrity (Foreign Keys)**: Enable SQLite foreign key constraints (`PRAGMA foreign_keys = ON;`). All relational tables (`equities_hwm`, `crypto_hwm`, `trade_cooldowns`) will reference `asset_universe(asset_id)` as a foreign key.

## Proposed Changes

---

### 1. Database Schema, Views & Migration

#### [NEW] [migrate_asset_universe.py](file:///home/tony/dev/MACE-LOCAL/migrate_asset_universe.py)
- Migration script to consolidate existing database tables into the unified `asset_universe` schema and create views:
  ```sql
  PRAGMA foreign_keys = ON;

  -- 1. Primary Universe Table
  CREATE TABLE IF NOT EXISTS asset_universe (
      asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol TEXT NOT NULL,
      asset_class TEXT NOT NULL,  -- 'TRADFI' or 'CRYPTO'
      asset_name TEXT,
      category TEXT,
      broker TEXT NOT NULL,       -- 'alpaca', 'binance', 'ibkr', 'moomoo'
      exchange TEXT NOT NULL,     -- 'SMART', 'BINANCE', 'LSE', 'ASX'
      currency TEXT NOT NULL,     -- 'USD', 'USDT', 'GBP', 'AUD'
      UNIQUE(symbol, broker, exchange)
  );

  -- 2. Relational Child Tables
  CREATE TABLE IF NOT EXISTS trade_cooldowns (
      cooldown_id INTEGER PRIMARY KEY AUTOINCREMENT,
      asset_id INTEGER NOT NULL,
      symbol TEXT NOT NULL,
      closed_at TEXT NOT NULL,
      reason TEXT NOT NULL,
      cooldown_until TEXT NOT NULL,
      FOREIGN KEY (asset_id) REFERENCES asset_universe(asset_id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS equities_hwm (
      asset_id INTEGER PRIMARY KEY,
      symbol TEXT NOT NULL,
      last_fill_time TEXT,
      high_water_mark REAL NOT NULL,
      loss_limit REAL NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY (asset_id) REFERENCES asset_universe(asset_id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS crypto_hwm (
      asset_id INTEGER PRIMARY KEY,
      symbol TEXT NOT NULL,
      high_water_mark REAL NOT NULL,
      loss_limit REAL NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY (asset_id) REFERENCES asset_universe(asset_id) ON DELETE CASCADE
  );

  -- 3. Optimization Database Views
  CREATE VIEW IF NOT EXISTS vw_tradfi_universe AS
  SELECT asset_id, symbol, broker, exchange, currency, asset_name, category
  FROM asset_universe WHERE asset_class = 'TRADFI';

  CREATE VIEW IF NOT EXISTS vw_crypto_universe AS
  SELECT asset_id, symbol, broker, exchange, currency AS pair, asset_name, category
  FROM asset_universe WHERE asset_class = 'CRYPTO';

  CREATE VIEW IF NOT EXISTS vw_active_cooldowns AS
  SELECT c.cooldown_id, c.asset_id, c.symbol, a.asset_class, a.broker, a.exchange, c.closed_at, c.reason, c.cooldown_until
  FROM trade_cooldowns c
  JOIN asset_universe a ON c.asset_id = a.asset_id
  WHERE c.cooldown_until > strftime('%Y-%m-%dT%H:%M:%SZ', 'now');

  CREATE VIEW IF NOT EXISTS vw_equities_risk_corridors AS
  SELECT h.asset_id, h.symbol, a.broker, a.exchange, a.currency, h.last_fill_time, h.high_water_mark, h.loss_limit,
         (h.high_water_mark * (1.0 - h.loss_limit)) AS stop_floor_price, h.updated_at
  FROM equities_hwm h
  JOIN asset_universe a ON h.asset_id = a.asset_id;

  CREATE VIEW IF NOT EXISTS vw_crypto_risk_corridors AS
  SELECT h.asset_id, h.symbol, a.broker, a.exchange, h.high_water_mark, h.loss_limit,
         (h.high_water_mark * (1.0 - h.loss_limit)) AS stop_floor_price, h.updated_at
  FROM crypto_hwm h
  JOIN asset_universe a ON h.asset_id = a.asset_id;
  ```

#### [MODIFY] [guardrail.py](file:///home/tony/dev/MACE-LOCAL/crypto/swarm/guardrail.py)
- In `get_db_connection()`, execute `PRAGMA foreign_keys = ON;` upon connection.
- In `init_db()`, define `asset_universe`, child tables, and all 5 database views.

---

### 2. Swarm & Orchestrators Refactoring

#### [MODIFY] [equities/swarm/orchestrator.py](file:///home/tony/dev/MACE-LOCAL/equities/swarm/orchestrator.py)
- Update `load_tradfi_universe()`: query `SELECT symbol FROM vw_tradfi_universe ORDER BY symbol`.

#### [MODIFY] [crypto/swarm/orchestrator.py](file:///home/tony/dev/MACE-LOCAL/crypto/swarm/orchestrator.py)
- Update `load_universe()`: query `SELECT pair FROM vw_crypto_universe ORDER BY symbol`.

#### [MODIFY] [equities/tradfi_news_guard.py](file:///home/tony/dev/MACE-LOCAL/equities/tradfi_news_guard.py)
- Update `load_tradfi_universe_metadata()`: query `SELECT asset_id, symbol, broker, exchange, currency FROM vw_tradfi_universe`.

#### [MODIFY] [brokers/base.py](file:///home/tony/dev/MACE-LOCAL/brokers/base.py)
- Update `get_client_for_symbol()`: query `SELECT broker FROM vw_tradfi_universe WHERE symbol = ?`.

---

### 3. Risk Layer & Shield Views Refactoring

#### [MODIFY] [equities/tradfi_shield.py](file:///home/tony/dev/MACE-LOCAL/equities/tradfi_shield.py)
- Update `get_position_metrics(symbol)`: query `SELECT high_water_mark, loss_limit, stop_floor_price FROM vw_equities_risk_corridors WHERE symbol = ?`.
- Upon stop-loss breach, insert into `trade_cooldowns` (`asset_id`, `symbol`, `closed_at`, `reason: STOP_LOSS_BREACH`, `cooldown_until`: UTC now + 24 hours).

#### [MODIFY] [crypto/crypto_shield.py](file:///home/tony/dev/MACE-LOCAL/crypto/crypto_shield.py)
- Update metrics lookups: query `SELECT high_water_mark, loss_limit, stop_floor_price FROM vw_crypto_risk_corridors WHERE symbol = ?`.
- Upon stop-loss breach, insert into `trade_cooldowns`.

#### [MODIFY] [equities/swarm/check_pullbacks.py](file:///home/tony/dev/MACE-LOCAL/equities/swarm/check_pullbacks.py)
- Update audit query: `SELECT symbol, last_fill_time, high_water_mark, loss_limit, stop_floor_price FROM vw_equities_risk_corridors`.

---

### 4. Feature: Post-Liquidation Cooldown Lock & Profit-Taking

#### [MODIFY] [portfolio_allocator.py](file:///home/tony/dev/MACE-LOCAL/equities/swarm/portfolio_allocator.py)
- Query active cooldown symbols from `vw_active_cooldowns` and filter out locked candidates in Pass 1.
- Audit currently held positions in Pass 2: generate trim `SELL` orders for positions surging >15% over target allocation without setting a cooldown lock.

#### [MODIFY] [guardrail.py](file:///home/tony/dev/MACE-LOCAL/crypto/swarm/guardrail.py)
- Check `vw_active_cooldowns` in `run_piped_risk_gate()`. Return `"gate_closed"` if symbol is in active cooldown.
- Audit existing crypto holdings for profit-taking trim thresholds.

## Verification Plan

### Automated Tests
1. **Migration & Views Verification**:
   - Run `python3 migrate_asset_universe.py`.
   - Verify `asset_universe` rows, `asset_id` primary key, `UNIQUE(symbol, broker, exchange)`, and all 5 view outputs (`vw_tradfi_universe`, `vw_crypto_universe`, `vw_active_cooldowns`, `vw_equities_risk_corridors`, `vw_crypto_risk_corridors`).
2. **Referential Integrity Test**:
   - Attempt inserting an entry into `trade_cooldowns` with a non-existent `asset_id`; verify SQLite raises `sqlite3.IntegrityError`.
3. **View Query Verification**:
   - Query `vw_equities_risk_corridors` and `vw_crypto_risk_corridors` to verify pre-calculated `stop_floor_price`.
4. **Cooldown & Profit-Taking Tests**:
   - Verify candidate rejection when symbol is present in `vw_active_cooldowns`.
   - Verify trim order generation for over-allocated position without creating a cooldown entry.
