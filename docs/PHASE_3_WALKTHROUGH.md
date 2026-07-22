# Walkthrough - Unified Database Architecture, Views & Post-Liquidation Cooldown

Successfully implemented the unified database architecture, referential integrity, database views, post-liquidation cooldown lock, and routine profit-taking features for MACE.

## Summary of Completed Changes

### 1. Unified `asset_universe` Schema & Database Views
- **Unified Table**: Created `asset_universe` in `config/portfolio.db`:
  - `asset_id` INTEGER PRIMARY KEY AUTOINCREMENT
  - `symbol` TEXT NOT NULL
  - `asset_class` TEXT NOT NULL (`TRADFI` or `CRYPTO`)
  - `asset_name`, `category`, `broker`, `exchange`, `currency`
  - Composite Uniqueness: `UNIQUE(symbol, broker, exchange)` supporting identical tickers across different brokers.
- **Referential Integrity**: Enabled `PRAGMA foreign_keys = ON;`. Child tables (`trade_cooldowns`, `equities_hwm`, `crypto_hwm`) enforce foreign key constraints referencing `asset_universe(asset_id)` with `ON DELETE CASCADE`.
- **Created 5 Database Views (`vw_*`)**:
  - `vw_tradfi_universe`: Filters TradFi assets.
  - `vw_crypto_universe`: Filters Crypto assets.
  - `vw_active_cooldowns`: Filters active non-expired cooldown locks (`cooldown_until > UTC now`).
  - `vw_equities_risk_corridors`: Pre-computes calculated `stop_floor_price` for equities.
  - `vw_crypto_risk_corridors`: Pre-computes calculated `stop_floor_price` for crypto.

### 2. Post-Liquidation Cooldown & Re-Entry Lock (Hedge-Fund Quarantine)
- **Automatic Cooldown Registration**:
  - `tradfi_shield.py` & `crypto_shield.py`: Registers a **24-hour lock** (`reason: STOP_LOSS_BREACH`) upon trailing stop-loss breach.
  - `tradfi_news_guard.py`: Registers a **24-hour lock** (`reason: QUALITATIVE_NEWS_THREAT`) inside `close_position_tool` upon existential threat liquidations.
- **BUY Gate Filtration**:
  - `portfolio_allocator.py` & `guardrail.py`: Queries `vw_active_cooldowns` and automatically filters out locked assets from BUY approvals.

### 3. Routine Profit-Taking & Weight Rebalancing
- Audits existing held positions against Kelly target allocation caps.
- Generates partial `SELL` (trim) orders when a position value surges >15% over its target allocation weight to lock in profits.
- Profit-taking trims do **NOT** trigger a 24-hour risk cooldown lock, allowing normal holding and rebalancing.

---

## Verification Results

```
=================================================================================
TEST # | COMPONENT               | VERIFICATION RESULT                | STATUS
=================================================================================
1      | Unified Universe        | asset_universe total rows: 192     | ✅ PASSED
       |                         | (100 TradFi, 92 Crypto)            |
---------------------------------------------------------------------------------
2      | Database Views          | - vw_tradfi_universe: 100 rows     | ✅ PASSED
       |                         | - vw_crypto_universe: 92 rows      |
       |                         | - vw_active_cooldowns: 0 active    |
       |                         | - vw_equities_risk_corridors: 6    |
       |                         | - vw_crypto_risk_corridors: 21     |
---------------------------------------------------------------------------------
3      | Foreign Key Integrity   | FOREIGN KEY constraint failed      | ✅ PASSED
       | Enforcement             | (Rejected non-existent asset_id)   |
---------------------------------------------------------------------------------
4      | Cooldown Registration & | Inserted AAPL 24h cooldown lock;   | ✅ PASSED
       | View Retrieval          | Query via vw_active_cooldowns OK  |
=================================================================================
```
