[mace_system_review.md](https://github.com/user-attachments/files/29602195/mace_system_review.md)
# M.A.C.E. (Momentum Autonomous Cognitive Engine) System Review

The **Momentum Autonomous Cognitive Engine (M.A.C.E.)** is a highly modular, multi-agent algorithmic trading infrastructure designed for decentralized cryptocurrency trading and TradFi equity markets. It combines state-of-the-art quantitative mathematical models (Gaussian Hidden Markov Models and dynamic fractional Kelly sizing) with autonomous qualitative risk analysis driven by large language models (LLMs).

This document provides an in-depth technical analysis of M.A.C.E.’s architecture, data flows, subsystems, strengths, and areas for improvement.

---

## 1. System Architecture Diagram

The diagram below illustrates the decoupled pipeline model of M.A.C.E., showcasing the division of labor between data scouts, quantitative brains, portfolio risk managers, and deterministic stop-loss shields.

```mermaid
graph TD
    %% Universes
    subgraph Universes
        U_TradFi[tradfi_universe.json]
        U_Crypto[crypto_universe.json]
    end

    %% TradFi Subsystem
    subgraph TradFi Subsystem [TradFi Equities Swarm]
        T_Orch[equities/swarm/orchestrator.py]
        T_Scout[equities/swarm/scout.py]
        T_Brain[equities/swarm/brain.py]
        T_Alloc[equities/swarm/portfolio_allocator.py]
        T_News[equities/tradfi_news_guard.py]
        T_Shield[equities/tradfi_shield.py]
        
        T_Orch -->|Spawns| T_Scout
        T_Scout -->|Pipe OHLCV| T_Brain
        T_Brain -->|Pipe Signals| T_Alloc
        T_Alloc -->|Pipes Approved Trades| T_Orch
        
        T_Orch -->|Alpaca MCP REST| Alpaca_API[(Alpaca Paper / Live API)]
        T_News -->|Alpaca News + REST| Alpaca_API
        T_Shield -->|Deterministic Stop-Loss| Alpaca_API
    end

    %% Crypto Subsystem
    subgraph Crypto Subsystem [Crypto Swarm]
        C_Orch[crypto/swarm/orchestrator.py]
        C_Scout[crypto/swarm/scout.py]
        C_Brain[crypto/swarm/brain.py]
        C_Guard[crypto/swarm/guardrail.py]
        C_Shield[crypto/crypto_shield.py]
        
        C_Orch -->|Spawns| C_Scout
        C_Scout -->|Pipe OHLCV| C_Brain
        C_Brain -->|Pipe Signals| C_Guard
        C_Guard -->|Simulates Trades| Portfolio_DB[(config/portfolio.db)]
        
        C_Shield -->|Determinstic Stop-Loss| Portfolio_DB
        C_Scout -->|CCXT Spot Prices| KuCoin_API[(KuCoin Spot API)]
        C_Shield -->|CCXT Spot Prices| Binance_API[(Binance Spot API)]
    end

    %% Centralized Infrastructure
    subgraph Central Infrastructure
        MQTT_Broker[[MQTT Telemetry Broker]]
        Systemd[Systemd Services]
    end

    %% Connect Telemetry
    T_Orch -.->|mace/telemetry/tradfi_sword| MQTT_Broker
    T_News -.->|mace/telemetry/tradfi_news_guard| MQTT_Broker
    T_Shield -.->|mace/telemetry/tradfi_shield| MQTT_Broker
    C_Orch -.->|mace/telemetry/crypto_sword| MQTT_Broker
    C_Shield -.->|mace/telemetry/crypto_shield| MQTT_Broker

    %% Service control
    Systemd ===>|Controls| T_Orch
    Systemd ===>|Controls| T_News
    Systemd ===>|Controls| T_Shield
    Systemd ===>|Controls| C_Orch
    Systemd ===>|Controls| C_Shield
```

---

## 2. Core Subsystems

### A. TradFi Equities Swarm
The TradFi pipeline operates on a periodic cycle (1h intervals in daemon mode), reading from a universe of 100 blue-chip stocks ([tradfi_universe.json](file:///mnt/MACE/config/tradfi_universe.json)).

1. **Data Scout ([scout.py](file:///mnt/MACE/equities/swarm/scout.py))**: An asynchronous data collector that fetches the last 2 years of daily OHLCV bar data from Alpaca's market data API and outputs a clean JSON payload to standard output.
2. **Quant Brain ([brain.py](file:///mnt/MACE/equities/swarm/brain.py))**: A math engine that consumes the Scout's output via standard input. It computes:
   - Rolling 20-day returns and 20-day volatility.
   - An empirical Markov transition matrix mapping historical state transitions (Bull, Bear, Sideways).
   - An independent **3-State Gaussian Hidden Markov Model (HMM)** fitted on returns and volatility to "confirm" the market regime.
   - A **Dynamic Half-Kelly Sizing** parameter. ⚠️ *Truth pass (v1.3)*: the win-rate/payoff priors are **hardcoded constants, not backtested values** (a prior audit found no backtest exists). Since v1.3 they are shrinkage-blended with **realized round-trip history** (see `realized_round_trips` table and `MACE_EMPIRICAL_KELLY_JSON`) — until at least `MACE_KELLY_MIN_ROUNDS` (default 10) realized exits exist, the priors remain in force. Sizing is scaled by signal strength (a Sharpe ratio proxy) and capped at 25%.
3. **Portfolio Allocator ([portfolio_allocator.py](file:///mnt/MACE/equities/swarm/portfolio_allocator.py))**: Takes the outputs of all candidate brains, filters out assets not confirmed to be in a "Bull" regime, clamps individual trade sizes to a maximum of 20% of total equity, and normalizes positions to fit within a 90% deployable cash limit.
4. **Orchestrator ([orchestrator.py](file:///mnt/MACE/equities/swarm/orchestrator.py))**: Manages the pipeline workflow. When trades are approved:
   - **Sell orders** are dispatched first via the direct `BrokerClient` path (`TRIM_PROFIT_TAKING` sells as market orders; full liquidations via `close_position`). The legacy Gemini-agent sell path referenced a tool that was never registered and was removed in v1.1.
   - **Buy orders** are placed next via the Alpaca paper REST API directly (default `MACE_BUY_DISPATCH=direct`, v1.2). The legacy Gemini agent dispatch (`MACE_BUY_DISPATCH=agent`) is retained as an escape hatch.
   - **News-guard fail-neutral gate (v1.3)**: if the TradFi news guard has not completed a fresh, healthy audit within `MACE_NEWS_STALENESS_HOURS` (default 12h), new entries are **held** (sells, trims, stop-outs and liquidations stay fully live). Set `MACE_NEWS_GATE=off` to restore the legacy fail-open behavior.
   - Emits structured state telemetry to the local MQTT broker.

### B. Crypto Swarm
The crypto pipeline is designed around a simulated multi-chain sandbox and runs on 4-hour UTC boundaries.

1. **Data Scout ([scout.py](file:///mnt/MACE/crypto/swarm/scout.py))**: Uses CCXT to fetch exactly 3 months (540 4-hour candles) of spot OHLCV data from KuCoin.
2. **Quant Brain ([brain.py](file:///mnt/MACE/crypto/swarm/brain.py))**: A pure 3-State Gaussian HMM fitted on 2D returns and rolling standard deviations. It calculates a Sharpe-scaled Half-Kelly fraction for assets in the "Bull" state.
3. **Guardrail / Virtual Ledger ([guardrail.py](file:///mnt/MACE/crypto/swarm/guardrail.py))**: Since crypto execution is simulated in this environment, this module acts as a **virtual ledger** backed by an SQLite database ([portfolio.db](file:///mnt/MACE/config/portfolio.db)). It stores:
   - Simulated wallet public keys and gas balances for Solana and Arbitrum.
   - Active coin balances, average entry prices, and available USDT cash (initialized at $10,000 USDT).
   - Applies portfolio constraints (max 25% single-asset exposure, min $10 trade sizing) and executes simulated BUY/SELL trades directly against the DB ledger.
   - **Taker fee (v1.3)**: fills are charged a configurable taker fee (default 0.1% = `MACE_TAKER_FEE`) — buys store a fee-inclusive cost basis, sells credit USDT net of fee — so simulated P&L tracks what a real spot account would realize. The crypto shield applies the same fee on stop-out exits (its settlement path bypasses the guardrail).
   - **Realized round trips (v1.3)**: every completed exit writes a `realized_round_trips` row feeding the empirical Kelly stats.
4. **Orchestrator ([orchestrator.py](file:///mnt/MACE/crypto/swarm/orchestrator.py))**: Loops through the crypto universe on UTC HH:05:00 boundaries, pipes OHLCV data into the brain subprocesses concurrently (capped with a semaphore of 10), evaluates results through the Guardrail, and dispatches telemetry. Ledger fills mark to a **venue pool** (default KuCoin → Binance → Bybit, `MACE_PRICE_VENUES`, v1.3) so a single-venue outage can no longer revert entries to the brain's stale 4h closes. Each sweep also purges virtual-ledger rows that no venue can price for `MACE_STALE_LEDGER_SWEEPS` (default 3) consecutive sweeps (stale-ledger write-down, recorded as a −100% round trip).

---

## 3. Risk Mitigation Shields

M.A.C.E. implements three parallel, asynchronous risk mitigation layers to protect capital against both sudden mathematical price drops and qualitative market panics.

### I. Volatility-Calibrated Crypto Shield ([crypto_shield.py](file:///mnt/MACE/crypto/crypto_shield.py))
* **Interval**: Runs every 15 minutes as a systemd service.
* **Mechanism**: Pulls active holdings from `portfolio.db`, fetches live spot prices via CCXT (KuCoin with Binance failover), and queries the `crypto_hwm` table.
* **Rule**: Enforces a dynamic, volatility-calibrated trailing stop-loss. Since v1.2 the per-position limit is recalibrated daily from genuine volatility: equities use a 20-day daily-bar basis (5%–12% corridor, clamped), crypto uses a 1-minute-bar trailing-24h basis (6%–16% corridor, clamped) — replacing the old static 8% (equities) and the 3–8% scout calibration (crypto).
* **Action**: If the drawdown from the High-Water Mark (HWM) breaches the limit, liquidates the position, resets HWM tracking, and returns recovered cash to the USDT wallet. Dynamically ratchets the HWM up if prices set new peaks.

### II. Deterministic TradFi Shield ([tradfi_shield.py](file:///mnt/MACE/equities/tradfi_shield.py))
* **Interval**: Runs every 1 minute.
* **Mechanism**: Connects to the Alpaca REST API.
* **Rule 1 (Portfolio-wide)**: If overall unrealized portfolio drawdown exceeds **5%**, triggers **full emergency liquidation** by closing all open positions.
* **Rule 2 (Single Asset)**: If any individual stock drawdown exceeds **8%**, closes that specific position.

### III. Qualitative TradFi News Guard ([tradfi_news_guard.py](file:///mnt/MACE/equities/tradfi_news_guard.py))
* **Interval**: Runs every 4 hours.
* **Mechanism**: Leverages LLMs to evaluate unstructured risk factors.
* **Workflow**:
  1. Fetches current Alpaca stock holdings.
  2. Aggregates recent headlines for held positions from **Yahoo Finance RSS** (⚠️ *truth pass*: this is the actual source in code — earlier revisions of this README claimed the Alpaca Data API news feed).
  3. Hands the aggregated context to a Gemini 2.5 Flash agent.
  4. The agent acts as an autonomous qualitative analyst, ignoring normal volatility but scanning for **existential threats** (e.g., bankruptcy, SEC fraud investigations, catastrophic product failures, CEO arrests).
  5. If a severe threat is found, the agent calls the guard's own `close_position_tool` for that symbol immediately (which also registers a 24h `QUALITATIVE_NEWS_THREAT` cooldown row).
  6. **Health heartbeat (v1.3)**: every audit cycle upserts a `component_health` row (`healthy` / `degraded` / `idle`). The equities orchestrator's fail-neutral buy gate reads it — while the sensor is blind (e.g. the Gemini key is rate-capped), new buys are held instead of silently proceeding as if every audit had come back clean.

---

## 4. Telemetry and System Control

All services run as background daemons orchestrated by Systemd configurations. They report real-time analytics to a centralized MQTT broker, allowing external dashboards to monitor system state.

### System Control Commands
* **Start Fleet**: [`start_all.sh`](file:///mnt/MACE/start_all.sh)
* **Stop Fleet**: [`stop_all.sh`](file:///mnt/MACE/stop_all.sh)

| Daemon Service | Target Script | Run Frequency | Telemetry Topic |
| :--- | :--- | :--- | :--- |
| `mace-hwm-updater.service` | `hwm_stop_updater.py` | 1 minute | `mace/telemetry/hwm_updater` |
| `mace-crypto-shield.service` | `crypto/crypto_shield.py` | 15 minutes | `mace/telemetry/crypto_shield` |
| `mace-tradfi-shield.service` | `equities/tradfi_shield.py` | 1 minute | `mace/telemetry/tradfi_shield` |
| `mace-crypto-orchestrator.service` | `crypto/swarm/orchestrator.py` | 4 hours (UTC synchronized) | `mace/telemetry/crypto_sword` |
| `mace-equities-orchestrator.service` | `equities/swarm/orchestrator.py` | 1 hour | `mace/telemetry/tradfi_sword` |
| `mace-tradfi-news-guard.service` | `equities/tradfi_news_guard.py` | 4 hours | `mace/telemetry/tradfi_news_guard` |
| `mace-whales-scout.timer` / `.service` | `equities/swarm/whales_scout.py` | Twice daily (US trading days) | `mace/telemetry/whales_scout` |

---

## 5. Architectural Strengths

* **Unix Piping & Low Coupling**: The Scout, Brain, and Allocator/Guardrail subcomponents communicate strictly via standard Unix I/O piping. This provides high isolation—making it easy to swap out the quantitative HMM model in `brain.py` with a deep-learning or statistical model without rewriting any data collection or risk management code.
* **Centralized Risk Allocation**: Sizing is decoupled from individual alpha generation. No single agent can over-allocate capital because the final decisions are evaluated by a centralized Allocator that respects total account equity and cash budgets.
* **Sharpe-Scaled Sizing**: Using the ratio of rolling returns over rolling standard deviations (Sharpe proxy) as a confidence scaling multiplier for Half-Kelly sizing is a highly effective, modern approach to protecting capital from low-volatility traps.
* **Hybrid Risk Paradigm**: Combining deterministic stop-losses (Shields) with qualitative sentiment analysis (News Guard) protects the fund against both instant flash crashes and slow-burning fundamental deterioration (like structural fraud or SEC crackdowns).

---

## 6. Recommendations for Improvement

> [!NOTE]
> Below are structural optimizations and operational enhancements that can be made to increase reliability and scalability.

1. **SQLite Concurrent Write Safety** [RESOLVED]:
   * Previously, `portfolio.db` suffered from write failures due to a faulty `?nolock=1` SQLite parameter and connection leaks when scripts encountered exceptions.
   * *Resolution*: Removed `?nolock=1` (which blocks write transactions), implemented standard SQLite connections with a robust `timeout=30.0` retry queue, and added strict try/finally cleanup structures to prevent resource leaks under exceptions.

2. **API Rate-Limiting & Jitter Management**:
   * The equities orchestrator scans assets in parallel. Although defensive rate-limiting spacing (`asyncio.sleep(0.2)`) and semaphore limiting are applied, large universes can still hit Alpaca and CCXT rate-limits.
   * *Recommendation*: Implement centralized token-bucket rate limiters in the scouts or coordinate queries using global connection pools.

3. **Error Resilience in MCP Sessions** [RESOLVED]:
   * Previously, `execute_mcp_agent` relied on raw network retries but lacked structured recovery from partial execution failures (e.g. if one order fails while others succeed).
   * *Resolution*: Implemented lifecycle hooks (`PreToolCall`, `PostToolCall`, `OnToolError`) backed by a persistent execution log (`mcp_requested_trades` and `mcp_execution_log`) to track intended vs. executed trades. Added an automated recovery loop that detects failed trades and dispatches up to 3 retries using a targeted recovery agent.

---

## 7. MCP Session Error Resilience & Automated Recovery

To prevent partial execution failures (e.g., half-filled sweeps or failed liquidations), the system integrates structured lifecycle hooks from the Google Antigravity SDK and a persistent recovery state engine.

```mermaid
graph TD
    A[Start Orchestrator / News Guard Sweep] --> B[Generate Unique Run ID]
    B --> C[Compute Target Allocations]
    C --> D[Write Intended Trades to mcp_requested_trades as PENDING]
    D --> E[Launch Gemini 2.5 Flash MCP Agent]
    E --> F{Tool Call Triggered?}
    F -- Yes --> G[PreToolCallHook: Record args/name in context]
    G --> H[Execute Tool Call]
    H --> I[PostToolCallHook: Log to mcp_execution_log]
    I --> J{Tool Success?}
    J -- Yes --> K[Update trade status to COMPLETED]
    J -- No --> L[Update trade status to FAILED]
    F -- No --> M[Complete initial Agent Chat]
    K --> N[Start Recovery Check Loop]
    L --> N
    M --> N
    N --> O{Any FAILED/PENDING trades remaining?}
    O -- Yes (Attempt < 3) --> P[Reset status to PENDING]
    P --> Q[Launch Recovery Agent with targeted retry prompt]
    Q --> E
    O -- No / Max Retries --> R[Finalize Sweep & Publish MQTT Telemetry]
```

### Lifecycle Hooks
- **`MACEPreToolCallHook`**: Intercepts the tool call before dispatching to capture arguments and store them in the operational context.
- **`MACEPostToolCallHook`**: Logs the tool execution status, parameters, and result to `mcp_execution_log` (linked via `trade_id` foreign key) and updates the matching request status to `COMPLETED` or `FAILED`.
- **`MACEToolErrorHook`**: Intercepts unhandled tool exceptions and registers them as `FAILED` execution records.

---

## 8. v1.3 Operational Reference (Environment Variables)

All v1.3 behavior is env-tunable with safe defaults; no configuration is required to deploy the patch.

| Variable | Default | Scope | Effect |
| :--- | :--- | :--- | :--- |
| `MACE_NEWS_GATE` | `on` | equities orchestrator | `on` = fail-neutral buy gate while the news guard is stale/degraded; `off` = legacy fail-open |
| `MACE_NEWS_STALENESS_HOURS` | `12` | news gate | A healthy audit older than this counts as blind (3× the 4h audit cadence) |
| `MACE_TAKER_FEE` | `0.001` | guardrail + crypto shield | Virtual-ledger taker fee (fraction, not percent) applied to entries and exits |
| `MACE_KELLY_MIN_ROUNDS` | `10` | both orchestrators | Minimum realized exits before empirical stats override priors |
| `MACE_KELLY_LOOKBACK` | `100` | both orchestrators | Round trips aggregated into the empirical edge stats |
| `MACE_PRICE_VENUES` | `kucoin,binance,bybit` | crypto orchestrator/shield pool | Ordered venue pool for live fills and the priceability oracle |
| `MACE_STALE_LEDGER_SWEEPS` | `3` | crypto orchestrator | Consecutive unpriceable sweeps before a ledger row is written down |
| `MACE_BUY_DISPATCH` | `direct` | equities orchestrator | `direct` = Alpaca REST; `agent` = legacy Gemini dispatch (escape hatch) |
| `MACE_REGIME_COOLDOWN_HOURS` | `24` | crypto orchestrator | Re-entry lock after a Bear-regime RISK-OFF liquidation |
| `MACE_EMPIRICAL_KELLY_JSON` | *(set per sweep)* | both brains | Internal side channel carrying empirical stats into the brain subprocesses — do not set manually |

**New tables (auto-created, idempotent)**: `realized_round_trips` (per-exit P&L records; `basis` = `ledger_exact` crypto / `dispatch_approx` equities) and `component_health` (heartbeat store powering the fail-neutral gate).

**New shared modules**: `realized_round_trips.py` (round-trip store + empirical Kelly stats + news-guard health) and `price_venues.py` (multi-venue live-price pool) at the repo root.
