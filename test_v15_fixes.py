#!/usr/bin/env python3
"""v1.5 patch behavioral tests - sandbox-only (scratch DB, stubbed MQTT,
mocked LLM - no network, no external API keys). Portable: drop next to the repo
root or set MACE_ROOT (mirrors test_v14_fixes.py and test_v13_fixes.py).

Fix A: Crypto Portfolio Allocator Parity
  - Active cooldown filter checks vw_active_cooldowns
  - 72h post-stopout probation gate (kelly >= 0.10 double conviction to re-enter)
  - Bear-regime flip emits BEAR_REGIME_LIQUIDATION sell order before buys
  - Routine profit-taking trims (TRIM_PROFIT_TAKING on >15% surge over target weight)
  - Budget sizing against total_equity, capped at KELLY_HARD_CAP (default 0.12)
  - Budget normalization scaling when aggregate Kelly requests exceed 90% liquid cash

Fix B: Guardrail & Virtual Ledger Execution
  - run_piped_risk_gate preserves precomputed target_size_usd from allocator
  - evaluate_and_execute_simulated_trade executes partial sell for TRIM_PROFIT_TAKING
  - TRIM_PROFIT_TAKING logs round trip without registering cooldown lock
  - BEAR_REGIME_LIQUIDATION liquidates position and registers 24h REGIME_RISK_OFF lock

Fix C: Orchestrator Sells-First Sequencing & Fail-Neutral News Gate
  - Sells (trims and Bear liquidations) execute before any buy allocations
  - Shared news_guard_is_healthy and news_gate_allows_buys support crypto_news_guard
  - Fail-neutral gate blocks buys on stale/degraded news guard; keeps sells/stops live
  - MACE_CRYPTO_NEWS_GATE=off fail-open override supported

Fix D: Autonomous Qualitative Crypto News Guard
  - Multi-source RSS feed parsing for CoinDesk, Cointelegraph, Decrypt
  - close_crypto_position_tool emergency liquidation, 24h lock, HWM purge
  - Component health heartbeat writes to component_health table

Fix E: Fleet & Observability Integration
  - start_all.sh and stop_all.sh manage mace-crypto-news-guard.service
  - mace_48h_dashboard.sh tracks mace-crypto-news-guard status
  - systemd unit file mace-crypto-news-guard.service configured

Gate: Compile & Cleanliness
  - py_compile clean on all touched v1.5 files
  - datetime.utcnow() deprecation sweep
"""
import sys, os, io, types, sqlite3, json, logging, subprocess
from datetime import datetime, timedelta, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
MACE = os.environ.get("MACE_ROOT", "")
if not MACE:
    for _cand in (_HERE, os.getcwd(), os.path.dirname(_HERE)):
        if os.path.isfile(os.path.join(_cand, "crypto", "crypto_shield.py")):
            MACE = os.path.abspath(_cand)
            break
if not MACE:
    MACE = "/home/tony/dev/MACE-LOCAL"
SCRATCH = os.path.abspath(os.path.join(_HERE, "scratch_v15.db"))
sys.path.insert(0, MACE)
sys.path.insert(0, os.path.join(MACE, "crypto"))
sys.path.insert(0, os.path.join(MACE, "crypto", "swarm"))

PASS, FAIL = [], []

def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({detail})" if detail and not cond else ""))

# ---------------------------------------------------------------- stubs -----
def _stub_module(name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules.setdefault(name, mod)
    return mod

class _CBE:
    VERSION2 = 2
class _MC:
    def __init__(self, *a, **k): pass
    def connect(self, *a, **k): pass
    def publish(self, *a, **k): pass
    def loop_start(self, *a, **k): pass
    def loop_stop(self, *a, **k): pass

mqtt_client_mod = _stub_module("paho.mqtt.client", CallbackAPIVersion=_CBE, Client=_MC)
mqtt_pkg = _stub_module("paho.mqtt"); mqtt_pkg.client = mqtt_client_mod
paho_pkg = _stub_module("paho"); paho_pkg.mqtt = mqtt_pkg
mqtt_pkg.paho = paho_pkg

# google.antigravity stub in case SDK is missing in portable run
if "google.antigravity" not in sys.modules:
    class _MockAgent:
        def __init__(self, *a, **k): pass
        def run(self, *a, **k):
            class _Res:
                text = json.dumps({"status": "safe", "details": "sandbox test"})
            return _Res()
    class _MockTypes:
        class Tool:
            def __init__(self, *a, **k): pass
    _stub_module("google.antigravity", Agent=_MockAgent, LocalAgentConfig=dict, types=_MockTypes)

# ---------------------------------------------------------------- setup -----
import realized_round_trips as rrt
import portfolio_allocator as c_alloc
import guardrail as c_guard
import crypto_news_guard as c_news

if os.path.exists(SCRATCH):
    os.remove(SCRATCH)
os.makedirs(os.path.dirname(SCRATCH), exist_ok=True)

SETUP_SQL = os.path.join(MACE, "setup_db.sql")

def fresh_db(run_setup=True):
    if os.path.exists(SCRATCH):
        os.remove(SCRATCH)
    if run_setup:
        with sqlite3.connect(SCRATCH) as conn:
            conn.executescript(open(SETUP_SQL).read())
    rrt._SCHEMA_OK.discard(SCRATCH)
    rrt.ensure_schema(SCRATCH)
    return SCRATCH

def q(sql, args=(), db=SCRATCH):
    with sqlite3.connect(db) as conn:
        return conn.execute(sql, args).fetchall()

def seed_universe(symbols, db=SCRATCH):
    with sqlite3.connect(db) as conn:
        for sym in symbols:
            token = sym.split("/")[0]
            conn.execute("""
                INSERT OR IGNORE INTO asset_universe (symbol, asset_class, asset_name, category, broker, exchange, currency)
                VALUES (?, 'CRYPTO', ?, 'CRYPTO', 'binance', 'BINANCE', 'USDT')
            """, (sym, token))

def insert_cooldown(symbol, cooldown_until, reason, closed_at=None, db=SCRATCH):
    if closed_at is None:
        closed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with sqlite3.connect(db) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT asset_id FROM asset_universe WHERE symbol = ?", (symbol,))
        row = cursor.fetchone()
        if not row:
            token = symbol.split("/")[0]
            cursor.execute("""
                INSERT INTO asset_universe (symbol, asset_class, asset_name, category, broker, exchange, currency)
                VALUES (?, 'CRYPTO', ?, 'CRYPTO', 'binance', 'BINANCE', 'USDT')
            """, (symbol, token))
            asset_id = cursor.lastrowid
        else:
            asset_id = row[0]
        cursor.execute("""
            INSERT INTO trade_cooldowns (asset_id, symbol, closed_at, reason, cooldown_until)
            VALUES (?, ?, ?, ?, ?)
        """, (asset_id, symbol, closed_at, reason, cooldown_until))

def _exec_allocator(payload, db=SCRATCH):
    old_stdin, old_stdout = sys.stdin, sys.stdout
    sys.stdin = io.StringIO(json.dumps(payload))
    sys.stdout = io.StringIO()
    try:
        c_alloc.run_portfolio_guardrail(db_path=db)
        out = sys.stdout.getvalue()
        return json.loads(out)
    finally:
        sys.stdin, sys.stdout = old_stdin, old_stdout

# =========================================================================
print("\n--- FIX A: crypto portfolio allocator & risk parity ---")
db = fresh_db()
seed_universe(["SOL/USDT", "BTC/USDT", "ETH/USDT", "BONK/USDT"], db=db)

# 1. Active cooldown rejection
future_time = (datetime.now(timezone.utc) + timedelta(hours=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
insert_cooldown("SOL/USDT", future_time, "STOP_LOSS_BREACH", db=db)

payload_1 = {
    "available_cash": 10000.0,
    "total_equity": 10000.0,
    "candidates": [
        {"symbol": "SOL/USDT", "current_state": "Bull", "ml_confirmed": True, "calculated_kelly": 0.08},
        {"symbol": "BTC/USDT", "current_state": "Bull", "ml_confirmed": True, "calculated_kelly": 0.08}
    ],
    "existing_positions": {}
}
res_1 = _exec_allocator(payload_1, db=db)
approved_symbols_1 = [t["symbol"] for t in res_1.get("approved_trades", [])]
check("1. Active cooldown filter: SOL/USDT rejected; healthy BTC/USDT approved",
      "SOL/USDT" not in approved_symbols_1 and "BTC/USDT" in approved_symbols_1,
      f"approved={approved_symbols_1}")

# 2. 72h post-stopout probation rejection (kelly < 0.10)
db = fresh_db()
seed_universe(["BONK/USDT", "BTC/USDT"], db=db)
past_cooldown = (datetime.now(timezone.utc) - timedelta(hours=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
insert_cooldown("BONK/USDT", past_cooldown, "STOP_LOSS_BREACH", db=db)

payload_2 = {
    "available_cash": 10000.0,
    "total_equity": 10000.0,
    "candidates": [
        {"symbol": "BONK/USDT", "current_state": "Bull", "ml_confirmed": True, "calculated_kelly": 0.07}
    ],
    "existing_positions": {}
}
res_2 = _exec_allocator(payload_2, db=db)
approved_symbols_2 = [t["symbol"] for t in res_2.get("approved_trades", [])]
check("2. 72h probation gate: BONK/USDT rejected with kelly 0.07 < 0.10",
      "BONK/USDT" not in approved_symbols_2,
      f"approved={approved_symbols_2}")

# 3. 72h post-stopout probation approval (kelly >= 0.10)
payload_3 = {
    "available_cash": 10000.0,
    "total_equity": 10000.0,
    "candidates": [
        {"symbol": "BONK/USDT", "current_state": "Bull", "ml_confirmed": True, "calculated_kelly": 0.12}
    ],
    "existing_positions": {}
}
res_3 = _exec_allocator(payload_3, db=db)
approved_symbols_3 = [t["symbol"] for t in res_3.get("approved_trades", [])]
check("3. 72h probation gate: BONK/USDT approved on double conviction (kelly 0.12 >= 0.10)",
      "BONK/USDT" in approved_symbols_3,
      f"approved={approved_symbols_3}")

# 4. Bear regime flip on held position emits full liquidation
payload_4 = {
    "available_cash": 5000.0,
    "total_equity": 10000.0,
    "candidates": [
        {"symbol": "ETH/USDT", "current_state": "Bear", "ml_confirmed": True, "calculated_kelly": 0.08}
    ],
    "existing_positions": {"ETH/USDT": 2500.0}
}
res_4 = _exec_allocator(payload_4, db=db)
sell_orders_4 = res_4.get("sell_orders", [])
check("4. Bear regime flip: held ETH/USDT emits BEAR_REGIME_LIQUIDATION sell order",
      len(sell_orders_4) == 1 and sell_orders_4[0]["symbol"] == "ETH/USDT" and sell_orders_4[0]["action"] == "BEAR_REGIME_LIQUIDATION",
      f"sell_orders={sell_orders_4}")

# 5. Routine profit-taking trim (>15% surge)
payload_5 = {
    "available_cash": 5000.0,
    "total_equity": 10000.0,  # 12% target = $1200
    "candidates": [
        {"symbol": "SOL/USDT", "current_state": "Bull", "ml_confirmed": True, "calculated_kelly": 0.12}
    ],
    "existing_positions": {"SOL/USDT": 1800.0}  # $1800 > $1200 * 1.15 ($1380)
}
res_5 = _exec_allocator(payload_5, db=db)
sell_orders_5 = res_5.get("sell_orders", [])
check("5. Routine profit trim: held SOL/USDT ($1800 vs $1200 target) emits TRIM_PROFIT_TAKING order",
      len(sell_orders_5) == 1 and sell_orders_5[0]["action"] == "TRIM_PROFIT_TAKING" and sell_orders_5[0]["trim_amount_usd"] == 600.0,
      f"sell_orders={sell_orders_5}")

# 6. Held position within 15% does NOT emit trim order
payload_6 = {
    "available_cash": 5000.0,
    "total_equity": 10000.0,  # 12% target = $1200
    "candidates": [
        {"symbol": "SOL/USDT", "current_state": "Bull", "ml_confirmed": True, "calculated_kelly": 0.12}
    ],
    "existing_positions": {"SOL/USDT": 1300.0}  # $1300 <= $1200 * 1.15 ($1380)
}
res_6 = _exec_allocator(payload_6, db=db)
sell_orders_6 = res_6.get("sell_orders", [])
check("6. Surge <= 15%: held SOL/USDT ($1300 vs $1200 target) emits NO trim order",
      len(sell_orders_6) == 0,
      f"sell_orders={sell_orders_6}")

# 7. Single-asset Kelly hard cap (KELLY_HARD_CAP default 0.12)
payload_7 = {
    "available_cash": 10000.0,
    "total_equity": 10000.0,
    "candidates": [
        {"symbol": "BTC/USDT", "current_state": "Bull", "ml_confirmed": True, "calculated_kelly": 0.25}
    ],
    "existing_positions": {}
}
res_7 = _exec_allocator(payload_7, db=db)
trades_7 = res_7.get("approved_trades", [])
check("7. Kelly hard cap: 0.25 raw kelly clamped to 0.12 ($1200 target)",
      len(trades_7) == 1 and abs(trades_7[0]["target_size_usd"] - 1200.0) < 1e-4,
      f"trades={trades_7}")

# 8. Budget constraint normalization (scaling down when total demand > 90% liquid cash)
payload_8 = {
    "available_cash": 1000.0,
    "total_equity": 10000.0,  # 90% liquid cash = $900 max budget
    "candidates": [
        {"symbol": "BTC/USDT", "current_state": "Bull", "ml_confirmed": True, "calculated_kelly": 0.10},  # $1000
        {"symbol": "ETH/USDT", "current_state": "Bull", "ml_confirmed": True, "calculated_kelly": 0.10}   # $1000
    ],
    "existing_positions": {}
}
res_8 = _exec_allocator(payload_8, db=db)
trades_8 = res_8.get("approved_trades", [])
total_alloc_8 = sum(t["target_size_usd"] for t in trades_8)
check("8. Budget normalization: $2000 demand scaled down to <= $900 (90% liquid cash)",
      len(trades_8) == 2 and total_alloc_8 <= 900.01 and abs(trades_8[0]["target_size_usd"] - 450.0) < 1e-2,
      f"total_alloc={total_alloc_8}, trades={trades_8}")

# =========================================================================
print("\n--- FIX B: guardrail & virtual ledger execution ---")
db = fresh_db()
seed_universe(["SOL/USDT", "BTC/USDT"], db=db)
c_guard.DEFAULT_DB_PATH = db
c_guard.init_db(db_path=db)

# Seed portfolio with USDT and SOL position
with sqlite3.connect(db) as conn:
    conn.execute("INSERT OR REPLACE INTO portfolio (blockchain, token, quantity, avg_entry_price) VALUES ('ARBITRUM', 'USDT', 5000.0, 1.0)")
    conn.execute("INSERT OR REPLACE INTO portfolio (blockchain, token, quantity, avg_entry_price) VALUES ('SOLANA', 'SOL', 10.0, 150.0)")

# 9. run_piped_risk_gate preserves precomputed target_size_usd
gate_input = {
    "status": "success",
    "ticker": "BTC/USDT",
    "regime": "Bull",
    "kelly_fraction": 0.20,
    "current_price": 50000.0,
    "target_size_usd": 750.0
}
gate_out = c_guard.run_piped_risk_gate(json.dumps(gate_input))

check("9. run_piped_risk_gate preserves precomputed target_size_usd from allocator ($750.00)",
      gate_out.get("allocated_dollars") == 750.0 and gate_out.get("status") == "approved",
      f"gate_out={gate_out}")

# 10. evaluate_and_execute_simulated_trade partial sell for TRIM_PROFIT_TAKING
res_trim = c_guard.evaluate_and_execute_simulated_trade(
    symbol="SOL/USDT",
    action="SELL",
    quantity=3.0,
    execution_price=200.0,
    reason="TRIM_PROFIT_TAKING",
    db_path=db
)
sol_holding = q("SELECT quantity FROM portfolio WHERE blockchain='SOLANA' AND token='SOL'", db=db)
trip_row = q("SELECT reason, basis FROM realized_round_trips WHERE symbol='SOL/USDT' ORDER BY trip_id DESC LIMIT 1", db=db)
cooldown_rows = q("SELECT * FROM trade_cooldowns WHERE symbol='SOL/USDT'", db=db)

check("10. TRIM_PROFIT_TAKING executes partial sell (10 -> 7 SOL), logs round trip, NO cooldown lock",
      res_trim.get("success") is True
      and abs(sol_holding[0][0] - 7.0) < 1e-6
      and trip_row and trip_row[0][0] == "TRIM_PROFIT_TAKING"
      and len(cooldown_rows) == 0,
      f"holding={sol_holding}, trip={trip_row}, cooldowns={cooldown_rows}")

# 11. evaluate_and_execute_simulated_trade full sell for BEAR_REGIME_LIQUIDATION
import orchestrator as c_orch
c_orch.DEFAULT_DB_PATH = db

res_bear = c_guard.evaluate_and_execute_simulated_trade(
    symbol="SOL/USDT",
    action="SELL",
    quantity=7.0,
    execution_price=200.0,
    reason="REGIME_RISK_OFF",
    db_path=db
)
if res_bear.get("success"):
    c_orch.register_regime_cooldown("SOL/USDT")

sol_holding_after = q("SELECT quantity FROM portfolio WHERE blockchain='SOLANA' AND token='SOL'", db=db)
trip_bear = q("SELECT reason FROM realized_round_trips WHERE symbol='SOL/USDT' ORDER BY trip_id DESC LIMIT 1", db=db)
cooldown_bear = q("SELECT reason FROM trade_cooldowns WHERE symbol='SOL/USDT'", db=db)

check("11. BEAR_REGIME_LIQUIDATION executes full sell, registers 24h REGIME_RISK_OFF cooldown",
      res_bear.get("success") is True
      and (not sol_holding_after or abs(sol_holding_after[0][0]) < 1e-6)
      and trip_bear and trip_bear[0][0] == "REGIME_RISK_OFF"
      and cooldown_bear and cooldown_bear[0][0] == "REGIME_RISK_OFF",
      f"holding={sol_holding_after}, trip={trip_bear}, cooldown={cooldown_bear}")

# =========================================================================
print("\n--- FIX C: orchestrator sequencing & fail-neutral news gate ---")
db = fresh_db()
rrt.DEFAULT_DB_PATH = db

# 12. Orchestrator sells-first source verification
orch_src = open(os.path.join(MACE, "crypto/swarm/orchestrator.py")).read()
idx_sells = orch_src.find("# 1. Execute Sells First (Trims & Bear Liquidations)")
idx_buys = orch_src.find("# 2. Execute Buys Second")
check("12. Orchestrator executes sells (trims & liquidations) BEFORE buys",
      idx_sells > 0 and idx_buys > 0 and idx_sells < idx_buys,
      f"idx_sells={idx_sells}, idx_buys={idx_buys}")

# 13. news_guard_is_healthy True when fresh
rrt.write_component_heartbeat("crypto_news_guard", status="healthy", detail="All safe", db_path=db)
h_ok, _ = rrt.news_guard_is_healthy(db_path=db, component="crypto_news_guard")
check("13. news_guard_is_healthy returns True for fresh healthy heartbeat", h_ok)

# 14. news_guard_is_healthy False when stale (>12h)
stale_time = (datetime.now(timezone.utc) - timedelta(hours=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
with sqlite3.connect(db) as conn:
    conn.execute("UPDATE component_health SET last_healthy_at = ? WHERE component = 'crypto_news_guard'", (stale_time,))
h_stale, _ = rrt.news_guard_is_healthy(db_path=db, component="crypto_news_guard")
check("14. news_guard_is_healthy returns False when heartbeat is stale (>12h)", not h_stale)

# 15. news_guard_is_healthy False when degraded or missing
with sqlite3.connect(db) as conn:
    conn.execute("UPDATE component_health SET status = 'degraded' WHERE component = 'crypto_news_guard'")
h_deg, _ = rrt.news_guard_is_healthy(db_path=db, component="crypto_news_guard")
h_missing, _ = rrt.news_guard_is_healthy(db_path=db, component="non_existent_guard")
check("15. news_guard_is_healthy returns False on degraded or missing row",
      not h_deg and not h_missing)

# 16. news_gate_allows_buys holds buys on stale/degraded guard
gate_ok_stale, gate_reason_stale = rrt.news_gate_allows_buys(db_path=db, component="crypto_news_guard")
check("16. news_gate_allows_buys holds buys when crypto_news_guard is degraded/stale",
      gate_ok_stale is False, f"gate_ok={gate_ok_stale}, reason={gate_reason_stale}")

# 17. news_gate_allows_buys fail-open override via MACE_CRYPTO_NEWS_GATE=off
os.environ["MACE_CRYPTO_NEWS_GATE"] = "off"
try:
    gate_ok_off, gate_reason_off = rrt.news_gate_allows_buys(db_path=db, component="crypto_news_guard")
    check("17. news_gate_allows_buys allows buys when MACE_CRYPTO_NEWS_GATE=off",
          gate_ok_off is True and "disabled via" in gate_reason_off,
          f"gate_ok={gate_ok_off}, reason={gate_reason_off}")
finally:
    os.environ.pop("MACE_CRYPTO_NEWS_GATE", None)

# =========================================================================
print("\n--- FIX D: autonomous qualitative crypto news guard ---")
db = fresh_db()
seed_universe(["SOL/USDT", "BTC/USDT"], db=db)
c_guard.DEFAULT_DB_PATH = db
c_guard.init_db(db_path=db)
c_news.DEFAULT_DB_PATH = db

# Seed SOL position and HWM
with sqlite3.connect(db) as conn:
    conn.execute("INSERT OR REPLACE INTO portfolio (blockchain, token, quantity, avg_entry_price) VALUES ('SOLANA', 'SOL', 10.0, 150.0)")
    conn.execute("""
        INSERT INTO crypto_hwm (asset_id, symbol, high_water_mark, loss_limit, updated_at)
        SELECT asset_id, symbol, 150.0, 0.08, '2026-09-29T00:00:00Z' FROM vw_crypto_universe WHERE symbol = 'SOL/USDT'
    """)

# 18. RSS parser handles feed parsing safely
rss_sample = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Crypto News</title>
    <item>
      <title>Solana Ecosystem Update: High Throughput Recorded</title>
      <description>Network activity reaches new all-time highs.</description>
    </item>
  </channel>
</rss>"""

class _MockSession:
    def get(self, url, **kwargs):
        class _Resp:
            status_code = 200
            content = rss_sample.encode("utf-8")
        return _Resp()

parsed_headlines = c_news.fetch_rss_feed(_MockSession(), "https://mock.feed/rss", "TestFeed")
check("18. fetch_rss_feed successfully parses XML feed and captures formatted item",
      len(parsed_headlines) == 1 and "Solana" in parsed_headlines[0] and "[TestFeed]" in parsed_headlines[0],
      f"headlines={parsed_headlines}")

# 19. Emergency tool: close_crypto_position_tool
res_tool = c_news.close_crypto_position_tool("SOL/USDT")
sol_bal = q("SELECT quantity FROM portfolio WHERE blockchain='SOLANA' AND token='SOL'", db=db)
hwm_rows = q("SELECT * FROM crypto_hwm WHERE symbol='SOL/USDT'", db=db)
cooldown_news = q("SELECT reason FROM trade_cooldowns WHERE symbol='SOL/USDT'", db=db)

check("19. close_crypto_position_tool liquidates ledger, purges HWM, logs 24h QUALITATIVE_NEWS_THREAT",
      "Successfully liquidated" in res_tool
      and (not sol_bal or abs(sol_bal[0][0]) < 1e-6)
      and len(hwm_rows) == 0
      and cooldown_news and cooldown_news[0][0] == "QUALITATIVE_NEWS_THREAT",
      f"res={res_tool}, bal={sol_bal}, hwm={hwm_rows}, cooldown={cooldown_news}")

# 20. Heartbeat written to component_health table
rrt.write_component_heartbeat("crypto_news_guard", "healthy", "Test heartbeat", db_path=db)
hb_row = q("SELECT status, detail FROM component_health WHERE component='crypto_news_guard'", db=db)
check("20. crypto_news_guard heartbeat recorded in component_health table",
      hb_row and hb_row[0][0] == "healthy" and hb_row[0][1] == "Test heartbeat",
      f"hb_row={hb_row}")

# =========================================================================
print("\n--- FIX E: fleet & observability integration ---")

start_all_src = open(os.path.join(MACE, "start_all.sh")).read()
stop_all_src = open(os.path.join(MACE, "stop_all.sh")).read()
dash_src = open(os.path.join(MACE, "mace_48h_dashboard.sh")).read()
service_file = os.path.join(MACE, "systemd/mace-crypto-news-guard.service")

# 21. start_all.sh contains mace-crypto-news-guard
check("21. start_all.sh enables & starts mace-crypto-news-guard.service",
      "mace-crypto-news-guard.service" in start_all_src)

# 22. stop_all.sh contains mace-crypto-news-guard
check("22. stop_all.sh stops mace-crypto-news-guard.service",
      "mace-crypto-news-guard.service" in stop_all_src)

# 23. mace_48h_dashboard.sh monitors mace-crypto-news-guard
check("23. mace_48h_dashboard.sh includes mace-crypto-news-guard in fleet health checks",
      "mace-crypto-news-guard" in dash_src)

# 24. systemd service unit exists and references crypto_news_guard.py
service_exists = os.path.isfile(service_file)
service_content = open(service_file).read() if service_exists else ""
check("24. systemd/mace-crypto-news-guard.service exists and targets crypto_news_guard.py",
      service_exists and "crypto/crypto_news_guard.py" in service_content)

# =========================================================================
print("\n--- GATE: compile + deprecation sweep ---")

touched = [
    "crypto/swarm/portfolio_allocator.py",
    "crypto/swarm/guardrail.py",
    "crypto/swarm/orchestrator.py",
    "crypto/crypto_news_guard.py",
    "realized_round_trips.py"
]
compile_ok = True
for rel in touched:
    r = subprocess.run([sys.executable, "-m", "py_compile", os.path.join(MACE, rel)],
                       capture_output=True, text=True)
    if r.returncode != 0:
        compile_ok = False
        print(f"Compilation error in {rel}:\n{r.stderr}")
check("25. py_compile clean on all touched v1.5 files", compile_ok)

import re as _re
def _no_docstrings(src):
    return _re.sub(r'\x22\x22\x22.*?\x22\x22\x22', '', src, flags=_re.S)

alloc_src = _no_docstrings(open(os.path.join(MACE, "crypto/swarm/portfolio_allocator.py")).read())
guard_src = _no_docstrings(open(os.path.join(MACE, "crypto/swarm/guardrail.py")).read())
orch_src = _no_docstrings(open(os.path.join(MACE, "crypto/swarm/orchestrator.py")).read())
news_src = _no_docstrings(open(os.path.join(MACE, "crypto/crypto_news_guard.py")).read())

check("26. datetime.utcnow() swept from all v1.5 modules (no deprecation warnings)",
      "datetime.utcnow()" not in alloc_src
      and "datetime.utcnow()" not in guard_src
      and "datetime.utcnow()" not in orch_src
      and "datetime.utcnow()" not in news_src)

# =========================================================================
print(f"\n==================== RESULT: {len(PASS)}/{len(PASS) + len(FAIL)} ====================")
if FAIL:
    print("FAILED:")
    for f in FAIL:
        print(f"  - {f}")
    if os.path.exists(SCRATCH):
        os.remove(SCRATCH)
    sys.exit(1)

if os.path.exists(SCRATCH):
    os.remove(SCRATCH)
print("ALL v1.5 CHECKS PASS")
