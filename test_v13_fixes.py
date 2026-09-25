#!/usr/bin/env python3
"""v1.3 patch behavioral tests — 40 checks across the 7 fixes, sandbox-only
(scratch DB, stubbed MQTT/antigravity, mocked ccxt — no network, no keys).

Fix 1: news-guard fail-neutral heartbeat + orchestrator buy gate
Fix 2: 0.1% taker fee in guardrail virtual ledger
Fix 3: honest Kelly — realized round trips + shrinkage blend
Fix 4: crypto Sharpe annualization (sqrt(2190), not sqrt(365))
Fix 5: multi-venue live-price pool
Fix 6: stale-ledger write-down purge with outage canary
Fix 7: README truth pass + schema/docs presence
Gate : py_compile on every touched file
"""
import sys, os, types, sqlite3, asyncio, json, logging, subprocess, tempfile

# Portable bootstrap: explicit MACE_ROOT env wins; otherwise auto-detect the
# repo next to this file or at the CWD, so the suite runs on any machine
# (e.g. drop it in ~/MACE and run `python3 test_v13_fixes.py`).
_HERE = os.path.dirname(os.path.abspath(__file__))
MACE = os.environ.get("MACE_ROOT", "")
if not MACE:
    for _cand in (os.getcwd(), _HERE, os.path.dirname(_HERE)):
        if os.path.isfile(os.path.join(_cand, "crypto", "swarm", "brain.py")):
            MACE = os.path.abspath(_cand)
            break
if not MACE:
    MACE = "/home/z/my-project/mace"
SCRATCH = os.path.join(_HERE, "scratch_v13.db")
sys.path.insert(0, MACE)
sys.path.insert(0, os.path.join(MACE, "crypto/swarm"))

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
mqtt_client_mod = _stub_module("paho.mqtt.client", CallbackAPIVersion=_CBE, Client=_MC)
mqtt_pkg = _stub_module("paho.mqtt"); mqtt_pkg.client = mqtt_client_mod
paho_pkg = _stub_module("paho"); paho_pkg.mqtt = mqtt_pkg
mqtt_pkg.paho = paho_pkg

ga_hooks_ns = types.SimpleNamespace(
    HookResult=object, PreToolCallDecideHook=object,
    PostToolCallHook=object, OnToolErrorHook=object,
)
ga_hooks = _stub_module("google.antigravity.hooks",
                        hooks=ga_hooks_ns, policy=types.SimpleNamespace(allow_all=lambda: None))
ga_pkg = _stub_module("google.antigravity"); ga_pkg.hooks = ga_hooks
google_pkg = _stub_module("google"); google_pkg.antigravity = ga_pkg

# ---------------------------------------------------------------- setup -----
if os.path.exists(SCRATCH):
    os.remove(SCRATCH)
os.makedirs(os.path.dirname(SCRATCH), exist_ok=True)

import realized_round_trips as rrt
import price_venues

def fresh_db():
    if os.path.exists(SCRATCH):
        os.remove(SCRATCH)
    rrt._SCHEMA_OK.discard(SCRATCH)  # schema guard caches per-path; reset after file removal
    rrt.ensure_schema(SCRATCH)
    return SCRATCH

def seed_usdt(amount=10000.0):
    with sqlite3.connect(SCRATCH) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS portfolio (blockchain TEXT, token TEXT, quantity REAL, avg_entry_price REAL, PRIMARY KEY (blockchain, token))")
        conn.execute("INSERT OR REPLACE INTO portfolio VALUES ('ARBITRUM', 'USDT', ?, 1.0)", (amount,))
    return SCRATCH

def q(sql, args=(), db=SCRATCH):
    with sqlite3.connect(db) as conn:
        return conn.execute(sql, args).fetchall()

# =========================================================================
print("\n--- FIX 1: news-guard fail-neutral heartbeat + buy gate ---")
db = fresh_db()

# 1
ok = rrt.write_news_guard_heartbeat("healthy", "verdict=safe", db_path=SCRATCH)
row = q("SELECT status, last_healthy_at, detail FROM component_health WHERE component='tradfi_news_guard'")
check("1. healthy heartbeat writes status+last_healthy_at", ok and row and row[0][0] == "healthy" and row[0][1] is not None)

# 2
rrt.write_news_guard_heartbeat("degraded", "429 capped", db_path=SCRATCH)
row = q("SELECT status, last_healthy_at FROM component_health WHERE component='tradfi_news_guard'")
lh_before = row[0][1]
import time as _t; _t.sleep(1.1)
rrt.write_news_guard_heartbeat("degraded", "still capped", db_path=SCRATCH)
row = q("SELECT status, last_healthy_at, detail FROM component_health WHERE component='tradfi_news_guard'")
check("2. degraded heartbeat preserves prior last_healthy_at", row[0][1] == lh_before and row[0][0] == "degraded" and "still capped" in row[0][2])

# 3
healthy, detail = rrt.news_guard_is_healthy(db_path=SCRATCH)
check("3. is_healthy=True for fresh healthy row", healthy, detail)

# 4
with sqlite3.connect(SCRATCH) as conn:
    conn.execute("UPDATE component_health SET last_healthy_at = '2026-09-01T00:00:00Z' WHERE component='tradfi_news_guard'")
healthy, detail = rrt.news_guard_is_healthy(db_path=SCRATCH, staleness_hours=12)
check("4. is_healthy=False when stale beyond window", not healthy, detail)

# 5
missing_db = SCRATCH + ".noexist"
healthy, detail = rrt.news_guard_is_healthy(db_path=missing_db)
check("5. is_healthy=False when table/db missing", not healthy, detail)

# 6
db2 = fresh_db()
healthy, detail = rrt.news_guard_is_healthy(db_path=db2)
check("6. is_healthy=False when no row on record", not healthy, detail)

# 7
rrt.write_news_guard_heartbeat("healthy", "verdict=safe", db_path=SCRATCH)
os.environ["MACE_NEWS_GATE"] = "off"
try:
    allowed, detail = rrt.news_gate_allows_buys(db_path=SCRATCH)
    check("7. MACE_NEWS_GATE=off bypasses gate (fail-open escape hatch)", allowed, detail)
finally:
    os.environ.pop("MACE_NEWS_GATE", None)

# 8
with sqlite3.connect(SCRATCH) as conn:
    conn.execute("UPDATE component_health SET status='degraded', last_healthy_at=NULL WHERE component='tradfi_news_guard'")
allowed, detail = rrt.news_gate_allows_buys(db_path=SCRATCH)
check("8. gate holds buys when guard degraded (fail-neutral posture)", not allowed, detail)

# =========================================================================
print("\n--- FIX 2: taker fee in guardrail virtual ledger ---")
import guardrail
_orig_get_conn = guardrail.get_db_connection
_orig_db_path = guardrail.DEFAULT_DB_PATH
guardrail.DEFAULT_DB_PATH = SCRATCH
guardrail.get_db_connection = lambda db_path=None: _orig_get_conn(SCRATCH)

db = fresh_db(); seed_usdt(10000.0)
r = guardrail.evaluate_and_execute_simulated_trade("BTC/USDT", "BUY", 0.5, 100.0)
cash = q("SELECT quantity FROM portfolio WHERE token='USDT'")[0][0]
# 9
check("9. BUY debits qty*price*(1+fee) from USDT", abs(cash - (10000.0 - 50.0 * 1.001)) < 1e-9, f"cash={cash}")
# 10
basis = q("SELECT avg_entry_price FROM portfolio WHERE token='BTC'")[0][0]
check("10. BUY stores fee-inclusive avg_entry_price", abs(basis - 100.1) < 1e-9, f"basis={basis}")
# 11
db = fresh_db(); seed_usdt(50.04)  # covers gross 50.0 but not gross+fee 50.05
r = guardrail.evaluate_and_execute_simulated_trade("BTC/USDT", "BUY", 0.5, 100.0)
check("11. BUY rejected when cash short of gross+fee", (not r["success"]) and "fee" in r.get("error", ""), str(r))
# 12
db = fresh_db(); seed_usdt(2000.0)
guardrail.evaluate_and_execute_simulated_trade("ETH/USDT", "BUY", 2.0, 500.0)
cash_before = q("SELECT quantity FROM portfolio WHERE token='USDT'")[0][0]
r = guardrail.evaluate_and_execute_simulated_trade("ETH/USDT", "SELL", 2.0, 600.0, reason="REGIME_RISK_OFF")
cash_after = q("SELECT quantity FROM portfolio WHERE token='USDT'")[0][0]
check("12. SELL credits net of fee (qty*price*(1-fee))", abs((cash_after - cash_before) - (1200.0 * 0.999)) < 1e-9, f"delta={cash_after-cash_before}")
# 13
trip = q("SELECT entry_price, exit_price, reason, basis, pnl_usd FROM realized_round_trips WHERE symbol='ETH/USDT'")
ok = trip and abs(trip[0][0] - 500.5) < 1e-9 and abs(trip[0][1] - 600.0 * 0.999) < 1e-9 and trip[0][2] == "REGIME_RISK_OFF" and trip[0][3] == "ledger_exact"
check("13. SELL writes round-trip row (fee-net exit, ledger_exact)", ok, str(trip))

# 14
env_backup = os.environ.get("MACE_TAKER_FEE")
os.environ["MACE_TAKER_FEE"] = "0.0025"
try:
    import importlib
    guardrail2 = importlib.reload(guardrail)
    check("14. MACE_TAKER_FEE env override parsed", guardrail2.TAKER_FEE == 0.0025, f"got {guardrail2.TAKER_FEE}")
    guardrail = guardrail2
    guardrail.DEFAULT_DB_PATH = SCRATCH
    guardrail.get_db_connection = lambda db_path=None: _orig_get_conn(SCRATCH)
finally:
    os.environ.pop("MACE_TAKER_FEE", None)
    if env_backup is not None:
        os.environ["MACE_TAKER_FEE"] = env_backup

# =========================================================================
print("\n--- FIX 3: honest Kelly — realized round trips + shrinkage blend ---")
db = fresh_db()

# 15
ok = rrt.record_trip("CRYPTO", "BTC/USDT", 1.0, 100.0, 110.0, reason="TRIM", db_path=SCRATCH)
row = q("SELECT qty, entry_price, exit_price, pnl_usd, pnl_pct FROM realized_round_trips")
check("15. record_trip writes row with exact pnl math", ok and row and abs(row[0][3] - 10.0) < 1e-9 and abs(row[0][4] - 0.10) < 1e-9, str(row))

# 16
rrt.record_trip("CRYPTO", "BEAT/USDT", 500.0, 0.50, 0.0, reason="STALE_LEDGER_WRITE_DOWN", db_path=SCRATCH)
row = q("SELECT pnl_pct, pnl_usd FROM realized_round_trips WHERE symbol='BEAT/USDT'")
check("16. exit=0 write-down records pnl_pct=-1.0", row and row[0][0] == -1.0 and abs(row[0][1] + 250.0) < 1e-9, str(row))

# 17
db2 = fresh_db()
stats = rrt.load_empirical_edge_stats("CRYPTO", db_path=db2)
check("17. edge stats None on empty table", stats is None, str(stats))

# 18
db = fresh_db()
for wr in [1, 1, 1, 0, 0]:  # 3 wins, 2 losses
    rrt.record_trip("TRADFI", "TEST", 10.0, 100.0, 120.0 if wr else 90.0, db_path=SCRATCH)
stats = rrt.load_empirical_edge_stats("TRADFI", db_path=SCRATCH)
check("18. win_rate computed over lookback (3/5=0.6)", stats and abs(stats["n"] - 5) < 1e-9 and abs(stats["win_rate"] - 0.6) < 1e-4, str(stats))

# 19
db = fresh_db()
rrt.record_trip("CRYPTO", "W1", 1.0, 100.0, 150.0, db_path=SCRATCH)
rrt.record_trip("CRYPTO", "W2", 1.0, 100.0, 130.0, db_path=SCRATCH)
stats = rrt.load_empirical_edge_stats("CRYPTO", db_path=SCRATCH)
check("19. payoff None when zero losing rounds", stats and stats["payoff"] is None, str(stats))

# 20
env_json = rrt.empirical_env_json("CRYPTO", db_path=SCRATCH, min_rounds=10)
check("20. env json None below min_rounds (priors stay)", env_json is None, str(env_json))

# 21
db = fresh_db()
for wr in [1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0]:
    rrt.record_trip("CRYPTO", "T", 1.0, 100.0, 120.0 if wr else 95.0, db_path=SCRATCH)
env_json = rrt.empirical_env_json("CRYPTO", db_path=SCRATCH, min_rounds=10)
stats = json.loads(env_json) if env_json else {}
check("21. env json carries n+asset_class+prior_strength at min",
      bool(env_json) and stats.get("n") == 11 and stats.get("asset_class") == "CRYPTO" and stats.get("prior_strength") == 20, str(stats))

# 22
os.environ["MACE_EMPIRICAL_KELLY_JSON"] = "{not json"
try:
    parsed = rrt.parse_empirical_env()
    check("22. parse_empirical_env None on malformed JSON", parsed is None, str(parsed))
finally:
    os.environ.pop("MACE_EMPIRICAL_KELLY_JSON", None)

# 23
os.environ.pop("MACE_EMPIRICAL_KELLY_JSON", None)
check("23. parse_empirical_env None when env unset", rrt.parse_empirical_env() is None)

# 24
emp = {"n": 40, "win_rate": 0.35, "payoff": 1.9}
wr, po = rrt.blend_with_prior(emp, 0.55, 1.20, k=20)
w = 40 / 60
check("24. shrinkage blend exact (w=n/(n+k))",
      abs(wr - (w * 0.35 + (1 - w) * 0.55)) < 1e-12 and abs(po - (w * 1.9 + (1 - w) * 1.20)) < 1e-12, f"wr={wr} po={po}")

# 25
emp = {"n": 5, "win_rate": 0.30, "payoff": None}
wr, po = rrt.blend_with_prior(emp, 0.55, 1.20, k=20)
check("25. blend keeps prior payoff when empirical payoff None (zero losses)", abs(po - 1.20) < 1e-12 and abs(wr - (5/25*0.30 + 20/25*0.55)) < 1e-12, f"wr={wr} po={po}")

# =========================================================================
print("\n--- FIX 4: crypto Sharpe annualization ---")
sys.path.insert(0, os.path.join(MACE, "crypto/swarm"))

# 26
brain_src = open(os.path.join(MACE, "crypto/swarm/brain.py")).read()
has_map = '"4h": 2190' in brain_src and '"1d": 365' in brain_src
check("26. brain maps 4h->2190 periods/year (was sqrt(365))", has_map)

# 27 + 28: end-to-end through the real brain subprocess
import random
random.seed(7)
p = 100.0
prices = []
for _ in range(540):
    p *= (1 + random.gauss(0.0002, 0.01))
    prices.append(round(p, 6))

def run_brain(payload, env_extra=None):
    env = dict(os.environ)
    env.pop("MACE_EMPIRICAL_KELLY_JSON", None)
    if env_extra:
        env.update(env_extra)
    r = subprocess.run([sys.executable, os.path.join(MACE, "crypto/swarm/brain.py")],
                       input=json.dumps(payload), capture_output=True, text=True, env=env)
    if r.returncode != 0:
        return None
    return json.loads(r.stdout.strip())

out_4h = run_brain({"symbol": "BTC/USDT", "prices": prices, "timeframe": "4h"})
out_1d = run_brain({"symbol": "BTC/USDT", "prices": prices, "timeframe": "1d"})
ratio = (out_4h["signal_strength"] / out_1d["signal_strength"]) if out_1d and out_1d["signal_strength"] else 0
check("27. signal_strength(4h)/signal_strength(1d) == sqrt(6)", out_4h and out_1d and abs(ratio - 6 ** 0.5) < 0.01, f"ratio={ratio}")

emp_env = {"MACE_EMPIRICAL_KELLY_JSON": json.dumps({"n": 40, "win_rate": 0.35, "payoff": 1.9, "avg_win_pct": 0.02, "avg_loss_pct": 0.01, "asset_class": "CRYPTO", "prior_strength": 20})}
out_emp = run_brain({"symbol": "BTC/USDT", "prices": prices, "timeframe": "4h"}, env_extra=emp_env)
expected_wr = 40 / 60 * 0.35 + 20 / 60 * 0.55
check("28. brain honors empirical env: kelly_basis + bar_timeframe fields",
      out_emp and out_emp.get("bar_timeframe") == "4h" and out_emp.get("kelly_basis", "").startswith("empirical n=40")
      and abs(float(out_emp["kelly_basis"].split("wr=")[1].split()[0]) - expected_wr) < 0.005,
      str(out_emp.get("kelly_basis") if out_emp else None))

# =========================================================================
print("\n--- FIX 5: multi-venue live-price pool ---")

# 29
vl = price_venues.get_venue_list()
check("29. default venue pool (kucoin,binance,bybit)", vl == ["kucoin", "binance", "bybit"], str(vl))

# 30
os.environ["MACE_PRICE_VENUES"] = "binance, okx ,kraken"
try:
    vl = price_venues.get_venue_list()
    check("30. MACE_PRICE_VENUES env override parsed", vl == ["binance", "okx", "kraken"], str(vl))
finally:
    os.environ.pop("MACE_PRICE_VENUES", None)

# 31 + 32: inject a fake ccxt module — price_venues imports it lazily inside
# fetch_live_price_pool, so sys.modules injection intercepts without network.
def install_fake_ccxt(venue_behaviors):
    """venue_behaviors: {venue_name: last_price_or_Exception}"""
    fake = types.ModuleType("ccxt")
    for vname, behavior in venue_behaviors.items():
        def _make_ex(behavior=behavior, vname=vname):
            class _Ex:
                def __init__(self, opts=None):
                    self._n = vname
                def fetch_ticker(self, pair):
                    if isinstance(behavior, Exception):
                        raise behavior
                    return {"last": behavior}
                def close(self):
                    return None
            return _Ex
        _Ex = _make_ex()
        _Ex.__name__ = vname
        setattr(fake, vname, _Ex)
    sys.modules["ccxt"] = fake
    return fake

_logs = []
async def _run_pool(venues=None):
    return await price_venues.fetch_live_price_pool(
        "BTC/USDT", fallback_price=123.0, log=lambda m: _logs.append(m), venue_list=venues)

# 31: all venues fail -> fallback + venue None
install_fake_ccxt({"kucoin": RuntimeError("down"), "binance": RuntimeError("down"), "bybit": RuntimeError("down")})
price, venue = asyncio.run(_run_pool(["kucoin", "binance", "bybit"]))
check("31. all venues fail -> fallback price, venue None (legacy contract)", price == 123.0 and venue is None, f"{price}/{venue}")

# 32: primary fails, secondary serves -> failover + log line
install_fake_ccxt({"kucoin": RuntimeError("primary down"), "binance": 555.5, "bybit": 999.0})
_logs.clear()
price, venue = asyncio.run(_run_pool(["kucoin", "binance", "bybit"]))
check("32. primary fails -> failover to secondary venue with log",
      abs(price - 555.5) < 1e-9 and venue == "binance" and any("served by binance" in m for m in _logs),
      f"{price}/{venue}/{_logs}")
sys.modules.pop("ccxt", None)

# =========================================================================
print("\n--- FIX 6: stale-ledger write-down purge (3-sweep patience + canary) ---")
sys.path.insert(0, MACE)
import importlib
orch = importlib.import_module("crypto.swarm.orchestrator") if "crypto.swarm.orchestrator" in sys.modules else None
if orch is None:
    import crypto.swarm.orchestrator as orch  # paho stubbed above

_orig_orch_db = orch.DEFAULT_DB_PATH
_orig_avail = price_venues.price_available_anywhere
orch.DEFAULT_DB_PATH = SCRATCH

def seed_ledger(tokens):
    db = fresh_db()
    seed_usdt(1000.0)
    for tok, qty, entry in tokens:
        with sqlite3.connect(SCRATCH) as conn:
            conn.execute("INSERT INTO portfolio VALUES ('ARBITRUM', ?, ?, ?)", (tok, qty, entry))
    orch._LEDGER_PRICE_FAILS.clear()
    return db

def fake_avail(unpriceable):
    async def _avail(pair):
        base = pair.split("/")[0]
        return base not in unpriceable
    return _avail

# 33: BEAT unpriceable sweep 1 -> deferred, row intact
db = seed_ledger([("BTC", 0.1, 100.0), ("BEAT", 2000.0, 0.50)])
price_venues.price_available_anywhere = fake_avail({"BEAT"})
asyncio.run(orch.purge_unpriceable_ledger_rows())
rows = q("SELECT token FROM portfolio WHERE token='BEAT'")
trips = q("SELECT symbol FROM realized_round_trips")
check("33. sweep 1 unpriceable -> deferred (row intact, no trip)", rows and not trips, f"rows={rows} trips={trips}")

# 34: sweeps 2+3 -> write-down fires: row deleted, exit=0 trip recorded
asyncio.run(orch.purge_unpriceable_ledger_rows())
asyncio.run(orch.purge_unpriceable_ledger_rows())
rows = q("SELECT token FROM portfolio WHERE token='BEAT'")
trip = q("SELECT exit_price, reason, basis, pnl_pct FROM realized_round_trips WHERE symbol='BEAT/USDT'")
check("34. sweep 3 -> write-down (row purged, -100% ledger_exact trip)",
      not rows and trip and trip[0][0] == 0.0 and trip[0][1] == "STALE_LEDGER_WRITE_DOWN" and trip[0][2] == "ledger_exact" and trip[0][3] == -1.0,
      f"rows={rows} trip={trip}")

# 35: outage canary -> 4/5 unpriceable = purge skipped entirely
db = seed_ledger([("BTC", 0.1, 100.0), ("ETH", 1.0, 50.0), ("SOL", 2.0, 20.0), ("XRP", 100.0, 1.0), ("DOGE", 500.0, 0.2)])
price_venues.price_available_anywhere = fake_avail({"BTC", "ETH", "SOL", "XRP"})
for _ in range(5):  # far beyond patience
    asyncio.run(orch.purge_unpriceable_ledger_rows())
n_rows = len(q("SELECT token FROM portfolio WHERE token != 'USDT'"))
trips = q("SELECT symbol FROM realized_round_trips WHERE reason='STALE_LEDGER_WRITE_DOWN' AND symbol != 'BEAT/USDT'")
check("35. outage canary: >=half unpriceable -> purge skipped (rows intact)", n_rows == 5 and not trips, f"rows={n_rows} trips={trips}")

# 36: fail counter resets after a successful price read
db = seed_ledger([("WIF", 100.0, 1.0), ("BTC", 0.1, 100.0)])
price_venues.price_available_anywhere = fake_avail({"WIF"})
asyncio.run(orch.purge_unpriceable_ledger_rows())
asyncio.run(orch.purge_unpriceable_ledger_rows())          # fails = 2
price_venues.price_available_anywhere = fake_avail(set())  # venue recovers
asyncio.run(orch.purge_unpriceable_ledger_rows())          # reset to 0
price_venues.price_available_anywhere = fake_avail({"WIF"})
asyncio.run(orch.purge_unpriceable_ledger_rows())          # fails = 1 again
rows = q("SELECT token FROM portfolio WHERE token='WIF'")
check("36. fail count resets on successful price (no premature purge)", rows and orch._LEDGER_PRICE_FAILS.get("WIF") == 1, str(orch._LEDGER_PRICE_FAILS))

# 37: empty ledger handled safely
db = fresh_db(); seed_usdt(100.0)
price_venues.price_available_anywhere = fake_avail(set())
try:
    asyncio.run(orch.purge_unpriceable_ledger_rows())
    check("37. empty/non-USDT-only ledger purge is a safe no-op", True)
except Exception as e:
    check("37. empty/non-USDT-only ledger purge is a safe no-op", False, str(e))
price_venues.price_available_anywhere = _orig_avail
orch.DEFAULT_DB_PATH = _orig_orch_db

# =========================================================================
print("\n--- FIX 7: README truth pass + schema presence ---")
readme = open(os.path.join(MACE, "README.md")).read()
# 38
env_vars_ok = all(v in readme for v in [
    "MACE_NEWS_GATE", "MACE_NEWS_STALENESS_HOURS", "MACE_TAKER_FEE",
    "MACE_KELLY_MIN_ROUNDS", "MACE_KELLY_LOOKBACK", "MACE_PRICE_VENUES",
    "MACE_STALE_LEDGER_SWEEPS", "MACE_EMPIRICAL_KELLY_JSON",
])
truth_ok = ("Yahoo Finance RSS" in readme and "hardcoded constants" in readme
            and "realized_round_trips" in readme and "component_health" in readme)
check("38. README documents v1.3 env vars + truth-pass corrections", env_vars_ok and truth_ok)

# 39
sql = open(os.path.join(MACE, "setup_db.sql")).read()
tbl_rtt = ("CREATE TABLE IF NOT EXISTS realized_round_trips" in sql and "asset_class" in sql)
tbl_ch = "CREATE TABLE IF NOT EXISTS component_health" in sql and "last_healthy_at" in sql
check("39. setup_db.sql defines both new tables (round trips + component health)", tbl_rtt and tbl_ch)

# =========================================================================
print("\n--- GATE: py_compile every touched file ---")
import py_compile
touched = [
    "realized_round_trips.py", "price_venues.py", "setup_db.sql",
    "crypto/swarm/orchestrator.py", "crypto/swarm/guardrail.py",
    "crypto/swarm/brain.py", "crypto/swarm/scout.py",
    "crypto/crypto_shield.py",
    "equities/swarm/orchestrator.py", "equities/swarm/brain.py",
    "equities/tradfi_shield.py", "equities/tradfi_news_guard.py",
]
compile_errors = []
for rel in touched:
    if rel.endswith(".sql"):
        continue
    try:
        py_compile.compile(os.path.join(MACE, rel), doraise=True)
    except Exception as e:
        compile_errors.append(f"{rel}: {e}")
check("40. py_compile clean on all 11 python files", not compile_errors, str(compile_errors))

# =========================================================================
print(f"\n==================== RESULT: {len(PASS)}/{len(PASS)+len(FAIL)} ====================")
if FAIL:
    print("FAILED:")
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL v1.3 CHECKS PASS")
if os.path.exists(SCRATCH):
    os.remove(SCRATCH)
