#!/usr/bin/env python3
"""v1.4 patch behavioral tests - sandbox-only (scratch DB, stubbed MQTT,
mocked ccxt - no network, no keys). Portable: drop next to the repo root or
set MACE_ROOT (mirrors test_v13_fixes.py).

Fix A: shield resilient feeds
  - live prices route through the price_venues pool with two_sided=True
  - total pool failure / pool exception -> None (skip position this sweep)
  - no long-lived ccxt sessions at boot
  - per-position crash isolation (one poisoned row cannot abort the sweep)
  - deterministic conn cleanup (finally), degraded-preserving heartbeat
Fix B: hwm updater venue-pool parity
  - per-venue isolation + failover, close() on every path (aiohttp-leak fix)
  - vol-stop compute exception widens to 8% (never tightens to min_bound)
  - hwm_updater heartbeat row
Fix C: ghost-ticker hardening
  - two_sided=True rejects last-only ghosts, default stays last-only
  - purge oracle requires two-sided quotes
Fix D: honest-stats guards
  - empirical payoff n-bar + [0.5, 3.0] clamp in blend_with_prior
  - generic component heartbeat (call-time db_path)
Gate : py_compile on every touched file + no datetime.utcnow() left
"""
import sys, os, types, sqlite3, asyncio, json, logging, subprocess, tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
MACE = os.environ.get("MACE_ROOT", "")
if not MACE:
    for _cand in (_HERE, os.getcwd(), os.path.dirname(_HERE)):
        if os.path.isfile(os.path.join(_cand, "crypto", "crypto_shield.py")):
            MACE = os.path.abspath(_cand)
            break
if not MACE:
    MACE = "/home/z/my-project/mace"
SCRATCH = os.path.join(_HERE, "scratch_v14.db")
sys.path.insert(0, MACE)
sys.path.insert(0, os.path.join(MACE, "crypto"))

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

# bare ccxt stub - enough for crypto_shield's module-level import; the pool
# and the hwm leg install their own behavior-rich fakes per test group.
_ccxt_stub = types.ModuleType("ccxt")
sys.modules.setdefault("ccxt", _ccxt_stub)

# ---------------------------------------------------------------- setup -----
import realized_round_trips as rrt
import price_venues
import crypto_shield as cs
import hwm_stop_updater as hwm

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

def seed_positional_book():
    """BTC = healthy corridor (hwm 100), POISON = zero HWM (div-by-zero in the
    drawdown math) - the isolation trigger."""
    with sqlite3.connect(SCRATCH) as conn:
        conn.execute("INSERT OR REPLACE INTO portfolio VALUES ('ARBITRUM', 'USDT', 10000.0, 1.0, 0)")
        conn.execute("INSERT OR REPLACE INTO portfolio VALUES ('ARBITRUM', 'BTC', 0.1, 100.0, 0)")
        conn.execute("INSERT OR REPLACE INTO portfolio VALUES ('ARBITRUM', 'POISON', 100.0, 0.5, 0)")
        conn.execute("""
            INSERT OR REPLACE INTO crypto_hwm (asset_id, symbol, high_water_mark, loss_limit, updated_at)
            SELECT asset_id, symbol, 100.0, 0.08, '2026-09-26T00:00:00Z' FROM vw_crypto_universe
            WHERE symbol IN ('BTC/USDT', 'POISON/USDT')
        """)
        conn.execute("UPDATE crypto_hwm SET high_water_mark = 0.0 WHERE symbol = 'POISON/USDT'")

def seed_universe():
    with sqlite3.connect(SCRATCH) as conn:
        for sym in ("BTC/USDT", "POISON/USDT"):
            conn.execute("""
                INSERT OR IGNORE INTO asset_universe (symbol, asset_class, asset_name, category, broker, exchange, currency)
                VALUES (?, 'CRYPTO', ?, 'CRYPTO', 'kucoin', 'BINANCE', 'USDT')
            """, (sym, sym.split("/")[0]))

# route every call-time DB default at the scratch file
rrt.DEFAULT_DB_PATH = SCRATCH
cs.DEFAULT_DB_PATH = SCRATCH
hwm.DB_PATH = SCRATCH

# log capture
class _Cap(logging.Handler):
    def __init__(self):
        super().__init__()
        self.msgs = []
    def emit(self, record):
        self.msgs.append(record.getMessage())
cap_shield = _Cap(); logging.getLogger("mace.crypto_shield").addHandler(cap_shield)
cap_hwm = _Cap(); logging.getLogger("mace.hwm_updater").addHandler(cap_hwm)

# =========================================================================
print("\n--- FIX A: shield resilient feeds ---")

# 1-3: fetch_live_price routes through the pool, two-sided, total-failure safe
pool_calls = []
_fake_result = {"value": (0.42, "binance")}
async def _fake_pool(pair, fallback_price=None, log=None, venue_list=None, two_sided=False):
    pool_calls.append({"pair": pair, "two_sided": two_sided, "has_log": log is not None})
    if isinstance(_fake_result["value"], Exception):
        raise _fake_result["value"]
    return _fake_result["value"]
_orig_pool = price_venues.fetch_live_price_pool
price_venues.fetch_live_price_pool = _fake_pool

shield = cs.CryptoShield()
px = asyncio.run(shield.fetch_live_price("BEAT_USDT"))
check("1. fetch routes through venue pool (pair normalized, log wired)",
      px == 0.42 and pool_calls and pool_calls[-1]["pair"] == "BEAT/USDT" and pool_calls[-1]["has_log"],
      f"px={px} calls={pool_calls}")

check("2. pool called with two_sided=True (ghost tickers cannot price a stop-out)",
      pool_calls and pool_calls[-1]["two_sided"] is True, str(pool_calls))

_fake_result["value"] = (0.0, None)
px = asyncio.run(shield.fetch_live_price("BEAT_USDT"))
check("3. total pool failure -> None (position skipped this sweep)", px is None, f"px={px}")

_fake_result["value"] = RuntimeError("pool exploded")
px = asyncio.run(shield.fetch_live_price("BEAT_USDT"))
check("4. pool exception -> None (belt-and-braces guard)", px is None, f"px={px}")
_fake_result["value"] = (0.42, "binance")

# 5: no long-lived ccxt sessions at boot
class _Boom:
    def __init__(self, *a, **k):
        raise AssertionError("long-lived exchange session constructed")
_ccxt_stub.kucoin = _Boom
_ccxt_stub.binance = _Boom
try:
    shield2 = cs.CryptoShield()
    ok = shield2.venues == ["kucoin", "binance", "bybit"]
except AssertionError as e:
    ok = False
check("5. boot builds no long-lived exchange sessions (pool list only)", ok, str(getattr(shield2, 'venues', None)))

# 6-8: full sweep - isolation, cleanup, heartbeat
class _ConnProxy:
    def __init__(self, conn):
        self._conn = conn
        self.closed = 0
    def close(self):
        self.closed += 1
        self._conn.close()
    def __getattr__(self, name):
        return getattr(self._conn, name)

_close_log = []
_orig_get_db = cs.get_db_connection
def _counted_conn(db_path=None):
    proxy = _ConnProxy(_orig_get_db(SCRATCH))
    _close_log.append(proxy)
    return proxy
cs.get_db_connection = _counted_conn

db = fresh_db(); seed_universe(); seed_positional_book()
_fake_result["value"] = (99.0, "kucoin")   # above BTC floor (92): no breach
cap_shield.msgs.clear()
asyncio.run(shield.run_shield_cycle())
rows = q("SELECT token FROM portfolio WHERE token IN ('BTC','POISON')")
hb = q("SELECT status, last_healthy_at, detail FROM component_health WHERE component='crypto_shield'")
isolated = any("Position check failed for POISON/USDT" in m for m in cap_shield.msgs)
check("6. poisoned HWM row isolated; healthy BTC row survives the sweep",
      sorted(r[0] for r in rows) == ["BTC", "POISON"] and isolated,
      f"rows={rows} msgs={[m[:60] for m in cap_shield.msgs]}")
check("7. sweep closes its DB connection exactly once (finally-cleanup)",
      len(_close_log) == 1 and _close_log[0].closed == 1,
      f"conns={len(_close_log)} closed={[p.closed for p in _close_log]}")
check("8. healthy sweep writes crypto_shield heartbeat (status + detail)",
      hb and hb[0][0] == "healthy" and hb[0][1] is not None and "positions=2" in (hb[0][2] or ""),
      str(hb))

# 9: degraded sweep preserves last_healthy_at
def _dead_conn(db_path=None):
    raise RuntimeError("database exploded")
cs.get_db_connection = _dead_conn
cap_shield.msgs.clear()
asyncio.run(shield.run_shield_cycle())
hb2 = q("SELECT status, last_healthy_at FROM component_health WHERE component='crypto_shield'")
check("9. degraded sweep flips status, preserves last_healthy_at",
      hb2 and hb2[0][0] == "degraded" and hb2[0][1] == hb[0][1],
      f"before={hb[0][1] if hb else None} after={hb2[0][1] if hb2 else None}")
cs.get_db_connection = _counted_conn
price_venues.fetch_live_price_pool = _orig_pool

# =========================================================================
print("\n--- FIX B: hwm updater venue-pool parity ---")

# 10-12: async venue pool with per-venue isolation
def install_fake_async_ccxt(behaviors, closes):
    fake_async = types.ModuleType("ccxt.async_support")
    for vname, beh in behaviors.items():
        def _make(beh=beh, vname=vname):
            class _Ex:
                id = vname
                def __init__(self, opts=None):
                    self.venue = vname
                async def fetch_ticker(self, pair):
                    t = beh.get("ticker")
                    if isinstance(t, Exception):
                        raise t
                    return dict(t)
                async def fetch_ohlcv(self, pair, timeframe='1m', limit=1440):
                    o = beh.get("ohlcv")
                    if isinstance(o, Exception):
                        raise o
                    return list(o)
                async def close(self):
                    closes.append(vname)
            _Ex.__name__ = vname
            return _Ex
        setattr(fake_async, vname, _make())
    fake_ccxt = types.ModuleType("ccxt")
    fake_ccxt.async_support = fake_async
    sys.modules["ccxt"] = fake_ccxt
    sys.modules["ccxt.async_support"] = fake_async
    return fake_async

BARS = [[1700000000 + i * 60, 100.0, 100.0, 100.0, 100.0, 1.0] for i in range(150)]

def seed_crypto_book():
    with sqlite3.connect(SCRATCH) as conn:
        conn.execute("INSERT OR REPLACE INTO portfolio VALUES ('ARBITRUM', 'USDT', 1000.0, 1.0, 0)")
        conn.execute("INSERT OR REPLACE INTO portfolio VALUES ('ARBITRUM', 'BTC', 0.1, 100.0, 0)")

# 10: primary (kucoin) fails, binance serves -> hwm updated from failover data
db = fresh_db(); seed_universe(); seed_crypto_book()
with sqlite3.connect(SCRATCH) as conn:
    conn.execute("""
        INSERT OR REPLACE INTO crypto_hwm (asset_id, symbol, high_water_mark, loss_limit, updated_at)
        SELECT asset_id, symbol, 90.0, 0.10, '2026-09-26T00:00:00Z' FROM vw_crypto_universe WHERE symbol='BTC/USDT'
    """)
closes = []
install_fake_async_ccxt({
    "kucoin": {"ticker": RuntimeError("kucoin transport degraded")},
    "binance": {"ticker": {"last": 120.0, "bid": 119.5, "ask": 120.5}, "ohlcv": BARS},
    "bybit": {"ticker": {"last": 121.0, "bid": 120.5, "ask": 121.5}, "ohlcv": BARS},
}, closes)
cap_hwm.msgs.clear()
summary = asyncio.run(hwm.sync_crypto_positions())
hwm_row = q("SELECT high_water_mark, loss_limit FROM crypto_hwm WHERE symbol='BTC/USDT'")
served = any("market data served by binance" in m for m in cap_hwm.msgs)
check("10. kucoin failure -> failover venue serves; ratchet updates (hwm 90 -> 120)",
      hwm_row and abs(hwm_row[0][0] - 120.0) < 1e-9 and served,
      f"hwm={hwm_row} msgs={[m[:70] for m in cap_hwm.msgs]}")
check("11. every async exchange instance close()d incl. the failed primary (aiohttp-leak fix)",
      sorted(closes) == ["binance", "bybit", "kucoin"], f"closes={closes}")

# 11b: all venues fail -> no crash, no row change, class name in warning
closes.clear()
install_fake_async_ccxt({
    "kucoin": {"ticker": RuntimeError("kucoin down")},
    "binance": {"ticker": RuntimeError("binance down")},
    "bybit": {"ticker": RuntimeError("bybit down")},
}, closes)
cap_hwm.msgs.clear()
summary = asyncio.run(hwm.sync_crypto_positions())
hwm_row = q("SELECT high_water_mark FROM crypto_hwm WHERE symbol='BTC/USDT'")
warned = any("Failed fetching crypto market data for BTC/USDT across 3 venues" in m
             and "RuntimeError" in m for m in cap_hwm.msgs)
check("11b. all-venues failure isolated per symbol; warning carries class name",
      warned and abs(hwm_row[0][0] - 120.0) < 1e-9 and sorted(closes) == ["binance", "bybit", "kucoin"],
      f"closes={closes} msgs={[m[:70] for m in cap_hwm.msgs]}")
sys.modules.pop("ccxt", None)
sys.modules.pop("ccxt.async_support", None)

# 12: vol-stop compute exception widens to 8% (was min_bound - wrong direction)
check("12. vol-stop compute exception -> 8% fallback (never tightens to min_bound)",
      hwm.calculate_24h_rolling_volatility_stop(None, min_bound=0.060) == 0.080,
      str(hwm.calculate_24h_rolling_volatility_stop(None, min_bound=0.060)))

# 13: run_update_sweep heartbeat
async def _fake_tradfi(client):
    return []
async def _fake_crypto():
    return [{"symbol": "BTC/USDT", "vol_basis": "crypto_1m"}]
_orig_tradfi, _orig_crypto = hwm.sync_tradfi_positions, hwm.sync_crypto_positions
_orig_client = hwm.get_client_by_name
hwm.sync_tradfi_positions, hwm.sync_crypto_positions = _fake_tradfi, _fake_crypto
hwm.get_client_by_name = lambda name: object()
asyncio.run(hwm.run_update_sweep())
hb_hwm = q("SELECT status, last_healthy_at, detail FROM component_health WHERE component='hwm_updater'")
check("13. hwm_updater heartbeat healthy with sweep detail",
      hb_hwm and hb_hwm[0][0] == "healthy" and hb_hwm[0][1] is not None and "crypto=1" in (hb_hwm[0][2] or ""),
      str(hb_hwm))
hwm.sync_tradfi_positions, hwm.sync_crypto_positions = _orig_tradfi, _orig_crypto
hwm.get_client_by_name = _orig_client

# =========================================================================
print("\n--- FIX C: ghost-ticker hardening ---")

def install_fake_sync_ccxt(venue_tickers):
    fake = types.ModuleType("ccxt")
    for vname, ticker in venue_tickers.items():
        def _make(ticker=ticker, vname=vname):
            class _Ex:
                def __init__(self, opts=None): pass
                def fetch_ticker(self, pair):
                    return dict(ticker)
                def close(self):
                    return None
            _Ex.__name__ = vname
            return _Ex
        setattr(fake, vname, _make())
    sys.modules["ccxt"] = fake
    return fake

GHOST = {"last": 100.0}                                   # stale last, no quote
TWOSIDED = {"last": 100.0, "bid": 99.5, "ask": 100.5}

install_fake_sync_ccxt({"kucoin": GHOST, "binance": TWOSIDED, "bybit": TWOSIDED})
px, vn = asyncio.run(price_venues.fetch_live_price_pool("X/USDT", two_sided=True))
check("14. two_sided=True skips ghost primary, served by two-sided venue",
      abs(px - 100.0) < 1e-9 and vn == "binance", f"{px}/{vn}")

px, vn = asyncio.run(price_venues.fetch_live_price_pool("X/USDT"))
check("15. default two_sided=False keeps last-only contract (orchestrator fills)",
      abs(px - 100.0) < 1e-9 and vn == "kucoin", f"{px}/{vn}")

install_fake_sync_ccxt({"kucoin": GHOST, "binance": dict(GHOST), "bybit": dict(GHOST)})
ok = not asyncio.run(price_venues.price_available_anywhere("X/USDT"))
px, vn = asyncio.run(price_venues.fetch_live_price_pool("X/USDT", fallback_price=7.0, two_sided=True))
check("16. all-ghost pool -> fallback + purge oracle says unpriceable",
      ok and vn is None and abs(px - 7.0) < 1e-9, f"oracle={ok} {px}/{vn}")

install_fake_sync_ccxt({"kucoin": TWOSIDED, "binance": TWOSIDED, "bybit": TWOSIDED})
check("17. two-sided ticker -> purge oracle priceable", asyncio.run(price_venues.price_available_anywhere("X/USDT")))
sys.modules.pop("ccxt", None)

# =========================================================================
print("\n--- FIX D: honest-stats guards ---")

# 18: below the payoff bar -> prior payoff exact (the live n=18/12.49 case)
wr, po = rrt.blend_with_prior({"n": 18, "win_rate": 0.532, "payoff": 12.49}, 0.55, 1.20, k=20)
w18 = 18 / 38
check("18. n=18 payoff 12.49 -> prior payoff (n-bar holds); wr still blended",
      abs(po - 1.20) < 1e-12 and abs(wr - (w18 * 0.532 + (1 - w18) * 0.55)) < 1e-12, f"wr={wr} po={po}")

# 19: at/above the bar -> clamped high
wr, po = rrt.blend_with_prior({"n": 40, "win_rate": 0.35, "payoff": 12.49}, 0.55, 1.20, k=20)
w40 = 40 / 60
check("19. n=40 payoff 12.49 -> clamped to 3.0 before blending",
      abs(po - (w40 * 3.0 + (1 - w40) * 1.20)) < 1e-12, f"po={po}")

# 20: clamped low
wr, po = rrt.blend_with_prior({"n": 40, "win_rate": 0.35, "payoff": 0.10}, 0.55, 1.20, k=20)
check("20. n=40 payoff 0.1 -> clamped up to 0.5 before blending",
      abs(po - (w40 * 0.5 + (1 - w40) * 1.20)) < 1e-12, f"po={po}")

# 21: in-range payoff -> exact v1.3 math (regression parity)
wr, po = rrt.blend_with_prior({"n": 40, "win_rate": 0.35, "payoff": 1.9}, 0.55, 1.20, k=20)
check("21. n=40 payoff 1.9 (in range) -> v1.3-exact blend math",
      abs(wr - (w40 * 0.35 + (1 - w40) * 0.55)) < 1e-12 and abs(po - (w40 * 1.9 + (1 - w40) * 1.20)) < 1e-12,
      f"wr={wr} po={po}")

# 22: env override honored
os.environ["MACE_KELLY_PAYOFF_MIN_ROUNDS"] = "5"
try:
    w6 = 6 / 26
    wr, po = rrt.blend_with_prior({"n": 6, "win_rate": 0.30, "payoff": 9.0}, 0.55, 1.20, k=20)
    check("22. MACE_KELLY_PAYOFF_MIN_ROUNDS env override honored (n=6 participates, clamped)",
          abs(po - (w6 * 3.0 + (1 - w6) * 1.20)) < 1e-12, f"po={po}")
finally:
    os.environ.pop("MACE_KELLY_PAYOFF_MIN_ROUNDS", None)

# 23: generic heartbeat, call-time db_path
db = fresh_db()
ok = rrt.write_component_heartbeat("crypto_shield", "healthy", "detail-here")
row = q("SELECT status, last_healthy_at, detail FROM component_health WHERE component='crypto_shield'")
check("23. write_component_heartbeat upserts arbitrary component rows",
      ok and row and row[0][0] == "healthy" and row[0][1] is not None and row[0][2] == "detail-here", str(row))

# 24: both brains consume the guarded blend (no local copies)
brain_c = open(os.path.join(MACE, "crypto/swarm/brain.py")).read()
brain_t = open(os.path.join(MACE, "equities/swarm/brain.py")).read()
check("24. both brains import blend_with_prior from the shared module (guards propagate)",
      "from realized_round_trips import parse_empirical_env, blend_with_prior" in brain_c
      and "from realized_round_trips import parse_empirical_env, blend_with_prior" in brain_t)

# =========================================================================
print("\n--- GATE: compile + deprecation sweep ---")

touched = ["crypto/crypto_shield.py", "hwm_stop_updater.py", "price_venues.py",
           "realized_round_trips.py", "crypto/swarm/brain.py", "equities/swarm/brain.py"]
compile_ok = True
for rel in touched:
    r = subprocess.run([sys.executable, "-m", "py_compile", os.path.join(MACE, rel)],
                       capture_output=True, text=True)
    if r.returncode != 0:
        compile_ok = False
        print(r.stderr[-300:])
check("25. py_compile clean on every touched file", compile_ok)

import re as _re
def _no_docstrings(src):
    return _re.sub(r'\x22\x22\x22.*?\x22\x22\x22', '', src, flags=_re.S)
shield_src = _no_docstrings(open(os.path.join(MACE, "crypto/crypto_shield.py")).read())
rrt_src = _no_docstrings(open(os.path.join(MACE, "realized_round_trips.py")).read())
check("26. datetime.utcnow() swept from shield + rrt (no deprecation warnings)",
      "datetime.utcnow()" not in shield_src and "datetime.utcnow()" not in rrt_src)

# =========================================================================
print(f"\n==================== RESULT: {len(PASS)}/{len(PASS) + len(FAIL)} ====================")
if FAIL:
    print("FAILED:")
    for f in FAIL:
        print(f"  - {f}")
    sys.exit(1)
print("ALL v1.4 CHECKS PASS")
