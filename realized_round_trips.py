#!/usr/bin/env python3.11
"""
M.A.C.E. v1.3 shared module: realized round trips + component health.

Fix 3 (honest Kelly): every completed SELL with a known entry basis writes a row
into realized_round_trips. The orchestrators aggregate the last N rows into
empirical edge stats (win rate, payoff ratio) and hand them to the brains via
the MACE_EMPIRICAL_KELLY_JSON env var; the brains shrinkage-blend them with the
hardcoded priors. Until n >= MACE_KELLY_MIN_ROUNDS the priors remain in force.

Fix 1 (news-guard fail-neutral): the tradfi news guard writes a heartbeat into
component_health after every audit cycle. The equities orchestrator's buy gate
reads it: no fresh healthy audit -> new entries are held (fail-NEUTRAL: sells,
trims, stop-outs and liquidations stay fully live).

Import contract: this file lives at the MACE repo root; consumers must put the
repo root on sys.path before importing (mirrors brokers/__init__.py usage).
"""

import os
import sys
import json
import sqlite3
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config", "portfolio.db")

# Idempotent schema guard: one CREATE TABLE IF NOT EXISTS attempt per db path
# per process lifetime (the 60s daemon sweep must not re-run DDL every cycle).
_SCHEMA_OK = set()

KELLY_PRIOR_STRENGTH = 20  # shrinkage k: empirical weight = n / (n + k)


def ensure_schema(db_path=DEFAULT_DB_PATH):
    db_path = os.path.abspath(db_path)
    if db_path in _SCHEMA_OK:
        return
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        with sqlite3.connect(db_path, timeout=30.0) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS realized_round_trips (
                    trip_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_class TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    qty REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_price REAL NOT NULL,
                    pnl_usd REAL NOT NULL,
                    pnl_pct REAL NOT NULL,
                    reason TEXT,
                    basis TEXT NOT NULL DEFAULT 'ledger_exact',
                    closed_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS component_health (
                    component TEXT PRIMARY KEY,
                    last_attempt_at TEXT,
                    last_healthy_at TEXT,
                    status TEXT NOT NULL,
                    detail TEXT
                )
            """)
        _SCHEMA_OK.add(db_path)
    except Exception:
        # Never let schema bookkeeping kill a trading path; the next writer
        # or sweep will retry the DDL.
        pass


# ----------------------------------------------------------------------------
# Fix 3: round-trip writer + empirical edge stats
# ----------------------------------------------------------------------------

def record_trip(asset_class, symbol, qty, entry_price, exit_price,
                reason="UNSPECIFIED", basis="ledger_exact", db_path=DEFAULT_DB_PATH):
    """Records one realized exit. exit_price=0 models a total write-down
    (STALE_LEDGER_WRITE_DOWN) -> pnl_pct = -1.0. Failures are swallowed:
    a stats-recording problem must never block an actual liquidation."""
    try:
        ensure_schema(db_path)
        entry_price = float(entry_price)
        exit_price = float(exit_price)
        qty = float(qty)
        pnl_usd = (exit_price - entry_price) * qty
        pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0.0
        with sqlite3.connect(os.path.abspath(db_path), timeout=30.0) as conn:
            conn.execute(
                """INSERT INTO realized_round_trips
                   (asset_class, symbol, qty, entry_price, exit_price, pnl_usd, pnl_pct, reason, basis, closed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (asset_class, symbol, qty, entry_price, exit_price,
                 pnl_usd, pnl_pct, reason, basis,
                 datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
            )
        return True
    except Exception:
        return False


def load_empirical_edge_stats(asset_class, lookback=100, db_path=DEFAULT_DB_PATH):
    """Returns {n, win_rate, payoff, avg_win_pct, avg_loss_pct} over the last
    `lookback` realized exits for the asset class, or None when no rounds
    exist. A payoff of None means zero losing rounds observed (payoff is
    unbounded; the caller should keep the prior payoff ratio)."""
    try:
        with sqlite3.connect(os.path.abspath(db_path), timeout=30.0) as conn:
            rows = conn.execute(
                "SELECT pnl_pct, pnl_usd FROM realized_round_trips WHERE asset_class = ? ORDER BY trip_id DESC LIMIT ?",
                (asset_class, int(lookback))
            ).fetchall()
    except Exception:
        return None
    if not rows:
        return None
    pcts = [float(r[0]) for r in rows]
    usds = [float(r[1]) for r in rows]
    n = len(pcts)
    wins = [p for p, u in zip(pcts, usds) if u > 0]
    losses = [p for p, u in zip(pcts, usds) if u <= 0]
    win_rate = len(wins) / n if n else 0.0
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = abs(sum(losses) / len(losses)) if losses else 0.0
    payoff = (avg_win / avg_loss) if (avg_loss > 0 and wins) else None
    return {
        "n": n,
        "win_rate": round(win_rate, 4),
        "payoff": round(payoff, 4) if payoff is not None else None,
        "avg_win_pct": round(avg_win, 4),
        "avg_loss_pct": round(avg_loss, 4),
    }


def empirical_env_json(asset_class, db_path=DEFAULT_DB_PATH,
                       min_rounds=None, lookback=None):
    """Builds the MACE_EMPIRICAL_KELLY_JSON payload for a sweep, or returns
    None when the empirical sample is below the minimum (priors stay in
    force - no behavioral cliff on fresh installs)."""
    if min_rounds is None:
        min_rounds = int(os.getenv("MACE_KELLY_MIN_ROUNDS", "10"))
    if lookback is None:
        lookback = int(os.getenv("MACE_KELLY_LOOKBACK", "100"))
    stats = load_empirical_edge_stats(asset_class, lookback=lookback, db_path=db_path)
    if not stats or stats["n"] < max(1, min_rounds):
        return None
    stats["asset_class"] = asset_class
    stats["prior_strength"] = KELLY_PRIOR_STRENGTH
    return json.dumps(stats)


def blend_with_prior(empirical, prior_win_rate, prior_payoff, k=KELLY_PRIOR_STRENGTH):
    """Shrinkage blend: w = n / (n + k). k=20 means 20 prior-equivalent
    observations back the hardcoded priors, so small samples move the
    estimate gently and the priors dominate until real history accrues."""
    n = float(empirical.get("n", 0))
    if n <= 0:
        return prior_win_rate, prior_payoff
    w = n / (n + float(k))
    emp_wr = float(empirical.get("win_rate", prior_win_rate))
    wr = w * emp_wr + (1 - w) * prior_win_rate
    emp_po = empirical.get("payoff")
    if emp_po is None:
        po = prior_payoff
    else:
        po = w * float(emp_po) + (1 - w) * prior_payoff
    return wr, po


def parse_empirical_env(raw=None):
    """Brain-side parse of MACE_EMPIRICAL_KELLY_JSON. Returns None on any
    malformed/missing input (brains must never fault on bad telemetry)."""
    if raw is None:
        raw = os.environ.get("MACE_EMPIRICAL_KELLY_JSON")
    if not raw:
        return None
    try:
        stats = json.loads(raw)
        if not isinstance(stats, dict) or float(stats.get("n", 0)) <= 0:
            return None
        if stats.get("win_rate") is None:
            return None
        return stats
    except Exception:
        return None


# ----------------------------------------------------------------------------
# Fix 1: news-guard heartbeat + fail-neutral buy gate
# ----------------------------------------------------------------------------

def write_news_guard_heartbeat(status, detail="", db_path=DEFAULT_DB_PATH,
                               component="tradfi_news_guard"):
    """Upserts the guard's health row. `healthy` refreshes last_healthy_at;
    every other status (degraded / idle) records the attempt only, so the
    last_healthy_at timestamp keeps telling the truth about how long the
    sensor has been blind. Never raises."""
    try:
        ensure_schema(db_path)
        now_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        with sqlite3.connect(os.path.abspath(db_path), timeout=30.0) as conn:
            conn.execute(
                """INSERT INTO component_health (component, last_attempt_at, last_healthy_at, status, detail)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(component) DO UPDATE SET
                     last_attempt_at = excluded.last_attempt_at,
                     status = excluded.status,
                     detail = excluded.detail,
                     last_healthy_at = CASE WHEN excluded.status = 'healthy'
                                            THEN excluded.last_healthy_at
                                            ELSE component_health.last_healthy_at END""",
                (component, now_str, now_str if status == "healthy" else None, status,
                 (detail or "")[:200])
            )
        return True
    except Exception:
        return False


def news_guard_is_healthy(db_path=DEFAULT_DB_PATH, staleness_hours=None):
    """Returns (healthy, human_detail). Staleness default = 12h = 3x the 4h
    audit cadence (tolerates two consecutive missed audits). Missing table,
    missing row and unparseable timestamps all resolve to NOT healthy - a
    blind sensor must never read as green by accident."""
    if staleness_hours is None:
        staleness_hours = float(os.getenv("MACE_NEWS_STALENESS_HOURS", "12"))
    try:
        with sqlite3.connect(os.path.abspath(db_path), timeout=30.0) as conn:
            row = conn.execute(
                "SELECT last_healthy_at, status FROM component_health WHERE component = 'tradfi_news_guard'"
            ).fetchone()
    except Exception:
        return False, "component_health table missing (news guard has never reported)"
    if not row:
        return False, "no news-guard audit on record"
    last_healthy, status = row
    if not last_healthy:
        return False, f"news guard status={status or 'unknown'}, never completed a healthy audit"
    try:
        lh = datetime.strptime(last_healthy, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return False, f"unparseable last_healthy_at '{last_healthy}'"
    age = (datetime.utcnow() - lh).total_seconds() / 3600.0
    if age > staleness_hours:
        return False, (f"last healthy audit {last_healthy} "
                       f"({age:.1f}h ago > {staleness_hours:.0f}h staleness window)")
    return True, f"healthy as of {last_healthy} (age {age:.1f}h)"


def news_gate_allows_buys(db_path=DEFAULT_DB_PATH):
    """The orchestrator-side gate decision. MACE_NEWS_GATE=off bypasses the
    check entirely (legacy fail-open escape hatch). Returns (allowed, detail)."""
    mode = os.getenv("MACE_NEWS_GATE", "on").strip().lower()
    if mode in ("off", "0", "false", "no"):
        return True, "news gate disabled via MACE_NEWS_GATE=off (fail-open legacy behavior)"
    return news_guard_is_healthy(db_path=db_path)


if __name__ == "__main__":
    # Manual smoke: python3 realized_round_trips.py [db_path]
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_PATH
    ensure_schema(path)
    print(f"[rrt] schema ensured at {path}")
    stats_c = load_empirical_edge_stats("CRYPTO", db_path=path)
    stats_t = load_empirical_edge_stats("TRADFI", db_path=path)
    print(f"[rrt] CRYPTO rounds: {stats_c}")
    print(f"[rrt] TRADFI rounds: {stats_t}")
    print(f"[rrt] news gate: {news_gate_allows_buys(path)}")
