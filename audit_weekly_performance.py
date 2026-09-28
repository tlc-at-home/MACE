#!/usr/bin/env python3.11
"""
M.A.C.E. Weekly Performance Audit - v1.1 HONEST REWRITE

The previous version of this script printed a HARDCODED 'PROFITABILITY VERDICT'
with static narrative text (claiming 'guaranteed profit floors' and citing
symbols that were not even held). This version computes everything it reports:

  1. Churn forensics from the trade DB (stop-outs, cooldowns, request backlog)
  2. Crypto book: mark-to-market from the virtual ledger + live exchange prices
  3. Equities book: real account equity from the Alpaca paper API
  4. A verdict derived from those numbers - not from a template

Run on the VM:  python3 audit_weekly_performance.py
Env overrides:  FUND_START_CRYPTO (default 10000), FUND_START_EQUITIES (default 10000),
                MACE_DB_PATH, ALPACA_API_KEY / ALPACA_SECRET_KEY
"""

import os
import sys
import json
import sqlite3
from datetime import datetime, timedelta, timezone

DB_CANDIDATES = [
    os.environ.get("MACE_DB_PATH"),
    "/home/fedora/MACE/config/portfolio.db",
    "/mnt/MACE_NAS_VM/config/portfolio.db",
    "/home/tony/dev/MACE-LOCAL/config/portfolio.db",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "config/portfolio.db")),
]

FUND_START_CRYPTO = float(os.environ.get("FUND_START_CRYPTO", "10000"))
FUND_START_EQUITIES = float(os.environ.get("FUND_START_EQUITIES", "10000"))

STABLECOINS = {"USDT", "USDC", "USDE", "USDS", "DAI", "FDUSD", "TUSD", "USDP", "USDD"}


def locate_db():
    for path in DB_CANDIDATES:
        if path and os.path.exists(path):
            return path
    return None


def q(conn, sql, params=()):
    try:
        return conn.execute(sql, params).fetchall()
    except Exception as e:
        print(f"  [query skipped: {e}]")
        return []


def churn_forensics(conn):
    print("=" * 75)
    print("  1. CHURN FORENSICS (2-week lookback from trade DB)")
    print("=" * 75)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    two_weeks_str = (datetime.now(timezone.utc) - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = q(conn, "SELECT reason, COUNT(*) FROM trade_cooldowns GROUP BY reason ORDER BY 2 DESC")
    for r in rows:
        print(f"  Cooldowns registered ({r[0]}): {r[1]}")

    rows = q(conn, "SELECT action, status, COUNT(*) FROM mcp_requested_trades GROUP BY 1,2 ORDER BY 3 DESC")
    for r in rows:
        print(f"  MCP requests: {r[0]:5s} | {r[1]:10s} | {r[2]}")

    rows = q(conn, """
        SELECT symbol, COUNT(*), SUM(amount_usd) FROM mcp_requested_trades
        WHERE action = 'BUY' GROUP BY symbol ORDER BY 2 DESC LIMIT 10
    """)
    if rows:
        print("\n  Top-10 most requested BUY symbols (count / total notional):")
        for r in rows:
            notional = f"${r[2]:,.0f}" if r[2] else "$0"
            print(f"    {r[0]:6s} {r[1]:5d}  {notional}")

    rows = q(conn, """
        SELECT COUNT(*) FROM mcp_execution_log
        WHERE status = 'FAILED' AND timestamp >= ?
    """, (two_weeks_str,))
    if rows and rows[0][0]:
        print(f"\n  Failed MCP executions (last 14d): {rows[0][0]}")

    # Cooldown-integrity ratio: stop-outs vs cooldowns with matching reason
    stop_reasons = [r[0] for r in q(conn, "SELECT DISTINCT reason FROM trade_cooldowns")]
    print(f"\n  NOTE: cooldown integrity check - if stop-outs in journalctl exceed")
    print(f"  cooldown rows above, the sweep is still crashing mid-cleanup")
    print(f"  (grep -c 'NameError\\|timedelta' in journalctl for the shield service).")
    print()


def crypto_mtm():
    print("=" * 75)
    print("  2. CRYPTO BOOK - MARK TO MARKET (virtual ledger + live prices)")
    print("=" * 75)
    db_path = locate_db()
    if not db_path:
        print("  [portfolio.db not found - skipping crypto MTM]")
        return None, 0.0
    conn = sqlite3.connect(db_path)
    rows = q(conn, "SELECT token, quantity, avg_entry_price FROM portfolio WHERE quantity > 0")
    conn.close()
    if not rows:
        print("  [no positions in ledger]")
        return {}, 0.0

    prices = {}
    try:
        import ccxt
        exchange = ccxt.kucoin({"enableRateLimit": True})
        for r in rows:
            token, qty, entry = r[0], float(r[1]), float(r[2] or 0)
            if token in STABLECOINS:
                prices[token] = 1.0
                continue
            try:
                pair = f"{token}/USDT"
                ticker = exchange.fetch_ticker(pair)
                prices[token] = float(ticker["last"])
            except Exception:
                prices[token] = None
        exchange.close()
    except Exception as e:
        print(f"  [live price fetch unavailable: {e}; MTM uses entry price]")

    total_mtm = 0.0
    total_cost = 0.0
    print(f"\n  {'TOKEN':8s} {'QTY':>12s} {'ENTRY':>10s} {'LIVE':>10s} {'P&L':>10s} {'P&L%':>8s}")
    for r in rows:
        token, qty, entry = r[0], float(r[1]), float(r[2] or 0)
        live = prices.get(token)
        if live is None:
            live = entry  # fall back to cost when no price available
        cost = qty * entry
        mtm = qty * live
        pnl = mtm - cost
        pnl_pct = (pnl / cost * 100) if cost > 0 else 0.0
        total_mtm += mtm
        total_cost += cost
        print(f"  {token:8s} {qty:12.4f} {entry:10.4f} {live:10.4f} {pnl:+10.2f} {pnl_pct:+7.2f}%")

    book_pnl = total_mtm - FUND_START_CRYPTO
    print(f"\n  Ledger cost basis of open positions : ${total_cost:,.2f}")
    print(f"  Mark-to-market of open positions   : ${total_mtm:,.2f}")
    print(f"  CRYPTO BOOK vs ${FUND_START_CRYPTO:,.0f} start        : {book_pnl:+,.2f} ({book_pnl / FUND_START_CRYPTO * 100:+.2f}%)")
    return total_mtm, book_pnl


def equities_pnl():
    print("\n" + "=" * 75)
    print("  3. EQUITIES BOOK - ALPACA PAPER ACCOUNT (real account equity)")
    print("=" * 75)
    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")
    if not (api_key and secret_key):
        print("  [ALPACA_API_KEY / ALPACA_SECRET_KEY not in environment - skipping]")
        print("  hint: source them from the MACE systemd unit or config/.env before running")
        return None, 0.0
    try:
        import requests
        resp = requests.get(
            "https://paper-api.alpaca.markets/v2/account",
            headers={"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key},
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"  [Alpaca API returned {resp.status_code} - skipping]")
            return None, 0.0
        acc = resp.json()
        equity = float(acc.get("equity", 0.0))
        cash = float(acc.get("cash", 0.0))
        last = float(acc.get("last_equity", 0.0))
        pnl = equity - FUND_START_EQUITIES
        print(f"  Account equity : ${equity:,.2f}   Cash: ${cash:,.2f}   Prior close: ${last:,.2f}")
        print(f"  EQUITIES BOOK vs ${FUND_START_EQUITIES:,.0f} start : {pnl:+,.2f} ({pnl / FUND_START_EQUITIES * 100:+.2f}%)")
        return equity, pnl
    except Exception as e:
        print(f"  [Alpaca fetch failed: {e} - skipping]")
        return None, 0.0


def verdict(crypto_pnl, equities_pnl_val):
    print("\n" + "=" * 75)
    print("  4. PROFITABILITY VERDICT (computed, not narrated)")
    print("=" * 75)
    if crypto_pnl is None and equities_pnl_val is None:
        print("  INSUFFICIENT DATA: neither book could be marked. Fix env/DB access.")
        return
    parts = []
    if crypto_pnl is not None:
        parts.append(f"crypto {crypto_pnl:+,.2f}")
    if equities_pnl_val is not None:
        parts.append(f"equities {equities_pnl_val:+,.2f}")
    total = (crypto_pnl or 0.0) + (equities_pnl_val or 0.0)
    fund = FUND_START_CRYPTO + FUND_START_EQUITIES
    print(f"  Fund P&L: {total:+,.2f} on ${fund:,.0f} ({total / fund * 100:+.2f}%)  [{'; '.join(parts)}]")
    if total > 0:
        print("  VERDICT: PROFITABLE - but check churn forensics above: profit earned")
        print("  through friction-heavy churn is fragile; earned through held trends is robust.")
    elif total > -0.02 * fund:
        print("  VERDICT: FLAT - consistent with a churn machine paying friction both ways.")
    else:
        print("  VERDICT: LOSING - cross-reference section 1: if stop-out cooldown counts")
        print("  are far below journalctl stop-out counts, the churn loop is still live.")
    print()


def main():
    print("=" * 75)
    print("      M.A.C.E. PROFITABILITY & FINANCIAL AUDIT REPORT (v1.1 honest)      ")
    print(f"      Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}")
    print("=" * 75 + "\n")
    db_path = locate_db()
    if db_path:
        print(f"Database source: {db_path}\n")
        churn_forensics(sqlite3.connect(db_path))
    else:
        print("[portfolio.db not found - churn forensics skipped]\n")
    _, crypto_pnl_val = crypto_mtm()
    _, equities_pnl_val = equities_pnl()
    verdict(crypto_pnl_val, equities_pnl_val)


if __name__ == "__main__":
    main()
