#!/usr/bin/env python3.11
import os
import sys
import json
import sqlite3

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

def get_active_cooldowns():
    cooldowns = {}
    if os.path.exists(DB_PATH):
        try:
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                rows = cursor.execute("SELECT symbol, cooldown_until, reason FROM vw_active_cooldowns").fetchall()
                for r in rows:
                    cooldowns[r[0]] = {"cooldown_until": r[1], "reason": r[2]}
        except Exception as e:
            sys.stderr.write(f"[!] Cooldown lookup exception: {e}\n")
    return cooldowns

def get_recent_cooldown_expiries(window_hours=72):
    """v1.1: symbols whose cooldown expired within the last N hours (probation window).
    Closes the mechanical 'cooldown expired -> rebuy same name next sweep' churn loop
    that produced ~547 buy requests/day on the equity book."""
    probation = set()
    if os.path.exists(DB_PATH):
        try:
            from datetime import datetime, timedelta
            now = datetime.utcnow()
            now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            window_str = (now - timedelta(hours=window_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                rows = cursor.execute(
                    "SELECT DISTINCT symbol FROM trade_cooldowns WHERE cooldown_until <= ? AND cooldown_until >= ?",
                    (now_str, window_str)
                ).fetchall()
                probation = {r[0] for r in rows}
        except Exception as e:
            sys.stderr.write(f"[!] Probation lookup exception: {e}\n")
    return probation

def run_portfolio_guardrail():
    try:
        input_str = sys.stdin.read()
        if not input_str.strip():
            print(json.dumps({"error": "Empty input to global guardrail."}))
            return

        payload = json.loads(input_str)

        candidates = payload.get("candidates", [])
        available_cash = float(payload.get("available_cash", 0.0))
        total_equity = float(payload.get("total_equity", available_cash))
        existing_positions = payload.get("existing_positions", [])

        active_cooldowns = get_active_cooldowns()
        probation_symbols = get_recent_cooldown_expiries(72)

        # v1.1: hard cap on single-name risk. The brain's Kelly inputs are hardcoded
        # placeholder stats (win rates 0.55-0.56) with zero realized-trade evidence,
        # so full-Kelly 20% weights are not defensible. Cap at 12% (env-overridable)
        # until honest stats are computed from mcp_execution_log realized rounds.
        kelly_hard_cap = float(os.environ.get("KELLY_HARD_CAP", "0.12"))

        approved_trades = []
        trim_sell_orders = []

        kelly_whale_mult = float(os.environ.get("KELLY_WHALE_MULT", "1.25"))
        kelly_static_mult = float(os.environ.get("KELLY_STATIC_MULT", "1.00"))

        # 1. First Pass: Filter out active cooldowns, illegal regimes, failed ML, and current holdings
        for asset in candidates:
            symbol = asset.get("symbol")
            current_state = asset.get("current_state")
            ml_confirmed = asset.get("ml_confirmed", False)
            source = asset.get("source", "static")
            base_kelly = float(asset.get("calculated_kelly", 0.0))

            # Apply source-aware Kelly conviction multiplier
            multiplier = kelly_whale_mult if source != "static" else kelly_static_mult
            calculated_kelly = base_kelly * multiplier
            asset["calculated_kelly"] = round(calculated_kelly, 4)

            # Filter active cooldown lock (Post-Liquidation Risk Gate)
            if symbol in active_cooldowns:
                sys.stderr.write(f"[*] [{symbol}] Rejected: Asset in post-liquidation cooldown until {active_cooldowns[symbol]['cooldown_until']}\n")
                continue

            # v1.1 BEAR-FLIP LIQUIDATION: a HELD position whose regime flipped to Bear
            # gets a full liquidation sell order. Must run BEFORE the Bull-only filter
            # below (Bear candidates are dropped there). Previously the allocator only
            # ever emitted profit-taking TRIMs for holdings - combined with the
            # unregistered mcp_alpaca_close_position tool this left the book fully
            # long through regime breakdowns (zero SELL rows ever recorded).
            if symbol in existing_positions and current_state == "Bear":
                trim_sell_orders.append({
                    "symbol": symbol,
                    "action": "BEAR_REGIME_LIQUIDATION",
                    "reason": f"Regime flipped to Bear on {symbol}; liquidate full position"
                })
                sys.stderr.write(f"[!!!] [{symbol}] BEAR REGIME FLIP: emitting full liquidation sell order.\n")
                continue

            if not ml_confirmed or current_state != "Bull" or calculated_kelly < 0.05:
                continue

            # v1.1 PROBATION GATE: symbols that stopped out within the last 72h must
            # show double conviction (kelly >= 0.10) to re-enter. Blocks mechanical
            # rebuys of just-stopped names while still allowing genuine fresh momentum.
            if symbol in probation_symbols and calculated_kelly < 0.10:
                sys.stderr.write(f"[*] [{symbol}] Rejected: post-stopout 72h probation (kelly {calculated_kelly:.3f} < 0.10)\n")
                continue

            # Routine Profit-Taking / Weight Audit for existing holdings
            if symbol in existing_positions:
                target_allocation_fraction = min(calculated_kelly, kelly_hard_cap)
                target_usd = total_equity * target_allocation_fraction
                # Use existing_positions dict which maps symbol to market_value
                current_val = float(existing_positions[symbol]) if isinstance(existing_positions, dict) else target_usd

                # If position value has surged >15% over target allocation weight, trigger routine profit-taking trim
                if current_val > (target_usd * 1.15):
                    trim_usd = current_val - target_usd
                    trim_sell_orders.append({
                        "symbol": symbol,
                        "action": "TRIM_PROFIT_TAKING",
                        "trim_amount_usd": round(trim_usd, 2),
                        "target_size_usd": round(target_usd, 2),
                        "reason": "Routine profit-taking weight rebalance (no cooldown lock)"
                    })
                continue

            approved_trades.append(asset)

        if not approved_trades and not trim_sell_orders:
            print(json.dumps({
                "approved_trades": [],
                "sell_orders": [],
                "reason": "No candidates passed initial filtration or rebalancing criteria."
            }))
            return

        # 2. Capital Allocation Pass: Raw Kelly Sizing based strictly on Total Equity
        total_requested_fraction = 0.0
        for trade in approved_trades:
            # Enforce hard asset-level ceiling constraint (v1.1: capped at kelly_hard_cap)
            allocated_fraction = min(trade["calculated_kelly"], kelly_hard_cap)
            trade["allocated_fraction"] = allocated_fraction
            # v1.3.2: Alpaca notional orders reject values with more than 2 decimal
            # places (HTTP 42210000 "notional value must be limited to 2 decimal
            # places"). Round at the source so every downstream path (direct buys,
            # TRIM sells, recovery retries, mcp_requested_trades.amount_usd records)
            # receives a broker-clean number. The normalization pass below already
            # rounds, but the no-normalization branch (line 158 else) skipped it.
            trade["target_size_usd"] = round(total_equity * allocated_fraction, 2)
            total_requested_fraction += allocated_fraction

        # 3. Portfolio Normalization Pass (The Budget Constraint)
        max_deployable_fraction = (available_cash * 0.90) / total_equity if total_equity > 0 else 0

        if total_requested_fraction > max_deployable_fraction:
            normalization_factor = max_deployable_fraction / total_requested_fraction
            for trade in approved_trades:
                trade["allocated_fraction"] = round(trade["allocated_fraction"] * normalization_factor, 4)
                trade["target_size_usd"] = round(trade["target_size_usd"] * normalization_factor, 2)
                trade["normalization_applied"] = True
        else:
            for trade in approved_trades:
                trade["normalization_applied"] = False

        print(json.dumps({
            "status": "success",
            "available_cash_pool": available_cash,
            "approved_trades": approved_trades,
            "sell_orders": trim_sell_orders
        }))

    except Exception as e:
        print(json.dumps({"error": f"Global Guardrail processing exception: {str(e)}"}))

if __name__ == "__main__":
    run_portfolio_guardrail()
