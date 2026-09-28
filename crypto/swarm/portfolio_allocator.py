#!/usr/bin/env python3.11
import os
import sys
import json
import sqlite3

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

def get_active_cooldowns(db_path=DB_PATH):
    cooldowns = {}
    if os.path.exists(db_path):
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                rows = cursor.execute("SELECT symbol, cooldown_until, reason FROM vw_active_cooldowns").fetchall()
                for r in rows:
                    cooldowns[r[0]] = {"cooldown_until": r[1], "reason": r[2]}
        except Exception as e:
            sys.stderr.write(f"[!] Cooldown lookup exception: {e}\n")
    return cooldowns

def get_recent_cooldown_expiries(window_hours=72, db_path=DB_PATH):
    """Symbols whose cooldown expired within the last N hours (probation window).
    Closes the mechanical 'cooldown expired -> rebuy same name next sweep' churn loop
    that produced whipsaws (e.g. BONK regime sell/rebuy churn)."""
    probation = set()
    if os.path.exists(db_path):
        try:
            from datetime import datetime, timedelta, timezone
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            window_str = (now - timedelta(hours=window_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                rows = cursor.execute(
                    "SELECT DISTINCT symbol FROM trade_cooldowns WHERE cooldown_until <= ? AND cooldown_until >= ?",
                    (now_str, window_str)
                ).fetchall()
                probation = {r[0] for r in rows}
        except Exception as e:
            sys.stderr.write(f"[!] Probation lookup exception: {e}\n")
    return probation

def run_portfolio_guardrail(db_path=DB_PATH):
    try:
        input_str = sys.stdin.read()
        if not input_str.strip():
            print(json.dumps({"error": "Empty input to global guardrail."}))
            return

        try:
            payload = json.loads(input_str)
        except Exception as e:
            print(json.dumps({"error": f"Guardrail failed to parse input JSON: {str(e)}"}))
            return

        candidates = payload.get("candidates", [])
        available_cash = float(payload.get("available_cash", 0.0))
        total_equity = float(payload.get("total_equity", available_cash))
        existing_positions = payload.get("existing_positions", {})

        active_cooldowns = get_active_cooldowns(db_path=db_path)
        probation_symbols = get_recent_cooldown_expiries(72, db_path=db_path)

        # Single-name risk cap. Env-overridable, default 12% to align with TradFi risk governance
        kelly_hard_cap = float(os.environ.get("KELLY_HARD_CAP", "0.12"))

        approved_trades = []
        sell_orders = []

        # 1. First Pass: Filter out active cooldowns, illegal regimes, failed ML, and current holdings
        for asset in candidates:
            symbol = asset.get("symbol")
            current_state = asset.get("current_state")
            ml_confirmed = asset.get("ml_confirmed", True)  # Crypto uses confidence multiplier
            calculated_kelly = float(asset.get("calculated_kelly", 0.0))

            # Filter active cooldown lock (Post-Liquidation Risk Gate)
            if symbol in active_cooldowns:
                sys.stderr.write(f"[*] [{symbol}] Rejected: Asset in active cooldown until {active_cooldowns[symbol]['cooldown_until']}\n")
                continue

            # BEAR-FLIP LIQUIDATION: a HELD position whose regime flipped to Bear
            # gets a full liquidation sell order. Must run BEFORE the Bull-only filter.
            if symbol in existing_positions and current_state == "Bear":
                sell_orders.append({
                    "symbol": symbol,
                    "action": "BEAR_REGIME_LIQUIDATION",
                    "reason": f"Regime flipped to Bear on {symbol}; liquidate full position"
                })
                sys.stderr.write(f"[!!!] [{symbol}] BEAR REGIME FLIP: emitting full liquidation sell order.\n")
                continue

            if not ml_confirmed or current_state != "Bull" or calculated_kelly < 0.05:
                continue

            # 72H PROBATION GATE: symbols that stopped out within the last 72h must
            # show double conviction (kelly >= 0.10) to re-enter.
            if symbol in probation_symbols and calculated_kelly < 0.10:
                sys.stderr.write(f"[*] [{symbol}] Rejected: post-stopout 72h probation (kelly {calculated_kelly:.3f} < 0.10)\n")
                continue

            # Routine Profit-Taking / Weight Audit for existing holdings
            if symbol in existing_positions:
                target_allocation_fraction = min(calculated_kelly, kelly_hard_cap)
                target_usd = total_equity * target_allocation_fraction
                current_val = float(existing_positions[symbol]) if isinstance(existing_positions, dict) else target_usd

                # If position value has surged >15% over target allocation weight, trigger routine profit-taking trim
                if current_val > (target_usd * 1.15):
                    trim_usd = current_val - target_usd
                    sell_orders.append({
                        "symbol": symbol,
                        "action": "TRIM_PROFIT_TAKING",
                        "trim_amount_usd": round(trim_usd, 2),
                        "target_size_usd": round(target_usd, 2),
                        "reason": "Routine profit-taking weight rebalance (no cooldown lock)"
                    })
                continue

            approved_trades.append(asset)

        if not approved_trades and not sell_orders:
            print(json.dumps({
                "approved_trades": [],
                "sell_orders": [],
                "reason": "No candidates passed initial filtration or rebalancing criteria."
            }))
            return

        # 2. Capital Allocation Pass: Kelly Sizing based strictly on Total Equity
        total_requested_fraction = 0.0
        for trade in approved_trades:
            allocated_fraction = min(trade["calculated_kelly"], kelly_hard_cap)
            trade["allocated_fraction"] = allocated_fraction
            trade["target_size_usd"] = round(total_equity * allocated_fraction, 2)
            total_requested_fraction += allocated_fraction

        # 3. Portfolio Normalization Pass (The Budget Constraint)
        max_deployable_fraction = (available_cash * 0.90) / total_equity if total_equity > 0 else 0

        if total_requested_fraction > max_deployable_fraction and total_requested_fraction > 0:
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
            "total_equity_pool": total_equity,
            "approved_trades": approved_trades,
            "sell_orders": sell_orders
        }))

    except Exception as e:
        print(json.dumps({"error": f"Global Guardrail processing exception: {str(e)}"}))

if __name__ == "__main__":
    run_portfolio_guardrail()
