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

            if not ml_confirmed or current_state != "Bull" or calculated_kelly < 0.05:
                continue

            # Routine Profit-Taking / Weight Audit for existing holdings
            if symbol in existing_positions:
                target_allocation_fraction = min(calculated_kelly, 0.20)
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
            # Enforce hard asset-level ceiling constraint (Max 20% allocation per single trade)
            allocated_fraction = min(trade["calculated_kelly"], 0.20)
            trade["allocated_fraction"] = allocated_fraction
            trade["target_size_usd"] = total_equity * allocated_fraction
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
