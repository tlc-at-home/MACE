#!/usr/bin/env python3.11
import sys
import json

def run_portfolio_guardrail():
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

        # Extract context variables passed from the orchestrator
        candidates = payload.get("candidates", [])
        available_cash = float(payload.get("available_cash", 0.0))
        existing_positions = payload.get("existing_positions", [])
        
        approved_trades = []
        
        # 1. First Pass: Filter out illegal regimes, failed ML, and current holdings
        for asset in candidates:
            symbol = asset.get("symbol")
            current_state = asset.get("current_state")
            ml_confirmed = asset.get("ml_confirmed", False)
            calculated_kelly = asset.get("calculated_kelly", 0.0)
            
            if not ml_confirmed or current_state != "Bull" or calculated_kelly < 0.05:
                continue
                
            if symbol in existing_positions:
                continue # Escape hatch triggered: Asset already owned
                
            approved_trades.append(asset)

        if not approved_trades:
            print(json.dumps({"approved_trades": [], "reason": "No candidates passed initial filtration filters."}))
            return

        # 2. Capital Allocation Pass: Raw Kelly Sizing based strictly on Liquid Cash
        total_requested_kelly_fraction = 0.0
        for trade in approved_trades:
            # Enforce hard asset-level ceiling constraint (Max 25% allocation per single trade)
            allocated_fraction = min(trade["calculated_kelly"], 0.25)
            trade["allocated_fraction"] = allocated_fraction
            trade["target_size_usd"] = available_cash * allocated_fraction
            total_requested_kelly_fraction += allocated_fraction

        # 3. Portfolio Normalization Pass (The Budget Constraint)
        # If collective allocations exceed 100% of available cash, scale down proportionally
        if total_requested_kelly_fraction > 1.0:
            normalization_factor = 1.0 / total_requested_kelly_fraction
            for trade in approved_trades:
                trade["allocated_fraction"] = round(trade["allocated_fraction"] * normalization_factor, 4)
                trade["target_size_usd"] = round(trade["target_size_usd"] * normalization_factor, 2)
                trade["normalization_applied"] = True
        else:
            for trade in approved_trades:
                trade["normalization_applied"] = False

        # Output pristine operational matrix back to the orchestrator
        print(json.dumps({
            "status": "success",
            "available_cash_pool": available_cash,
            "approved_trades": approved_trades
        }))

    except Exception as e:
        print(json.dumps({"error": f"Global Guardrail processing exception: {str(e)}"}))

if __name__ == "__main__":
    run_portfolio_guardrail()
