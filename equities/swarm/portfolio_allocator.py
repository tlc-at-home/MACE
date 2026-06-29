#!/usr/bin/env python3.11
import sys
import json

def run_portfolio_guardrail():
    try:
        input_str = sys.stdin.read()
        if not input_str.strip():
            print(json.dumps({"error": "Empty input to global guardrail."}))
            return

        payload = json.loads(input_str)

        candidates = payload.get("candidates", [])
        available_cash = float(payload.get("available_cash", 0.0))
        total_equity = float(payload.get("total_equity", available_cash)) # FIX: Accept total portfolio value
        existing_positions = payload.get("existing_positions", [])

        approved_trades = []
        sell_orders = [] # FIX: Add sell array

        for asset in candidates:
            symbol = asset.get("symbol")
            current_state = asset.get("current_state")
            ml_confirmed = asset.get("ml_confirmed", False)
            calculated_kelly = asset.get("calculated_kelly", 0.0)

            # ==========================================
            # FIX: EXIT LOGIC (BEAR REGIME)
            # ==========================================
            if current_state == "Bear" and ml_confirmed and symbol in existing_positions:
                sell_orders.append({"symbol": symbol, "reason": "HMM confirmed Bear regime shift."})
                continue

            # ==========================================
            # ENTRY LOGIC (BULL REGIME)
            # ==========================================
            if not ml_confirmed or current_state != "Bull" or calculated_kelly < 0.05:
                continue

            if symbol in existing_positions:
                continue

            # FIX: Size against TOTAL EQUITY, not just cash, to maintain true 20% cap
            max_allowed_usd = total_equity * 0.20
            desired_usd = available_cash * calculated_kelly

            # Take the lesser of Kelly desire, max exposure limit, or actual cash
            target_size_usd = min(desired_usd, max_allowed_usd, available_cash)

            if target_size_usd >= 25.0: # Alpaca minimum
                approved_trades.append({
                    "symbol": symbol,
                    "calculated_kelly": calculated_kelly,
                    "signal_strength": asset.get("signal_strength", 0.0),
                    "target_size_usd": round(target_size_usd, 2)
                })

        # Output pristine operational matrix back to the orchestrator
        print(json.dumps({
            "status": "success",
            "available_cash_pool": available_cash,
            "approved_trades": approved_trades,
            "sell_orders": sell_orders # PASS SELLS BACK TO ORCHESTRATOR
        }))

    except Exception as e:
        print(json.dumps({"error": f"Global Guardrail processing exception: {str(e)}"}))

if __name__ == "__main__":
    run_portfolio_guardrail()
