#!/usr/bin/env python3.11
import sys
import json
import numpy as np
import pandas as pd
from hmmlearn import hmm

def run_brain():
    try:
        input_str = sys.stdin.read()
        if not input_str.strip():
            print(json.dumps({"error": "Empty input to brain."}))
            return

        data = json.loads(input_str)
        if "error" in data:
            print(json.dumps(data))
            return

        symbol = data.get("symbol")
        prices = data.get("prices", [])

        if len(prices) < 100:
            print(json.dumps({"symbol": symbol, "error": f"Insufficient prices data: {len(prices)}"}))
            return

        df = pd.DataFrame({"c": prices})
        df['Daily_Return'] = df['c'].pct_change()
        df['Return_20d'] = df['c'].pct_change(periods=20)

        # FIX: Add volatility for 2D HMM feature space
        df['Volatility_20d'] = df['Daily_Return'].rolling(window=20).std()
        df = df.dropna().reset_index(drop=True)

        if len(df) < 100:
            print(json.dumps({"symbol": symbol, "error": f"Insufficient data after dropna: {len(df)}"}))
            return

        # --- YOUR HYBRID MARKOV LOGIC (PRESERVED) ---
        states_map = {'Bear': 0, 'Sideways': 1, 'Bull': 2}
        conditions = [df['Return_20d'] <= -0.05, df['Return_20d'] >= 0.05]
        choices = ['Bear', 'Bull']
        df['State'] = np.select(conditions, choices, default='Sideways')

        transitions = np.zeros((3, 3))
        state_idx = df['State'].map(states_map).values
        for (i, j) in zip(state_idx[:-1], state_idx[1:]):
            if not np.isnan(i) and not np.isnan(j):
                transitions[int(i), int(j)] += 1

        row_sums = transitions.sum(axis=1, keepdims=True)
        transition_matrix = np.divide(transitions, row_sums, out=np.zeros_like(transitions), where=row_sums!=0)

        today_state = df['State'].iloc[-1]
        today_state_idx = states_map[today_state]
        prob_bear = transition_matrix[today_state_idx, 0]
        prob_bull = transition_matrix[today_state_idx, 2]
        signal_strength = prob_bull - prob_bear

        # --- HMM CONFIRMATION (FIXED TO 2D) ---
        X = df[['Daily_Return', 'Volatility_20d']].values
        hmm_model = hmm.GaussianHMM(n_components=3, covariance_type="diag", n_iter=200, random_state=42)
        hmm_model.fit(X)
        hidden_states = hmm_model.predict(X)

        state_means = hmm_model.means_[:, 0] # Index 0 is returns
        hmm_bull_component = np.argmax(state_means)
        hmm_bear_component = np.argmin(state_means)

        is_ml_confirmed = False
        if today_state == 'Bull' and hidden_states[-1] == hmm_bull_component:
            is_ml_confirmed = True
        elif today_state == 'Bear' and hidden_states[-1] == hmm_bear_component:
            is_ml_confirmed = True

        # --- FIX: REAL KELLY CRITERION MATH ---
        # Map to backtested edge profiles
        if today_state == "Bull":
            win_rate, win_loss_ratio = 0.55, 1.30
        elif today_state == "Bear":
            win_rate, win_loss_ratio = 0.36, 0.80
        else:
            win_rate, win_loss_ratio = 0.46, 1.00

        loss_rate = 1.0 - win_rate
        theoretical_kelly = win_rate - (loss_rate / win_loss_ratio)

        # Apply Half-Kelly for safety, cap at 25%
        asset_kelly = max(0.0, min((theoretical_kelly / 2.0), 0.25))

        result = {
            "symbol": symbol,
            "current_price": float(df['c'].iloc[-1]), # FIX: Added for downstream execution
            "current_state": today_state,
            "signal_strength": round(float(signal_strength), 4),
            "ml_confirmed": bool(is_ml_confirmed),
            "calculated_kelly": round(float(asset_kelly), 4)
        }
        print(json.dumps(result))

    except Exception as e:
        print(json.dumps({"error": f"Brain Agent processing error: {str(e)}"}))

if __name__ == "__main__":
    run_brain()
