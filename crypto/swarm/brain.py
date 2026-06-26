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

        try:
            data = json.loads(input_str)
        except Exception as e:
            print(json.dumps({"error": f"Brain failed to parse input JSON: {str(e)}"}))
            return

        if "error" in data:
            print(json.dumps(data))
            return

        symbol = data.get("symbol")
        prices = data.get("prices", [])

        if len(prices) < 50:
            print(json.dumps({"symbol": symbol, "error": f"Insufficient prices data: {len(prices)}"}))
            return

        df = pd.DataFrame({"c": prices})
        df['Daily_Return'] = df['c'].pct_change()
        df['Return_20d'] = df['c'].pct_change(periods=20)
        df = df.dropna().reset_index(drop=True)

        if len(df) < 30:
            print(json.dumps({"symbol": symbol, "error": f"Insufficient returns data after dropna: {len(df)}"}))
            return

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

        # HMM Confirmation
        returns_array = df['Daily_Return'].values.reshape(-1, 1)
        hmm_model = hmm.GaussianHMM(n_components=3, covariance_type="diag", n_iter=1000, tol=0.01, min_covar=1e-3, random_state=42)
        hmm_model.fit(returns_array)
        hidden_states = hmm_model.predict(returns_array)

        state_means = hmm_model.means_.flatten()
        hmm_bull_component = np.argmax(state_means)
        hmm_bear_component = np.argmin(state_means)

        is_ml_confirmed = False
        if today_state == 'Bull' and hidden_states[-1] == hmm_bull_component:
            is_ml_confirmed = True
        elif today_state == 'Bear' and hidden_states[-1] == hmm_bear_component:
            is_ml_confirmed = True

        # Kelly Fraction & Conviction Floor
        recent_variance = np.var(df['Daily_Return'].tail(90).values)
        recent_mean = np.mean(df['Daily_Return'].tail(90).values)

        asset_kelly = 0.0
        if recent_variance > 0:
            raw_kelly = recent_mean / recent_variance
            asset_kelly = min(0.25, abs(raw_kelly * 0.25))

        result = {
            "symbol": symbol,
            "current_state": today_state,
            "signal_strength": round(float(signal_strength), 4),
            "ml_confirmed": bool(is_ml_confirmed),
            "calculated_kelly": round(float(asset_kelly), 4),
            "latest_price": round(float(prices[-1]), 4)
        }
        print(json.dumps(result))

    except Exception as e:
        print(json.dumps({"error": f"Brain Agent processing error: {str(e)}"}))

if __name__ == "__main__":
    run_brain()
