#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Multi-Agent Swarm Pipeline
Component: The Math/Quant Agent (brain.py)

Role: Pure mathematical sandbox. Accepts serialized JSON market data via stdin,
      runs a 3-State Gaussian Hidden Markov Model (HMM) to classify market regimes,
      computes systemic signal strength, and applies Dynamic Half-Kelly sizing math.
Output: Emissions print cleanly as a single-line JSON string to stdout.
Safety: Internalizes exceptions to output structured JSON error messages down-pipe.
"""

import sys
import json
import numpy as np
import pandas as pd
from hmmlearn.hmm import GaussianHMM

def compute_quantitative_signals(raw_input):
    """
    Parses incoming payload, executes the Hidden Markov Model matrix math,
    and returns localized alpha parameters and sizing coefficients.
    """
    payload = json.loads(raw_input)

    if payload.get("status") == "error":
        return {"status": "error", "message": f"Scout error passed to Brain: {payload.get('message')}"}

    ticker = payload.get("ticker", "UNKNOWN/USDT")
    df_json_str = payload.get("data")

    if not df_json_str:
        return {"status": "error", "message": "Brain received empty or missing data block payload."}

    df = pd.read_json(df_json_str)

    # INCREASED: HMMs need more data to stabilize EM convergence
    if len(df) < 200:
        return {
            "status": "insufficient_data",
            "ticker": ticker,
            "regime": "Unknown",
            "kelly_fraction": 0.0,
            "signal_strength": 0.0
        }

    # 3. Calculate 2D Feature Matrices (Returns + Volatility)
    df['returns'] = df['close'].pct_change()
    df['volatility'] = df['returns'].rolling(window=14).std()
    df.dropna(inplace=True)

    # Reshape for 2D features [returns, volatility]
    X = df[['returns', 'volatility']].values

    # 4. Instantiate and fit the 3-State Gaussian HMM
    model = GaussianHMM(
        n_components=3,
        covariance_type="diag", # Stabilizes matrix math on limited data
        n_iter=200,
        random_state=42
    )
    model.fit(X)

    hidden_states = model.predict(X)
    current_state = hidden_states[-1]

    # Extract structural means matrix based on the RETURNS column (index 0 of 2D means)
    state_means = model.means_[:, 0]
    sorted_state_indices = np.argsort(state_means)

    bear_state_idx = sorted_state_indices[0]
    neutral_state_idx = sorted_state_indices[1]
    bull_state_idx = sorted_state_indices[2]

    # 5. Base Kelly Parameters (Theoretical maximum boundaries)
    if current_state == bull_state_idx:
        regime = "Bull"
        base_win_rate = 0.56
        base_win_loss_ratio = 1.35
    elif current_state == bear_state_idx:
        regime = "Bear"
        base_win_rate = 0.38
        base_win_loss_ratio = 0.85
    else:
        regime = "Neutral"
        base_win_rate = 0.48
        base_win_loss_ratio = 1.00

    # 6. Quant Fix: Dynamic Fractional Kelly Sizing
    loss_rate = 1.0 - base_win_rate
    theoretical_kelly = base_win_rate - (loss_rate / base_win_loss_ratio)

    # Calculate signal strength (Sharpe Proxy)
    rolling_window = 20
    recent_returns = df['returns'].iloc[-rolling_window:]
    returns_std = recent_returns.std()

    signal_strength = 0.0
    if returns_std > 0:
        signal_strength = float(recent_returns.mean() / returns_std)

    # DYNAMIC SIZING: Scale Kelly by Signal Strength Confidence
    confidence_multiplier = np.clip(signal_strength, 0.2, 1.0)

    # Apply Half-Kelly (divide by 2) to protect the fund, then scale by confidence
    dynamic_kelly = (theoretical_kelly / 2.0) * confidence_multiplier

    # Bound the final output
    sanitized_kelly = max(0.0, min(float(dynamic_kelly), 1.0))

    return {
        "status": "success",
        "ticker": ticker,
        "current_price": float(df['close'].iloc[-1]), # Added for downstream execution
        "regime": regime,
        "kelly_fraction": round(sanitized_kelly, 4),
        "signal_strength": round(signal_strength, 4)
    }

if __name__ == "__main__":
    try:
        input_stream_data = sys.stdin.read()

        if not input_stream_data.strip():
            print(json.dumps({"status": "error", "message": "Brain stdin received blank data stream."}))
            sys.exit(1)

        signal_payload = compute_quantitative_signals(input_stream_data)
        print(json.dumps(signal_payload))
        sys.exit(0)

    except Exception as e:
        print(json.dumps({"status": "error", "message": f"Brain processing core faulted: {str(e)}"}))
        sys.exit(1)
