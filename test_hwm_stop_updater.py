#!/usr/bin/env python3.11
"""
Verification Script for M.A.C.E. 24-Hour Rolling Volatility Stop & HWM State Engine
"""

import os
import sys
import sqlite3
import numpy as np

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from hwm_stop_updater import (
    calculate_24h_rolling_volatility_stop,
    get_db_connection,
    get_or_create_asset_id,
    update_db_hwm
)

TEST_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

def test_volatility_calculation():
    print("[*] TEST 1: Volatility Stop Calculation Logic...")
    
    # 1. Edge Case: Insufficient bars (<120)
    few_bars = [{"c": 100.0 + i} for i in range(50)]
    vol_few = calculate_24h_rolling_volatility_stop(few_bars)
    assert vol_few == 0.040, f"Expected fallback 0.040, got {vol_few}"
    print("    [✓] Fallback for <120 bars passed (0.040).")

    # 2. Synthetic Low Volatility Series
    np.random.seed(42)
    low_vol_prices = 100.0 + np.cumsum(np.random.normal(0, 0.05, 1440))
    low_vol_bars = [{"c": float(p)} for p in low_vol_prices]
    vol_low = calculate_24h_rolling_volatility_stop(low_vol_bars, multiplier=2.5, min_bound=0.030, max_bound=0.080)
    assert 0.030 <= vol_low <= 0.080, f"Expected between 0.03 and 0.08, got {vol_low}"
    print(f"    [✓] Low volatility series output: {vol_low*100:.2f}% (bounded correctly).")

    # 3. Synthetic Extreme High Volatility Series (should clamp to max_bound 0.080)
    high_vol_prices = 100.0 * np.exp(np.cumsum(np.random.normal(0, 0.05, 1440)))
    high_vol_bars = [{"c": float(p)} for p in high_vol_prices]
    vol_high = calculate_24h_rolling_volatility_stop(high_vol_bars, multiplier=2.5, min_bound=0.030, max_bound=0.080)
    assert vol_high == 0.080, f"Expected upper bound clamp 0.080, got {vol_high}"
    print(f"    [✓] High volatility upper clamp passed: {vol_high*100:.2f}%.")

def test_database_hwm_upsert_and_views():
    print("[*] TEST 2: HWM Ratchet & View Queries...")

    with get_db_connection() as conn:
        asset_id = get_or_create_asset_id(conn, "TEST_TICKER", asset_class="TRADFI", broker="alpaca")
        assert asset_id is not None, "Failed to resolve asset_id for TEST_TICKER"
        print(f"    [✓] Resolved asset_id: {asset_id} for TEST_TICKER")

        # Initial HWM insertion: Peak $100.0, Loss limit 5%
        update_db_hwm("equities_hwm", asset_id, "TEST_TICKER", 100.0, 0.050)
        
        # Verify via view
        cursor = conn.cursor()
        cursor.execute("SELECT high_water_mark, loss_limit, stop_floor_price FROM vw_equities_risk_corridors WHERE symbol = 'TEST_TICKER'")
        row = cursor.fetchone()
        assert row is not None, "View vw_equities_risk_corridors did not return TEST_TICKER"
        hwm, loss_lim, floor = float(row[0]), float(row[1]), float(row[2])
        assert hwm == 100.0, f"Expected HWM 100.0, got {hwm}"
        assert floor == 95.0, f"Expected floor price 95.0, got {floor}"
        print(f"    [✓] Initial view record: HWM=${hwm:.2f}, LossLimit={loss_lim*100:.1f}%, Floor=${floor:.2f}")

        # Attempt updating with LOWER price ($90.0) -> HWM must NOT ratchet down
        update_db_hwm("equities_hwm", asset_id, "TEST_TICKER", 90.0, 0.040)
        cursor.execute("SELECT high_water_mark, loss_limit, stop_floor_price FROM vw_equities_risk_corridors WHERE symbol = 'TEST_TICKER'")
        row = cursor.fetchone()
        assert float(row[0]) == 100.0, f"HWM improperly ratcheted down to {row[0]}"
        assert float(row[1]) == 0.040, f"Loss limit failed to update to 0.040"
        print("    [✓] HWM floor ratchet protection verified (did not decrease on lower price).")

        # Update with HIGHER price ($120.0) -> HWM must ratchet UP to $120.0
        update_db_hwm("equities_hwm", asset_id, "TEST_TICKER", 120.0, 0.050)
        cursor.execute("SELECT high_water_mark, loss_limit, stop_floor_price FROM vw_equities_risk_corridors WHERE symbol = 'TEST_TICKER'")
        row = cursor.fetchone()
        assert float(row[0]) == 120.0, f"HWM failed to ratchet up to 120.0, got {row[0]}"
        assert float(row[2]) == 114.0, f"Expected floor price 114.0, got {row[2]}"
        print(f"    [✓] HWM ratchet UP verified: New HWM=${row[0]:.2f}, Floor=${row[2]:.2f}")

        # Cleanup test entry
        cursor.execute("DELETE FROM equities_hwm WHERE asset_id = ?", (asset_id,))
        cursor.execute("DELETE FROM asset_universe WHERE symbol = 'TEST_TICKER'")
        conn.commit()

if __name__ == "__main__":
    print("==================================================================")
    print("   M.A.C.E. 24-Hour Rolling Volatility Stop & HWM Test Suite     ")
    print("==================================================================")
    test_volatility_calculation()
    test_database_hwm_upsert_and_views()
    print("==================================================================")
    print("   [+] ALL TESTS PASSED SUCCESSFULLY!                             ")
    print("==================================================================")
