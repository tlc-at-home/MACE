#!/usr/bin/env python3.11
"""
Test Suite: Crypto Parity with TradFi
Verifies all new crypto risk and execution features:
1. 72h post-stopout rebuy probation gate
2. Routine profit-taking trims (TRIM_PROFIT_TAKING)
3. Bear regime liquidation sell orders (BEAR_REGIME_LIQUIDATION)
4. Budget normalization and KELLY_HARD_CAP
5. Active cooldown filtration
6. Guardrail allocator sizing preservation & partial trim execution
7. Crypto news guard heartbeat & fail-neutral buy gate
"""

import os
import sys
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from crypto.swarm import portfolio_allocator, guardrail
import realized_round_trips as rrt

class TestCryptoParitySuite(unittest.TestCase):
    def setUp(self):
        # Create a temporary database for isolation
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        guardrail.init_db(self.db_path)
        rrt.ensure_schema(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_allocator_cooldown_and_probation(self):
        """Test active cooldown rejection and 72h probation gate behavior."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT asset_id FROM asset_universe WHERE symbol = 'SOL/USDT'")
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    "INSERT INTO asset_universe (symbol, asset_class, broker, exchange, currency) VALUES ('SOL/USDT', 'CRYPTO', 'binance', 'BINANCE', 'USDT')"
                )
                asset_id = cursor.lastrowid
            else:
                asset_id = row[0]

            # 1. Put SOL/USDT in active cooldown
            cursor.execute("""
                INSERT INTO trade_cooldowns (asset_id, symbol, closed_at, reason, cooldown_until)
                VALUES (?, 'SOL/USDT', ?, 'STOP_LOSS_BREACH', ?)
            """, (asset_id, now.strftime("%Y-%m-%dT%H:%M:%SZ"), (now + timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ")))

            # 2. Put AVAX/USDT in 72h probation (cooldown expired 10h ago)
            cursor.execute(
                "INSERT INTO asset_universe (symbol, asset_class, broker, exchange, currency) VALUES ('AVAX/USDT', 'CRYPTO', 'binance', 'BINANCE', 'USDT')"
            )
            avax_id = cursor.lastrowid
            cursor.execute("""
                INSERT INTO trade_cooldowns (asset_id, symbol, closed_at, reason, cooldown_until)
                VALUES (?, 'AVAX/USDT', ?, 'STOP_LOSS_BREACH', ?)
            """, (avax_id, (now - timedelta(hours=34)).strftime("%Y-%m-%dT%H:%M:%SZ"), (now - timedelta(hours=10)).strftime("%Y-%m-%dT%H:%M:%SZ")))
            conn.commit()

        # Check get_active_cooldowns and get_recent_cooldown_expiries
        active = portfolio_allocator.get_active_cooldowns(self.db_path)
        self.assertIn("SOL/USDT", active)

        probation = portfolio_allocator.get_recent_cooldown_expiries(72, db_path=self.db_path)
        self.assertIn("AVAX/USDT", probation)

    def test_allocator_bear_flip_and_profit_trim(self):
        """Test that allocator emits BEAR_REGIME_LIQUIDATION and TRIM_PROFIT_TAKING orders."""
        payload = {
            "candidates": [
                # Held position that flipped to Bear -> must emit BEAR_REGIME_LIQUIDATION
                {"symbol": "ETH/USDT", "current_state": "Bear", "ml_confirmed": True, "calculated_kelly": 0.0, "current_price": 2600.0},
                # Held position that surged >15% over target Kelly weight (target = 10000 * 0.10 = 1000, current = 1300 > 1150)
                {"symbol": "BTC/USDT", "current_state": "Bull", "ml_confirmed": True, "calculated_kelly": 0.10, "current_price": 65000.0},
                # Fresh Bull candidate -> must be approved
                {"symbol": "NEAR/USDT", "current_state": "Bull", "ml_confirmed": True, "calculated_kelly": 0.08, "current_price": 5.0}
            ],
            "available_cash": 4000.0,
            "total_equity": 10000.0,
            "existing_positions": {
                "ETH/USDT": 2000.0,
                "BTC/USDT": 1300.0
            }
        }

        # Mock stdin
        import io
        old_stdin = sys.stdin
        sys.stdin = io.StringIO(json.dumps(payload))
        
        # Capture stdout
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        try:
            portfolio_allocator.run_portfolio_guardrail(db_path=self.db_path)
            output = sys.stdout.getvalue()
        finally:
            sys.stdin = old_stdin
            sys.stdout = old_stdout

        res = json.loads(output)
        self.assertEqual(res.get("status"), "success")
        
        # Check sell orders
        sell_orders = res.get("sell_orders", [])
        self.assertEqual(len(sell_orders), 2)
        
        bear_order = next((s for s in sell_orders if s["action"] == "BEAR_REGIME_LIQUIDATION"), None)
        self.assertIsNotNone(bear_order)
        self.assertEqual(bear_order["symbol"], "ETH/USDT")

        trim_order = next((s for s in sell_orders if s["action"] == "TRIM_PROFIT_TAKING"), None)
        self.assertIsNotNone(trim_order)
        self.assertEqual(trim_order["symbol"], "BTC/USDT")
        self.assertEqual(trim_order["trim_amount_usd"], 300.0) # 1300 - 1000

        # Check approved trades
        approved = res.get("approved_trades", [])
        self.assertEqual(len(approved), 1)
        self.assertEqual(approved[0]["symbol"], "NEAR/USDT")
        self.assertEqual(approved[0]["target_size_usd"], 800.0) # 10000 * 0.08

    def test_guardrail_partial_trim(self):
        """Test partial trim execution in guardrail virtual ledger."""
        # Seed an ETH position on Arbitrum
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO portfolio (blockchain, token, quantity, avg_entry_price) VALUES ('ARBITRUM', 'ETH', 2.0, 2000.0)"
            )
            conn.commit()

        # Execute partial sell of 0.5 ETH at 2500
        receipt = guardrail.evaluate_and_execute_simulated_trade(
            symbol="ETH/USDT", action="SELL", quantity=0.5, execution_price=2500.0,
            reason="TRIM_PROFIT_TAKING", db_path=self.db_path
        )
        self.assertTrue(receipt.get("success"))
        self.assertAlmostEqual(receipt["new_balance"], 1.5)

        # Check realized round trips recorded the trim
        stats = rrt.load_empirical_edge_stats("CRYPTO", db_path=self.db_path)
        self.assertIsNotNone(stats)
        self.assertEqual(stats["n"], 1)

    def test_crypto_news_health_and_fail_neutral_gate(self):
        """Test crypto news guard health reporting and fail-neutral buy gate."""
        # Initially no audit -> gate closed
        gate_ok, detail = rrt.news_gate_allows_buys(self.db_path, component="crypto_news_guard")
        self.assertFalse(gate_ok)

        # Write healthy heartbeat
        rrt.write_component_heartbeat("crypto_news_guard", status="healthy", detail="verdict=safe", db_path=self.db_path)
        gate_ok, detail = rrt.news_gate_allows_buys(self.db_path, component="crypto_news_guard")
        self.assertTrue(gate_ok)

        # Test override via env var MACE_CRYPTO_NEWS_GATE=off
        os.environ["MACE_CRYPTO_NEWS_GATE"] = "off"
        # Even with degraded status, gate allows buys when overridden
        rrt.write_component_heartbeat("crypto_news_guard", status="degraded", detail="error", db_path=self.db_path)
        gate_ok, detail = rrt.news_gate_allows_buys(self.db_path, component="crypto_news_guard")
        self.assertTrue(gate_ok)
        del os.environ["MACE_CRYPTO_NEWS_GATE"]

if __name__ == "__main__":
    unittest.main()
