#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Multi-Agent Swarm Pipeline
Component: The Swarm Orchestrator (orchestrator.py)
"""

import sys
import os
import json
import asyncio
import argparse
import logging
import sqlite3
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt_client

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

sys.path.append(os.path.join(BASE_DIR, "crypto/swarm"))
import guardrail

MQTT_BROKER_IP = os.getenv("MQTT_BROKER_IP", "192.168.0.110")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "mace/telemetry/crypto_sword"

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.orchestrator")

def load_universe():
    """Reads and parses the broad-market token list from SQLite database."""
    if not os.path.exists(DEFAULT_DB_PATH):
        logger.warning(f"Database missing at {DEFAULT_DB_PATH}. Deploying default core triage targets.")
        return ["BTC/USDT", "SOL/USDT", "ETH/USDT", "NEAR/USDT", "AVAX/USDT"]
    try:
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            cursor = conn.cursor()
            rows = cursor.execute("SELECT pair FROM vw_crypto_universe ORDER BY symbol").fetchall()
            if rows:
                return [r[0] for r in rows]
    except Exception as e:
        logger.error(f"[-] Critical exception querying vw_crypto_universe table: {e}")

    return ["BTC/USDT", "SOL/USDT", "ETH/USDT", "NEAR/USDT", "AVAX/USDT"]

def push_mqtt_telemetry(payload):
    try:
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        user = os.environ.get("MQTT_USER")
        password = os.environ.get("MQTT_PASSWORD")
        if user and password:
            client.username_pw_set(user, password)
        client.connect(MQTT_BROKER_IP, MQTT_PORT, 60)
        client.loop_start()
        info = client.publish(MQTT_TOPIC, json.dumps(payload), retain=True)
        info.wait_for_publish(timeout=5)
        client.loop_stop()
        client.disconnect()
    except Exception as e:
        logger.error(f"[!] Asynchronous telemetry network link bottleneck: {e}")

def get_seconds_until_next_4h_offset():
    """
    Calculates exact seconds to sleep until the next 4-hour UTC boundary + 5 minutes (e.g., 00:05, 04:05, 08:05).
    """
    now = datetime.utcnow()

    # Calculate which 4-hour block we are in (0, 4, 8, 12, 16, 20)
    current_block = (now.hour // 4) * 4
    next_block = current_block + 4

    # Target time is next_block:05:00 UTC
    target_time = now.replace(hour=next_block if next_block < 24 else 0, minute=5, second=0, microsecond=0)

    # If the target time is in the past, it means we missed the 05-minute window for the current block
    # Wait until the NEXT 4-hour block.
    if target_time <= now:
        target_time += timedelta(days=1)

    # Calculate the delta
    delta = target_time - now
    return int(delta.total_seconds())

async def process_single_asset_pipeline(symbol, semaphore):
    async with semaphore:
        scout_path = os.path.join(BASE_DIR, "crypto/swarm/scout.py")
        brain_path = os.path.join(BASE_DIR, "crypto/swarm/brain.py")
        try:
            scout_proc = await asyncio.create_subprocess_exec(sys.executable, scout_path, symbol, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            scout_stdout, scout_stderr = await scout_proc.communicate()
            if scout_proc.returncode != 0:
                logger.error(f"[-] Data Scout failed for {symbol}: {scout_stderr.decode().strip()}")
                return None
            brain_proc = await asyncio.create_subprocess_exec(sys.executable, brain_path, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            brain_stdout, brain_stderr = await brain_proc.communicate(input=scout_stdout)
            if brain_proc.returncode != 0:
                logger.error(f"[-] Quant Brain math core faulted for {symbol}: {brain_stderr.decode().strip()}")
                return None
            return json.loads(brain_stdout.decode().strip())
        except Exception as e:
            logger.error(f"[!] Engine pipe runtime failure for asset node {symbol}: {e}")
            return None

async def execute_swarm_sweep(args):
    logger.info("[*] Initializing broad market swarm processing sweep...")
    # Publish SCANNING state to MQTT to force Home Assistant state update
    start_payload = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "engine": "crypto_sword",
        "status": "SCANNING",
        "top_regime_signal": {"ticker": "N/A", "regime": "N/A", "calculated_kelly": 0.0, "signal_strength": 0.0},
        "execution_payload": {"status": "SCANNING", "allocated_dollars": 0.0}
    }
    await asyncio.to_thread(push_mqtt_telemetry, start_payload)

    guardrail.init_db()
    raw_universe = load_universe()
    if args.limit:
        raw_universe = raw_universe[:args.limit]
    if not raw_universe:
        logger.error("[-] Active evaluation asset array contains zero targets. Processing cancelled.")
        return

    concurrency_semaphore = asyncio.Semaphore(10)
    tasks = [process_single_asset_pipeline(symbol, concurrency_semaphore) for symbol in raw_universe]
    brain_signals = await asyncio.gather(*tasks)

    logger.info("[*] Complete asset matrix evaluated. Processing risk boundaries and execution gates...")

    for signal in brain_signals:
        if not signal or signal.get("status") not in ["success", "insufficient_data"]:
            continue

        symbol = signal.get("ticker", "UNKNOWN/USDT")
        regime = signal.get("regime", "Unknown")
        kelly_fraction = float(signal.get("kelly_fraction", 0.0))
        signal_strength = float(signal.get("signal_strength", 0.0))

        execution_status = "IDLE"
        allocated_dollars = 0.0
        telemetry_payload = {}

        # ==========================================
        # THE EXIT LOGIC (SELL CONDITION)
        # ==========================================
        if regime == "Bear":
            token_symbol = symbol.split("/")[0]
            balances = guardrail.get_wallet_balances_summary()
            held_token = None
            for chain, chain_data in balances.items():
                if token_symbol in chain_data.get("tokens", {}):
                    held_token = chain_data["tokens"][token_symbol]
                    break
            if held_token and held_token["quantity"] > 0:
                current_price = float(signal.get("current_price", held_token["avg_entry_price"]))
                ledger_receipt = guardrail.evaluate_and_execute_simulated_trade(symbol=symbol, action="SELL", quantity=held_token["quantity"], execution_price=current_price)
                if ledger_receipt.get("success"):
                    logger.info(f"[!!!] RISK-OFF SELL: Liquidated {held_token['quantity']:.4f} {symbol} due to Bear regime.")
                    execution_status = "SOLD_BEAR_REGIME"
                else:
                    execution_status = "SELL_FAILED"

        # ==========================================
        # THE ENTRY LOGIC (BUY CONDITION)
        # ==========================================
        elif regime == "Bull":
            brain_output_dump = json.dumps(signal)
            verdict = guardrail.run_piped_risk_gate(brain_output_dump)
            if verdict.get("status") == "approved":
                allocated_dollars = float(verdict.get("allocated_dollars", 0.0))
                current_price = float(signal.get("current_price", 0.0))
                if current_price <= 0:
                    execution_status = "INVALID_PRICE"
                else:
                    trade_qty = allocated_dollars / current_price
                    ledger_receipt = guardrail.evaluate_and_execute_simulated_trade(symbol=symbol, action="BUY", quantity=trade_qty, execution_price=current_price)
                    if ledger_receipt.get("success"):
                        logger.info(f"[+] LEDGER TRANSACTION SUCCESS: Bought {trade_qty:.4f} {symbol} at ${current_price:.2f}")
                        execution_status = "DISPATCHED"
                    else:
                        logger.warning(f"[-] Ledger transactional entry failure: {ledger_receipt.get('error')}")
                        execution_status = "REJECTED_BY_LEDGER"

        telemetry_payload = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "engine": "crypto_sword",
            "status": "SCAN_COMPLETE",
            "top_regime_signal": {"ticker": symbol, "regime": regime, "calculated_kelly": kelly_fraction, "signal_strength": signal_strength},
            "execution_payload": {"status": execution_status, "allocated_dollars": allocated_dollars}
        }
        await asyncio.to_thread(push_mqtt_telemetry, telemetry_payload)

async def main_async():
    parser = argparse.ArgumentParser(description="M.A.C.E. Phase 2 Multi-Agent Swarm Orchestrator Engine")
    parser.add_argument("--limit", type=int, default=0, help="Enforce limit constraints to test smaller token arrays")
    parser.add_argument("--daemon", action="store_true", help="Instantiates script as an uninterrupted polling daemon service")
    parser.add_argument("--interval", type=int, default=14400, help="Interval window duration parameters in seconds (Default 4h/14400s) - IGNORED IN DAEMON MODE IN FAVOR OF UTC SYNC")
    args = parser.parse_args()

    if args.daemon:
        logger.info("[*] Booting M.A.C.E. Continuous Background Worker. Synchronized to 4-Hour UTC Boundaries (HH:05:00).")
        while True:
            try:
                await execute_swarm_sweep(args)
            except Exception as e:
                logger.error(f"[!] Error during daemon run sweep: {e}")

            logger.info("[*] Task sequence finalized. Calculating sleep duration until next 4H UTC offset (HH:05:00)...")
            sleep_duration = get_seconds_until_next_4h_offset()
            logger.info(f"[*] Sleeping for {sleep_duration}s...")
            await asyncio.sleep(sleep_duration)
    else:
        await execute_swarm_sweep(args)
        logger.info("[+] Single swarm routing pass executed cleanly. Core shut down.")

if __name__ == "__main__":
    asyncio.run(main_async())
