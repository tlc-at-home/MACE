#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Multi-Agent Architecture Pipeline
Component: TradFi Equities Shield (tradfi_shield.py)
"""

import os
import sys
import json
import asyncio
import argparse
import logging
from datetime import datetime
import paho.mqtt.client as mqtt_client
import alpaca_trade_api as tradeapi

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

MQTT_BROKER_IP = "192.168.0.110"
MQTT_PORT = 1883
MQTT_TOPIC = "mace/telemetry/tradfi_shield"

MAX_PORTFOLIO_DRAWDOWN_LIMIT = 0.05
MAX_SINGLE_POSITION_LOSS_LIMIT = 0.08

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.tradfi_shield")

def push_mqtt_telemetry(payload):
    try:
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        client.connect(MQTT_BROKER_IP, MQTT_PORT, 60)
        client.publish(MQTT_TOPIC, json.dumps(payload))
        client.disconnect()
    except Exception as e:
        logger.error(f"[!] Telemetry update path bottlenecked: {e}")

async def execute_deterministic_risk_loop(args):
    logger.info("[*] Initializing M.A.C.E. Deterministic Risk Shield Loop...")
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

    if not api_key or not secret_key:
        logger.error("[-] Alpaca API keys missing. Shield aborted.")
        return

    api = tradeapi.REST(api_key, secret_key, base_url, api_version='v2')

    while True:
        try:
            account = await asyncio.to_thread(api.get_account)
            positions = await asyncio.to_thread(api.list_positions)
            execution_status = "SECURE"
            breach_details = ""

            # Safe fallback for different versions of Alpaca SDK
            if hasattr(account, 'unrealized_plpc'):
                portfolio_plpc = float(account.unrealized_plpc)
            else:
                # Calculate it manually: (Current Equity - Last Equity) / Last Equity
                portfolio_plpc = (float(account.equity) - float(account.last_equity)) / float(account.last_equity)

            if portfolio_plpc <= -MAX_PORTFOLIO_DRAWDOWN_LIMIT:
                logger.critical(f"[!!!] PORTFOLIO DRAWDOWN BREACH: {portfolio_plpc*100:.2f}%! INITIATING FULL LIQUIDATION!")
                execution_status = "FULL_LIQUIDATION_TRIGGERED"
                breach_details = f"Portfolio Down {portfolio_plpc*100:.2f}%"
                for pos in positions:
                    logger.warning(f"[*] EMERGENCY CLOSE: {pos.symbol}")
                    await asyncio.to_thread(api.close_position, pos.symbol)
            else:
                for pos in positions:
                    pos_plpc = float(pos.unrealized_plpc)
                    if pos_plpc <= -MAX_SINGLE_POSITION_LOSS_LIMIT:
                        logger.warning(f"[!] SINGLE ASSET BREACH: {pos.symbol} is down {pos_plpc*100:.2f}%. LIQUIDATING.")
                        execution_status = "SINGLE_ASSET_LIQUIDATED"
                        breach_details = f"{pos.symbol} Down {pos_plpc*100:.2f}%"
                        await asyncio.to_thread(api.close_position, pos.symbol)

            telemetry_payload = {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "engine": "tradfi_shield",
                "status": "MONITORING_ACTIVE",
                "risk_metrics": {"portfolio_plpc": round(portfolio_plpc, 4), "open_positions": len(positions)},
                "execution_payload": {"status": execution_status, "breach_details": breach_details}
            }
            push_mqtt_telemetry(telemetry_payload)

        except Exception as e:
            logger.error(f"[!] Exception inside Shield loop: {e}")
            push_mqtt_telemetry({"engine": "tradfi_shield", "status": "SHIELD_EXCEPTION_ERROR"})

        if not args.continuous:
            break
        await asyncio.sleep(args.interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="M.A.C.E. Phase 2 Deterministic TradFi Shield")
    parser.add_argument("--continuous", action="store_true", default=True, help="Enforces permanent looping")
    parser.add_argument("--interval", type=int, default=60, help="Frequency for evaluation checks in seconds (Default 1m/60s)")
    args = parser.parse_args()
    asyncio.run(execute_deterministic_risk_loop(args))
