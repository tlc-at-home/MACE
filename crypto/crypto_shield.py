#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Crypto Shield (Deterministic Paper Math Version)
Reads portfolio.db, fetches live CCXT prices, and enforces hard stop-losses on trades.
"""

import os
import sys
import json
import asyncio
import argparse
import logging
import sqlite3
from datetime import datetime
import ccxt
import paho.mqtt.client as mqtt_client

# Base Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

# MQTT Telemetry
MQTT_BROKER_IP = os.getenv("MQTT_BROKER_IP", "192.168.0.110")
MQTT_PORT = 1883
MQTT_TOPIC = "mace/telemetry/crypto_shield"

# Strict Math Limits
MAX_SINGLE_POSITION_LOSS_LIMIT = 0.08 # 8% single asset stop-loss

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.crypto_shield")

def get_db_connection(db_path=DEFAULT_DB_PATH):
    db_uri = f"file:{db_path}?nolock=1"
    conn = sqlite3.connect(db_uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn

def push_mqtt_telemetry(payload):
    try:
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        client.connect(MQTT_BROKER_IP, MQTT_PORT, 60)
        client.publish(MQTT_TOPIC, json.dumps(payload))
        client.disconnect()
    except Exception as e:
        logger.error(f"[!] Telemetry update path bottlenecked: {e}")

async def execute_deterministic_risk_loop(args):
    logger.info("[*] Initializing M.A.C.E. Crypto Deterministic Risk Shield...")

    # Initialize CCXT to fetch live prices for drawdown checks
    exchange = ccxt.binance({'enableRateLimit': True})

    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # 1. Fetch all holdings (ignore USDT cash)
            cursor.execute("SELECT token_symbol, balance, avg_entry_price FROM token_balances WHERE token_symbol != 'USDT'")
            paper_holdings = cursor.fetchall()

            execution_status = "PAPER_PORTFOLIO_SECURE"
            breach_details = []

            for holding in paper_holdings:
                token_symbol = holding["token_symbol"]
                paper_balance = holding["balance"]
                entry_price = holding["avg_entry_price"]

                if paper_balance <= 0 or entry_price <= 0:
                    continue

                # Reconstruct the CCXT pair (e.g., BTC -> BTC/USDT)
                pair = f"{token_symbol}/USDT"

                # 2. Fetch live market price in a thread to prevent blocking
                try:
                    ticker = await asyncio.to_thread(exchange.fetch_ticker, pair)
                    current_price = ticker['last']
                except Exception as e:
                    logger.warning(f"[!] Could not fetch price for {pair}: {e}")
                    continue

                # 3. Calculate Drawdown
                paper_pnl_pct = (current_price - entry_price) / entry_price

                # 4. Enforce Stop-Loss Logic
                if paper_pnl_pct <= -MAX_SINGLE_POSITION_LOSS_LIMIT:
                    logger.warning(f"[!!!] PAPER STOP-LOSS TRIGGERED: {pair} is down {paper_pnl_pct*100:.2f}%. Simulating liquidation.")

                    # Calculate USDT recovered
                    usdt_recovered = paper_balance * current_price

                    # Delete the token holding
                    cursor.execute("DELETE FROM token_balances WHERE token_symbol = ?", (token_symbol,))

                    # Add recovered cash back to USDT bucket
                    cursor.execute("UPDATE token_balances SET balance = balance + ? WHERE token_symbol = 'USDT'", (usdt_recovered,))

                    conn.commit()
                    execution_status = "PAPER_STOP_LOSS_EXECUTED"
                    breach_details.append(f"{pair} stopped out at {paper_pnl_pct*100:.2f}% loss.")

            conn.close()

            # 5. Telemetry Dispatch
            telemetry_payload = {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "engine": "crypto_shield",
                "status": "MONITORING_ACTIVE_PAPER",
                "execution_status": execution_status,
                "breaches": breach_details
            }
            push_mqtt_telemetry(telemetry_payload)

        except Exception as e:
            logger.error(f"[!] Exception inside Shield loop: {e}")
            push_mqtt_telemetry({"engine": "crypto_shield", "status": "SHIELD_EXCEPTION_ERROR"})

        if not args.daemon:
            break

        await asyncio.sleep(args.interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="M.A.C.E. Crypto Deterministic Shield")
    parser.add_argument("--daemon", action="store_true", default=True, help="Enforces permanent looping")
    parser.add_argument("--interval", type=int, default=900, help="Frequency for evaluation checks in seconds (Default 15m/900s)")
    args = parser.parse_args()
    asyncio.run(execute_deterministic_risk_loop(args))
