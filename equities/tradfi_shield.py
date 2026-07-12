#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 - TradFi Equities Shield (Stateless Consumer Layer)
Extracts portfolio-wide allocations and evaluates dynamic pullback tolerances
calculated individually per asset by the upstream scout process.
"""

import os
import sys
import json
import asyncio
import argparse
import logging
import sqlite3
from datetime import datetime
import paho.mqtt.client as mqtt_client
import alpaca_trade_api as tradeapi

DEFAULT_DB_PATH = "/home/fedora/MACE/config/portfolio.db"
MQTT_BROKER_IP = "192.168.0.110"
MQTT_PORT = 1883
MQTT_TOPIC = "mace/telemetry/tradfi_shield"

def push_mqtt_telemetry(payload):
    try:
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        user = os.environ.get("MQTT_USER")
        password = os.environ.get("MQTT_PASSWORD")
        if user and password:
            client.username_pw_set(user, password)
        client.connect(MQTT_BROKER_IP, MQTT_PORT, 60)
        client.publish(MQTT_TOPIC, json.dumps(payload))
        client.disconnect()
    except Exception as e:
        logger.error(f"[!] Telemetry update path bottlenecked: {e}")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.tradfi_shield")

class TradFiShield:
    def __init__(self):
        logger.info("[*] Initializing M.A.C.E. Automated Risk Reflex Shield...")
        key_id = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_SECRET_KEY")
        is_paper = os.getenv("ALPACA_PAPER_TRADE", "true").lower() == "true"
        base_url = "https://paper-api.alpaca.markets" if is_paper else "https://api.alpaca.markets"

        if not key_id or not secret_key:
            logger.critical("[!] API Context credentials unresolved from environment variables.")
            sys.exit(1)

        self.api = tradeapi.REST(key_id=key_id, secret_key=secret_key, base_url=base_url)

    def get_position_metrics(self, symbol):
        """Pulls the high-water mark and custom calibrated loss limit from SQLite."""
        try:
            with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT high_water_mark, loss_limit FROM equities_hwm WHERE symbol = ?", (symbol,))
                row = cursor.fetchone()
                if row:
                    return float(row[0]), float(row[1])
        except Exception as e:
            logger.error(f"[-] Database lookup failed for {symbol}: {e}")
        return None, 0.05  # Fallback to standard 5% boundary on missing data anomalies

    async def run_shield_sweep(self):
        logger.info("[*] Commencing stateless calculated risk evaluation sweep...")
        breach_details = []
        positions_telemetry = []

        try:
            account = await asyncio.to_thread(self.api.get_account)
            total_portfolio_value = float(account.equity)
            cash_balance = float(account.cash)

            positions = await asyncio.to_thread(self.api.list_positions)

            for pos in positions:
                symbol = pos.symbol
                qty = float(pos.qty)
                live_price = float(pos.current_price)
                avg_entry = float(pos.avg_entry_price)

                # Fetch tracking parameters calculated straight from historical market noise
                hwm, loss_limit = self.get_position_metrics(symbol)

                if not hwm:
                    hwm = max(avg_entry, live_price)

                trailing_drawdown_pct = (live_price - hwm) / hwm
                floor_price = hwm * (1 - loss_limit)

                logger.info(f"[*] [{symbol}] Calibrated Corridor: {loss_limit*100:.2f}% | Peak High: ${hwm:.2f} | Live Price: ${live_price:.2f} | Stop Floor: ${floor_price:.2f} | Delta: {trailing_drawdown_pct*100:+.2f}%")

                if trailing_drawdown_pct <= -loss_limit:
                    logger.warning(f"[!!!] CALIBRATED THRESHOLD BREACHED ON {symbol}: Drawdown hit {trailing_drawdown_pct*100:.2f}%")
                    logger.warning(f"[!!!] DISPATCHING LIQUIDATION: Exiting open position for {symbol}...")

                    await asyncio.to_thread(self.api.close_position, symbol)

                    with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                        conn.cursor().execute("DELETE FROM equities_hwm WHERE symbol = ?", (symbol,))
                        conn.commit()

                    breach_details.append(f"{symbol} stopped out at {trailing_drawdown_pct*100:.2f}% loss from HWM.")
                else:
                    positions_telemetry.append({
                        "symbol": symbol,
                        "qty": qty,
                        "avg_cost": avg_entry,
                        "live_price": live_price,
                        "hwm": hwm,
                        "loss_limit": loss_limit,
                        "floor_price": round(floor_price, 2),
                        "drawdown_pct": round(trailing_drawdown_pct * 100, 2),
                        "market_value": round(qty * live_price, 2)
                    })

            telemetry_payload = {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "engine": "tradfi_shield",
                "status": "MONITORING_ACTIVE",
                "cash_balance": round(cash_balance, 2),
                "total_portfolio_value": round(total_portfolio_value, 2),
                "positions": positions_telemetry,
                "breaches": breach_details
            }
            push_mqtt_telemetry(telemetry_payload)

        except Exception as e:
            logger.error(f"[!] Exception within protection loop context: {e}")

async def main():
    shield = TradFiShield()
    while True:
        await shield.run_shield_sweep()
        if not args.daemon:
            break
        await asyncio.sleep(args.interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="M.A.C.E. Phase 2 Calibrated Risk Shield")
    parser.add_argument("--daemon", action="store_true", default=True, help="Enforces service looping")
    parser.add_argument("--interval", type=int, default=60, help="Check intervals in seconds")
    args = parser.parse_args()

    asyncio.run(main())
