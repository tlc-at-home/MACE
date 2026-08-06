#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 - TradFi Equities Shield (Stateless Consumer Layer)
Extracts portfolio-wide allocations and evaluates dynamic pullback tolerances
calculated individually per asset by the upstream scout process.
Refactored to use BrokerClient abstraction layer.
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

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from brokers import get_client_for_symbol, get_client_by_name

DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")
MQTT_BROKER_IP = os.getenv("MQTT_BROKER_IP", "192.168.0.110")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "mace/telemetry/tradfi_shield"

def push_mqtt_telemetry(payload):
    try:
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        user = os.environ.get("MQTT_USER")
        password = os.environ.get("MQTT_PASSWORD")
        if user and password:
            client.username_pw_set(user, password)
        client.connect(MQTT_BROKER_IP, MQTT_PORT, 60)
        client.publish(MQTT_TOPIC, json.dumps(payload), retain=True)
        client.disconnect()
    except Exception as e:
        logger.error(f"[!] Telemetry update path bottlenecked: {e}")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.tradfi_shield")

class TradFiShield:
    def __init__(self):
        logger.info("[*] Initializing M.A.C.E. Automated Risk Reflex Shield via BrokerClient...")

    def get_position_metrics(self, symbol):
        """Pulls the high-water mark, loss limit, and stop floor price from SQLite view."""
        try:
            with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT high_water_mark, loss_limit, stop_floor_price FROM vw_equities_risk_corridors WHERE symbol = ?", (symbol,))
                row = cursor.fetchone()
                if row:
                    return float(row[0]), float(row[1]), float(row[2])
        except Exception as e:
            logger.error(f"[-] Database lookup failed for {symbol}: {e}")
        return None, 0.05, None  # Fallback to standard 5% boundary on missing data anomalies

    async def run_shield_sweep(self):
        logger.info("[*] Commencing stateless calculated risk evaluation sweep...")
        breach_details = []
        positions_telemetry = []

        try:
            primary_client = get_client_by_name("alpaca")
            account = await asyncio.to_thread(primary_client.get_account)
            total_portfolio_value = float(account.get("equity", 0.0))
            cash_balance = float(account.get("cash", 0.0))

            positions = await asyncio.to_thread(primary_client.get_positions)

            for pos in positions:
                symbol = pos["symbol"]
                qty = float(pos["qty"])
                live_price = float(pos["current_price"])
                avg_entry = float(pos["avg_entry_price"])

                symbol_client = get_client_for_symbol(symbol)

                # Fetch tracking parameters calculated straight from historical market noise via view
                hwm, loss_limit, calc_floor = self.get_position_metrics(symbol)

                if not hwm:
                    hwm = max(avg_entry, live_price)

                trailing_drawdown_pct = (live_price - hwm) / hwm
                floor_price = calc_floor if calc_floor is not None else hwm * (1 - loss_limit)

                logger.info(f"[*] [{symbol}] Calibrated Corridor: {loss_limit*100:.2f}% | Peak High: ${hwm:.2f} | Live Price: ${live_price:.2f} | Stop Floor: ${floor_price:.2f} | Delta: {trailing_drawdown_pct*100:+.2f}%")

                if trailing_drawdown_pct <= -loss_limit:
                    mkt_hours = symbol_client.get_market_hours(symbol)
                    if not mkt_hours.get("is_open", True):
                        logger.warning(f"[!] {symbol} threshold breached but market ({mkt_hours.get('exchange')}) is closed. Deferring liquidation.")
                        continue

                    logger.warning(f"[!!!] CALIBRATED THRESHOLD BREACHED ON {symbol}: Drawdown hit {trailing_drawdown_pct*100:.2f}%")
                    logger.warning(f"[!!!] DISPATCHING LIQUIDATION: Exiting open position for {symbol}...")

                    await asyncio.to_thread(symbol_client.close_position, symbol)

                    with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                        conn.execute("PRAGMA foreign_keys = ON;")
                        cursor = conn.cursor()
                        cursor.execute("SELECT asset_id FROM vw_equities_universe WHERE symbol = ?", (symbol,))
                        row = cursor.fetchone()
                        if row:
                            asset_id = row[0]
                            now_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                            cooldown_until_str = (datetime.utcnow() + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
                            cursor.execute("""
                                INSERT INTO trade_cooldowns (asset_id, symbol, closed_at, reason, cooldown_until)
                                VALUES (?, ?, ?, 'STOP_LOSS_BREACH', ?)
                            """, (asset_id, symbol, now_str, cooldown_until_str))
                        cursor.execute("DELETE FROM equities_hwm WHERE symbol = ?", (symbol,))
                        conn.commit()

                    breach_details.append(f"{symbol} stopped out at {trailing_drawdown_pct*100:.2f}% loss from HWM. 24h Cooldown Lock registered.")
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
