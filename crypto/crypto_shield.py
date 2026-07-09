#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Crypto Shield (True Trailing Stop-Loss - Explicit Formatting)
Tracks High-Water Marks natively in portfolio.db to protect profits independently
of Orchestrator rebalancing loops and capital constraints.
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

# Base Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

# Strict Math Limits
MAX_SINGLE_POSITION_LOSS_LIMIT = 0.08  # 8% trailing stop-loss from peak

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.crypto_shield")

def get_db_connection(db_path=DEFAULT_DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    abs_db_path = os.path.abspath(db_path)
    db_uri = f"file:{abs_db_path}?nolock=1"
    return sqlite3.connect(db_uri, uri=True)

class CryptoShield:
    def __init__(self):
        self.primary_exchange = ccxt.kucoin({'enableRateLimit': True})
        self.fallback_exchange = ccxt.binance({'enableRateLimit': True})
        logger.info("[*] Autonomous Trailing Defensive Shield Connected. Source: KuCoin")

    async def fetch_live_price(self, pair):
        target_pair = pair.replace("_", "/")
        try:
            ticker = await asyncio.to_thread(self.primary_exchange.fetch_ticker, target_pair)
            return float(ticker['last'])
        except (ccxt.BadSymbol, ccxt.MarketNotReady, ccxt.ExchangeError):
            try:
                logger.warning(f"[-] Synced feed failed or symbol {target_pair} unavailable on KuCoin. Failover routing to Binance...")
                ticker = await asyncio.to_thread(self.fallback_exchange.fetch_ticker, target_pair)
                return float(ticker['last'])
            except Exception as backup_error:
                logger.error(f"[!] Critical Error: Ticker lookup failed for {target_pair} across both pools: {backup_error}")
                return None
        except Exception as e:
            logger.error(f"[!] Unexpected error fetching price for {target_pair}: {e}")
            return None

    async def run_shield_cycle(self):
        logger.info("[*] Commencing deterministic 15-minute risk trailing sweep...")
        breach_details = []

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Extract active assets with entry price and high water mark columns
            cursor.execute("SELECT token, quantity, avg_entry_price, high_water_mark FROM portfolio WHERE quantity > 0 AND token != 'USDT'")
            active_positions = cursor.fetchall()

            if not active_positions:
                logger.info("[*] Sweep complete: No active token holdings found in the matrix database ledger.")
                conn.close()
                return

            for token, qty, cost_basis, hwm in active_positions:
                pair = f"{token}/USDT"

                if not cost_basis or cost_basis <= 0:
                    continue

                live_price = await self.fetch_live_price(pair)
                if not live_price:
                    continue

                # Architectural Catch: If high water mark is uninitialized, seed it with the current price
                if not hwm or hwm <= 0:
                    hwm = max(cost_basis, live_price)
                    cursor.execute("UPDATE portfolio SET high_water_mark = ? WHERE token = ?", (hwm, token))
                    conn.commit()

                # Peak Ratchet: If the price breaks a new high, lock it into database state immediately
                if live_price > hwm:
                    if hwm < 0.01 or live_price < 0.01:
                        logger.info(f"[+] NEW PEAK RECORDED: {token} shifted high water mark from ${hwm:.8f} -> ${live_price:.8f}")
                    else:
                        logger.info(f"[+] NEW PEAK RECORDED: {token} shifted high water mark from ${hwm:.4f} -> ${live_price:.4f}")
                    hwm = live_price
                    cursor.execute("UPDATE portfolio SET high_water_mark = ? WHERE token = ?", (hwm, token))
                    conn.commit()

                # Compute drawdown metrics relative directly to the Peak High Water Mark
                trailing_drawdown_pct = (live_price - hwm) / hwm
                floor_price = hwm * (1 - MAX_SINGLE_POSITION_LOSS_LIMIT)

                # Dynamic conditional formatting blocks based on asset scale properties
                if live_price < 0.01:
                    logger.info(f"[*] Position Check: {pair} | Qty: {qty} | Peak HWM: ${hwm:.8f} | Live: ${live_price:.8f} | Floor Target: ${floor_price:.8f} | Drop from Peak: {trailing_drawdown_pct*100:+.2f}%")
                else:
                    logger.info(f"[*] Position Check: {pair} | Qty: {qty} | Peak HWM: ${hwm:.4f} | Live: ${live_price:.4f} | Floor Target: ${floor_price:.4f} | Drop from Peak: {trailing_drawdown_pct*100:+.2f}%")

                # Hard Check: Validate if current asset values breach the trailing peak floor
                if trailing_drawdown_pct <= -MAX_SINGLE_POSITION_LOSS_LIMIT:
                    logger.warning(f"[!!!] TRAILING STOP-LOSS BREACHED: {pair} dropped {trailing_drawdown_pct*100:.2f}% below peak!")

                    usdt_recovered = qty * live_price
                    if live_price < 0.01:
                        logger.warning(f"[!!!] EXECUTION REFLEX: Liquidating {qty} {token} at ${live_price:.8f} -> Recovering ${usdt_recovered:.2f} USDT")
                    else:
                        logger.warning(f"[!!!] EXECUTION REFLEX: Liquidating {qty} {token} at ${live_price:.4f} -> Recovering ${usdt_recovered:.2f} USDT")

                    # Execute atomic database balance swap and reset the tracking anchors
                    cursor.execute("UPDATE portfolio SET quantity = 0, avg_entry_price = 0, high_water_mark = 0 WHERE token = ?", (token,))
                    cursor.execute("UPDATE portfolio SET quantity = quantity + ? WHERE token = 'USDT'", (usdt_recovered,))
                    conn.commit()
                    breach_details.append(f"{pair} stopped out at trailing floor.")

            conn.close()

        except Exception as e:
            logger.error(f"[!] Exception caught inside main Shield trailing execution block: {e}")

async def main():
    shield = CryptoShield()
    while True:
        await shield.run_shield_cycle()
        if not args.daemon:
            break
        await asyncio.sleep(args.interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="M.A.C.E. Crypto Trailing Shield")
    parser.add_argument("--daemon", action="store_true", default=True, help="Enforces permanent looping")
    parser.add_argument("--interval", type=int, default=900, help="Frequency for evaluation checks in seconds")
    args = parser.parse_args()

    asyncio.run(main())
