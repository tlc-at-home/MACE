#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Crypto Shield (Dynamic Volatility-Based Trailing Stop-Loss)
Enforces trailing stop-losses calculated dynamically from asset volatility.
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

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.crypto_shield")

def get_db_connection(db_path=DEFAULT_DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    abs_db_path = os.path.abspath(db_path)
    conn = sqlite3.connect(abs_db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn

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

            # Extract active assets with entry price columns
            cursor.execute("SELECT token, quantity, avg_entry_price FROM portfolio WHERE quantity > 0 AND token != 'USDT'")
            active_positions = cursor.fetchall()

            if not active_positions:
                logger.info("[*] Sweep complete: No active token holdings found in the matrix database ledger.")
                conn.close()
                return

            for row in active_positions:
                token = row["token"]
                qty = float(row["quantity"])
                cost_basis = float(row["avg_entry_price"])
                pair = f"{token}/USDT"

                if not cost_basis or cost_basis <= 0:
                    continue

                live_price = await self.fetch_live_price(pair)
                if live_price is None:
                    continue

                # Query trailing stop-loss metrics from crypto_hwm
                cursor.execute("SELECT high_water_mark, loss_limit FROM crypto_hwm WHERE symbol = ?", (pair,))
                hwm_row = cursor.fetchone()
                if hwm_row:
                    hwm = float(hwm_row["high_water_mark"])
                    loss_limit = float(hwm_row["loss_limit"])
                else:
                    hwm = max(cost_basis, live_price)
                    loss_limit = 0.08
                    now_str = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                    cursor.execute("""
                        INSERT OR IGNORE INTO crypto_hwm (symbol, high_water_mark, loss_limit, updated_at)
                        VALUES (?, ?, ?, ?)
                    """, (pair, hwm, loss_limit, now_str))
                    conn.commit()

                # Peak Ratchet: If the price breaks a new high, lock it into database state immediately
                if live_price > hwm:
                    if hwm < 0.01 or live_price < 0.01:
                        logger.info(f"[+] NEW PEAK RECORDED: {token} shifted high water mark from ${hwm:.8f} -> ${live_price:.8f}")
                    else:
                        logger.info(f"[+] NEW PEAK RECORDED: {token} shifted high water mark from ${hwm:.4f} -> ${live_price:.4f}")
                    hwm = live_price
                    cursor.execute("UPDATE crypto_hwm SET high_water_mark = ? WHERE symbol = ?", (hwm, pair))
                    # Also update the portfolio table column if present
                    try:
                        cursor.execute("UPDATE portfolio SET high_water_mark = ? WHERE token = ?", (hwm, token))
                    except sqlite3.OperationalError:
                        pass
                    conn.commit()

                # Compute drawdown metrics relative directly to the Peak High Water Mark
                trailing_drawdown_pct = (live_price - hwm) / hwm
                floor_price = hwm * (1 - loss_limit)

                # Dynamic conditional formatting blocks based on asset scale properties
                if live_price < 0.01:
                    logger.info(f"[*] Position Check: {pair} | Qty: {qty} | Peak HWM: ${hwm:.8f} | Live: ${live_price:.8f} | Floor Target: ${floor_price:.8f} | Drop from Peak: {trailing_drawdown_pct*100:+.2f}%")
                else:
                    logger.info(f"[*] Position Check: {pair} | Qty: {qty} | Peak HWM: ${hwm:.4f} | Live: ${live_price:.4f} | Floor Target: ${floor_price:.4f} | Drop from Peak: {trailing_drawdown_pct*100:+.2f}%")

                # Hard Check: Validate if current asset values breach the trailing peak floor
                if trailing_drawdown_pct <= -loss_limit:
                    logger.warning(f"[!!!] TRAILING STOP-LOSS BREACHED: {pair} dropped {trailing_drawdown_pct*100:.2f}% below peak!")

                    usdt_recovered = qty * live_price
                    if live_price < 0.01:
                        logger.warning(f"[!!!] EXECUTION REFLEX: Liquidating {qty} {token} at ${live_price:.8f} -> Recovering ${usdt_recovered:.2f} USDT")
                    else:
                        logger.warning(f"[!!!] EXECUTION REFLEX: Liquidating {qty} {token} at ${live_price:.4f} -> Recovering ${usdt_recovered:.2f} USDT")

                    # Execute atomic database balance swap and reset the tracking anchors
                    try:
                        cursor.execute("UPDATE portfolio SET quantity = 0, avg_entry_price = 0, high_water_mark = 0 WHERE token = ?", (token,))
                    except sqlite3.OperationalError:
                        cursor.execute("UPDATE portfolio SET quantity = 0, avg_entry_price = 0 WHERE token = ?", (token,))
                    cursor.execute("DELETE FROM crypto_hwm WHERE symbol = ?", (pair,))
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
