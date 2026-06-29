#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Multi-Agent Swarm Pipeline
Component: The Portfolio Guardrail & Virtual Ledger Agent (guardrail.py)
"""

import sqlite3
import os
import sys
import json
import logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.guardrail")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

MAX_SINGLE_ASSET_EXPOSURE = 0.25
MIN_TRADE_SIZE_USD = 10.0

def get_db_connection(db_path=DEFAULT_DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    abs_db_path = os.path.abspath(db_path)
    db_uri = f"file:{abs_db_path}?nolock=1"
    conn = sqlite3.connect(db_uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(db_path=DEFAULT_DB_PATH):
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS wallets (blockchain TEXT PRIMARY KEY, public_key TEXT NOT NULL, gas_balance REAL NOT NULL, gas_token TEXT NOT NULL)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS token_balances (blockchain TEXT NOT NULL, token_symbol TEXT NOT NULL, balance REAL NOT NULL, avg_entry_price REAL NOT NULL, PRIMARY KEY (blockchain, token_symbol), FOREIGN KEY (blockchain) REFERENCES wallets(blockchain))""")

        cursor.execute("SELECT COUNT(*) FROM wallets")
        if cursor.fetchone()[0] == 0:
            logger.info("[*] Seeding fresh virtual multi-chain network infrastructure...")
            cursor.execute("INSERT INTO wallets VALUES ('SOLANA', 'MaceSolanaWallet111111111111111111111', 10.0, 'SOL')")
            cursor.execute("INSERT INTO wallets VALUES ('ARBITRUM', '0xMaceArbitrumExecutionSandboxWalletAddress', 0.5, 'ETH')")
            cursor.execute("INSERT INTO token_balances VALUES ('ARBITRUM', 'USDT', 10000.00, 1.00)")
            conn.commit()
            logger.info("[+] Seeding sequence completed successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"[-] Database structural initialization failure: {e}")
    finally:
        conn.close()

def get_wallet_balances_summary():
    conn = get_db_connection()
    cursor = conn.cursor()
    summary = {}
    try:
        cursor.execute("SELECT * FROM wallets")
        for w in cursor.fetchall():
            chain = w["blockchain"]
            summary[chain] = {"public_key": w["public_key"], "gas_balance": w["gas_balance"], "gas_token": w["gas_token"], "tokens": {}}
        cursor.execute("SELECT * FROM token_balances")
        for t in cursor.fetchall():
            chain = t["blockchain"]
            if chain in summary:
                summary[chain]["tokens"][t["token_symbol"]] = {"balance": t["balance"], "avg_entry_price": t["avg_entry_price"]}
    except Exception as e:
        logger.error(f"[-] Failed to aggregate wallet balances: {e}")
    finally:
        conn.close()
    return summary

def evaluate_and_execute_simulated_trade(symbol, action, quantity, execution_price):
    blockchain = "SOLANA" if "SOL" in symbol else "ARBITRUM"
    token_symbol = symbol.split("/")[0]
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT balance FROM token_balances WHERE blockchain = 'ARBITRUM' AND token_symbol = 'USDT'")
        usdt_row = cursor.fetchone()
        current_usdt_cash = float(usdt_row["balance"]) if usdt_row else 0.0
        trade_value_usd = quantity * execution_price

        if action == "BUY":
            if trade_value_usd > current_usdt_cash:
                return {"success": False, "error": "Insufficient USDT cash liquidity."}
            if trade_value_usd < MIN_TRADE_SIZE_USD:
                return {"success": False, "error": f"Transaction size (${trade_value_usd:.2f}) beneath minimum scale filter."}

            cursor.execute("UPDATE token_balances SET balance = balance - ? WHERE blockchain = 'ARBITRUM' AND token_symbol = 'USDT'", (trade_value_usd,))
            cursor.execute("SELECT balance, avg_entry_price FROM token_balances WHERE blockchain = ? AND token_symbol = ?", (blockchain, token_symbol))
            existing_token_row = cursor.fetchone()

            if existing_token_row:
                old_balance, old_price = existing_token_row["balance"], existing_token_row["avg_entry_price"]
                new_balance = old_balance + quantity
                new_avg_price = ((old_balance * old_price) + trade_value_usd) / new_balance
                cursor.execute("UPDATE token_balances SET balance = ?, avg_entry_price = ? WHERE blockchain = ? AND token_symbol = ?", (new_balance, new_avg_price, blockchain, token_symbol))
            else:
                cursor.execute("INSERT INTO token_balances VALUES (?, ?, ?, ?)", (blockchain, token_symbol, quantity, execution_price))
                new_balance, new_avg_price = quantity, execution_price
            conn.commit()
            return {"success": True, "action": "BUY", "symbol": symbol, "quantity": quantity, "execution_price": execution_price, "new_balance": new_balance, "avg_entry_price": round(new_avg_price, 4), "remaining_cash": round(current_usdt_cash - trade_value_usd, 2)}

        elif action == "SELL":
            cursor.execute("SELECT balance FROM token_balances WHERE blockchain = ? AND token_symbol = ?", (blockchain, token_symbol))
            balance_row = cursor.fetchone()
            current_token_holdings = float(balance_row["balance"]) if balance_row else 0.0

            if quantity > current_token_holdings:
                return {"success": False, "error": f"Attempting to liquidate more {token_symbol} than held on ledger."}
            usdt_gained = quantity * execution_price
            cursor.execute("UPDATE token_balances SET balance = balance + ? WHERE blockchain = 'ARBITRUM' AND token_symbol = 'USDT'", (usdt_gained,))

            if quantity == current_token_holdings:
                cursor.execute("DELETE FROM token_balances WHERE blockchain = ? AND token_symbol = ?", (blockchain, token_symbol))
                final_holdings = 0.0
            else:
                cursor.execute("UPDATE token_balances SET balance = balance - ? WHERE blockchain = ? AND token_symbol = ?", (quantity, blockchain, token_symbol))
                final_holdings = current_token_holdings - quantity
            conn.commit()
            return {"success": True, "action": "SELL", "symbol": symbol, "quantity": quantity, "execution_price": execution_price, "new_balance": final_holdings, "avg_entry_price": 0.0 if final_holdings == 0.0 else balance_row["avg_entry_price"], "remaining_cash": round(current_usdt_cash + usdt_gained, 2)}
        else:
            return {"success": False, "error": f"Action parameter '{action}' is un-routable."}
    except Exception as e:
        conn.rollback()
        logger.error(f"[-] Database transactional rollback triggered on {action} command for {symbol}: {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def run_piped_risk_gate(brain_output_str):
    try:
        signal_data = json.loads(brain_output_str)
    except Exception:
        return {"status": "error", "message": "Guardrail failed to parse standard input token payload."}

    if signal_data.get("status") != "success":
        return {"status": "ignored", "reason": "Brain signal packet reported upstream error configuration."}

    ticker = signal_data.get("ticker", "UNKNOWN/USDT")
    regime = signal_data.get("regime", "Unknown")
    kelly_f = float(signal_data.get("kelly_fraction", 0.0))
    current_price = float(signal_data.get("current_price", 0.0))

    if current_price <= 0:
        return {"status": "error", "message": "Guardrail received invalid current price from brain."}

    if regime != "Bull" or kelly_f <= 0.0:
        return {"status": "gate_closed", "ticker": ticker, "regime": regime, "allocated_dollars": 0.0, "reason": "Market regime does not conform to risk-on constraints."}

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT balance FROM token_balances WHERE blockchain = 'ARBITRUM' AND token_symbol = 'USDT'")
        cash_row = cursor.fetchone()
        available_usdt = float(cash_row["balance"]) if cash_row else 0.0

        cursor.execute("SELECT token_symbol, balance, avg_entry_price FROM token_balances WHERE token_symbol != 'USDT'")
        existing_holdings = cursor.fetchall()

        total_portfolio_value = available_usdt
        for holding in existing_holdings:
            proxy_price = holding["avg_entry_price"]
            if holding["token_symbol"] == ticker.split("/")[0]:
                proxy_price = current_price
            total_portfolio_value += (holding["balance"] * proxy_price)

        max_allowed_dollars = total_portfolio_value * MAX_SINGLE_ASSET_EXPOSURE
        desired_allocation_usd = available_usdt * kelly_f
        effective_cap = min(desired_allocation_usd, max_allowed_dollars, (available_usdt - MIN_TRADE_SIZE_USD))
        target_allocation_usd = max(0.0, effective_cap)

        if target_allocation_usd < MIN_TRADE_SIZE_USD:
            return {"status": "gate_closed", "ticker": ticker, "allocated_dollars": 0.0, "total_portfolio_value": round(total_portfolio_value, 2), "reason": "Calculated position scale maps below absolute trade sizing floor."}

        return {"status": "approved", "ticker": ticker, "regime": regime, "kelly_fraction": kelly_f, "total_portfolio_value": round(total_portfolio_value, 2), "max_exposure_limit": round(max_allowed_dollars, 2), "allocated_dollars": round(target_allocation_usd, 2)}
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
    if not sys.stdin.isatty():
        raw_stdin = sys.stdin.read()
        if raw_stdin.strip():
            verdict = run_piped_risk_gate(raw_stdin)
            print(json.dumps(verdict))
            sys.exit(0)
    summary = get_wallet_balances_summary()
    print("\n" + "="*50 + "\n      MACE VIRTUAL BLOCKCHAIN LEDGER SUMMARY\n" + "="*50)
    for chain, data in summary.items():
        print(f"\nChain: {chain}\n  Wallet Public Key : {data['public_key']}\n  Native Gas Balance: {data['gas_balance']:.4f} {data['gas_token']}\n  Token Assets:")
        if not data["tokens"]: print("    (No positive balances held)")
        else:
            for tok, tdata in data["tokens"].items(): print(f"    - {tok}: {tdata['balance']:.2f} (Avg Cost: ${tdata['avg_entry_price']:.2f})")
    print("="*50 + "\n")
