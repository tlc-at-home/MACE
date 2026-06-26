#!/usr/bin/env python3.11
import sqlite3
import os
import sys
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.guardrail")

# Default database path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

def get_db_connection(db_path=DEFAULT_DB_PATH):
    """
    Establishes and returns an active sqlite3 connection, ensuring parent directories exist.
    Uses the URI parameter nolock=1 to fully support NAS network filesystems without database locks.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    # Ensure absolute path is formatted for file: URI
    abs_db_path = os.path.abspath(db_path)
    db_uri = f"file:{abs_db_path}?nolock=1"
    conn = sqlite3.connect(db_uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(db_path=DEFAULT_DB_PATH):
    """
    Initializes the local virtual blockchain network database if it doesn't exist,
    creating the wallets and token_balances tables, then seeding with initial balances.
    """
    db_exists = os.path.exists(db_path)
    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    try:
        # Create Wallet Registry Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                wallet_id INTEGER PRIMARY KEY AUTOINCREMENT,
                blockchain TEXT UNIQUE NOT NULL,
                public_key TEXT NOT NULL,
                gas_balance REAL NOT NULL
            )
        """)

        # Create Token Balances Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS token_balances (
                wallet_id INTEGER,
                token_symbol TEXT NOT NULL,
                balance REAL NOT NULL,
                avg_entry_price REAL NOT NULL,
                PRIMARY KEY (wallet_id, token_symbol),
                FOREIGN KEY (wallet_id) REFERENCES wallets (wallet_id) ON DELETE CASCADE
            )
        """)
        conn.commit()

        # Seed Database if newly created or empty
        cursor.execute("SELECT COUNT(*) as count FROM wallets")
        if cursor.fetchone()["count"] == 0:
            logger.info("[*] Database is empty. Seeding virtual multi-chain portfolios...")
            
            # Simulated seed wallets
            wallets_seed = [
                ("SOLANA", "MaceSolV1xx3a88Z4p9QqrStUVwXyZ", 10.0),      # 10 SOL gas
                ("ARBITRUM", "MaceArbV1xx7c99E7b8Hhi9JKlAbC", 0.5),      # 0.5 ETH gas
                ("BASE", "MaceBaseV1xx5d77D6e5Ffg4OPqDeF", 0.25),       # 0.25 ETH gas
                ("ETHEREUM", "MaceEthV1xx2f44C3d2Ijk1LMmGhI", 1.0)       # 1.0 ETH gas
            ]
            
            for blockchain, pubkey, gas in wallets_seed:
                cursor.execute(
                    "INSERT INTO wallets (blockchain, public_key, gas_balance) VALUES (?, ?, ?)",
                    (blockchain, pubkey, gas)
                )
                wallet_id = cursor.lastrowid
                
                # Each wallet gets a starting cash pool of USDT
                # Solana and Ethereum get $5,000, Arbitrum $3,000, Base $2,000
                starting_cash = 5000.0
                if blockchain == "ARBITRUM":
                    starting_cash = 3000.0
                elif blockchain == "BASE":
                    starting_cash = 2000.0
                    
                cursor.execute(
                    "INSERT INTO token_balances (wallet_id, token_symbol, balance, avg_entry_price) VALUES (?, 'USDT', ?, 1.0)",
                    (wallet_id, starting_cash)
                )
            
            conn.commit()
            logger.info("[+] Seeding complete. Virtual wallets loaded with starting assets.")
            
    except Exception as e:
        logger.error(f"[-] Database initialization failure: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_blockchain_for_symbol(symbol):
    """
    Maps an asset symbol to its native blockchain network.
    """
    # Extract the base asset (e.g. "SOL" from "SOL/USDT")
    base = symbol.split("/")[0] if "/" in symbol else symbol
    base = base.upper()

    # Blockchain classifications based on the crypto universe mapping
    solana_assets = {"SOL", "JUP", "PYTH", "WIF", "BONK", "RENDER", "TRUMP"}
    arbitrum_assets = {"ARB"}
    base_assets = {"PEPE", "AAVE", "OP"}

    if base in solana_assets:
        return "SOLANA"
    elif base in arbitrum_assets:
        return "ARBITRUM"
    elif base in base_assets:
        return "BASE"
    else:
        # Default fallback is Ethereum mainnet (standard EVM ERC-20)
        return "ETHEREUM"

def get_gas_token(blockchain):
    """
    Returns the ticker symbol of the native gas token for the blockchain.
    """
    if blockchain == "SOLANA":
        return "SOL"
    elif blockchain in ("ARBITRUM", "BASE", "ETHEREUM"):
        return "ETH"
    return "GAS"

def get_portfolio_context_from_db(db_path=DEFAULT_DB_PATH):
    """
    Queries the database and aggregates all available USDT cash balances and currently
    held non-stablecoin token positions for portfolio-wide risk calculations.
    """
    # Initialize DB just in case
    init_db(db_path)
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    available_cash = 0.0
    existing_positions = []
    
    try:
        # Calculate sum of USDT balances across all wallets
        cursor.execute("""
            SELECT SUM(balance) as total_usdt 
            FROM token_balances 
            WHERE token_symbol = 'USDT'
        """)
        row = cursor.fetchone()
        if row and row["total_usdt"] is not None:
            available_cash = float(row["total_usdt"])
            
        # Get all non-stablecoin tokens held with positive balance
        cursor.execute("""
            SELECT token_symbol 
            FROM token_balances 
            WHERE token_symbol != 'USDT' AND balance > 0
        """)
        rows = cursor.fetchall()
        for r in rows:
            # Format held asset back to ticker format (e.g., "SOL/USDT")
            existing_positions.append(f"{r['token_symbol']}/USDT")
            
    except Exception as e:
        logger.error(f"[-] Error fetching portfolio context from DB: {e}")
    finally:
        conn.close()
        
    return available_cash, existing_positions

def get_wallet_balances_summary(db_path=DEFAULT_DB_PATH):
    """
    Returns a structured dictionary of all wallets and their nested token/gas balances.
    """
    init_db(db_path)
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    summary = {}
    try:
        cursor.execute("SELECT wallet_id, blockchain, public_key, gas_balance FROM wallets")
        wallets = cursor.fetchall()
        
        for w in wallets:
            w_id = w["wallet_id"]
            chain = w["blockchain"]
            
            cursor.execute("""
                SELECT token_symbol, balance, avg_entry_price 
                FROM token_balances 
                WHERE wallet_id = ? AND balance > 0
            """, (w_id,))
            tokens = cursor.fetchall()
            
            token_map = {}
            for t in tokens:
                token_map[t["token_symbol"]] = {
                    "balance": t["balance"],
                    "avg_entry_price": t["avg_entry_price"]
                }
                
            summary[chain] = {
                "wallet_id": w_id,
                "public_key": w["public_key"],
                "gas_balance": w["gas_balance"],
                "gas_token": get_gas_token(chain),
                "tokens": token_map
            }
    except Exception as e:
        logger.error(f"[-] Summary retrieval failed: {e}")
    finally:
        conn.close()
    return summary

def execute_db_trade(symbol, action, size_usd, asset_price, db_path=DEFAULT_DB_PATH):
    """
    Executes a simulated paper trade atomically on the SQLite virtual blockchain database.
    Increments/decrements asset balances, tracks average entry price, and deducts gas.
    """
    init_db(db_path)
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    blockchain = get_blockchain_for_symbol(symbol)
    base_token = symbol.split("/")[0] if "/" in symbol else symbol
    base_token = base_token.upper()
    action = action.upper()
    
    logger.info(f"[*] Executing DB simulated trade: {action} {symbol} of size ${size_usd:.2f} at price ${asset_price:.4f} on {blockchain}")
    
    try:
        # 1. Fetch wallet associated with blockchain
        cursor.execute("SELECT wallet_id, gas_balance FROM wallets WHERE blockchain = ?", (blockchain,))
        wallet = cursor.fetchone()
        if not wallet:
            return {"success": False, "error": f"No simulated wallet registered for blockchain {blockchain}."}
            
        wallet_id = wallet["wallet_id"]
        gas_balance = wallet["gas_balance"]
        
        # Determine gas fee cost to simulate on-chain transactions
        gas_token = get_gas_token(blockchain)
        if blockchain == "SOLANA":
            gas_fee = 0.0005  # 0.0005 SOL
        elif blockchain in ("ARBITRUM", "BASE"):
            gas_fee = 0.0001  # 0.0001 ETH
        else:
            gas_fee = 0.001   # 0.001 ETH (Ethereum mainnet)
            
        # Verify gas balance
        if gas_balance < gas_fee:
            return {"success": False, "error": f"Insufficient gas on {blockchain}. Available: {gas_balance} {gas_token}, Required: {gas_fee} {gas_token}"}
            
        # 2. Fetch cash (USDT) balance for this wallet
        cursor.execute("""
            SELECT balance FROM token_balances 
            WHERE wallet_id = ? AND token_symbol = 'USDT'
        """, (wallet_id,))
        cash_row = cursor.fetchone()
        usdt_balance = float(cash_row["balance"]) if cash_row else 0.0
        
        # 3. Handle BUY Action
        if action == "BUY":
            if usdt_balance < size_usd:
                return {"success": False, "error": f"Insufficient USDT cash pool on {blockchain}. Available: ${usdt_balance:.2f}, Requested: ${size_usd:.2f}"}
                
            qty_bought = size_usd / asset_price
            
            # Deduct USDT cash
            cursor.execute("""
                UPDATE token_balances 
                SET balance = balance - ? 
                WHERE wallet_id = ? AND token_symbol = 'USDT'
            """, (size_usd, wallet_id))
            
            # Check existing asset balance to compute correct average cost basis
            cursor.execute("""
                SELECT balance, avg_entry_price FROM token_balances 
                WHERE wallet_id = ? AND token_symbol = ?
            """, (wallet_id, base_token))
            asset_row = cursor.fetchone()
            
            if asset_row:
                current_bal = float(asset_row["balance"])
                current_avg_price = float(asset_row["avg_entry_price"])
                new_bal = current_bal + qty_bought
                new_avg_price = ((current_bal * current_avg_price) + (qty_bought * asset_price)) / new_bal
                
                cursor.execute("""
                    UPDATE token_balances 
                    SET balance = ?, avg_entry_price = ? 
                    WHERE wallet_id = ? AND token_symbol = ?
                """, (new_bal, new_avg_price, wallet_id, base_token))
            else:
                cursor.execute("""
                    INSERT INTO token_balances (wallet_id, token_symbol, balance, avg_entry_price) 
                    VALUES (?, ?, ?, ?)
                """, (wallet_id, base_token, qty_bought, asset_price))
                new_bal = qty_bought
                new_avg_price = asset_price
                
            # Deduct Gas
            cursor.execute("""
                UPDATE wallets 
                SET gas_balance = gas_balance - ? 
                WHERE wallet_id = ?
            """, (gas_fee, wallet_id))
            
            conn.commit()
            
            logger.info(f"[+] DB BUY trade executed. Bought {qty_bought:.4f} {base_token}. New balance: {new_bal:.4f}, cost basis: ${new_avg_price:.4f}")
            return {
                "success": True,
                "blockchain": blockchain,
                "wallet_id": wallet_id,
                "action": "BUY",
                "symbol": symbol,
                "gas_fee": f"{gas_fee} {gas_token}",
                "amount_usd": size_usd,
                "quantity": qty_bought,
                "new_balance": new_bal,
                "avg_entry_price": new_avg_price,
                "remaining_cash": usdt_balance - size_usd
            }
            
        # 4. Handle SELL Action
        elif action == "SELL":
            cursor.execute("""
                SELECT balance, avg_entry_price FROM token_balances 
                WHERE wallet_id = ? AND token_symbol = ?
            """, (wallet_id, base_token))
            asset_row = cursor.fetchone()
            if not asset_row or asset_row["balance"] <= 0:
                return {"success": False, "error": f"No token balance to sell for {base_token} in wallet {blockchain}."}
                
            current_bal = float(asset_row["balance"])
            # In simple orchestrator setups, selling refers to fully liquidating the position
            qty_to_sell = current_bal 
            usdt_gained = qty_to_sell * asset_price
            
            # Add USDT cash
            cursor.execute("""
                UPDATE token_balances 
                SET balance = balance + ? 
                WHERE wallet_id = ? AND token_symbol = 'USDT'
            """, (usdt_gained, wallet_id))
            
            # Clear/Delete token row since it is fully liquidated
            cursor.execute("""
                DELETE FROM token_balances 
                WHERE wallet_id = ? AND token_symbol = ?
            """, (wallet_id, base_token))
            
            # Deduct Gas
            cursor.execute("""
                UPDATE wallets 
                SET gas_balance = gas_balance - ? 
                WHERE wallet_id = ?
            """, (gas_fee, wallet_id))
            
            conn.commit()
            
            logger.info(f"[+] DB SELL trade executed. Sold {qty_to_sell:.4f} {base_token} for ${usdt_gained:.2f} USDT.")
            return {
                "success": True,
                "blockchain": blockchain,
                "wallet_id": wallet_id,
                "action": "SELL",
                "symbol": symbol,
                "gas_fee": f"{gas_fee} {gas_token}",
                "amount_usd": usdt_gained,
                "quantity": qty_to_sell,
                "new_balance": 0.0,
                "avg_entry_price": 0.0,
                "remaining_cash": usdt_balance + usdt_gained
            }
            
        else:
            return {"success": False, "error": f"Unsupported action: {action}"}
            
    except Exception as e:
        conn.rollback()
        logger.error(f"[-] DB execution exception during {action} for {symbol}: {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

if __name__ == "__main__":
    # If run standalone, print wallet and balance summary for quick debugging
    init_db()
    summary = get_wallet_balances_summary()
    print("\n" + "="*50)
    print("      MACE VIRTUAL BLOCKCHAIN LEDGER SUMMARY")
    print("="*50)
    for chain, data in summary.items():
        print(f"\nChain: {chain}")
        print(f"  Wallet Public Key : {data['public_key']}")
        print(f"  Native Gas Balance: {data['gas_balance']:.4f} {data['gas_token']}")
        print("  Token Assets:")
        if not data["tokens"]:
            print("    (No positive balances held)")
        else:
            for tok, tdata in data["tokens"].items():
                print(f"    - {tok:5}: Balance = {tdata['balance']:10.4f} | Avg Cost Basis = ${tdata['avg_entry_price']:.4f}")
    print("\n" + "="*50)
