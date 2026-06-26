#!/usr/bin/env python3
import os
import sys
import argparse
import asyncio
import ccxt.async_support as ccxt

async def check_balance(use_sandbox=False):
    # Load credentials from environment
    api_key = os.environ.get("KUCOIN_API_KEY")
    secret_key = os.environ.get("KUCOIN_SECRET_KEY")
    password = os.environ.get("KUCOIN_PASSWORD")
    
    if not api_key or not secret_key or not password:
        print("[!] Error: KUCOIN_API_KEY, KUCOIN_SECRET_KEY, and KUCOIN_PASSWORD must be set in your environment.")
        print("Make sure they are exported in your current shell session.")
        sys.exit(1)
        
    env_name = "KuCoin Sandbox" if use_sandbox else "KuCoin Spot Trading Account"
    print(f"[*] Querying live balances from {env_name}...")
    
    exchange = ccxt.kucoin({
        'apiKey': api_key,
        'secret': secret_key,
        'password': password,
        'enableRateLimit': True,
    })
    
    if use_sandbox:
        exchange.set_sandbox_mode(True)
        
    try:
        # Fetch Trading (Spot) account balances
        balance = await exchange.fetch_balance({'type': 'trade'})
        total = balance.get('total', {})
        free = balance.get('free', {})
        used = balance.get('used', {})
        
        # Filter for non-zero holdings
        active_holdings = []
        for asset, amount in total.items():
            if amount > 1e-8:  # ignore tiny dust balances
                active_holdings.append({
                    "Asset": asset,
                    "Total": amount,
                    "Available": free.get(asset, 0),
                    "Locked": used.get(asset, 0)
                })
                
        if not active_holdings:
            print(f"\n[+] Connection successful! Your {env_name} has 0 active holdings (only dust/empty).")
            return
            
        print("\n==========================================================================")
        print(f"                     {env_name.upper()} HOLDINGS                          ")
        print("==========================================================================")
        print(f" {'Asset':<10} | {'Total Balance':<18} | {'Available':<18} | {'In Orders (Locked)':<18}")
        print("-" * 74)
        for h in active_holdings:
            print(f" {h['Asset']:<10} | {h['Total']:<18.8f} | {h['Available']:<18.8f} | {h['Locked']:<18.8f}")
        print("==========================================================================")
        
    except Exception as e:
        print(f"[!] API query failed: {e}")
        if "KC-API-KEY not exists" in str(e) and use_sandbox:
            print("\n[TIP] For KuCoin Sandbox, you must use Sandbox API keys generated from sandbox.kucoin.com.")
            print("Production API keys from kucoin.com will not work on the Sandbox environment.")
    finally:
        await exchange.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KuCoin Balance Checker")
    parser.add_argument("--sandbox", action="store_true", help="Enable KuCoin Sandbox mode")
    args = parser.parse_args()
    
    asyncio.run(check_balance(use_sandbox=args.sandbox))
