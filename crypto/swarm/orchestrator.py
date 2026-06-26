import sys
import json
import asyncio
import os
import argparse
from datetime import datetime
from paho.mqtt import client as mqtt_client
import ccxt.async_support as ccxt

# Base Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_UNIVERSE_PATH = os.path.join(BASE_DIR, "config/crypto_universe.json")

# Import local modules
sys.path.append(os.path.join(BASE_DIR, "crypto/swarm"))
import guardrail

# MQTT Broker config
MQTT_BROKER = "192.168.0.110"
MQTT_PORT = 1883
MQTT_TOPIC = "mace/telemetry/crypto_sword"

async def run_swarm_pipeline_for_asset(symbol):
    """
    Spawns scout.py and brain.py using UNIX pipes as async subprocesses.
    """
    scout_path = os.path.join(BASE_DIR, "crypto/swarm/scout.py")
    brain_path = os.path.join(BASE_DIR, "crypto/swarm/brain.py")

    # 1. Spawn scout (Data Agent)
    scout_proc = await asyncio.create_subprocess_exec(
        sys.executable, scout_path, symbol,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    scout_stdout, scout_stderr = await scout_proc.communicate()
    if scout_proc.returncode != 0:
        return {"symbol": symbol, "error": f"Scout failed: {scout_stderr.decode().strip()}"}

    # 2. Spawn brain (Math Agent) and pipe scout output into it
    brain_proc = await asyncio.create_subprocess_exec(
        sys.executable, brain_path,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    brain_stdout, brain_stderr = await brain_proc.communicate(input=scout_stdout)
    if brain_proc.returncode != 0:
        return {"symbol": symbol, "error": f"Brain failed: {brain_stderr.decode().strip()}"}

    try:
        brain_data = json.loads(brain_stdout.decode().strip())
        if "error" in brain_data:
            return {"symbol": symbol, "error": brain_data["error"]}
        return brain_data
    except Exception as e:
        return {"symbol": symbol, "error": f"Failed to parse brain output: {str(e)}"}

async def sem_pipeline(symbol, sem):
    async with sem:
        res = await run_swarm_pipeline_for_asset(symbol)
        await asyncio.sleep(0.2)  # Defensive rate-limit spacing
        return res

async def get_portfolio_context():
    """
    Fetches the total USDT balance (available cash) and the list of currently held symbols
    from KuCoin using CCXT, or returns mock values if API keys are missing.
    """
    api_key = os.environ.get("KUCOIN_API_KEY")
    secret_key = os.environ.get("KUCOIN_SECRET_KEY")
    password = os.environ.get("KUCOIN_PASSWORD")
    
    is_mock = not (api_key and secret_key)
    
    if is_mock:
        # Load state dynamically from SQLite virtual ledger
        available_cash, existing_positions = guardrail.get_portfolio_context_from_db()
        print(f"[*] Retrieved local DB portfolio context: {available_cash:.2f} USDT, holdings: {existing_positions}")
    else:
        available_cash = 10000.0  # Default fallback if fetch fails
        existing_positions = []
        exchange = ccxt.kucoin({
            'apiKey': api_key,
            'secret': secret_key,
            'password': password,
            'enableRateLimit': True,
        })
        try:
            balance = await exchange.fetch_balance()
            usdt_info = balance.get('USDT', {})
            available_cash = usdt_info.get('free') or usdt_info.get('total') or 10000.0
            
            # Identify held positions where value > $5
            for cur, total_bal in balance.get('total', {}).items():
                if total_bal > 0 and cur != 'USDT':
                    try:
                        symbol = f"{cur}/USDT"
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get('last', 0.0)
                        if total_bal * last_price > 5.0:
                            existing_positions.append(symbol)
                    except Exception:
                        pass
        except Exception as e:
            print(f"[!] Error fetching KuCoin live portfolio: {e}")
        finally:
            await exchange.close()
            
    return available_cash, existing_positions

async def process_global_risk(raw_signals, available_cash, active_positions):
    """
    Feeds the accumulated brain signals into the centralized portfolio allocator via a single pipe stream.
    """
    allocator_path = os.path.join(BASE_DIR, "crypto/swarm/portfolio_allocator.py")
    
    # Pack the global state metrics into the composite payload
    allocator_input = {
        "candidates": raw_signals,
        "available_cash": available_cash,
        "existing_positions": active_positions
    }
    
    # Launch portfolio allocator as a centralized pipeline process
    proc = await asyncio.create_subprocess_exec(
        sys.executable, allocator_path,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    stdout, stderr = await proc.communicate(input=json.dumps(allocator_input).encode('utf-8'))
    
    if proc.returncode != 0:
        print(f"[!] Portfolio Allocator process failed with code {proc.returncode}")
        return []
        
    try:
        risk_verdict = json.loads(stdout.decode('utf-8'))
        approved_trades = risk_verdict.get("approved_trades", [])
        
        # Inject target_size_usd as size_usd for backwards-compatibility
        for trade in approved_trades:
            trade["size_usd"] = trade.get("target_size_usd", 0.0)
            
        return approved_trades
    except Exception as e:
        print(f"[!] Critical: Failed to parse centralized risk response: {e}")
        return []

def push_telemetry(payload):
    print(f"\n=== CRYPTO SWARM TELEMETRY ===\n{json.dumps(payload, indent=2)}\n==============================\n")
    try:
        mqttc = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        
        # Support secure credentials for Home Assistant / local secure brokers
        user = os.environ.get("MQTT_USER")
        password = os.environ.get("MQTT_PASSWORD")
        if user and password:
            mqttc.username_pw_set(user, password)
            
        mqttc.connect(MQTT_BROKER, MQTT_PORT, 10)
        mqttc.loop_start()
        
        # Publish and wait for completion (timeout after 5 seconds to prevent blocking)
        info = mqttc.publish(MQTT_TOPIC, json.dumps(payload))
        info.wait_for_publish(timeout=5)
        
        mqttc.loop_stop()
        mqttc.disconnect()
        print("[+] Telemetry published successfully via MQTT.")
    except Exception as e:
        print(f"[!] MQTT Telemetry failed: {e}")

async def run_sweep(args):
    # Load Universe
    symbols = []
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    else:
        if os.path.exists(DEFAULT_UNIVERSE_PATH):
            try:
                with open(DEFAULT_UNIVERSE_PATH, "r") as f:
                    universe_list = json.load(f)
                    if args.limit:
                        symbols = universe_list[:args.limit]
                    else:
                        symbols = universe_list
            except Exception as e:
                print(f"[!] Error loading universe: {e}")
                symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
        else:
            print("[!] Universe file not found. Defaulting to major assets.")
            symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]

    print(f"[*] Starting async scanning of {len(symbols)} crypto assets...")

    # Run swarm pipelines concurrently
    sem = asyncio.Semaphore(3)  # Maximum 3 concurrent asset pipelines to protect rate limit
    tasks = [sem_pipeline(symbol, sem) for symbol in symbols]
    scan_results = await asyncio.gather(*tasks)

    # Process results (collect raw alpha signals)
    raw_signals = []
    errors = []
    scanned_count = 0

    for res in scan_results:
        if "error" in res:
            errors.append(res)
        else:
            scanned_count += 1
            raw_signals.append(res)

    print(f"[+] Scan completed. Scanned: {scanned_count}, Raw Alpha Candidates: {len(raw_signals)}, Errors: {len(errors)}")

    # Fetch live cash and existing positions to feed into global portfolio optimizer
    print("[*] Retrieving KuCoin portfolio context...")
    available_cash, active_positions = await get_portfolio_context()
    print(f"[+] Cash Available: {available_cash} USDT, Existing Holdings: {active_positions}")

    # Invoke the centralized global portfolio allocator
    print("[*] Running centralized global portfolio risk and sizing allocation...")
    approved_trades = await process_global_risk(raw_signals, available_cash, active_positions)
    print(f"[+] Allocation completed. Approved Trades: {len(approved_trades)}")

    # Sort approved candidates by signal strength in descending order
    approved_trades = sorted(approved_trades, key=lambda x: x.get("signal_strength", 0.0), reverse=True)

    execution_status = "No trades approved or candidates held cash."
    chosen_trade = None

    # Execute approved trades
    if approved_trades:
        # Compatibility choice: select the highest signal trade as "chosen_trade"
        top_asset = approved_trades[0]
        chosen_trade = {
            "symbol": top_asset["symbol"],
            "size_usd": top_asset["size_usd"],
            "signal_strength": top_asset["signal_strength"],
            "calculated_kelly": top_asset["calculated_kelly"]
        }

        # Order Submission Setup
        api_key = os.environ.get("KUCOIN_API_KEY")
        secret_key = os.environ.get("KUCOIN_SECRET_KEY")
        password = os.environ.get("KUCOIN_PASSWORD")
        is_live_execution = api_key and secret_key and not args.dry_run

        execution_statuses = []
        for trade in approved_trades:
            symbol = trade["symbol"]
            size_usd = trade["size_usd"]

            if is_live_execution:
                sandbox_label = " SANDBOX" if args.sandbox else ""
                print(f"[*] Placing{sandbox_label} market buy order for {symbol} of size {size_usd} USDT...")
                exchange = ccxt.kucoin({
                    'apiKey': api_key,
                    'secret': secret_key,
                    'password': password,
                    'enableRateLimit': True,
                })
                if args.sandbox:
                    exchange.set_sandbox_mode(True)
                try:
                    # Fetch price to determine quantity
                    ticker = await exchange.fetch_ticker(symbol)
                    last_price = ticker.get('last')
                    if last_price:
                        qty = size_usd / last_price
                        # Execute order
                        order = await exchange.create_market_buy_order(symbol, qty)
                        status = f"Executed {sandbox_label.strip() or 'LIVE'} market order for {symbol}. Order ID: {order.get('id')}"
                    else:
                        status = f"Failed {sandbox_label.strip() or 'live'} order execution for {symbol}: Price fetch failed"
                except Exception as e:
                    status = f"Failed {sandbox_label.strip() or 'live'} order execution for {symbol}: {str(e)}"
                finally:
                    await exchange.close()
            else:
                if args.dry_run:
                    status = f"Simulated order approved for {symbol} with size {size_usd} USDT (Dry Run Mode)"
                else:
                    # Stateful Paper Trading Mode using the local database wallet ledger
                    asset_price = trade.get("latest_price", 0.0)
                    if asset_price <= 0.0:
                        # Fallback to a placeholder price if brain didn't output one (unlikely)
                        asset_price = 1.0
                        
                    db_res = guardrail.execute_db_trade(symbol, "BUY", size_usd, asset_price)
                    if db_res.get("success"):
                        status = (
                            f"Paper executed BUY for {symbol} on {db_res['blockchain']}. "
                            f"Bought {db_res['quantity']:.4f} units at ${asset_price:.4f}. "
                            f"New balance: {db_res['new_balance']:.4f}, remaining cash: ${db_res['remaining_cash']:.2f} USDT. "
                            f"Gas fee: {db_res['gas_fee']}"
                        )
                    else:
                        status = f"Failed virtual ledger trade execution for {symbol}: {db_res.get('error')}"

            print(f"[+] {status}")
            execution_statuses.append(status)

        execution_status = "; ".join(execution_statuses)

    # Dispatch Telemetry
    telemetry_payload = {
        "timestamp": str(datetime.now()),
        "engine": "CRYPTO_SWARM",
        "scan_summary": {
            "total_scanned_count": scanned_count,
            "approved_count": len(approved_trades),
            "error_count": len(errors),
            "approved_candidates": [
                {"symbol": t["symbol"], "signal_strength": t["signal_strength"], "size_usd": t["size_usd"]}
                for t in approved_trades
            ]
        },
        "chosen_trade": chosen_trade,
        "execution_status": execution_status
    }

    push_telemetry(telemetry_payload)

async def main():
    parser = argparse.ArgumentParser(description="M.A.C.E. Crypto Swarm Router")
    parser.add_argument("--dry-run", action="store_true", help="Run without sending real trade orders")
    parser.add_argument("--sandbox", action="store_true", help="Enable KuCoin Sandbox mode for testing")
    parser.add_argument("--symbols", type=str, help="Comma-separated list of symbols to scan")
    parser.add_argument("--limit", type=int, help="Limit number of assets scanned from universe (scans entire universe by default)")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon mode")
    parser.add_argument("--interval", type=int, default=14400, help="Interval between scans in seconds in daemon mode (default: 14400s / 4h)")
    args = parser.parse_args()

    print("[*] Initiating M.A.C.E. Crypto Swarm Pipeline...")

    if args.daemon:
        print(f"[*] Starting M.A.C.E. Crypto Swarm in DAEMON mode (interval: {args.interval}s)...")
        while True:
            try:
                await run_sweep(args)
            except Exception as e:
                print(f"[!] Error during daemon run sweep: {e}")
            print(f"[*] Sleeping for {args.interval} seconds before next sweep...")
            await asyncio.sleep(args.interval)
    else:
        await run_sweep(args)

if __name__ == "__main__":
    asyncio.run(main())
