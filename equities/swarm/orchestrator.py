import sys
import json
import asyncio
import os
import argparse
from datetime import datetime
from paho.mqtt import client as mqtt_client
from google.antigravity import Agent, LocalAgentConfig, types

# Base Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_UNIVERSE_PATH = os.path.join(BASE_DIR, "config/tradfi_universe.json")

# MQTT Broker config
MQTT_BROKER = "192.168.0.110"
MQTT_PORT = 1883
MQTT_TOPIC = "mace/telemetry/tradfi_sword"

async def run_swarm_pipeline_for_asset(symbol):
    """
    Spawns scout.py and brain.py using UNIX pipes as async subprocesses.
    """
    scout_path = os.path.join(BASE_DIR, "equities/swarm/scout.py")
    brain_path = os.path.join(BASE_DIR, "equities/swarm/brain.py")

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

async def get_equities_portfolio_context():
    """
    Fetches the total equity (available cash/buying power) and the list of currently held symbols
    from Alpaca using requests, or returns mock values if API keys are missing.
    """
    import requests
    
    api_key = os.environ.get("ALPACA_API_KEY", "PKPQBCDBNXHU4XUXRDUB7AFIEF")
    secret_key = os.environ.get("ALPACA_SECRET_KEY", "3GZDRF2b1fxKrmvNLp9ZdQRo6rceDw4KFve9W9kYS1R9")
    paper_trade = os.environ.get("ALPACA_PAPER_TRADE", "true").lower() == "true"
    
    base_url = "https://paper-api.alpaca.markets" if paper_trade else "https://api.alpaca.markets"
    
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key
    }
    
    total_equity = 100000.0  # Default mock equity
    existing_positions = []
    
    try:
        # Check Account Info for Equity
        acc_response = requests.get(f"{base_url}/v2/account", headers=headers, timeout=10)
        if acc_response.status_code == 200:
            acc_data = acc_response.json()
            total_equity = float(acc_data.get("equity", 100000.0))
        
        # Check Positions
        pos_response = requests.get(f"{base_url}/v2/positions", headers=headers, timeout=10)
        if pos_response.status_code == 200:
            pos_data = pos_response.json()
            for pos in pos_data:
                symbol = pos.get("symbol")
                if symbol:
                    existing_positions.append(symbol)
                    
        # Check Open Orders
        ord_response = requests.get(f"{base_url}/v2/orders?status=open", headers=headers, timeout=10)
        if ord_response.status_code == 200:
            ord_data = ord_response.json()
            for order in ord_data:
                symbol = order.get("symbol")
                if symbol and symbol not in existing_positions:
                    existing_positions.append(symbol)
                    
    except Exception as e:
        print(f"[!] Error fetching Alpaca portfolio context: {e}")
        
    return total_equity, existing_positions

async def process_global_risk(raw_signals, available_cash, active_positions):
    """
    Feeds the accumulated brain signals into the centralized portfolio allocator via a single pipe stream.
    """
    allocator_path = os.path.join(BASE_DIR, "equities/swarm/portfolio_allocator.py")
    
    # Pack the global state metrics into the composite payload
    allocator_input = {
        "candidates": raw_signals,
        "available_cash": available_cash,
        "total_equity": total_equity, # FIX: Pass total portfolio value for true risk caps
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
        sell_orders = risk_verdict.get("sell_orders", []) # FIX: Catch sell orders

        for trade in approved_trades:
            trade["size_usd"] = trade.get("target_size_usd", 0.0)

        return approved_trades, sell_orders # FIX: Return both
    except Exception as e:
        print(f"[!] Critical: Failed to parse centralized risk response: {e}")
        return []

def push_telemetry(payload):
    print(f"\n=== EQUITIES SWARM TELEMETRY ===\n{json.dumps(payload, indent=2)}\n==============================\n")
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
                symbols = ["SPY", "AAPL", "MSFT"]
        else:
            print("[!] Universe file not found. Defaulting to major assets.")
            symbols = ["SPY", "AAPL", "MSFT"]

    print(f"[*] Starting async scanning of {len(symbols)} equities assets...")

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
    print("[*] Retrieving Alpaca portfolio context...")
    available_cash, active_positions = await get_equities_portfolio_context()
    print(f"[+] Cash/Equity Available: {available_cash} USD, Existing Holdings/Orders: {active_positions}")

    # Invoke the centralized global portfolio allocator
    print("[*] Running centralized global portfolio risk and sizing allocation...")
    approved_trades, sell_orders = await process_global_risk(raw_signals, available_cash, active_positions)
    print(f"[+] Allocation completed. Approved Trades: {len(approved_trades)}")

    # Sort approved candidates by signal strength in descending order
    approved_trades = sorted(approved_trades, key=lambda x: x.get("signal_strength", 0.0), reverse=True)

    execution_status = "No trades approved or candidates held cash."
    chosen_trade = None

    # FIX: Execute Sell Orders via Gemini MCP if Bear regimes detected
    if sell_orders and is_live_execution:
        sells_description = "\n".join([f"- {s['symbol']}: {s['reason']}" for s in sell_orders])
        sell_prompt = (
            f"You are M.A.C.E. risk manager.\n"
            f"The swarm has detected high-risk Bear regimes. Liquidate these positions immediately:\n{sells_description}\n\n"
            f"Call the `mcp_alpaca_place_stock_order` tool for each symbol with side: 'sell', type: 'market', notional: 0 (to close full position)."
        )
        # (You will need to instantiate a new Agent block here similar to your buy block, passing sell_prompt instead of system_prompt)
        print(f"[!!!] WARNING: Dispatching SELL ORDERS to Gemini MCP:\n{sells_description}")

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
        api_key = os.environ.get("ALPACA_API_KEY", "PKPQBCDBNXHU4XUXRDUB7AFIEF")
        secret_key = os.environ.get("ALPACA_SECRET_KEY", "3GZDRF2b1fxKrmvNLp9ZdQRo6rceDw4KFve9W9kYS1R9")
        is_live_execution = api_key and secret_key and not args.dry_run

        if is_live_execution:
            trades_description = "\n".join([
                f"- Symbol: '{t['symbol']}', Size: {t['size_usd']} USD"
                for t in approved_trades
            ])
            print(f"[*] Placing REAL market buy orders via Alpaca MCP Agent for:\n{trades_description}...")
            
            # Setup environment variables for Alpaca MCP server
            os.environ["ALPACA_API_KEY"] = api_key
            os.environ["ALPACA_SECRET_KEY"] = secret_key
            os.environ["ALPACA_PAPER_TRADE"] = "true"
            os.environ["GEMINI_API_KEY"] = "AIzaSyCQaUmUG74HEHD2AUCdVwOJdFp3lDrEfFs"

            system_prompt = (
                f"You are M.A.C.E., an autonomous quantitative allocator.\n"
                f"The equities swarm pipeline has determined the following approved trades and sizes:\n"
                f"{trades_description}\n\n"
                f"YOUR EXPLICIT DIRECTIVES:\n"
                f"1. Submit market orders for ALL approved assets by calling the `mcp_alpaca_place_stock_order` tool for each asset with parameters:\n"
                f"   - symbol: <symbol>\n"
                f"   - notional: <size_usd>\n"
                f"   - side: 'buy'\n"
                f"   - type: 'market'\n"
                f"   - time_in_force: 'day'\n\n"
                f"Return a clean JSON summarizing the chosen assets, direction, exact dollar sizes, and execution status."
            )

            # Check where uvx is located
            uvx_cmd = "/usr/local/bin/uvx" if os.path.exists("/usr/local/bin/uvx") else "uvx"

            # Configure stdio MCP server for Alpaca using env wrapper to guarantee key propagation
            mcp_servers = [
                types.McpStdioServer(
                    name="alpaca",
                    command="/usr/bin/env",
                    args=[
                        f"ALPACA_API_KEY={api_key}",
                        f"ALPACA_SECRET_KEY={secret_key}",
                        "ALPACA_PAPER_TRADE=true",
                        uvx_cmd,
                        "alpaca-mcp-server",
                    ],
                )
            ]

            config = LocalAgentConfig(
                model="gemini-2.5-pro",
                system_instructions=system_prompt,
                mcp_servers=mcp_servers,
            )

            print("[*] Handing control to Google Antigravity Agent (Gemini 2.5 Pro)...")

            try:
                async with Agent(config=config) as agent:
                    max_retries = 3
                    retry_delay = 10
                    response = None

                    for attempt in range(max_retries):
                        try:
                            response = await agent.chat("Place the approved stock orders via Alpaca MCP.")
                            break
                        except Exception as e:
                            error_str = str(e)
                            if ("503" in error_str or "429" in error_str) and attempt < max_retries - 1:
                                print(f"[!] API issue. Retrying in {retry_delay}s... (Attempt {attempt + 1}/{max_retries})")
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2
                            else:
                                raise e

                    if response:
                        try:
                            text_content = await response.text()
                            if text_content and text_content.strip():
                                execution_status = text_content
                            else:
                                execution_status = "Alpaca MCP trade execution completed successfully."
                        except Exception as e:
                            execution_status = f"Alpaca MCP trade executed but failed to retrieve text: {str(e)}"
                    else:
                        execution_status = "No response generated by the agent."
            except Exception as e:
                execution_status = f"Failed live order execution: {str(e)}"
        else:
            simulated_statuses = [
                f"Simulated order approved for {t['symbol']} with size ${t['size_usd']} USD (Dry Run Mode)"
                for t in approved_trades
            ]
            execution_status = "; ".join(simulated_statuses)

    # Dispatch Telemetry
    telemetry_payload = {
        "timestamp": str(datetime.now()),
        "engine": "EQUITIES_SWARM",
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
    parser = argparse.ArgumentParser(description="M.A.C.E. Equities Swarm Router")
    parser.add_argument("--dry-run", action="store_true", help="Run without sending real trade orders")
    parser.add_argument("--symbols", type=str, help="Comma-separated list of symbols to scan")
    parser.add_argument("--limit", type=int, help="Limit number of assets scanned from universe (scans entire universe by default)")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon mode")
    parser.add_argument("--interval", type=int, default=3600, help="Interval between scans in seconds in daemon mode (default: 3600s / 1h)")
    args = parser.parse_args()

    print("[*] Initiating M.A.C.E. Equities Swarm Pipeline...")

    if args.daemon:
        print(f"[*] Starting M.A.C.E. Equities Swarm in DAEMON mode (interval: {args.interval}s)...")
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
