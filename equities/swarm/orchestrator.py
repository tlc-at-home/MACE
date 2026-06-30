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
    Fetches the total equity and existing positions from Alpaca.
    """
    import requests

    # SECURE FIX: Removed hardcoded fallback keys. Relies entirely on mace.env via Systemd.
    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")
    paper_trade = os.environ.get("ALPACA_PAPER_TRADE", "true").lower() == "true"

    if not api_key or not secret_key:
        print("[!] Critical: ALPACA_API_KEY or ALPACA_SECRET_KEY not found in environment.")
        return 0.0, []

    base_url = "https://paper-api.alpaca.markets" if paper_trade else "https://api.alpaca.markets"
    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key}

    total_equity = 0.0
    existing_positions = []

    try:
        acc_response = requests.get(f"{base_url}/v2/account", headers=headers, timeout=10)
        if acc_response.status_code == 200:
            acc_data = acc_response.json()
            total_equity = float(acc_data.get("equity", 0.0))

        pos_response = requests.get(f"{base_url}/v2/positions", headers=headers, timeout=10)
        if pos_response.status_code == 200:
            for pos in pos_response.json():
                symbol = pos.get("symbol")
                if symbol:
                    existing_positions.append(symbol)

        ord_response = requests.get(f"{base_url}/v2/orders?status=open", headers=headers, timeout=10)
        if ord_response.status_code == 200:
            for order in ord_response.json():
                symbol = order.get("symbol")
                if symbol and symbol not in existing_positions:
                    existing_positions.append(symbol)

    except Exception as e:
        print(f"[!] Error fetching Alpaca portfolio context: {e}")

    return total_equity, existing_positions

async def process_global_risk(raw_signals, total_equity, active_positions):
    """
    Feeds the accumulated brain signals into the centralized portfolio allocator via a single pipe stream.
    """
    allocator_path = os.path.join(BASE_DIR, "equities/swarm/portfolio_allocator.py")

    # FIX: Map variables correctly for the downstream allocator
    allocator_input = {
        "candidates": raw_signals,
        "available_cash": total_equity,
        "total_equity": total_equity,
        "existing_positions": active_positions
    }

    proc = await asyncio.create_subprocess_exec(
        sys.executable, allocator_path,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate(input=json.dumps(allocator_input).encode('utf-8'))

    if proc.returncode != 0:
        print(f"[!] Portfolio Allocator process failed with code {proc.returncode}: {stderr.decode().strip()}")
        [], []

    try:
        risk_verdict = json.loads(stdout.decode('utf-8'))
        approved_trades = risk_verdict.get("approved_trades", [])
        sell_orders = risk_verdict.get("sell_orders", [])

        for trade in approved_trades:
            trade["size_usd"] = trade.get("target_size_usd", 0.0)

        return approved_trades, sell_orders
    except Exception as e:
        print(f"[!] Critical: Failed to parse centralized risk response: {e}")
        return [], []

def push_telemetry(payload):
    print(f"\n=== EQUITIES SWARM TELEMETRY ===\n{json.dumps(payload, indent=2)}\n==============================\n")
    try:
        mqttc = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        user = os.environ.get("MQTT_USER")
        password = os.environ.get("MQTT_PASSWORD")
        if user and password:
            mqttc.username_pw_set(user, password)
        mqttc.connect(MQTT_BROKER, MQTT_PORT, 10)
        mqttc.loop_start()
        info = mqttc.publish(MQTT_TOPIC, json.dumps(payload))
        info.wait_for_publish(timeout=5)
        mqttc.loop_stop()
        mqttc.disconnect()
    except Exception as e:
        print(f"[!] MQTT Telemetry failed: {e}")

async def execute_mcp_agent(system_prompt, user_message):
    """Helper function to handle Gemini MCP Agent execution and retries."""
    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")

    os.environ["ALPACA_API_KEY"] = api_key
    os.environ["ALPACA_SECRET_KEY"] = secret_key
    os.environ["ALPACA_PAPER_TRADE"] = "true"

    uvx_cmd = "/usr/local/bin/uvx" if os.path.exists("/usr/local/bin/uvx") else "uvx"

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
        model="gemini-2.5-flash", # Upgraded from Pro to save API quota
        system_instructions=system_prompt,
        mcp_servers=mcp_servers,
    )

    try:
        async with Agent(config=config) as agent:
            max_retries = 3
            retry_delay = 10
            response = None

            for attempt in range(max_retries):
                try:
                    response = await agent.chat(user_message)
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
                    return text_content if text_content and text_content.strip() else "MCP execution completed successfully."
                except Exception:
                    return "MCP execution completed but failed to retrieve text."
            else:
                return "No response generated by the agent."
    except Exception as e:
        return f"Failed MCP execution: {str(e)}"

async def run_sweep(args):
    symbols = []
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    else:
        if os.path.exists(DEFAULT_UNIVERSE_PATH):
            try:
                with open(DEFAULT_UNIVERSE_PATH, "r") as f:
                    universe_list = json.load(f)
                    symbols = universe_list[:args.limit] if args.limit else universe_list
            except Exception as e:
                print(f"[!] Error loading universe: {e}")
                symbols = ["SPY", "AAPL", "MSFT"]
        else:
            print("[!] Universe file not found. Defaulting to major assets.")
            symbols = ["SPY", "AAPL", "MSFT"]

    print(f"[*] Starting async scanning of {len(symbols)} equities assets...")

    sem = asyncio.Semaphore(3)
    tasks = [sem_pipeline(symbol, sem) for symbol in symbols]
    scan_results = await asyncio.gather(*tasks)

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

    print("[*] Retrieving Alpaca portfolio context...")
    total_equity, active_positions = await get_equities_portfolio_context()
    available_cash = total_equity
    print(f"[+] Cash/Equity Available: {available_cash} USD, Existing Holdings/Orders: {active_positions}")

    print("[*] Running centralized global portfolio risk and sizing allocation...")
    approved_trades, sell_orders = await process_global_risk(raw_signals, total_equity, active_positions)
    print(f"[+] Allocation completed. Approved Trades: {len(approved_trades)}, Sell Orders: {len(sell_orders)}")

    approved_trades = sorted(approved_trades, key=lambda x: x.get("signal_strength", 0.0), reverse=True)

    execution_status = "No trades approved or candidates held cash."
    chosen_trade = None

    # SECURE FIX: Check for keys safely without hardcoding fallbacks
    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")
    is_live_execution = bool(api_key and secret_key and not args.dry_run)

    if is_live_execution:
        # ==========================================
        # 1. EXECUTE SELLS FIRST
        # ==========================================
        if sell_orders:
            sells_description = "\n".join([f"- {s['symbol']}: {s['reason']}" for s in sell_orders])
            sell_prompt = (
                f"You are M.A.C.E. risk manager.\n"
                f"The swarm has detected high-risk Bear regimes. Liquidate these positions immediately:\n{sells_description}\n\n"
                f"Call the `mcp_alpaca_close_position` tool for each symbol to close the full position."
            )
            print(f"[!!!] WARNING: Dispatching SELL ORDERS to Gemini MCP:\n{sells_description}")
            sell_result = await execute_mcp_agent(sell_prompt, "Execute the sell orders now.")
            print(f"[+] Sell Order Result: {sell_result}")

        # ==========================================
        # 2. EXECUTE BUYS SECOND
        # ==========================================
        if approved_trades:
            top_asset = approved_trades[0]
            chosen_trade = {
                "symbol": top_asset["symbol"],
                "size_usd": top_asset["size_usd"],
                "signal_strength": top_asset["signal_strength"],
                "calculated_kelly": top_asset["calculated_kelly"]
            }

            trades_description = "\n".join([
                f"- Symbol: '{t['symbol']}', Size: {t['size_usd']} USD"
                for t in approved_trades
            ])
            print(f"[*] Placing REAL market buy orders via Alpaca MCP Agent for:\n{trades_description}...")
            buy_prompt = (
                f"You are an autonomous trade execution terminal. You must execute trades by calling tools, NOT by writing text.\n"
                f"Take the following list of trades and call the `mcp_alpaca_place_stock_order` tool EXACTLY ONCE for each trade.\n"
                f"Do NOT output a JSON list or summarize the trades before calling the tools. Just call the tools one after another.\n"
                f"Parameters for each tool call: symbol, notional (use the size_usd provided), side: 'buy', type: 'market', time_in_force: 'day'.\n\n"
                f"TRADES TO EXECUTE:\n{trades_description}\n\n"
                f"Execute the tools now."
            )

            print("[*] Handing control to Google Antigravity Agent (Gemini 2.5 Flash)...")
            execution_status = await execute_mcp_agent(buy_prompt, "Place the approved stock orders via Alpaca MCP.")

    else:
        # Dry Run Mode Handling
        dry_statuses = []
        if sell_orders:
            dry_statuses.extend([f"Simulated SELL for {s['symbol']} (Dry Run Mode)" for s in sell_orders])
        if approved_trades:
            dry_statuses.extend([f"Simulated BUY for {t['symbol']} with size ${t['size_usd']} USD (Dry Run Mode)" for t in approved_trades])
        execution_status = "; ".join(dry_statuses) if dry_statuses else "No trades generated."

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
    parser.add_argument("--limit", type=int, help="Limit number of assets scanned from universe")
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
                print(f"[!] Unhandled error during daemon run sweep: {e}")
            print(f"[*] Sleeping for {args.interval} seconds before next sweep...")
            await asyncio.sleep(args.interval)
    else:
        await run_sweep(args)

if __name__ == "__main__":
    asyncio.run(main())
