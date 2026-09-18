import sys
import json
import asyncio
import os
import argparse
from datetime import datetime, timedelta, timezone
from paho.mqtt import client as mqtt_client
from google.antigravity import Agent, LocalAgentConfig, types
from google.antigravity.hooks import hooks, policy
import sqlite3

def safe_json_dumps(obj):
    if obj is None:
        return None
    try:
        return json.dumps(obj)
    except TypeError:
        if hasattr(obj, "model_dump") and callable(obj.model_dump):
            try:
                return json.dumps(obj.model_dump())
            except Exception:
                pass
        elif hasattr(obj, "dict") and callable(obj.dict):
            try:
                return json.dumps(obj.dict())
            except Exception:
                pass
        if hasattr(obj, "__dict__"):
            try:
                return json.dumps(obj, default=lambda o: o.__dict__ if hasattr(o, "__dict__") else str(o))
            except Exception:
                pass
        try:
            return json.dumps(str(obj))
        except Exception:
            return '"unserializable"'

DEFAULT_DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config/portfolio.db"))

# Base Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from brokers import get_client_by_name

# MQTT Broker config
MQTT_BROKER = os.getenv("MQTT_BROKER_IP", "192.168.0.110")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "mace/telemetry/tradfi_sword"

async def run_swarm_pipeline_for_asset(symbol, source="static"):
    """
    Spawns scout.py and brain.py using UNIX pipes as async subprocesses.
    """
    scout_path = os.path.join(BASE_DIR, "equities/swarm/scout.py")
    brain_path = os.path.join(BASE_DIR, "equities/swarm/brain.py")

    # 1. Spawn scout (Data Agent)
    scout_proc = await asyncio.create_subprocess_exec(
        sys.executable, scout_path, symbol,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=os.environ
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
        brain_data["source"] = source
        return brain_data
    except Exception as e:
        return {"symbol": symbol, "error": f"Failed to parse brain output: {str(e)}"}

async def sem_pipeline(symbol, source, sem):
    async with sem:
        res = await run_swarm_pipeline_for_asset(symbol, source)
        await asyncio.sleep(0.2)  # Defensive rate-limit spacing
        return res

def load_tradfi_universe(db_path=DEFAULT_DB_PATH, limit=None):
    symbols = []
    sources = {}
    if os.path.exists(db_path):
        try:
            with sqlite3.connect(db_path, timeout=10.0) as conn:
                cursor = conn.cursor()
                query = "SELECT symbol, source FROM vw_equities_universe ORDER BY symbol"
                if limit:
                    query += f" LIMIT {int(limit)}"
                rows = cursor.execute(query).fetchall()
                symbols = [r[0] for r in rows]
                sources = {r[0]: r[1] for r in rows}
        except Exception as e:
            print(f"[!] Error querying vw_equities_universe from DB: {e}")
    if not symbols:
        print("[!] DB universe lookup empty. Defaulting to major assets.")
        symbols = ["SPY", "AAPL", "MSFT"]
        sources = {s: "static" for s in symbols}
    return symbols, sources

async def get_equities_portfolio_context():
    """
    Fetches the total equity, available cash, and existing positions using BrokerClient abstraction.
    """
    total_equity = 0.0
    available_cash = 0.0
    existing_positions = {}

    try:
        client = get_client_by_name("alpaca")
        acc = client.get_account()
        total_equity = float(acc.get("equity", 0.0))
        available_cash = float(acc.get("cash", 0.0))

        positions = client.get_positions()
        for pos in positions:
            symbol = pos.get("symbol")
            market_value = float(pos.get("market_value", 0.0))
            if symbol:
                existing_positions[symbol] = market_value

    except Exception as e:
        print(f"[!] Error fetching portfolio context via BrokerClient: {e}")

    return total_equity, available_cash, existing_positions

async def process_global_risk(raw_signals, total_equity, available_cash, active_positions):
    """
    Feeds the accumulated brain signals into the centralized portfolio allocator via a single pipe stream.
    """
    allocator_path = os.path.join(BASE_DIR, "equities/swarm/portfolio_allocator.py")

    allocator_input = {
        "candidates": raw_signals,
        "available_cash": available_cash,
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
        info = mqttc.publish(MQTT_TOPIC, json.dumps(payload), retain=True)
        info.wait_for_publish(timeout=5)
        mqttc.loop_stop()
        mqttc.disconnect()
    except Exception as e:
        print(f"[!] MQTT Telemetry failed: {e}")

def set_ctx_val(context, key, value):
    if hasattr(context, "set"):
        context.set(key, value)
    else:
        context.set_state(key, value)

def get_ctx_val(context, key, default=None):
    if hasattr(context, "get"):
        return context.get(key, default)
    else:
        return context.get_state(key, default)

class MACEPreToolCallHook(hooks.PreToolCallDecideHook):
    def __init__(self, run_id, db_path):
        self.run_id = run_id
        self.db_path = db_path

    async def run(self, context, data):
        if data.id:
            set_ctx_val(context, f"args_{data.id}", data.args)
            set_ctx_val(context, f"name_{data.id}", data.name)
        return types.HookResult(allow=True)

class MACEPostToolCallHook(hooks.PostToolCallHook):
    def __init__(self, run_id, db_path):
        self.run_id = run_id
        self.db_path = db_path

    async def run(self, context, data):
        args = get_ctx_val(context, f"args_{data.id}", {}) if data.id else {}
        symbol = args.get("symbol")
        
        # Determine if it is a buy or sell action
        action = None
        if "place_stock_order" in str(data.name):
            action = "BUY" if args.get("side") == "buy" else "SELL"
        elif "close_position" in str(data.name):
            action = "SELL"
            
        trade_id = None
        if symbol and action:
            try:
                with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT trade_id FROM mcp_requested_trades WHERE run_id = ? AND symbol = ? AND action = ? AND status = 'PENDING'",
                        (self.run_id, symbol, action)
                    )
                    row = cursor.fetchone()
                    if row:
                        trade_id = row[0]
            except Exception as e:
                print(f"[!] Failed to lookup trade_id for {symbol}: {e}")

        status_str = "SUCCESS" if (not data.error and "ORDER_REJECTED" not in str(data.result or "")) else "FAILED"
        try:
            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                conn.execute("""
                    INSERT INTO mcp_execution_log (run_id, trade_id, timestamp, tool_name, arguments, status, result, error)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    self.run_id,
                    trade_id,
                    datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    str(data.name),
                    safe_json_dumps(args),
                    status_str,
                    safe_json_dumps(data.result) if data.result is not None else None,
                    data.error
                ))
                if trade_id:
                    new_trade_status = "COMPLETED" if status_str == "SUCCESS" else "FAILED"
                    conn.execute(
                        "UPDATE mcp_requested_trades SET status = ?, updated_at = ? WHERE trade_id = ?",
                        (new_trade_status, datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'), trade_id)
                    )
                conn.commit()
        except Exception as e:
            print(f"[!] Failed to log tool execution: {e}")

class MACEToolErrorHook(hooks.OnToolErrorHook):
    def __init__(self, run_id, db_path):
        self.run_id = run_id
        self.db_path = db_path

    async def run(self, context, data):
        error_msg = str(data)
        try:
            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                conn.execute("""
                    INSERT INTO mcp_execution_log (run_id, timestamp, tool_name, arguments, status, error)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    self.run_id,
                    datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "UNHANDLED_EXCEPTION",
                    "{}",
                    "FAILED",
                    error_msg
                ))
                conn.commit()
        except Exception as e:
            print(f"[!] Failed to log tool execution exception: {e}")
        return None

def submit_direct_market_order(symbol, notional, side):
    """v1.1: Submits a notional market order DIRECTLY to the Alpaca paper API.
    Removes the LLM dispatch layer from order submission - sells previously went
    through a Gemini prompt referencing a tool (mcp_alpaca_close_position) that was
    never registered, so every liquidation silently failed."""
    import requests
    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
        "accept": "application/json",
        "content-type": "application/json"
    }
    url = "https://paper-api.alpaca.markets/v2/orders"
    payload = {
        "symbol": symbol,
        "notional": str(notional),
        "side": side,
        "type": "market",
        "time_in_force": "day"
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        if resp.status_code < 400 and '"status":"rejected"' not in resp.text.replace(" ", ""):
            return {"ok": True, "response": resp.text}
        return {"ok": False, "message": resp.text[:300]}
    except Exception as e:
        return {"ok": False, "message": str(e)}

async def execute_mcp_agent(system_prompt, user_message, run_id=None):
    """Helper function to handle Gemini MCP Agent execution and retries."""
    if not run_id:
        run_id = "tradfi_sweep_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")

    os.environ["ALPACA_API_KEY"] = api_key
    os.environ["ALPACA_SECRET_KEY"] = secret_key
    os.environ["ALPACA_PAPER_TRADE"] = "true"

    def mcp_alpaca_place_stock_order(symbol: str, notional: str, side: str, type: str, time_in_force: str) -> str:
        """Places a stock order on Alpaca."""
        import requests
        headers = {
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": secret_key,
            "accept": "application/json",
            "content-type": "application/json"
        }
        url = "https://paper-api.alpaca.markets/v2/orders"
        payload = {
            "symbol": symbol,
            "notional": str(notional),
            "side": side,
            "type": type,
            "time_in_force": time_in_force
        }
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            # v1.1: surface broker rejections (insufficient buying power, halted
            # symbols, etc.) so the execution log stops counting them as SUCCESS.
            if resp.status_code >= 400 or '"status":"rejected"' in resp.text.replace(" ", ""):
                return f"ORDER_REJECTED: {resp.text[:300]}"
            return resp.text
        except Exception as e:
            return str(e)

    pre_tool_hook = MACEPreToolCallHook(run_id, DEFAULT_DB_PATH)
    post_tool_hook = MACEPostToolCallHook(run_id, DEFAULT_DB_PATH)
    tool_error_hook = MACEToolErrorHook(run_id, DEFAULT_DB_PATH)

    config = LocalAgentConfig(
        model="gemini-2.5-flash", # Upgraded from Pro to save API quota
        system_instructions=system_prompt,
        tools=[mcp_alpaca_place_stock_order],
        policies=[policy.allow_all()],
        hooks=[pre_tool_hook, post_tool_hook, tool_error_hook],
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
    sources = {}
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        sources = {s: "static" for s in symbols}
    else:
        symbols, sources = load_tradfi_universe(DEFAULT_DB_PATH, args.limit)

    print(f"[*] Starting async scanning of {len(symbols)} equities assets...")

    sem = asyncio.Semaphore(3)
    tasks = [sem_pipeline(symbol, sources.get(symbol, "static"), sem) for symbol in symbols]
    scan_results = await asyncio.gather(*tasks)

    raw_signals = []
    errors = []
    scanned_count = 0

    for res in scan_results:
        if "error" in res:
            print(f"[!] Worker Error for symbol {res.get('symbol')}: {res['error']}")
            errors.append(res)
        else:
            scanned_count += 1
            raw_signals.append(res)

    print(f"[+] Scan completed. Scanned: {scanned_count}, Raw Alpha Candidates: {len(raw_signals)}, Errors: {len(errors)}")

    print("[*] Retrieving Alpaca portfolio context...")
    total_equity, available_cash, active_positions = await get_equities_portfolio_context()
    print(f"[+] Cash Available: {available_cash} USD, Total Equity: {total_equity} USD, Existing Holdings/Orders: {active_positions}")

    print("[*] Running centralized global portfolio risk and sizing allocation...")
    approved_trades, sell_orders = await process_global_risk(raw_signals, total_equity, available_cash, active_positions)
    print(f"[+] Allocation completed. Approved Trades: {len(approved_trades)}, Sell Orders: {len(sell_orders)}")

    approved_trades = sorted(approved_trades, key=lambda x: x.get("signal_strength", 0.0), reverse=True)

    execution_status = "No trades approved or candidates held cash."
    chosen_trade = None

    # SECURE FIX: Check for keys safely without hardcoding fallbacks
    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")
    is_live_execution = bool(api_key and secret_key and not args.dry_run)

    if is_live_execution:
        run_id = f"tradfi_sweep_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        print(f"[*] Starting live execution run: {run_id}")

        # v1.1: expire stale PENDING requests from previous runs (>1h old). A jammed
        # queue of 3,968 never-resolved rows was observed; stale entries both distort
        # the trade log and risk surprise-execution if re-dispatched.
        try:
            with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
                cutoff = (datetime.utcnow() - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
                conn.execute(
                    "UPDATE mcp_requested_trades SET status = 'EXPIRED', updated_at = ? WHERE status = 'PENDING' AND updated_at < ?",
                    (datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'), cutoff)
                )
                conn.commit()
        except Exception as e:
            print(f"[!] Stale PENDING expiry sweep failed: {e}")
        
        # Log intended trades as 'PENDING'
        try:
            with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
                for s in sell_orders:
                    conn.execute(
                        "INSERT INTO mcp_requested_trades (run_id, symbol, action, status, updated_at) VALUES (?, ?, ?, ?, ?)",
                        (run_id, s["symbol"], "SELL", "PENDING", datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))
                    )
                for t in approved_trades:
                    conn.execute(
                        "INSERT INTO mcp_requested_trades (run_id, symbol, action, amount_usd, status, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (run_id, t["symbol"], "BUY", t["size_usd"], "PENDING", datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))
                    )
                conn.commit()
        except Exception as e:
            print(f"[!] Failed to log initial trade requests to DB: {e}")

        # ==========================================
        # 1. EXECUTE SELLS FIRST (v1.1: DIRECT BROKER PATH - no LLM dispatch)
        #    The original prompt told Gemini to call `mcp_alpaca_close_position`, a
        #    tool that was never registered (only mcp_alpaca_place_stock_order
        #    exists) - every liquidation silently failed. Sells now execute via the
        #    same BrokerClient.close_position path the risk shield uses (proven:
        #    261 shield-driven sell fills vs 0 orchestrator-driven sells in 2 weeks).
        # ==========================================
        if sell_orders:
            from brokers import get_client_for_symbol
            for s in sell_orders:
                symbol = s["symbol"]
                action = s.get("action", "SELL")
                try:
                    if action == "TRIM_PROFIT_TAKING":
                        trim_usd = float(s.get("trim_amount_usd", 0.0))
                        receipt = submit_direct_market_order(symbol, trim_usd, "sell")
                        ok = receipt.get("ok", False)
                        print(f"[+] TRIM SELL {symbol} ${trim_usd:.2f}: {'submitted' if ok else 'REJECTED: ' + str(receipt.get('message'))}")
                    else:
                        # BEAR_REGIME_LIQUIDATION and any other risk-off sell: full exit
                        client = get_client_for_symbol(symbol)
                        await asyncio.to_thread(client.close_position, symbol)
                        ok = True
                        print(f"[+] LIQUIDATION SELL {symbol} ({action}): dispatched via BrokerClient.close_position")
                    try:
                        with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
                            conn.execute(
                                "UPDATE mcp_requested_trades SET status = ?, updated_at = ? WHERE run_id = ? AND symbol = ? AND action = 'SELL'",
                                ("COMPLETED" if ok else "FAILED", datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'), run_id, symbol)
                            )
                            conn.commit()
                    except Exception as db_err:
                        print(f"[!] Sell status DB update failed for {symbol}: {db_err}")
                except Exception as sell_err:
                    print(f"[!] DIRECT SELL FAILED for {symbol}: {sell_err}")

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
            execution_status = await execute_mcp_agent(buy_prompt, "Place the approved stock orders via Alpaca MCP.", run_id)

        # ==========================================
        # 3. RECOVERY LOOP FOR INCOMPLETE TRADES
        # ==========================================
        for attempt in range(1, 4):
            pending_trades = []
            try:
                with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT trade_id, symbol, action, amount_usd FROM mcp_requested_trades WHERE run_id = ? AND status != 'COMPLETED'",
                        (run_id,)
                    )
                    pending_trades = cursor.fetchall()
            except Exception as e:
                print(f"[!] Recovery database query failed: {e}")
                break

            if not pending_trades:
                print("[+] All requested trades completed successfully. No recovery needed.")
                break

            print(f"[!] Recovery Attempt {attempt}/3: Found {len(pending_trades)} incomplete/failed trades.")
            
            retry_sells = [t for t in pending_trades if t[2] == "SELL"]
            retry_buys = [t for t in pending_trades if t[2] == "BUY"]

            recovery_prompt = (
                f"You are an autonomous trade execution recovery terminal. The previous execution failed or was half-filled.\n"
                f"You MUST retry executing only the remaining incomplete orders listed below.\n"
            )
            
            if retry_sells:
                # v1.1: recovery sells go through the direct broker path too
                from brokers import get_client_for_symbol
                for t in retry_sells:
                    try:
                        client = get_client_for_symbol(t[1])
                        await asyncio.to_thread(client.close_position, t[1])
                        print(f"[+] Recovery LIQUIDATION SELL {t[1]} dispatched via BrokerClient.close_position")
                        with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
                            conn.execute(
                                "UPDATE mcp_requested_trades SET status = 'COMPLETED', updated_at = ? WHERE trade_id = ?",
                                (datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'), t[0])
                            )
                            conn.commit()
                    except Exception as sell_err:
                        print(f"[!] Recovery sell failed for {t[1]}: {sell_err}")
            
            if retry_buys:
                buys_desc = "\n".join([f"- Symbol: '{t[1]}', Size: {t[3]} USD" for t in retry_buys])
                recovery_prompt += f"\nBUY ORDERS TO RETRY:\n{buys_desc}\nCall `mcp_alpaca_place_stock_order` tool (side='buy', type='market', time_in_force='day', notional=size) for each symbol."

            recovery_prompt += "\n\nExecute the recovery tool calls now."
            
            print(f"[*] Dispatching recovery attempt {attempt} to agent...")
            recovery_status = await execute_mcp_agent(recovery_prompt, f"Retry the incomplete trades for run {run_id}", run_id)
            print(f"[+] Recovery execution status: {recovery_status}")
            await asyncio.sleep(5)

        # Extract final status summary
        try:
            with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT symbol, action, status FROM mcp_requested_trades WHERE run_id = ?", (run_id,))
                final_rows = cursor.fetchall()
                summary_list = [f"{r[1]} {r[0]} ({r[2]})" for r in final_rows]
                execution_status = "; ".join(summary_list) if summary_list else "No trades requested."
        except Exception as e:
            execution_status = f"Completed run {run_id} but failed to extract final statuses: {e}"

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
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
