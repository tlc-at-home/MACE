#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Architecture
Component: TradFi Qualitative News Guard (tradfi_news_guard.py)

Role: Asynchronous Qualitative Risk Agent.
      - Fetches currently held positions via Alpaca REST.
      - Pulls recent news headlines for held assets.
      - Uses Gemini 2.5 Flash via MCP to evaluate text for existential threats.
      - Fires emergency liquidations if severe negative sentiment is detected.
"""

import os
import json
import asyncio
import logging
import requests
import argparse
from datetime import datetime
from paho.mqtt import client as mqtt_client
from google.antigravity import Agent, LocalAgentConfig, types
from google.antigravity.hooks import hooks
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

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

MQTT_BROKER_IP = os.getenv("MQTT_BROKER_IP", "192.168.0.110")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "mace/telemetry/tradfi_news_guard"

# STRICT QUALITATIVE RISK PROMPT
SYSTEM_PROMPT = (
    "You are M.A.C.E. NEWS GUARD, an autonomous qualitative risk analyst.\n"
    "You will be provided with a list of currently held stock positions and the latest news headlines for those stocks.\n\n"
    "EXPLICIT DIRECTIVES:\n"
    "1. ANALYZE: Read the news headlines carefully. Evaluate the contextual severity.\n"
    "2. THRESHOLD: Do NOT react to normal market volatility, minor price drops, or standard analyst downgrades.\n"
    "3. TRIGGER: ONLY trigger a liquidation if the news implies an EXISTENTIAL THREAT to the company. "
    "Examples of existential threats: SEC fraud investigations, bankruptcy filings, massive catastrophic product failures, "
    "CEO arrests, or severe regulatory crackdowns that threaten the company's ability to operate.\n"
    "4. ACTION: If an existential threat is detected for a symbol, call the `mcp_alpaca_close_position` tool with the `symbol` parameter to liquidate it immediately.\n"
    "5. REPORT: Return a JSON summary of what you analyzed and what actions you took. If no threats were found, return: {\"status\": \"safe\", \"details\": \"No existential threats detected in recent news.\"}"
)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.news_guard")

# ==============================================================================
# 2. DATA FETCHING (Pure Python - No LLM needed here)
# ==============================================================================
def get_alpaca_context():
    """Fetches current positions and their latest news via standard Alpaca REST API."""
    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")
    paper_trade = os.environ.get("ALPACA_PAPER_TRADE", "true").lower() == "true"

    base_url = "https://paper-api.alpaca.markets" if paper_trade else "https://api.alpaca.markets"
    data_url = "https://data.alpaca.markets"
    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key}

    if not api_key or not secret_key:
        logger.error("[-] Alpaca API keys missing.")
        return None

    try:
        # 1. Get Positions
        pos_resp = requests.get(f"{base_url}/v2/positions", headers=headers, timeout=10)
        if pos_resp.status_code != 200 or not pos_resp.json():
            logger.info("[*] No open positions found. Aborting news check.")
            return None

        positions = pos_resp.json()
        symbols = [pos["symbol"] for pos in positions]

        # 2. Get Latest News for those symbols
        news_resp = requests.get(
            f"{data_url}/v1/news",
            headers=headers,
            params={"symbols": ",".join(symbols), "limit": 10, "sort": "desc"},
            timeout=10
        )

        news_headlines = []
        if news_resp.status_code == 200:
            for article in news_resp.json():
                news_headlines.append(f"[{article.get('symbol', 'UNKNOWN')}] {article.get('headline', '')} - {article.get('summary', '')[:100]}")

        return {
            "positions": [pos["symbol"] for pos in positions],
            "news": news_headlines if news_headlines else ["No recent news found."]
        }

    except Exception as e:
        logger.error(f"[-] Failed to fetch Alpaca context/news: {e}")
        return None

# ==============================================================================
# 3. GEMINI MCP EXECUTION
# ==============================================================================
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
        action = "SELL" if "close_position" in str(data.name) else None
        
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
                    else:
                        cursor.execute(
                            "INSERT INTO mcp_requested_trades (run_id, symbol, action, status, updated_at) VALUES (?, ?, ?, ?, ?)",
                            (self.run_id, symbol, action, "PENDING", datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))
                        )
                        trade_id = cursor.lastrowid
                        conn.commit()
            except Exception as e:
                print(f"[!] Failed to insert/lookup trade request for {symbol}: {e}")

        status_str = "SUCCESS" if not data.error else "FAILED"
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

async def run_qualitative_audit():
    logger.info("[*] Booting M.A.C.E. Qualitative News Guard...")
    run_id = f"news_guard_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"[*] Starting News Guard run: {run_id}")

    context_data = get_alpaca_context()
    if not context_data:
        return json.dumps({"status": "idle", "reason": "No positions or failed to fetch data."})

    user_prompt = (
        f"Currently Held Positions: {json.dumps(context_data['positions'])}\n\n"
        f"Latest News Headlines:\n{chr(10).join(context_data['news'])}"
    )

    os.environ["ALPACA_PAPER_TRADE"] = "true"
    uvx_cmd = "/usr/local/bin/uvx" if os.path.exists("/usr/local/bin/uvx") else "uvx"

    mcp_servers = [
        types.McpStdioServer(
            name="alpaca",
            command="/usr/bin/env",
            args=[
                f"ALPACA_API_KEY={os.environ.get('ALPACA_API_KEY')}",
                f"ALPACA_SECRET_KEY={os.environ.get('ALPACA_SECRET_KEY')}",
                "ALPACA_PAPER_TRADE=true",
                uvx_cmd,
                "alpaca-mcp-server",
            ],
        )
    ]

    pre_tool_hook = MACEPreToolCallHook(run_id, DEFAULT_DB_PATH)
    post_tool_hook = MACEPostToolCallHook(run_id, DEFAULT_DB_PATH)
    tool_error_hook = MACEToolErrorHook(run_id, DEFAULT_DB_PATH)

    config = LocalAgentConfig(
        model="gemini-2.5-flash",
        system_instructions=SYSTEM_PROMPT,
        mcp_servers=mcp_servers,
        hooks=[pre_tool_hook, post_tool_hook, tool_error_hook],
    )

    logger.info("[*] Handing news context to Gemini 2.5 Flash for qualitative analysis...")

    response_text = ""
    async with Agent(config=config) as agent:
        max_retries = 3
        retry_delay = 10
        response = None

        for attempt in range(max_retries):
            try:
                response = await agent.chat(user_prompt)
                break
            except Exception as e:
                if ("503" in str(e) or "429" in str(e)) and attempt < max_retries - 1:
                    logger.warning(f"[!] Gemini API spike. Retrying in {retry_delay}s... (Attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    raise e

        if response:
            try:
                response_text = await response.text()
            except Exception:
                response_text = json.dumps({"status": "safe", "details": "News audit completed, tool execution assumed successful."})
        else:
            response_text = json.dumps({"error": "No response generated by news guard agent."})

    # ==========================================
    # RECOVERY LOOP FOR FAILED LIQUIDATIONS
    # ==========================================
    for attempt in range(1, 4):
        failed_trades = []
        try:
            with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT trade_id, symbol FROM mcp_requested_trades WHERE run_id = ? AND status = 'FAILED'",
                    (run_id,)
                )
                failed_trades = cursor.fetchall()
        except Exception as e:
            logger.error(f"[!] News Guard recovery query failed: {e}")
            break

        if not failed_trades:
            break

        logger.warning(f"[!] News Guard Recovery Attempt {attempt}/3: Retrying {len(failed_trades)} failed liquidations.")
        
        try:
            with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
                for t in failed_trades:
                    conn.execute(
                        "UPDATE mcp_requested_trades SET status = 'PENDING', updated_at = ? WHERE trade_id = ?",
                        (datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'), t[0])
                    )
                conn.commit()
        except Exception as e:
            logger.error(f"[!] Failed to reset trade statuses for retry: {e}")

        symbols_to_close = [t[1] for t in failed_trades]
        recovery_prompt = (
            f"You are M.A.C.E. News Guard trade execution recovery agent.\n"
            f"The previous attempt to liquidate the following high-risk positions failed:\n"
            f"{', '.join(symbols_to_close)}\n\n"
            f"Please retry calling the `mcp_alpaca_close_position` tool immediately for each symbol."
        )
        
        recovery_config = LocalAgentConfig(
            model="gemini-2.5-flash",
            system_instructions="You are a recovery agent. Call mcp_alpaca_close_position for the requested symbols.",
            mcp_servers=mcp_servers,
            hooks=[pre_tool_hook, post_tool_hook, tool_error_hook],
        )
        
        async with Agent(config=recovery_config) as recovery_agent:
            await recovery_agent.chat(recovery_prompt)
        
        await asyncio.sleep(5)

    return response_text

# ==============================================================================
# 4. TELEMETRY & MAIN LOOP
# ==============================================================================
def push_telemetry(result):
    logger.info(f"News Guard Result: {result}")
    try:
        mqttc = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        user = os.environ.get("MQTT_USER")
        password = os.environ.get("MQTT_PASSWORD")
        if user and password:
            mqttc.username_pw_set(user, password)

        mqttc.connect(MQTT_BROKER_IP, MQTT_PORT, 10)
        mqttc.loop_start()

        payload = {"timestamp": str(datetime.now()), "engine": "TRADFI_NEWS_GUARD", "report": result}
        info = mqttc.publish(MQTT_TOPIC, json.dumps(payload))
        info.wait_for_publish(timeout=5)

        mqttc.loop_stop()
        mqttc.disconnect()
    except Exception as e:
        logger.error(f"[!] MQTT Telemetry failed: {e}")

async def main():
    parser = argparse.ArgumentParser(description="M.A.C.E. TradFi Qualitative News Guard")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon mode")
    # News doesn't change every minute. 4 hours (14400s) is perfect for macro/micro news checks.
    parser.add_argument("--interval", type=int, default=14400, help="Interval between news audits in seconds (default: 14400s / 4h)")
    args = parser.parse_args()

    if args.daemon:
        logger.info(f"[*] Starting M.A.C.E. News Guard in DAEMON mode (interval: {args.interval}s)...")
        while True:
            try:
                result = await run_qualitative_audit()
                push_telemetry(result)
            except Exception as e:
                logger.error(f"[!] Error in News Guard daemon cycle: {e}")
            logger.info(f"[*] Sleeping for {args.interval} seconds before next news sweep...")
            await asyncio.sleep(args.interval)
    else:
        result = await run_qualitative_audit()
        push_telemetry(result)

if __name__ == "__main__":
    asyncio.run(main())
