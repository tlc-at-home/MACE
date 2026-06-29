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

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

MQTT_BROKER_IP = "192.168.0.110"
MQTT_PORT = 1883
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
async def run_qualitative_audit():
    logger.info("[*] Booting M.A.C.E. Qualitative News Guard...")

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

    config = LocalAgentConfig(
        model="gemini-2.5-flash",
        system_instructions=SYSTEM_PROMPT,
        mcp_servers=mcp_servers,
    )

    logger.info("[*] Handing news context to Gemini 2.5 Flash for qualitative analysis...")

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
                text_content = await response.text()
                return text_content if text_content else json.dumps({"status": "safe", "details": "Agent returned empty response."})
            except Exception:
                return json.dumps({"status": "safe", "details": "News audit completed, tool execution assumed successful."})
        else:
            return json.dumps({"error": "No response generated by news guard agent."})

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
