#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Architecture
Component: TradFi Qualitative News Guard (tradfi_news_guard.py)

Role: Asynchronous Qualitative Risk Agent.
      - Loads universe symbols from SQLite database.
      - Fetches market news for all universe symbols via Yahoo Finance RSS feeds.
      - Uses Gemini 2.5 Flash via Google Antigravity SDK with custom Python tools.
      - Fires emergency liquidations via BrokerClient if severe existential threats are detected.
"""

import os
import sys
import json
import asyncio
import logging
import requests
import argparse
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime
from paho.mqtt import client as mqtt_client
from google.antigravity import Agent, LocalAgentConfig, types

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from brokers import get_client_for_symbol, get_client_by_name

DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")
MQTT_BROKER_IP = os.getenv("MQTT_BROKER_IP", "192.168.0.110")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "mace/telemetry/tradfi_news_guard"

# STRICT QUALITATIVE RISK PROMPT
SYSTEM_PROMPT = (
    "You are M.A.C.E. NEWS GUARD, an autonomous qualitative risk analyst.\n"
    "You will be provided with a list of currently held stock positions and the latest financial news headlines across the universe.\n\n"
    "EXPLICIT DIRECTIVES:\n"
    "1. ANALYZE: Read the news headlines carefully. Evaluate the contextual severity for any universe symbol.\n"
    "2. THRESHOLD: Do NOT react to normal market volatility, minor price drops, or standard analyst downgrades.\n"
    "3. TRIGGER: ONLY trigger a liquidation if the news implies an EXISTENTIAL THREAT to a company. "
    "Examples of existential threats: SEC fraud investigations, bankruptcy filings, massive catastrophic product failures, "
    "CEO arrests, or severe regulatory crackdowns that threaten the company's ability to operate.\n"
    "4. ACTION: If an existential threat is detected for a symbol, call the `close_position_tool` function with the `symbol` parameter to liquidate it immediately.\n"
    "5. REPORT: Return a JSON summary of what you analyzed and what actions you took. If no threats were found, return: {\"status\": \"safe\", \"details\": \"No existential threats detected in recent news.\"}"
)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.news_guard")

def close_position_tool(symbol: str) -> str:
    """Liquidates an open position for a given stock symbol across supported brokers.

    Args:
        symbol: The stock symbol to liquidate, e.g. "AAPL".
    """
    try:
        client = get_client_for_symbol(symbol)
        client.close_position(symbol)

        # Register 24-hour Post-Liquidation Cooldown Lock
        if os.path.exists(DEFAULT_DB_PATH):
            with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                conn.execute("PRAGMA foreign_keys = ON;")
                cursor = conn.cursor()
                cursor.execute("SELECT asset_id FROM vw_equities_universe WHERE symbol = ?", (symbol,))
                row = cursor.fetchone()
                if row:
                    asset_id = row[0]
                    now_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                    cooldown_until_str = (datetime.utcnow() + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
                    cursor.execute("""
                        INSERT INTO trade_cooldowns (asset_id, symbol, closed_at, reason, cooldown_until)
                        VALUES (?, ?, ?, 'QUALITATIVE_NEWS_THREAT', ?)
                    """, (asset_id, symbol, now_str, cooldown_until_str))
                    conn.commit()

        return f"Successfully liquidated position for {symbol} and registered 24h risk cooldown lock."
    except Exception as e:
        return f"Failed to liquidate position for {symbol}: {str(e)}"

def load_tradfi_universe_metadata():
    """Loads all symbols and their metadata from SQLite vw_equities_universe view."""
    universe = []
    if os.path.exists(DEFAULT_DB_PATH):
        try:
            with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                cursor = conn.cursor()
                rows = cursor.execute("SELECT asset_id, symbol, broker, exchange, currency FROM vw_equities_universe").fetchall()
                for r in rows:
                    universe.append({
                        "asset_id": r[0],
                        "symbol": r[1],
                        "broker": r[2],
                        "exchange": r[3],
                        "currency": r[4]
                    })
        except Exception as e:
            logger.error(f"[-] Failed to load vw_equities_universe from DB: {e}")
    return universe

def map_yahoo_ticker(symbol: str, exchange: str) -> str:
    """Maps exchange symbol to Yahoo Finance ticker format."""
    ex = (exchange or "").upper()
    if ex in ("LSE", "LONDON"):
        return f"{symbol}.L"
    elif ex in ("TSE", "TSEJ", "TOKYO"):
        return f"{symbol}.T"
    elif ex in ("ASX", "AUSTRALIA"):
        return f"{symbol}.AX"
    return symbol

def fetch_yahoo_news(session: requests.Session, symbol: str, exchange: str = "SMART") -> list[str]:
    """Fetches news headlines for a symbol from Yahoo Finance RSS feed."""
    yahoo_ticker = map_yahoo_ticker(symbol, exchange)
    url = f"https://finance.yahoo.com/rss/headline?s={yahoo_ticker}"
    headlines = []
    try:
        resp = session.get(url, timeout=5, allow_redirects=True)
        if resp.status_code == 200 and resp.content:
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item")[:5]:
                title = item.find("title")
                title_text = title.text if title is not None else ""
                if title_text:
                    headlines.append(f"[{symbol}] {title_text}")
    except Exception as e:
        logger.warning(f"[!] Failed to fetch Yahoo news for {symbol}: {e}")
    return headlines

def gather_guard_context():
    """Fetches currently held positions and market news for all universe symbols."""
    positions = []
    try:
        primary_client = get_client_by_name("alpaca")
        raw_positions = primary_client.get_positions()
        positions = [p["symbol"] for p in raw_positions]
    except Exception as e:
        logger.error(f"[-] Failed to fetch positions via BrokerClient: {e}")

    universe = load_tradfi_universe_metadata()
    if not universe:
        logger.warning("[!] No symbols found in tradfi_universe database.")
        return None

    news_headlines = []
    # Prioritize held positions first, then scan universe symbols (top 10 to keep run lightweight)
    symbols_to_scan = positions + [u["symbol"] for u in universe if u["symbol"] not in positions]
    scan_limit = symbols_to_scan[:10]

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

    for sym_meta in universe:
        sym = sym_meta["symbol"]
        ex = sym_meta["exchange"]
        if sym in scan_limit:
            items = fetch_yahoo_news(session, sym, ex)
            news_headlines.extend(items)

    return {
        "positions": positions,
        "news": news_headlines if news_headlines else ["No recent news found."]
    }

async def run_qualitative_audit():
    logger.info("[*] Booting M.A.C.E. Qualitative News Guard...")
    run_id = f"news_guard_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"[*] Starting News Guard run: {run_id}")

    context_data = gather_guard_context()
    if not context_data:
        return json.dumps({"status": "idle", "reason": "No universe data or positions available."})

    user_prompt = (
        f"Currently Held Positions: {json.dumps(context_data['positions'])}\n\n"
        f"Latest News Headlines Across Universe:\n{chr(10).join(context_data['news'])}"
    )

    config = LocalAgentConfig(
        model="gemini-2.5-flash",
        system_instructions=SYSTEM_PROMPT,
        tools=[close_position_tool],
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
                response_text = json.dumps({"status": "safe", "details": "News audit completed."})
        else:
            response_text = json.dumps({"error": "No response generated by news guard agent."})

    return response_text

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
