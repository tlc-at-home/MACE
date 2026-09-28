#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Architecture
Component: Crypto Qualitative News Guard (crypto_news_guard.py)

Role: Asynchronous Qualitative Risk Agent.
      - Loads crypto universe symbols and held positions from SQLite database.
      - Fetches market news across major crypto RSS feeds (CoinDesk, Cointelegraph, Decrypt).
      - Uses Gemini 2.5 Flash via Google Antigravity SDK with custom Python tools.
      - Fires emergency simulated liquidations via guardrail if severe existential threats are detected
        (exploits, protocol hacks, stablecoin de-pegging, founder arrests, regulatory freezes).
      - Registers 24-hour QUALITATIVE_NEWS_THREAT cooldown locks and purges HWM entries.
      - Writes component health heartbeats driving the orchestrator's fail-neutral buy gate.
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
from datetime import datetime, timedelta, timezone
from paho.mqtt import client as mqtt_client
from google.antigravity import Agent, LocalAgentConfig, types

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

# Import shared repo-root modules
import realized_round_trips as rrt
import price_venues

sys.path.append(os.path.join(BASE_DIR, "crypto/swarm"))
import guardrail

MQTT_BROKER_IP = os.getenv("MQTT_BROKER_IP", "192.168.0.110")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "mace/telemetry/crypto_news_guard"

# STRICT QUALITATIVE RISK PROMPT FOR CRYPTO
SYSTEM_PROMPT = (
    "You are M.A.C.E. CRYPTO NEWS GUARD, an autonomous qualitative risk analyst.\n"
    "You will be provided with a list of currently held crypto token positions and the latest crypto news headlines across the universe.\n\n"
    "EXPLICIT DIRECTIVES:\n"
    "1. ANALYZE: Read the news headlines carefully. Evaluate the contextual severity for any held token or universe asset.\n"
    "2. THRESHOLD: Do NOT react to normal crypto market volatility, FUD, standard pullbacks, or meme sentiment.\n"
    "3. TRIGGER: ONLY trigger a liquidation if the news implies an EXISTENTIAL THREAT to a project or token.\n"
    "   Examples of existential threats: smart contract exploits/hacks, treasury drain, stablecoin de-pegging (e.g. USDS/USDT/USDE),\n"
    "   founder/team criminal indictments or rug pulls, or emergency exchange freezes/delistings.\n"
    "4. ACTION: If an existential threat is detected for a held token, call the `close_crypto_position_tool` function with the `symbol` parameter (e.g., 'SOL/USDT') to liquidate it immediately.\n"
    "5. REPORT: Return a JSON summary of what you analyzed and what actions you took. If no threats were found, return: {\"status\": \"safe\", \"details\": \"No existential threats detected in recent news.\"}"
)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.crypto_news_guard")

def _utcnow_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def close_crypto_position_tool(symbol: str) -> str:
    """Liquidates an open token position on the virtual ledger and registers a 24h cooldown lock.

    Args:
        symbol: The crypto pair to liquidate, e.g. "SOL/USDT" or token "SOL".
    """
    try:
        pair = symbol if "/" in symbol else f"{symbol}/USDT"
        token = pair.split("/")[0]

        balances = guardrail.get_wallet_balances_summary()
        held_token = None
        for chain, chain_data in balances.items():
            if token in chain_data.get("tokens", {}):
                held_token = chain_data["tokens"][token]
                break

        if not held_token or held_token["quantity"] <= 0:
            return f"No open balance found for {token} on ledger. No liquidation needed."

        # Fetch live price from price venues pool
        live_price = 0.0
        try:
            live_price, venue = asyncio.run(price_venues.fetch_live_price_pool(pair, fallback_price=held_token["avg_entry_price"]))
        except Exception:
            live_price = float(held_token["avg_entry_price"])

        receipt = guardrail.evaluate_and_execute_simulated_trade(
            symbol=pair, action="SELL", quantity=held_token["quantity"],
            execution_price=live_price, reason="QUALITATIVE_NEWS_THREAT",
            db_path=DEFAULT_DB_PATH
        )

        if not receipt.get("success"):
            return f"Failed to execute simulated liquidation for {pair}: {receipt.get('error')}"

        # Register 24-hour Post-Liquidation Cooldown Lock and clear HWM row
        if os.path.exists(DEFAULT_DB_PATH):
            with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
                conn.execute("PRAGMA foreign_keys = ON;")
                cursor = conn.cursor()
                cursor.execute("SELECT asset_id FROM vw_crypto_universe WHERE symbol = ?", (pair,))
                row = cursor.fetchone()
                if row:
                    asset_id = row[0]
                    now_str = _utcnow_naive().strftime("%Y-%m-%dT%H:%M:%SZ")
                    cooldown_until_str = (_utcnow_naive() + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
                    cursor.execute("""
                        INSERT INTO trade_cooldowns (asset_id, symbol, closed_at, reason, cooldown_until)
                        VALUES (?, ?, ?, 'QUALITATIVE_NEWS_THREAT', ?)
                    """, (asset_id, pair, now_str, cooldown_until_str))
                    cursor.execute("DELETE FROM crypto_hwm WHERE symbol = ?", (pair,))
                    conn.commit()

        return f"Successfully liquidated {held_token['quantity']:.4f} {pair} at ${live_price:.4f} and registered 24h QUALITATIVE_NEWS_THREAT lock."
    except Exception as e:
        logger.error(f"[-] Exception liquidating {symbol}: {e}")
        return f"Failed to liquidate position for {symbol}: {str(e)}"

def load_crypto_universe_metadata():
    """Loads all crypto symbols from SQLite vw_crypto_universe view."""
    universe = []
    if os.path.exists(DEFAULT_DB_PATH):
        try:
            with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                cursor = conn.cursor()
                rows = cursor.execute("SELECT asset_id, symbol, broker, exchange, pair FROM vw_crypto_universe").fetchall()
                for r in rows:
                    universe.append({
                        "asset_id": r[0],
                        "symbol": r[1],
                        "broker": r[2],
                        "exchange": r[3],
                        "pair": r[4]
                    })
        except Exception as e:
            logger.error(f"[-] Failed to load vw_crypto_universe from DB: {e}")
    return universe

def fetch_rss_feed(session: requests.Session, feed_url: str, source_name: str) -> list[str]:
    """Fetches and parses headlines from a public crypto RSS feed."""
    headlines = []
    try:
        resp = session.get(feed_url, timeout=8, allow_redirects=True)
        if resp.status_code == 200 and resp.content:
            root = ET.fromstring(resp.content)
            # Standard RSS channel/item parsing
            for item in root.findall(".//item")[:15]:
                title = item.find("title")
                title_text = title.text.strip() if title is not None and title.text else ""
                desc = item.find("description")
                desc_text = desc.text.strip() if desc is not None and desc.text else ""
                # Clean up simple HTML tags from description if present
                desc_summary = desc_text.split("<")[0].strip()[:150]
                if title_text:
                    entry = f"[{source_name}] {title_text}"
                    if desc_summary:
                        entry += f" — {desc_summary}"
                    headlines.append(entry)
    except Exception as e:
        logger.warning(f"[!] Failed to fetch {source_name} RSS feed: {e}")
    return headlines

def gather_guard_context():
    """Fetches currently held positions and market news across crypto RSS sources."""
    positions = []
    try:
        if os.path.exists(DEFAULT_DB_PATH):
            with sqlite3.connect(DEFAULT_DB_PATH) as conn:
                rows = conn.execute("SELECT DISTINCT token FROM portfolio WHERE token != 'USDT' AND quantity > 0").fetchall()
                positions = [f"{r[0]}/USDT" for r in rows]
    except Exception as e:
        logger.error(f"[-] Failed to fetch crypto positions from portfolio: {e}")

    universe = load_crypto_universe_metadata()
    if not universe and not positions:
        logger.warning("[!] No crypto universe or positions found in database.")
        return None

    session = requests.Session()
    session.headers.update({"User-Agent": "MACE-Crypto-News-Guard/1.4 (Automated Risk Analyst)"})

    # Public Crypto RSS Feeds
    feeds = [
        ("https://www.coindesk.com/arc/outboundfeeds/rss/", "CoinDesk"),
        ("https://cointelegraph.com/rss", "Cointelegraph"),
        ("https://decrypt.co/feed", "Decrypt")
    ]

    raw_headlines = []
    for url, name in feeds:
        items = fetch_rss_feed(session, url, name)
        raw_headlines.extend(items)

    # Keywords of interest for qualitative risk filtering
    threat_keywords = [
        "hack", "exploit", "drain", "insolvent", "bankruptcy", "sec", "lawsuit", "arrest",
        "fraud", "depeg", "freeze", "halt", "rug pull", "investigation", "stolen"
    ]
    held_tokens = [p.split("/")[0].upper() for p in positions]

    # Prioritize headlines that mention held tokens or threat keywords
    relevant_headlines = []
    for h in raw_headlines:
        h_upper = h.upper()
        is_held = any(t in h_upper for t in held_tokens)
        is_threat = any(k in h.lower() for k in threat_keywords)
        if is_held or is_threat:
            relevant_headlines.append(h)

    # Cap to top 25 headlines to keep token context concise
    final_headlines = relevant_headlines[:25] if relevant_headlines else raw_headlines[:15]

    return {
        "positions": positions,
        "news": final_headlines if final_headlines else ["No recent news found."]
    }

def _heartbeat(status, detail=""):
    """Persists guard health so the crypto orchestrator's fail-neutral buy gate can verify it."""
    ok = rrt.write_component_heartbeat("crypto_news_guard", status, detail=detail, db_path=DEFAULT_DB_PATH)
    if status == "healthy":
        logger.info(f"[*] Crypto News Guard heartbeat: healthy ({(detail or '')[:80]})")
    else:
        logger.warning(f"[!] Crypto News Guard heartbeat: {status} ({(detail or '')[:80]}) [persisted={ok}]")
    return ok

async def run_qualitative_audit():
    logger.info("[*] Booting M.A.C.E. Crypto Qualitative News Guard...")
    run_id = f"crypto_news_guard_{_utcnow_naive().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"[*] Starting Crypto News Guard run: {run_id}")

    context_data = gather_guard_context()
    if not context_data:
        _heartbeat("idle", "No universe data or positions available")
        return json.dumps({"status": "idle", "reason": "No universe data or positions available."})

    user_prompt = (
        f"Currently Held Crypto Positions: {json.dumps(context_data['positions'])}\n\n"
        f"Latest Crypto News Headlines Across Feeds:\n{chr(10).join(context_data['news'])}"
    )

    config = LocalAgentConfig(
        model="gemini-2.5-flash",
        system_instructions=SYSTEM_PROMPT,
        tools=[close_crypto_position_tool],
    )

    logger.info("[*] Handing crypto news context to Gemini 2.5 Flash for qualitative analysis...")

    response_text = ""
    try:
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
                    try:
                        verdict_status = json.loads(response_text).get("status", "unknown")
                    except Exception:
                        verdict_status = "unparsed"
                    _heartbeat("healthy", f"verdict={verdict_status}")
                except Exception as e:
                    response_text = json.dumps({"status": "degraded", "details": f"News audit response unreadable: {e}"})
                    _heartbeat("degraded", f"response.text() failed: {e}")
            else:
                response_text = json.dumps({"error": "No response generated by news guard agent."})
                _heartbeat("degraded", "no response generated by agent")
    except Exception as e:
        logger.error(f"[!] Agent execution failure: {e}")
        response_text = json.dumps({"status": "degraded", "details": f"Agent failure: {e}"})
        _heartbeat("degraded", str(e)[:150])

    return response_text

def push_telemetry(result):
    logger.info(f"Crypto News Guard Result: {result}")
    try:
        mqttc = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        user = os.environ.get("MQTT_USER")
        password = os.environ.get("MQTT_PASSWORD")
        if user and password:
            mqttc.username_pw_set(user, password)

        mqttc.connect(MQTT_BROKER_IP, MQTT_PORT, 10)
        mqttc.loop_start()

        payload = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "engine": "CRYPTO_NEWS_GUARD",
            "report": result
        }
        info = mqttc.publish(MQTT_TOPIC, json.dumps(payload))
        info.wait_for_publish(timeout=5)

        mqttc.loop_stop()
        mqttc.disconnect()
    except Exception as e:
        logger.error(f"[!] MQTT Telemetry failed: {e}")

async def main():
    parser = argparse.ArgumentParser(description="M.A.C.E. Crypto Qualitative News Guard")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon mode")
    parser.add_argument("--interval", type=int, default=14400, help="Interval between news audits in seconds (default: 14400s / 4h)")
    args = parser.parse_args()

    if args.daemon:
        logger.info(f"[*] Starting M.A.C.E. Crypto News Guard in DAEMON mode (interval: {args.interval}s)...")
        while True:
            try:
                result = await run_qualitative_audit()
                push_telemetry(result)
            except Exception as e:
                logger.error(f"[!] Error in daemon sweep: {e}")
                _heartbeat("degraded", f"daemon sweep error: {e}")

            logger.info(f"[*] Sleeping for {args.interval}s until next qualitative audit...")
            await asyncio.sleep(args.interval)
    else:
        result = await run_qualitative_audit()
        push_telemetry(result)
        logger.info("[+] Single audit pass completed.")

if __name__ == "__main__":
    asyncio.run(main())
