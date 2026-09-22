#!/usr/bin/env python3.11
"""
M.A.C.E. Phase 2 Multi-Agent Swarm Pipeline
Component: The Swarm Orchestrator (orchestrator.py)
"""

import sys
import os
import json
import asyncio
import argparse
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
import paho.mqtt.client as mqtt_client

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "config/portfolio.db")

sys.path.append(os.path.join(BASE_DIR, "crypto/swarm"))
import guardrail

# v1.3: shared repo-root modules - multi-venue live prices (fix 5) and the
# realized round-trip store (fix 3, honest Kelly stats).
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
import price_venues
import realized_round_trips as rrt

MQTT_BROKER_IP = os.getenv("MQTT_BROKER_IP", "192.168.0.110")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "mace/telemetry/crypto_sword"

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger("mace.orchestrator")

def load_universe():
    """Reads and parses the broad-market token list from SQLite database."""
    if not os.path.exists(DEFAULT_DB_PATH):
        logger.warning(f"Database missing at {DEFAULT_DB_PATH}. Deploying default core triage targets.")
        return ["BTC/USDT", "SOL/USDT", "ETH/USDT", "NEAR/USDT", "AVAX/USDT"]
    try:
        with sqlite3.connect(DEFAULT_DB_PATH) as conn:
            cursor = conn.cursor()
            rows = cursor.execute("SELECT symbol FROM vw_crypto_universe ORDER BY symbol").fetchall()
            if rows:
                return [r[0] for r in rows]
    except Exception as e:
        logger.error(f"[-] Critical exception querying vw_crypto_universe table: {e}")

    return ["BTC/USDT", "SOL/USDT", "ETH/USDT", "NEAR/USDT", "AVAX/USDT"]

def push_mqtt_telemetry(payload):
    try:
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        user = os.environ.get("MQTT_USER")
        password = os.environ.get("MQTT_PASSWORD")
        if user and password:
            client.username_pw_set(user, password)
        client.connect(MQTT_BROKER_IP, MQTT_PORT, 60)
        client.loop_start()
        info = client.publish(MQTT_TOPIC, json.dumps(payload), retain=True)
        info.wait_for_publish(timeout=5)
        client.loop_stop()
        client.disconnect()
    except Exception as e:
        logger.error(f"[!] Asynchronous telemetry network link bottleneck: {e}")

def get_seconds_until_next_4h_offset():
    """
    Calculates exact seconds to sleep until the next 4-hour UTC boundary + 5 minutes (e.g., 00:05, 04:05, 08:05).
    """
    now = datetime.utcnow()

    # Calculate which 4-hour block we are in (0, 4, 8, 12, 16, 20)
    current_block = (now.hour // 4) * 4
    next_block = current_block + 4

    # Target time is next_block:05:00 UTC
    target_time = now.replace(hour=next_block if next_block < 24 else 0, minute=5, second=0, microsecond=0)

    # If the target time is in the past, it means we missed the 05-minute window for the current block
    # Wait until the NEXT 4-hour block.
    if target_time <= now:
        target_time += timedelta(days=1)

    # Calculate the delta
    delta = target_time - now
    return int(delta.total_seconds())

async def fetch_live_fill_price(pair, fallback_price):
    """v1.3: ledger fills now price off a venue POOL (default KuCoin -> Binance
    -> Bybit, MACE_PRICE_VENUES) mirroring the crypto shield's proven failover.
    v1.1 fetched live spot so entries/exits mark off one feed; v1.1.1 fixed the
    sync close() clobber; v1.3 removes the single-venue failure mode that
    silently reverted fills to the brain's stale 4h closes."""
    try:
        price, venue = await price_venues.fetch_live_price_pool(
            pair, fallback_price=fallback_price, log=logger.info)
        if venue is not None:
            return price
        if fallback_price:
            logger.warning(f"[!] Live fill price fetch failed for {pair} on all venues; using brain price.")
            return float(fallback_price)
        return 0.0
    except Exception as e:
        logger.warning(f"[!] Live fill price fetch failed for {pair} ({e}); using brain price.")
        return float(fallback_price) if fallback_price else 0.0

# v1.2: re-entry lock applied after a Bear-regime RISK-OFF liquidation. Default 24h
# = 6 sweeps of the 4h cycle (the regime brain must stay Bull for six consecutive
# reads before re-entry is possible again). Tunable via MACE_REGIME_COOLDOWN_HOURS.
REGIME_COOLDOWN_HOURS = float(os.getenv("MACE_REGIME_COOLDOWN_HOURS", "24"))


def register_regime_cooldown(symbol):
    """
    v1.2: registers a post-RISK-OFF re-entry lock in trade_cooldowns.

    The buy-side guardrail gate (vw_active_cooldowns check inside
    run_piped_risk_gate) already blocks buys during active cooldowns - it just
    never received rows from the regime-sell path, which is exactly how BONK
    churned sell -> rebuy on every regime flip: sold 371M @ 20:08Z, rebought
    43.8M, sold again 04:07Z, rebought 367.6M - 3 round trips in 36h, each
    re-entry consuming recovered USDT within the same or next sweep.

    Also mirrors the crypto shield's stop-out path by deleting the pair's
    crypto_hwm row: a liquidated pair keeps no stale high-water mark (re-entry
    re-seeds HWM from max(avg_entry, live_price)).
    """
    try:
        with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT asset_id FROM vw_crypto_universe WHERE symbol = ?", (symbol,))
            row = cursor.fetchone()
            if not row:
                logger.warning(f"[!] Cannot register regime cooldown for {symbol}: no asset_id in vw_crypto_universe")
                return
            now_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            cooldown_until_str = (datetime.utcnow() + timedelta(hours=REGIME_COOLDOWN_HOURS)).strftime("%Y-%m-%dT%H:%M:%SZ")
            cursor.execute(
                "INSERT INTO trade_cooldowns (asset_id, symbol, closed_at, reason, cooldown_until) VALUES (?, ?, ?, ?, ?)",
                (row[0], symbol, now_str, "REGIME_RISK_OFF", cooldown_until_str)
            )
            cursor.execute("DELETE FROM crypto_hwm WHERE symbol = ?", (symbol,))
            conn.commit()
            logger.info(f"[i] RISK-OFF REBUY PROBATION: {symbol} locked out of re-entry until {cooldown_until_str} ({REGIME_COOLDOWN_HOURS:.0f}h).")
    except Exception as e:
        logger.error(f"[!] Regime cooldown registration failed for {symbol}: {e}")


def purge_orphan_crypto_hwm_rows():
    """
    v1.2: crypto HWM hygiene - deletes crypto_hwm rows for pairs no longer held
    in the virtual ledger. Pre-v1.2 the RISK-OFF path left rows behind (PAXG
    3.21%, JST 5.49%, an 8.0% block), pinning stale corridors for unheld pairs
    on every dashboard read. Mirrors the equities-side HWM hygiene from v1.
    """
    try:
        with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
            cursor = conn.cursor()
            held = cursor.execute("SELECT DISTINCT token FROM portfolio WHERE quantity > 0").fetchall()
            held_pairs = {f"{r[0]}/USDT" for r in held}
            if not held_pairs:
                return  # ledger read empty/unavailable - never wipe on uncertainty
            rows = cursor.execute("SELECT symbol FROM crypto_hwm").fetchall()
            orphans = [r[0] for r in rows if r[0] not in held_pairs]
            if orphans:
                for sym in orphans:
                    cursor.execute("DELETE FROM crypto_hwm WHERE symbol = ?", (sym,))
                conn.commit()
                logger.info(f"[+] Crypto HWM hygiene: purged {len(orphans)} orphaned row(s): {', '.join(orphans)}")
    except Exception as e:
        logger.warning(f"[!] Crypto HWM hygiene sweep failed: {e}")


# v1.3 fix 6: stale-ledger write-down state. Tokens that cannot price on ANY
# configured venue for MACE_STALE_LEDGER_SWEEPS consecutive sweeps get their
# ledger row written down (deleted, exit=0 round trip) instead of silently
# inflating total_portfolio_value via stale avg-entry proxy pricing (BEAT/USDT
# sat at ~$989 unpriceable on both KuCoin and Binance since Sept).
_LEDGER_PRICE_FAILS = {}
STALE_LEDGER_SWEEPS = int(os.getenv("MACE_STALE_LEDGER_SWEEPS", "3"))


async def purge_unpriceable_ledger_rows():
    """
    v1.3: removes virtual-ledger rows that no venue can price anymore.

    Canary guard: when >= half of held tokens are unpriceable in the SAME
    sweep, the purge is skipped entirely - that signature is a venue/network
    outage, not N simultaneous delistings. Fail counts reset on any successful
    price read, and daemon restart resets patience (harmless: 3 sweeps = 12h).
    """
    try:
        with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
            rows = conn.execute(
                "SELECT token, quantity, avg_entry_price FROM portfolio WHERE token != 'USDT' AND quantity > 0"
            ).fetchall()
        if not rows:
            return

        priced, unpriced = [], []
        for token, qty, avg_entry in rows:
            pair = f"{token}/USDT"
            ok = await price_venues.price_available_anywhere(pair)
            if ok:
                _LEDGER_PRICE_FAILS.pop(token, None)
                priced.append(token)
            else:
                _LEDGER_PRICE_FAILS[token] = _LEDGER_PRICE_FAILS.get(token, 0) + 1
                unpriced.append((token, qty, avg_entry))

        # Outage canary: never mass-purge on a suspected network/venue outage.
        if unpriced and len(unpriced) >= max(2, (len(rows) + 1) // 2):
            logger.warning(
                f"[i] Stale-ledger purge skipped: {len(unpriced)}/{len(rows)} tokens unpriceable "
                f"(suspected venue/network outage, not delisting)")
            return

        for token, qty, avg_entry in unpriced:
            fails = _LEDGER_PRICE_FAILS.get(token, 0)
            if fails < STALE_LEDGER_SWEEPS:
                logger.info(
                    f"[i] {token}/USDT unpriceable ({fails}/{STALE_LEDGER_SWEEPS} sweeps); "
                    f"write-down deferred pending confirmation")
                continue
            with sqlite3.connect(DEFAULT_DB_PATH, timeout=30.0) as conn:
                conn.execute("DELETE FROM portfolio WHERE token = ?", (token,))
                conn.commit()
            rrt.record_trip(
                asset_class="CRYPTO", symbol=f"{token}/USDT", qty=float(qty),
                entry_price=float(avg_entry), exit_price=0.0,
                reason="STALE_LEDGER_WRITE_DOWN", basis="ledger_exact",
                db_path=DEFAULT_DB_PATH)
            logger.warning(
                f"[!] STALE LEDGER WRITE-DOWN: {token} removed from virtual ledger "
                f"(unpriceable across all venues for {fails} sweeps, cost basis "
                f"${float(qty) * float(avg_entry):.2f} written off)")
    except Exception as e:
        logger.warning(f"[!] Stale-ledger purge sweep failed: {e}")

async def process_single_asset_pipeline(symbol, semaphore):
    async with semaphore:
        scout_path = os.path.join(BASE_DIR, "crypto/swarm/scout.py")
        brain_path = os.path.join(BASE_DIR, "crypto/swarm/brain.py")
        try:
            scout_proc = await asyncio.create_subprocess_exec(sys.executable, scout_path, symbol, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            scout_stdout, scout_stderr = await scout_proc.communicate()
            if scout_proc.returncode != 0:
                logger.error(f"[-] Data Scout failed for {symbol}: {scout_stderr.decode().strip()}")
                return None
            brain_proc = await asyncio.create_subprocess_exec(sys.executable, brain_path, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            brain_stdout, brain_stderr = await brain_proc.communicate(input=scout_stdout)
            if brain_proc.returncode != 0:
                logger.error(f"[-] Quant Brain math core faulted for {symbol}: {brain_stderr.decode().strip()}")
                return None
            return json.loads(brain_stdout.decode().strip())
        except Exception as e:
            logger.error(f"[!] Engine pipe runtime failure for asset node {symbol}: {e}")
            return None

async def execute_swarm_sweep(args):
    logger.info("[*] Initializing broad market swarm processing sweep...")
    # Publish SCANNING state to MQTT to force Home Assistant state update
    start_payload = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "engine": "crypto_sword",
        "status": "SCANNING",
        "top_regime_signal": {"ticker": "Evaluating...", "regime": "Scanning", "calculated_kelly": 0.0, "signal_strength": 0.0},
        "execution_payload": {"status": "SCANNING", "allocated_dollars": 0.0}
    }
    await asyncio.to_thread(push_mqtt_telemetry, start_payload)

    guardrail.init_db()
    rrt.ensure_schema(DEFAULT_DB_PATH)  # v1.3: realized_round_trips + component_health
    purge_orphan_crypto_hwm_rows()  # v1.2: stale-corridor hygiene for unheld pairs
    await purge_unpriceable_ledger_rows()  # v1.3: stale-ledger write-down (BEAT-class)

    # v1.3 fix 3: hand realized edge stats to the brain subprocesses via env
    # (brains are pure math sandboxes piped scout->brain; env is the only side
    # channel that leaves their stdin contract intact). None -> priors stay.
    empirical = rrt.empirical_env_json("CRYPTO", db_path=DEFAULT_DB_PATH)
    if empirical:
        os.environ["MACE_EMPIRICAL_KELLY_JSON"] = empirical
        logger.info(f"[i] Empirical Kelly stats active: {empirical}")
    else:
        os.environ.pop("MACE_EMPIRICAL_KELLY_JSON", None)

    raw_universe = load_universe()
    if args.limit:
        raw_universe = raw_universe[:args.limit]
    if not raw_universe:
        logger.error("[-] Active evaluation asset array contains zero targets. Processing cancelled.")
        return

    concurrency_semaphore = asyncio.Semaphore(10)
    tasks = [process_single_asset_pipeline(symbol, concurrency_semaphore) for symbol in raw_universe]
    brain_signals = await asyncio.gather(*tasks)


    logger.info("[*] Complete asset matrix evaluated. Processing risk boundaries and execution gates...")

    # Aggregate candidates for portfolio allocator
    candidates = []
    for signal in brain_signals:
        if not signal or signal.get("status") not in ["success", "insufficient_data"]:
            continue
        # Map to portfolio allocator expected keys
        candidates.append({
            "symbol": signal.get("ticker", "UNKNOWN/USDT"),
            "current_state": signal.get("regime", "Unknown"),
            "ml_confirmed": True, # Crypto uses confidence multiplier instead of binary ML confirmation
            "calculated_kelly": float(signal.get("kelly_fraction", 0.0)),
            "signal_strength": float(signal.get("signal_strength", 0.0)),
            "current_price": float(signal.get("current_price", 0.0)),
            "raw_signal": signal
        })

    # Get available cash and existing positions
    conn = sqlite3.connect(DEFAULT_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT quantity FROM portfolio WHERE blockchain = 'ARBITRUM' AND token = 'USDT'")
    cash_row = cursor.fetchone()
    available_usdt = float(cash_row[0]) if cash_row else 0.0
    
    cursor.execute("SELECT token FROM portfolio WHERE token != 'USDT'")
    existing = [f"{r[0]}/USDT" for r in cursor.fetchall()]
    conn.close()

    payload = {
        "candidates": candidates,
        "available_cash": available_usdt,
        "existing_positions": existing
    }

    allocator_path = os.path.join(BASE_DIR, "crypto/swarm/portfolio_allocator.py")
    allocator_proc = await asyncio.create_subprocess_exec(
        sys.executable, allocator_path, 
        stdin=asyncio.subprocess.PIPE, 
        stdout=asyncio.subprocess.PIPE, 
        stderr=asyncio.subprocess.PIPE
    )
    alloc_stdout, alloc_stderr = await allocator_proc.communicate(input=json.dumps(payload).encode())
    
    top_candidate = None
    best_signal_strength = -1.0
    overall_execution_status = "IDLE"
    max_allocated_dollars = 0.0
    
    if allocator_proc.returncode != 0:
        logger.error(f"[-] Portfolio Allocator failed: {alloc_stderr.decode().strip()}")
    else:
        alloc_res = json.loads(alloc_stdout.decode().strip())
        approved_trades = alloc_res.get("approved_trades", [])
        
        # Execute sells for Bear regime
        for cand in candidates:
            if cand["current_state"] == "Bear" and cand["symbol"] in existing:
                symbol = cand["symbol"]
                token_symbol = symbol.split("/")[0]
                balances = guardrail.get_wallet_balances_summary()
                held_token = None
                for chain, chain_data in balances.items():
                    if token_symbol in chain_data.get("tokens", {}):
                        held_token = chain_data["tokens"][token_symbol]
                        break
                if held_token and held_token["quantity"] > 0:
                    current_price = await fetch_live_fill_price(symbol, cand.get("current_price", 0.0))
                    ledger_receipt = guardrail.evaluate_and_execute_simulated_trade(
                        symbol=symbol, action="SELL", quantity=held_token["quantity"],
                        execution_price=current_price, reason="REGIME_RISK_OFF")
                    if ledger_receipt.get("success"):
                        logger.info(f"[!!!] RISK-OFF SELL: Liquidated {held_token['quantity']:.4f} {symbol} due to Bear regime.")
                        register_regime_cooldown(symbol)  # v1.2: block same-token rebuy whipsaw
                        overall_execution_status = "SOLD_BEAR_REGIME"
                    else:
                        overall_execution_status = "SELL_FAILED"
        
        # Execute buys
        for trade in approved_trades:
            symbol = trade["symbol"]
            allocated_dollars = trade.get("target_size_usd", 0.0)
            current_price = trade.get("current_price", 0.0)
            
            if trade.get("signal_strength", 0.0) > best_signal_strength:
                best_signal_strength = trade.get("signal_strength", 0.0)
                top_candidate = {
                    "ticker": symbol,
                    "regime": trade.get("current_state"),
                    "calculated_kelly": trade.get("calculated_kelly"),
                    "signal_strength": best_signal_strength
                }
            
            if current_price <= 0:
                overall_execution_status = "INVALID_PRICE"
            else:
                current_price = await fetch_live_fill_price(symbol, current_price)
                brain_output_dump = json.dumps(trade["raw_signal"])
                verdict = guardrail.run_piped_risk_gate(brain_output_dump)
                if verdict.get("status") == "approved":
                    allocated_dollars = float(verdict.get("allocated_dollars", 0.0))
                    trade_qty = allocated_dollars / current_price
                    ledger_receipt = guardrail.evaluate_and_execute_simulated_trade(symbol=symbol, action="BUY", quantity=trade_qty, execution_price=current_price)
                    if ledger_receipt.get("success"):
                        logger.info(f"[+] LEDGER TRANSACTION SUCCESS: Bought {trade_qty:.4f} {symbol} at ${current_price:.2f}")
                        overall_execution_status = "DISPATCHED"
                        max_allocated_dollars = max(max_allocated_dollars, allocated_dollars)
                    else:
                        logger.warning(f"[-] Ledger transactional entry failure: {ledger_receipt.get('error')}")
                        overall_execution_status = "REJECTED_BY_LEDGER"
                else:
                    # v1.2: surface risk-gate blocks in the journal. Previously these
                    # were silent, so regime-cooldown rejections were indistinguishable
                    # from no-ops in telemetry and the BONK whipsaw was invisible.
                    logger.info(f"[i] RISK GATE: BUY {symbol} blocked - {verdict.get('reason', verdict.get('status', 'unknown'))}")

    if not top_candidate:
        top_candidate = {"ticker": "N/A", "regime": "Neutral", "calculated_kelly": 0.0, "signal_strength": 0.0}

    telemetry_payload = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "engine": "crypto_sword",
        "status": "SCAN_COMPLETE",
        "top_regime_signal": top_candidate,
        "execution_payload": {"status": overall_execution_status, "allocated_dollars": max_allocated_dollars}
    }
    await asyncio.to_thread(push_mqtt_telemetry, telemetry_payload)

async def main_async():
    parser = argparse.ArgumentParser(description="M.A.C.E. Phase 2 Multi-Agent Swarm Orchestrator Engine")
    parser.add_argument("--limit", type=int, default=0, help="Enforce limit constraints to test smaller token arrays")
    parser.add_argument("--daemon", action="store_true", help="Instantiates script as an uninterrupted polling daemon service")
    parser.add_argument("--interval", type=int, default=14400, help="Interval window duration parameters in seconds (Default 4h/14400s) - IGNORED IN DAEMON MODE IN FAVOR OF UTC SYNC")
    args = parser.parse_args()

    if args.daemon:
        logger.info("[*] Booting M.A.C.E. Continuous Background Worker. Synchronized to 4-Hour UTC Boundaries (HH:05:00).")
        while True:
            try:
                await execute_swarm_sweep(args)
            except Exception as e:
                logger.error(f"[!] Error during daemon run sweep: {e}")

            logger.info("[*] Task sequence finalized. Calculating sleep duration until next 4H UTC offset (HH:05:00)...")
            sleep_duration = get_seconds_until_next_4h_offset()
            logger.info(f"[*] Sleeping for {sleep_duration}s...")
            await asyncio.sleep(sleep_duration)
    else:
        await execute_swarm_sweep(args)
        logger.info("[+] Single swarm routing pass executed cleanly. Core shut down.")

if __name__ == "__main__":
    asyncio.run(main_async())
