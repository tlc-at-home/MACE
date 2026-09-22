#!/usr/bin/env python3.11
"""
M.A.C.E. v1.3 shared module: multi-venue live price pool.

Fix 5: the crypto orchestrator's fetch_live_fill_price was KuCoin-only, so a
single-venue outage silently reverted virtual-ledger fills to the brain's
stale 4h closes - the exact distortion v1.1.1 fixed. The crypto shield already
fails over KuCoin -> Binance; this module gives the orchestrator's LEDGER
fills the same resilience, env-tunable via MACE_PRICE_VENUES.

Fix 6 reuses the pool as the "is this token priceable at all" oracle for the
stale-ledger write-down purge (BEAT/USDT cleanup).

Import contract: repo root must be on sys.path (see realized_round_trips.py).
Venues are public ticker endpoints - no API keys required. Venue failures are
isolated per venue and fall through to the next; total failure returns the
caller's fallback (same contract as the old single-venue behavior).
"""

import os
import asyncio

DEFAULT_VENUES = "kucoin,binance,bybit"


def get_venue_list():
    raw = os.getenv("MACE_PRICE_VENUES", DEFAULT_VENUES)
    venues = [v.strip().lower() for v in raw.split(",") if v.strip()]
    return venues or [v.strip().lower() for v in DEFAULT_VENUES.split(",")]


async def fetch_live_price_pool(pair, fallback_price=None, log=None, venue_list=None):
    """Tries each venue in order via ccxt (sync exchange wrapped in a worker
    thread, mirroring the shield's proven pattern). Returns
    (price_or_fallback, venue_or_None). Non-primary venue service is logged
    when a `log` callable is supplied (orchestrator passes logger.info)."""
    import ccxt
    venues = venue_list if venue_list is not None else get_venue_list()
    primary = venues[0] if venues else None
    first_err = None
    for venue in venues:
        exchange = None
        try:
            exchange_cls = getattr(ccxt, venue, None)
            if exchange_cls is None:
                continue
            exchange = exchange_cls({'enableRateLimit': True})
            ticker = await asyncio.to_thread(exchange.fetch_ticker, pair)
            last = float(ticker['last'])
            if last and last > 0:
                if log and primary and venue != primary:
                    log(f"[i] Live price {pair} served by {venue} (primary {primary} failed: {first_err})")
                return last, venue
        except Exception as e:
            if first_err is None:
                first_err = str(e)
        finally:
            if exchange is not None:
                try:
                    maybe = exchange.close()
                    if asyncio.iscoroutine(maybe) or asyncio.isfuture(maybe):
                        await maybe
                except Exception:
                    pass
    return (float(fallback_price) if fallback_price else 0.0), None


async def price_available_anywhere(pair):
    """Boolean oracle for the stale-ledger purge: a token that cannot price on
    ANY configured venue is a delisting candidate, not a network blip."""
    price, venue = await fetch_live_price_pool(pair)
    return (venue is not None)
