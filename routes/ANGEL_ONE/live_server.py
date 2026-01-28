# routes/ANGEL_ONE/live_server.py
"""
Live Market Movement streaming (SSE) — production-ready for multi-worker setups.

✅ Goals solved:
- 5 workers / multiple clients: everyone receives SAME snapshot (no worker mismatch)
- server not slow: ONLY ONE leader computes snapshot; others just stream from Redis
- fast UI updates: SSE pushes snapshots immediately on publish

Requirements:
    pip install redis orjson sse-starlette httpx uvloop

Env:
    REDIS_URL=redis://127.0.0.1:6379/0

Notes:
- Uses routes/ANGEL_ONE/realtime_hub.py (RealtimeHub)
- Uses routes/ANGEL_ONE/signals_async.py (build_signals_async)
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime
from typing import Any, Dict, Optional

import orjson
from fastapi import APIRouter, Query
from sse_starlette.sse import EventSourceResponse

from routes.ANGEL_ONE.realtime_hub import RealtimeHub
from routes.ANGEL_ONE.signals_async import build_signals_async

router = APIRouter(prefix="/market-movement", tags=["Market Movement"])

hub = RealtimeHub()
WORKER_ID = f"pid:{os.getpid()}"


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
def _json_dumps(obj: Any) -> str:
    return orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS).decode("utf-8")


async def _safe_pubsub_close(pubsub) -> None:
    try:
        await pubsub.unsubscribe()
    except Exception:
        pass
    try:
        await pubsub.close()
    except Exception:
        pass


# -----------------------------------------------------------------------------
# leader producer loop (only ONE worker runs compute)
# -----------------------------------------------------------------------------
async def _producer_loop(
    refresh_sec: int,
    stocklist_path: str,
    tokens_path: str,
    interval_30m: str,
    interval_day: str,
    lookback_days_30m: int,
    lookback_days_day: int,
    candle_concurrency: int,
) -> None:
    """
    5 workers can run this, but leader-lock ensures only ONE publishes.
    """
    # basic jitter to avoid thundering herd at startup
    await asyncio.sleep(0.2)

    while True:
        try:
            # Try to become leader
            is_leader = await hub.try_become_leader(WORKER_ID)
            if not is_leader:
                await asyncio.sleep(1.0)
                continue

            # Leader mode
            while True:
                still = await hub.renew_leader_lock(WORKER_ID)
                if not still:
                    # Lost lock, another worker took over
                    break

                snapshot = await build_signals_async(
                    stocklist_path=stocklist_path,
                    tokens_path=tokens_path,
                    interval_30m=interval_30m,
                    interval_day=interval_day,
                    lookback_days_30m=lookback_days_30m,
                    lookback_days_day=lookback_days_day,
                    candle_concurrency=candle_concurrency,
                    quote_chunk_size=50,
                    quote_sleep_s=0.15,
                    min_candles_30m=60,
                    min_candles_day=60,
                )

                await hub.publish_snapshot(snapshot)

                await asyncio.sleep(float(refresh_sec))

        except Exception:
            # Never kill background producer
            await asyncio.sleep(2.0)


# -----------------------------------------------------------------------------
# Public endpoints
# -----------------------------------------------------------------------------
@router.get("/signals/once")
async def signals_once():
    """
    One-shot snapshot (debugging/cron). This does compute once.
    """
    return await build_signals_async(
        stocklist_path="routes/ANGEL_ONE/stockList.json",
        tokens_path="tokens.json",
        interval_30m="THIRTY_MINUTE",
        interval_day="ONE_DAY",
        lookback_days_30m=60,
        lookback_days_day=520,
        candle_concurrency=12,
    )


@router.get("/signals/stream")
async def signals_stream(
    refresh_sec: int = Query(5, ge=1, le=60),
):
    """
    Real-time stream (SSE):
    - Sends latest snapshot immediately (from Redis)
    - Then streams every publish via Redis PubSub

    ✅ Works correctly with 5 workers: all clients see identical data.
    """

    async def event_generator():
        # Immediately send latest snapshot (so UI doesn't wait)
        latest = await hub.get_latest()
        if latest:
            yield {
                "event": "snapshot",
                "id": datetime.now().isoformat(),
                "data": latest.decode("utf-8"),
            }

        pubsub = await hub.subscribe()

        try:
            while True:
                # PubSub message
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg.get("type") == "message":
                    data = msg.get("data")
                    if isinstance(data, (bytes, bytearray)):
                        yield {
                            "event": "snapshot",
                            "id": datetime.now().isoformat(),
                            "data": data.decode("utf-8"),
                        }

                # Keep loop responsive
                await asyncio.sleep(0.05)

        except asyncio.CancelledError:
            # Client disconnected
            raise
        finally:
            await _safe_pubsub_close(pubsub)

    # SSE response
    return EventSourceResponse(event_generator())


# -----------------------------------------------------------------------------
# Startup hook helper (call this from your main FastAPI startup)
# -----------------------------------------------------------------------------
_started = False


def start_background_producer(
    refresh_sec: int = 5,
    stocklist_path: str = "routes/ANGEL_ONE/stockList.json",
    tokens_path: str = "tokens.json",
    interval_30m: str = "THIRTY_MINUTE",
    interval_day: str = "ONE_DAY",
    lookback_days_30m: int = 60,
    lookback_days_day: int = 520,
    candle_concurrency: int = 15,
) -> None:
    """
    Call this ONCE in your FastAPI startup event:

        from routes.ANGEL_ONE.live_server import start_background_producer
        @app.on_event("startup")
        async def startup():
            start_background_producer(refresh_sec=5)

    It is safe with multiple workers because of leader-lock.
    """
    global _started
    if _started:
        return
    _started = True

    asyncio.create_task(
        _producer_loop(
            refresh_sec=refresh_sec,
            stocklist_path=stocklist_path,
            tokens_path=tokens_path,
            interval_30m=interval_30m,
            interval_day=interval_day,
            lookback_days_30m=lookback_days_30m,
            lookback_days_day=lookback_days_day,
            candle_concurrency=candle_concurrency,
        )
    )
