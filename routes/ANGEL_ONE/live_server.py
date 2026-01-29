# routes/Angel_One/live_server.py
import os
import json
import asyncio
import hashlib
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

import redis.asyncio as redis

from routes.Angel_One.signals import main as build_signals  # your existing snapshot builder


router = APIRouter(tags=["Angel One Live Signals"])

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

# Keys / channels
SNAPSHOT_KEY = "angel:signals:snapshot"          # latest snapshot JSON
SNAPSHOT_TS_KEY = "angel:signals:snapshot:ts"   # ISO timestamp
PUBSUB_CH = "angel:signals:pubsub"              # publish snapshots here

# Leader lock
LOCK_KEY = "angel:signals:leader"
LOCK_TTL_SEC = 15  # lock auto-expires if leader dies


def to_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


async def get_redis() -> redis.Redis:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    # quick ping to fail early if misconfigured
    await r.ping()
    return r


# ---------------------------
# Leader election (SET NX EX)
# ---------------------------
async def try_become_leader(r: redis.Redis, leader_id: str) -> bool:
    # SET key value NX EX seconds
    return bool(await r.set(LOCK_KEY, leader_id, nx=True, ex=LOCK_TTL_SEC))


async def refresh_leader_lock(r: redis.Redis, leader_id: str) -> bool:
    """
    Keep lock only if we still own it.
    """
    cur = await r.get(LOCK_KEY)
    if cur != leader_id:
        return False
    await r.expire(LOCK_KEY, LOCK_TTL_SEC)
    return True


# ---------------------------
# Snapshot publish/store
# ---------------------------
async def publish_snapshot(r: redis.Redis, payload: Dict[str, Any]) -> None:
    s = to_json(payload)
    ts = datetime.now().isoformat()

    # store latest snapshot
    await r.set(SNAPSHOT_KEY, s)
    await r.set(SNAPSHOT_TS_KEY, ts)

    # publish to pubsub
    await r.publish(PUBSUB_CH, s)


async def read_latest_snapshot(r: redis.Redis) -> Optional[str]:
    return await r.get(SNAPSHOT_KEY)


# ---------------------------
# Background producer
# ---------------------------
_producer_task: Optional[asyncio.Task] = None


def start_background_producer(
    refresh_sec: int = 5,
    stocklist_path: str = "routes/Angel_One/stockList.json",
    tokens_path: str = "tokens.json",
    interval_30m: str = "THIRTY_MINUTE",
    interval_day: str = "ONE_DAY",
    lookback_days_30m: int = 60,
    lookback_days_day: int = 520,
    candle_concurrency: int = 15,   # kept for future (websocket/candles parallel)
):
    """
    Safe to call from every worker.
    Only ONE worker becomes leader and does work. Others do nothing.
    """
    global _producer_task
    if _producer_task and not _producer_task.done():
        return

    leader_id = sha1_text(f"{os.getpid()}-{os.urandom(6).hex()}")

    async def _run():
        try:
            r = await get_redis()
        except Exception as e:
            # If redis is down, we can't guarantee “no mismatch” across workers.
            # Better to stop producer than create inconsistent data.
            print(f"[AngelProducer] Redis not available: {e}")
            return

        # loop forever
        while True:
            try:
                # become leader or keep leadership
                is_leader = await try_become_leader(r, leader_id)
                if not is_leader:
                    # if not leader, just sleep
                    await asyncio.sleep(1.0)
                    continue

                # leader loop
                while True:
                    # ensure we still own lock
                    ok = await refresh_leader_lock(r, leader_id)
                    if not ok:
                        break  # lost leadership; go back to election loop

                    # build snapshot (REST-based now)
                    res = build_signals(
                        stocklist_path=stocklist_path,
                        tokens_path=tokens_path,
                        interval_30m=interval_30m,
                        interval_day=interval_day,
                        lookback_days_30m=lookback_days_30m,
                        lookback_days_day=lookback_days_day,
                    )

                    await publish_snapshot(r, res)

                    await asyncio.sleep(refresh_sec)

            except Exception as e:
                # do not crash the whole loop
                err_payload = {"ok": False, "error": str(e), "ts": datetime.now().isoformat()}
                try:
                    await publish_snapshot(r, err_payload)
                except Exception:
                    pass
                await asyncio.sleep(2.0)

    _producer_task = asyncio.create_task(_run())


# ---------------------------
# Routes
# ---------------------------
@router.get("/angel/health")
async def health():
    return {"ok": True, "ts": datetime.now().isoformat()}


@router.get("/angel/signals/once")
def signals_once():
    """
    One-shot snapshot (debug/cron) - still uses REST builder.
    """
    res = build_signals(
        stocklist_path="routes/Angel_One/stockList.json",
        tokens_path="tokens.json",
        interval_30m="THIRTY_MINUTE",
        interval_day="ONE_DAY",
        lookback_days_30m=60,
        lookback_days_day=520,
    )
    return JSONResponse(res)


@router.get("/angel/signals/stream")
async def signals_stream(
    ping_sec: int = Query(15, ge=5, le=60),
):
    """
    Multi-worker safe SSE:
    - Every worker subscribes to Redis pubsub
    - All clients get identical snapshots
    """

    r = await get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe(PUBSUB_CH)

    async def event_generator():
        try:
            # Send latest immediately (so UI shows data instantly)
            latest = await read_latest_snapshot(r)
            if latest:
                yield {"event": "snapshot", "id": datetime.now().isoformat(), "data": latest}
            else:
                yield {"event": "snapshot", "id": datetime.now().isoformat(), "data": to_json({"ok": True, "items": [], "note": "No snapshot yet"})}

            last_ping = asyncio.get_event_loop().time()

            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg.get("type") == "message":
                    yield {"event": "snapshot", "id": datetime.now().isoformat(), "data": msg["data"]}

                # keep-alive ping (prevents proxies closing SSE)
                now = asyncio.get_event_loop().time()
                if now - last_ping >= ping_sec:
                    yield {"event": "ping", "data": to_json({"ts": datetime.now().isoformat()})}
                    last_ping = now

                await asyncio.sleep(0.05)

        finally:
            try:
                await pubsub.unsubscribe(PUBSUB_CH)
                await pubsub.close()
            except Exception:
                pass

    return EventSourceResponse(event_generator())
