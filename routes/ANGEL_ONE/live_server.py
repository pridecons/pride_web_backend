import os
import json
import asyncio
import hashlib
from datetime import datetime
from typing import Any, Dict, Optional, List, Tuple

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse
import redis.asyncio as redis

# ✅ Heavy snapshot builder (candles + indicators)
from routes.Angel_One.signals import (
    main as build_signals,
    load_stocklist,
    flatten_stocklist,
    chunk_tokens,
    parse_quote_map,
    score_signal,
)

# ✅ Quote API (FAST)
from routes.Angel_One.angel_data import quote_full_bulk

router = APIRouter(tags=["Angel One Live Signals"])

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

# Redis Keys
SNAPSHOT_KEY = "angel:signals:snapshot"
SNAPSHOT_TS_KEY = "angel:signals:snapshot:ts"
PUBSUB_CH = "angel:signals:pubsub"

# Cache indicators separately (so fast quotes can reuse)
IND_CACHE_KEY = "angel:signals:indicators_cache"
IND_TS_KEY = "angel:signals:indicators_cache:ts"

# Leader lock
LOCK_KEY = "angel:signals:leader"
LOCK_TTL_SEC = 30

def to_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)

def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

async def get_redis() -> redis.Redis:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    await r.ping()
    return r

# ---------------------------
# Leader election
# ---------------------------
async def try_become_leader(r: redis.Redis, leader_id: str) -> bool:
    return bool(await r.set(LOCK_KEY, leader_id, nx=True, ex=LOCK_TTL_SEC))

async def refresh_leader_lock(r: redis.Redis, leader_id: str) -> bool:
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
    await r.set(SNAPSHOT_KEY, s)
    await r.set(SNAPSHOT_TS_KEY, ts)
    await r.publish(PUBSUB_CH, s)

async def read_latest_snapshot(r: redis.Redis) -> Optional[str]:
    return await r.get(SNAPSHOT_KEY)

# ---------------------------
# Indicators cache
# ---------------------------
async def write_indicators_cache(r: redis.Redis, items: List[Dict[str, Any]]) -> None:
    """
    Store only indicators for each (exchange, token)
    """
    cache: Dict[str, Any] = {}
    for it in items:
        ex = str(it.get("exchange", "")).upper()
        tok = str(it.get("token", "")).strip()
        if not ex or not tok:
            continue
        cache[f"{ex}:{tok}"] = it.get("indicators") or {}
    await r.set(IND_CACHE_KEY, to_json(cache))
    await r.set(IND_TS_KEY, datetime.now().isoformat())

async def read_indicators_cache(r: redis.Redis) -> Dict[str, Any]:
    raw = await r.get(IND_CACHE_KEY)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}

# ---------------------------
# Producer
# ---------------------------
_producer_task: Optional[asyncio.Task] = None

def start_background_producer(
    # ✅ FAST live quotes refresh (LTP updates)
    fast_refresh_sec: int = 3,
    # ✅ Heavy refresh (candles+indicators)
    heavy_refresh_sec: int = 60,
    stocklist_path: str = "routes/Angel_One/stockList.json",
    tokens_path: str = "tokens.json",
    interval_30m: str = "THIRTY_MINUTE",
    interval_day: str = "ONE_DAY",
    lookback_days_30m: int = 60,
    lookback_days_day: int = 520,
    quote_chunk_size: int = 50,
    quote_sleep_s: float = 0.2,
    candle_concurrency: int = 15,  # kept for compatibility
):
    """
    ✅ FAST + LIGHT:
    - Every 2-3 sec: only quote_full_bulk (LTP) => fast updates
    - Every 60 sec: heavy build_signals => updates indicators cache
    - SSE publishes latest snapshot every fast tick
    """
    global _producer_task
    if _producer_task and not _producer_task.done():
        return

    leader_id = sha1_text(f"{os.getpid()}-{os.urandom(6).hex()}")

    async def _run():
        try:
            r = await get_redis()
        except Exception as e:
            print(f"[AngelProducer] Redis not available: {e}")
            return

        # Load base items once (static list)
        stocklist = load_stocklist(stocklist_path)
        base_items = flatten_stocklist(stocklist)  # category/exchange/token/name/symbol

        # Locks
        publish_lock = asyncio.Lock()
        heavy_lock = asyncio.Lock()

        async def heavy_loop():
            """
            Heavy indicators refresh every heavy_refresh_sec
            """
            while True:
                try:
                    async with heavy_lock:
                        # run heavy builder in thread
                        res = await asyncio.to_thread(
                            build_signals,
                            stocklist_path=stocklist_path,
                            tokens_path=tokens_path,
                            interval_30m=interval_30m,
                            interval_day=interval_day,
                            lookback_days_30m=lookback_days_30m,
                            lookback_days_day=lookback_days_day,
                        )
                        # cache indicators for fast loop
                        await write_indicators_cache(r, res.get("items") or [])
                        print(f"[AngelProducer] ✅ heavy indicators refreshed at {datetime.now().isoformat()}")
                except Exception as e:
                    print(f"[AngelProducer] ❌ heavy_loop error: {e}")
                await asyncio.sleep(max(5, int(heavy_refresh_sec)))

        async def fast_loop():
            """
            Fast quotes refresh (LTP) every fast_refresh_sec
            """
            while True:
                try:
                    # indicators cache (may be empty at first)
                    ind_cache = await read_indicators_cache(r)

                    # build quote maps in chunks (run in thread because requests)
                    quote_maps: Dict[Tuple[str, str], Dict[str, Any]] = {}
                    chunks = chunk_tokens(base_items, chunk_size=quote_chunk_size)

                    for ex_tokens in chunks:
                        resp = await asyncio.to_thread(
                            quote_full_bulk,
                            ex_tokens,
                            tokens_path,
                            3,
                        )
                        quote_maps.update(parse_quote_map(resp))
                        await asyncio.sleep(quote_sleep_s)

                    # build output items using latest quotes + cached indicators
                    out_items: List[Dict[str, Any]] = []
                    errors: List[Dict[str, Any]] = []

                    for it in base_items:
                        ex = it["exchange"]
                        tok = str(it["token"]).strip()

                        q = quote_maps.get((ex, tok))
                        if not q:
                            errors.append({"type": "QUOTE_MISSING", "item": it})
                            continue

                        indicators = ind_cache.get(f"{ex}:{tok}") or {}
                        ind30 = (indicators.get("30m") if isinstance(indicators, dict) else None) or {}

                        # decision based on latest quote + cached ind30
                        sig = score_signal(q, ind30)

                        out_items.append(
                            {
                                **it,
                                "quote_full": {
                                    "ltp": q.get("ltp"),
                                    "open": q.get("open"),
                                    "high": q.get("high"),
                                    "low": q.get("low"),
                                    "close": q.get("close"),
                                    "tradeVolume": q.get("tradeVolume"),
                                    "opnInterest": q.get("opnInterest"),
                                    "totBuyQuan": q.get("totBuyQuan"),
                                    "totSellQuan": q.get("totSellQuan"),
                                    "52WeekLow": q.get("52WeekLow"),
                                    "52WeekHigh": q.get("52WeekHigh"),
                                    "depth": q.get("depth"),
                                },
                                "indicators": indicators,  # may be empty until heavy loop runs
                                "decision": sig,
                            }
                        )

                    payload = {
                        "ok": True,
                        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "mode": "FAST_QUOTES",
                        "intervals": {"30m": interval_30m, "day": interval_day},
                        "refresh": {"fast_sec": fast_refresh_sec, "heavy_sec": heavy_refresh_sec},
                        "count": len(out_items),
                        "errors_count": len(errors),
                        "items": out_items,
                        "errors": errors,
                    }

                    async with publish_lock:
                        await publish_snapshot(r, payload)

                except Exception as e:
                    err_payload = {"ok": False, "error": str(e), "ts": datetime.now().isoformat()}
                    try:
                        async with publish_lock:
                            await publish_snapshot(r, err_payload)
                    except Exception:
                        pass

                await asyncio.sleep(max(1, int(fast_refresh_sec)))

        # Leader election loop
        while True:
            try:
                is_leader = await try_become_leader(r, leader_id)
                if not is_leader:
                    await asyncio.sleep(1.0)
                    continue

                print("[AngelProducer] ✅ Leader:", leader_id)

                # start both loops
                heavy_task = asyncio.create_task(heavy_loop())
                fast_task = asyncio.create_task(fast_loop())

                # keep leadership
                while True:
                    ok = await refresh_leader_lock(r, leader_id)
                    if not ok:
                        print("[AngelProducer] ⚠️ Lost leadership")
                        heavy_task.cancel()
                        fast_task.cancel()
                        break
                    await asyncio.sleep(5)

            except Exception as e:
                print(f"[AngelProducer] ❌ Leader loop error: {e}")
                await asyncio.sleep(2)

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
    Heavy one-shot (debug)
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
    r = await get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe(PUBSUB_CH)

    async def event_generator():
        try:
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
