# live_server.py
import os
import json
import asyncio
import hashlib
from datetime import datetime, date
from typing import Any, Dict, Optional, List, Tuple

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse
import redis.asyncio as redis
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

# ✅ Heavy snapshot builder (candles + indicators)
from routes.AngelOne.signals import (
    main as build_signals,
    load_stocklist,
    flatten_stocklist,
    chunk_tokens,
    parse_quote_map,
    score_signal,
)

from routes.AngelOne.Grok_recomendation import generate_trade_plan_with_grok

# ✅ Quote API (FAST)
from routes.AngelOne.angel_data import quote_full_bulk

from db.connection import SessionLocal  # <-- change if your file name is different
from db.models import GrokRecommendation  # <-- change to your actual model import
import logging

logger = logging.getLogger(__name__)
# ✅ Optional realtime channel for Grok SSE (store in DB only; publish only for streaming)
GROK_PUBSUB_CH = "angel:grok:pubsub"

router = APIRouter(tags=["Angel One Live Signals"])

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

# Redis Keys (signals snapshot)
SNAPSHOT_KEY = "angel:signals:snapshot"
SNAPSHOT_TS_KEY = "angel:signals:snapshot:ts"
PUBSUB_CH = "angel:signals:pubsub"

# Cache indicators separately (so fast quotes can reuse)
IND_CACHE_KEY = "angel:signals:indicators_cache"
IND_TS_KEY = "angel:signals:indicators_cache:ts"

# Leader lock
LOCK_KEY = "angel:signals:leader"
LOCK_TTL_SEC = 180


def to_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def today_date() -> date:
    return date.today()


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
# Snapshot publish/store (signals)
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
# Indicators cache (Redis)
# ---------------------------
async def write_indicators_cache(r: redis.Redis, items: List[Dict[str, Any]]) -> None:
    """
    Store indicators for each (exchange, token)
    """
    cache: Dict[str, Any] = {}
    stored = 0
    for it in items:
        ex = str(it.get("exchange", "")).upper().strip()
        tok = str(it.get("token", "")).strip()
        if not ex or not tok:
            continue

        indicators = it.get("indicators") or {}
        if isinstance(indicators, dict) and indicators:
            cache[f"{ex}:{tok}"] = indicators
            stored += 1

    await r.set(IND_CACHE_KEY, to_json(cache))
    await r.set(IND_TS_KEY, datetime.now().isoformat())
    print(f"[AngelProducer] ✅ indicators_cache stored={stored} keys (total_cache={len(cache)})")


async def read_indicators_cache(r: redis.Redis) -> Dict[str, Any]:
    raw = await r.get(IND_CACHE_KEY)
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


async def read_indicators_ts(r: redis.Redis) -> Optional[str]:
    return await r.get(IND_TS_KEY)


# ---------------------------
# Local trade plan (fallback for Grok)
# ---------------------------
def build_local_trade_plan(*, signal: str, score: int, quote_full: Dict[str, Any]) -> Dict[str, Any]:
    """
    Conservative local plan so Grok can validate and expand rationale.
    Uses day range when available (high/low), else fallback to %.
    """
    ltp = quote_full.get("ltp")
    try:
        entry = float(ltp)
    except Exception:
        entry = None

    if entry is None or entry <= 0:
        return {
            "direction": "WAIT",
            "entry": None,
            "stop_loss": None,
            "targets": {"t1": None, "t2": None, "t3": None},
            "timeframe": "intraday",
            "note": "No valid LTP",
        }

    # stop distance
    stop_dist = None
    try:
        hi = quote_full.get("high")
        lo = quote_full.get("low")
        if hi is not None and lo is not None:
            hi = float(hi)
            lo = float(lo)
            rng = max(0.0, hi - lo)
            stop_dist = max(rng * 0.35, entry * 0.006)  # at least 0.6%
    except Exception:
        stop_dist = None

    if not stop_dist:
        stop_dist = entry * 0.0075  # 0.75% fallback

    direction = str(signal or "WAIT").upper()
    if direction not in ("BUY", "SELL"):
        direction = "BUY" if score > 0 else "SELL"

    if direction == "BUY":
        sl = entry - stop_dist
        t1 = entry + stop_dist * 1.0
        t2 = entry + stop_dist * 1.7
        t3 = entry + stop_dist * 2.4
    else:
        sl = entry + stop_dist
        t1 = entry - stop_dist * 1.0
        t2 = entry - stop_dist * 1.7
        t3 = entry - stop_dist * 2.4

    return {
        "direction": direction,
        "entry": round(entry, 4),
        "stop_loss": round(sl, 4),
        "targets": {"t1": round(t1, 4), "t2": round(t2, 4), "t3": round(t3, 4)},
        "timeframe": "intraday",
        "note": "Local RR ladder plan; validate manually.",
    }


# ---------------------------
# DB helpers (Grok recommendations)
# ---------------------------
def grok_exists_today(db: Session, *, trade_date: date, exchange: str, token: str) -> bool:
    """
    Check if (trade_date, exchange, token) already exists in DB.
    """
    return (
        db.query(GrokRecommendation.id)
        .filter(
            GrokRecommendation.trade_date == trade_date,
            GrokRecommendation.exchange == exchange,
            GrokRecommendation.token == token,
        )
        .first()
        is not None
    )


def insert_grok_reco(db: Session, payload: Dict[str, Any]) -> bool:
    """
    Insert recommendation. Returns True if inserted, False if duplicate (unique constraint).
    """
    row = GrokRecommendation(**payload)
    db.add(row)
    try:
        db.commit()
        return True
    except IntegrityError:
        db.rollback()
        return False
    except Exception:
        db.rollback()
        raise


def serialize_grok_row(r: GrokRecommendation) -> Dict[str, Any]:
    return {
        "id": r.id,
        "trade_date": str(r.trade_date),
        "exchange": r.exchange,
        "token": r.token,
        "symbol": getattr(r, "symbol", None),
        "name": getattr(r, "name", None),
        "tradingsymbol": getattr(r, "tradingsymbol", None),
        "category": getattr(r, "category", None),
        "score": getattr(r, "score", None),
        "signal": getattr(r, "signal", None),
        "quote_full": getattr(r, "quote_full", None),
        "indicators": getattr(r, "indicators", None),
        "local_plan": getattr(r, "local_plan", None),
        "grok_plan": getattr(r, "grok_plan", None),
        "news": getattr(r, "news", None),
        "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
    }


# ---------------------------
# Producer
# ---------------------------
_producer_task: Optional[asyncio.Task] = None


def start_background_producer(
    fast_refresh_sec: int = 3,
    heavy_refresh_sec: int = 60,
    stocklist_path: str = "routes/AngelOne/stockList.json",
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
    - Every fast_refresh_sec: only quote_full_bulk (LTP) => fast updates
    - Every heavy_refresh_sec: heavy build_signals => updates indicators cache
    - SSE publishes latest snapshot every fast tick

    ✅ GROK:
    - If score > 4 or < -4 => generate recommendation
    - Store recommendation in DB (no redis cache)
    - Same day duplicate check = DB + unique constraint
    - Optional publish on GROK_PUBSUB_CH for realtime SSE
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

        stocklist = load_stocklist(stocklist_path)
        base_items = flatten_stocklist(stocklist)

        publish_lock = asyncio.Lock()
        heavy_lock = asyncio.Lock()

        async def run_heavy_once(tag: str = "manual"):
            try:
                async with heavy_lock:
                    res = await asyncio.to_thread(
                        build_signals,
                        stocklist_path=stocklist_path,
                        tokens_path=tokens_path,
                        interval_30m=interval_30m,
                        interval_day=interval_day,
                        lookback_days_30m=lookback_days_30m,
                        lookback_days_day=lookback_days_day,
                        min_candles_30m=20,
                        min_candles_day=20,
                    )
                    items = (res.get("items") or [])
                    logger.info(f"[AngelProducer][HEAVY] starting heavy run for {len(items)} stocks")

                    await write_indicators_cache(r, items)
                    print(f"[AngelProducer] ✅ heavy({tag}) refreshed at {datetime.now().isoformat()} items={len(items)}")
            except Exception as e:
                print(f"[AngelProducer] ❌ heavy({tag}) error: {e}")

        async def heavy_loop():
            while True:
                await run_heavy_once(tag="loop")
                await asyncio.sleep(max(5, int(heavy_refresh_sec)))

        async def fast_loop():
            while True:
                try:
                    ind_cache = await read_indicators_cache(r)
                    ind_ts = await read_indicators_ts(r)

                    if not ind_cache and not ind_ts:
                        await run_heavy_once(tag="warmup")
                        ind_cache = await read_indicators_cache(r)
                        ind_ts = await read_indicators_ts(r)

                    quote_maps: Dict[Tuple[str, str], Dict[str, Any]] = {}
                    chunks = chunk_tokens(base_items, chunk_size=quote_chunk_size)

                    for ex_tokens in chunks:
                        resp = await asyncio.to_thread(quote_full_bulk, ex_tokens, tokens_path, 3)
                        data = (resp or {}).get("data") or {}
                        fetched = data.get("fetched") or []
                        if not fetched:
                            print("[AngelProducer] quote_full_bulk empty:",
                                "status=", resp.get("status"),
                                "message=", resp.get("message"),
                                "errorcode=", resp.get("errorcode"))
                        quote_maps.update(parse_quote_map(resp))
                        await asyncio.sleep(quote_sleep_s)

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
                        sig = score_signal(q, ind30)

                        # ✅ GROK recommendation trigger
                        try:
                            sc = int(sig.get("score") or 0)
                        except Exception:
                            sc = 0

                        if sc > 4 or sc < -4:
                            tdate = today_date()

                            # 1) Quick DB exists check (non-blocking via thread)
                            def _should_generate() -> bool:
                                db = SessionLocal()
                                try:
                                    return not grok_exists_today(db, trade_date=tdate, exchange=ex, token=tok)
                                finally:
                                    db.close()

                            should_generate = await asyncio.to_thread(_should_generate)

                            if should_generate:
                                indicators_day = (indicators.get("day") if isinstance(indicators, dict) else None) or {}
                                local_plan = build_local_trade_plan(
                                    signal=str(sig.get("signal") or "WAIT"),
                                    score=sc,
                                    quote_full={
                                        "ltp": q.get("ltp"),
                                        "open": q.get("open"),
                                        "high": q.get("high"),
                                        "low": q.get("low"),
                                        "close": q.get("close"),
                                        "tradeVolume": q.get("tradeVolume"),
                                        "totBuyQuan": q.get("totBuyQuan"),
                                        "totSellQuan": q.get("totSellQuan"),
                                    },
                                )

                                # 2) Call grok (in thread)
                                try:
                                    grok_plan = await asyncio.to_thread(
                                        generate_trade_plan_with_grok,
                                        symbol=it.get("symbol") or it.get("name") or it.get("tradingsymbol") or "",
                                        exchange=ex,
                                        score=sc,
                                        signal=str(sig.get("signal") or "WAIT"),
                                        quote_full={
                                            "ltp": q.get("ltp"),
                                            "open": q.get("open"),
                                            "high": q.get("high"),
                                            "low": q.get("low"),
                                            "close": q.get("close"),
                                            "tradeVolume": q.get("tradeVolume"),
                                            "totBuyQuan": q.get("totBuyQuan"),
                                            "totSellQuan": q.get("totSellQuan"),
                                            "52WeekLow": q.get("52WeekLow"),
                                            "52WeekHigh": q.get("52WeekHigh"),
                                        },
                                        indicators_30m=ind30,
                                        indicators_day=indicators_day,
                                        local_plan=local_plan,
                                        news_context=[],
                                        timeout_sec=20,
                                    )
                                except Exception as e:
                                    grok_plan = {
                                        "symbol": it.get("symbol") or it.get("name") or "",
                                        "exchange": ex,
                                        "direction": "BUY" if str(sig.get("signal")) == "BUY" else "SELL",
                                        "entry": (q.get("ltp") or None),
                                        "stop_loss": (local_plan or {}).get("stop_loss"),
                                        "targets": (local_plan or {}).get("targets") or {"t1": None, "t2": None, "t3": None},
                                        "timeframe": "intraday",
                                        "confidence": 0,
                                        "rationale": ["Grok call failed", str(e)],
                                        "risk_notes": ["Validate manually before taking any trade."],
                                        "news": [],
                                    }

                                # 3) DB payload
                                db_payload = {
                                    "trade_date": tdate,
                                    "exchange": ex,
                                    "token": tok,
                                    "symbol": it.get("symbol"),
                                    "name": it.get("name"),
                                    "tradingsymbol": it.get("tradingsymbol"),
                                    "category": it.get("category"),
                                    "score": sc,
                                    "signal": str(sig.get("signal") or "WAIT"),
                                    "quote_full": {
                                        "ltp": q.get("ltp"),
                                        "open": q.get("open"),
                                        "high": q.get("high"),
                                        "low": q.get("low"),
                                        "close": q.get("close"),
                                        "tradeVolume": q.get("tradeVolume"),
                                        "totBuyQuan": q.get("totBuyQuan"),
                                        "totSellQuan": q.get("totSellQuan"),
                                    },
                                    "indicators": {"30m": ind30, "day": indicators_day},
                                    "local_plan": local_plan,
                                    "grok_plan": grok_plan,
                                    "news": grok_plan.get("news") if isinstance(grok_plan, dict) else [],
                                }

                                # 4) Insert (duplicate-safe via unique constraint)
                                def _insert() -> bool:
                                    db = SessionLocal()
                                    try:
                                        return insert_grok_reco(db, db_payload)
                                    finally:
                                        db.close()

                                inserted = await asyncio.to_thread(_insert)

                                # 5) Optional realtime publish (DB remains source of truth)
                                if inserted:
                                    try:
                                        await r.publish(GROK_PUBSUB_CH, to_json(db_payload))
                                    except Exception:
                                        pass

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
                                "indicators": indicators,
                                "decision": sig,
                            }
                        )

                    payload = {
                        "ok": True,
                        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "mode": "FAST_QUOTES",
                        "intervals": {"30m": interval_30m, "day": interval_day},
                        "refresh": {"fast_sec": fast_refresh_sec, "heavy_sec": heavy_refresh_sec},
                        "indicators_cache_ts": ind_ts,
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

                lost = asyncio.Event()

                async def lock_keeper():
                    # ✅ keeps the lock alive even during heavy warmup
                    try:
                        while True:
                            ok = await refresh_leader_lock(r, leader_id)
                            if not ok:
                                lost.set()
                                break
                            await asyncio.sleep(5)
                    except Exception:
                        lost.set()

                keeper_task = asyncio.create_task(lock_keeper())

                # ✅ Warmup (heavy) - lock will NOT expire now
                await run_heavy_once(tag="leader_start")

                heavy_task = asyncio.create_task(heavy_loop())
                fast_task = asyncio.create_task(fast_loop())

                # ✅ wait until leadership is lost
                await lost.wait()

                print("[AngelProducer] ⚠️ Lost leadership")
                heavy_task.cancel()
                fast_task.cancel()
                keeper_task.cancel()

                # small backoff
                await asyncio.sleep(1)

            except Exception as e:
                print(f"[AngelProducer] ❌ Leader loop error: {e}")
                await asyncio.sleep(2)


            except Exception as e:
                print(f"[AngelProducer] ❌ Leader loop error: {e}")
                await asyncio.sleep(2)

    _producer_task = asyncio.create_task(_run())


# ---------------------------
# Routes (Signals)
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
        stocklist_path="routes/AngelOne/stockList.json",
        tokens_path="tokens.json",
        interval_30m="THIRTY_MINUTE",
        interval_day="ONE_DAY",
        lookback_days_30m=60,
        lookback_days_day=520,
        min_candles_30m=20,
        min_candles_day=20,
    )
    return JSONResponse(res)


@router.get("/angel/signals/stream")
async def signals_stream(ping_sec: int = Query(15, ge=5, le=60)):
    r = await get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe(PUBSUB_CH)

    async def event_generator():
        try:
            latest = await read_latest_snapshot(r)
            if latest:
                yield {"event": "snapshot", "id": datetime.now().isoformat(), "data": latest}
            else:
                yield {
                    "event": "snapshot",
                    "id": datetime.now().isoformat(),
                    "data": to_json({"ok": True, "items": [], "note": "No snapshot yet"}),
                }

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


# ---------------------------
# Routes (Grok Recommendations - DB source of truth)
# ---------------------------
@router.get("/angel/grok/recommendations")
async def grok_recommendations(limit: int = Query(50, ge=1, le=200)):
    def _read():
        db = SessionLocal()
        try:
            rows = (
                db.query(GrokRecommendation)
                .order_by(GrokRecommendation.id.desc())
                .limit(int(limit))
                .all()
            )
            return [serialize_grok_row(x) for x in rows]
        finally:
            db.close()

    items = await asyncio.to_thread(_read)
    return {"ok": True, "count": len(items), "items": items}


@router.get("/angel/grok/recommendations/today")
async def grok_recommendations_today(limit: int = Query(200, ge=1, le=500)):
    tdate = today_date()

    def _read():
        db = SessionLocal()
        try:
            rows = (
                db.query(GrokRecommendation)
                .filter(GrokRecommendation.trade_date == tdate)
                .order_by(GrokRecommendation.id.desc())
                .limit(int(limit))
                .all()
            )
            return [serialize_grok_row(x) for x in rows]
        finally:
            db.close()

    items = await asyncio.to_thread(_read)
    return {"ok": True, "trade_date": str(tdate), "count": len(items), "items": items}


@router.get("/angel/grok/stream")
async def grok_stream(ping_sec: int = Query(15, ge=5, le=60)):
    """
    Realtime SSE for new Grok recos.
    DB stores the reco; this stream listens on Redis pubsub channel for new inserts.
    """
    r = await get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe(GROK_PUBSUB_CH)

    async def event_generator():
        try:
            # Send latest DB record once on connect (init)
            def _latest():
                db = SessionLocal()
                try:
                    row = db.query(GrokRecommendation).order_by(GrokRecommendation.id.desc()).first()
                    return serialize_grok_row(row) if row else None
                finally:
                    db.close()

            latest = await asyncio.to_thread(_latest)
            if latest:
                yield {"event": "reco", "id": datetime.now().isoformat(), "data": to_json(latest)}
            else:
                yield {"event": "reco", "id": datetime.now().isoformat(), "data": to_json({"ok": True, "note": "No reco yet"})}

            last_ping = asyncio.get_event_loop().time()

            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg.get("type") == "message":
                    yield {"event": "reco", "id": datetime.now().isoformat(), "data": msg["data"]}

                now = asyncio.get_event_loop().time()
                if now - last_ping >= ping_sec:
                    yield {"event": "ping", "data": to_json({"ts": datetime.now().isoformat()})}
                    last_ping = now

                await asyncio.sleep(0.05)

        finally:
            try:
                await pubsub.unsubscribe(GROK_PUBSUB_CH)
                await pubsub.close()
            except Exception:
                pass

    return EventSourceResponse(event_generator())
