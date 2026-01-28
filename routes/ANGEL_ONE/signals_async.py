# routes/ANGEL_ONE/signals_async.py
"""
Async signal builder (FAST) for live streaming:
- Uses async Angel One calls (aquote_full_bulk / aget_candles)
- Bounded concurrency for candles
- Small per-process TTL cache (works best when you use "single leader producer" architecture)
- Keeps your existing business logic intact (candles_to_df, compute_indicators, score_signal)

Usage:
    from routes.ANGEL_ONE.signals_async import build_signals_async
    res = await build_signals_async(...)
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd

from routes.ANGEL_ONE.angel_data import aquote_full_bulk, aget_candles
from routes.ANGEL_ONE.indicators import compute_indicators
from routes.ANGEL_ONE.signals import (
    load_stocklist,
    flatten_stocklist,
    chunk_tokens,
    parse_quote_map,
    candles_to_df,
    score_signal,
)

# -----------------------------------------------------------------------------
# Optional in-memory TTL cache (per worker process)
# -----------------------------------------------------------------------------
# Key: (exchange, token, interval, fromdate, todate)
# Value: (ts, raw_resp_dict)
_CANDLE_CACHE: Dict[Tuple[str, str, str, str, str], Tuple[float, Dict[str, Any]]] = {}


def _cache_get(key: Tuple[str, str, str, str, str], ttl_sec: int) -> Optional[Dict[str, Any]]:
    now = time.time()
    v = _CANDLE_CACHE.get(key)
    if not v:
        return None
    ts, data = v
    if now - ts > ttl_sec:
        _CANDLE_CACHE.pop(key, None)
        return None
    return data


def _cache_set(key: Tuple[str, str, str, str, str], data: Dict[str, Any]) -> None:
    _CANDLE_CACHE[key] = (time.time(), data)


def _safe_sleep(s: float) -> asyncio.Future:
    return asyncio.sleep(s)


# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------
async def build_signals_async(
    stocklist_path: str = "routes/ANGEL_ONE/stockList.json",
    tokens_path: str = "tokens.json",
    interval_30m: str = "THIRTY_MINUTE",
    interval_day: str = "ONE_DAY",
    lookback_days_30m: int = 60,
    lookback_days_day: int = 520,
    quote_chunk_size: int = 50,
    quote_sleep_s: float = 0.15,          # small pause to reduce burst
    min_candles_30m: int = 60,
    min_candles_day: int = 60,
    candle_concurrency: int = 15,         # tune for your VPS + Angel limits
    candle_cache_ttl_30m: int = 20,       # seconds
    candle_cache_ttl_day: int = 60,       # seconds
) -> Dict[str, Any]:
    """
    Returns the same response shape as your sync `signals.main()`:
        {
          ok, generated_at, intervals, lookbacks,
          count, errors_count,
          items: [...],
          errors: [...]
        }
    """

    # 0) Load list + flatten
    stocklist = load_stocklist(stocklist_path)
    items = flatten_stocklist(stocklist)

    # 1) QUOTES (bulk, chunked)
    quote_maps: Dict[Tuple[str, str], Dict[str, Any]] = {}
    chunks = chunk_tokens(items, chunk_size=quote_chunk_size)

    for ex_tokens in chunks:
        resp = await aquote_full_bulk(ex_tokens, tokens_path=tokens_path, max_retries=3)
        quote_maps.update(parse_quote_map(resp))
        if quote_sleep_s:
            await _safe_sleep(quote_sleep_s)

    # 2) Time windows for candles
    now = datetime.now()

    from_30m = (now - timedelta(days=lookback_days_30m)).strftime("%Y-%m-%d %H:%M")
    to_30m = now.strftime("%Y-%m-%d %H:%M")

    from_day = (now - timedelta(days=lookback_days_day)).strftime("%Y-%m-%d %H:%M")
    to_day = now.strftime("%Y-%m-%d %H:%M")

    sem = asyncio.Semaphore(max(1, int(candle_concurrency)))

    async def fetch_item(it: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Returns (item_out, err_dict)
        """
        ex = str(it.get("exchange", "")).upper()
        tok = str(it.get("token", "")).strip()

        q = quote_maps.get((ex, tok))
        if not q:
            return None, {"type": "QUOTE_MISSING", "item": it}

        # -------- 30m candles --------
        key30 = (ex, tok, interval_30m, from_30m, to_30m)
        async with sem:
            c30 = _cache_get(key30, candle_cache_ttl_30m)
            if c30 is None:
                c30 = await aget_candles(
                    ex, tok, interval_30m, from_30m, to_30m,
                    tokens_path=tokens_path,
                    max_retries=4,
                )
                _cache_set(key30, c30)

        df30 = candles_to_df(c30)
        if df30 is None or len(df30) < min_candles_30m:
            return None, {"type": "CANDLE_30M_MISSING", "item": it, "raw": c30}

        # Indicators (CPU)
        try:
            ind30 = compute_indicators(df30)
        except Exception as e:
            return None, {"type": "INDICATOR_30M_FAIL", "item": it, "error": str(e)}

        # -------- DAY candles --------
        keyd = (ex, tok, interval_day, from_day, to_day)
        async with sem:
            cday = _cache_get(keyd, candle_cache_ttl_day)
            if cday is None:
                cday = await aget_candles(
                    ex, tok, interval_day, from_day, to_day,
                    tokens_path=tokens_path,
                    max_retries=4,
                )
                _cache_set(keyd, cday)

        dfday = candles_to_df(cday)
        if dfday is None or len(dfday) < min_candles_day:
            return None, {"type": "CANDLE_DAY_MISSING", "item": it, "raw": cday}

        try:
            indday = compute_indicators(dfday)
        except Exception as e:
            return None, {"type": "INDICATOR_DAY_FAIL", "item": it, "error": str(e)}

        # Signal score (your logic)
        sig = score_signal(q, ind30)

        out = {
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
            "indicators": {"30m": ind30, "day": indday},
            "decision": sig,
        }

        return out, None

    # 3) Parallel fetch all items
    tasks = [fetch_item(it) for it in items]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    out_items: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for item_out, err in results:
        if err:
            errors.append(err)
        elif item_out:
            out_items.append(item_out)

    return {
        "ok": True,
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "intervals": {"30m": interval_30m, "day": interval_day},
        "lookbacks": {"30m_days": lookback_days_30m, "day_days": lookback_days_day},
        "count": len(out_items),
        "errors_count": len(errors),
        "items": out_items,
        "errors": errors,
    }
