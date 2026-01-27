# routes/NSE/Preopen_Movers.py

import logging
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from db.connection import get_db
from db.models import (
    NseIndexMaster,
    NseCmIntraday1Min,
    NseCmSecurity,
    PreopenMoversCache,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/preopen-movers", tags=["Preopen Movers"])

INDEX_MAP = {
    "NIFTY50": ("NIFTY 50", "NIFTY50"),
    "NIFTY100": ("NIFTY 100", "NIFTY100"),
    "NIFTY500": ("NIFTY 500", "NIFTY500"),
}

# ✅ Preopen window (UTC) — as per your data timestamps
PREOPEN_UTC_START = "03:30:00"
PREOPEN_UTC_END = "03:45:00"

IST = ZoneInfo("Asia/Kolkata")

# after this time, we prefer cached response (fast)
CACHE_PREFER_AFTER_IST = dtime(9, 20)  # 09:20 IST


# -----------------------------
# Helpers
# -----------------------------
def _get_index_row(db: Session, index_code: str) -> NseIndexMaster:
    index_code = (index_code or "").upper().strip()
    if index_code not in INDEX_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported index '{index_code}'. Use NIFTY50 / NIFTY100 / NIFTY500.",
        )

    full_name, short_code = INDEX_MAP[index_code]

    row = (
        db.query(NseIndexMaster)
        .filter(
            (NseIndexMaster.index_symbol == full_name)
            | (NseIndexMaster.short_code == short_code)
        )
        .one_or_none()
    )

    if row is None:
        row = (
            db.query(NseIndexMaster)
            .filter(func.lower(NseIndexMaster.index_symbol) == full_name.lower())
            .one_or_none()
        )

    if row is None:
        row = (
            db.query(NseIndexMaster)
            .filter(func.lower(NseIndexMaster.short_code) == short_code.lower())
            .one_or_none()
        )

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Index not found in NseIndexMaster: {index_code}",
        )

    return row


def _now_ist_time() -> dtime:
    return datetime.now(tz=IST).time()


def _read_cache(db: Session, index_id: int, trade_date, limit: int):
    return (
        db.query(PreopenMoversCache)
        .filter(
            PreopenMoversCache.index_id == index_id,
            PreopenMoversCache.trade_date == trade_date,
            PreopenMoversCache.limit == limit,
        )
        .one_or_none()
    )


def _upsert_cache(db: Session, index_id: int, trade_date, limit: int, gainers: list, losers: list):
    row = _read_cache(db, index_id, trade_date, limit)
    if row:
        row.gainers = gainers
        row.losers = losers
    else:
        row = PreopenMoversCache(
            index_id=index_id,
            trade_date=trade_date,
            limit=limit,
            gainers=gainers,
            losers=losers,
        )
        db.add(row)
    db.commit()
    return row


def _compute_preopen_snapshot_fast(db: Session, index_row: NseIndexMaster, limit: int):
    """
    Ultra-fast:
    - token universe inside SQL (no python list)
    - per token LATERAL queries using index (token_id, trade_date, interval_start)
    - no interval_start::time (index-friendly timestamp range)
    """

    # 1) latest_trade_date (fast)
    latest_trade_date = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date))
    ).scalar()

    if latest_trade_date is None:
        raise HTTPException(status_code=404, detail="No intraday data found")

    # 2) prev_trade_date (fast)
    prev_trade_date = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date))
        .where(NseCmIntraday1Min.trade_date < latest_trade_date)
    ).scalar()

    if prev_trade_date is None:
        raise HTTPException(status_code=404, detail="Previous trading day intraday data not found")

    sql = text(f"""
WITH tokens AS (
  SELECT DISTINCT s.token_id
  FROM nse_index_constituent c
  JOIN nse_cm_securities s ON s.symbol = c.symbol
  WHERE c.index_id = :index_id
    AND s.series = 'EQ'
)
SELECT
  s.token_id,
  s.symbol,
  s.company_name,

  COALESCE(pre.preopen_price, df.dayfirst_price) AS preopen_price,
  COALESCE(pre.preopen_time,  df.dayfirst_time)  AS preopen_time,

  pl.prev_close,

  (COALESCE(pre.preopen_price, df.dayfirst_price) - pl.prev_close) AS preopen_change_abs,
  ((COALESCE(pre.preopen_price, df.dayfirst_price) - pl.prev_close) * 100.0 / NULLIF(pl.prev_close, 0.0)) AS preopen_change_pct

FROM tokens t
JOIN nse_cm_securities s
  ON s.token_id = t.token_id
 AND s.series = 'EQ'

-- ✅ day first candle (index seek)
LEFT JOIN LATERAL (
  SELECT
    timezone('Asia/Kolkata', i.interval_start) AS dayfirst_time,
    COALESCE(i.last_price, i.close_price)      AS dayfirst_price
  FROM nse_cm_intraday_1min i
  WHERE i.trade_date = :latest_trade_date
    AND i.token_id = s.token_id
  ORDER BY i.interval_start ASC
  LIMIT 1
) df ON TRUE

-- ✅ preopen first candle (index seek + timestamp range)
LEFT JOIN LATERAL (
  SELECT
    timezone('Asia/Kolkata', i.interval_start) AS preopen_time,
    COALESCE(i.last_price, i.close_price)      AS preopen_price
  FROM nse_cm_intraday_1min i
  WHERE i.trade_date = :latest_trade_date
    AND i.token_id = s.token_id
    AND i.interval_start >= (:latest_trade_date::date + time '{PREOPEN_UTC_START}')
    AND i.interval_start <  (:latest_trade_date::date + time '{PREOPEN_UTC_END}')
  ORDER BY i.interval_start ASC
  LIMIT 1
) pre ON TRUE

-- ✅ prev close = prev day last candle (index seek)
JOIN LATERAL (
  SELECT
    COALESCE(i.close_price, i.last_price) AS prev_close
  FROM nse_cm_intraday_1min i
  WHERE i.trade_date = :prev_trade_date
    AND i.token_id = s.token_id
  ORDER BY i.interval_start DESC
  LIMIT 1
) pl ON TRUE

WHERE df.dayfirst_price IS NOT NULL
  AND pl.prev_close IS NOT NULL
  AND pl.prev_close <> 0
""")

    rows = db.execute(
        sql,
        {
            "index_id": index_row.id,
            "latest_trade_date": latest_trade_date,
            "prev_trade_date": prev_trade_date,
        },
    ).mappings().all()

    def _f(x):
        return float(x) if x is not None else None

    def _mini(r: dict) -> dict:
        return {
            "token_id": r["token_id"],
            "symbol": r["symbol"],
            "company_name": r["company_name"],
            "prev_close": _f(r["prev_close"]),
            "preopen_price": _f(r["preopen_price"]),
            "preopen_time": r["preopen_time"].isoformat() if r["preopen_time"] else None,
            "preopen_change_abs": _f(r["preopen_change_abs"]),
            "preopen_change_pct": _f(r["preopen_change_pct"]),
        }

    gainers_sorted = sorted(
        (r for r in rows if r.get("preopen_change_pct") is not None and r["preopen_change_pct"] > 0),
        key=lambda x: x["preopen_change_pct"],
        reverse=True,
    )[:limit]

    losers_sorted = sorted(
        (r for r in rows if r.get("preopen_change_pct") is not None and r["preopen_change_pct"] < 0),
        key=lambda x: x["preopen_change_pct"],
    )[:limit]

    gainers = [_mini(r) for r in gainers_sorted]
    losers = [_mini(r) for r in losers_sorted]

    payload = {
        "index_id": index_row.id,
        "code": index_row.short_code,
        "name": index_row.index_symbol,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "gainers_count": len(gainers),
        "losers_count": len(losers),
        "gainers": gainers,
        "losers": losers,
    }

    return payload, latest_trade_date


# -----------------------------
# Endpoint (cache-first)
# -----------------------------
@router.get("/")
async def preopen_movers(
    index: str = Query("NIFTY50", description="Index: NIFTY50 / NIFTY100 / NIFTY500"),
    limit: int = Query(3, ge=1, le=50, description="Top gainers/losers to return"),
    mode: str = Query("auto", description="auto | cached | live"),
    db: Session = Depends(get_db),
):
    index_row = _get_index_row(db, index)

    # ✅ cached mode
    if mode == "cached":
        cached = (
            db.query(PreopenMoversCache)
            .filter(
                PreopenMoversCache.index_id == index_row.id,
                PreopenMoversCache.limit == limit,
            )
            .order_by(PreopenMoversCache.trade_date.desc())
            .first()
        )
        if not cached:
            raise HTTPException(status_code=404, detail="Cache not available yet.")
        return {
            "index_id": index_row.id,
            "code": index_row.short_code,
            "name": index_row.index_symbol,
            "trade_date": cached.trade_date.isoformat(),
            "cached": True,
            "gainers_count": len(cached.gainers or []),
            "losers_count": len(cached.losers or []),
            "gainers": cached.gainers,
            "losers": cached.losers,
        }

    # ✅ AUTO: after 09:20 -> cache-first
    if mode == "auto" and _now_ist_time() >= CACHE_PREFER_AFTER_IST:
        cached = (
            db.query(PreopenMoversCache)
            .filter(
                PreopenMoversCache.index_id == index_row.id,
                PreopenMoversCache.limit == limit,
            )
            .order_by(PreopenMoversCache.trade_date.desc())
            .first()
        )
        if cached:
            return {
                "index_id": index_row.id,
                "code": index_row.short_code,
                "name": index_row.index_symbol,
                "trade_date": cached.trade_date.isoformat(),
                "cached": True,
                "gainers_count": len(cached.gainers or []),
                "losers_count": len(cached.losers or []),
                "gainers": cached.gainers,
                "losers": cached.losers,
            }
        # cache missing -> fallthrough to compute once & store

    # ✅ live compute
    payload, latest_trade_date = _compute_preopen_snapshot_fast(db, index_row, limit)

    # after 09:20 store (freeze)
    if _now_ist_time() >= CACHE_PREFER_AFTER_IST:
        _upsert_cache(
            db,
            index_id=index_row.id,
            trade_date=latest_trade_date,
            limit=limit,
            gainers=payload["gainers"],
            losers=payload["losers"],
        )
        payload["cached"] = True
    else:
        payload["cached"] = False

    return payload
