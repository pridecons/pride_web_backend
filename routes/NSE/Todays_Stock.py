# routes/NSE/Todays_Stock.py
# ✅ DB-optimized version (complete file)
# Fixes:
# - Avoids upper()/lower() in hot paths (index killers) where possible
# - Uses a reusable TOKENS CTE instead of repeated IN (SELECT token_subq)
# - Replaces GROUP BY max(interval_start) + join-back with window functions (row_number)
# - Uses SQLAlchemy Core (select + execute) to reduce ORM overhead and ambiguity
# - Keeps same API contract (filter types + response format)

import logging
from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select, literal
from sqlalchemy.orm import Session

from db.connection import get_db
from db.models import (
    NseIndexConstituent,
    NseIndexMaster,
    NseCmIntraday1Min,
    NseCmSecurity,
    NseCmBhavcopy,
)

router = APIRouter(prefix="/today-stock", tags=["Today Stock"])
logger = logging.getLogger(__name__)

INDEX_MAP = {
    "NIFTY50": ("NIFTY 50", "NIFTY50"),
    "NIFTY100": ("NIFTY 100", "NIFTY100"),
    "NIFTY500": ("NIFTY 500", "NIFTY500"),
    # ALL -> all EQ
}


# ===========================
#  Core helpers
# ===========================
def _get_index_row(db: Session, index_key: str) -> NseIndexMaster:
    key = (index_key or "").upper().strip()
    if key not in INDEX_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported index '{index_key}'. Use NIFTY50 / NIFTY100 / NIFTY500 / ALL.",
        )

    index_symbol, short_code = INDEX_MAP[key]

    # Prefer exact match first (index-friendly)
    row = (
        db.query(NseIndexMaster)
        .filter(
            (NseIndexMaster.index_symbol == index_symbol)
            | (NseIndexMaster.short_code == short_code)
        )
        .one_or_none()
    )

    # Fallback to case-insensitive
    if row is None:
        row = (
            db.query(NseIndexMaster)
            .filter(func.lower(NseIndexMaster.index_symbol) == index_symbol.lower())
            .one_or_none()
        )
    if row is None:
        row = (
            db.query(NseIndexMaster)
            .filter(func.lower(NseIndexMaster.short_code) == short_code.lower())
            .one_or_none()
        )

    if row is None:
        raise HTTPException(status_code=404, detail=f"Index not found in NseIndexMaster for {index_key}")

    return row


def _get_index_name_and_tokens_cte(db: Session, index_key: str):
    """
    Returns:
      - index_name (str)
      - tokens_cte: CTE of token_id
      - token_filter for intraday queries
    """
    key = (index_key or "").upper().strip()

    # ALL: all EQ tokens
    if key == "ALL":
        tokens_cte = (
            select(NseCmSecurity.token_id)
            .where(NseCmSecurity.series == "EQ")  # ✅ no upper()
            .distinct()
            .cte("tokens")
        )
        return "ALL_EQ", tokens_cte, NseCmIntraday1Min.token_id.in_(select(tokens_cte.c.token_id))

    if key not in INDEX_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported index '{index_key}'. Use NIFTY50 / NIFTY100 / NIFTY500 / ALL.",
        )

    index_row = _get_index_row(db, key)

    tokens_cte = (
        select(NseCmSecurity.token_id)
        .select_from(NseIndexConstituent)
        .join(NseCmSecurity, NseCmSecurity.symbol == NseIndexConstituent.symbol)
        .where(
            NseIndexConstituent.index_id == index_row.id,
            NseCmSecurity.series == "EQ",  # ✅ no upper()
        )
        .distinct()
        .cte("tokens")
    )

    return index_row.index_symbol, tokens_cte, NseCmIntraday1Min.token_id.in_(select(tokens_cte.c.token_id))


def _get_latest_trade_date(db: Session, token_filter):
    latest_trade_date = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(token_filter)
    ).scalar()

    if latest_trade_date is None:
        raise HTTPException(status_code=404, detail="No intraday data found for given index tokens")

    return latest_trade_date


def _get_prev_trade_date(db: Session, token_filter, latest_trade_date):
    return db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(
            token_filter, NseCmIntraday1Min.trade_date < latest_trade_date
        )
    ).scalar()


def _latest_one_cte(token_filter, latest_trade_date):
    """
    Latest (last) candle per token for latest_trade_date via window function.
    """
    ranked = (
        select(
            NseCmIntraday1Min.token_id.label("token_id"),
            NseCmIntraday1Min.interval_start.label("interval_start"),
            NseCmIntraday1Min.last_price.label("last_price"),
            NseCmIntraday1Min.close_price.label("close_price"),
            NseCmIntraday1Min.total_traded_qty.label("total_traded_qty"),
            NseCmIntraday1Min.volume.label("volume"),
            func.row_number()
            .over(
                partition_by=NseCmIntraday1Min.token_id,
                order_by=NseCmIntraday1Min.interval_start.desc(),
            )
            .label("rn"),
        )
        .where(
            NseCmIntraday1Min.trade_date == latest_trade_date,
            token_filter,
        )
        .cte("latest_ranked")
    )

    return (
        select(
            ranked.c.token_id,
            ranked.c.interval_start,
            ranked.c.last_price,
            ranked.c.close_price,
            ranked.c.total_traded_qty,
            ranked.c.volume,
        )
        .where(ranked.c.rn == 1)
        .cte("latest_one")
    )


def _prev_one_cte(token_filter, prev_trade_date):
    """
    Previous day last candle per token via window function.
    """
    ranked = (
        select(
            NseCmIntraday1Min.token_id.label("token_id"),
            func.coalesce(NseCmIntraday1Min.close_price, NseCmIntraday1Min.last_price).label(
                "prev_close"
            ),
            func.row_number()
            .over(
                partition_by=NseCmIntraday1Min.token_id,
                order_by=NseCmIntraday1Min.interval_start.desc(),
            )
            .label("rn"),
        )
        .where(
            NseCmIntraday1Min.trade_date == prev_trade_date,
            token_filter,
        )
        .cte("prev_ranked")
    )

    return (
        select(ranked.c.token_id, ranked.c.prev_close)
        .where(ranked.c.rn == 1)
        .cte("prev_one")
    )


def _serialize_rows(rows):
    return [
        {
            "token_id": r["token_id"],
            "symbol": r["symbol"],
            "series": r["series"],
            "company_name": r["company_name"],
            "last_price": float(r["last_price"]) if r["last_price"] is not None else None,
            "prev_close": float(r["prev_close"]) if r.get("prev_close", None) is not None else None,
            "change_pct": float(r["change_pct"]) if r.get("change_pct", None) is not None else None,
            "close_price": float(r["close_price"]) if r.get("close_price", None) is not None else None,
            "total_traded_qty": int(r["total_traded_qty"]) if r.get("total_traded_qty", None) is not None else None,
            "volume": int(r["volume"]) if r.get("volume", None) is not None else None,
            "activity_metric": int(r["activity_metric"]) if r.get("activity_metric", None) is not None else None,
            "high_52": float(r["high_52"]) if r.get("high_52", None) is not None else None,
            "low_52": float(r["low_52"]) if r.get("low_52", None) is not None else None,
            "interval_start": r["interval_start"].isoformat() if r.get("interval_start") else None,
        }
        for r in rows
    ]


# ===========================
#  Filter implementations
# ===========================
def _fetch_gainers(db: Session, index_key: str, limit: int):
    index_name, _tokens_cte, token_filter = _get_index_name_and_tokens_cte(db, index_key)

    latest_trade_date = _get_latest_trade_date(db, token_filter)
    prev_trade_date = _get_prev_trade_date(db, token_filter, latest_trade_date)

    latest_one = _latest_one_cte(token_filter, latest_trade_date)

    if prev_trade_date is None:
        rows = db.execute(
            select(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.series,
                NseCmSecurity.company_name,
                latest_one.c.last_price.label("last_price"),
                literal(None).label("prev_close"),
                literal(None).label("change_pct"),
                latest_one.c.interval_start.label("interval_start"),
            )
            .select_from(latest_one)
            .join(NseCmSecurity, NseCmSecurity.token_id == latest_one.c.token_id)
            .where(NseCmSecurity.series == "EQ")
            .order_by(NseCmSecurity.symbol.asc())
            .limit(limit)
        ).mappings().all()

        return {
            "filter": "GAINERS",
            "index": index_name,
            "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
            "prev_trade_date": None,
            "count": len(rows),
            "data": _serialize_rows(rows),
        }

    prev_one = _prev_one_cte(token_filter, prev_trade_date)
    prev_close = prev_one.c.prev_close
    change_pct = (latest_one.c.last_price - prev_close) * 100.0 / func.nullif(prev_close, 0.0)

    stmt = (
        select(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            latest_one.c.last_price.label("last_price"),
            prev_close.label("prev_close"),
            change_pct.label("change_pct"),
            latest_one.c.interval_start.label("interval_start"),
        )
        .select_from(latest_one)
        .join(NseCmSecurity, NseCmSecurity.token_id == latest_one.c.token_id)
        .outerjoin(prev_one, prev_one.c.token_id == latest_one.c.token_id)
        .where(
            NseCmSecurity.series == "EQ",
            change_pct.isnot(None),
            change_pct > 0,
        )
        .order_by(change_pct.desc().nullslast())
        .limit(limit)
    )

    rows = db.execute(stmt).mappings().all()
    db.close()
    return {
        "filter": "GAINERS",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "count": len(rows),
        "data": _serialize_rows(rows),
    }


def _fetch_losers(db: Session, index_key: str, limit: int):
    index_name, _tokens_cte, token_filter = _get_index_name_and_tokens_cte(db, index_key)

    latest_trade_date = _get_latest_trade_date(db, token_filter)
    prev_trade_date = _get_prev_trade_date(db, token_filter, latest_trade_date)

    latest_one = _latest_one_cte(token_filter, latest_trade_date)

    if prev_trade_date is None:
        rows = db.execute(
            select(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.series,
                NseCmSecurity.company_name,
                latest_one.c.last_price.label("last_price"),
                literal(None).label("prev_close"),
                literal(None).label("change_pct"),
                latest_one.c.interval_start.label("interval_start"),
            )
            .select_from(latest_one)
            .join(NseCmSecurity, NseCmSecurity.token_id == latest_one.c.token_id)
            .where(NseCmSecurity.series == "EQ")
            .order_by(NseCmSecurity.symbol.asc())
            .limit(limit)
        ).mappings().all()
        db.close()

        return {
            "filter": "LOSERS",
            "index": index_name,
            "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
            "prev_trade_date": None,
            "count": len(rows),
            "data": _serialize_rows(rows),
        }

    prev_one = _prev_one_cte(token_filter, prev_trade_date)
    prev_close = prev_one.c.prev_close
    change_pct = (latest_one.c.last_price - prev_close) * 100.0 / func.nullif(prev_close, 0.0)

    stmt = (
        select(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            latest_one.c.last_price.label("last_price"),
            prev_close.label("prev_close"),
            change_pct.label("change_pct"),
            latest_one.c.interval_start.label("interval_start"),
        )
        .select_from(latest_one)
        .join(NseCmSecurity, NseCmSecurity.token_id == latest_one.c.token_id)
        .outerjoin(prev_one, prev_one.c.token_id == latest_one.c.token_id)
        .where(
            NseCmSecurity.series == "EQ",
            change_pct.isnot(None),
            change_pct < 0,
        )
        .order_by(change_pct.asc())
        .limit(limit)
    )

    rows = db.execute(stmt).mappings().all()
    db.close()
    return {
        "filter": "LOSERS",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "count": len(rows),
        "data": _serialize_rows(rows),
    }


def _fetch_most_active(db: Session, index_key: str, limit: int):
    index_name, _tokens_cte, token_filter = _get_index_name_and_tokens_cte(db, index_key)
    latest_trade_date = _get_latest_trade_date(db, token_filter)

    latest_one = _latest_one_cte(token_filter, latest_trade_date)
    activity_metric = func.coalesce(latest_one.c.total_traded_qty, latest_one.c.volume)

    stmt = (
        select(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            latest_one.c.last_price.label("last_price"),
            latest_one.c.close_price.label("close_price"),
            latest_one.c.total_traded_qty.label("total_traded_qty"),
            latest_one.c.volume.label("volume"),
            activity_metric.label("activity_metric"),
            latest_one.c.interval_start.label("interval_start"),
        )
        .select_from(latest_one)
        .join(NseCmSecurity, NseCmSecurity.token_id == latest_one.c.token_id)
        .where(
            NseCmSecurity.series == "EQ",
            activity_metric.isnot(None),
        )
        .order_by(activity_metric.desc())
        .limit(limit)
    )

    rows = db.execute(stmt).mappings().all()
    db.close()
    return {
        "filter": "MOST_ACTIVE",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "count": len(rows),
        "data": _serialize_rows(rows),
    }


def _fetch_52w_high(db: Session, index_key: str, limit: int):
    index_name, tokens_cte, token_filter = _get_index_name_and_tokens_cte(db, index_key)
    latest_trade_date = _get_latest_trade_date(db, token_filter)
    latest_one = _latest_one_cte(token_filter, latest_trade_date)

    cutoff_end = latest_trade_date
    cutoff_start = cutoff_end - timedelta(days=365)

    # 52w high (bhavcopy) per token
    high_52 = (
        select(
            NseCmBhavcopy.token_id.label("token_id"),
            func.max(NseCmBhavcopy.high_price).label("high_52"),
        )
        .where(
            NseCmBhavcopy.trade_date >= cutoff_start,
            NseCmBhavcopy.trade_date <= cutoff_end,
            NseCmBhavcopy.token_id.in_(select(tokens_cte.c.token_id)),
        )
        .group_by(NseCmBhavcopy.token_id)
        .cte("high_52")
    )

    ratio = latest_one.c.last_price / func.nullif(high_52.c.high_52, 0.0)

    stmt = (
        select(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            latest_one.c.last_price.label("last_price"),
            high_52.c.high_52.label("high_52"),
            ratio.label("high_proximity"),
            latest_one.c.interval_start.label("interval_start"),
        )
        .select_from(latest_one)
        .join(NseCmSecurity, NseCmSecurity.token_id == latest_one.c.token_id)
        .join(high_52, high_52.c.token_id == latest_one.c.token_id)
        .where(
            NseCmSecurity.series == "EQ",
            high_52.c.high_52.isnot(None),
            high_52.c.high_52 > 0,
            latest_one.c.last_price.isnot(None),
            latest_one.c.last_price > 0,
            latest_one.c.last_price >= 0.98 * high_52.c.high_52,
        )
        .order_by(ratio.desc())
        .limit(limit)
    )

    rows = db.execute(stmt).mappings().all()
    # add derived near_high_pct
    data = []
    for r in rows:
        lp = r["last_price"]
        h52 = r["high_52"]
        near_high_pct = ((float(lp) / float(h52) - 1.0) * 100) if (lp and h52 and h52 != 0) else None
        d = dict(r)
        d["near_high_pct"] = near_high_pct
        data.append(d)

    db.close()

    return {
        "filter": "52W_HIGH",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "cutoff_start": cutoff_start.isoformat(),
        "cutoff_end": cutoff_end.isoformat(),
        "count": len(rows),
        "data": _serialize_rows(data),
    }


def _fetch_52w_low(db: Session, index_key: str, limit: int):
    index_name, tokens_cte, token_filter = _get_index_name_and_tokens_cte(db, index_key)
    latest_trade_date = _get_latest_trade_date(db, token_filter)
    latest_one = _latest_one_cte(token_filter, latest_trade_date)

    cutoff_end = latest_trade_date
    cutoff_start = cutoff_end - timedelta(days=365)

    low_52 = (
        select(
            NseCmBhavcopy.token_id.label("token_id"),
            func.min(NseCmBhavcopy.low_price).label("low_52"),
        )
        .where(
            NseCmBhavcopy.trade_date >= cutoff_start,
            NseCmBhavcopy.trade_date <= cutoff_end,
            NseCmBhavcopy.token_id.in_(select(tokens_cte.c.token_id)),
        )
        .group_by(NseCmBhavcopy.token_id)
        .cte("low_52")
    )

    ratio = latest_one.c.last_price / func.nullif(low_52.c.low_52, 0.0)

    stmt = (
        select(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            latest_one.c.last_price.label("last_price"),
            low_52.c.low_52.label("low_52"),
            ratio.label("low_proximity"),
            latest_one.c.interval_start.label("interval_start"),
        )
        .select_from(latest_one)
        .join(NseCmSecurity, NseCmSecurity.token_id == latest_one.c.token_id)
        .join(low_52, low_52.c.token_id == latest_one.c.token_id)
        .where(
            NseCmSecurity.series == "EQ",
            low_52.c.low_52.isnot(None),
            low_52.c.low_52 > 0,
            latest_one.c.last_price.isnot(None),
            latest_one.c.last_price > 0,
            latest_one.c.last_price <= 1.02 * low_52.c.low_52,
        )
        .order_by(ratio.asc())
        .limit(limit)
    )

    rows = db.execute(stmt).mappings().all()
    data = []
    for r in rows:
        lp = r["last_price"]
        l52 = r["low_52"]
        above_low_pct = ((float(lp) / float(l52) - 1.0) * 100) if (lp and l52 and l52 != 0) else None
        d = dict(r)
        d["above_low_pct"] = above_low_pct
        data.append(d)

    db.close()

    return {
        "filter": "52W_LOW",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "cutoff_start": cutoff_start.isoformat(),
        "cutoff_end": cutoff_end.isoformat(),
        "count": len(rows),
        "data": _serialize_rows(data),
    }


# ===========================
#  MAIN ENDPOINT
# ===========================
@router.get("/")
async def todays_stock(
    index: str = Query("NIFTY100", description="Index filter: NIFTY50 / NIFTY100 / NIFTY500 / ALL"),
    filter_type: str = Query("GAINERS", description="Filter: GAINERS / LOSERS / MOST_ACTIVE / 52W_HIGH / 52W_LOW"),
    limit: int = Query(10, ge=1, le=500, description="Max number of rows to return"),
    db: Session = Depends(get_db),
):
    f = (filter_type or "").upper().strip()

    if f == "GAINERS":
        return _fetch_gainers(db, index, limit)
    if f == "LOSERS":
        return _fetch_losers(db, index, limit)
    if f == "MOST_ACTIVE":
        return _fetch_most_active(db, index, limit)
    if f == "52W_HIGH":
        return _fetch_52w_high(db, index, limit)
    if f == "52W_LOW":
        return _fetch_52w_low(db, index, limit)

    raise HTTPException(
        status_code=400,
        detail="Invalid filter_type. Use: GAINERS / LOSERS / MOST_ACTIVE / 52W_HIGH / 52W_LOW",
    )
