# routes/NSE/Todays_Stock.py

import logging
from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session, aliased

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

# ---- Helper: index mapping ----
INDEX_MAP = {
    "NIFTY50": ("NIFTY 50", "NIFTY50"),
    "NIFTY100": ("NIFTY 100", "NIFTY100"),
    "NIFTY500": ("NIFTY 500", "NIFTY500"),
    # "ALL" -> no index filter, sirf EQ series
}


# ===========================
#  Core helpers
# ===========================

def _get_index_and_token_subq(db: Session, index_key: str):
    """
    index_key: 'NIFTY50' / 'NIFTY100' / 'NIFTY500' / 'ALL'
    Returns:
      - index_name (str)
      - token_subq (subquery of token_id for that index / ALL EQ)
    """
    key = (index_key or "").upper().strip()

    # ALL -> saare EQ securities (no index filter)
    if key == "ALL":
        token_subq = (
            db.query(NseCmSecurity.token_id)
            .filter(func.upper(NseCmSecurity.series) == "EQ")
            .subquery()
        )
        return "ALL_EQ", token_subq

    if key not in INDEX_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported index '{index_key}'. Use NIFTY50 / NIFTY100 / NIFTY500 / ALL.",
        )

    index_symbol, short_code = INDEX_MAP[key]

    index_row = (
        db.query(NseIndexMaster)
        .filter(func.lower(NseIndexMaster.index_symbol) == func.lower(index_symbol))
        .one_or_none()
    )

    if index_row is None:
        index_row = (
            db.query(NseIndexMaster)
            .filter(func.lower(NseIndexMaster.short_code) == func.lower(short_code))
            .one_or_none()
        )

    if index_row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Index not found in NseIndexMaster for {index_key}",
        )

    token_subq = (
        db.query(NseCmSecurity.token_id)
        .join(
            NseIndexConstituent,
            NseIndexConstituent.symbol == NseCmSecurity.symbol,
        )
        .filter(
            NseIndexConstituent.index_id == index_row.id,
            func.upper(NseCmSecurity.series) == "EQ",
        )
        .subquery()
    )

    return index_row.index_symbol, token_subq


def _get_latest_and_prev_trade_info(db: Session, token_subq):
    """
    Common helper:
      - latest_trade_date
      - prev_trade_date
      - latest_bar_subq (max interval_start on latest_trade_date)
      - prev_bar_subq (max interval_start on prev_trade_date, can be None)
    """
    # Latest trading date
    latest_trade_date = (
        db.query(func.max(NseCmIntraday1Min.trade_date))
        .filter(NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id)))
        .scalar()
    )

    if latest_trade_date is None:
        raise HTTPException(
            status_code=404,
            detail="No intraday data found for given index tokens",
        )

    # Previous trading date
    prev_trade_date = (
        db.query(func.max(NseCmIntraday1Min.trade_date))
        .filter(
            NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id)),
            NseCmIntraday1Min.trade_date < latest_trade_date,
        )
        .scalar()
    )

    # Latest bar subquery
    latest_bar_subq = (
        db.query(
            NseCmIntraday1Min.token_id.label("token_id"),
            func.max(NseCmIntraday1Min.interval_start).label("max_ts"),
        )
        .filter(
            NseCmIntraday1Min.trade_date == latest_trade_date,
            NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id)),
        )
        .group_by(NseCmIntraday1Min.token_id)
        .subquery()
    )

    # Previous bar subquery (if prev_trade_date exists)
    prev_bar_subq = None
    if prev_trade_date is not None:
        prev_bar_subq = (
            db.query(
                NseCmIntraday1Min.token_id.label("token_id"),
                func.max(NseCmIntraday1Min.interval_start).label("max_ts"),
            )
            .filter(
                NseCmIntraday1Min.trade_date == prev_trade_date,
                NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id)),
            )
            .group_by(NseCmIntraday1Min.token_id)
            .subquery()
        )

    return latest_trade_date, prev_trade_date, latest_bar_subq, prev_bar_subq


def _get_latest_bars_only(db: Session, token_subq):
    """
    Only latest trading day & its last candle per token
    (for MOST_ACTIVE, 52W filters use this pattern).
    """
    latest_trade_date = (
        db.query(func.max(NseCmIntraday1Min.trade_date))
        .filter(NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id)))
        .scalar()
    )

    if latest_trade_date is None:
        raise HTTPException(
            status_code=404,
            detail="No intraday data found for given index tokens",
        )

    latest_bar_subq = (
        db.query(
            NseCmIntraday1Min.token_id.label("token_id"),
            func.max(NseCmIntraday1Min.interval_start).label("max_ts"),
        )
        .filter(
            NseCmIntraday1Min.trade_date == latest_trade_date,
            NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id)),
        )
        .group_by(NseCmIntraday1Min.token_id)
        .subquery()
    )

    return latest_trade_date, latest_bar_subq


# ===========================
#  Filter implementations
# ===========================

def _fetch_gainers(db: Session, index_key: str, limit: int):
    index_name, token_subq = _get_index_and_token_subq(db, index_key)
    latest_trade_date, prev_trade_date, latest_bar_subq, prev_bar_subq = (
        _get_latest_and_prev_trade_info(db, token_subq)
    )

    LatestBar = aliased(NseCmIntraday1Min)
    PrevBar = aliased(NseCmIntraday1Min)

    if prev_bar_subq is not None:
        prev_close_expr = func.coalesce(PrevBar.close_price, PrevBar.last_price)
        change_pct_expr = (
            (LatestBar.last_price - prev_close_expr)
            * 100.0
            / func.nullif(prev_close_expr, 0.0)
        )

        query = (
            db.query(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.series,
                NseCmSecurity.company_name,
                LatestBar.last_price.label("last_price"),
                prev_close_expr.label("prev_close"),
                change_pct_expr.label("change_pct"),
                LatestBar.interval_start,
            )
            .join(
                latest_bar_subq,
                latest_bar_subq.c.token_id == NseCmSecurity.token_id,
            )
            .join(
                LatestBar,
                (LatestBar.token_id == latest_bar_subq.c.token_id)
                & (LatestBar.interval_start == latest_bar_subq.c.max_ts),
            )
            .outerjoin(
                prev_bar_subq,
                prev_bar_subq.c.token_id == NseCmSecurity.token_id,
            )
            .outerjoin(
                PrevBar,
                (PrevBar.token_id == prev_bar_subq.c.token_id)
                & (PrevBar.interval_start == prev_bar_subq.c.max_ts),
            )
            .filter(
                NseCmSecurity.token_id.in_(select(token_subq.c.token_id)),
                func.upper(NseCmSecurity.series) == "EQ",
                change_pct_expr.isnot(None),
                change_pct_expr > 0,
            )
            .order_by(change_pct_expr.desc().nullslast())
            .limit(limit)
        )
    else:
        # No previous trading day -> can't compute gains
        query = (
            db.query(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.series,
                NseCmSecurity.company_name,
                LatestBar.last_price.label("last_price"),
                func.cast(None, LatestBar.close_price.type).label("prev_close"),
                func.cast(None, LatestBar.close_price.type).label("change_pct"),
                LatestBar.interval_start,
            )
            .join(
                latest_bar_subq,
                latest_bar_subq.c.token_id == NseCmSecurity.token_id,
            )
            .join(
                LatestBar,
                (LatestBar.token_id == latest_bar_subq.c.token_id)
                & (LatestBar.interval_start == latest_bar_subq.c.max_ts),
            )
            .filter(
                NseCmSecurity.token_id.in_(select(token_subq.c.token_id)),
                func.upper(NseCmSecurity.series) == "EQ",
            )
            .order_by(NseCmSecurity.symbol.asc())
            .limit(limit)
        )

    rows = query.all()

    return {
        "filter": "GAINERS",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat()
        if latest_trade_date
        else None,
        "prev_trade_date": prev_trade_date.isoformat()
        if prev_trade_date
        else None,
        "count": len(rows),
        "data": [
            {
                "token_id": r.token_id,
                "symbol": r.symbol,
                "series": r.series,
                "company_name": r.company_name,
                "last_price": float(r.last_price) if r.last_price is not None else None,
                "prev_close": float(r.prev_close) if r.prev_close is not None else None,
                "change_pct": float(r.change_pct)
                if getattr(r, "change_pct", None) is not None
                else None,
                "interval_start": r.interval_start.isoformat()
                if r.interval_start
                else None,
            }
            for r in rows
        ],
    }


def _fetch_losers(db: Session, index_key: str, limit: int):
    index_name, token_subq = _get_index_and_token_subq(db, index_key)
    latest_trade_date, prev_trade_date, latest_bar_subq, prev_bar_subq = (
        _get_latest_and_prev_trade_info(db, token_subq)
    )

    LatestBar = aliased(NseCmIntraday1Min)
    PrevBar = aliased(NseCmIntraday1Min)

    if prev_bar_subq is not None:
        prev_close_expr = func.coalesce(PrevBar.close_price, PrevBar.last_price)
        change_pct_expr = (
            (LatestBar.last_price - prev_close_expr)
            * 100.0
            / func.nullif(prev_close_expr, 0.0)
        )

        query = (
            db.query(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.series,
                NseCmSecurity.company_name,
                LatestBar.last_price.label("last_price"),
                prev_close_expr.label("prev_close"),
                change_pct_expr.label("change_pct"),
                LatestBar.interval_start,
            )
            .join(
                latest_bar_subq,
                latest_bar_subq.c.token_id == NseCmSecurity.token_id,
            )
            .join(
                LatestBar,
                (LatestBar.token_id == latest_bar_subq.c.token_id)
                & (LatestBar.interval_start == latest_bar_subq.c.max_ts),
            )
            .outerjoin(
                prev_bar_subq,
                prev_bar_subq.c.token_id == NseCmSecurity.token_id,
            )
            .outerjoin(
                PrevBar,
                (PrevBar.token_id == prev_bar_subq.c.token_id)
                & (PrevBar.interval_start == prev_bar_subq.c.max_ts),
            )
            .filter(
                NseCmSecurity.token_id.in_(select(token_subq.c.token_id)),
                func.upper(NseCmSecurity.series) == "EQ",
                change_pct_expr.isnot(None),
                change_pct_expr < 0,
            )
            # sabse zyada negative (bada loss) top par
            .order_by(change_pct_expr.asc())
            .limit(limit)
        )
    else:
        query = (
            db.query(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.series,
                NseCmSecurity.company_name,
                LatestBar.last_price.label("last_price"),
                func.cast(None, LatestBar.close_price.type).label("prev_close"),
                func.cast(None, LatestBar.close_price.type).label("change_pct"),
                LatestBar.interval_start,
            )
            .join(
                latest_bar_subq,
                latest_bar_subq.c.token_id == NseCmSecurity.token_id,
            )
            .join(
                LatestBar,
                (LatestBar.token_id == latest_bar_subq.c.token_id)
                & (LatestBar.interval_start == latest_bar_subq.c.max_ts),
            )
            .filter(
                NseCmSecurity.token_id.in_(select(token_subq.c.token_id)),
                func.upper(NseCmSecurity.series) == "EQ",
            )
            .order_by(NseCmSecurity.symbol.asc())
            .limit(limit)
        )

    rows = query.all()

    return {
        "filter": "LOSERS",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat()
        if latest_trade_date
        else None,
        "prev_trade_date": prev_trade_date.isoformat()
        if prev_trade_date
        else None,
        "count": len(rows),
        "data": [
            {
                "token_id": r.token_id,
                "symbol": r.symbol,
                "series": r.series,
                "company_name": r.company_name,
                "last_price": float(r.last_price) if r.last_price is not None else None,
                "prev_close": float(r.prev_close) if r.prev_close is not None else None,
                "change_pct": float(r.change_pct)
                if getattr(r, "change_pct", None) is not None
                else None,
                "interval_start": r.interval_start.isoformat()
                if r.interval_start
                else None,
            }
            for r in rows
        ],
    }


def _fetch_most_active(db: Session, index_key: str, limit: int):
    index_name, token_subq = _get_index_and_token_subq(db, index_key)
    latest_trade_date, latest_bar_subq = _get_latest_bars_only(db, token_subq)

    LatestBar = aliased(NseCmIntraday1Min)

    volume_expr = func.coalesce(LatestBar.total_traded_qty, LatestBar.volume)

    query = (
        db.query(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            LatestBar.last_price.label("last_price"),
            LatestBar.close_price.label("close_price"),
            LatestBar.total_traded_qty.label("total_traded_qty"),
            LatestBar.volume.label("volume"),
            volume_expr.label("activity_metric"),
            LatestBar.interval_start,
        )
        .join(
            latest_bar_subq,
            latest_bar_subq.c.token_id == NseCmSecurity.token_id,
        )
        .join(
            LatestBar,
            (LatestBar.token_id == latest_bar_subq.c.token_id)
            & (LatestBar.interval_start == latest_bar_subq.c.max_ts),
        )
        .filter(
            NseCmSecurity.token_id.in_(select(token_subq.c.token_id)),
            func.upper(NseCmSecurity.series) == "EQ",
            volume_expr.isnot(None),
        )
        .order_by(volume_expr.desc())
        .limit(limit)
    )

    rows = query.all()

    return {
        "filter": "MOST_ACTIVE",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat()
        if latest_trade_date
        else None,
        "count": len(rows),
        "data": [
            {
                "token_id": r.token_id,
                "symbol": r.symbol,
                "series": r.series,
                "company_name": r.company_name,
                "last_price": float(r.last_price) if r.last_price is not None else None,
                "close_price": float(r.close_price)
                if r.close_price is not None
                else None,
                "total_traded_qty": int(r.total_traded_qty)
                if r.total_traded_qty is not None
                else None,
                "volume": int(r.volume) if r.volume is not None else None,
                "activity_metric": int(r.activity_metric)
                if r.activity_metric is not None
                else None,
                "interval_start": r.interval_start.isoformat()
                if r.interval_start
                else None,
            }
            for r in rows
        ],
    }


def _fetch_52w_high(db: Session, index_key: str, limit: int):
    index_name, token_subq = _get_index_and_token_subq(db, index_key)
    latest_trade_date, latest_bar_subq = _get_latest_bars_only(db, token_subq)

    cutoff_end = latest_trade_date
    cutoff_start = cutoff_end - timedelta(days=365)

    high_52_subq = (
        db.query(
            NseCmBhavcopy.token_id.label("token_id"),
            func.max(NseCmBhavcopy.high_price).label("high_52"),
        )
        .filter(
            NseCmBhavcopy.trade_date >= cutoff_start,
            NseCmBhavcopy.trade_date <= cutoff_end,
            NseCmBhavcopy.token_id.in_(select(token_subq.c.token_id)),
        )
        .group_by(NseCmBhavcopy.token_id)
        .subquery()
    )

    LatestBar = aliased(NseCmIntraday1Min)
    ratio_expr = LatestBar.last_price / func.nullif(high_52_subq.c.high_52, 0.0)

    query = (
        db.query(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            LatestBar.last_price.label("last_price"),
            high_52_subq.c.high_52.label("high_52"),
            ratio_expr.label("high_proximity"),
            LatestBar.interval_start,
        )
        .join(
            latest_bar_subq,
            latest_bar_subq.c.token_id == NseCmSecurity.token_id,
        )
        .join(
            LatestBar,
            (LatestBar.token_id == latest_bar_subq.c.token_id)
            & (LatestBar.interval_start == latest_bar_subq.c.max_ts),
        )
        .join(
            high_52_subq,
            high_52_subq.c.token_id == NseCmSecurity.token_id,
        )
        .filter(
            NseCmSecurity.token_id.in_(select(token_subq.c.token_id)),
            func.upper(NseCmSecurity.series) == "EQ",
            high_52_subq.c.high_52.isnot(None),
            high_52_subq.c.high_52 > 0,
            LatestBar.last_price.isnot(None),
            LatestBar.last_price > 0,
            LatestBar.last_price >= 0.98 * high_52_subq.c.high_52,
        )
        .order_by(ratio_expr.desc())
        .limit(limit)
    )

    rows = query.all()

    return {
        "filter": "52W_HIGH",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat()
        if latest_trade_date
        else None,
        "cutoff_start": cutoff_start.isoformat(),
        "cutoff_end": cutoff_end.isoformat(),
        "count": len(rows),
        "data": [
            {
                "token_id": r.token_id,
                "symbol": r.symbol,
                "series": r.series,
                "company_name": r.company_name,
                "last_price": float(r.last_price)
                if r.last_price is not None
                else None,
                "high_52": float(r.high_52) if r.high_52 is not None else None,
                "near_high_pct": (
                    (float(r.last_price) / float(r.high_52) - 1.0) * 100
                    if (r.last_price is not None and r.high_52 is not None and r.high_52 != 0)
                    else None
                ),
                "interval_start": r.interval_start.isoformat()
                if r.interval_start
                else None,
            }
            for r in rows
        ],
    }


def _fetch_52w_low(db: Session, index_key: str, limit: int):
    index_name, token_subq = _get_index_and_token_subq(db, index_key)
    latest_trade_date, latest_bar_subq = _get_latest_bars_only(db, token_subq)

    cutoff_end = latest_trade_date
    cutoff_start = cutoff_end - timedelta(days=365)

    low_52_subq = (
        db.query(
            NseCmBhavcopy.token_id.label("token_id"),
            func.min(NseCmBhavcopy.low_price).label("low_52"),
        )
        .filter(
            NseCmBhavcopy.trade_date >= cutoff_start,
            NseCmBhavcopy.trade_date <= cutoff_end,
            NseCmBhavcopy.token_id.in_(select(token_subq.c.token_id)),
        )
        .group_by(NseCmBhavcopy.token_id)
        .subquery()
    )

    LatestBar = aliased(NseCmIntraday1Min)
    ratio_expr = LatestBar.last_price / func.nullif(low_52_subq.c.low_52, 0.0)

    query = (
        db.query(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            LatestBar.last_price.label("last_price"),
            low_52_subq.c.low_52.label("low_52"),
            ratio_expr.label("low_proximity"),
            LatestBar.interval_start,
        )
        .join(
            latest_bar_subq,
            latest_bar_subq.c.token_id == NseCmSecurity.token_id,
        )
        .join(
            LatestBar,
            (LatestBar.token_id == latest_bar_subq.c.token_id)
            & (LatestBar.interval_start == latest_bar_subq.c.max_ts),
        )
        .join(
            low_52_subq,
            low_52_subq.c.token_id == NseCmSecurity.token_id,
        )
        .filter(
            NseCmSecurity.token_id.in_(select(token_subq.c.token_id)),
            func.upper(NseCmSecurity.series) == "EQ",
            low_52_subq.c.low_52.isnot(None),
            low_52_subq.c.low_52 > 0,
            LatestBar.last_price.isnot(None),
            LatestBar.last_price > 0,
            LatestBar.last_price <= 1.02 * low_52_subq.c.low_52,
        )
        .order_by(ratio_expr.asc())
        .limit(limit)
    )

    rows = query.all()

    return {
        "filter": "52W_LOW",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat()
        if latest_trade_date
        else None,
        "cutoff_start": cutoff_start.isoformat(),
        "cutoff_end": cutoff_end.isoformat(),
        "count": len(rows),
        "data": [
            {
                "token_id": r.token_id,
                "symbol": r.symbol,
                "series": r.series,
                "company_name": r.company_name,
                "last_price": float(r.last_price)
                if r.last_price is not None
                else None,
                "low_52": float(r.low_52) if r.low_52 is not None else None,
                "above_low_pct": (
                    (float(r.last_price) / float(r.low_52) - 1.0) * 100
                    if (r.last_price is not None and r.low_52 is not None and r.low_52 != 0)
                    else None
                ),
                "interval_start": r.interval_start.isoformat()
                if r.interval_start
                else None,
            }
            for r in rows
        ],
    }


# ===========================
#  MAIN ENDPOINT
# ===========================

@router.get("/")
async def todays_stock(
    index: str = Query(
        "NIFTY100",
        description="Index filter: NIFTY50 / NIFTY100 / NIFTY500 / ALL",
    ),
    filter_type: str = Query(
        "GAINERS",
        description=(
            "Filter: GAINERS / LOSERS / MOST_ACTIVE / 52W_HIGH / 52W_LOW"
        ),
    ),
    limit: int = Query(
        10,
        ge=1,
        le=500,
        description="Max number of rows to return",
    ),
    db: Session = Depends(get_db),
):
    """
    Single endpoint:
    /today-stock/?index=NIFTY100&filter_type=GAINERS&limit=10
    """

    f = (filter_type or "").upper().strip()

    if f == "GAINERS":
        return _fetch_gainers(db, index, limit)
    elif f == "LOSERS":
        return _fetch_losers(db, index, limit)
    elif f == "MOST_ACTIVE":
        return _fetch_most_active(db, index, limit)
    elif f == "52W_HIGH":
        return _fetch_52w_high(db, index, limit)
    elif f == "52W_LOW":
        return _fetch_52w_low(db, index, limit)

    raise HTTPException(
        status_code=400,
        detail="Invalid filter_type. Use: GAINERS / LOSERS / MOST_ACTIVE / 52W_HIGH / 52W_LOW",
    )
