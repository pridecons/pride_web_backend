# routes/NSE/Most_Traded.py

import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session, aliased

from db.connection import get_db
from db.models import (
    NseIndexConstituent,
    NseIndexMaster,
    NseCmIntraday1Min,
    NseCmSecurity,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/most-traded", tags=["Most Traded"])

# ---- Helper: index mapping ----
INDEX_MAP = {
    "NIFTY50": ("NIFTY 50", "NIFTY50"),
    "NIFTY100": ("NIFTY 100", "NIFTY100"),
    "NIFTY500": ("NIFTY 500", "NIFTY500"),
    # "ALL" -> koi index filter nahi lagega
}


def _get_index_row(db: Session, index_code: str | None):
    """
    index_code: "NIFTY50" / "NIFTY100" / "NIFTY500" / "ALL" / None
    return: (index_row or None)
    """
    if not index_code or index_code.upper() == "ALL":
        return None

    idx = index_code.upper()
    if idx not in INDEX_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid index code '{index_code}'. Use ALL / NIFTY50 / NIFTY100 / NIFTY500.",
        )

    name, short_code = INDEX_MAP[idx]

    index_row = (
        db.query(NseIndexMaster)
        .filter(func.lower(NseIndexMaster.index_symbol) == name.lower())
        .one_or_none()
    )

    if index_row is None:
        index_row = (
            db.query(NseIndexMaster)
            .filter(func.lower(NseIndexMaster.short_code) == short_code.lower())
            .one_or_none()
        )

    if index_row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Index '{idx}' not found in NseIndexMaster",
        )

    return index_row


def _build_token_subq_for_index(db: Session, index_row):
    """
    Agar index_row diya hai to uske constituents ke EQ token_ids ka subquery return karega.
    Agar index_row None hai to None return karega (ALL).
    """
    if index_row is None:
        return None

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
    return token_subq


def _get_latest_and_prev_trade_dates(db: Session, token_subq):
    """
    latest & previous intraday trade_date for given tokens (or sabke liye if token_subq None).
    """
    base = db.query(func.max(NseCmIntraday1Min.trade_date))

    if token_subq is not None:
        base = base.filter(
            NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id))
        )

    latest_trade_date = base.scalar()

    if latest_trade_date is None:
        raise HTTPException(
            status_code=404,
            detail="No intraday data found.",
        )

    prev_q = db.query(func.max(NseCmIntraday1Min.trade_date)).filter(
        NseCmIntraday1Min.trade_date < latest_trade_date
    )
    if token_subq is not None:
        prev_q = prev_q.filter(
            NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id))
        )

    prev_trade_date = prev_q.scalar()
    return latest_trade_date, prev_trade_date


def _build_latest_bar_subq(db: Session, latest_trade_date, token_subq):
    """
    Har token ke liye latest_trade_date par last candle (max interval_start).
    """
    q = (
        db.query(
            NseCmIntraday1Min.token_id.label("token_id"),
            func.max(NseCmIntraday1Min.interval_start).label("max_ts"),
        )
        .filter(NseCmIntraday1Min.trade_date == latest_trade_date)
    )

    if token_subq is not None:
        q = q.filter(
            NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id))
        )

    return q.group_by(NseCmIntraday1Min.token_id).subquery()


def _build_prev_bar_subq(db: Session, prev_trade_date, token_subq):
    """
    Pichle trading day ka last candle per token.
    """
    if prev_trade_date is None:
        return None

    q = (
        db.query(
            NseCmIntraday1Min.token_id.label("token_id"),
            func.max(NseCmIntraday1Min.interval_start).label("max_ts"),
        )
        .filter(NseCmIntraday1Min.trade_date == prev_trade_date)
    )

    if token_subq is not None:
        q = q.filter(
            NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id))
        )

    return q.group_by(NseCmIntraday1Min.token_id).subquery()


def _compute_most_traded(
    db: Session,
    index_code: str = "ALL",
    limit: int = 50,
):
    """
    MOST TRADED COMPANIES (volume-wise)
    - latest intraday last_price
    - previous trading day ka prev_close (intraday last candle se)
    - change_pct = (last_price - prev_close) * 100 / prev_close
    - activity_metric = coalesce(total_traded_qty, volume)
    - sorted by activity_metric desc
    """

    index_row = _get_index_row(db, index_code)
    token_subq = _build_token_subq_for_index(db, index_row)

    latest_trade_date, prev_trade_date = _get_latest_and_prev_trade_dates(
        db, token_subq
    )

    latest_bar_subq = _build_latest_bar_subq(db, latest_trade_date, token_subq)
    prev_bar_subq = _build_prev_bar_subq(db, prev_trade_date, token_subq)

    LatestBar = aliased(NseCmIntraday1Min)
    PrevBar = aliased(NseCmIntraday1Min)

    # Volume metric (most traded)
    volume_expr = func.coalesce(LatestBar.total_traded_qty, LatestBar.volume)

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
                NseCmSecurity.company_name,
                LatestBar.last_price.label("last_price"),
                prev_close_expr.label("prev_close"),
                change_pct_expr.label("change_pct"),
                LatestBar.total_traded_qty.label("total_traded_qty"),
                LatestBar.volume.label("volume"),
                volume_expr.label("activity_metric"),
                LatestBar.interval_start,
            )
            .select_from(LatestBar)
            .join(
                latest_bar_subq,
                (LatestBar.token_id == latest_bar_subq.c.token_id)
                & (LatestBar.interval_start == latest_bar_subq.c.max_ts),
            )
            .join(
                NseCmSecurity,
                NseCmSecurity.token_id == LatestBar.token_id,
            )
            .outerjoin(
                prev_bar_subq,
                prev_bar_subq.c.token_id == LatestBar.token_id,
            )
            .outerjoin(
                PrevBar,
                (PrevBar.token_id == prev_bar_subq.c.token_id)
                & (PrevBar.interval_start == prev_bar_subq.c.max_ts),
            )
            .filter(
                func.upper(NseCmSecurity.series) == "EQ",
                LatestBar.last_price.isnot(None),
                volume_expr.isnot(None),
            )
        )
    else:
        # prev day missing -> no prev_close/change_pct
        query = (
            db.query(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.company_name,
                LatestBar.last_price.label("last_price"),
                func.cast(None, LatestBar.last_price.type).label("prev_close"),
                func.cast(None, LatestBar.last_price.type).label("change_pct"),
                LatestBar.total_traded_qty.label("total_traded_qty"),
                LatestBar.volume.label("volume"),
                volume_expr.label("activity_metric"),
                LatestBar.interval_start,
            )
            .select_from(LatestBar)
            .join(
                latest_bar_subq,
                (LatestBar.token_id == latest_bar_subq.c.token_id)
                & (LatestBar.interval_start == latest_bar_subq.c.max_ts),
            )
            .join(
                NseCmSecurity,
                NseCmSecurity.token_id == LatestBar.token_id,
            )
            .filter(
                func.upper(NseCmSecurity.series) == "EQ",
                LatestBar.last_price.isnot(None),
                volume_expr.isnot(None),
            )
        )

    # Agar specific index hai to symbol/index_id se constrain karo
    if index_row is not None:
        query = query.join(
            NseIndexConstituent,
            (NseIndexConstituent.symbol == NseCmSecurity.symbol)
            & (NseIndexConstituent.index_id == index_row.id),
        )

    query = query.order_by(volume_expr.desc()).limit(limit)

    rows = query.all()

    return {
        "index": index_code.upper() if index_code else "ALL",
        "index_id": index_row.id if index_row is not None else None,
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
                "company_name": r.company_name,
                "last_price": float(r.last_price)
                if r.last_price is not None
                else None,
                "prev_close": float(r.prev_close)
                if getattr(r, "prev_close", None) is not None
                else None,
                "change_pct": float(r.change_pct)
                if getattr(r, "change_pct", None) is not None
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


# ------------ FastAPI endpoint ------------

@router.get("/")
def most_traded_companies(
    index: str = Query(
        "ALL",
        description="Index filter: ALL / NIFTY50 / NIFTY100 / NIFTY500",
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
    Most traded companies volume-wise.
    By default ALL EQ stocks; optionally restrict by index (NIFTY50/100/500).
    """
    return _compute_most_traded(db=db, index_code=index, limit=limit)
