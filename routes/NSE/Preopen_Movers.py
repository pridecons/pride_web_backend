# routes/NSE/Preopen_Movers.py

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

router = APIRouter(prefix="/preopen-movers", tags=["Preopen Movers"])

# -------------------------------------------------------------------
# Helper: index code → (index_row, short_code)
# -------------------------------------------------------------------

INDEX_MAP = {
    "NIFTY50": ("NIFTY 50", "NIFTY50"),
    "NIFTY100": ("NIFTY 100", "NIFTY100"),
    "NIFTY500": ("NIFTY 500", "NIFTY500"),
}


def _get_index_row(db: Session, index_code: str) -> NseIndexMaster:
    """
    INDEX_MAP ka use karke NseIndexMaster se row nikalta hai.
    """
    index_code = index_code.upper().strip()
    if index_code not in INDEX_MAP:
        raise HTTPException(status_code=400, detail=f"Unsupported index: {index_code}")

    full_name, short_code = INDEX_MAP[index_code]

    index_row = (
        db.query(NseIndexMaster)
        .filter(func.lower(NseIndexMaster.index_symbol) == func.lower(full_name))
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
            detail=f"Index not found in NseIndexMaster: {index_code}",
        )

    return index_row


# -------------------------------------------------------------------
# Core logic: ek index ke liye preopen snapshot
# -------------------------------------------------------------------

def _compute_preopen_snapshot(
    db: Session,
    index_row: NseIndexMaster,
    limit: int,
):
    """
    Diye gaye index ke liye preopen gainers & losers:

    - index ke saare constituents -> NseCmSecurity (EQ) -> token_ids
    - latest_trade_date (intraday table me se)
    - us din ka PREOPEN candle = sabse pehla interval_start per token
    - previous trading day ka last candle (close/last as prev_close)
    - change_pct = (preopen_last - prev_close) * 100 / prev_close

    Response: top `limit` gainers & losers.
    """

    # -------- 1) NIFTY50 / 100 / 500 ke EQ tokens --------
    token_subq = (
        db.query(NseCmSecurity.token_id)
        .select_from(NseIndexConstituent)
        .join(
            NseCmSecurity,
            NseCmSecurity.symbol == NseIndexConstituent.symbol,
        )
        .filter(
            NseIndexConstituent.index_id == index_row.id,
            func.upper(NseCmSecurity.series) == "EQ",
        )
        .subquery()
    )

    # -------- 2) Latest trade_date (yahi "today" hoga) --------
    latest_trade_date = (
        db.query(func.max(NseCmIntraday1Min.trade_date))
        .filter(NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id)))
        .scalar()
    )

    if latest_trade_date is None:
        raise HTTPException(
            status_code=404,
            detail=f"No intraday data found for index {index_row.short_code}",
        )

    # -------- 3) Previous trade_date --------
    prev_trade_date = (
        db.query(func.max(NseCmIntraday1Min.trade_date))
        .filter(
            NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id)),
            NseCmIntraday1Min.trade_date < latest_trade_date,
        )
        .scalar()
    )

    if prev_trade_date is None:
        # first day of data – preopen movers meaningful nahi honge
        raise HTTPException(
            status_code=404,
            detail="Previous trading day intraday data not found",
        )

    # -------- 4) Preopen candle per token (latest_trade_date ka sabse pehla bar) --------
    preopen_bar_subq = (
        db.query(
            NseCmIntraday1Min.token_id.label("token_id"),
            func.min(NseCmIntraday1Min.interval_start).label("min_ts"),
        )
        .filter(
            NseCmIntraday1Min.trade_date == latest_trade_date,
            NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id)),
        )
        .group_by(NseCmIntraday1Min.token_id)
        .subquery()
    )

    # -------- 5) Previous day ka last candle per token --------
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

    PreopenBar = aliased(NseCmIntraday1Min)
    PrevBar = aliased(NseCmIntraday1Min)

    # -------- 6) Base query (without gainers/losers filter) --------
    # NOTE: select_from(NseCmSecurity) use kiya hai taaki join ambiguity na aaye.
    prev_close_expr = func.coalesce(PrevBar.close_price, PrevBar.last_price)
    change_pct_expr = (
        (PreopenBar.last_price - prev_close_expr)
        * 100.0
        / func.nullif(prev_close_expr, 0.0)
    )

    base_q = (
        db.query(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            PreopenBar.last_price.label("preopen_price"),
            prev_close_expr.label("prev_close"),
            change_pct_expr.label("change_pct"),
            PreopenBar.interval_start.label("preopen_time"),
        )
        .select_from(NseCmSecurity)
        .join(
            NseIndexConstituent,
            (NseIndexConstituent.symbol == NseCmSecurity.symbol)
            & (NseIndexConstituent.index_id == index_row.id),
        )
        .join(
            preopen_bar_subq,
            preopen_bar_subq.c.token_id == NseCmSecurity.token_id,
        )
        .join(
            PreopenBar,
            (PreopenBar.token_id == preopen_bar_subq.c.token_id)
            & (PreopenBar.interval_start == preopen_bar_subq.c.min_ts),
        )
        .join(
            prev_bar_subq,
            prev_bar_subq.c.token_id == NseCmSecurity.token_id,
        )
        .join(
            PrevBar,
            (PrevBar.token_id == prev_bar_subq.c.token_id)
            & (PrevBar.interval_start == prev_bar_subq.c.max_ts),
        )
        .filter(
            func.upper(NseCmSecurity.series) == "EQ",
            PreopenBar.last_price.isnot(None),
            prev_close_expr.isnot(None),
            prev_close_expr != 0,
        )
    )

    # -------- 7) Gainers (change_pct > 0 desc) --------
    gainers_rows = (
        base_q.filter(change_pct_expr > 0)
        .order_by(change_pct_expr.desc())
        .limit(limit)
        .all()
    )

    # -------- 8) Losers (change_pct < 0 asc -> sabse bada loser top par) --------
    losers_rows = (
        base_q.filter(change_pct_expr < 0)
        .order_by(change_pct_expr.asc())
        .limit(limit)
        .all()
    )

    # -------- 9) Serialize --------
    def _serialize_row(r):
        pre = float(r.preopen_price) if r.preopen_price is not None else None
        prev = float(r.prev_close) if r.prev_close is not None else None
        chg = float(r.change_pct) if r.change_pct is not None else None
        return {
            "token_id": r.token_id,
            "symbol": r.symbol,
            "series": r.series,
            "company_name": r.company_name,
            "preopen_price": pre,
            "prev_close": prev,
            "change_abs": (pre - prev) if (pre is not None and prev is not None) else None,
            "change_pct": chg,
            "preopen_time": r.preopen_time.isoformat() if r.preopen_time else None,
        }

    return {
        "index_id": index_row.id,
        "code": index_row.short_code,
        "name": index_row.index_symbol,
        "latest_trade_date": latest_trade_date.isoformat(),
        "prev_trade_date": prev_trade_date.isoformat(),
        "gainers_count": len(gainers_rows),
        "losers_count": len(losers_rows),
        "gainers": [_serialize_row(r) for r in gainers_rows],
        "losers": [_serialize_row(r) for r in losers_rows],
    }


# -------------------------------------------------------------------
# Public endpoint
# -------------------------------------------------------------------

@router.get("/")
async def preopen_movers(
    index: str = Query(
        "NIFTY50",
        description="Index: NIFTY50 / NIFTY100 / NIFTY500",
    ),
    limit: int = Query(
        3,
        ge=1,
        le=50,
        description="Top gainers/losers to return",
    ),
    db: Session = Depends(get_db),
):
    """
    Today's Preopen Movers API.

    Example:
      GET /api/v1/preopen-movers?index=NIFTY50&limit=3

    Response:
    {
      "index_id": 1,
      "code": "NIFTY50",
      "name": "NIFTY 50",
      "latest_trade_date": "...",
      "prev_trade_date": "...",
      "gainers": [...],
      "losers": [...]
    }
    """
    index_row = _get_index_row(db, index)
    snapshot = _compute_preopen_snapshot(db, index_row, limit)
    return snapshot
