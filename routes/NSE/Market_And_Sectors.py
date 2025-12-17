# routes/NSE/Market_And_Sectors.py

import logging
from typing import List, Dict, Any

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

router = APIRouter(
    prefix="/market-and-sectors",
    tags=["Market & Sectors"],
)

# -------------------------------------------------
# Index mapping: UI codes -> (index_symbol, short_code)
# -------------------------------------------------
INDEX_MAP = {
    "NIFTY50": ("NIFTY 50", "NIFTY50"),
    "NIFTY100": ("NIFTY 100", "NIFTY100"),
    "NIFTY500": ("NIFTY 500", "NIFTY500"),
    # yahan aage Bank, IT, Pharma jaise sector indices add kar sakte ho
    # "NIFTYBANK": ("NIFTY BANK", "BANKNIFTY"),
}


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def _get_index_row_by_code(db: Session, code: str) -> NseIndexMaster:
    """
    UI se aaya hua short code (e.g. NIFTY50) ko use karke
    NseIndexMaster row nikalta hai.
    """
    code = code.upper().strip()

    if code not in INDEX_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported index code: {code}",
        )

    index_symbol, short_code = INDEX_MAP[code]

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
        raise HTTPException(
            status_code=404,
            detail=f"Index not found in NseIndexMaster for code={code}",
        )

    return row


def _compute_index_snapshot(
    db: Session,
    index_row: NseIndexMaster,
    ui_code: str,
) -> Dict[str, Any]:
    """
    Diye gaye index ke liye:
      - latest trading date (intraday)
      - previous trading date (intraday)
      - har constituent ka latest last_price + prev_close (prev day ka last candle)
      - unka sum -> synthetic index value
      - change_abs & change_pct

    NOTE:
      'synthetic_value' / 'synthetic_prev_value' official NSE index level nahi hain.
      Ye sirf sum of constituent prices se nikle hue synthetic levels hain.
      'change_pct' mathematically sahi hota hai, isi ko UI pe dikhana sahi rahega.
    """

    # 1) NIFTY constituents -> securities -> sirf EQ tokens
    token_subq = (
        db.query(NseCmSecurity.token_id)
        .select_from(NseCmSecurity)
        .join(
            NseIndexConstituent,
            (NseIndexConstituent.symbol == NseCmSecurity.symbol)
            & (NseIndexConstituent.index_id == index_row.id),
        )
        .filter(func.upper(NseCmSecurity.series) == "EQ")
        .subquery()
    )

    # 2) Latest intraday trading date for these tokens
    latest_trade_date = (
        db.query(func.max(NseCmIntraday1Min.trade_date))
        .filter(
            NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id)),
        )
        .scalar()
    )

    if latest_trade_date is None:
        raise HTTPException(
            status_code=404,
            detail=f"No intraday data found for index {index_row.index_symbol}",
        )

    # 3) Previous intraday trading date
    prev_trade_date = (
        db.query(func.max(NseCmIntraday1Min.trade_date))
        .filter(
            NseCmIntraday1Min.token_id.in_(select(token_subq.c.token_id)),
            NseCmIntraday1Min.trade_date < latest_trade_date,
        )
        .scalar()
    )

    # 4) Latest intraday candle per token for latest_trade_date
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

    # 5) Prev day ka last candle per token (agar prev_trade_date mila)
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

    LatestBar = aliased(NseCmIntraday1Min)
    PrevBar = aliased(NseCmIntraday1Min)

    # 6) Per-token prices fetch karo (explicit select_from to avoid ambiguity)
    if prev_bar_subq is not None:
        prev_close_expr = func.coalesce(PrevBar.close_price, PrevBar.last_price)

        price_query = (
            db.query(
                NseCmSecurity.token_id,
                LatestBar.last_price.label("last_price"),
                prev_close_expr.label("prev_close"),
                LatestBar.interval_start,
            )
            .select_from(NseCmSecurity)
            .join(
                token_subq,
                token_subq.c.token_id == NseCmSecurity.token_id,
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
            .filter(func.upper(NseCmSecurity.series) == "EQ")
        )
    else:
        # koi previous trading day nahi: prev_close None
        price_query = (
            db.query(
                NseCmSecurity.token_id,
                LatestBar.last_price.label("last_price"),
                func.cast(None, LatestBar.last_price.type).label("prev_close"),
                LatestBar.interval_start,
            )
            .select_from(NseCmSecurity)
            .join(
                token_subq,
                token_subq.c.token_id == NseCmSecurity.token_id,
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
            .filter(func.upper(NseCmSecurity.series) == "EQ")
        )

    rows = price_query.all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No constituent prices found for index {index_row.index_symbol}",
        )

    # 7) Aggregate in Python: synthetic sums
    sum_last = 0.0
    sum_prev = 0.0
    interval_end = None

    for r in rows:
        if r.last_price is not None:
            sum_last += float(r.last_price)
        if r.prev_close is not None:
            sum_prev += float(r.prev_close)
        if interval_end is None or (
            r.interval_start is not None and r.interval_start > interval_end
        ):
            interval_end = r.interval_start

    if sum_prev > 0:
        change_abs = sum_last - sum_prev
        change_pct = (change_abs * 100.0) / sum_prev
    else:
        change_abs = None
        change_pct = None

    snapshot = {
        "index_id": index_row.id,
        "code": ui_code,
        "name": index_row.index_symbol or index_row.short_code or ui_code,
        "latest_trade_date": latest_trade_date.isoformat()
        if latest_trade_date
        else None,
        "prev_trade_date": prev_trade_date.isoformat()
        if prev_trade_date
        else None,
        # synthetic values (sum of constituent prices)
        "synthetic_value": sum_last,
        "synthetic_prev_value": sum_prev if sum_prev > 0 else None,
        "change_abs": change_abs,
        "change_pct": change_pct,
        "interval_end": interval_end.isoformat() if interval_end else None,
    }

    return snapshot


# -------------------------------------------------
# Main endpoint
# -------------------------------------------------
@router.get("/")
async def market_and_sectors(
    indices: str = Query(
        "NIFTY50,NIFTY100,NIFTY500",
        description="Comma separated list of indices, e.g. NIFTY50,NIFTY100,NIFTY500",
    ),
    db: Session = Depends(get_db),
):
    """
    Market & Sectors widget ke liye API.

    - indices: comma separated index codes (e.g. NIFTY50,NIFTY100,NIFTY500)
    - Har index ke liye:
        * synthetic_value         -> sum of constituent last_price
        * synthetic_prev_value    -> sum of constituent prev_close
        * change_abs              -> difference of synthetic values
        * change_pct              -> % change (use this for UI arrows/colours)
        * interval_end            -> latest intraday candle time
    """

    codes: List[str] = [
        c.strip().upper() for c in indices.split(",") if c.strip()
    ]
    if not codes:
        raise HTTPException(status_code=400, detail="No indices supplied")

    snapshots: List[Dict[str, Any]] = []

    for code in codes:
        try:
            index_row = _get_index_row_by_code(db, code)
        except HTTPException as exc:
            # unsupported code / index not found -> skip or propagate
            logger.warning(f"Skipping index {code}: {exc.detail}")
            continue

        snap = _compute_index_snapshot(db, index_row, code)
        snapshots.append(snap)

    return {
        "count": len(snapshots),
        "indices": snapshots,
    }
