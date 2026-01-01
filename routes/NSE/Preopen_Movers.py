# routes/NSE/Preopen_Movers.py
# ✅ Optimized DB-friendly version
# Fixes:
# - Removes upper()/lower() index killers where possible
# - Avoids repeated IN (SELECT token_subq) scans by using a tokens CTE
# - Replaces GROUP BY min/max + join-back with window functions (row_number)
# - Single base statement reused for gainers/losers (still 2 queries due to different ORDER BY)

import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select, literal
from sqlalchemy.orm import Session

from db.connection import get_db
from db.models import (
    NseIndexConstituent,
    NseIndexMaster,
    NseCmIntraday1Min,
    NseCmSecurity,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/preopen-movers", tags=["Preopen Movers"])

INDEX_MAP = {
    "NIFTY50": ("NIFTY 50", "NIFTY50"),
    "NIFTY100": ("NIFTY 100", "NIFTY100"),
    "NIFTY500": ("NIFTY 500", "NIFTY500"),
}


# -----------------------------
# Helpers
# -----------------------------
def _get_index_row(db: Session, index_code: str) -> NseIndexMaster:
    index_code = index_code.upper().strip()
    if index_code not in INDEX_MAP:
        raise HTTPException(status_code=400, detail=f"Unsupported index: {index_code}")

    full_name, short_code = INDEX_MAP[index_code]

    # Prefer exact match first (index-friendly)
    row = (
        db.query(NseIndexMaster)
        .filter(
            (NseIndexMaster.index_symbol == full_name)
            | (NseIndexMaster.short_code == short_code)
        )
        .one_or_none()
    )

    # Fallback to case-insensitive
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


def _compute_preopen_snapshot(db: Session, index_row: NseIndexMaster, limit: int) -> dict:
    """
    Preopen movers:
    - token universe from index constituents (EQ)
    - latest_trade_date
    - preopen bar = FIRST interval_start of that day per token
    - prev_close = LAST interval_start of previous day per token (close_price fallback last_price)
    - change_pct computed
    """

    # 1) Tokens CTE (reused)
    tokens_cte = (
        select(NseCmSecurity.token_id)
        .select_from(NseIndexConstituent)
        .join(NseCmSecurity, NseCmSecurity.symbol == NseIndexConstituent.symbol)
        .where(
            NseIndexConstituent.index_id == index_row.id,
            NseCmSecurity.series == "EQ",  # ✅ avoid upper(series)
        )
        .distinct()
        .cte("tokens")
    )
    token_filter = NseCmIntraday1Min.token_id.in_(select(tokens_cte.c.token_id))

    # 2) Latest trade_date
    latest_trade_date = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(token_filter)
    ).scalar()

    if latest_trade_date is None:
        raise HTTPException(
            status_code=404,
            detail=f"No intraday data found for index {index_row.short_code}",
        )

    # 3) Previous trade_date
    prev_trade_date = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(
            token_filter, NseCmIntraday1Min.trade_date < latest_trade_date
        )
    ).scalar()

    if prev_trade_date is None:
        raise HTTPException(status_code=404, detail="Previous trading day intraday data not found")

    # 4) Preopen bar per token (first bar of latest day)
    preopen_ranked = (
        select(
            NseCmIntraday1Min.token_id.label("token_id"),
            NseCmIntraday1Min.interval_start.label("interval_start"),
            NseCmIntraday1Min.last_price.label("preopen_price"),
            func.row_number()
            .over(
                partition_by=NseCmIntraday1Min.token_id,
                order_by=NseCmIntraday1Min.interval_start.asc(),
            )
            .label("rn"),
        )
        .where(
            NseCmIntraday1Min.trade_date == latest_trade_date,
            token_filter,
        )
        .cte("preopen_ranked")
    )

    preopen_one = (
        select(
            preopen_ranked.c.token_id,
            preopen_ranked.c.interval_start.label("preopen_time"),
            preopen_ranked.c.preopen_price,
        )
        .where(preopen_ranked.c.rn == 1)
        .cte("preopen_one")
    )

    # 5) Prev day last candle per token
    prev_ranked = (
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

    prev_one = (
        select(prev_ranked.c.token_id, prev_ranked.c.prev_close)
        .where(prev_ranked.c.rn == 1)
        .cte("prev_one")
    )

    prev_close = prev_one.c.prev_close
    change_pct = (
        (preopen_one.c.preopen_price - prev_close)
        * 100.0
        / func.nullif(prev_close, 0.0)
    )

    # Base statement
    base_stmt = (
        select(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            preopen_one.c.preopen_price.label("preopen_price"),
            prev_close.label("prev_close"),
            change_pct.label("change_pct"),
            preopen_one.c.preopen_time.label("preopen_time"),
        )
        .select_from(preopen_one)
        .join(NseCmSecurity, NseCmSecurity.token_id == preopen_one.c.token_id)
        .join(prev_one, prev_one.c.token_id == preopen_one.c.token_id)
        .where(
            NseCmSecurity.series == "EQ",
            preopen_one.c.preopen_price.isnot(None),
            prev_close.isnot(None),
            prev_close != 0,
        )
    )

    # 6) gainers & losers (2 queries due to different ordering)
    gainers = db.execute(
        base_stmt.where(change_pct > 0).order_by(change_pct.desc()).limit(limit)
    ).mappings().all()

    losers = db.execute(
        base_stmt.where(change_pct < 0).order_by(change_pct.asc()).limit(limit)
    ).mappings().all()

    def _serialize(r: dict) -> dict:
        pre = float(r["preopen_price"]) if r["preopen_price"] is not None else None
        prev = float(r["prev_close"]) if r["prev_close"] is not None else None
        chg = float(r["change_pct"]) if r["change_pct"] is not None else None
        return {
            "token_id": r["token_id"],
            "symbol": r["symbol"],
            "series": r["series"],
            "company_name": r["company_name"],
            "preopen_price": pre,
            "prev_close": prev,
            "change_abs": (pre - prev) if (pre is not None and prev is not None) else None,
            "change_pct": chg,
            "preopen_time": r["preopen_time"].isoformat() if r["preopen_time"] else None,
        }
    db.close()

    return {
        "index_id": index_row.id,
        "code": index_row.short_code,
        "name": index_row.index_symbol,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "gainers_count": len(gainers),
        "losers_count": len(losers),
        "gainers": [_serialize(r) for r in gainers],
        "losers": [_serialize(r) for r in losers],
    }


# -----------------------------
# Endpoint
# -----------------------------
@router.get("/")
async def preopen_movers(
    index: str = Query("NIFTY50", description="Index: NIFTY50 / NIFTY100 / NIFTY500"),
    limit: int = Query(3, ge=1, le=50, description="Top gainers/losers to return"),
    db: Session = Depends(get_db),
):
    index_row = _get_index_row(db, index)
    return _compute_preopen_snapshot(db, index_row, limit)
