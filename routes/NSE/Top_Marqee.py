# routes/NSE/Top_Marqee.py
# ✅ DB-optimized version (complete file)
# Fixes:
# - Avoids func.lower() on hot paths as much as possible (index killer) + keeps fallback
# - Ensures "latest bar" is for the latest TRADE DATE (not across all history)
# - Uses window function (row_number) instead of GROUP BY max() + join-back
# - Uses tokens CTE to avoid repeated IN(subquery) overhead
# - Uses Core select + execute (lighter than ORM .all() for big joins)

import logging
from fastapi import APIRouter, Depends, HTTPException
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
router = APIRouter(prefix="/top-marqee", tags=["top marqee"])


def _get_nifty100_index_row(db: Session) -> NseIndexMaster:
    # Prefer exact matches (index-friendly)
    row = (
        db.query(NseIndexMaster)
        .filter(
            (NseIndexMaster.index_symbol == "NIFTY 100")
            | (NseIndexMaster.short_code == "NIFTY100")
        )
        .one_or_none()
    )

    # Fallback (in case your DB stores different casing)
    if row is None:
        row = (
            db.query(NseIndexMaster)
            .filter(func.lower(NseIndexMaster.index_symbol) == "nifty 100")
            .one_or_none()
        )
    if row is None:
        row = (
            db.query(NseIndexMaster)
            .filter(func.lower(NseIndexMaster.short_code) == "nifty100")
            .one_or_none()
        )

    if row is None:
        raise HTTPException(status_code=404, detail="NIFTY 100 index not found in NseIndexMaster")

    return row


@router.get("/")
async def nifty100TopMarqee(db: Session = Depends(get_db)):
    """
    NIFTY 100 constituents (EQ series) + latest intraday price per token (latest trade_date, last candle).
    """

    index_row = _get_nifty100_index_row(db)

    # 1) Token universe (NIFTY100 constituents + EQ) as CTE
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
    token_filter = NseCmIntraday1Min.token_id.in_(select(tokens_cte.c.token_id))

    # 2) Latest trade_date for these tokens (IMPORTANT: don't pick max interval across all history)
    latest_trade_date = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(token_filter)
    ).scalar()

    if latest_trade_date is None:
        raise HTTPException(status_code=404, detail="No intraday data found for NIFTY 100 tokens")

    # 3) Latest (last) candle per token for that latest_trade_date (window function)
    ranked = (
        select(
            NseCmIntraday1Min.token_id.label("token_id"),
            NseCmIntraday1Min.interval_start.label("interval_start"),
            NseCmIntraday1Min.last_price.label("last_price"),
            NseCmIntraday1Min.close_price.label("close_price"),
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
        .cte("ranked")
    )

    latest_one = (
        select(
            ranked.c.token_id,
            ranked.c.interval_start,
            ranked.c.last_price,
            ranked.c.close_price,
        )
        .where(ranked.c.rn == 1)
        .cte("latest_one")
    )

    # 4) Join security metadata + latest candle
    stmt = (
        select(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            latest_one.c.last_price.label("last_price"),
            latest_one.c.close_price.label("close_price"),
            latest_one.c.interval_start.label("interval_start"),
        )
        .select_from(latest_one)
        .join(NseCmSecurity, NseCmSecurity.token_id == latest_one.c.token_id)
        .where(NseCmSecurity.series == "EQ")
        .order_by(NseCmSecurity.symbol.asc())
    )

    rows = db.execute(stmt).mappings().all()

    result = [
        {
            "token_id": r["token_id"],
            "symbol": r["symbol"],
            "series": r["series"],
            "company_name": r["company_name"],
            "last_price": float(r["last_price"]) if r["last_price"] is not None else None,
            "close_price": float(r["close_price"]) if r["close_price"] is not None else None,
            "interval_start": r["interval_start"].isoformat() if r["interval_start"] else None,
        }
        for r in rows
    ]

    db.close()

    return {
        "index": "NIFTY 100",
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "count": len(result),
        "data": result,
    }
