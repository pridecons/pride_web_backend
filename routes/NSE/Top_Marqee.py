# routes/NSE/Top_Marqee.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func

from db.connection import get_db
from db.models import (
    NseIndexConstituent,
    NseIndexMaster,
    NseCmIntraday1Min,
    NseCmSecurity,
)

router = APIRouter(prefix="/top-marqee", tags=["top marqee"])


@router.get("/")
async def nifty100TopMarqee(db: Session = Depends(get_db)):
    """
    NIFTY 100 ke saare constituents (EQ series) + unka latest price.
    """

    # 1) Index master se NIFTY 100 index nikalo
    index_row = (
        db.query(NseIndexMaster)
        .filter(func.lower(NseIndexMaster.index_symbol) == "nifty 100")
        .one_or_none()
    )

    if index_row is None:
        index_row = (
            db.query(NseIndexMaster)
            .filter(func.lower(NseIndexMaster.short_code) == "nifty100")
            .one_or_none()
        )

    if index_row is None:
        raise HTTPException(
            status_code=404,
            detail="NIFTY 100 index not found in NseIndexMaster",
        )

    # 2) NIFTY100 constituents -> securities (symbol match) -> sirf EQ series ke token_ids
    token_subq = (
        db.query(NseCmSecurity.token_id)
        .join(
            NseIndexConstituent,
            (NseIndexConstituent.symbol == NseCmSecurity.symbol),
        )
        .filter(
            NseIndexConstituent.index_id == index_row.id,
            NseCmSecurity.series == "EQ",
        )
        .subquery()
    )

    # 3) Har token ke liye latest interval_start (most recent candle)
    latest_bar_subq = (
        db.query(
            NseCmIntraday1Min.token_id.label("token_id"),
            func.max(NseCmIntraday1Min.interval_start).label("max_ts"),
        )
        .filter(NseCmIntraday1Min.token_id.in_(token_subq))
        .group_by(NseCmIntraday1Min.token_id)
        .subquery()
    )

    # 4) Securities (sirf EQ) + unke latest candles
    query = (
        db.query(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            NseCmIntraday1Min.last_price,
            NseCmIntraday1Min.close_price,
            NseCmIntraday1Min.interval_start,
        )
        .join(
            NseIndexConstituent,
            (NseIndexConstituent.symbol == NseCmSecurity.symbol)
            & (NseIndexConstituent.index_id == index_row.id),
        )
        .join(
            latest_bar_subq,
            latest_bar_subq.c.token_id == NseCmSecurity.token_id,
        )
        .join(
            NseCmIntraday1Min,
            (NseCmIntraday1Min.token_id == latest_bar_subq.c.token_id)
            & (NseCmIntraday1Min.interval_start == latest_bar_subq.c.max_ts),
        )
        .filter(NseCmSecurity.series == "EQ")  # ✅ sirf EQ series
        .order_by(NseCmSecurity.symbol.asc())
    )

    rows = query.all()

    result = [
        {
            "token_id": r.token_id,
            "symbol": r.symbol,
            "series": r.series,
            "company_name": r.company_name,
            "last_price": float(r.last_price) if r.last_price is not None else None,
            "close_price": float(r.close_price) if r.close_price is not None else None,
            "interval_start": r.interval_start.isoformat() if r.interval_start else None,
        }
        for r in rows
    ]

    return {
        "index": "NIFTY 100",
        "count": len(result),
        "data": result,
    }
