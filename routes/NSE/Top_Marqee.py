# routes/NSE/Top_Marqee.py

import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
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
    row = (
        db.query(NseIndexMaster)
        .filter(
            (NseIndexMaster.index_symbol == "NIFTY 100")
            | (NseIndexMaster.short_code == "NIFTY100")
        )
        .one_or_none()
    )

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
    NIFTY 100 constituents (EQ) +:
    - today_last: latest intraday last_price (latest trade_date, last candle)
    - prev_close: previous trade_date last candle close_price (or last_price)
    """

    index_row = _get_nifty100_index_row(db)

    # 1) Token universe (NIFTY100 constituents + EQ) as CTE
    tokens_cte = (
        select(NseCmSecurity.token_id)
        .select_from(NseIndexConstituent)
        .join(NseCmSecurity, NseCmSecurity.symbol == NseIndexConstituent.symbol)
        .where(
            NseIndexConstituent.index_id == index_row.id,
            NseCmSecurity.series == "EQ",
        )
        .distinct()
        .cte("tokens")
    )
    token_filter = NseCmIntraday1Min.token_id.in_(select(tokens_cte.c.token_id))

    # 2) Latest trade_date for these tokens
    latest_trade_date = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(token_filter)
    ).scalar()

    if latest_trade_date is None:
        raise HTTPException(status_code=404, detail="No intraday data found for NIFTY 100 tokens")

    # ✅ previous trade_date (strictly < latest_trade_date)
    prev_trade_date = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date))
        .where(token_filter, NseCmIntraday1Min.trade_date < latest_trade_date)
    ).scalar()

    # 3) Latest candle per token for latest_trade_date
    ranked_today = (
        select(
            NseCmIntraday1Min.token_id.label("token_id"),
            NseCmIntraday1Min.interval_start.label("interval_start"),
            NseCmIntraday1Min.last_price.label("today_last"),
            NseCmIntraday1Min.close_price.label("today_close"),
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
        .cte("ranked_today")
    )

    latest_today = (
        select(
            ranked_today.c.token_id,
            ranked_today.c.interval_start,
            ranked_today.c.today_last,
            ranked_today.c.today_close,
        )
        .where(ranked_today.c.rn == 1)
        .cte("latest_today")
    )

    # 4) Prev day close per token (last candle on prev_trade_date)
    # If prev_trade_date missing, we will return None
    if prev_trade_date is not None:
        ranked_prev = (
            select(
                NseCmIntraday1Min.token_id.label("token_id"),
                NseCmIntraday1Min.close_price.label("prev_close"),
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
            .cte("ranked_prev")
        )

        latest_prev = (
            select(
                ranked_prev.c.token_id,
                ranked_prev.c.prev_close,
            )
            .where(ranked_prev.c.rn == 1)
            .cte("latest_prev")
        )
    else:
        latest_prev = None

    # 5) Join security metadata + latest candle(s)
    if latest_prev is not None:
        stmt = (
            select(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.series,
                NseCmSecurity.company_name,
                latest_today.c.today_last.label("last_price"),
                latest_prev.c.prev_close.label("close_price"),  # ✅ this is prev day close now
                latest_today.c.interval_start.label("interval_start"),
            )
            .select_from(latest_today)
            .join(NseCmSecurity, NseCmSecurity.token_id == latest_today.c.token_id)
            .outerjoin(latest_prev, latest_prev.c.token_id == latest_today.c.token_id)
            .where(NseCmSecurity.series == "EQ")
            .order_by(NseCmSecurity.symbol.asc())
        )
    else:
        # no prev date available
        stmt = (
            select(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.series,
                NseCmSecurity.company_name,
                latest_today.c.today_last.label("last_price"),
                latest_today.c.today_close.label("close_price"),  # fallback same-day close
                latest_today.c.interval_start.label("interval_start"),
            )
            .select_from(latest_today)
            .join(NseCmSecurity, NseCmSecurity.token_id == latest_today.c.token_id)
            .where(NseCmSecurity.series == "EQ")
            .order_by(NseCmSecurity.symbol.asc())
        )

    rows = db.execute(stmt).mappings().all()

    result = []
    for r in rows:
        last_price = float(r["last_price"]) if r["last_price"] is not None else None
        close_price = float(r["close_price"]) if r["close_price"] is not None else None

        change = None
        change_pct = None
        if last_price is not None and close_price not in (None, 0):
            change = last_price - close_price
            change_pct = (change / close_price) * 100.0

        result.append(
            {
                "token_id": r["token_id"],
                "symbol": r["symbol"],
                "series": r["series"],
                "company_name": r["company_name"],
                "last_price": last_price,
                "close_price": close_price,  # ✅ prev close (if available)
                "change": round(change, 4) if change is not None else None,
                "change_pct": round(change_pct, 4) if change_pct is not None else None,
                "interval_start": r["interval_start"].isoformat() if r["interval_start"] else None,
            }
        )

    db.close()

    return {
        "index": "NIFTY 100",
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "count": len(result),
        "data": result,
    }
