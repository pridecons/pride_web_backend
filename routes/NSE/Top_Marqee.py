# routes/NSE/Top_Marqee.py
import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select, text
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
    index_row = _get_nifty100_index_row(db)

    # 1) token list (small list ~100)
    token_rows = (
        db.execute(
            select(NseCmSecurity.token_id)
            .select_from(NseIndexConstituent)
            .join(NseCmSecurity, NseCmSecurity.symbol == NseIndexConstituent.symbol)
            .where(
                NseIndexConstituent.index_id == index_row.id,
                NseCmSecurity.series == "EQ",
            )
            .distinct()
        )
        .scalars()
        .all()
    )

    if not token_rows:
        raise HTTPException(status_code=404, detail="No tokens found for NIFTY 100")

    token_ids = list(token_rows)

    # 2) latest trade_date (global max is faster; if you want token-filtered keep old)
    latest_trade_date = db.execute(select(func.max(NseCmIntraday1Min.trade_date))).scalar()
    if latest_trade_date is None:
        raise HTTPException(status_code=404, detail="No intraday data found")

    # prev trade date (global)
    prev_trade_date = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(NseCmIntraday1Min.trade_date < latest_trade_date)
    ).scalar()

    # 3) TODAY latest candle per token using DISTINCT ON (Postgres fast)
    today_sql = text("""
        SELECT DISTINCT ON (token_id)
          token_id,
          interval_start,
          last_price AS today_last,
          close_price AS today_close
        FROM nse_cm_intraday_1min
        WHERE trade_date = :td
          AND token_id = ANY(:token_ids)
        ORDER BY token_id, interval_start DESC
    """)
    today_rows = db.execute(today_sql, {"td": latest_trade_date, "token_ids": token_ids}).mappings().all()

    if not today_rows:
        raise HTTPException(status_code=404, detail="No intraday rows for latest trade_date + tokens")

    # map token -> today
    today_map = {r["token_id"]: r for r in today_rows}

    # 4) PREV latest close per token (optional)
    prev_map = {}
    if prev_trade_date is not None:
        prev_sql = text("""
            SELECT DISTINCT ON (token_id)
              token_id,
              close_price AS prev_close
            FROM nse_cm_intraday_1min
            WHERE trade_date = :td
              AND token_id = ANY(:token_ids)
            ORDER BY token_id, interval_start DESC
        """)
        prev_rows = db.execute(prev_sql, {"td": prev_trade_date, "token_ids": token_ids}).mappings().all()
        prev_map = {r["token_id"]: r["prev_close"] for r in prev_rows}

    # 5) security metadata (small)
    sec_rows = (
        db.execute(
            select(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.series,
                NseCmSecurity.company_name,
            )
            .where(NseCmSecurity.token_id.in_(token_ids), NseCmSecurity.series == "EQ")
        )
        .mappings()
        .all()
    )
    sec_map = {r["token_id"]: r for r in sec_rows}

    result = []
    for token_id, t in today_map.items():
        s = sec_map.get(token_id)
        if not s:
            continue

        last_price = float(t["today_last"]) if t["today_last"] is not None else None

        # prev_close preferred else fallback today_close
        close_raw = prev_map.get(token_id)
        if close_raw is None:
            close_raw = t["today_close"]
        close_price = float(close_raw) if close_raw is not None else None

        change = None
        change_pct = None
        if last_price is not None and close_price not in (None, 0):
            change = last_price - close_price
            change_pct = (change / close_price) * 100.0

        result.append(
            {
                "token_id": token_id,
                "symbol": s["symbol"],
                "series": s["series"],
                "company_name": s["company_name"],
                "last_price": last_price,
                "close_price": close_price,
                "change": round(change, 4) if change is not None else None,
                "change_pct": round(change_pct, 4) if change_pct is not None else None,
                "interval_start": t["interval_start"].isoformat() if t["interval_start"] else None,
            }
        )

    result.sort(key=lambda x: x["symbol"])

    return {
        "index": "NIFTY 100",
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "count": len(result),
        "data": result,
    }
