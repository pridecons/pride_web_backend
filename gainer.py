# routes/NSE/Top_Marqee.py

from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, aliased
from sqlalchemy import func, select

from db.connection import get_db, SessionLocal
from db.models import (
    NseIndexConstituent,
    NseIndexMaster,
    NseCmIntraday1Min,
    NseCmSecurity,
    NseCmBhavcopy,
)

router = APIRouter(prefix="/top-marqee", tags=["top marqee"])


# ---------- COMMON HELPER: NIFTY100 token_subq + latest_trade_date + latest_bar_subq ----------

def _get_nifty100_latest_bars(db: Session):
    """
    Return:
      - index_row
      - latest_trade_date
      - token_subq (NIFTY100 EQ tokens)
      - latest_bar_subq (per token latest candle on latest_trade_date)
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
            func.upper(NseCmSecurity.series) == "EQ",
        )
        .subquery()
    )

    # 3) Latest trading date (intraday) for these tokens
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
            detail="No intraday data found for NIFTY 100 tokens",
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

    return index_row, latest_trade_date, token_subq, latest_bar_subq


# ---------- NIFTY100: 52-WEEK HIGH ----------

@router.get("/nifty100/52w-high")
def nifty100_52w_high(
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """
    NIFTY 100 ke stocks jo apne 52-week high ke kareeb hain.

    52-week high = pichhle 365 din ke bhavcopy (HIGH) ka max per token.
    hum:
      - latest intraday last_price lete hain
      - 365 din me se max(high_price) lete hain
      - filter: last_price >= 0.98 * high_52  (yaani <= 2% niche)
      - order: last_price / high_52 desc (jo sabse zyada close to high)
    """

    # 1) Index master se NIFTY 100 lo
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

    # 2) NIFTY100 constituents -> securities -> sirf EQ tokens
    token_subq = (
        db.query(NseCmSecurity.token_id)
        .join(
            NseIndexConstituent,
            (NseIndexConstituent.symbol == NseCmSecurity.symbol),
        )
        .filter(
            NseIndexConstituent.index_id == index_row.id,
            func.upper(NseCmSecurity.series) == "EQ",
        )
        .subquery()
    )

    # 3) Latest intraday trading date for these tokens
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
            detail="No intraday data found for NIFTY 100 tokens",
        )

    # 4) 52-week window (365 days) for bhavcopy
    cutoff_end = latest_trade_date
    cutoff_start = cutoff_end - timedelta(days=365)

    # 5) 52-week high per token (BHAVCOPY se)
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

    # 6) Latest intraday candle per token for latest_trade_date
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

    LatestBar = aliased(NseCmIntraday1Min)

    # 7) last_price + 52w high join karo, filter & ranking
    ratio_expr = LatestBar.last_price / func.nullif(high_52_subq.c.high_52, 0.0)

    query = (
        db.query(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            LatestBar.last_price.label("last_price"),
            high_52_subq.c.high_52.label("high_52"),
            ratio_expr.label("high_proximity"),  # last_price / high_52
            LatestBar.interval_start,
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
            LatestBar,
            (LatestBar.token_id == latest_bar_subq.c.token_id)
            & (LatestBar.interval_start == latest_bar_subq.c.max_ts),
        )
        .join(
            high_52_subq,
            high_52_subq.c.token_id == NseCmSecurity.token_id,
        )
        .filter(
            func.upper(NseCmSecurity.series) == "EQ",
            high_52_subq.c.high_52.isnot(None),
            high_52_subq.c.high_52 > 0,
            LatestBar.last_price.isnot(None),
            LatestBar.last_price > 0,
            # ✅ sirf woh jaha last_price 52w high ke 2% ke andar hai:
            LatestBar.last_price >= 0.98 * high_52_subq.c.high_52,
        )
        # ✅ jo 52w high ke sabse kareeb hai, wo top par
        .order_by(ratio_expr.desc())
        .limit(limit)
    )

    rows = query.all()

    # Debug print (optional)
    print(
        f"{'token_id':>8}  "
        f"{'symbol':<12}  "
        f"{'last_price':>12}  "
        f"{'high_52':>12}  "
        f"{'near_high_%':>12}  "
        f"{'interval_start'}"
    )
    print("-" * 130)
    for r in rows:
        lp = float(r.last_price) if r.last_price is not None else 0.0
        h52 = float(r.high_52) if r.high_52 is not None else 0.0
        pct_from_high = (lp / h52 - 1.0) * 100 if h52 > 0 else 0.0
        print(
            f"{r.token_id:>8}  "
            f"{r.symbol:<12}  "
            f"{lp:>12.2f}  "
            f"{h52:>12.2f}  "
            f"{pct_from_high:>12.2f}  "
            f"{r.interval_start}"
        )

    return {
        "index": index_row.index_symbol,
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
                "high_52": float(r.high_52)
                if r.high_52 is not None
                else None,
                # kitna pass hai 52w high se (% terms)
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


# ---------- NIFTY100: 52-WEEK LOW ----------

@router.get("/nifty100/52w-low")
def nifty100_52w_low(
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """
    NIFTY 100 ke stocks jo apne 52-week low ke kareeb hain.

    52-week low = pichhle 365 din ke bhavcopy (LOW) ka min per token.
    hum:
      - latest intraday last_price lete hain
      - 365 din me se min(low_price) lete hain
      - filter: last_price <= 1.02 * low_52  (yaani low se max +2% upar)
      - order: last_price / low_52 asc (jo sabse zyada close to low, wo top par)
    """

    # 1) Index master se NIFTY 100 lo
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

    # 2) NIFTY100 constituents -> securities -> sirf EQ tokens
    token_subq = (
        db.query(NseCmSecurity.token_id)
        .join(
            NseIndexConstituent,
            (NseIndexConstituent.symbol == NseCmSecurity.symbol),
        )
        .filter(
            NseIndexConstituent.index_id == index_row.id,
            func.upper(NseCmSecurity.series) == "EQ",
        )
        .subquery()
    )

    # 3) Latest intraday trading date for these tokens
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
            detail="No intraday data found for NIFTY 100 tokens",
        )

    # 4) 52-week window (365 days) for bhavcopy
    cutoff_end = latest_trade_date
    cutoff_start = cutoff_end - timedelta(days=365)

    # 5) 52-week low per token (BHAVCOPY se)
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

    # 6) Latest intraday candle per token for latest_trade_date
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

    LatestBar = aliased(NseCmIntraday1Min)

    # 7) last_price + 52w low join karo, filter & ranking
    ratio_expr = LatestBar.last_price / func.nullif(low_52_subq.c.low_52, 0.0)

    query = (
        db.query(
            NseCmSecurity.token_id,
            NseCmSecurity.symbol,
            NseCmSecurity.series,
            NseCmSecurity.company_name,
            LatestBar.last_price.label("last_price"),
            low_52_subq.c.low_52.label("low_52"),
            ratio_expr.label("low_proximity"),  # last_price / low_52
            LatestBar.interval_start,
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
            LatestBar,
            (LatestBar.token_id == latest_bar_subq.c.token_id)
            & (LatestBar.interval_start == latest_bar_subq.c.max_ts),
        )
        .join(
            low_52_subq,
            low_52_subq.c.token_id == NseCmSecurity.token_id,
        )
        .filter(
            func.upper(NseCmSecurity.series) == "EQ",
            low_52_subq.c.low_52.isnot(None),
            low_52_subq.c.low_52 > 0,
            LatestBar.last_price.isnot(None),
            LatestBar.last_price > 0,
            # ✅ ideally price low se niche nahi aata, but corporate actions ke case me ho sakta hai.
            # Hum sirf wo le rahe hain jo low se max 2% upar hain:
            LatestBar.last_price <= 1.02 * low_52_subq.c.low_52,
        )
        # ✅ jo 52w low ke sabse kareeb hai, wo top par (ratio 1.00 ke paas)
        .order_by(ratio_expr.asc())
        .limit(limit)
    )

    rows = query.all()

    # Debug print (optional)
    print(
        f"{'token_id':>8}  "
        f"{'symbol':<12}  "
        f"{'last_price':>12}  "
        f"{'low_52':>12}  "
        f"{'above_low_%':>12}  "
        f"{'interval_start'}"
    )
    print("-" * 130)
    for r in rows:
        lp = float(r.last_price) if r.last_price is not None else 0.0
        l52 = float(r.low_52) if r.low_52 is not None else 0.0
        pct_above_low = (lp / l52 - 1.0) * 100 if l52 > 0 else 0.0
        print(
            f"{r.token_id:>8}  "
            f"{r.symbol:<12}  "
            f"{lp:>12.2f}  "
            f"{l52:>12.2f}  "
            f"{pct_above_low:>12.2f}  "
            f"{r.interval_start}"
        )

    return {
        "index": index_row.index_symbol,
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
                "low_52": float(r.low_52)
                if r.low_52 is not None
                else None,
                # low ke kitna upar hai (% terms)
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


# Manual test run
if __name__ == "__main__":
    db = SessionLocal()
    try:
        print("=== 52W HIGH ===")
        nifty100_52w_high(db=db)
        print("\n=== 52W LOW ===")
        nifty100_52w_low(db=db)
    finally:
        db.close()
