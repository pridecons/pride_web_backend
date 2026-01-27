# routes/NSE/Historical_data.py

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from zoneinfo import ZoneInfo

from db.connection import get_db
from db.models import NseCmBhavcopy, NseCmIntraday1Min, NseCmSecurity

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/nse/historical", tags=["Historical Data"])


def _ist_now_date() -> date:
    return datetime.now(ZoneInfo("Asia/Kolkata")).date()


def _resolve_token_id(db: Session, symbol: str) -> int:
    """
    ✅ IMPORTANT:
    Same symbol can exist with multiple series (EQ/BE/BL/etc).
    For trading + intraday, we must prefer series='EQ'.
    """
    sym = symbol.strip().upper()

    # 1️⃣ Prefer EQ series
    sec_eq = (
        db.query(NseCmSecurity.token_id)
        .filter(
            NseCmSecurity.symbol == sym,
            NseCmSecurity.series == "EQ",
            NseCmSecurity.active_flag.is_(True),
        )
        .first()
    )
    if sec_eq:
        return int(sec_eq[0])

    # 2️⃣ Fallback: any active series
    sec_any = (
        db.query(NseCmSecurity.token_id)
        .filter(
            NseCmSecurity.symbol == sym,
            NseCmSecurity.active_flag.is_(True),
        )
        .first()
    )
    if sec_any:
        return int(sec_any[0])

    # 3️⃣ Final fallback: latest bhavcopy token
    sec_bhav = (
        db.query(NseCmBhavcopy.token_id)
        .filter(NseCmBhavcopy.symbol == sym)
        .order_by(NseCmBhavcopy.trade_date.desc())
        .first()
    )
    if sec_bhav and sec_bhav[0]:
        return int(sec_bhav[0])

    raise HTTPException(status_code=404, detail=f"Token not found for symbol: {sym}")


def _bhav_row_to_candle(r: NseCmBhavcopy) -> Dict[str, Any]:
    return {
        "t": str(r.trade_date),
        "o": float(r.open_price) if r.open_price is not None else None,
        "h": float(r.high_price) if r.high_price is not None else None,
        "l": float(r.low_price) if r.low_price is not None else None,
        "c": float(r.close_price) if r.close_price is not None else None,
        "v": int(r.total_traded_qty) if r.total_traded_qty is not None else None,
            "source": "bhavcopy",
    }


def _intraday_row_to_candle(r: NseCmIntraday1Min) -> Dict[str, Any]:
    return {
        "t": r.interval_start.isoformat(),
        "o": float(r.open_price) if r.open_price is not None else None,
        "h": float(r.high_price) if r.high_price is not None else None,
        "l": float(r.low_price) if r.low_price is not None else None,
        "c": float(r.close_price) if r.close_price is not None else None,
        "v": int(r.volume) if r.volume is not None else None,
        "source": "intraday_1m",
    }


def _aggregate_intraday_to_daily(
    rows: List[NseCmIntraday1Min], trade_date: date
) -> Optional[Dict[str, Any]]:
    """
    Intraday rows -> 1 daily candle
    Assumes rows are ordered by interval_start asc
    """
    if not rows:
        return None

    o = rows[0].open_price
    c = rows[-1].close_price

    highs = [r.high_price for r in rows if r.high_price is not None]
    lows = [r.low_price for r in rows if r.low_price is not None]
    h = max(highs) if highs else None
    l = min(lows) if lows else None

    vols = [r.volume for r in rows if r.volume is not None]
    v = int(sum(vols)) if vols else None

    return {
        "t": str(trade_date),
        "o": float(o) if o is not None else None,
        "h": float(h) if h is not None else None,
        "l": float(l) if l is not None else None,
        "c": float(c) if c is not None else None,
        "v": v,
        "source": "intraday_agg_1d",
    }


@router.get("/candles", summary="Single candles API (auto chooses Bhavcopy vs Intraday)")
def get_candles(
    symbol: str = Query(..., min_length=1),
    interval: str = Query("1d", description="1d or 1m"),
    # for 1d: YYYY-MM-DD ; for 1m: ISO datetime
    from_date: Optional[date] = Query(None, alias="from", description="YYYY-MM-DD (for 1d)"),
    to_date: Optional[date] = Query(None, alias="to", description="YYYY-MM-DD (for 1d)"),
    from_dt: Optional[datetime] = Query(None, alias="from_dt", description="ISO datetime (for 1m)"),
    to_dt: Optional[datetime] = Query(None, alias="to_dt", description="ISO datetime (for 1m)"),
    limit: int = Query(5000, ge=1, le=50000),
    db: Session = Depends(get_db),
):
    sym = symbol.strip().upper()
    interval = interval.strip().lower()

    token_id = _resolve_token_id(db, sym)

    # -------------------------
    # 1) INTRADAY 1m (IST aware)
    # -------------------------
    if interval in ("1m", "1min", "1minute"):
        IST = ZoneInfo("Asia/Kolkata")
        now_ist = datetime.now(IST)

        # defaults: today market hours 09:15 IST to now
        if not to_dt:
            to_dt = now_ist
        else:
            if to_dt.tzinfo is None:
                to_dt = to_dt.replace(tzinfo=IST)

        if not from_dt:
            from_dt = datetime.combine(to_dt.date(), datetime.min.time(), tzinfo=IST).replace(hour=9, minute=15)
        else:
            if from_dt.tzinfo is None:
                from_dt = from_dt.replace(tzinfo=IST)

        if from_dt > to_dt:
            raise HTTPException(status_code=400, detail="from_dt cannot be after to_dt")

        rows = (
            db.query(NseCmIntraday1Min)
            .filter(
                NseCmIntraday1Min.token_id == token_id,
                NseCmIntraday1Min.interval_start >= from_dt,
                NseCmIntraday1Min.interval_start <= to_dt,
            )
            .order_by(NseCmIntraday1Min.interval_start.asc())
            .limit(limit)
            .all()
        )

        return {
            "symbol": sym,
            "interval": "1m",
            "token_id": token_id,
            "count": len(rows),
            "from_dt": from_dt.isoformat(),
            "to_dt": to_dt.isoformat(),
            "data": [_intraday_row_to_candle(r) for r in rows],
        }

    # -------------------------
    # 2) DAILY 1d (Bhavcopy + intraday for today's candle)
    # -------------------------
    if interval in ("1d", "1day", "day"):
        today = _ist_now_date()

        if not to_date:
            to_date = today
        if not from_date:
            from_date = to_date - timedelta(days=365 * 5)

        if from_date > to_date:
            raise HTTPException(status_code=400, detail="from cannot be after to")

        # Fetch bhavcopy for [from_date..min(to_date, yesterday)]
        end_bhav = min(to_date, today - timedelta(days=1))

        out: List[Dict[str, Any]] = []

        if from_date <= end_bhav:
            bhav_rows = (
                db.query(NseCmBhavcopy)
                .filter(
                    NseCmBhavcopy.symbol == sym,
                    NseCmBhavcopy.trade_date >= from_date,
                    NseCmBhavcopy.trade_date <= end_bhav,
                )
                .order_by(NseCmBhavcopy.trade_date.asc())
                .limit(limit)
                .all()
            )
            out.extend([_bhav_row_to_candle(r) for r in bhav_rows])

        # If request includes today, build today's daily candle from intraday
        if to_date >= today:
            intra_rows_today = (
                db.query(NseCmIntraday1Min)
                .filter(
                    NseCmIntraday1Min.token_id == token_id,
                    NseCmIntraday1Min.trade_date == today,
                )
                .order_by(NseCmIntraday1Min.interval_start.asc())
                .all()
            )
            today_candle = _aggregate_intraday_to_daily(intra_rows_today, today)
            if today_candle:
                out.append(today_candle)

        if not out:
            raise HTTPException(status_code=404, detail="No candle data found for requested range.")

        if len(out) > limit:
            out = out[:limit]

        return {
            "symbol": sym,
            "interval": "1d",
            "token_id": token_id,
            "count": len(out),
            "from": str(from_date),
            "to": str(to_date),
            "data": out,
        }

    raise HTTPException(status_code=400, detail="Invalid interval. Use '1d' or '1m'.")
