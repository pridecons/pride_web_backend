# routes/NSE/Todays_Stock.py

import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select, literal, text
from sqlalchemy.orm import Session

from db.connection import get_db
from db.models import (
    NseIndexConstituent,
    NseIndexMaster,
    NseCmIntraday1Min,
    NseCmSecurity,
    NseCmBhavcopy,
)

router = APIRouter(prefix="/today-stock", tags=["Today Stock"])
logger = logging.getLogger(__name__)

INDEX_MAP = {
    "NIFTY50": ("NIFTY 50", "NIFTY50"),
    "NIFTY100": ("NIFTY 100", "NIFTY100"),
    "NIFTY500": ("NIFTY 500", "NIFTY500"),
    # ALL -> all EQ
}


# ===========================
#  Token universe helpers
# ===========================
def _get_index_row(db: Session, index_key: str) -> NseIndexMaster:
    key = (index_key or "").upper().strip()
    if key not in INDEX_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported index '{index_key}'. Use NIFTY50 / NIFTY100 / NIFTY500 / ALL.",
        )

    index_symbol, short_code = INDEX_MAP[key]

    row = (
        db.query(NseIndexMaster)
        .filter((NseIndexMaster.index_symbol == index_symbol) | (NseIndexMaster.short_code == short_code))
        .one_or_none()
    )

    if row is None:
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
        raise HTTPException(status_code=404, detail=f"Index not found in NseIndexMaster for {index_key}")

    return row


def _get_index_name_and_token_ids(db: Session, index_key: str) -> Tuple[str, List[int]]:
    key = (index_key or "").upper().strip()

    if key == "ALL":
        token_ids = (
            db.execute(
                select(NseCmSecurity.token_id)
                .where(NseCmSecurity.series == "EQ", NseCmSecurity.token_id.isnot(None))
                .distinct()
            )
            .scalars()
            .all()
        )
        return "ALL_EQ", [int(x) for x in token_ids if x is not None]

    if key not in INDEX_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported index '{index_key}'. Use NIFTY50 / NIFTY100 / NIFTY500 / ALL.",
        )

    index_row = _get_index_row(db, key)

    token_ids = (
        db.execute(
            select(NseCmSecurity.token_id)
            .select_from(NseIndexConstituent)
            .join(NseCmSecurity, NseCmSecurity.symbol == NseIndexConstituent.symbol)
            .where(
                NseIndexConstituent.index_id == index_row.id,
                NseCmSecurity.series == "EQ",
                NseCmSecurity.token_id.isnot(None),
            )
            .distinct()
        )
        .scalars()
        .all()
    )

    return index_row.index_symbol, [int(x) for x in token_ids if x is not None]


# ===========================
#  Trade date helpers
# ===========================
def _get_latest_trade_date_for_tokens(db: Session, token_ids: List[int]):
    if not token_ids:
        raise HTTPException(status_code=404, detail="No tokens found for given index")

    # token-filtered max date (accurate)
    latest_td = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(NseCmIntraday1Min.token_id.in_(token_ids))
    ).scalar()

    if latest_td is None:
        raise HTTPException(status_code=404, detail="No intraday data found for given index tokens")
    return latest_td


def _get_prev_trade_date_for_tokens(db: Session, token_ids: List[int], latest_trade_date):
    if not token_ids or latest_trade_date is None:
        return None

    return db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(
            NseCmIntraday1Min.token_id.in_(token_ids),
            NseCmIntraday1Min.trade_date < latest_trade_date,
        )
    ).scalar()


# ===========================
#  Fast intraday lookup (DISTINCT ON)
# ===========================
def _latest_intraday_map(db: Session, token_ids: List[int], trade_date):
    """
    token_id -> latest row on that trade_date
    """
    if not token_ids or trade_date is None:
        return {}

    sql = text("""
        SELECT DISTINCT ON (token_id)
          token_id,
          interval_start,
          last_price,
          close_price,
          total_traded_qty,
          volume
        FROM nse_cm_intraday_1min
        WHERE trade_date = :td
          AND token_id = ANY(:token_ids)
        ORDER BY token_id, interval_start DESC
    """)

    rows = db.execute(sql, {"td": trade_date, "token_ids": token_ids}).mappings().all()
    return {int(r["token_id"]): r for r in rows}


def _prev_close_map(db: Session, token_ids: List[int], trade_date):
    """
    token_id -> prev_close (coalesce close_price,last_price) from prev day last candle
    """
    if not token_ids or trade_date is None:
        return {}

    sql = text("""
        SELECT DISTINCT ON (token_id)
          token_id,
          COALESCE(close_price, last_price) AS prev_close
        FROM nse_cm_intraday_1min
        WHERE trade_date = :td
          AND token_id = ANY(:token_ids)
        ORDER BY token_id, interval_start DESC
    """)

    rows = db.execute(sql, {"td": trade_date, "token_ids": token_ids}).mappings().all()
    return {int(r["token_id"]): r["prev_close"] for r in rows}


# ===========================
#  DB-side 10-point sampling (fast)
# ===========================
def _sample_1day_lastprice_10_batch_fast(
    db: Session, token_ids: List[int], trade_date
) -> Dict[int, List[Dict[str, Any]]]:
    """
    Returns {token_id: [{interval_start,last} x10]}
    DB side sampling: no full-day pull to python.
    """
    if not token_ids or trade_date is None:
        return {}

    sql = text("""
        WITH base AS (
          SELECT
            token_id,
            interval_start,
            COALESCE(last_price, close_price) AS last,
            row_number() OVER (PARTITION BY token_id ORDER BY interval_start) AS rn,
            count(*)    OVER (PARTITION BY token_id) AS cnt
          FROM nse_cm_intraday_1min
          WHERE trade_date = :td
            AND token_id = ANY(:token_ids)
        ),
        picks AS (
          SELECT token_id, interval_start, last
          FROM base
          WHERE cnt <= 10
             OR rn IN (
               1,
               (1 + (cnt-1) * 1 / 9),
               (1 + (cnt-1) * 2 / 9),
               (1 + (cnt-1) * 3 / 9),
               (1 + (cnt-1) * 4 / 9),
               (1 + (cnt-1) * 5 / 9),
               (1 + (cnt-1) * 6 / 9),
               (1 + (cnt-1) * 7 / 9),
               (1 + (cnt-1) * 8 / 9),
               cnt
             )
        )
        SELECT token_id, interval_start, last
        FROM picks
        ORDER BY token_id, interval_start;
    """)

    rows = db.execute(sql, {"td": trade_date, "token_ids": token_ids}).mappings().all()

    out: Dict[int, List[Dict[str, Any]]] = {}
    for r in rows:
        tid = int(r["token_id"])
        out.setdefault(tid, []).append(
            {
                "interval_start": r["interval_start"].isoformat() if r["interval_start"] else None,
                "last": float(r["last"]) if r["last"] is not None else None,
            }
        )
    return out


def _attach_samples(db: Session, rows: List[Dict[str, Any]], latest_trade_date):
    token_ids = [int(r["token_id"]) for r in rows if r.get("token_id") is not None]
    if not token_ids:
        return {}

    # Safety: very large limits => skip samples (optional but recommended)
    # if len(token_ids) > 80:
    #     return {}

    return _sample_1day_lastprice_10_batch_fast(db, token_ids, latest_trade_date)


# ===========================
#  Serialization
# ===========================
def _serialize_rows(rows: List[Dict[str, Any]], sample_map: Optional[Dict[int, List[Dict[str, Any]]]] = None):
    sample_map = sample_map or {}
    return [
        {
            "token_id": r["token_id"],
            "symbol": r["symbol"],
            "series": r["series"],
            "company_name": r["company_name"],
            "last_price": float(r["last_price"]) if r.get("last_price") is not None else None,
            "prev_close": float(r["prev_close"]) if r.get("prev_close") is not None else None,
            "change_pct": float(r["change_pct"]) if r.get("change_pct") is not None else None,
            "close_price": float(r["close_price"]) if r.get("close_price") is not None else None,
            "total_traded_qty": int(r["total_traded_qty"]) if r.get("total_traded_qty") is not None else None,
            "volume": int(r["volume"]) if r.get("volume") is not None else None,
            "activity_metric": int(r["activity_metric"]) if r.get("activity_metric") is not None else None,
            "high_52": float(r["high_52"]) if r.get("high_52") is not None else None,
            "low_52": float(r["low_52"]) if r.get("low_52") is not None else None,
            "near_high_pct": float(r["near_high_pct"]) if r.get("near_high_pct") is not None else None,
            "above_low_pct": float(r["above_low_pct"]) if r.get("above_low_pct") is not None else None,
            "interval_start": r["interval_start"].isoformat() if r.get("interval_start") else None,
            "sample_1d_last": sample_map.get(int(r["token_id"]), []),
        }
        for r in rows
    ]


# ===========================
#  Common securities fetch
# ===========================
def _security_meta_map(db: Session, token_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not token_ids:
        return {}
    rows = (
        db.execute(
            select(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.series,
                NseCmSecurity.company_name,
            ).where(NseCmSecurity.token_id.in_(token_ids), NseCmSecurity.series == "EQ")
        )
        .mappings()
        .all()
    )
    return {int(r["token_id"]): r for r in rows if r.get("token_id") is not None}


# ===========================
#  Filter implementations (FAST)
# ===========================
def _fetch_gainers(db: Session, index_key: str, limit: int):
    index_name, token_ids = _get_index_name_and_token_ids(db, index_key)

    latest_trade_date = _get_latest_trade_date_for_tokens(db, token_ids)
    prev_trade_date = _get_prev_trade_date_for_tokens(db, token_ids, latest_trade_date)

    latest_map = _latest_intraday_map(db, token_ids, latest_trade_date)
    if not latest_map:
        raise HTTPException(status_code=404, detail="No intraday rows for latest trade_date + tokens")

    prev_map = _prev_close_map(db, token_ids, prev_trade_date) if prev_trade_date else {}

    sec_map = _security_meta_map(db, list(latest_map.keys()))

    items: List[Dict[str, Any]] = []
    for tid, t in latest_map.items():
        s = sec_map.get(tid)
        if not s:
            continue

        last_raw = t.get("last_price")
        last_price = float(last_raw) if last_raw is not None else None

        prev_raw = prev_map.get(tid)
        prev_close = float(prev_raw) if prev_raw is not None else None

        change_pct = None
        if last_price is not None and prev_close not in (None, 0):
            change_pct = (last_price - prev_close) * 100.0 / prev_close

        items.append(
            {
                "token_id": tid,
                "symbol": s["symbol"],
                "series": s["series"],
                "company_name": s["company_name"],
                "last_price": last_price,
                "prev_close": prev_close,
                "change_pct": change_pct,
                "close_price": None,
                "total_traded_qty": t.get("total_traded_qty"),
                "volume": t.get("volume"),
                "activity_metric": None,
                "high_52": None,
                "low_52": None,
                "near_high_pct": None,
                "above_low_pct": None,
                "interval_start": t.get("interval_start"),
            }
        )

    # filter + sort
    items = [x for x in items if x.get("change_pct") is not None and x["change_pct"] > 0]
    items.sort(key=lambda x: x["change_pct"], reverse=True)

    items = items[:limit]
    sample_map = _attach_samples(db, items, latest_trade_date)

    return {
        "filter": "GAINERS",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "count": len(items),
        "data": _serialize_rows(items, sample_map),
    }


def _fetch_losers(db: Session, index_key: str, limit: int):
    index_name, token_ids = _get_index_name_and_token_ids(db, index_key)

    latest_trade_date = _get_latest_trade_date_for_tokens(db, token_ids)
    prev_trade_date = _get_prev_trade_date_for_tokens(db, token_ids, latest_trade_date)

    latest_map = _latest_intraday_map(db, token_ids, latest_trade_date)
    if not latest_map:
        raise HTTPException(status_code=404, detail="No intraday rows for latest trade_date + tokens")

    prev_map = _prev_close_map(db, token_ids, prev_trade_date) if prev_trade_date else {}
    sec_map = _security_meta_map(db, list(latest_map.keys()))

    items: List[Dict[str, Any]] = []
    for tid, t in latest_map.items():
        s = sec_map.get(tid)
        if not s:
            continue

        last_raw = t.get("last_price")
        last_price = float(last_raw) if last_raw is not None else None

        prev_raw = prev_map.get(tid)
        prev_close = float(prev_raw) if prev_raw is not None else None

        change_pct = None
        if last_price is not None and prev_close not in (None, 0):
            change_pct = (last_price - prev_close) * 100.0 / prev_close

        items.append(
            {
                "token_id": tid,
                "symbol": s["symbol"],
                "series": s["series"],
                "company_name": s["company_name"],
                "last_price": last_price,
                "prev_close": prev_close,
                "change_pct": change_pct,
                "close_price": None,
                "total_traded_qty": t.get("total_traded_qty"),
                "volume": t.get("volume"),
                "activity_metric": None,
                "high_52": None,
                "low_52": None,
                "near_high_pct": None,
                "above_low_pct": None,
                "interval_start": t.get("interval_start"),
            }
        )

    items = [x for x in items if x.get("change_pct") is not None and x["change_pct"] < 0]
    items.sort(key=lambda x: x["change_pct"])  # ascending => most negative first

    items = items[:limit]
    sample_map = _attach_samples(db, items, latest_trade_date)

    return {
        "filter": "LOSERS",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "count": len(items),
        "data": _serialize_rows(items, sample_map),
    }


def _fetch_most_active(db: Session, index_key: str, limit: int):
    index_name, token_ids = _get_index_name_and_token_ids(db, index_key)

    latest_trade_date = _get_latest_trade_date_for_tokens(db, token_ids)
    prev_trade_date = _get_prev_trade_date_for_tokens(db, token_ids, latest_trade_date)

    latest_map = _latest_intraday_map(db, token_ids, latest_trade_date)
    if not latest_map:
        raise HTTPException(status_code=404, detail="No intraday rows for latest trade_date + tokens")

    prev_map = _prev_close_map(db, token_ids, prev_trade_date) if prev_trade_date else {}
    sec_map = _security_meta_map(db, list(latest_map.keys()))

    items: List[Dict[str, Any]] = []
    for tid, t in latest_map.items():
        s = sec_map.get(tid)
        if not s:
            continue

        last_raw = t.get("last_price")
        last_price = float(last_raw) if last_raw is not None else None

        prev_raw = prev_map.get(tid)
        prev_close = float(prev_raw) if prev_raw is not None else None

        change_pct = None
        if last_price is not None and prev_close not in (None, 0):
            change_pct = (last_price - prev_close) * 100.0 / prev_close

        activity_metric = t.get("total_traded_qty")  # ONLY total_traded_qty

        items.append(
            {
                "token_id": tid,
                "symbol": s["symbol"],
                "series": s["series"],
                "company_name": s["company_name"],
                "last_price": last_price,
                "prev_close": prev_close,
                "change_pct": change_pct,
                "close_price": (float(t["close_price"]) if t.get("close_price") is not None else None),
                "total_traded_qty": t.get("total_traded_qty"),
                "volume": t.get("volume"),
                "activity_metric": int(activity_metric) if activity_metric is not None else None,
                "high_52": None,
                "low_52": None,
                "near_high_pct": None,
                "above_low_pct": None,
                "interval_start": t.get("interval_start"),
            }
        )

    items = [x for x in items if x.get("activity_metric") is not None and x["activity_metric"] > 0]
    items.sort(key=lambda x: x["activity_metric"], reverse=True)

    items = items[:limit]
    sample_map = _attach_samples(db, items, latest_trade_date)

    return {
        "filter": "MOST_ACTIVE",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "count": len(items),
        "data": _serialize_rows(items, sample_map),
    }


# ===========================
#  52W High/Low (bhavcopy)
# ===========================
def _fetch_52w_high(db: Session, index_key: str, limit: int):
    index_name, token_ids = _get_index_name_and_token_ids(db, index_key)

    latest_trade_date = _get_latest_trade_date_for_tokens(db, token_ids)
    prev_trade_date = _get_prev_trade_date_for_tokens(db, token_ids, latest_trade_date)

    cutoff_end = latest_trade_date
    cutoff_start = cutoff_end - timedelta(days=365)

    # latest last for tokens
    latest_map = _latest_intraday_map(db, token_ids, latest_trade_date)
    if not latest_map:
        raise HTTPException(status_code=404, detail="No intraday rows for latest trade_date + tokens")

    prev_map = _prev_close_map(db, token_ids, prev_trade_date) if prev_trade_date else {}
    sec_map = _security_meta_map(db, list(latest_map.keys()))

    # 52w high from bhavcopy (grouped)
    # Note: join symbol+series; bhavcopy series can be null => treat as EQ via coalesce
    bhav_series = func.coalesce(NseCmBhavcopy.series, literal("EQ"))

    high_rows = (
        db.execute(
            select(
                NseCmSecurity.token_id.label("token_id"),
                func.max(NseCmBhavcopy.high_price).label("high_52"),
            )
            .select_from(NseCmBhavcopy)
            .join(
                NseCmSecurity,
                (NseCmSecurity.symbol == NseCmBhavcopy.symbol) & (NseCmSecurity.series == bhav_series),
            )
            .where(
                NseCmBhavcopy.trade_date >= cutoff_start,
                NseCmBhavcopy.trade_date <= cutoff_end,
                NseCmSecurity.token_id.in_(token_ids),
                NseCmSecurity.series == "EQ",
                NseCmBhavcopy.high_price.isnot(None),
            )
            .group_by(NseCmSecurity.token_id)
        )
        .mappings()
        .all()
    )

    high_map = {int(r["token_id"]): r["high_52"] for r in high_rows if r.get("token_id") is not None}

    items: List[Dict[str, Any]] = []
    for tid, t in latest_map.items():
        s = sec_map.get(tid)
        if not s:
            continue

        high_raw = high_map.get(tid)
        if high_raw is None:
            continue

        last_raw = t.get("last_price")
        last_price = float(last_raw) if last_raw is not None else None
        if last_price in (None, 0):
            continue

        high_52 = float(high_raw) if high_raw is not None else None
        if high_52 in (None, 0):
            continue

        prev_raw = prev_map.get(tid)
        prev_close = float(prev_raw) if prev_raw is not None else None

        change_pct = None
        if last_price is not None and prev_close not in (None, 0):
            change_pct = (last_price - prev_close) * 100.0 / prev_close

        near_high_pct = (high_52 - last_price) * 100.0 / high_52

        items.append(
            {
                "token_id": tid,
                "symbol": s["symbol"],
                "series": s["series"],
                "company_name": s["company_name"],
                "last_price": last_price,
                "prev_close": prev_close,
                "change_pct": change_pct,
                "close_price": (float(t["close_price"]) if t.get("close_price") is not None else None),
                "total_traded_qty": None,
                "volume": None,
                "activity_metric": None,
                "high_52": high_52,
                "low_52": None,
                "near_high_pct": near_high_pct,
                "above_low_pct": None,
                "interval_start": t.get("interval_start"),
            }
        )

    # nearer to high => smaller near_high_pct
    items.sort(key=lambda x: (x["near_high_pct"] if x.get("near_high_pct") is not None else 1e18))

    items = items[:limit]
    sample_map = _attach_samples(db, items, latest_trade_date)

    return {
        "filter": "52W_HIGH",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "cutoff_start": cutoff_start.isoformat(),
        "cutoff_end": cutoff_end.isoformat(),
        "count": len(items),
        "data": _serialize_rows(items, sample_map),
    }


def _fetch_52w_low(db: Session, index_key: str, limit: int):
    index_name, token_ids = _get_index_name_and_token_ids(db, index_key)

    latest_trade_date = _get_latest_trade_date_for_tokens(db, token_ids)
    prev_trade_date = _get_prev_trade_date_for_tokens(db, token_ids, latest_trade_date)

    cutoff_end = latest_trade_date
    cutoff_start = cutoff_end - timedelta(days=365)

    latest_map = _latest_intraday_map(db, token_ids, latest_trade_date)
    if not latest_map:
        raise HTTPException(status_code=404, detail="No intraday rows for latest trade_date + tokens")

    prev_map = _prev_close_map(db, token_ids, prev_trade_date) if prev_trade_date else {}
    sec_map = _security_meta_map(db, list(latest_map.keys()))

    bhav_series = func.coalesce(NseCmBhavcopy.series, literal("EQ"))

    low_rows = (
        db.execute(
            select(
                NseCmSecurity.token_id.label("token_id"),
                func.min(NseCmBhavcopy.low_price).label("low_52"),
            )
            .select_from(NseCmBhavcopy)
            .join(
                NseCmSecurity,
                (NseCmSecurity.symbol == NseCmBhavcopy.symbol) & (NseCmSecurity.series == bhav_series),
            )
            .where(
                NseCmBhavcopy.trade_date >= cutoff_start,
                NseCmBhavcopy.trade_date <= cutoff_end,
                NseCmSecurity.token_id.in_(token_ids),
                NseCmSecurity.series == "EQ",
                NseCmBhavcopy.low_price.isnot(None),
            )
            .group_by(NseCmSecurity.token_id)
        )
        .mappings()
        .all()
    )

    low_map = {int(r["token_id"]): r["low_52"] for r in low_rows if r.get("token_id") is not None}

    items: List[Dict[str, Any]] = []
    for tid, t in latest_map.items():
        s = sec_map.get(tid)
        if not s:
            continue

        low_raw = low_map.get(tid)
        if low_raw is None:
            continue

        last_raw = t.get("last_price")
        last_price = float(last_raw) if last_raw is not None else None
        if last_price in (None, 0):
            continue

        low_52 = float(low_raw) if low_raw is not None else None
        if low_52 in (None, 0):
            continue

        prev_raw = prev_map.get(tid)
        prev_close = float(prev_raw) if prev_raw is not None else None

        change_pct = None
        if last_price is not None and prev_close not in (None, 0):
            change_pct = (last_price - prev_close) * 100.0 / prev_close

        above_low_pct = (last_price - low_52) * 100.0 / low_52

        items.append(
            {
                "token_id": tid,
                "symbol": s["symbol"],
                "series": s["series"],
                "company_name": s["company_name"],
                "last_price": last_price,
                "prev_close": prev_close,
                "change_pct": change_pct,
                "close_price": (float(t["close_price"]) if t.get("close_price") is not None else None),
                "total_traded_qty": None,
                "volume": None,
                "activity_metric": None,
                "high_52": None,
                "low_52": low_52,
                "near_high_pct": None,
                "above_low_pct": above_low_pct,
                "interval_start": t.get("interval_start"),
            }
        )

    # closer above low => smaller above_low_pct
    items.sort(key=lambda x: (x["above_low_pct"] if x.get("above_low_pct") is not None else 1e18))

    items = items[:limit]
    sample_map = _attach_samples(db, items, latest_trade_date)

    return {
        "filter": "52W_LOW",
        "index": index_name,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "cutoff_start": cutoff_start.isoformat(),
        "cutoff_end": cutoff_end.isoformat(),
        "count": len(items),
        "data": _serialize_rows(items, sample_map),
    }


# ===========================
#  MAIN ENDPOINT
# ===========================
@router.get("/")
async def todays_stock(
    index: str = Query("NIFTY100", description="Index filter: NIFTY50 / NIFTY100 / NIFTY500 / ALL"),
    filter_type: str = Query("GAINERS", description="Filter: GAINERS / LOSERS / MOST_ACTIVE / 52W_HIGH / 52W_LOW"),
    limit: int = Query(10, ge=1, le=500, description="Max number of rows to return"),
    db: Session = Depends(get_db),
):
    f = (filter_type or "").upper().strip()

    if f == "GAINERS":
        return _fetch_gainers(db, index, limit)
    if f == "LOSERS":
        return _fetch_losers(db, index, limit)
    if f == "MOST_ACTIVE":
        return _fetch_most_active(db, index, limit)
    if f == "52W_HIGH":
        return _fetch_52w_high(db, index, limit)
    if f == "52W_LOW":
        return _fetch_52w_low(db, index, limit)

    raise HTTPException(
        status_code=400,
        detail="Invalid filter_type. Use: GAINERS / LOSERS / MOST_ACTIVE / 52W_HIGH / 52W_LOW",
    )
