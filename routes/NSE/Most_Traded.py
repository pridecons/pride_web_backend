# routes/NSE/Most_Traded.py

import logging
from fastapi import APIRouter, Depends, HTTPException, Query
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
router = APIRouter(prefix="/most-traded", tags=["Most Traded"])

INDEX_MAP = {
    "NIFTY50": ("NIFTY 50", "NIFTY50"),
    "NIFTY100": ("NIFTY 100", "NIFTY100"),
    "NIFTY500": ("NIFTY 500", "NIFTY500"),
}


# -----------------------------
# Helpers
# -----------------------------
def _get_index_row(db: Session, index_code: str | None) -> NseIndexMaster | None:
    """
    Returns NseIndexMaster row for provided index_code.
    If index_code is None/ALL => return None (means all EQ universe).
    """
    if not index_code or (str(index_code).upper().strip() == "ALL"):
        return None

    idx = str(index_code).upper().strip()
    if idx not in INDEX_MAP:
        raise HTTPException(status_code=400, detail=f"Invalid index '{index_code}'")

    name, short_code = INDEX_MAP[idx]

    row = (
        db.query(NseIndexMaster)
        .filter((NseIndexMaster.index_symbol == name) | (NseIndexMaster.short_code == short_code))
        .one_or_none()
    )

    if row is None:
        row = (
            db.query(NseIndexMaster)
            .filter(func.lower(NseIndexMaster.index_symbol) == name.lower())
            .one_or_none()
        )

    if row is None:
        row = (
            db.query(NseIndexMaster)
            .filter(func.lower(NseIndexMaster.short_code) == short_code.lower())
            .one_or_none()
        )

    if not row:
        raise HTTPException(status_code=404, detail=f"Index '{idx}' not found")

    return row


def _get_token_ids_for_index(db: Session, index_row: NseIndexMaster | None) -> list[int]:
    """
    Returns token_ids for index_row (EQ only).
    If index_row is None => ALL EQ universe tokens from security master.
    """
    if index_row is None:
        token_ids = (
            db.execute(
                select(NseCmSecurity.token_id)
                .where(NseCmSecurity.series == "EQ", NseCmSecurity.token_id.isnot(None))
                .distinct()
            )
            .scalars()
            .all()
        )
        return [int(x) for x in token_ids if x is not None]

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
    return [int(x) for x in token_ids if x is not None]


def _get_latest_trade_date(db: Session, token_ids: list[int]) -> object:
    """
    Latest trade_date restricted to token universe (fast + accurate).
    """
    if not token_ids:
        raise HTTPException(status_code=404, detail="No tokens found for given universe")

    latest_td = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(NseCmIntraday1Min.token_id.in_(token_ids))
    ).scalar()

    if latest_td is None:
        raise HTTPException(status_code=404, detail="No intraday data found for tokens")
    return latest_td


def _get_prev_trade_date(db: Session, token_ids: list[int], latest_trade_date) -> object | None:
    if not token_ids or latest_trade_date is None:
        return None

    prev_td = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(
            NseCmIntraday1Min.token_id.in_(token_ids),
            NseCmIntraday1Min.trade_date < latest_trade_date,
        )
    ).scalar()

    return prev_td


# -----------------------------
# Core logic (FAST)
# -----------------------------
def _compute_most_traded(db: Session, index_code: str = "ALL", limit: int = 50) -> dict:
    index_row = _get_index_row(db, index_code)
    token_ids = _get_token_ids_for_index(db, index_row)

    # Latest/Prev trade dates restricted to token universe
    latest_trade_date = _get_latest_trade_date(db, token_ids)
    prev_trade_date = _get_prev_trade_date(db, token_ids, latest_trade_date)

    # Latest row per token on latest_trade_date (DISTINCT ON)
    latest_sql = text("""
        SELECT DISTINCT ON (token_id)
          token_id,
          interval_start,
          COALESCE(last_price, close_price) AS last_price,
          total_traded_qty
        FROM nse_cm_intraday_1min
        WHERE trade_date = :td
          AND token_id = ANY(:token_ids)
        ORDER BY token_id, interval_start DESC
    """)
    latest_rows = db.execute(
        latest_sql,
        {"td": latest_trade_date, "token_ids": token_ids},
    ).mappings().all()

    if not latest_rows:
        raise HTTPException(status_code=404, detail="No latest intraday rows found")

    latest_map = {int(r["token_id"]): r for r in latest_rows}

    # Prev close per token (if prev_trade_date exists)
    prev_map = {}
    if prev_trade_date:
        prev_sql = text("""
            SELECT DISTINCT ON (token_id)
              token_id,
              COALESCE(close_price, last_price) AS prev_close
            FROM nse_cm_intraday_1min
            WHERE trade_date = :td
              AND token_id = ANY(:token_ids)
            ORDER BY token_id, interval_start DESC
        """)
        prev_rows = db.execute(
            prev_sql,
            {"td": prev_trade_date, "token_ids": token_ids},
        ).mappings().all()
        prev_map = {int(r["token_id"]): r["prev_close"] for r in prev_rows}

    # Join security meta (EQ only)
    sec_rows = (
        db.execute(
            select(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.company_name,
                NseCmSecurity.series,
            ).where(
                NseCmSecurity.token_id.in_(list(latest_map.keys())),
                NseCmSecurity.series == "EQ",
            )
        )
        .mappings()
        .all()
    )
    sec_map = {int(r["token_id"]): r for r in sec_rows}

    # Build items
    items = []
    for tid, t in latest_map.items():
        s = sec_map.get(tid)
        if not s:
            continue

        qty = t.get("total_traded_qty")
        if qty is None or int(qty) <= 0:
            continue

        last_price = float(t["last_price"]) if t.get("last_price") is not None else None
        prev_close_raw = prev_map.get(tid)
        prev_close = float(prev_close_raw) if prev_close_raw is not None else None

        change_pct = None
        if last_price is not None and prev_close not in (None, 0):
            change_pct = (last_price - prev_close) * 100.0 / prev_close

        items.append(
            {
                "token_id": tid,
                "symbol": s["symbol"],
                "company_name": s["company_name"],
                "last_price": last_price,
                "prev_close": prev_close,
                "change_pct": change_pct,
                "total_traded_qty": int(qty) if qty is not None else None,
                "interval_start": t["interval_start"],
            }
        )

    # Sort + limit
    items.sort(key=lambda x: x["total_traded_qty"] or 0, reverse=True)
    items = items[:limit]

    return {
        "index": (index_code or "ALL").upper(),
        "index_id": index_row.id if index_row else None,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "count": len(items),
        "data": [
            {
                "token_id": r["token_id"],
                "symbol": r["symbol"],
                "company_name": r["company_name"],
                "last_price": r["last_price"],
                "prev_close": r["prev_close"],
                "change_pct": round(r["change_pct"], 4) if r["change_pct"] is not None else None,
                "total_traded_qty": r["total_traded_qty"],
                "interval_start": r["interval_start"].isoformat() if r["interval_start"] else None,
            }
            for r in items
        ],
    }


# -----------------------------
# Endpoint
# -----------------------------
@router.get("/")
def most_traded_companies(
    index: str = Query("ALL", description="Index filter: ALL / NIFTY50 / NIFTY100 / NIFTY500"),
    limit: int = Query(10, ge=1, le=500),
    db: Session = Depends(get_db),
):
    return _compute_most_traded(db=db, index_code=index, limit=limit)
