# routes/NSE/Most_Traded.py
# ✅ Optimized DB-friendly version (fixes: heavy IN-subquery reuse, upper/lower index-killers,
# ✅ avoids GROUP BY + join-back pattern by using window functions, and reduces scans)

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
router = APIRouter(prefix="/most-traded", tags=["Most Traded"])

INDEX_MAP = {
    "NIFTY50": ("NIFTY 50", "NIFTY50"),
    "NIFTY100": ("NIFTY 100", "NIFTY100"),
    "NIFTY500": ("NIFTY 500", "NIFTY500"),
    # "ALL" means no index filter
}


# -----------------------------
# Helpers
# -----------------------------
def _get_index_row(db: Session, index_code: str | None) -> NseIndexMaster | None:
    """
    index_code: ALL / NIFTY50 / NIFTY100 / NIFTY500 / None
    """
    if not index_code or index_code.upper() == "ALL":
        return None

    idx = index_code.upper().strip()
    if idx not in INDEX_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid index code '{index_code}'. Use ALL / NIFTY50 / NIFTY100 / NIFTY500.",
        )

    name, short_code = INDEX_MAP[idx]

    # Prefer exact match first (index-friendly)
    row = (
        db.query(NseIndexMaster)
        .filter(
            (NseIndexMaster.index_symbol == name)
            | (NseIndexMaster.short_code == short_code)
        )
        .one_or_none()
    )

    # Fallback case-insensitive
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

    if row is None:
        raise HTTPException(status_code=404, detail=f"Index '{idx}' not found in NseIndexMaster")

    return row


def _compute_most_traded(db: Session, index_code: str = "ALL", limit: int = 50) -> dict:
    """
    MOST TRADED COMPANIES (volume-wise)
    - Picks latest candle per token for latest_trade_date (window fn)
    - Picks last candle per token for prev_trade_date (window fn)
    - Calculates prev_close, change_pct
    - Sorts by activity_metric desc
    """

    index_row = _get_index_row(db, index_code)

    # 1) Token universe as a CTE (reused)
    if index_row is not None:
        tokens_cte = (
            select(NseCmSecurity.token_id)
            .select_from(NseCmSecurity)
            .join(NseIndexConstituent, NseIndexConstituent.symbol == NseCmSecurity.symbol)
            .where(
                NseIndexConstituent.index_id == index_row.id,
                NseCmSecurity.series == "EQ",  # ✅ avoid upper(series)
            )
            .distinct()
            .cte("tokens")
        )
        token_filter = NseCmIntraday1Min.token_id.in_(select(tokens_cte.c.token_id))
    else:
        tokens_cte = None
        token_filter = literal(True)

    # 2) Latest trade_date (restricted if index provided)
    latest_trade_date = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(token_filter)
    ).scalar()

    if latest_trade_date is None:
        raise HTTPException(status_code=404, detail="No intraday data found.")

    # 3) Previous trade_date
    prev_trade_date = db.execute(
        select(func.max(NseCmIntraday1Min.trade_date)).where(
            token_filter, NseCmIntraday1Min.trade_date < latest_trade_date
        )
    ).scalar()

    # 4) Latest candle per token for latest_trade_date (window fn)
    latest_ranked = (
        select(
            NseCmIntraday1Min.token_id.label("token_id"),
            NseCmIntraday1Min.interval_start.label("interval_start"),
            NseCmIntraday1Min.last_price.label("last_price"),
            NseCmIntraday1Min.total_traded_qty.label("total_traded_qty"),
            NseCmIntraday1Min.volume.label("volume"),
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
        .cte("latest_ranked")
    )

    latest_one = (
        select(
            latest_ranked.c.token_id,
            latest_ranked.c.interval_start,
            latest_ranked.c.last_price,
            latest_ranked.c.total_traded_qty,
            latest_ranked.c.volume,
        )
        .where(latest_ranked.c.rn == 1)
        .cte("latest_one")
    )

    # volume metric
    activity_metric = func.coalesce(latest_one.c.total_traded_qty, latest_one.c.volume)

    # 5) Prev day last candle per token (if available)
    if prev_trade_date is not None:
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
    else:
        prev_one = None

    # 6) Final query: join security metadata + optional prev close
    if prev_one is not None:
        prev_close = prev_one.c.prev_close
        change_pct = (
            (latest_one.c.last_price - prev_close)
            * 100.0
            / func.nullif(prev_close, 0.0)
        )

        stmt = (
            select(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.company_name,
                latest_one.c.last_price.label("last_price"),
                prev_close.label("prev_close"),
                change_pct.label("change_pct"),
                latest_one.c.total_traded_qty.label("total_traded_qty"),
                latest_one.c.volume.label("volume"),
                activity_metric.label("activity_metric"),
                latest_one.c.interval_start.label("interval_start"),
            )
            .select_from(latest_one)
            .join(NseCmSecurity, NseCmSecurity.token_id == latest_one.c.token_id)
            .outerjoin(prev_one, prev_one.c.token_id == latest_one.c.token_id)
            .where(
                NseCmSecurity.series == "EQ",  # ✅ avoid upper(series)
                latest_one.c.last_price.isnot(None),
                activity_metric.isnot(None),
            )
            .order_by(activity_metric.desc())
            .limit(limit)
        )
    else:
        stmt = (
            select(
                NseCmSecurity.token_id,
                NseCmSecurity.symbol,
                NseCmSecurity.company_name,
                latest_one.c.last_price.label("last_price"),
                literal(None).label("prev_close"),
                literal(None).label("change_pct"),
                latest_one.c.total_traded_qty.label("total_traded_qty"),
                latest_one.c.volume.label("volume"),
                activity_metric.label("activity_metric"),
                latest_one.c.interval_start.label("interval_start"),
            )
            .select_from(latest_one)
            .join(NseCmSecurity, NseCmSecurity.token_id == latest_one.c.token_id)
            .where(
                NseCmSecurity.series == "EQ",
                latest_one.c.last_price.isnot(None),
                activity_metric.isnot(None),
            )
            .order_by(activity_metric.desc())
            .limit(limit)
        )

    rows = db.execute(stmt).mappings().all()

    db.close()

    return {
        "index": index_code.upper() if index_code else "ALL",
        "index_id": index_row.id if index_row is not None else None,
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "prev_trade_date": prev_trade_date.isoformat() if prev_trade_date else None,
        "count": len(rows),
        "data": [
            {
                "token_id": r["token_id"],
                "symbol": r["symbol"],
                "company_name": r["company_name"],
                "last_price": float(r["last_price"]) if r["last_price"] is not None else None,
                "prev_close": float(r["prev_close"]) if r["prev_close"] is not None else None,
                "change_pct": float(r["change_pct"]) if r["change_pct"] is not None else None,
                "total_traded_qty": int(r["total_traded_qty"]) if r["total_traded_qty"] is not None else None,
                "volume": int(r["volume"]) if r["volume"] is not None else None,
                "activity_metric": int(r["activity_metric"]) if r["activity_metric"] is not None else None,
                "interval_start": r["interval_start"].isoformat() if r["interval_start"] else None,
            }
            for r in rows
        ],
    }


# -----------------------------
# FastAPI endpoint
# -----------------------------
@router.get("/")
def most_traded_companies(
    index: str = Query("ALL", description="Index filter: ALL / NIFTY50 / NIFTY100 / NIFTY500"),
    limit: int = Query(10, ge=1, le=500, description="Max number of rows to return"),
    db: Session = Depends(get_db),
):
    return _compute_most_traded(db=db, index_code=index, limit=limit)
