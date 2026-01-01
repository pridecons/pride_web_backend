# routes/NSE/Market_And_Sectors.py

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from db.connection import get_db
from db.models import NseCmIndex1Min

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/market-and-sectors",
    tags=["Market & Sectors"],
)

# -----------------------------
# Master index list (single source of truth)
# -----------------------------

INDEXES = [
  { "index_id": 0, "name": "NIFTY 50" },
  { "index_id": 1, "name": "NIFTY IT" },
  { "index_id": 2, "name": "NIFTY NEXT 50" },
  { "index_id": 3, "name": "NIFTY50 USD (NOT IN USE)" },
  { "index_id": 4, "name": "NIFTY BANK" },
  { "index_id": 5, "name": "NIFTY MIDCAP 100" },
  { "index_id": 6, "name": "NIFTY 500" },
  { "index_id": 7, "name": "NIFTY 100" },
  { "index_id": 8, "name": "NIFTY MIDCAP 50" },
  { "index_id": 9, "name": "NIFTY REALTY" },
  { "index_id": 10, "name": "NIFTY INFRA" },
  { "index_id": 11, "name": "INDIA VIX" },
  { "index_id": 12, "name": "NIFTY ENERGY" },
  { "index_id": 13, "name": "NIFTY FMCG" },
  { "index_id": 14, "name": "NIFTY MNC" },
  { "index_id": 15, "name": "NIFTY PHARMA" },
  { "index_id": 16, "name": "NIFTY PSE" },
  { "index_id": 17, "name": "NIFTY PSU BANK" },
  { "index_id": 18, "name": "NIFTY SERV SECTOR" },
  { "index_id": 19, "name": "NIFTY SMLCAP 100" },
  { "index_id": 20, "name": "NIFTY 200" },
  { "index_id": 21, "name": "NIFTY AUTO" },
  { "index_id": 22, "name": "NIFTY MEDIA" },
  { "index_id": 23, "name": "NIFTY METAL" },
  { "index_id": 24, "name": "NIFTY DIV OPPS 50" },
  { "index_id": 25, "name": "NIFTY COMMODITIES" },
  { "index_id": 26, "name": "NIFTY CONSUMPTION" },
  { "index_id": 27, "name": "NIFTY FIN SERVICE" },
  { "index_id": 28, "name": "NIFTY50 DIV POINT" },
  { "index_id": 29, "name": "NIFTY100 LIQ 15" },
  { "index_id": 30, "name": "NIFTY CPSE" },
  { "index_id": 31, "name": "NIFTY GROWSECT 15" },
  { "index_id": 32, "name": "NIFTY50 TR 2X LEV" },
  { "index_id": 33, "name": "NIFTY50 PR 2X LEV" },
  { "index_id": 34, "name": "NIFTY50 TR 1X INV" },
  { "index_id": 35, "name": "NIFTY50 PR 1X INV" },
  { "index_id": 36, "name": "NIFTY50 VALUE 20" },
  { "index_id": 37, "name": "NIFTY100 QUALTY30" },
  { "index_id": 38, "name": "NIFTY MID LIQ 15" },
  { "index_id": 39, "name": "NIFTY PVT BANK" },
  { "index_id": 40, "name": "NIFTY GS 8 13YR" },
  { "index_id": 41, "name": "NIFTY GS 10YR" },
  { "index_id": 42, "name": "NIFTY GS 10YR CLN" },
  { "index_id": 43, "name": "NIFTY GS 4 8YR" },
  { "index_id": 44, "name": "NIFTY GS 11 15YR" },
  { "index_id": 45, "name": "NIFTY GS 15YRPLUS" },
  { "index_id": 46, "name": "NIFTY GS COMPSITE" },
  { "index_id": 47, "name": "NIFTY50 EQL WGT" },
  { "index_id": 48, "name": "NIFTY100 EQL WGT" },
  { "index_id": 49, "name": "NIFTY100 LOWVOL30" },
  { "index_id": 50, "name": "NIFTY ALPHA 50" },
  { "index_id": 51, "name": "NIFTY MIDCAP 150" },
  { "index_id": 52, "name": "NIFTY SMALLCAP 50" },
  { "index_id": 53, "name": "NIFTY SMALLCAP 250" },
  { "index_id": 54, "name": "NIFTY MIDSMALLCAP 400" },
  { "index_id": 55, "name": "NIFTY200 QUALITY 30" },
  { "index_id": 56, "name": "NIFTY FINSRV25 50" },
  { "index_id": 57, "name": "NIFTY ALPHALOWVOL" },
  { "index_id": 58, "name": "NIFTY200MOMENTM30" },
  { "index_id": 59, "name": "NIFTY100ESGSECLDR" },
  { "index_id": 60, "name": "NIFTY HEALTHCARE" },
  { "index_id": 61, "name": "NIFTY CONSR DURBL" },
  { "index_id": 62, "name": "NIFTY OIL AND GAS" },
  { "index_id": 63, "name": "NIFTY500 MULTICAP" },
  { "index_id": 64, "name": "NIFTY LARGEMID250" },
  { "index_id": 65, "name": "NIFTY MID SELECT" },
  { "index_id": 66, "name": "NIFTY TOTAL MKT" },
  { "index_id": 67, "name": "NIFTY MICROCAP250" },
  { "index_id": 68, "name": "NIFTY IND DIGITAL" },
  { "index_id": 69, "name": "NIFTY100 ESG" },
  { "index_id": 70, "name": "NIFTY M150 QLTY50" },
  { "index_id": 71, "name": "NIFTY INDIA MFG" },
  { "index_id": 74, "name": "NIFTY200 ALPHA 30" },
  { "index_id": 75, "name": "NIFTYM150MOMNTM50" },
  { "index_id": 76, "name": "NIFTY TATA 25 CAP" },
  { "index_id": 77, "name": "NIFTY MIDSML HLTH" },
  { "index_id": 78, "name": "NIFTY MULTI MFG" },
  { "index_id": 79, "name": "NIFTY MULTI INFRA" },
  { "index_id": 80, "name": "BHARATBOND-APR25 (NOT IN USE)" },
  { "index_id": 81, "name": "BHARATBOND-APR30" },
  { "index_id": 82, "name": "BHARATBOND-APR31" },
  { "index_id": 83, "name": "BHARATBOND-APR32" },
  { "index_id": 84, "name": "BHARATBOND-APR33" },
  { "index_id": 85, "name": "Nifty Ind Defence" },
  { "index_id": 86, "name": "Nifty Ind Tourism" },
  { "index_id": 87, "name": "Nifty Capital Mkt" },
  { "index_id": 88, "name": "Nifty500Momentm50" },
  { "index_id": 89, "name": "NiftyMS400 MQ 100" },
  { "index_id": 90, "name": "NiftySml250MQ 100" },
  { "index_id": 91, "name": "Nifty Top 10 EW" },
  { "index_id": 92, "name": "NIFTY AQL 30" },
  { "index_id": 93, "name": "NIFTY AQLV 30" },
  { "index_id": 94, "name": "NIFTY EV" },
  { "index_id": 95, "name": "NIFTY HIGHBETA 50" },
  { "index_id": 96, "name": "NIFTY NEW CONSUMP" },
  { "index_id": 97, "name": "NIFTY CORP MAATR" },
  { "index_id": 98, "name": "NIFTY LOW VOL 50" },
  { "index_id": 99, "name": "NIFTY MOBILITY" },
  { "index_id": 100, "name": "NIFTY QLTY LV 30" },
  { "index_id": 101, "name": "NIFTY SML250 Q50" },
  { "index_id": 102, "name": "NIFTY TOP 15 EW" },
  { "index_id": 103, "name": "NIFTY100 ALPHA 30" },
  { "index_id": 104, "name": "NIFTY100 ENH ESG" },
  { "index_id": 105, "name": "NIFTY200 VALUE 30" },
  { "index_id": 106, "name": "NIFTY500 EW" },
  { "index_id": 107, "name": "NIFTY MULTI MQ 50" },
  { "index_id": 108, "name": "NIFTY500 VALUE 50" },
  { "index_id": 109, "name": "NIFTY TOP 20 EW" },
  { "index_id": 110, "name": "NIFTY COREHOUSING" },
  { "index_id": 111, "name": "NIFTY FINSEREXBNK" },
  { "index_id": 112, "name": "NIFTY HOUSING" },
  { "index_id": 113, "name": "NIFTY IPO" },
  { "index_id": 114, "name": "NIFTY MS FIN SERV" },
  { "index_id": 115, "name": "NIFTY MS IND CONS" },
  { "index_id": 116, "name": "NIFTY MS IT TELCM" },
  { "index_id": 117, "name": "NIFTY NONCYC CONS" },
  { "index_id": 118, "name": "NIFTY RURAL" },
  { "index_id": 119, "name": "NIFTY SHARIAH 25" },
  { "index_id": 120, "name": "NIFTY TRANS LOGIS" },
  { "index_id": 121, "name": "NIFTY50 SHARIAH" },
  { "index_id": 122, "name": "NIFTY500 LMS EQL" },
  { "index_id": 123, "name": "NIFTY500 SHARIAH" },
  { "index_id": 124, "name": "NIFTY500 QLTY50" },
  { "index_id": 125, "name": "NIFTY500 LOWVOL50" },
  { "index_id": 126, "name": "NIFTY500 MQVLV50" }
]

INDEX_ID_TO_NAME = {x["index_id"]: x["name"] for x in INDEXES}
NAME_TO_INDEX_ID = {x["name"].strip().upper(): x["index_id"] for x in INDEXES}

# UI codes -> index_id
INDEX_MAP = {
    "NIFTY50": 0,
    "NIFTY100": 7,
    "NIFTY500": 6,
    "NIFTYBANK": 4,
    "IT": 1,
    "NEXT50": 2,
}

# -----------------------------
# Helpers
# -----------------------------
def _resolve_index_ids(codes: List[str]) -> List[int]:
    out: List[int] = []
    for code in codes:
        c = code.strip().upper()
        if not c:
            continue
        if c in INDEX_MAP:
            out.append(INDEX_MAP[c])
            continue
        # allow passing numeric index_id as well
        if c.isdigit():
            out.append(int(c))
            continue
        raise HTTPException(status_code=400, detail=f"Unknown index code: {code}")
    # remove duplicates while preserving order
    seen = set()
    uniq = []
    for i in out:
        if i not in seen:
            uniq.append(i)
            seen.add(i)
    return uniq

def _index_name(index_id: int) -> str:
    return INDEX_ID_TO_NAME.get(index_id, f"INDEX_{index_id}")

def _latest_row(db: Session, index_id: int):
    return (
        db.query(NseCmIndex1Min)
        .filter(NseCmIndex1Min.index_id == index_id)
        .order_by(NseCmIndex1Min.interval_start.desc())
        .first()
    )

def _compute_snapshot(db: Session, index_id: int) -> Dict[str, Any]:
    row = _latest_row(db, index_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"No data for index_id={index_id}")

    # Always return name from mapping (ignore DB's broken index_name="0")
    return {
        "index_id": index_id,
        "name": _index_name(index_id),
        "interval_start": row.interval_start,
        "trade_date": row.trade_date,
        "open": float(row.open_price) if row.open_price is not None else None,
        "high": float(row.high_price) if row.high_price is not None else None,
        "low": float(row.low_price) if row.low_price is not None else None,
        "close": float(row.close_price) if row.close_price is not None else None,
        "last": float(row.last_price) if row.last_price is not None else None,
        "pct_change": float(row.percentage_change) if row.percentage_change is not None else None,
        "volume": int(row.volume) if row.volume is not None else None,
        "turnover": float(row.turnover) if row.turnover is not None else None,
    }

# -----------------------------
# Endpoints
# -----------------------------
@router.get("/")
def market_and_sectors(
    indices: str = Query(
        "NIFTY50,NIFTY100,NIFTY500",
        description="Comma separated list of indices codes (NIFTY50,NIFTY100,NIFTY500) or numeric index_id",
    ),
    db: Session = Depends(get_db),
):
    codes = [c.strip() for c in indices.split(",") if c.strip()]
    if not codes:
        raise HTTPException(status_code=400, detail="No indices supplied")

    index_ids = _resolve_index_ids(codes)

    snapshots: List[Dict[str, Any]] = []
    for idx in index_ids:
        try:
            snapshots.append(_compute_snapshot(db, idx))
        except HTTPException as exc:
            logger.warning(f"Skipping index_id={idx}: {exc.detail}")
            continue
        except Exception:
            logger.exception(f"Failed computing snapshot for index_id={idx}")
            continue

    return {"count": len(snapshots), "indices": snapshots}


@router.get("/historical")
def market_and_sectors_historical(
    index: str = Query(
        "NIFTY50",
        description="Single index code (e.g. NIFTY50) or numeric index_id",
    ),
    days: int = Query(7, ge=1, le=90, description="Number of days (default 7 = 1 week)"),
    limit: int = Query(2000, ge=10, le=20000, description="Max rows to return"),
    db: Session = Depends(get_db),
):
    # resolve index id
    index_id = _resolve_index_ids([index])[0]

    # time range (use DB timezone-aware timestamps)
    # For safety, use now() in UTC and filter interval_start >= now - days
    now_utc = datetime.now(timezone.utc)
    start_ts = now_utc - timedelta(days=days)

    q = (
        db.query(
            NseCmIndex1Min.interval_start,
            NseCmIndex1Min.trade_date,
            NseCmIndex1Min.open_price,
            NseCmIndex1Min.high_price,
            NseCmIndex1Min.low_price,
            NseCmIndex1Min.close_price,
            NseCmIndex1Min.last_price,
            NseCmIndex1Min.percentage_change,
            NseCmIndex1Min.volume,
            NseCmIndex1Min.turnover,
        )
        .filter(NseCmIndex1Min.index_id == index_id)
        .filter(NseCmIndex1Min.interval_start >= start_ts)
        .order_by(NseCmIndex1Min.interval_start.asc())
        .limit(limit)
    )

    rows = q.all()

    data = []
    for r in rows:
        data.append(
            {
                "interval_start": r.interval_start,
                "trade_date": r.trade_date,
                "open": float(r.open_price) if r.open_price is not None else None,
                "high": float(r.high_price) if r.high_price is not None else None,
                "low": float(r.low_price) if r.low_price is not None else None,
                "close": float(r.close_price) if r.close_price is not None else None,
                "last": float(r.last_price) if r.last_price is not None else None,
                "pct_change": float(r.percentage_change) if r.percentage_change is not None else None,
                "volume": int(r.volume) if r.volume is not None else None,
                "turnover": float(r.turnover) if r.turnover is not None else None,
            }
        )

    return {
        "index_id": index_id,
        "name": _index_name(index_id),
        "days": days,
        "count": len(data),
        "data": data,
    }
