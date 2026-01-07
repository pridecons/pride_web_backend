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
  { "index_id": 0, "name": "NIFTY 50", "symbol": "NIFTY50" },
  { "index_id": 1, "name": "NIFTY IT", "symbol": "NIFTYIT" },
  { "index_id": 2, "name": "NIFTY NEXT 50", "symbol": "NIFTYNEXT50" },
  { "index_id": 3, "name": "NIFTY50 USD (NOT IN USE)", "symbol": "NIFTY50USD" },
  { "index_id": 4, "name": "NIFTY BANK", "symbol": "NIFTYBANK" },
  { "index_id": 5, "name": "NIFTY MIDCAP 100", "symbol": "NIFTYMIDCAP100" },
  { "index_id": 6, "name": "NIFTY 500", "symbol": "NIFTY500" },
  { "index_id": 7, "name": "NIFTY 100", "symbol": "NIFTY100" },
  { "index_id": 8, "name": "NIFTY MIDCAP 50", "symbol": "NIFTYMIDCAP50" },
  { "index_id": 9, "name": "NIFTY REALTY", "symbol": "NIFTYREALTY" },
  { "index_id": 10, "name": "NIFTY INFRA", "symbol": "NIFTYINFRA" },
  { "index_id": 11, "name": "INDIA VIX", "symbol": "INDIAVIX" },
  { "index_id": 12, "name": "NIFTY ENERGY", "symbol": "NIFTYENERGY" },
  { "index_id": 13, "name": "NIFTY FMCG", "symbol": "NIFTYFMCG" },
  { "index_id": 14, "name": "NIFTY MNC", "symbol": "NIFTYMNC" },
  { "index_id": 15, "name": "NIFTY PHARMA", "symbol": "NIFTYPHARMA" },
  { "index_id": 16, "name": "NIFTY PSE", "symbol": "NIFTYPSE" },
  { "index_id": 17, "name": "NIFTY PSU BANK", "symbol": "NIFTYPSUBANK" },
  { "index_id": 18, "name": "NIFTY SERV SECTOR", "symbol": "NIFTYSERVSECTOR" },
  { "index_id": 19, "name": "NIFTY SMLCAP 100", "symbol": "NIFTYSMLCAP100" },
  { "index_id": 20, "name": "NIFTY 200", "symbol": "NIFTY200" },
  { "index_id": 21, "name": "NIFTY AUTO", "symbol": "NIFTYAUTO" },
  { "index_id": 22, "name": "NIFTY MEDIA", "symbol": "NIFTYMEDIA" },
  { "index_id": 23, "name": "NIFTY METAL", "symbol": "NIFTYMETAL" },
  { "index_id": 24, "name": "NIFTY DIV OPPS 50", "symbol": "NIFTYDIVOPPS50" },
  { "index_id": 25, "name": "NIFTY COMMODITIES", "symbol": "NIFTYCOMMODITIES" },
  { "index_id": 26, "name": "NIFTY CONSUMPTION", "symbol": "NIFTYCONSUMPTION" },
  { "index_id": 27, "name": "NIFTY FIN SERVICE", "symbol": "NIFTYFINSERVICE" },
  { "index_id": 28, "name": "NIFTY50 DIV POINT", "symbol": "NIFTY50DIVPOINT" },
  { "index_id": 29, "name": "NIFTY100 LIQ 15", "symbol": "NIFTY100LIQ15" },
  { "index_id": 30, "name": "NIFTY CPSE", "symbol": "NIFTYCPSE" },
  { "index_id": 31, "name": "NIFTY GROWSECT 15", "symbol": "NIFTYGROWSECT15" },
  { "index_id": 32, "name": "NIFTY50 TR 2X LEV", "symbol": "NIFTY50TR2XLEV" },
  { "index_id": 33, "name": "NIFTY50 PR 2X LEV", "symbol": "NIFTY50PR2XLEV" },
  { "index_id": 34, "name": "NIFTY50 TR 1X INV", "symbol": "NIFTY50TR1XINV" },
  { "index_id": 35, "name": "NIFTY50 PR 1X INV", "symbol": "NIFTY50PR1XINV" },
  { "index_id": 36, "name": "NIFTY50 VALUE 20", "symbol": "NIFTY50VALUE20" },
  { "index_id": 37, "name": "NIFTY100 QUALTY30", "symbol": "NIFTY100QUALTY30" },
  { "index_id": 38, "name": "NIFTY MID LIQ 15", "symbol": "NIFTYMIDLIQ15" },
  { "index_id": 39, "name": "NIFTY PVT BANK", "symbol": "NIFTYPVTBANK" },
  { "index_id": 40, "name": "NIFTY GS 8 13YR", "symbol": "NIFTYGS813YR" },
  { "index_id": 41, "name": "NIFTY GS 10YR", "symbol": "NIFTYGS10YR" },
  { "index_id": 42, "name": "NIFTY GS 10YR CLN", "symbol": "NIFTYGS10YRCLN" },
  { "index_id": 43, "name": "NIFTY GS 4 8YR", "symbol": "NIFTYGS48YR" },
  { "index_id": 44, "name": "NIFTY GS 11 15YR", "symbol": "NIFTYGS1115YR" },
  { "index_id": 45, "name": "NIFTY GS 15YRPLUS", "symbol": "NIFTYGS15YRPLUS" },
  { "index_id": 46, "name": "NIFTY GS COMPSITE", "symbol": "NIFTYGSCOMPOSITE" },
  { "index_id": 47, "name": "NIFTY50 EQL WGT", "symbol": "NIFTY50EQLWGT" },
  { "index_id": 48, "name": "NIFTY100 EQL WGT", "symbol": "NIFTY100EQLWGT" },
  { "index_id": 49, "name": "NIFTY100 LOWVOL30", "symbol": "NIFTY100LOWVOL30" },
  { "index_id": 50, "name": "NIFTY ALPHA 50", "symbol": "NIFTYALPHA50" },
  { "index_id": 51, "name": "NIFTY MIDCAP 150", "symbol": "NIFTYMIDCAP150" },
  { "index_id": 52, "name": "NIFTY SMALLCAP 50", "symbol": "NIFTYSMALLCAP50" },
  { "index_id": 53, "name": "NIFTY SMALLCAP 250", "symbol": "NIFTYSMALLCAP250" },
  { "index_id": 54, "name": "NIFTY MIDSMALLCAP 400", "symbol": "NIFTYMIDSMALLCAP400" },
  { "index_id": 55, "name": "NIFTY200 QUALITY 30", "symbol": "NIFTY200QUALITY30" },
  { "index_id": 56, "name": "NIFTY FINSRV25 50", "symbol": "NIFTYFINSRV2550" },
  { "index_id": 57, "name": "NIFTY ALPHALOWVOL", "symbol": "NIFTYALPHALOWVOL" },
  { "index_id": 58, "name": "NIFTY200MOMENTM30", "symbol": "NIFTY200MOMENTM30" },
  { "index_id": 59, "name": "NIFTY100ESGSECLDR", "symbol": "NIFTY100ESGSECLDR" },
  { "index_id": 60, "name": "NIFTY HEALTHCARE", "symbol": "NIFTYHEALTHCARE" },
  { "index_id": 61, "name": "NIFTY CONSR DURBL", "symbol": "NIFTYCONSRDURBL" },
  { "index_id": 62, "name": "NIFTY OIL AND GAS", "symbol": "NIFTYOILANDGAS" },
  { "index_id": 63, "name": "NIFTY500 MULTICAP", "symbol": "NIFTY500MULTICAP" },
  { "index_id": 64, "name": "NIFTY LARGEMID250", "symbol": "NIFTYLARGEMID250" },
  { "index_id": 65, "name": "NIFTY MID SELECT", "symbol": "NIFTYMIDSELECT" },
  { "index_id": 66, "name": "NIFTY TOTAL MKT", "symbol": "NIFTYTOTALMKT" },
  { "index_id": 67, "name": "NIFTY MICROCAP250", "symbol": "NIFTYMICROCAP250" },
  { "index_id": 68, "name": "NIFTY IND DIGITAL", "symbol": "NIFTYINDDIGITAL" },
  { "index_id": 69, "name": "NIFTY100 ESG", "symbol": "NIFTY100ESG" },
  { "index_id": 70, "name": "NIFTY M150 QLTY50", "symbol": "NIFTYM150QLTY50" },
  { "index_id": 71, "name": "NIFTY INDIA MFG", "symbol": "NIFTYINDIAMFG" },
  { "index_id": 74, "name": "NIFTY200 ALPHA 30", "symbol": "NIFTY200ALPHA30" },
  { "index_id": 75, "name": "NIFTYM150MOMNTM50", "symbol": "NIFTYM150MOMNTM50" },
  { "index_id": 76, "name": "NIFTY TATA 25 CAP", "symbol": "NIFTYTATA25CAP" },
  { "index_id": 77, "name": "NIFTY MIDSML HLTH", "symbol": "NIFTYMIDSMLHLTH" },
  { "index_id": 78, "name": "NIFTY MULTI MFG", "symbol": "NIFTYMULTIMFG" },
  { "index_id": 79, "name": "NIFTY MULTI INFRA", "symbol": "NIFTYMULTIINFRA" },
  { "index_id": 80, "name": "BHARATBOND-APR25 (NOT IN USE)", "symbol": "BHARATBONDAPR25" },
  { "index_id": 81, "name": "BHARATBOND-APR30", "symbol": "BHARATBONDAPR30" },
  { "index_id": 82, "name": "BHARATBOND-APR31", "symbol": "BHARATBONDAPR31" },
  { "index_id": 83, "name": "BHARATBOND-APR32", "symbol": "BHARATBONDAPR32" },
  { "index_id": 84, "name": "BHARATBOND-APR33", "symbol": "BHARATBONDAPR33" },
  { "index_id": 85, "name": "Nifty Ind Defence", "symbol": "NIFTYINDDEFENCE" },
  { "index_id": 86, "name": "Nifty Ind Tourism", "symbol": "NIFTYINDTOURISM" },
  { "index_id": 87, "name": "Nifty Capital Mkt", "symbol": "NIFTYCAPITALMKT" },
  { "index_id": 88, "name": "Nifty500Momentm50", "symbol": "NIFTY500MOMENTM50" },
  { "index_id": 89, "name": "NiftyMS400 MQ 100", "symbol": "NIFTYMS400MQ100" },
  { "index_id": 90, "name": "NiftySml250MQ 100", "symbol": "NIFTYSML250MQ100" },
  { "index_id": 91, "name": "Nifty Top 10 EW", "symbol": "NIFTYTOP10EW" },
  { "index_id": 92, "name": "NIFTY AQL 30", "symbol": "NIFTYAQL30" },
  { "index_id": 93, "name": "NIFTY AQLV 30", "symbol": "NIFTYAQLV30" },
  { "index_id": 94, "name": "NIFTY EV", "symbol": "NIFTYEV" },
  { "index_id": 95, "name": "NIFTY HIGHBETA 50", "symbol": "NIFTYHIGHBETA50" },
  { "index_id": 96, "name": "NIFTY NEW CONSUMP", "symbol": "NIFTYNEWCONSUMP" },
  { "index_id": 97, "name": "NIFTY CORP MAATR", "symbol": "NIFTYCORPMAATR" },
  { "index_id": 98, "name": "NIFTY LOW VOL 50", "symbol": "NIFTYLOWVOL50" },
  { "index_id": 99, "name": "NIFTY MOBILITY", "symbol": "NIFTYMOBILITY" },
  { "index_id": 100, "name": "NIFTY QLTY LV 30", "symbol": "NIFTYQLTYLV30" },
  { "index_id": 101, "name": "NIFTY SML250 Q50", "symbol": "NIFTYSML250Q50" },
  { "index_id": 102, "name": "NIFTY TOP 15 EW", "symbol": "NIFTYTOP15EW" },
  { "index_id": 103, "name": "NIFTY100 ALPHA 30", "symbol": "NIFTY100ALPHA30" },
  { "index_id": 104, "name": "NIFTY100 ENH ESG", "symbol": "NIFTY100ENHESG" },
  { "index_id": 105, "name": "NIFTY200 VALUE 30", "symbol": "NIFTY200VALUE30" },
  { "index_id": 106, "name": "NIFTY500 EW", "symbol": "NIFTY500EW" },
  { "index_id": 107, "name": "NIFTY MULTI MQ 50", "symbol": "NIFTYMULTIMQ50" },
  { "index_id": 108, "name": "NIFTY500 VALUE 50", "symbol": "NIFTY500VALUE50" },
  { "index_id": 109, "name": "NIFTY TOP 20 EW", "symbol": "NIFTYTOP20EW" },
  { "index_id": 110, "name": "NIFTY COREHOUSING", "symbol": "NIFTYCOREHOUSING" },
  { "index_id": 111, "name": "NIFTY FINSEREXBNK", "symbol": "NIFTYFINSEREXBNK" },
  { "index_id": 112, "name": "NIFTY HOUSING", "symbol": "NIFTYHOUSING" },
  { "index_id": 113, "name": "NIFTY IPO", "symbol": "NIFTYIPO" },
  { "index_id": 114, "name": "NIFTY MS FIN SERV", "symbol": "NIFTYMSFINSERV" },
  { "index_id": 115, "name": "NIFTY MS IND CONS", "symbol": "NIFTYMSINDCONS" },
  { "index_id": 116, "name": "NIFTY MS IT TELCM", "symbol": "NIFTYMSITTELCM" },
  { "index_id": 117, "name": "NIFTY NONCYC CONS", "symbol": "NIFTYNONCYCCONS" },
  { "index_id": 118, "name": "NIFTY RURAL", "symbol": "NIFTYRURAL" },
  { "index_id": 119, "name": "NIFTY SHARIAH 25", "symbol": "NIFTYSHARIAH25" },
  { "index_id": 120, "name": "NIFTY TRANS LOGIS", "symbol": "NIFTYTRANSLOGIS" },
  { "index_id": 121, "name": "NIFTY50 SHARIAH", "symbol": "NIFTY50SHARIAH" },
  { "index_id": 122, "name": "NIFTY500 LMS EQL", "symbol": "NIFTY500LMSEQL" },
  { "index_id": 123, "name": "NIFTY500 SHARIAH", "symbol": "NIFTY500SHARIAH" },
  { "index_id": 124, "name": "NIFTY500 QLTY50", "symbol": "NIFTY500QLTY50" },
  { "index_id": 125, "name": "NIFTY500 LOWVOL50", "symbol": "NIFTY500LOWVOL50" },
  { "index_id": 126, "name": "NIFTY500 MQVLV50", "symbol": "NIFTY500MQVLV50" }
]

INDEX_ID_TO_NAME = {x["index_id"]: x["name"] for x in INDEXES}
NAME_TO_INDEX_ID = {x["name"].strip().upper(): x["index_id"] for x in INDEXES}

SYMBOL_TO_INDEX_ID = {
    str(x.get("symbol", "")).strip().upper(): x["index_id"]
    for x in INDEXES
    if x.get("symbol")
}

# -----------------------------
# Helpers
# -----------------------------
def _sample_1day_lastprice_10(db: Session, index_id: int) -> List[Dict[str, Any]]:
    """
    Returns 10 sampled points for last 1 day:
    - first (morning) row
    - last (end) row
    - 8 evenly spaced rows in between
    Each point has only: interval_start, last
    """
    now_utc = datetime.now(timezone.utc)
    start_ts = now_utc - timedelta(days=1)

    rows = (
        db.query(NseCmIndex1Min.interval_start, NseCmIndex1Min.last_price)
        .filter(NseCmIndex1Min.index_id == index_id)
        .filter(NseCmIndex1Min.interval_start >= start_ts)
        .order_by(NseCmIndex1Min.interval_start.asc())
        .all()
    )

    if not rows:
        return []

    n = len(rows)
    target = 10

    # If less/equal rows than target, return all (still only lastprice)
    if n <= target:
        return [
            {
                "interval_start": r.interval_start,
                "last": float(r.last_price) if r.last_price is not None else None,
            }
            for r in rows
        ]

    # Always include first + last, and pick remaining evenly
    # indices: 0 ... n-1
    sample_idx = [0]
    for k in range(1, target - 1):  # 1..8
        # map k to range (1..n-2)
        idx = round(k * (n - 1) / (target - 1))
        sample_idx.append(min(max(idx, 1), n - 2))
    sample_idx.append(n - 1)

    # unique + ordered
    uniq = []
    seen = set()
    for i in sample_idx:
        if i not in seen:
            uniq.append(i)
            seen.add(i)

    return [
        {
            "interval_start": rows[i].interval_start,
            "last": float(rows[i].last_price) if rows[i].last_price is not None else None,
        }
        for i in uniq
    ]

def _resolve_index_ids(codes: List[str]) -> List[int]:
    out: List[int] = []

    for code in codes:
        c = str(code).strip()
        if not c:
            continue

        uc = c.upper()

        # ✅ allow passing numeric index_id as well
        if uc.isdigit():
            out.append(int(uc))
            continue

        # ✅ allow passing symbol like "NIFTY50", "NIFTYBANK", etc.
        idx = SYMBOL_TO_INDEX_ID.get(uc)
        if idx is not None:
            out.append(idx)
            continue

        # ✅ (optional) allow passing exact NAME too (like "NIFTY 50")
        # This keeps backward compatibility if someone sends name.
        name_match = next(
            (x["index_id"] for x in INDEXES if x["name"].strip().upper() == uc),
            None,
        )
        if name_match is not None:
            out.append(name_match)
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

    # 1-day sampled data (10 points) for graph (only lastprice)
    hist_1d_10 = _sample_1day_lastprice_10(db, index_id)

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

        # ✅ NEW: chart data
        "historical": hist_1d_10,
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

