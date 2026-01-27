# routes/Nse_Stock_Details/Indian_Stock_Exchange_Details.py

import logging
from typing import Any, Dict, Optional
from datetime import date, datetime

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from db.connection import get_db
from db.connection import SessionLocal  # ✅ make sure this exists
from db.models import StockDetail
from config import RAPID_INDIAN_STOCK_EXCHANGE

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/stock-details",
    tags=["Stock Details"],
)

RAPID_HOST = "indian-stock-exchange-api2.p.rapidapi.com"
RAPID_URL = f"https://{RAPID_HOST}/stock"


def today_ist() -> date:
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("Asia/Kolkata")).date()


def normalize_key(name: str) -> str:
    # ✅ consistent DB key
    # tata steel -> TATA_STEEL
    return "_".join(name.strip().upper().split())


async def fetch_from_rapidapi(name: str) -> Dict[str, Any]:
    headers = {
        "x-rapidapi-host": RAPID_HOST,
        "x-rapidapi-key": RAPID_INDIAN_STOCK_EXCHANGE,
    }
    params = {"name": name}

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(RAPID_URL, headers=headers, params=params)

    if resp.status_code != 200:
        logger.error("RapidAPI error %s: %s", resp.status_code, resp.text)
        raise HTTPException(
            status_code=502,
            detail="Upstream stock API failed. Please try again later.",
        )

    data = resp.json()
    if not data:
        raise HTTPException(status_code=404, detail="Stock not found in upstream API.")
    return data


def save_stock_snapshot_bg(
    *,
    symbol: str,
    fetch_date: date,
    payload: Dict[str, Any],
    company_name: Optional[str] = None,
    industry: Optional[str] = None,
) -> None:
    """
    ✅ BackgroundTask: creates its own DB session (important).
    Upsert-ish behavior: if exists for (symbol, fetch_date), update data.
    """
    db: Session = SessionLocal()
    try:
        existing = (
            db.query(StockDetail)
            .filter(StockDetail.symbol == symbol, StockDetail.fetch_date == fetch_date)
            .first()
        )

        if existing:
            existing.data = payload
            # optionally refresh core fields
            if company_name:
                existing.company_name = company_name
            if industry is not None:
                existing.industry = industry
            db.commit()
            return

        row = StockDetail(
            symbol=symbol,
            company_name=company_name or symbol,
            industry=industry,
            fetch_date=fetch_date,
            data=payload,
        )
        db.add(row)
        db.commit()
    except Exception as e:
        logger.exception("Failed to save stock snapshot in BG: %s", e)
        db.rollback()
    finally:
        db.close()


@router.get("/", summary="Get stock details (DB cache daily, else fetch + save)")
async def get_stock_details(
    background_tasks: BackgroundTasks,
    symbol: Optional[str] = Query(None, description="Stock symbol, e.g. TCS"),
    name: Optional[str] = Query(None, description="Stock name, e.g. tata steel"),
    db: Session = Depends(get_db),
):
    if not symbol and not name:
        raise HTTPException(status_code=400, detail="Please provide either 'symbol' or 'name'.")

    fdate = today_ist()

    # ✅ normalize keys
    symbol_key = normalize_key(symbol) if symbol else None
    name_key = normalize_key(name) if name else None

    # ✅ 1) DB check by symbol (priority)
    if symbol_key:
        cached = (
            db.query(StockDetail)
            .filter(StockDetail.symbol == symbol_key, StockDetail.fetch_date == fdate)
            .first()
        )
        if cached:
            return {
                "source": "db",
                "match": "symbol",
                "symbol": cached.symbol,
                "fetch_date": str(cached.fetch_date),
                "data": cached.data,
            }

    # ✅ 2) DB check by name (fallback)
    if name_key:
        cached = (
            db.query(StockDetail)
            .filter(StockDetail.symbol == name_key, StockDetail.fetch_date == fdate)
            .first()
        )
        if cached:
            return {
                "source": "db",
                "match": "name",
                "symbol": cached.symbol,
                "fetch_date": str(cached.fetch_date),
                "data": cached.data,
            }

    # ✅ 3) RapidAPI fetch: prefer name, else symbol
    query_term = name or symbol  # RapidAPI expects `name` param
    payload = await fetch_from_rapidapi(query_term)

    # ✅ DB ke liye slim payload (duplicate key hata do)
    payload_for_db = dict(payload)
    payload_for_db.pop("stockFinancialData", None)

    company_name = payload.get("companyName") or (name or symbol or "")
    industry = payload.get("industry")

    # ✅ store key: prefer provided symbol, else name_key
    store_key = symbol_key or name_key or normalize_key(company_name)

    background_tasks.add_task(
        save_stock_snapshot_bg,
        symbol=store_key,
        fetch_date=fdate,
        payload=payload_for_db,
        company_name=company_name,
        industry=industry,
    )

    return {
        "source": "rapidapi",
        "symbol": store_key,
        "fetch_date": str(fdate),
        "data": payload,
    }
