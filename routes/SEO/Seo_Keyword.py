# routes/SEO/Stock_SEO.py
# ✅ No BeautifulSoup / No external scraping
# ✅ SEO is generated ONLY from your DB data (NseCmSecurity + Bhavcopy + Intraday if needed)
# ✅ If SEO exists in seo_keyword -> return latest
# ✅ If not -> auto-generate "static" SEO + JSON-LD and save for today (upsert)

import logging
import re
from datetime import date as dt_date, datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, or_
from sqlalchemy.orm import Session

from db.connection import get_db
from db.models import NseCmSecurity, NseCmBhavcopy, SEOKeyword

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/seo", tags=["SEO"])


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

PRIDECONS_HOME = "https://pridecons.com"
PRIDECONS_LOGO = "https://pridecons.com/logo.png"

DEFAULT_ROBOTS = "index,follow"

# Optional: brand handle (if you have)
TWITTER_SITE = None  # e.g. "@pridecons"
TWITTER_CREATOR = None


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _slugify(name: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() else "-" for ch in (name or "").strip())
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "stock"


def build_pride_url(symbol: str, stock_name: str) -> str:
    sym = _clean(symbol).upper()
    slug = _slugify(stock_name)
    return f"{PRIDECONS_HOME}/nse/stock/{sym}/{slug}"


def _get_security_by_symbol_or_name(db: Session, q: str) -> Optional[NseCmSecurity]:
    q_clean = _clean(q)
    if not q_clean:
        return None

    q_sym = q_clean.upper()

    # 1) symbol exact
    sec = db.query(NseCmSecurity).filter(NseCmSecurity.symbol == q_sym).first()
    if sec:
        return sec

    # 2) name match
    return (
        db.query(NseCmSecurity)
        .filter(NseCmSecurity.company_name.ilike(f"%{q_clean}%"))
        .order_by(NseCmSecurity.symbol.asc())
        .first()
    )


def _get_company_name(sec: NseCmSecurity) -> str:
    return _clean(sec.company_name) or _clean(sec.symbol) or "Stock"


def _get_latest_bhavcopy(db: Session, symbol: str) -> Optional[NseCmBhavcopy]:
    sym = _clean(symbol).upper()
    return (
        db.query(NseCmBhavcopy)
        .filter(NseCmBhavcopy.symbol == sym)
        .order_by(desc(NseCmBhavcopy.trade_date))
        .first()
    )


def _get_latest_seo_row(db: Session, symbol: str) -> Optional[SEOKeyword]:
    sym = _clean(symbol).upper()
    return (
        db.query(SEOKeyword)
        .filter(SEOKeyword.symbol == sym)
        .order_by(desc(SEOKeyword.fetch_date))
        .first()
    )


def _upsert_today_seo(db: Session, symbol: str, company_name: str, payload: Dict[str, Any]) -> SEOKeyword:
    sym = _clean(symbol).upper()
    today = datetime.now().date()

    row = (
        db.query(SEOKeyword)
        .filter(SEOKeyword.symbol == sym, SEOKeyword.fetch_date == today)
        .first()
    )
    if row:
        row.company_name = company_name
        row.data = payload
        return row

    row = SEOKeyword(symbol=sym, company_name=company_name, fetch_date=today, data=payload)
    db.add(row)
    return row


def _num_to_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        return str(v)
    except Exception:
        return None


def build_static_seo_from_db(
    *,
    symbol: str,
    stock_name: str,
    bhav: Optional[NseCmBhavcopy],
) -> Dict[str, Any]:
    """
    ✅ Fully DB-based SEO payload:
    - title/description
    - canonical
    - open graph
    - twitter
    - JSON-LD: WebPage + BreadcrumbList + (optional) FinancialProduct snippet
    """
    sym = _clean(symbol).upper()
    name = _clean(stock_name)
    canonical = build_pride_url(sym, name)

    # --- Use bhavcopy snapshot if available (for stronger description)
    last_price = _num_to_str(getattr(bhav, "close_price", None) or getattr(bhav, "last_price", None))
    trade_date = getattr(bhav, "trade_date", None)

    # Title/Desc (static template)
    title = f"{name} ({sym}) Share Price, Chart, Fundamentals | PrideCons"
    if last_price and trade_date:
        desc = (
            f"Check {name} ({sym}) share price and key fundamentals on PrideCons. "
            f"Latest close price: ₹{last_price} as of {trade_date}."
        )
    else:
        desc = (
            f"Check {name} ({sym}) share price, performance, fundamentals and key company information on PrideCons."
        )

    # Breadcrumb JSON-LD
    breadcrumb = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "item": {"@id": f"{PRIDECONS_HOME}/", "name": "Home"}},
            {"@type": "ListItem", "position": 2, "item": {"@id": f"{PRIDECONS_HOME}/nse", "name": "NSE"}},
            {"@type": "ListItem", "position": 3, "item": {"@id": canonical, "name": f"{name} ({sym})"}},
        ],
    }

    # WebPage JSON-LD
    webpage = {
        "@context": "https://schema.org",
        "@type": "WebPage",
        "name": title,
        "description": desc,
        "url": canonical,
        "publisher": {
            "@type": "Organization",
            "name": "PrideCons",
            "url": PRIDECONS_HOME,
            "logo": {"@type": "ImageObject", "contentUrl": PRIDECONS_LOGO},
        },
    }

    # Optional: FinancialProduct-like schema (lightweight, uses DB data only)
    # (kept minimal to avoid wrong claims)
    financial = None
    if bhav and last_price:
        financial = {
            "@context": "https://schema.org",
            "@type": "FinancialProduct",
            "name": f"{name} Equity Share",
            "description": f"{name} ({sym}) equity share basic information and price snapshot.",
            "url": canonical,
            "provider": {"@type": "Organization", "name": "NSE (data source)"},
        }

    jsonld = [webpage, breadcrumb] + ([financial] if financial else [])

    # OG/Twitter
    og = {
        "og:type": "website",
        "og:site_name": "PrideCons",
        "og:url": canonical,
        "og:title": title,
        "og:description": desc,
        "og:image": PRIDECONS_LOGO,
    }

    tw = {
        "twitter:card": "summary_large_image",
        "twitter:title": title,
        "twitter:description": desc,
        "twitter:image": PRIDECONS_LOGO,
    }
    if TWITTER_SITE:
        tw["twitter:site"] = TWITTER_SITE
    if TWITTER_CREATOR:
        tw["twitter:creator"] = TWITTER_CREATOR

    return {
        "symbol": sym,
        "stock_name": name,
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
        "canonical": canonical,
        "title": title,
        "description": desc,
        "robots": DEFAULT_ROBOTS,
        "open_graph": og,
        "twitter": tw,
        "jsonld": jsonld,
        "generated_from": "db_static",
    }


# ---------------------------------------------------------------------
# API
# ---------------------------------------------------------------------

@router.get("/stock", response_model=Dict[str, Any])
def get_stock_seo(
    q: str = Query(..., description="Stock symbol (e.g. RELIANCE) or company name"),
    refresh: bool = Query(False, description="If true, force regenerate for today from DB"),
    save: bool = Query(True, description="If true, save generated SEO into DB (seo_keyword)"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    ✅ Behavior
    - If exists in DB and refresh=false -> return latest
    - Else -> generate static SEO using ONLY your DB data and return
    - If save=true -> upsert into SEOKeyword for today
    """
    sec = _get_security_by_symbol_or_name(db, q)
    if not sec:
        raise HTTPException(status_code=404, detail=f"Stock not found for query: {q}")

    symbol = _clean(sec.symbol).upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="Security record missing symbol")

    company_name = _get_company_name(sec)

    # 1) return cached SEO
    if not refresh:
        row = _get_latest_seo_row(db, symbol)
        if row and row.data:
            return {
                "success": True,
                "source": "db",
                "symbol": symbol,
                "company_name": company_name,
                "fetch_date": str(getattr(row, "fetch_date", "")),
                "data": row.data,
            }

    # 2) Generate from DB only
    bhav = _get_latest_bhavcopy(db, symbol)
    pride_seo = build_static_seo_from_db(symbol=symbol, stock_name=company_name, bhav=bhav)

    payload = {
        "symbol": symbol,
        "company_name": company_name,
        "fetch_date": str(datetime.now().date()),
        "merged_pride": pride_seo,
    }

    # 3) Save to DB if needed
    if save:
        try:
            _upsert_today_seo(db, symbol, company_name, payload)
            db.commit()
        except Exception as e:
            db.rollback()
            logger.exception("DB save failed for %s: %s", symbol, e)
            # still return generated payload

    return {
        "success": True,
        "source": "generated",
        "generated_from": "db_static",
        "symbol": symbol,
        "company_name": company_name,
        "fetch_date": str(datetime.now().date()),
        "data": payload,
    }
