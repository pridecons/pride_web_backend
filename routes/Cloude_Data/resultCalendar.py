# routes/Cloude_Data/resultCalendar.py
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query

from utils.Cloude.Cloude import get_json, get_latest_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/result-calendar",
    tags=["Result Calendar"],
)

# ✅ R2 folder prefix (as per your setup)
R2_PREFIX = "/resultCalendar/"


# ---------------------------
# helpers
# ---------------------------

def _parse_date(d: Optional[str]) -> Optional[date]:
    if not d:
        return None
    d = d.strip()

    # try ISO first
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        pass

    # try dd-mm-yyyy
    try:
        return datetime.strptime(d, "%d-%m-%Y").date()
    except Exception:
        return None


def _pick_item_date(
    item: Dict[str, Any],
    range_start: Optional[date] = None,
    range_end: Optional[date] = None,
) -> Optional[date]:
    """
    Try common keys from Moneycontrol earnings items.

    Supports:
    - "2026-01-08"
    - "08/01/2026"
    - "08 Jan 2026"
    - ✅ "15 Jan" (year missing) -> picks correct year using range_start/range_end
    """
    candidates = [
        item.get("date"),             # e.g. "15 Jan" (no year) OR sometimes ISO
        item.get("resultDate"),       # e.g. "2026-01-08"
        item.get("announcementDate"),
        item.get("reportDate"),
        item.get("asOnDate"),
        item.get("meetingDate"),
        item.get("earningsDate"),
    ]

    def _pick_yearless_dd_mon(vv: str) -> Optional[date]:
        # vv like "15 Jan"
        try:
            dm = datetime.strptime(vv, "%d %b")  # year=1900 internally
        except Exception:
            return None

        if not range_start and not range_end:
            return None  # no context => can't decide year

        years: List[int] = []
        if range_start:
            years.append(range_start.year)
        if range_end and range_end.year not in years:
            years.append(range_end.year)

        # Prefer a date that lies within [range_start, range_end]
        for y in years:
            try:
                d2 = date(y, dm.month, dm.day)
            except Exception:
                continue

            if range_start and d2 < range_start:
                continue
            if range_end and d2 > range_end:
                continue
            return d2

        # If none fits strictly (rare), fallback to start_year
        if range_start:
            try:
                return date(range_start.year, dm.month, dm.day)
            except Exception:
                return None

        return None

    for v in candidates:
        if not v:
            continue
        if isinstance(v, str):
            vv = v.strip()

            # ISO yyyy-mm-dd
            try:
                return datetime.strptime(vv[:10], "%Y-%m-%d").date()
            except Exception:
                pass

            # dd/mm/yyyy
            try:
                return datetime.strptime(vv[:10], "%d/%m/%Y").date()
            except Exception:
                pass

            # dd Mon yyyy
            try:
                return datetime.strptime(vv, "%d %b %Y").date()
            except Exception:
                pass

            # dd-Mon-yyyy
            try:
                return datetime.strptime(vv, "%d-%b-%Y").date()
            except Exception:
                pass

            # dd-mm-yyyy
            try:
                return datetime.strptime(vv[:10], "%d-%m-%Y").date()
            except Exception:
                pass

            # ✅ dd Mon (no year) e.g. "15 Jan"
            d3 = _pick_yearless_dd_mon(vv)
            if d3:
                return d3

    return None


def _paginate(items: List[Dict[str, Any]], page: int, page_size: int) -> Tuple[List[Dict[str, Any]], int]:
    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    if start >= total:
        return [], total
    return items[start:end], total


@router.get("/latest")
def get_latest_resultCalendar(
    # date filter (inclusive)
    start_date: Optional[str] = Query(None, description="Filter start date (inclusive). Format: YYYY-MM-DD or DD-MM-YYYY"),
    end_date: Optional[str] = Query(None, description="Filter end date (inclusive). Format: YYYY-MM-DD or DD-MM-YYYY"),

    # pagination
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=500, description="Items per page (max 500)"),

    # optional search
    q: Optional[str] = Query(None, description="Search in company fields (stockName/companyName/name)"),
):
    """
    Get latest JSON file from R2 folder '/resultCalendar/' and return filtered + paginated JSON.

    Query Params:
    - start_date, end_date: inclusive date filters
    - page, page_size: pagination
    - q: optional search
    """
    try:
        latest_key = get_latest_key(R2_PREFIX)
        data = get_json(latest_key)

        # expected schema from your scraper:
        # data = { ... , "items": [ ... ] }
        items: List[Dict[str, Any]] = []
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            items = data["items"]
        elif isinstance(data, dict) and isinstance(data.get("data"), dict) and isinstance(data["data"].get("items"), list):
            # fallback if you stored nested
            items = data["data"]["items"]

        if not isinstance(items, list):
            items = []

        # ✅ get range context for yearless dates like "15 Jan"
        range_start = None
        range_end = None
        try:
            if isinstance(data, dict) and isinstance(data.get("range"), dict):
                rs = data["range"].get("start")
                re_ = data["range"].get("end")
                range_start = _parse_date(rs) if isinstance(rs, str) else None
                range_end = _parse_date(re_) if isinstance(re_, str) else None
        except Exception:
            range_start = None
            range_end = None

        sd = _parse_date(start_date) if start_date else None
        ed = _parse_date(end_date) if end_date else None
        if (start_date and sd is None) or (end_date and ed is None):
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD or DD-MM-YYYY.")

        # filter
        filtered: List[Dict[str, Any]] = []
        qn = (q or "").strip().lower()

        for it in items:
            if not isinstance(it, dict):
                continue

            # search filter
            if qn:
                name = str(it.get("stockName") or it.get("companyName") or it.get("name") or "").lower()
                if qn not in name:
                    continue

            # date filter
            if sd or ed:
                d = _pick_item_date(it, range_start=range_start, range_end=range_end)
                if d is None:
                    # if date missing, drop when date filter is requested
                    continue
                if sd and d < sd:
                    continue
                if ed and d > ed:
                    continue

            filtered.append(it)

        # ✅ sort by detected date asc (now works for "15 Jan" too) + stable tie-breakers
        def _sort_key(x: Dict[str, Any]):
            d = _pick_item_date(x, range_start=range_start, range_end=range_end)
            name = str(x.get("stockName") or x.get("companyName") or x.get("name") or "")
            # (date asc, name asc, marketCap desc)
            mcap = x.get("marketCap")
            try:
                mcap_n = float(mcap) if mcap is not None else 0.0
            except Exception:
                mcap_n = 0.0
            return (d or date.min, name, -mcap_n)

        filtered.sort(key=_sort_key)

        page_items, total = _paginate(filtered, page, page_size)

        return {
            "latest_key": latest_key,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": total,
                "total_pages": (total + page_size - 1) // page_size if page_size else 0,
                "returned": len(page_items),
            },
            "data": {
                **(data if isinstance(data, dict) else {"raw": data}),
                "items": page_items,  # ✅ replace items with paginated items
            },
        }

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to fetch latest resultCalendar file")
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
