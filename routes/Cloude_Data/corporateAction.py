# routes/Cloude_Data/corporateAction.py
import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query

from utils.Cloude.Cloude import get_json, get_latest_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/corporate-action",
    tags=["Corporate Action"],
)

R2_PREFIX = "corporateAction/"  # folder inside bucket (important: trailing slash)


# ---------------------------
# helpers
# ---------------------------

def _norm(s: str) -> str:
    return " ".join(str(s).strip().split()).lower()

def _parse_ddmmyyyy(s: str) -> Optional[date]:
    """
    Parses 'DD/MM/YYYY' -> date
    Returns None if invalid.
    """
    try:
        dd, mm, yyyy = str(s).strip().split("/")
        return __import__("datetime").date(int(yyyy), int(mm), int(dd))
    except Exception:
        return None

def _paginate(items: List[Dict[str, Any]], page: int, page_size: int) -> Tuple[List[Dict[str, Any]], int, int]:
    total = len(items)
    total_pages = (total + page_size - 1) // page_size if page_size else 0
    start = (page - 1) * page_size
    end = start + page_size
    if start >= total:
        return [], total, total_pages
    return items[start:end], total, total_pages


@router.get("/latest")
def get_latest_corporate_action(
    # ✅ eventType filter: pass one or many
    event_type: Optional[List[str]] = Query(
        default=None,
        description='Filter by eventType. Repeat param: ?event_type=AGM-EGM&event_type=Bonus. Use "all" for no filter.',
    ),

    # ✅ date filtering (uses groupDate if present, else exDate)
    date_from: Optional[str] = Query(
        default=None,
        description='Filter from date (DD/MM/YYYY), compared with groupDate/exDate.',
    ),
    date_to: Optional[str] = Query(
        default=None,
        description='Filter to date (DD/MM/YYYY), compared with groupDate/exDate.',
    ),

    # ✅ pagination
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=500, description="Items per page (max 500)"),
):
    """
    Get latest JSON file from R2 folder 'corporateAction/' and return filtered + paginated JSON.

    Filters:
    - event_type: list of eventType values (repeat param); "all" disables filter
    - date_from/date_to: DD/MM/YYYY inclusive (compares with groupDate/exDate)

    Sorting:
    - ✅ announcementDate ascending (04/12/2025, 05/12/2025, 07/01/2026 ...)
      fallback: groupDate/exDate if announcementDate missing/invalid
    """
    try:
        latest_key = get_latest_key(R2_PREFIX)
        data = get_json(latest_key)

        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return {"latest_key": latest_key, "data": data}

        # ---------------------------
        # ✅ Normalize filters
        # ---------------------------
        event_type_norm = None
        if event_type:
            cleaned = [e for e in event_type if e and str(e).strip()]
            if cleaned:
                if any(_norm(x) == "all" for x in cleaned):
                    event_type_norm = None
                else:
                    event_type_norm = {_norm(x) for x in cleaned}

        d_from = _parse_ddmmyyyy(date_from) if date_from else None
        d_to = _parse_ddmmyyyy(date_to) if date_to else None

        if date_from and d_from is None:
            raise HTTPException(status_code=400, detail="Invalid date_from. Use DD/MM/YYYY")
        if date_to and d_to is None:
            raise HTTPException(status_code=400, detail="Invalid date_to. Use DD/MM/YYYY")

        # ---------------------------
        # ✅ Date pickers
        # ---------------------------
        def _get_filter_date(it: Dict[str, Any]) -> Optional[date]:
            # Filter uses groupDate (your enriched field), else exDate
            s = it.get("groupDate") or it.get("exDate") or ""
            return _parse_ddmmyyyy(s) if s else None

        def _get_announcement_date(it: Dict[str, Any]) -> Optional[date]:
            # Sorting uses announcementDate primarily
            s = it.get("announcementDate") or ""
            dt = _parse_ddmmyyyy(s) if s else None
            return dt

        # ---------------------------
        # ✅ Apply filters
        # ---------------------------
        filtered: List[Dict[str, Any]] = []
        for it in items:
            if not isinstance(it, dict):
                continue

            # eventType filter
            if event_type_norm is not None:
                et = it.get("eventType")
                if not et or _norm(et) not in event_type_norm:
                    continue

            # date filter (groupDate/exDate)
            if d_from or d_to:
                dtf = _get_filter_date(it)
                if dtf is None:
                    continue
                if d_from and dtf < d_from:
                    continue
                if d_to and dtf > d_to:
                    continue

            filtered.append(it)

        # ---------------------------
        # ✅ Sort: announcementDate ASC
        # fallback: groupDate/exDate
        # ---------------------------
        def _sort_key(it: Dict[str, Any]):
            ann = _get_announcement_date(it)
            fallback = _get_filter_date(it)
            # push missing dates to bottom
            primary = ann or fallback
            return (primary is None, primary or date.max)

        filtered.sort(key=_sort_key)

        # ---------------------------
        # ✅ Pagination
        # ---------------------------
        page_items, total, total_pages = _paginate(filtered, page, page_size)

        return {
            "latest_key": latest_key,
            "filters_applied": {
                "event_type": event_type,
                "date_from": date_from,
                "date_to": date_to,
            },
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": total,
                "total_pages": total_pages,
                "returned": len(page_items),
            },
            "count": len(page_items),
            "data": {
                **(data if isinstance(data, dict) else {}),
                "count": total,      # ✅ total after filters
                "items": page_items, # ✅ only paginated items
            },
        }

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to fetch latest corporate action file")
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
