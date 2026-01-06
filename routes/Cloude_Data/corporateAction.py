# routes/Cloude_Data/corporateAction.py
import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from utils.Cloude.Cloude import get_json, get_latest_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/corporate-action",
    tags=["Corporate Action"],
)

R2_PREFIX = "corporateAction/"  # folder inside bucket (important: trailing slash)

# Example filters:
# ['AGM-EGM', 'Announcement', 'Board Meeting', 'Bonus', 'Dividend - Interim', 'Splits', 'all']


def _norm(s: str) -> str:
    return " ".join(str(s).strip().split()).lower()


def _parse_ddmmyyyy(s: str):
    """
    Parses 'DD/MM/YYYY' -> date
    Returns None if invalid.
    """
    try:
        dd, mm, yyyy = s.split("/")
        return __import__("datetime").date(int(yyyy), int(mm), int(dd))
    except Exception:
        return None


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
):
    """
    Get latest JSON file from R2 folder 'corporateAction/' and return its JSON.
    Optional filters:
    - event_type: list of eventType values
    - date_from/date_to: DD/MM/YYYY (inclusive)
    """
    try:
        latest_key = get_latest_key(R2_PREFIX)
        data = get_json(latest_key)

        # data expected like:
        # { "source":..., "count":..., "items":[{...}] }
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
                # if "all" included => ignore filter
                if any(_norm(x) == "all" for x in cleaned):
                    event_type_norm = None
                else:
                    event_type_norm = {_norm(x) for x in cleaned}

        d_from = _parse_ddmmyyyy(date_from) if date_from else None
        d_to = _parse_ddmmyyyy(date_to) if date_to else None

        # validate date format
        if date_from and d_from is None:
            raise HTTPException(status_code=400, detail="Invalid date_from. Use DD/MM/YYYY")
        if date_to and d_to is None:
            raise HTTPException(status_code=400, detail="Invalid date_to. Use DD/MM/YYYY")

        # ---------------------------
        # ✅ Apply filters
        # ---------------------------
        def _get_item_date(it: dict):
            # Prefer groupDate (your enriched field), else exDate
            s = it.get("groupDate") or it.get("exDate") or ""
            return _parse_ddmmyyyy(str(s)) if s else None

        filtered = []
        for it in items:
            if not isinstance(it, dict):
                continue

            # eventType filter
            if event_type_norm is not None:
                et = it.get("eventType")
                if not et or _norm(et) not in event_type_norm:
                    continue

            # date filter
            if d_from or d_to:
                dt = _get_item_date(it)
                if dt is None:
                    continue
                if d_from and dt < d_from:
                    continue
                if d_to and dt > d_to:
                    continue

            filtered.append(it)

        def _sort_key(it: dict):
            dt = _get_item_date(it)  # date or None
            # None should go to bottom
            return (dt is not None, dt)

        filtered.sort(key=_sort_key, reverse=True)

        return {
            "latest_key": latest_key,
            "filters_applied": {
                "event_type": event_type,
                "date_from": date_from,
                "date_to": date_to,
            },
            "count": len(filtered),
            "data": {
                **(data if isinstance(data, dict) else {}),
                "count": len(filtered),
                "items": filtered,
            },
        }

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to fetch latest corporate action file")
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
