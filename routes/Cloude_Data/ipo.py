# routes/Cloude_Data/ipo.py
import logging
from typing import Literal, Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from utils.Cloude.Cloude import get_json, list_objects

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ipo", tags=["IPO"])

# ✅ Your upload pattern: ipo/YYYY-MM-DD/<section>.json
R2_PREFIX = "ipo/"


def _latest_section_key(section: str) -> str:
    """
    Find latest key for a given section under ipo/ by LastModified.
    Example return: ipo/2026-01-02/current.json
    """
    objs = list_objects(prefix=R2_PREFIX, limit=2000)

    suffix = f"/{section}.json"
    matches = [o for o in (objs or []) if (o.get("Key") or "").endswith(suffix)]

    if not matches:
        raise FileNotFoundError(f"No {section}.json found under prefix: {R2_PREFIX}")

    latest = max(matches, key=lambda x: x["LastModified"])
    return latest["Key"]


def _as_list(payload: Any) -> List[Dict[str, Any]]:
    """
    Normalize JSON to a list so pagination always works.
    - If payload is list => return it
    - If payload is dict with common list keys => return that list
    - If payload is dict => wrap as single item
    - Else => []
    """
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        # common patterns: {"data": [...]}, {"rows":[...]}, {"items":[...]}
        for k in ("data", "rows", "items", "results"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
        return [payload]

    return []


@router.get("/latest")
def get_latest_ipo(
    section: Literal["current", "upcoming", "listed"] = Query("current"),
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    limit: int = Query(50, ge=1, le=200, description="Items per page"),
):
    """
    Returns latest IPO JSON from R2 for the given section + pagination.

    Expected keys:
      ipo/<YYYY-MM-DD>/current.json
      ipo/<YYYY-MM-DD>/upcoming.json
      ipo/<YYYY-MM-DD>/listed.json

    Pagination works when JSON is a list, OR dict containing list under:
      data / rows / items / results
    """
    try:
        latest_key = _latest_section_key(section)
        raw = get_json(latest_key)

        rows = _as_list(raw)
        total = len(rows)

        start = (page - 1) * limit
        end = start + limit
        paged = rows[start:end]

        return {
            "ok": True,
            "latest_key": latest_key,
            "section": section,
            "page": page,
            "limit": limit,
            "total": total,
            "has_next": end < total,
            "has_prev": page > 1,
            "data": paged,
        }

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("Failed to fetch latest IPO file")
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
