# routes/Cloude_Data/ipo.py
import logging
from typing import Literal

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

    # ✅ only match files that end with "/<section>.json"
    suffix = f"/{section}.json"
    matches = [o for o in (objs or []) if (o.get("Key") or "").endswith(suffix)]

    if not matches:
        raise FileNotFoundError(f"No {section}.json found under prefix: {R2_PREFIX}")

    latest = max(matches, key=lambda x: x["LastModified"])
    return latest["Key"]


@router.get("/latest")
def get_latest_ipo(
    section: Literal["current", "upcoming", "listed"] = Query("current"),
):
    """
    Returns latest IPO JSON from R2 for the given section.

    Expected keys:
      ipo/<YYYY-MM-DD>/current.json
      ipo/<YYYY-MM-DD>/upcoming.json
      ipo/<YYYY-MM-DD>/listed.json
    """
    try:
        latest_key = _latest_section_key(section)
        data = get_json(latest_key)

        return {
            "latest_key": latest_key,
            "section": section,
            "data": data,
        }

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("Failed to fetch latest IPO file")
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
