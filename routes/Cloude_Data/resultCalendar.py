# routes/Cloude_Data/resultCalendar.py
import logging
from fastapi import APIRouter, HTTPException

from utils.Cloude.Cloude import get_json, get_latest_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/result-calendar",
    tags=["Result Calendar"],
)

# ✅ screenshot ke according exact prefix
R2_PREFIX = "/resultCalendar/"

@router.get("/latest")
def get_latest_resultCalendar():
    """
    Get latest JSON file from R2 folder 'pride-web/resultCalendar/' and return its JSON.
    """
    try:
        latest_key = get_latest_key(R2_PREFIX)
        data = get_json(latest_key)
        return {
            "latest_key": latest_key,
            "data": data,
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("Failed to fetch latest resultCalendar file")
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
