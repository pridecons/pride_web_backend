# routes/Cloude_Data/ipo.py
import logging
from fastapi import APIRouter, HTTPException

from utils.Cloude.Cloude import get_json, get_latest_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/ipo",
    tags=["IPO"],
)

# ✅ screenshot ke according exact prefix (NO leading slash)
R2_PREFIX = "/ipo/"

@router.get("/latest")
def get_latest_ipo():
    """
    Get latest JSON file from R2 folder 'pride-web/ipo/' and return its JSON.
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
        logger.exception("Failed to fetch latest IPO file")
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
