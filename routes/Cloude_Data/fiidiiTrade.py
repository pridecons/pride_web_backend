# routes/Cloude_Data/fiidiiTrade.py
import logging
from fastapi import APIRouter, HTTPException

from utils.Cloude.Cloude import get_json, get_latest_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/fiidii-trade",
    tags=["fiidii trade"],
)

# ✅ screenshot ke according exact prefix
R2_PREFIX = "/fiidiiTrade/"

@router.get("/latest")
def get_latest_fiidiiTrade():
    """
    Get latest JSON file from R2 folder 'pride-web/fiidiiTrade/' and return its JSON.
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
        logger.exception("Failed to fetch latest fiidiiTrade file")
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
