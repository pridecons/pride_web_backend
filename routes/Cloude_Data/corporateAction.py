# routes/Cloude_Data/corporateAction.py
import logging
from fastapi import APIRouter, HTTPException

from utils.Cloude.Cloude import get_json, get_latest_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/corporate-action",
    tags=["Corporate Action"],
)

R2_PREFIX = "corporateAction/"  # folder inside bucket (important: trailing slash)

@router.get("/latest")
def get_latest_corporate_action():
    """
    Get latest JSON file from R2 folder 'corporateAction/' and return its JSON.
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
        logger.exception("Failed to fetch latest corporate action file")
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
