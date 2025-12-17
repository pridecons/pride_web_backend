# routes/Cloude_Data/faoOiParticipant.py
import logging
from fastapi import APIRouter, HTTPException

from utils.Cloude.Cloude import get_json, get_latest_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/fao-oi",
    tags=["Fao Oi Participant"],
)

# ✅ screenshot ke according exact prefix
R2_PREFIX = "/faoOiParticipant/"

@router.get("/latest")
def get_latest_faoOiParticipant():
    """
    Get latest JSON file from R2 folder 'pride-web/faoOiParticipant/' and return its JSON.
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
        logger.exception("Failed to fetch latest faoOiParticipant file")
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
