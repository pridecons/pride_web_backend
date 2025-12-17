# routes/Cloude_Data/News/news.py
import logging
from fastapi import APIRouter, HTTPException

from utils.Cloude.Cloude import get_json, get_latest_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/news",
    tags=["News"],
)

# ✅ exact folder path from screenshot (NO leading slash)
R2_PREFIX = "/news/rediff/"

@router.get("/latest")
def get_latest_news():
    """
    Get latest JSON file from R2 folder 'pride-web/news/rediff/' and return its JSON.
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
        logger.exception("Failed to fetch latest news file")
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
