# routes/Cloude_Data/earnometer.py
import logging
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Path

from utils.Cloude.Cloude import get_json

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/technical-analysis",
    tags=["Technical Analysis"],
)

# ✅ base folder in R2
R2_BASE_PREFIX = "earnometer"


def _safe_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    if not s:
        raise HTTPException(status_code=400, detail="symbol is required")
    # only allow simple symbols like RELIANCE, TCS, HDFCBANK, etc.
    if not all(ch.isalnum() or ch in ("_", "-", ".") for ch in s):
        raise HTTPException(status_code=400, detail="invalid symbol")
    return s


@router.get("/{symbol}")
def get_earnometer_technical_analysis(
    symbol: str = Path(..., description="Trading symbol e.g. RELIANCE, TCS, HDFCBANK"),
) -> Dict[str, Any]:
    """
    Fetches latest earnometer technical analysis JSON from R2:
    key: earnometer/{SYMBOL}.json
    """
    sym = _safe_symbol(symbol)
    key = f"{R2_BASE_PREFIX}/{sym}.json"

    try:
        data = get_json(key)
        if not data:
            raise HTTPException(status_code=404, detail=f"No data found for {sym}")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to fetch earnometer JSON for %s: %s", sym, e)
        raise HTTPException(status_code=500, detail="Failed to fetch technical analysis")
