# routes/Payment/plan.py

import logging
import time
import threading
import requests
from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/service/plan", tags=["plan"])

url = "https://crm.pridecons.com/api/v1/services/"

# -------------------- 30-min TTL cache (in-memory) --------------------
CACHE_TTL_SECONDS = 30 * 60  # 30 minutes

_cache = {
    "data": None,
    "fetched_at": 0.0,
}
_cache_lock = threading.Lock()


def _is_cache_valid() -> bool:
    return _cache["data"] is not None and (time.time() - _cache["fetched_at"]) < CACHE_TTL_SECONDS


def _fetch_from_upstream():
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _get_data_with_cache():
    """Return upstream json using TTL cache + stale fallback."""
    # 1) Fast path
    if _is_cache_valid():
        return _cache["data"], True, False  # data, cached, stale

    # 2) Slow path with lock
    with _cache_lock:
        if _is_cache_valid():
            return _cache["data"], True, False

        try:
            data = _fetch_from_upstream()
            _cache["data"] = data
            _cache["fetched_at"] = time.time()
            return data, False, False

        except requests.exceptions.Timeout:
            logger.exception("Timeout while calling services API")
            if _cache["data"] is not None:
                return _cache["data"], True, True
            raise HTTPException(status_code=504, detail="Upstream timeout (services API)")

        except requests.exceptions.HTTPError as e:
            logger.exception("HTTP error from services API: %s", getattr(e.response, "text", ""))
            if _cache["data"] is not None:
                return _cache["data"], True, True
            status = e.response.status_code if e.response is not None else 502
            raise HTTPException(status_code=status, detail="Upstream error (services API)")

        except requests.exceptions.RequestException as e:
            logger.exception("Request failed: %s", str(e))
            if _cache["data"] is not None:
                return _cache["data"], True, True
            raise HTTPException(status_code=502, detail="Failed to reach services API")


def _extract_list(payload):
    """
    Upstream response shape varies:
    - sometimes list directly
    - sometimes {"data": [...]}
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return payload["data"]
    return []


@router.get("/")
def get_plans():
    data, cached, stale = _get_data_with_cache()
    return {
        "cached": cached,
        "stale": stale,
        "cached_at": _cache["fetched_at"],
        "ttl_seconds": CACHE_TTL_SECONDS,
        "data": data,
    }


@router.get("/{service_id}")
def get_plan_by_id(service_id: int):
    data, cached, stale = _get_data_with_cache()

    items = _extract_list(data)
    match = next((x for x in items if str(x.get("id")) == str(service_id)), None)

    if not match:
        raise HTTPException(status_code=404, detail=f"Service not found for id={service_id}")

    return {
        "cached": cached,
        "stale": stale,
        "cached_at": _cache["fetched_at"],
        "ttl_seconds": CACHE_TTL_SECONDS,
        "data": match,  # ✅ only matched service
    }
