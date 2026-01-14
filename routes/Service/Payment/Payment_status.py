from typing import Optional, Dict
from fastapi import APIRouter, HTTPException, Query
from httpx import AsyncClient
from config import CASHFREE_APP_ID, CASHFREE_SECRET_KEY

router = APIRouter(prefix="/service/payment", tags=["payment"])

def _base_url() -> str:
    return "https://api.cashfree.com/pg"

def _headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "x-client-id": CASHFREE_APP_ID,
        "x-client-secret": CASHFREE_SECRET_KEY,
        "x-api-version": "2025-01-01",
    }

async def _call_cashfree(method: str, path: str, json_data: Optional[dict] = None) -> dict:
    url = _base_url() + path
    async with AsyncClient(timeout=30.0) as client:
        resp = await client.request(method, url, headers=_headers(), json=json_data)

    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail=f"Resource not found: {path}")
    if resp.status_code == 401:
        raise HTTPException(status_code=401, detail="Invalid Cashfree credentials")
    if resp.status_code not in (200, 201):
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    try:
        return resp.json()
    except Exception:
        raise HTTPException(status_code=500, detail="Invalid JSON from Cashfree")


@router.get("/status")
async def web_payment_status(order_id: str = Query(..., min_length=3)):
    """
    Returns order_status from Cashfree:
    - PAID => success
    - ACTIVE/PENDING => still processing
    - EXPIRED/CANCELLED/FAILED etc => failed
    """
    if not order_id:
        raise HTTPException(status_code=400, detail="order_id is required")

    cf = await _call_cashfree("GET", f"/orders/{order_id}")

    order_status = (cf.get("order_status") or "").upper()

    # Normalize to our UI statuses
    if order_status == "PAID":
        ui_status = "PAID"
    elif order_status in ("ACTIVE", "PENDING"):
        ui_status = "PENDING"
    else:
        ui_status = "FAILED"

    return {
        "order_id": order_id,
        "order_status": order_status,
        "status": ui_status,          # ✅ frontend friendly
        "is_paid": ui_status == "PAID",
    }
