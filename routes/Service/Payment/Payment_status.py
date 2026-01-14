from typing import Optional, Dict
from fastapi import APIRouter, HTTPException
from httpx import AsyncClient
from config import CASHFREE_APP_ID, CASHFREE_SECRET_KEY

# ──────────────────────────────────────────────────────────────────────────────
# Cashfree helpers
# ──────────────────────────────────────────────────────────────────────────────
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
async def web_payment_status(order_id: str):
    # 1) Cashfree se order fetch
    cf = await _call_cashfree("GET", f"/orders/{order_id}")

    # Cashfree order_status examples: PAID / ACTIVE / EXPIRED / CANCELLED (etc)
    order_status = (cf.get("order_status") or "").upper()

    return {
        "order_id": order_id,
        "order_status": order_status,
        "is_paid": order_status == "PAID",
    }
