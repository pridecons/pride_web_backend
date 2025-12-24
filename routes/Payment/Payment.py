# routes/Payment/Payment.py

from __future__ import annotations

import os
import uuid
import logging
from typing import Optional, Dict, Any

import requests
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from pydantic import BaseModel, EmailStr, Field, validator

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/service/payment", tags=["payment"])

# Upstream endpoint
UPSTREAM_URL = os.getenv("UPSTREAM_PAYMENT_URL", "https://crm.pridecons.com/api/v1/web/payment")

# timeouts: (connect, read)
UPSTREAM_TIMEOUT = (
    int(os.getenv("UPSTREAM_CONNECT_TIMEOUT", "10")),
    int(os.getenv("UPSTREAM_READ_TIMEOUT", "30")),
)

# ----------------------------- Payload model -----------------------------

class WebPaymentCreate(BaseModel):
    # lead fields (website)
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    number: str = Field(..., description="Mobile number")

    # payment fields
    service_id: Optional[int] = None
    amount: float

    # consent fields
    consent: bool = True
    consent_text: Optional[str] = None
    tz_offset_minutes: int = 330
    channel: str = "WEB"
    purpose: str = "PAYMENT"
    device_info: Optional[dict] = None

    @validator("email", pre=True)
    def empty_email_to_none(cls, v):
        if v is None:
            return None
        s = str(v).strip()
        return s or None

    @validator("number", pre=True)
    def normalize_mobile(cls, v):
        if not v:
            raise ValueError("number is required")
        s = str(v).strip()
        # keep exactly as user sent (aap chahe to digits-only enforce kar sakte ho)
        return s

    @validator("amount")
    def amount_positive(cls, v):
        if v is None or float(v) <= 0:
            raise ValueError("amount must be > 0")
        return float(v)


# ----------------------------- Helpers -----------------------------

def _first_ip_from_xff(xff: str) -> str:
    # "client, proxy1, proxy2" -> client
    return xff.split(",")[0].strip()

def _get_real_client_ip(request: Request) -> str:
    """
    Real IP priority:
      1) Cloudflare: CF-Connecting-IP
      2) X-Forwarded-For (first IP)
      3) X-Real-IP
      4) direct socket ip (request.client.host)
    """
    cf_ip = request.headers.get("cf-connecting-ip")
    if cf_ip:
        return cf_ip.strip()

    xff = request.headers.get("x-forwarded-for")
    if xff:
        return _first_ip_from_xff(xff)

    xri = request.headers.get("x-real-ip")
    if xri:
        return xri.strip()

    return (request.client.host if request.client else "").strip() or "unknown"


def _build_forward_headers(request: Request, request_id: str) -> Dict[str, str]:
    """
    Forward a safe, useful subset of headers + user identity context.
    Note: TCP level IP upstream ko aapke server ki dikhegi,
    user IP headers me forward hoti hai.
    """
    client_ip = _get_real_client_ip(request)

    headers: Dict[str, str] = {
        "accept": request.headers.get("accept", "application/json"),
        "content-type": "application/json",

        # ✅ user info headers
        "x-request-id": request_id,
        "x-real-ip": client_ip,

        # append original chain if present
        "x-forwarded-for": (
            f"{client_ip}, {request.headers.get('x-forwarded-for')}"
            if request.headers.get("x-forwarded-for")
            else client_ip
        ),

        "x-forwarded-proto": request.headers.get("x-forwarded-proto", "https"),
        "x-forwarded-host": request.headers.get("x-forwarded-host") or request.headers.get("host", ""),
    }

    # Forward common client context headers (helpful for upstream logs/analytics)
    for h in ("user-agent", "referer", "origin", "accept-language"):
        v = request.headers.get(h)
        if v:
            headers[h] = v

    # Forward auth if client sends it (only if upstream needs it)
    auth = request.headers.get("authorization")
    if auth:
        headers["authorization"] = auth

    return headers


def _json_or_text(resp: requests.Response) -> JSONResponse:
    """
    Return upstream response:
      - if json -> forward json
      - else -> forward raw text
    """
    try:
        return JSONResponse(status_code=resp.status_code, content=resp.json())
    except Exception:
        return JSONResponse(status_code=resp.status_code, content={"raw": resp.text})


# ----------------------------- Route -----------------------------

@router.post("/")
async def Create_payment(payload: WebPaymentCreate, request: Request):
    """
    Proxy POST -> UPSTREAM_URL
    - Validates body (WebPaymentCreate)
    - Forwards user IP + headers
    - Forwards query params too (if any)
    - Returns upstream response as-is
    """
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())

    # body already validated by Pydantic
    body: Dict[str, Any] = payload.model_dump(exclude_none=True)

    headers = _build_forward_headers(request, request_id)

    # Forward query params too (rare but safe)
    params = dict(request.query_params) if request.query_params else None

    try:
        resp = requests.post(
            UPSTREAM_URL,
            json=body,
            headers=headers,
            params=params,
            timeout=UPSTREAM_TIMEOUT,
        )
    except requests.exceptions.Timeout:
        logger.exception("Timeout while calling payment upstream | request_id=%s", request_id)
        raise HTTPException(status_code=504, detail="Upstream timeout (payment)")
    except requests.exceptions.RequestException as e:
        logger.exception("Payment upstream request failed | request_id=%s | err=%s", request_id, e)
        raise HTTPException(status_code=502, detail="Upstream error (payment)")

    # passthrough upstream response (including errors)
    out = _json_or_text(resp)

    # add trace header in response too
    out.headers["x-request-id"] = request_id
    out.headers["x-proxy"] = "service/payment"

    return out
