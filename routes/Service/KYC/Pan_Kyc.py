from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Form, Query
from sqlalchemy.orm import Session
from db.models import leadData
from db.connection import get_db
from config import PAN_API_ID, PAN_API_KEY, PAN_TASK_ID_1
import httpx
import asyncio
from db.connection import SessionLocal  # make sure you have this (like your other code)
from datetime import datetime
from typing import Dict, Any, Tuple, List, Optional

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Form, Query
from sqlalchemy.orm import Session
from db.models import leadData
from db.connection import get_db
from config import PAN_API_ID, PAN_API_KEY, PAN_TASK_ID_1
import httpx
import asyncio
from db.connection import SessionLocal
from datetime import datetime
from typing import Dict, Any, List, Optional

router = APIRouter(prefix="/kyc", tags=["KYC"])

KYC_GENERATE_URL = "https://crm.pridecons.com/api/v1/web/kyc"  # generate kyc

# ... your existing helpers: _parse_dob, extract_pan_details, missing_fields_for_user, etc.

def _dt_iso_from_date(d):
    """Convert date -> ISO datetime string for /web/kyc (Pydantic datetime)."""
    if not d:
        return None
    # 00:00:00
    return datetime.combine(d, datetime.min.time()).isoformat()

async def _call_generate_kyc(entry: leadData) -> Dict[str, Any]:
    """
    Calls /web/kyc to generate signing url.
    Returns safe subset: {ok, kyc_id, signing_url, raw_status, error?}
    """
    payload = {
        "mobile": entry.mobile,
        "email": entry.email,

        "full_name": entry.full_name,
        "director_name": getattr(entry, "director_name", None),
        "father_name": entry.father_name,
        "gender": entry.gender,
        "aadhaar": entry.aadhaar,  # masked ok (XXXX...)
        "pan": entry.pan,

        "state": entry.state,
        "city": entry.city,
        "district": entry.district,
        "address": entry.address,
        "pincode": entry.pincode,
        "country": entry.country,
        "dob": _dt_iso_from_date(entry.dob),

        "gstin": entry.gstin,
        "alternate_mobile": entry.alternate_mobile,
        "marital_status": entry.marital_status,
        "occupation": entry.occupation,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(KYC_GENERATE_URL, json=payload)

        ok = r.status_code in (200, 201)

        try:
            data = r.json()
        except Exception:
            data = {}

        # Your /web/kyc returns kyc_res from update_kyc_details
        # example keys: kyc_id, signing_url
        signing_url = None
        kyc_id = None

        if isinstance(data, dict):
            kyc_id = data.get("kyc_id") or data.get("id") or data.get("group_id")
            signing_url = data.get("signing_url") or data.get("kyc_url") or data.get("url")

        return {
            "ok": ok,
            "status_code": r.status_code,
            "kyc_id": kyc_id,
            "signing_url": signing_url,
            "error": None if ok else (data.get("detail") or data.get("message") or r.text),
        }

    except Exception as e:
        return {
            "ok": False,
            "status_code": None,
            "kyc_id": None,
            "signing_url": None,
            "error": str(e),
        }


def _parse_dob(dob_str: Optional[str]):
    """
    API gives "07-12-2001" (DD-MM-YYYY). Convert to python date.
    """
    if not dob_str:
        return None
    try:
        return datetime.strptime(dob_str, "%d-%m-%Y").date()
    except Exception:
        return None

def extract_pan_details(api_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract only the fields you want to store in DB from the PAN response.
    """
    result = (api_data or {}).get("result") or {}
    addr = result.get("user_address") or {}

    details = {
        "full_name": result.get("user_full_name"),
        "father_name": result.get("user_father_name"),
        "gender": result.get("user_gender"),
        "dob": _parse_dob(result.get("user_dob")),
        "state": addr.get("state"),
        "city": addr.get("city"),
        "district": (addr.get("full") or "").split(",")[-2].strip() if addr.get("full") else None,  # optional guess
        "address": addr.get("full") or None,
        "pincode": addr.get("zip"),
        "country": addr.get("country"),
        "pan_type": result.get("pan_type"),
        "aadhaar_linked_status": result.get("aadhaar_linked_status"),
        "kyc_id": api_data.get("group_id"),
        "request_id": api_data.get("request_id"),
        "task_id": api_data.get("task_id"),
        "aadhaar": result.get("masked_aadhaar"),
    }
    return details

def missing_fields_for_user(details: Dict[str, Any]) -> List[str]:
    """
    Decide what you still need from user.
    Return ONLY missing field names (no sensitive values).
    """
    needed = [
        "full_name",
        "father_name",
        "dob",
        "gender",
        "address",
        "city",
        "state",
        "pincode",
        "country",
        "aadhaar",
        "district",

        "gstin",
        "alternate_mobile",
        "marital_status",
        "occupation",
    ]
    return [k for k in needed if not details.get(k)]

async def post_with_retries(
    url: str,
    headers: dict,
    payload: dict,
    *,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 30.0,
) -> dict:
    """
    POST to `url` with httpx until success.
    """
    attempt = 0
    delay = initial_delay

    while True:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            return resp.json()

        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            detail = f"Error calling {url}: {exc.response.text}"
        except httpx.HTTPError as exc:
            status = 500
            detail = f"Error calling {url}: {str(exc)}"

        attempt += 1
        if attempt > max_retries:
            raise HTTPException(status_code=status, detail=detail)

        await asyncio.sleep(delay)
        delay = min(delay * backoff_factor, max_delay)

def save_pan_verification_to_db(pan: str, api_data: dict, session_id: str):
    db = SessionLocal()
    try:
        entry = db.query(leadData).filter(leadData.session_id == session_id).first()

        details = extract_pan_details(api_data)

        if not entry:
            entry = leadData(session_id=session_id, pan=pan)
            db.add(entry)
        
        # ✅ ALWAYS SAVE PAN (create OR update)
        entry.pan = pan

        # ✅ Save available details
        if details.get("full_name"): entry.full_name = details["full_name"]
        if details.get("father_name"): entry.father_name = details["father_name"]
        if details.get("gender"): entry.gender = details["gender"]
        if details.get("dob"): entry.dob = details["dob"]
        if details.get("address"): entry.address = details["address"]
        if details.get("city"): entry.city = details["city"]
        if details.get("state"): entry.state = details["state"]
        if details.get("pincode"): entry.pincode = details["pincode"]
        if details.get("country"): entry.country = details["country"]
        if details.get("aadhaar"): entry.aadhaar = details["aadhaar"]
        if details.get("district"): entry.district = details["district"]

        # optional tracking (if your model has these)
        if hasattr(entry, "kyc_id") and details.get("kyc_id"):
            entry.kyc_id = details["kyc_id"]

        # step2 = PAN verified (if you want)
        if hasattr(entry, "step2"):
            entry.step2 = True

        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()

@router.post("/pan")
async def micro_pan_verification(
    background_tasks: BackgroundTasks,
    pan: str = Form(...),
    session_id: str = Form(...),
    panType: str = Form(None),
    hard: bool = Form(False),  # ✅ NEW
    db: Session = Depends(get_db),
):
    if not session_id:
        raise HTTPException(status_code=400, detail="Not Authorized")
    if not pan or len(pan.strip()) != 10:
        raise HTTPException(status_code=400, detail="Invalid PAN format. Must be 10 characters.")
    pan = pan.upper().strip()

    # ✅ 0) CACHE CHECK (only when hard=False)
    if not hard:
        cached = (
            db.query(leadData)
              .filter(leadData.session_id == session_id, leadData.pan == pan)
              .first()
        )

        # "name bhi he" => full_name present (you can add father_name too)
        if cached and cached.full_name:
            # ✅ Build details dict from DB (NOT API) so missing_fields works
            db_details = {
                "full_name": cached.full_name,
                "father_name": cached.father_name,
                "gender": cached.gender,
                "dob": cached.dob,           # date object ok
                "state": cached.state,
                "city": cached.city,
                "district": cached.district,
                "address": cached.address,
                "pincode": cached.pincode,
                "country": cached.country,
                "aadhaar": cached.aadhaar,   # NOTE: your model has aadhaar (12). Here you store masked aadhaar, ok.
                "gstin": cached.gstin,
                "alternate_mobile": cached.alternate_mobile,
                "marital_status": cached.marital_status,
                "occupation": cached.occupation,
            }

            missing = missing_fields_for_user(db_details)

            return {
                "success": True,
                "session_id": session_id,
                "pan_number": pan,
                "verification_type": "company" if panType == "company" else "micro",
                "hard": False,
                "source": "cache",  # ✅ tells frontend no API call happened
                "cache_key": f"PAN:{pan}:{session_id}",  # ✅ simple key
                "missing_fields": missing,
                "message": "PAN already verified (cache). Please provide only the missing details.",
                "step1": cached.step1,
                "step2": cached.step2,
                "step3": cached.step3,
                "step4": cached.step4,
                "step5": cached.step5,
                "kyc_url": cached.kyc_url
            }

    # ✅ 1) Zoop call (when cache miss or hard=True)
    url = "https://live.zoop.one/api/v1/in/identity/pan/pro" if panType == "company" else "https://live.zoop.one/api/v1/in/identity/pan/micro"

    headers = {"app-id": PAN_API_ID, "api-key": PAN_API_KEY, "Content-Type": "application/json"}
    payload = {
        "mode": "sync",
        "data": {
            "customer_pan_number": pan,
            "pan_details": True,
            "consent": "Y",
            "consent_text": "I hereby declare my consent agreement for fetching my information via ZOOP API"
        },
        "task_id": PAN_TASK_ID_1
    }

    try:
        api_data = await post_with_retries(url, headers, payload)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    details = extract_pan_details(api_data)
    missing = missing_fields_for_user(details)

    # ✅ 2) Save in background
    # hard=True me PAN change allowed: since you pass new pan + session_id, DB will update that record.
    background_tasks.add_task(save_pan_verification_to_db, pan, api_data, session_id)

    return {
        "success": True,
        "session_id": session_id,
        "pan_number": pan,
        "verification_type": "company" if panType == "company" else "micro",
        "hard": hard,
        "source": "zoop",
        "cache_key": f"PAN:{pan}:{session_id}",
        "missing_fields": missing,
        "message": "PAN verified. Please provide only the missing details.",
        "step1": True,
        "step2": False,
        "step3": False,
        "step4": False,
        "step5": False,
        "kyc_url": ""
    }


@router.get("/pan/check")
async def pan_check(
    session_id: str = Query(...),
    db: Session = Depends(get_db),
):
    if not session_id:
        raise HTTPException(status_code=400, detail="Not Authorized")

    cached = (
        db.query(leadData)
          .filter(leadData.session_id == session_id)
          .first()
    )
    if not cached:
        raise HTTPException(status_code=404, detail="No KYC session found for given session_id")

    # ✅ compute missing fields
    db_details = {
        "full_name": cached.full_name,
        "father_name": cached.father_name,
        "gender": cached.gender,
        "dob": cached.dob,
        "state": cached.state,
        "city": cached.city,
        "district": cached.district,
        "address": cached.address,
        "pincode": cached.pincode,
        "country": cached.country,
        "aadhaar": cached.aadhaar,
        "gstin": cached.gstin,
        "alternate_mobile": cached.alternate_mobile,
        "marital_status": cached.marital_status,
        "occupation": cached.occupation,
    }
    missing = missing_fields_for_user(db_details)

    # ✅ If complete but url not generated => auto generate
    kyc_triggered = False
    kyc_ok = None
    kyc_error = None

    url_missing = (cached.kyc_url is None) or (str(cached.kyc_url).strip() == "")
    if len(missing) == 0 and url_missing:
        kyc_triggered = True

        gen = await _call_generate_kyc(cached)
        kyc_ok = gen["ok"]

        if gen["ok"] and gen.get("signing_url"):
            # ✅ save url + step4
            cached.kyc_url = gen["signing_url"]

            if hasattr(cached, "kyc_id") and gen.get("kyc_id"):
                cached.kyc_id = gen["kyc_id"]

            if hasattr(cached, "step4"):
                cached.step4 = True

            db.commit()
            db.refresh(cached)
        else:
            kyc_error = gen.get("error") or "KYC generate failed"

    return {
        "success": True,
        "session_id": session_id,
        "pan_number": cached.pan,
        "source": "db",
        "missing_fields": missing,
        "is_complete": len(missing) == 0,
        "message": "Missing fields checked from database.",

        # ✅ steps
        "step1": cached.step1,
        "step2": cached.step2,
        "step3": cached.step3,
        "step4": cached.step4,
        "step5": cached.step5,

        # ✅ URL (after auto generate)
        "kyc_url": cached.kyc_url,

        # ✅ info (safe)
        "kyc_triggered": kyc_triggered,
        "kyc_ok": kyc_ok,
        "kyc_error": kyc_error,
    }

#api response = {
#   "success": true,
#   "pan_number": "FXPPM4004P",
#   "verification_type": "micro",
#   "data": {
#     "cached": true,
#     "api_call_count": 2,
#     "request_id": "a2143490-4b71-4641-ac98-78ca5d398e1b",
#     "task_id": "f26eb21e-4c35-4491-b2d5-41fa0e545a34",
#     "group_id": "3a2b136c-e49f-49b1-b57b-816b7d5cabd0",
#     "success": true,
#     "response_code": "100",
#     "response_message": "Valid Authentication",
#     "metadata": {
#       "billable": "Y"
#     },
#     "result": {
#       "user_father_name": "PARSRAM MALVIYA",
#       "pan_number": "FXPPM4004P",
#       "user_full_name": "DHEERAJ MALVIYA",
#       "user_full_name_split": [
#         "DHEERAJ",
#         "",
#         "MALVIYA"
#       ],
#       "masked_aadhaar": "XXXXXXXX1919",
#       "user_address": {
#         "line_1": "32 Hanumantya Mohalla",
#         "line_2": "ward no 08 gram hanumantya",
#         "street_name": "Manawar S.O",
#         "zip": "454446",
#         "city": "Manawar",
#         "state": "Madhya Pradesh",
#         "country": "India",
#         "full": "32 Hanumantya Mohalla, ward no 08 gram hanumantya, Manawar S.O, Manawar, DHAR, 454446, Madhya Pradesh"
#       },
#       "user_email": "ra*********45@gmail.com",
#       "user_phone_number": "78XXXXXX90",
#       "user_gender": "M",
#       "user_dob": "07-12-2001",
#       "aadhaar_linked_status": true,
#       "pan_type": "Person"
#     },
#     "request_timestamp": "2025-10-14T12:10:02.471Z",
#     "response_timestamp": "2025-10-14T12:10:07.723Z"
#   }
# }
