from fastapi import APIRouter, HTTPException, Depends, Form
from sqlalchemy.orm import Session
from db.models import leadData
from db.connection import get_db
from datetime import datetime
from typing import Dict, Any, List
import httpx
from sqlalchemy import func

router = APIRouter(prefix="/kyc", tags=["KYC"])

url = "https://crm.pridecons.com/api/v1/web/kyc"  # generate kyc

def ts_now():
    return func.now()

def missing_fields_for_user(details: Dict[str, Any]) -> List[str]:
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


@router.post("/missing-fields")
async def save_missing_fields(
    session_id: str = Form(...),
    pan: str = Form(None),

    full_name: str = Form(None),
    father_name: str = Form(None),
    gender: str = Form(None),
    dob: str = Form(None),  # "DD-MM-YYYY"
    address: str = Form(None),
    city: str = Form(None),
    district: str = Form(None),
    state: str = Form(None),
    pincode: str = Form(None),
    country: str = Form(None),
    aadhaar: str = Form(None),

    alternate_mobile: str = Form(None),
    marital_status: str = Form(None),
    occupation: str = Form(None),
    gstin: str = Form(None),

    db: Session = Depends(get_db),
):
    # 1) Find row by session_id (and optionally pan)
    q = db.query(leadData).filter(leadData.session_id == session_id)

    entry = q.first()
    if not entry:
        raise HTTPException(status_code=404, detail="Invalid session_id (no lead_data found).")

    # 2) Parse DOB if provided
    parsed_dob = None
    if dob:
        try:
            parsed_dob = datetime.strptime(dob.strip(), "%d-%m-%Y").date()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid dob format. Use DD-MM-YYYY")

    # 3) Prepare incoming payload (only provided values)
    incoming = {
        "full_name": full_name,
        "father_name": father_name,
        "gender": gender,
        "dob": parsed_dob,
        "address": address,
        "city": city,
        "district": district,
        "state": state,
        "pincode": pincode,
        "country": country,
        "alternate_mobile": alternate_mobile,
        "marital_status": marital_status,
        "occupation": occupation,
        "gstin": gstin,
        "aadhaar": aadhaar,
        "pan": pan,
    }

    updated_fields = []
    skipped_fields = []

    # 4) Update logic (fill only when empty)
    for field, value in incoming.items():
        if value is None or (isinstance(value, str) and not value.strip()):
            continue

        if isinstance(value, str):
            value = value.strip()

        current = getattr(entry, field, None)

        if current is None or (isinstance(current, str) and not current.strip()):
            setattr(entry, field, value)
            updated_fields.append(field)
        else:
            skipped_fields.append(field)

    if hasattr(entry, "step3") and updated_fields:
        entry.step3 = True

    db.commit()
    db.refresh(entry)

    # 5) Still missing?
    details = {
        "full_name": entry.full_name,
        "father_name": entry.father_name,
        "gender": entry.gender,
        "dob": entry.dob,
        "address": entry.address,
        "city": entry.city,
        "district": entry.district,
        "state": entry.state,
        "pincode": entry.pincode,
        "country": entry.country,
        "alternate_mobile": entry.alternate_mobile,
        "marital_status": entry.marital_status,
        "occupation": entry.occupation,
        "gstin": entry.gstin,
        "aadhaar": entry.aadhaar,
        "pan": entry.pan
    }
    still_missing = missing_fields_for_user(details)

    # ✅ 6) If nothing missing => call generate KYC API
    kyc_triggered = False
    kyc_api_ok = None
    kyc_api_response: Dict[str, Any] | None = None
    kyc_api_error: str | None = None

    signing_url = None
    new_kyc_id = None

    if len(still_missing) == 0:
        payload = {
            "mobile": entry.mobile,
            "email": entry.email,
            "pan": (entry.pan or "").strip(),
            "full_name": entry.full_name,
            "father_name": entry.father_name,
            "dob": entry.dob.isoformat() if entry.dob else None,
            "gender": entry.gender,
            "address": entry.address,
            "city": entry.city,
            "district": entry.district,
            "state": entry.state,
            "pincode": entry.pincode,
            "country": entry.country,
            "aadhaar": entry.aadhaar,
            "gstin": entry.gstin,
            "alternate_mobile": entry.alternate_mobile,
            "marital_status": entry.marital_status,
            "occupation": entry.occupation,
        }

        kyc_triggered = True

        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                r = await client.post(url, json=payload)

            kyc_api_ok = r.status_code in (200, 201)

            # parse json
            try:
                kyc_api_response = r.json()
            except Exception:
                kyc_api_response = {"raw": r.text}

            # ✅ extract from dict safely (keys depend on your API)
            # example keys: kyc_id, signing_url
            if isinstance(kyc_api_response, dict):
                new_kyc_id = kyc_api_response.get("kyc_id") or kyc_api_response.get("id")
                signing_url = kyc_api_response.get("signing_url") or kyc_api_response.get("url")

            # ✅ save in DB only if got something
            if signing_url:
                # your lead model has: kyc_id, kyc (bool). If you also want kyc_url/step4,
                # ensure these columns exist in leadData model.
                if hasattr(entry, "kyc_id"):
                    entry.kyc_id = new_kyc_id or entry.kyc_id

                if hasattr(entry, "kyc_url"):
                    entry.kyc_url = signing_url  # only if column exists
                    entry.url_date = ts_now()

                if hasattr(entry, "step4"):
                    entry.step4 = True  # only if column exists

                db.commit()
                db.refresh(entry)

        except Exception as e:
            kyc_api_ok = False
            kyc_api_error = str(e)

    return {
        "success": True,
        "session_id": session_id,
        "pan_number": entry.pan,
        "updated_fields": updated_fields,
        "skipped_fields": skipped_fields,
        "still_missing_fields": still_missing,
        "message": "Missing fields saved successfully.",

        # ✅ safe kyc url (never crash)
        "kyc_url": signing_url,

        "kyc_triggered": kyc_triggered,
        "kyc_api_ok": kyc_api_ok,
        "kyc_api_response": kyc_api_response,
        "kyc_api_error": kyc_api_error,
    }
