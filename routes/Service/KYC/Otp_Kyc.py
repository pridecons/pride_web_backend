from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from db.models import leadData
from db.connection import get_db
from routes.Service.OTP.otp_service import send_otp_kyc, verify_otp
from db.Schema.KYC_Verify import KYCOTPRequest, KYCOTPVerifyRequest

import uuid
import secrets

router = APIRouter(prefix="/kyc/kyc_otp", tags=["KYC"])

def generate_session_id(prefix: str = "KYC") -> str:
    # Example:
    # KYC_6f4c8b1c3f9a4d7aa2d6b3f1b12a8c9e_2qVf3kJpQ9m7rT1uXz8aBg
    return f"{prefix}_{uuid.uuid4().hex}_{secrets.token_urlsafe(24)}"

@router.post("/")
async def kyc_send_otp(
    request: KYCOTPRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    # ✅ Check existing KYC by mobile
    existing = (
        db.query(leadData)
          .filter(leadData.mobile == request.mobile)
          .order_by(leadData.id.desc())
          .first()
    )

    if existing and existing.kyc is True:
        return {
            "success": False,
            "message": "KYC is already done for this mobile number.",
            "session_id": existing.session_id,
            "steps": {
                "step1": bool(existing.step1),
                "step2": bool(existing.step2),
                "step3": bool(existing.step3),
                "step4": bool(existing.step4),
                "step5": bool(existing.step5),
                "kyc": True,
            }
        }

    # ✅ Otherwise send OTP
    tracking_id = await send_otp_kyc(
        request.mobile,
        background_tasks,
        db,
        request.email
    )

    return {
        "success": True,
        "message": f"OTP sent to {request.mobile}",
        "tracking_id": tracking_id
    }

@router.post("/verify")
def kyc_verify_otp(request: KYCOTPVerifyRequest, db: Session = Depends(get_db)):
    otp_verification_result = verify_otp(request.mobile, request.otp, db)

    if not (otp_verification_result.get("status") == "success" and otp_verification_result.get("status_code") == 200):
        raise HTTPException(status_code=400, detail="Invalid OTP")

    # ✅ Find existing user by mobile
    existing = (
        db.query(leadData)
          .filter(leadData.mobile == request.mobile)
          .order_by(leadData.id.desc())
          .first()
    )

    # ✅ Generate fresh session_id every successful verify (recommended)
    session_id = generate_session_id()

    if existing:
        # update existing
        existing.email = request.email or existing.email
        existing.session_id = session_id
        existing.step1 = True

        # optional: reset next steps if you want new flow start (uncomment if needed)
        # existing.step2 = False
        # existing.step3 = False
        # existing.step4 = False
        # existing.step5 = False
        # existing.kyc = False
        kyc_user = existing
    else:
        # create new
        kyc_user = leadData(
            mobile=request.mobile,
            email=request.email,
            session_id=session_id,
            step1=True,
        )
        db.add(kyc_user)

    db.commit()
    db.refresh(kyc_user)

    return {
        "message": "OTP verified successfully",
        "session_id": kyc_user.session_id,
        "steps": {
            "step1": bool(kyc_user.step1),
            "step2": bool(kyc_user.step2),
            "step3": bool(kyc_user.step3),
            "step4": bool(kyc_user.step4),
            "step5": bool(kyc_user.step5),
            "kyc": bool(kyc_user.kyc),
        }
    }

