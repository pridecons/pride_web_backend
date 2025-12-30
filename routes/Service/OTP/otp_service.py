from fastapi import APIRouter, Form, HTTPException, Depends, BackgroundTasks
from sqlalchemy.orm import Session
import asyncio
import random
import re
from config import logger
from db.connection import get_db, SessionLocal
from db.models import OTP
from datetime import datetime, timedelta, timezone
import requests
from routes.Service.mail_service.Otp_mail import Otp_mail

router = APIRouter()
PHONE_REGEX = re.compile(r"^\d{10}$")

def validate_phone(phone_number: str):
    """
    Validates the phone number format (should be exactly 10 digits).
    """
    if not PHONE_REGEX.match(phone_number):
        raise HTTPException(status_code=400, detail="Invalid phone number format")
    

SMS_API_URL = "http://msg.nistechnology.in/api/sendhttp.php"
SMS_AUTHKEY = "383970726964653737373130301672472913"
DLT_TE_ID = "1707166866886563812"
SENDER_ID = "PRRIDE"
ROUTE = "2"
COUNTRY = "91"

async def _delete_otp_after(otp_id: int, delay_seconds: int = 1800):
    """
    Sleep for `delay_seconds`, then delete the OTP record if it still exists.
    """
    await asyncio.sleep(delay_seconds)
    db = SessionLocal()
    try:
        record = db.query(OTP).get(otp_id)
        if record:
            db.delete(record)
            db.commit()
    finally:
        db.close()

async def send_otp_kyc(phone_number: str,background_tasks: BackgroundTasks,db: Session = Depends(get_db), email: str = None):
    otp = random.randint(1000, 9999)
    # validate_phone(phone_number)
    # 2. Build your message
    msg = (
        f"Your otp for mobile mobile verification is {otp}  "
        "www.pridecons.com +91-8141054547 PRIDE TRADING CONSULTANCY PVT. LTD."
    )

    url = f"https://api.greentickapi.io/v1/basic-messages?username=89422b93db924e498071e89448a43e4b&password=2fc87a5a648f4d22803f6e13425e4ee3&from=919981919424&to=91{phone_number}&name=otpkyc&langcode=en&variables={otp}&buttonurl={otp}&buttonindex=0"

    # 3. Send SMS via HTTP GET
    params = {
        "authkey": SMS_AUTHKEY,
        "mobiles": phone_number,
        "message": msg,
        "sender": SENDER_ID,
        "route": ROUTE,
        "country": COUNTRY,
        "DLT_TE_ID": DLT_TE_ID,
    }

    try:
        resp = requests.get(SMS_API_URL, params=params, timeout=5)
        resp.raise_for_status()
        print("resp : ",resp.json())
    except requests.RequestException as e:
        # mirror your curl_error behavior
        raise HTTPException(status_code=502, detail=str(e))
    
    try:
        response = requests.get(url)
        response.json()
    
    except requests.RequestException as e:
        logger.error(f"Error connecting to OTP service for {phone_number}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error connecting to OTP service")

    if email:
        await Otp_mail(email, otp)
    
    # 4. Save the OTP in the database
    db_obj = OTP(mobile=phone_number, otp=otp)
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
     # 5. Schedule it for deletion in 30 minutes
    background_tasks.add_task(_delete_otp_after, db_obj.id, 30 * 60)

    # 4. Return the OTP (for debugging; in prod you might omit this)
    return {
                "status": "success",
                "status_code": 200,
                "message": f"OTP has been sent successfully to {phone_number}"
           }


def send_otp(phone_number: str,background_tasks: BackgroundTasks,db: Session = Depends(get_db)):
    otp = random.randint(1000, 9999)
    # validate_phone(phone_number)
    # 2. Build your message
    msg = (
        f"Your otp for mobile mobile verification is {otp}  "
        "www.pridecons.com +91-8141054547 PRIDE TRADING CONSULTANCY PVT. LTD."
    )

    url = f"https://api.greentickapi.io/v1/basic-messages?username=89422b93db924e498071e89448a43e4b&password=2fc87a5a648f4d22803f6e13425e4ee3&from=919981919424&to=91{phone_number}&name=otpkyc&langcode=en&variables={otp}&buttonurl={otp}&buttonindex=0"

    # 3. Send SMS via HTTP GET
    params = {
        "authkey": SMS_AUTHKEY,
        "mobiles": phone_number,
        "message": msg,
        "sender": SENDER_ID,
        "route": ROUTE,
        "country": COUNTRY,
        "DLT_TE_ID": DLT_TE_ID,
    }

    try:
        resp = requests.get(SMS_API_URL, params=params, timeout=5)
        resp.raise_for_status()
        print("resp : ",resp.json())
    except requests.RequestException as e:
        # mirror your curl_error behavior
        raise HTTPException(status_code=502, detail=str(e))
    
    try:
        response = requests.get(url)
        response.json()
    
    except requests.RequestException as e:
        logger.error(f"Error connecting to OTP service for {phone_number}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error connecting to OTP service")
    
    # 4. Save the OTP in the database
    db_obj = OTP(mobile=phone_number, otp=otp)
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
     # 5. Schedule it for deletion in 30 minutes
    background_tasks.add_task(_delete_otp_after, db_obj.id, 30 * 60)

    # 4. Return the OTP (for debugging; in prod you might omit this)
    return {
                "status": "success",
                "status_code": 200,
                "message": f"OTP has been sent successfully to {phone_number}"
           }


def verify_otp(phone_number: str, otp: str,db: Session = Depends(get_db)):
    validate_phone(phone_number)

    record = (
        db.query(OTP)
          .filter(OTP.mobile == phone_number)
          .order_by(OTP.id.desc())
          .first()
    )
    if not record:
        raise HTTPException(status_code=400, detail="Invalid OTP")

    # expire if older than 30 minutes
    if record.timestamp < datetime.now(timezone.utc) - timedelta(minutes=30):
        db.delete(record)
        db.commit()
        raise HTTPException(status_code=400, detail="OTP expired")

    if str(record.otp) != otp:
        raise HTTPException(status_code=400, detail="Invalid OTP")

    # consume it
    db.delete(record)
    db.commit()
    return {
                "status": "success",
                "status_code": 200,
                "message": "OTP verified successfully."
            }
            
