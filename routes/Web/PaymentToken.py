# routes/web/PaymentToken.py
import logging
from typing import Optional, List
from datetime import datetime
import uuid

from fastapi import APIRouter, HTTPException, Depends, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import desc

from db.models import PaymentToken
from db.connection import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/payment-token", tags=["PaymentToken"])

# -------------------- Schemas --------------------
class PaymentTokenCreate(BaseModel):
    jwt_token: str = Field(..., min_length=1, max_length=255)

class PaymentTokenOut(BaseModel):
    id: int
    code: str
    jwt_token: str

    class Config:
        from_attributes = True  # pydantic v2


# -------------------- helpers --------------------
def _generate_code_25() -> str:
    """
    25-char unique-ish code:
    - 14 chars datetime: YYYYMMDDHHMMSS
    - 11 chars from uuid4 hex
    total = 25
    """
    ts = datetime.now().strftime("%Y%m%d%H%M%S")  # 14
    u = uuid.uuid4().hex[:11]                     # 11
    return ts + u                                 # 25


def _generate_unique_code(db: Session, max_tries: int = 10) -> str:
    """
    Ensures uniqueness in DB. Very low collision, but we still check.
    """
    for _ in range(max_tries):
        code = _generate_code_25()
        exists = db.query(PaymentToken.id).filter(PaymentToken.code == code).first()
        if not exists:
            return code
    raise HTTPException(status_code=500, detail="Could not generate unique code, try again")


# -------------------- POST: create (auto code) --------------------
@router.post("/", response_model=PaymentTokenOut, status_code=status.HTTP_201_CREATED)
def create_payment_token(payload: PaymentTokenCreate, db: Session = Depends(get_db)):
    # jwt_token should also be unique (as per model)
    exists_jwt = db.query(PaymentToken.id).filter(PaymentToken.jwt_token == payload.jwt_token).first()
    if exists_jwt:
        raise HTTPException(status_code=409, detail="jwt_token already exists")

    code = _generate_unique_code(db)  # 25 chars + unique

    obj = PaymentToken(code=code, jwt_token=payload.jwt_token.strip())
    db.add(obj)
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        # In rare race condition, unique constraint may fail
        raise HTTPException(status_code=409, detail="Duplicate detected, retry") from e

    db.refresh(obj)
    return obj


# -------------------- GET: list --------------------
# @router.get("/", response_model=List[PaymentTokenOut])
# def list_payment_tokens(
#     skip: int = Query(0, ge=0),
#     limit: int = Query(50, ge=1, le=500),
#     q: Optional[str] = Query(None, description="Search by code (contains)"),
#     db: Session = Depends(get_db),
# ):
#     query = db.query(PaymentToken)

#     if q:
#         query = query.filter(PaymentToken.code.ilike(f"%{q.strip()}%"))

#     query = query.order_by(desc(PaymentToken.id))
#     return query.offset(skip).limit(limit).all()


# -------------------- GET: by code --------------------
@router.get("/{code}", response_model=PaymentTokenOut)
def get_payment_token_by_code(code: str, db: Session = Depends(get_db)):
    obj = db.query(PaymentToken).filter(PaymentToken.code == code).first()
    if not obj:
        raise HTTPException(status_code=404, detail="PaymentToken not found")
    return obj
