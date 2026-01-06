# route/missing_logo.py  (or wherever you keep routers)
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from db.connection import get_db
from db.models import MissingLogo

router = APIRouter(prefix="/missing-logo", tags=["logo"])


# -------------------- Schemas --------------------

class MissingLogoCreate(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=100)
    name: str = Field(..., min_length=1, max_length=200)

class MissingLogoUpdate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)

class MissingLogoOut(BaseModel):
    id: int
    symbol: str
    name: str

    class Config:
        from_attributes = True


# -------------------- Helpers --------------------

def _norm_symbol(s: str) -> str:
    return (s or "").strip().upper()


# -------------------- APIs --------------------

@router.post(
    "/",
    response_model=MissingLogoOut,
    status_code=status.HTTP_201_CREATED,
)
def create_missing_logo(payload: MissingLogoCreate, db: Session = Depends(get_db)):
    symbol = _norm_symbol(payload.symbol)
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")

    obj = MissingLogo(symbol=symbol, name=payload.name.strip())
    db.add(obj)

    try:
        db.commit()
        db.refresh(obj)
        return obj
    except IntegrityError:
        db.rollback()
        # ✅ symbol unique constraint assumed
        raise HTTPException(
            status_code=409,
            detail=f"symbol '{symbol}' already exists",
        )


@router.get("/", response_model=List[MissingLogoOut])
def list_missing_logos(
    db: Session = Depends(get_db),
    q: Optional[str] = Query(None, description="search by symbol/name"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
):
    qs = db.query(MissingLogo)

    if q:
        qq = q.strip()
        # simple search (works cross-db)
        qs = qs.filter(
            (MissingLogo.symbol.ilike(f"%{qq}%")) | (MissingLogo.name.ilike(f"%{qq}%"))
        )

    return qs.order_by(MissingLogo.id.desc()).offset(skip).limit(limit).all()


@router.get("/by-symbol/{symbol}", response_model=MissingLogoOut)
def get_by_symbol(symbol: str, db: Session = Depends(get_db)):
    sym = _norm_symbol(symbol)
    obj = db.query(MissingLogo).filter(MissingLogo.symbol == sym).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")
    return obj


@router.get("/{id}", response_model=MissingLogoOut)
def get_missing_logo(id: int, db: Session = Depends(get_db)):
    obj = db.query(MissingLogo).filter(MissingLogo.id == id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")
    return obj


@router.put("/{id}", response_model=MissingLogoOut)
def update_missing_logo(id: int, payload: MissingLogoUpdate, db: Session = Depends(get_db)):
    obj = db.query(MissingLogo).filter(MissingLogo.id == id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")

    obj.name = payload.name.strip()
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_missing_logo(id: int, db: Session = Depends(get_db)):
    obj = db.query(MissingLogo).filter(MissingLogo.id == id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")

    db.delete(obj)
    db.commit()
    return None


# ✅ optional: bulk add (skips duplicates gracefully)
class MissingLogoBulkCreate(BaseModel):
    items: List[MissingLogoCreate]

@router.post("/bulk", response_model=dict)
def bulk_create_missing_logo(payload: MissingLogoBulkCreate, db: Session = Depends(get_db)):
    inserted = 0
    skipped = 0

    for it in payload.items:
        sym = _norm_symbol(it.symbol)
        if not sym:
            skipped += 1
            continue

        obj = MissingLogo(symbol=sym, name=it.name.strip())
        db.add(obj)
        try:
            db.commit()
            inserted += 1
        except IntegrityError:
            db.rollback()
            skipped += 1

    return {"inserted": inserted, "skipped": skipped}
