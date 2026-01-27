# routes/mf/mf_api.py

import re
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from db.connection import get_db
from db.models import MfAmc, MfScheme, MfNavDaily, MfSchemeSnapshot, MfJobLog

router = APIRouter(prefix="/mf", tags=["mutual-fund"])


# -----------------------------
# Helpers
# -----------------------------
def _norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return re.sub(r"\s+", " ", str(s)).strip()


def _to_date(s: str) -> date:
    # supports "YYYY-MM-DD"
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")


def _scheme_code_int(scheme_code: str) -> int:
    try:
        return int(str(scheme_code).strip())
    except Exception:
        raise HTTPException(status_code=400, detail="scheme_code must be integer-like")


def _safe_getattr(obj, attr: str, default=None):
    return getattr(obj, attr, default)


# -----------------------------
# 0) Health / Stats (UI dashboard)
# -----------------------------
@router.get("/stats")
def mf_stats(db: Session = Depends(get_db)):
    amc_count = db.query(func.count(MfAmc.id)).scalar() or 0
    scheme_count = db.query(func.count(MfScheme.scheme_code)).scalar() or 0
    active_scheme_count = (
        db.query(func.count(MfScheme.scheme_code))
        .filter(MfScheme.is_active.is_(True))
        .scalar()
        or 0
    )
    nav_rows = db.query(func.count(MfNavDaily.scheme_code)).scalar() or 0
    snapshot_rows = db.query(func.count(MfSchemeSnapshot.scheme_code)).scalar() or 0

    latest_nav_date = db.query(func.max(MfNavDaily.nav_date)).scalar()

    return {
        "amc_count": amc_count,
        "scheme_count": scheme_count,
        "active_scheme_count": active_scheme_count,
        "nav_rows": nav_rows,
        "snapshot_rows": snapshot_rows,
        "latest_nav_date": latest_nav_date,
    }


@router.get("/nav/latest-date")
def latest_nav_date(db: Session = Depends(get_db)):
    d = db.query(func.max(MfNavDaily.nav_date)).scalar()
    return {"latest_nav_date": d}


# -----------------------------
# 1) AMC APIs
# -----------------------------
@router.get("/amcs")
def list_amcs(
    q: Optional[str] = Query(None, description="Search AMC by name"),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    query = db.query(MfAmc)

    if q:
        qq = f"%{_norm(q)}%"
        # name field assumed
        query = query.filter(MfAmc.name.ilike(qq))

    total = query.count()
    rows = query.order_by(MfAmc.name.asc()).offset(skip).limit(limit).all()

    out = []
    for a in rows:
        out.append(
            {
                "id": a.id,
                "name": a.name,
                # if your model has logo_url or similar, expose it
                "logo_url": _safe_getattr(a, "logo_url", None),
                "created_at": _safe_getattr(a, "created_at", None),
                "updated_at": _safe_getattr(a, "updated_at", None),
            }
        )

    return {"total": total, "limit": limit, "skip": skip, "items": out}


@router.get("/amcs/{amc_id}")
def get_amc(amc_id: int, db: Session = Depends(get_db)):
    amc = db.query(MfAmc).filter(MfAmc.id == amc_id).first()
    if not amc:
        raise HTTPException(status_code=404, detail="AMC not found")

    return {
        "id": amc.id,
        "name": amc.name,
        "logo_url": _safe_getattr(amc, "logo_url", None),
        "created_at": _safe_getattr(amc, "created_at", None),
        "updated_at": _safe_getattr(amc, "updated_at", None),
    }


@router.get("/amcs/{amc_id}/schemes")
def list_amc_schemes(
    amc_id: int,
    is_active: Optional[bool] = Query(True),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    q = db.query(MfScheme).filter(MfScheme.amc_id == amc_id)

    if is_active is not None:
        q = q.filter(MfScheme.is_active.is_(is_active))

    total = q.count()
    rows = q.order_by(MfScheme.scheme_name.asc()).offset(skip).limit(limit).all()

    return {
        "amc_id": amc_id,
        "total": total,
        "limit": limit,
        "skip": skip,
        "items": [
            {
                "scheme_code": s.scheme_code,
                "scheme_name": s.scheme_name,
                "category": _safe_getattr(s, "category", None),
                "sub_category": _safe_getattr(s, "sub_category", None),
                "plan": _safe_getattr(s, "plan", None),
                "option": _safe_getattr(s, "option", None),
                "is_active": _safe_getattr(s, "is_active", None),
                "updated_at": _safe_getattr(s, "updated_at", None),
            }
            for s in rows
        ],
    }


# -----------------------------
# 2) Scheme APIs (listing + filters)
# -----------------------------
@router.get("/schemes")
def list_schemes(
    q: Optional[str] = Query(None, description="Search scheme by name or scheme_code"),
    amc_id: Optional[int] = Query(None),
    category: Optional[str] = Query(None),
    sub_category: Optional[str] = Query(None),
    plan: Optional[str] = Query(None),
    option: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(True),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    query = db.query(MfScheme)

    if is_active is not None:
        query = query.filter(MfScheme.is_active.is_(is_active))

    if amc_id is not None:
        query = query.filter(MfScheme.amc_id == amc_id)

    if category:
        query = query.filter(func.upper(MfScheme.category) == _norm(category).upper())

    if sub_category:
        query = query.filter(func.upper(MfScheme.sub_category) == _norm(sub_category).upper())

    if plan:
        query = query.filter(func.upper(MfScheme.plan) == _norm(plan).upper())

    if option:
        query = query.filter(func.upper(MfScheme.option) == _norm(option).upper())

    if q:
        qq = _norm(q)
        like = f"%{qq}%"
        # allow numeric scheme_code search too
        if qq.isdigit():
            query = query.filter(or_(MfScheme.scheme_code == int(qq), MfScheme.scheme_name.ilike(like)))
        else:
            query = query.filter(MfScheme.scheme_name.ilike(like))

    total = query.count()
    rows = query.order_by(MfScheme.scheme_name.asc()).offset(skip).limit(limit).all()

    return {
        "total": total,
        "limit": limit,
        "skip": skip,
        "items": [
            {
                "scheme_code": s.scheme_code,
                "scheme_name": s.scheme_name,
                "amc_id": s.amc_id,
                "category": _safe_getattr(s, "category", None),
                "sub_category": _safe_getattr(s, "sub_category", None),
                "plan": _safe_getattr(s, "plan", None),
                "option": _safe_getattr(s, "option", None),
                "is_active": _safe_getattr(s, "is_active", None),
                "updated_at": _safe_getattr(s, "updated_at", None),
            }
            for s in rows
        ],
    }


@router.get("/schemes/{scheme_code}")
def get_scheme(scheme_code: str, db: Session = Depends(get_db)):
    code = _scheme_code_int(scheme_code)

    s = db.query(MfScheme).filter(MfScheme.scheme_code == code).first()
    if not s:
        raise HTTPException(status_code=404, detail="Scheme not found")

    amc = db.query(MfAmc).filter(MfAmc.id == s.amc_id).first()

    # latest NAV via snapshot if exists else from nav table
    snap = db.query(MfSchemeSnapshot).filter(MfSchemeSnapshot.scheme_code == code).first()

    if snap and _safe_getattr(snap, "as_of_date", None) is not None:
        latest_nav_date = _safe_getattr(snap, "as_of_date", None)
        latest_nav = _safe_getattr(snap, "latest_nav", None)
    else:
        latest = (
            db.query(MfNavDaily.nav_date, MfNavDaily.nav)
            .filter(MfNavDaily.scheme_code == code)
            .order_by(MfNavDaily.nav_date.desc())
            .first()
        )
        latest_nav_date = latest[0] if latest else None
        latest_nav = float(latest[1]) if latest and latest[1] is not None else None

    return {
        "scheme": {
            "scheme_code": s.scheme_code,
            "scheme_name": s.scheme_name,
            "amc_id": s.amc_id,
            "category": _safe_getattr(s, "category", None),
            "sub_category": _safe_getattr(s, "sub_category", None),
            "plan": _safe_getattr(s, "plan", None),
            "option": _safe_getattr(s, "option", None),
            "is_active": _safe_getattr(s, "is_active", None),
            "updated_at": _safe_getattr(s, "updated_at", None),
        },
        "amc": None
        if not amc
        else {
            "id": amc.id,
            "name": amc.name,
            "logo_url": _safe_getattr(amc, "logo_url", None),
        },
        "latest": {
            "nav_date": latest_nav_date,
            "nav": latest_nav,
        },
        "snapshot": None
        if not snap
        else {
            "scheme_code": snap.scheme_code,
            "as_of_date": _safe_getattr(snap, "as_of_date", None),
            "latest_nav": float(_safe_getattr(snap, "latest_nav", None))
            if _safe_getattr(snap, "latest_nav", None) is not None
            else None,
            "updated_at": _safe_getattr(snap, "updated_at", None),
        },
    }


# -----------------------------
# 3) Snapshot APIs
# -----------------------------
@router.get("/schemes/{scheme_code}/snapshot")
def get_scheme_snapshot(scheme_code: str, db: Session = Depends(get_db)):
    code = _scheme_code_int(scheme_code)

    snap = db.query(MfSchemeSnapshot).filter(MfSchemeSnapshot.scheme_code == code).first()
    if not snap:
        raise HTTPException(status_code=404, detail="Snapshot not found for this scheme_code")

    return {
        "scheme_code": snap.scheme_code,
        "as_of_date": _safe_getattr(snap, "as_of_date", None),
        "latest_nav": float(_safe_getattr(snap, "latest_nav", None))
        if _safe_getattr(snap, "latest_nav", None) is not None
        else None,
        "updated_at": _safe_getattr(snap, "updated_at", None),
    }


# -----------------------------
# 4) NAV APIs (history for charts)
# -----------------------------
@router.get("/schemes/{scheme_code}/nav")
def nav_history_days(
    scheme_code: str,
    days: int = Query(30, ge=1, le=3650),
    limit: int = Query(2000, ge=10, le=20000),
    db: Session = Depends(get_db),
):
    code = _scheme_code_int(scheme_code)
    start_date = (datetime.utcnow().date() - timedelta(days=days))

    rows = (
        db.query(MfNavDaily.nav_date, MfNavDaily.nav)
        .filter(MfNavDaily.scheme_code == code)
        .filter(MfNavDaily.nav_date >= start_date)
        .order_by(MfNavDaily.nav_date.asc())
        .limit(limit)
        .all()
    )

    data = [{"nav_date": r[0], "nav": float(r[1]) if r[1] is not None else None} for r in rows]
    return {
        "scheme_code": code,
        "days": days,
        "count": len(data),
        "data": data,
    }


@router.get("/schemes/{scheme_code}/nav/range")
def nav_history_range(
    scheme_code: str,
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    limit: int = Query(20000, ge=10, le=50000),
    db: Session = Depends(get_db),
):
    code = _scheme_code_int(scheme_code)
    start_d = _to_date(start)
    end_d = _to_date(end)
    if end_d < start_d:
        raise HTTPException(status_code=400, detail="end must be >= start")

    rows = (
        db.query(MfNavDaily.nav_date, MfNavDaily.nav)
        .filter(MfNavDaily.scheme_code == code)
        .filter(MfNavDaily.nav_date >= start_d)
        .filter(MfNavDaily.nav_date <= end_d)
        .order_by(MfNavDaily.nav_date.asc())
        .limit(limit)
        .all()
    )

    data = [{"nav_date": r[0], "nav": float(r[1]) if r[1] is not None else None} for r in rows]
    return {
        "scheme_code": code,
        "start": start_d,
        "end": end_d,
        "count": len(data),
        "data": data,
    }


@router.get("/schemes/{scheme_code}/nav/latest")
def nav_latest(scheme_code: str, db: Session = Depends(get_db)):
    code = _scheme_code_int(scheme_code)

    latest = (
        db.query(MfNavDaily.nav_date, MfNavDaily.nav)
        .filter(MfNavDaily.scheme_code == code)
        .order_by(MfNavDaily.nav_date.desc())
        .first()
    )
    if not latest:
        raise HTTPException(status_code=404, detail="No NAV found for this scheme")

    return {"scheme_code": code, "nav_date": latest[0], "nav": float(latest[1]) if latest[1] is not None else None}


# -----------------------------
# 5) Filters APIs (for UI dropdowns)
# -----------------------------
@router.get("/filters")
def mf_filters(db: Session = Depends(get_db)):
    # Return distinct lists for dropdowns
    categories = [r[0] for r in db.query(MfScheme.category).filter(MfScheme.category.isnot(None)).distinct().all()]
    sub_categories = [
        r[0] for r in db.query(MfScheme.sub_category).filter(MfScheme.sub_category.isnot(None)).distinct().all()
    ]
    plans = [r[0] for r in db.query(MfScheme.plan).filter(MfScheme.plan.isnot(None)).distinct().all()]
    options = [r[0] for r in db.query(MfScheme.option).filter(MfScheme.option.isnot(None)).distinct().all()]

    # sort for UI
    categories = sorted({str(x).strip() for x in categories if str(x).strip()})
    sub_categories = sorted({str(x).strip() for x in sub_categories if str(x).strip()})
    plans = sorted({str(x).strip() for x in plans if str(x).strip()})
    options = sorted({str(x).strip() for x in options if str(x).strip()})

    return {
        "categories": categories,
        "sub_categories": sub_categories,
        "plans": plans,
        "options": options,
    }


# -----------------------------
# 6) Search API (single endpoint for UI search bar)
# -----------------------------
@router.get("/search")
def mf_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    qq = _norm(q)
    like = f"%{qq}%"

    amcs = (
        db.query(MfAmc)
        .filter(MfAmc.name.ilike(like))
        .order_by(MfAmc.name.asc())
        .limit(limit)
        .all()
    )

    schemes_query = db.query(MfScheme)
    if qq.isdigit():
        schemes_query = schemes_query.filter(or_(MfScheme.scheme_code == int(qq), MfScheme.scheme_name.ilike(like)))
    else:
        schemes_query = schemes_query.filter(MfScheme.scheme_name.ilike(like))

    schemes = schemes_query.order_by(MfScheme.scheme_name.asc()).limit(limit).all()

    return {
        "q": qq,
        "amcs": [
            {
                "id": a.id,
                "name": a.name,
                "logo_url": _safe_getattr(a, "logo_url", None),
            }
            for a in amcs
        ],
        "schemes": [
            {
                "scheme_code": s.scheme_code,
                "scheme_name": s.scheme_name,
                "amc_id": s.amc_id,
                "category": _safe_getattr(s, "category", None),
                "sub_category": _safe_getattr(s, "sub_category", None),
                "plan": _safe_getattr(s, "plan", None),
                "option": _safe_getattr(s, "option", None),
                "is_active": _safe_getattr(s, "is_active", None),
            }
            for s in schemes
        ],
    }


# -----------------------------
# 7) Job Logs (admin UI)
# -----------------------------
@router.get("/jobs")
def mf_jobs(
    limit: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db),
):
    # safest ordering: by id desc (since some db had created_at mismatch earlier)
    rows = db.query(MfJobLog).order_by(MfJobLog.id.desc()).limit(limit).all()

    out = []
    for j in rows:
        out.append(
            {
                "id": j.id,
                "job_name": _safe_getattr(j, "job_name", None),
                "status": _safe_getattr(j, "status", None),
                "message": _safe_getattr(j, "message", None),
                "inserted": _safe_getattr(j, "inserted", None),
                "updated": _safe_getattr(j, "updated", None),
                "skipped": _safe_getattr(j, "skipped", None),
                "created_at": _safe_getattr(j, "created_at", None),
                "updated_at": _safe_getattr(j, "updated_at", None),
            }
        )

    return {"count": len(out), "items": out}
