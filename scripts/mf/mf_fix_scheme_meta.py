# scripts/mf/mf_fix_scheme_meta.py
import re
from sqlalchemy.orm import Session
from sqlalchemy import func

from db.connection import SessionLocal
from db.models import MfScheme

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def upper(s: str) -> str:
    return norm(s).upper()

def infer_plan(name: str) -> str:
    n = upper(name)
    # explicit DIRECT/REGULAR
    if re.search(r"\bDIRECT\b", n):
        return "DIRECT"
    if re.search(r"\bREGULAR\b", n):
        return "REGULAR"
    # some funds write "Direct Plan" / "Regular Plan"
    if "DIRECT PLAN" in n:
        return "DIRECT"
    if "REGULAR PLAN" in n:
        return "REGULAR"
    return "UNKNOWN"

def infer_option(name: str) -> str:
    n = upper(name)

    # explicit
    if re.search(r"\bGROWTH\b", n):
        return "GROWTH"
    if "BONUS" in n:
        return "BONUS"

    # IDCW / DIVIDEND variants
    if "IDCW" in n:
        return "IDCW"

    # many AMFI lines still use "DIV" or "DIVIDEND" or "DIV OPTION"
    if re.search(r"\bDIVIDEND\b", n) or re.search(r"\bDIV\b", n) or "DIV OPTION" in n or "DIV. OPTION" in n:
        return "IDCW"

    # "Dividend Payout" / "Dividend Reinvestment"
    if "PAYOUT" in n or "REINVEST" in n or "REINVESTMENT" in n:
        return "IDCW"

    return "UNKNOWN"

def infer_frequency(name: str, option: str) -> str:
    if option != "IDCW":
        return "NONE"
    n = upper(name)
    for f in ["DAILY", "WEEKLY", "MONTHLY", "QUARTERLY", "ANNUAL", "YEARLY", "HALF YEARLY", "SEMI ANNUAL"]:
        if f in n:
            if f == "YEARLY":
                return "ANNUAL"
            if f in ["HALF YEARLY", "SEMI ANNUAL"]:
                return "SEMI_ANNUAL"
            return f
    return "NONE"

def infer_payout_type(name: str, option: str) -> str:
    if option != "IDCW":
        return "NONE"
    n = upper(name)
    if "REINVEST" in n or "REINVESTMENT" in n:
        return "REINVESTMENT"
    if "PAYOUT" in n:
        return "PAYOUT"
    # if just dividend/idcw but no payout/reinvest mentioned
    return "UNKNOWN"

def split_category_subcategory(raw: str):
    raw = norm(raw)
    if not raw:
        return None, None
    if " - " in raw:
        a, b = raw.split(" - ", 1)
        return norm(a), norm(b)
    return raw, None

def run(limit: int = 200000):
    db: Session = SessionLocal()
    try:
        q = (
            db.query(MfScheme)
            .filter(
                (MfScheme.sub_category.is_(None)) | (MfScheme.sub_category == "") |
                (MfScheme.plan.is_(None)) | (MfScheme.plan == "") | (MfScheme.plan == "UNKNOWN") |
                (MfScheme.option.is_(None)) | (MfScheme.option == "") | (MfScheme.option == "UNKNOWN")
            )
            .order_by(MfScheme.scheme_code.asc())
            .limit(limit)
        )

        rows = q.all()
        total = len(rows)
        print(f"🔧 Fixing scheme metadata: candidates={total}")

        fixed = 0
        for i, s in enumerate(rows, start=1):
            name = s.scheme_name or ""

            # fill plan/option/frequency/payout if missing/unknown
            if not s.plan or s.plan == "UNKNOWN":
                s.plan = infer_plan(name)

            if not s.option or s.option == "UNKNOWN":
                s.option = infer_option(name)

            # frequency/payout only meaningful for IDCW
            if not getattr(s, "frequency", None) or s.frequency in [None, "", "UNKNOWN"]:
                s.frequency = infer_frequency(name, s.option)

            if not getattr(s, "payout_type", None) or s.payout_type in [None, "", "UNKNOWN"]:
                s.payout_type = infer_payout_type(name, s.option)

            # sub_category: if missing, try derive from scheme_category_raw if you store it,
            # BUT aapne mf_scheme me already category/sub_category text store kiya.
            # so here we only try: if sub_category empty but category has " - " joined mistakenly.
            if (s.sub_category is None or s.sub_category == "") and s.category:
                # sometimes raw "Debt Scheme - Banking and PSU Fund" might be in category
                cat, sub = split_category_subcategory(s.category)
                if sub and (not s.sub_category):
                    s.category = cat
                    s.sub_category = sub

            fixed += 1
            if i % 500 == 0:
                db.commit()
                print(f"✅ processed {i}/{total}")

        db.commit()
        print(f"\n✅ DONE. fixed_rows={fixed}")

        # quick summary counts after fix
        miss_sub = db.query(func.count()).select_from(MfScheme).filter(
            (MfScheme.sub_category.is_(None)) | (MfScheme.sub_category == "")
        ).scalar() or 0

        miss_plan = db.query(func.count()).select_from(MfScheme).filter(
            (MfScheme.plan.is_(None)) | (MfScheme.plan == "") | (MfScheme.plan == "UNKNOWN")
        ).scalar() or 0

        miss_opt = db.query(func.count()).select_from(MfScheme).filter(
            (MfScheme.option.is_(None)) | (MfScheme.option == "") | (MfScheme.option == "UNKNOWN")
        ).scalar() or 0

        print("\n📊 After Fix (remaining missing)")
        print(f"  sub_category missing: {miss_sub}")
        print(f"  plan missing/UNKNOWN: {miss_plan}")
        print(f"  option missing/UNKNOWN: {miss_opt}")

    finally:
        db.close()

if __name__ == "__main__":
    run()
