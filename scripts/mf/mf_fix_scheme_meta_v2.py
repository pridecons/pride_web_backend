# scripts/mf/mf_fix_scheme_meta_v2.py
import re
import time
import requests
from sqlalchemy.orm import Session
from sqlalchemy import func

from db.connection import SessionLocal
from db.models import MfScheme

MFAPI_URL = "https://api.mfapi.in/mf/{scheme_code}"

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def upper(s: str) -> str:
    return norm(s).upper()

# ---------------------------
# Better PLAN inference
# ---------------------------
def infer_plan(name: str) -> str:
    n = upper(name)

    if re.search(r"\bDIRECT\b", n) or "DIRECT PLAN" in n:
        return "DIRECT"
    if re.search(r"\bREGULAR\b", n) or "REGULAR PLAN" in n:
        return "REGULAR"

    # Many AMFI lines use " - Direct -" " - Regular -"
    if re.search(r"\-\s*DIRECT\s*\-", n):
        return "DIRECT"
    if re.search(r"\-\s*REGULAR\s*\-", n):
        return "REGULAR"

    # Retail/Institutional are NOT plan types (keep UNKNOWN)
    return "UNKNOWN"


# ---------------------------
# Better OPTION inference
# ---------------------------
def infer_option(name: str) -> str:
    n = upper(name)

    # Growth
    if re.search(r"\bGROWTH\b", n) or "GROWTH OPTION" in n:
        return "GROWTH"

    # Bonus
    if "BONUS" in n:
        return "BONUS"

    # IDCW (explicit)
    if "IDCW" in n:
        return "IDCW"

    # IDCW full form (this is the main missing)
    if "INCOME DISTRIBUTION" in n and "CAPITAL WITHDRAWAL" in n:
        return "IDCW"

    # Dividend synonyms
    if re.search(r"\bDIVIDEND\b", n) or "DIV OPTION" in n or "DIV. OPTION" in n:
        return "IDCW"

    # payout/reinvest are typically dividend/IDCW
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
    return "UNKNOWN"


def fetch_mfapi_meta(session: requests.Session, scheme_code: int, tries: int = 3, timeout: int = 20):
    url = MFAPI_URL.format(scheme_code=scheme_code)
    last = None
    for t in range(tries):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            js = r.json()
            return (js or {}).get("meta")
        except Exception as e:
            last = e
            time.sleep(0.6 * (t + 1))
    return None


def run(limit: int = 200000, fill_subcat_from_mfapi: bool = True, sleep_sec: float = 0.02):
    db: Session = SessionLocal()
    try:
        # Candidates: any missing/unknown plan/option/sub_category
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
        print(f"🔧 Fix v2: candidates={total}")

        session = None
        if fill_subcat_from_mfapi:
            session = requests.Session()
            session.headers.update({"User-Agent": "pride-mf-fix/2.0"})

        fixed = 0
        subcat_filled = 0

        for i, s in enumerate(rows, start=1):
            name = s.scheme_name or ""

            # plan
            if not s.plan or s.plan == "UNKNOWN":
                s.plan = infer_plan(name)

            # option
            if not s.option or s.option == "UNKNOWN":
                s.option = infer_option(name)

            # frequency/payout_type only if columns exist in your table
            if hasattr(s, "frequency") and (not s.frequency or s.frequency in ["", "UNKNOWN", None]):
                s.frequency = infer_frequency(name, s.option)

            if hasattr(s, "payout_type") and (not s.payout_type or s.payout_type in ["", "UNKNOWN", None]):
                s.payout_type = infer_payout_type(name, s.option)

            # sub_category from MFAPI meta (best source for missing sub_category)
            if fill_subcat_from_mfapi and (s.sub_category is None or s.sub_category == ""):
                meta = fetch_mfapi_meta(session, int(s.scheme_code))
                if meta:
                    mfapi_cat = (meta.get("scheme_category") or "").strip()
                    # mfapi_cat example: "Money Market Fund", "Large Cap Fund", ...
                    if mfapi_cat:
                        s.sub_category = mfapi_cat
                        subcat_filled += 1
                time.sleep(sleep_sec)

            fixed += 1

            if i % 500 == 0:
                db.commit()
                print(f"✅ processed {i}/{total} | subcat_filled={subcat_filled}")

        db.commit()
        print(f"\n✅ DONE. fixed_rows={fixed} | subcat_filled={subcat_filled}")

        # summary
        miss_sub = db.query(func.count()).select_from(MfScheme).filter(
            (MfScheme.sub_category.is_(None)) | (MfScheme.sub_category == "")
        ).scalar() or 0

        miss_plan = db.query(func.count()).select_from(MfScheme).filter(
            (MfScheme.plan.is_(None)) | (MfScheme.plan == "") | (MfScheme.plan == "UNKNOWN")
        ).scalar() or 0

        miss_opt = db.query(func.count()).select_from(MfScheme).filter(
            (MfScheme.option.is_(None)) | (MfScheme.option == "") | (MfScheme.option == "UNKNOWN")
        ).scalar() or 0

        print("\n📊 Remaining Missing After Fix v2")
        print(f"  sub_category missing: {miss_sub}")
        print(f"  plan missing/UNKNOWN: {miss_plan}")
        print(f"  option missing/UNKNOWN: {miss_opt}")

        print("\nℹ️ Note:")
        print("  plan UNKNOWN ka major reason: scheme_name me DIRECT/REGULAR mention nahi hota.")
        print("  (Retail/Institutional plan nahi hote)")

    finally:
        db.close()


if __name__ == "__main__":
    run(fill_subcat_from_mfapi=True, sleep_sec=0.02)
