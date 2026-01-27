# scripts/mf_sync_amfi.py
import re
import requests
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from db.connection import SessionLocal  # <-- aapke project me usually ye hota hai
from db.models import MfAmc, MfScheme, MfNavDaily, MfSchemeSnapshot, MfJobLog


AMFI_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"


# -----------------------------
# Helpers
# -----------------------------
def norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def norm_upper(s: str) -> str:
    return norm(s).upper()

def parse_amfi_date(s: str):
    # "07-Jan-2026"
    return datetime.strptime(s.strip(), "%d-%b-%Y").date()

def parse_scheme_name_parts(scheme_name: str):
    """
    Extract DIRECT/REGULAR, GROWTH/IDCW/BONUS, MONTHLY/WEEKLY/DAILY/QUARTERLY, PAYOUT/REINVESTMENT
    from scheme_name text.
    """
    n = norm_upper(scheme_name)

    # Plan
    if "DIRECT" in n:
        plan = "DIRECT"
    elif "REGULAR" in n:
        plan = "REGULAR"
    else:
        plan = "UNKNOWN"

    # Option
    if "GROWTH" in n:
        option = "GROWTH"
    elif "BONUS" in n:
        option = "BONUS"
    elif "IDCW" in n or "DIVIDEND" in n:
        option = "IDCW"
    else:
        option = "UNKNOWN"

    # Frequency
    frequency = "NONE"
    if option == "IDCW":
        for f in ["DAILY", "WEEKLY", "MONTHLY", "QUARTERLY", "ANNUAL", "YEARLY"]:
            if f in n:
                frequency = "ANNUAL" if f == "YEARLY" else f
                break

    # Payout type
    if "REINVEST" in n:
        payout_type = "REINVESTMENT"
    elif "PAYOUT" in n:
        payout_type = "PAYOUT"
    else:
        payout_type = "UNKNOWN"

    # Cap bucket (only if equity)
    # NOTE: category/sub_category AMFI headings se aayega. Yaha fallback.
    cap_bucket = None
    if "LARGE CAP" in n:
        cap_bucket = "LARGE"
    elif "MID CAP" in n:
        cap_bucket = "MID"
    elif "SMALL CAP" in n:
        cap_bucket = "SMALL"
    elif "FLEXI CAP" in n:
        cap_bucket = "FLEXI"
    elif "MULTI CAP" in n:
        cap_bucket = "MULTI"

    return plan, option, frequency, payout_type, cap_bucket


def parse_heading_category(line: str):
    """
    Example: "Open Ended Schemes(Debt Scheme - Banking and PSU Fund)"
    returns scheme_type="Open Ended Schemes", sub_category="Debt Scheme - Banking and PSU Fund"
    """
    m = re.match(r"^(.*?)\((.*?)\)\s*$", line.strip())
    if not m:
        return None, None
    return norm(m.group(1)), norm(m.group(2))


def is_data_row(line: str) -> bool:
    # data row starts with digits and has ';'
    s = line.strip()
    return bool(s) and s[0].isdigit() and s.count(";") >= 5


# -----------------------------
# Core: Fetch + Parse
# -----------------------------
def fetch_amfi_text() -> str:
    res = requests.get(AMFI_URL, timeout=30)
    res.raise_for_status()
    return res.text


def parse_amfi_navall(text_data: str):
    """
    Returns:
      amc_map: {amc_name: {id etc}} (created later)
      rows: list of dicts with scheme_code, isin_growth, isin_div_reinvestment, scheme_name, nav, nav_date,
            fund_house, scheme_type, category, sub_category
    """
    lines = text_data.splitlines()

    current_scheme_type = None      # "Open Ended Schemes"
    current_scheme_category = None  # "Debt Scheme - Banking and PSU Fund"
    current_amc = None              # "Axis Mutual Fund" etc.

    out = []

    for raw in lines:
        line = norm(raw)
        if not line:
            continue

        # Skip header line
        if line.startswith("Scheme Code;"):
            continue

        # Heading: "Open Ended Schemes(Debt Scheme - Banking and PSU Fund)"
        if "(" in line and ")" in line and not ";" in line and "Schemes" in line:
            st, sc = parse_heading_category(line)
            if st:
                current_scheme_type = st
                current_scheme_category = sc
            continue

        # AMC name lines typically end with "Mutual Fund"
        # ex: "Axis Mutual Fund"
        if not ";" in line and line.lower().endswith("mutual fund"):
            current_amc = line
            continue

        # Data row
        if is_data_row(line):
            parts = line.split(";")
            scheme_code = int(parts[0].strip())
            isin_growth = parts[1].strip() if parts[1].strip() != "-" else None
            isin_div_reinv = parts[2].strip() if parts[2].strip() != "-" else None
            scheme_name = parts[3].strip()
            nav = parts[4].strip()
            nav_date = parts[5].strip()

            try:
                nav_val = float(nav)
            except Exception:
                # if NAV missing/invalid, skip
                continue

            try:
                nav_dt = parse_amfi_date(nav_date)
            except Exception:
                continue

            out.append({
                "scheme_code": scheme_code,
                "isin_growth": isin_growth,
                "isin_div_reinvestment": isin_div_reinv,
                "scheme_name": scheme_name,
                "nav": nav_val,
                "nav_date": nav_dt,
                "fund_house": current_amc,
                "scheme_type": current_scheme_type,
                # We'll store category/sub_category as TEXT
                # We can split current_scheme_category -> "Debt Scheme - Banking and PSU Fund"
                # category="Debt Scheme", sub_category="Banking and PSU Fund"
                "scheme_category_raw": current_scheme_category,
            })

    return out


def split_category_subcategory(scheme_category_raw: str):
    """
    "Debt Scheme - Banking and PSU Fund"
      -> category="Debt Scheme"
      -> sub_category="Banking and PSU Fund"
    If no '-', then category=raw, sub_category=None
    """
    if not scheme_category_raw:
        return None, None
    raw = norm(scheme_category_raw)
    if " - " in raw:
        a, b = raw.split(" - ", 1)
        return norm(a), norm(b)
    return raw, None


# -----------------------------
# DB Upsert helpers
# -----------------------------
def get_or_create_amc(db: Session, amc_name: str) -> MfAmc:
    amc_name = norm(amc_name)
    obj = db.query(MfAmc).filter(MfAmc.name == amc_name).first()
    if obj:
        return obj
    obj = MfAmc(name=amc_name)
    db.add(obj)
    db.flush()  # to get obj.id
    return obj


def upsert_scheme(db: Session, amc_id: int, row: dict) -> bool:
    """
    Returns: True if INSERT happened, False if UPDATE happened.
    """
    scheme_code = row["scheme_code"]
    scheme_name = row["scheme_name"]

    scheme_type = row.get("scheme_type")
    cat_raw = row.get("scheme_category_raw")
    category, sub_category = split_category_subcategory(cat_raw)

    plan, option, frequency, payout_type, cap_bucket_from_name = parse_scheme_name_parts(scheme_name)

    cap_bucket = None
    if category and category.lower().startswith("equity"):
        sc = (sub_category or "").upper()
        if "LARGE CAP" in sc:
            cap_bucket = "LARGE"
        elif "MID CAP" in sc:
            cap_bucket = "MID"
        elif "SMALL CAP" in sc:
            cap_bucket = "SMALL"
        elif "FLEXI CAP" in sc:
            cap_bucket = "FLEXI"
        elif "MULTI CAP" in sc:
            cap_bucket = "MULTI"
        elif "LARGE & MID" in sc:
            cap_bucket = "LARGE_MID"
        else:
            cap_bucket = cap_bucket_from_name or "OTHER"
    else:
        cap_bucket = "NOT_APPLICABLE"

    # RETURNING (xmax = 0) => True means inserted
    q = text("""
        INSERT INTO mf_scheme (
          scheme_code, amc_id, scheme_name, scheme_type, category, sub_category,
          plan, option, frequency, payout_type, cap_bucket,
          isin_growth, isin_div_reinvestment, is_active,
          created_at, updated_at
        )
        VALUES (
          :scheme_code, :amc_id, :scheme_name, :scheme_type, :category, :sub_category,
          :plan, :option, :frequency, :payout_type, :cap_bucket,
          :isin_growth, :isin_div_reinvestment, true,
          now(), now()
        )
        ON CONFLICT (scheme_code) DO UPDATE SET
          amc_id = EXCLUDED.amc_id,
          scheme_name = EXCLUDED.scheme_name,
          scheme_type = EXCLUDED.scheme_type,
          category = EXCLUDED.category,
          sub_category = EXCLUDED.sub_category,
          plan = EXCLUDED.plan,
          option = EXCLUDED.option,
          frequency = EXCLUDED.frequency,
          payout_type = EXCLUDED.payout_type,
          cap_bucket = EXCLUDED.cap_bucket,
          isin_growth = EXCLUDED.isin_growth,
          isin_div_reinvestment = EXCLUDED.isin_div_reinvestment,
          is_active = true,
          updated_at = now()
        RETURNING (xmax = 0) AS inserted
    """)

    inserted_flag = db.execute(q, {
        "scheme_code": scheme_code,
        "amc_id": amc_id,
        "scheme_name": scheme_name,
        "scheme_type": scheme_type,
        "category": category,
        "sub_category": sub_category,
        "plan": plan,
        "option": option,
        "frequency": frequency,
        "payout_type": payout_type,
        "cap_bucket": cap_bucket,
        "isin_growth": row.get("isin_growth"),
        "isin_div_reinvestment": row.get("isin_div_reinvestment"),
    }).scalar()

    return bool(inserted_flag)


def insert_nav(db: Session, scheme_code: int, nav_date, nav_val: float) -> bool:
    """
    Returns: True if INSERT, False if UPDATE
    """
    q = text("""
        INSERT INTO mf_nav_daily (scheme_code, nav_date, nav, created_at, updated_at)
        VALUES (:scheme_code, :nav_date, :nav, now(), now())
        ON CONFLICT (scheme_code, nav_date) DO UPDATE SET
          nav = EXCLUDED.nav,
          updated_at = now()
        RETURNING (xmax = 0) AS inserted
    """)
    inserted_flag = db.execute(q, {
        "scheme_code": scheme_code,
        "nav_date": nav_date,
        "nav": nav_val,
    }).scalar()
    return bool(inserted_flag)


def upsert_snapshot_from_latest_nav(db: Session, scheme_code: int):
    """
    Snapshot update using latest NAV from nav_daily.
    (returns/aum/expense later fill kar sakte ho)
    """
    db.execute(text("""
        INSERT INTO mf_scheme_snapshot (scheme_code, as_of_date, latest_nav, updated_at, created_at)
        SELECT
          n.scheme_code,
          n.nav_date as as_of_date,
          n.nav as latest_nav,
          now() as updated_at,
          now() as created_at
        FROM mf_nav_daily n
        WHERE n.scheme_code = :scheme_code
        ORDER BY n.nav_date DESC
        LIMIT 1
        ON CONFLICT (scheme_code) DO UPDATE SET
          as_of_date = EXCLUDED.as_of_date,
          latest_nav = EXCLUDED.latest_nav,
          updated_at = now()
    """), {"scheme_code": scheme_code})


def log_job(db: Session, job_name: str, status: str, message: str = None,
            inserted: int = None, updated: int = None, skipped: int = None):
    j = MfJobLog(
        job_name=job_name,
        status=status,
        message=message,
        inserted=inserted,
        updated=updated,
        skipped=skipped,
    )
    db.add(j)


# -----------------------------
# Main runner
# -----------------------------
def run_amfi_sync(batch_print_every: int = 500):
    db: Session = SessionLocal()

    # Counters
    total_rows = 0

    amc_new = 0
    scheme_inserted = 0
    scheme_updated = 0

    nav_inserted = 0
    nav_updated = 0

    snapshot_updated = 0
    skipped = 0

    # For speed: cache AMC name -> id
    amc_cache = {}

    started = datetime.now()

    try:
        print("⬇️ Downloading AMFI NAVAll.txt ...")
        txt = fetch_amfi_text()
        print(f"✅ Downloaded bytes: {len(txt)}")

        print("🧩 Parsing AMFI file ...")
        rows = parse_amfi_navall(txt)
        total_rows = len(rows)
        print(f"✅ Parsed rows: {total_rows}")

        if total_rows == 0:
            log_job(db, "MF_AMFI_NAV_SYNC", "FAILED", message="No rows parsed from AMFI NAVAll.txt")
            db.commit()
            print("❌ No rows parsed. Exiting.")
            return

        print("🚀 DB sync started ...")

        for i, r in enumerate(rows, start=1):
            amc_name = (r.get("fund_house") or "UNKNOWN").strip()

            # AMC lookup/create with cache
            amc = amc_cache.get(amc_name)
            if amc is None:
                amc_obj = db.query(MfAmc).filter(MfAmc.name == amc_name).first()
                if not amc_obj:
                    amc_obj = MfAmc(name=amc_name)
                    db.add(amc_obj)
                    db.flush()
                    amc_new += 1
                    # small print for first few only
                    if amc_new <= 10:
                        print(f"➕ New AMC: {amc_name}")
                amc_cache[amc_name] = amc_obj.id
                amc = amc_obj.id

            # Scheme upsert
            scheme_is_insert = upsert_scheme(db, amc, r)
            if scheme_is_insert:
                scheme_inserted += 1
            else:
                scheme_updated += 1

            # NAV upsert
            nav_is_insert = insert_nav(db, r["scheme_code"], r["nav_date"], r["nav"])
            if nav_is_insert:
                nav_inserted += 1
            else:
                nav_updated += 1

            # Snapshot
            upsert_snapshot_from_latest_nav(db, r["scheme_code"])
            snapshot_updated += 1

            # Progress prints
            if i % batch_print_every == 0 or i == total_rows:
                elapsed = (datetime.now() - started).total_seconds()
                print(
                    f"📌 Progress {i}/{total_rows} | "
                    f"AMC new={amc_new} | "
                    f"scheme ins/upd={scheme_inserted}/{scheme_updated} | "
                    f"NAV ins/upd={nav_inserted}/{nav_updated} | "
                    f"snapshot={snapshot_updated} | "
                    f"elapsed={elapsed:.1f}s"
                )

                # optional: commit in chunks (safer on huge data)
                db.commit()

        # Final commit (if not already)
        db.commit()

        msg = (
            f"Processed={total_rows}, AMC_new={amc_new}, "
            f"scheme_inserted={scheme_inserted}, scheme_updated={scheme_updated}, "
            f"nav_inserted={nav_inserted}, nav_updated={nav_updated}, "
            f"snapshot_updated={snapshot_updated}, skipped={skipped}"
        )
        log_job(
            db,
            "MF_AMFI_NAV_SYNC",
            "SUCCESS",
            message=msg,
            inserted=(scheme_inserted + nav_inserted),
            updated=(scheme_updated + nav_updated),
            skipped=skipped,
        )
        db.commit()

        print("\n✅ SYNC COMPLETED")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(msg)
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    except Exception as e:
        db.rollback()
        try:
            log_job(db, "MF_AMFI_NAV_SYNC", "FAILED", message=str(e))
            db.commit()
        except Exception:
            pass
        print(f"❌ FAILED: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    run_amfi_sync()
