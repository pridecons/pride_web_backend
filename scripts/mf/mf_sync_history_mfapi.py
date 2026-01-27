# scripts/mf/mf_sync_history_mfapi_optimized.py
import time
import requests
from datetime import datetime, date
from typing import Optional, Dict, Any, List, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from db.connection import SessionLocal
from db.models import MfJobLog

MFAPI_URL = "https://api.mfapi.in/mf/{scheme_code}"


# -----------------------------
# Helpers
# -----------------------------
def parse_mfapi_date(s: str):
    # "07-01-2026" => dd-mm-yyyy
    return datetime.strptime(s.strip(), "%d-%m-%Y").date()


def log_job(db: Session, job_name: str, status: str, message: str = None,
            inserted: int = None, updated: int = None, skipped: int = None):
    db.add(MfJobLog(
        job_name=job_name,
        status=status,
        message=message,
        inserted=inserted,
        updated=updated,
        skipped=skipped,
    ))


def fetch_json(session: requests.Session, scheme_code: int, tries: int = 3, timeout: int = 25) -> Optional[dict]:
    url = MFAPI_URL.format(scheme_code=scheme_code)
    last = None
    for t in range(tries):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(0.8 * (t + 1))
    raise last


def get_scheme_max_dates(db: Session) -> List[Tuple[int, Optional[date]]]:
    """
    One query to get scheme_code and max(nav_date) for all active schemes.
    """
    rows = db.execute(text("""
        SELECT s.scheme_code, MAX(n.nav_date) AS max_date
        FROM mf_scheme s
        LEFT JOIN mf_nav_daily n ON n.scheme_code = s.scheme_code
        WHERE s.is_active = true
        GROUP BY s.scheme_code
        ORDER BY s.scheme_code
    """)).fetchall()
    return [(int(r[0]), r[1]) for r in rows]


def bulk_upsert_nav(db: Session, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    db.execute(text("""
        INSERT INTO mf_nav_daily (scheme_code, nav_date, nav, created_at, updated_at)
        VALUES (:scheme_code, :nav_date, :nav, now(), now())
        ON CONFLICT (scheme_code, nav_date) DO UPDATE SET
          nav = EXCLUDED.nav,
          updated_at = now()
    """), rows)
    return len(rows)


# -----------------------------
# Main optimized sync
# -----------------------------
def run_history_sync_optimized(
    target_date: Optional[date] = None,
    per_scheme_limit: int = 120,          # daily mode: 60-200 good; full backfill: 8000
    sleep_sec: float = 0.03,
    commit_every: int = 50,
    print_every: int = 25,
):
    """
    - target_date: if scheme max_date >= target_date => skip MFAPI call
      default: today (UTC date)
    - per_scheme_limit: MFAPI data is latest-first; we only read first N items
      For daily updates: 120 is good. For full backfill: 8000.
    """
    db: Session = SessionLocal()
    started = datetime.now()

    # Counters
    total_schemes = 0
    outdated_schemes = 0
    schemes_done = 0
    schemes_skipped = 0
    schemes_failed = 0
    nav_written = 0

    if target_date is None:
        target_date = datetime.utcnow().date()

    try:
        print("🔎 Loading scheme max(nav_date) in ONE query ...")
        scheme_max_dates = get_scheme_max_dates(db)
        total_schemes = len(scheme_max_dates)

        # Filter only outdated schemes
        todo = []
        for scheme_code, max_date in scheme_max_dates:
            if max_date is None or max_date < target_date:
                todo.append((scheme_code, max_date))
            else:
                schemes_skipped += 1

        outdated_schemes = len(todo)

        print(
            f"🚀 OPTIMIZED Historical NAV Sync started\n"
            f"   total_schemes={total_schemes}\n"
            f"   target_date={target_date}\n"
            f"   outdated_schemes={outdated_schemes}\n"
            f"   per_scheme_limit={per_scheme_limit}\n"
            f"   sleep={sleep_sec}s"
        )

        if outdated_schemes == 0:
            msg = f"All schemes already up-to-date for target_date={target_date}. Nothing to do."
            log_job(db, "MF_MFAPI_HISTORY_SYNC_OPT", "SUCCESS", message=msg, inserted=0, skipped=schemes_skipped)
            db.commit()
            print("✅", msg)
            return

        session = requests.Session()
        session.headers.update({"User-Agent": "pride-mf-sync/1.0"})

        for idx, (scheme_code, max_date) in enumerate(todo, start=1):
            try:
                payload = fetch_json(session, scheme_code)
                if not payload or "data" not in payload:
                    schemes_failed += 1
                    continue

                data = payload.get("data") or []
                if per_scheme_limit:
                    data = data[:per_scheme_limit]

                to_write = []
                for item in data:
                    nav_date = parse_mfapi_date(item["date"])

                    # MFAPI is latest-first; stop once we hit already-known dates
                    if max_date and nav_date <= max_date:
                        break

                    try:
                        nav_val = float(item["nav"])
                    except Exception:
                        continue

                    to_write.append({
                        "scheme_code": scheme_code,
                        "nav_date": nav_date,
                        "nav": nav_val
                    })

                written = bulk_upsert_nav(db, to_write)
                nav_written += written
                schemes_done += 1

                if idx % print_every == 0 or idx == outdated_schemes:
                    elapsed = (datetime.now() - started).total_seconds()
                    print(
                        f"📌 {idx}/{outdated_schemes} outdated schemes | "
                        f"done={schemes_done} fail={schemes_failed} | "
                        f"nav_written={nav_written} | elapsed={elapsed:.1f}s"
                    )

                if schemes_done % commit_every == 0:
                    db.commit()

                time.sleep(sleep_sec)

            except Exception as e:
                schemes_failed += 1
                db.rollback()
                print(f"⚠️ scheme_code={scheme_code} failed: {e}")
                time.sleep(0.2)

        db.commit()

        msg = (
            f"total_schemes={total_schemes}, target_date={target_date}, "
            f"outdated={outdated_schemes}, done={schemes_done}, failed={schemes_failed}, "
            f"already_uptodate_skipped={schemes_skipped}, nav_written={nav_written}"
        )
        log_job(db, "MF_MFAPI_HISTORY_SYNC_OPT", "SUCCESS", message=msg, inserted=nav_written, skipped=schemes_skipped)
        db.commit()

        print("\n✅ OPTIMIZED HISTORICAL SYNC COMPLETED")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(msg)
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    except Exception as e:
        db.rollback()
        try:
            log_job(db, "MF_MFAPI_HISTORY_SYNC_OPT", "FAILED", message=str(e))
            db.commit()
        except Exception:
            pass
        print(f"❌ FAILED: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    # ✅ DAILY MODE:
    # - only recent data read (120)
    # - skips up-to-date schemes automatically
    run_history_sync_optimized(
        target_date=datetime.utcnow().date(),
        per_scheme_limit=120,
        sleep_sec=0.03,
        commit_every=50,
        print_every=25,
    )

    # ✅ FULL BACKFILL MODE (run once if you want full history):
    # run_history_sync_optimized(
    #     target_date=date(1900, 1, 1),  # force all schemes to be considered outdated
    #     per_scheme_limit=8000,
    #     sleep_sec=0.05,
    #     commit_every=20,
    #     print_every=10,
    # )
