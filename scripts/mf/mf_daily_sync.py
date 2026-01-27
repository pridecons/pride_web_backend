# # scripts/mf/mf_daily_sync.py
# # ✅ Daily MF sync (schemes + NAV + snapshots) with:
# # - Clear prints/logs (what is running, what data is coming)
# # - NAV insert is SAFE (ON CONFLICT DO NOTHING)
# # - Snapshot step fixed for SQLAlchemy 2.0 (text(...) required)
# # - Reasonable progress printing
# # - Job log saved in mf_job_log

# import time
# import requests
# from datetime import datetime, date, timedelta
# from typing import Dict, Any, Optional, Tuple, List

# from sqlalchemy.orm import Session
# from sqlalchemy import func, text
# from sqlalchemy.dialects.postgresql import insert as pg_insert

# from db.connection import SessionLocal
# from db.models import MfAmc, MfScheme, MfNavDaily, MfSchemeSnapshot, MfJobLog

# MFAPI_ALL_SCHEMES_URL = "https://api.mfapi.in/mf"
# MFAPI_SCHEME_HISTORY_URL = "https://api.mfapi.in/mf/{id}"

# PRINT_EVERY = 200          # progress print
# NAV_PRINT_SAMPLES = 10     # show first N nav inserts
# FAIL_PRINT_SAMPLES = 10    # show first N failures


# # -----------------------------
# # Utilities
# # -----------------------------
# def now_utc() -> datetime:
#     return datetime.utcnow()


# def _log(msg: str):
#     print(f"[{now_utc().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# def _http_get(url: str, retries: int = 3, sleep_s: float = 1.0) -> Any:
#     last_err = None
#     for i in range(retries):
#         try:
#             _log(f"HTTP GET -> {url} (try {i+1}/{retries})")
#             r = requests.get(url, timeout=30)
#             r.raise_for_status()
#             return r.json()
#         except Exception as e:
#             last_err = e
#             _log(f"HTTP ERROR: {type(e).__name__}: {e}")
#             time.sleep(sleep_s * (i + 1))
#     raise last_err


# def _parse_nav_date(d: str) -> Optional[date]:
#     try:
#         # mfapi returns "07-01-2026"
#         return datetime.strptime(d, "%d-%m-%Y").date()
#     except Exception:
#         return None


# def _to_float(x) -> Optional[float]:
#     try:
#         return float(x)
#     except Exception:
#         return None


# def _infer_plan_option(name: str) -> Tuple[str, str]:
#     n = (name or "").upper()
#     plan = "DIRECT" if "DIRECT" in n else ("REGULAR" if "REGULAR" in n else "OTHER")

#     if "GROWTH" in n:
#         opt = "GROWTH"
#     elif "IDCW" in n or "DIVIDEND" in n:
#         opt = "IDCW"
#     elif "BONUS" in n:
#         opt = "BONUS"
#     else:
#         opt = "OTHER"

#     return plan, opt


# def _infer_category_subcat(name: str) -> Tuple[Optional[str], Optional[str]]:
#     n = (name or "").lower()
#     if "etf" in n:
#         return "ETF", "ETF"
#     if "index" in n:
#         return "Index", "Index Fund"
#     if "elss" in n or "tax saver" in n:
#         return "Equity", "ELSS"
#     if "mid cap" in n:
#         return "Equity", "Mid Cap"
#     if "large cap" in n:
#         return "Equity", "Large Cap"
#     if "small cap" in n:
#         return "Equity", "Small Cap"
#     if "gold" in n:
#         return "Commodity", "Gold"
#     return None, None


# def _counts(db: Session) -> Dict[str, int]:
#     return {
#         "amc": db.query(func.count()).select_from(MfAmc).scalar() or 0,
#         "scheme": db.query(func.count()).select_from(MfScheme).scalar() or 0,
#         "nav": db.query(func.count()).select_from(MfNavDaily).scalar() or 0,
#         "snapshot": db.query(func.count()).select_from(MfSchemeSnapshot).scalar() or 0,
#         "joblog": db.query(func.count()).select_from(MfJobLog).scalar() or 0,
#     }


# # -----------------------------
# # STEP-1: schemes sync
# # -----------------------------
# def sync_amc_and_schemes(db: Session) -> Dict[str, int]:
#     stats = {"scheme_inserted": 0, "scheme_updated": 0, "amc_created": 0}

#     _log("STEP-1: Fetching all schemes list from MFAPI ...")
#     all_schemes = _http_get(MFAPI_ALL_SCHEMES_URL)
#     _log(f"MFAPI schemes received = {len(all_schemes) if isinstance(all_schemes, list) else 'UNKNOWN'}")

#     # Keep unknown AMC for schemes we can't map
#     unknown_amc = db.query(MfAmc).filter(MfAmc.name == "Unknown Mutual Fund").first()
#     if not unknown_amc:
#         _log("Creating AMC -> 'Unknown Mutual Fund'")
#         unknown_amc = MfAmc(name="Unknown Mutual Fund", is_active=True)
#         db.add(unknown_amc)
#         db.flush()
#         stats["amc_created"] += 1

#     _log("Loading existing schemes from DB ...")
#     existing_objs = db.query(MfScheme).all()
#     existing_map = {int(s.scheme_code): s for s in existing_objs}
#     _log(f"Existing schemes in DB loaded = {len(existing_map)}")

#     processed = 0
#     for item in (all_schemes or []):
#         processed += 1
#         if processed % PRINT_EVERY == 0:
#             _log(
#                 f"STEP-1 progress: processed {processed}/{len(all_schemes)} "
#                 f"| inserted={stats['scheme_inserted']} updated={stats['scheme_updated']}"
#             )

#         try:
#             sc = int(item.get("schemeCode"))
#         except Exception:
#             continue

#         name = item.get("schemeName") or ""
#         plan, opt = _infer_plan_option(name)
#         cat, subcat = _infer_category_subcat(name)

#         obj = existing_map.get(sc)
#         if not obj:
#             obj = MfScheme(
#                 scheme_code=sc,
#                 amc_id=unknown_amc.id,
#                 scheme_name=name,
#                 plan=plan,
#                 option=opt,
#                 category=cat,
#                 sub_category=subcat,
#                 is_active=True,
#             )
#             db.add(obj)
#             stats["scheme_inserted"] += 1
#         else:
#             changed = False
#             if obj.scheme_name != name and name:
#                 obj.scheme_name = name
#                 changed = True

#             # Fill missing only
#             if (not obj.plan or obj.plan == "UNKNOWN") and plan:
#                 obj.plan = plan
#                 changed = True
#             if (not obj.option or obj.option == "UNKNOWN") and opt:
#                 obj.option = opt
#                 changed = True
#             if not obj.category and cat:
#                 obj.category = cat
#                 changed = True
#             if not obj.sub_category and subcat:
#                 obj.sub_category = subcat
#                 changed = True
#             if obj.is_active is not True:
#                 obj.is_active = True
#                 changed = True

#             if changed:
#                 stats["scheme_updated"] += 1

#     _log(f"STEP-1 DONE: inserted={stats['scheme_inserted']} updated={stats['scheme_updated']} amc_created={stats['amc_created']}")
#     return stats


# # -----------------------------
# # STEP-2: NAV sync (daily safe)
# # -----------------------------
# def sync_nav_for_missing(db: Session, target_date: date, max_schemes: int = 2000) -> Dict[str, int]:
#     """
#     Daily-safe NAV sync:
#     - For each scheme: fetch mfapi history
#     - pick latest nav_date <= target_date
#     - ✅ skip if DB already has same/newer nav_date
#     - ✅ insert with ON CONFLICT DO NOTHING (never crash)
#     """
#     stats = {
#         "nav_inserted": 0,
#         "nav_skipped_uptodate": 0,
#         "nav_conflict_skipped": 0,
#         "nav_failed": 0,
#     }

#     _log(f"STEP-2: NAV sync start | target_date={target_date} | max_schemes={max_schemes}")

#     schemes = (
#         db.query(MfScheme.scheme_code)
#         .filter(MfScheme.is_active.is_(True))
#         .order_by(MfScheme.scheme_code.asc())
#         .limit(max_schemes)
#         .all()
#     )
#     scheme_codes = [int(x[0]) for x in schemes]
#     _log(f"Active schemes picked for NAV sync = {len(scheme_codes)}")

#     if not scheme_codes:
#         return stats

#     # Current max nav_date per scheme (one query)
#     rows = (
#         db.query(MfNavDaily.scheme_code, func.max(MfNavDaily.nav_date))
#         .filter(MfNavDaily.scheme_code.in_(scheme_codes))
#         .group_by(MfNavDaily.scheme_code)
#         .all()
#     )
#     max_dt_map = {int(sc): dt for sc, dt in rows}  # dt can be None

#     insert_samples = 0
#     fail_samples = 0

#     for idx, scheme_code in enumerate(scheme_codes, start=1):
#         if idx % PRINT_EVERY == 0:
#             _log(
#                 f"STEP-2 progress: {idx}/{len(scheme_codes)} "
#                 f"| inserted={stats['nav_inserted']} skipped={stats['nav_skipped_uptodate']} "
#                 f"conflict={stats['nav_conflict_skipped']} failed={stats['nav_failed']}"
#             )

#         try:
#             data = _http_get(MFAPI_SCHEME_HISTORY_URL.format(id=scheme_code), retries=3)
#             hist = data.get("data") or []
#             if not hist:
#                 stats["nav_failed"] += 1
#                 if fail_samples < FAIL_PRINT_SAMPLES:
#                     _log(f"NAV FAIL (no data): scheme_code={scheme_code}")
#                     fail_samples += 1
#                 continue

#             # Pick latest dt <= target_date
#             pick_dt = None
#             pick_nav = None
#             for r in hist:
#                 dt = _parse_nav_date(r.get("date") or "")
#                 if not dt:
#                     continue
#                 if dt <= target_date:
#                     if pick_dt is None or dt > pick_dt:
#                         pick_dt = dt
#                         pick_nav = _to_float(r.get("nav"))

#             if pick_dt is None or pick_nav is None:
#                 stats["nav_failed"] += 1
#                 if fail_samples < FAIL_PRINT_SAMPLES:
#                     _log(f"NAV FAIL (no <= target_date): scheme_code={scheme_code} target={target_date}")
#                     fail_samples += 1
#                 continue

#             max_dt = max_dt_map.get(scheme_code)
#             if max_dt and pick_dt <= max_dt:
#                 stats["nav_skipped_uptodate"] += 1
#                 continue

#             stmt = (
#                 pg_insert(MfNavDaily)
#                 .values(scheme_code=scheme_code, nav_date=pick_dt, nav=pick_nav)
#                 .on_conflict_do_nothing(index_elements=["scheme_code", "nav_date"])
#             )
#             res = db.execute(stmt)

#             if getattr(res, "rowcount", 0) == 1:
#                 stats["nav_inserted"] += 1
#                 max_dt_map[scheme_code] = pick_dt

#                 if insert_samples < NAV_PRINT_SAMPLES:
#                     _log(f"NAV INSERT: scheme_code={scheme_code} nav_date={pick_dt} nav={pick_nav}")
#                     insert_samples += 1
#             else:
#                 stats["nav_conflict_skipped"] += 1

#         except Exception as e:
#             stats["nav_failed"] += 1
#             if fail_samples < FAIL_PRINT_SAMPLES:
#                 _log(f"NAV FAIL ({type(e).__name__}): scheme_code={scheme_code} err={e}")
#                 fail_samples += 1

#     _log(
#         f"STEP-2 DONE: inserted={stats['nav_inserted']} "
#         f"skipped={stats['nav_skipped_uptodate']} conflict={stats['nav_conflict_skipped']} failed={stats['nav_failed']}"
#     )
#     return stats


# # -----------------------------
# # STEP-3: Snapshot refresh (fixed SQLAlchemy 2.0)
# # -----------------------------
# def refresh_snapshots(db: Session) -> Dict[str, int]:
#     stats = {"snapshot_upserted": 0, "snapshot_failed": 0}

#     _log("STEP-3: Snapshot refresh start (latest nav per scheme) ...")

#     latest_rows = db.execute(text("""
#         WITH latest AS (
#           SELECT scheme_code, MAX(nav_date) AS latest_date
#           FROM mf_nav_daily
#           GROUP BY scheme_code
#         )
#         SELECT l.scheme_code, l.latest_date, nd.nav AS latest_nav
#         FROM latest l
#         JOIN mf_nav_daily nd
#           ON nd.scheme_code = l.scheme_code
#          AND nd.nav_date = l.latest_date
#     """)).fetchall()

#     _log(f"Latest NAV rows found = {len(latest_rows)}")

#     latest_map = {int(r[0]): (r[1], float(r[2])) for r in latest_rows if r[2] is not None}

#     def nav_on_or_before(sc: int, dt: date) -> Optional[float]:
#         row = (
#             db.query(MfNavDaily.nav)
#             .filter(MfNavDaily.scheme_code == sc, MfNavDaily.nav_date <= dt)
#             .order_by(MfNavDaily.nav_date.desc())
#             .first()
#         )
#         return float(row[0]) if row else None

#     for i, (sc, (as_of, latest_nav)) in enumerate(latest_map.items(), start=1):
#         if i % PRINT_EVERY == 0:
#             _log(
#                 f"STEP-3 progress: {i}/{len(latest_map)} "
#                 f"| upserted={stats['snapshot_upserted']} failed={stats['snapshot_failed']}"
#             )

#         try:
#             r1 = r3 = r5 = None

#             nav1 = nav_on_or_before(sc, as_of - timedelta(days=365))
#             if nav1:
#                 r1 = round(((latest_nav / nav1) - 1) * 100, 4)

#             nav3 = nav_on_or_before(sc, as_of - timedelta(days=365 * 3))
#             if nav3:
#                 r3 = round(((latest_nav / nav3) - 1) * 100, 4)

#             nav5 = nav_on_or_before(sc, as_of - timedelta(days=365 * 5))
#             if nav5:
#                 r5 = round(((latest_nav / nav5) - 1) * 100, 4)

#             snap = db.query(MfSchemeSnapshot).filter(MfSchemeSnapshot.scheme_code == sc).first()
#             if not snap:
#                 snap = MfSchemeSnapshot(
#                     scheme_code=sc,
#                     as_of_date=as_of,
#                     latest_nav=latest_nav,
#                     return_1y=r1,
#                     return_3y=r3,
#                     return_5y=r5,
#                 )
#                 db.add(snap)
#             else:
#                 snap.as_of_date = as_of
#                 snap.latest_nav = latest_nav
#                 snap.return_1y = r1
#                 snap.return_3y = r3
#                 snap.return_5y = r5

#             stats["snapshot_upserted"] += 1

#         except Exception as e:
#             stats["snapshot_failed"] += 1
#             if stats["snapshot_failed"] <= FAIL_PRINT_SAMPLES:
#                 _log(f"SNAPSHOT FAIL ({type(e).__name__}): scheme_code={sc} err={e}")

#     _log(f"STEP-3 DONE: upserted={stats['snapshot_upserted']} failed={stats['snapshot_failed']}")
#     return stats


# # -----------------------------
# # Main runner (daily)
# # -----------------------------
# def run_daily(target_date: Optional[date] = None, max_schemes_for_nav: int = 2000):
#     db: Session = SessionLocal()

#     _log("========================================")
#     _log("🚀 MF_DAILY_SYNC START")
#     _log("========================================")

#     start_counts = _counts(db)
#     _log(f"DB COUNTS (before): {start_counts}")

#     job = MfJobLog(job_name="MF_DAILY_SYNC", status="RUNNING", message="started")
#     db.add(job)
#     db.commit()

#     try:
#         if target_date is None:
#             latest = db.query(func.max(MfNavDaily.nav_date)).scalar()
#             target_date = date.today()
#             _log(f"Detected latest_nav_date_in_db={latest} | target_date set to today={target_date}")

#         # STEP-1
#         stats1 = sync_amc_and_schemes(db)
#         db.commit()
#         _log("DB commit done after STEP-1")

#         # STEP-2
#         stats2 = sync_nav_for_missing(db, target_date=target_date, max_schemes=max_schemes_for_nav)
#         db.commit()
#         _log("DB commit done after STEP-2")

#         # STEP-3
#         stats3 = refresh_snapshots(db)
#         db.commit()
#         _log("DB commit done after STEP-3")

#         end_counts = _counts(db)
#         _log(f"DB COUNTS (after):  {end_counts}")

#         final_msg = {
#             "target_date": str(target_date),
#             **stats1,
#             **stats2,
#             **stats3,
#             "counts_before": start_counts,
#             "counts_after": end_counts,
#         }

#         job.status = "SUCCESS"
#         job.message = str(final_msg)[:2000]
#         db.commit()

#         _log(f"✅ MF_DAILY_SYNC DONE: {final_msg}")

#     except Exception as e:
#         db.rollback()
#         job.status = "FAILED"
#         job.message = f"{type(e).__name__}: {e}"[:2000]
#         db.commit()
#         _log(f"❌ MF_DAILY_SYNC FAILED: {type(e).__name__}: {e}")
#         raise
#     finally:
#         db.close()
#         _log("DB session closed.")


# if __name__ == "__main__":
#     run_daily()

# scripts/mf/mf_nav_backfill.py

import time
import requests
from datetime import datetime, date, timedelta
from typing import Any, Optional, List, Dict

from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.connection import SessionLocal
from db.models import MfScheme, MfNavDaily, MfJobLog

MFAPI_SCHEME_HISTORY_URL = "https://api.mfapi.in/mf/{id}"

PRINT_EVERY = 50
SLEEP_BETWEEN = 0.12  # 120ms (safe)
MAX_HISTORY_ROWS_PER_SCHEME = 8000  # mfapi can be large, but fine

def now_utc():
    return datetime.utcnow()

def _log(msg: str):
    print(f"[{now_utc().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def _http_get(url: str, retries: int = 3) -> Any:
    last_err = None
    for i in range(retries):
        try:
            _log(f"HTTP GET -> {url} (try {i+1}/{retries})")
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            _log(f"HTTP ERROR: {type(e).__name__}: {e}")
            time.sleep(1.0 * (i + 1))
    raise last_err

def _parse_nav_date(d: str) -> Optional[date]:
    try:
        return datetime.strptime(d, "%d-%m-%Y").date()
    except Exception:
        return None

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def backfill_last_days(
    days: int = 365,
    max_schemes: int = 500,
    only_active: bool = True,
):
    db: Session = SessionLocal()

    job = MfJobLog(job_name="MF_NAV_BACKFILL", status="RUNNING", message="started")
    db.add(job)
    db.commit()

    try:
        end_dt = date.today()
        start_dt = end_dt - timedelta(days=days)

        _log("========================================")
        _log("🚀 MF_NAV_BACKFILL START")
        _log(f"Window: {start_dt} -> {end_dt} ({days} days)")
        _log("========================================")

        q = db.query(MfScheme.scheme_code)
        if only_active:
            q = q.filter(MfScheme.is_active.is_(True))
        q = q.order_by(MfScheme.scheme_code.asc()).limit(max_schemes)

        scheme_codes = [int(x[0]) for x in q.all()]
        _log(f"Schemes picked = {len(scheme_codes)}")

        inserted_total = 0
        skipped_conflict = 0
        failed = 0

        for idx, sc in enumerate(scheme_codes, start=1):
            if idx % PRINT_EVERY == 0:
                _log(f"Progress: {idx}/{len(scheme_codes)} | inserted={inserted_total} conflict={skipped_conflict} failed={failed}")

            try:
                payload = _http_get(MFAPI_SCHEME_HISTORY_URL.format(id=sc), retries=3)
                hist = payload.get("data") or []
                if not hist:
                    failed += 1
                    _log(f"NAV EMPTY: scheme={sc}")
                    continue

                # filter to window only
                values: List[Dict[str, Any]] = []
                for r in hist[:MAX_HISTORY_ROWS_PER_SCHEME]:
                    dt = _parse_nav_date(r.get("date") or "")
                    if not dt:
                        continue
                    if dt < start_dt or dt > end_dt:
                        continue
                    nav = _to_float(r.get("nav"))
                    if nav is None:
                        continue
                    values.append({"scheme_code": sc, "nav_date": dt, "nav": nav})

                if not values:
                    # no rows in window
                    continue

                # bulk insert for this scheme, ignore duplicates
                stmt = pg_insert(MfNavDaily).values(values)
                stmt = stmt.on_conflict_do_nothing(index_elements=["scheme_code", "nav_date"])
                res = db.execute(stmt)

                # rowcount can be -1 sometimes, so best-effort:
                rc = getattr(res, "rowcount", 0) or 0
                if rc > 0:
                    inserted_total += rc
                else:
                    # when conflict happens or driver doesn't report rowcount
                    skipped_conflict += 1

                db.commit()
                time.sleep(SLEEP_BETWEEN)

            except Exception as e:
                db.rollback()
                failed += 1
                _log(f"FAIL: scheme={sc} err={type(e).__name__}: {e}")

        final = {
            "window_from": str(start_dt),
            "window_to": str(end_dt),
            "schemes": len(scheme_codes),
            "inserted_total": inserted_total,
            "conflict_skips": skipped_conflict,
            "failed": failed,
        }

        job.status = "SUCCESS"
        job.message = str(final)[:2000]
        job.inserted = inserted_total
        job.skipped = skipped_conflict
        job.updated = 0
        db.commit()

        _log(f"✅ MF_NAV_BACKFILL DONE: {final}")

    except Exception as e:
        db.rollback()
        job.status = "FAILED"
        job.message = f"{type(e).__name__}: {e}"[:2000]
        db.commit()
        raise
    finally:
        db.close()
        _log("DB session closed.")

if __name__ == "__main__":
    # Example: last 365 days, first 500 schemes
    backfill_last_days(days=365, max_schemes=500, only_active=True)
