# scripts/mf/mf_audit.py
from datetime import datetime
from sqlalchemy import func, inspect
from sqlalchemy.orm import Session

from db.connection import SessionLocal
from db.models import MfAmc, MfScheme, MfNavDaily, MfSchemeSnapshot, MfJobLog

SAMPLE_LIMIT = 5
MISSING_SAMPLE = 15


def _has_col(engine, table_name: str, col: str) -> bool:
    insp = inspect(engine)
    cols = [c["name"] for c in insp.get_columns(table_name)]
    return col in cols


def _pick_joblog_order_col(engine) -> str:
    # mf_job_log me created_at ho bhi sakta hai ya nahi
    for c in ["created_at", "updated_at", "run_at", "timestamp", "id"]:
        if _has_col(engine, MfJobLog.__tablename__, c):
            return c
    return "id"


def run():
    db: Session = SessionLocal()
    try:
        engine = db.get_bind()

        print("\n============================")
        print("✅ MUTUAL FUND DB AUDIT")
        print("============================\n")

        # -----------------------------
        # 1) Basic counts (ORM)
        # -----------------------------
        amc_count = db.query(func.count(MfAmc.id)).scalar() or 0
        scheme_count = db.query(func.count(MfScheme.scheme_code)).scalar() or 0
        nav_count = db.query(func.count()).select_from(MfNavDaily).scalar() or 0
        snapshot_count = db.query(func.count()).select_from(MfSchemeSnapshot).scalar() or 0
        joblog_count = db.query(func.count()).select_from(MfJobLog).scalar() or 0

        print("📦 Table Counts")
        print(f"  mf_amc            : {amc_count}")
        print(f"  mf_scheme         : {scheme_count}")
        print(f"  mf_nav_daily      : {nav_count}")
        print(f"  mf_scheme_snapshot: {snapshot_count}")
        print(f"  mf_job_log        : {joblog_count}\n")

        # -----------------------------
        # 2) Latest NAV date (ORM)
        # -----------------------------
        latest_nav_date = db.query(func.max(MfNavDaily.nav_date)).scalar()
        print("📅 NAV Freshness")
        print(f"  Latest nav_date in mf_nav_daily: {latest_nav_date}\n")

        # -----------------------------
        # 3) Missing AMC mapping (ORM)
        # - amc_id null OR invalid id
        # -----------------------------
        # NOTE: invalid check via outer join
        miss_amc_cnt = (
            db.query(func.count(MfScheme.scheme_code))
            .outerjoin(MfAmc, MfAmc.id == MfScheme.amc_id)
            .filter((MfScheme.amc_id.is_(None)) | (MfAmc.id.is_(None)))
            .scalar()
            or 0
        )

        print("🏦 AMC Mapping Check")
        print(f"  Schemes with missing/invalid amc_id: {miss_amc_cnt}")

        if miss_amc_cnt:
            rows = (
                db.query(MfScheme.scheme_code, MfScheme.scheme_name, MfScheme.amc_id)
                .outerjoin(MfAmc, MfAmc.id == MfScheme.amc_id)
                .filter((MfScheme.amc_id.is_(None)) | (MfAmc.id.is_(None)))
                .order_by(MfScheme.scheme_code.asc())
                .limit(MISSING_SAMPLE)
                .all()
            )
            print(f"  Sample (top {MISSING_SAMPLE}):")
            for scheme_code, scheme_name, amc_id in rows:
                print(f"    - {scheme_code} | amc_id={amc_id} | {scheme_name}")
        print()

        # -----------------------------
        # 4) Missing metadata fields in mf_scheme
        # -----------------------------
        miss_category = db.query(func.count()).select_from(MfScheme).filter(
            (MfScheme.category.is_(None)) | (MfScheme.category == "")
        ).scalar() or 0

        miss_sub_category = db.query(func.count()).select_from(MfScheme).filter(
            (MfScheme.sub_category.is_(None)) | (MfScheme.sub_category == "")
        ).scalar() or 0

        miss_plan = db.query(func.count()).select_from(MfScheme).filter(
            (MfScheme.plan.is_(None)) | (MfScheme.plan == "") | (MfScheme.plan == "UNKNOWN")
        ).scalar() or 0

        miss_option = db.query(func.count()).select_from(MfScheme).filter(
            (MfScheme.option.is_(None)) | (MfScheme.option == "") | (MfScheme.option == "UNKNOWN")
        ).scalar() or 0

        print("🧩 Scheme Metadata Missing")
        print(f"  Missing category      : {miss_category}")
        print(f"  Missing sub_category  : {miss_sub_category}")
        print(f"  Missing plan          : {miss_plan}")
        print(f"  Missing option        : {miss_option}")

        if miss_category or miss_sub_category or miss_plan or miss_option:
            rows = (
                db.query(
                    MfScheme.scheme_code,
                    MfScheme.scheme_name,
                    MfScheme.category,
                    MfScheme.sub_category,
                    MfScheme.plan,
                    MfScheme.option,
                )
                .filter(
                    (MfScheme.category.is_(None)) | (MfScheme.category == "") |
                    (MfScheme.sub_category.is_(None)) | (MfScheme.sub_category == "") |
                    (MfScheme.plan.is_(None)) | (MfScheme.plan == "") | (MfScheme.plan == "UNKNOWN") |
                    (MfScheme.option.is_(None)) | (MfScheme.option == "") | (MfScheme.option == "UNKNOWN")
                )
                .order_by(MfScheme.scheme_code.asc())
                .limit(MISSING_SAMPLE)
                .all()
            )
            print(f"  Sample (top {MISSING_SAMPLE}):")
            for r in rows:
                print(f"    - {r.scheme_code} | plan={r.plan} option={r.option} | {r.scheme_name}")
        print()

        # -----------------------------
        # 5) Schemes with NO NAV rows
        # -----------------------------
        no_nav_cnt = (
            db.query(func.count(MfScheme.scheme_code))
            .outerjoin(MfNavDaily, MfNavDaily.scheme_code == MfScheme.scheme_code)
            .filter(MfNavDaily.scheme_code.is_(None))
            .scalar()
            or 0
        )

        print("📉 NAV Missing Check")
        print(f"  Schemes with 0 NAV rows: {no_nav_cnt}")

        if no_nav_cnt:
            rows = (
                db.query(MfScheme.scheme_code, MfScheme.scheme_name)
                .outerjoin(MfNavDaily, MfNavDaily.scheme_code == MfScheme.scheme_code)
                .filter(MfNavDaily.scheme_code.is_(None))
                .order_by(MfScheme.scheme_code.asc())
                .limit(MISSING_SAMPLE)
                .all()
            )
            print(f"  Sample (top {MISSING_SAMPLE}):")
            for scheme_code, scheme_name in rows:
                print(f"    - {scheme_code} | {scheme_name}")
        print()

        # -----------------------------
        # 6) Snapshot missing
        # -----------------------------
        snap_missing_cnt = (
            db.query(func.count(MfScheme.scheme_code))
            .outerjoin(MfSchemeSnapshot, MfSchemeSnapshot.scheme_code == MfScheme.scheme_code)
            .filter(MfSchemeSnapshot.scheme_code.is_(None))
            .scalar()
            or 0
        )

        print("⚡ Snapshot Missing Check")
        print(f"  Schemes with no snapshot row: {snap_missing_cnt}")

        if snap_missing_cnt:
            rows = (
                db.query(MfScheme.scheme_code, MfScheme.scheme_name)
                .outerjoin(MfSchemeSnapshot, MfSchemeSnapshot.scheme_code == MfScheme.scheme_code)
                .filter(MfSchemeSnapshot.scheme_code.is_(None))
                .order_by(MfScheme.scheme_code.asc())
                .limit(MISSING_SAMPLE)
                .all()
            )
            print(f"  Sample (top {MISSING_SAMPLE}):")
            for scheme_code, scheme_name in rows:
                print(f"    - {scheme_code} | {scheme_name}")
        print()

        # -----------------------------
        # 7) Recent job logs (SAFE order col)
        # -----------------------------
        print("🧾 Recent Job Logs (last 10)")

        order_col = _pick_joblog_order_col(engine)

        # Build query dynamically but still via ORM object
        # We'll just fetch full objects and sort by column
        col_attr = getattr(MfJobLog, order_col, None)
        if col_attr is None:
            col_attr = MfJobLog.id

        logs = db.query(MfJobLog).order_by(col_attr.desc()).limit(10).all()

        if not logs:
            print("  (no logs found)\n")
        else:
            for l in logs:
                when = getattr(l, order_col, None)
                msg = (getattr(l, "message", "") or "")[:120].replace("\n", " ")
                print(f"  - #{getattr(l,'id',None)} | {when} | {getattr(l,'job_name',None)} | {getattr(l,'status',None)} | {msg}")
        print()

        # -----------------------------
        # 8) Sample rows (ORM)
        # -----------------------------
        print("🔍 Sample rows (top 5 each)\n")

        print("mf_amc:")
        amcs = db.query(MfAmc).order_by(MfAmc.id.desc()).limit(SAMPLE_LIMIT).all()
        for a in amcs:
            print(f"  - {a.id} | {getattr(a,'name',None)} | {getattr(a,'created_at',None)}")
        print()

        print("mf_scheme:")
        schemes = db.query(MfScheme).order_by(MfScheme.updated_at.desc()).limit(SAMPLE_LIMIT).all()
        for s in schemes:
            print(f"  - {s.scheme_code} | amc_id={s.amc_id} | {s.plan}/{s.option} | {s.scheme_name}")
        print()

        print("mf_nav_daily:")
        navs = db.query(MfNavDaily).order_by(MfNavDaily.nav_date.desc()).limit(SAMPLE_LIMIT).all()
        for n in navs:
            print(f"  - {n.scheme_code} | {n.nav_date} | nav={n.nav} | {getattr(n,'updated_at',None)}")
        print()

        print("mf_scheme_snapshot:")
        snaps = db.query(MfSchemeSnapshot).order_by(MfSchemeSnapshot.updated_at.desc()).limit(SAMPLE_LIMIT).all()
        for ss in snaps:
            print(f"  - {ss.scheme_code} | as_of={ss.as_of_date} | latest_nav={ss.latest_nav} | {getattr(ss,'updated_at',None)}")
        print()

        print("✅ Audit complete.\n")

    finally:
        db.close()


if __name__ == "__main__":
    run()
