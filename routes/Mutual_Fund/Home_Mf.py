# routes/mf/Home_Mf.py

from typing import List, Dict, Any, Optional
from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text, func

from db.connection import get_db
from db.models import MfAmc, MfScheme, MfNavDaily

router = APIRouter(prefix="/mf", tags=["mutual-fund"])


# ----------------------------
# Helpers
# ----------------------------
def _nav_freshness(db: Session) -> Optional[date]:
    return db.query(func.max(MfNavDaily.nav_date)).scalar()


def _print_debug_counts(db: Session, where_sql: str, plan: Optional[str], option: Optional[str], days: int):
    """
    Console debug to understand WHY rows are not coming.
    """
    print("\n============================")
    print("🧪 MF HOME DEBUG")
    print("============================")
    print("where_sql =", where_sql)
    print("plan =", plan, "option =", option, "days =", days)

    # 1) How many schemes match the filter (just mf_scheme)
    q = db.query(func.count(MfScheme.scheme_code)).filter(MfScheme.is_active.is_(True))
    if plan:
        q = q.filter(MfScheme.plan == plan)
    if option:
        q = q.filter(MfScheme.option == option)

    # where_sql is text on alias 's' in raw SQL, so here we do a broad approximation for counts
    # We'll also run a raw count with same where_sql below (accurate).
    approx_count = q.scalar() or 0
    print("Approx active scheme count (plan/option filtered) =", approx_count)

    # 2) Accurate count using raw SQL for same where_sql
    sql_match = text(f"""
        SELECT COUNT(*) AS c
        FROM mf_scheme s
        WHERE s.is_active = TRUE
          {("AND s.plan = :plan" if plan else "")}
          {("AND s.option = :option" if option else "")}
          AND ({where_sql})
    """)
    params = {}
    if plan:
        params["plan"] = plan
    if option:
        params["option"] = option

    match_cnt = db.execute(sql_match, params).scalar() or 0
    print("✅ Schemes matching tab filter =", match_cnt)

    # 3) How many have at least 1 NAV row
    sql_has_nav = text(f"""
        SELECT COUNT(DISTINCT s.scheme_code) AS c
        FROM mf_scheme s
        JOIN mf_nav_daily nd ON nd.scheme_code = s.scheme_code
        WHERE s.is_active = TRUE
          {("AND s.plan = :plan" if plan else "")}
          {("AND s.option = :option" if option else "")}
          AND ({where_sql})
    """)
    has_nav_cnt = db.execute(sql_has_nav, params).scalar() or 0
    print("✅ Matching schemes with >=1 NAV row =", has_nav_cnt)

    # 4) How many have a prev nav (<= latest - days)
    sql_has_prev = text(f"""
        WITH latest AS (
            SELECT scheme_code, MAX(nav_date) AS latest_date
            FROM mf_nav_daily
            GROUP BY scheme_code
        ),
        prev_date AS (
            SELECT
                l.scheme_code,
                MAX(nd.nav_date) AS prev_date
            FROM latest l
            JOIN mf_nav_daily nd
              ON nd.scheme_code = l.scheme_code
             AND nd.nav_date <= (l.latest_date - (:days || ' days')::interval)
            GROUP BY l.scheme_code
        )
        SELECT COUNT(*) AS c
        FROM mf_scheme s
        JOIN prev_date p ON p.scheme_code = s.scheme_code
        WHERE s.is_active = TRUE
          {("AND s.plan = :plan" if plan else "")}
          {("AND s.option = :option" if option else "")}
          AND ({where_sql})
    """)
    params2 = dict(params)
    params2["days"] = int(days)
    has_prev_cnt = db.execute(sql_has_prev, params2).scalar() or 0
    print(f"✅ Matching schemes having NAV <= latest-{days}d =", has_prev_cnt)

    # 5) Sample 5 scheme_names from tab filter
    sql_sample = text(f"""
        SELECT s.scheme_code, s.scheme_name, s.category, s.sub_category, s.plan, s.option
        FROM mf_scheme s
        WHERE s.is_active = TRUE
          {("AND s.plan = :plan" if plan else "")}
          {("AND s.option = :option" if option else "")}
          AND ({where_sql})
        ORDER BY s.scheme_code ASC
        LIMIT 5
    """)
    samp = db.execute(sql_sample, params).mappings().all()
    print("\nSample schemes matching filter:")
    for r in samp:
        print(" -", r["scheme_code"], "|", r["plan"], r["option"], "|", r["sub_category"], "|", r["scheme_name"])

    # 6) Show NAV window for 1 scheme (if exists)
    if samp:
        sc = samp[0]["scheme_code"]
        sql_nav_window = text("""
            SELECT MIN(nav_date) AS min_dt, MAX(nav_date) AS max_dt, COUNT(*) AS c
            FROM mf_nav_daily
            WHERE scheme_code = :sc
        """)
        navinfo = db.execute(sql_nav_window, {"sc": sc}).mappings().first()
        print(f"\nNAV window for scheme_code={sc}:",
              "min=", navinfo["min_dt"], "max=", navinfo["max_dt"], "count=", navinfo["c"])
    print("============================\n")


def _fetch_top_by_return(
    db: Session,
    where_sql: str,
    limit: int = 5,
    days: int = 365,
    plan: Optional[str] = "DIRECT",
    option: Optional[str] = "GROWTH",
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Returns list for home tab.
    ✅ Fallback return: if 1Y not available, try 6M/3M/1M.
    """
    if debug:
        _print_debug_counts(db, where_sql, plan, option, days)

    sql = text(f"""
    WITH latest AS (
        SELECT nd.scheme_code, MAX(nd.nav_date) AS latest_date
        FROM mf_nav_daily nd
        GROUP BY nd.scheme_code
    ),
    latest_nav AS (
        SELECT nd.scheme_code, nd.nav_date AS latest_date, nd.nav AS latest_nav
        FROM mf_nav_daily nd
        JOIN latest l
          ON l.scheme_code = nd.scheme_code
         AND l.latest_date = nd.nav_date
    ),

    -- helper: find prev date for any requested days
    prev_date_1y AS (
        SELECT l.scheme_code, MAX(nd.nav_date) AS prev_date
        FROM latest l
        JOIN mf_nav_daily nd
          ON nd.scheme_code = l.scheme_code
         AND nd.nav_date <= (l.latest_date - (:d1 || ' days')::interval)
        GROUP BY l.scheme_code
    ),
    prev_nav_1y AS (
        SELECT nd.scheme_code, nd.nav_date AS prev_date, nd.nav AS prev_nav
        FROM mf_nav_daily nd
        JOIN prev_date_1y p
          ON p.scheme_code = nd.scheme_code
         AND p.prev_date = nd.nav_date
    ),

    prev_date_6m AS (
        SELECT l.scheme_code, MAX(nd.nav_date) AS prev_date
        FROM latest l
        JOIN mf_nav_daily nd
          ON nd.scheme_code = l.scheme_code
         AND nd.nav_date <= (l.latest_date - (:d2 || ' days')::interval)
        GROUP BY l.scheme_code
    ),
    prev_nav_6m AS (
        SELECT nd.scheme_code, nd.nav_date AS prev_date, nd.nav AS prev_nav
        FROM mf_nav_daily nd
        JOIN prev_date_6m p
          ON p.scheme_code = nd.scheme_code
         AND p.prev_date = nd.nav_date
    ),

    prev_date_3m AS (
        SELECT l.scheme_code, MAX(nd.nav_date) AS prev_date
        FROM latest l
        JOIN mf_nav_daily nd
          ON nd.scheme_code = l.scheme_code
         AND nd.nav_date <= (l.latest_date - (:d3 || ' days')::interval)
        GROUP BY l.scheme_code
    ),
    prev_nav_3m AS (
        SELECT nd.scheme_code, nd.nav_date AS prev_date, nd.nav AS prev_nav
        FROM mf_nav_daily nd
        JOIN prev_date_3m p
          ON p.scheme_code = nd.scheme_code
         AND p.prev_date = nd.nav_date
    ),

    prev_date_1m AS (
        SELECT l.scheme_code, MAX(nd.nav_date) AS prev_date
        FROM latest l
        JOIN mf_nav_daily nd
          ON nd.scheme_code = l.scheme_code
         AND nd.nav_date <= (l.latest_date - (:d4 || ' days')::interval)
        GROUP BY l.scheme_code
    ),
    prev_nav_1m AS (
        SELECT nd.scheme_code, nd.nav_date AS prev_date, nd.nav AS prev_nav
        FROM mf_nav_daily nd
        JOIN prev_date_1m p
          ON p.scheme_code = nd.scheme_code
         AND p.prev_date = nd.nav_date
    )

    SELECT
        s.scheme_code,
        s.scheme_name,
        a.name AS amc_name,
        s.category,
        s.sub_category,
        s.plan,
        s.option,
        ln.latest_date,
        ln.latest_nav,

        -- fallback choose first available
        COALESCE(p1.prev_date, p6.prev_date, p3.prev_date, pM.prev_date) AS prev_nav_date,
        COALESCE(p1.prev_nav,  p6.prev_nav,  p3.prev_nav,  pM.prev_nav)  AS prev_nav,

        CASE
          WHEN p1.prev_nav IS NOT NULL THEN '1Y'
          WHEN p6.prev_nav IS NOT NULL THEN '6M'
          WHEN p3.prev_nav IS NOT NULL THEN '3M'
          WHEN pM.prev_nav IS NOT NULL THEN '1M'
          ELSE NULL
        END AS return_period,

        CASE
          WHEN COALESCE(p1.prev_nav, p6.prev_nav, p3.prev_nav, pM.prev_nav) IS NULL THEN NULL
          ELSE ROUND(((ln.latest_nav / NULLIF(COALESCE(p1.prev_nav, p6.prev_nav, p3.prev_nav, pM.prev_nav), 0)) - 1) * 100, 2)
        END AS return_pct

    FROM mf_scheme s
    JOIN mf_amc a
      ON a.id = s.amc_id
    JOIN latest_nav ln
      ON ln.scheme_code = s.scheme_code
    LEFT JOIN prev_nav_1y p1 ON p1.scheme_code = s.scheme_code
    LEFT JOIN prev_nav_6m p6 ON p6.scheme_code = s.scheme_code
    LEFT JOIN prev_nav_3m p3 ON p3.scheme_code = s.scheme_code
    LEFT JOIN prev_nav_1m pM ON pM.scheme_code = s.scheme_code

    WHERE
        s.is_active = TRUE
        {("AND s.plan = :plan" if plan else "")}
        {("AND s.option = :option" if option else "")}
        AND ({where_sql})

    ORDER BY return_pct DESC NULLS LAST, ln.latest_nav DESC
    LIMIT :limit
    """)

    params = {
        "limit": int(limit),
        "d1": int(days),     # 1Y requested
        "d2": 180,           # 6M fallback
        "d3": 90,            # 3M fallback
        "d4": 30,            # 1M fallback
    }
    if plan:
        params["plan"] = plan
    if option:
        params["option"] = option

    rows = db.execute(sql, params).mappings().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        cat = (r.get("category") or "").strip() or "—"
        opt = (r.get("option") or "").strip() or "—"
        label = f"{cat} • {opt}" if cat != "—" else opt

        out.append(
            {
                "scheme_code": int(r["scheme_code"]),
                "scheme_name": r["scheme_name"],
                "amc_name": r["amc_name"],
                "category": r.get("category"),
                "sub_category": r.get("sub_category"),
                "plan": r.get("plan"),
                "option": r.get("option"),
                "label": label,
                "latest_nav_date": r.get("latest_date"),
                "latest_nav": float(r["latest_nav"]) if r.get("latest_nav") is not None else None,
                "prev_nav_date": r.get("prev_nav_date"),
                "prev_nav": float(r["prev_nav"]) if r.get("prev_nav") is not None else None,
                "return_period": r.get("return_period"),
                "return_pct": float(r["return_pct"]) if r.get("return_pct") is not None else None,
            }
        )

    return out


@router.get("/home")
def mf_home_tab(
    tab: str = Query("large_cap", description="large_cap | mid_cap | small_cap | tax_saving | index_funds | etfs"),
    limit: int = Query(5, ge=1, le=50),
    days: int = Query(365, ge=30, le=2000),
    debug: bool = Query(False, description="true -> server console prints debug info"),
    db: Session = Depends(get_db),
):
    tab_key = (tab or "").strip().lower()

    if tab_key == "large_cap":
        where_sql = "COALESCE(s.sub_category,'') ILIKE '%large cap%'"
        plan, option = "DIRECT", "GROWTH"

    elif tab_key == "mid_cap":
        where_sql = "COALESCE(s.sub_category,'') ILIKE '%mid cap%'"
        plan, option = "DIRECT", "GROWTH"

    elif tab_key == "small_cap":
        where_sql = "COALESCE(s.sub_category,'') ILIKE '%small cap%'"
        plan, option = "DIRECT", "GROWTH"

    elif tab_key in ("tax_saving", "elss"):
        where_sql = """
            COALESCE(s.sub_category,'') ILIKE '%elss%'
            OR COALESCE(s.scheme_name,'') ILIKE '%elss%'
            OR COALESCE(s.category,'') ILIKE '%tax%'
        """
        plan, option = "DIRECT", "GROWTH"

    elif tab_key in ("index_funds", "index"):
        where_sql = """
            COALESCE(s.sub_category,'') ILIKE '%index%'
            OR COALESCE(s.scheme_name,'') ILIKE '%index fund%'
            OR COALESCE(s.category,'') ILIKE '%index%'
        """
        plan, option = "DIRECT", "GROWTH"

    elif tab_key in ("etfs", "etf"):
        where_sql = """
            COALESCE(s.scheme_name,'') ILIKE '%etf%'
            OR COALESCE(s.category,'') ILIKE '%etf%'
            OR COALESCE(s.sub_category,'') ILIKE '%etf%'
        """
        plan, option = "DIRECT", None

    else:
        return {
            "ok": False,
            "error": "Invalid tab",
            "allowed": ["large_cap", "mid_cap", "small_cap", "tax_saving", "index_funds", "etfs"],
        }

    data = _fetch_top_by_return(
        db=db,
        where_sql=where_sql,
        limit=limit,
        days=days,
        plan=plan,
        option=option,
        debug=debug,
    )

    return {
        "ok": True,
        "tab": tab_key,
        "sort": f"{days}D_return_desc",
        "latest_nav_date": _nav_freshness(db),
        "count": len(data),
        "data": data,
    }
