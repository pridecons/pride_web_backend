# scripts/mf/mf_fix_scheme_meta_v3.py
import re
from typing import Optional, Tuple
from sqlalchemy.orm import Session

from db.connection import SessionLocal
from db.models import MfScheme


BATCH_SIZE = 500


def norm_upper(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).upper()


def guess_plan(name: str) -> Optional[str]:
    """
    Priority:
    1) DIRECT / REGULAR if present
    2) else return OTHER (Retail/Institutional etc. are not DIRECT/REGULAR)
    """
    n = norm_upper(name)

    if "DIRECT" in n:
        return "DIRECT"
    if "REGULAR" in n:
        return "REGULAR"

    # Many schemes use Retail/Institutional/Plan etc. (not direct/regular)
    if any(k in n for k in ["RETAIL", "INSTITUTIONAL", "INST ", "INST.", "PLAN", "PLANS", "UNCLAIMED"]):
        return "OTHER"

    return None  # keep UNKNOWN if truly no clue


def guess_option(name: str) -> Optional[str]:
    """
    Try to infer Growth/IDCW/Bonus.
    """
    n = norm_upper(name)

    # Strong signals
    if "GROWTH" in n or "(G)" in n or " (G) " in n:
        return "GROWTH"

    # IDCW / DIVIDEND
    if "IDCW" in n:
        return "IDCW"
    if "DIVIDEND" in n:
        return "IDCW"

    # Some schemes use "INCOME DISTRIBUTION CUM CAPITAL WITHDRAWAL"
    if "INCOME DISTRIBUTION" in n and "CAPITAL WITHDRAWAL" in n:
        return "IDCW"

    # Payout words imply dividend-type
    if any(k in n for k in ["PAYOUT", "REINVEST", "RE-INVEST", "REINVESTMENT"]):
        return "IDCW"

    if "BONUS" in n:
        return "BONUS"

    return None


def run():
    db: Session = SessionLocal()
    try:
        # candidates where plan or option missing/UNKNOWN
        q = (
            db.query(MfScheme)
            .filter(MfScheme.is_active == True)
            .filter(
                (MfScheme.plan == None) | (MfScheme.plan == "") | (MfScheme.plan == "UNKNOWN")
                | (MfScheme.option == None) | (MfScheme.option == "") | (MfScheme.option == "UNKNOWN")
            )
            .order_by(MfScheme.scheme_code.asc())
        )

        total = q.count()
        print(f"🔧 Fix v3 (plan/option): candidates={total}")

        processed = 0
        plan_filled = 0
        option_filled = 0

        while True:
            batch = q.limit(BATCH_SIZE).offset(processed).all()
            if not batch:
                break

            for s in batch:
                name = s.scheme_name or ""

                # ---- plan ----
                if (s.plan is None) or (s.plan == "") or (s.plan == "UNKNOWN"):
                    gp = guess_plan(name)
                    if gp:
                        s.plan = gp
                        plan_filled += 1

                # ---- option ----
                if (s.option is None) or (s.option == "") or (s.option == "UNKNOWN"):
                    go = guess_option(name)
                    if go:
                        s.option = go
                        option_filled += 1

            processed += len(batch)

            db.commit()

            if processed % 500 == 0 or processed >= total:
                print(
                    f"✅ processed {processed}/{total} | "
                    f"plan_filled={plan_filled} | option_filled={option_filled}"
                )

        # Remaining missing after this fix
        rem = (
            db.query(MfScheme)
            .filter(MfScheme.is_active == True)
            .with_entities(
                (MfScheme.plan == None).label("plan_null"),
            )
        )

        rem_plan = db.query(MfScheme).filter(
            MfScheme.is_active == True,
            ((MfScheme.plan == None) | (MfScheme.plan == "") | (MfScheme.plan == "UNKNOWN"))
        ).count()

        rem_opt = db.query(MfScheme).filter(
            MfScheme.is_active == True,
            ((MfScheme.option == None) | (MfScheme.option == "") | (MfScheme.option == "UNKNOWN"))
        ).count()

        print("\n✅ DONE Fix v3")
        print(f"  plan_filled={plan_filled}")
        print(f"  option_filled={option_filled}")
        print("\n📊 Remaining Missing After Fix v3")
        print(f"  plan missing/UNKNOWN  : {rem_plan}")
        print(f"  option missing/UNKNOWN: {rem_opt}")

    finally:
        db.close()


if __name__ == "__main__":
    run()
