# scripts/mf/mf_fix_scheme_meta_v5_force_nooffset.py
import re
from sqlalchemy.orm import Session

from db.connection import SessionLocal
from db.models import MfScheme

BATCH_SIZE = 1000

def norm_upper(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).upper()

def is_missing(v: str) -> bool:
    return v is None or str(v).strip() == "" or str(v).strip().upper() == "UNKNOWN"

def guess_plan(name: str):
    n = norm_upper(name)
    if "DIRECT" in n:
        return "DIRECT"
    if "REGULAR" in n:
        return "REGULAR"

    # extra hints
    if any(k in n for k in ["UNCLAIMED", "REDEMPTION", "PLAN", "SERIES", "FMP", "INSTITUTIONAL", "RETAIL"]):
        return "OTHER"

    return None

def guess_option(name: str):
    n = norm_upper(name)

    # ✅ cumulative means growth (very common in FMP series)
    if "CUMULATIVE" in n or "CUMUL" in n:
        return "GROWTH"

    # growth
    if "GROWTH" in n:
        return "GROWTH"

    # idcw/dividend
    if "IDCW" in n:
        return "IDCW"
    if "DIVIDEND" in n:
        return "IDCW"
    if "INCOME DISTRIBUTION" in n and "CAPITAL WITHDRAWAL" in n:
        return "IDCW"
    if any(k in n for k in ["PAYOUT", "REINVEST", "RE-INVEST", "REINVESTMENT"]):
        return "IDCW"

    # bonus
    if "BONUS" in n:
        return "BONUS"

    # unclaimed/redemption -> usually not inferable
    if any(k in n for k in ["UNCLAIMED", "REDEMPTION"]):
        return "OTHER"

    return None

def run(force_plan_other: bool = True, force_option_other: bool = True):
    db: Session = SessionLocal()
    try:
        print("🔧 Fix v5 FORCE (NO OFFSET) starting...")

        total_plan_filled = 0
        total_plan_forced = 0
        total_option_filled = 0
        total_option_forced = 0

        round_no = 0
        while True:
            round_no += 1

            # ✅ IMPORTANT: NO OFFSET. Always fetch first N missing rows
            batch = (
                db.query(MfScheme)
                .filter(MfScheme.is_active == True)
                .filter(
                    (MfScheme.plan == None) | (MfScheme.plan == "") | (MfScheme.plan == "UNKNOWN")
                    | (MfScheme.option == None) | (MfScheme.option == "") | (MfScheme.option == "UNKNOWN")
                )
                .order_by(MfScheme.scheme_code.asc())
                .limit(BATCH_SIZE)
                .all()
            )

            if not batch:
                break

            plan_filled = plan_forced = option_filled = option_forced = 0

            for s in batch:
                name = s.scheme_name or ""

                # ---- PLAN ----
                if is_missing(s.plan):
                    gp = guess_plan(name)
                    if gp:
                        s.plan = gp
                        plan_filled += 1
                    elif force_plan_other:
                        s.plan = "OTHER"
                        plan_forced += 1

                # ---- OPTION ----
                if is_missing(s.option):
                    go = guess_option(name)
                    if go:
                        s.option = go
                        option_filled += 1
                    elif force_option_other:
                        s.option = "OTHER"
                        option_forced += 1

            db.commit()

            total_plan_filled += plan_filled
            total_plan_forced += plan_forced
            total_option_filled += option_filled
            total_option_forced += option_forced

            remaining = (
                db.query(MfScheme)
                .filter(MfScheme.is_active == True)
                .filter(
                    (MfScheme.plan == None) | (MfScheme.plan == "") | (MfScheme.plan == "UNKNOWN")
                    | (MfScheme.option == None) | (MfScheme.option == "") | (MfScheme.option == "UNKNOWN")
                )
                .count()
            )

            print(
                f"✅ round {round_no} | batch={len(batch)} | "
                f"plan_filled={plan_filled}, plan_forced={plan_forced} | "
                f"option_filled={option_filled}, option_forced={option_forced} | "
                f"remaining_missing={remaining}"
            )

        rem_plan = (
            db.query(MfScheme)
            .filter(MfScheme.is_active == True)
            .filter((MfScheme.plan == None) | (MfScheme.plan == "") | (MfScheme.plan == "UNKNOWN"))
            .count()
        )
        rem_opt = (
            db.query(MfScheme)
            .filter(MfScheme.is_active == True)
            .filter((MfScheme.option == None) | (MfScheme.option == "") | (MfScheme.option == "UNKNOWN"))
            .count()
        )

        print("\n✅ DONE Fix v5 FORCE (NO OFFSET)")
        print(f"  total_plan_filled={total_plan_filled} | total_plan_forced={total_plan_forced}")
        print(f"  total_option_filled={total_option_filled} | total_option_forced={total_option_forced}")
        print("\n📊 Remaining Missing After Fix v5")
        print(f"  plan missing/UNKNOWN  : {rem_plan}")
        print(f"  option missing/UNKNOWN: {rem_opt}")

    finally:
        db.close()

if __name__ == "__main__":
    run(force_plan_other=True, force_option_other=True)
