from __future__ import annotations

import csv
import re
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime, date
from typing import Any, Dict, Optional, List, Tuple

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from db.connection import get_db
from db.models import NseCmBhavcopy


DATA_DIR = Path("/home/pride/data_intraday/nse_bhavdata")

# ✅ only data files (not logs)
FILE_RE = re.compile(r"^sec_bhavdata_full_(\d{8})\.csv$", re.IGNORECASE)

CHUNK_SIZE = 2000


# ---------------- helpers ----------------

def parse_ddmmyyyy_from_filename(p: Path) -> Optional[date]:
    m = FILE_RE.match(p.name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%d%m%Y").date()
    except Exception:
        return None


def to_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.upper() in {"NA", "N/A", "NULL", "-"}:
        return None
    s = s.replace(",", "")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def to_int(v: Any) -> Optional[int]:
    d = to_decimal(v)
    if d is None:
        return None
    try:
        return int(d)
    except Exception:
        return None


def parse_date1(s: Any) -> Optional[date]:
    """
    Some bhavcopy formats have DATE1 like '07-Jan-2026'
    """
    if s is None:
        return None
    v = str(s).strip()
    if not v:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(v, fmt).date()
        except Exception:
            pass
    return None


def get_val(row: Dict[str, Any], *keys: str) -> Any:
    """
    Return first existing key from row (case-insensitive & space/underscore tolerant).
    """
    # normalize available headers once per call
    norm = {}
    for k in row.keys():
        nk = str(k).strip().lower().replace(" ", "").replace("_", "")
        norm[nk] = k

    for key in keys:
        nk = key.strip().lower().replace(" ", "").replace("_", "")
        if nk in norm:
            return row.get(norm[nk])
    return None


def row_to_payload(row: Dict[str, Any], fallback_trade_date: date) -> Optional[Dict[str, Any]]:
    """
    Works with both:
    - sec_bhavdata_full (usually NO DATE1 -> date from filename)
    - cm bhavcopy (DATE1 present)
    """

    # 1) trade date
    trade_date = parse_date1(get_val(row, "DATE1")) or fallback_trade_date

    # 2) symbol/series
    symbol = (get_val(row, "SYMBOL") or "").strip()
    series = (get_val(row, "SERIES") or "").strip() or None

    if not trade_date or not symbol:
        return None

    # 3) map columns with fallbacks
    open_p = to_decimal(get_val(row, "OPEN_PRICE", "OPEN"))
    high_p = to_decimal(get_val(row, "HIGH_PRICE", "HIGH"))
    low_p = to_decimal(get_val(row, "LOW_PRICE", "LOW"))
    close_p = to_decimal(get_val(row, "CLOSE_PRICE", "CLOSE"))
    last_p = to_decimal(get_val(row, "LAST_PRICE", "LAST"))

    prev_close = to_decimal(get_val(row, "PREV_CLOSE", "PREVCLOSE", "PREV. CLOSE", "PREV_CLOSE_PRICE"))

    qty = to_int(get_val(row, "TTL_TRD_QNTY", "TOTTRDQTY", "TOTAL_TRADED_QTY"))
    trades = to_int(get_val(row, "NO_OF_TRADES", "TOTALTRADES", "TOTAL_TRADES"))

    # value: some files have TOTTRDVAL (rupees), some have TURNOVER_LACS (lacs)
    val_rupees = to_decimal(get_val(row, "TOTTRDVAL"))
    val_lacs = to_decimal(get_val(row, "TURNOVER_LACS"))

    total_value = val_rupees if val_rupees is not None else val_lacs  # keep as-is (as you want)

    isin = (get_val(row, "ISIN") or get_val(row, "ISIN_CODE") or "")
    isin = str(isin).strip() or None

    return {
        "trade_date": trade_date,
        "symbol": symbol,
        "series": series,

        "open_price": open_p,
        "high_price": high_p,
        "low_price": low_p,
        "close_price": close_p,
        "last_price": last_p,
        "prev_close": prev_close,

        "total_traded_qty": qty,
        "total_traded_value": total_value,
        "total_trades": trades,

        "isin": isin,

        "updated_at": datetime.utcnow(),
        "created_at": datetime.utcnow(),
    }


# ---------------- upsert ----------------

def upsert_chunk(db: Session, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    table = NseCmBhavcopy.__table__
    stmt = insert(table).values(rows)

    update_cols = {
        c.name: getattr(stmt.excluded, c.name)
        for c in table.columns
        if c.name not in {"id", "created_at"}
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=["trade_date", "symbol", "series"],
        set_=update_cols,
    )
    res = db.execute(stmt)
    return res.rowcount or 0


def import_file(db: Session, fp: Path, print_headers_once: bool = False) -> Tuple[int, int]:
    file_date = parse_ddmmyyyy_from_filename(fp)
    if not file_date:
        # should never happen because we filter by FILE_RE
        return 0, 0

    done = 0
    skipped = 0
    buf: List[Dict[str, Any]] = []

    with fp.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if print_headers_once:
            print(f"\n🧾 Headers sample ({fp.name}): {reader.fieldnames}\n")

        for row in reader:
            payload = row_to_payload(row, file_date)
            if not payload:
                skipped += 1
                continue

            buf.append(payload)
            if len(buf) >= CHUNK_SIZE:
                done += upsert_chunk(db, buf)
                buf.clear()

        if buf:
            done += upsert_chunk(db, buf)
            buf.clear()

    return done, skipped


def find_data_files(root: Path) -> List[Path]:
    files = [p for p in root.rglob("*.csv") if FILE_RE.match(p.name)]
    files.sort()
    return files


def run_import(root: Path):
    files = find_data_files(root)
    if not files:
        print(f"❌ No sec_bhavdata_full_*.csv files found in: {root}")
        return

    db_gen = get_db()
    db: Session = next(db_gen)

    total_files = 0
    total_upserted = 0
    total_skipped = 0

    try:
        for i, fp in enumerate(files):
            total_files += 1
            try:
                upserted, skipped = import_file(db, fp, print_headers_once=(i == 0))
                db.commit()
                total_upserted += upserted
                total_skipped += skipped
                print(f"✅ {fp.name} | upserted: {upserted} | skipped: {skipped}")
            except Exception as e:
                db.rollback()
                print(f"❌ {fp.name} failed: {e}")

        print("\n================ SUMMARY ================")
        print(f"Files processed: {total_files}")
        print(f"Upserted rows:   {total_upserted}")
        print(f"Skipped rows:    {total_skipped}")

    finally:
        try:
            db.close()
        except Exception:
            pass
        try:
            next(db_gen)
        except Exception:
            pass


if __name__ == "__main__":
    run_import(DATA_DIR)
