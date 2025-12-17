# utils/NSE_Formater/bhavcopy_ingestor.py

import csv
import io
from datetime import date
from typing import List, Dict, Any

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert  # ✅ UPSERT ke liye

from db.connection import SessionLocal
from db.models import NseCmBhavcopy, NseCmSecurity
from sftp.NSE.sftp_client import SFTPClient


def _to_float_safe(val: str | None) -> float | None:
    if val is None:
        return None
    val = str(val).strip()
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _to_int_safe(val: str | None) -> int | None:
    if val is None:
        return None
    val = str(val).strip()
    if not val:
        return None
    try:
        return int(float(val))
    except ValueError:
        return None


def parse_cm_bhavcopy(content: bytes, trade_date: date) -> List[Dict[str, Any]]:
    """
    NSE CM bhavcopy ko parse kare.

    2 format support karta hai:

    1) Header-based CSV / pipe:
       SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE,
       TOTTRDQTY, TOTTRDVAL, TOTALTRADES, ISIN

    2) Snapshot text (jaise tumhare CMBhavcopy_24112025.txt):
       Example line:
         0MOFSL26  N1   1130.00   1110.20   1110.20   1130.00   1131.00          30                 33603.00

       Hum isko as:
         code        = "0MOFSL26"
           - market_type = code[0]
           - symbol_raw  = code[1:]  (e.g. "MOFSL26")
         series      = "N1"
         open        = 1130.00
         high        = 1110.20
         low         = 1110.20
         close       = 1130.00
         prev_close  = 1131.00
         tottrdqty   = 30
         tottrdval   = 33603.00
         total_trades = None (file me nahi dikh raha)
    """
    text = content.decode("utf-8", errors="ignore")
    lines = [ln.rstrip("\r") for ln in text.splitlines() if ln.strip()]

    if not lines:
        print("[CM-BHAV] File has no non-empty lines.")
        return []

    first_line = lines[0]
    upper_first = first_line.upper()

    # --------------------------
    # CASE 1: HEADER BASED CSV
    # --------------------------
    if "SYMBOL" in upper_first and ("," in first_line or "|" in first_line):
        # delimiter detect
        if "|" in first_line and first_line.count("|") >= first_line.count(","):
            delimiter = "|"
        else:
            delimiter = ","

        print(f"[CM-BHAV] Detected header-based format, delimiter='{delimiter}'")
        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        print(f"[CM-BHAV] Detected columns: {reader.fieldnames}")

        records: List[Dict[str, Any]] = []

        for row in reader:
            symbol = row.get("SYMBOL") or row.get("Symbol")
            series = row.get("SERIES") or row.get("Series")

            if not symbol:
                continue

            rec = {
                "trade_date": trade_date,
                "symbol": symbol.strip(),
                "series": (series or "").strip(),
                "open_price": _to_float_safe(row.get("OPEN")),
                "high_price": _to_float_safe(row.get("HIGH")),
                "low_price": _to_float_safe(row.get("LOW")),
                "close_price": _to_float_safe(row.get("CLOSE")),
                "last_price": _to_float_safe(row.get("LAST")),
                "prev_close": _to_float_safe(row.get("PREVCLOSE")),
                "total_traded_qty": _to_int_safe(row.get("TOTTRDQTY")),
                "total_traded_value": _to_float_safe(row.get("TOTTRDVAL")),
                "total_trades": _to_int_safe(row.get("TOTALTRADES")),
                "isin": (row.get("ISIN") or "").strip() or None,
            }
            records.append(rec)

        print(f"[CM-BHAV] parse_cm_bhavcopy (header) → {len(records)} records")
        return records

    # --------------------------
    # CASE 2: SNAPSHOT TXT (NO HEADER)
    # --------------------------
    print("[CM-BHAV] No header with SYMBOL found, assuming snapshot text format.")
    records: List[Dict[str, Any]] = []

    for line in lines:
        parts = line.split()
        # Expect at least: code, series, 5 prices, qty, val => 9 fields minimum
        if len(parts) < 9:
            # Extra-safe: skip short/weird lines
            continue

        code = parts[0]
        series = parts[1]

        if not code or len(code) < 2:
            continue

        # First char often market_type (0/1/etc.), baaki symbol-ish part
        market_type = code[0]
        symbol_raw = code[1:]  # e.g. "MOFSL26" etc.

        # Numeric mapping
        open_str = parts[2]
        high_str = parts[3]
        low_str = parts[4]
        close_str = parts[5]
        prevclose_str = parts[6]
        qty_str = parts[7]
        val_str = parts[8]

        open_price = _to_float_safe(open_str)
        high_price = _to_float_safe(high_str)
        low_price = _to_float_safe(low_str)
        close_price = _to_float_safe(close_str)
        prev_close = _to_float_safe(prevclose_str)
        total_qty = _to_int_safe(qty_str)
        total_val = _to_float_safe(val_str)

        # Snapshot bhavcopy me "LAST" alag na ho, to abhi close hi rakh dete hain
        last_price = close_price

        rec = {
            "trade_date": trade_date,
            "symbol": symbol_raw.strip(),   # master ke symbol se match karega (e.g. "MOFSL26")
            "series": (series or "").strip(),
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "last_price": last_price,
            "prev_close": prev_close,
            "total_traded_qty": total_qty,
            "total_traded_value": total_val,
            "total_trades": None,   # is format me nahi dikh raha
            "isin": None,
        }
        records.append(rec)

    print(f"[CM-BHAV] parse_cm_bhavcopy (snapshot) → {len(records)} records")
    return records


def process_cm_bhavcopy_for_date(trade_date: date) -> None:
    """
    Given a date, CM30 bhavcopy ko SFTP se laata hai, parse karta hai,
    aur nse_cm_bhavcopy table me insert/update karta hai.

    Tumhare SFTP structure ke hisaab se (example se):
      /CM30/BHAVCOPY/November182025/CMBhavcopy_18112025.txt

    Ab yahan:
      - In-memory dedup by (trade_date, symbol, series)
      - Postgres UPSERT (ON CONFLICT) use kar rahe hain
        → koi UniqueViolation nahi aayega
    """

    # Folder name: MonthNameDDYYYY  -> November182025
    folder_name = trade_date.strftime("%B%d%Y")  # e.g. "November182025"

    dd = trade_date.strftime("%d")   # "18"
    mm = trade_date.strftime("%m")   # "11"
    yyyy = trade_date.strftime("%Y") # "2025"

    # Base directory for CM30 bhavcopy
    remote_dir = f"/CM30/BHAVCOPY/{folder_name}"

    # Tumhare example ke hisaab se primary file:
    #   CMBhavcopy_18112025.txt
    file_candidates = [
        f"CMBhavcopy_{dd}{mm}{yyyy}.txt",
        f"CMBhavcopy_{dd}{mm}{yyyy}.csv",  # fallback agar kabhi csv extension ho
    ]

    sftp = SFTPClient()
    db: Session = SessionLocal()

    try:
        print(f"[CM-BHAV] Processing bhavcopy for {trade_date}")
        print(f"[CM-BHAV] Looking in dir: {remote_dir}")

        file_bytes = None
        used_remote_path = None

        # 🟢 1) Correct file khojo (txt / csv dono try)
        for fname in file_candidates:
            remote_path = f"{remote_dir}/{fname}"
            try:
                print(f"[CM-BHAV] Trying: {remote_path}")
                file_bytes = sftp.download_file(remote_path)
                used_remote_path = remote_path
                print(f"[CM-BHAV] ✅ Found & downloaded: {remote_path}")
                break
            except Exception as e:
                print(f"[CM-BHAV] Not found / error: {remote_path} -> {e}")

        if not file_bytes:
            print(f"[CM-BHAV] ❌ No bhavcopy file found for {trade_date} in {remote_dir}")
            return

        # 🟢 2) Parse content (CSV/header ya snapshot text)
        records = parse_cm_bhavcopy(file_bytes, trade_date)
        print(f"[CM-BHAV] Parsed {len(records)} bhavcopy records from {used_remote_path}")

        if not records:
            print("[CM-BHAV] No records parsed. Exiting.")
            return

        # 🟢 2.1 In-memory DEDUPLICATION by (trade_date, symbol, series)
        dedup_map: Dict[tuple, Dict[str, Any]] = {}
        duplicate_count = 0

        for r in records:
            key = (r["trade_date"], r["symbol"], r["series"] or "")
            if key in dedup_map:
                duplicate_count += 1
                # Latest line wins
            dedup_map[key] = r

        deduped_records = list(dedup_map.values())
        print(
            f"[CM-BHAV] Deduped on (trade_date, symbol, series): "
            f"original={len(records)}, deduped={len(deduped_records)}, "
            f"duplicates_skipped={duplicate_count}"
        )

        if not deduped_records:
            print("[CM-BHAV] Deduped record list empty. Exiting.")
            return

        # 🟢 3) Preload security mapping for fast token lookup
        #    - Prefer ISIN + series (agar future me aaye)
        #    - Fallback symbol + series
        securities = db.query(NseCmSecurity).all()

        by_isin_series: Dict[tuple, NseCmSecurity] = {}
        by_symbol_series: Dict[tuple, NseCmSecurity] = {}

        for sec in securities:
            key1 = (sec.isin or "", (sec.series or "").upper())
            key2 = (sec.symbol.upper(), (sec.series or "").upper())
            by_isin_series[key1] = sec
            by_symbol_series[key2] = sec

        print(f"[CM-BHAV] Loaded {len(securities)} securities for mapping")

        # 🟢 4) Build rows for bulk UPSERT
        bhav_table = NseCmBhavcopy.__table__
        rows_to_insert: List[Dict[str, Any]] = []

        for r in deduped_records:
            sym = r["symbol"].upper()
            ser = (r["series"] or "").upper()
            isin = r.get("isin")

            sec = None
            token_id = None

            # ISIN + series first
            if isin:
                sec = by_isin_series.get((isin, ser))
            # fallback: symbol + series
            if sec is None:
                sec = by_symbol_series.get((sym, ser))

            if sec is not None:
                token_id = sec.token_id

            rows_to_insert.append(
                {
                    "trade_date": r["trade_date"],
                    "token_id": token_id,
                    "symbol": r["symbol"],
                    "series": r["series"],
                    "open_price": r["open_price"],
                    "high_price": r["high_price"],
                    "low_price": r["low_price"],
                    "close_price": r["close_price"],
                    "last_price": r["last_price"],
                    "prev_close": r["prev_close"],
                    "total_traded_qty": r["total_traded_qty"],
                    "total_traded_value": r["total_traded_value"],
                    "total_trades": r["total_trades"],
                    "isin": r["isin"],
                    "delivery_data_available": False,
                }
            )

        print(f"[CM-BHAV] Prepared {len(rows_to_insert)} rows for UPSERT")

        if not rows_to_insert:
            print("[CM-BHAV] No rows to upsert. Exiting.")
            return

        # 🟢 5) PostgreSQL UPSERT: ON CONFLICT (trade_date, symbol, series)
        stmt = insert(bhav_table).values(rows_to_insert)

        # Columns to update on conflict
        excluded = stmt.excluded
        update_cols = {
            "token_id": excluded.token_id,
            "open_price": excluded.open_price,
            "high_price": excluded.high_price,
            "low_price": excluded.low_price,
            "close_price": excluded.close_price,
            "last_price": excluded.last_price,
            "prev_close": excluded.prev_close,
            "total_traded_qty": excluded.total_traded_qty,
            "total_traded_value": excluded.total_traded_value,
            "total_trades": excluded.total_trades,
            "isin": excluded.isin,
            "delivery_data_available": excluded.delivery_data_available,
            "updated_at": excluded.updated_at,  # column default handle karega
        }

        stmt = stmt.on_conflict_do_update(
            constraint="uq_nse_cm_bhavcopy_date_symbol_series",
            set_=update_cols,
        )

        result = db.execute(stmt)
        db.commit()

        print(
            f"[CM-BHAV] ✅ UPSERT complete for {trade_date}: "
            f"rows={len(rows_to_insert)} (deduped, conflict-safe)"
        )

    except Exception as e:
        db.rollback()
        print(f"[CM-BHAV] ❌ ERROR for date {trade_date}: {e}")
        raise
    finally:
        db.close()
        sftp.close()
