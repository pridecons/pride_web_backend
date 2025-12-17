# utils/NSE_Formater/data_ingestor.py

import os
import tempfile
from datetime import datetime, date
from typing import List, Dict, Any
from zoneinfo import ZoneInfo  # Python 3.9+

from sqlalchemy.orm import Session

from db.connection import SessionLocal
from db.models import (
    NseCmIntraday1Min,
    NseCmIndex1Min,
    NseCmSecurity,
    NseCmBhavcopy,
)
from sftp.NSE.sftp_client import SFTPClient
from utils.NSE_Formater.parser import parse_mkt, parse_ind
from utils.NSE_Formater.security_format import SecuritiesConverter

IST = ZoneInfo("Asia/Kolkata")

# Prices in snapshots (mkt / ind) are in paise -> divide by 100
PRICE_SCALE = 100.0


def _safe_price(v: int | float | None) -> float | None:
    """
    CM equity / index values usually in paise. Divide by 100.
    """
    if v is None:
        return None
    try:
        return float(v) / PRICE_SCALE
    except (TypeError, ValueError):
        return None

def _safe_pct(v: int | float | None) -> float | None:
    """
    Percentage change from index feed.

    - Raw feed kabhi kabhi absurd values deta hai (e.g. 42949672.63),
      jo numeric(10,4) range se bahar chali jaati hain.
    - Yahan hum:
        1) safely float me convert karte hain
        2) /100 karte hain (NSE feed usually *100 store karta hai)
        3) agar result 10000% se zyada / -10000% se kam hai to ignore (None)
    """
    if v is None:
        return None

    try:
        raw = float(v)
    except (TypeError, ValueError):
        return None

    # Kabhi-kabhi 32-bit overflow pattern wale values (4,294,967,2x) aate hain,
    # unko direct drop kar do.
    if abs(raw) > 1_000_000_00:  # > 1e8 = clearly garbage
        return None

    val = raw / 100.0  # convert to actual percent

    # Ab agar result bhi extremely large hai (e.g. > 10000%),
    # ya DB range ke bahar ho sakta hai, to bhi drop.
    if abs(val) >= 10_000:  # 10000% se upar = unrealistic, ignore
        return None

    # numeric(10,4) ke liye max abs < 1e6 hona chahiye
    if abs(val) >= 1_000_000:
        return None

    return val

def _nse_folder_name(trade_date: date) -> str:
    """date -> 'November242025'"""
    return trade_date.strftime("%B%d%Y")


def _parse_folder_date_from_path(remote_dir: str) -> date | None:
    """
    '/CM30/DATA/November242025' -> date(2025, 11, 24)
    """
    try:
        folder = os.path.basename(remote_dir.rstrip("/"))
        return datetime.strptime(folder, "%B%d%Y").date()
    except Exception:
        return None


# ======================================================================
#  CM BHAVCOPY CSV (Equities)  -> NseCmBhavcopy
# ======================================================================

import csv
import io


def parse_cm_bhavcopy_csv(content: bytes, trade_date: date) -> List[Dict[str, Any]]:
    """
    NSE CM bhavcopy CSV (Equities) ko parse karta hai.
    content: raw bytes of CSV file
    trade_date: date for which bhavcopy is downloaded
    """
    text = content.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))

    records: List[Dict[str, Any]] = []

    for row in reader:
        symbol = row.get("SYMBOL") or row.get("Symbol")
        series = row.get("SERIES") or row.get("Series")

        if not symbol:
            continue

        def _to_decimal(key: str):
            v = row.get(key)
            if v is None or v == "":
                return None
            try:
                return float(v)
            except ValueError:
                return None

        def _to_int(key: str):
            v = row.get(key)
            if v is None or v == "":
                return None
            try:
                return int(float(v))
            except ValueError:
                return None

        rec = {
            "trade_date": trade_date,
            "symbol": symbol.strip(),
            "series": (series or "").strip(),
            "open_price": _to_decimal("OPEN"),
            "high_price": _to_decimal("HIGH"),
            "low_price": _to_decimal("LOW"),
            "close_price": _to_decimal("CLOSE"),
            "last_price": _to_decimal("LAST"),
            "prev_close": _to_decimal("PREVCLOSE"),
            "total_traded_qty": _to_int("TOTTRDQTY"),
            "total_traded_value": _to_decimal("TOTTRDVAL"),
            "total_trades": _to_int("TOTALTRADES"),
            "isin": (row.get("ISIN") or "").strip() or None,
        }
        records.append(rec)

    return records


def process_cm_bhavcopy_for_date(trade_date: date) -> None:
    """
    Given a date, /CM/BHAV/ se bhavcopy CSV laata hai, parse karta hai,
    aur nse_cm_bhavcopy me upsert karta hai.
    """

    mon = trade_date.strftime("%b").upper()      # NOV
    dd = trade_date.strftime("%d")               # 24
    yyyy = trade_date.strftime("%Y")             # 2025

    file_name = f"cm{dd}{mon}{yyyy}bhav.csv"
    remote_dir = "/CM/BHAV"
    remote_path = f"{remote_dir}/{file_name}"

    sftp = SFTPClient()
    db: Session = SessionLocal()

    try:
        print(f"[CM-BHAV] Processing bhavcopy for {trade_date} -> {remote_path}")

        # 1) Download file
        try:
            file_bytes = sftp.download_file(remote_path)
        except Exception as e:
            print(f"[CM-BHAV] ERROR downloading {remote_path}: {e}")
            return

        # If compressed (.gz), you can add gzip.decompress here.

        # 2) Parse CSV
        records = parse_cm_bhavcopy_csv(file_bytes, trade_date)
        print(f"[CM-BHAV] Parsed {len(records)} bhavcopy records")

        if not records:
            return

        # 3) Preload security mapping for fast token lookup
        securities = db.query(NseCmSecurity).all()

        by_isin_series: dict[tuple[str, str], NseCmSecurity] = {}
        by_symbol_series: dict[tuple[str, str], NseCmSecurity] = {}

        for sec in securities:
            key1 = (sec.isin or "", (sec.series or "").upper())
            key2 = (sec.symbol.upper(), (sec.series or "").upper())
            by_isin_series[key1] = sec
            by_symbol_series[key2] = sec

        print(f"[CM-BHAV] Loaded {len(securities)} securities for mapping")

        inserted = 0
        updated = 0

        for r in records:
            sym = r["symbol"].upper()
            ser = (r["series"] or "").upper()
            isin = r.get("isin")

            sec = None
            token_id = None

            if isin:
                sec = by_isin_series.get((isin, ser))
            if sec is None:
                sec = by_symbol_series.get((sym, ser))

            if sec is not None:
                token_id = sec.token_id

            existing: NseCmBhavcopy | None = (
                db.query(NseCmBhavcopy)
                .filter(
                    NseCmBhavcopy.trade_date == r["trade_date"],
                    NseCmBhavcopy.symbol == r["symbol"],
                    NseCmBhavcopy.series == r["series"],
                )
                .one_or_none()
            )

            if existing is None:
                row = NseCmBhavcopy(
                    trade_date=r["trade_date"],
                    token_id=token_id,
                    symbol=r["symbol"],
                    series=r["series"],
                    open_price=r["open_price"],
                    high_price=r["high_price"],
                    low_price=r["low_price"],
                    close_price=r["close_price"],
                    last_price=r["last_price"],
                    prev_close=r["prev_close"],
                    total_traded_qty=r["total_traded_qty"],
                    total_traded_value=r["total_traded_value"],
                    total_trades=r["total_trades"],
                    isin=r["isin"],
                    delivery_data_available=False,
                )
                db.add(row)
                inserted += 1
            else:
                existing.token_id = token_id
                existing.open_price = r["open_price"]
                existing.high_price = r["high_price"]
                existing.low_price = r["low_price"]
                existing.close_price = r["close_price"]
                existing.last_price = r["last_price"]
                existing.prev_close = r["prev_close"]
                existing.total_traded_qty = r["total_traded_qty"]
                existing.total_traded_value = r["total_traded_value"]
                existing.total_trades = r["total_trades"]
                existing.isin = r["isin"]
                updated += 1

        db.commit()
        print(f"[CM-BHAV] Committed bhavcopy: inserted={inserted}, updated={updated}")

    except Exception as e:
        db.rollback()
        print(f"[CM-BHAV] ERROR for date {trade_date}: {e}")
        raise
    finally:
        db.close()
        sftp.close()


# ======================================================================
#  CM30 DATA: .mkt.gz  -> NseCmIntraday1Min (+ stub NseCmSecurity)
# ======================================================================

def process_cm30_mkt_folder(remote_dir: str) -> None:
    """
    Given /CM30/DATA/November242025 jaise folder,
    uske saare .mkt.gz files:
      - SFTP se download
      - parse_mkt se parse
      - nse_cm_intraday_1min me save
      - agar koi naya token_id mila jo nse_cm_securities me nahi hai,
        to uska ek stub security row create karo (FK satisfy karne ke liye).
    """
    sftp = SFTPClient()
    db: Session = SessionLocal()

    try:
        trade_date = _parse_folder_date_from_path(remote_dir)
        if not trade_date:
            trade_date = datetime.now(IST).date()

        print(f"[CM30-MKT] Processing folder: {remote_dir} (trade_date={trade_date})")

        sftp_paths = sftp.list_files(remote_dir)

        mkt_paths = sorted(
            [p for p in sftp_paths if p.lower().endswith(".mkt.gz")],
            key=lambda x: int(os.path.basename(x).split(".")[0]),
        )

        if not mkt_paths:
            print(f"[CM30-MKT] No .mkt.gz files in {remote_dir}")
            return

        # 1) Existing token_ids
        existing_token_ids = {t[0] for t in db.query(NseCmSecurity.token_id).all()}
        print(f"[CM30-MKT] Existing securities loaded: {len(existing_token_ids)}")

        for remote_path in mkt_paths:
            file_name = os.path.basename(remote_path)  # e.g. '123.mkt.gz'
            seq_str = file_name.split(".")[0]
            try:
                sequence_no = int(seq_str)
            except ValueError:
                sequence_no = None

            print(f"[CM30-MKT] Downloading {remote_path} (seq={sequence_no})")

            gz_bytes = sftp.download_file(remote_path)

            fd, tmp_path = tempfile.mkstemp(suffix=".mkt.gz")
            try:
                with os.fdopen(fd, "wb") as tmp:
                    tmp.write(gz_bytes)

                records: List[Dict[str, Any]] = parse_mkt(tmp_path)

            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

            print(f"[CM30-MKT] Parsed {len(records)} records from {file_name}")

            # 2) Ensure security master row exists per token
            for r in records:
                token_id = int(r["security_token"])

                if token_id not in existing_token_ids:
                    sec = NseCmSecurity(
                        token_id=token_id,
                        symbol=f"TOKEN_{token_id}",
                        series=None,
                        isin=None,
                        company_name=None,
                        lot_size=None,
                        face_value=None,
                        segment="CM",
                        active_flag=True,
                    )
                    db.add(sec)
                    existing_token_ids.add(token_id)

            # 3) Insert intraday bars
            for r in records:
                ts_utc = datetime.fromtimestamp(r["timestamp"])
                ts_ist = ts_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(IST)

                total_traded_qty = int(r.get("total_traded_quantity") or 0)
                interval_traded_qty = int(
                    r.get("interval_total_traded_quantity") or 0
                )

                bar = NseCmIntraday1Min(
                    trade_date=trade_date,
                    interval_start=ts_ist,
                    token_id=int(r["security_token"]),

                    # prices
                    last_price=_safe_price(r["last_traded_price"]),
                    best_bid_price=_safe_price(r["best_buy_price"]),
                    best_bid_qty=int(r["best_buy_quantity"] or 0),
                    best_ask_price=_safe_price(r["best_sell_price"]),
                    best_ask_qty=int(r["best_sell_quantity"] or 0),

                    # Volume: prefer interval quantity if present
                    volume=interval_traded_qty or total_traded_qty or None,
                    avg_price=_safe_price(r["average_traded_price"]),
                    open_price=_safe_price(
                        r.get("interval_open_price") or r.get("open_price")
                    ),
                    high_price=_safe_price(
                        r.get("interval_high_price") or r.get("high_price")
                    ),
                    low_price=_safe_price(
                        r.get("interval_low_price") or r.get("low_price")
                    ),
                    close_price=_safe_price(
                        r.get("interval_close_price") or r.get("close_price")
                    ),

                    # Extra fields
                    total_traded_qty=total_traded_qty or None,
                    interval_traded_qty=interval_traded_qty or None,
                    indicative_close_price=_safe_price(
                        r.get("indicative_close_price")
                    ),

                    # Not provided explicitly in this feed
                    value=None,
                    total_trades=None,
                    open_interest=None,
                )
                db.add(bar)

            db.commit()
            print(f"[CM30-MKT] Committed data for {file_name}")

        print(f"[CM30-MKT] Done folder {remote_dir}")

    except Exception as e:
        db.rollback()
        print(f"[CM30-MKT] ERROR in folder {remote_dir}: {e}")
        raise
    finally:
        db.close()
        sftp.close()


# ======================================================================
#  CM30 DATA: .ind.gz  -> NseCmIndex1Min
# ======================================================================

def process_cm30_ind_folder(remote_dir: str) -> None:
    """
    Given /CM30/DATA/November242025 jaise folder,
    uske saare .ind.gz files:
      - SFTP se download
      - parse_ind se parse
      - nse_cm_indices_1min me save
    """
    sftp = SFTPClient()
    db: Session = SessionLocal()

    try:
        trade_date = _parse_folder_date_from_path(remote_dir)
        if not trade_date:
            trade_date = datetime.now(IST).date()

        print(f"[CM30-IND] Processing folder: {remote_dir} (trade_date={trade_date})")

        sftp_paths = sftp.list_files(remote_dir)

        ind_paths = sorted(
            [p for p in sftp_paths if p.lower().endswith(".ind.gz")],
            key=lambda x: int(os.path.basename(x).split(".")[0]),
        )

        if not ind_paths:
            print(f"[CM30-IND] No .ind.gz files in {remote_dir}")
            return

        for remote_path in ind_paths:
            file_name = os.path.basename(remote_path)
            seq_str = file_name.split(".")[0]
            try:
                sequence_no = int(seq_str)
            except ValueError:
                sequence_no = None

            print(f"[CM30-IND] Downloading {remote_path} (seq={sequence_no})")
            gz_bytes = sftp.download_file(remote_path)

            fd, tmp_path = tempfile.mkstemp(suffix=".ind.gz")
            try:
                with os.fdopen(fd, "wb") as tmp:
                    tmp.write(gz_bytes)

                records: List[Dict[str, Any]] = parse_ind(tmp_path)
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

            print(f"[CM30-IND] Parsed {len(records)} records from {file_name}")

            for r in records:
                ts_utc = datetime.fromtimestamp(r["timestamp"])
                ts_ist = ts_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(IST)

                index_token = int(r["index_token"] or 0)

                idx = NseCmIndex1Min(
                    trade_date=trade_date,
                    interval_start=ts_ist,
                    index_id=index_token,
                    # Abhi ke liye index_name = token string; later NseIndexMaster se resolve kar sakte
                    index_name=str(index_token),

                    open_price=_safe_price(r["open_index_value"]),
                    high_price=_safe_price(r["high_index_value"]),
                    low_price=_safe_price(r["low_index_value"]),
                    close_price=_safe_price(
                        r.get("interval_close_index_value")
                        or r.get("current_index_value")
                    ),
                    last_price=_safe_price(r["current_index_value"]),
                    avg_price=None,

                    percentage_change=_safe_pct(r.get("percentage_change")),
                    indicative_close_value=_safe_price(
                        r.get("indicative_close_index_value")
                    ),

                    interval_open_price=_safe_price(
                        r.get("interval_open_index_value")
                    ),
                    interval_high_price=_safe_price(
                        r.get("interval_high_index_value")
                    ),
                    interval_low_price=_safe_price(
                        r.get("interval_low_index_value")
                    ),
                    interval_close_price=_safe_price(
                        r.get("interval_close_index_value")
                    ),

                    volume=None,
                    turnover=None,
                )
                db.add(idx)

            db.commit()

        print(f"[CM30-IND] Done folder {remote_dir}")

    except Exception as e:
        db.rollback()
        print(f"[CM30-IND] ERROR in folder {remote_dir}: {e}")
        raise
    finally:
        db.close()
        sftp.close()


# ======================================================================
#  Helper: One-shot for a given trade_date (today, backfill, etc.)
# ======================================================================

def process_cm30_for_date(trade_date: date) -> None:
    """
    date -> /CM30/DATA/<MonthDDYYYY> -> process .mkt.gz + .ind.gz
    """
    folder_name = _nse_folder_name(trade_date)
    remote_dir = f"/CM30/DATA/{folder_name}"
    process_cm30_mkt_folder(remote_dir)
    process_cm30_ind_folder(remote_dir)


# ======================================================================
#  Securities.dat parsing + upsert into NseCmSecurity (binary)
# ======================================================================

def process_cm30_security_for_date(trade_date: date) -> None:
    """
    date -> /CM30/SECURITY/<MonthDDYYYY>/Securities.dat
    NSE binary Securities.dat ko parse karke nse_cm_securities me upsert karega.
    """
    print("trade_date :: ",trade_date)
    folder_name = _nse_folder_name(trade_date)
    remote_dir = f"/CM30/SECURITY/{folder_name}"
    remote_file = f"{remote_dir}/Securities.dat"

    sftp = SFTPClient()
    db: Session = SessionLocal()
    local_path = None

    try:
        print(f"[CM30-SEC] Processing Securities master for {trade_date} ({remote_file})")

        # --- SFTP: download binary file ---
        try:
            file_bytes = sftp.download_file(remote_file)
        except Exception as e:
            print(f"[CM30-SEC] ERROR downloading {remote_file}: {e}")
            return

        # --- Local temp file me save karo (converter file path leta hai) ---
        fd, local_path = tempfile.mkstemp(suffix=".dat")
        with os.fdopen(fd, "wb") as tmp:
            tmp.write(file_bytes)

        conv = SecuritiesConverter()

        # 1) Try proper header-based dynamic parsing
        securities = conv.extract_securities_dynamic(local_path)

        # 2) Agar kuch nahi mila to brute-force fallback
        if not securities:
            print("[CM30-SEC] extract_securities_dynamic returned 0, trying alternative parsing...")
            securities = conv.try_alternative_parsing(local_path)

        if not securities:
            print("[CM30-SEC] ❌ No securities parsed from Securities.dat")
            return

        print(f"[CM30-SEC] Parsed {len(securities)} securities from {remote_file}")

        upserted = 0
        for rec in securities:
            try:
                token_id = int(rec["token_number"])
            except (KeyError, ValueError, TypeError):
                continue

            symbol = (rec.get("symbol") or "").strip()
            if not symbol:
                # model me symbol nullable=False hai, to blank waale skip kar dete hain
                continue

            series = (rec.get("series") or "").strip() or None
            company_name = (rec.get("company_name") or "").strip() or None

            issued_capital = rec.get("issued_capital")
            settlement_cycle = rec.get("settlement_cycle")
            permitted_to_trade = rec.get("permitted_to_trade")

            sec: NseCmSecurity | None = (
                db.query(NseCmSecurity)
                .filter(NseCmSecurity.token_id == token_id)
                .one_or_none()
            )

            if sec is None:
                # INSERT
                sec = NseCmSecurity(
                    token_id=token_id,
                    symbol=symbol,
                    series=series,
                    isin=None,              # abhi binary se ISIN nahi nikal rahe
                    company_name=company_name,
                    lot_size=None,
                    face_value=None,
                    segment="CM",
                    active_flag=True,
                    issued_capital=issued_capital,
                    settlement_cycle=settlement_cycle,
                    permitted_to_trade=permitted_to_trade,
                )
                db.add(sec)
            else:
                # UPDATE (sirf non-empty values overwrite karein)
                sec.symbol = symbol or sec.symbol
                if series:
                    sec.series = series
                if company_name:
                    sec.company_name = company_name
                if issued_capital is not None:
                    sec.issued_capital = issued_capital
                if settlement_cycle is not None:
                    sec.settlement_cycle = settlement_cycle
                if permitted_to_trade is not None:
                    sec.permitted_to_trade = permitted_to_trade
                sec.active_flag = True

            upserted += 1

        db.commit()
        print(f"[CM30-SEC] ✅ Upserted {upserted} records into nse_cm_securities")

    except Exception as e:
        db.rollback()
        print(f"[CM30-SEC] ERROR for date {trade_date}: {e}")
        raise
    finally:
        db.close()
        sftp.close()
        if local_path:
            try:
                os.remove(local_path)
            except OSError:
                pass
