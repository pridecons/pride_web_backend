# utils/NSE_Formater/data_ingestor.py

import os
import csv
import io
import gzip
import tempfile
import json
import redis
from datetime import datetime, date, timezone
from typing import List, Dict, Any, Optional
from zoneinfo import ZoneInfo  # Python 3.9+

from sqlalchemy.orm import Session
from sqlalchemy import select

from db.connection import SessionLocal
from db.models import NseIngestionLog
from db.models import (
    NseCmIntraday1Min,
    NseCmIndex1Min,
    NseCmSecurity,
    NseCmBhavcopy,
)
from sftp.NSE.sftp_client import SFTPClient
from utils.NSE_Formater.parser import parse_mkt, parse_ind
from utils.NSE_Formater.security_format import SecuritiesConverter
from sqlalchemy.sql import expression

IST = ZoneInfo("Asia/Kolkata")

# Prices in snapshots (mkt / ind) are in paise -> divide by 100
PRICE_SCALE = 100.0

# ======================================================================
#  REDIS (LIVE CACHE + PUBSUB)
# ======================================================================

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
LIVE_TTL_SECONDS = int(os.getenv("LIVE_TTL_SECONDS", "21600"))  # 6 hours default
LIVE_PUBLISH = os.getenv("LIVE_PUBLISH", "true").lower() in ("1", "true", "yes", "y")

def get_redis() -> redis.Redis:
    # decode_responses=True -> str in/out (easy for hash + JSON)
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)

def _hset_mapping_str(pipe, key: str, payload: Dict[str, Any]):
    # Redis hash mapping requires string/bytes/int/float; we convert None -> ""
    mapping = {k: ("" if v is None else str(v)) for k, v in payload.items()}
    pipe.hset(key, mapping=mapping)

def publish_latest_quotes_by_symbol(
    db: Session,
    trade_date: date,
    seq: int,
    bars: List[NseCmIntraday1Min],
) -> None:
    """
    Publish only the LATEST snapshot per token from this file to Redis,
    but keyed & broadcast by SYMBOL (not token_id).

    - Cache key: live:cm:symbol:<SYMBOL>
    - PubSub channel: pub:cm:symbol:<SYMBOL>
    Payload contains token_id + symbol + ltp/bid/ask/ts etc.
    """
    if not LIVE_PUBLISH or not bars:
        return

    # 1) de-dupe: last bar per token wins (reduces flood)
    latest_by_token: dict[int, NseCmIntraday1Min] = {}
    for b in bars:
        if b is None:
            continue
        # only publish if we have a meaningful last_price
        if b.last_price is None:
            continue
        latest_by_token[int(b.token_id)] = b

    if not latest_by_token:
        return

    token_ids = list(latest_by_token.keys())

    # 2) map token_id -> symbol (from DB master)
    #    NOTE: stub securities may have symbol=str(token_id) until security master runs.
    rows = (
        db.query(NseCmSecurity.token_id, NseCmSecurity.symbol)
        .filter(NseCmSecurity.token_id.in_(token_ids))
        .all()
    )
    token_to_symbol = {int(t): (s or str(t)).upper() for (t, s) in rows}

    # 3) push to redis: cache hash + publish json
    rds = get_redis()
    pipe = rds.pipeline(transaction=False)

    for token_id, b in latest_by_token.items():
        sym = token_to_symbol.get(token_id, str(token_id)).upper()

        payload = {
            "symbol": sym,
            "token_id": token_id,
            "ltp": float(b.last_price) if b.last_price is not None else None,
            "bid": float(b.best_bid_price) if b.best_bid_price is not None else None,
            "ask": float(b.best_ask_price) if b.best_ask_price is not None else None,
            "bid_qty": int(b.best_bid_qty) if b.best_bid_qty is not None else None,
            "ask_qty": int(b.best_ask_qty) if b.best_ask_qty is not None else None,
            "volume": int(b.volume) if b.volume is not None else None,
            "avg": float(b.avg_price) if b.avg_price is not None else None,
            "o": float(b.open_price) if b.open_price is not None else None,
            "h": float(b.high_price) if b.high_price is not None else None,
            "l": float(b.low_price) if b.low_price is not None else None,
            "c": float(b.close_price) if b.close_price is not None else None,
            "indicative_close": float(b.indicative_close_price) if b.indicative_close_price is not None else None,
            "ts": b.interval_start.isoformat(),
            "trade_date": str(trade_date),
            "seq": seq,
        }

        cache_key = f"live:cm:symbol:{sym}"
        channel = f"pub:cm:symbol:{sym}"

        _hset_mapping_str(pipe, cache_key, payload)
        pipe.expire(cache_key, LIVE_TTL_SECONDS)

        # publish compact JSON
        pipe.publish(channel, json.dumps(payload, separators=(",", ":")))

    pipe.execute()


# ======================================================================
#  Generic helpers
# ======================================================================

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


def _safe_div(v: int | float | None, div: float) -> float | None:
    if v is None:
        return None
    try:
        return float(v) / div
    except (TypeError, ValueError):
        return None


def _epoch_to_date(v: int | float | None) -> date | None:
    """
    NSE epoch seconds -> UTC date
    0 / None -> None
    """
    if v is None:
        return None
    try:
        iv = int(float(v))
    except (TypeError, ValueError):
        return None
    if iv <= 0:
        return None
    return datetime.utcfromtimestamp(iv).date()


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

    # 32-bit overflow pattern / garbage
    if abs(raw) > 1_000_000_00:  # > 1e8
        return None

    val = raw / 100.0  # convert to actual percent

    if abs(val) >= 10_000:
        return None

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


def _maybe_gunzip(content: bytes, file_name: str) -> bytes:
    """
    If file is gzipped (endswith .gz), decompress.
    """
    if file_name.lower().endswith(".gz"):
        try:
            return gzip.decompress(content)
        except Exception:
            return content
    return content


# ======================================================================
#  CM BHAVCOPY CSV (Equities)  -> NseCmBhavcopy
# ======================================================================

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

        try:
            file_bytes = sftp.download_file(remote_path)
        except Exception as e:
            print(f"[CM-BHAV] ERROR downloading {remote_path}: {e}")
            return

        file_bytes = _maybe_gunzip(file_bytes, file_name)

        records = parse_cm_bhavcopy_csv(file_bytes, trade_date)
        print(f"[CM-BHAV] Parsed {len(records)} bhavcopy records")
        if not records:
            return

        # preload mapping
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
    sftp = SFTPClient()
    db: Session = SessionLocal()

    try:
        trade_date = _parse_folder_date_from_path(remote_dir) or datetime.now(IST).date()
        print(f"[CM30-MKT] Processing folder: {remote_dir} (trade_date={trade_date})")

        sftp_paths = sftp.list_files(remote_dir)
        mkt_paths = sorted(
            [p for p in sftp_paths if p.lower().endswith(".mkt.gz")],
            key=lambda x: int(os.path.basename(x).split(".")[0]),
        )

        if not mkt_paths:
            print(f"[CM30-MKT] No .mkt.gz files in {remote_dir}")
            return

        # ✅ Already processed seq list (one query)
        done_seqs = {
            r[0]
            for r in db.query(NseIngestionLog.seq)
            .filter(
                NseIngestionLog.trade_date == trade_date,
                NseIngestionLog.segment == "CM30_MKT",
            )
            .all()
        }

        # ✅ Existing token_ids cache
        existing_token_ids = {t[0] for t in db.query(NseCmSecurity.token_id).all()}

        skipped = 0
        processed = 0

        for remote_path in mkt_paths:
            file_name = os.path.basename(remote_path)  # "37.mkt.gz"
            seq_str = file_name.split(".")[0]

            try:
                seq = int(seq_str)
            except ValueError:
                continue

            # ✅ SKIP if already processed
            if seq in done_seqs:
                skipped += 1
                continue

            print(f"[CM30-MKT] Downloading {remote_path} (seq={seq})")
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

            # Ensure security master exists (stubs)
            new_secs = []
            for r in records:
                token_id = int(r["security_token"])
                if token_id not in existing_token_ids:
                    new_secs.append(
                        NseCmSecurity(
                            token_id=token_id,
                            symbol=str(token_id),
                            series=None,
                            isin=None,
                            company_name=None,
                            lot_size=None,
                            face_value=None,
                            segment="CM",
                            active_flag=True,
                        )
                    )
                    existing_token_ids.add(token_id)

            if new_secs:
                db.bulk_save_objects(new_secs)

            # Insert intraday bars (no duplicate protection here; file-level log prevents duplicates)
            bars: List[NseCmIntraday1Min] = []
            for r in records:
                ts_ist = datetime.fromtimestamp(int(r["timestamp"]), tz=timezone.utc).astimezone(IST)

                total_traded_qty = int(r.get("total_traded_quantity") or 0)
                interval_traded_qty = int(r.get("interval_total_traded_quantity") or 0)

                bars.append(
                    NseCmIntraday1Min(
                        trade_date=trade_date,
                        interval_start=ts_ist,
                        token_id=int(r["security_token"]),
                        last_price=_safe_price(r["last_traded_price"]),
                        best_bid_price=_safe_price(r["best_buy_price"]),
                        best_bid_qty=int(r["best_buy_quantity"] or 0),
                        best_ask_price=_safe_price(r["best_sell_price"]),
                        best_ask_qty=int(r["best_sell_quantity"] or 0),
                        volume=interval_traded_qty or total_traded_qty or None,
                        avg_price=_safe_price(r["average_traded_price"]),
                        open_price=_safe_price(r.get("interval_open_price") or r.get("open_price")),
                        high_price=_safe_price(r.get("interval_high_price") or r.get("high_price")),
                        low_price=_safe_price(r.get("interval_low_price") or r.get("low_price")),
                        close_price=_safe_price(r.get("interval_close_price") or r.get("close_price")),
                        total_traded_qty=total_traded_qty or None,
                        interval_traded_qty=interval_traded_qty or None,
                        indicative_close_price=_safe_price(r.get("indicative_close_price")),
                        value=None,
                        total_trades=None,
                        open_interest=None,
                    )
                )

            if bars:
                db.bulk_save_objects(bars)

            # ✅ Mark this file seq as processed (UNIQUE prevents double insert)
            db.add(
                NseIngestionLog(
                    trade_date=trade_date,
                    segment="CM30_MKT",
                    seq=seq,
                    remote_path=remote_path,
                )
            )

            db.commit()
            done_seqs.add(seq)
            processed += 1
            print(f"[CM30-MKT] ✅ Committed data for {file_name}")

            # ✅ LIVE: publish latest by SYMBOL to Redis (cache + pubsub)
            try:
                publish_latest_quotes_by_symbol(db=db, trade_date=trade_date, seq=seq, bars=bars)
            except Exception as e:
                # do not break ingestion on redis issues
                print(f"[CM30-MKT] ⚠️ LIVE publish failed for seq={seq}: {e}")

        print(f"[CM30-MKT] Done folder {remote_dir} | processed={processed}, skipped={skipped}")

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
    /CM30/DATA/<MonthDDYYYY> folder:
      - download all .ind.gz
      - parse_ind
      - store into nse_cm_indices_1min
      - ✅ skip already processed seq using NseIngestionLog
    """
    sftp = SFTPClient()
    db: Session = SessionLocal()

    try:
        trade_date = _parse_folder_date_from_path(remote_dir) or datetime.now(IST).date()
        print(f"[CM30-IND] Processing folder: {remote_dir} (trade_date={trade_date})")

        sftp_paths = sftp.list_files(remote_dir)

        ind_paths = sorted(
            [p for p in sftp_paths if p.lower().endswith(".ind.gz")],
            key=lambda x: int(os.path.basename(x).split(".")[0]),
        )

        if not ind_paths:
            print(f"[CM30-IND] No .ind.gz files in {remote_dir}")
            return

        # ✅ Already processed seq list (one query)
        done_seqs = {
            r[0]
            for r in db.query(NseIngestionLog.seq)
            .filter(
                NseIngestionLog.trade_date == trade_date,
                NseIngestionLog.segment == "CM30_IND",
            )
            .all()
        }

        skipped = 0
        processed = 0

        for remote_path in ind_paths:
            file_name = os.path.basename(remote_path)  # "79.ind.gz"
            seq_str = file_name.split(".")[0]

            try:
                seq = int(seq_str)
            except ValueError:
                continue

            # ✅ SKIP if already processed
            if seq in done_seqs:
                skipped += 1
                continue

            print(f"[CM30-IND] Downloading {remote_path} (seq={seq})")
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

            rows = []
            for r in records:
                ts_ist = datetime.fromtimestamp(int(r["timestamp"]), tz=timezone.utc).astimezone(IST)

                index_token = int(r.get("index_token") or 0)

                rows.append(
                    NseCmIndex1Min(
                        trade_date=trade_date,
                        interval_start=ts_ist,
                        index_id=index_token,
                        index_name=str(index_token),

                        open_price=_safe_price(r.get("open_index_value")),
                        high_price=_safe_price(r.get("high_index_value")),
                        low_price=_safe_price(r.get("low_index_value")),
                        close_price=_safe_price(
                            r.get("interval_close_index_value") or r.get("current_index_value")
                        ),
                        last_price=_safe_price(r.get("current_index_value")),
                        avg_price=None,

                        percentage_change=_safe_pct(r.get("percentage_change")),
                        indicative_close_value=_safe_price(r.get("indicative_close_index_value")),

                        interval_open_price=_safe_price(r.get("interval_open_index_value")),
                        interval_high_price=_safe_price(r.get("interval_high_index_value")),
                        interval_low_price=_safe_price(r.get("interval_low_index_value")),
                        interval_close_price=_safe_price(r.get("interval_close_index_value")),

                        volume=None,
                        turnover=None,
                    )
                )

            if rows:
                db.bulk_save_objects(rows)

            # ✅ Mark this seq as processed
            db.add(
                NseIngestionLog(
                    trade_date=trade_date,
                    segment="CM30_IND",
                    seq=seq,
                    remote_path=remote_path,
                )
            )

            db.commit()
            done_seqs.add(seq)
            processed += 1
            print(f"[CM30-IND] ✅ Committed data for {file_name}")

        print(f"[CM30-IND] Done folder {remote_dir} | processed={processed}, skipped={skipped}")

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
    Fast upsert Securities.dat into nse_cm_securities

    Key optimizations:
    - NO per-row SELECT
    - preload token_id -> id map once
    - batch bulk inserts + bulk updates
    - commit in chunks
    """
    print("trade_date :: ", trade_date)
    folder_name = _nse_folder_name(trade_date)
    remote_dir = f"/CM30/SECURITY/{folder_name}"
    remote_file = f"{remote_dir}/Securities.dat"

    sftp = SFTPClient()
    db: Session = SessionLocal()
    local_path: Optional[str] = None

    # batching
    BATCH_SIZE = 2000

    try:
        print(f"[CM30-SEC] Processing Securities master for {trade_date} ({remote_file})")

        try:
            file_bytes = sftp.download_file(remote_file)
        except Exception as e:
            print(f"[CM30-SEC] ERROR downloading {remote_file}: {e}")
            return

        fd, local_path = tempfile.mkstemp(suffix=".dat")
        with os.fdopen(fd, "wb") as tmp:
            tmp.write(file_bytes)

        conv = SecuritiesConverter()

        securities = conv.extract_securities_dynamic(local_path)
        if not securities:
            print("[CM30-SEC] extract_securities_dynamic returned 0, trying alternative parsing...")
            securities = conv.try_alternative_parsing(local_path)

        if not securities:
            print("[CM30-SEC] ❌ No securities parsed from Securities.dat")
            return

        print(f"[CM30-SEC] Parsed {len(securities)} securities from {remote_file}")

        # ✅ Preload: token_id -> (row id)  (only once)
        existing = db.query(NseCmSecurity.id, NseCmSecurity.token_id).all()
        token_to_pk = {t: pk for pk, t in existing}
        print(f"[CM30-SEC] Existing securities in DB: {len(token_to_pk)}")

        FREEZE_DIV = 100.0
        TICK_DIV = 100.0

        inserts: List[NseCmSecurity] = []
        updates: List[Dict[str, Any]] = []

        inserted = 0
        updated = 0

        def flush_batch():
            nonlocal inserted, updated, inserts, updates
            if inserts:
                db.bulk_save_objects(inserts)
                inserted += len(inserts)
                inserts = []

            if updates:
                db.bulk_update_mappings(NseCmSecurity, updates)
                updated += len(updates)
                updates = []

            db.commit()

        for i, rec in enumerate(securities, start=1):
            try:
                token_id = int(rec["token_number"])
            except (KeyError, ValueError, TypeError):
                continue

            symbol = (rec.get("symbol") or "").strip()
            if not symbol:
                continue

            series = (rec.get("series") or "").strip() or None
            company_name = (rec.get("company_name") or "").strip() or None

            issued_capital = rec.get("issued_capital")
            settlement_cycle = rec.get("settlement_cycle")

            lot_size = rec.get("board_lot_quantity") or None
            tick_size = _safe_div(rec.get("tick_size"), TICK_DIV)
            freeze_pct = _safe_div(rec.get("freeze_percent"), FREEZE_DIV)
            credit_rating = (rec.get("credit_rating") or "").strip() or None
            permitted_to_trade = rec.get("permitted_to_trade")

            issue_start_date = _epoch_to_date(rec.get("issue_start_date"))
            issue_end_date = _epoch_to_date(rec.get("issue_pdate"))
            record_date = _epoch_to_date(rec.get("record_date"))
            book_closure_start_date = _epoch_to_date(rec.get("book_closure_start_date"))
            book_closure_end_date = _epoch_to_date(rec.get("book_closure_end_date"))
            no_delivery_start_date = _epoch_to_date(rec.get("no_delivery_start_date"))
            no_delivery_end_date = _epoch_to_date(rec.get("no_delivery_end_date"))

            pk = token_to_pk.get(token_id)

            if pk is None:
                # INSERT object
                inserts.append(
                    NseCmSecurity(
                        token_id=token_id,
                        symbol=symbol,
                        series=series,
                        isin=None,
                        company_name=company_name,
                        lot_size=int(lot_size) if lot_size is not None else None,
                        face_value=None,
                        segment="CM",
                        active_flag=True,
                        issued_capital=issued_capital,
                        settlement_cycle=settlement_cycle,
                        tick_size=tick_size,
                        freeze_percentage=freeze_pct,
                        credit_rating=credit_rating,
                        issue_start_date=issue_start_date,
                        issue_end_date=issue_end_date,
                        listing_date=None,
                        record_date=record_date,
                        book_closure_start_date=book_closure_start_date,
                        book_closure_end_date=book_closure_end_date,
                        no_delivery_start_date=no_delivery_start_date,
                        no_delivery_end_date=no_delivery_end_date,
                        permitted_to_trade=permitted_to_trade,
                    )
                )
            else:
                # UPDATE mapping (fast)
                m: Dict[str, Any] = {"id": pk}

                # always refresh basics
                m["symbol"] = symbol
                m["series"] = series
                m["company_name"] = company_name
                m["active_flag"] = True

                # extras
                m["lot_size"] = int(lot_size) if lot_size is not None else None
                m["issued_capital"] = issued_capital
                m["settlement_cycle"] = settlement_cycle
                m["tick_size"] = tick_size
                m["freeze_percentage"] = freeze_pct
                m["credit_rating"] = credit_rating
                m["issue_start_date"] = issue_start_date
                m["issue_end_date"] = issue_end_date
                m["record_date"] = record_date
                m["book_closure_start_date"] = book_closure_start_date
                m["book_closure_end_date"] = book_closure_end_date
                m["no_delivery_start_date"] = no_delivery_start_date
                m["no_delivery_end_date"] = no_delivery_end_date
                m["permitted_to_trade"] = permitted_to_trade

                updates.append(m)

            # ✅ flush every batch
            if (len(inserts) + len(updates)) >= BATCH_SIZE:
                flush_batch()

            if i % 5000 == 0:
                print(f"[CM30-SEC] progress: {i}/{len(securities)}")

        # final flush
        flush_batch()

        print(f"[CM30-SEC] ✅ Done. inserted={inserted}, updated={updated}")

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
