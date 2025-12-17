# scripts/fetch_index_constituents.py
import io
import csv
from datetime import date
import requests
from sqlalchemy.orm import Session
from db.connection import SessionLocal
from db.models import NseIndexMaster, NseIndexConstituent

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    )
}

INDEX_CONFIG = [
    {
        "index_symbol": "NIFTY 50",
        "short_code": "NIFTY50",
        "csv_url": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
    },
    {
        "index_symbol": "NIFTY 100",
        "short_code": "NIFTY100",
        "csv_url": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20100",
    },
    {
        "index_symbol": "NIFTY 500",
        "short_code": "NIFTY500",
        "csv_url": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",
    },
]


def upsert_index(db: Session, index_symbol: str, short_code: str) -> NseIndexMaster:
    idx = (
        db.query(NseIndexMaster)
        .filter(NseIndexMaster.index_symbol == index_symbol)
        .one_or_none()
    )
    if idx is None:
        idx = NseIndexMaster(
            index_symbol=index_symbol,
            short_code=short_code,
            full_name=index_symbol,
            is_active=True,
        )
        db.add(idx)
        db.flush()
    return idx


def fetch_and_save_index(index_cfg, as_of: date):
    db: Session = SessionLocal()
    try:
        idx = upsert_index(db, index_cfg["index_symbol"], index_cfg["short_code"])

        # same-date purane records hata do
        db.query(NseIndexConstituent).filter(
            NseIndexConstituent.index_id == idx.id,
            NseIndexConstituent.as_of_date == as_of,
        ).delete()

        url = index_cfg["csv_url"]
        print(f"[INDEX] Fetching {idx.index_symbol} from {url}")

        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()

        payload = resp.json()
        raw_rows = payload.get("data", []) or []

        # --------- 1st pass: usable rows collect + total_ffmc nikaalna ----------
        usable_rows = []
        total_ffmc = 0.0

        for row in raw_rows:
            symbol = (row.get("symbol") or "").strip()
            if not symbol:
                continue
            # index row skip
            if symbol.upper() == idx.index_symbol.upper():
                continue

            ffmc_raw = row.get("ffmc")
            try:
                ffmc_val = float(ffmc_raw) if ffmc_raw is not None else 0.0
            except (TypeError, ValueError):
                ffmc_val = 0.0

            usable_rows.append((row, ffmc_val))
            total_ffmc += ffmc_val

        if not usable_rows or total_ffmc <= 0:
            print(f"[INDEX] {idx.index_symbol}: no usable rows or total_ffmc <= 0")
            db.commit()
            return

        # --------- 2nd pass: weight % calc karke insert ----------
        added = 0

        for row, ffmc_val in usable_rows:
            symbol = (row.get("symbol") or "").strip()
            meta = row.get("meta") or {}
            isin = (meta.get("isin") or "").strip() or None

            weight = None
            if ffmc_val > 0:
                # percentage weight 0–100 range me
                try:
                    weight_percent = (ffmc_val / total_ffmc) * 100.0
                    # numeric(10,4) ke liye safe rounding
                    weight = round(weight_percent, 6)
                except (TypeError, ValueError, ZeroDivisionError):
                    weight = None

            cons = NseIndexConstituent(
                index_id=idx.id,
                symbol=symbol,
                isin=isin,
                weight=weight,
                as_of_date=as_of,
            )
            db.add(cons)
            added += 1

        db.commit()
        print(f"[INDEX] {idx.index_symbol}: saved {added} constituents for {as_of}")

    except Exception as e:
        db.rollback()
        print(f"[INDEX] ERROR {index_cfg['index_symbol']}: {e}")
        raise
    finally:
        db.close()


def main():
    today = date.today()
    for cfg in INDEX_CONFIG:
        fetch_and_save_index(cfg, today)


if __name__ == "__main__":
    main()
