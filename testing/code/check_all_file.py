# utils/NSE_Formater/bhavcopy_txt_to_csv.py

import os
import csv
from datetime import date
from typing import List, Dict, Any

from sftp.NSE.sftp_client import SFTPClient
from utils.NSE_Formater.bhavcopy_ingestor import parse_cm_bhavcopy


CSV_COLUMNS = [
    "trade_date",
    "symbol",
    "series",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "last_price",
    "prev_close",
    "total_traded_qty",
    "total_traded_value",
    "total_trades",
    "isin",
]


def download_bhavcopy_txt_as_csv(trade_date: date, out_dir: str = "./downloads") -> str:
    folder_name = trade_date.strftime("%B%d%Y")  # e.g. January072026
    dd = trade_date.strftime("%d")
    mm = trade_date.strftime("%m")
    yyyy = trade_date.strftime("%Y")

    remote_path = f"/CM30/BHAVCOPY/{folder_name}/CMBhavcopy_{dd}{mm}{yyyy}.txt"

    sftp = SFTPClient()
    try:
        # 1) download txt bytes
        content = sftp.download_file(remote_path)

        # 2) parse -> list[dict]
        records: List[Dict[str, Any]] = parse_cm_bhavcopy(content, trade_date)
        if not records:
            raise RuntimeError(f"No records parsed for {trade_date} from {remote_path}")

        # 3) write csv
        os.makedirs(out_dir, exist_ok=True)
        csv_path = os.path.join(out_dir, f"CMBhavcopy_{dd}{mm}{yyyy}.csv")

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            for r in records:
                writer.writerow({k: r.get(k) for k in CSV_COLUMNS})

        return csv_path

    finally:
        sftp.close()


if __name__ == "__main__":
    path = download_bhavcopy_txt_as_csv(date(2026, 1, 7), out_dir="./downloads")
    print("✅ CSV saved:", os.path.abspath(path))
