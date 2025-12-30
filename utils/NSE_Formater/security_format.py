# utils/NSE_Formater/security_format.py

import struct
import os
from typing import List, Dict, Any, Optional

import pandas as pd


class SecuritiesConverter:
    """
    NSE CM30 Securities.dat binary parser (v1.24/v1.25 compatible) with
    header framed records (transcode=7).

    ✅ Fix included:
    - company_name is parsed from fixed 25-byte field (NO heuristic scan)
    - supports payload sizes 114 and 115 (115 has 1 extra padding byte)
    """

    def __init__(self):
        # Header: short(H), long(L), short(H)  => 8 bytes
        #   - Transcode
        #   - TimeStamp (epoch)
        #   - MessageLength
        self.header_format = "<HLH"  # 8 bytes

        # ✅ NSE CM30 Securities payload (v1.24+)
        # struct size = 114 bytes
        # Some snapshots send payload as 115 bytes (last byte padding/extra) -> ignore extra
        self.v124_format = "<L10s2sdHH12sHLLLLL25sLLLLLLH1s"
        self.v124_size = struct.calcsize(self.v124_format)  # 114

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _clean_str(self, b: bytes) -> str:
        return b.decode("ascii", errors="ignore").rstrip("\x00").strip()

    def _byte_to_int(self, b: bytes) -> int:
        """
        permitted_to_trade comes as 1 byte in many dumps.
        It can be numeric 0/1/2 or ASCII '0'/'1'/'2'.
        """
        if not b:
            return 1
        v = b[0]
        if 48 <= v <= 57:  # ASCII
            return int(chr(v))
        return int(v)

    def _unpack_from(self, fmt: str, data: bytes, offset: int):
        size = struct.calcsize(fmt)
        if offset + size > len(data):
            raise struct.error("Not enough bytes")
        return struct.unpack_from(fmt, data, offset)

    # ------------------------------------------------------------------
    # Structure analysis (debug/inspection)
    # ------------------------------------------------------------------

    def analyze_file_structure(self, file_path: str) -> Optional[int]:
        """
        Analyze the file to get a rough idea of record boundaries and data size.

        Returns:
            data_size (int) if record structure seems consistent,
            otherwise None.
        """
        if not os.path.exists(file_path):
            print(f"[SecuritiesConverter] File not found: {file_path}")
            return None

        file_size = os.path.getsize(file_path)
        print(f"[SecuritiesConverter] File size: {file_size} bytes")

        with open(file_path, "rb") as f:
            first_bytes = f.read(100)
            print(
                f"[SecuritiesConverter] First 20 bytes (hex): {first_bytes[:20].hex()}"
            )

            f.seek(0)
            record_positions: List[int] = []
            record_count = 0

            # Analyze first 10 KB for patterns
            limit = min(file_size, 10_000)

            while f.tell() < limit:
                pos = f.tell()
                try:
                    header_data = f.read(8)
                    if len(header_data) < 8:
                        break

                    transcode, timestamp, message_length = struct.unpack(
                        self.header_format, header_data
                    )

                    # transcode=7 usually denotes securities records
                    if transcode == 7 and 80 < message_length < 256:
                        record_positions.append(pos)
                        remaining = message_length - 8
                        if remaining > 0:
                            f.read(remaining)
                        record_count += 1
                    else:
                        f.seek(pos + 1)

                except Exception:
                    f.seek(pos + 1)

                if record_count > 10:
                    # enough samples
                    break

        if len(record_positions) >= 2:
            record_size = record_positions[1] - record_positions[0]
            data_size = record_size - 8
            print(f"[SecuritiesConverter] Detected record size: {record_size} bytes")
            print(f"[SecuritiesConverter] Data portion size: {data_size} bytes")
            return data_size

        print("[SecuritiesConverter] Could not reliably detect record structure.")
        return None

    # ------------------------------------------------------------------
    # Main extraction: framed records (header + data)
    # ------------------------------------------------------------------

    def extract_securities_dynamic(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract securities with header-based dynamic parsing.

        Returns a list of dictionaries with at least these keys:
          token_number, symbol, series, company_name, issued_capital,
          settlement_cycle, permitted_to_trade, data_length
        """
        securities: List[Dict[str, Any]] = []

        if not os.path.exists(file_path):
            print(f"[SecuritiesConverter] File not found: {file_path}")
            return securities

        file_size = os.path.getsize(file_path)

        with open(file_path, "rb") as f:
            while f.tell() < file_size:
                pos = f.tell()
                try:
                    header_data = f.read(8)
                    if len(header_data) < 8:
                        break

                    transcode, timestamp, message_length = struct.unpack(
                        self.header_format, header_data
                    )

                    # Guard for nonsense
                    if message_length <= 8 or message_length > 512:
                        # Not a valid record – move 1 byte forward
                        f.seek(pos + 1)
                        continue

                    data_size = message_length - 8
                    if data_size <= 0:
                        f.seek(pos + 1)
                        continue

                    data = f.read(data_size)
                    if len(data) < data_size:
                        break

                    if transcode == 7:
                        security = self.parse_security_dynamic(data)
                        if security:
                            # attach timestamp metadata if useful
                            security["timestamp"] = int(timestamp)
                            security["message_length"] = int(message_length)
                            securities.append(security)
                    else:
                        # Unknown record type – skip
                        continue

                except Exception as e:
                    print(f"[SecuritiesConverter] Error at pos={pos}: {e}")
                    f.seek(pos + 1)

        return securities

    # ------------------------------------------------------------------
    # Dispatcher for security data block by length
    # ------------------------------------------------------------------

    def parse_security_dynamic(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Decide which format to use based on data length.

        CM30 payload usually 114/115 bytes.
        """
        try:
            if len(data) >= self.v124_size:
                return self.parse_v124_format(data)
            elif len(data) >= 100:
                return self.parse_older_format(data)
            else:
                return self.parse_minimal_format(data)
        except Exception as e:
            print(f"[SecuritiesConverter] parse_security_dynamic error: {e}")
            return None

    # ------------------------------------------------------------------
    # v1.24+ format (fixed layout)
    # ------------------------------------------------------------------

    def parse_v124_format(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        ✅ Fixed-layout parse for CM30 Securities.dat payload.

        Handles payload size 114 or 115:
        - If 115 bytes, ignores the extra last byte.
        """
        if len(data) < self.v124_size:
            return None

        block = data[: self.v124_size]  # ignore extra padding if present

        (
            token_number,
            symbol_b,
            series_b,
            issued_capital,
            settlement_cycle,
            freeze_percent,
            credit_b,
            issue_rate,
            issue_start_date,
            issue_pdate,
            issue_maturity_date,
            board_lot_quantity,
            tick_size,
            company_name_b,
            record_date,
            expiry_date,
            no_delivery_start_date,
            no_delivery_end_date,
            book_closure_start_date,
            book_closure_end_date,
            ssec,
            permitted_b,
        ) = struct.unpack(self.v124_format, block)

        symbol = self._clean_str(symbol_b)
        series = self._clean_str(series_b)
        company_name = self._clean_str(company_name_b)
        credit_rating = self._clean_str(credit_b)
        permitted_to_trade = self._byte_to_int(permitted_b)

        return {
            "token_number": int(token_number),
            "symbol": symbol,
            "series": series,
            "issued_capital": float(issued_capital),
            "settlement_cycle": int(settlement_cycle),
            "freeze_percent": int(freeze_percent),
            "credit_rating": credit_rating,
            "issue_rate": int(issue_rate),
            "issue_start_date": int(issue_start_date),
            "issue_pdate": int(issue_pdate),
            "issue_maturity_date": int(issue_maturity_date),
            "board_lot_quantity": int(board_lot_quantity),
            "tick_size": int(tick_size),
            "company_name": company_name,
            "record_date": int(record_date),
            "expiry_date": int(expiry_date),
            "no_delivery_start_date": int(no_delivery_start_date),
            "no_delivery_end_date": int(no_delivery_end_date),
            "book_closure_start_date": int(book_closure_start_date),
            "book_closure_end_date": int(book_closure_end_date),
            "ssec": int(ssec),
            "permitted_to_trade": permitted_to_trade,
            "data_length": len(data),
        }

    # ------------------------------------------------------------------
    # Older format (simple)
    # ------------------------------------------------------------------

    def parse_older_format(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Older or shorter variants where we rely on:
          - token_number
          - symbol
          - series
        Everything else defaults.
        """
        if len(data) < 16:
            return None

        token_number = struct.unpack("<L", data[0:4])[0]
        symbol = data[4:14].decode("utf-8", errors="ignore").rstrip("\x00").strip()
        series = data[14:16].decode("utf-8", errors="ignore").rstrip("\x00").strip()

        return {
            "token_number": int(token_number),
            "symbol": symbol,
            "series": series,
            "issued_capital": 0.0,
            "settlement_cycle": 0,
            "freeze_percent": 0,
            "credit_rating": "",
            "issue_rate": 0,
            "issue_start_date": 0,
            "issue_pdate": 0,
            "issue_maturity_date": 0,
            "board_lot_quantity": 0,
            "tick_size": 0,
            "company_name": "",
            "record_date": 0,
            "expiry_date": 0,
            "no_delivery_start_date": 0,
            "no_delivery_end_date": 0,
            "book_closure_start_date": 0,
            "book_closure_end_date": 0,
            "ssec": 0,
            "permitted_to_trade": 1,
            "data_length": len(data),
        }

    # ------------------------------------------------------------------
    # Minimal format
    # ------------------------------------------------------------------

    def parse_minimal_format(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Very small payloads: try only token_number + symbol (up to 10 bytes).
        """
        if len(data) < 4:
            return None

        token_number = struct.unpack("<L", data[0:4])[0]
        symbol = ""
        if len(data) > 4:
            symbol = (
                data[4 : min(14, len(data))]
                .decode("utf-8", errors="ignore")
                .rstrip("\x00")
                .strip()
            )

        return {
            "token_number": int(token_number),
            "symbol": symbol,
            "series": "",
            "issued_capital": 0.0,
            "settlement_cycle": 0,
            "freeze_percent": 0,
            "credit_rating": "",
            "issue_rate": 0,
            "issue_start_date": 0,
            "issue_pdate": 0,
            "issue_maturity_date": 0,
            "board_lot_quantity": 0,
            "tick_size": 0,
            "company_name": "",
            "record_date": 0,
            "expiry_date": 0,
            "no_delivery_start_date": 0,
            "no_delivery_end_date": 0,
            "book_closure_start_date": 0,
            "book_closure_end_date": 0,
            "ssec": 0,
            "permitted_to_trade": 1,
            "data_length": len(data),
        }

    # ------------------------------------------------------------------
    # CSV conversion helper
    # ------------------------------------------------------------------

    def convert_to_csv(self, dat_file_path: str, csv_file_path: str) -> Optional[pd.DataFrame]:
        """
        Convert Securities.dat → CSV.
        Returns the DataFrame if successful, else None.
        """
        if not os.path.exists(dat_file_path):
            print(f"[SecuritiesConverter] File not found: {dat_file_path}")
            return None

        # Optional analysis
        self.analyze_file_structure(dat_file_path)

        securities = self.extract_securities_dynamic(dat_file_path)
        if not securities:
            securities = self.try_alternative_parsing(dat_file_path)

        if not securities:
            print("[SecuritiesConverter] ❌ No securities parsed.")
            return None

        df = pd.DataFrame(securities)

        df["settlement_cycle_desc"] = df["settlement_cycle"].map(
            {0: "T+0", 1: "T+1", 2: "T+2", 3: "T+3"}
        ).fillna("Unknown")

        df["permitted_to_trade_desc"] = df["permitted_to_trade"].map(
            {
                0: "Listed but not permitted to trade",
                1: "Permitted to trade",
                2: "BSE listed (BSE exclusive security)",
            }
        ).fillna("Unknown")

        df = df.sort_values("token_number")
        df.to_csv(csv_file_path, index=False)

        print(f"✅ Converted {len(df)} records → {csv_file_path}")
        print(df[["token_number", "symbol", "series", "company_name"]].head(5))
        print(f"Total records: {len(df)} | Unique symbols: {df['symbol'].nunique()}")
        print(f"Data lengths seen: {sorted(df['data_length'].unique())}")

        return df

    # ------------------------------------------------------------------
    # Fallback: raw pattern scan (no framing)
    # ------------------------------------------------------------------

    def try_alternative_parsing(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Heuristic parsing without relying on the header structure.

        NOTE: This is heuristic and may produce duplicates/spurious hits.
        """
        results: List[Dict[str, Any]] = []

        if not os.path.exists(file_path):
            print(f"[SecuritiesConverter] File not found: {file_path}")
            return results

        with open(file_path, "rb") as f:
            data = f.read()

        n = len(data)
        for i in range(0, n - 20, 4):
            try:
                token = struct.unpack("<L", data[i : i + 4])[0]
                if not (1 <= token <= 1_000_000):
                    continue

                symbol_raw = data[i + 4 : i + 14]
                symbol = symbol_raw.decode("utf-8", errors="ignore").rstrip("\x00").strip()

                if not symbol or len(symbol) < 2:
                    continue

                cleaned = symbol.replace("$", "").replace("&", "").replace("-", "")
                if not cleaned.isalnum():
                    continue

                series_raw = data[i + 14 : i + 16]
                series = series_raw.decode("utf-8", errors="ignore").rstrip("\x00").strip()

                results.append(
                    {
                        "token_number": int(token),
                        "symbol": symbol,
                        "series": series,
                        "issued_capital": 0.0,
                        "settlement_cycle": 0,
                        "freeze_percent": 0,
                        "credit_rating": "",
                        "issue_rate": 0,
                        "issue_start_date": 0,
                        "issue_pdate": 0,
                        "issue_maturity_date": 0,
                        "board_lot_quantity": 0,
                        "tick_size": 0,
                        "company_name": "",
                        "record_date": 0,
                        "expiry_date": 0,
                        "no_delivery_start_date": 0,
                        "no_delivery_end_date": 0,
                        "book_closure_start_date": 0,
                        "book_closure_end_date": 0,
                        "ssec": 0,
                        "permitted_to_trade": 1,
                        "data_length": 0,
                    }
                )
            except Exception:
                continue

        # Deduplicate by (token_number, symbol, series)
        seen = set()
        unique: List[Dict[str, Any]] = []
        for sec in results:
            key = (sec["token_number"], sec["symbol"], sec["series"])
            if key in seen:
                continue
            seen.add(key)
            unique.append(sec)

        print(
            f"[SecuritiesConverter] Fallback parser produced {len(unique)} unique candidates "
            f"(raw hits={len(results)})"
        )
        return unique
