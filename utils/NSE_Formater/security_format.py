# utils/NSE_Formater/security_format.py

import struct
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd


class SecuritiesConverter:
    """
    NSE CM30 Securities.dat binary parser (v1.24 compatible) with
    a best-effort dynamic approach.

    - Primary path: read framed records (header + data) with transcode=7
      and parse data block according to v1.24 layout (113 bytes).
    - Fallback: scan raw bytes for token/symbol patterns if framing fails.
    """

    def __init__(self):
        # Header: short (H), long (L), short (H)  => 8 bytes
        #   - Transcode
        #   - TimeStamp (epoch)
        #   - MessageLength
        self.header_format = "<HLH"  # 8 bytes

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _unpack_from(self, fmt: str, data: bytes, offset: int):
        size = struct.calcsize(fmt)
        if offset + size > len(data):
            raise struct.error("Not enough bytes")
        return struct.unpack_from(fmt, data, offset)

    # ------------------------------------------------------------------
    # Structure analysis (for debugging / inspection)
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
            print(f"[SecuritiesConverter] First 20 bytes (hex): {first_bytes[:20].hex()}")

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

          token_number
          symbol
          series
          company_name
          issued_capital
          settlement_cycle
          permitted_to_trade
          data_length
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
                            securities.append(security)
                    else:
                        # Unknown record type – just skip its data portion
                        continue

                except Exception as e:
                    # If parsing fails, move 1 byte forward from starting pos
                    print(f"[SecuritiesConverter] Error at pos={pos}: {e}")
                    f.seek(pos + 1)

        return securities

    # ------------------------------------------------------------------
    # Dispatcher for security data block by length
    # ------------------------------------------------------------------

    def parse_security_dynamic(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Decide which format to use based on data length.
        NSE v1.24 Securities.dat uses 113-byte info area.

        Fallbacks for older / minimal formats are also supported.
        """
        try:
            if len(data) >= 113:
                return self.parse_v124_format(data)
            elif len(data) >= 100:
                return self.parse_older_format(data)
            else:
                return self.parse_minimal_format(data)
        except Exception as e:
            print(f"[SecuritiesConverter] parse_security_dynamic error: {e}")
            return None

    # ------------------------------------------------------------------
    # v1.24 format (113 bytes info)
    # ------------------------------------------------------------------

    def parse_v124_format(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Parse v1.24 Securities.dat payload (approx 113 bytes) according
        to NSE spec. Exact positions for each field are not completely
        disclosed, so this is a best-effort mapping that is good enough
        for:

          - token_number
          - symbol
          - series
          - issued_capital
          - settlement_cycle
          - a heuristic company_name
          - permitted_to_trade (last 2 bytes)
        """
        if len(data) < 24:
            return None

        try:
            # 0-3  : token number (uint32)
            token_number = struct.unpack("<L", data[0:4])[0]

            # 4-13 : symbol (10 bytes, padded)
            raw_symbol = data[4:14]
            symbol = raw_symbol.decode("utf-8", errors="ignore").rstrip("\x00").strip()

            # 14-15: series (2 bytes)
            raw_series = data[14:16]
            series = raw_series.decode("utf-8", errors="ignore").rstrip("\x00").strip()

            # 16-23: issued capital (double / 8 bytes)
            try:
                issued_capital = struct.unpack("<d", data[16:24])[0]
            except Exception:
                issued_capital = 0.0

            # 24-25: settlement_cycle (uint16)
            try:
                settlement_cycle = struct.unpack("<H", data[24:26])[0]
            except Exception:
                settlement_cycle = 0

            # Heuristic: company name somewhere in mid-chunk.
            # We scan for the "best" 25-char window that looks printable.
            company_name = ""
            best_len = 0
            max_len = min(80, len(data))  # don't go too far
            for start in range(32, max_len - 25):
                chunk = data[start : start + 25]
                try:
                    text = chunk.decode("utf-8", errors="ignore").strip("\x00").strip()
                except Exception:
                    continue
                if len(text) > best_len and text.isprintable():
                    best_len = len(text)
                    company_name = text

            # Last 2 bytes (111-112) typically "Permitted to trade"
            permitted_to_trade = 1
            if len(data) >= 113:
                try:
                    permitted_to_trade = struct.unpack("<H", data[111:113])[0]
                except Exception:
                    permitted_to_trade = 1

            return {
                "token_number": token_number,
                "symbol": symbol,
                "series": series,
                "issued_capital": issued_capital,
                "settlement_cycle": settlement_cycle,
                "company_name": company_name,
                "permitted_to_trade": permitted_to_trade,
                "data_length": len(data),
            }

        except Exception as e:
            print(f"[SecuritiesConverter] parse_v124_format error: {e}")
            return None

    # ------------------------------------------------------------------
    # Older format (simpler layout)
    # ------------------------------------------------------------------

    def parse_older_format(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Older or shorter variants where we rely on:
          - token_number
          - symbol
          - series
        Everything else defaults / dummy.
        """
        if len(data) < 16:
            return None

        try:
            token_number = struct.unpack("<L", data[0:4])[0]
            symbol = data[4:14].decode("utf-8", errors="ignore").rstrip("\x00").strip()
            series = data[14:16].decode("utf-8", errors="ignore").rstrip("\x00").strip()

            return {
                "token_number": token_number,
                "symbol": symbol,
                "series": series,
                "issued_capital": 0.0,
                "settlement_cycle": 0,
                "company_name": "",
                "permitted_to_trade": 1,
                "data_length": len(data),
            }

        except Exception as e:
            print(f"[SecuritiesConverter] parse_older_format error: {e}")
            return None

    # ------------------------------------------------------------------
    # Minimal format (just token + symbol at least)
    # ------------------------------------------------------------------

    def parse_minimal_format(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Very small payloads: try only token_number + symbol (up to 10 bytes).
        """
        if len(data) < 4:
            return None

        try:
            token_number = struct.unpack("<L", data[0:4])[0]
            symbol = ""
            if len(data) > 4:
                symbol = data[4 : min(14, len(data))].decode("utf-8", errors="ignore").rstrip(
                    "\x00"
                ).strip()

            return {
                "token_number": token_number,
                "symbol": symbol,
                "series": "",
                "issued_capital": 0.0,
                "settlement_cycle": 0,
                "company_name": "",
                "permitted_to_trade": 1,
                "data_length": len(data),
            }

        except Exception as e:
            print(f"[SecuritiesConverter] parse_minimal_format error: {e}")
            return None

    # ------------------------------------------------------------------
    # CSV conversion helper (optional, for debugging / offline inspection)
    # ------------------------------------------------------------------

    def convert_to_csv(self, dat_file_path: str, csv_file_path: str) -> Optional[pd.DataFrame]:
        """
        Convert Securities.dat → CSV, apply formatting, print stats.
        Returns the DataFrame if successful, else None.
        """
        if not os.path.exists(dat_file_path):
            print(f"[SecuritiesConverter] File not found: {dat_file_path}")
            return None

        # Optional: structure analysis
        self.analyze_file_structure(dat_file_path)

        # 1) Extract via framed parsing
        securities = self.extract_securities_dynamic(dat_file_path)
        # 2) Fallback to brute-force if nothing returned
        if not securities:
            securities = self.try_alternative_parsing(dat_file_path)

        if not securities:
            print("[SecuritiesConverter] ❌ No securities parsed.")
            return None

        # 3) Build DataFrame + descriptive columns
        df = pd.DataFrame(securities)

        # Settlement cycle description
        df["settlement_cycle_desc"] = df["settlement_cycle"].map(
            {
                0: "T+0",
                1: "T+1",
                2: "T+2",
                3: "T+3",
            }
        ).fillna("Unknown")

        # Permitted to trade description (as per NSE v1.24)
        df["permitted_to_trade_desc"] = df["permitted_to_trade"].map(
            {
                0: "Listed but not permitted to trade",
                1: "Permitted to trade",
                2: "BSE listed (BSE exclusive security)",
            }
        ).fillna("Unknown")

        # 4) Sort & write
        df = df.sort_values("token_number")
        df.to_csv(csv_file_path, index=False)

        # 5) Print sample & stats
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
        Try parsing without relying on the header structure.

        Strategy:
          - Scan the raw bytes in 4-byte steps
          - Interpret each 4 bytes as a possible token_number
          - If token_number is in a reasonable range and followed by a
            plausible symbol, accept as a security record.

        NOTE: This is heuristic and may produce duplicates or spurious hits.
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
                # Very rough token sanity check
                if not (1 <= token <= 1_000_000):
                    continue

                # Try to read symbol (next 10 bytes)
                symbol_raw = data[i + 4 : i + 14]
                symbol = symbol_raw.decode("utf-8", errors="ignore").rstrip("\x00").strip()

                # Quick symbol plausibility check
                if not symbol or len(symbol) < 2:
                    continue
                # allow alnum and $ (BSE-only flag) etc.
                cleaned = symbol.replace("$", "").replace("&", "").replace("-", "")
                if not cleaned.isalnum():
                    continue

                # Series attempt
                series_raw = data[i + 14 : i + 16]
                series = series_raw.decode("utf-8", errors="ignore").rstrip("\x00").strip()

                results.append(
                    {
                        "token_number": token,
                        "symbol": symbol,
                        "series": series,
                        "issued_capital": 0.0,
                        "settlement_cycle": 0,
                        "company_name": "",
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
