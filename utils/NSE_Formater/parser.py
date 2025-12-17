# utils/NSE_Formater/parser.py

import gzip
import struct
from typing import List, Dict, Any


# ============================================================
# Helpers
# ============================================================

def _read_gz(path: str) -> bytes:
    with gzip.open(path, "rb") as f:
        return f.read()


def _unpack_from(fmt: str, data: bytes, offset: int):
    """Safe wrapper around struct.unpack_from."""
    size = struct.calcsize(fmt)
    if offset + size > len(data):
        raise struct.error("Not enough bytes")
    return struct.unpack_from(fmt, data, offset)


# ============================================================
# CM30 MARKET SNAPSHOT (*.mkt.gz)
#   - Header:  8 bytes  (H I H)  => transcode, timestamp, msg_len
#   - Info:   88 bytes  (NSE v1.24 spec)  => record_size = 96
# ============================================================

def parse_mkt(path: str) -> List[Dict[str, Any]]:
    """
    Parse a CM 15-min delayed snapshot file (*.mkt.gz).

    Layout per record (v1.24):

      HEADER (8 bytes, little-endian):
        - short  TransCode
        - long   TimeStamp (epoch seconds, UTC)
        - short  MsgLen  (should be 96)

      INFO DATA (88 bytes, little-endian):
        0   : uint32  Security Token
        4   : uint32  Last Traded Price
        8   : uint64  Best Buy Quantity
        16  : uint32  Best Buy Price
        20  : uint64  Best Sell Quantity
        28  : uint32  Best Sell Price
        32  : uint64  Total Traded Quantity
        40  : uint32  Average Traded Price
        44  : uint32  Open Price
        48  : uint32  High Price
        52  : uint32  Low Price
        56  : uint32  Close Price
        60  : uint32  Interval Open Price
        64  : uint32  Interval High Price
        68  : uint32  Interval Low Price
        72  : uint32  Interval Close Price
        76  : uint64  Interval Total Traded Quantity
        84  : uint32  Indicative Close Price
    """
    records: List[Dict[str, Any]] = []

    file_data = _read_gz(path)
    if not file_data or len(file_data) < 8:
        return records

    RECORD_SIZE = 96        # 8 header + 88 info
    INFO_SIZE = 88

    file_len = len(file_data)
    # Optional: quick sanity print
    print(f"[parse_mkt] file={path}, size={file_len}, approx_records={file_len // RECORD_SIZE}")

    offset = 0
    while offset + RECORD_SIZE <= file_len:
        try:
            # ----- HEADER -----
            transcode, timestamp, msg_len = _unpack_from("<H I H", file_data, offset)

            # Safety: if msg_len looks wrong, break
            if msg_len <= 0 or msg_len > 512:
                # Probably garbage at end
                break

            info_start = offset + 8
            info_end = info_start + INFO_SIZE
            if info_end > file_len:
                break

            info = file_data[info_start:info_end]
            o = 0

            # ----- INFO DATA -----
            security_token = _unpack_from("<I", info, o)[0]; o += 4
            last_traded_price = _unpack_from("<I", info, o)[0]; o += 4
            best_buy_quantity = _unpack_from("<Q", info, o)[0]; o += 8
            best_buy_price = _unpack_from("<I", info, o)[0]; o += 4
            best_sell_quantity = _unpack_from("<Q", info, o)[0]; o += 8
            best_sell_price = _unpack_from("<I", info, o)[0]; o += 4
            total_traded_quantity = _unpack_from("<Q", info, o)[0]; o += 8
            average_traded_price = _unpack_from("<I", info, o)[0]; o += 4
            open_price = _unpack_from("<I", info, o)[0]; o += 4
            high_price = _unpack_from("<I", info, o)[0]; o += 4
            low_price = _unpack_from("<I", info, o)[0]; o += 4
            close_price = _unpack_from("<I", info, o)[0]; o += 4
            interval_open_price = _unpack_from("<I", info, o)[0]; o += 4
            interval_high_price = _unpack_from("<I", info, o)[0]; o += 4
            interval_low_price = _unpack_from("<I", info, o)[0]; o += 4
            interval_close_price = _unpack_from("<I", info, o)[0]; o += 4
            interval_total_traded_quantity = _unpack_from("<Q", info, o)[0]; o += 8
            indicative_close_price = _unpack_from("<I", info, o)[0]; o += 4

            rec = {
                "transcode": transcode,
                "timestamp": timestamp,
                "message_length": msg_len,

                "security_token": security_token,
                "last_traded_price": last_traded_price,
                "best_buy_quantity": best_buy_quantity,
                "best_buy_price": best_buy_price,
                "best_sell_quantity": best_sell_quantity,
                "best_sell_price": best_sell_price,
                "total_traded_quantity": total_traded_quantity,
                "average_traded_price": average_traded_price,
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "close_price": close_price,
                "interval_open_price": interval_open_price,
                "interval_high_price": interval_high_price,
                "interval_low_price": interval_low_price,
                "interval_close_price": interval_close_price,
                "interval_total_traded_quantity": interval_total_traded_quantity,
                "indicative_close_price": indicative_close_price,
            }
            records.append(rec)

            offset += RECORD_SIZE

        except struct.error as e:
            print(f"[parse_mkt] struct error at offset={offset}: {e}")
            break
        except Exception as e:
            print(f"[parse_mkt] error at offset={offset}: {e}")
            break

    return records


# ============================================================
# CM30 INDICES SNAPSHOT (*.ind.gz)
#   - Header:  8 bytes (H I H)
#   - Info:   44 bytes => record_size = 52
# ============================================================

def parse_ind(path: str) -> List[Dict[str, Any]]:
    """
    Parse an Indices 15-min delayed snapshot file (*.ind.gz).

    Layout per record (v1.24):

      HEADER (8 bytes, little-endian):
        - short  TransCode
        - long   TimeStamp (epoch seconds, UTC)
        - short  MsgLen  (should be 52)

      INFO DATA (44 bytes, little-endian):
        0   : uint32  Index Token
        4   : uint32  Open Index Value
        8   : uint32  Current Index Value
        12  : uint32  High Index Value
        16  : uint32  Low Index Value
        20  : uint32  Percentage Change
        24  : uint32  Interval Open Index Value
        28  : uint32  Interval High Index Value
        32  : uint32  Interval Low Index Value
        36  : uint32  Interval Close Index Value
        40  : uint32  Indicative Close Index Value
    """
    records: List[Dict[str, Any]] = []

    file_data = _read_gz(path)
    if not file_data or len(file_data) < 8:
        return records

    RECORD_SIZE = 52       # 8 header + 44 info
    INFO_SIZE = 44

    file_len = len(file_data)
    print(f"[parse_ind] file={path}, size={file_len}, approx_records={file_len // RECORD_SIZE}")

    offset = 0
    while offset + RECORD_SIZE <= file_len:
        try:
            # ----- HEADER -----
            transcode, timestamp, msg_len = _unpack_from("<H I H", file_data, offset)

            if msg_len <= 0 or msg_len > 256:
                break

            info_start = offset + 8
            info_end = info_start + INFO_SIZE
            if info_end > file_len:
                break

            info = file_data[info_start:info_end]
            o = 0

            # ----- INFO DATA -----
            index_token = _unpack_from("<I", info, o)[0]; o += 4
            open_index_value = _unpack_from("<I", info, o)[0]; o += 4
            current_index_value = _unpack_from("<I", info, o)[0]; o += 4
            high_index_value = _unpack_from("<I", info, o)[0]; o += 4
            low_index_value = _unpack_from("<I", info, o)[0]; o += 4
            percentage_change = _unpack_from("<I", info, o)[0]; o += 4
            interval_open_index_value = _unpack_from("<I", info, o)[0]; o += 4
            interval_high_index_value = _unpack_from("<I", info, o)[0]; o += 4
            interval_low_index_value = _unpack_from("<I", info, o)[0]; o += 4
            interval_close_index_value = _unpack_from("<I", info, o)[0]; o += 4
            indicative_close_index_value = _unpack_from("<I", info, o)[0]; o += 4

            rec = {
                "transcode": transcode,
                "timestamp": timestamp,
                "message_length": msg_len,

                "index_token": index_token,
                "open_index_value": open_index_value,
                "current_index_value": current_index_value,
                "high_index_value": high_index_value,
                "low_index_value": low_index_value,
                "percentage_change": percentage_change,
                "interval_open_index_value": interval_open_index_value,
                "interval_high_index_value": interval_high_index_value,
                "interval_low_index_value": interval_low_index_value,
                "interval_close_index_value": interval_close_index_value,
                "indicative_close_index_value": indicative_close_index_value,
            }
            records.append(rec)

            offset += RECORD_SIZE

        except struct.error as e:
            print(f"[parse_ind] struct error at offset={offset}: {e}")
            break
        except Exception as e:
            print(f"[parse_ind] error at offset={offset}: {e}")
            break

    return records


# ============================================================
# CALL AUCTION 2 SNAPSHOT (*.ca2.gz)
#   - Header:  8 bytes (H I H)
#   - Info:   78 bytes => record_size = 86   (per spec)
#
#   NOTE: CA2 parsing is provided for completeness. Currently
#   your ingestion pipeline does not persist CA2, but this
#   structure is aligned with the NSE doc.
# ============================================================

def parse_ca2(path: str) -> List[Dict[str, Any]]:
    """
    Parse a Call-Auction-2 snapshot (*.ca2.gz).

    Layout per record (v1.24) – approximate based on spec:

      HEADER (8 bytes, little-endian):
        - short  TransCode
        - long   TimeStamp
        - short  MsgLen

      INFO DATA (78 bytes, little-endian, approximate fields):
        0   : uint32  Security Token
        4   : uint32  Last Traded Price
        8   : uint64  Best Buy Quantity
        16  : uint32  Best Buy Price
        20  : uint16  Best Buy MMBB Flag        (0..3)
        22  : uint64  Best Sell Quantity
        30  : uint32  Best Sell Price
        34  : uint16  Best Sell MMBB Flag       (0..3)
        36  : uint64  Total Traded Quantity
        44  : uint64  Indicative Traded Qty     (equilibrium qty)
        52  : uint32  Average Traded Price
        56  : uint32  First Open Price
        60  : uint32  Open Price
        64  : uint32  High Price
        68  : uint32  Low Price
        72  : uint32  Close Price
        76  : uint16  Filler / Reserved
    """
    records: List[Dict[str, Any]] = []

    file_data = _read_gz(path)
    if not file_data or len(file_data) < 8:
        return records

    RECORD_SIZE = 86        # 8 + 78
    INFO_SIZE = 78

    file_len = len(file_data)
    print(f"[parse_ca2] file={path}, size={file_len}, approx_records={file_len // RECORD_SIZE}")

    offset = 0
    while offset + 8 <= file_len:
        try:
            if offset + RECORD_SIZE > file_len:
                break

            # ----- HEADER -----
            transcode, timestamp, msg_len = _unpack_from("<H I H", file_data, offset)

            if msg_len <= 0 or msg_len > 256:
                break

            info_start = offset + 8
            info_end = info_start + INFO_SIZE
            if info_end > file_len:
                break

            info = file_data[info_start:info_end]
            o = 0

            # ----- INFO DATA (approximate) -----
            security_token = _unpack_from("<I", info, o)[0]; o += 4
            last_traded_price = _unpack_from("<I", info, o)[0]; o += 4

            best_buy_quantity = _unpack_from("<Q", info, o)[0]; o += 8
            best_buy_price = _unpack_from("<I", info, o)[0]; o += 4
            best_buy_mmbb = _unpack_from("<H", info, o)[0]; o += 2

            best_sell_quantity = _unpack_from("<Q", info, o)[0]; o += 8
            best_sell_price = _unpack_from("<I", info, o)[0]; o += 4
            best_sell_mmbb = _unpack_from("<H", info, o)[0]; o += 2

            total_traded_quantity = _unpack_from("<Q", info, o)[0]; o += 8
            indicative_traded_quantity = _unpack_from("<Q", info, o)[0]; o += 8

            average_traded_price = _unpack_from("<I", info, o)[0]; o += 4
            first_open_price = _unpack_from("<I", info, o)[0]; o += 4

            open_price = _unpack_from("<I", info, o)[0]; o += 4
            high_price = _unpack_from("<I", info, o)[0]; o += 4
            low_price = _unpack_from("<I", info, o)[0]; o += 4
            close_price = _unpack_from("<I", info, o)[0]; o += 4

            # remaining 2 bytes filler
            # filler = _unpack_from("<H", info, o)[0]; o += 2

            rec = {
                "transcode": transcode,
                "timestamp": timestamp,
                "message_length": msg_len,

                "security_token": security_token,
                "last_traded_price": last_traded_price,
                "best_buy_quantity": best_buy_quantity,
                "best_buy_price": best_buy_price,
                "best_buy_mmbb": best_buy_mmbb,
                "best_sell_quantity": best_sell_quantity,
                "best_sell_price": best_sell_price,
                "best_sell_mmbb": best_sell_mmbb,
                "total_traded_quantity": total_traded_quantity,
                "indicative_traded_quantity": indicative_traded_quantity,
                "average_traded_price": average_traded_price,
                "first_open_price": first_open_price,
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "close_price": close_price,
            }
            records.append(rec)

            offset += RECORD_SIZE

        except struct.error as e:
            print(f"[parse_ca2] struct error at offset={offset}: {e}")
            break
        except Exception as e:
            print(f"[parse_ca2] error at offset={offset}: {e}")
            break

    return records


# ============================================================
# Generic dispatcher
# ============================================================

def parse_snapshot(path: str) -> List[Dict[str, Any]]:
    """
    Dispatch based on filename suffix with error handling.
    """
    try:
        lower = path.lower()
        if lower.endswith(".mkt.gz"):
            return parse_mkt(path)
        elif lower.endswith(".ind.gz"):
            return parse_ind(path)
        elif lower.endswith(".ca2.gz"):
            return parse_ca2(path)
        else:
            raise ValueError(f"Unrecognized snapshot type: {path}")
    except Exception as e:
        print(f"❌ Error parsing {path}: {e}")
        return []
