# routes/Angel_One/signals.py
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd

from routes.Angel_One.angel_data import load_json, quote_full_bulk, get_candles
from routes.Angel_One.indicators import compute_indicators


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_stocklist(path: str = "routes/Angel_One/stockList.json") -> Dict[str, List[Dict[str, Any]]]:
    data = load_json(path)
    if not isinstance(data, dict):
        raise ValueError("routes/Angel_One/stockList.json must be an object with categories as keys.")
    return data


def infer_exchange(category: str) -> str:
    c = (category or "").lower()
    if "mcx" in c:
        return "MCX"
    if "index" in c:
        return "NSE"
    # if you later add real derivatives tokens, switch futures/options to NFO
    if "option" in c or "future" in c:
        return "NSE"
    return "NSE"


def flatten_stocklist(stocklist: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for category, items in stocklist.items():
        if not isinstance(items, list):
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            name = str(it.get("name", "")).strip()
            symbol = str(it.get("symbol", "")).strip()
            token = str(it.get("token", "")).strip()
            if not token:
                continue

            exchange = str(it.get("exchange", "")).strip().upper() or infer_exchange(category)
            tradingsymbol = symbol or name

            out.append(
                {
                    "category": category,
                    "exchange": exchange,
                    "name": name,
                    "symbol": symbol,
                    "tradingsymbol": tradingsymbol,
                    "token": token,
                }
            )
    return out


def chunk_tokens(items: List[Dict[str, Any]], chunk_size: int = 50) -> List[Dict[str, List[str]]]:
    chunks: List[Dict[str, List[str]]] = []
    cur: Dict[str, List[str]] = {}
    count = 0

    def push():
        nonlocal cur, count
        if count > 0:
            chunks.append(cur)
        cur = {}
        count = 0

    for it in items:
        ex = it["exchange"]
        tok = str(it["token"]).strip()
        if not tok:
            continue
        cur.setdefault(ex, []).append(tok)
        count += 1
        if count >= chunk_size:
            push()

    push()
    return chunks


def parse_quote_map(quote_resp: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    data = (quote_resp or {}).get("data") or {}
    fetched = data.get("fetched") or []
    for row in fetched:
        ex = str(row.get("exchange", "")).upper()
        tok = str(row.get("symbolToken", "")).strip()
        if ex and tok:
            out[(ex, tok)] = row
    return out


def candles_to_df(candle_resp: Dict[str, Any]) -> Optional[pd.DataFrame]:
    if not candle_resp or not candle_resp.get("status"):
        return None

    data = candle_resp.get("data")
    if not data or not isinstance(data, list):
        return None

    rows = []
    for r in data:
        if not isinstance(r, list) or len(r) < 6:
            continue
        rows.append(
            {
                "time": r[0],
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "volume": float(r[5]),
            }
        )

    if not rows:
        return None

    return pd.DataFrame(rows)


def score_signal(quote_row: Dict[str, Any], ind: Dict[str, Any]) -> Dict[str, Any]:
    ltp = quote_row.get("ltp")
    ema20 = ind.get("ema20")
    rsi14 = ind.get("rsi14")
    vol = quote_row.get("tradeVolume")
    buy_qty = quote_row.get("totBuyQuan")
    sell_qty = quote_row.get("totSellQuan")

    score = 0

    if ltp is not None and ema20 is not None:
        score += 2 if float(ltp) > float(ema20) else -2

    if rsi14 is not None:
        if float(rsi14) > 60:
            score += 2
        elif float(rsi14) < 40:
            score -= 2

    if vol is not None and float(vol) > 0:
        score += 1

    if buy_qty is not None and sell_qty is not None:
        score += 1 if float(buy_qty) > float(sell_qty) else -1

    signal = "WAIT"
    if score >= 3:
        signal = "BUY"
    elif score <= -3:
        signal = "SELL"

    return {"score": score, "signal": signal}


def main(
    stocklist_path: str = "routes/Angel_One/stockList.json",
    tokens_path: str = "tokens.json",
    interval_30m: str = "THIRTY_MINUTE",
    interval_day: str = "ONE_DAY",
    lookback_days_30m: int = 60,
    lookback_days_day: int = 520,
    quote_chunk_size: int = 50,
    quote_sleep_s: float = 1.1,
    min_candles_30m: int = 60,
    min_candles_day: int = 60,
) -> Dict[str, Any]:
    stocklist = load_stocklist(stocklist_path)
    items = flatten_stocklist(stocklist)

    # 1) QUOTES (FULL)  ✅ NEW angel_data signature (no headers)
    quote_maps: Dict[Tuple[str, str], Dict[str, Any]] = {}
    chunks = chunk_tokens(items, chunk_size=quote_chunk_size)

    for ex_tokens in chunks:
        resp = quote_full_bulk(ex_tokens, tokens_path=tokens_path, max_retries=3)
        quote_maps.update(parse_quote_map(resp))
        time.sleep(quote_sleep_s)

    # 2) CANDLES + INDICATORS + SIGNAL
    now = datetime.now()

    from_30m = (now - timedelta(days=lookback_days_30m)).strftime("%Y-%m-%d %H:%M")
    to_30m = now.strftime("%Y-%m-%d %H:%M")

    from_day = (now - timedelta(days=lookback_days_day)).strftime("%Y-%m-%d %H:%M")
    to_day = now.strftime("%Y-%m-%d %H:%M")

    out_items: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for it in items:
        ex = it["exchange"]
        tok = str(it["token"]).strip()

        q = quote_maps.get((ex, tok))
        if not q:
            errors.append({"type": "QUOTE_MISSING", "item": it})
            continue

        # ✅ 30 MIN candles (NEW signature: no headers)
        c30 = get_candles(ex, tok, interval_30m, from_30m, to_30m, tokens_path=tokens_path, max_retries=3)
        df30 = candles_to_df(c30)
        if df30 is None or len(df30) < min_candles_30m:
            errors.append({"type": "CANDLE_30M_MISSING", "item": it, "raw": c30})
            continue
        ind30 = compute_indicators(df30)

        # ✅ DAILY candles
        cday = get_candles(ex, tok, interval_day, from_day, to_day, tokens_path=tokens_path, max_retries=3)
        dfday = candles_to_df(cday)
        if dfday is None or len(dfday) < min_candles_day:
            errors.append({"type": "CANDLE_DAY_MISSING", "item": it, "raw": cday})
            continue
        indday = compute_indicators(dfday)

        sig = score_signal(q, ind30)

        out_items.append(
            {
                **it,
                "quote_full": {
                    "ltp": q.get("ltp"),
                    "open": q.get("open"),
                    "high": q.get("high"),
                    "low": q.get("low"),
                    "close": q.get("close"),
                    "tradeVolume": q.get("tradeVolume"),
                    "opnInterest": q.get("opnInterest"),
                    "totBuyQuan": q.get("totBuyQuan"),
                    "totSellQuan": q.get("totSellQuan"),
                    "52WeekLow": q.get("52WeekLow"),
                    "52WeekHigh": q.get("52WeekHigh"),
                    "depth": q.get("depth"),
                },
                "indicators": {"30m": ind30, "day": indday},
                "decision": sig,
            }
        )

    return {
        "ok": True,
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "intervals": {"30m": interval_30m, "day": interval_day},
        "lookbacks": {"30m_days": lookback_days_30m, "day_days": lookback_days_day},
        "count": len(out_items),
        "errors_count": len(errors),
        "items": out_items,
        "errors": errors,
    }


if __name__ == "__main__":
    res = main(
        stocklist_path="routes/Angel_One/stockList.json",
        tokens_path="tokens.json",
        interval_30m="THIRTY_MINUTE",
        interval_day="ONE_DAY",
        lookback_days_30m=60,
        lookback_days_day=520,
    )
    fname = f"signals_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    save_json(fname, res)
    print(f"\n✅ Saved: {fname}")
    print(f"✅ Signals: {res['count']} | Errors: {res['errors_count']}")
