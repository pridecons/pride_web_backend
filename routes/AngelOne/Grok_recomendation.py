"""
Grok recommendation helper.

Used by the live signals producer to generate a structured trade plan when a stock's
score crosses a threshold.

Notes:
- We keep the output JSON-only (no markdown) so you can store/stream it directly.
- This does NOT do any news search by itself. If you want news, pass it in `news_context`.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import requests

from config import GROK_API_KEY


GROK_URL = "https://api.x.ai/v1/chat/completions"
GROK_MODEL = "grok-4-latest"
GROK_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {GROK_API_KEY}",
}


def _json_only_schema() -> str:
    """Strict schema to force JSON output."""
    return (
        "Return ONLY valid JSON (no markdown). Schema:\n"
        "{\n"
        "  'symbol': str,\n"
        "  'exchange': str,\n"
        "  'direction': 'BUY'|'SELL',\n"
        "  'entry': number,\n"
        "  'stop_loss': number,\n"
        "  'targets': {'t1': number, 't2': number, 't3': number},\n"
        "  'timeframe': str,\n"
        "  'confidence': number,  // 0..100\n"
        "  'rationale': [str, ...],\n"
        "  'risk_notes': [str, ...],\n"
        "  'news': [ {'title': str, 'source': str, 'published_at': str, 'url': str, 'accuracy': number}, ... ]\n"
        "}\n"
        "Rules:\n"
        "- Use 0..100 for confidence and news accuracy.\n"
        "- If you have no news, return an empty list for 'news'.\n"
        "- Keep rationale concrete and derived from provided data (RSI/EMA/MACD/Volume/Orderflow).\n"
    )


def generate_trade_plan_with_grok(
    *,
    symbol: str,
    exchange: str,
    score: int,
    signal: str,
    quote_full: Dict[str, Any],
    indicators_30m: Optional[Dict[str, Any]] = None,
    indicators_day: Optional[Dict[str, Any]] = None,
    local_plan: Optional[Dict[str, Any]] = None,
    news_context: Optional[List[Dict[str, Any]]] = None,
    timeout_sec: int = 20,
) -> Dict[str, Any]:
    """
    Call Grok and return a structured plan.

    `local_plan` is a precomputed plan (entry/SL/targets) which Grok should validate and
    add rational reasons for.
    """
    indicators_30m = indicators_30m or {}
    indicators_day = indicators_day or {}
    news_context = news_context or []

    system = (
        "You are a trading assistant. Create a practical intraday trade plan using ONLY the provided data. "
        "No guarantees. Be conservative and include risk notes."
    )

    user = {
        "symbol": symbol,
        "exchange": exchange,
        "score": score,
        "signal": signal,
        "quote_full": quote_full,
        "indicators": {
            "30m": indicators_30m,
            "day": indicators_day,
        },
        "suggested_plan": local_plan or {},
        "news_context": news_context,
        "instruction": _json_only_schema(),
    }

    payload = {
        "model": GROK_MODEL,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
    }

    r = requests.post(GROK_URL, headers=GROK_HEADERS, json=payload, timeout=(10, 300))
    r.raise_for_status()
    data = r.json()

    content = (
        (((data.get("choices") or [None])[0] or {}).get("message") or {}).get("content")
        or ""
    ).strip()

    try:
        obj = json.loads(content)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # Fallback: return raw if model didn't comply
    return {
        "symbol": symbol,
        "exchange": exchange,
        "direction": "BUY" if signal == "BUY" else "SELL",
        "entry": float(quote_full.get("ltp") or 0) or None,
        "stop_loss": None,
        "targets": {"t1": None, "t2": None, "t3": None},
        "timeframe": "intraday",
        "confidence": 0,
        "rationale": ["Grok output was not valid JSON; returning fallback."],
        "risk_notes": ["Validate manually before taking any trade."],
        "news": [],
        "raw": content,
    }
