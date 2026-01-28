# routes/ANGEL_ONE/indicators.py
import numpy as np
import pandas as pd
from typing import Dict, Any


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.bfill()


def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    fast_ema = ema(series, fast)
    slow_ema = ema(series, slow)
    macd_line = fast_ema - slow_ema
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def compute_indicators(df: pd.DataFrame) -> Dict[str, Any]:
    """
    df columns required: close
    """
    close = df["close"].astype(float)

    out = {}
    out["ema20"] = float(ema(close, 20).iloc[-1])
    out["ema50"] = float(ema(close, 50).iloc[-1])

    # ✅ NEW
    out["sma50"] = float(sma(close, 50).iloc[-1]) if len(close) >= 50 else None
    out["sma200"] = float(sma(close, 200).iloc[-1]) if len(close) >= 200 else None

    r = rsi(close, 14)
    out["rsi14"] = float(r.iloc[-1])

    m, s, h = macd(close, 12, 26, 9)
    out["macd"] = float(m.iloc[-1])
    out["macd_signal"] = float(s.iloc[-1])
    out["macd_hist"] = float(h.iloc[-1])
    return out

