"""
Technical indicator engine used by scanners, watchlists and portfolio logic.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import ADXIndicator, EMAIndicator, MACD, SMAIndicator
from ta.volatility import AverageTrueRange, BollingerBands
from ta.volume import OnBalanceVolumeIndicator


def _f(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if np.isnan(out) or np.isinf(out):
            return default
        return out
    except Exception:
        return default


def _round(value: Any, digits: int = 2, default: float = 0.0) -> float:
    return round(_f(value, default), digits)


def _pct_gap(price: float, base: float) -> float:
    if base == 0:
        return 0.0
    return (price - base) / base * 100


def calculate_indicators(df: pd.DataFrame) -> dict[str, Any] | None:
    """Compute a broad indicator set from OHLCV data."""
    if df is None or len(df) < 200:
        return None

    required = ["Open", "High", "Low", "Close", "Volume"]
    if any(col not in df.columns for col in required):
        return None

    frame = df[required].copy()
    frame = frame.dropna(subset=["Open", "High", "Low", "Close"])
    if len(frame) < 200:
        return None

    close = pd.to_numeric(frame["Close"], errors="coerce")
    high = pd.to_numeric(frame["High"], errors="coerce")
    low = pd.to_numeric(frame["Low"], errors="coerce")
    open_ = pd.to_numeric(frame["Open"], errors="coerce")
    volume = pd.to_numeric(frame["Volume"], errors="coerce").fillna(0)

    close = close.ffill().bfill()
    high = high.ffill().bfill()
    low = low.ffill().bfill()
    open_ = open_.ffill().bfill()

    ma5 = SMAIndicator(close, window=5).sma_indicator()
    ma20 = SMAIndicator(close, window=20).sma_indicator()
    ma50 = SMAIndicator(close, window=50).sma_indicator()
    ma200 = SMAIndicator(close, window=200).sma_indicator()
    ema12 = EMAIndicator(close, window=12).ema_indicator()
    ema26 = EMAIndicator(close, window=26).ema_indicator()

    rsi = RSIIndicator(close, window=14).rsi()
    stoch = StochasticOscillator(high, low, close, window=14, smooth_window=3)
    stoch_k = stoch.stoch()
    stoch_d = stoch.stoch_signal()

    macd = MACD(close)
    macd_line = macd.macd()
    macd_signal = macd.macd_signal()
    macd_hist = macd.macd_diff()

    bb = BollingerBands(close, window=20, window_dev=2)
    bb_upper = bb.bollinger_hband()
    bb_lower = bb.bollinger_lband()
    bb_mid = bb.bollinger_mavg()

    atr = AverageTrueRange(high, low, close, window=14).average_true_range()
    adx = ADXIndicator(high, low, close, window=14).adx()
    obv = OnBalanceVolumeIndicator(close, volume).on_balance_volume()

    volume_avg = volume.rolling(window=20, min_periods=1).mean()
    high_52w = high.rolling(window=252, min_periods=20).max()
    low_52w = low.rolling(window=252, min_periods=20).min()

    price = _f(close.iloc[-1])
    price_prev = _f(close.iloc[-2], price)

    bb_u = _f(bb_upper.iloc[-1], price)
    bb_l = _f(bb_lower.iloc[-1], price)
    bb_m = _f(bb_mid.iloc[-1], price)
    bb_range = max(0.0, bb_u - bb_l)
    bb_position = 50.0 if bb_range == 0 else (price - bb_l) / bb_range * 100

    high52 = _f(high_52w.iloc[-1], price)
    low52 = _f(low_52w.iloc[-1], price)
    range52 = max(0.0, high52 - low52)
    position_52w = 50.0 if range52 == 0 else (price - low52) / range52 * 100

    price_5d_ago = _f(close.iloc[-6], price) if len(close) >= 6 else price
    change_5d = _pct_gap(price, price_5d_ago)
    down_days = int((close.tail(5).diff().dropna() < 0).sum())

    support, resistance = find_support_resistance(frame)
    crosses = detect_crosses(ma5, ma20, ma50, ma200, macd_line, macd_signal)
    vol_signal = analyze_volume(frame, volume_avg)
    fib_levels = calculate_fibonacci(high52, low52)
    candle_patterns = detect_candle_patterns(frame)

    ma5_now = _f(ma5.iloc[-1], price)
    ma20_now = _f(ma20.iloc[-1], price)
    ma50_now = _f(ma50.iloc[-1], price)
    ma200_now = _f(ma200.iloc[-1], price)
    vol_now = _f(volume.iloc[-1], 0)
    vol_avg_now = _f(volume_avg.iloc[-1], 0)
    obv_now = _f(obv.iloc[-1], 0)
    obv_prev = _f(obv.iloc[-6], obv_now) if len(obv) >= 6 else obv_now

    return {
        "price": _round(price),
        "ma5": _round(ma5_now),
        "ma20": _round(ma20_now),
        "ma50": _round(ma50_now),
        "ma200": _round(ma200_now),
        "ema12": _round(ema12.iloc[-1]),
        "ema26": _round(ema26.iloc[-1]),
        "rsi": _round(rsi.iloc[-1], 1, 50.0),
        "stoch_k": _round(stoch_k.iloc[-1], 1, 50.0),
        "stoch_d": _round(stoch_d.iloc[-1], 1, 50.0),
        "macd": _round(macd_line.iloc[-1], 3),
        "macd_signal": _round(macd_signal.iloc[-1], 3),
        "macd_hist": _round(macd_hist.iloc[-1], 3),
        "bb_upper": _round(bb_u),
        "bb_lower": _round(bb_l),
        "bb_mid": _round(bb_m),
        "bb_position": _round(bb_position, 1, 50.0),
        "atr": _round(atr.iloc[-1]),
        "atr_pct": _round(_pct_gap(price + _f(atr.iloc[-1]), price), 2) if price else 0.0,
        "adx": _round(adx.iloc[-1], 1, 0.0),
        "volume": int(max(0, vol_now)),
        "volume_avg": int(max(0, vol_avg_now)),
        "volume_ratio": _round(vol_now / vol_avg_now, 2, 1.0) if vol_avg_now > 0 else 1.0,
        "obv": int(obv_now),
        "obv_change": _round(_pct_gap(obv_now, obv_prev), 1) if obv_prev != 0 else 0.0,
        "high_52w": _round(high52),
        "low_52w": _round(low52),
        "position_52w": _round(position_52w, 1, 50.0),
        "ma5_gap": _round(_pct_gap(price, ma5_now), 1),
        "ma20_gap": _round(_pct_gap(price, ma20_now), 1),
        "ma50_gap": _round(_pct_gap(price, ma50_now), 1),
        "ma200_gap": _round(_pct_gap(price, ma200_now), 1),
        "change_5d": _round(change_5d, 1),
        "down_days": down_days,
        "ma5_prev": _round(ma5.iloc[-2], 2, ma5_now),
        "ma20_prev": _round(ma20.iloc[-2], 2, ma20_now),
        "macd_prev": _round(macd_line.iloc[-2], 3),
        "macd_signal_prev": _round(macd_signal.iloc[-2], 3),
        "price_prev": _round(price_prev, 2, price),
        "bb_lower_prev": _round(bb_lower.iloc[-2], 2, bb_l),
        "candle_patterns": candle_patterns,
        "support": support,
        "resistance": resistance,
        "crosses": crosses,
        "volume_signal": vol_signal,
        "fib_levels": fib_levels,
    }


def detect_candle_patterns(df: pd.DataFrame) -> list[dict[str, str]]:
    """Detect a small, high-signal subset of candle patterns."""
    patterns: list[dict[str, str]] = []
    if df is None or len(df) < 3:
        return patterns

    recent = df.tail(5).copy()
    for _, row in recent.iterrows():
        o = _f(row["Open"])
        h = _f(row["High"])
        l = _f(row["Low"])
        c = _f(row["Close"])
        body = abs(c - o)
        full = max(0.0001, h - l)
        upper = h - max(o, c)
        lower = min(o, c) - l

        if body / full <= 0.1:
            patterns.append({"pattern": "Doji", "signal": "중립", "desc": "추세 전환 가능성"})
        elif lower > body * 2 and upper < body:
            signal = "매수" if c >= o else "주의"
            patterns.append({"pattern": "Hammer", "signal": signal, "desc": "하단 지지 확인"})
        elif upper > body * 2 and lower < body:
            signal = "매도" if c <= o else "주의"
            patterns.append({"pattern": "Shooting Star", "signal": signal, "desc": "상단 저항 가능성"})

    prev = recent.iloc[-2]
    curr = recent.iloc[-1]
    if _f(prev["Close"]) < _f(prev["Open"]) and _f(curr["Close"]) > _f(curr["Open"]):
        if _f(curr["Open"]) <= _f(prev["Close"]) and _f(curr["Close"]) >= _f(prev["Open"]):
            patterns.append({"pattern": "Bullish Engulfing", "signal": "매수", "desc": "반등 신호"})
    if _f(prev["Close"]) > _f(prev["Open"]) and _f(curr["Close"]) < _f(curr["Open"]):
        if _f(curr["Open"]) >= _f(prev["Close"]) and _f(curr["Close"]) <= _f(prev["Open"]):
            patterns.append({"pattern": "Bearish Engulfing", "signal": "매도", "desc": "하락 신호"})

    return patterns[-5:]


def find_support_resistance(df: pd.DataFrame, lookback: int = 80) -> tuple[list[float], list[float]]:
    """
    Detect local extrema and return nearest levels.

    Supports: sorted descending (nearest below first)
    Resistances: sorted ascending (nearest above first)
    """
    if df is None or len(df) < 20:
        return [], []

    recent = df.tail(lookback).copy()
    highs = recent["High"].to_numpy(dtype=float)
    lows = recent["Low"].to_numpy(dtype=float)
    price = _f(recent["Close"].iloc[-1])

    supports: list[float] = []
    resistances: list[float] = []
    for i in range(2, len(recent) - 2):
        if highs[i] > highs[i - 1] and highs[i] > highs[i - 2] and highs[i] > highs[i + 1] and highs[i] > highs[i + 2]:
            if highs[i] > price:
                resistances.append(round(float(highs[i]), 2))
        if lows[i] < lows[i - 1] and lows[i] < lows[i - 2] and lows[i] < lows[i + 1] and lows[i] < lows[i + 2]:
            if lows[i] < price:
                supports.append(round(float(lows[i]), 2))

    supports = sorted(set(supports), reverse=True)[:3]
    resistances = sorted(set(resistances))[:3]
    return supports, resistances


def detect_crosses(
    ma5: pd.Series,
    ma20: pd.Series,
    ma50: pd.Series,
    ma200: pd.Series,
    macd_line: pd.Series,
    macd_signal: pd.Series,
) -> list[dict[str, str]]:
    """Detect moving-average and MACD crosses."""
    crosses: list[dict[str, str]] = []
    if len(ma5) < 2 or len(ma20) < 2:
        return crosses

    if _f(ma5.iloc[-2]) <= _f(ma20.iloc[-2]) and _f(ma5.iloc[-1]) > _f(ma20.iloc[-1]):
        crosses.append({"type": "골든크로스", "detail": "5일선 > 20일선", "signal": "매수"})
    if _f(ma5.iloc[-2]) >= _f(ma20.iloc[-2]) and _f(ma5.iloc[-1]) < _f(ma20.iloc[-1]):
        crosses.append({"type": "데드크로스", "detail": "5일선 < 20일선", "signal": "매도"})

    if len(ma50) >= 2 and len(ma200) >= 2:
        if _f(ma50.iloc[-2]) <= _f(ma200.iloc[-2]) and _f(ma50.iloc[-1]) > _f(ma200.iloc[-1]):
            crosses.append({"type": "장기골든크로스", "detail": "50일선 > 200일선", "signal": "강한매수"})
        if _f(ma50.iloc[-2]) >= _f(ma200.iloc[-2]) and _f(ma50.iloc[-1]) < _f(ma200.iloc[-1]):
            crosses.append({"type": "장기데드크로스", "detail": "50일선 < 200일선", "signal": "강한매도"})

    if len(macd_line) >= 2 and len(macd_signal) >= 2:
        if _f(macd_line.iloc[-2]) <= _f(macd_signal.iloc[-2]) and _f(macd_line.iloc[-1]) > _f(macd_signal.iloc[-1]):
            crosses.append({"type": "MACD골든", "detail": "MACD 상향 돌파", "signal": "매수"})
        if _f(macd_line.iloc[-2]) >= _f(macd_signal.iloc[-2]) and _f(macd_line.iloc[-1]) < _f(macd_signal.iloc[-1]):
            crosses.append({"type": "MACD데드", "detail": "MACD 하향 돌파", "signal": "매도"})

    return crosses


def analyze_volume(df: pd.DataFrame, volume_avg: pd.Series) -> dict[str, Any]:
    """Classify today's volume context."""
    if df is None or len(df) < 2:
        return {"signal": "중립", "ratio": 1.0, "desc": "데이터 부족"}

    curr = _f(df["Volume"].iloc[-1], 0)
    avg = _f(volume_avg.iloc[-1], 0)
    prev_close = _f(df["Close"].iloc[-2], 0)
    curr_close = _f(df["Close"].iloc[-1], prev_close)
    change_pct = _pct_gap(curr_close, prev_close) if prev_close else 0.0

    if avg <= 0:
        return {"signal": "중립", "ratio": 1.0, "desc": "거래량 평균 없음"}

    ratio = curr / avg
    if ratio >= 3 and change_pct > 0:
        return {"signal": "강한매수", "ratio": round(ratio, 1), "desc": "대량 거래 동반 상승"}
    if ratio >= 3 and change_pct < 0:
        return {"signal": "강한매도", "ratio": round(ratio, 1), "desc": "대량 거래 동반 하락"}
    if ratio >= 2 and change_pct > 0:
        return {"signal": "매수", "ratio": round(ratio, 1), "desc": "거래량 급증 + 상승"}
    if ratio >= 2 and change_pct < 0:
        return {"signal": "매도", "ratio": round(ratio, 1), "desc": "거래량 급증 + 하락"}
    if ratio <= 0.6:
        return {"signal": "중립", "ratio": round(ratio, 1), "desc": "거래 부진"}
    return {"signal": "중립", "ratio": round(ratio, 1), "desc": "평균 수준"}


def calculate_fibonacci(high: float, low: float) -> dict[str, float]:
    """Calculate common Fibonacci retracement levels."""
    high = _f(high)
    low = _f(low)
    if high <= low:
        return {
            "0.0": round(low, 2),
            "0.236": round(low, 2),
            "0.382": round(low, 2),
            "0.5": round(low, 2),
            "0.618": round(low, 2),
            "0.786": round(low, 2),
            "1.0": round(high, 2),
        }

    diff = high - low
    return {
        "0.0": round(low, 2),
        "0.236": round(low + diff * 0.236, 2),
        "0.382": round(low + diff * 0.382, 2),
        "0.5": round(low + diff * 0.5, 2),
        "0.618": round(low + diff * 0.618, 2),
        "0.786": round(low + diff * 0.786, 2),
        "1.0": round(high, 2),
    }


def get_full_analysis(symbol: str) -> dict[str, Any] | None:
    """Fetch technical + basic data in one payload."""
    from core.stock_data import get_finviz_data, get_stock_data, get_stock_info

    df = get_stock_data(symbol)
    if df is None:
        return None

    indicators = calculate_indicators(df)
    if indicators is None:
        return None

    info = get_stock_info(symbol)
    finviz = get_finviz_data(symbol) or {}

    return {
        "symbol": symbol,
        **indicators,
        **{k: v for k, v in info.items() if k != "symbol"},
        "finviz": finviz,
    }


__all__ = [
    "calculate_indicators",
    "detect_candle_patterns",
    "find_support_resistance",
    "detect_crosses",
    "analyze_volume",
    "calculate_fibonacci",
    "get_full_analysis",
]

