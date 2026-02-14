"""
Legacy strategy module kept for backward-compatible tests.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from ta.trend import MACD
from ta.volatility import BollingerBands


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)


def add_all_indicators(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None or len(df) < 200:
        return None

    out = df.copy()
    out["MA5"] = out["Close"].rolling(5).mean()
    out["MA20"] = out["Close"].rolling(20).mean()
    out["MA50"] = out["Close"].rolling(50).mean()
    out["MA200"] = out["Close"].rolling(200).mean()
    out["RSI"] = _rsi(out["Close"], 14)

    macd = MACD(out["Close"])
    out["MACD"] = macd.macd()
    out["MACD_Signal"] = macd.macd_signal()

    bb = BollingerBands(out["Close"], window=20, window_dev=2)
    out["BB_Upper"] = bb.bollinger_hband()
    out["BB_Lower"] = bb.bollinger_lband()
    out["BB_Mid"] = bb.bollinger_mavg()

    out["Volume_Avg"] = out["Volume"].rolling(20).mean()
    out["High_52w"] = out["High"].rolling(min(252, len(out))).max()
    out["Low_52w"] = out["Low"].rolling(min(252, len(out))).min()

    return out


def _signal(symbol: str, strategy_name: str, emoji: str, price: float, reason: str) -> dict:
    return {
        "symbol": symbol,
        "strategy": strategy_name,
        "emoji": emoji,
        "price": round(float(price), 2),
        "reason": reason,
    }


def strategy_conservative_momentum(df: pd.DataFrame | None, symbol: str) -> dict | None:
    if df is None or len(df) < 200:
        return None
    last = df.iloc[-1]
    if (
        float(last["Close"]) > float(last["MA50"])
        and 40 <= float(last["RSI"]) <= 65
        and float(last["Volume"]) >= float(last["Volume_Avg"] or 0) * 0.8
    ):
        return _signal(symbol, "보수적 모멘텀", "🛡", last["Close"], "추세와 RSI가 안정적")
    return None


def strategy_golden_cross(df: pd.DataFrame | None, symbol: str) -> dict | None:
    if df is None or len(df) < 2:
        return None
    prev = df.iloc[-2]
    last = df.iloc[-1]
    if float(prev["MA5"]) <= float(prev["MA20"]) and float(last["MA5"]) > float(last["MA20"]):
        return _signal(symbol, "골든크로스", "✨", last["Close"], "MA5가 MA20 상향 돌파")
    return None


def strategy_bollinger_bounce(df: pd.DataFrame | None, symbol: str) -> dict | None:
    if df is None or len(df) < 1:
        return None
    last = df.iloc[-1]
    if float(last["Close"]) <= float(last["BB_Lower"]) * 1.01 and float(last["RSI"]) < 35:
        return _signal(symbol, "볼린저 반등", "📊", last["Close"], "하단 밴드 + 과매도")
    return None


def strategy_macd_crossover(df: pd.DataFrame | None, symbol: str) -> dict | None:
    if df is None or len(df) < 2:
        return None
    prev = df.iloc[-2]
    last = df.iloc[-1]
    if float(prev["MACD"]) <= float(prev["MACD_Signal"]) and float(last["MACD"]) > float(last["MACD_Signal"]):
        return _signal(symbol, "MACD 크로스", "📈", last["Close"], "MACD 상향 교차")
    return None


def strategy_near_52w_high(df: pd.DataFrame | None, symbol: str) -> dict | None:
    if df is None or len(df) < 1:
        return None
    last = df.iloc[-1]
    high_52w = float(last["High_52w"] or 0)
    if high_52w <= 0:
        return None
    gap = (high_52w - float(last["Close"])) / high_52w * 100
    if 0 <= gap <= 5:
        return _signal(symbol, "52주 신고가 근접", "🏅", last["Close"], "52주 고점 5% 이내")
    return None


def strategy_dip_bounce(df: pd.DataFrame | None, symbol: str) -> dict | None:
    if df is None or len(df) < 6:
        return None
    close = df["Close"]
    drop_5d = (float(close.iloc[-1]) - float(close.iloc[-6])) / float(close.iloc[-6]) * 100
    if drop_5d <= -8 and float(close.iloc[-1]) > float(close.iloc[-2]):
        return _signal(symbol, "급락 반등", "🪂", close.iloc[-1], f"5일 {drop_5d:.1f}% 후 반등")
    return None


def strategy_volume_surge(df: pd.DataFrame | None, symbol: str) -> dict | None:
    if df is None or len(df) < 2:
        return None
    last = df.iloc[-1]
    prev = df.iloc[-2]
    vol_avg = float(last["Volume_Avg"] or 0)
    if vol_avg <= 0:
        return None
    ratio = float(last["Volume"]) / vol_avg
    if ratio >= 2 and float(last["Close"]) > float(prev["Close"]):
        return _signal(symbol, "거래량 급증", "🚀", last["Close"], f"평균 대비 {ratio:.1f}배")
    return None


def analyze_risk_level(df: pd.DataFrame | None, symbol: str) -> dict:
    if df is None or len(df) == 0:
        return {
            "symbol": symbol,
            "price": 0.0,
            "risk_score": 100,
            "risk_grade": "🔴 고위험",
            "warnings": ["데이터 부족"],
        }

    last = df.iloc[-1]
    risk = 0
    warnings = []

    rsi = float(last.get("RSI", 50))
    if rsi >= 70:
        risk += 30
        warnings.append("RSI 과매수")
    elif rsi <= 30:
        risk += 20
        warnings.append("RSI 과매도")

    ma200 = float(last.get("MA200", last["Close"]))
    close = float(last["Close"])
    if close < ma200:
        risk += 25
        warnings.append("200일선 하회")

    vol_avg = float(last.get("Volume_Avg", 0) or 0)
    if vol_avg > 0 and float(last["Volume"]) / vol_avg >= 2:
        risk += 10
        warnings.append("거래량 급증")

    risk = int(max(0, min(100, risk)))
    if risk >= 60:
        grade = "🔴 고위험"
    elif risk >= 35:
        grade = "🟡 주의"
    else:
        grade = "🟢 양호"

    return {
        "symbol": symbol,
        "price": round(close, 2),
        "risk_score": risk,
        "risk_grade": grade,
        "warnings": warnings,
    }


ALL_STRATEGIES = [
    ("🛡", "보수적 모멘텀", strategy_conservative_momentum),
    ("✨", "골든크로스", strategy_golden_cross),
    ("📊", "볼린저 반등", strategy_bollinger_bounce),
    ("📈", "MACD 크로스", strategy_macd_crossover),
    ("🏅", "52주 신고가 근접", strategy_near_52w_high),
    ("🪂", "급락 반등", strategy_dip_bounce),
    ("🚀", "거래량 급증", strategy_volume_surge),
]

