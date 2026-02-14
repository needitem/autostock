"""
Compatibility analyzer module used by legacy tests.
"""
from __future__ import annotations

from data_fetcher import check_market_condition, get_stock_data
from market_data import get_fear_greed_index
from strategies import (
    ALL_STRATEGIES,
    add_all_indicators,
    analyze_risk_level,
)

try:
    from config import NASDAQ_100
except Exception:
    NASDAQ_100 = []


def _default_symbols() -> list[str]:
    if NASDAQ_100:
        return NASDAQ_100
    try:
        from config import load_nasdaq_100

        return load_nasdaq_100()
    except Exception:
        return []


def analyze_stock_all_strategies(symbol: str) -> list[dict]:
    df = get_stock_data(symbol)
    if df is None:
        return []

    enriched = add_all_indicators(df)
    if enriched is None:
        return []

    results = []
    for _emoji, _name, func in ALL_STRATEGIES:
        try:
            signal = func(enriched, symbol)
            if signal:
                results.append(signal)
        except Exception:
            continue
    return results


def analyze_single_stock(symbol: str) -> dict | None:
    df = get_stock_data(symbol)
    if df is None:
        return None

    enriched = add_all_indicators(df)
    if enriched is None:
        return None

    latest = enriched.iloc[-1]
    risk = analyze_risk_level(enriched, symbol)
    signals = analyze_stock_all_strategies(symbol)

    return {
        "symbol": symbol,
        "price": round(float(latest["Close"]), 2),
        "rsi": round(float(latest.get("RSI", 50)), 1),
        "ma50_gap": round((float(latest["Close"]) / float(latest["MA50"]) - 1) * 100, 1)
        if float(latest.get("MA50", 0) or 0) > 0
        else 0.0,
        "change_5d": round((float(enriched["Close"].iloc[-1]) / float(enriched["Close"].iloc[-6]) - 1) * 100, 1)
        if len(enriched) >= 6
        else 0.0,
        "risk_score": risk["risk_score"],
        "risk_grade": risk["risk_grade"],
        "warnings": risk["warnings"],
        "strategies_matched": [s["strategy"] for s in signals],
    }


def scan_all_stocks(symbols: list[str] | None = None) -> dict:
    symbols = symbols or _default_symbols()
    strategy_results = {}
    for symbol in symbols:
        strategy_results[symbol] = analyze_stock_all_strategies(symbol)

    return {
        "market": check_market_condition(),
        "fear_greed": get_fear_greed_index(),
        "strategy_results": strategy_results,
    }


def get_recommendations(top_n: int = 20, symbols: list[str] | None = None) -> dict:
    symbols = symbols or _default_symbols()
    recommendations = []

    for symbol in symbols:
        analysis = analyze_single_stock(symbol)
        if not analysis:
            continue
        if analysis.get("strategies_matched") or analysis.get("risk_score", 100) <= 35:
            recommendations.append(analysis)

    recommendations.sort(
        key=lambda x: (-len(x.get("strategies_matched", [])), x.get("risk_score", 100))
    )

    return {
        "recommendations": recommendations[:top_n],
        "total_analyzed": len(symbols),
    }
