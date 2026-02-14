"""
Financial-data helpers used by legacy tests.
"""
from __future__ import annotations

import math

import yfinance as yf


def _to_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            v = value.replace("%", "").replace(",", "").strip()
            if not v:
                return default
            return float(v)
        return float(value)
    except Exception:
        return default


def _to_pct(value) -> float:
    v = _to_float(value, 0.0)
    if math.isnan(v):
        return 0.0
    return v * 100 if abs(v) <= 1 else v


def _safe_info(symbol: str) -> dict:
    try:
        ticker = yf.Ticker(symbol)
        return ticker.info or {}
    except Exception:
        return {}


def get_financial_data(symbol: str) -> dict:
    symbol = (symbol or "").upper()
    info = _safe_info(symbol)

    data = {
        "symbol": symbol,
        "name": info.get("shortName", symbol),
        "roe": _to_float(info.get("returnOnEquity", 0.0)),
        "roa": _to_float(info.get("returnOnAssets", 0.0)),
        "profit_margin": _to_float(info.get("profitMargins", 0.0)),
        "operating_margin": _to_float(info.get("operatingMargins", 0.0)),
        "pe_trailing": _to_float(info.get("trailingPE", 0.0)),
        "pb": _to_float(info.get("priceToBook", 0.0)),
        "peg": _to_float(info.get("pegRatio", 0.0)),
        "revenue_growth": _to_float(info.get("revenueGrowth", 0.0)),
        "earnings_growth": _to_float(info.get("earningsGrowth", 0.0)),
        "debt_to_equity": _to_float(info.get("debtToEquity", 0.0)),
        "current_ratio": _to_float(info.get("currentRatio", 0.0)),
        "free_cash_flow": _to_float(info.get("freeCashflow", 0.0)),
        "dividend_yield": _to_float(info.get("dividendYield", 0.0)),
        "payout_ratio": _to_float(info.get("payoutRatio", 0.0)),
    }

    if not info:
        data["error"] = "financial data unavailable"
    return data


def calculate_financial_score(data: dict) -> dict:
    symbol = data.get("symbol", "")

    roe = _to_pct(data.get("roe", 0))
    roa = _to_pct(data.get("roa", 0))
    margin = _to_pct(data.get("profit_margin", 0))
    pe = _to_float(data.get("pe_trailing", 0))
    pb = _to_float(data.get("pb", 0))
    peg = _to_float(data.get("peg", 0))
    rev_growth = _to_pct(data.get("revenue_growth", 0))
    earn_growth = _to_pct(data.get("earnings_growth", 0))
    debt = _to_float(data.get("debt_to_equity", 0))
    current = _to_float(data.get("current_ratio", 0))
    fcf = _to_float(data.get("free_cash_flow", 0))
    div_yield = _to_pct(data.get("dividend_yield", 0))
    payout = _to_pct(data.get("payout_ratio", 0))

    profitability = 50
    profitability += 20 if roe >= 20 else 10 if roe >= 12 else -10 if roe < 0 else 0
    profitability += 15 if margin >= 20 else 8 if margin >= 10 else -10 if margin < 0 else 0
    profitability += 10 if roa >= 8 else 5 if roa >= 4 else 0
    profitability = max(0, min(100, profitability))

    valuation = 50
    if pe > 0:
        valuation += 20 if pe <= 15 else 10 if pe <= 25 else -10 if pe > 40 else 0
    if pb > 0:
        valuation += 10 if pb <= 2 else 5 if pb <= 4 else -8 if pb > 8 else 0
    if peg > 0:
        valuation += 10 if peg <= 1.2 else 5 if peg <= 2 else -10 if peg > 3 else 0
    valuation = max(0, min(100, valuation))

    growth = 50
    growth += 20 if rev_growth >= 20 else 10 if rev_growth >= 8 else -8 if rev_growth < 0 else 0
    growth += 20 if earn_growth >= 20 else 10 if earn_growth >= 8 else -10 if earn_growth < 0 else 0
    growth = max(0, min(100, growth))

    financial_health = 50
    financial_health += 15 if current >= 2 else 8 if current >= 1.2 else -10 if current < 1 else 0
    financial_health += 15 if debt <= 60 else 8 if debt <= 120 else -12 if debt > 250 else 0
    financial_health += 10 if fcf > 0 else -10 if fcf < 0 else 0
    financial_health = max(0, min(100, financial_health))

    dividend = 50
    dividend += 10 if div_yield >= 2 else 5 if div_yield > 0 else 0
    dividend += 10 if 20 <= payout <= 60 else -5 if payout > 90 else 0
    dividend = max(0, min(100, dividend))

    total = (
        profitability * 0.30
        + valuation * 0.25
        + growth * 0.20
        + financial_health * 0.20
        + dividend * 0.05
    )
    financial_score = round(max(0.0, min(100.0, total)), 1)

    if financial_score >= 80:
        grade = "A"
    elif financial_score >= 65:
        grade = "B"
    elif financial_score >= 50:
        grade = "C"
    elif financial_score >= 35:
        grade = "D"
    else:
        grade = "F"

    return {
        "symbol": symbol,
        "financial_score": financial_score,
        "financial_grade": grade,
        "scores": {
            "profitability": round(profitability, 1),
            "valuation": round(valuation, 1),
            "growth": round(growth, 1),
            "financial_health": round(financial_health, 1),
            "dividend": round(dividend, 1),
        },
    }


def get_financial_summary(symbol: str) -> dict:
    base = get_financial_data(symbol)
    score = calculate_financial_score(base)

    key_metrics = {
        "roe": f"{_to_pct(base.get('roe', 0)):.1f}%",
        "pe": f"{_to_float(base.get('pe_trailing', 0)):.1f}",
        "pb": f"{_to_float(base.get('pb', 0)):.1f}",
        "peg": f"{_to_float(base.get('peg', 0)):.1f}",
        "debt_equity": f"{_to_float(base.get('debt_to_equity', 0)):.0f}%",
        "revenue_growth": f"{_to_pct(base.get('revenue_growth', 0)):.1f}%",
        "dividend_yield": f"{_to_pct(base.get('dividend_yield', 0)):.1f}%",
    }

    return {
        **base,
        **score,
        "key_metrics": key_metrics,
    }


def format_financial_report(data: dict) -> str:
    symbol = data.get("symbol", "N/A")
    if data.get("error"):
        return f"❌ {symbol}: {data['error']}"

    return (
        f"📊 {symbol} 재무 리포트\n"
        f"등급: {data.get('financial_grade', 'N/A')} ({data.get('financial_score', 0):.1f})\n"
        f"ROE: {data.get('key_metrics', {}).get('roe', '0.0%')}\n"
        f"P/E: {data.get('key_metrics', {}).get('pe', '0.0')}\n"
        f"매출 성장: {data.get('key_metrics', {}).get('revenue_growth', '0.0%')}"
    )

