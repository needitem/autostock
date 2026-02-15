"""
Compatibility news module kept for legacy tests.

Production code lives in `core.news` (keyless RSS + yfinance).
"""

from __future__ import annotations

from core.news import (
    get_company_news as _get_company_news,
    get_earnings_calendar as _get_earnings_calendar,
    get_market_news as _get_market_news,
    get_price_target as _get_price_target,
    get_recommendation_trends as _get_recommendation_trends,
)


EVENT_DESCRIPTIONS = {
    "FOMC": {"name": "FOMC Meeting", "impact": "high", "desc": "Federal Reserve rate decision."},
    "CPI": {"name": "Consumer Price Index", "impact": "high", "desc": "Inflation gauge affecting rates."},
    "NFP": {"name": "Non-Farm Payrolls", "impact": "high", "desc": "US labor market strength indicator."},
    "earnings": {"name": "Earnings", "impact": "medium", "desc": "Quarterly company earnings releases."},
}


def get_company_news(symbol: str, days: int = 7) -> list[dict]:
    try:
        return _get_company_news(symbol, days=days) or []
    except Exception:
        return []


def get_earnings_calendar(days: int = 14) -> list[dict]:
    try:
        return _get_earnings_calendar(days=days) or []
    except Exception:
        return []


def get_insider_transactions(symbol: str, days: int = 90) -> list[dict]:
    # Optional source: keep a lightweight fallback for compatibility tests.
    return []


def get_recommendation_trends(symbol: str) -> dict | None:
    try:
        return _get_recommendation_trends(symbol)
    except Exception:
        return None


def get_price_target(symbol: str) -> dict | None:
    try:
        return _get_price_target(symbol)
    except Exception:
        return None


def get_ipo_calendar(days: int = 30) -> list[dict]:
    # Optional source: keep a lightweight fallback for compatibility tests.
    return []


def get_market_news(category: str = "general") -> list[dict]:
    try:
        return _get_market_news(category=category) or []
    except Exception:
        return []

