"""
Compatibility data fetcher module used by legacy tests.
"""
from __future__ import annotations

import pandas as pd
import yfinance as yf

from core.stock_data import get_market_condition as _core_market_condition


def get_stock_data(symbol: str, period: str = "15mo") -> pd.DataFrame | None:
    """Fetch OHLCV data via yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period)
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None


def check_market_condition() -> dict:
    """Return normalized market condition payload for tests."""
    try:
        result = _core_market_condition() or {}
    except Exception:
        result = {}

    return {
        "status": result.get("status", "unknown"),
        "emoji": result.get("emoji", "❓"),
        "message": result.get("message", "데이터 없음"),
        "price": float(result.get("price", 0) or 0),
        "ma50": float(result.get("ma50", 0) or 0),
        "ma200": float(result.get("ma200", 0) or 0),
    }

