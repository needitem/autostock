"""
Core market-data access layer.

This module intentionally keeps API contracts stable for the rest of the app:
- get_stock_data
- get_stock_info
- get_finviz_data
- get_market_condition
- get_fear_greed_index
"""

from __future__ import annotations

import os
import math
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


REQUEST_TIMEOUT = 10
_RETRYABLE = [429, 500, 502, 503, 504]


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=0.2,
        status_forcelist=_RETRYABLE,
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "autostock/2.0"})
    return session


_SESSION = _build_session()


def _init_yfinance_cache() -> None:
    """Force yfinance sqlite cache into a writable project path."""
    try:
        root = Path(__file__).resolve().parents[2]
        cache_dir = Path(
            os.getenv("AI_YF_CACHE_DIR") or os.getenv("YF_CACHE_DIR") or str(root / "data" / "yf_cache")
        ).resolve()
        cache_dir.mkdir(parents=True, exist_ok=True)
        yf.set_tz_cache_location(str(cache_dir))
        os.environ["YF_CACHE_DIR"] = str(cache_dir)
    except Exception:
        # Keep market-data functions resilient even if cache init fails.
        pass


_init_yfinance_cache()


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
            if value in {"", "-", "N/A", "None", "nan"}:
                return default
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _clean_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _env_bool(key: str, default: bool = False) -> bool:
    raw = str(os.getenv(key, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "on", "y"}


def _cache_bucket(env_key: str, default_minutes: int) -> int:
    try:
        ttl = int(os.getenv(env_key, str(default_minutes)))
    except Exception:
        ttl = int(default_minutes)
    ttl = max(1, ttl)
    return int(time.time() // (ttl * 60))


def _days_until_ts(value: Any) -> int | None:
    ts = int(_to_float(value, 0))
    if ts <= 0:
        return None
    try:
        event_dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone()
        today = datetime.now(tz=event_dt.tzinfo).date()
        return (event_dt.date() - today).days
    except Exception:
        return None


@lru_cache(maxsize=512)
def _get_stock_data_cached(
    symbol: str,
    period: str,
    auto_adjust: bool,
    bucket: int,
) -> pd.DataFrame | None:
    _ = bucket
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, actions=False, auto_adjust=auto_adjust)
        if df is None or df.empty:
            return None

        required = ["Open", "High", "Low", "Close", "Volume"]
        if any(col not in df.columns for col in required):
            return None

        clean = (
            df[required]
            .dropna(subset=["Open", "High", "Low", "Close"])
            .sort_index()
            .copy()
        )
        return clean if not clean.empty else None
    except Exception:
        return None


def get_stock_data(symbol: str, period: str = "15mo", auto_adjust: bool | None = None) -> pd.DataFrame | None:
    """
    Fetch OHLCV history from Yahoo Finance.

    Returns None when data is unavailable.
    """
    symbol = _clean_symbol(symbol)
    if not symbol:
        return None

    if auto_adjust is None:
        auto_adjust = _env_bool("AI_YF_AUTO_ADJUST", True)
    bucket = _cache_bucket("AI_YF_CACHE_TTL_MINUTES", 60)
    return _get_stock_data_cached(symbol, period, bool(auto_adjust), bucket)


@lru_cache(maxsize=512)
def _get_ticker_info_cached(symbol: str, bucket: int) -> dict[str, Any]:
    _ = bucket
    symbol = _clean_symbol(symbol)
    if not symbol:
        return {}
    try:
        return yf.Ticker(symbol).info or {}
    except Exception:
        return {}


def _get_ticker_info(symbol: str) -> dict[str, Any]:
    bucket = _cache_bucket("AI_YF_INFO_CACHE_TTL_MINUTES", 360)
    return _get_ticker_info_cached(symbol, bucket)


def get_stock_info(symbol: str) -> dict[str, Any]:
    """Fetch fundamentals/metadata with safe defaults."""
    symbol = _clean_symbol(symbol)
    info = _get_ticker_info(symbol)
    earnings_ts = info.get("earningsTimestamp")
    earnings_ts_start = info.get("earningsTimestampStart")
    earnings_ts_end = info.get("earningsTimestampEnd")

    earnings_days_candidates = [
        days
        for days in (
            _days_until_ts(earnings_ts),
            _days_until_ts(earnings_ts_start),
            _days_until_ts(earnings_ts_end),
        )
        if days is not None
    ]
    # We only expose upcoming earnings D-day. Past events should not produce negative D values.
    future_days = [days for days in earnings_days_candidates if days >= 0]
    days_to_earnings = min(future_days) if future_days else None

    price = _to_float(info.get("currentPrice") or info.get("regularMarketPrice"), 0.0)
    target_price = _to_float(info.get("targetMeanPrice"), 0.0)
    target_upside_pct = ((target_price - price) / price * 100) if price > 0 and target_price > 0 else 0.0

    forward_eps = _to_float(info.get("forwardEps"), 0.0)
    trailing_eps = _to_float(info.get("trailingEps"), 0.0)
    forward_eps_growth_pct = (
        ((forward_eps - trailing_eps) / abs(trailing_eps) * 100)
        if trailing_eps != 0
        else 0.0
    )

    # yfinance values can be ratios (0.15) or percents (15). Keep raw form.
    return {
        "symbol": symbol,
        "name": info.get("shortName", symbol),
        "sector": info.get("sector", "N/A"),
        "industry": info.get("industry", "N/A"),
        "price": price,
        "market_cap": _to_float(info.get("marketCap"), 0.0),
        "avg_volume": _to_float(info.get("averageVolume") or info.get("averageDailyVolume10Day"), 0.0),
        "shares_outstanding": _to_float(info.get("sharesOutstanding"), 0.0),
        "float_shares": _to_float(info.get("floatShares"), 0.0),
        "roe": info.get("returnOnEquity", 0),
        "roa": info.get("returnOnAssets", 0),
        "profit_margin": info.get("profitMargins", 0),
        "operating_margin": info.get("operatingMargins", 0),
        "pe": info.get("trailingPE", 0),
        "forward_pe": info.get("forwardPE", 0),
        "peg": info.get("pegRatio", 0),
        "pb": info.get("priceToBook", 0),
        "revenue_growth": info.get("revenueGrowth", 0),
        "earnings_growth": info.get("earningsGrowth", 0),
        "debt_to_equity": info.get("debtToEquity", 0),
        "current_ratio": info.get("currentRatio", 0),
        "free_cash_flow": info.get("freeCashflow", 0),
        "dividend_yield": info.get("dividendYield", 0),
        "target_price": target_price,
        "target_upside_pct": round(target_upside_pct, 2),
        "recommendation": info.get("recommendationKey", "N/A"),
        "recommendation_mean": _to_float(info.get("recommendationMean"), 0.0),
        "analyst_count": int(_to_float(info.get("numberOfAnalystOpinions"), 0)),
        "forward_eps": forward_eps,
        "trailing_eps": trailing_eps,
        "forward_eps_growth_pct": round(forward_eps_growth_pct, 2),
        "beta": info.get("beta", 1),
        "52w_high": info.get("fiftyTwoWeekHigh", 0),
        "52w_low": info.get("fiftyTwoWeekLow", 0),
        "earnings_timestamp": _to_float(earnings_ts, 0.0),
        "earnings_timestamp_start": _to_float(earnings_ts_start, 0.0),
        "earnings_timestamp_end": _to_float(earnings_ts_end, 0.0),
        "days_to_earnings": days_to_earnings,
    }


def get_finviz_data(symbol: str) -> dict[str, Any] | None:
    """Scrape Finviz snapshot-table2 as an optional data source."""
    symbol = _clean_symbol(symbol)
    if not symbol:
        return None

    try:
        resp = _SESSION.get(
            f"https://finviz.com/quote.ashx?t={symbol}",
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", class_="snapshot-table2")
        if table is None:
            return None

        kv: dict[str, str] = {}
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            for i in range(0, len(cells) - 1, 2):
                key = cells[i].get_text(strip=True)
                val = cells[i + 1].get_text(strip=True)
                if key:
                    kv[key] = val

        return {
            "symbol": symbol,
            "pe": kv.get("P/E", "N/A"),
            "forward_pe": kv.get("Forward P/E", "N/A"),
            "peg": kv.get("PEG", "N/A"),
            "pb": kv.get("P/B", "N/A"),
            "ps": kv.get("P/S", "N/A"),
            "roe": kv.get("ROE", "N/A"),
            "roa": kv.get("ROA", "N/A"),
            "debt_eq": kv.get("Debt/Eq", "N/A"),
            "eps": kv.get("EPS (ttm)", "N/A"),
            "dividend": kv.get("Dividend %", "N/A"),
            "rsi": kv.get("RSI (14)", "N/A"),
            "target_price": kv.get("Target Price", "N/A"),
            "price": kv.get("Price", "N/A"),
            "change": kv.get("Change", "N/A"),
            "volume": kv.get("Volume", "N/A"),
            "rel_volume": kv.get("Rel Volume", "N/A"),
            "short_float": kv.get("Short Float", "N/A"),
            "sector": kv.get("Sector", "N/A"),
            "industry": kv.get("Industry", "N/A"),
        }
    except Exception:
        return None


def get_market_condition() -> dict[str, Any]:
    """Evaluate broad market regime from QQQ trend structure."""
    from core.indicators import calculate_indicators

    df = get_stock_data("QQQ")
    if df is None:
        return {
            "status": "unknown",
            "emoji": "⚪",
            "message": "데이터 없음",
            "benchmark": "QQQ",
            "benchmark_return_21d": 0.0,
            "benchmark_return_63d": 0.0,
        }

    ind = calculate_indicators(df)
    if ind is None:
        return {
            "status": "unknown",
            "emoji": "⚪",
            "message": "지표 계산 실패",
            "benchmark": "QQQ",
            "benchmark_return_21d": 0.0,
            "benchmark_return_63d": 0.0,
        }

    price = _to_float(ind.get("price"))
    ma50 = _to_float(ind.get("ma50"))
    ma200 = _to_float(ind.get("ma200"))

    if price > ma50 and ma50 >= ma200:
        status, emoji, message = "bullish", "🟢", "상승 추세"
    elif price > ma200:
        status, emoji, message = "neutral", "🟡", "중립"
    else:
        status, emoji, message = "bearish", "🔴", "하락 추세"

    return {
        "status": status,
        "emoji": emoji,
        "message": message,
        "price": round(price, 2),
        "ma50": round(ma50, 2),
        "ma200": round(ma200, 2),
        "benchmark": "QQQ",
        "benchmark_return_21d": round(_to_float(ind.get("return_21d"), 0.0), 2),
        "benchmark_return_63d": round(_to_float(ind.get("return_63d"), 0.0), 2),
    }


def get_fear_greed_index() -> dict[str, Any]:
    """
    Fetch CNN-style Fear & Greed proxy from alternative.me.

    We keep a strict fallback shape to avoid runtime branching in callers.
    """
    try:
        resp = _SESSION.get("https://api.alternative.me/fng/?limit=1", timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            payload = resp.json()
            data = (payload.get("data") or [{}])[0]
            score = int(_to_float(data.get("value"), 50))
            label = data.get("value_classification", "Neutral")

            rating_map = {
                "Extreme Fear": "극단적 공포",
                "Fear": "공포",
                "Neutral": "중립",
                "Greed": "탐욕",
                "Extreme Greed": "극단적 탐욕",
            }
            rating = rating_map.get(label, label)

            if score <= 20:
                emoji, advice = "🔴", "과도한 공포 구간입니다. 분할 매수 관점 검토."
            elif score <= 40:
                emoji, advice = "🟠", "공포 우세 구간입니다. 변동성 관리가 필요합니다."
            elif score <= 60:
                emoji, advice = "🟡", "중립 구간입니다. 추세 확인 후 대응하세요."
            elif score <= 80:
                emoji, advice = "🟢", "탐욕 구간입니다. 추격 매수는 보수적으로."
            else:
                emoji, advice = "🔵", "과열 가능성이 큽니다. 리스크 우선 관리."

            return {
                "score": max(0, min(100, score)),
                "rating": rating,
                "emoji": emoji,
                "advice": advice,
            }
    except Exception:
        pass

    return {
        "score": 50,
        "rating": "N/A",
        "emoji": "⚪",
        "advice": "공포탐욕 지수 데이터를 가져오지 못했습니다.",
    }


__all__ = [
    "get_stock_data",
    "get_stock_info",
    "get_finviz_data",
    "get_market_condition",
    "get_fear_greed_index",
]
