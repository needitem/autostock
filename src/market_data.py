"""
Compatibility market-data module used by legacy tests.
"""
from __future__ import annotations

from core.stock_data import (
    get_fear_greed_index as _core_fear_greed_index,
    get_finviz_data as _core_finviz_data,
    get_market_condition as _core_market_condition,
    get_stock_info as _core_stock_info,
)

try:
    from config import load_stock_categories
except Exception:
    load_stock_categories = None


def _clamp_score(value) -> int:
    try:
        score = int(float(value))
    except Exception:
        score = 50
    return max(0, min(100, score))


def _emoji_for_score(score: int) -> str:
    if score <= 20:
        return "🔴"
    if score <= 40:
        return "🟠"
    if score <= 60:
        return "🟡"
    if score <= 80:
        return "🟢"
    return "🔵"


def _recommendation_trends_from_stock_info(symbol: str) -> dict:
    try:
        info = _core_stock_info(symbol) or {}
    except Exception:
        info = {}

    rec_key = str(info.get("recommendation", "")).strip().lower()
    analyst_count = max(0, int(float(info.get("analyst_count", 0) or 0)))
    if analyst_count <= 0:
        return {}

    if rec_key in {"strong_buy", "buy"}:
        strong_buy = analyst_count // 3 if rec_key == "strong_buy" else analyst_count // 6
        buy = max(1, analyst_count - strong_buy - analyst_count // 6)
        hold = max(0, analyst_count - strong_buy - buy)
        sell = 0
        strong_sell = 0
    elif rec_key == "hold":
        strong_buy = 0
        buy = analyst_count // 4
        hold = analyst_count - buy
        sell = 0
        strong_sell = 0
    else:
        strong_buy = 0
        buy = analyst_count // 6
        hold = analyst_count // 3
        sell = max(0, analyst_count - buy - hold)
        strong_sell = 0

    return {
        "strong_buy": strong_buy,
        "buy": buy,
        "hold": hold,
        "sell": sell,
        "strong_sell": strong_sell,
    }


def get_fear_greed_index() -> dict:
    try:
        data = _core_fear_greed_index() or {}
    except Exception:
        data = {}

    score = _clamp_score(data.get("score", 50))
    allowed = {"🔴", "🟠", "🟡", "🟢", "🔵", "⚪"}
    raw_emoji = data.get("emoji")
    emoji = raw_emoji if raw_emoji in allowed else _emoji_for_score(score)
    return {
        "score": score,
        "rating": data.get("rating", "N/A"),
        "emoji": emoji,
        "advice": data.get("advice", "?곗씠???놁쓬"),
    }


def get_finviz_market_overview() -> dict:
    try:
        market = _core_market_condition() or {}
    except Exception:
        market = {}
    return {
        "status": market.get("status", "unknown"),
        "message": market.get("message", "?곗씠???놁쓬"),
        "price": market.get("price", 0),
        "ma50": market.get("ma50", 0),
        "ma200": market.get("ma200", 0),
    }


def get_finviz_sector_performance() -> list[dict]:
    results = []
    categories = load_stock_categories() if callable(load_stock_categories) else {}
    for sector, info in (categories or {}).items():
        results.append(
            {
                "sector": sector,
                "name": info.get("name", sector),
                "etf": info.get("etf", ""),
                "change": 0.0,
            }
        )
    return results


def get_finviz_stock_data(symbol: str) -> dict:
    symbol = (symbol or "").upper()
    try:
        raw = _core_finviz_data(symbol) or {}
    except Exception:
        raw = {}

    return {
        "symbol": symbol,
        "price": raw.get("price", "N/A"),
        "change": raw.get("change", "N/A"),
        "pe": raw.get("pe", "N/A"),
        "rsi": raw.get("rsi", "N/A"),
        "target_price": raw.get("target_price", "N/A"),
    }


def get_tipranks_rating(symbol: str) -> dict:
    symbol = (symbol or "").upper()
    data = _recommendation_trends_from_stock_info(symbol) or {}
    if not data:
        return {
            "symbol": symbol,
            "consensus": "N/A",
            "buy": 0,
            "hold": 0,
            "sell": 0,
        }
    buy = int(data.get("buy", 0)) + int(data.get("strong_buy", 0))
    hold = int(data.get("hold", 0))
    sell = int(data.get("sell", 0)) + int(data.get("strong_sell", 0))
    consensus = "Buy" if buy > max(hold, sell) else "Hold" if hold >= sell else "Sell"
    return {
        "symbol": symbol,
        "consensus": consensus,
        "buy": buy,
        "hold": hold,
        "sell": sell,
    }


def get_seeking_alpha_ratings(symbol: str) -> dict:
    symbol = (symbol or "").upper()
    return {
        "symbol": symbol,
        "quant_rating": "N/A",
        "wall_street_rating": "N/A",
        "authors_rating": "N/A",
    }


def get_comprehensive_stock_analysis(symbol: str) -> dict:
    symbol = (symbol or "").upper()
    finviz = get_finviz_stock_data(symbol)
    tipranks = get_tipranks_rating(symbol)
    seeking_alpha = get_seeking_alpha_ratings(symbol)
    return {
        "symbol": symbol,
        "sources": {
            "finviz": finviz,
            "tipranks": tipranks,
            "seeking_alpha": seeking_alpha,
        },
    }


def get_market_sentiment_summary() -> dict:
    return {
        "fear_greed": get_fear_greed_index(),
        "sectors": get_finviz_sector_performance(),
        "market_overview": get_finviz_market_overview(),
    }
