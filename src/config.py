# -*- coding: utf-8 -*-
"""
Runtime configuration and universe builders.

Design goals:
- Keep import-time cost low (no mandatory network calls on module import).
- Preserve legacy names (`NASDAQ_100`, `ALL_US_STOCKS`, etc.) for compatibility.
- Provide explicit lazy loaders for runtime paths that need full data.
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from functools import lru_cache
from io import StringIO
from typing import Any, Callable

import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Cache files
CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
NASDAQ_CACHE_FILE = os.path.join(CACHE_DIR, "nasdaq100_cache.json")
SP500_CACHE_FILE = os.path.join(CACHE_DIR, "sp500_cache.json")
ALL_STOCKS_CACHE_FILE = os.path.join(CACHE_DIR, "all_stocks_cache.json")
SECTOR_CACHE_FILE = os.path.join(CACHE_DIR, "sector_cache.json")
CACHE_DAYS = 7

MARKET_INDICATOR = "QQQ"


def _normalize_symbol(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    symbol = value.strip().upper()
    if not symbol:
        return None
    return symbol.replace(".", "-")


def _fetch_table_symbols(url: str, columns: list[str]) -> list[str]:
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        response.raise_for_status()
        tables = pd.read_html(StringIO(response.text))
    except Exception:
        return []

    for table in tables:
        for col in columns:
            if col in table.columns:
                out: list[str] = []
                seen: set[str] = set()
                for raw in table[col].tolist():
                    symbol = _normalize_symbol(raw)
                    if not symbol or symbol in seen:
                        continue
                    seen.add(symbol)
                    out.append(symbol)
                return out
    return []


def fetch_nasdaq_100() -> list[str]:
    """Fetch Nasdaq-100 symbols from Wikipedia."""
    return _fetch_table_symbols(
        "https://en.wikipedia.org/wiki/Nasdaq-100",
        columns=["Ticker", "Ticker symbol", "Symbol"],
    )


def fetch_sp500() -> list[str]:
    """Fetch S&P 500 symbols from Wikipedia."""
    return _fetch_table_symbols(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        columns=["Symbol", "Ticker", "Ticker symbol"],
    )


def get_cached_list(cache_file: str, fetch_func: Callable[[], list[str]], name: str) -> list[str]:
    """Return cached symbol list or refresh cache when stale."""
    os.makedirs(CACHE_DIR, exist_ok=True)

    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as fh:
                cache = json.load(fh)
            cached_date = datetime.fromisoformat(cache.get("date", "1970-01-01T00:00:00"))
            if datetime.now() - cached_date < timedelta(days=CACHE_DAYS):
                symbols = cache.get("symbols", [])
                if isinstance(symbols, list):
                    return [s for s in symbols if isinstance(s, str)]
        except Exception:
            pass

    print(f"Loading {name} list...")
    symbols = fetch_func() or []

    if symbols:
        try:
            with open(cache_file, "w", encoding="utf-8") as fh:
                json.dump({"date": datetime.now().isoformat(), "symbols": symbols}, fh)
            print(f"  -> {len(symbols)} symbols")
        except Exception:
            pass
        return symbols

    # Fallback to stale cache when fetch failed.
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as fh:
                stale = json.load(fh).get("symbols", [])
            if isinstance(stale, list):
                return [s for s in stale if isinstance(s, str)]
        except Exception:
            pass
    return []


def get_nasdaq_100() -> list[str]:
    return get_cached_list(NASDAQ_CACHE_FILE, fetch_nasdaq_100, "Nasdaq-100")


def get_sp500() -> list[str]:
    return get_cached_list(SP500_CACHE_FILE, fetch_sp500, "S&P 500")


def get_all_us_stocks() -> list[str]:
    """Union of Nasdaq-100 + S&P 500."""
    os.makedirs(CACHE_DIR, exist_ok=True)

    if os.path.exists(ALL_STOCKS_CACHE_FILE):
        try:
            with open(ALL_STOCKS_CACHE_FILE, "r", encoding="utf-8") as fh:
                cache = json.load(fh)
            cached_date = datetime.fromisoformat(cache.get("date", "1970-01-01T00:00:00"))
            if datetime.now() - cached_date < timedelta(days=CACHE_DAYS):
                symbols = cache.get("symbols", [])
                if isinstance(symbols, list):
                    return [s for s in symbols if isinstance(s, str)]
        except Exception:
            pass

    nasdaq = get_nasdaq_100()
    sp500 = get_sp500()
    all_stocks = sorted(set(nasdaq + sp500))

    try:
        with open(ALL_STOCKS_CACHE_FILE, "w", encoding="utf-8") as fh:
            json.dump({"date": datetime.now().isoformat(), "symbols": all_stocks}, fh)
        print(
            f"Universe: {len(all_stocks)} symbols "
            f"(Nasdaq100={len(nasdaq)}, S&P500={len(sp500)}, deduped)"
        )
    except Exception:
        pass
    return all_stocks


def fetch_stock_sector(symbol: str) -> dict[str, str]:
    """Fetch sector/industry metadata from yfinance."""
    import yfinance as yf

    symbol = _normalize_symbol(symbol) or ""
    if not symbol:
        return {"symbol": "", "sector": "Unknown", "industry": "Unknown", "name": ""}
    try:
        info = yf.Ticker(symbol).info or {}
        return {
            "symbol": symbol,
            "sector": str(info.get("sector") or "Unknown"),
            "industry": str(info.get("industry") or "Unknown"),
            "name": str(info.get("shortName") or symbol),
        }
    except Exception:
        return {"symbol": symbol, "sector": "Unknown", "industry": "Unknown", "name": symbol}


def fetch_all_sectors(symbols: list[str], max_workers: int = 10) -> dict[str, dict[str, str]]:
    """Fetch sector metadata for symbols in parallel."""
    results: dict[str, dict[str, str]] = {}
    if not symbols:
        return results

    workers = min(max(2, max_workers), 16)
    print(f"Loading sector metadata for {len(symbols)} symbols...")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_stock_sector, s): s for s in symbols}
        for i, future in enumerate(as_completed(futures), 1):
            item = future.result()
            symbol = item.get("symbol")
            if symbol:
                results[symbol] = item
            if i % 25 == 0 or i == len(symbols):
                print(f"  -> {i}/{len(symbols)}")
    return results


def get_sector_data(symbols: list[str] | None = None) -> dict[str, dict[str, str]]:
    """Return cached sector metadata; fetch only missing entries when needed."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    symbols = symbols or get_all_us_stocks()

    if os.path.exists(SECTOR_CACHE_FILE):
        try:
            with open(SECTOR_CACHE_FILE, "r", encoding="utf-8") as fh:
                cache = json.load(fh)
            cached_date = datetime.fromisoformat(cache.get("date", "1970-01-01T00:00:00"))
            data = cache.get("data", {})
            if isinstance(data, dict) and datetime.now() - cached_date < timedelta(days=CACHE_DAYS):
                cached_symbols = set(data.keys())
                missing = [s for s in symbols if s not in cached_symbols]
                if not missing:
                    return data

                print(f"Loading sector metadata for missing {len(missing)} symbols...")
                data.update(fetch_all_sectors(missing))
                with open(SECTOR_CACHE_FILE, "w", encoding="utf-8") as fh:
                    json.dump({"date": datetime.now().isoformat(), "data": data}, fh, ensure_ascii=False, indent=2)
                return data
        except Exception:
            pass

    data = fetch_all_sectors(symbols)
    if data:
        try:
            with open(SECTOR_CACHE_FILE, "w", encoding="utf-8") as fh:
                json.dump({"date": datetime.now().isoformat(), "data": data}, fh, ensure_ascii=False, indent=2)
        except Exception:
            pass
    return data


def build_stock_categories(symbols: list[str] | None = None) -> dict[str, dict[str, Any]]:
    """Build dynamic category map from sector metadata."""
    symbols = symbols or get_all_us_stocks()
    sector_data = get_sector_data(symbols)

    sector_info: dict[str, dict[str, str]] = {
        "Technology": {"emoji": "💻", "etf": "XLK", "name": "Technology"},
        "Communication Services": {"emoji": "📡", "etf": "XLC", "name": "Communication"},
        "Consumer Cyclical": {"emoji": "🛍️", "etf": "XLY", "name": "Consumer Cyclical"},
        "Consumer Defensive": {"emoji": "🛒", "etf": "XLP", "name": "Consumer Defensive"},
        "Healthcare": {"emoji": "🏥", "etf": "XLV", "name": "Healthcare"},
        "Financial Services": {"emoji": "🏦", "etf": "XLF", "name": "Financials"},
        "Industrials": {"emoji": "🏭", "etf": "XLI", "name": "Industrials"},
        "Energy": {"emoji": "⚡", "etf": "XLE", "name": "Energy"},
        "Utilities": {"emoji": "🔌", "etf": "XLU", "name": "Utilities"},
        "Real Estate": {"emoji": "🏘️", "etf": "XLRE", "name": "Real Estate"},
        "Basic Materials": {"emoji": "🧱", "etf": "XLB", "name": "Materials"},
        "Unknown": {"emoji": "❓", "etf": "SPY", "name": "Other"},
    }

    categories: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        info = sector_data.get(symbol, {"sector": "Unknown", "industry": "Unknown"})
        sector = str(info.get("sector") or "Unknown")
        industry = str(info.get("industry") or "Unknown")

        if sector not in categories:
            meta = sector_info.get(sector, sector_info["Unknown"])
            categories[sector] = {
                "emoji": meta["emoji"],
                "etf": meta["etf"],
                "name": meta["name"],
                "stocks": [],
                "industries": {},
            }

        categories[sector]["stocks"].append(symbol)
        categories[sector]["industries"].setdefault(industry, []).append(symbol)

    for sector in categories:
        categories[sector]["stocks"].sort()
        categories[sector]["description"] = f"{len(categories[sector]['stocks'])} symbols"

    return categories


# ===== Runtime lazy loaders =====


@lru_cache(maxsize=1)
def load_nasdaq_100() -> list[str]:
    return get_nasdaq_100()


@lru_cache(maxsize=1)
def load_sp500() -> list[str]:
    return get_sp500()


@lru_cache(maxsize=1)
def load_all_us_stocks() -> list[str]:
    return get_all_us_stocks()


@lru_cache(maxsize=1)
def load_stock_categories() -> dict[str, dict[str, Any]]:
    try:
        return build_stock_categories(load_all_us_stocks())
    except Exception as exc:
        print(f"Category build failed: {exc}")
        return {}


def load_all_category_stocks() -> list[str]:
    return load_all_us_stocks()


# ===== Backward-compatible globals =====
#
# Legacy modules/tests import these names directly. Keep them as lightweight
# defaults and allow explicit eager mode for users who still prefer it.

NASDAQ_100: list[str] = []
SP500: list[str] = []
ALL_US_STOCKS: list[str] = []
STOCK_CATEGORIES: dict[str, dict[str, Any]] = {}
ALL_CATEGORY_STOCKS: list[str] = []

if os.getenv("CONFIG_EAGER_LOAD", "false").lower() == "true":
    NASDAQ_100 = load_nasdaq_100()
    SP500 = load_sp500()
    ALL_US_STOCKS = load_all_us_stocks()
    STOCK_CATEGORIES = load_stock_categories()
    ALL_CATEGORY_STOCKS = load_all_category_stocks()


def get_category_summary() -> str:
    categories = load_stock_categories()
    lines = ["Sector summary:"]
    for sector, info in sorted(categories.items(), key=lambda x: -len(x[1].get("stocks", []))):
        lines.append(
            f"  {info.get('emoji', '')} {info.get('name', sector)} "
            f"({sector}): {len(info.get('stocks', []))}"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    universe = load_all_category_stocks()
    print(get_category_summary())
    print(f"\nTotal symbols: {len(universe)}")
