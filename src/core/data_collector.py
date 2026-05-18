from __future__ import annotations

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
import requests

from core.chart_structure import ChartStructureCollector
from core.indicators import calculate_indicators
from core.market_regime import MarketRegimeCollector
from core.news_collectors import build_next_known_events, fetch_rss_events, fetch_sec_submission_events
from core.stock_data import (
    _build_session,
    get_fear_greed_index,
    get_intraday_stock_data,
    get_market_condition,
    get_realtime_stock_snapshots,
    get_stock_data,
    get_stock_info,
)


ROOT = Path(__file__).resolve().parents[2]
ALL_STOCKS_CACHE_PATH = ROOT / "data" / "all_stocks_cache.json"
SP500_CACHE_PATH = ROOT / "data" / "sp500_cache.json"
NASDAQ100_CACHE_PATH = ROOT / "data" / "nasdaq100_cache.json"
FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"
FINRA_REG_SHO_URL = "https://api.finra.org/data/group/OTCMarket/name/regShoDaily"
CBOE_DAILY_STATS_URL = "https://www.cboe.com/us/options/market_statistics/daily/"


def _s(value: Any) -> str:
    return str(value or "").strip()


def _f(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    if out != out or out in {float("inf"), float("-inf")}:
        return default
    return out


def _round_optional(value: Any, digits: int = 2) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if out != out or out in {float("inf"), float("-inf")}:
        return None
    return round(out, digits)


def _pct_change(current: float, reference: float) -> float | None:
    if current <= 0 or reference <= 0:
        return None
    return round((current / reference - 1.0) * 100.0, 2)


def _ratio_pct(value: Any) -> float:
    raw = _f(value, 0.0)
    if -1.0 <= raw <= 1.0:
        return round(raw * 100.0, 2)
    return round(raw, 2)


def _positive(value: Any) -> float | None:
    raw = _f(value, 0.0)
    return raw if raw > 0 else None


def _env_int(key: str, default: int, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        value = int(os.getenv(key, str(default)))
    except Exception:
        value = int(default)
    value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _env_float(key: str, default: float, minimum: float | None = None, maximum: float | None = None) -> float:
    try:
        value = float(os.getenv(key, str(default)))
    except Exception:
        value = float(default)
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _env_bool(key: str, default: bool) -> bool:
    raw = _s(os.getenv(key, "1" if default else "0")).lower()
    return raw in {"1", "true", "yes", "on", "y"}


class DataCollector:
    """Centralized market, fundamental, news, macro, and sentiment data collection."""

    def __init__(self, *, root: Path | None = None, session: requests.Session | None = None) -> None:
        self.root = root or ROOT
        self.session = session or _build_session()
        self.session.headers.update({"User-Agent": os.getenv("AUTOSTOCK_USER_AGENT", "autostock/2.0")})
        self.chart_structure = ChartStructureCollector()
        self.market_regime = MarketRegimeCollector(
            get_stock_data_fn=self.get_stock_data,
            chart_structure=self.chart_structure,
        )

    def get_stock_data(self, symbol: str, period: str = "15mo", auto_adjust: bool | None = None) -> pd.DataFrame | None:
        return get_stock_data(symbol, period=period, auto_adjust=auto_adjust)

    def get_intraday_stock_data(
        self,
        symbol: str,
        period: str = "5d",
        interval: str = "5m",
        auto_adjust: bool | None = None,
        prepost: bool | None = None,
    ) -> pd.DataFrame | None:
        return get_intraday_stock_data(symbol, period=period, interval=interval, auto_adjust=auto_adjust, prepost=prepost)

    def get_stock_info(self, symbol: str) -> dict[str, Any]:
        return get_stock_info(symbol)

    def get_market_condition(self) -> dict[str, Any]:
        return get_market_condition()

    def get_fear_greed_index(self) -> dict[str, Any]:
        return get_fear_greed_index()

    def build_next_known_events(self, symbol: str, stock_info: dict[str, Any], generated_at: datetime) -> list[dict[str, Any]]:
        return build_next_known_events(symbol, stock_info, generated_at)

    def fetch_sec_submission_events(self, symbol: str, limit: int = 20, max_age_days: int = 21) -> list[dict[str, Any]]:
        return fetch_sec_submission_events(symbol, limit=limit, max_age_days=max_age_days, session=self.session)

    def fetch_rss_events(
        self,
        feed_urls: list[str],
        *,
        symbol: str,
        max_per_feed: int = 10,
        source_name: str = "wire",
        category_hint: str = "product",
    ) -> list[dict[str, Any]]:
        return fetch_rss_events(
            feed_urls,
            symbol=symbol,
            max_per_feed=max_per_feed,
            source_name=source_name,
            category_hint=category_hint,
            session=self.session,
        )

    def _load_json(self, path: Path) -> dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))

    def _load_symbol_list(self, path: Path) -> list[str]:
        if not path.exists():
            return []
        try:
            payload = self._load_json(path)
        except Exception:
            return []
        rows = payload.get("symbols") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            return []
        seen: set[str] = set()
        out: list[str] = []
        for row in rows:
            symbol = _s(row).upper().replace(".", "-")
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            out.append(symbol)
        return out

    def fetch_table_symbols(self, url: str, columns: list[str]) -> list[str]:
        try:
            response = self.session.get(url, timeout=20)
            response.raise_for_status()
            tables = pd.read_html(StringIO(response.text))
        except Exception:
            return []
        seen: set[str] = set()
        out: list[str] = []
        for table in tables:
            for col in columns:
                if col not in table.columns:
                    continue
                for raw in table[col].tolist():
                    symbol = _s(raw).upper().replace(".", "-")
                    if not symbol or symbol in seen:
                        continue
                    seen.add(symbol)
                    out.append(symbol)
                if out:
                    return out
        return out

    def load_all_us_symbols(self) -> list[str]:
        cached = self._load_symbol_list(self.root / "data" / "all_stocks_cache.json")
        if cached:
            return cached
        sp500 = self._load_symbol_list(self.root / "data" / "sp500_cache.json") or self.fetch_table_symbols(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            ["Symbol", "Ticker", "Ticker symbol"],
        )
        nasdaq100 = self._load_symbol_list(self.root / "data" / "nasdaq100_cache.json") or self.fetch_table_symbols(
            "https://en.wikipedia.org/wiki/Nasdaq-100",
            ["Ticker", "Ticker symbol", "Symbol"],
        )
        return sorted(set(sp500 + nasdaq100))

    def collect_market_context(self) -> dict[str, Any]:
        tasks = {
            "marketCondition": self.get_market_condition,
            "fearGreed": self.get_fear_greed_index,
            "macro": self.collect_macro_context,
            "optionsMarket": self.collect_options_market_context,
        }
        results: dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=len(tasks), thread_name_prefix="market-ctx") as executor:
            futures = {executor.submit(fn): key for key, fn in tasks.items()}
            for future in as_completed(futures):
                key = futures[future]
                try:
                    results[key] = future.result()
                except Exception as exc:
                    results[key] = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

        market_condition = results.get("marketCondition") or {}
        fear_greed = results.get("fearGreed") or {}
        macro = results.get("macro") or {}
        options_market = results.get("optionsMarket") or {}
        return {
            "marketCondition": market_condition,
            "fearGreed": fear_greed,
            "macro": macro,
            "optionsMarket": options_market,
            "marketRegime": self.market_regime.collect(
                market_condition=market_condition,
                fear_greed=fear_greed,
                macro=macro,
                options_market=options_market,
            ),
        }

    def _fetch_fred_series(self, series_id: str, api_key: str) -> dict[str, Any]:
        try:
            response = self.session.get(
                FRED_API_URL,
                params={
                    "series_id": series_id,
                    "api_key": api_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 8,
                },
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

        valid = []
        for row in payload.get("observations") or []:
            value = _s(row.get("value"))
            if value in {"", "."}:
                continue
            valid.append({"date": _s(row.get("date")), "value": _f(value)})
        latest = valid[0] if valid else {}
        previous = valid[1] if len(valid) > 1 else {}
        return {
            "status": "ok" if latest else "empty",
            "date": latest.get("date", ""),
            "value": latest.get("value"),
            "previousValue": previous.get("value"),
            "change": round(_f(latest.get("value")) - _f(previous.get("value")), 4) if latest and previous else None,
        }

    def collect_macro_context(self, series_ids: list[str] | None = None) -> dict[str, Any]:
        if not _env_bool("FRED_MACRO_ENABLED", True):
            return {"status": "disabled", "source": "fred"}
        api_key = _s(os.getenv("FRED_API_KEY") or os.getenv("FRED_TOKEN"))
        if not api_key:
            return {"status": "unavailable", "source": "fred", "reason": "missing_fred_api_key"}
        if series_ids is None:
            raw = _s(os.getenv("FRED_SERIES") or "DGS10,DGS2,T10Y2Y,FEDFUNDS,VIXCLS")
            series_ids = [part.strip().upper() for part in raw.replace(";", ",").split(",") if part.strip()]

        observations: dict[str, Any] = {}
        if series_ids:
            workers = _env_int("FRED_MACRO_WORKERS", min(len(series_ids), 6), minimum=1, maximum=12)
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(self._fetch_fred_series, series_id, api_key): series_id
                    for series_id in series_ids
                }
                for future in as_completed(futures):
                    series_id = futures[future]
                    observations[series_id] = future.result()

        return {
            "status": "ok",
            "source": "fred",
            "series": {sid: observations[sid] for sid in series_ids if sid in observations},
            "notes": "FRED observations may be revised; use ALFRED/vintage data for point-in-time backtests.",
        }

    def collect_options_market_context(self) -> dict[str, Any]:
        if not _env_bool("CBOE_OPTIONS_STATS_ENABLED", True):
            return {"status": "disabled", "source": "cboe"}
        try:
            response = self.session.get(CBOE_DAILY_STATS_URL, timeout=12)
            response.raise_for_status()
            tables = pd.read_html(StringIO(response.text))
        except Exception as exc:
            return {"status": "unavailable", "source": "cboe", "reason": f"{type(exc).__name__}: {exc}"}

        ratios: dict[str, float] = {}
        volumes: dict[str, dict[str, int]] = {}
        if tables:
            table = tables[0]
            if {"Ratios", "Value"}.issubset(set(table.columns)):
                for _, row in table.iterrows():
                    key = re.sub(r"[^a-z0-9]+", "_", _s(row.get("Ratios")).lower()).strip("_")
                    if key:
                        ratios[key] = _f(row.get("Value"))

        for table in tables[1:8]:
            if table.empty:
                continue
            flattened = [" ".join(str(part) for part in col if str(part) != "nan").strip() if isinstance(col, tuple) else str(col) for col in table.columns]
            table = table.copy()
            table.columns = flattened
            name_col = next((col for col in table.columns if col.lower().endswith("name") or col.lower() == "name"), "")
            call_col = next((col for col in table.columns if col.lower().endswith("call") or col.lower() == "call"), "")
            put_col = next((col for col in table.columns if col.lower().endswith("put") or col.lower() == "put"), "")
            total_col = next((col for col in table.columns if col.lower().endswith("total") or col.lower() == "total"), "")
            if not all([name_col, call_col, put_col, total_col]):
                continue
            group = re.sub(r"[^a-z0-9]+", "_", table.columns[0].lower()).strip("_") or f"group_{len(volumes) + 1}"
            for _, row in table.iterrows():
                metric = re.sub(r"[^a-z0-9]+", "_", _s(row.get(name_col)).lower()).strip("_")
                if metric == "volume":
                    volumes[group] = {
                        "call": int(_f(row.get(call_col))),
                        "put": int(_f(row.get(put_col))),
                        "total": int(_f(row.get(total_col))),
                    }
                    break

        asof_match = re.search(r"Cboe Daily Market Statistics(?: for)? ([A-Za-z]+, [A-Za-z]+ \d{1,2}, \d{4})", response.text)
        return {
            "status": "ok",
            "source": "cboe_daily_market_statistics",
            "asOf": asof_match.group(1) if asof_match else "",
            "ratios": ratios,
            "volumes": volumes,
            "notes": "Cboe states this summary is informational and not guaranteed for accuracy.",
        }

    def collect_short_volume(self, symbol: str, limit: int = 30) -> dict[str, Any]:
        symbol = _s(symbol).upper()
        if not symbol:
            return {}
        if not _env_bool("FINRA_SHORT_VOLUME_ENABLED", True):
            return {"status": "disabled", "source": "finra_reg_sho_daily"}
        try:
            response = self.session.post(
                FINRA_REG_SHO_URL,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                json={
                    "compareFilters": [
                        {
                            "fieldName": "securitiesInformationProcessorSymbolIdentifier",
                            "fieldValue": symbol,
                            "compareType": "EQUAL",
                        }
                    ],
                    "limit": max(3, int(limit)),
                },
                timeout=12,
            )
            response.raise_for_status()
            rows = response.json()
        except Exception as exc:
            return {"status": "unavailable", "source": "finra_reg_sho_daily", "reason": f"{type(exc).__name__}: {exc}"}

        grouped: dict[str, dict[str, float]] = {}
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            date = _s(row.get("tradeReportDate"))
            if not date:
                continue
            bucket = grouped.setdefault(date, {"totalVolume": 0.0, "shortVolume": 0.0, "shortExemptVolume": 0.0})
            bucket["totalVolume"] += _f(row.get("totalParQuantity"))
            bucket["shortVolume"] += _f(row.get("shortParQuantity"))
            bucket["shortExemptVolume"] += _f(row.get("shortExemptParQuantity"))
        if not grouped:
            return {"status": "empty", "source": "finra_reg_sho_daily"}

        latest_date = sorted(grouped.keys(), reverse=True)[0]
        latest = grouped[latest_date]
        total = latest["totalVolume"]
        short_volume = latest["shortVolume"]
        return {
            "status": "ok",
            "source": "finra_reg_sho_daily",
            "tradeReportDate": latest_date,
            "totalVolume": int(total),
            "shortVolume": int(short_volume),
            "shortExemptVolume": int(latest["shortExemptVolume"]),
            "shortVolumePct": round(short_volume / total * 100.0, 2) if total > 0 else None,
            "notes": "FINRA short-sale volume is not consolidated with exchange data and is not short interest.",
        }

    def collect_short_volume_batch(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        if not _env_bool("FINRA_SHORT_VOLUME_ENABLED", True):
            return {}
        clean_symbols = sorted({_s(symbol).upper() for symbol in symbols if _s(symbol)})
        workers = _env_int("FINRA_SHORT_VOLUME_WORKERS", 4, minimum=1, maximum=12)
        out: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self.collect_short_volume, symbol): symbol for symbol in clean_symbols}
            for future in as_completed(futures):
                symbol = futures[future]
                row = future.result()
                if row:
                    out[symbol] = row
        return out

    def _generic_news_urls(self, symbol: str, company_name: str) -> list[str]:
        company = re.sub(r"\s+", " ", company_name or symbol).strip()
        base = f'"{symbol}" OR "{company}"'
        queries = [
            f"{base} earnings OR guidance OR outlook OR contract OR partnership when:21d",
            f"{base} Reuters OR \"Business Wire\" OR \"PR Newswire\" OR investor relations OR analyst upgrade OR downgrade OR price target when:21d",
        ]
        return [
            f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
            for query in queries
        ]

    def _dedupe_events(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[tuple[str, str, str]] = set()
        out: list[dict[str, Any]] = []
        for row in events:
            if not isinstance(row, dict):
                continue
            key = (
                _s(row.get("symbol")).upper(),
                _s(row.get("headline")).lower(),
                _s(row.get("published_at")),
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
        out.sort(key=lambda item: _s(item.get("published_at")), reverse=True)
        return out

    def collect_news_bundle(self, symbol: str, chart_row: dict[str, Any] | None = None) -> dict[str, Any]:
        info = self.get_stock_info(symbol)
        next_events = build_next_known_events(symbol, info, datetime.now(timezone.utc))
        raw_events: list[dict[str, Any]] = []
        try:
            raw_events.extend(fetch_sec_submission_events(symbol, limit=8, max_age_days=21, session=self.session))
        except Exception:
            pass
        try:
            raw_events.extend(
                fetch_rss_events(
                    self._generic_news_urls(symbol, _s(info.get("name") or symbol)),
                    symbol=symbol,
                    max_per_feed=5,
                    source_name="wire",
                    category_hint="product",
                    session=self.session,
                )
            )
        except Exception:
            pass
        return {
            "symbol": symbol,
            "info": info,
            "nextEvents": next_events,
            "events": self._dedupe_events(raw_events)[:10],
            "chartRow": chart_row or {"symbol": symbol},
        }

    def collect_news_bundles(
        self,
        symbols: list[str],
        chart_rows_by_symbol: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, dict[str, Any]]:
        bundles: dict[str, dict[str, Any]] = {}
        workers = _env_int("TELEGRAM_NEWS_WORKERS", 8, minimum=2)
        chart_rows_by_symbol = chart_rows_by_symbol or {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self.collect_news_bundle, symbol, chart_rows_by_symbol.get(symbol)): symbol
                for symbol in symbols
            }
            for future in as_completed(futures):
                bundle = future.result()
                symbol = _s(bundle.get("symbol")).upper()
                if symbol:
                    bundles[symbol] = bundle
        return bundles

    def _merge_data_quality_flags(self, row: dict[str, Any], flags: list[str]) -> None:
        existing = row.get("dataQualityFlags") if isinstance(row.get("dataQualityFlags"), list) else []
        merged = sorted({str(item) for item in [*existing, *flags] if _s(item)})
        row["dataQualityFlags"] = merged

    def _single_source_price_quality(self, row: dict[str, Any]) -> dict[str, Any]:
        volatile_threshold = _env_float("PRICE_VOLATILE_DAY_WARN_PCT", 8.0, minimum=0.1)
        day_return = abs(_f(row.get("dayReturnPct"), 0.0))
        day_range = abs(_f(row.get("dayRangePct"), 0.0))
        gap = abs(_f(row.get("gapPct"), 0.0))
        flags: list[str] = []
        if max(day_return, day_range, gap) >= volatile_threshold:
            flags.extend(["volatile_day_single_source", "ohlc_verification_recommended"])
        quality = {
            "status": "needs_verification" if flags else "single_source",
            "primarySource": _s(row.get("priceSource")) or "yfinance",
            "secondarySource": "massive_snapshot_missing",
            "flags": flags,
            "checks": [],
            "note": (
                "High-volatility OHLC is primary-source only; verify high/low before relying on tight stops."
                if flags
                else ""
            ),
        }
        self._merge_data_quality_flags(row, flags)
        return quality

    def _price_cross_check(self, field: str, primary: Any, secondary: Any, warn_pct: float) -> dict[str, Any] | None:
        primary_value = _f(primary, 0.0)
        secondary_value = _f(secondary, 0.0)
        if primary_value <= 0 or secondary_value <= 0:
            return None
        diff_pct = round((secondary_value / primary_value - 1.0) * 100.0, 2)
        return {
            "field": field,
            "primarySource": "yfinance",
            "primaryValue": round(primary_value, 4),
            "secondarySource": "massive_snapshot",
            "secondaryValue": round(secondary_value, 4),
            "diffPct": diff_pct,
            "status": "mismatch" if abs(diff_pct) >= warn_pct else "ok",
        }

    def _cross_checked_price_quality(self, row: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
        warn_pct = _env_float("PRICE_CROSSCHECK_WARN_PCT", 2.0, minimum=0.05)
        checks = [
            self._price_cross_check("close", row.get("latestClosePrice"), snapshot.get("closePrice"), warn_pct),
            self._price_cross_check("open", row.get("latestOpenPrice"), snapshot.get("openPrice"), warn_pct),
            self._price_cross_check("high", row.get("latestHighPrice"), snapshot.get("highPrice"), warn_pct),
            self._price_cross_check("low", row.get("latestLowPrice"), snapshot.get("lowPrice"), warn_pct),
            self._price_cross_check("previous_close", row.get("previousClosePrice"), snapshot.get("previousClosePrice"), warn_pct),
            self._price_cross_check("volume", row.get("latestDailyVolume"), snapshot.get("sessionVolume"), warn_pct),
        ]
        clean_checks = [check for check in checks if isinstance(check, dict)]
        mismatch_fields = [str(check.get("field")) for check in clean_checks if check.get("status") == "mismatch"]
        flags = [
            "volume_cross_source_mismatch" if field == "volume" else f"ohlc_cross_source_mismatch_{field}"
            for field in mismatch_fields
        ]
        if any(field in {"high", "low"} for field in mismatch_fields):
            flags.append("ohlc_verification_required")
        base = self._single_source_price_quality(row)
        flags = sorted({*[str(item) for item in base.get("flags", [])], *flags})
        quality = {
            "status": "mismatch" if mismatch_fields else "cross_checked",
            "primarySource": "yfinance",
            "secondarySource": "massive_snapshot",
            "warnPct": warn_pct,
            "flags": flags,
            "checks": clean_checks,
            "note": (
                "Cross-source OHLC mismatch found; verify regular-session versus extended-hours data before sizing."
                if mismatch_fields
                else ""
            ),
        }
        self._merge_data_quality_flags(row, flags)
        return quality

    def collect_fundamental_snapshot(self, symbol: str) -> dict[str, Any] | None:
        info = self.get_stock_info(symbol)
        if not isinstance(info, dict):
            return None
        price = _f(info.get("price"), 0.0)
        market_cap = _f(info.get("market_cap"), 0.0)
        if price <= 0 or market_cap <= 0:
            return None

        currency = _s(info.get("currency")).upper()
        financial_currency = _s(info.get("financial_currency")).upper()
        currency_mismatch = bool(currency and financial_currency and currency != financial_currency)
        free_cash_flow = _f(info.get("free_cash_flow"), 0.0)
        pe = _positive(info.get("pe"))
        forward_pe = _positive(info.get("forward_pe"))
        pb = _positive(info.get("pb"))
        p_fcf = market_cap / free_cash_flow if free_cash_flow > 0 and not currency_mismatch else None
        fcf_yield = free_cash_flow / market_cap * 100.0 if free_cash_flow > 0 and not currency_mismatch else None
        earnings_yield = 100.0 / pe if pe and pe > 0 else None
        return {
            "symbol": symbol,
            "name": _s(info.get("name") or symbol),
            "sector": _s(info.get("sector") or "N/A"),
            "industry": _s(info.get("industry") or "N/A"),
            "currency": currency,
            "financialCurrency": financial_currency,
            "financialCurrencyMismatch": currency_mismatch,
            "latestClosePrice": round(price, 2),
            "marketCap": round(market_cap, 2),
            "pe": round(pe, 2) if pe else None,
            "forwardPe": round(forward_pe, 2) if forward_pe else None,
            "pb": round(pb, 2) if pb else None,
            "peg": round(_f(info.get("peg"), 0.0), 2) if _f(info.get("peg"), 0.0) > 0 else None,
            "freeCashFlow": round(free_cash_flow, 2),
            "pFcf": round(p_fcf, 2) if p_fcf else None,
            "fcfYieldPct": round(fcf_yield, 2) if fcf_yield else None,
            "earningsYieldPct": round(earnings_yield, 2) if earnings_yield else None,
            "roePct": _ratio_pct(info.get("roe")),
            "roaPct": _ratio_pct(info.get("roa")),
            "profitMarginPct": _ratio_pct(info.get("profit_margin")),
            "operatingMarginPct": _ratio_pct(info.get("operating_margin")),
            "revenueGrowthPct": _ratio_pct(info.get("revenue_growth")),
            "earningsGrowthPct": _ratio_pct(info.get("earnings_growth")),
            "forwardEpsGrowthPct": round(_f(info.get("forward_eps_growth_pct"), 0.0), 2),
            "debtToEquity": round(_f(info.get("debt_to_equity"), 0.0), 2),
            "currentRatio": round(_f(info.get("current_ratio"), 0.0), 2),
            "dividendYieldPct": _ratio_pct(info.get("dividend_yield")),
            "targetPrice": round(_f(info.get("target_price"), 0.0), 2),
            "targetUpsidePct": round(_f(info.get("target_upside_pct"), 0.0), 2),
            "analystCount": int(_f(info.get("analyst_count"), 0.0)),
            "recommendation": _s(info.get("recommendation")),
            "fundamentalSource": "yfinance",
            "fundamentalCollectedAt": datetime.now(timezone.utc).isoformat(),
        }

    def scan_fundamentals(self, symbols: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        workers = _env_int("TELEGRAM_FUNDAMENTAL_WORKERS", 12, minimum=4)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self.collect_fundamental_snapshot, symbol): symbol for symbol in symbols}
            for future in as_completed(futures):
                row = future.result()
                if row:
                    rows.append(row)
        return rows

    def _daily_price_context(self, bars: pd.DataFrame, indicators: dict[str, Any]) -> dict[str, Any]:
        if bars is None or bars.empty:
            return {}
        last = bars.iloc[-1]
        prev = bars.iloc[-2] if len(bars) >= 2 else last
        close = _f(last.get("Close"), _f(indicators.get("price"), 0.0))
        prev_close = _f(prev.get("Close"), close)
        open_price = _f(last.get("Open"), close)
        high = _f(last.get("High"), close)
        low = _f(last.get("Low"), close)
        daily_volume = _f(last.get("Volume"), 0.0)
        volume_avg_20 = 0.0
        if "Volume" in bars:
            history_volume = bars["Volume"].iloc[:-1].tail(20) if len(bars) > 1 else bars["Volume"].tail(20)
            volume_avg_20 = _f(history_volume.mean(), 0.0)
            if volume_avg_20 != volume_avg_20:
                volume_avg_20 = 0.0
        atr = _f(indicators.get("atr"), 0.0)
        atr_pct = _f(indicators.get("atr_pct"), 0.0)
        if atr_pct <= 0 and close > 0 and atr > 0:
            atr_pct = atr / close * 100.0
        day_return_pct = _pct_change(close, prev_close)
        gap_pct = _pct_change(open_price, prev_close)
        intraday_return_pct = _pct_change(close, open_price)
        day_range_pct = _pct_change(high, low)
        close_location = 50.0
        if high > low:
            close_location = max(0.0, min(100.0, (close - low) / (high - low) * 100.0))
        move_atr = 0.0
        if atr_pct > 0 and day_return_pct is not None:
            move_atr = day_return_pct / atr_pct
        gap_atr = 0.0
        if atr_pct > 0 and gap_pct is not None:
            gap_atr = gap_pct / atr_pct
        avg_volume = int(max(0.0, volume_avg_20))
        return {
            "previousClosePrice": round(prev_close, 2),
            "latestOpenPrice": round(open_price, 2),
            "latestHighPrice": round(high, 2),
            "latestLowPrice": round(low, 2),
            "dayReturnPct": round(day_return_pct or 0.0, 2),
            "gapPct": round(gap_pct or 0.0, 2),
            "intradayReturnPct": round(intraday_return_pct or 0.0, 2),
            "dayRangePct": round(day_range_pct or 0.0, 2),
            "closeLocationPct": round(close_location, 1),
            "eventMoveAtr": round(move_atr, 2),
            "gapAtr": round(gap_atr, 2),
            "atrPct": round(atr_pct, 2),
            "realtimeVolume": None,
            "lastMinuteVolume": None,
            "latestDailyVolume": int(max(0.0, daily_volume)),
            "dailyVolumeAvg20": avg_volume,
            "dollarVolumeDaily": round(close * daily_volume, 2) if close > 0 and daily_volume > 0 else None,
            "dollarVolumeAvg20": round(close * avg_volume, 2) if close > 0 and avg_volume > 0 else None,
            "volumeRatio": None,
            "volumeSource": "massive_snapshot_missing",
            "volumeAsOf": "",
        }

    def scan_symbol_price(self, symbol: str, rebalance_hint: dict[str, Any] | None = None) -> dict[str, Any] | None:
        bars = self.get_stock_data(symbol, period="15mo", auto_adjust=False)
        if bars is None or bars.empty:
            return None
        indicators = calculate_indicators(bars)
        if indicators is None:
            return None
        chart_structure = self.chart_structure.analyze_daily(symbol, bars, indicators)
        payload = {
            "symbol": symbol,
            "latestClosePrice": round(_f(indicators.get("price"), 0.0), 2),
            "latestCloseAsOf": bars.tail(1).index[0].isoformat() if len(bars.index) else "",
            "priceSource": "yfinance",
            "chartState": _s(chart_structure.get("chartState") or "reference_only"),
            "volumeRatio": None,
            "volumeSource": "massive_snapshot_missing",
            "rsi": round(_f(indicators.get("rsi"), 0.0), 1),
            "adx": round(_f(indicators.get("adx"), 0.0), 1),
            "atr": round(_f(indicators.get("atr"), 0.0), 2),
            "atrPct": round(_f(indicators.get("atr_pct"), 0.0), 2),
            "ma20Gap": round(_f(indicators.get("ma20_gap"), 0.0), 2),
            "ma50Gap": round(_f(indicators.get("ma50_gap"), 0.0), 2),
            "ma200Gap": round(_f(indicators.get("ma200_gap"), 0.0), 2),
            "return21d": round(_f(indicators.get("return_21d"), 0.0), 2),
            "return63d": round(_f(indicators.get("return_63d"), 0.0), 2),
            "support": chart_structure.get("support") if isinstance(chart_structure.get("support"), list) else [],
            "resistance": chart_structure.get("resistance") if isinstance(chart_structure.get("resistance"), list) else [],
            "nearestSupportZone": chart_structure.get("nearestSupportZone") if isinstance(chart_structure.get("nearestSupportZone"), dict) else {},
            "nearestResistanceZone": chart_structure.get("nearestResistanceZone") if isinstance(chart_structure.get("nearestResistanceZone"), dict) else {},
            "chartStructure": chart_structure.get("chartStructure") if isinstance(chart_structure.get("chartStructure"), dict) else {},
            "rebalanceHint": bool(rebalance_hint),
        }
        payload.update(self._daily_price_context(bars, indicators))
        payload["priceDataQuality"] = self._single_source_price_quality(payload)
        return payload

    def apply_realtime_volume(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        symbols = [_s(row.get("symbol")).upper() for row in rows if _s(row.get("symbol"))]
        snapshots = get_realtime_stock_snapshots(symbols)
        for row in rows:
            symbol = _s(row.get("symbol")).upper()
            snapshot = snapshots.get(symbol)
            if not snapshot:
                row["volumeRatio"] = None
                row["realtimeVolume"] = None
                row["lastMinuteVolume"] = None
                row["volumeSource"] = "massive_snapshot_missing"
                row["volumeAsOf"] = ""
                row["priceDataQuality"] = self._single_source_price_quality(row)
                continue

            realtime_volume = _f(snapshot.get("sessionVolume"), 0.0)
            avg_volume = _f(row.get("dailyVolumeAvg20"), 0.0)
            latest_price = _f(snapshot.get("closePrice"), _f(row.get("latestClosePrice"), 0.0))
            row["snapshotOpenPrice"] = round(_f(snapshot.get("openPrice"), 0.0), 2) or None
            row["snapshotHighPrice"] = round(_f(snapshot.get("highPrice"), 0.0), 2) or None
            row["snapshotLowPrice"] = round(_f(snapshot.get("lowPrice"), 0.0), 2) or None
            row["snapshotPreviousClosePrice"] = round(_f(snapshot.get("previousClosePrice"), 0.0), 2) or None
            row["realtimeVolume"] = int(realtime_volume) if realtime_volume > 0 else None
            row["lastMinuteVolume"] = int(_f(snapshot.get("lastMinuteVolume"), 0.0)) or None
            row["volumeRatio"] = round(realtime_volume / avg_volume, 2) if realtime_volume > 0 and avg_volume > 0 else None
            row["dollarVolumeRealtime"] = round(latest_price * realtime_volume, 2) if latest_price > 0 and realtime_volume > 0 else None
            row["volumeSource"] = _s(snapshot.get("source")) or "massive_snapshot"
            row["volumeAsOf"] = _s(snapshot.get("updatedAt"))
            if latest_price > 0:
                row["realtimePrice"] = round(latest_price, 2)
            row["priceDataQuality"] = self._cross_checked_price_quality(row, snapshot)
        return rows

    def apply_relative_strength(self, rows: list[dict[str, Any]], benchmark_symbol: str | None = None) -> list[dict[str, Any]]:
        benchmark = _s(benchmark_symbol or os.getenv("AI_MARKET_INDICATOR", "QQQ")).upper() or "QQQ"
        bars = self.get_stock_data(benchmark, period="15mo", auto_adjust=False)
        indicators = calculate_indicators(bars) if bars is not None else None
        if indicators is None:
            for row in rows:
                row["benchmarkSymbol"] = benchmark
            return rows
        bench_21d = round(_f(indicators.get("return_21d"), 0.0), 2)
        bench_63d = round(_f(indicators.get("return_63d"), 0.0), 2)
        for row in rows:
            row["benchmarkSymbol"] = benchmark
            row["benchmarkReturn21d"] = bench_21d
            row["benchmarkReturn63d"] = bench_63d
            row["relativeStrength21dPct"] = round(_f(row.get("return21d"), 0.0) - bench_21d, 2)
            row["relativeStrength63dPct"] = round(_f(row.get("return63d"), 0.0) - bench_63d, 2)
        return rows

    def scan_price_rows(self, symbols: list[str], rebalance_hints: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        workers = _env_int("TELEGRAM_SCAN_WORKERS", 12, minimum=4)
        scanned: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self.scan_symbol_price, symbol, rebalance_hints.get(symbol)): symbol
                for symbol in symbols
            }
            for future in as_completed(futures):
                row = future.result()
                if row:
                    scanned.append(row)
        self.apply_realtime_volume(scanned)
        self.apply_relative_strength(scanned)
        scanned.sort(
            key=lambda row: (
                -_f(row.get("relativeStrength63dPct"), _f(row.get("return63d"), 0.0)),
                -_f(row.get("return21d"), 0.0),
                _s(row.get("symbol")),
            )
        )
        return scanned

    def refresh_payload_realtime_volume(self, payload: dict[str, Any]) -> dict[str, Any]:
        all_rows = payload.get("all") if isinstance(payload.get("all"), list) else []
        rows = [row for row in all_rows if isinstance(row, dict)]
        if not rows:
            return payload

        self.apply_realtime_volume(rows)
        by_symbol = {_s(row.get("symbol")).upper(): row for row in rows if _s(row.get("symbol"))}
        for key in (
            "strictBuyable",
            "actionableNow",
            "priceReady",
            "buyable",
            "waitPullback",
            "avoid",
            "chartLeaders",
            "overextended",
            "referenceOnly",
        ):
            bucket = payload.get(key)
            if not isinstance(bucket, list):
                continue
            for row in bucket:
                if isinstance(row, dict):
                    row.update(by_symbol.get(_s(row.get("symbol")).upper(), {}))
        return payload
