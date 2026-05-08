from __future__ import annotations

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html import escape
from io import StringIO
from pathlib import Path
from statistics import median
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
import requests

from ai.analyzer import ai
from core.event_watchlist import chart_volume_gate
from core.indicators import calculate_indicators
from core.news_collectors import build_next_known_events, fetch_rss_events, fetch_sec_submission_events
from core.stock_data import get_fear_greed_index, get_market_condition, get_stock_data, get_stock_info


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "outputs" / "telegram"
CACHE_PATH = OUTPUT_ROOT / "universe_trade_analysis.json"
CHART_CACHE_PATH = OUTPUT_ROOT / "current_chart_analysis_full.json"
CHART_SCHEMA_VERSION = "valuation-first-v1"
TRADE_CACHE_SCHEMA_VERSION = "valuation-first-v1"
REBALANCE_ROOT = ROOT / "data" / "rebalance"
ALL_STOCKS_CACHE_PATH = ROOT / "data" / "all_stocks_cache.json"
SP500_CACHE_PATH = ROOT / "data" / "sp500_cache.json"
NASDAQ100_CACHE_PATH = ROOT / "data" / "nasdaq100_cache.json"


def _s(value: Any) -> str:
    return str(value or "").strip()


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _trade_cache_path(analysis_limit: int) -> Path:
    return OUTPUT_ROOT / f"universe_trade_analysis_{int(analysis_limit)}.json"


def _write_trade_cache(payload: dict[str, Any], analysis_limit: int) -> None:
    payload["schemaVersion"] = TRADE_CACHE_SCHEMA_VERSION
    _write_json(_trade_cache_path(analysis_limit), payload)
    _write_json(CACHE_PATH, payload)


def _latest_file(root: Path, pattern: str) -> Path | None:
    if not root.exists():
        return None
    candidates = [item for item in root.glob(pattern) if item.is_file()]
    if not candidates:
        return None
    candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return candidates[0]


def _latest_rebalance_result_path() -> Path | None:
    return _latest_file(REBALANCE_ROOT, "rebalance_recommendation_*.json")


def _event_cache_minutes() -> int:
    try:
        return max(1, int(os.getenv("TELEGRAM_ANALYSIS_CACHE_MINUTES", "15")))
    except Exception:
        return 15


def _analysis_limit() -> int:
    try:
        return max(10, int(os.getenv("TELEGRAM_NEWS_ANALYSIS_MAX_SYMBOLS", os.getenv("TELEGRAM_ANALYSIS_MAX_SYMBOLS", "120"))))
    except Exception:
        return 120


def full_news_analysis_limit() -> int:
    return max(_analysis_limit(), len(_load_all_us_symbols()))


def _scan_workers() -> int:
    try:
        return max(4, int(os.getenv("TELEGRAM_SCAN_WORKERS", "12")))
    except Exception:
        return 12


def _news_workers() -> int:
    try:
        return max(2, int(os.getenv("TELEGRAM_NEWS_WORKERS", "8")))
    except Exception:
        return 8


def _fundamental_workers() -> int:
    try:
        return max(4, int(os.getenv("TELEGRAM_FUNDAMENTAL_WORKERS", "12")))
    except Exception:
        return 12


def _codex_batch_size() -> int:
    try:
        return max(4, int(os.getenv("TELEGRAM_CODEX_BATCH_SIZE", "12")))
    except Exception:
        return 12


def _max_actionable_positions() -> int:
    try:
        return max(0, int(os.getenv("TELEGRAM_MAX_ACTIONABLE_POSITIONS", "3")))
    except Exception:
        return 3


def _portfolio_risk_budget_pct() -> float:
    try:
        return max(0.05, float(os.getenv("TELEGRAM_PORTFOLIO_RISK_BUDGET_PCT", "0.35")))
    except Exception:
        return 0.35


def _max_position_weight_pct() -> float:
    try:
        return max(0.5, float(os.getenv("TELEGRAM_MAX_POSITION_WEIGHT_PCT", "4.0")))
    except Exception:
        return 4.0


def _news_discovery_limit() -> int:
    try:
        return max(0, int(os.getenv("TELEGRAM_NEWS_DISCOVERY_MAX_SYMBOLS", "0")))
    except Exception:
        return 0


def _generic_news_urls(symbol: str, company_name: str) -> list[str]:
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


def _dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def _load_symbol_list(path: Path) -> list[str]:
    if not path.exists():
        return []
    try:
        payload = _load_json(path)
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


def _fetch_table_symbols(url: str, columns: list[str]) -> list[str]:
    try:
        response = requests.get(url, headers={"User-Agent": "autostock/2.0"}, timeout=20)
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


def _load_all_us_symbols() -> list[str]:
    cached = _load_symbol_list(ALL_STOCKS_CACHE_PATH)
    if cached:
        return cached
    sp500 = _load_symbol_list(SP500_CACHE_PATH) or _fetch_table_symbols(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        ["Symbol", "Ticker", "Ticker symbol"],
    )
    nasdaq100 = _load_symbol_list(NASDAQ100_CACHE_PATH) or _fetch_table_symbols(
        "https://en.wikipedia.org/wiki/Nasdaq-100",
        ["Ticker", "Ticker symbol", "Symbol"],
    )
    return sorted(set(sp500 + nasdaq100))


def _derive_warnings(indicators: dict[str, Any], rebalance_hint: dict[str, Any] | None = None) -> list[str]:
    if isinstance(rebalance_hint, dict) and isinstance(rebalance_hint.get("warnings"), list):
        return [str(item) for item in rebalance_hint.get("warnings", [])]
    warnings: list[str] = []
    rsi = _f(indicators.get("rsi"), 50.0)
    bb_pos = _f(indicators.get("bb_position"), 50.0)
    volume_ratio = _f(indicators.get("volume_ratio"), 1.0)
    if rsi >= 80 or bb_pos >= 95:
        warnings.append("overheat_extreme")
    elif rsi >= 70 and bb_pos >= 80:
        warnings.append("overheat_dual")
    elif rsi >= 70 or bb_pos >= 80:
        warnings.append("overheat_warning")
    if volume_ratio < 1.0:
        warnings.extend(["volume_below_warn_threshold", "volume_below_regime_min"])
    return list(dict.fromkeys(warnings))


def _daily_price_context(bars: pd.DataFrame, indicators: dict[str, Any]) -> dict[str, Any]:
    if bars is None or bars.empty:
        return {}
    last = bars.iloc[-1]
    prev = bars.iloc[-2] if len(bars) >= 2 else last
    close = _f(last.get("Close"), _f(indicators.get("price"), 0.0))
    prev_close = _f(prev.get("Close"), close)
    open_price = _f(last.get("Open"), close)
    high = _f(last.get("High"), close)
    low = _f(last.get("Low"), close)
    volume = _f(last.get("Volume"), 0.0)
    volume_avg_20 = _f(bars["Volume"].tail(20).mean(), volume) if "Volume" in bars else volume
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
    volume_ratio_daily = volume / volume_avg_20 if volume_avg_20 > 0 else 1.0
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
        "dailyVolume": int(max(0.0, volume)),
        "dailyVolumeAvg20": int(max(0.0, volume_avg_20)),
        "dailyVolumeRatio": round(volume_ratio_daily, 2),
    }


def _scan_symbol(symbol: str, rebalance_hint: dict[str, Any] | None = None) -> dict[str, Any] | None:
    bars = get_stock_data(symbol, period="15mo", auto_adjust=False)
    if bars is None or bars.empty:
        return None
    indicators = calculate_indicators(bars)
    if indicators is None:
        return None
    chart_gate = chart_volume_gate(indicators)
    payload = {
        "symbol": symbol,
        "latestClosePrice": round(_f(indicators.get("price"), 0.0), 2),
        "latestCloseAsOf": bars.tail(1).index[0].isoformat() if len(bars.index) else "",
        "chartState": _s(chart_gate.get("state")),
        "volumeRatio": round(_f(chart_gate.get("volume_ratio"), 0.0), 2),
        "rsi": round(_f(indicators.get("rsi"), 0.0), 1),
        "adx": round(_f(indicators.get("adx"), 0.0), 1),
        "atr": round(_f(indicators.get("atr"), 0.0), 2),
        "atrPct": round(_f(indicators.get("atr_pct"), 0.0), 2),
        "support": indicators.get("support") if isinstance(indicators.get("support"), list) else [],
        "resistance": indicators.get("resistance") if isinstance(indicators.get("resistance"), list) else [],
        "ma50Gap": round(_f(indicators.get("ma50_gap"), 0.0), 2),
        "ma200Gap": round(_f(indicators.get("ma200_gap"), 0.0), 2),
        "return21d": round(_f(indicators.get("return_21d"), 0.0), 2),
        "return63d": round(_f(indicators.get("return_63d"), 0.0), 2),
        "warnings": _derive_warnings(indicators, rebalance_hint),
        "rebalanceHint": bool(rebalance_hint),
    }
    payload.update(_daily_price_context(bars, indicators))
    return payload


def _scan_full_universe(rebalance_hints: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return _scan_symbols(_load_all_us_symbols(), rebalance_hints)


def _scan_symbols(symbols: list[str], rebalance_hints: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    workers = _scan_workers()
    scanned: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_scan_symbol, symbol, rebalance_hints.get(symbol)): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            row = future.result()
            if row:
                scanned.append(row)
    scanned.sort(
        key=lambda row: (
            -_f(row.get("return63d"), 0.0),
            -_f(row.get("return21d"), 0.0),
            _s(row.get("symbol")),
        )
    )
    return scanned


def _news_discovery_symbols(selected_symbols: set[str]) -> list[str]:
    universe = _load_all_us_symbols()
    limit = _news_discovery_limit()
    symbols: list[str] = []
    seen: set[str] = set()

    def _push(symbol: str) -> None:
        sym = _s(symbol).upper()
        if not sym or sym in seen:
            return
        seen.add(sym)
        symbols.append(sym)

    for symbol in sorted(selected_symbols):
        _push(symbol)
    for symbol in universe:
        _push(symbol)
        if limit and len(symbols) >= max(len(selected_symbols), limit):
            break
    return symbols


def _collect_symbol_news_bundle(
    symbol: str,
    chart_row: dict[str, Any] | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    info = get_stock_info(symbol)
    next_events = build_next_known_events(symbol, info, datetime.now(timezone.utc))
    raw_events: list[dict[str, Any]] = []
    sess = session or requests.Session()
    try:
        raw_events.extend(fetch_sec_submission_events(symbol, limit=8, max_age_days=21, session=sess))
    except Exception:
        pass
    try:
        raw_events.extend(
            fetch_rss_events(
                _generic_news_urls(symbol, _s(info.get("name") or symbol)),
                symbol=symbol,
                max_per_feed=5,
                source_name="wire",
                category_hint="product",
                session=sess,
            )
        )
    except Exception:
        pass
    return {
        "symbol": symbol,
        "info": info,
        "nextEvents": next_events,
        "events": _dedupe_events(raw_events)[:10],
        "chartRow": chart_row or {"symbol": symbol},
    }


def _collect_news_bundles(
    news_symbols: list[str],
    chart_rows_by_symbol: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    bundles: dict[str, dict[str, Any]] = {}
    workers = _news_workers()
    chart_rows_by_symbol = chart_rows_by_symbol or {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_collect_symbol_news_bundle, symbol, chart_rows_by_symbol.get(symbol)): symbol
            for symbol in news_symbols
        }
        for future in as_completed(futures):
            bundle = future.result()
            symbol = _s(bundle.get("symbol")).upper()
            if symbol:
                bundles[symbol] = bundle
    return bundles


def _event_date_key(value: Any) -> str:
    raw = _s(value)
    return raw[:10] if raw else ""


def _bundle_has_news(bundle: dict[str, Any]) -> bool:
    events = bundle.get("events") if isinstance(bundle.get("events"), list) else []
    next_events = bundle.get("nextEvents") if isinstance(bundle.get("nextEvents"), list) else []
    return bool(events or next_events)


def _bundle_evidence_flags(bundle: dict[str, Any]) -> set[str]:
    events = bundle.get("events") if isinstance(bundle.get("events"), list) else []
    categories = {_s(row.get("category")).lower() for row in (bundle.get("events") or []) if isinstance(row, dict)}
    form_types = {_s(row.get("form")).lower() for row in (bundle.get("events") or []) if isinstance(row, dict)}
    sources = {_s(row.get("source")).lower() for row in events if isinstance(row, dict)}
    flags: set[str] = set()
    if sources & {"sec", "ir"}:
        flags.add("primary_source")
    if {"earnings", "guidance", "sec", "product", "deal", "analyst"} & categories:
        flags.add("fundamental_event")
    if {"8-k", "10-q", "10-k"} & form_types:
        flags.add("sec_event")
    next_events = bundle.get("nextEvents") if isinstance(bundle.get("nextEvents"), list) else []
    if next_events:
        flags.add("scheduled_event")
    return flags


def _news_bundle_key(bundle: dict[str, Any], selected_symbols: set[str]) -> tuple[int, int, str]:
    symbol = _s(bundle.get("symbol")).upper()
    events = bundle.get("events") if isinstance(bundle.get("events"), list) else []
    if symbol in selected_symbols:
        tier = 0
    else:
        flags = _bundle_evidence_flags(bundle)
        if "sec_event" in flags or "primary_source" in flags:
            tier = 1
        elif "fundamental_event" in flags:
            tier = 2
        elif events:
            tier = 3
        elif "scheduled_event" in flags:
            tier = 4
        else:
            tier = 5
    latest_event_date = max((_event_date_key(row.get("published_at")) for row in events if isinstance(row, dict)), default="")
    latest_event_rank = int(latest_event_date.replace("-", "") or "0")
    return (tier, -latest_event_rank, symbol)


def _select_news_symbols_from_bundles(
    bundles: dict[str, dict[str, Any]],
    selected_symbols: set[str],
    news_limit: int,
) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()

    def _push(symbol: str) -> None:
        sym = _s(symbol).upper()
        if not sym or sym in seen or sym not in bundles:
            return
        seen.add(sym)
        selected.append(sym)

    for symbol in sorted(selected_symbols):
        if _bundle_has_news(bundles.get(symbol, {})):
            _push(symbol)

    ranked = sorted(
        [bundle for bundle in bundles.values() if isinstance(bundle, dict) and _bundle_has_news(bundle)],
        key=lambda bundle: _news_bundle_key(bundle, selected_symbols),
    )
    for bundle in ranked:
        _push(_s(bundle.get("symbol")))
        if len(selected) >= max(len(selected_symbols), news_limit):
            break

    if not selected:
        for symbol in sorted(bundles):
            _push(symbol)
            if len(selected) >= news_limit:
                break
    return selected


def _attach_chart_rows_to_bundles(
    bundles: dict[str, dict[str, Any]],
    chart_rows_by_symbol: dict[str, dict[str, Any]],
) -> None:
    for symbol, row in chart_rows_by_symbol.items():
        if symbol in bundles:
            bundles[symbol]["chartRow"] = row


def _ratio_pct(value: Any) -> float:
    raw = _f(value, 0.0)
    if -1.0 <= raw <= 1.0:
        return round(raw * 100.0, 2)
    return round(raw, 2)


def _positive(value: Any) -> float | None:
    raw = _f(value, 0.0)
    return raw if raw > 0 else None


def _fundamental_snapshot(symbol: str) -> dict[str, Any] | None:
    info = get_stock_info(symbol)
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
    }


def _scan_fundamentals(symbols: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=_fundamental_workers()) as executor:
        futures = {executor.submit(_fundamental_snapshot, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            row = future.result()
            if row:
                rows.append(row)
    return rows


def _median_metric(rows: list[dict[str, Any]], key: str, *, positive_only: bool = False) -> float | None:
    values: list[float] = []
    for row in rows:
        raw = row.get(key)
        if raw is None:
            continue
        value = _f(raw, 0.0)
        if positive_only and value <= 0:
            continue
        values.append(value)
    if not values:
        return None
    return float(median(values))


def _valuation_benchmarks(rows: list[dict[str, Any]]) -> dict[str, dict[str, float | None]]:
    keys = {
        "pFcf": True,
        "fcfYieldPct": True,
        "pe": True,
        "forwardPe": True,
        "pb": True,
        "earningsYieldPct": True,
        "roePct": False,
        "profitMarginPct": False,
        "operatingMarginPct": False,
        "revenueGrowthPct": False,
        "earningsGrowthPct": False,
        "forwardEpsGrowthPct": False,
        "debtToEquity": True,
        "currentRatio": True,
    }
    sectors = sorted({_s(row.get("sector")) or "N/A" for row in rows})
    grouped: dict[str, list[dict[str, Any]]] = {"__UNIVERSE__": rows}
    for sector in sectors:
        grouped[sector] = [row for row in rows if (_s(row.get("sector")) or "N/A") == sector]

    out: dict[str, dict[str, float | None]] = {}
    for sector, sector_rows in grouped.items():
        out[sector] = {
            key: _median_metric(sector_rows, key, positive_only=positive_only)
            for key, positive_only in keys.items()
        }
    return out


def _bench_value(
    row: dict[str, Any],
    benchmarks: dict[str, dict[str, float | None]],
    key: str,
) -> float | None:
    sector = _s(row.get("sector")) or "N/A"
    value = (benchmarks.get(sector) or {}).get(key)
    if value is None:
        value = (benchmarks.get("__UNIVERSE__") or {}).get(key)
    return value


def _valuation_assessment(row: dict[str, Any], benchmarks: dict[str, dict[str, float | None]]) -> dict[str, Any]:
    absolute_value_reasons: list[str] = []
    value_reasons: list[str] = []
    quality_reasons: list[str] = []
    growth_reasons: list[str] = []
    risk_reasons: list[str] = []

    def below(key: str, label: str) -> bool:
        value = _positive(row.get(key))
        bench = _bench_value(row, benchmarks, key)
        if value is not None and bench is not None and value <= bench:
            value_reasons.append(f"{label} {value:.2f} <= 섹터 중앙값 {bench:.2f}")
            return True
        return False

    def above(key: str, label: str, target: list[str]) -> bool:
        raw = row.get(key)
        if raw is None:
            return False
        value = _f(raw, 0.0)
        bench = _bench_value(row, benchmarks, key)
        if bench is not None and value >= bench:
            target.append(f"{label} {value:.2f}% >= 섹터 중앙값 {bench:.2f}%")
            return True
        return False

    fcf_yield_ok = above("fcfYieldPct", "FCF 수익률", value_reasons)
    earnings_yield_ok = above("earningsYieldPct", "이익수익률", value_reasons)
    p_fcf_ok = below("pFcf", "P/FCF")
    pe_ok = below("pe", "PER")
    forward_pe_ok = below("forwardPe", "Forward PER")
    pb_ok = below("pb", "P/B")

    fcf_yield = _positive(row.get("fcfYieldPct"))
    p_fcf = _positive(row.get("pFcf"))
    pe = _positive(row.get("pe"))
    forward_pe = _positive(row.get("forwardPe"))
    earnings_yield = _positive(row.get("earningsYieldPct"))
    cash_return_ok = (fcf_yield is not None and fcf_yield >= 4.0) or (p_fcf is not None and p_fcf <= 25.0)
    earnings_price_ok = (
        (earnings_yield is not None and earnings_yield >= 5.0)
        or (pe is not None and pe <= 20.0)
        or (forward_pe is not None and forward_pe <= 18.0)
    )
    if cash_return_ok:
        absolute_value_reasons.append("절대 FCF 안전마진 통과")
    else:
        risk_reasons.append("절대 FCF 수익률/P-FCF 기준 안전마진 부족")
    if earnings_price_ok:
        absolute_value_reasons.append("절대 이익/PER 안전마진 통과")
    else:
        risk_reasons.append("절대 이익수익률/PER 기준 안전마진 부족")

    roe_ok = above("roePct", "ROE", quality_reasons)
    profit_margin_ok = above("profitMarginPct", "순마진", quality_reasons)
    operating_margin_ok = above("operatingMarginPct", "영업마진", quality_reasons)

    revenue_growth_ok = above("revenueGrowthPct", "매출 성장", growth_reasons)
    earnings_growth_ok = above("earningsGrowthPct", "이익 성장", growth_reasons)
    forward_eps_growth_ok = above("forwardEpsGrowthPct", "Forward EPS 성장", growth_reasons)

    debt = _positive(row.get("debtToEquity"))
    debt_bench = _bench_value(row, benchmarks, "debtToEquity")
    debt_limit = max(200.0, (debt_bench or 0.0) * 1.75)
    debt_ok = debt is None or debt <= debt_limit
    if debt is not None and not debt_ok:
        risk_reasons.append(f"부채비율 {debt:.2f}가 허용 범위 {debt_limit:.2f} 초과")

    current_ratio = _positive(row.get("currentRatio"))
    sector = _s(row.get("sector")).lower()
    liquidity_ratio_less_relevant = sector in {"financial services", "real estate", "utilities"}
    liquidity_ok = current_ratio is None or current_ratio >= 0.75 or liquidity_ratio_less_relevant
    if current_ratio is not None and not liquidity_ok:
        risk_reasons.append(f"유동비율 {current_ratio:.2f}가 0.75 미만")

    free_cash_flow = _f(row.get("freeCashFlow"), 0.0)
    if bool(row.get("financialCurrencyMismatch")):
        risk_reasons.append(
            f"시세 통화({_s(row.get('currency'))})와 재무 통화({_s(row.get('financialCurrency'))})가 달라 FCF 배수 제외"
        )
    if free_cash_flow <= 0:
        risk_reasons.append("잉여현금흐름이 양수가 아님")

    value_signal_count = sum(bool(item) for item in [fcf_yield_ok, earnings_yield_ok, p_fcf_ok, pe_ok, forward_pe_ok, pb_ok])
    quality_signal_count = sum(bool(item) for item in [roe_ok, profit_margin_ok, operating_margin_ok])
    growth_signal_count = sum(bool(item) for item in [revenue_growth_ok, earnings_growth_ok, forward_eps_growth_ok])

    relative_cash_value_ok = fcf_yield_ok or p_fcf_ok
    relative_earnings_value_ok = pe_ok or forward_pe_ok or earnings_yield_ok
    if (
        free_cash_flow > 0
        and not bool(row.get("financialCurrencyMismatch"))
        and cash_return_ok
        and earnings_price_ok
        and (relative_cash_value_ok or relative_earnings_value_ok)
    ):
        value_state = "UNDERVALUED"
    elif value_signal_count or (cash_return_ok and earnings_price_ok):
        value_state = "RELATIVE_VALUE"
    else:
        value_state = "NOT_CHEAP"

    quality_state = "QUALITY_OK" if quality_signal_count else "QUALITY_UNPROVEN"
    growth_state = "GROWTH_OK" if growth_signal_count else "GROWTH_UNPROVEN"
    balance_state = "BALANCE_OK" if debt_ok and liquidity_ok and free_cash_flow > 0 else "BALANCE_RISK"

    return {
        "valueState": value_state,
        "qualityState": quality_state,
        "growthState": growth_state,
        "balanceState": balance_state,
        "valueSignalCount": value_signal_count,
        "qualitySignalCount": quality_signal_count,
        "growthSignalCount": growth_signal_count,
        "valuationReasons": [*absolute_value_reasons, *value_reasons][:6],
        "qualityReasons": quality_reasons[:4],
        "growthReasons": growth_reasons[:4],
        "riskReasons": risk_reasons[:4],
        "sectorBenchmarks": {
            key: _bench_value(row, benchmarks, key)
            for key in ("pFcf", "fcfYieldPct", "pe", "forwardPe", "pb", "roePct", "profitMarginPct", "revenueGrowthPct")
        },
    }


def _value_decision(
    row: dict[str, Any],
    benchmarks: dict[str, dict[str, float | None]],
    news: dict[str, Any] | None = None,
) -> dict[str, Any]:
    assessment = _valuation_assessment(row, benchmarks)
    news_signal = _s((news or {}).get("signal")).lower()
    news_strength = _s((news or {}).get("strength")).lower()
    negative_event = news_signal == "bearish" and news_strength in {"strong", "moderate"}

    value_state = _s(assessment.get("valueState"))
    quality_state = _s(assessment.get("qualityState"))
    growth_state = _s(assessment.get("growthState"))
    balance_state = _s(assessment.get("balanceState"))
    target_upside = _f(row.get("targetUpsidePct"), 0.0)
    recommendation = _s(row.get("recommendation")).lower()
    external_view_negative = target_upside < 0 or recommendation in {"sell", "underperform"}
    external_view_positive = target_upside > 0 and recommendation in {"buy", "strong_buy"}
    if external_view_negative:
        assessment.setdefault("riskReasons", []).append(
            f"외부 컨센서스가 보수적: 목표가 업사이드 {target_upside:.2f}%, 추천 {recommendation or 'N/A'}"
        )

    if (
        value_state == "UNDERVALUED"
        and quality_state == "QUALITY_OK"
        and growth_state == "GROWTH_OK"
        and balance_state == "BALANCE_OK"
        and not negative_event
        and not external_view_negative
    ):
        state = "VALUE_ACTIONABLE"
        bucket = "actionable_now"
        reason = "섹터 대비 싸고, 현금흐름/수익성 품질이 같이 확인됨"
    elif (
        value_state == "UNDERVALUED"
        and quality_state == "QUALITY_OK"
        and balance_state == "BALANCE_OK"
        and external_view_positive
        and not negative_event
        and not external_view_negative
    ):
        state = "VALUE_ACTIONABLE"
        bucket = "actionable_now"
        reason = "섹터 대비 싸고, 품질과 외부 컨센서스 업사이드가 확인됨"
    elif value_state in {"UNDERVALUED", "RELATIVE_VALUE"} and not negative_event:
        state = "VALUE_WATCH"
        bucket = "wait_pullback"
        reason = "일부 할인 신호는 있으나 품질/재무/성장 확인이 부족"
    elif negative_event:
        state = "VALUE_WATCH"
        bucket = "wait_pullback"
        reason = "밸류에이션은 보이지만 최근 이벤트 리스크 확인 필요"
    else:
        state = "VALUE_REJECTED"
        bucket = "avoid"
        reason = "섹터 대비 싸다는 근거가 부족"

    reasons = [
        *[str(item) for item in assessment.get("valuationReasons") or []],
        *[str(item) for item in assessment.get("riskReasons") or []],
        *[str(item) for item in assessment.get("qualityReasons") or []],
        *[str(item) for item in assessment.get("growthReasons") or []],
    ]
    if negative_event:
        reasons.append(f"뉴스 {news_signal}/{news_strength}: {_s((news or {}).get('headline'))}")
    return {
        **assessment,
        "decisionState": state,
        "actionBucket": bucket,
        "actionReason": reason,
        "portfolioWeightPct": _max_position_weight_pct() if state == "VALUE_ACTIONABLE" else 0.0,
        "decisionReasons": [item for item in reasons if item][:8],
    }


def _value_rank_key(row: dict[str, Any]) -> tuple[int, int, int, int, float, float, float, str]:
    state_rank = {"VALUE_ACTIONABLE": 0, "VALUE_WATCH": 1, "VALUE_REJECTED": 2}.get(_s(row.get("decisionState")), 9)
    return (
        state_rank,
        -int(_f(row.get("valueSignalCount"), 0.0)),
        -int(_f(row.get("qualitySignalCount"), 0.0)),
        -int(_f(row.get("growthSignalCount"), 0.0)),
        -_f(row.get("fcfYieldPct"), 0.0),
        _f(row.get("pFcf"), 999999.0) if row.get("pFcf") is not None else 999999.0,
        _f(row.get("forwardPe"), 999999.0) if row.get("forwardPe") is not None else 999999.0,
        _s(row.get("symbol")),
    )


def _value_synthesis_payload(evaluated: list[dict[str, Any]], universe_count: int) -> dict[str, Any]:
    actionable = [row for row in evaluated if _s(row.get("actionBucket")) == "actionable_now"]
    watch = [row for row in evaluated if _s(row.get("actionBucket")) == "wait_pullback"]
    rejected = [row for row in evaluated if _s(row.get("actionBucket")) == "avoid"]
    if actionable:
        names = "·".join(_s(row.get("symbol")) for row in actionable[:5])
        summary = f"가치평가 우선 기준 할인 후보는 {names}다. 뉴스는 매수 근거가 아니라 가치 훼손 리스크 확인용으로만 반영했다."
    else:
        summary = "가치평가 우선 기준 섹터 대비 할인과 품질이 동시에 확인되는 후보가 없다."
    return {
        "summary": summary,
        "decisionEngine": "valuation-first-v1",
        "universeCount": universe_count,
        "candidateCount": len(evaluated),
        "actionableCap": _max_actionable_positions(),
        "stateCounts": {
            "VALUE_ACTIONABLE": len(actionable),
            "VALUE_WATCH": len(watch),
            "VALUE_REJECTED": len(rejected),
        },
        "items": [
            {
                "symbol": _s(row.get("symbol")),
                "decisionState": _s(row.get("decisionState")),
                "finalBucket": _s(row.get("actionBucket")),
                "valueState": _s(row.get("valueState")),
                "qualityState": _s(row.get("qualityState")),
                "growthState": _s(row.get("growthState")),
                "balanceState": _s(row.get("balanceState")),
                "portfolioWeightPct": row.get("portfolioWeightPct"),
                "decisionReason": _s(row.get("actionReason")),
            }
            for row in evaluated
        ],
    }


def _batched_ai_news_analysis(bundles: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]] | dict[str, str]:
    if not bundles:
        return {}
    batch_size = _codex_batch_size()
    symbols = sorted(bundles.keys())
    analyzed: dict[str, dict[str, Any]] = {}
    for idx in range(0, len(symbols), batch_size):
        group_symbols = symbols[idx : idx + batch_size]
        items = []
        for symbol in group_symbols:
            bundle = bundles[symbol]
            chart_row = bundle["chartRow"]
            items.append(
                {
                    "symbol": symbol,
                    "chart_gate": {
                        "state": chart_row.get("chartState"),
                        "volume_ratio": chart_row.get("volumeRatio"),
                        "rsi": chart_row.get("rsi"),
                        "adx": chart_row.get("adx"),
                    },
                    "recent_events": [
                        {
                            "headline": row.get("headline"),
                            "source": row.get("source"),
                            "category": row.get("category"),
                            "published_at": row.get("published_at"),
                        }
                        for row in bundle.get("events", [])[:8]
                    ],
                    "next_known_events": bundle.get("nextEvents", [])[:4],
                }
            )
        prompt = (
            "You are a valuation risk analyst for public equities.\n"
            "Write in Korean internally, but output STRICT JSON only.\n"
            "For each symbol, classify whether recent events improve or impair the investment value case.\n"
            "News must never promote a symbol by itself: bullish means value impairment risk is lower or fundamentals improved; "
            "bearish means the value case may be impaired; neutral means no material value-case change.\n"
            "Use enums only:\n"
            "- signal: bullish | bearish | neutral\n"
            "- strength: strong | moderate | weak | none\n\n"
            f"Items: {items}\n\n"
            "Return JSON only in this shape:\n"
            '{"items":[{"symbol":"AAPL","signal":"bullish|bearish|neutral","strength":"strong|moderate|weak|none","headline":"key headline","rationale":["short reason 1","short reason 2"]}]}'
        )
        text = ai._call(prompt, max_tokens=2200)
        if not text:
            return {"error": "codex_batch_analysis_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}
        obj = ai._extract_json_object(text)
        if not isinstance(obj, dict):
            return {"error": "codex_batch_json_parse_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}
        rows = obj.get("items")
        if not isinstance(rows, list):
            return {"error": "codex_batch_items_missing", "model": ai.model, "reasoningEffort": ai.reasoning_effort}
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = _s(row.get("symbol")).upper()
            if not symbol:
                continue
            signal = _s(row.get("signal")).lower()
            strength = _s(row.get("strength")).lower()
            if signal not in {"bullish", "bearish", "neutral"}:
                signal = "neutral"
            if strength not in {"strong", "moderate", "weak", "none"}:
                strength = "none"
            analyzed[symbol] = {
                "signal": signal,
                "strength": strength,
                "headline": _s(row.get("headline")),
                "rationale": [str(item) for item in (row.get("rationale") or [])[:4]] if isinstance(row.get("rationale"), list) else [],
                "mode": "codex-batch",
                "ok": True,
            }
    return analyzed


def _build_execution_plan(row: dict[str, Any], latest_price: float) -> dict[str, Any]:
    supports = row.get("support") if isinstance(row.get("support"), list) else []
    resistances = row.get("resistance") if isinstance(row.get("resistance"), list) else []
    support_candidates = sorted(
        {_f(value, 0.0) for value in supports if _f(value, 0.0) > 0 and _f(value, 0.0) < latest_price},
        reverse=True,
    )
    resistance_candidates = sorted(
        {_f(value, 0.0) for value in resistances if _f(value, 0.0) > latest_price}
    )
    support = support_candidates[0] if support_candidates else 0.0
    resistance = resistance_candidates[0] if resistance_candidates else 0.0
    resistance2 = resistance_candidates[1] if len(resistance_candidates) > 1 else 0.0
    average_entry = round(latest_price, 2) if latest_price > 0 else 0.0
    close_stop = round(support, 2) if support > 0 else 0.0
    hard_stop = close_stop
    tp1 = round(resistance, 2) if resistance > 0 else 0.0
    tp2 = round(resistance2, 2) if resistance2 > 0 else 0.0

    return {
        "entryMode": "event_driven_current",
        "activationReason": "current_price_with_observed_support_resistance",
        "entryLevels": [
            {"name": "current", "price": average_entry, "splitPct": 100.0},
        ],
        "averageEntryPrice": average_entry,
        "closeStopPrice": close_stop,
        "hardStopPrice": hard_stop,
        "tp1Price": tp1,
        "tp2Price": tp2,
        "supportUsed": round(support, 2),
        "resistanceUsed": round(resistance, 2),
    }


def _risk_pct(current: float, stop: float) -> float | None:
    if current <= 0 or stop <= 0:
        return None
    return round((current - stop) / current * 100.0, 2)


def _reward_pct(current: float, target: float) -> float | None:
    if current <= 0 or target <= 0:
        return None
    return round((target - current) / current * 100.0, 2)


def _pct_change(current: float, reference: float) -> float | None:
    if current <= 0 or reference <= 0:
        return None
    return round((current / reference - 1.0) * 100.0, 2)


def _rr(reward_pct: float | None, risk_pct: float | None) -> float | None:
    if reward_pct is None or risk_pct is None or risk_pct <= 0:
        return None
    return round(reward_pct / risk_pct, 2)


def _trade_verdict(plan: dict[str, Any], latest_price: float, warnings: list[str]) -> tuple[str, str]:
    entry_prices = [
        _f(level.get("price"), 0.0)
        for level in plan.get("entryLevels") or []
        if isinstance(level, dict) and _f(level.get("price"), 0.0) > 0
    ]
    stop = _f(plan.get("closeStopPrice"), 0.0)
    tp1 = _f(plan.get("tp1Price"), 0.0)

    if latest_price <= 0 or stop <= 0 or tp1 <= 0:
        return ("watch", "price_plan_incomplete")
    if latest_price <= stop:
        return ("avoid", "below_stop")
    if tp1 > 0 and latest_price >= tp1:
        return ("wait_pullback", "target_already_reached")
    if not entry_prices:
        return ("watch", "entry_levels_missing")
    entry_floor = min(entry_prices)
    entry_ceiling = max(entry_prices)
    if entry_floor <= latest_price <= entry_ceiling:
        return ("plan_active", "inside_entry_plan")
    if latest_price > entry_ceiling:
        return ("wait_pullback", "above_entry_plan")
    return ("watch", "below_entry_plan")


def _catalyst_assessment(
    news: dict[str, Any],
    raw_events: list[dict[str, Any]],
    next_events: list[dict[str, Any]],
) -> dict[str, Any]:
    signal = _s(news.get("signal")).lower()
    strength = _s(news.get("strength")).lower()
    headline = _s(news.get("headline"))
    source_set = {_s(item.get("source")).lower() for item in raw_events if isinstance(item, dict)}
    category_set = {_s(item.get("category")).lower() for item in raw_events if isinstance(item, dict)}
    has_primary_source = bool(source_set & {"sec", "ir"})
    has_fundamental_event = bool(category_set & {"earnings", "guidance", "sec", "product", "deal", "analyst"})

    if signal == "bearish" and strength in {"strong", "moderate"}:
        state = "NEGATIVE"
    elif signal == "bullish" and strength in {"strong", "moderate"} and (has_primary_source or has_fundamental_event):
        state = "PRIMARY_BULLISH"
    elif signal == "bullish" and strength in {"strong", "moderate", "weak"}:
        state = "BULLISH"
    elif has_primary_source or has_fundamental_event:
        state = "EVENT_ONLY"
    else:
        state = "NONE"

    reasons: list[str] = []
    if signal:
        reasons.append(f"뉴스 {signal}/{strength or 'none'}")
    if has_primary_source:
        reasons.append("공시/IR 1차 출처 포함")
    if has_fundamental_event:
        reasons.append("실적/가이던스/제품/계약/애널리스트 이벤트 분류")
    if next_events:
        reasons.append(f"다가오는 이벤트 {len(next_events)}개")
    if not reasons:
        reasons.append("검증 가능한 이벤트 촉매 부족")
    return {
        "catalystState": state,
        "catalystHeadline": headline,
        "catalystReasons": reasons,
    }


def _market_reaction_assessment(row: dict[str, Any]) -> dict[str, Any]:
    move_atr = _f(row.get("eventMoveAtr"), 0.0)
    volume_ratio = _f(row.get("dailyVolumeRatio"), _f(row.get("volumeRatio"), 1.0))
    close_location = _f(row.get("closeLocationPct"), 50.0)
    day_return = _f(row.get("dayReturnPct"), 0.0)
    latest_price = _f(row.get("latestClosePrice"), 0.0)
    previous_close = _f(row.get("previousClosePrice"), 0.0)
    open_price = _f(row.get("latestOpenPrice"), 0.0)

    if latest_price <= 0 or previous_close <= 0 or open_price <= 0:
        state = "UNKNOWN"
        reason = "전일 종가/당일 시가 데이터 부족"
    elif latest_price >= previous_close and latest_price >= open_price:
        state = "CONFIRMED"
        reason = "전일 종가와 당일 시가 위에서 마감"
    elif latest_price < previous_close and latest_price < open_price:
        state = "REJECTED"
        reason = "전일 종가와 당일 시가 아래에서 마감"
    else:
        state = "MIXED"
        reason = "전일 대비와 당일 흐름이 엇갈림"

    return {
        "marketReactionState": state,
        "marketReactionReason": reason,
        "eventMoveAtr": round(move_atr, 2),
        "reactionSnapshot": (
            f"일간 {day_return:.2f}%, {move_atr:.2f} ATR, "
            f"거래량 {volume_ratio:.2f}x, 종가위치 {close_location:.1f}%"
        ),
    }


def _entry_structure_assessment(row: dict[str, Any]) -> dict[str, Any]:
    latest_price = _f(row.get("latestClosePrice"), 0.0)
    stop = _f(row.get("closeStopPrice"), 0.0)
    atr = _f(row.get("atr"), 0.0)
    risk = _f(row.get("riskToStopPct"), 0.0)
    tp1 = _f(row.get("tp1Price"), 0.0)
    reward_price = tp1 - latest_price if tp1 > 0 and latest_price > 0 else 0.0
    risk_price = latest_price - stop if latest_price > 0 and stop > 0 else 0.0
    entry_prices = [
        _f(level.get("price"), 0.0)
        for level in row.get("entryLevels") or []
        if isinstance(level, dict) and _f(level.get("price"), 0.0) > 0
    ]
    entry_floor = min(entry_prices) if entry_prices else 0.0
    entry_ceiling = max(entry_prices) if entry_prices else 0.0
    entry_reference = _f(row.get("averageEntryPrice"), entry_ceiling)
    entry_distance_atr = 0.0
    if atr > 0 and latest_price > 0 and entry_reference > 0:
        entry_distance_atr = (latest_price - entry_reference) / atr

    if latest_price <= 0 or stop <= 0 or tp1 <= 0 or not entry_prices:
        state = "INVALID"
        reason = "가격/진입/손절/목표 데이터 부족"
    elif latest_price <= stop:
        state = "BROKEN"
        reason = "현재가가 손절선 이하"
    elif tp1 > 0 and latest_price >= tp1:
        state = "TARGET_REACHED"
        reason = "TP1까지 보상 구간이 남아 있지 않음"
    elif reward_price <= risk_price:
        state = "NO_REWARD_EDGE"
        reason = "관측 TP1까지 남은 보상이 손절 리스크보다 크지 않음"
    elif risk <= 0:
        state = "INVALID"
        reason = "손절 리스크 계산 불가"
    elif entry_floor <= latest_price <= entry_ceiling:
        state = "INSIDE_PLAN"
        reason = "현재가가 계산된 진입 가격 구간 안에 있음"
    elif latest_price > entry_ceiling:
        state = "ABOVE_PLAN"
        reason = "현재가가 계산된 진입 가격 구간 위에 있음"
    else:
        state = "BELOW_PLAN"
        reason = "현재가가 계산된 진입 가격 구간 아래에 있음"

    return {
        "entryStructureState": state,
        "entryStructureReason": reason,
        "entryDistanceAtr": round(entry_distance_atr, 2),
        "entryPlanFloor": round(entry_floor, 2),
        "entryPlanCeiling": round(entry_ceiling, 2),
    }


def _position_weight_from_risk(risk_to_stop_pct: float | None, state: str) -> float:
    if state != "ACTIONABLE":
        return 0.0
    risk = _f(risk_to_stop_pct, 0.0)
    if risk <= 0:
        return 0.0
    weight = _portfolio_risk_budget_pct() / risk * 100.0
    return round(max(0.0, min(_max_position_weight_pct(), weight)), 2)


def _strategy_decision(
    row: dict[str, Any],
    news: dict[str, Any],
    raw_events: list[dict[str, Any]],
    next_events: list[dict[str, Any]],
) -> dict[str, Any]:
    catalyst = _catalyst_assessment(news, raw_events, next_events)
    reaction = _market_reaction_assessment(row)
    entry = _entry_structure_assessment(row)
    catalyst_state = _s(catalyst.get("catalystState"))
    reaction_state = _s(reaction.get("marketReactionState"))
    entry_state = _s(entry.get("entryStructureState"))

    if catalyst_state == "NEGATIVE":
        state = "REJECTED"
        reason = "부정 이벤트가 우선"
    elif entry_state in {"INVALID", "BROKEN", "TARGET_REACHED"}:
        state = "REJECTED"
        reason = _s(entry.get("entryStructureReason"))
    elif catalyst_state == "NONE":
        state = "WATCH"
        reason = "매수 판단에 충분한 이벤트 촉매가 없음"
    elif reaction_state == "REJECTED":
        state = "WATCH"
        reason = _s(reaction.get("marketReactionReason"))
    elif reaction_state in {"UNKNOWN", "MIXED"}:
        state = "WATCH"
        reason = _s(reaction.get("marketReactionReason"))
    elif entry_state in {"ABOVE_PLAN", "BELOW_PLAN", "NO_REWARD_EDGE"}:
        state = "SETUP_FORMING"
        reason = _s(entry.get("entryStructureReason"))
    elif catalyst_state == "PRIMARY_BULLISH" and reaction_state == "CONFIRMED" and entry_state == "INSIDE_PLAN":
        state = "ACTIONABLE"
        reason = "검증 가능한 bullish 이벤트, 당일 방향 확인, 진입 구간이 모두 충족"
    elif catalyst_state == "BULLISH" and reaction_state == "CONFIRMED" and entry_state == "INSIDE_PLAN":
        state = "SETUP_FORMING"
        reason = "bullish 뉴스는 있으나 1차/기초 이벤트 확인 전까지 대기"
    else:
        state = "SETUP_FORMING"
        reason = "관심 후보이나 이벤트, 당일 방향, 진입 구간 중 일부가 미충족"

    bucket = "actionable_now" if state == "ACTIONABLE" else "avoid" if state == "REJECTED" else "wait_pullback"
    weight = _position_weight_from_risk(row.get("riskToStopPct"), state)
    decision_reasons = [
        *[str(item) for item in (catalyst.get("catalystReasons") or [])[:3]],
        _s(reaction.get("marketReactionReason")),
        _s(entry.get("entryStructureReason")),
    ]
    return {
        **catalyst,
        **reaction,
        **entry,
        "decisionState": state,
        "actionBucket": bucket,
        "actionReason": reason,
        "portfolioWeightPct": weight,
        "decisionReasons": [item for item in decision_reasons if item],
    }


def _strategy_rank_key(row: dict[str, Any]) -> tuple[int, int, int, int, str]:
    state_rank = {"ACTIONABLE": 0, "SETUP_FORMING": 1, "WATCH": 2, "REJECTED": 3}.get(_s(row.get("decisionState")), 9)
    catalyst_rank = {"PRIMARY_BULLISH": 0, "BULLISH": 1, "EVENT_ONLY": 2, "NONE": 3, "NEGATIVE": 4}.get(_s(row.get("catalystState")), 9)
    reaction_rank = {"CONFIRMED": 0, "MIXED": 1, "UNKNOWN": 2, "REJECTED": 3}.get(_s(row.get("marketReactionState")), 9)
    entry_rank = {"INSIDE_PLAN": 0, "ABOVE_PLAN": 1, "BELOW_PLAN": 2, "NO_REWARD_EDGE": 3, "TARGET_REACHED": 4, "BROKEN": 5, "INVALID": 6}.get(_s(row.get("entryStructureState")), 9)
    return (
        state_rank,
        catalyst_rank,
        reaction_rank,
        entry_rank,
        _s(row.get("symbol")),
    )


def _apply_actionable_cap(evaluated: list[dict[str, Any]]) -> None:
    cap = _max_actionable_positions()
    actionable = sorted(
        [row for row in evaluated if _s(row.get("actionBucket")) == "actionable_now"],
        key=_value_rank_key if any(_s(row.get("decisionState")).startswith("VALUE_") for row in evaluated) else _strategy_rank_key,
    )
    for idx, row in enumerate(actionable, start=1):
        if idx <= cap:
            row["finalRank"] = idx
            continue
        row["decisionState"] = "VALUE_WATCH" if _s(row.get("decisionState")).startswith("VALUE_") else "SETUP_FORMING"
        row["actionBucket"] = "wait_pullback"
        row["actionReason"] = "일일 후보 슬롯 초과로 관찰"
        row["portfolioWeightPct"] = 0.0


def _strategy_synthesis_payload(evaluated: list[dict[str, Any]]) -> dict[str, Any]:
    actionable = [row for row in evaluated if _s(row.get("actionBucket")) == "actionable_now"]
    setup = [row for row in evaluated if _s(row.get("decisionState")) == "SETUP_FORMING"]
    watch = [row for row in evaluated if _s(row.get("decisionState")) == "WATCH"]
    rejected = [row for row in evaluated if _s(row.get("decisionState")) == "REJECTED"]
    if actionable:
        names = "·".join(_s(row.get("symbol")) for row in actionable[:3])
        summary = f"증거 게이트 기준 즉시 진입은 {names}로 제한하고, 나머지는 이벤트·당일 방향·진입 구간이 모두 맞기 전까지 대기한다."
    else:
        summary = "증거 게이트 기준 검증 가능한 이벤트, 당일 방향, 진입 구간이 동시에 맞는 즉시 진입 후보가 없다."
    return {
        "summary": summary,
        "decisionEngine": "evidence-gates-v1",
        "candidateCount": len(evaluated),
        "actionableCap": _max_actionable_positions(),
        "stateCounts": {
            "ACTIONABLE": len(actionable),
            "SETUP_FORMING": len(setup),
            "WATCH": len(watch),
            "REJECTED": len(rejected),
        },
        "items": [
            {
                "symbol": _s(row.get("symbol")),
                "decisionState": _s(row.get("decisionState")),
                "finalBucket": _s(row.get("actionBucket")),
                "portfolioWeightPct": row.get("portfolioWeightPct"),
                "decisionReason": _s(row.get("actionReason")),
                "marketReactionState": _s(row.get("marketReactionState")),
                "entryStructureState": _s(row.get("entryStructureState")),
                "catalystState": _s(row.get("catalystState")),
            }
            for row in evaluated
        ],
    }


def _chart_clean_enough(row: dict[str, Any]) -> bool:
    warnings = {str(item) for item in (row.get("warnings") or [])}
    return not bool({"entry_negative", "overheat_extreme", "overheat_dual"} & warnings)


def _chart_buyable(row: dict[str, Any]) -> bool:
    return (
        _chart_clean_enough(row)
        and _s(row.get("tradeVerdict")) in {"plan_active", "watch"}
    )


def _chart_wait(row: dict[str, Any]) -> bool:
    return (
        _chart_clean_enough(row)
        and _s(row.get("tradeVerdict")) == "wait_pullback"
        and _s(row.get("chartState")) in {"constructive", "confirmed_breakout", "mixed"}
    )


def _chart_gate_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    current_vs_entry = _f(row.get("currentVsEntryPct"), 999.0)
    reward = _f(row.get("rewardToTp1Pct"), 0.0)
    rsi = _f(row.get("rsi"), 50.0)
    volume_ratio = _f(row.get("volumeRatio"), 0.0)
    chart_state = _s(row.get("chartState"))
    warnings = {str(item) for item in (row.get("warnings") or [])}

    if _s(row.get("tradeVerdict")) == "plan_active":
        reasons.append("현재가가 관측 진입 기준")
    elif _s(row.get("tradeVerdict")) == "wait_pullback":
        reasons.append(f"눌림 대기: 진입가 대비 {current_vs_entry:.2f}%")

    if chart_state in {"constructive", "confirmed_breakout"}:
        reasons.append(f"차트 {chart_state}")
    elif chart_state:
        reasons.append(f"차트 {chart_state}")

    reasons.append(f"TP1 여력 {reward:.2f}%")
    reasons.append(f"손절 리스크 {_f(row.get('riskToStopPct')):.2f}%")
    reasons.append(f"RSI {rsi:.1f}")
    reasons.append(f"거래량 {volume_ratio:.2f}x")

    severe_warnings = {"entry_negative", "overheat_extreme", "overheat_dual"} & warnings
    if severe_warnings:
        reasons.append("추격/과열 경고")
    elif "overheat_warning" in warnings:
        reasons.append("과열 주의")
    else:
        reasons.append("심각 과열 없음")
    return reasons


def _chart_buyable_key(row: dict[str, Any]) -> str:
    return _s(row.get("symbol"))


def _chart_wait_key(row: dict[str, Any]) -> str:
    return _s(row.get("symbol"))


def _chart_leader_key(row: dict[str, Any]) -> tuple[float, float, str]:
    return (
        -_f(row.get("return63d"), 0.0),
        -_f(row.get("return21d"), 0.0),
        _s(row.get("symbol")),
    )


def _evaluate_chart_row(
    row: dict[str, Any],
    *,
    executed_weights_pct: dict[str, Any],
    selected_symbols: set[str],
) -> dict[str, Any] | None:
    latest_price = round(_f(row.get("latestClosePrice"), 0.0), 2)
    if latest_price <= 0:
        return None
    normalized_plan = _build_execution_plan(
        {
            **row,
            "atr": row.get("atr"),
            "support": row.get("support"),
            "resistance": row.get("resistance"),
            "volume_ratio": row.get("volumeRatio"),
        },
        latest_price=latest_price,
    )
    warnings = [str(item) for item in (row.get("warnings") or [])]
    trade_verdict, trade_reason = _trade_verdict(normalized_plan, latest_price, warnings)
    risk_to_stop_pct = _risk_pct(latest_price, _f(normalized_plan.get("closeStopPrice"), 0.0))
    reward_to_tp1_pct = _reward_pct(latest_price, _f(normalized_plan.get("tp1Price"), 0.0))
    reward_to_tp2_pct = _reward_pct(latest_price, _f(normalized_plan.get("tp2Price"), 0.0))
    rr_to_tp1 = _rr(reward_to_tp1_pct, risk_to_stop_pct)
    rr_to_tp2 = _rr(reward_to_tp2_pct, risk_to_stop_pct)
    current_vs_entry_pct = _pct_change(latest_price, _f(normalized_plan.get("averageEntryPrice"), 0.0))
    symbol = _s(row.get("symbol")).upper()
    portfolio_weight_pct = round(_f(executed_weights_pct.get(symbol), 0.0), 2)
    out = {
        **row,
        "rebalanceSelected": symbol in selected_symbols,
        "entryMode": _s(normalized_plan.get("entryMode")),
        "activationReason": _s(normalized_plan.get("activationReason")),
        "entryLevels": normalized_plan.get("entryLevels") or [],
        "averageEntryPrice": normalized_plan.get("averageEntryPrice"),
        "closeStopPrice": normalized_plan.get("closeStopPrice"),
        "hardStopPrice": normalized_plan.get("hardStopPrice"),
        "tp1Price": normalized_plan.get("tp1Price"),
        "tp2Price": normalized_plan.get("tp2Price"),
        "currentVsEntryPct": current_vs_entry_pct,
        "riskToStopPct": risk_to_stop_pct,
        "rewardToTp1Pct": reward_to_tp1_pct,
        "rewardToTp2Pct": reward_to_tp2_pct,
        "rrToTp1": rr_to_tp1,
        "rrToTp2": rr_to_tp2,
        "tradeVerdict": trade_verdict,
        "tradeReason": trade_reason,
        "portfolioWeightPct": portfolio_weight_pct,
        "warnings": warnings,
    }
    out["chartGateReasons"] = _chart_gate_reasons(out)
    return out


def _cache_is_fresh(path: Path, ttl_minutes: int) -> bool:
    if not path.exists():
        return False
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return datetime.now(timezone.utc) - modified <= timedelta(minutes=max(1, ttl_minutes))


def _cache_matches_limit(path: Path, expected_limit: int) -> bool:
    if not path.exists():
        return False
    try:
        payload = _load_json(path)
    except Exception:
        return False
    return int(_f(payload.get("newsAnalysisLimit"), _f(payload.get("analysisLimit"), 0.0))) == int(expected_limit)


def _load_trade_cache(analysis_limit: int, ttl_minutes: int) -> dict[str, Any] | None:
    for path in (_trade_cache_path(analysis_limit), CACHE_PATH):
        if not (_cache_is_fresh(path, ttl_minutes) and _cache_matches_limit(path, analysis_limit)):
            continue
        payload = _load_json(path)
        if _s(payload.get("schemaVersion")) == TRADE_CACHE_SCHEMA_VERSION:
            return payload
    return None


def _chart_cache_matches_schema(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        payload = _load_json(path)
    except Exception:
        return False
    return _s(payload.get("schemaVersion")) == CHART_SCHEMA_VERSION


def _load_cached_chart_rows(ttl_minutes: int) -> list[dict[str, Any]] | None:
    if not _cache_is_fresh(CHART_CACHE_PATH, ttl_minutes) or not _chart_cache_matches_schema(CHART_CACHE_PATH):
        return None
    payload = _load_json(CHART_CACHE_PATH)
    rows = payload.get("all") if isinstance(payload.get("all"), list) else []
    out = [row for row in rows if isinstance(row, dict)]
    return out or None


def _analyze_event_universe_legacy(force_refresh: bool = False, news_limit: int | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    ttl_minutes = _event_cache_minutes()
    analysis_limit = max(10, int(news_limit)) if news_limit is not None else _analysis_limit()
    timings: dict[str, Any] = {}
    if not force_refresh:
        cached = _load_trade_cache(analysis_limit, ttl_minutes)
        if cached is not None:
            return cached

    if not ai.has_api_access:
        payload = {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "available": False,
            "reason": "codex_login_required",
            "detail": "Codex CLI login is required and heuristic fallback is disabled.",
            "aiModel": ai.model,
            "aiReasoningEffort": ai.reasoning_effort,
            "newsAnalysisLimit": analysis_limit,
            "universeNewsScannedCount": 0,
            "newsCandidateCount": 0,
            "newsAnalyzedCount": 0,
            "chartScannedCount": 0,
            "actionableNow": [],
            "waitPullback": [],
            "avoid": [],
            "all": [],
            "summary": {
                "actionableCount": 0,
                "waitPullbackCount": 0,
                "avoidCount": 0,
                "cashPct": 100.0,
            },
            "timingsSec": {
                "total": round(time.perf_counter() - started, 3),
            },
        }
        _write_trade_cache(payload, analysis_limit)
        return payload

    rebalance_path = _latest_rebalance_result_path()
    rebalance = _load_json(rebalance_path) if rebalance_path is not None else {}
    candidates = [row for row in (rebalance.get("candidates") or []) if isinstance(row, dict)]
    rebalance_hints = {_s(row.get("symbol")).upper(): row for row in candidates if _s(row.get("symbol"))}
    selected_symbols = {_s(symbol).upper() for symbol in (rebalance.get("final_selected_symbols") or []) if _s(symbol)}
    execution_plans = rebalance.get("execution_plans") if isinstance(rebalance.get("execution_plans"), dict) else {}
    executed_weights_pct = rebalance.get("executed_weights_pct") if isinstance(rebalance.get("executed_weights_pct"), dict) else {}
    market_ctx = get_market_condition()
    fear_greed = get_fear_greed_index()

    news_started = time.perf_counter()
    discovery_symbols = _news_discovery_symbols(selected_symbols)
    discovery_bundles = _collect_news_bundles(discovery_symbols)
    news_symbols = _select_news_symbols_from_bundles(discovery_bundles, selected_symbols, analysis_limit)
    timings["newsCollectSec"] = round(time.perf_counter() - news_started, 3)
    timings["newsFirst"] = True

    timings["newsDiscoveryCount"] = len(discovery_symbols)
    timings["newsCandidateCount"] = len([bundle for bundle in discovery_bundles.values() if _bundle_has_news(bundle)])

    scan_started = time.perf_counter()
    cached_chart_rows = None if force_refresh else _load_cached_chart_rows(ttl_minutes)
    chart_cache_hit = cached_chart_rows is not None
    if cached_chart_rows is not None:
        cached_by_symbol = {_s(row.get("symbol")).upper(): row for row in cached_chart_rows if _s(row.get("symbol"))}
        scanned_rows = [cached_by_symbol[symbol] for symbol in news_symbols if symbol in cached_by_symbol]
        missing_symbols = [symbol for symbol in news_symbols if symbol not in cached_by_symbol]
        if missing_symbols:
            scanned_rows.extend(_scan_symbols(missing_symbols, rebalance_hints))
    else:
        scanned_rows = _scan_symbols(news_symbols, rebalance_hints)
    timings["chartCacheHit"] = chart_cache_hit
    timings["chartRowsSec"] = round(time.perf_counter() - scan_started, 3)

    chart_rows_by_symbol = {_s(row.get("symbol")).upper(): row for row in scanned_rows if _s(row.get("symbol"))}
    _attach_chart_rows_to_bundles(discovery_bundles, chart_rows_by_symbol)
    bundles = {symbol: discovery_bundles[symbol] for symbol in news_symbols if symbol in discovery_bundles and symbol in chart_rows_by_symbol}
    news_symbols = [symbol for symbol in news_symbols if symbol in bundles]
    ai_started = time.perf_counter()
    news_analysis = _batched_ai_news_analysis(bundles)
    timings["codexAnalysisSec"] = round(time.perf_counter() - ai_started, 3)
    if isinstance(news_analysis, dict) and news_analysis.get("error"):
        payload = {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "available": False,
            "reason": "codex_event_analysis_failed",
            "detail": f"{_s(news_analysis.get('error'))} | model={_s(news_analysis.get('model') or ai.model)} | reasoning={_s(news_analysis.get('reasoningEffort') or ai.reasoning_effort)}",
            "aiModel": ai.model,
            "aiReasoningEffort": ai.reasoning_effort,
            "newsAnalysisLimit": analysis_limit,
            "universeNewsScannedCount": len(discovery_symbols),
            "newsCandidateCount": int(timings.get("newsCandidateCount", 0)),
            "newsAnalyzedCount": len(news_symbols),
            "chartScannedCount": len(scanned_rows),
            "actionableNow": [],
            "waitPullback": [],
            "avoid": [],
            "all": [],
            "summary": {
                "actionableCount": 0,
                "waitPullbackCount": 0,
                "avoidCount": 0,
                "cashPct": 100.0,
            },
            "timingsSec": {
                **timings,
                "total": round(time.perf_counter() - started, 3),
            },
        }
        _write_trade_cache(payload, analysis_limit)
        return payload

    eval_started = time.perf_counter()
    evaluated: list[dict[str, Any]] = []
    for symbol in news_symbols:
        row = chart_rows_by_symbol.get(symbol)
        if not isinstance(row, dict):
            continue
        news = news_analysis.get(symbol)
        if not isinstance(news, dict):
            payload = {
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "available": False,
                "reason": "codex_event_analysis_missing_symbol",
                "detail": symbol,
                "aiModel": ai.model,
                "aiReasoningEffort": ai.reasoning_effort,
                "newsAnalysisLimit": analysis_limit,
                "universeNewsScannedCount": len(discovery_symbols),
                "newsCandidateCount": int(timings.get("newsCandidateCount", 0)),
                "newsAnalyzedCount": len(news_symbols),
                "chartScannedCount": len(scanned_rows),
                "actionableNow": [],
                "waitPullback": [],
                "avoid": [],
                "all": [],
                "summary": {
                    "actionableCount": 0,
                    "waitPullbackCount": 0,
                    "avoidCount": 0,
                    "cashPct": 100.0,
                },
                "timingsSec": {
                    **timings,
                    "total": round(time.perf_counter() - started, 3),
                },
            }
            _write_trade_cache(payload, analysis_limit)
            return payload

        bundle = bundles.get(symbol, {})
        info = bundle.get("info") if isinstance(bundle.get("info"), dict) else {}
        raw_events = bundle.get("events") if isinstance(bundle.get("events"), list) else []
        next_events = bundle.get("nextEvents") if isinstance(bundle.get("nextEvents"), list) else []

        plan = execution_plans.get(symbol) if isinstance(execution_plans.get(symbol), dict) else None
        if plan:
            normalized_plan = {
                "entryMode": _s((plan.get("entry") or {}).get("mode")),
                "activationReason": _s((plan.get("entry") or {}).get("activation_reason")),
                "entryLevels": [
                    {
                        "name": _s(level.get("name")),
                        "price": round(_f(level.get("price"), 0.0), 2),
                        "splitPct": round(_f(level.get("split_pct"), 0.0), 2),
                    }
                    for level in ((plan.get("entry") or {}).get("levels") or [])
                    if isinstance(level, dict)
                ],
                "averageEntryPrice": round(_f((plan.get("entry") or {}).get("average_entry_price"), 0.0), 2),
                "closeStopPrice": round(_f((plan.get("stop") or {}).get("close_stop_price"), 0.0), 2),
                "hardStopPrice": round(_f((plan.get("stop") or {}).get("hard_stop_price"), 0.0), 2),
                "tp1Price": round(_f((plan.get("targets") or {}).get("tp1_price"), 0.0), 2),
                "tp2Price": round(_f((plan.get("targets") or {}).get("tp2_price"), 0.0), 2),
                "supportUsed": round(_f((plan.get("anchors") or {}).get("support_used"), 0.0), 2),
                "resistanceUsed": round(_f((plan.get("anchors") or {}).get("resistance_used"), 0.0), 2),
                "supportDistanceAtr": round(_f((plan.get("anchors") or {}).get("support_distance_atr"), 0.0), 2),
            }
        else:
            normalized_plan = _build_execution_plan(
                {
                    **row,
                    "atr": row.get("atr"),
                    "support": row.get("support"),
                    "resistance": row.get("resistance"),
                    "volume_ratio": row.get("volumeRatio"),
                },
                latest_price=_f(row.get("latestClosePrice"), 0.0),
            )

        latest_price = round(_f(row.get("latestClosePrice"), _f(info.get("price"), 0.0)), 2)
        warnings = [str(item) for item in (row.get("warnings") or [])]
        trade_verdict, trade_reason = _trade_verdict(normalized_plan, latest_price, warnings)
        risk_to_stop_pct = _risk_pct(latest_price, _f(normalized_plan.get("closeStopPrice"), 0.0))
        reward_to_tp1_pct = _reward_pct(latest_price, _f(normalized_plan.get("tp1Price"), 0.0))
        reward_to_tp2_pct = _reward_pct(latest_price, _f(normalized_plan.get("tp2Price"), 0.0))
        rr_to_tp1 = _rr(reward_to_tp1_pct, risk_to_stop_pct)
        rr_to_tp2 = _rr(reward_to_tp2_pct, risk_to_stop_pct)
        evaluated_row = {
            "symbol": symbol,
            "name": _s(info.get("name") or symbol),
            "sector": _s(info.get("sector") or rebalance_hints.get(symbol, {}).get("sector")),
            "rebalanceSelected": symbol in selected_symbols,
            "existingPortfolioWeightPct": round(_f(executed_weights_pct.get(symbol), 0.0), 2),
            "latestClosePrice": latest_price,
            "latestCloseAsOf": _s(row.get("latestCloseAsOf")),
            "previousClosePrice": row.get("previousClosePrice"),
            "latestOpenPrice": row.get("latestOpenPrice"),
            "latestHighPrice": row.get("latestHighPrice"),
            "latestLowPrice": row.get("latestLowPrice"),
            "dayReturnPct": row.get("dayReturnPct"),
            "gapPct": row.get("gapPct"),
            "intradayReturnPct": row.get("intradayReturnPct"),
            "dayRangePct": row.get("dayRangePct"),
            "closeLocationPct": row.get("closeLocationPct"),
            "eventMoveAtr": row.get("eventMoveAtr"),
            "gapAtr": row.get("gapAtr"),
            "dailyVolumeRatio": row.get("dailyVolumeRatio"),
            "chartState": _s(row.get("chartState")),
            "volumeRatio": round(_f(row.get("volumeRatio"), 0.0), 2),
            "rsi": round(_f(row.get("rsi"), 0.0), 1),
            "adx": round(_f(row.get("adx"), 0.0), 1),
            "atr": row.get("atr"),
            "atrPct": row.get("atrPct"),
            "newsSignal": _s(news.get("signal")),
            "newsStrength": _s(news.get("strength")),
            "newsHeadline": _s(news.get("headline")),
            "newsReasons": [str(item) for item in (news.get("rationale") or [])[:4]],
            "newsMode": _s(news.get("mode")),
            "newsEventCount": len(raw_events),
            "nextEventCount": len(next_events),
            "eventHeadlines": [
                " | ".join(
                    part
                    for part in (
                        _s(event.get("published_at"))[:10],
                        _s(event.get("source")),
                        _s(event.get("headline")),
                    )
                    if part
                )
                for event in raw_events[:5]
                if isinstance(event, dict)
            ],
            "nextEvents": next_events[:4],
            "entryMode": _s(normalized_plan.get("entryMode")),
            "activationReason": _s(normalized_plan.get("activationReason")),
            "entryLevels": normalized_plan.get("entryLevels") or [],
            "averageEntryPrice": normalized_plan.get("averageEntryPrice"),
            "closeStopPrice": normalized_plan.get("closeStopPrice"),
            "hardStopPrice": normalized_plan.get("hardStopPrice"),
            "tp1Price": normalized_plan.get("tp1Price"),
            "tp2Price": normalized_plan.get("tp2Price"),
            "currentVsEntryPct": _pct_change(latest_price, _f(normalized_plan.get("averageEntryPrice"), 0.0)),
            "riskToStopPct": risk_to_stop_pct,
            "rewardToTp1Pct": reward_to_tp1_pct,
            "rewardToTp2Pct": reward_to_tp2_pct,
            "rrToTp1": rr_to_tp1,
            "rrToTp2": rr_to_tp2,
            "tradeVerdict": trade_verdict,
            "tradeReason": trade_reason,
            "warnings": warnings,
        }
        evaluated_row.update(_strategy_decision(evaluated_row, news, raw_events, next_events))
        evaluated.append(evaluated_row)
    timings["evaluateSec"] = round(time.perf_counter() - eval_started, 3)

    synth_started = time.perf_counter()
    _apply_actionable_cap(evaluated)
    evaluated.sort(key=_strategy_rank_key)
    for idx, row in enumerate(evaluated, start=1):
        row["finalRank"] = idx
    final_synthesis = _strategy_synthesis_payload(evaluated)
    timings["finalSynthesisSec"] = round(time.perf_counter() - synth_started, 3)
    actionable = [row for row in evaluated if _s(row.get("actionBucket")) == "actionable_now"]
    wait_pullback = [row for row in evaluated if _s(row.get("actionBucket")) == "wait_pullback"]
    avoid = [row for row in evaluated if _s(row.get("actionBucket")) == "avoid"]
    cash_pct = round(max(0.0, 100.0 - sum(_f(row.get("portfolioWeightPct"), 0.0) for row in actionable)), 2)
    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "available": True,
        "schemaVersion": TRADE_CACHE_SCHEMA_VERSION,
        "aiModel": ai.model,
        "aiReasoningEffort": ai.reasoning_effort,
        "rebalanceSourceFile": str(rebalance_path) if rebalance_path is not None else "",
        "rebalanceGeneratedAt": _s(rebalance.get("generated_at")),
        "marketStatus": {
            "marketCondition": market_ctx,
            "fearGreed": fear_greed,
        },
        "newsAnalysisLimit": analysis_limit,
        "universeScannedCount": len(discovery_symbols),
        "universeNewsScannedCount": len(discovery_symbols),
        "newsCandidateCount": int(timings.get("newsCandidateCount", 0)),
        "chartScannedCount": len(scanned_rows),
        "newsAnalyzedCount": len(news_symbols),
        "selectedCount": len(selected_symbols),
        "actionableNow": actionable,
        "waitPullback": wait_pullback,
        "avoid": avoid,
        "all": evaluated,
        "chartLeaders": scanned_rows[:20],
        "finalSynthesis": final_synthesis,
        "summary": {
            "actionableCount": len(actionable),
            "waitPullbackCount": len(wait_pullback),
            "avoidCount": len(avoid),
            "cashPct": cash_pct,
            "topActionableSymbol": _s(actionable[0].get("symbol")) if actionable else "",
        },
        "timingsSec": {
            **timings,
            "total": round(time.perf_counter() - started, 3),
        },
    }
    _write_trade_cache(payload, analysis_limit)
    return payload


def analyze_rebalance_universe(force_refresh: bool = False, news_limit: int | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    ttl_minutes = _event_cache_minutes()
    analysis_limit = max(10, int(news_limit)) if news_limit is not None else _analysis_limit()
    timings: dict[str, Any] = {}
    if not force_refresh:
        cached = _load_trade_cache(analysis_limit, ttl_minutes)
        if cached is not None:
            return cached

    rebalance_path = _latest_rebalance_result_path()
    rebalance = _load_json(rebalance_path) if rebalance_path is not None else {}
    selected_symbols = {_s(symbol).upper() for symbol in (rebalance.get("final_selected_symbols") or []) if _s(symbol)}
    executed_weights_pct = rebalance.get("executed_weights_pct") if isinstance(rebalance.get("executed_weights_pct"), dict) else {}
    market_ctx = get_market_condition()
    fear_greed = get_fear_greed_index()

    universe_symbols = sorted(set(_load_all_us_symbols()) | selected_symbols)
    fundamental_started = time.perf_counter()
    fundamental_rows = _scan_fundamentals(universe_symbols)
    benchmarks = _valuation_benchmarks(fundamental_rows)
    preliminary: list[dict[str, Any]] = []
    for row in fundamental_rows:
        assessed = {**row}
        assessed.update(_value_decision(assessed, benchmarks, None))
        preliminary.append(assessed)
    preliminary.sort(key=_value_rank_key)
    candidate_rows = [row for row in preliminary if _s(row.get("actionBucket")) != "avoid"][:analysis_limit]
    if len(candidate_rows) < analysis_limit:
        seen = {_s(row.get("symbol")) for row in candidate_rows}
        for row in preliminary:
            symbol = _s(row.get("symbol"))
            if symbol in seen:
                continue
            candidate_rows.append(row)
            seen.add(symbol)
            if len(candidate_rows) >= analysis_limit:
                break
    candidate_symbols = [_s(row.get("symbol")) for row in candidate_rows if _s(row.get("symbol"))]
    timings["fundamentalScanSec"] = round(time.perf_counter() - fundamental_started, 3)
    timings["valueFirst"] = True

    news_started = time.perf_counter()
    bundles = _collect_news_bundles(candidate_symbols)
    timings["newsCollectSec"] = round(time.perf_counter() - news_started, 3)
    timings["newsCandidateCount"] = len([bundle for bundle in bundles.values() if _bundle_has_news(bundle)])

    news_analysis: dict[str, Any] = {}
    ai_started = time.perf_counter()
    if ai.has_api_access and bundles:
        analyzed = _batched_ai_news_analysis(bundles)
        if isinstance(analyzed, dict) and analyzed.get("error"):
            payload = {
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "available": False,
                "reason": "codex_event_analysis_failed",
                "detail": f"{_s(analyzed.get('error'))} | model={_s(analyzed.get('model') or ai.model)} | reasoning={_s(analyzed.get('reasoningEffort') or ai.reasoning_effort)}",
                "aiModel": ai.model,
                "aiReasoningEffort": ai.reasoning_effort,
                "newsAnalysisLimit": analysis_limit,
                "universeScannedCount": len(fundamental_rows),
                "actionableNow": [],
                "waitPullback": [],
                "avoid": [],
                "all": [],
                "summary": {
                    "actionableCount": 0,
                    "waitPullbackCount": 0,
                    "avoidCount": 0,
                    "cashPct": 100.0,
                },
                "timingsSec": {
                    **timings,
                    "total": round(time.perf_counter() - started, 3),
                },
            }
            _write_trade_cache(payload, analysis_limit)
            return payload
        news_analysis = analyzed
    timings["codexAnalysisSec"] = round(time.perf_counter() - ai_started, 3)

    scan_started = time.perf_counter()
    cached_chart_rows = None if force_refresh else _load_cached_chart_rows(ttl_minutes)
    chart_cache_hit = cached_chart_rows is not None
    if cached_chart_rows is not None:
        cached_by_symbol = {_s(row.get("symbol")).upper(): row for row in cached_chart_rows if _s(row.get("symbol"))}
        scanned_rows = [cached_by_symbol[symbol] for symbol in candidate_symbols if symbol in cached_by_symbol]
        missing_symbols = [symbol for symbol in candidate_symbols if symbol not in cached_by_symbol]
        if missing_symbols:
            scanned_rows.extend(_scan_symbols(missing_symbols, {}))
    else:
        scanned_rows = _scan_symbols(candidate_symbols, {})
    timings["chartCacheHit"] = chart_cache_hit
    timings["chartRowsSec"] = round(time.perf_counter() - scan_started, 3)

    chart_rows_by_symbol = {_s(row.get("symbol")).upper(): row for row in scanned_rows if _s(row.get("symbol"))}
    fundamental_by_symbol = {_s(row.get("symbol")).upper(): row for row in fundamental_rows if _s(row.get("symbol"))}

    eval_started = time.perf_counter()
    evaluated: list[dict[str, Any]] = []
    for symbol in candidate_symbols:
        fundamental = fundamental_by_symbol.get(symbol)
        if not isinstance(fundamental, dict):
            continue
        row = chart_rows_by_symbol.get(symbol, {})
        latest_price = round(_f(row.get("latestClosePrice"), _f(fundamental.get("latestClosePrice"), 0.0)), 2)
        normalized_plan = _build_execution_plan(
            {
                **row,
                "support": row.get("support"),
                "resistance": row.get("resistance"),
            },
            latest_price=latest_price,
        )
        warnings = [str(item) for item in (row.get("warnings") or [])]
        trade_verdict, trade_reason = _trade_verdict(normalized_plan, latest_price, warnings)
        risk_to_stop_pct = _risk_pct(latest_price, _f(normalized_plan.get("closeStopPrice"), 0.0))
        reward_to_tp1_pct = _reward_pct(latest_price, _f(normalized_plan.get("tp1Price"), 0.0))
        reward_to_tp2_pct = _reward_pct(latest_price, _f(normalized_plan.get("tp2Price"), 0.0))
        rr_to_tp1 = _rr(reward_to_tp1_pct, risk_to_stop_pct)
        rr_to_tp2 = _rr(reward_to_tp2_pct, risk_to_stop_pct)
        bundle = bundles.get(symbol, {})
        raw_events = bundle.get("events") if isinstance(bundle.get("events"), list) else []
        next_events = bundle.get("nextEvents") if isinstance(bundle.get("nextEvents"), list) else []
        news = news_analysis.get(symbol) if isinstance(news_analysis.get(symbol), dict) else {}
        evaluated_row = {
            **fundamental,
            "rebalanceSelected": symbol in selected_symbols,
            "existingPortfolioWeightPct": round(_f(executed_weights_pct.get(symbol), 0.0), 2),
            "latestClosePrice": latest_price,
            "latestCloseAsOf": _s(row.get("latestCloseAsOf")),
            "previousClosePrice": row.get("previousClosePrice"),
            "latestOpenPrice": row.get("latestOpenPrice"),
            "latestHighPrice": row.get("latestHighPrice"),
            "latestLowPrice": row.get("latestLowPrice"),
            "dayReturnPct": row.get("dayReturnPct"),
            "gapPct": row.get("gapPct"),
            "intradayReturnPct": row.get("intradayReturnPct"),
            "dayRangePct": row.get("dayRangePct"),
            "closeLocationPct": row.get("closeLocationPct"),
            "chartState": _s(row.get("chartState")),
            "volumeRatio": round(_f(row.get("volumeRatio"), 0.0), 2),
            "rsi": round(_f(row.get("rsi"), 0.0), 1),
            "adx": round(_f(row.get("adx"), 0.0), 1),
            "newsSignal": _s(news.get("signal")),
            "newsStrength": _s(news.get("strength")),
            "newsHeadline": _s(news.get("headline")),
            "newsReasons": [str(item) for item in (news.get("rationale") or [])[:4]] if isinstance(news.get("rationale"), list) else [],
            "newsMode": _s(news.get("mode")),
            "newsEventCount": len(raw_events),
            "nextEventCount": len(next_events),
            "eventHeadlines": [
                " | ".join(
                    part
                    for part in (
                        _s(event.get("published_at"))[:10],
                        _s(event.get("source")),
                        _s(event.get("headline")),
                    )
                    if part
                )
                for event in raw_events[:5]
                if isinstance(event, dict)
            ],
            "nextEvents": next_events[:4],
            "entryMode": _s(normalized_plan.get("entryMode")),
            "activationReason": _s(normalized_plan.get("activationReason")),
            "entryLevels": normalized_plan.get("entryLevels") or [],
            "averageEntryPrice": normalized_plan.get("averageEntryPrice"),
            "closeStopPrice": normalized_plan.get("closeStopPrice"),
            "hardStopPrice": normalized_plan.get("hardStopPrice"),
            "tp1Price": normalized_plan.get("tp1Price"),
            "tp2Price": normalized_plan.get("tp2Price"),
            "currentVsEntryPct": _pct_change(latest_price, _f(normalized_plan.get("averageEntryPrice"), 0.0)),
            "riskToStopPct": risk_to_stop_pct,
            "rewardToTp1Pct": reward_to_tp1_pct,
            "rewardToTp2Pct": reward_to_tp2_pct,
            "rrToTp1": rr_to_tp1,
            "rrToTp2": rr_to_tp2,
            "tradeVerdict": trade_verdict,
            "tradeReason": trade_reason,
            "warnings": warnings,
        }
        evaluated_row.update(_value_decision(evaluated_row, benchmarks, news))
        evaluated.append(evaluated_row)
    timings["evaluateSec"] = round(time.perf_counter() - eval_started, 3)

    synth_started = time.perf_counter()
    _apply_actionable_cap(evaluated)
    evaluated.sort(key=_value_rank_key)
    for idx, row in enumerate(evaluated, start=1):
        row["finalRank"] = idx
    final_synthesis = _value_synthesis_payload(evaluated, len(fundamental_rows))
    timings["finalSynthesisSec"] = round(time.perf_counter() - synth_started, 3)

    actionable = [row for row in evaluated if _s(row.get("actionBucket")) == "actionable_now"]
    wait_pullback = [row for row in evaluated if _s(row.get("actionBucket")) == "wait_pullback"]
    avoid = [row for row in evaluated if _s(row.get("actionBucket")) == "avoid"]
    cash_pct = round(max(0.0, 100.0 - sum(_f(row.get("portfolioWeightPct"), 0.0) for row in actionable)), 2)
    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "available": True,
        "schemaVersion": TRADE_CACHE_SCHEMA_VERSION,
        "analysisMode": "valuation-first",
        "aiModel": ai.model,
        "aiReasoningEffort": ai.reasoning_effort,
        "rebalanceSourceFile": str(rebalance_path) if rebalance_path is not None else "",
        "rebalanceGeneratedAt": _s(rebalance.get("generated_at")),
        "marketStatus": {
            "marketCondition": market_ctx,
            "fearGreed": fear_greed,
        },
        "newsAnalysisLimit": analysis_limit,
        "universeSymbolCount": len(universe_symbols),
        "universeScannedCount": len(fundamental_rows),
        "valueCandidateCount": len(candidate_symbols),
        "universeNewsScannedCount": len(candidate_symbols),
        "newsCandidateCount": int(timings.get("newsCandidateCount", 0)),
        "chartScannedCount": len(scanned_rows),
        "newsAnalyzedCount": len(news_analysis),
        "selectedCount": len(selected_symbols),
        "actionableNow": actionable,
        "waitPullback": wait_pullback,
        "avoid": avoid,
        "all": evaluated,
        "chartLeaders": scanned_rows[:20],
        "finalSynthesis": final_synthesis,
        "summary": {
            "actionableCount": len(actionable),
            "waitPullbackCount": len(wait_pullback),
            "avoidCount": len(avoid),
            "cashPct": cash_pct,
            "topActionableSymbol": _s(actionable[0].get("symbol")) if actionable else "",
        },
        "timingsSec": {
            **timings,
            "total": round(time.perf_counter() - started, 3),
        },
    }
    _write_trade_cache(payload, analysis_limit)
    return payload


def analyze_current_charts(force_refresh: bool = False) -> dict[str, Any]:
    started = time.perf_counter()
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    if (
        not force_refresh
        and _cache_is_fresh(CHART_CACHE_PATH, _event_cache_minutes())
        and _chart_cache_matches_schema(CHART_CACHE_PATH)
    ):
        return _load_json(CHART_CACHE_PATH)

    rebalance_path = _latest_rebalance_result_path()
    rebalance = _load_json(rebalance_path) if rebalance_path is not None else {}
    candidates = [row for row in (rebalance.get("candidates") or []) if isinstance(row, dict)]
    rebalance_hints = {_s(row.get("symbol")).upper(): row for row in candidates if _s(row.get("symbol"))}
    selected_symbols = {_s(symbol).upper() for symbol in (rebalance.get("final_selected_symbols") or []) if _s(symbol)}
    executed_weights_pct = rebalance.get("executed_weights_pct") if isinstance(rebalance.get("executed_weights_pct"), dict) else {}

    scan_started = time.perf_counter()
    scanned_rows = _scan_full_universe(rebalance_hints)
    scan_sec = round(time.perf_counter() - scan_started, 3)
    eval_started = time.perf_counter()
    evaluated = [
        row
        for row in (
            _evaluate_chart_row(
                row,
                executed_weights_pct=executed_weights_pct,
                selected_symbols=selected_symbols,
            )
            for row in scanned_rows
        )
        if isinstance(row, dict)
    ]
    eval_sec = round(time.perf_counter() - eval_started, 3)

    price_ready: list[dict[str, Any]] = []
    strict_buyable: list[dict[str, Any]] = []
    buyable = sorted([row for row in evaluated if _chart_buyable(row)], key=_chart_buyable_key)
    wait_pullback = sorted([row for row in evaluated if _chart_wait(row)], key=_chart_wait_key)
    leaders = sorted(evaluated, key=_chart_leader_key)
    overextended = [
        row
        for row in leaders
        if bool({"entry_negative", "overheat_extreme", "overheat_dual"} & {str(item) for item in (row.get("warnings") or [])})
    ]
    avoid = [row for row in evaluated if _s(row.get("tradeVerdict")) == "avoid"]
    latest_dates = [_s(row.get("latestCloseAsOf")) for row in evaluated if _s(row.get("latestCloseAsOf"))]
    cash_pct = 100.0

    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "available": True,
        "mode": "chart-only",
        "chartOnlyPolicy": "chart_is_veto_not_buy_signal",
        "schemaVersion": CHART_SCHEMA_VERSION,
        "rebalanceSourceFile": str(rebalance_path) if rebalance_path is not None else "",
        "rebalanceGeneratedAt": _s(rebalance.get("generated_at")),
        "universeSymbolCount": len(_load_all_us_symbols()),
        "universeScannedCount": len(scanned_rows),
        "latestCloseMin": min(latest_dates) if latest_dates else "",
        "latestCloseMax": max(latest_dates) if latest_dates else "",
        "strictBuyable": strict_buyable,
        "actionableNow": strict_buyable,
        "priceReady": price_ready,
        "buyable": buyable,
        "waitPullback": wait_pullback,
        "avoid": avoid,
        "chartLeaders": leaders[:30],
        "overextended": overextended[:30],
        "all": evaluated,
        "summary": {
            "strictBuyableCount": len(strict_buyable),
            "actionableCount": len(strict_buyable),
            "priceReadyCount": len(price_ready),
            "buyableCount": len(buyable),
            "waitPullbackCount": len(wait_pullback),
            "avoidCount": len(avoid),
            "overextendedCount": len(overextended),
            "cashPct": cash_pct,
            "topActionableSymbol": _s(strict_buyable[0].get("symbol")) if strict_buyable else "",
        },
        "timingsSec": {
            "chartScanSec": scan_sec,
            "evaluateSec": eval_sec,
            "total": round(time.perf_counter() - started, 3),
        },
    }
    _write_json(CHART_CACHE_PATH, payload)
    return payload


def _format_pct(value: Any) -> str:
    if value is None:
        return "-"
    return f"{_f(value):.2f}%"


def _format_price(value: Any) -> str:
    if value is None:
        return "-"
    return f"{_f(value):.2f}"


def _format_multiple(value: Any) -> str:
    if value is None:
        return "-"
    return f"{_f(value):.2f}x"


def _format_market_cap(value: Any) -> str:
    raw = _f(value, 0.0)
    if raw <= 0:
        return "-"
    if raw >= 1_000_000_000_000:
        return f"{raw / 1_000_000_000_000:.2f}T"
    if raw >= 1_000_000_000:
        return f"{raw / 1_000_000_000:.1f}B"
    return f"{raw / 1_000_000:.1f}M"


def _bucket_rows(payload: dict[str, Any], bucket: str) -> list[dict[str, Any]]:
    key_map = {
        "actionable": "actionableNow",
        "wait": "waitPullback",
        "avoid": "avoid",
    }
    rows = payload.get(key_map.get(bucket, "")) if isinstance(payload, dict) else []
    return rows if isinstance(rows, list) else []


def _fmt_asof(value: Any) -> str:
    raw = _s(value)
    if not raw:
        return "-"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return raw


def _timing_line(payload: dict[str, Any]) -> str:
    timings = payload.get("timingsSec") if isinstance(payload.get("timingsSec"), dict) else {}
    if not timings:
        return ""
    total = timings.get("total")
    if total is None:
        return ""
    cache_note = " | 차트캐시 hit" if timings.get("chartCacheHit") is True else ""
    return f"처리 {_f(total):.2f}s{cache_note}"


def _one_line_row(row: dict[str, Any]) -> str:
    symbol = escape(_s(row.get("symbol")))
    price = _format_price(row.get("latestClosePrice"))
    if _s(row.get("valueState")):
        p_fcf = _format_multiple(row.get("pFcf"))
        fpe = _format_multiple(row.get("forwardPe"))
        fcf_yield = _format_pct(row.get("fcfYieldPct"))
        roe = _format_pct(row.get("roePct"))
        weight = _format_pct(row.get("portfolioWeightPct"))
        return f"<b>{symbol}</b>  {price} | P/FCF {p_fcf} | Fwd PER {fpe} | FCF yield {fcf_yield} | ROE {roe} | 비중 {weight}"
    entry = _format_price(row.get("averageEntryPrice"))
    stop = _format_price(row.get("closeStopPrice"))
    tp1 = _format_price(row.get("tp1Price"))
    weight = _format_pct(row.get("portfolioWeightPct"))
    return f"<b>{symbol}</b>  {price} | 진입 {entry} | 손절 {stop} | 1차 {tp1} | 비중 {weight}"


def _compact_row(row: dict[str, Any]) -> str:
    symbol = escape(_s(row.get("symbol")))
    price = _format_price(row.get("latestClosePrice"))
    if _s(row.get("valueState")):
        return (
            f"<b>{symbol}</b>  {price} | P/FCF {_format_multiple(row.get('pFcf'))} | "
            f"Fwd PER {_format_multiple(row.get('forwardPe'))} | FCF {_format_pct(row.get('fcfYieldPct'))}"
        )
    entry = _format_price(row.get("averageEntryPrice"))
    tp1 = _format_price(row.get("tp1Price"))
    return f"<b>{symbol}</b>  {price} | 진입 {entry} | 1차 {tp1}"


def _decision_line(row: dict[str, Any]) -> str:
    state = escape(_s(row.get("decisionState")))
    if _s(row.get("valueState")):
        value = escape(_s(row.get("valueState")))
        quality = escape(_s(row.get("qualityState")))
        growth = escape(_s(row.get("growthState")))
        balance = escape(_s(row.get("balanceState")))
        return f"{state} | 가치 {value} | 품질 {quality} | 성장 {growth} | 재무 {balance}"
    catalyst = escape(_s(row.get("catalystState")))
    reaction = escape(_s(row.get("marketReactionState")))
    entry = escape(_s(row.get("entryStructureState")))
    snapshot = escape(_s(row.get("reactionSnapshot")))
    return f"{state} | 촉매 {catalyst} | 반응 {reaction} | 진입 {entry}" + (f" | {snapshot}" if snapshot else "")


def _decision_reason_text(row: dict[str, Any]) -> str:
    reasons = row.get("decisionReasons") if isinstance(row.get("decisionReasons"), list) else []
    if reasons:
        return " / ".join(_s(item) for item in reasons[:4] if _s(item))
    return _s(row.get("actionReason") or row.get("newsHeadline") or row.get("tradeReason"))


def render_trade_view_html(payload: dict[str, Any], view: str = "summary") -> str:
    if not bool(payload.get("available")):
        return (
            f"<b>분석 불가</b>\n"
            f"이유: <code>{escape(_s(payload.get('reason') or 'unknown'))}</code>\n"
            f"{escape(_s(payload.get('detail') or 'Codex 기반 뉴스 해석이 필요합니다.'))}"
        )

    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    final_synthesis = payload.get("finalSynthesis") if isinstance(payload.get("finalSynthesis"), dict) else {}
    market_condition = ((payload.get("marketStatus") or {}).get("marketCondition") or {}) if isinstance(payload.get("marketStatus"), dict) else {}
    fear_greed = ((payload.get("marketStatus") or {}).get("fearGreed") or {}) if isinstance(payload.get("marketStatus"), dict) else {}
    actionable = _bucket_rows(payload, "actionable")
    wait_pullback = _bucket_rows(payload, "wait")
    avoid = _bucket_rows(payload, "avoid")

    header = [
        "<b>Autostock Value Desk</b>" if _s(payload.get("analysisMode")) == "valuation-first" else "<b>Autostock Trade Desk</b>",
        f"<code>{escape(_fmt_asof(payload.get('generatedAt')))}</code>",
        f"시장 {_s(market_condition.get('message'))} | 공포탐욕 {fear_greed.get('score', '-')}",
        (
            f"가치 스캔 {payload.get('universeScannedCount', '-')} | 후보 {payload.get('valueCandidateCount', '-')} | 뉴스 리스크 {payload.get('newsAnalyzedCount', '-')}"
            if _s(payload.get("analysisMode")) == "valuation-first"
            else f"뉴스 탐색 {payload.get('universeNewsScannedCount', payload.get('universeScannedCount', '-'))} | 촉매후보 {payload.get('newsCandidateCount', '-')} | Codex {payload.get('newsAnalyzedCount', '-')}"
        ),
        f"가격 검증 {payload.get('chartScannedCount', payload.get('universeScannedCount', '-'))}",
        f"모델 {escape(_s(payload.get('aiModel') or ai.model))} / {escape(_s(payload.get('aiReasoningEffort') or ai.reasoning_effort))}",
        f"할인후보 {summary.get('actionableCount', 0)} | 관찰 {summary.get('waitPullbackCount', 0)} | 제외 {summary.get('avoidCount', 0)} | 현금 {_format_pct(summary.get('cashPct'))}",
    ]
    timing = _timing_line(payload)
    if timing:
        header.append(timing)
    header.append("")

    if view == "summary":
        lines = header[:]
        if _s(final_synthesis.get("summary")):
            lines.extend(["<b>최종 종합</b>", escape(_s(final_synthesis.get("summary"))), ""])
        lines.append("<b>가치 할인 후보</b>" if _s(payload.get("analysisMode")) == "valuation-first" else "<b>지금 진입 가능</b>")
        if actionable:
            lines.extend(_compact_row(row) for row in actionable[:3])
        else:
            lines.append("없음")
        lines.append("")
        lines.append("<b>가치 관찰</b>" if _s(payload.get("analysisMode")) == "valuation-first" else "<b>눌림 대기</b>")
        if wait_pullback:
            lines.extend(_compact_row(row) for row in wait_pullback[:5])
        else:
            lines.append("없음")
        lines.append("")
        lines.append("<b>제외</b>" if _s(payload.get("analysisMode")) == "valuation-first" else "<b>추격 금지</b>")
        if avoid:
            lines.extend(f"{escape(_s(row.get('symbol')))}  {_s(row.get('actionReason') or row.get('tradeReason'))}" for row in avoid[:3])
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "actionable":
        lines = header + ["<b>가치 할인 후보</b>" if _s(payload.get("analysisMode")) == "valuation-first" else "<b>즉시 매수 후보</b>"]
        if actionable:
            for row in actionable[:8]:
                lines.append(_one_line_row(row))
                lines.append(_decision_line(row))
                lines.append(escape(_decision_reason_text(row)))
                lines.append("")
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "wait":
        lines = header + ["<b>가치 관찰 후보</b>" if _s(payload.get("analysisMode")) == "valuation-first" else "<b>눌림 대기 후보</b>"]
        if wait_pullback:
            for row in wait_pullback[:10]:
                lines.append(_one_line_row(row))
                lines.append(_decision_line(row))
                lines.append(escape(_decision_reason_text(row)))
                lines.append("")
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "avoid":
        lines = header + ["<b>지금 제외</b>"]
        if avoid:
            for row in avoid[:10]:
                label = f"{escape(_s(row.get('valueState')))}" if _s(row.get("valueState")) else f"{escape(_s(row.get('newsSignal')))} / {escape(_s(row.get('newsStrength')))}"
                lines.append(f"<b>{escape(_s(row.get('symbol')))}</b> | {label}")
                lines.append(_decision_line(row))
                lines.append(escape(_decision_reason_text(row)))
                lines.append("")
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "portfolio":
        lines = header + ["<b>추천 포트폴리오</b>", f"현금 {_format_pct(summary.get('cashPct'))}"]
        if actionable:
            for row in actionable[:10]:
                if _s(row.get("valueState")):
                    lines.append(
                        f"{escape(_s(row.get('symbol')))}  {_format_pct(row.get('portfolioWeightPct'))} | "
                        f"P/FCF {_format_multiple(row.get('pFcf'))} | Fwd PER {_format_multiple(row.get('forwardPe'))} | FCF yield {_format_pct(row.get('fcfYieldPct'))}"
                    )
                else:
                    lines.append(
                        f"{escape(_s(row.get('symbol')))}  {_format_pct(row.get('portfolioWeightPct'))}  | 진입 {_format_price(row.get('averageEntryPrice'))} | 손절 {_format_price(row.get('closeStopPrice'))}"
                    )
                if _s(row.get("actionReason")):
                    lines.append(f"이유: {escape(_s(row.get('actionReason')))}")
        else:
            lines.append("즉시 편입 후보가 없어 현금 대기가 기본입니다.")
            for row in wait_pullback[:5]:
                if _s(row.get("valueState")):
                    lines.append(f"{escape(_s(row.get('symbol')))}  관찰 | {escape(_s(row.get('actionReason')))}")
                else:
                    lines.append(
                        f"{escape(_s(row.get('symbol')))}  대기  | 진입 {_format_price(row.get('averageEntryPrice'))} | 손절 {_format_price(row.get('closeStopPrice'))}"
                    )
        return "\n".join(lines)

    return "\n".join(header)


def _chart_reason(row: dict[str, Any]) -> str:
    reasons = row.get("chartGateReasons") if isinstance(row.get("chartGateReasons"), list) else []
    if reasons:
        return " | ".join(escape(_s(item)) for item in reasons[:6])
    return (
        f"차트 {escape(_s(row.get('chartState')))} | "
        f"TP1 여력 {_f(row.get('rewardToTp1Pct')):.2f}% | "
        f"손절 리스크 {_f(row.get('riskToStopPct')):.2f}% | "
        f"RSI {_f(row.get('rsi')):.1f} | "
        f"거래량 {_f(row.get('volumeRatio')):.2f}x"
    )


def render_chart_view_html(payload: dict[str, Any], view: str = "summary") -> str:
    if not bool(payload.get("available")):
        return (
            f"<b>차트 분석 불가</b>\n"
            f"이유: <code>{escape(_s(payload.get('reason') or 'unknown'))}</code>\n"
            f"{escape(_s(payload.get('detail') or '차트 데이터를 가져오지 못했습니다.'))}"
        )

    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    strict_buyable = payload.get("strictBuyable") if isinstance(payload.get("strictBuyable"), list) else []
    price_ready = payload.get("priceReady") if isinstance(payload.get("priceReady"), list) else []
    buyable = payload.get("buyable") if isinstance(payload.get("buyable"), list) else []
    wait_pullback = payload.get("waitPullback") if isinstance(payload.get("waitPullback"), list) else []
    overextended = payload.get("overextended") if isinstance(payload.get("overextended"), list) else []
    leaders = payload.get("chartLeaders") if isinstance(payload.get("chartLeaders"), list) else []

    latest_min = _fmt_asof(payload.get("latestCloseMin"))
    latest_max = _fmt_asof(payload.get("latestCloseMax"))
    latest_label = latest_max if latest_min == latest_max else f"{latest_min}~{latest_max}"
    header = [
        "<b>Autostock Chart Desk</b>",
        f"<code>{escape(_fmt_asof(payload.get('generatedAt')))}</code>",
        f"기준가 {escape(latest_label)} | 차트 스캔 {payload.get('universeScannedCount', '-')}",
        f"즉시 {summary.get('strictBuyableCount', 0)} | 가격준비 {summary.get('priceReadyCount', 0)} | 관찰 {summary.get('buyableCount', 0)} | 추격금지 {summary.get('overextendedCount', 0)}",
        f"권장 현금 {_format_pct(summary.get('cashPct'))}",
    ]
    timing = _timing_line(payload)
    if timing:
        header.append(timing)
    header.append("")

    if view == "summary":
        lines = header + ["<b>차트 단독 즉시 진입</b>"]
        if strict_buyable:
            lines.extend(_compact_row(row) for row in strict_buyable[:5])
        else:
            lines.append("없음 - 차트는 매수 근거가 아니라 veto/관찰 도구로만 사용")
        lines.append("")
        lines.append("<b>가격 구조 관찰</b>")
        if price_ready:
            lines.extend(_compact_row(row) for row in price_ready[:5])
        elif buyable:
            lines.extend(_compact_row(row) for row in buyable[:5])
        else:
            lines.append("없음")
        lines.append("")
        lines.append("<b>주도주 추격 금지</b>")
        if overextended:
            lines.extend(
                f"{escape(_s(row.get('symbol')))}  현재 {_format_price(row.get('latestClosePrice'))} | 진입 {_format_price(row.get('averageEntryPrice'))} | {_s(row.get('tradeReason'))}"
                for row in overextended[:5]
            )
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "actionable":
        lines = header + ["<b>차트 단독 즉시 진입</b>"]
        if strict_buyable:
            for row in strict_buyable[:10]:
                lines.append(_one_line_row(row))
                lines.append(_chart_reason(row))
                lines.append("")
        else:
            lines.append("없음 - 뉴스/실적 촉매와 시장 반응 확인 없이 차트만으로 매수하지 않습니다.")
        return "\n".join(lines)

    if view == "wait":
        lines = header + ["<b>눌림 대기</b>"]
        if wait_pullback:
            for row in wait_pullback[:10]:
                lines.append(_one_line_row(row))
                lines.append(_chart_reason(row))
                lines.append("")
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "avoid":
        lines = header + ["<b>주도주 추격 금지</b>"]
        rows = overextended or leaders
        if rows:
            for row in rows[:12]:
                lines.append(
                    f"<b>{escape(_s(row.get('symbol')))}</b> 현재 {_format_price(row.get('latestClosePrice'))} | 진입 {_format_price(row.get('averageEntryPrice'))} | 1차 {_format_price(row.get('tp1Price'))}"
                )
                lines.append(_chart_reason(row))
                lines.append("")
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "portfolio":
        lines = header + ["<b>차트 기준 포트폴리오</b>"]
        if strict_buyable:
            for row in strict_buyable[:8]:
                lines.append(
                    f"{escape(_s(row.get('symbol')))}  {_format_pct(row.get('portfolioWeightPct'))} | 진입 {_format_price(row.get('averageEntryPrice'))} | 손절 {_format_price(row.get('closeStopPrice'))} | 1차 {_format_price(row.get('tp1Price'))}"
                )
            lines.append(f"현금 {_format_pct(summary.get('cashPct'))}")
        else:
            lines.append("차트 단독으로 편입하지 않습니다. 뉴스/실적 촉매 확인 전 현금 대기가 기본입니다.")
            for row in (price_ready or buyable)[:5]:
                lines.append(
                    f"{escape(_s(row.get('symbol')))}  관찰 | 진입 {_format_price(row.get('averageEntryPrice'))} | 손절 {_format_price(row.get('closeStopPrice'))}"
                )
        return "\n".join(lines)

    return "\n".join(header)


__all__ = [
    "analyze_current_charts",
    "analyze_rebalance_universe",
    "full_news_analysis_limit",
    "render_chart_view_html",
    "render_trade_view_html",
    "CACHE_PATH",
]
