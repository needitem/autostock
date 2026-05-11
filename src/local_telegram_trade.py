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
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
import requests

from ai.analyzer import ai
from core.indicators import calculate_indicators
from core.news_collectors import build_next_known_events, fetch_rss_events, fetch_sec_submission_events
from core.stock_data import (
    get_fear_greed_index,
    get_market_condition,
    get_realtime_stock_snapshots,
    get_stock_data,
    get_stock_info,
)


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "outputs" / "telegram"
CACHE_PATH = OUTPUT_ROOT / "universe_trade_analysis.json"
CHART_CACHE_PATH = OUTPUT_ROOT / "current_chart_analysis_full.json"
CHART_SCHEMA_VERSION = "raw-evidence-v3"
TRADE_CACHE_SCHEMA_VERSION = "raw-evidence-v4"
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
        "dailyVolumeAvg20": int(max(0.0, volume_avg_20)),
        "volumeRatio": None,
        "volumeSource": "massive_snapshot_missing",
        "volumeAsOf": "",
    }


def _scan_symbol(symbol: str, rebalance_hint: dict[str, Any] | None = None) -> dict[str, Any] | None:
    bars = get_stock_data(symbol, period="15mo", auto_adjust=False)
    if bars is None or bars.empty:
        return None
    indicators = calculate_indicators(bars)
    if indicators is None:
        return None
    payload = {
        "symbol": symbol,
        "latestClosePrice": round(_f(indicators.get("price"), 0.0), 2),
        "latestCloseAsOf": bars.tail(1).index[0].isoformat() if len(bars.index) else "",
        "chartState": "reference_only",
        "volumeRatio": None,
        "volumeSource": "massive_snapshot_missing",
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
        "rebalanceHint": bool(rebalance_hint),
    }
    payload.update(_daily_price_context(bars, indicators))
    return payload


def _apply_realtime_volume(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
            continue

        realtime_volume = _f(snapshot.get("sessionVolume"), 0.0)
        avg_volume = _f(row.get("dailyVolumeAvg20"), 0.0)
        row["realtimeVolume"] = int(realtime_volume) if realtime_volume > 0 else None
        row["lastMinuteVolume"] = int(_f(snapshot.get("lastMinuteVolume"), 0.0)) or None
        row["volumeRatio"] = round(realtime_volume / avg_volume, 2) if realtime_volume > 0 and avg_volume > 0 else None
        row["volumeSource"] = _s(snapshot.get("source")) or "massive_snapshot"
        row["volumeAsOf"] = _s(snapshot.get("updatedAt"))
        if _f(snapshot.get("closePrice"), 0.0) > 0:
            row["realtimePrice"] = round(_f(snapshot.get("closePrice"), 0.0), 2)
    return rows


def _refresh_payload_realtime_volume(payload: dict[str, Any]) -> dict[str, Any]:
    all_rows = payload.get("all") if isinstance(payload.get("all"), list) else []
    rows = [row for row in all_rows if isinstance(row, dict)]
    if not rows:
        return payload

    _apply_realtime_volume(rows)
    by_symbol = {_s(row.get("symbol")).upper(): row for row in rows if _s(row.get("symbol"))}
    for key in ("strictBuyable", "actionableNow", "priceReady", "buyable", "waitPullback", "avoid", "chartLeaders", "overextended"):
        bucket = payload.get(key)
        if not isinstance(bucket, list):
            continue
        for row in bucket:
            if isinstance(row, dict):
                row.update(by_symbol.get(_s(row.get("symbol")).upper(), {}))
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
    _apply_realtime_volume(scanned)
    scanned.sort(
        key=lambda row: (
            -_f(row.get("return63d"), 0.0),
            -_f(row.get("return21d"), 0.0),
            _s(row.get("symbol")),
        )
    )
    return scanned


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


def _bundle_has_news(bundle: dict[str, Any]) -> bool:
    events = bundle.get("events") if isinstance(bundle.get("events"), list) else []
    next_events = bundle.get("nextEvents") if isinstance(bundle.get("nextEvents"), list) else []
    return bool(events or next_events)


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


def _valuation_assessment(row: dict[str, Any]) -> dict[str, Any]:
    valuation_lines = [
        (
            f"P/FCF {_format_multiple(row.get('pFcf'))}, FCF yield {_format_pct(row.get('fcfYieldPct'))}, "
            f"PER {_format_multiple(row.get('pe'))}, Fwd PER {_format_multiple(row.get('forwardPe'))}"
        ),
        (
            f"ROE {_format_pct(row.get('roePct'))}, 순마진 {_format_pct(row.get('profitMarginPct'))}, "
            f"매출성장 {_format_pct(row.get('revenueGrowthPct'))}, EPS성장 {_format_pct(row.get('forwardEpsGrowthPct'))}"
        ),
        (
            f"부채비율 {_format_multiple(row.get('debtToEquity'))}, 유동비율 {_format_multiple(row.get('currentRatio'))}, "
            f"애널리스트 {_f(row.get('targetUpsidePct')):.2f}%"
        ),
    ]

    return {
        "valueState": "REFERENCE_ONLY",
        "qualityState": "REFERENCE_ONLY",
        "growthState": "REFERENCE_ONLY",
        "balanceState": "REFERENCE_ONLY",
        "valuationReasons": valuation_lines,
        "qualityReasons": [],
        "growthReasons": [],
        "riskReasons": [],
    }


def _value_decision(
    row: dict[str, Any],
    news: dict[str, Any] | None = None,
) -> dict[str, Any]:
    assessment = _valuation_assessment(row)
    news_signal = _s((news or {}).get("signal")).lower()
    news_strength = _s((news or {}).get("strength")).lower()
    state = "REVIEW"
    bucket = "wait_pullback"
    if news_signal:
        reason = f"Codex 뉴스 요약 {news_signal}/{news_strength or 'none'}; 자동 점수 판정 없이 수동 검토"
    else:
        reason = "정량 지표는 참고 데이터만 제공; 자동 점수 판정 없이 수동 검토"

    reasons = [
        *[str(item) for item in assessment.get("valuationReasons") or []],
    ]
    if news_signal:
        reasons.append(f"뉴스 {news_signal}/{news_strength}: {_s((news or {}).get('headline'))}")
    return {
        **assessment,
        "decisionState": state,
        "actionBucket": bucket,
        "actionReason": reason,
        "portfolioWeightPct": 0.0,
        "decisionReasons": [item for item in reasons if item][:8],
    }


def _value_rank_key(row: dict[str, Any]) -> tuple[int, float, str]:
    return (
        0 if bool(row.get("rebalanceSelected")) else 1,
        -_f(row.get("marketCap"), 0.0),
        _s(row.get("symbol")),
    )


def _batch_rows_from_json(value: Any) -> list[dict[str, Any]] | None:
    if isinstance(value, list):
        rows = [row for row in value if isinstance(row, dict)]
        return rows if rows else None

    if not isinstance(value, dict):
        return None

    for key in ("items", "results", "analysis", "analyses", "data"):
        rows = value.get(key)
        if isinstance(rows, list):
            parsed = [row for row in rows if isinstance(row, dict)]
            return parsed if parsed else None

    if _s(value.get("symbol")) and ("signal" in value or "strength" in value):
        return [value]

    mapped_rows: list[dict[str, Any]] = []
    for symbol, row in value.items():
        if not isinstance(row, dict):
            continue
        parsed = dict(row)
        parsed.setdefault("symbol", symbol)
        if _s(parsed.get("symbol")) and ("signal" in parsed or "strength" in parsed):
            mapped_rows.append(parsed)
    return mapped_rows if mapped_rows else None


def _rows_match_expected_symbols(rows: list[dict[str, Any]], expected_symbols: set[str]) -> bool:
    if not expected_symbols:
        return True
    return any(_s(row.get("symbol")).upper() in expected_symbols for row in rows)


def _extract_batch_rows(text: str, expected_symbols: set[str] | None = None) -> list[dict[str, Any]] | None:
    expected_symbols = expected_symbols or set()
    value = ai._extract_json_value(text)
    rows = _batch_rows_from_json(value)
    if rows is not None and _rows_match_expected_symbols(rows, expected_symbols):
        return rows

    # Last-resort recovery for replies that include prose before the useful JSON.
    cleaned = ai._clean_json_text(text)
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(cleaned):
        if ch not in "{[":
            continue
        try:
            candidate, _ = decoder.raw_decode(cleaned[idx:])
        except Exception:
            continue
        rows = _batch_rows_from_json(candidate)
        if rows is not None and _rows_match_expected_symbols(rows, expected_symbols):
            return rows
    return None


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
        prompt_items = json.dumps(items, ensure_ascii=False, separators=(",", ":"))
        prompt = (
            "You are a valuation risk analyst for public equities.\n"
            "Write in Korean internally, but output STRICT JSON only.\n"
            "For each symbol, classify whether recent events improve or impair the investment value case.\n"
            "News must never promote a symbol by itself: bullish means value impairment risk is lower or fundamentals improved; "
            "bearish means the value case may be impaired; neutral means no material value-case change.\n"
            "Use enums only:\n"
            "- signal: bullish | bearish | neutral\n"
            "- strength: strong | moderate | weak | none\n\n"
            f"Items JSON: {prompt_items}\n\n"
            "Return JSON only in this shape:\n"
            '{"items":[{"symbol":"AAPL","signal":"bullish|bearish|neutral","strength":"strong|moderate|weak|none","headline":"key headline","rationale":["short reason 1","short reason 2"]}]}'
        )
        text = ai._call(prompt, max_tokens=2200)
        if not text:
            return {"error": "codex_batch_analysis_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}
        rows = _extract_batch_rows(text, set(group_symbols))
        if rows is None:
            return {"error": "codex_batch_json_parse_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}
        for row in rows:
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


def _trade_verdict(plan: dict[str, Any], latest_price: float) -> tuple[str, str]:
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


def _price_plan_context(row: dict[str, Any], latest_price: float) -> dict[str, Any]:
    normalized_plan = _build_execution_plan(
        {
            **row,
            "support": row.get("support"),
            "resistance": row.get("resistance"),
        },
        latest_price=latest_price,
    )
    risk_to_stop_pct = _risk_pct(latest_price, _f(normalized_plan.get("closeStopPrice"), 0.0))
    reward_to_tp1_pct = _reward_pct(latest_price, _f(normalized_plan.get("tp1Price"), 0.0))
    reward_to_tp2_pct = _reward_pct(latest_price, _f(normalized_plan.get("tp2Price"), 0.0))
    trade_verdict, trade_reason = _trade_verdict(normalized_plan, latest_price)
    return {
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
        "rrToTp1": _rr(reward_to_tp1_pct, risk_to_stop_pct),
        "rrToTp2": _rr(reward_to_tp2_pct, risk_to_stop_pct),
        "tradeVerdict": trade_verdict,
        "tradeReason": trade_reason,
    }


def _news_context(bundle: dict[str, Any], news: dict[str, Any]) -> dict[str, Any]:
    raw_events = bundle.get("events") if isinstance(bundle.get("events"), list) else []
    next_events = bundle.get("nextEvents") if isinstance(bundle.get("nextEvents"), list) else []
    return {
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
    }


def _chart_buyable(row: dict[str, Any]) -> bool:
    return _s(row.get("tradeVerdict")) in {"plan_active", "watch"}


def _chart_wait(row: dict[str, Any]) -> bool:
    return _s(row.get("tradeVerdict")) == "wait_pullback"


def _chart_gate_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    current_vs_entry = _f(row.get("currentVsEntryPct"), 999.0)
    reward = _f(row.get("rewardToTp1Pct"), 0.0)
    rsi = _f(row.get("rsi"), 50.0)
    chart_state = _s(row.get("chartState"))
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
    reasons.append(f"거래량 {_format_volume_ratio(row.get('volumeRatio'))}")
    return reasons


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
    symbol = _s(row.get("symbol")).upper()
    portfolio_weight_pct = round(_f(executed_weights_pct.get(symbol), 0.0), 2)
    out = {
        **row,
        "rebalanceSelected": symbol in selected_symbols,
        "portfolioWeightPct": portfolio_weight_pct,
        **_price_plan_context(row, latest_price),
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
        if _s(payload.get("schemaVersion")) == TRADE_CACHE_SCHEMA_VERSION and bool(payload.get("available")):
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
    return _apply_realtime_volume(out) if out else None


def _build_raw_evidence_row(
    *,
    symbol: str,
    fundamental: dict[str, Any],
    chart_row: dict[str, Any],
    bundle: dict[str, Any],
    news: dict[str, Any],
    selected_symbols: set[str],
    executed_weights_pct: dict[str, Any],
) -> dict[str, Any]:
    latest_price = round(_f(chart_row.get("latestClosePrice"), _f(fundamental.get("latestClosePrice"), 0.0)), 2)
    row = {
        **fundamental,
        "rebalanceSelected": symbol in selected_symbols,
        "existingPortfolioWeightPct": round(_f(executed_weights_pct.get(symbol), 0.0), 2),
        "latestClosePrice": latest_price,
        "latestCloseAsOf": _s(chart_row.get("latestCloseAsOf")),
        "previousClosePrice": chart_row.get("previousClosePrice"),
        "latestOpenPrice": chart_row.get("latestOpenPrice"),
        "latestHighPrice": chart_row.get("latestHighPrice"),
        "latestLowPrice": chart_row.get("latestLowPrice"),
        "dayReturnPct": chart_row.get("dayReturnPct"),
        "gapPct": chart_row.get("gapPct"),
        "intradayReturnPct": chart_row.get("intradayReturnPct"),
        "dayRangePct": chart_row.get("dayRangePct"),
        "closeLocationPct": chart_row.get("closeLocationPct"),
        "chartState": _s(chart_row.get("chartState")),
        "volumeRatio": _round_optional(chart_row.get("volumeRatio"), 2),
        "realtimeVolume": chart_row.get("realtimeVolume"),
        "lastMinuteVolume": chart_row.get("lastMinuteVolume"),
        "dailyVolumeAvg20": chart_row.get("dailyVolumeAvg20"),
        "volumeSource": _s(chart_row.get("volumeSource")),
        "volumeAsOf": _s(chart_row.get("volumeAsOf")),
        "rsi": round(_f(chart_row.get("rsi"), 0.0), 1),
        "adx": round(_f(chart_row.get("adx"), 0.0), 1),
        **_news_context(bundle, news),
        **_price_plan_context(chart_row, latest_price),
    }
    row.update(_value_decision(row, news))
    return row


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
    candidate_rows = sorted(
        fundamental_rows,
        key=lambda row: (
            0 if _s(row.get("symbol")).upper() in selected_symbols else 1,
            -_f(row.get("marketCap"), 0.0),
            _s(row.get("symbol")),
        ),
    )[:analysis_limit]
    candidate_symbols = [_s(row.get("symbol")) for row in candidate_rows if _s(row.get("symbol"))]
    timings["fundamentalScanSec"] = round(time.perf_counter() - fundamental_started, 3)

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
        chart_row = chart_rows_by_symbol.get(symbol, {})
        bundle = bundles.get(symbol, {})
        news = news_analysis.get(symbol) if isinstance(news_analysis.get(symbol), dict) else {}
        evaluated.append(
            _build_raw_evidence_row(
                symbol=symbol,
                fundamental=fundamental,
                chart_row=chart_row,
                bundle=bundle,
                news=news,
                selected_symbols=selected_symbols,
                executed_weights_pct=executed_weights_pct,
            )
        )
    timings["evaluateSec"] = round(time.perf_counter() - eval_started, 3)

    evaluated.sort(key=_value_rank_key)
    for idx, row in enumerate(evaluated, start=1):
        row["finalRank"] = idx

    actionable = [row for row in evaluated if _s(row.get("actionBucket")) == "actionable_now"]
    wait_pullback = [row for row in evaluated if _s(row.get("actionBucket")) == "wait_pullback"]
    avoid = [row for row in evaluated if _s(row.get("actionBucket")) == "avoid"]
    cash_pct = round(max(0.0, 100.0 - sum(_f(row.get("portfolioWeightPct"), 0.0) for row in actionable)), 2)
    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "available": True,
        "schemaVersion": TRADE_CACHE_SCHEMA_VERSION,
        "analysisMode": "raw-evidence",
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
        return _refresh_payload_realtime_volume(_load_json(CHART_CACHE_PATH))

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
    buyable = sorted([row for row in evaluated if _chart_buyable(row)], key=lambda row: _s(row.get("symbol")))
    wait_pullback = sorted([row for row in evaluated if _chart_wait(row)], key=lambda row: _s(row.get("symbol")))
    leaders = sorted(evaluated, key=_chart_leader_key)
    overextended: list[dict[str, Any]] = []
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


def _format_volume_ratio(value: Any) -> str:
    ratio = _round_optional(value, 2)
    if ratio is None:
        return "-"
    return f"{ratio:.2f}x"


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
    market_condition = ((payload.get("marketStatus") or {}).get("marketCondition") or {}) if isinstance(payload.get("marketStatus"), dict) else {}
    fear_greed = ((payload.get("marketStatus") or {}).get("fearGreed") or {}) if isinstance(payload.get("marketStatus"), dict) else {}
    actionable = _bucket_rows(payload, "actionable")
    wait_pullback = _bucket_rows(payload, "wait")
    avoid = _bucket_rows(payload, "avoid")
    raw_mode = _s(payload.get("analysisMode")) in {"valuation-first", "raw-evidence"}

    header = [
        "<b>Autostock Evidence Desk</b>" if raw_mode else "<b>Autostock Trade Desk</b>",
        f"<code>{escape(_fmt_asof(payload.get('generatedAt')))}</code>",
        f"시장 {_s(market_condition.get('message'))} | 공포탐욕 {fear_greed.get('score', '-')}",
        (
            f"원자료 스캔 {payload.get('universeScannedCount', '-')} | 후보 {payload.get('valueCandidateCount', '-')} | 뉴스 요약 {payload.get('newsAnalyzedCount', '-')}"
            if raw_mode
            else f"뉴스 탐색 {payload.get('universeNewsScannedCount', payload.get('universeScannedCount', '-'))} | 촉매후보 {payload.get('newsCandidateCount', '-')} | Codex {payload.get('newsAnalyzedCount', '-')}"
        ),
        f"가격 검증 {payload.get('chartScannedCount', payload.get('universeScannedCount', '-'))}",
        f"모델 {escape(_s(payload.get('aiModel') or ai.model))} / {escape(_s(payload.get('aiReasoningEffort') or ai.reasoning_effort))}",
        f"자동편입 {summary.get('actionableCount', 0)} | 검토 {summary.get('waitPullbackCount', 0)} | 자동제외 {summary.get('avoidCount', 0)} | 현금 {_format_pct(summary.get('cashPct'))}",
    ]
    timing = _timing_line(payload)
    if timing:
        header.append(timing)
    header.append("")

    if view == "summary":
        lines = header[:]
        lines.append("<b>자동 편입 후보</b>" if raw_mode else "<b>지금 진입 가능</b>")
        if actionable:
            lines.extend(_compact_row(row) for row in actionable[:3])
        else:
            lines.append("없음")
        lines.append("")
        lines.append("<b>수동 검토 후보</b>" if raw_mode else "<b>눌림 대기</b>")
        if wait_pullback:
            lines.extend(_compact_row(row) for row in wait_pullback[:5])
        else:
            lines.append("없음")
        lines.append("")
        lines.append("<b>자동 제외</b>" if raw_mode else "<b>추격 금지</b>")
        if avoid:
            lines.extend(f"{escape(_s(row.get('symbol')))}  {_s(row.get('actionReason') or row.get('tradeReason'))}" for row in avoid[:3])
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "actionable":
        lines = header + ["<b>자동 편입 후보</b>" if raw_mode else "<b>즉시 매수 후보</b>"]
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
        lines = header + ["<b>수동 검토 후보</b>" if raw_mode else "<b>눌림 대기 후보</b>"]
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
        lines = header + ["<b>자동 제외</b>" if raw_mode else "<b>지금 제외</b>"]
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
        lines = header + ["<b>포트폴리오</b>" if raw_mode else "<b>추천 포트폴리오</b>", f"현금 {_format_pct(summary.get('cashPct'))}"]
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
            lines.append("자동 점수/임계값 기반 편입은 제거했습니다.")
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
        f"거래량 {_format_volume_ratio(row.get('volumeRatio'))}"
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
