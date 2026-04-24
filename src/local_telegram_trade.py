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
from core.event_watchlist import chart_volume_gate
from core.indicators import calculate_indicators
from core.news_collectors import build_next_known_events, fetch_rss_events, fetch_sec_submission_events
from core.stock_data import get_fear_greed_index, get_market_condition, get_stock_data, get_stock_info


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "outputs" / "telegram"
CACHE_PATH = OUTPUT_ROOT / "universe_trade_analysis.json"
CHART_CACHE_PATH = OUTPUT_ROOT / "current_chart_analysis_full.json"
CHART_SCHEMA_VERSION = "condition-gates-v1"
TRADE_CACHE_SCHEMA_VERSION = "manual-synthesis-v1"
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


def _codex_batch_size() -> int:
    try:
        return max(4, int(os.getenv("TELEGRAM_CODEX_BATCH_SIZE", "12")))
    except Exception:
        return 12


def _final_synthesis_limit() -> int:
    try:
        return max(40, int(os.getenv("TELEGRAM_FINAL_SYNTHESIS_MAX_SYMBOLS", "240")))
    except Exception:
        return 240


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


def _derive_selection_score(indicators: dict[str, Any], rebalance_hint: dict[str, Any] | None = None) -> float:
    if isinstance(rebalance_hint, dict) and rebalance_hint.get("selection_score") is not None:
        return round(_f(rebalance_hint.get("selection_score"), 0.0), 2)
    return round(_f(indicators.get("return_63d"), 0.0) * 0.6 + _f(indicators.get("return_21d"), 0.0) * 0.4, 2)


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
        "selectionScore": _derive_selection_score(indicators, rebalance_hint),
        "chartState": _s(chart_gate.get("state")),
        "volumeRatio": round(_f(chart_gate.get("volume_ratio"), 0.0), 2),
        "rsi": round(_f(indicators.get("rsi"), 0.0), 1),
        "adx": round(_f(indicators.get("adx"), 0.0), 1),
        "atr": round(_f(indicators.get("atr"), 0.0), 2),
        "support": indicators.get("support") if isinstance(indicators.get("support"), list) else [],
        "resistance": indicators.get("resistance") if isinstance(indicators.get("resistance"), list) else [],
        "ma50Gap": round(_f(indicators.get("ma50_gap"), 0.0), 2),
        "ma200Gap": round(_f(indicators.get("ma200_gap"), 0.0), 2),
        "return21d": round(_f(indicators.get("return_21d"), 0.0), 2),
        "return63d": round(_f(indicators.get("return_63d"), 0.0), 2),
        "warnings": _derive_warnings(indicators, rebalance_hint),
        "rebalanceHint": bool(rebalance_hint),
    }
    return payload


def _scan_full_universe(rebalance_hints: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    symbols = _load_all_us_symbols()
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
            -_f(row.get("selectionScore"), 0.0),
            -_f(row.get("return63d"), 0.0),
            -_f(row.get("return21d"), 0.0),
            _s(row.get("symbol")),
        )
    )
    return scanned


def _select_news_symbols(
    scanned_rows: list[dict[str, Any]],
    selected_symbols: set[str],
    news_limit: int,
) -> list[str]:
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
    for row in scanned_rows:
        _push(_s(row.get("symbol")))
        if len(symbols) >= max(len(selected_symbols), news_limit):
            break
    return symbols


def _collect_symbol_news_bundle(symbol: str, chart_row: dict[str, Any], session: requests.Session | None = None) -> dict[str, Any]:
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
        "chartRow": chart_row,
    }


def _collect_news_bundles(news_symbols: list[str], chart_rows_by_symbol: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    bundles: dict[str, dict[str, Any]] = {}
    workers = _news_workers()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_collect_symbol_news_bundle, symbol, chart_rows_by_symbol[symbol]): symbol
            for symbol in news_symbols
            if symbol in chart_rows_by_symbol
        }
        for future in as_completed(futures):
            bundle = future.result()
            symbol = _s(bundle.get("symbol")).upper()
            if symbol:
                bundles[symbol] = bundle
    return bundles


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
            "You are an event-driven equity trading analyst.\n"
            "Write in Korean internally, but output STRICT JSON only.\n"
            "For each symbol, decide the current news/event direction and strength.\n"
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


def _final_synthesis_key(row: dict[str, Any]) -> tuple[int, float, float, float, str]:
    bucket = _s(row.get("actionBucket"))
    bucket_rank = 0 if bucket == "actionable_now" else 1 if bucket == "wait_pullback" else 2
    signal = _s(row.get("newsSignal")).lower()
    strength = _s(row.get("newsStrength")).lower()
    news_rank = 0
    if signal == "bullish" and strength == "strong":
        news_rank = -3
    elif signal == "bullish" and strength == "moderate":
        news_rank = -2
    elif signal == "bullish":
        news_rank = -1
    elif signal == "bearish":
        news_rank = 2
    return (
        bucket_rank + news_rank,
        -_f(row.get("rrToTp1"), -99.0),
        -_f(row.get("rewardToTp1Pct"), 0.0),
        -_f(row.get("selectionScore"), 0.0),
        _s(row.get("symbol")),
    )


def _final_synthesis_rows(evaluated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = sorted(evaluated, key=_final_synthesis_key)[: _final_synthesis_limit()]
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "symbol": _s(row.get("symbol")),
                "name": _s(row.get("name")),
                "sector": _s(row.get("sector")),
                "price": row.get("latestClosePrice"),
                "asOf": _s(row.get("latestCloseAsOf")),
                "prelimBucket": _s(row.get("actionBucket")),
                "prelimReason": _s(row.get("actionReason")),
                "chartState": _s(row.get("chartState")),
                "tradeVerdict": _s(row.get("tradeVerdict")),
                "entry": row.get("averageEntryPrice"),
                "stop": row.get("closeStopPrice"),
                "tp1": row.get("tp1Price"),
                "tp2": row.get("tp2Price"),
                "rrToTp1": row.get("rrToTp1"),
                "currentVsEntryPct": row.get("currentVsEntryPct"),
                "rewardToTp1Pct": row.get("rewardToTp1Pct"),
                "rsi": row.get("rsi"),
                "volumeRatio": row.get("volumeRatio"),
                "warnings": row.get("warnings") or [],
                "rebalanceSelected": bool(row.get("rebalanceSelected")),
                "newsSignal": _s(row.get("newsSignal")),
                "newsStrength": _s(row.get("newsStrength")),
                "newsHeadline": _s(row.get("newsHeadline")),
                "newsReasons": row.get("newsReasons") or [],
                "eventHeadlines": row.get("eventHeadlines") or [],
                "nextEvents": row.get("nextEvents") or [],
            }
        )
    return out


def _final_trade_synthesis(
    evaluated: list[dict[str, Any]],
    market_ctx: dict[str, Any],
    fear_greed: dict[str, Any],
    analysis_limit: int,
) -> dict[str, Any] | dict[str, str]:
    rows = _final_synthesis_rows(evaluated)
    if not rows:
        return {"summary": "No candidates to synthesize.", "items": []}
    prompt = (
        "You are the final portfolio decision layer for a US equity trade desk.\n"
        "Use the same style as a careful manual analyst: compare all candidates against each other, not one-by-one in isolation.\n"
        "Do NOT use a numeric score as the final reason. Prefer concrete, recent, verifiable catalysts and entry quality.\n"
        "Promotion catalysts: major AI/data-center demand, hyperscaler/NVIDIA partnerships, large investment, index inclusion, capacity expansion, earnings beat, raised guidance, contract win, credible analyst/IR catalyst.\n"
        "Demotion catalysts: dividend cut, guide-down, structural industry weakness, legal/accounting risk, bearish SEC/news, bad entry after overextension, poor RR, low volume confirmation.\n"
        "When several names share the same theme, rank the cleaner catalyst + cleaner entry higher, like LITE vs CIEN vs GLW style relative comparison.\n"
        "Return STRICT JSON only. Korean text values are allowed.\n"
        "Allowed finalBucket values: actionable_now, wait_pullback, avoid.\n"
        "portfolioWeightPct should be 0 for avoid, 0 to 2 for wait_pullback, and usually 1.5 to 5 for actionable_now. Keep total actionable weight prudent.\n\n"
        f"Model expectation: gpt-5.5 with xhigh reasoning.\n"
        f"Market context: {market_ctx}\n"
        f"Fear/greed: {fear_greed}\n"
        f"Analyzed symbols requested: {analysis_limit}\n"
        f"Candidates for final cross-sectional synthesis: {rows}\n\n"
        "Return this shape exactly:\n"
        '{"summary":"one Korean sentence","items":[{"symbol":"AAPL","finalBucket":"actionable_now|wait_pullback|avoid","rank":1,"portfolioWeightPct":3.0,"decisionReason":"why this belongs here","catalystSummary":"key catalyst or lack of catalyst","riskNote":"main risk"}]}'
    )
    text = ai._call(prompt, max_tokens=5200)
    if not text:
        return {"error": "codex_final_synthesis_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}
    obj = ai._extract_json_object(text)
    if not isinstance(obj, dict):
        return {"error": "codex_final_synthesis_json_parse_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}
    items = obj.get("items")
    if not isinstance(items, list):
        return {"error": "codex_final_synthesis_items_missing", "model": ai.model, "reasoningEffort": ai.reasoning_effort}
    return {
        "summary": _s(obj.get("summary")),
        "model": ai.model,
        "reasoningEffort": ai.reasoning_effort,
        "candidateCount": len(rows),
        "items": items,
    }


def _apply_final_synthesis(evaluated: list[dict[str, Any]], synthesis: dict[str, Any]) -> list[dict[str, Any]]:
    updates: dict[str, dict[str, Any]] = {}
    for item in synthesis.get("items") or []:
        if not isinstance(item, dict):
            continue
        symbol = _s(item.get("symbol")).upper()
        if not symbol:
            continue
        bucket = _s(item.get("finalBucket")).lower()
        if bucket not in {"actionable_now", "wait_pullback", "avoid"}:
            continue
        updates[symbol] = item

    for row in evaluated:
        symbol = _s(row.get("symbol")).upper()
        item = updates.get(symbol)
        if not item:
            continue
        bucket = _s(item.get("finalBucket")).lower()
        row["actionBucket"] = bucket
        row["actionReason"] = _s(item.get("decisionReason")) or _s(row.get("actionReason"))
        row["finalCatalystSummary"] = _s(item.get("catalystSummary"))
        row["finalRiskNote"] = _s(item.get("riskNote"))
        row["finalRank"] = int(_f(item.get("rank"), 9999.0))
        row["manualSynthesisApplied"] = True
        weight = _f(item.get("portfolioWeightPct"), _f(row.get("portfolioWeightPct"), 0.0))
        if bucket == "avoid":
            weight = 0.0
        row["portfolioWeightPct"] = round(max(0.0, min(8.0, weight)), 2)
    return evaluated


def _build_execution_plan(row: dict[str, Any], latest_price: float) -> dict[str, Any]:
    atr = max(0.01, _f(row.get("atr"), 0.0))
    supports = row.get("support") if isinstance(row.get("support"), list) else []
    resistances = row.get("resistance") if isinstance(row.get("resistance"), list) else []
    support = _f(supports[0], latest_price - atr * 2) if supports else latest_price - atr * 2
    resistance = _f(resistances[0], latest_price + atr * 2.5) if resistances else latest_price + atr * 2.5
    volume_ratio = _f(row.get("volume_ratio"), 1.0)
    support_distance_atr = (latest_price - support) / atr if atr > 0 else 99.0
    watch_retest = support_distance_atr > 2.0 or volume_ratio < 1.0

    buy1 = round(support + atr * 0.2, 2)
    buy2 = round(support - atr * 0.6, 2)
    buy3 = round(support - atr * 1.0, 2)
    close_stop = round(support - atr * 1.5, 2)
    hard_stop = round(support - atr * 1.7, 2)
    splits = [100.0, 0.0, 0.0] if watch_retest else [40.0, 35.0, 25.0]
    average_entry = round((buy1 * splits[0] + buy2 * splits[1] + buy3 * splits[2]) / max(sum(splits), 1.0), 2)
    risk_price = max(0.01, average_entry - close_stop)
    tp1 = round(average_entry + risk_price * 1.5, 2)
    tp2 = round(average_entry + risk_price * 2.5, 2)

    return {
        "entryMode": "watch_retest" if watch_retest else "active",
        "activationReason": "support_too_far_or_volume_soft" if watch_retest else "support_valid",
        "entryLevels": [
            {"name": "buy1", "price": buy1, "splitPct": splits[0]},
            {"name": "buy2", "price": buy2, "splitPct": splits[1]},
            {"name": "buy3", "price": buy3, "splitPct": splits[2]},
        ],
        "averageEntryPrice": average_entry,
        "closeStopPrice": close_stop,
        "hardStopPrice": hard_stop,
        "tp1Price": tp1,
        "tp2Price": tp2,
        "supportUsed": round(support, 2),
        "resistanceUsed": round(resistance, 2),
        "supportDistanceAtr": round(support_distance_atr, 2),
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
    current_vs_entry = _pct_change(latest_price, _f(plan.get("averageEntryPrice"), 0.0))
    reward_to_tp1 = _reward_pct(latest_price, _f(plan.get("tp1Price"), 0.0))
    reward_to_tp2 = _reward_pct(latest_price, _f(plan.get("tp2Price"), 0.0))
    risk_to_stop = _risk_pct(latest_price, _f(plan.get("closeStopPrice"), 0.0))
    warning_set = {str(item) for item in warnings}

    if latest_price <= _f(plan.get("closeStopPrice"), 0.0):
        return ("avoid", "below_stop")
    if {"overheat_extreme", "overheat_dual", "entry_negative"} & warning_set:
        if reward_to_tp2 is None or reward_to_tp2 < 6.0:
            return ("avoid", "severe_warning")
    if reward_to_tp1 is not None and risk_to_stop is not None:
        if reward_to_tp1 >= 4.0 and _rr(reward_to_tp1, risk_to_stop) is not None and _rr(reward_to_tp1, risk_to_stop) >= 1.2:
            if current_vs_entry is None or current_vs_entry <= 6.0:
                return ("marketable_now", "reward_to_tp1_valid")
    if current_vs_entry is not None and current_vs_entry > 6.0:
        return ("wait_pullback", "price_extended_above_entry")
    if reward_to_tp1 is not None and reward_to_tp1 <= 0:
        if reward_to_tp2 is None or reward_to_tp2 <= 3.0:
            return ("wait_pullback", "tp1_already_near")
    if current_vs_entry is not None and current_vs_entry > 0:
        return ("wait_pullback", "entry_below_market")
    return ("avoid", "reward_risk_not_enough")


def _action_bucket(
    *,
    news_signal: str,
    news_strength: str,
    chart_state: str,
    trade_verdict: str,
    rebalance_selected: bool,
    rr_to_tp1: float | None,
) -> tuple[str, str]:
    if news_signal == "bearish" and news_strength in {"strong", "moderate"}:
        return ("avoid", "bearish_news")
    if chart_state in {"confirmed_breakdown", "weak"} and news_signal != "bullish":
        return ("avoid", "weak_chart")
    if trade_verdict == "marketable_now":
        if news_signal == "bullish" and news_strength in {"strong", "moderate", "weak"}:
            return ("actionable_now", "bullish_news_plus_trade_setup")
        if rebalance_selected and (rr_to_tp1 is None or rr_to_tp1 >= 1.0):
            return ("actionable_now", "selected_trade_setup")
        return ("wait_pullback", "trade_ok_but_news_not_confirmed")
    if trade_verdict == "wait_pullback":
        if news_signal == "bullish" and chart_state in {"confirmed_breakout", "constructive"}:
            return ("wait_pullback", "good_story_but_better_on_pullback")
        return ("wait_pullback", "price_not_clean")
    return ("avoid", "insufficient_edge")


def _rank_priority(bucket: str) -> int:
    return {
        "actionable_now": 0,
        "wait_pullback": 1,
        "avoid": 2,
    }.get(bucket, 9)


def _chart_clean_enough(row: dict[str, Any]) -> bool:
    warnings = {str(item) for item in (row.get("warnings") or [])}
    return not bool({"entry_negative", "overheat_extreme", "overheat_dual"} & warnings)


def _chart_entry_near(row: dict[str, Any], max_abs_pct: float = 1.0) -> bool:
    current_vs_entry = _f(row.get("currentVsEntryPct"), 999.0)
    return abs(current_vs_entry) <= max_abs_pct


def _strict_chart_buyable(row: dict[str, Any]) -> bool:
    return (
        _chart_clean_enough(row)
        and _s(row.get("tradeVerdict")) == "marketable_now"
        and _s(row.get("chartState")) in {"constructive", "confirmed_breakout"}
        and _chart_entry_near(row, 1.0)
        and _f(row.get("volumeRatio"), 0.0) >= 0.9
        and 42.0 <= _f(row.get("rsi"), 50.0) <= 70.0
        and _f(row.get("rewardToTp1Pct"), 0.0) >= 4.0
        and _f(row.get("rrToTp1"), -99.0) >= 1.0
    )


def _chart_buyable(row: dict[str, Any]) -> bool:
    return (
        _chart_clean_enough(row)
        and _s(row.get("tradeVerdict")) == "marketable_now"
        and _s(row.get("chartState")) in {"constructive", "confirmed_breakout", "mixed"}
        and _chart_entry_near(row, 2.0)
        and _f(row.get("rewardToTp1Pct"), 0.0) >= 4.0
        and _f(row.get("rrToTp1"), -99.0) >= 1.0
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
    rr = _f(row.get("rrToTp1"), 0.0)
    rsi = _f(row.get("rsi"), 50.0)
    volume_ratio = _f(row.get("volumeRatio"), 0.0)
    chart_state = _s(row.get("chartState"))
    warnings = {str(item) for item in (row.get("warnings") or [])}

    if _s(row.get("tradeVerdict")) == "marketable_now":
        reasons.append("진입가 근처" if abs(current_vs_entry) <= 1.0 else f"진입가 대비 {current_vs_entry:.2f}%")
    elif _s(row.get("tradeVerdict")) == "wait_pullback":
        reasons.append(f"눌림 대기: 진입가 대비 {current_vs_entry:.2f}%")

    if chart_state in {"constructive", "confirmed_breakout"}:
        reasons.append(f"차트 {chart_state}")
    elif chart_state:
        reasons.append(f"차트 {chart_state}")

    reasons.append(f"TP1 여력 {reward:.2f}%")
    reasons.append(f"RR1 {rr:.2f}")
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


def _chart_buyable_key(row: dict[str, Any]) -> tuple[float, float, float, str]:
    return (
        abs(_f(row.get("currentVsEntryPct"), 999.0)),
        -_f(row.get("rrToTp1"), -99.0),
        -_f(row.get("rewardToTp1Pct"), 0.0),
        _s(row.get("symbol")),
    )


def _chart_wait_key(row: dict[str, Any]) -> tuple[float, float, float, str]:
    distance = abs(_f(row.get("currentVsEntryPct"), 999.0))
    return (
        distance,
        -_f(row.get("rrToTp1"), -99.0),
        -_f(row.get("rewardToTp1Pct"), 0.0),
        _s(row.get("symbol")),
    )


def _chart_leader_key(row: dict[str, Any]) -> tuple[float, float, float, str]:
    return (
        -_f(row.get("selectionScore"), 0.0),
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
    if trade_verdict == "marketable_now" and portfolio_weight_pct <= 0:
        portfolio_weight_pct = 1.5
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


def analyze_rebalance_universe(force_refresh: bool = False, news_limit: int | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    ttl_minutes = _event_cache_minutes()
    analysis_limit = max(10, int(news_limit)) if news_limit is not None else _analysis_limit()
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

    timings: dict[str, float | bool] = {}
    scan_started = time.perf_counter()
    scanned_rows = None if force_refresh else _load_cached_chart_rows(ttl_minutes)
    chart_cache_hit = scanned_rows is not None
    if scanned_rows is None:
        scanned_rows = _scan_full_universe(rebalance_hints)
    timings["chartCacheHit"] = chart_cache_hit
    timings["chartRowsSec"] = round(time.perf_counter() - scan_started, 3)

    chart_rows_by_symbol = {_s(row.get("symbol")).upper(): row for row in scanned_rows if _s(row.get("symbol"))}
    news_symbols = _select_news_symbols(scanned_rows, selected_symbols, analysis_limit)
    news_started = time.perf_counter()
    bundles = _collect_news_bundles(news_symbols, chart_rows_by_symbol)
    timings["newsCollectSec"] = round(time.perf_counter() - news_started, 3)
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
        bucket, bucket_reason = _action_bucket(
            news_signal=_s(news.get("signal")).lower(),
            news_strength=_s(news.get("strength")).lower(),
            chart_state=_s(row.get("chartState")).lower(),
            trade_verdict=trade_verdict,
            rebalance_selected=symbol in selected_symbols,
            rr_to_tp1=rr_to_tp1,
        )
        portfolio_weight_pct = round(_f(executed_weights_pct.get(symbol), 0.0), 2)
        if bucket == "actionable_now" and portfolio_weight_pct <= 0:
            portfolio_weight_pct = round(min(4.0, max(1.5, _f(row.get("selectionScore"), 0.0) / 20.0)), 2)

        evaluated.append(
            {
                "symbol": symbol,
                "name": _s(info.get("name") or symbol),
                "sector": _s(info.get("sector") or rebalance_hints.get(symbol, {}).get("sector")),
                "selectionScore": round(_f(row.get("selectionScore"), 0.0), 2),
                "rebalanceSelected": symbol in selected_symbols,
                "latestClosePrice": latest_price,
                "latestCloseAsOf": _s(row.get("latestCloseAsOf")),
                "chartState": _s(row.get("chartState")),
                "volumeRatio": round(_f(row.get("volumeRatio"), 0.0), 2),
                "rsi": round(_f(row.get("rsi"), 0.0), 1),
                "adx": round(_f(row.get("adx"), 0.0), 1),
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
                "actionBucket": bucket,
                "actionReason": bucket_reason,
                "portfolioWeightPct": portfolio_weight_pct,
                "warnings": warnings,
            }
        )
    timings["evaluateSec"] = round(time.perf_counter() - eval_started, 3)

    synth_started = time.perf_counter()
    final_synthesis = _final_trade_synthesis(evaluated, market_ctx, fear_greed, analysis_limit)
    timings["finalSynthesisSec"] = round(time.perf_counter() - synth_started, 3)
    if isinstance(final_synthesis, dict) and final_synthesis.get("error"):
        payload = {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "available": False,
            "reason": "codex_final_synthesis_failed",
            "detail": f"{_s(final_synthesis.get('error'))} | model={_s(final_synthesis.get('model') or ai.model)} | reasoning={_s(final_synthesis.get('reasoningEffort') or ai.reasoning_effort)}",
            "aiModel": ai.model,
            "aiReasoningEffort": ai.reasoning_effort,
            "newsAnalysisLimit": analysis_limit,
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
    evaluated = _apply_final_synthesis(evaluated, final_synthesis)

    evaluated.sort(
        key=lambda row: (
            _rank_priority(_s(row.get("actionBucket"))),
            _f(row.get("finalRank"), 9999.0),
            -_f(row.get("rrToTp1"), -999.0),
            -_f(row.get("selectionScore"), 0.0),
            _s(row.get("symbol")),
        )
    )
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
        "universeScannedCount": len(scanned_rows),
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

    strict_buyable = sorted([row for row in evaluated if _strict_chart_buyable(row)], key=_chart_buyable_key)
    buyable = sorted([row for row in evaluated if _chart_buyable(row)], key=_chart_buyable_key)
    wait_pullback = sorted([row for row in evaluated if _chart_wait(row)], key=_chart_wait_key)
    leaders = sorted(evaluated, key=_chart_leader_key)
    overextended = [
        row
        for row in leaders
        if _f(row.get("rewardToTp1Pct"), 0.0) <= 2.0
        or "entry_negative" in (row.get("warnings") or [])
        or _s(row.get("tradeVerdict")) != "marketable_now"
    ]
    avoid = [row for row in evaluated if _s(row.get("tradeVerdict")) == "avoid"]
    latest_dates = [_s(row.get("latestCloseAsOf")) for row in evaluated if _s(row.get("latestCloseAsOf"))]
    cash_pct = round(max(0.0, 100.0 - sum(_f(row.get("portfolioWeightPct"), 0.0) for row in strict_buyable[:8])), 2)

    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "available": True,
        "mode": "chart-only",
        "schemaVersion": CHART_SCHEMA_VERSION,
        "rebalanceSourceFile": str(rebalance_path) if rebalance_path is not None else "",
        "rebalanceGeneratedAt": _s(rebalance.get("generated_at")),
        "universeSymbolCount": len(_load_all_us_symbols()),
        "universeScannedCount": len(scanned_rows),
        "latestCloseMin": min(latest_dates) if latest_dates else "",
        "latestCloseMax": max(latest_dates) if latest_dates else "",
        "strictBuyable": strict_buyable,
        "actionableNow": strict_buyable,
        "buyable": buyable,
        "waitPullback": wait_pullback,
        "avoid": avoid,
        "chartLeaders": leaders[:30],
        "overextended": overextended[:30],
        "all": evaluated,
        "summary": {
            "strictBuyableCount": len(strict_buyable),
            "actionableCount": len(strict_buyable),
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
    entry = _format_price(row.get("averageEntryPrice"))
    stop = _format_price(row.get("closeStopPrice"))
    tp1 = _format_price(row.get("tp1Price"))
    weight = _format_pct(row.get("portfolioWeightPct"))
    return f"<b>{symbol}</b>  {price} | 진입 {entry} | 손절 {stop} | 1차 {tp1} | 비중 {weight}"


def _compact_row(row: dict[str, Any]) -> str:
    symbol = escape(_s(row.get("symbol")))
    price = _format_price(row.get("latestClosePrice"))
    entry = _format_price(row.get("averageEntryPrice"))
    tp1 = _format_price(row.get("tp1Price"))
    return f"<b>{symbol}</b>  {price} | 진입 {entry} | 1차 {tp1}"


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
        "<b>Autostock Trade Desk</b>",
        f"<code>{escape(_fmt_asof(payload.get('generatedAt')))}</code>",
        f"시장 {_s(market_condition.get('message'))} | 공포탐욕 {fear_greed.get('score', '-')}",
        f"차트 스캔 {payload.get('universeScannedCount', '-')} | 뉴스/Codex {payload.get('newsAnalyzedCount', '-')}",
        f"모델 {escape(_s(payload.get('aiModel') or ai.model))} / {escape(_s(payload.get('aiReasoningEffort') or ai.reasoning_effort))}",
        f"즉시 {summary.get('actionableCount', 0)} | 대기 {summary.get('waitPullbackCount', 0)} | 제외 {summary.get('avoidCount', 0)} | 현금 {_format_pct(summary.get('cashPct'))}",
    ]
    timing = _timing_line(payload)
    if timing:
        header.append(timing)
    header.append("")

    if view == "summary":
        lines = header[:]
        if _s(final_synthesis.get("summary")):
            lines.extend(["<b>최종 종합</b>", escape(_s(final_synthesis.get("summary"))), ""])
        lines.append("<b>지금 진입 가능</b>")
        if actionable:
            lines.extend(_compact_row(row) for row in actionable[:3])
        else:
            lines.append("없음")
        lines.append("")
        lines.append("<b>눌림 대기</b>")
        if wait_pullback:
            lines.extend(_compact_row(row) for row in wait_pullback[:5])
        else:
            lines.append("없음")
        lines.append("")
        lines.append("<b>추격 금지</b>")
        if avoid:
            lines.extend(f"{escape(_s(row.get('symbol')))}  {_s(row.get('actionReason') or row.get('tradeReason'))}" for row in avoid[:3])
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "actionable":
        lines = header + ["<b>즉시 매수 후보</b>"]
        if actionable:
            for row in actionable[:8]:
                lines.append(_one_line_row(row))
                lines.append(
                    f"뉴스 {escape(_s(row.get('newsSignal')))} / {escape(_s(row.get('newsStrength')))} | RR1 {_f(row.get('rrToTp1')):.2f} | RR2 {_f(row.get('rrToTp2')):.2f}"
                )
                lines.append(escape(_s(row.get("finalCatalystSummary") or row.get("newsHeadline") or row.get("actionReason"))))
                if _s(row.get("finalRiskNote")):
                    lines.append(f"리스크: {escape(_s(row.get('finalRiskNote')))}")
                lines.append("")
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "wait":
        lines = header + ["<b>눌림 대기 후보</b>"]
        if wait_pullback:
            for row in wait_pullback[:10]:
                lines.append(_one_line_row(row))
                lines.append(
                    f"뉴스 {escape(_s(row.get('newsSignal')))} / {escape(_s(row.get('newsStrength')))} | RR2 {_f(row.get('rrToTp2')):.2f} | 이유 {escape(_s(row.get('actionReason')))}"
                )
                if _s(row.get("finalCatalystSummary")):
                    lines.append(escape(_s(row.get("finalCatalystSummary"))))
                lines.append("")
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "avoid":
        lines = header + ["<b>지금 제외</b>"]
        if avoid:
            for row in avoid[:10]:
                lines.append(f"<b>{escape(_s(row.get('symbol')))}</b> | {escape(_s(row.get('newsSignal')))} / {escape(_s(row.get('newsStrength')))}")
                lines.append(escape(_s(row.get("finalRiskNote") or row.get("finalCatalystSummary") or row.get("newsHeadline") or row.get("actionReason") or row.get("tradeReason"))))
                lines.append("")
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "portfolio":
        lines = header + ["<b>추천 포트폴리오</b>", f"현금 {_format_pct(summary.get('cashPct'))}"]
        if actionable:
            for row in actionable[:10]:
                lines.append(
                    f"{escape(_s(row.get('symbol')))}  {_format_pct(row.get('portfolioWeightPct'))}  | 진입 {_format_price(row.get('averageEntryPrice'))} | 손절 {_format_price(row.get('closeStopPrice'))}"
                )
                if _s(row.get("actionReason")):
                    lines.append(f"이유: {escape(_s(row.get('actionReason')))}")
        else:
            lines.append("즉시 편입 후보가 없어 현금 대기가 기본입니다.")
            for row in wait_pullback[:5]:
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
        f"RR1 {_f(row.get('rrToTp1')):.2f} | "
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
        f"조건통과 {summary.get('strictBuyableCount', 0)} | 후보 {summary.get('buyableCount', 0)} | 대기 {summary.get('waitPullbackCount', 0)} | 추격금지 {summary.get('overextendedCount', 0)}",
        f"권장 현금 {_format_pct(summary.get('cashPct'))}",
    ]
    timing = _timing_line(payload)
    if timing:
        header.append(timing)
    header.append("")

    if view == "summary":
        lines = header + ["<b>조건 통과 진입 가능</b>"]
        if strict_buyable:
            lines.extend(_compact_row(row) for row in strict_buyable[:5])
        else:
            lines.append("없음")
        lines.append("")
        lines.append("<b>차트 후보</b>")
        if buyable:
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
        lines = header + ["<b>조건 통과 진입 가능</b>"]
        if strict_buyable:
            for row in strict_buyable[:10]:
                lines.append(_one_line_row(row))
                lines.append(_chart_reason(row))
                lines.append("")
        else:
            lines.append("없음")
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
            lines.append("조건 통과 진입 후보가 없어 현금 대기가 기본입니다.")
            for row in buyable[:5]:
                lines.append(
                    f"{escape(_s(row.get('symbol')))}  후보 | 진입 {_format_price(row.get('averageEntryPrice'))} | 손절 {_format_price(row.get('closeStopPrice'))}"
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
