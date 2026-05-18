from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Any

from ai.analyzer import ai
from core.data_collector import DataCollector


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "outputs" / "telegram"
CACHE_PATH = OUTPUT_ROOT / "universe_trade_analysis.json"
CHART_CACHE_PATH = OUTPUT_ROOT / "current_chart_analysis_full.json"
CHART_SCHEMA_VERSION = "chart-structure-v4"
TRADE_CACHE_SCHEMA_VERSION = "ai-evidence-v7"
REBALANCE_ROOT = ROOT / "data" / "rebalance"
_DATA_COLLECTOR = DataCollector(root=ROOT)


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
        return max(
            10,
            int(
                os.getenv(
                    "TELEGRAM_RESEARCH_ANALYSIS_MAX_SYMBOLS",
                    os.getenv("TELEGRAM_NEWS_ANALYSIS_MAX_SYMBOLS", os.getenv("TELEGRAM_ANALYSIS_MAX_SYMBOLS", "120")),
                )
            ),
        )
    except Exception:
        return 120


def full_news_analysis_limit() -> int:
    return max(_analysis_limit(), len(_load_all_us_symbols()))


def _final_synthesis_max_symbols() -> int:
    try:
        return max(10, int(os.getenv("TELEGRAM_FINAL_SYNTHESIS_MAX_SYMBOLS", "240")))
    except Exception:
        return 240


def _codex_batch_size() -> int:
    try:
        return max(4, int(os.getenv("TELEGRAM_CODEX_BATCH_SIZE", "12")))
    except Exception:
        return 12


def _codex_news_batch_workers() -> int:
    try:
        return max(1, min(8, int(os.getenv("TELEGRAM_CODEX_NEWS_BATCH_WORKERS", "2"))))
    except Exception:
        return 2


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


def _load_all_us_symbols() -> list[str]:
    return _DATA_COLLECTOR.load_all_us_symbols()


def _apply_realtime_volume(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _DATA_COLLECTOR.apply_realtime_volume(rows)


def _refresh_payload_realtime_volume(payload: dict[str, Any]) -> dict[str, Any]:
    return _DATA_COLLECTOR.refresh_payload_realtime_volume(payload)


def _scan_full_universe(rebalance_hints: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return _scan_symbols(_load_all_us_symbols(), rebalance_hints)


def _scan_symbols(symbols: list[str], rebalance_hints: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return _DATA_COLLECTOR.scan_price_rows(symbols, rebalance_hints)


def _collect_news_bundles(
    news_symbols: list[str],
    chart_rows_by_symbol: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    return _DATA_COLLECTOR.collect_news_bundles(news_symbols, chart_rows_by_symbol)


def _bundle_has_news(bundle: dict[str, Any]) -> bool:
    events = bundle.get("events") if isinstance(bundle.get("events"), list) else []
    next_events = bundle.get("nextEvents") if isinstance(bundle.get("nextEvents"), list) else []
    return bool(events or next_events)


def _scan_fundamentals(symbols: list[str]) -> list[dict[str, Any]]:
    return _DATA_COLLECTOR.scan_fundamentals(symbols)


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


def _reference_decision(
    row: dict[str, Any],
    news: dict[str, Any] | None = None,
) -> dict[str, Any]:
    assessment = _valuation_assessment(row)
    news_signal = _s((news or {}).get("signal")).lower()
    news_strength = _s((news or {}).get("strength")).lower()
    if news_signal:
        reason = f"Codex 뉴스 요약 {news_signal}/{news_strength or 'none'}; 최종 판단 대기"
    else:
        reason = "뉴스/SEC 촉매 없음; 최종 판단 대기"

    reasons = [
        *[str(item) for item in assessment.get("valuationReasons") or []],
        f"RR {_f(row.get('rrToTp1')):.2f}, TP1 여력 {_f(row.get('rewardToTp1Pct')):.2f}%, 손절 리스크 {_f(row.get('riskToStopPct')):.2f}%",
        f"RS63 {_f(row.get('relativeStrength63dPct')):.2f}% vs {row.get('benchmarkSymbol') or 'benchmark'}, RSI {_f(row.get('rsi')):.1f}",
    ]
    chart_structure = row.get("chartStructure") if isinstance(row.get("chartStructure"), dict) else {}
    if chart_structure:
        reasons.append(f"차트 구조 {chart_structure.get('state') or row.get('chartState')}")
    if news_signal:
        reasons.append(f"뉴스 {news_signal}/{news_strength}: {_s((news or {}).get('headline'))}")
    return {
        **assessment,
        "decisionState": "REFERENCE_ONLY",
        "actionBucket": "reference_only",
        "actionReason": reason,
        "portfolioWeightPct": 0.0,
        "decisionReasons": [item for item in reasons if item][:8],
    }


def _value_rank_key(row: dict[str, Any]) -> tuple[int, float, float, str]:
    bucket_rank = {
        "actionable_now": 0,
        "wait_pullback": 1,
        "avoid": 2,
        "reference_only": 3,
    }.get(_s(row.get("actionBucket")), 9)
    return (
        bucket_rank,
        -_f(row.get("portfolioWeightPct"), 0.0),
        -_f(row.get("marketCap"), 0.0),
        _s(row.get("symbol")),
    )


def _compact_dict(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if value not in (None, "", [], {})}


def _data_quality_flags(row: dict[str, Any]) -> list[str]:
    flags = row.get("dataQualityFlags") if isinstance(row.get("dataQualityFlags"), list) else []
    quality = row.get("priceDataQuality") if isinstance(row.get("priceDataQuality"), dict) else {}
    quality_flags = quality.get("flags") if isinstance(quality.get("flags"), list) else []
    return sorted({str(item) for item in [*flags, *quality_flags] if _s(item)})


def _compact_price_quality(row: dict[str, Any]) -> dict[str, Any]:
    quality = row.get("priceDataQuality") if isinstance(row.get("priceDataQuality"), dict) else {}
    checks = quality.get("checks") if isinstance(quality.get("checks"), list) else []
    return _compact_dict(
        {
            "status": quality.get("status"),
            "primarySource": quality.get("primarySource"),
            "secondarySource": quality.get("secondarySource"),
            "flags": _data_quality_flags(row)[:8],
            "note": quality.get("note"),
            "checks": [
                _compact_dict(
                    {
                        "field": check.get("field"),
                        "primaryValue": check.get("primaryValue"),
                        "secondaryValue": check.get("secondaryValue"),
                        "diffPct": check.get("diffPct"),
                        "status": check.get("status"),
                    }
                )
                for check in checks[:8]
                if isinstance(check, dict)
            ],
        }
    )


def _compact_fundamental_item(row: dict[str, Any]) -> dict[str, Any]:
    return _compact_dict(
        {
            "symbol": _s(row.get("symbol")).upper(),
            "name": _s(row.get("name")),
            "sector": _s(row.get("sector")),
            "industry": _s(row.get("industry")),
            "marketCap": row.get("marketCap"),
            "latestClosePrice": row.get("latestClosePrice"),
            "pFcf": row.get("pFcf"),
            "fcfYieldPct": row.get("fcfYieldPct"),
            "pe": row.get("pe"),
            "forwardPe": row.get("forwardPe"),
            "pb": row.get("pb"),
            "peg": row.get("peg"),
            "roePct": row.get("roePct"),
            "profitMarginPct": row.get("profitMarginPct"),
            "operatingMarginPct": row.get("operatingMarginPct"),
            "revenueGrowthPct": row.get("revenueGrowthPct"),
            "earningsGrowthPct": row.get("earningsGrowthPct"),
            "forwardEpsGrowthPct": row.get("forwardEpsGrowthPct"),
            "debtToEquity": row.get("debtToEquity"),
            "currentRatio": row.get("currentRatio"),
            "targetUpsidePct": row.get("targetUpsidePct"),
            "analystCount": row.get("analystCount"),
            "recommendation": _s(row.get("recommendation")),
        }
    )


def _compact_market_context(market_bundle: dict[str, Any]) -> dict[str, Any]:
    market_condition = market_bundle.get("marketCondition") if isinstance(market_bundle.get("marketCondition"), dict) else {}
    fear_greed = market_bundle.get("fearGreed") if isinstance(market_bundle.get("fearGreed"), dict) else {}
    macro = market_bundle.get("macro") if isinstance(market_bundle.get("macro"), dict) else {}
    options_market = market_bundle.get("optionsMarket") if isinstance(market_bundle.get("optionsMarket"), dict) else {}
    market_regime = market_bundle.get("marketRegime") if isinstance(market_bundle.get("marketRegime"), dict) else {}
    series = macro.get("series") if isinstance(macro.get("series"), dict) else {}
    ratios = options_market.get("ratios") if isinstance(options_market.get("ratios"), dict) else {}
    benchmarks = market_regime.get("benchmarks") if isinstance(market_regime.get("benchmarks"), dict) else {}
    compact_benchmarks = {
        symbol: _compact_dict(
            {
                "latestClosePrice": row.get("latestClosePrice"),
                "dayReturnPct": row.get("dayReturnPct"),
                "return5dPct": row.get("return5dPct"),
                "return21dPct": row.get("return21dPct"),
                "return63dPct": row.get("return63dPct"),
                "ma50GapPct": row.get("ma50GapPct"),
                "ma200GapPct": row.get("ma200GapPct"),
                "structureState": row.get("structureState"),
                "nearestSupportZone": row.get("nearestSupportZone"),
                "breakdowns": row.get("breakdowns")[:3] if isinstance(row.get("breakdowns"), list) else [],
            }
        )
        for symbol, row in benchmarks.items()
        if isinstance(row, dict)
    }
    return _compact_dict(
        {
            "marketCondition": _compact_dict(
                {
                    "message": market_condition.get("message"),
                    "score": market_condition.get("score"),
                    "state": market_condition.get("state"),
                }
            ),
            "fearGreed": _compact_dict(
                {
                    "score": fear_greed.get("score"),
                    "rating": fear_greed.get("rating"),
                }
            ),
            "macro": {
                key: _compact_dict(
                    {
                        "date": value.get("date"),
                        "value": value.get("value"),
                        "change": value.get("change"),
                    }
                )
                for key, value in series.items()
                if isinstance(value, dict)
            },
            "options": _compact_dict(
                {
                    "asOf": options_market.get("asOf"),
                    "totalPutCallRatio": ratios.get("total_put_call_ratio"),
                    "equityPutCallRatio": ratios.get("equity_put_call_ratio"),
                    "indexPutCallRatio": ratios.get("index_put_call_ratio"),
                }
            ),
            "marketRegime": _compact_dict(
                {
                    "label": market_regime.get("regimeLabel"),
                    "primaryBenchmark": market_regime.get("primaryBenchmark"),
                    "benchmarks": compact_benchmarks,
                    "evidence": market_regime.get("evidence"),
                }
            ),
        }
    )


def _compact_chart_structure(row: dict[str, Any]) -> dict[str, Any]:
    structure = row.get("chartStructure") if isinstance(row.get("chartStructure"), dict) else {}
    moving_averages = structure.get("movingAverages") if isinstance(structure.get("movingAverages"), dict) else {}
    swing_structure = structure.get("swingStructure") if isinstance(structure.get("swingStructure"), dict) else {}
    return _compact_dict(
        {
            "state": structure.get("state") or row.get("chartState"),
            "movingAverageStack": moving_averages.get("stack"),
            "ma20GapPct": moving_averages.get("ma20GapPct") or row.get("ma20Gap"),
            "ma50GapPct": moving_averages.get("ma50GapPct") or row.get("ma50Gap"),
            "ma200GapPct": moving_averages.get("ma200GapPct") or row.get("ma200Gap"),
            "swingPhase": swing_structure.get("phase"),
            "highPattern": swing_structure.get("highPattern"),
            "lowPattern": swing_structure.get("lowPattern"),
            "nearestSupportZone": structure.get("nearestSupportZone") or row.get("nearestSupportZone"),
            "nearestResistanceZone": structure.get("nearestResistanceZone") or row.get("nearestResistanceZone"),
            "breakdowns": structure.get("breakdowns")[:4] if isinstance(structure.get("breakdowns"), list) else [],
            "retests": structure.get("retests")[:3] if isinstance(structure.get("retests"), list) else [],
            "riskMap": structure.get("riskMap") if isinstance(structure.get("riskMap"), dict) else {},
        }
    )


def _compact_evidence_item(row: dict[str, Any]) -> dict[str, Any]:
    return _compact_dict(
        {
            "symbol": _s(row.get("symbol")).upper(),
            "name": _s(row.get("name")),
            "sector": _s(row.get("sector")),
            "industry": _s(row.get("industry")),
            "rebalanceSelected": bool(row.get("rebalanceSelected")),
            "existingPortfolioWeightPct": row.get("existingPortfolioWeightPct"),
            "marketCap": row.get("marketCap"),
            "latestClosePrice": row.get("latestClosePrice"),
            "latestCloseAsOf": _s(row.get("latestCloseAsOf")),
            "pFcf": row.get("pFcf"),
            "fcfYieldPct": row.get("fcfYieldPct"),
            "pe": row.get("pe"),
            "forwardPe": row.get("forwardPe"),
            "pb": row.get("pb"),
            "peg": row.get("peg"),
            "roePct": row.get("roePct"),
            "profitMarginPct": row.get("profitMarginPct"),
            "operatingMarginPct": row.get("operatingMarginPct"),
            "revenueGrowthPct": row.get("revenueGrowthPct"),
            "earningsGrowthPct": row.get("earningsGrowthPct"),
            "forwardEpsGrowthPct": row.get("forwardEpsGrowthPct"),
            "debtToEquity": row.get("debtToEquity"),
            "currentRatio": row.get("currentRatio"),
            "targetUpsidePct": row.get("targetUpsidePct"),
            "analystCount": row.get("analystCount"),
            "recommendation": _s(row.get("recommendation")),
            "return21d": row.get("return21d"),
            "return63d": row.get("return63d"),
            "relativeStrength21dPct": row.get("relativeStrength21dPct"),
            "relativeStrength63dPct": row.get("relativeStrength63dPct"),
            "benchmarkSymbol": _s(row.get("benchmarkSymbol")),
            "dayReturnPct": row.get("dayReturnPct"),
            "gapPct": row.get("gapPct"),
            "intradayReturnPct": row.get("intradayReturnPct"),
            "dayRangePct": row.get("dayRangePct"),
            "closeLocationPct": row.get("closeLocationPct"),
            "ma20Gap": row.get("ma20Gap"),
            "ma50Gap": row.get("ma50Gap"),
            "ma200Gap": row.get("ma200Gap"),
            "rsi": row.get("rsi"),
            "adx": row.get("adx"),
            "volumeRatio": row.get("volumeRatio"),
            "latestDailyVolume": row.get("latestDailyVolume"),
            "realtimeVolume": row.get("realtimeVolume"),
            "dollarVolumeAvg20": row.get("dollarVolumeAvg20"),
            "dollarVolumeRealtime": row.get("dollarVolumeRealtime"),
            "priceDataQuality": _compact_price_quality(row),
            "averageEntryPrice": row.get("averageEntryPrice"),
            "closeStopPrice": row.get("closeStopPrice"),
            "tp1Price": row.get("tp1Price"),
            "tp2Price": row.get("tp2Price"),
            "stopBasis": row.get("stopBasis"),
            "targetBasis": row.get("targetBasis"),
            "riskToStopPct": row.get("riskToStopPct"),
            "rewardToTp1Pct": row.get("rewardToTp1Pct"),
            "rrToTp1": row.get("rrToTp1"),
            "tradeVerdict": _s(row.get("tradeVerdict")),
            "tradeReason": _s(row.get("tradeReason")),
            "newsSignal": _s(row.get("newsSignal")),
            "newsStrength": _s(row.get("newsStrength")),
            "newsHeadline": _s(row.get("newsHeadline")),
            "newsReasons": row.get("newsReasons")[:3] if isinstance(row.get("newsReasons"), list) else [],
            "eventHeadlines": row.get("eventHeadlines")[:3] if isinstance(row.get("eventHeadlines"), list) else [],
            "nextEvents": row.get("nextEvents")[:2] if isinstance(row.get("nextEvents"), list) else [],
            "shortVolumePct": row.get("shortVolumePct"),
            "shortVolumeAsOf": _s(row.get("shortVolumeAsOf")),
            "chartStructure": _compact_chart_structure(row),
        }
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


def _symbols_from_json_value(value: Any) -> list[str] | None:
    raw_rows: Any = None
    if isinstance(value, dict):
        for key in ("symbols", "items", "selectedSymbols", "candidates", "data"):
            if isinstance(value.get(key), list):
                raw_rows = value.get(key)
                break
    elif isinstance(value, list):
        raw_rows = value

    if not isinstance(raw_rows, list):
        return None

    symbols: list[str] = []
    for item in raw_rows:
        symbol = _s(item.get("symbol") if isinstance(item, dict) else item).upper()
        if symbol:
            symbols.append(symbol)
    return symbols if symbols else None


def _extract_symbol_selection(text: str, expected_symbols: set[str]) -> list[str] | None:
    value = ai._extract_json_value(text)
    symbols = _symbols_from_json_value(value)
    if symbols:
        filtered = [symbol for symbol in symbols if symbol in expected_symbols]
        if filtered:
            return filtered

    cleaned = ai._clean_json_text(text)
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(cleaned):
        if ch not in "{[":
            continue
        try:
            candidate, _ = decoder.raw_decode(cleaned[idx:])
        except Exception:
            continue
        symbols = _symbols_from_json_value(candidate)
        if symbols:
            filtered = [symbol for symbol in symbols if symbol in expected_symbols]
            if filtered:
                return filtered
    return None


def _decision_rows_from_json_value(value: Any) -> list[dict[str, Any]] | None:
    if isinstance(value, dict):
        for key in ("items", "decisions", "results", "data"):
            rows = value.get(key)
            if isinstance(rows, list):
                parsed = [row for row in rows if isinstance(row, dict)]
                return parsed if parsed else []
        if _s(value.get("symbol")) and (_s(value.get("actionBucket")) or _s(value.get("decisionState"))):
            return [value]
        mapped_rows: list[dict[str, Any]] = []
        for symbol, row in value.items():
            if not isinstance(row, dict):
                continue
            parsed = dict(row)
            parsed.setdefault("symbol", symbol)
            if _s(parsed.get("symbol")) and (_s(parsed.get("actionBucket")) or _s(parsed.get("decisionState"))):
                mapped_rows.append(parsed)
        return mapped_rows if mapped_rows else None

    if isinstance(value, list):
        rows = [row for row in value if isinstance(row, dict)]
        return rows if rows else []
    return None


def _extract_trade_decisions(text: str, expected_symbols: set[str]) -> list[dict[str, Any]] | None:
    value = ai._extract_json_value(text)
    rows = _decision_rows_from_json_value(value)
    if rows is not None:
        return [row for row in rows if _s(row.get("symbol")).upper() in expected_symbols]

    cleaned = ai._clean_json_text(text)
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(cleaned):
        if ch not in "{[":
            continue
        try:
            candidate, _ = decoder.raw_decode(cleaned[idx:])
        except Exception:
            continue
        rows = _decision_rows_from_json_value(candidate)
        if rows is not None:
            return [row for row in rows if _s(row.get("symbol")).upper() in expected_symbols]
    return None


def _select_research_symbols(
    fundamental_rows: list[dict[str, Any]],
    *,
    selected_symbols: set[str],
    limit: int,
    market_bundle: dict[str, Any],
) -> dict[str, Any]:
    if not fundamental_rows:
        return {"symbols": []}
    if not ai.has_api_access:
        return {"error": "codex_symbol_selection_unavailable", "model": ai.model, "reasoningEffort": ai.reasoning_effort}

    known_symbols = {_s(row.get("symbol")).upper() for row in fundamental_rows if _s(row.get("symbol"))}
    items = [_compact_fundamental_item(row) for row in sorted(fundamental_rows, key=lambda item: _s(item.get("symbol")))]
    prompt_items = json.dumps(items, ensure_ascii=False, separators=(",", ":"))
    selected_items = sorted(symbol for symbol in selected_symbols if symbol in known_symbols)
    market_context = json.dumps(_compact_market_context(market_bundle), ensure_ascii=False, separators=(",", ":"))
    prompt = (
        "You are selecting public equities for deeper trade/investment research.\n"
        "Output STRICT JSON only.\n"
        "Do not use fixed numeric thresholds, point scores, pass/fail gates, or forced sector quotas.\n"
        "Treat numbers as context and compare candidates relative to the provided universe.\n"
        "Do not favor mega-cap stocks just because they are large; prefer names where fundamentals, valuation, quality, growth, "
        "balance-sheet risk, analyst context, and review value together justify deeper chart/news collection.\n"
        "It is okay to return fewer symbols than the requested limit. Include currently held/rebalance-selected symbols only when "
        "they still deserve review or need risk monitoring.\n\n"
        f"Maximum symbols to select: {int(limit)}\n"
        f"Current/rebalance symbols: {json.dumps(selected_items, ensure_ascii=False)}\n"
        f"Market context JSON: {market_context}\n"
        f"Universe fundamentals JSON: {prompt_items}\n\n"
        'Return JSON only in this shape: {"symbols":["AAPL","MSFT"],"rationale":"one short Korean sentence"}'
    )
    text = ai._call(prompt, max_tokens=5000)
    if not text:
        return {"error": "codex_symbol_selection_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}
    symbols = _extract_symbol_selection(text, known_symbols)
    if symbols is None:
        return {"error": "codex_symbol_selection_json_parse_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}

    out: list[str] = []
    seen: set[str] = set()
    for symbol in [*selected_items, *symbols]:
        if symbol not in known_symbols or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
        if len(out) >= limit:
            break
    return {"symbols": out, "requestedLimit": limit, "selectedByCodexCount": len(symbols)}


def _normalize_action_bucket(value: Any) -> str:
    raw = _s(value).lower()
    mapping = {
        "actionable": "actionable_now",
        "actionable_now": "actionable_now",
        "buy": "actionable_now",
        "buy_now": "actionable_now",
        "immediate": "actionable_now",
        "wait": "wait_pullback",
        "wait_pullback": "wait_pullback",
        "pullback": "wait_pullback",
        "hold_for_pullback": "wait_pullback",
        "reference": "reference_only",
        "reference_only": "reference_only",
        "watch": "reference_only",
        "monitor": "reference_only",
        "avoid": "avoid",
        "exclude": "avoid",
        "sell": "avoid",
    }
    if raw in mapping:
        return mapping[raw]
    if raw in {"actionable", "reference_only", "wait_pullback", "avoid"}:
        return raw
    return "reference_only"


def _decision_state_for_bucket(bucket: str) -> str:
    return {
        "actionable_now": "ACTIONABLE",
        "wait_pullback": "WAIT_PULLBACK",
        "avoid": "AVOID",
        "reference_only": "REFERENCE_ONLY",
    }.get(bucket, "REFERENCE_ONLY")


def _default_reason_for_bucket(bucket: str) -> str:
    return {
        "actionable_now": "Codex 최종 종합에서 현재 편입 후보로 판단",
        "wait_pullback": "투자 근거는 있으나 현재 진입 구조는 대기",
        "avoid": "Codex 최종 종합에서 현재 제외가 낫다고 판단",
        "reference_only": "편입/제외 판단 근거가 충분하지 않아 참고 유지",
    }.get(bucket, "편입/제외 판단 근거가 충분하지 않아 참고 유지")


def _action_profile_flags(row: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    for flag in _data_quality_flags(row):
        flags.append(f"price_data:{flag}")
    if row.get("relativeStrength63dPct") is not None and _f(row.get("relativeStrength63dPct")) < 0:
        flags.append("negative_63d_relative_strength")
    if row.get("ma200Gap") is not None and _f(row.get("ma200Gap")) < 0:
        flags.append("below_200dma")
    chart_state = _s(row.get("chartState") or (row.get("chartStructure") or {}).get("state"))
    if chart_state in {"breakdown_or_distribution", "downtrend"}:
        flags.append(f"chart_{chart_state}")
    risk = row.get("riskToStopPct")
    if risk is None or _f(risk) <= 0:
        flags.append("incomplete_stop_risk")
    return sorted({flag for flag in flags if flag})


def _apply_action_profiles(evaluated: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for row in evaluated:
        bucket = _s(row.get("actionBucket"))
        weight = _round_optional(row.get("portfolioWeightPct"), 2) or 0.0
        flags = _action_profile_flags(row)
        if bucket == "actionable_now":
            if weight <= 1.0 or flags:
                profile = "small_tactical_entry"
                label = "소형 1차 전술 진입"
                max_initial = round(min(weight if weight > 0 else 1.0, 1.0), 2)
            elif weight <= 2.0:
                profile = "starter_entry"
                label = "1차 진입"
                max_initial = weight
            else:
                profile = "core_entry"
                label = "일반 편입"
                max_initial = weight
            row["entryDiscipline"] = {
                "maxInitialWeightPct": max_initial,
                "requiresPriceDataCheck": any(flag.startswith("price_data:") for flag in flags),
                "addOnlyOn": [
                    "눌림 지지 확인",
                    "200일선 회복 또는 상대강도 개선",
                ]
                if profile == "small_tactical_entry"
                else [],
                "avoidChase": "급등 직후 1차 저항 접근 구간 추격 금지" if profile == "small_tactical_entry" else "",
            }
        elif bucket == "wait_pullback":
            profile = "wait_pullback"
            label = "눌림 대기"
        elif bucket == "avoid":
            profile = "avoid"
            label = "제외"
        else:
            profile = "reference_only"
            label = "참고"
        row["actionProfile"] = profile
        row["actionProfileLabel"] = label
        row["actionProfileFlags"] = flags
        counts[profile] = counts.get(profile, 0) + 1
    return {"counts": counts}


def _apply_final_synthesis(evaluated: list[dict[str, Any]], market_bundle: dict[str, Any]) -> dict[str, Any]:
    if not evaluated:
        return {"items": [], "symbolCount": 0}
    if not ai.has_api_access:
        return {"error": "codex_final_synthesis_unavailable", "model": ai.model, "reasoningEffort": ai.reasoning_effort}

    max_symbols = _final_synthesis_max_symbols()
    rows = evaluated[:max_symbols]
    expected_symbols = {_s(row.get("symbol")).upper() for row in rows if _s(row.get("symbol"))}
    prompt_items = json.dumps([_compact_evidence_item(row) for row in rows], ensure_ascii=False, separators=(",", ":"))
    market_context = json.dumps(_compact_market_context(market_bundle), ensure_ascii=False, separators=(",", ":"))
    prompt = (
        "You are the final trade/investment synthesis layer for a US equity assistant.\n"
        "Write concise Korean reasons, but output STRICT JSON only.\n"
        "Do not use fixed numeric thresholds, point scores, mechanical pass/fail gates, or forced counts.\n"
        "Use the numeric fields only as evidence. Compare each symbol contextually against the rest of the evidence, "
        "its sector, market condition, valuation/quality/growth balance, news or SEC risk, relative strength, liquidity, "
        "market regime, benchmark breakdown evidence, chart support/resistance zones, trend-line structure, retests, "
        "and entry/risk/reward structure.\n"
        "It is okay to return no actionable symbols. Do not promote mega-caps merely because they are familiar or liquid.\n"
        "In risk-off or stressed regimes, prefer wait_pullback/reference_only unless the symbol clearly reconciles adverse "
        "facts such as weak relative strength, price below long-term trend, nearby resistance, and limited reward/risk.\n"
        "A quality company can still be a bad present entry; do not mark it actionable merely because the business is good.\n"
        "Respect priceDataQuality. If OHLC verification is recommended or cross-source mismatches exist, treat tight stops, "
        "same-day lows/highs, and reward/risk as provisional. Severe high/low mismatches should usually mean wait_pullback "
        "until the regular-session OHLC is verified. A high-volatility single-source day may still be actionable only as a "
        "small tactical starter, with conservative sizing and an explicit reason.\n"
        "Choose actionBucket values only from: actionable_now, wait_pullback, reference_only, avoid.\n"
        "Use actionable_now only when the combined evidence supports a present entry/investment. Use wait_pullback when the "
        "case is interesting but entry structure is not attractive now. Use avoid when risk or valuation/catalyst damage dominates. "
        "Use reference_only when evidence is ordinary, stale, incomplete, or not differentiated.\n"
        "portfolioWeightPct is a suggested portfolio percent for actionable_now only; use 0 for other buckets. Keep sizing conservative "
        "and do not force the portfolio to be fully invested.\n"
        "You may omit pure reference_only symbols from the output; omitted symbols will remain reference_only.\n\n"
        f"Market context JSON: {market_context}\n"
        f"Evidence JSON: {prompt_items}\n\n"
        "Return JSON only in this shape:\n"
        '{"items":[{"symbol":"AAPL","actionBucket":"actionable_now|wait_pullback|reference_only|avoid","portfolioWeightPct":2.5,'
        '"actionReason":"short Korean reason","decisionReasons":["Korean reason 1","Korean reason 2"]}]}'
    )
    text = ai._call(prompt, max_tokens=7000)
    if not text:
        return {"error": "codex_final_synthesis_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}
    decisions = _extract_trade_decisions(text, expected_symbols)
    if decisions is None:
        return {"error": "codex_final_synthesis_json_parse_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}

    by_symbol: dict[str, dict[str, Any]] = {}
    for decision in decisions:
        symbol = _s(decision.get("symbol")).upper()
        if symbol in expected_symbols:
            by_symbol[symbol] = decision

    for row in rows:
        symbol = _s(row.get("symbol")).upper()
        decision = by_symbol.get(symbol)
        row["decisionMode"] = "codex-final-synthesis"
        if not decision:
            row.update(
                {
                    "decisionState": "REFERENCE_ONLY",
                    "actionBucket": "reference_only",
                    "actionReason": "Codex 최종 종합에서 편입/제외 근거가 충분하지 않아 참고 유지",
                    "portfolioWeightPct": 0.0,
                }
            )
            continue

        bucket = _normalize_action_bucket(decision.get("actionBucket") or decision.get("decisionState"))
        weight = _round_optional(decision.get("portfolioWeightPct"), 2)
        if bucket != "actionable_now":
            weight = 0.0
        elif weight is None:
            weight = 0.0

        raw_reasons = decision.get("decisionReasons")
        reasons = [str(item) for item in raw_reasons[:5] if _s(item)] if isinstance(raw_reasons, list) else []
        action_reason = _s(decision.get("actionReason") or decision.get("reason") or decision.get("rationale"))
        row.update(
            {
                "decisionState": _decision_state_for_bucket(bucket),
                "actionBucket": bucket,
                "actionReason": action_reason or _default_reason_for_bucket(bucket),
                "portfolioWeightPct": weight,
                "decisionReasons": reasons or [action_reason or _default_reason_for_bucket(bucket)],
            }
        )

    return {
        "items": decisions,
        "symbolCount": len(rows),
        "decisionCount": len(by_symbol),
        "maxSymbols": max_symbols,
    }


def _apply_risk_review(evaluated: list[dict[str, Any]], market_bundle: dict[str, Any]) -> dict[str, Any]:
    actionables = [row for row in evaluated if _s(row.get("actionBucket")) == "actionable_now"]
    if not actionables:
        return {"items": [], "actionableReviewedCount": 0}
    if not ai.has_api_access:
        return {"error": "codex_risk_review_unavailable", "model": ai.model, "reasoningEffort": ai.reasoning_effort}

    expected_symbols = {_s(row.get("symbol")).upper() for row in actionables if _s(row.get("symbol"))}
    review_items = []
    for row in actionables:
        review_items.append(
            _compact_dict(
                {
                    "currentDecision": {
                        "symbol": _s(row.get("symbol")).upper(),
                        "actionBucket": row.get("actionBucket"),
                        "portfolioWeightPct": row.get("portfolioWeightPct"),
                        "actionReason": row.get("actionReason"),
                        "decisionReasons": row.get("decisionReasons"),
                    },
                    "evidence": _compact_evidence_item(row),
                }
            )
        )

    prompt_items = json.dumps(review_items, ensure_ascii=False, separators=(",", ":"))
    market_context = json.dumps(_compact_market_context(market_bundle), ensure_ascii=False, separators=(",", ":"))
    prompt = (
        "You are a risk manager reviewing only the symbols already marked actionable_now by another model.\n"
        "Write concise Korean reasons, but output STRICT JSON only.\n"
        "Do not use fixed scores, fixed numeric thresholds, or mechanical pass/fail gates. Use judgment from the evidence.\n"
        "Your job is to challenge weak actionables. Small position sizing is not enough to justify a poor setup.\n"
        "Pay special attention to risk-off market regime, benchmark breakdown evidence, negative relative strength, "
        "below-long-term-trend or breakdown/downtrend chart structure, nearby resistance, failed retests, and reward/risk that "
        "does not compensate for the stop risk. If the original reason does not explicitly reconcile these adverse facts, "
        "downgrade to wait_pullback, reference_only, or avoid.\n"
        "If your review says the setup has unresolved long-term trend, relative-strength, nearby-resistance, or marginal "
        "reward/risk problems, do not keep it actionable just because position size is small.\n"
        "Challenge priceDataQuality too. Cross-source high/low mismatches or unverified high-volatility single-source OHLC can "
        "make the stop and RR provisional; downgrade severe mismatches, and only keep high-volatility single-source setups as "
        "small tactical starters when the rest of the evidence is unusually strong.\n"
        "Keep actionable_now only when the evidence still supports a present entry after this adversarial review.\n"
        "Use actionBucket values only from: actionable_now, wait_pullback, reference_only, avoid. "
        "Use portfolioWeightPct only for actionable_now and 0 for the others.\n\n"
        f"Market context JSON: {market_context}\n"
        f"Actionable decisions to review JSON: {prompt_items}\n\n"
        "Return JSON only in this shape:\n"
        '{"items":[{"symbol":"AAPL","actionBucket":"wait_pullback|reference_only|avoid|actionable_now","portfolioWeightPct":0,'
        '"actionReason":"short Korean risk-reviewed reason","decisionReasons":["Korean reason 1","Korean reason 2"],"riskReview":"short Korean audit note"}]}'
    )
    text = ai._call(prompt, max_tokens=5000)
    if not text:
        return {"error": "codex_risk_review_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}
    decisions = _extract_trade_decisions(text, expected_symbols)
    if decisions is None:
        return {"error": "codex_risk_review_json_parse_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}

    by_symbol: dict[str, dict[str, Any]] = {}
    for decision in decisions:
        symbol = _s(decision.get("symbol")).upper()
        if symbol in expected_symbols:
            by_symbol[symbol] = decision

    for row in actionables:
        symbol = _s(row.get("symbol")).upper()
        decision = by_symbol.get(symbol)
        if not decision:
            row["riskReviewMode"] = "codex-risk-review"
            row["riskReview"] = "리스크 리뷰 결과가 반환되지 않아 기존 편입 판단 유지"
            continue
        bucket = _normalize_action_bucket(decision.get("actionBucket") or decision.get("decisionState"))
        weight = _round_optional(decision.get("portfolioWeightPct"), 2)
        if bucket != "actionable_now":
            weight = 0.0
        elif weight is None:
            weight = _round_optional(row.get("portfolioWeightPct"), 2) or 0.0
        raw_reasons = decision.get("decisionReasons")
        reasons = [str(item) for item in raw_reasons[:5] if _s(item)] if isinstance(raw_reasons, list) else []
        action_reason = _s(decision.get("actionReason") or decision.get("reason") or decision.get("rationale"))
        risk_review = _s(decision.get("riskReview") or decision.get("review"))
        row.update(
            {
                "decisionState": _decision_state_for_bucket(bucket),
                "actionBucket": bucket,
                "actionReason": action_reason or _default_reason_for_bucket(bucket),
                "portfolioWeightPct": weight,
                "decisionReasons": reasons or [action_reason or _default_reason_for_bucket(bucket)],
                "riskReview": risk_review,
                "riskReviewMode": "codex-risk-review",
            }
        )

    return {
        "items": decisions,
        "actionableReviewedCount": len(actionables),
        "decisionCount": len(by_symbol),
    }


def _market_regime_label(market_bundle: dict[str, Any]) -> str:
    market_regime = market_bundle.get("marketRegime") if isinstance(market_bundle.get("marketRegime"), dict) else {}
    return _s(market_regime.get("regimeLabel") or market_regime.get("label"))


def _actionable_integrity_flags(row: dict[str, Any], market_bundle: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    regime_label = _market_regime_label(market_bundle)
    adverse_regime = regime_label in {"risk_off_downtrend", "market_stress_or_breakdown"}
    if adverse_regime:
        flags.append(f"시장 체제 {regime_label}")
    for flag in _data_quality_flags(row):
        flags.append(f"가격 데이터 {flag}")

    rs63 = row.get("relativeStrength63dPct")
    if rs63 is not None and _f(rs63) < 0:
        flags.append(f"63일 상대강도 {_f(rs63):.2f}%")

    ma200_gap = row.get("ma200Gap")
    if ma200_gap is None:
        structure = row.get("chartStructure") if isinstance(row.get("chartStructure"), dict) else {}
        moving_averages = structure.get("movingAverages") if isinstance(structure.get("movingAverages"), dict) else {}
        ma200_gap = moving_averages.get("ma200GapPct")
    if ma200_gap is not None and _f(ma200_gap) < 0:
        flags.append(f"200일선 아래 {_f(ma200_gap):.2f}%")

    chart_state = _s(row.get("chartState") or (row.get("chartStructure") or {}).get("state"))
    if chart_state in {"breakdown_or_distribution", "downtrend"}:
        flags.append(f"차트 구조 {chart_state}")

    risk = row.get("riskToStopPct")
    reward = row.get("rewardToTp1Pct")
    rr = row.get("rrToTp1")
    min_risk_off_rr = _env_float("TELEGRAM_ACTIONABLE_RISK_OFF_MIN_RR", 1.7, minimum=0.1)
    min_tp1_reward = _env_float("TELEGRAM_ACTIONABLE_MIN_TP1_REWARD_PCT", 3.0, minimum=0.0)
    if rr is None or _f(rr) <= 0:
        flags.append("손익비 계산 불완전")
    elif adverse_regime and _f(rr) < min_risk_off_rr:
        flags.append(f"손익비 {_f(rr):.2f}x")
    if reward is None or _f(reward) <= 0:
        flags.append("1차 목표 보상폭 계산 불완전")
    elif adverse_regime and _f(reward) < min_tp1_reward:
        flags.append(f"1차 목표 보상폭 {_f(reward):.2f}%")
    if risk is None or _f(risk) <= 0:
        flags.append("손절 리스크 계산 불완전")

    resistance = row.get("nearestResistanceZone") if isinstance(row.get("nearestResistanceZone"), dict) else {}
    resistance_distance = resistance.get("distancePct")
    if adverse_regime and resistance_distance is not None and 0 <= _f(resistance_distance) <= min_tp1_reward:
        flags.append(f"근접 저항 {_f(resistance_distance):.2f}%")
    return flags


def _apply_actionable_integrity_audit(evaluated: list[dict[str, Any]], market_bundle: dict[str, Any]) -> dict[str, Any]:
    audited: list[dict[str, Any]] = []
    regime_label = _market_regime_label(market_bundle)
    adverse_regime = regime_label in {"risk_off_downtrend", "market_stress_or_breakdown"}

    for row in evaluated:
        if _s(row.get("actionBucket")) != "actionable_now":
            continue
        flags = _actionable_integrity_flags(row, market_bundle)
        raw_price_flags = _data_quality_flags(row)
        severe_price_flags = [
            flag
            for flag in raw_price_flags
            if flag
            in {
                "ohlc_verification_required",
                "ohlc_cross_source_mismatch_high",
                "ohlc_cross_source_mismatch_low",
            }
        ]
        price_warning_flags = [
            flag
            for flag in raw_price_flags
            if flag in {"ohlc_verification_recommended", "volatile_day_single_source"}
        ]
        adverse_flags = [flag for flag in flags if not flag.startswith("시장 체제 ") and not flag.startswith("가격 데이터 ")]
        market_downgrade = adverse_regime and len(adverse_flags) >= (2 if regime_label == "market_stress_or_breakdown" else 3)
        should_downgrade = bool(severe_price_flags) or market_downgrade
        if not should_downgrade:
            row["integrityAudit"] = {
                "status": "passed_with_price_warning" if price_warning_flags else "passed",
                "flags": flags,
                "priceDataQualityFlags": raw_price_flags,
            }
            continue

        symbol = _s(row.get("symbol")).upper()
        old_bucket = _s(row.get("actionBucket"))
        old_weight = _round_optional(row.get("portfolioWeightPct"), 2) or 0.0
        if severe_price_flags:
            reason = "가격 데이터 불일치 검증 전 편입 보류: " + ", ".join(severe_price_flags[:4])
        else:
            reason = "리스크 감사에서 편입 보류: " + ", ".join(adverse_flags[:4])
        previous_reasons = row.get("decisionReasons") if isinstance(row.get("decisionReasons"), list) else []
        row.update(
            {
                "decisionState": "WAIT_PULLBACK",
                "actionBucket": "wait_pullback",
                "actionReason": reason,
                "portfolioWeightPct": 0.0,
                "decisionReasons": [
                    reason,
                    *[str(item) for item in previous_reasons[:3] if _s(item)],
                ][:5],
                "integrityAudit": {
                    "status": "downgraded",
                    "fromBucket": old_bucket,
                    "fromPortfolioWeightPct": old_weight,
                    "flags": flags,
                    "priceDataQualityFlags": raw_price_flags,
                },
            }
        )
        audited.append(
            {
                "symbol": symbol,
                "fromBucket": old_bucket,
                "toBucket": "wait_pullback",
                "fromPortfolioWeightPct": old_weight,
                "flags": flags,
                "priceDataQualityFlags": raw_price_flags,
            }
        )
    return {"items": audited, "adjustedCount": len(audited), "regimeLabel": regime_label}


def _run_news_batch(
    group_symbols: list[str],
    bundles: dict[str, dict[str, Any]],
) -> tuple[list[str], dict[str, Any] | None, list[dict[str, Any]] | None]:
    """Run a single Codex news-batch call. Returns (group, error_payload, rows)."""
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
        return group_symbols, {"error": "codex_batch_analysis_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}, None
    rows = _extract_batch_rows(text, set(group_symbols))
    if rows is None:
        return group_symbols, {"error": "codex_batch_json_parse_failed", "model": ai.model, "reasoningEffort": ai.reasoning_effort}, None
    return group_symbols, None, rows


def _batched_ai_news_analysis(bundles: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]] | dict[str, str]:
    if not bundles:
        return {}
    batch_size = _codex_batch_size()
    symbols = sorted(bundles.keys())
    groups = [symbols[idx : idx + batch_size] for idx in range(0, len(symbols), batch_size)]
    if not groups:
        return {}

    workers = min(_codex_news_batch_workers(), len(groups))
    analyzed: dict[str, dict[str, Any]] = {}
    first_error: dict[str, Any] | None = None

    def _record_rows(rows: list[dict[str, Any]]) -> None:
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

    if workers <= 1:
        for group in groups:
            _group, error, rows = _run_news_batch(group, bundles)
            if error is not None:
                return error
            _record_rows(rows or [])
        return analyzed

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="codex-news") as executor:
        futures = [executor.submit(_run_news_batch, group, bundles) for group in groups]
        for future in as_completed(futures):
            _group, error, rows = future.result()
            if error is not None:
                if first_error is None:
                    first_error = error
                continue
            _record_rows(rows or [])

    if first_error is not None:
        return first_error
    return analyzed


def _zone_price(zone: dict[str, Any], key: str, latest_price: float, *, below: bool) -> float:
    price = _f(zone.get(key), 0.0) if isinstance(zone, dict) else 0.0
    if price <= 0:
        return 0.0
    if below and price < latest_price:
        return round(price, 2)
    if not below and price > latest_price:
        return round(price, 2)
    return 0.0


def _structure_zones(row: dict[str, Any], key: str) -> list[dict[str, Any]]:
    structure = row.get("chartStructure") if isinstance(row.get("chartStructure"), dict) else {}
    zones = structure.get(key) if isinstance(structure.get(key), list) else []
    return [zone for zone in zones if isinstance(zone, dict)]


def _build_execution_plan(row: dict[str, Any], latest_price: float) -> dict[str, Any]:
    supports = row.get("support") if isinstance(row.get("support"), list) else []
    resistances = row.get("resistance") if isinstance(row.get("resistance"), list) else []
    nearest_support_zone = row.get("nearestSupportZone") if isinstance(row.get("nearestSupportZone"), dict) else {}
    nearest_resistance_zone = row.get("nearestResistanceZone") if isinstance(row.get("nearestResistanceZone"), dict) else {}
    support_zones = [
        nearest_support_zone,
        *_structure_zones(row, "supportZones"),
    ]
    resistance_zones = [
        nearest_resistance_zone,
        *_structure_zones(row, "resistanceZones"),
    ]
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
    support_stop = next(
        (price for price in (_zone_price(zone, "lower", latest_price, below=True) for zone in support_zones) if price > 0),
        0.0,
    )
    resistance_target = next(
        (price for price in (_zone_price(zone, "lower", latest_price, below=False) for zone in resistance_zones) if price > 0),
        0.0,
    )
    next_resistance_target = next(
        (
            price
            for price in (_zone_price(zone, "lower", latest_price, below=False) for zone in resistance_zones[1:])
            if price > 0 and price != resistance_target
        ),
        0.0,
    )
    average_entry = round(latest_price, 2) if latest_price > 0 else 0.0
    close_stop = round(support_stop or support, 2) if (support_stop or support) > 0 else 0.0
    hard_stop = close_stop
    tp1 = round(resistance_target or resistance, 2) if (resistance_target or resistance) > 0 else 0.0
    tp2 = round(next_resistance_target or resistance2, 2) if (next_resistance_target or resistance2) > 0 else 0.0

    return {
        "entryMode": "event_driven_current",
        "activationReason": "current_price_with_observed_support_resistance_zone",
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
        "supportZoneUsed": nearest_support_zone,
        "resistanceZoneUsed": nearest_resistance_zone,
        "stopBasis": "support_zone_lower" if support_stop > 0 else "support_midpoint_fallback",
        "targetBasis": "resistance_zone_lower" if resistance_target > 0 else "resistance_midpoint_fallback",
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
        "supportZoneUsed": normalized_plan.get("supportZoneUsed") if isinstance(normalized_plan.get("supportZoneUsed"), dict) else {},
        "resistanceZoneUsed": normalized_plan.get("resistanceZoneUsed") if isinstance(normalized_plan.get("resistanceZoneUsed"), dict) else {},
        "stopBasis": _s(normalized_plan.get("stopBasis")),
        "targetBasis": _s(normalized_plan.get("targetBasis")),
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
    return int(
        _f(payload.get("researchAnalysisLimit"), _f(payload.get("newsAnalysisLimit"), _f(payload.get("analysisLimit"), 0.0)))
    ) == int(expected_limit)


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
    short_volume: dict[str, Any] | None = None,
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
        "return21d": chart_row.get("return21d"),
        "return63d": chart_row.get("return63d"),
        "benchmarkSymbol": _s(chart_row.get("benchmarkSymbol")),
        "benchmarkReturn21d": chart_row.get("benchmarkReturn21d"),
        "benchmarkReturn63d": chart_row.get("benchmarkReturn63d"),
        "relativeStrength21dPct": chart_row.get("relativeStrength21dPct"),
        "relativeStrength63dPct": chart_row.get("relativeStrength63dPct"),
        "chartState": _s(chart_row.get("chartState")),
        "ma20Gap": chart_row.get("ma20Gap"),
        "ma50Gap": chart_row.get("ma50Gap"),
        "ma200Gap": chart_row.get("ma200Gap"),
        "nearestSupportZone": chart_row.get("nearestSupportZone") if isinstance(chart_row.get("nearestSupportZone"), dict) else {},
        "nearestResistanceZone": chart_row.get("nearestResistanceZone") if isinstance(chart_row.get("nearestResistanceZone"), dict) else {},
        "chartStructure": chart_row.get("chartStructure") if isinstance(chart_row.get("chartStructure"), dict) else {},
        "volumeRatio": _round_optional(chart_row.get("volumeRatio"), 2),
        "latestDailyVolume": chart_row.get("latestDailyVolume"),
        "dollarVolumeDaily": chart_row.get("dollarVolumeDaily"),
        "realtimeVolume": chart_row.get("realtimeVolume"),
        "lastMinuteVolume": chart_row.get("lastMinuteVolume"),
        "dailyVolumeAvg20": chart_row.get("dailyVolumeAvg20"),
        "dollarVolumeAvg20": chart_row.get("dollarVolumeAvg20"),
        "dollarVolumeRealtime": chart_row.get("dollarVolumeRealtime"),
        "volumeSource": _s(chart_row.get("volumeSource")),
        "volumeAsOf": _s(chart_row.get("volumeAsOf")),
        "snapshotOpenPrice": chart_row.get("snapshotOpenPrice"),
        "snapshotHighPrice": chart_row.get("snapshotHighPrice"),
        "snapshotLowPrice": chart_row.get("snapshotLowPrice"),
        "snapshotPreviousClosePrice": chart_row.get("snapshotPreviousClosePrice"),
        "priceDataQuality": chart_row.get("priceDataQuality") if isinstance(chart_row.get("priceDataQuality"), dict) else {},
        "dataQualityFlags": _data_quality_flags(chart_row),
        "rsi": round(_f(chart_row.get("rsi"), 0.0), 1),
        "adx": round(_f(chart_row.get("adx"), 0.0), 1),
        "shortVolume": short_volume or {},
        "shortVolumePct": (short_volume or {}).get("shortVolumePct"),
        "shortVolumeAsOf": _s((short_volume or {}).get("tradeReportDate")),
        **_news_context(bundle, news),
        **_price_plan_context(chart_row, latest_price),
    }
    row.update(_reference_decision(row, news))
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
    context_started = time.perf_counter()
    market_bundle = _DATA_COLLECTOR.collect_market_context()
    timings["marketContextSec"] = round(time.perf_counter() - context_started, 3)
    market_ctx = market_bundle.get("marketCondition") if isinstance(market_bundle.get("marketCondition"), dict) else {}
    fear_greed = market_bundle.get("fearGreed") if isinstance(market_bundle.get("fearGreed"), dict) else {}
    macro_ctx = market_bundle.get("macro") if isinstance(market_bundle.get("macro"), dict) else {}
    options_market = market_bundle.get("optionsMarket") if isinstance(market_bundle.get("optionsMarket"), dict) else {}
    market_regime = market_bundle.get("marketRegime") if isinstance(market_bundle.get("marketRegime"), dict) else {}

    universe_symbols = sorted(set(_load_all_us_symbols()) | selected_symbols)
    fundamental_started = time.perf_counter()
    fundamental_rows = _scan_fundamentals(universe_symbols)
    fundamental_by_symbol = {_s(row.get("symbol")).upper(): row for row in fundamental_rows if _s(row.get("symbol"))}
    selection_limit = min(analysis_limit, len(fundamental_rows)) if news_limit is not None else min(analysis_limit, _final_synthesis_max_symbols())
    selection_started = time.perf_counter()
    selected_research = _select_research_symbols(
        fundamental_rows,
        selected_symbols=selected_symbols,
        limit=selection_limit,
        market_bundle=market_bundle,
    )
    timings["researchSelectionSec"] = round(time.perf_counter() - selection_started, 3)
    if isinstance(selected_research, dict) and selected_research.get("error"):
        payload = {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "available": False,
            "reason": "codex_symbol_selection_failed",
            "detail": f"{_s(selected_research.get('error'))} | model={_s(selected_research.get('model') or ai.model)} | reasoning={_s(selected_research.get('reasoningEffort') or ai.reasoning_effort)}",
            "aiModel": ai.model,
            "aiReasoningEffort": ai.reasoning_effort,
            "marketStatus": {
                "marketCondition": market_ctx,
                "fearGreed": fear_greed,
                "macro": macro_ctx,
                "optionsMarket": options_market,
                "marketRegime": market_regime,
            },
            "newsAnalysisLimit": analysis_limit,
            "researchAnalysisLimit": analysis_limit,
            "universeSymbolCount": len(universe_symbols),
            "universeScannedCount": len(fundamental_rows),
            "actionableNow": [],
            "waitPullback": [],
            "avoid": [],
            "referenceOnly": [],
            "all": [],
            "summary": {
                "actionableCount": 0,
                "waitPullbackCount": 0,
                "avoidCount": 0,
                "referenceOnlyCount": 0,
            },
            "timingsSec": {
                **timings,
                "fundamentalScanSec": round(time.perf_counter() - fundamental_started, 3),
                "total": round(time.perf_counter() - started, 3),
            },
        }
        _write_trade_cache(payload, analysis_limit)
        return payload
    candidate_symbols = [
        _s(symbol).upper()
        for symbol in (selected_research.get("symbols") if isinstance(selected_research, dict) else [])
        if _s(symbol).upper() in fundamental_by_symbol
    ]
    candidate_rows = [fundamental_by_symbol[symbol] for symbol in candidate_symbols]
    candidate_symbols = [_s(row.get("symbol")).upper() for row in candidate_rows if _s(row.get("symbol"))]
    timings["fundamentalScanSec"] = round(time.perf_counter() - fundamental_started, 3)
    timings["researchSelectionLimit"] = selection_limit
    timings["researchSelectedCount"] = len(candidate_symbols)

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
                "marketStatus": {
                    "marketCondition": market_ctx,
                    "fearGreed": fear_greed,
                    "macro": macro_ctx,
                    "optionsMarket": options_market,
                    "marketRegime": market_regime,
                },
                "newsAnalysisLimit": analysis_limit,
                "researchAnalysisLimit": analysis_limit,
                "universeScannedCount": len(fundamental_rows),
                "actionableNow": [],
                "waitPullback": [],
                "avoid": [],
                "referenceOnly": [],
                "all": [],
                "summary": {
                    "actionableCount": 0,
                    "waitPullbackCount": 0,
                    "avoidCount": 0,
                    "referenceOnlyCount": 0,
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

    short_started = time.perf_counter()
    short_volume_by_symbol = _DATA_COLLECTOR.collect_short_volume_batch(candidate_symbols)
    timings["shortVolumeSec"] = round(time.perf_counter() - short_started, 3)

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

    eval_started = time.perf_counter()
    evaluated: list[dict[str, Any]] = []
    for symbol in candidate_symbols:
        fundamental = fundamental_by_symbol.get(symbol.upper())
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
                short_volume=short_volume_by_symbol.get(symbol),
                selected_symbols=selected_symbols,
                executed_weights_pct=executed_weights_pct,
            )
        )
    timings["evaluateSec"] = round(time.perf_counter() - eval_started, 3)

    final_started = time.perf_counter()
    final_synthesis = _apply_final_synthesis(evaluated, market_bundle)
    timings["finalSynthesisSec"] = round(time.perf_counter() - final_started, 3)
    if isinstance(final_synthesis, dict) and final_synthesis.get("error"):
        payload = {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "available": False,
            "reason": "codex_final_synthesis_failed",
            "detail": f"{_s(final_synthesis.get('error'))} | model={_s(final_synthesis.get('model') or ai.model)} | reasoning={_s(final_synthesis.get('reasoningEffort') or ai.reasoning_effort)}",
            "aiModel": ai.model,
            "aiReasoningEffort": ai.reasoning_effort,
            "marketStatus": {
                "marketCondition": market_ctx,
                "fearGreed": fear_greed,
                "macro": macro_ctx,
                "optionsMarket": options_market,
                "marketRegime": market_regime,
            },
            "newsAnalysisLimit": analysis_limit,
            "researchAnalysisLimit": analysis_limit,
            "universeSymbolCount": len(universe_symbols),
            "universeScannedCount": len(fundamental_rows),
            "valueCandidateCount": len(candidate_symbols),
            "chartScannedCount": len(scanned_rows),
            "newsAnalyzedCount": len(news_analysis),
            "shortVolumeAnalyzedCount": len(short_volume_by_symbol),
            "selectedCount": len(selected_symbols),
            "actionableNow": [],
            "waitPullback": [],
            "avoid": [],
            "referenceOnly": evaluated,
            "all": evaluated,
            "summary": {
                "actionableCount": 0,
                "waitPullbackCount": 0,
                "avoidCount": 0,
                "referenceOnlyCount": len(evaluated),
            },
            "timingsSec": {
                **timings,
                "total": round(time.perf_counter() - started, 3),
            },
        }
        _write_trade_cache(payload, analysis_limit)
        return payload

    risk_review_started = time.perf_counter()
    risk_review = _apply_risk_review(evaluated, market_bundle)
    timings["riskReviewSec"] = round(time.perf_counter() - risk_review_started, 3)
    if isinstance(risk_review, dict) and risk_review.get("error"):
        payload = {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "available": False,
            "reason": "codex_risk_review_failed",
            "detail": f"{_s(risk_review.get('error'))} | model={_s(risk_review.get('model') or ai.model)} | reasoning={_s(risk_review.get('reasoningEffort') or ai.reasoning_effort)}",
            "aiModel": ai.model,
            "aiReasoningEffort": ai.reasoning_effort,
            "marketStatus": {
                "marketCondition": market_ctx,
                "fearGreed": fear_greed,
                "macro": macro_ctx,
                "optionsMarket": options_market,
                "marketRegime": market_regime,
            },
            "newsAnalysisLimit": analysis_limit,
            "researchAnalysisLimit": analysis_limit,
            "universeSymbolCount": len(universe_symbols),
            "universeScannedCount": len(fundamental_rows),
            "valueCandidateCount": len(candidate_symbols),
            "chartScannedCount": len(scanned_rows),
            "newsAnalyzedCount": len(news_analysis),
            "shortVolumeAnalyzedCount": len(short_volume_by_symbol),
            "selectedCount": len(selected_symbols),
            "actionableNow": [],
            "waitPullback": [],
            "avoid": [],
            "referenceOnly": evaluated,
            "all": evaluated,
            "summary": {
                "actionableCount": 0,
                "waitPullbackCount": 0,
                "avoidCount": 0,
                "referenceOnlyCount": len(evaluated),
            },
            "timingsSec": {
                **timings,
                "total": round(time.perf_counter() - started, 3),
            },
        }
        _write_trade_cache(payload, analysis_limit)
        return payload
    integrity_started = time.perf_counter()
    integrity_audit = _apply_actionable_integrity_audit(evaluated, market_bundle)
    timings["integrityAuditSec"] = round(time.perf_counter() - integrity_started, 3)
    timings["integrityAuditAdjustedCount"] = int(integrity_audit.get("adjustedCount") or 0)
    profile_started = time.perf_counter()
    action_profile_summary = _apply_action_profiles(evaluated)
    timings["actionProfileSec"] = round(time.perf_counter() - profile_started, 3)
    evaluated.sort(key=_value_rank_key)
    for idx, row in enumerate(evaluated, start=1):
        row["finalRank"] = idx

    actionable = [row for row in evaluated if _s(row.get("actionBucket")) == "actionable_now"]
    wait_pullback = [row for row in evaluated if _s(row.get("actionBucket")) == "wait_pullback"]
    avoid = [row for row in evaluated if _s(row.get("actionBucket")) == "avoid"]
    reference_only = [row for row in evaluated if _s(row.get("actionBucket")) == "reference_only"]
    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "available": True,
        "schemaVersion": TRADE_CACHE_SCHEMA_VERSION,
        "analysisMode": "ai-evidence",
        "aiModel": ai.model,
        "aiReasoningEffort": ai.reasoning_effort,
        "rebalanceSourceFile": str(rebalance_path) if rebalance_path is not None else "",
        "rebalanceGeneratedAt": _s(rebalance.get("generated_at")),
        "marketStatus": {
            "marketCondition": market_ctx,
            "fearGreed": fear_greed,
            "macro": macro_ctx,
            "optionsMarket": options_market,
            "marketRegime": market_regime,
        },
        "newsAnalysisLimit": analysis_limit,
        "researchAnalysisLimit": analysis_limit,
        "universeSymbolCount": len(universe_symbols),
        "universeScannedCount": len(fundamental_rows),
        "valueCandidateCount": len(candidate_symbols),
        "researchSelection": selected_research,
        "finalSynthesis": final_synthesis,
        "riskReview": risk_review,
        "integrityAudit": integrity_audit,
        "actionProfileSummary": action_profile_summary,
        "universeNewsScannedCount": len(candidate_symbols),
        "newsCandidateCount": int(timings.get("newsCandidateCount", 0)),
        "chartScannedCount": len(scanned_rows),
        "newsAnalyzedCount": len(news_analysis),
        "shortVolumeAnalyzedCount": len(short_volume_by_symbol),
        "selectedCount": len(selected_symbols),
        "actionableNow": actionable,
        "waitPullback": wait_pullback,
        "avoid": avoid,
        "referenceOnly": reference_only,
        "all": evaluated,
        "chartLeaders": scanned_rows[:20],
        "summary": {
            "actionableCount": len(actionable),
            "waitPullbackCount": len(wait_pullback),
            "avoidCount": len(avoid),
            "referenceOnlyCount": len(reference_only),
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

    context_started = time.perf_counter()
    market_bundle = _DATA_COLLECTOR.collect_market_context()
    context_sec = round(time.perf_counter() - context_started, 3)

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

    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "available": True,
        "mode": "chart-only",
        "chartOnlyPolicy": "chart_is_veto_not_buy_signal",
        "schemaVersion": CHART_SCHEMA_VERSION,
        "rebalanceSourceFile": str(rebalance_path) if rebalance_path is not None else "",
        "rebalanceGeneratedAt": _s(rebalance.get("generated_at")),
        "marketStatus": market_bundle,
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
            "topActionableSymbol": _s(strict_buyable[0].get("symbol")) if strict_buyable else "",
        },
        "timingsSec": {
            "marketContextSec": context_sec,
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
        "reference": "referenceOnly",
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


def _market_extra_line(payload: dict[str, Any], *, include_regime: bool = True) -> str:
    market_status = payload.get("marketStatus") if isinstance(payload.get("marketStatus"), dict) else {}
    options_market = market_status.get("optionsMarket") if isinstance(market_status.get("optionsMarket"), dict) else {}
    macro = market_status.get("macro") if isinstance(market_status.get("macro"), dict) else {}
    market_regime = market_status.get("marketRegime") if isinstance(market_status.get("marketRegime"), dict) else {}
    ratios = options_market.get("ratios") if isinstance(options_market.get("ratios"), dict) else {}
    series = macro.get("series") if isinstance(macro.get("series"), dict) else {}
    parts: list[str] = []
    regime_label = _s(market_regime.get("regimeLabel"))
    if include_regime and regime_label:
        parts.append(f"Regime {regime_label}")
    if ratios.get("total_put_call_ratio") is not None:
        parts.append(f"P/C {_f(ratios.get('total_put_call_ratio')):.2f}")
    if ratios.get("equity_put_call_ratio") is not None:
        parts.append(f"Equity P/C {_f(ratios.get('equity_put_call_ratio')):.2f}")
    vix = series.get("VIXCLS") if isinstance(series.get("VIXCLS"), dict) else {}
    dgs10 = series.get("DGS10") if isinstance(series.get("DGS10"), dict) else {}
    spread = series.get("T10Y2Y") if isinstance(series.get("T10Y2Y"), dict) else {}
    if vix.get("value") is not None:
        parts.append(f"VIX {_f(vix.get('value')):.2f}")
    if dgs10.get("value") is not None:
        parts.append(f"10Y {_f(dgs10.get('value')):.2f}%")
    if spread.get("value") is not None:
        parts.append(f"10Y-2Y {_f(spread.get('value')):.2f}%")
    return " | ".join(parts)


def _market_header_line(payload: dict[str, Any], market_condition: dict[str, Any], fear_greed: dict[str, Any]) -> str:
    market_status = payload.get("marketStatus") if isinstance(payload.get("marketStatus"), dict) else {}
    market_regime = market_status.get("marketRegime") if isinstance(market_status.get("marketRegime"), dict) else {}
    regime_label = _s(market_regime.get("regimeLabel"))
    parts: list[str] = []
    message = _s(market_condition.get("message"))
    if message:
        parts.append(f"시장(MA) {message}")
    if regime_label:
        parts.append(f"체제 {regime_label}")
    parts.append(f"공포탐욕 {fear_greed.get('score', '-')}")
    return " | ".join(parts)


def _one_line_row(row: dict[str, Any]) -> str:
    symbol = escape(_s(row.get("symbol")))
    price = _format_price(row.get("latestClosePrice"))
    profile = escape(_s(row.get("actionProfileLabel")))
    profile_suffix = f" | {profile}" if profile else ""
    if _s(row.get("valueState")):
        p_fcf = _format_multiple(row.get("pFcf"))
        fpe = _format_multiple(row.get("forwardPe"))
        fcf_yield = _format_pct(row.get("fcfYieldPct"))
        roe = _format_pct(row.get("roePct"))
        weight = _format_pct(row.get("portfolioWeightPct"))
        rs63 = row.get("relativeStrength63dPct")
        short_pct = row.get("shortVolumePct")
        extra = []
        if rs63 is not None:
            extra.append(f"RS63 {_format_pct(rs63)}")
        if short_pct is not None:
            extra.append(f"ShortVol {_format_pct(short_pct)}")
        suffix = f" | {' | '.join(extra)}" if extra else ""
        return f"<b>{symbol}</b>  {price} | P/FCF {p_fcf} | Fwd PER {fpe} | FCF yield {fcf_yield} | ROE {roe} | 비중 {weight}{profile_suffix}{suffix}"
    entry = _format_price(row.get("averageEntryPrice"))
    stop = _format_price(row.get("closeStopPrice"))
    tp1 = _format_price(row.get("tp1Price"))
    weight = _format_pct(row.get("portfolioWeightPct"))
    return f"<b>{symbol}</b>  {price} | 진입 {entry} | 손절 {stop} | 1차 {tp1} | 비중 {weight}{profile_suffix}"


def _compact_row(row: dict[str, Any]) -> str:
    symbol = escape(_s(row.get("symbol")))
    price = _format_price(row.get("latestClosePrice"))
    profile = escape(_s(row.get("actionProfileLabel")))
    profile_suffix = f" | {profile}" if profile else ""
    if _s(row.get("valueState")):
        rs63 = row.get("relativeStrength63dPct")
        rs_text = f" | RS63 {_format_pct(rs63)}" if rs63 is not None else ""
        return (
            f"<b>{symbol}</b>  {price} | P/FCF {_format_multiple(row.get('pFcf'))} | "
            f"Fwd PER {_format_multiple(row.get('forwardPe'))} | FCF {_format_pct(row.get('fcfYieldPct'))}{profile_suffix}{rs_text}"
        )
    entry = _format_price(row.get("averageEntryPrice"))
    tp1 = _format_price(row.get("tp1Price"))
    return f"<b>{symbol}</b>  {price} | 진입 {entry} | 1차 {tp1}{profile_suffix}"


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
    quality_flags = _data_quality_flags(row)
    quality_note = "가격검증 " + ", ".join(quality_flags[:3]) if quality_flags else ""
    if reasons:
        parts = [_s(item) for item in reasons[:4] if _s(item)]
        if quality_note:
            parts.append(quality_note)
        return " / ".join(parts)
    return " / ".join(part for part in [_s(row.get("actionReason") or row.get("newsHeadline") or row.get("tradeReason")), quality_note] if part)


def render_trade_view_html(payload: dict[str, Any], view: str = "summary") -> str:
    if not bool(payload.get("available")):
        return (
            f"<b>분석 불가</b>\n"
            f"이유: <code>{escape(_s(payload.get('reason') or 'unknown'))}</code>\n"
            f"{escape(_s(payload.get('detail') or 'Codex 기반 후보 선정/최종 종합이 필요합니다.'))}"
        )

    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    market_condition = ((payload.get("marketStatus") or {}).get("marketCondition") or {}) if isinstance(payload.get("marketStatus"), dict) else {}
    fear_greed = ((payload.get("marketStatus") or {}).get("fearGreed") or {}) if isinstance(payload.get("marketStatus"), dict) else {}
    actionable = _bucket_rows(payload, "actionable")
    wait_pullback = _bucket_rows(payload, "wait")
    avoid = _bucket_rows(payload, "avoid")
    reference_only = _bucket_rows(payload, "reference")
    raw_mode = _s(payload.get("analysisMode")) in {"valuation-first", "raw-evidence", "ai-evidence"}
    decision_summary = (
        f"편입 {summary.get('actionableCount', 0)} | 대기 {summary.get('waitPullbackCount', 0)} | 제외 {summary.get('avoidCount', 0)} | 참고 {summary.get('referenceOnlyCount', len(reference_only))}"
        if raw_mode
        else f"편입 {summary.get('actionableCount', 0)} | 대기 {summary.get('waitPullbackCount', 0)} | 제외 {summary.get('avoidCount', 0)}"
    )

    header = [
        "<b>Autostock Evidence Desk</b>" if raw_mode else "<b>Autostock Trade Desk</b>",
        f"<code>{escape(_fmt_asof(payload.get('generatedAt')))}</code>",
        _market_header_line(payload, market_condition, fear_greed),
        (
            f"원자료 스캔 {payload.get('universeScannedCount', '-')} | 후보 {payload.get('valueCandidateCount', '-')} | 뉴스 요약 {payload.get('newsAnalyzedCount', '-')}"
            if raw_mode
            else f"뉴스 탐색 {payload.get('universeNewsScannedCount', payload.get('universeScannedCount', '-'))} | 촉매후보 {payload.get('newsCandidateCount', '-')} | Codex {payload.get('newsAnalyzedCount', '-')}"
        ),
        f"가격 검증 {payload.get('chartScannedCount', payload.get('universeScannedCount', '-'))}",
        f"모델 {escape(_s(payload.get('aiModel') or ai.model))} / {escape(_s(payload.get('aiReasoningEffort') or ai.reasoning_effort))}",
        decision_summary,
    ]
    timing = _timing_line(payload)
    if timing:
        header.append(timing)
    market_extra = _market_extra_line(payload, include_regime=False)
    if market_extra:
        header.append(market_extra)
    header.append("")

    if view == "summary":
        lines = header[:]
        lines.append("<b>편입 후보</b>" if raw_mode else "<b>지금 진입 가능</b>")
        if actionable:
            lines.extend(_compact_row(row) for row in actionable[:3])
        else:
            lines.append("없음")
        lines.append("")
        if raw_mode:
            lines.append("<b>대기 후보</b>")
            if wait_pullback:
                for row in wait_pullback[:5]:
                    lines.append(_compact_row(row))
                    if _s(row.get("actionReason")):
                        lines.append(escape(_s(row.get("actionReason"))))
            else:
                lines.append("없음")
            if reference_only:
                lines.append(f"참고 원자료 {len(reference_only)}개")
        else:
            lines.append("<b>눌림 대기</b>")
            if wait_pullback:
                lines.extend(_compact_row(row) for row in wait_pullback[:5])
            else:
                lines.append("없음")
        lines.append("")
        lines.append("<b>제외</b>" if raw_mode else "<b>추격 금지</b>")
        if avoid:
            lines.extend(f"{escape(_s(row.get('symbol')))}  {_s(row.get('actionReason') or row.get('tradeReason'))}" for row in avoid[:3])
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "actionable":
        lines = header + ["<b>편입 후보</b>" if raw_mode else "<b>즉시 매수 후보</b>"]
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
        rows = [*wait_pullback, *reference_only] if raw_mode else wait_pullback
        lines = header + ["<b>대기/참고 원자료</b>" if raw_mode else "<b>눌림 대기 후보</b>"]
        if rows:
            for row in rows[:10]:
                lines.append(_one_line_row(row))
                lines.append(_decision_line(row))
                lines.append(escape(_decision_reason_text(row)))
                lines.append("")
        else:
            lines.append("없음")
        return "\n".join(lines)

    if view == "avoid":
        lines = header + ["<b>제외</b>" if raw_mode else "<b>지금 제외</b>"]
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
        lines = header + ["<b>포트폴리오</b>" if raw_mode else "<b>추천 포트폴리오</b>"]
        if actionable:
            for row in actionable[:10]:
                if _s(row.get("valueState")):
                    lines.append(
                        f"{escape(_s(row.get('symbol')))}  {_format_pct(row.get('portfolioWeightPct'))} | "
                        f"{escape(_s(row.get('actionProfileLabel')))} | "
                        f"P/FCF {_format_multiple(row.get('pFcf'))} | Fwd PER {_format_multiple(row.get('forwardPe'))} | FCF yield {_format_pct(row.get('fcfYieldPct'))}"
                    )
                else:
                    lines.append(
                        f"{escape(_s(row.get('symbol')))}  {_format_pct(row.get('portfolioWeightPct'))} | "
                        f"{escape(_s(row.get('actionProfileLabel')))} | 진입 {_format_price(row.get('averageEntryPrice'))} | 손절 {_format_price(row.get('closeStopPrice'))}"
                    )
                if _s(row.get("actionReason")):
                    lines.append(f"이유: {escape(_s(row.get('actionReason')))}")
        else:
            if raw_mode:
                lines.append("Codex 최종 종합에서 현재 편입 후보가 없습니다.")
                if reference_only:
                    lines.append(f"참고 원자료 {len(reference_only)}개는 포트폴리오 비중 0%입니다.")
            else:
                lines.append("점수/임계값 기반 편입은 제거했습니다.")
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
    quality_flags = _data_quality_flags(row)
    quality_text = f" | 가격검증 {escape(', '.join(quality_flags[:3]))}" if quality_flags else ""
    if reasons:
        return " | ".join(escape(_s(item)) for item in reasons[:6]) + quality_text
    return (
        f"차트 {escape(_s(row.get('chartState')))} | "
        f"TP1 여력 {_f(row.get('rewardToTp1Pct')):.2f}% | "
        f"손절 리스크 {_f(row.get('riskToStopPct')):.2f}% | "
        f"RSI {_f(row.get('rsi')):.1f} | "
        f"거래량 {_format_volume_ratio(row.get('volumeRatio'))}{quality_text}"
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
    ]
    timing = _timing_line(payload)
    if timing:
        header.append(timing)
    market_extra = _market_extra_line(payload)
    if market_extra:
        header.append(market_extra)
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
        else:
            lines.append("차트 단독으로 편입하지 않습니다. 뉴스/실적 촉매 확인 전 관찰이 기본입니다.")
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
