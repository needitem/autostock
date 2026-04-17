from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ai.analyzer import ai
from config import MARKET_INDICATOR
from core.earnings_pit import EarningsEventStore
from core.event_watchlist import chart_volume_gate, classify_action, macro_overlay
from core.indicators import calculate_indicators, calculate_intraday_snapshot
from core.news_collectors import build_next_known_events
from core.sec_pit import SecPointInTimeStore
from core.stock_data import get_fear_greed_index, get_intraday_stock_data, get_market_condition, get_stock_data, get_stock_info
from event_runtime.collect import collect_profile_calendar_events, collect_profile_events, fresh_symbol_events


ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = ROOT / "data" / "autostock_v2"
DEFAULT_WATCHLIST = [
    "TSLA",
    "NVDA",
    "AMD",
    "PLTR",
    "META",
    "MSFT",
    "AMZN",
    "GOOGL",
]


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _s(value: Any) -> str:
    return str(value or "").strip()


def _parse_symbols(raw: str) -> list[str]:
    text = (raw or "").replace("\n", ",").replace(";", ",")
    out: list[str] = []
    seen: set[str] = set()
    for part in text.split(","):
        symbol = str(part).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _watchlist() -> list[str]:
    symbols = _parse_symbols(os.getenv("AI_V2_WATCHLIST", ""))
    return symbols or list(DEFAULT_WATCHLIST)


def _rss_urls() -> list[str]:
    raw = os.getenv("AI_V2_RSS_URLS", "")
    text = str(raw or "").replace("\n", ",").replace(";", ",").replace("|", ",")
    return [part.strip() for part in text.split(",") if part.strip()]


def _event_feed_path() -> str | None:
    explicit = _s(os.getenv("AI_V2_EVENT_FILE"))
    if explicit:
        return explicit
    dated = OUTPUT_DIR / f"events_{date.today().isoformat()}.json"
    return str(dated) if dated.exists() else None


def _build_markdown(payload: dict[str, Any]) -> str:
    macro = payload.get("macro_overlay", {}) if isinstance(payload, dict) else {}
    next_events = payload.get("next_known_events", []) if isinstance(payload, dict) else []
    lines = [
        "# Autostock V2",
        "",
        f"- generated_at: {payload.get('generated_at', '-')}",
        f"- watchlist: {', '.join(payload.get('watchlist', []))}",
        f"- macro_mode: {macro.get('mode', '-')}",
        f"- macro_reason: {macro.get('reason', '-')}",
        f"- fear_greed_score: {macro.get('fear_greed_score', '-')}",
        f"- benchmark: {payload.get('market_ctx', {}).get('benchmark', MARKET_INDICATOR)}",
        "",
        "## Next Known Events",
    ]
    if isinstance(next_events, list) and next_events:
        for event in next_events[:5]:
            if not isinstance(event, dict):
                continue
            lines.append(
                f"- {event.get('symbol', '-')}: {event.get('headline', '-')} ({event.get('expected_date', '-')}, D-{event.get('days_until', '-')})"
            )
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
        "## Recommendations",
        ]
    )
    recommendations = payload.get("recommendations", [])
    if not isinstance(recommendations, list) or not recommendations:
        lines.append("- none")
        return "\n".join(lines) + "\n"

    for row in recommendations:
        if not isinstance(row, dict):
            continue
        lines.extend(
            [
                f"### {row.get('symbol', '-')}: {row.get('action', '-')}",
                f"- event_signal: {row.get('event_signal', '-')}",
                f"- event_strength: {row.get('event_strength', '-')}",
                f"- confidence: {row.get('confidence', '-')}",
                f"- chart_state: {row.get('chart_gate', {}).get('state', '-')}",
                f"- volume_ratio: {row.get('chart_gate', {}).get('volume_ratio', '-')}",
                f"- rationale: {row.get('rationale', '-')}",
                f"- price: {row.get('price', '-')}",
                f"- reasons: {', '.join(row.get('reason_lines', [])) or '-'}",
                f"- raw_events: {', '.join([str(event.get('headline', '-')) for event in row.get('raw_events', [])[:3]]) or '-'}",
                "",
            ]
        )
    return "\n".join(lines)


def run_autostock_v2(
    *,
    profile: dict[str, Any] | None = None,
    watchlist_override: list[str] | None = None,
    event_feed_path: str | None = None,
    rss_urls: list[str] | None = None,
) -> dict[str, Any]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc)
    watchlist = watchlist_override or _watchlist()
    rss_urls = rss_urls if rss_urls is not None else _rss_urls()
    configured_event_file = event_feed_path or _event_feed_path()
    event_feed = collect_profile_events(
        profile=profile,
        watchlist=watchlist,
        event_feed_path=configured_event_file,
        rss_urls=rss_urls,
    )
    extra_calendar_events = collect_profile_calendar_events(profile=profile, watchlist=watchlist)
    market_ctx = get_market_condition()
    fear_greed = get_fear_greed_index()
    market_events = [event for event in event_feed if _s(event.get("scope")).lower() == "market"]
    macro = macro_overlay(market_ctx, fear_greed, market_events)

    sec_store = SecPointInTimeStore(watchlist)
    earnings_store = EarningsEventStore(watchlist)
    asof_date = generated_at.date()
    asof_ts = pd.Timestamp(generated_at)

    recommendations: list[dict[str, Any]] = []
    next_known_events: list[dict[str, Any]] = []
    for symbol in watchlist:
        df = get_stock_data(symbol)
        indicators = calculate_indicators(df) if df is not None else None
        intraday_df = get_intraday_stock_data(symbol, period="5d", interval="5m")
        intraday = calculate_intraday_snapshot(intraday_df, interval_label="5m") if intraday_df is not None else None
        info = get_stock_info(symbol)
        raw_symbol_events = fresh_symbol_events(event_feed, symbol)
        next_known_events.extend(build_next_known_events(symbol, info, generated_at))
        next_known_events.extend([row for row in extra_calendar_events if _s(row.get("symbol")).upper() == symbol])
        if indicators is None:
            recommendations.append(
                {
                    "symbol": symbol,
                    "action": "AVOID",
                    "confidence": 0.2,
                    "event_signal": "none",
                    "event_strength": "none",
                    "price": _f(info.get("price"), 0.0),
                    "chart_gate": {"state": "missing_data", "volume_ratio": None},
                    "rationale": "missing_chart_data",
                    "reason_lines": ["insufficient price history"],
                    "events": [],
                    "raw_events": raw_symbol_events,
                }
            )
            continue

        indicator_payload = dict(indicators)
        if isinstance(intraday, dict):
            indicator_payload.update(intraday)
        chart_gate = chart_volume_gate(indicator_payload)
        earnings_ctx = earnings_store.latest_event_asof(symbol, asof_ts)
        sec_ctx = sec_store.features_asof(symbol, asof_date)
        reason_lines: list[str] = []
        if bool(earnings_ctx.get("earnings_has_data")):
            reason_lines.append(f"earnings data available ({earnings_ctx.get('earnings_event_date', '-')})")
        if bool(sec_ctx.get("pit_has_data")) and sec_ctx.get("pit_filing_age_days") is not None:
            reason_lines.append(f"latest SEC filing age {sec_ctx.get('pit_filing_age_days')}d")

        model_event = ai.analyze_event_bundle(
            symbol=symbol,
            events=raw_symbol_events,
            chart_gate=chart_gate,
            intraday=intraday or {},
            next_known_events=[row for row in next_known_events if _s(row.get("symbol")).upper() == symbol][:5],
        )
        if "error" in model_event:
            raise RuntimeError(f"Event analysis failed for {symbol}: {model_event.get('error')}")
        event_signal = _s(model_event.get("signal")) or "neutral"
        event_strength = _s(model_event.get("strength")) or "none"
        if event_signal not in {"bullish", "bearish", "neutral"}:
            raise RuntimeError(f"Invalid event signal for {symbol}: {event_signal}")
        if event_strength not in {"strong", "moderate", "weak", "none"}:
            raise RuntimeError(f"Invalid event strength for {symbol}: {event_strength}")
        model_rationale = model_event.get("rationale")
        if isinstance(model_rationale, list) and model_rationale:
            reason_lines = [str(item) for item in model_rationale[:5]]

        action = classify_action(
            event_signal,
            event_strength,
            chart_gate,
            macro,
        )
        recommendations.append(
            {
                "symbol": symbol,
                "name": _s(info.get("name")) or symbol,
                "price": round(_f(indicators.get("price"), _f(info.get("price"), 0.0)), 2),
                "action": action["action"],
                "confidence": action["confidence"],
                "rationale": action["rationale"],
                "event_signal": event_signal,
                "event_strength": event_strength,
                "chart_gate": chart_gate,
                "intraday": intraday or {},
                "macro_mode": macro.get("mode"),
                "days_to_earnings": info.get("days_to_earnings"),
                "sector": info.get("sector"),
                "events": raw_symbol_events[:5],
                "event_analysis_mode": _s(model_event.get("mode")) if isinstance(model_event, dict) else "",
                "raw_events": raw_symbol_events,
                "earnings_context": earnings_ctx,
                "sec_context": sec_ctx,
                "reason_lines": reason_lines[:5],
            }
        )

    recommendations.sort(
        key=lambda row: (
            {"BUY": 0, "SELL": 1, "WATCH": 2, "AVOID": 3}.get(_s(row.get("action")), 9),
            {"strong": 0, "moderate": 1, "weak": 2, "none": 3}.get(_s(row.get("event_strength")).lower(), 9),
            -_f(row.get("confidence"), 0.0),
            _s(row.get("symbol")),
        )
    )

    payload = {
        "generated_at": generated_at.isoformat(),
        "watchlist": watchlist,
        "event_feed_path": configured_event_file,
        "rss_urls": rss_urls,
        "market_ctx": market_ctx,
        "fear_greed": fear_greed,
        "macro_overlay": macro,
        "next_known_events": sorted(
            next_known_events,
            key=lambda row: (int(_f(row.get("days_until"), 9999)), _s(row.get("symbol"))),
        ),
        "recommendations": recommendations,
    }

    date_tag = generated_at.astimezone().strftime("%Y-%m-%d")
    json_path = OUTPUT_DIR / f"autostock_v2_{date_tag}.json"
    md_path = OUTPUT_DIR / f"autostock_v2_{date_tag}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_build_markdown(payload), encoding="utf-8")
    return {
        "report_path": str(json_path),
        "md_path": str(md_path),
        "payload": payload,
    }
