from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from event_profile import load_event_rules


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _s(value: Any) -> str:
    return str(value or "").strip()


def _rules(config: dict[str, Any] | None = None) -> dict[str, Any]:
    return load_event_rules(config if isinstance(config, dict) else None)


def _priority_index(value: str, ordered: list[str]) -> int:
    try:
        return ordered.index(value)
    except ValueError:
        return len(ordered)


def load_event_feed(path: str | Path | None) -> list[dict[str, Any]]:
    if not path:
        return []
    file_path = Path(path)
    if not file_path.exists():
        return []
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    raw_events: list[Any]
    if isinstance(payload, dict):
        events = payload.get("events")
        raw_events = events if isinstance(events, list) else []
    elif isinstance(payload, list):
        raw_events = payload
    else:
        raw_events = []

    out: list[dict[str, Any]] = []
    for row in raw_events:
        normalized = normalize_event(row)
        if normalized is not None:
            out.append(normalized)
    return out


def normalize_event(row: Any) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    symbol = _s(row.get("symbol")).upper()
    scope = _s(row.get("scope")).lower() or ("stock" if symbol else "market")
    sentiment = _s(row.get("sentiment")).lower() or "neutral"
    category = _s(row.get("category")).lower() or "macro"
    source = _s(row.get("source")).lower() or "manual"
    magnitude = max(0.0, min(3.0, _f(row.get("magnitude"), 1.0)))
    confirmed = bool(row.get("confirmed", True))
    headline = _s(row.get("headline"))
    published_at = _s(row.get("published_at"))
    tags = row.get("tags")
    if not isinstance(tags, list):
        tags = []
    return {
        "symbol": symbol,
        "scope": scope,
        "sentiment": sentiment,
        "category": category,
        "source": source,
        "magnitude": magnitude,
        "confirmed": confirmed,
        "headline": headline,
        "published_at": published_at,
        "tags": [str(tag) for tag in tags],
    }


def assess_events(
    events: list[dict[str, Any]],
    symbol: str | None = None,
    scope: str | None = None,
    rules: dict[str, Any] | None = None,
) -> dict[str, Any]:
    wanted_symbol = _s(symbol).upper()
    wanted_scope = _s(scope).lower()
    cfg = _rules(rules)
    score_cfg = cfg.get("event_scoring", {}) if isinstance(cfg.get("event_scoring"), dict) else {}
    source_priority = [str(item).lower() for item in score_cfg.get("source_priority", [])]
    category_priority = [str(item).lower() for item in score_cfg.get("category_priority", [])]
    positive_cfg = score_cfg.get("positive", {}) if isinstance(score_cfg.get("positive"), dict) else {}
    negative_cfg = score_cfg.get("negative", {}) if isinstance(score_cfg.get("negative"), dict) else {}
    pos_decisive_sources = {str(item).lower() for item in positive_cfg.get("decisive_sources", [])}
    pos_decisive_categories = {str(item).lower() for item in positive_cfg.get("decisive_categories", [])}
    pos_supportive_sources = {str(item).lower() for item in positive_cfg.get("supportive_sources", [])}
    pos_supportive_categories = {str(item).lower() for item in positive_cfg.get("supportive_categories", [])}
    neg_decisive_sources = {str(item).lower() for item in negative_cfg.get("decisive_sources", [])}
    neg_decisive_categories = {str(item).lower() for item in negative_cfg.get("decisive_categories", [])}
    neg_supportive_sources = {str(item).lower() for item in negative_cfg.get("supportive_sources", [])}
    neg_supportive_categories = {str(item).lower() for item in negative_cfg.get("supportive_categories", [])}
    sentiment_cfg = cfg.get("sentiment", {}) if isinstance(cfg.get("sentiment"), dict) else {}
    positive_sentiment = {str(item).lower() for item in sentiment_cfg.get("positive", [])}
    negative_sentiment = {str(item).lower() for item in sentiment_cfg.get("negative", [])}
    selected: list[dict[str, Any]] = []
    for event in events:
        event_symbol = _s(event.get("symbol")).upper()
        event_scope = _s(event.get("scope")).lower()
        if wanted_symbol and event_symbol != wanted_symbol:
            continue
        if wanted_scope and event_scope != wanted_scope:
            continue
        sentiment = _s(event.get("sentiment")).lower()
        source = _s(event.get("source")).lower()
        category = _s(event.get("category")).lower()
        signal = "neutral"
        strength = "none"
        rank = 0
        if sentiment in positive_sentiment:
            signal = "bullish"
            if source in pos_decisive_sources and category in pos_decisive_categories:
                strength, rank = "strong", 3
            elif source in pos_supportive_sources or category in pos_supportive_categories or source in pos_decisive_sources or category in pos_decisive_categories:
                strength, rank = "moderate", 2
            else:
                strength, rank = "weak", 1
        elif sentiment in negative_sentiment:
            signal = "bearish"
            if source in neg_decisive_sources and category in neg_decisive_categories:
                strength, rank = "strong", 3
            elif source in neg_supportive_sources or category in neg_supportive_categories or source in neg_decisive_sources or category in neg_decisive_categories:
                strength, rank = "moderate", 2
            else:
                strength, rank = "weak", 1
        if rank == 0:
            continue
        selected.append(
            {
                **event,
                "event_signal": signal,
                "event_strength": strength,
                "rank": rank,
            }
        )
    selected.sort(
        key=lambda item: (
            -int(item.get("rank", 0)),
            _priority_index(_s(item.get("source")).lower(), source_priority),
            _priority_index(_s(item.get("category")).lower(), category_priority),
            _s(item.get("published_at")),
        ),
        reverse=False,
    )
    top = selected[0] if selected else {}
    return {
        "signal": _s(top.get("event_signal")) or "neutral",
        "strength": _s(top.get("event_strength")) or "none",
        "events": selected,
    }


def chart_volume_gate(ind: dict[str, Any], rules: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = _rules(rules)
    gate_cfg = cfg.get("chart_gate", {}) if isinstance(cfg.get("chart_gate"), dict) else {}
    price = _f(ind.get("price"))
    ma20 = _f(ind.get("ma20"))
    ma50 = _f(ind.get("ma50"))
    ma200 = _f(ind.get("ma200"))
    adx = _f(ind.get("adx"))
    rsi = _f(ind.get("rsi"), 50.0)
    bb_position = _f(ind.get("bb_position"), 50.0)
    volume_ratio_daily = _f(ind.get("volume_ratio"), 1.0)
    volume_ratio_intraday = _f(ind.get("intraday_volume_ratio_5m"), 0.0)
    intraday_above_vwap = bool(ind.get("intraday_above_vwap", False))
    return21d = _f(ind.get("return_21d"))
    return63d = _f(ind.get("return_63d"))
    active_volume_ratio = volume_ratio_intraday if volume_ratio_intraday > 0 else volume_ratio_daily

    bullish_structure = price > ma20 and price > ma50 and ma50 >= ma200
    bearish_structure = price < ma20 and price < ma50 and ma50 <= ma200
    trend_strength = adx >= _f(gate_cfg.get("trend_adx_min"), 18.0)
    bullish_volume = active_volume_ratio >= _f(gate_cfg.get("bullish_volume_min"), 1.5) and (intraday_above_vwap if volume_ratio_intraday > 0 else True)
    bearish_volume = active_volume_ratio >= _f(gate_cfg.get("bearish_volume_min"), 1.3) and (not intraday_above_vwap if volume_ratio_intraday > 0 else True)
    overheat = rsi >= _f(gate_cfg.get("overheat_rsi"), 78.0) or bb_position >= _f(gate_cfg.get("overheat_bb_position"), 95.0)

    state = "mixed"
    if bullish_structure and bullish_volume and trend_strength and not overheat:
        state = "confirmed_breakout"
    elif bullish_structure and not overheat:
        state = "constructive"
    elif bearish_structure and bearish_volume:
        state = "confirmed_breakdown"
    elif bearish_structure:
        state = "weak"
    elif overheat:
        state = "overheat"

    return {
        "state": state,
        "bullish_structure": bullish_structure,
        "bearish_structure": bearish_structure,
        "trend_strength": trend_strength,
        "bullish_volume": bullish_volume,
        "bearish_volume": bearish_volume,
        "overheat": overheat,
        "volume_ratio": round(active_volume_ratio, 2),
        "volume_ratio_daily": round(volume_ratio_daily, 2),
        "volume_ratio_intraday_5m": round(volume_ratio_intraday, 2),
        "intraday_above_vwap": intraday_above_vwap,
    }


def macro_overlay(
    market_ctx: dict[str, Any],
    fear_greed: dict[str, Any],
    market_events: list[dict[str, Any]],
    rules: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = _rules(rules)
    overlay_cfg = cfg.get("macro_overlay", {}) if isinstance(cfg.get("macro_overlay"), dict) else {}
    status = _s(market_ctx.get("status")).lower() or "unknown"
    fear_score = int(_f(fear_greed.get("score"), 50.0))
    market_event_bias = assess_events(market_events, scope="market", rules=cfg)
    market_signal = _s(market_event_bias.get("signal")).lower() or "neutral"
    market_strength = _s(market_event_bias.get("strength")).lower() or "none"

    mode = "neutral"
    scale = _f(overlay_cfg.get("neutral_scale"), 0.7)
    allow_new_longs = True
    selective_longs_only = False
    reason = "mixed_macro"

    if status == "bearish" or fear_score <= _f(overlay_cfg.get("fear_score_risk_off"), 25) or (market_signal == "bearish" and market_strength == "strong"):
        mode = "crisis" if market_signal == "bearish" and market_strength == "strong" else "risk_off"
        scale = _f(overlay_cfg.get("crisis_scale"), 0.2) if mode == "crisis" else _f(overlay_cfg.get("risk_off_scale"), 0.4)
        allow_new_longs = False
        selective_longs_only = mode != "crisis"
        reason = "macro_risk_event"
    elif status == "neutral" or fear_score < _f(overlay_cfg.get("fear_score_defensive"), 45) or (market_signal == "bearish" and market_strength in {"moderate", "weak"}):
        mode = "defensive"
        scale = _f(overlay_cfg.get("defensive_scale"), 0.55)
        allow_new_longs = True
        selective_longs_only = True
        reason = "macro_uncertain"
    elif status == "bullish" and fear_score >= _f(overlay_cfg.get("fear_score_risk_on"), 55) and market_signal != "bearish":
        mode = "risk_on"
        scale = _f(overlay_cfg.get("risk_on_scale"), 1.0)
        allow_new_longs = True
        selective_longs_only = False
        reason = "supportive_macro"

    return {
        "mode": mode,
        "position_scale": scale,
        "allow_new_longs": allow_new_longs,
        "selective_longs_only": selective_longs_only,
        "fear_greed_score": fear_score,
        "market_event_signal": market_signal,
        "market_event_strength": market_strength,
        "market_event_count": len(market_event_bias.get("events", [])),
        "reason": reason,
        "market_events": market_event_bias.get("events", [])[:5],
    }


def classify_action(
    event_signal: str,
    event_strength: str,
    gate: dict[str, Any],
    macro: dict[str, Any],
    rules: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = _rules(rules)
    decision_cfg = cfg.get("decision", {}) if isinstance(cfg.get("decision"), dict) else {}
    gate_state = _s(gate.get("state")).lower() or "mixed"
    buy_gate_states = {str(item).lower() for item in decision_cfg.get("buy_gate_states", [])}
    bullish_watch_gate_states = {str(item).lower() for item in decision_cfg.get("bullish_watch_gate_states", [])}
    bearish_watch_gate_states = {str(item).lower() for item in decision_cfg.get("bearish_watch_gate_states", [])}
    fallback_breakout_watch_conf = _f(decision_cfg.get("fallback_breakout_watch_confidence"), 0.45)
    default_avoid_conf = _f(decision_cfg.get("default_avoid_confidence"), 0.35)
    default_buy_conf = _f(decision_cfg.get("default_buy_confidence"), 0.75)
    strong_buy_conf = _f(decision_cfg.get("strong_buy_confidence"), 0.85)
    bearish_sell_conf = _f(decision_cfg.get("bearish_sell_confidence"), 0.85)
    bearish_watch_conf = _f(decision_cfg.get("bearish_watch_confidence"), 0.55)
    bullish_watch_conf = _f(decision_cfg.get("bullish_watch_confidence"), 0.65)
    bullish_unconfirmed_watch_conf = _f(decision_cfg.get("bullish_unconfirmed_watch_confidence"), 0.55)
    non_decisive_watch_conf = _f(decision_cfg.get("non_decisive_watch_confidence"), 0.5)

    action = "AVOID"
    confidence = default_avoid_conf
    rationale = "event_not_strong_enough"

    signal = _s(event_signal).lower()
    strength = _s(event_strength).lower()

    if signal == "bearish" and strength in {"strong", "moderate", "weak"}:
        if gate_state in bearish_watch_gate_states:
            action = "SELL"
            confidence = bearish_sell_conf
            rationale = "bearish_event_with_weak_chart"
        else:
            action = "WATCH"
            confidence = bearish_watch_conf
            rationale = "bearish_event_without_full_breakdown"
    elif signal == "bullish" and strength in {"strong", "moderate", "weak"}:
        if gate_state in buy_gate_states:
            action = "BUY"
            confidence = strong_buy_conf if strength == "strong" else default_buy_conf
            rationale = "bullish_event_confirmed_by_chart_and_volume"
        elif gate_state in bullish_watch_gate_states:
            action = "WATCH"
            confidence = bullish_watch_conf
            rationale = "bullish_event_needs_volume_or_better_entry"
        else:
            action = "WATCH"
            confidence = bullish_unconfirmed_watch_conf
            rationale = "bullish_event_without_chart_confirmation"
    else:
        if gate_state in bearish_watch_gate_states:
            action = "WATCH"
            confidence = non_decisive_watch_conf
            rationale = "chart_weak_but_event_not_decisive"
        elif gate_state in buy_gate_states:
            action = "WATCH"
            confidence = fallback_breakout_watch_conf
            rationale = "chart_good_but_missing_event_catalyst"
        else:
            action = "AVOID"
            confidence = default_avoid_conf
            rationale = "no_edge"

    return {
        "action": action,
        "confidence": round(confidence, 2),
        "rationale": rationale,
    }
