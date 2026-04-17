from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from event_profile import load_event_profile
from event_runtime.notify import append_notifications
from event_runtime.models import RuntimeNotification, RuntimeState
from event_runtime.state import load_runtime_state, save_runtime_state
from pipelines.autostock_v2_pipeline import run_autostock_v2


ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOT = ROOT / "data" / "event_runtime"


def _s(value: Any) -> str:
    return str(value or "").strip()


def _profile_name(profile: dict[str, Any]) -> str:
    return _s(profile.get("name") or profile.get("primary_symbol") or "default")


def _runtime_dir(profile: dict[str, Any]) -> Path:
    return RUNTIME_ROOT / _profile_name(profile)


def _state_path(profile: dict[str, Any]) -> Path:
    return _runtime_dir(profile) / "state.json"


def _latest_payload_path(profile: dict[str, Any]) -> Path:
    return _runtime_dir(profile) / "latest_payload.json"


def _outbox_path(profile: dict[str, Any]) -> Path:
    return _runtime_dir(profile) / "outbox.jsonl"


def _event_key(symbol: str, event: dict[str, Any]) -> str:
    return "|".join(
        [
            _s(symbol).upper(),
            _s(event.get("source")),
            _s(event.get("published_at")),
            _s(event.get("headline")),
        ]
    )


def _notification_for_new_event(profile_name: str, symbol: str, created_at: str, event: dict[str, Any]) -> RuntimeNotification:
    headline = _s(event.get("headline")) or f"{symbol} new event"
    body = (
        f"{symbol} new {str(event.get('source', '-')).upper()} event\n"
        f"{headline}\n"
        f"category={event.get('category', '-')}, sentiment={event.get('sentiment', '-')}, importance={event.get('importance', '-')}"
    )
    return RuntimeNotification(
        profile=profile_name,
        kind="new_event",
        symbol=symbol,
        created_at=created_at,
        title=f"{symbol} new event",
        body=body,
        payload=event,
    )


def _notification_for_action_change(
    profile_name: str,
    symbol: str,
    created_at: str,
    previous: str | None,
    current: str,
    row: dict[str, Any],
) -> RuntimeNotification:
    body = (
        f"{symbol} action changed: {previous or 'NONE'} -> {current}\n"
        f"price={row.get('price', '-')}, confidence={row.get('confidence', '-')}, "
        f"event={row.get('event_signal', '-')}/{row.get('event_strength', '-')}, chart={((row.get('chart_gate') or {}).get('state', '-'))}"
    )
    return RuntimeNotification(
        profile=profile_name,
        kind="action_change",
        symbol=symbol,
        created_at=created_at,
        title=f"{symbol} action changed",
        body=body,
        payload={
            "previous_action": previous,
            "current_action": current,
            "row": row,
        },
    )


def summarize_cycle_changes(
    *,
    profile_name: str,
    payload: dict[str, Any],
    previous_state: RuntimeState,
    created_at: str,
) -> tuple[list[RuntimeNotification], RuntimeState]:
    notifications: list[RuntimeNotification] = []
    next_actions = dict(previous_state.last_actions)
    seen_event_keys = list(previous_state.seen_event_keys)
    seen_set = set(seen_event_keys)

    recommendations = payload.get("recommendations", []) if isinstance(payload, dict) else []
    for row in recommendations if isinstance(recommendations, list) else []:
        if not isinstance(row, dict):
            continue
        symbol = _s(row.get("symbol")).upper()
        if not symbol:
            continue
        current_action = _s(row.get("action"))
        previous_action = previous_state.last_actions.get(symbol)
        next_actions[symbol] = current_action
        if previous_action != current_action:
            notifications.append(
                _notification_for_action_change(
                    profile_name,
                    symbol,
                    created_at,
                    previous_action,
                    current_action,
                    row,
                )
            )
        raw_events = row.get("raw_events", [])
        for event in raw_events if isinstance(raw_events, list) else []:
            if not isinstance(event, dict):
                continue
            key = _event_key(symbol, event)
            if key in seen_set:
                continue
            seen_set.add(key)
            seen_event_keys.append(key)
            notifications.append(_notification_for_new_event(profile_name, symbol, created_at, event))

    trimmed_seen = seen_event_keys[-500:]
    next_state = RuntimeState(
        profile=profile_name,
        last_run_at=created_at,
        last_actions=next_actions,
        seen_event_keys=trimmed_seen,
    )
    return notifications, next_state


def persist_runtime_cycle(
    profile: dict[str, Any],
    payload: dict[str, Any],
    notifications: list[RuntimeNotification],
    state: RuntimeState,
) -> dict[str, str]:
    runtime_dir = _runtime_dir(profile)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    payload_path = _latest_payload_path(profile)
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    outbox_path = _outbox_path(profile)
    append_notifications(outbox_path, notifications)

    state_path = _state_path(profile)
    save_runtime_state(state_path, state)
    return {
        "runtime_dir": str(runtime_dir),
        "payload_path": str(payload_path),
        "outbox_path": str(outbox_path),
        "state_path": str(state_path),
    }


def run_runtime_cycle(
    *,
    profile_name: str | None = None,
    watchlist_override: list[str] | None = None,
    event_feed_path: str | None = None,
    rss_urls: list[str] | None = None,
) -> dict[str, Any]:
    profile = load_event_profile(profile_name)
    profile_name_resolved = _profile_name(profile)
    resolved_watchlist = watchlist_override or list(profile.get("symbols", []))
    resolved_event_file = event_feed_path or (_s(profile.get("event_file")) or None)
    resolved_rss = rss_urls or list(profile.get("rss_urls", []))

    payload_result = run_autostock_v2(
        profile=profile,
        watchlist_override=resolved_watchlist,
        event_feed_path=resolved_event_file,
        rss_urls=resolved_rss,
    )
    payload = payload_result.get("payload", {}) if isinstance(payload_result, dict) else {}
    now_iso = datetime.now(timezone.utc).isoformat()
    state = load_runtime_state(_state_path(profile), profile_name_resolved)
    notifications, next_state = summarize_cycle_changes(
        profile_name=profile_name_resolved,
        payload=payload if isinstance(payload, dict) else {},
        previous_state=state,
        created_at=now_iso,
    )
    paths = persist_runtime_cycle(profile, payload if isinstance(payload, dict) else {}, notifications, next_state)
    return {
        "profile": profile_name_resolved,
        "payload_result": payload_result,
        "payload": payload,
        "notifications": [item.to_dict() for item in notifications],
        "notification_count": len(notifications),
        "state": next_state.to_dict(),
        **paths,
    }


def run_runtime_loop(
    *,
    profile_name: str | None = None,
    interval_seconds: int = 60,
    max_cycles: int | None = None,
) -> dict[str, Any]:
    cycles = 0
    last_result: dict[str, Any] = {}
    while True:
        last_result = run_runtime_cycle(profile_name=profile_name)
        cycles += 1
        if max_cycles is not None and cycles >= int(max_cycles):
            break
        time.sleep(max(5, int(interval_seconds)))
    last_result["cycles"] = cycles
    return last_result
