from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROFILE_DIR = ROOT / "configs" / "event_profiles"
RULES_PATH = ROOT / "configs" / "event_rules" / "default.json"


def _s(value: Any) -> str:
    return str(value or "").strip()


def _profile_path(name_or_path: str) -> Path:
    raw = _s(name_or_path)
    path = Path(raw)
    if path.suffix.lower() == ".json":
        return path if path.is_absolute() else (ROOT / path)
    return PROFILE_DIR / f"{raw}.json"


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_event_rules(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = json.loads(RULES_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid event rules payload: {RULES_PATH}")
    if overrides and isinstance(overrides, dict):
        payload = _deep_merge(payload, overrides)
    return payload


def load_event_profile(name_or_path: str | None = None) -> dict[str, Any]:
    name = _s(name_or_path or os.getenv("AI_EVENT_PROFILE") or "tsla")
    path = _profile_path(name)
    if not path.exists():
        raise FileNotFoundError(f"Event profile not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid event profile payload: {path}")
    payload.setdefault("name", path.stem)
    symbols = payload.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        symbol = _s(payload.get("symbol"))
        if symbol:
            payload["symbols"] = [symbol.upper()]
        else:
            raise ValueError(f"Event profile missing symbols: {path}")
    else:
        payload["symbols"] = [str(item).upper() for item in symbols if str(item).strip()]
    payload["primary_symbol"] = str(payload["symbols"][0]).upper()
    payload["rules"] = load_event_rules(payload.get("rules") if isinstance(payload.get("rules"), dict) else None)
    payload.setdefault("rss_urls", [])
    payload.setdefault("event_file", None)
    payload.setdefault("sources", {})
    payload.setdefault("nautilus", {})
    sources = payload["sources"]
    if not isinstance(sources, dict):
        raise ValueError(f"Event profile sources section must be an object: {path}")
    sources.setdefault("sec", {"enabled": True, "limit": 20, "max_age_days": 21})
    if "rss" not in sources:
        sources["rss"] = (
            [
                {
                    "name": "legacy-rss",
                    "enabled": True,
                    "source": "wire",
                    "category_hint": "product",
                    "urls": list(payload.get("rss_urls", [])),
                }
            ]
            if payload.get("rss_urls")
            else []
        )
    if "manual" not in sources:
        sources["manual"] = (
            [
                {
                    "name": "legacy-manual",
                    "enabled": True,
                    "source": "manual",
                    "path": payload.get("event_file"),
                }
            ]
            if payload.get("event_file")
            else []
        )
    sources.setdefault("calendar", [])
    nautilus = payload["nautilus"]
    if not isinstance(nautilus, dict):
        raise ValueError(f"Event profile nautilus section must be an object: {path}")
    defaults = payload["rules"].get("nautilus", {}) if isinstance(payload["rules"], dict) else {}
    primary = payload["primary_symbol"]
    venue = _s(nautilus.get("venue") or "XNAS")
    nautilus.setdefault("venue", venue)
    nautilus.setdefault("instrument_id", f"{primary}.{venue}")
    nautilus.setdefault("bar_type", f"{primary}.{venue}-1-DAY-LAST-EXTERNAL")
    nautilus.setdefault("trade_size", str(defaults.get("trade_size", "10")))
    nautilus.setdefault("custom_data_client_id", str(defaults.get("custom_data_client_id", "CUSTOM")))
    nautilus.setdefault("signal_name", f"{primary.lower()}_event_action")
    nautilus.setdefault("min_volume_ratio_for_entry", payload["rules"]["chart_gate"]["bullish_volume_min"])
    nautilus.setdefault("allow_entries_in_risk_off", bool(defaults.get("allow_entries_in_risk_off", False)))
    return payload


def symbol_slug(profile: dict[str, Any]) -> str:
    return _s(profile.get("primary_symbol")).lower()
