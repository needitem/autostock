from __future__ import annotations

from typing import Any

from core.event_watchlist import load_event_feed
from core.news_collectors import fetch_rss_events, fetch_sec_submission_events, load_calendar_events, load_manual_events


def _s(value: Any) -> str:
    return str(value or "").strip()


def collect_profile_events(
    *,
    profile: dict[str, Any] | None = None,
    watchlist: list[str],
    event_feed_path: str | None = None,
    rss_urls: list[str] | None = None,
) -> list[dict[str, Any]]:
    events = load_event_feed(event_feed_path)
    profile_sources = (profile or {}).get("sources") if isinstance((profile or {}).get("sources"), dict) else {}
    rss_urls = rss_urls or []
    sec_cfg = profile_sources.get("sec") if isinstance(profile_sources, dict) else None
    sec_enabled = bool((sec_cfg or {}).get("enabled", True)) if isinstance(sec_cfg, dict) else True
    sec_limit = int((sec_cfg or {}).get("limit", 20)) if isinstance(sec_cfg, dict) else 20
    sec_max_age = int((sec_cfg or {}).get("max_age_days", 21)) if isinstance(sec_cfg, dict) else 21

    for symbol in watchlist:
        if sec_enabled:
            try:
                events.extend(fetch_sec_submission_events(symbol, limit=sec_limit, max_age_days=sec_max_age))
            except Exception:
                pass
        if rss_urls:
            try:
                events.extend(fetch_rss_events(rss_urls, symbol=symbol))
            except Exception:
                pass
        for cfg in profile_sources.get("rss", []) if isinstance(profile_sources, dict) else []:
            if not isinstance(cfg, dict) or not bool(cfg.get("enabled", False)):
                continue
            urls = cfg.get("urls") if isinstance(cfg.get("urls"), list) else []
            if not urls:
                continue
            try:
                events.extend(
                    fetch_rss_events(
                        [str(url) for url in urls if str(url).strip()],
                        symbol=symbol,
                        max_per_feed=int(cfg.get("max_per_feed", 10)),
                        source_name=_s(cfg.get("source") or "wire").lower(),
                        category_hint=_s(cfg.get("category_hint") or "product").lower(),
                    )
                )
            except Exception:
                pass
        for cfg in profile_sources.get("manual", []) if isinstance(profile_sources, dict) else []:
            if not isinstance(cfg, dict) or not bool(cfg.get("enabled", False)):
                continue
            try:
                events.extend(
                    load_manual_events(
                        _s(cfg.get("path")),
                        default_source=_s(cfg.get("source") or "manual").lower(),
                        default_category=_s(cfg.get("category_hint") or "product").lower(),
                        default_scope=_s(cfg.get("scope") or "stock").lower(),
                        symbol=symbol,
                    )
                )
            except Exception:
                pass
    return events


def collect_profile_calendar_events(
    *,
    profile: dict[str, Any] | None = None,
    watchlist: list[str],
) -> list[dict[str, Any]]:
    profile_sources = (profile or {}).get("sources") if isinstance((profile or {}).get("sources"), dict) else {}
    out: list[dict[str, Any]] = []
    for symbol in watchlist:
        for cfg in profile_sources.get("calendar", []) if isinstance(profile_sources, dict) else []:
            if not isinstance(cfg, dict) or not bool(cfg.get("enabled", False)):
                continue
            try:
                out.extend(load_calendar_events(_s(cfg.get("path")), symbol=symbol))
            except Exception:
                pass
    return out


def fresh_symbol_events(event_feed: list[dict[str, Any]], symbol: str, max_count: int = 5) -> list[dict[str, Any]]:
    rows = [
        event
        for event in event_feed
        if _s((event or {}).get("symbol")).upper() == _s(symbol).upper()
    ]
    rows.sort(
        key=lambda event: (
            _s(event.get("published_at")),
            str(event.get("importance", "")) == "high",
        ),
        reverse=True,
    )
    return rows[: max(1, int(max_count))]
