from __future__ import annotations

import email.utils
import html
import json
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from typing import Any

import requests

from core.sec_pit import SEC_HEADERS, load_ticker_to_cik
from core.event_watchlist import normalize_event

_IMPORTANT_SEC_FORMS = {
    "8-K",
    "8-K/A",
    "10-Q",
    "10-K",
    "10-Q/A",
    "10-K/A",
    "6-K",
    "6-K/A",
}

_SEC_ITEM_EARNINGS = {"2.02"}
_SEC_ITEM_REGULATORY = {"8.01"}


def _s(value: Any) -> str:
    return str(value or "").strip()


def _parse_iso_date(value: Any) -> date | None:
    raw = _s(value)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw[:10]).date()
    except Exception:
        return None


def _parse_pubdate(value: str) -> str:
    raw = _s(value)
    if not raw:
        return ""
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        return dt.isoformat()
    except Exception:
        return raw


def _sec_category(form: str, description: str, item_text: str) -> str:
    items = {part.strip() for part in item_text.replace(",", " ").split() if part.strip()}
    if form in {"10-Q", "10-K", "10-Q/A", "10-K/A"}:
        return "earnings"
    if items & _SEC_ITEM_EARNINGS:
        return "earnings"
    if items & _SEC_ITEM_REGULATORY:
        return "regulatory"
    return "sec_filing"


def _sec_magnitude(form: str, category: str, filing_age_days: int) -> float:
    magnitude = 0.55
    if form in {"8-K", "8-K/A", "6-K", "6-K/A"}:
        magnitude = 0.8
    if form in {"10-Q", "10-K", "10-Q/A", "10-K/A"}:
        magnitude = 0.9
    if category in {"earnings", "regulatory"}:
        magnitude += 0.1
    if filing_age_days <= 2:
        magnitude += 0.05
    return max(0.4, min(1.4, magnitude))


def fetch_sec_submission_events(
    symbol: str,
    limit: int = 20,
    max_age_days: int = 21,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    ticker_to_cik = load_ticker_to_cik()
    cik = ticker_to_cik.get(_s(symbol).upper())
    if not cik:
        return []
    sess = session or requests.Session()
    resp = sess.get(
        f"https://data.sec.gov/submissions/CIK{cik}.json",
        headers=dict(SEC_HEADERS),
        timeout=20,
    )
    resp.raise_for_status()
    payload = resp.json()
    recent = ((payload.get("filings") or {}).get("recent") or {}) if isinstance(payload, dict) else {}
    forms = list(recent.get("form") or [])
    dates = list(recent.get("filingDate") or [])
    docs = list(recent.get("primaryDocDescription") or [])
    items = list(recent.get("items") or [])

    out: list[dict[str, Any]] = []
    today_utc = datetime.now(timezone.utc).date()
    for idx, form in enumerate(forms[: max(1, int(limit))]):
        form = _s(form).upper()
        if form not in _IMPORTANT_SEC_FORMS:
            continue
        filing_date = _s(dates[idx] if idx < len(dates) else "")
        filing_day = _parse_iso_date(filing_date)
        if filing_day is None:
            continue
        filing_age_days = max(0, (today_utc - filing_day).days)
        if filing_age_days > max(0, int(max_age_days)):
            continue
        description = _s(docs[idx] if idx < len(docs) else "")
        item_text = _s(items[idx] if idx < len(items) else "")
        headline = " | ".join(part for part in [description, f"form {form}", item_text] if part).strip()
        if not headline:
            headline = f"{symbol.upper()} filed {form}"
        category = _sec_category(form, description, item_text)
        sentiment = "neutral"
        magnitude = _sec_magnitude(form, category, filing_age_days)

        out.append(
            {
                "symbol": _s(symbol).upper(),
                "scope": "stock",
                "sentiment": sentiment,
                "category": category,
                "source": "sec",
                "magnitude": magnitude,
                "confirmed": True,
                "headline": headline,
                "published_at": filing_date,
                "tags": [str(form)] + ([item_text] if item_text else []),
                "filing_age_days": filing_age_days,
                "importance": "high" if category in {"earnings", "regulatory"} else "medium",
            }
        )
    out.sort(key=lambda row: (_parse_iso_date(row.get("published_at")) or date.min), reverse=True)
    return out


def fetch_rss_events(
    feed_urls: list[str],
    *,
    symbol: str,
    max_per_feed: int = 10,
    source_name: str = "wire",
    category_hint: str = "product",
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    sess = session or requests.Session()
    upper_symbol = _s(symbol).upper()
    out: list[dict[str, Any]] = []

    for url in feed_urls:
        feed_url = _s(url)
        if not feed_url:
            continue
        resp = sess.get(feed_url, timeout=20, headers={"User-Agent": "autostock/2.0"})
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        channel = root.find("channel")
        items = channel.findall("item") if channel is not None else root.findall(".//item")
        for item in items[: max(1, int(max_per_feed))]:
            title = html.unescape(_s(item.findtext("title")))
            description = html.unescape(_s(item.findtext("description")))
            text = " ".join(part for part in [title, description] if part).strip()
            if upper_symbol and upper_symbol not in text.upper():
                continue
            out.append(
                {
                    "symbol": upper_symbol,
                    "scope": "stock",
                    "sentiment": "neutral",
                    "category": category_hint,
                    "source": source_name,
                    "magnitude": 1.0,
                    "confirmed": True,
                    "headline": title or text,
                    "published_at": _parse_pubdate(_s(item.findtext("pubDate"))),
                    "tags": [],
                    "link": _s(item.findtext("link")),
                }
            )
    return out


def load_manual_events(
    path: str | None,
    *,
    default_source: str = "manual",
    default_category: str = "product",
    default_scope: str = "stock",
    symbol: str | None = None,
) -> list[dict[str, Any]]:
    if not path:
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception:
        return []
    if isinstance(payload, dict):
        rows = payload.get("events")
    else:
        rows = payload
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    wanted_symbol = _s(symbol).upper()
    for row in rows:
        if not isinstance(row, dict):
            continue
        candidate = dict(row)
        candidate.setdefault("source", default_source)
        candidate.setdefault("category", default_category)
        candidate.setdefault("scope", default_scope)
        if wanted_symbol:
            candidate.setdefault("symbol", wanted_symbol)
        normalized = normalize_event(candidate)
        if normalized is None:
            continue
        if wanted_symbol and _s(normalized.get("symbol")).upper() != wanted_symbol:
            continue
        out.append(normalized)
    return out


def load_calendar_events(path: str | None, *, symbol: str | None = None) -> list[dict[str, Any]]:
    if not path:
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception:
        return []
    rows = payload if isinstance(payload, list) else payload.get("events") if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    wanted_symbol = _s(symbol).upper()
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_symbol = _s(row.get("symbol") or wanted_symbol).upper()
        if wanted_symbol and row_symbol != wanted_symbol:
            continue
        event_date = _s(row.get("expected_date") or row.get("published_at"))
        event_day = _parse_iso_date(event_date)
        if event_day is None:
            continue
        out.append(
            {
                "symbol": row_symbol,
                "type": _s(row.get("type") or row.get("category") or "event"),
                "priority": _s(row.get("priority") or "medium"),
                "days_until": max(0, (event_day - datetime.now(timezone.utc).date()).days),
                "expected_date": event_day.isoformat(),
                "headline": _s(row.get("headline") or row.get("title") or row.get("description") or "Upcoming event"),
                "source": _s(row.get("source") or "calendar"),
            }
        )
    out.sort(key=lambda row: (int(row.get("days_until", 9999)), _s(row.get("symbol"))))
    return out


def build_next_known_events(symbol: str, stock_info: dict[str, Any], generated_at: datetime) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    days_to_earnings = stock_info.get("days_to_earnings")
    try:
        dte = int(days_to_earnings) if days_to_earnings is not None else None
    except Exception:
        dte = None
    if dte is not None and dte >= 0:
        event_day = generated_at.date().fromordinal(generated_at.date().toordinal() + dte)
        out.append(
            {
                "symbol": _s(symbol).upper(),
                "type": "earnings",
                "priority": "high" if dte <= 7 else "medium",
                "days_until": dte,
                "expected_date": event_day.isoformat(),
                "headline": f"{_s(symbol).upper()} earnings in {dte} day(s)",
                "source": "calendar",
            }
        )
    return out
