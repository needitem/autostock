"""
Compatibility economic-calendar module used by legacy tests.
"""
from __future__ import annotations

from datetime import datetime, timedelta


ECONOMIC_EVENTS = {
    "FOMC": {
        "name": "FOMC Interest Rate Decision",
        "impact": "🔴 High",
        "desc": "US Federal Reserve policy meeting and rate decision.",
    },
    "CPI": {
        "name": "Consumer Price Index (CPI)",
        "impact": "🔴 High",
        "desc": "US inflation metric used for monetary policy.",
    },
    "NFP": {
        "name": "Non-Farm Payrolls (NFP)",
        "impact": "🔴 High",
        "desc": "Monthly US labor market report.",
    },
    "GDP": {
        "name": "Gross Domestic Product (GDP)",
        "impact": "🟡 Medium",
        "desc": "US economic growth estimate.",
    },
    "earnings": {
        "name": "Corporate Earnings",
        "impact": "🟡 Medium",
        "desc": "Company quarterly earnings reports.",
    },
}


ECONOMIC_CALENDAR_2025 = [
    {"date": "2025-01-29", "event": "FOMC"},
    {"date": "2025-02-12", "event": "CPI"},
    {"date": "2025-03-07", "event": "NFP"},
    {"date": "2025-04-30", "event": "GDP"},
    {"date": "2025-07-30", "event": "FOMC"},
    {"date": "2025-10-31", "event": "NFP"},
]


def get_event_description(event: str) -> dict:
    event_key = (event or "").strip()
    info = ECONOMIC_EVENTS.get(event_key)
    if info:
        return dict(info)
    return {
        "name": f"{event_key or 'Unknown'} Event",
        "impact": "🟡 Medium",
        "desc": "No detailed description available.",
    }


def fetch_investing_calendar(days: int = 14) -> list[dict]:
    # Compatibility stub: network scraping intentionally omitted.
    return []


def get_upcoming_events(days: int = 14) -> list[dict]:
    end_date = datetime.now().date() + timedelta(days=max(0, int(days)))
    rows = []

    for item in ECONOMIC_CALENDAR_2025:
        try:
            date_obj = datetime.strptime(item["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if datetime.now().date() <= date_obj <= end_date:
            desc = get_event_description(item.get("event", ""))
            rows.append(
                {
                    "date": item["date"],
                    "event": item.get("event", ""),
                    "name": desc["name"],
                }
            )

    for item in fetch_investing_calendar(days=days):
        if not isinstance(item, dict):
            continue
        date_str = item.get("date")
        event = item.get("event", "")
        if not date_str or not event:
            continue
        desc = get_event_description(event)
        rows.append({"date": date_str, "event": event, "name": desc["name"]})

    rows.sort(key=lambda x: x["date"])
    return rows

