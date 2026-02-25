"""Scheduler and notification formatting helpers for Telegram bot."""

from __future__ import annotations

import os
from typing import Any

DEFAULT_BOT_TIMEZONE = "Asia/Seoul"
DEFAULT_US_REPORT_TIME = "00:00"
DEFAULT_US_REBALANCE_TIME = "00:10"
DEFAULT_US_REBALANCE_WEEKDAY = 0  # Monday (Python weekday convention)


def parse_hhmm(raw: str | None, fallback: tuple[int, int]) -> tuple[int, int]:
    value = (raw or "").strip()
    if not value:
        return fallback
    parts = value.split(":")
    if len(parts) != 2:
        return fallback
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except Exception:
        return fallback
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return fallback
    return hour, minute


def parse_weekday(raw: str | None, fallback: int = DEFAULT_US_REBALANCE_WEEKDAY) -> int:
    value = (raw or "").strip()
    if not value:
        return fallback
    try:
        day = int(value)
    except Exception:
        return fallback
    if day < 0:
        return 0
    if day > 6:
        return 6
    return day


def schedule_settings() -> dict[str, int | str]:
    tz_name = (os.getenv("BOT_TIMEZONE") or DEFAULT_BOT_TIMEZONE).strip() or DEFAULT_BOT_TIMEZONE
    report_hour, report_minute = parse_hhmm(os.getenv("US_REPORT_TIME"), parse_hhmm(DEFAULT_US_REPORT_TIME, (0, 0)))
    rebalance_hour, rebalance_minute = parse_hhmm(
        os.getenv("US_REBALANCE_TIME"),
        parse_hhmm(DEFAULT_US_REBALANCE_TIME, (0, 10)),
    )
    rebalance_weekday = parse_weekday(os.getenv("US_REBALANCE_WEEKDAY"), DEFAULT_US_REBALANCE_WEEKDAY)
    return {
        "timezone": tz_name,
        "report_hour": report_hour,
        "report_minute": report_minute,
        "rebalance_hour": rebalance_hour,
        "rebalance_minute": rebalance_minute,
        "rebalance_weekday": rebalance_weekday,
    }


def format_us_report_message(result: dict[str, Any]) -> str:
    report_path = str(result.get("report_path", "") or "")
    lines = ["US daily report saved."]
    if report_path:
        lines.append(f"report_json: {report_path}")
    return "\n".join(lines)


def format_us_rebalance_message(result: dict[str, Any]) -> str:
    result_json = str(result.get("result_json", "") or "")
    orders_csv = str(result.get("orders_csv", "") or "")
    report_path = ""
    payload = result.get("result")
    if isinstance(payload, dict):
        report_path = str(payload.get("report_path", "") or "")

    lines = ["US weekly rebalance saved."]
    if report_path:
        lines.append(f"source_report_json: {report_path}")
    if result_json:
        lines.append(f"rebalance_json: {result_json}")
    if orders_csv:
        lines.append(f"orders_csv: {orders_csv}")
    return "\n".join(lines)

