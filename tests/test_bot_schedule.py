from __future__ import annotations

from bot.scheduler_config import (
    format_us_rebalance_message,
    parse_hhmm,
    parse_weekday,
    schedule_settings,
)


def test_parse_hhmm_valid() -> None:
    assert parse_hhmm("00:00", (9, 9)) == (0, 0)
    assert parse_hhmm("23:59", (9, 9)) == (23, 59)


def test_parse_hhmm_invalid_falls_back() -> None:
    fallback = (1, 2)
    assert parse_hhmm("", fallback) == fallback
    assert parse_hhmm("24:00", fallback) == fallback
    assert parse_hhmm("09:60", fallback) == fallback
    assert parse_hhmm("ab:cd", fallback) == fallback
    assert parse_hhmm("0900", fallback) == fallback


def test_parse_weekday_bounds() -> None:
    assert parse_weekday("0", 3) == 0
    assert parse_weekday("6", 3) == 6
    assert parse_weekday("-1", 3) == 0
    assert parse_weekday("9", 3) == 6
    assert parse_weekday("bad", 3) == 3
    assert parse_weekday("", 3) == 3


def test_schedule_settings_from_env(monkeypatch) -> None:
    monkeypatch.setenv("BOT_TIMEZONE", "UTC")
    monkeypatch.setenv("US_REPORT_TIME", "01:23")
    monkeypatch.setenv("US_REBALANCE_TIME", "09:45")
    monkeypatch.setenv("US_REBALANCE_WEEKDAY", "4")

    settings = schedule_settings()
    assert settings["timezone"] == "UTC"
    assert settings["report_hour"] == 1
    assert settings["report_minute"] == 23
    assert settings["rebalance_hour"] == 9
    assert settings["rebalance_minute"] == 45
    assert settings["rebalance_weekday"] == 4


def test_rebalance_message_uses_json_paths() -> None:
    msg = format_us_rebalance_message(
        {
            "result_json": "outputs/rebalance_recommendation_2026-02-25.json",
            "orders_csv": "outputs/rebalance_orders_2026-02-25.csv",
            "result": {"report_path": "outputs/report_2026-02-25.json"},
        }
    )
    assert "source_report_json: outputs/report_2026-02-25.json" in msg
    assert "rebalance_json: outputs/rebalance_recommendation_2026-02-25.json" in msg
    assert "orders_csv: outputs/rebalance_orders_2026-02-25.csv" in msg
