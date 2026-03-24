from __future__ import annotations

from bot.scheduler_config import (
    format_inventory_report_message,
    format_rebalance_snapshot,
    format_strategy_v2_message,
    format_strategy_v2_snapshot,
    format_us_rebalance_message,
    parse_bool,
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


def test_parse_bool_values() -> None:
    assert parse_bool("1", False) is True
    assert parse_bool("true", False) is True
    assert parse_bool("on", False) is True
    assert parse_bool("0", True) is False
    assert parse_bool("false", True) is False
    assert parse_bool("", True) is True


def test_schedule_settings_from_env(monkeypatch) -> None:
    monkeypatch.setenv("BOT_TIMEZONE", "UTC")
    monkeypatch.setenv("US_REPORT_TIME", "01:23")
    monkeypatch.setenv("US_REBALANCE_TIME", "09:45")
    monkeypatch.setenv("US_REBALANCE_WEEKDAY", "4")
    monkeypatch.setenv("INVENTORY_MODE_ENABLED", "true")
    monkeypatch.setenv("INVENTORY_REPORT_TIME", "02:34")

    settings = schedule_settings()
    assert settings["timezone"] == "UTC"
    assert settings["report_hour"] == 1
    assert settings["report_minute"] == 23
    assert settings["rebalance_hour"] == 9
    assert settings["rebalance_minute"] == 45
    assert settings["rebalance_weekday"] == 4
    assert settings["inventory_enabled"] is True
    assert settings["inventory_report_hour"] == 2
    assert settings["inventory_report_minute"] == 34


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


def test_rebalance_snapshot_includes_price_for_top_positions() -> None:
    snapshot = format_rebalance_snapshot(
        {
            "generated_at": "2026-03-04T00:10:00",
            "desired_exposure_pct": 92.5,
            "achieved_exposure_after_execution_pct": 90.0,
            "executed_cash_pct": 10.0,
            "risk_on_off": {"label": "risk_on", "score": 0.71},
            "executed_weights_pct": {"AAPL": 12.34},
            "execution_plans": {"AAPL": {"anchors": {"price": 210.12}}},
        }
    )
    assert "AAPL" in snapshot
    assert "$210.12" in snapshot


def test_inventory_report_message_includes_summary_paths() -> None:
    msg = format_inventory_report_message(
        {
            "report_path": "data/inventory/inventory_report_20260304_120000.json",
            "md_path": "data/inventory/inventory_report_20260304_120000.md",
            "summary": {"movement_count": 21, "balance_count": 8, "low_stock_count": 3, "mismatch_count": 2},
        }
    )
    assert "movements: 21" in msg
    assert "balances: 8" in msg
    assert "low_stock_candidates: 3" in msg
    assert "reconcile_mismatches: 2" in msg
    assert "report_json: data/inventory/inventory_report_20260304_120000.json" in msg
    assert "report_md: data/inventory/inventory_report_20260304_120000.md" in msg


def test_strategy_v2_snapshot_includes_metrics_and_paths() -> None:
    msg = format_strategy_v2_snapshot(
        {
            "run_tag": "strategy_v2_baseline_20260310T032635Z",
            "start_date": "2016-03-01",
            "end_date": "2026-03-01",
            "snapshot_freq": "weekly",
            "decision_engine": "regime",
            "portfolio_metrics": {
                "ai_portfolio": {"cagr_pct": 29.67, "sharpe": 1.01, "max_drawdown_pct": -40.63},
                "benchmark": {"cagr_pct": 18.96, "sharpe": 0.94, "max_drawdown_pct": -34.47},
            },
        },
        {
            "alpha": {"nw_p_two_sided": 0.139, "nw_p_gt0": 0.931},
            "turnover": {"ai": {"mean": 0.251}},
        },
        summary_path="data/ai_portfolio_backtest_summary.json",
        verification_path="data/ai_portfolio_backtest_verification.json",
    )
    assert "Strategy V2 Baseline" in msg
    assert "29.67%" in msg
    assert "18.96%" in msg
    assert "0.931" in msg
    assert "data/ai_portfolio_backtest_summary.json" in msg
    assert "data/ai_portfolio_backtest_verification.json" in msg


def test_strategy_v2_message_uses_nested_payload() -> None:
    msg = format_strategy_v2_message(
        {
            "summary_path": "data/ai_portfolio_backtest_summary.json",
            "verification_json_path": "data/ai_portfolio_backtest_verification.json",
            "summary": {
                "run_tag": "strategy_v2_baseline_20260310T032635Z",
                "start_date": "2016-03-01",
                "end_date": "2026-03-01",
                "snapshot_freq": "weekly",
                "decision_engine": "regime",
                "portfolio_metrics": {
                    "ai_portfolio": {"cagr_pct": 29.67, "sharpe": 1.01, "max_drawdown_pct": -40.63},
                    "benchmark": {"cagr_pct": 18.96, "sharpe": 0.94, "max_drawdown_pct": -34.47},
                },
            },
            "verification": {
                "alpha": {"nw_p_two_sided": 0.139, "nw_p_gt0": 0.931},
                "turnover": {"ai": {"mean": 0.251}},
            },
        }
    )
    assert "Strategy V2 Baseline" in msg
    assert "Alpha CAGR diff" in msg
