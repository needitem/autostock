from __future__ import annotations

from bot.menu_content import build_main_menu_text
from bot.keyboards import main_menu
from bot.scheduler_config import format_strategy_v14_message
from bot.strategy_support import command_name, get_strategy_spec, latest_command_name


def test_strategy_support_exposes_v2_and_v14_specs() -> None:
    v2 = get_strategy_spec("v2")
    v14 = get_strategy_spec("v14")

    assert v2.label == "Strategy V2"
    assert v14.label == "Strategy V14"
    assert v14.pipeline_module == "pipelines.strategy_v14_pipeline"


def test_menu_content_reports_trading_state() -> None:
    text = build_main_menu_text(style="beginner", trading_enabled=False, inventory_enabled=True)

    assert "Trading: OFF" in text
    assert "Run Strategy V2" in text
    assert "Run Strategy V14" in text


def test_format_strategy_v14_message_uses_strategy_title() -> None:
    msg = format_strategy_v14_message(
        {
            "summary_path": "data/runs/v14_summary.json",
            "verification_json_path": "data/runs/v14_verify.json",
            "summary": {
                "run_tag": "strategy_v14_regime_gld_dynamic_defense_demo",
                "start_date": "2016-03-01",
                "end_date": "2026-03-01",
                "snapshot_freq": "weekly",
                "decision_engine": "regime",
                "portfolio_metrics": {
                    "ai_portfolio": {"cagr_pct": 29.18, "sharpe": 1.05, "max_drawdown_pct": -36.64},
                    "benchmark": {"cagr_pct": 18.96, "sharpe": 0.94, "max_drawdown_pct": -34.47},
                },
            },
            "verification": {
                "alpha": {"nw_p_two_sided": 0.142, "nw_p_gt0": 0.929},
                "turnover": {"ai": {"mean": 0.289}},
            },
        }
    )

    assert "Strategy V14 Dynamic Defense" in msg
    assert "29.18%" in msg
    assert "0.929" in msg


def test_strategy_commands_and_menu_include_v14() -> None:
    markup = main_menu()
    callback_data = [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data is not None
    ]

    assert command_name("v14") == "strategy_v14"
    assert latest_command_name("v14") == "strategy_v14_latest"
    assert "run_strategy_v14" in callback_data
    assert "latest_strategy_v14" in callback_data
