from __future__ import annotations

import os
from pathlib import Path

from strategy_runner_utils import apply_default_env, run_backtest_selector


SCRIPT_DIR = Path(__file__).resolve().parent


DEFAULT_ENV: dict[str, str] = {
    "AI_DECISION_ENGINE": "stock_momentum",
    "AI_UNIVERSE": "nasdaq100",
    "AI_UNIVERSE_MODE": "by_date",
    "AI_UNIVERSE_BY_DATE_FILE": "data/universe/nasdaq100_by_date_weekly_2006_2026.json",
    "AI_SNAPSHOT_FREQ": "weekly",
    "AI_HORIZON_MODE": "next_snapshot",
    "AI_EXECUTION_TIMING": "next_open",
    "AI_PORTFOLIO_TOP_K": "5",
    "AI_PORTFOLIO_MAX_WEIGHT_PCT": "20",
    "AI_PORTFOLIO_MIN_OVERLAP": "4",
    "AI_TRADE_COST_BPS": "20",
    "AI_SLIPPAGE_BPS": "0",
    "AI_SPREAD_BPS": "0",
    "AI_TAX_BPS": "0",
    "AI_START_DATE": "2016-03-01",
    "AI_END_DATE": "2026-03-01",
    "AI_SAFE_MODE": "1",
    "AI_SAFE_REQUIRE_RISK_ON": "0",
    "AI_SAFE_USE_TREND_TEMPLATE": "1",
    "AI_SAFE_MIN_VOLUME_RATIO": "0.0",
    "AI_ALGO_USE_BENCHMARK_FEATURES": "1",
    "AI_STOCK_MOMO_WEIGHT_MODE": "equal",
    "AI_STOCK_MOMO_TOP_K_NEUTRAL": "5",
    "AI_STOCK_MOMO_TOP_K_RISK_OFF": "0",
    "AI_STOCK_MOMO_MIN_POSITIONS_FOR_INVEST": "2",
    "AI_STOCK_MOMO_MAX_PER_SECTOR": "2",
    "AI_STOCK_MOMO_SECTOR_BONUS": "0.15",
    "AI_MOMENTUM_BLEND_PCT": "0",
}


def _apply_defaults() -> None:
    apply_default_env(
        DEFAULT_ENV,
        run_tag_prefix="strategy_v4_stock_momentum",
        overwrite_existing=True,
    )


def main() -> None:
    _apply_defaults()
    run_backtest_selector(
        script_dir=SCRIPT_DIR,
        heading="Running Strategy V4 stock momentum baseline...",
        echo_keys=(
            "AI_RUN_TAG",
            "AI_DECISION_ENGINE",
            "AI_UNIVERSE",
            "AI_UNIVERSE_MODE",
            "AI_UNIVERSE_BY_DATE_FILE",
            "AI_SNAPSHOT_FREQ",
            "AI_EXECUTION_TIMING",
            "AI_START_DATE",
            "AI_END_DATE",
            "AI_TRADE_COST_BPS",
        ),
    )


if __name__ == "__main__":
    main()
