from __future__ import annotations

import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from strategy_runner_utils import apply_default_env, run_backtest_selector


DEFAULT_ENV: dict[str, str] = {
    "AI_DECISION_ENGINE": "stock_momentum",
    "AI_UNIVERSE": "all_us",
    "AI_UNIVERSE_MODE": "static",
    "AI_UNIVERSE_BY_DATE_FILE": "",
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
    "AI_STOCK_MOMO_TOP_K_RISK_OFF": "2",
    "AI_STOCK_MOMO_MIN_POSITIONS_FOR_INVEST": "2",
    "AI_STOCK_MOMO_MAX_PER_SECTOR": "2",
    "AI_STOCK_MOMO_SECTOR_BONUS": "0.15",
    "AI_STOCK_MOMO_PIT_BONUS": "0.07",
    "AI_STOCK_MOMO_PIT_MAX_FILING_AGE": "180",
    "AI_STOCK_MOMO_PIT_VETO_THRESHOLD": "-3.25",
    "AI_STOCK_MOMO_PIT_VETO_MAX_FILING_AGE": "180",
    "AI_STOCK_MOMO_PIT_VETO_NEW_ONLY": "1",
    "AI_STOCK_MOMO_PIT_VETO_REGIMES": "neutral",
    "AI_STOCK_MOMO_NEUTRAL_ENTRY_MIN_BREADTH_UP200": "0.50",
    "AI_STOCK_MOMO_NEUTRAL_ENTRY_MIN_BREADTH_POS63": "0.45",
    "AI_STOCK_MOMO_NEUTRAL_MAX_NEW_WHEN_WEAK": "1",
    "AI_DAILY_DEFENSE_OVERLAY": "1",
    "AI_DAILY_DEFENSE_SOFT_EXPOSURE_PCT": "85",
    "AI_DAILY_DEFENSE_HARD_EXPOSURE_PCT": "50",
    "AI_DAILY_DEFENSE_VIX_SOFT": "26",
    "AI_DAILY_DEFENSE_VIX_HARD": "32",
    "AI_DAILY_DEFENSE_RETURN21_SOFT": "-4",
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
        heading="Running Strategy V4 broad stock momentum baseline...",
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
