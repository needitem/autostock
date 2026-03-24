from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent


DEFAULT_ENV: dict[str, str] = {
    "AI_DECISION_ENGINE": "quality_momentum",
    "AI_UNIVERSE": "nasdaq100",
    "AI_UNIVERSE_MODE": "by_date",
    "AI_UNIVERSE_BY_DATE_FILE": "data/universe/nasdaq100_by_date_weekly_2006_2026.json",
    "AI_SNAPSHOT_FREQ": "monthly",
    "AI_HORIZON_MODE": "next_snapshot",
    "AI_EXECUTION_TIMING": "next_open",
    "AI_PORTFOLIO_TOP_K": "8",
    "AI_PORTFOLIO_MAX_WEIGHT_PCT": "20",
    "AI_PORTFOLIO_MIN_OVERLAP": "6",
    "AI_PROMPT_MAX_SYMBOLS": "40",
    "AI_TRADE_COST_BPS": "20",
    "AI_SLIPPAGE_BPS": "0",
    "AI_SPREAD_BPS": "0",
    "AI_TAX_BPS": "0",
    "AI_START_DATE": "2011-03-01",
    "AI_END_DATE": "2026-03-01",
    "AI_SAFE_MODE": "1",
    "AI_SAFE_REQUIRE_RISK_ON": "0",
    "AI_SAFE_USE_TREND_TEMPLATE": "0",
    "AI_SAFE_MIN_VOLUME_RATIO": "0.0",
    "AI_ALGO_USE_BENCHMARK_FEATURES": "1",
    "AI_QM_WEIGHT_MODE": "inv_vol",
    "AI_QM_TOP_K_NEUTRAL": "5",
    "AI_QM_TOP_K_RISK_OFF": "0",
    "AI_QM_MIN_POSITIONS_FOR_INVEST": "4",
    "AI_QM_MAX_PER_SECTOR": "2",
    "AI_QM_SECTOR_BONUS": "0.15",
    "AI_QM_MAX_FILING_AGE": "240",
    "AI_QM_QUALITY_WEIGHT": "0.65",
    "AI_QM_MOMENTUM_WEIGHT": "0.35",
    "AI_QM_MIN_QUALITY_SCORE": "-1.0",
    "AI_QM_MIN_MOMENTUM_SCORE": "0.0",
    "AI_QM_REQUIRE_TREND_TEMPLATE": "0",
    "AI_MOMENTUM_BLEND_PCT": "0",
}


def _apply_defaults() -> None:
    for key, value in DEFAULT_ENV.items():
        os.environ[key] = value
    os.environ.setdefault(
        "AI_RUN_TAG",
        f"strategy_v5_quality_momentum_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
    )


def main() -> None:
    _apply_defaults()
    sys.path.insert(0, str(SCRIPT_DIR))

    import backtest_ai_portfolio_selector as selector

    print("Running Strategy V5 quality momentum baseline...")
    for key in (
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
    ):
        print(f"  {key}={os.environ.get(key, '')}")

    selector.run()


if __name__ == "__main__":
    main()
