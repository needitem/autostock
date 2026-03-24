from __future__ import annotations

import os
from pathlib import Path

from strategy_runner_utils import apply_default_env, run_backtest_selector


SCRIPT_DIR = Path(__file__).resolve().parent


DEFAULT_ENV: dict[str, str] = {
    "AI_DECISION_ENGINE": "regime",
    "AI_SYMBOLS": "TQQQ,QLD,QQQ,GLD,IEF,TLT,BIL",
    "AI_SNAPSHOT_FREQ": "weekly",
    "AI_HORIZON_MODE": "next_snapshot",
    "AI_EXECUTION_TIMING": "next_open",
    "AI_PORTFOLIO_TOP_K": "1",
    "AI_PORTFOLIO_MAX_WEIGHT_PCT": "100",
    "AI_TRADE_COST_BPS": "20",
    "AI_SLIPPAGE_BPS": "0",
    "AI_SPREAD_BPS": "0",
    "AI_TAX_BPS": "0",
    "AI_START_DATE": "2016-03-01",
    "AI_END_DATE": "2026-03-01",
    "AI_SAFE_MODE": "1",
    "AI_SAFE_USE_TREND_TEMPLATE": "0",
    "AI_SAFE_MIN_VOLUME_RATIO": "0",
    "AI_ALGO_USE_BENCHMARK_FEATURES": "0",
    "AI_REGIME_SOURCE": "QQQ",
    "AI_REGIME_MA_FAST": "100",
    "AI_REGIME_MA_SLOW": "200",
    "AI_REGIME_MOM_LB": "21",
    "AI_REGIME_MOM_THR": "0.0",
    "AI_REGIME_RISK_ON": "TQQQ:80,QQQ:20",
    "AI_REGIME_RISK_ON_ALT": "QLD",
    "AI_REGIME_NEUTRAL": "QLD",
    "AI_REGIME_RECOVERY": "QQQ",
    "AI_REGIME_RISK_OFF": "GLD",
    "AI_REGIME_CRASH": "BIL",
    "AI_REGIME_RISK_OFF_DYNAMIC": "1",
    "AI_REGIME_RISK_OFF_POOL": "GLD",
    "AI_REGIME_RISK_OFF_TOP_N": "1",
    "AI_REGIME_RISK_OFF_MIN_RET63": "0.0",
    "AI_REGIME_RISK_OFF_WEIGHT_MODE": "inv_vol",
    "AI_REGIME_RISK_OFF_FALLBACK": "GLD",
    "AI_REGIME_CRASH_DYNAMIC": "1",
    "AI_REGIME_CRASH_POOL": "BIL,GLD,IEF",
    "AI_REGIME_CRASH_TOP_N": "1",
    "AI_REGIME_CRASH_MAX_VOL": "0.03",
    "AI_REGIME_CRASH_WEIGHT_MODE": "inv_vol",
    "AI_REGIME_CRASH_FALLBACK": "BIL",
    "AI_REGIME_VOL_CAP": "0.05",
    "AI_REGIME_VOL_LOW": "0.035",
    "AI_REGIME_VOL_MID": "0.04",
    "AI_REGIME_MOM_STRONG": "0.06",
    "AI_REGIME_CRASH_VOL": "0.06",
    "AI_REGIME_CRASH_DD": "-0.2",
    "AI_REGIME_HYSTERESIS": "0.0",
    "AI_REGIME_RECOVERY_SLOW_BUFFER": "0.03",
    "AI_REGIME_RECOVERY_MIN_MOM": "0.015",
    "AI_REGIME_RECOVERY_MAX_VOL": "0.045",
    "AI_REGIME_RECOVERY_DD_FLOOR": "-0.12",
    "AI_REGIME_FILTER_ASSET": "QLD",
    "AI_REGIME_FILTER_MA": "50",
    "AI_REGIME_FILTER_SAFE": "QQQ",
    "AI_REGIME_EXPOSURE": "0",
    "AI_MOMENTUM_BLEND_PCT": "0",
}


def _apply_defaults() -> None:
    apply_default_env(
        DEFAULT_ENV,
        run_tag_prefix="strategy_v14_regime_gld_dynamic_defense",
        overwrite_existing=False,
    )


def main() -> None:
    _apply_defaults()
    run_backtest_selector(
        script_dir=SCRIPT_DIR,
        heading="Running Strategy V14 regime GLD dynamic defense...",
        echo_keys=(
            "AI_RUN_TAG",
            "AI_DECISION_ENGINE",
            "AI_START_DATE",
            "AI_END_DATE",
            "AI_REGIME_NEUTRAL",
            "AI_REGIME_RISK_OFF_DYNAMIC",
            "AI_REGIME_RISK_OFF_POOL",
            "AI_REGIME_RISK_OFF_FALLBACK",
            "AI_REGIME_CRASH_DYNAMIC",
            "AI_REGIME_FILTER_SAFE",
        ),
    )


if __name__ == "__main__":
    main()
