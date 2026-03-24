from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "scripts" / "run_strategy_v8_levered_trend_best.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_v8_levered_trend_runner_defaults():
    runner = _load_module(RUNNER_PATH, "run_strategy_v8_levered_trend_best")

    assert runner.DEFAULT_ENV["LT_BEST_JSON"] == "data/runs/levered_trend_best_lt_v1_grid_20260305.json"
    assert runner.DEFAULT_ENV["LT_START_DATE"] == "2016-03-01"
    assert runner.DEFAULT_ENV["LT_END_DATE"] == "2026-03-01"
    assert runner.DEFAULT_ENV["LT_BENCHMARK"] == "QQQ"
    assert runner.DEFAULT_ENV["LT_TRADE_COST_BPS"] == "20"
