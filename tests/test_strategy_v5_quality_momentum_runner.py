from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "scripts" / "run_strategy_v5_quality_momentum.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_v5_quality_momentum_runner_defaults():
    runner = _load_module(RUNNER_PATH, "run_strategy_v5_quality_momentum")

    assert runner.DEFAULT_ENV["AI_DECISION_ENGINE"] == "quality_momentum"
    assert runner.DEFAULT_ENV["AI_SNAPSHOT_FREQ"] == "monthly"
    assert runner.DEFAULT_ENV["AI_UNIVERSE"] == "nasdaq100"
    assert runner.DEFAULT_ENV["AI_PORTFOLIO_TOP_K"] == "8"
    assert runner.DEFAULT_ENV["AI_PORTFOLIO_MIN_OVERLAP"] == "6"
    assert runner.DEFAULT_ENV["AI_SAFE_USE_TREND_TEMPLATE"] == "0"
    assert runner.DEFAULT_ENV["AI_QM_WEIGHT_MODE"] == "inv_vol"
    assert runner.DEFAULT_ENV["AI_QM_TOP_K_NEUTRAL"] == "5"
    assert runner.DEFAULT_ENV["AI_QM_MAX_PER_SECTOR"] == "2"
    assert runner.DEFAULT_ENV["AI_QM_QUALITY_WEIGHT"] == "0.65"
    assert runner.DEFAULT_ENV["AI_QM_MOMENTUM_WEIGHT"] == "0.35"
