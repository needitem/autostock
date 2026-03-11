from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "scripts" / "run_strategy_v4_stock_momentum.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_v4_stock_momentum_runner_defaults():
    runner = _load_module(RUNNER_PATH, "run_strategy_v4_stock_momentum")

    assert runner.DEFAULT_ENV["AI_DECISION_ENGINE"] == "stock_momentum"
    assert runner.DEFAULT_ENV["AI_UNIVERSE"] == "nasdaq100"
    assert runner.DEFAULT_ENV["AI_STOCK_MOMO_WEIGHT_MODE"] == "equal"
    assert runner.DEFAULT_ENV["AI_STOCK_MOMO_TOP_K_NEUTRAL"] == "5"
    assert runner.DEFAULT_ENV["AI_STOCK_MOMO_TOP_K_RISK_OFF"] == "0"
    assert runner.DEFAULT_ENV["AI_PORTFOLIO_MAX_WEIGHT_PCT"] == "20"
    assert runner.DEFAULT_ENV["AI_PORTFOLIO_MIN_OVERLAP"] == "4"
    assert runner.DEFAULT_ENV["AI_SAFE_MIN_VOLUME_RATIO"] == "0.0"
    assert runner.DEFAULT_ENV["AI_STOCK_MOMO_MAX_PER_SECTOR"] == "2"
    assert runner.DEFAULT_ENV["AI_STOCK_MOMO_SECTOR_BONUS"] == "0.15"
