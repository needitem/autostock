from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "scripts" / "run_strategy_v3_stock_selector.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_v3_stock_runner_defaults_to_selector_engine():
    runner = _load_module(RUNNER_PATH, "run_strategy_v3_stock_selector")

    assert runner.DEFAULT_ENV["AI_DECISION_ENGINE"] == "selector"
    assert runner.DEFAULT_ENV["AI_UNIVERSE"] == "nasdaq100"
    assert runner.DEFAULT_ENV["AI_UNIVERSE_MODE"] == "by_date"
    assert runner.DEFAULT_ENV["AI_PORTFOLIO_TOP_K"] == "5"
    assert runner.DEFAULT_ENV["AI_SELECTOR_USE_UNIVERSE_REGIME"] == "0"
    assert runner.DEFAULT_ENV["AI_SELECTOR_TOP_K_NEUTRAL"] == "2"
    assert runner.DEFAULT_ENV["AI_SELECTOR_TOP_K_RISK_OFF"] == "0"
