from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "scripts" / "run_strategy_v2_baseline.py"
SELECTOR_PATH = ROOT / "scripts" / "backtest_ai_portfolio_selector.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_v2_runner_uses_mixed_risk_on_sleeve():
    runner = _load_module(RUNNER_PATH, "run_strategy_v2_baseline")

    assert runner.DEFAULT_ENV["AI_REGIME_RISK_ON"] == "TQQQ:80,QQQ:20"
    assert runner.DEFAULT_ENV["AI_REGIME_FILTER_ASSET"] == "QLD"
    assert runner.DEFAULT_ENV["AI_REGIME_FILTER_MA"] == "50"
    assert runner.DEFAULT_ENV["AI_REGIME_FILTER_SAFE"] == "QQQ"


def test_parse_allocation_spec_supports_mixed_risk_on_sleeve():
    selector = _load_module(SELECTOR_PATH, "backtest_ai_portfolio_selector")

    alloc = selector._parse_allocation_spec("TQQQ:80,QQQ:20")

    assert alloc == {"TQQQ": 0.8, "QQQ": 0.2}
