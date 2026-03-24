from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "scripts" / "run_strategy_v14_regime_gld_dynamic_defense.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_v14_regime_gld_dynamic_defense_defaults():
    runner = _load_module(RUNNER_PATH, "run_strategy_v14_regime_gld_dynamic_defense")

    assert runner.DEFAULT_ENV["AI_DECISION_ENGINE"] == "regime"
    assert runner.DEFAULT_ENV["AI_REGIME_NEUTRAL"] == "QLD"
    assert runner.DEFAULT_ENV["AI_REGIME_RISK_OFF_POOL"] == "GLD"
    assert runner.DEFAULT_ENV["AI_REGIME_RISK_OFF_FALLBACK"] == "GLD"
    assert runner.DEFAULT_ENV["AI_REGIME_FILTER_SAFE"] == "QQQ"
