from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "scripts" / "run_strategy_v10_regime_dynamic_defense.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_v10_regime_dynamic_defense_defaults():
    runner = _load_module(RUNNER_PATH, "run_strategy_v10_regime_dynamic_defense")

    assert runner.DEFAULT_ENV["AI_DECISION_ENGINE"] == "regime"
    assert runner.DEFAULT_ENV["AI_REGIME_RISK_OFF_DYNAMIC"] == "1"
    assert runner.DEFAULT_ENV["AI_REGIME_RISK_OFF_POOL"] == "IEF,TLT,GLD"
    assert runner.DEFAULT_ENV["AI_REGIME_CRASH_DYNAMIC"] == "1"
    assert runner.DEFAULT_ENV["AI_REGIME_CRASH_POOL"] == "BIL,GLD,IEF"
    assert runner.DEFAULT_ENV["AI_REGIME_RISK_ON"] == "TQQQ:80,QQQ:20"
