from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "scripts" / "run_champion_challenger_pipeline.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_champion_challenger_runner_defaults():
    runner = _load_module(RUNNER_PATH, "run_champion_challenger_pipeline")

    assert runner.DEFAULT_ENV["CCP_CHAMPION"] == "weekly_baseline_v4"
    assert runner.DEFAULT_ENV["CCP_FIXED_OOS_START_YEAR"] == "2016"
    assert runner.DEFAULT_ENV["HYP_START_DATE"] == "2011-03-01"
    assert runner.DEFAULT_ENV["HYP_END_DATE"] == "2026-03-01"
    assert runner.DEFAULT_ENV["WF_START_DATE"] == "2006-03-01"
    assert runner.DEFAULT_ENV["WF_TRAIN_YEARS"] == "5"
