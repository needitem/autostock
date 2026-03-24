from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PIPELINE_PATH = ROOT / "src" / "pipelines" / "strategy_v14_pipeline.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_v14_pipeline_points_to_v14_runner():
    pipeline = _load_module(PIPELINE_PATH, "strategy_v14_pipeline")

    assert pipeline.RUNNER_SCRIPT.name == "run_strategy_v14_regime_gld_dynamic_defense.py"
    assert pipeline.VERIFY_SCRIPT.name == "verify_ai_portfolio_backtest.py"


def test_v14_pipeline_artifact_paths_are_run_tag_specific():
    pipeline = _load_module(PIPELINE_PATH, "strategy_v14_pipeline_paths")

    paths = pipeline._artifact_paths("demo_tag")

    assert paths["summary_json"].name == "ai_portfolio_backtest_summary_demo_tag.json"
    assert paths["verification_json"].name == "ai_portfolio_backtest_verification_demo_tag.json"
