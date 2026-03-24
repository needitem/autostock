from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UTILS_PATH = ROOT / "src" / "pipelines" / "strategy_pipeline_utils.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_strategy_pipeline_utils_builds_run_tag_artifact_paths():
    utils = _load_module(UTILS_PATH, "strategy_pipeline_utils_paths")

    paths = utils.artifact_paths("demo_tag")

    assert paths["summary_json"].name == "ai_portfolio_backtest_summary_demo_tag.json"
    assert paths["results_csv"].name == "ai_portfolio_backtest_results_demo_tag.csv"
    assert paths["verification_md"].name == "ai_portfolio_backtest_verification_demo_tag.md"
