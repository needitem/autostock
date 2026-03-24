from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UTILS_PATH = ROOT / "scripts" / "strategy_runner_utils.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_apply_default_env_respects_overwrite_flag(monkeypatch):
    utils = _load_module(UTILS_PATH, "strategy_runner_utils")
    monkeypatch.setenv("TEST_KEEP", "existing")
    monkeypatch.delenv("AI_RUN_TAG", raising=False)

    utils.apply_default_env({"TEST_KEEP": "default"}, run_tag_prefix="demo", overwrite_existing=False)
    assert os.environ["TEST_KEEP"] == "existing"
    assert os.environ["AI_RUN_TAG"].startswith("demo_")

    monkeypatch.setenv("TEST_KEEP", "existing")
    monkeypatch.delenv("AI_RUN_TAG", raising=False)
    utils.apply_default_env({"TEST_KEEP": "default"}, run_tag_prefix="demo", overwrite_existing=True)
    assert os.environ["TEST_KEEP"] == "default"
