from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_stock_hypothesis_param_sweep.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_parse_float_grid_uses_defaults_when_empty():
    runner = _load_module(SCRIPT_PATH, "run_stock_hypothesis_param_sweep")

    out = runner._parse_float_grid("", [0.03, 0.05])

    assert out == [0.03, 0.05]


def test_variant_name_is_stable_and_readable():
    runner = _load_module(SCRIPT_PATH, "run_stock_hypothesis_param_sweep_name")

    name = runner._variant_name("weekly_veto_recentq_newonly_neutral_soft_bonus_ro2", 0.05, -4.0)

    assert name == "weekly_veto_recentq_newonly_neutral_soft_bonus_ro2__pb0p05__vtm4p00"
