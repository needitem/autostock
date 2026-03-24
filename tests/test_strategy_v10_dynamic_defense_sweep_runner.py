from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_strategy_v10_dynamic_defense_sweep.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_v10_sweep_builds_expected_variant_grid():
    runner = _load_module(SCRIPT_PATH, "run_strategy_v10_dynamic_defense_sweep")

    variants = runner._variants()

    assert len(variants) == 16
    assert any(v.neutral == "QLD" and v.filter_safe == "QLD" for v in variants)
    assert any(v.hysteresis == 0.01 and v.crash_dynamic is False for v in variants)


def test_v10_sweep_ranking_prefers_more_criteria_then_lower_turnover():
    runner = _load_module(SCRIPT_PATH, "run_strategy_v10_dynamic_defense_sweep_ranking")

    strong = {
        "criteria_pass_count": 3,
        "horizon_3y_pass": True,
        "drawdown_guardrail_pass": True,
        "full_p_alpha_gt0": 0.72,
        "full_cagr_diff_pct": 2.7,
        "turnover_mean": 0.32,
        "horizon_3y_cagr_diff_pct": 26.0,
        "horizon_3y_nw_p_two": 0.08,
    }
    weak = {
        "criteria_pass_count": 2,
        "horizon_3y_pass": False,
        "drawdown_guardrail_pass": True,
        "full_p_alpha_gt0": 0.68,
        "full_cagr_diff_pct": 2.9,
        "turnover_mean": 0.29,
        "horizon_3y_cagr_diff_pct": 10.0,
        "horizon_3y_nw_p_two": 0.30,
    }
    tie_low_turnover = {
        "criteria_pass_count": 3,
        "horizon_3y_pass": True,
        "drawdown_guardrail_pass": True,
        "full_p_alpha_gt0": 0.72,
        "full_cagr_diff_pct": 2.7,
        "turnover_mean": 0.28,
        "horizon_3y_cagr_diff_pct": 26.0,
        "horizon_3y_nw_p_two": 0.08,
    }

    assert runner._ranking_key(strong) > runner._ranking_key(weak)
    assert runner._ranking_key(tie_low_turnover) > runner._ranking_key(strong)
