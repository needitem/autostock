from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_stock_hypothesis_eval.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_env_for_nrisk_challenger_maps_veto_settings():
    runner = _load_module(SCRIPT_PATH, "run_stock_hypothesis_eval")
    hypothesis = runner._load_hypothesis("weekly_veto_recentq_newonly_nrisk_soft_bonus")

    env = runner._env_for_hypothesis(hypothesis)

    assert env["AI_DECISION_ENGINE"] == "stock_momentum"
    assert env["AI_SNAPSHOT_FREQ"] == "weekly"
    assert env["AI_STOCK_MOMO_PIT_BONUS"] == "0.05"
    assert env["AI_STOCK_MOMO_PIT_VETO_THRESHOLD"] == "-4.0"
    assert env["AI_STOCK_MOMO_PIT_VETO_NEW_ONLY"] == "1"
    assert env["AI_STOCK_MOMO_PIT_VETO_REGIMES"] == "neutral,risk_off"


def test_env_for_entryfreeze_hypothesis_maps_breadth_entry_gate():
    runner = _load_module(SCRIPT_PATH, "run_stock_hypothesis_eval_entryfreeze")
    hypothesis = runner._load_hypothesis("weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1")

    env = runner._env_for_hypothesis(hypothesis)

    assert env["AI_STOCK_MOMO_NEUTRAL_ENTRY_MIN_BREADTH_UP200"] == "0.5"
    assert env["AI_STOCK_MOMO_NEUTRAL_ENTRY_MIN_BREADTH_POS63"] == "0.45"
    assert env["AI_STOCK_MOMO_NEUTRAL_MAX_NEW_WHEN_WEAK"] == "1"


def test_build_compare_report_computes_primary_minus_compare_delta():
    runner = _load_module(SCRIPT_PATH, "run_stock_hypothesis_eval_compare")
    primary = runner.RunArtifacts(
        hypothesis_name="challenger",
        run_tag="primary_tag",
        results_csv=ROOT / "data" / "runs" / "primary.csv",
        summary_json=ROOT / "data" / "runs" / "primary.json",
        verification_json=ROOT / "data" / "runs" / "primary_verify.json",
        verification_md=ROOT / "data" / "runs" / "primary_verify.md",
    )
    compare = runner.RunArtifacts(
        hypothesis_name="baseline",
        run_tag="compare_tag",
        results_csv=ROOT / "data" / "runs" / "compare.csv",
        summary_json=ROOT / "data" / "runs" / "compare.json",
        verification_json=ROOT / "data" / "runs" / "compare_verify.json",
        verification_md=ROOT / "data" / "runs" / "compare_verify.md",
    )
    primary_verify = {
        "metrics": {
            "ai_portfolio": {"cagr_pct": 21.5, "sharpe": 0.95, "max_drawdown_pct": -31.0},
            "benchmark": {"cagr_pct": 18.3},
        },
        "alpha": {"nw_p_two_sided": 0.61},
        "bootstrap": {"p_cagr_diff_gt0": 0.69},
        "turnover": {"ai": {"mean": 0.226}},
        "yearly": [{"year": 2025, "ai_ret_pct": 21.0, "qqq_ret_pct": 18.0, "n": 52}],
    }
    compare_verify = {
        "metrics": {
            "ai_portfolio": {"cagr_pct": 21.1, "sharpe": 0.93, "max_drawdown_pct": -31.5},
            "benchmark": {"cagr_pct": 18.3},
        },
        "alpha": {"nw_p_two_sided": 0.64},
        "bootstrap": {"p_cagr_diff_gt0": 0.67},
        "turnover": {"ai": {"mean": 0.225}},
        "yearly": [{"year": 2025, "ai_ret_pct": 19.5, "qqq_ret_pct": 18.0, "n": 52}],
    }

    report, md = runner._build_compare_report(primary, compare, primary_verify, compare_verify)

    assert report["headline"]["primary_minus_compare_cagr_pct"] == pytest.approx(0.4)
    assert report["headline"]["primary_minus_compare_sharpe"] == pytest.approx(0.02)
    assert report["yearly"][0]["primary_minus_compare_pct"] == pytest.approx(1.5)
    assert "challenger" in md
    assert "baseline" in md
