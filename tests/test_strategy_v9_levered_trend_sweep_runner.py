from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_strategy_v9_levered_trend_sweep.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_v9_levered_trend_runner_defaults_and_local_sweep_shape():
    runner = _load_module(SCRIPT_PATH, "run_strategy_v9_levered_trend_sweep")

    assert runner.DEFAULT_ENV["LT_BEST_JSON"] == "data/runs/levered_trend_best_lt_v1_grid_20260305.json"
    assert runner.DEFAULT_ENV["LT_RECENT_YEARS"] == "3"
    assert runner.DEFAULT_ENV["LT_TRADE_COST_BPS"] == "20"

    configs = runner._candidate_configs(
        {
            "risk": "QLD",
            "safe": "GLD",
            "ma_window": 125,
            "qqq_confirm": False,
            "vix_max": 0.0,
        }
    )

    assert len(configs) == 18
    assert configs[0] == runner.SweepConfig(risk="QLD", safe="GLD", ma_window=125, qqq_confirm=False, vix_max=0.0)
    assert {cfg.ma_window for cfg in configs} == {100, 125, 150}
    assert {cfg.qqq_confirm for cfg in configs} == {False, True}
    assert {cfg.vix_max for cfg in configs} == {0.0, 28.0, 32.0}


def test_v9_levered_trend_recent_window_summary_and_markdown():
    runner = _load_module(SCRIPT_PATH, "run_strategy_v9_levered_trend_sweep_summary")
    df = runner.pd.DataFrame(
        {
            "entry_day": runner.pd.date_range("2020-01-03", periods=260, freq="W-FRI"),
            "net_return_pct": [1.0] * 260,
            "benchmark_return_pct": [0.5] * 260,
        }
    )

    full = runner._window_summary(df, label="full_window")
    recent = runner._window_slice(df, start="2023-01-01", end="2024-12-31")
    recent_summary = runner._window_summary(recent, label="recent_3y")

    assert recent_summary["periods"] < full["periods"]
    assert full["alpha"]["cagr_diff_pctp"] > 0
    assert recent_summary["alpha"]["cagr_diff_pctp"] > 0

    report = {
        "inputs": {"best_json": "data/runs/levered_trend_best_lt_v1_grid_20260305.json", "recent_years": 3},
        "base_config_id": "qld_gld_w125_qc0_vx0",
        "sweep_size": 1,
        "best": {
            "full_window": {"config_id": "qld_gld_w125_qc0_vx0"},
            "recent_3y": {"config_id": "qld_gld_w125_qc0_vx28"},
            "robust": {"config_id": "qld_gld_w125_qc0_vx0"},
        },
        "results": [
            {
                "rank": 1,
                "config_id": "qld_gld_w125_qc0_vx0",
                "full_cagr_diff_pctp": 1.0,
                "full_mdd_diff_pctp": 2.0,
                "recent_3y_cagr_diff_pctp": 0.5,
                "recent_3y_mdd_diff_pctp": 1.5,
                "robust_score": 0.5,
            }
        ],
    }

    md = runner._render_markdown(report)

    assert "Levered Trend Local Sweep" in md
    assert "recent 3y" in md
    assert "Sweep Table" in md
