from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_stock_hypothesis_breadth_sweep.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_build_profiles_crosses_breadth_and_regime_modes(monkeypatch):
    monkeypatch.setenv("STOCK_BREADTH_SOURCE_GRID", "universe,safe")
    monkeypatch.setenv("STOCK_BREADTH_REGIME_GRID", "off,protective")

    runner = _load_module(SCRIPT_PATH, "run_stock_hypothesis_breadth_sweep_profiles")
    profiles = runner._build_profiles()

    assert [profile.name for profile in profiles] == [
        "breadth_universe__regime_off",
        "breadth_universe__regime_protective",
        "breadth_safe__regime_off",
        "breadth_safe__regime_protective",
    ]
    assert profiles[0].env["AI_BREADTH_SOURCE"] == "universe"
    assert profiles[1].env["AI_REGIME_EXPOSURE"] == "1"
    assert profiles[3].env["AI_REGIME_RISK_OFF_EXPOSURE_PCT"] == "35"


def test_run_sweep_aggregates_and_ranks_results(monkeypatch, tmp_path):
    runner = _load_module(SCRIPT_PATH, "run_stock_hypothesis_breadth_sweep_run")
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "RUNS_DIR", tmp_path / "data" / "runs")
    runner.RUNS_DIR.mkdir(parents=True, exist_ok=True)

    fake_eval_runner = SimpleNamespace(
        _load_hypothesis=lambda name: SimpleNamespace(
            name=name,
            engine="stock_momentum",
            freq="weekly",
            top_k=5,
            top_k_neutral=5,
            top_k_risk_off=2,
            pit_bonus=0.05,
            pit_veto_threshold=-4.0,
            pit_veto_regimes=("neutral", "risk_off") if name == "weekly_beta" else (),
        ),
        _env_for_hypothesis=lambda hypothesis: {
            "AI_DECISION_ENGINE": str(hypothesis.engine),
            "AI_SNAPSHOT_FREQ": str(hypothesis.freq),
        },
    )

    def fake_run_selector(eval_runner, env):
        summary_path = runner.RUNS_DIR / f"ai_portfolio_backtest_summary_{env['AI_RUN_TAG']}.json"
        breadth_bonus = 1.0 if env.get("AI_BREADTH_SOURCE") == "safe" else 0.0
        regime_bonus = 1.5 if env.get("AI_REGIME_EXPOSURE") == "1" else 0.0
        hyp_bonus = 0.75 if "weekly_beta" in env.get("AI_RUN_TAG", "") else 0.0
        summary = {
            "config_hash": f"hash_{env['AI_RUN_TAG']}",
            "portfolio_metrics": {
                "ai_portfolio": {
                    "cagr_pct": 10.0 + breadth_bonus + regime_bonus + hyp_bonus,
                    "sharpe": 1.0 + (breadth_bonus * 0.1) + (regime_bonus * 0.1),
                    "max_drawdown_pct": -10.0,
                },
                "benchmark": {
                    "cagr_pct": 7.0,
                    "sharpe": 0.4,
                    "max_drawdown_pct": -12.0,
                },
            },
            "avg_turnover": 0.2,
            "periods": 100,
            "ai_calls": 5,
            "sit_out_rate_pct": 1.0,
            "breadth_source_mode": env.get("AI_BREADTH_SOURCE", "universe"),
            "regime_exposure_enabled": env.get("AI_REGIME_EXPOSURE") == "1",
        }
        summary_path.write_text(json.dumps(summary), encoding="utf-8")

    monkeypatch.setattr(runner, "_run_selector", fake_run_selector)

    profiles = [
        runner.SweepProfile(name="breadth_universe__regime_off", env={"AI_BREADTH_SOURCE": "universe", "AI_REGIME_EXPOSURE": "0"}),
        runner.SweepProfile(
            name="breadth_safe__regime_protective",
            env={
                "AI_BREADTH_SOURCE": "safe",
                "AI_REGIME_EXPOSURE": "1",
                "AI_REGIME_NEUTRAL_EXPOSURE_PCT": "95",
                "AI_REGIME_RISK_OFF_EXPOSURE_PCT": "35",
                "AI_REGIME_CRASH_EXPOSURE_PCT": "20",
            },
        ),
    ]

    rows, cells = runner._run_sweep(
        eval_runner=fake_eval_runner,
        hypothesis_names=["weekly_alpha", "weekly_beta"],
        profiles=profiles,
        run_tag="suite",
        start_date="2011-03-01",
        end_date="2026-03-01",
    )

    assert len(rows) == 4
    assert len(cells) == 4
    assert rows[0]["hypothesis_name"] == "weekly_beta"
    assert rows[0]["profile_name"] == "breadth_safe__regime_protective"
    assert rows[0]["cagr_diff_pct"] == pytest.approx(6.25)
    assert cells[0]["selector_run_tag"].startswith("suite__weekly_beta")
