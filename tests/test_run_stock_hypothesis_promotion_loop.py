from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_stock_hypothesis_promotion_loop.py"
RESEARCH_PATH = ROOT / "scripts" / "research_stock_hypotheses.py"
PROMOTION_PATH = ROOT / "scripts" / "run_strategy_promotion_check.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _loop_config(runner):
    return runner.LoopConfig(
        run_tag="promo_loop_test",
        start_date="2011-03-01",
        end_date="2026-03-01",
        oos_start_year=2016,
        trade_cost_bps=20.0,
        max_rounds=3,
        frontier_size=2,
        top_candidates_per_round=3,
        save_round_leaders=0,
        seed_names=("weekly_baseline_v4",),
        bonus_override=(),
        threshold_override=(),
        risk_off_override=(),
        entry_freeze_override=(),
        sector_cap_override=(),
        min_overlap_override=(),
        weight_mode_override=(),
        promotion_min_cost_bps=20.0,
        promotion_max_mdd_worse_pctp=10.0,
        promotion_min_p_alpha_gt0=0.90,
    )


def test_expand_frontier_adds_entry_and_round2_risk_axes():
    runner = _load_module(SCRIPT_PATH, "run_stock_hypothesis_promotion_loop")
    research = _load_module(RESEARCH_PATH, "research_stock_hypotheses_for_promo_loop")
    config = _loop_config(runner)
    base = next(
        h
        for h in research._hypotheses()
        if h.name == "weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325_entryfreeze1_pb007"
    )

    round1 = runner._expand_frontier([base], 1, config)
    round2 = runner._expand_frontier([base], 2, config)

    assert len(round1) > 1
    assert any(
        abs(float(h.pit_bonus) - float(base.pit_bonus)) > 1e-9 or abs(float(h.pit_veto_threshold) - float(base.pit_veto_threshold)) > 1e-9
        for h in round1
    )
    assert any(int(h.neutral_max_new_names_when_weak) != int(base.neutral_max_new_names_when_weak) for h in round1)
    assert not any(int(h.neutral_max_positions_when_weak) == 2 for h in round1)
    assert any(int(h.neutral_max_positions_when_weak) == 2 for h in round2)
    assert any(tuple(h.pit_veto_regimes) == ("neutral", "risk_off") for h in round2)
    assert any(int(h.max_per_sector) == 1 for h in round2)
    assert any(int(h.min_overlap) == 5 for h in round2)
    assert any(str(h.weight_mode) in {"score", "inv_vol"} for h in round2)


def test_promotion_report_can_mark_candidate_as_pass():
    runner = _load_module(SCRIPT_PATH, "run_stock_hypothesis_promotion_loop_report")
    promotion = _load_module(PROMOTION_PATH, "run_strategy_promotion_check_for_promo_loop")
    config = _loop_config(runner)
    df = runner.pd.DataFrame(
        {
            "entry_day": runner.pd.date_range("2016-01-01", periods=520, freq="W-FRI"),
            "net_return_pct": [1.2 if i % 2 == 0 else 0.8 for i in range(520)],
            "benchmark_return_pct": [0.2 if i % 2 == 0 else 0.1 for i in range(520)],
            "turnover": [0.2] * 520,
        }
    )
    full_summary = {
        "cagr_pct": 25.0,
        "benchmark_cagr_pct": 15.0,
        "cagr_diff_pct": 10.0,
        "sharpe": 1.10,
        "benchmark_sharpe": 0.90,
        "mdd_pct": -30.0,
        "benchmark_mdd_pct": -35.0,
        "mdd_diff_pct": 5.0,
        "avg_turnover": 0.20,
        "nw_p_two": 0.05,
        "nw_p_gt0": 0.95,
    }

    report = runner._promotion_report_from_df(
        promotion_runner=promotion,
        df=df,
        full_summary=full_summary,
        periods_per_year=52,
        config=config,
    )

    assert report["overall_pass"] is True
    assert report["criteria_pass_count"] == 7
    assert all(item["passes"] for item in report["criteria"])


def test_run_loop_stops_once_winner_is_found(monkeypatch):
    runner = _load_module(SCRIPT_PATH, "run_stock_hypothesis_promotion_loop_stop")

    @dataclass(frozen=True)
    class FakeHypothesis:
        name: str
        freq: str = "weekly"

    config = runner.LoopConfig(
        run_tag="promo_loop_stop",
        start_date="2011-03-01",
        end_date="2026-03-01",
        oos_start_year=2016,
        trade_cost_bps=20.0,
        max_rounds=3,
        frontier_size=1,
        top_candidates_per_round=2,
        save_round_leaders=0,
        seed_names=("seed",),
        bonus_override=(),
        threshold_override=(),
        risk_off_override=(),
        entry_freeze_override=(),
        sector_cap_override=(),
        min_overlap_override=(),
        weight_mode_override=(),
        promotion_min_cost_bps=20.0,
        promotion_max_mdd_worse_pctp=10.0,
        promotion_min_p_alpha_gt0=0.90,
    )

    def fake_expand(frontier, round_index, loop_config):
        assert loop_config is config
        assert round_index == 1
        return [FakeHypothesis("fail"), FakeHypothesis("winner")]

    def fake_eval(research, promotion_runner, bt, records, hypothesis, config):
        passed = hypothesis.name == "winner"
        return {
            "hypothesis": hypothesis,
            "candidate_hash": hypothesis.name,
            "short_label": hypothesis.name,
            "config": {"name": hypothesis.name},
            "brief_config": {},
            "results_df": runner.pd.DataFrame(
                {
                    "entry_day": runner.pd.date_range("2024-01-05", periods=4, freq="W-FRI"),
                    "net_return_pct": [1.0, 1.0, 1.0, 1.0],
                    "benchmark_return_pct": [0.5, 0.5, 0.5, 0.5],
                    "turnover": [0.2, 0.2, 0.2, 0.2],
                }
            ),
            "full": {"cagr_diff_pct": 3.0 if passed else 1.0, "mdd_diff_pct": 1.0, "sharpe": 1.0, "benchmark_sharpe": 0.8, "avg_turnover": 0.2},
            "fixed_oos": {"cagr_diff_pct": 3.0 if passed else 1.0, "mdd_diff_pct": 1.0, "sharpe": 1.0, "benchmark_sharpe": 0.8},
            "promotion": {
                "criteria": [{"name": "x", "passes": passed}],
                "criteria_map": {"full_window_alpha": passed},
                "criteria_pass_count": 1 if passed else 0,
                "overall_pass": passed,
                "horizon_checks": [{"years": 3, "passes": passed, "cagr_diff_pct": 1.0, "nw_p_two": 0.05}],
                "headline": {"p_alpha_gt0": 0.95 if passed else 0.5, "nw_p_two_sided": 0.05},
            },
        }

    monkeypatch.setattr(runner, "_expand_frontier", fake_expand)
    monkeypatch.setattr(runner, "_evaluate_candidate", fake_eval)

    out = runner._run_loop(
        research=object(),
        promotion_runner=object(),
        bt=object(),
        records=[],
        frontier=[FakeHypothesis("seed")],
        config=config,
    )

    assert out["winner"]["short_label"] == "winner"
    assert out["best"]["short_label"] == "winner"
    assert len(out["rounds"]) == 1
    assert out["rounds"][0]["leader"]["short_label"] == "winner"
    assert out["rounds"][0]["artifacts"] == []
