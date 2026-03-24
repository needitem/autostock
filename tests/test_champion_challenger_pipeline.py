from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PIPELINE_PATH = ROOT / "src" / "pipelines" / "champion_challenger_pipeline.py"
RESEARCH_PATH = ROOT / "scripts" / "research_stock_hypotheses.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_build_sensitivity_suite_covers_sector_and_turnover_axes():
    pipeline = _load_module(PIPELINE_PATH, "champion_challenger_pipeline")
    research = _load_module(RESEARCH_PATH, "research_stock_hypotheses_for_ccp_test")

    champion = next(h for h in research._hypotheses() if h.name == "weekly_baseline_v4")
    suite = pipeline._build_sensitivity_suite(champion)
    by_name = {item.name: item for item in suite}

    assert len(suite) == 13
    assert suite[0].name == "weekly_baseline_v4"
    assert by_name["weekly_baseline_v4__sector_free"].max_per_sector == 0
    assert by_name["weekly_baseline_v4__sector_tight"].max_per_sector == 1
    assert by_name["weekly_baseline_v4__turnover_loose"].min_overlap == 3
    assert by_name["weekly_baseline_v4__turnover_tight"].min_overlap == 5
    assert by_name["weekly_veto_recentq_newonly_nrisk_soft_bonus"].pit_veto_new_only is True
    assert by_name["weekly_veto_recentq_newonly_neutral_soft_bonus_ro2"].top_k_risk_off == 2
    assert by_name["weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto35"].pit_veto_threshold == -3.5
    assert by_name["weekly_veto_recentq_newonly_neutral_soft_bonus_ro2_veto325"].pit_veto_threshold == -3.25
    assert by_name["weekly_veto_recentq_newonly_nrisk_soft_bonus_ro2"].pit_veto_regimes == ("neutral", "risk_off")


def test_ranking_key_prefers_positive_oos_alpha_then_lower_turnover():
    pipeline = _load_module(PIPELINE_PATH, "champion_challenger_pipeline_ranking")

    strong = {
        "oos": {
            "cagr_diff_pct": 2.0,
            "mdd_diff_pct": 1.0,
            "sharpe": 1.0,
            "benchmark_sharpe": 0.9,
            "avg_turnover": 0.25,
        }
    }
    weak = {
        "oos": {
            "cagr_diff_pct": 1.0,
            "mdd_diff_pct": 1.0,
            "sharpe": 1.2,
            "benchmark_sharpe": 0.9,
            "avg_turnover": 0.10,
        }
    }
    negative = {
        "oos": {
            "cagr_diff_pct": -1.0,
            "mdd_diff_pct": 5.0,
            "sharpe": 1.5,
            "benchmark_sharpe": 0.9,
            "avg_turnover": 0.05,
        }
    }

    assert pipeline._ranking_key(strong) > pipeline._ranking_key(weak)
    assert pipeline._ranking_key(weak) > pipeline._ranking_key(negative)


def test_quality_veto_filters_recent_low_quality_names_but_keeps_missing_data():
    research = _load_module(RESEARCH_PATH, "research_stock_hypotheses_for_veto_test")

    class FakeBt:
        @staticmethod
        def _pit_symbol_bonus(features, max_filing_age_days):
            assert max_filing_age_days == 180
            return {"AAA": -5.0, "BBB": -1.0}

    hypothesis = research.Hypothesis(
        name="test",
        freq="weekly",
        engine="stock_momentum",
        top_k=5,
        top_k_neutral=5,
        top_k_risk_off=0,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        sector_bonus=0.15,
        min_overlap=4,
        pit_veto_threshold=-2.5,
        pit_veto_max_filing_age=180,
    )
    feats = [{"symbol": "AAA"}, {"symbol": "BBB"}, {"symbol": "CCC"}]

    out = research._apply_pit_quality_veto(FakeBt(), feats, hypothesis)

    assert [row["symbol"] for row in out] == ["BBB", "CCC"]


def test_quality_veto_falls_back_when_filter_would_leave_too_few_names():
    research = _load_module(RESEARCH_PATH, "research_stock_hypotheses_for_veto_fallback_test")

    class FakeBt:
        @staticmethod
        def _pit_symbol_bonus(features, max_filing_age_days):
            return {"AAA": -5.0, "BBB": -4.0}

    hypothesis = research.Hypothesis(
        name="test",
        freq="weekly",
        engine="stock_momentum",
        top_k=5,
        top_k_neutral=5,
        top_k_risk_off=0,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        sector_bonus=0.15,
        min_overlap=4,
        pit_veto_threshold=-2.5,
        pit_veto_max_filing_age=180,
    )
    feats = [{"symbol": "AAA"}, {"symbol": "BBB"}]

    out = research._apply_pit_quality_veto(FakeBt(), feats, hypothesis)

    assert out == feats


def test_quality_veto_can_exempt_existing_holdings():
    research = _load_module(RESEARCH_PATH, "research_stock_hypotheses_for_veto_exempt_test")

    class FakeBt:
        @staticmethod
        def _pit_symbol_bonus(features, max_filing_age_days):
            return {"AAA": -5.0, "BBB": -1.0, "CCC": 0.5}

    hypothesis = research.Hypothesis(
        name="test",
        freq="weekly",
        engine="stock_momentum",
        top_k=5,
        top_k_neutral=5,
        top_k_risk_off=0,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        sector_bonus=0.15,
        min_overlap=4,
        pit_veto_threshold=-2.5,
        pit_veto_max_filing_age=180,
        pit_veto_new_only=True,
    )
    feats = [{"symbol": "AAA"}, {"symbol": "BBB"}, {"symbol": "CCC"}]

    out = research._apply_pit_quality_veto(FakeBt(), feats, hypothesis, exempt_symbols={"AAA"})

    assert [row["symbol"] for row in out] == ["AAA", "BBB", "CCC"]


def test_quality_veto_regime_gate_can_disable_filter():
    research = _load_module(RESEARCH_PATH, "research_stock_hypotheses_for_veto_regime_test")

    hypothesis = research.Hypothesis(
        name="test",
        freq="weekly",
        engine="stock_momentum",
        top_k=5,
        top_k_neutral=5,
        top_k_risk_off=0,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        sector_bonus=0.15,
        min_overlap=4,
        pit_veto_threshold=-2.5,
        pit_veto_regimes=("neutral", "risk_off"),
    )

    assert research._pit_veto_enabled_for_regime(hypothesis, "neutral") is True
    assert research._pit_veto_enabled_for_regime(hypothesis, "risk_off") is True
    assert research._pit_veto_enabled_for_regime(hypothesis, "risk_on") is False


def test_breadth_topk_gate_caps_weak_neutral_market():
    research = _load_module(RESEARCH_PATH, "research_stock_hypotheses_for_breadth_gate_test")

    class FakeBt:
        @staticmethod
        def _resolve_breadth_features(universe_features, safe_features, source_mode):
            assert source_mode == "universe"
            return universe_features

        @staticmethod
        def _market_breadth(features):
            return {"up200": 0.45, "positive_63d": 0.40}

        @staticmethod
        def _f(x, d=0.0):
            try:
                return float(x)
            except Exception:
                return d

    hypothesis = research.Hypothesis(
        name="test",
        freq="weekly",
        engine="stock_momentum",
        top_k=5,
        top_k_neutral=5,
        top_k_risk_off=2,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        sector_bonus=0.15,
        min_overlap=4,
        breadth_source_mode="universe",
        neutral_min_breadth_up200=0.50,
        neutral_min_breadth_positive63=0.45,
        neutral_max_positions_when_weak=2,
    )

    out = research._apply_breadth_topk_gate(
        bt=FakeBt(),
        feats=[{"symbol": "A"}],
        safe_feats=[{"symbol": "A"}],
        hypothesis=hypothesis,
        market_regime="neutral",
        target_top_k=5,
    )

    assert out == 2


def test_breadth_topk_gate_is_noop_outside_neutral():
    research = _load_module(RESEARCH_PATH, "research_stock_hypotheses_for_breadth_gate_regime_test")

    class FakeBt:
        @staticmethod
        def _resolve_breadth_features(universe_features, safe_features, source_mode):
            raise AssertionError("should not be called")

        @staticmethod
        def _market_breadth(features):
            raise AssertionError("should not be called")

        @staticmethod
        def _f(x, d=0.0):
            return d

    hypothesis = research.Hypothesis(
        name="test",
        freq="weekly",
        engine="stock_momentum",
        top_k=5,
        top_k_neutral=5,
        top_k_risk_off=2,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        sector_bonus=0.15,
        min_overlap=4,
        neutral_min_breadth_up200=0.50,
        neutral_min_breadth_positive63=0.45,
        neutral_max_positions_when_weak=2,
    )

    out = research._apply_breadth_topk_gate(
        bt=FakeBt(),
        feats=[],
        safe_feats=[],
        hypothesis=hypothesis,
        market_regime="risk_off",
        target_top_k=2,
    )

    assert out == 2


def test_sector_focus_filters_to_top_ranked_sectors_but_keeps_exempt_symbols():
    research = _load_module(RESEARCH_PATH, "research_stock_hypotheses_for_sector_focus_test")

    hypothesis = research.Hypothesis(
        name="test",
        freq="weekly",
        engine="stock_momentum",
        top_k=5,
        top_k_neutral=5,
        top_k_risk_off=2,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        sector_bonus=0.15,
        min_overlap=4,
        sector_focus_top_n=2,
    )
    feats = [
        {"symbol": "AAA", "sector": "Tech"},
        {"symbol": "BBB", "sector": "Health"},
        {"symbol": "CCC", "sector": "Energy"},
        {"symbol": "DDD", "sector": "Utilities"},
    ]
    sector_scores = {"Tech": 5.0, "Health": 4.0, "Energy": 1.0, "Utilities": -1.0}

    out = research._apply_sector_focus(feats, hypothesis, sector_scores, exempt_symbols={"DDD"})

    assert [row["symbol"] for row in out] == ["AAA", "BBB", "DDD"]


def test_sector_focus_regime_gate_can_disable_filter():
    research = _load_module(RESEARCH_PATH, "research_stock_hypotheses_for_sector_regime_test")

    hypothesis = research.Hypothesis(
        name="test",
        freq="weekly",
        engine="stock_momentum",
        top_k=5,
        top_k_neutral=5,
        top_k_risk_off=2,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        sector_bonus=0.15,
        min_overlap=4,
        sector_focus_top_n=3,
        sector_focus_regimes=("neutral",),
    )

    assert research._sector_focus_enabled_for_regime(hypothesis, "neutral") is True
    assert research._sector_focus_enabled_for_regime(hypothesis, "risk_off") is False


def test_breadth_new_entry_gate_can_freeze_new_entries_in_weak_neutral():
    research = _load_module(RESEARCH_PATH, "research_stock_hypotheses_for_entry_gate_test")

    class FakeBt:
        @staticmethod
        def _resolve_breadth_features(universe_features, safe_features, source_mode):
            assert source_mode == "universe"
            return universe_features

        @staticmethod
        def _market_breadth(features):
            return {"up200": 0.40, "positive_63d": 0.40}

        @staticmethod
        def _f(x, d=0.0):
            try:
                return float(x)
            except Exception:
                return d

    hypothesis = research.Hypothesis(
        name="test",
        freq="weekly",
        engine="stock_momentum",
        top_k=5,
        top_k_neutral=5,
        top_k_risk_off=2,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        sector_bonus=0.15,
        min_overlap=4,
        breadth_entry_source_mode="universe",
        neutral_entry_min_breadth_up200=0.50,
        neutral_entry_min_breadth_positive63=0.45,
        neutral_max_new_names_when_weak=0,
    )
    candidates = [{"symbol": "AAA"}, {"symbol": "BBB"}, {"symbol": "CCC"}]

    out = research._apply_breadth_new_entry_gate(
        bt=FakeBt(),
        feats=candidates,
        safe_feats=candidates,
        candidates=candidates,
        hypothesis=hypothesis,
        market_regime="neutral",
        held_syms=["AAA"],
    )

    assert [row["symbol"] for row in out] == ["AAA"]


def test_breadth_new_entry_gate_can_allow_limited_new_names():
    research = _load_module(RESEARCH_PATH, "research_stock_hypotheses_for_entry_gate_limit_test")

    class FakeBt:
        @staticmethod
        def _resolve_breadth_features(universe_features, safe_features, source_mode):
            return universe_features

        @staticmethod
        def _market_breadth(features):
            return {"up200": 0.40, "positive_63d": 0.40}

        @staticmethod
        def _f(x, d=0.0):
            try:
                return float(x)
            except Exception:
                return d

    hypothesis = research.Hypothesis(
        name="test",
        freq="weekly",
        engine="stock_momentum",
        top_k=5,
        top_k_neutral=5,
        top_k_risk_off=2,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        sector_bonus=0.15,
        min_overlap=4,
        neutral_entry_min_breadth_up200=0.50,
        neutral_entry_min_breadth_positive63=0.45,
        neutral_max_new_names_when_weak=1,
    )
    candidates = [{"symbol": "AAA"}, {"symbol": "BBB"}, {"symbol": "CCC"}]

    out = research._apply_breadth_new_entry_gate(
        bt=FakeBt(),
        feats=candidates,
        safe_feats=candidates,
        candidates=candidates,
        hypothesis=hypothesis,
        market_regime="neutral",
        held_syms=["AAA"],
    )

    assert [row["symbol"] for row in out] == ["AAA", "BBB"]
