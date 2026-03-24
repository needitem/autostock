from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "backtest_ai_portfolio_selector.py"
SPEC = importlib.util.spec_from_file_location("backtest_ai_portfolio_selector", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
bt = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bt)


def test_resolve_breadth_features_uses_universe_by_default():
    universe = [
        {"symbol": "AAA", "ma200_gap": 5.0, "ma50_gap": 4.0, "return_63d": 8.0},
        {"symbol": "BBB", "ma200_gap": -3.0, "ma50_gap": -2.0, "return_63d": -6.0},
    ]
    safe = [universe[0]]

    out = bt._resolve_breadth_features(universe, safe, "universe")

    assert out == universe


def test_resolve_breadth_features_can_use_safe_subset():
    universe = [
        {"symbol": "AAA", "ma200_gap": 5.0, "ma50_gap": 4.0, "return_63d": 8.0},
        {"symbol": "BBB", "ma200_gap": -3.0, "ma50_gap": -2.0, "return_63d": -6.0},
    ]
    safe = [universe[0]]

    out = bt._resolve_breadth_features(universe, safe, "safe")

    assert out == safe


def test_resolve_momentum_blend_scope_defaults_to_disabled_for_ai(monkeypatch):
    monkeypatch.delenv("AI_MOMENTUM_BLEND_SCOPE", raising=False)

    out = bt._resolve_momentum_blend_scope("ai", False)

    assert out == "disabled"


def test_resolve_momentum_blend_scope_keeps_legacy_sitout_flag(monkeypatch):
    monkeypatch.delenv("AI_MOMENTUM_BLEND_SCOPE", raising=False)

    out = bt._resolve_momentum_blend_scope("ai", True)

    assert out == "sitout_only"


def test_resolve_momentum_blend_scope_respects_explicit_env(monkeypatch):
    monkeypatch.setenv("AI_MOMENTUM_BLEND_SCOPE", "portfolio")

    out = bt._resolve_momentum_blend_scope("ai", False)

    assert out == "portfolio"


def test_resolve_universe_asof_uses_latest_available_snapshot():
    lookup = bt._build_universe_asof_lookup(
        {
            "2024-01-05": ["AAA", "BBB"],
            "2024-01-12": ["BBB", "CCC"],
            "2024-01-26": ["DDD"],
        }
    )

    out = bt._resolve_universe_asof(lookup, "2024-01-31", ["ZZZ"])

    assert out == ["DDD"]


def test_expand_regime_allowed_symbols_keeps_loaded_regime_assets():
    out = bt._expand_regime_allowed_symbols(
        allowed={"AAPL", "MSFT"},
        regime_symbols=["TQQQ", "QQQ", "GLD"],
        by_symbol={
            "AAPL": {"symbol": "AAPL"},
            "MSFT": {"symbol": "MSFT"},
            "QQQ": {"symbol": "QQQ"},
            "GLD": {"symbol": "GLD"},
        },
    )

    assert out == {"AAPL", "MSFT", "QQQ", "GLD"}


def test_ai_regime_target_cap_allows_constructive_neutral_entries():
    out = bt._ai_regime_target_cap(
        target_top_k=0,
        regime="neutral",
        safe_feats=[{"symbol": "A"}, {"symbol": "B"}, {"symbol": "C"}],
        breadth={"up200": 0.62, "positive_63d": 0.58},
        vix_close=21.0,
        neutral_cap=2,
        neutral_min_up200=0.55,
        neutral_min_pos63=0.55,
        risk_off_cap=0,
        risk_off_min_up200=0.25,
        risk_off_min_pos63=0.25,
        risk_off_max_vix=45.0,
    )

    assert out == 2


def test_ai_regime_target_cap_blocks_weak_neutral_entries():
    out = bt._ai_regime_target_cap(
        target_top_k=0,
        regime="neutral",
        safe_feats=[{"symbol": "A"}, {"symbol": "B"}, {"symbol": "C"}],
        breadth={"up200": 0.40, "positive_63d": 0.41},
        vix_close=26.0,
        neutral_cap=2,
        neutral_min_up200=0.55,
        neutral_min_pos63=0.55,
        risk_off_cap=0,
        risk_off_min_up200=0.25,
        risk_off_min_pos63=0.25,
        risk_off_max_vix=45.0,
    )

    assert out == 0


def test_ai_regime_target_cap_allows_limited_risk_off_when_breadth_and_vix_ok():
    out = bt._ai_regime_target_cap(
        target_top_k=0,
        regime="risk_off",
        safe_feats=[{"symbol": "A"}, {"symbol": "B"}],
        breadth={"up200": 0.31, "positive_63d": 0.36},
        vix_close=33.0,
        neutral_cap=0,
        neutral_min_up200=0.55,
        neutral_min_pos63=0.55,
        risk_off_cap=1,
        risk_off_min_up200=0.25,
        risk_off_min_pos63=0.30,
        risk_off_max_vix=35.0,
    )

    assert out == 1


def test_regime_portfolio_uses_recovery_state_for_shallow_below_ma_rebounds():
    out = bt._regime_portfolio_from_features(
        by_symbol={
            "QQQ": {
                "symbol": "QQQ",
                "close": 98.5,
                "ma100": 99.0,
                "ma200": 100.0,
                "return_21d": 2.2,
                "vol_20": 0.038,
                "dd_252": -0.08,
            }
        },
        prev_state=None,
        regime_source="QQQ",
        ma_fast=100,
        ma_slow=200,
        mom_lb=21,
        mom_thr=0.0,
        risk_on_alloc={"TQQQ": 1.0},
        risk_on_alt_alloc={"QLD": 1.0},
        neutral_alloc={"QLD": 1.0},
        recovery_alloc={"QQQ": 1.0},
        risk_off_alloc={"GLD": 1.0},
        crash_alloc={"GLD": 1.0},
        vol_cap=0.05,
        vol_low=0.035,
        vol_mid=0.04,
        mom_strong=0.06,
        crash_vol=0.06,
        crash_dd=-0.2,
        hysteresis=0.0,
        recovery_slow_buffer=0.03,
        recovery_min_mom=0.015,
        recovery_max_vol=0.045,
        recovery_dd_floor=-0.12,
        risk_on_filter_asset=None,
        risk_on_filter_ma=0,
        risk_on_filter_safe_alloc={"BIL": 1.0},
    )

    assert out["_regime_state"] == "recovery"
    assert out["_regime_reason"] == "recovery"
    assert out["positions"] == [{"symbol": "QQQ", "weight_pct": 100.0}]


def test_regime_portfolio_keeps_risk_off_for_deeper_damage():
    out = bt._regime_portfolio_from_features(
        by_symbol={
            "QQQ": {
                "symbol": "QQQ",
                "close": 95.0,
                "ma100": 98.0,
                "ma200": 100.0,
                "return_21d": 0.8,
                "vol_20": 0.041,
                "dd_252": -0.18,
            }
        },
        prev_state=None,
        regime_source="QQQ",
        ma_fast=100,
        ma_slow=200,
        mom_lb=21,
        mom_thr=0.0,
        risk_on_alloc={"TQQQ": 1.0},
        risk_on_alt_alloc={"QLD": 1.0},
        neutral_alloc={"QLD": 1.0},
        recovery_alloc={"QQQ": 1.0},
        risk_off_alloc={"GLD": 1.0},
        crash_alloc={"GLD": 1.0},
        vol_cap=0.05,
        vol_low=0.035,
        vol_mid=0.04,
        mom_strong=0.06,
        crash_vol=0.06,
        crash_dd=-0.2,
        hysteresis=0.0,
        recovery_slow_buffer=0.03,
        recovery_min_mom=0.015,
        recovery_max_vol=0.045,
        recovery_dd_floor=-0.12,
        risk_on_filter_asset=None,
        risk_on_filter_ma=0,
        risk_on_filter_safe_alloc={"BIL": 1.0},
    )

    assert out["_regime_state"] == "risk_off"
    assert out["_regime_reason"] == "risk_off"
    assert out["positions"] == [{"symbol": "GLD", "weight_pct": 100.0}]


def test_regime_portfolio_downshifts_levered_state_when_filter_asset_breaks_ma():
    out = bt._regime_portfolio_from_features(
        by_symbol={
            "QQQ": {
                "symbol": "QQQ",
                "close": 110.0,
                "ma100": 104.0,
                "ma200": 100.0,
                "return_21d": 7.0,
                "vol_20": 0.028,
                "dd_252": -0.04,
            },
            "QLD": {
                "symbol": "QLD",
                "close": 96.0,
                "ma50": 100.0,
            },
        },
        prev_state=None,
        regime_source="QQQ",
        ma_fast=100,
        ma_slow=200,
        mom_lb=21,
        mom_thr=0.0,
        risk_on_alloc={"TQQQ": 0.8, "QQQ": 0.2},
        risk_on_alt_alloc={"QLD": 1.0},
        neutral_alloc={"QLD": 1.0},
        recovery_alloc={"QQQ": 1.0},
        risk_off_alloc={"GLD": 1.0},
        crash_alloc={"GLD": 1.0},
        vol_cap=0.05,
        vol_low=0.035,
        vol_mid=0.04,
        mom_strong=0.06,
        crash_vol=0.06,
        crash_dd=-0.2,
        hysteresis=0.0,
        recovery_slow_buffer=0.03,
        recovery_min_mom=0.015,
        recovery_max_vol=0.045,
        recovery_dd_floor=-0.12,
        risk_on_filter_asset="QLD",
        risk_on_filter_ma=50,
        risk_on_filter_safe_alloc={"QQQ": 1.0},
    )

    assert out["_regime_state"] == "neutral"
    assert out["_regime_reason"] == "risk_filter_safe"
    assert out["positions"] == [{"symbol": "QQQ", "weight_pct": 100.0}]


def test_regime_portfolio_keeps_levered_state_when_filter_asset_confirms():
    out = bt._regime_portfolio_from_features(
        by_symbol={
            "QQQ": {
                "symbol": "QQQ",
                "close": 110.0,
                "ma100": 104.0,
                "ma200": 100.0,
                "return_21d": 7.0,
                "vol_20": 0.028,
                "dd_252": -0.04,
            },
            "QLD": {
                "symbol": "QLD",
                "close": 103.0,
                "ma50": 100.0,
            },
        },
        prev_state=None,
        regime_source="QQQ",
        ma_fast=100,
        ma_slow=200,
        mom_lb=21,
        mom_thr=0.0,
        risk_on_alloc={"TQQQ": 0.8, "QQQ": 0.2},
        risk_on_alt_alloc={"QLD": 1.0},
        neutral_alloc={"QLD": 1.0},
        recovery_alloc={"QQQ": 1.0},
        risk_off_alloc={"GLD": 1.0},
        crash_alloc={"GLD": 1.0},
        vol_cap=0.05,
        vol_low=0.035,
        vol_mid=0.04,
        mom_strong=0.06,
        crash_vol=0.06,
        crash_dd=-0.2,
        hysteresis=0.0,
        recovery_slow_buffer=0.03,
        recovery_min_mom=0.015,
        recovery_max_vol=0.045,
        recovery_dd_floor=-0.12,
        risk_on_filter_asset="QLD",
        risk_on_filter_ma=50,
        risk_on_filter_safe_alloc={"QQQ": 1.0},
    )

    assert out["_regime_state"] == "risk_on"
    assert out["_regime_reason"] == "risk_on"
    assert out["positions"] == [
        {"symbol": "TQQQ", "weight_pct": 80.0},
        {"symbol": "QQQ", "weight_pct": 20.0},
    ]


def test_stock_momentum_portfolio_picks_top_relative_strength_names():
    out = bt._stock_momentum_portfolio(
        candidates=[
            {"symbol": "NVDA", "relative_strength_63d": 18.0, "relative_strength_21d": 8.0},
            {"symbol": "META", "relative_strength_63d": 14.0, "relative_strength_21d": 6.0},
            {"symbol": "AMD", "relative_strength_63d": 9.0, "relative_strength_21d": 2.0},
        ],
        top_k=2,
        weight_mode="equal",
        min_positions_for_invest=2,
    )

    assert out["_stock_momo_mode"] is True
    assert out["_sit_out"] is False
    assert out["positions"] == [
        {"symbol": "NVDA", "weight_pct": 50.0},
        {"symbol": "META", "weight_pct": 50.0},
    ]


def test_stock_momentum_portfolio_sits_out_when_not_enough_names():
    out = bt._stock_momentum_portfolio(
        candidates=[{"symbol": "NVDA", "relative_strength_63d": 18.0, "relative_strength_21d": 8.0}],
        top_k=2,
        weight_mode="equal",
        min_positions_for_invest=2,
    )

    assert out["_stock_momo_mode"] is True
    assert out["_sit_out"] is True
    assert out["positions"] == []
    assert out["cash_pct"] == 100.0


def test_enforce_min_overlap_keeps_prior_names_when_possible():
    out = bt._enforce_min_overlap(
        weights_pct={"NVDA": 20.0, "META": 20.0, "AMD": 20.0, "AVGO": 20.0, "NFLX": 20.0},
        prev_port={"AAPL": 0.2, "MSFT": 0.2, "NVDA": 0.2, "META": 0.2, "AMD": 0.2},
        allowed={"AAPL", "MSFT", "NVDA", "META", "AMD", "AVGO", "NFLX"},
        feats_by_symbol={
            "AAPL": {"relative_strength_63d": 9.0},
            "MSFT": {"relative_strength_63d": 8.0},
            "NVDA": {"relative_strength_63d": 16.0},
            "META": {"relative_strength_63d": 14.0},
            "AMD": {"relative_strength_63d": 12.0},
            "AVGO": {"relative_strength_63d": 11.0},
            "NFLX": {"relative_strength_63d": 10.0},
        },
        min_overlap=4,
        top_k=5,
    )

    kept = set(out.keys()) & {"AAPL", "MSFT", "NVDA", "META", "AMD"}
    assert len(kept) >= 4


def test_sector_strength_scores_reward_stronger_sector():
    scores = bt._sector_strength_scores(
        [
            {"sector": "Tech", "relative_strength_63d": 12.0, "relative_strength_21d": 6.0, "ma200_gap": 8.0, "ma50_gap": 4.0},
            {"sector": "Tech", "relative_strength_63d": 10.0, "relative_strength_21d": 5.0, "ma200_gap": 7.0, "ma50_gap": 3.0},
            {"sector": "Health", "relative_strength_63d": 3.0, "relative_strength_21d": 1.0, "ma200_gap": -2.0, "ma50_gap": -1.0},
            {"sector": "Health", "relative_strength_63d": 2.0, "relative_strength_21d": 0.5, "ma200_gap": -1.0, "ma50_gap": -0.5},
        ]
    )

    assert scores["Tech"] > scores["Health"]


def test_stock_momentum_portfolio_can_limit_sector_concentration():
    out = bt._stock_momentum_portfolio(
        candidates=[
            {"symbol": "NVDA", "relative_strength_63d": 18.0, "relative_strength_21d": 8.0, "sector": "Tech"},
            {"symbol": "AMD", "relative_strength_63d": 17.0, "relative_strength_21d": 7.0, "sector": "Tech"},
            {"symbol": "META", "relative_strength_63d": 16.0, "relative_strength_21d": 6.0, "sector": "Comm"},
            {"symbol": "LLY", "relative_strength_63d": 15.0, "relative_strength_21d": 5.0, "sector": "Health"},
        ],
        top_k=3,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=1,
        sector_bonus_mult=0.0,
        sector_scores={},
    )

    picked = {row["symbol"] for row in out["positions"]}
    assert picked == {"NVDA", "META", "LLY"}


def test_stock_event_portfolio_requires_recent_positive_filing():
    out = bt._stock_event_portfolio(
        candidates=[
            {
                "symbol": "NVDA",
                "sector": "Technology",
                "relative_strength_63d": 18.0,
                "relative_strength_21d": 8.0,
                "ma200_gap": 12.0,
                "pit_has_data": True,
                "pit_filing_age_days": 20,
                "pit_rev_yoy_pct": 25.0,
                "pit_eps_yoy_pct": 30.0,
                "pit_ni_yoy_pct": 28.0,
            },
            {
                "symbol": "META",
                "sector": "Communication",
                "relative_strength_63d": 14.0,
                "relative_strength_21d": 5.0,
                "ma200_gap": 10.0,
                "pit_has_data": True,
                "pit_filing_age_days": 26,
                "pit_rev_yoy_pct": 12.0,
                "pit_eps_yoy_pct": 11.0,
                "pit_ni_yoy_pct": 8.0,
            },
            {
                "symbol": "OLD",
                "sector": "Technology",
                "relative_strength_63d": 20.0,
                "relative_strength_21d": 9.0,
                "ma200_gap": 14.0,
                "pit_has_data": True,
                "pit_filing_age_days": 120,
                "pit_rev_yoy_pct": 30.0,
                "pit_eps_yoy_pct": 35.0,
                "pit_ni_yoy_pct": 33.0,
            },
        ],
        top_k=2,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        max_filing_age_days=45,
        min_rev_yoy=5.0,
        min_eps_yoy=5.0,
        min_ni_yoy=0.0,
    )

    assert out["_stock_event_mode"] is True
    assert out["_sit_out"] is False
    assert out["positions"] == [
        {"symbol": "NVDA", "weight_pct": 50.0},
        {"symbol": "META", "weight_pct": 50.0},
    ]


def test_earnings_drift_portfolio_prefers_recent_positive_surprises():
    out = bt._earnings_drift_portfolio(
        candidates=[
            {
                "symbol": "NVDA",
                "sector": "Technology",
                "earnings_has_data": True,
                "earnings_days_since": 12,
                "earnings_surprise_pct": 8.0,
                "relative_strength_21d": 6.0,
                "relative_strength_63d": 10.0,
                "ma50_gap": 8.0,
                "ma200_gap": 15.0,
            },
            {
                "symbol": "META",
                "sector": "Communication",
                "earnings_has_data": True,
                "earnings_days_since": 18,
                "earnings_surprise_pct": 6.0,
                "relative_strength_21d": 5.0,
                "relative_strength_63d": 9.0,
                "ma50_gap": 7.0,
                "ma200_gap": 12.0,
            },
            {
                "symbol": "OLD",
                "sector": "Technology",
                "earnings_has_data": True,
                "earnings_days_since": 80,
                "earnings_surprise_pct": 20.0,
                "relative_strength_21d": 8.0,
                "relative_strength_63d": 12.0,
                "ma50_gap": 9.0,
                "ma200_gap": 16.0,
            },
        ],
        top_k=2,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        max_days_since=35,
        min_surprise_pct=3.0,
    )

    assert out["_earnings_drift_mode"] is True
    assert out["positions"] == [
        {"symbol": "NVDA", "weight_pct": 50.0},
        {"symbol": "META", "weight_pct": 50.0},
    ]


def test_pit_symbol_bonus_prefers_better_fundamentals_with_recent_filing():
    bonus = bt._pit_symbol_bonus(
        [
            {
                "symbol": "AAPL",
                "pit_has_data": True,
                "pit_filing_age_days": 30,
                "pit_rev_yoy_pct": 18.0,
                "pit_ni_yoy_pct": 20.0,
                "pit_eps_yoy_pct": 21.0,
                "pit_op_margin_pct": 31.0,
                "pit_gross_margin_pct": 46.0,
                "pit_equity_ratio": 0.42,
                "pit_debt_to_assets": 0.35,
            },
            {
                "symbol": "CSCO",
                "pit_has_data": True,
                "pit_filing_age_days": 40,
                "pit_rev_yoy_pct": 4.0,
                "pit_ni_yoy_pct": 3.0,
                "pit_eps_yoy_pct": 2.0,
                "pit_op_margin_pct": 21.0,
                "pit_gross_margin_pct": 32.0,
                "pit_equity_ratio": 0.25,
                "pit_debt_to_assets": 0.58,
            },
            {
                "symbol": "OLD",
                "pit_has_data": True,
                "pit_filing_age_days": 260,
                "pit_rev_yoy_pct": 30.0,
                "pit_ni_yoy_pct": 30.0,
                "pit_eps_yoy_pct": 30.0,
                "pit_op_margin_pct": 40.0,
                "pit_gross_margin_pct": 60.0,
                "pit_equity_ratio": 0.60,
                "pit_debt_to_assets": 0.10,
            },
            {
                "symbol": "NONE",
                "pit_has_data": False,
            },
            {
                "symbol": "MID",
                "pit_has_data": True,
                "pit_filing_age_days": 35,
                "pit_rev_yoy_pct": 9.0,
                "pit_ni_yoy_pct": 8.0,
                "pit_eps_yoy_pct": 10.0,
                "pit_op_margin_pct": 25.0,
                "pit_gross_margin_pct": 37.0,
                "pit_equity_ratio": 0.34,
                "pit_debt_to_assets": 0.47,
            },
        ],
        max_filing_age_days=180,
    )

    assert "OLD" not in bonus
    assert "NONE" not in bonus
    assert bonus["AAPL"] > bonus["MID"] > bonus["CSCO"]


def test_apply_pit_quality_veto_filters_low_scoring_new_candidates(monkeypatch):
    def fake_bonus(features, max_filing_age_days):
        assert max_filing_age_days == 180
        return {"AAA": -5.0, "BBB": -1.0}

    monkeypatch.setattr(bt, "_pit_symbol_bonus", fake_bonus)

    out = bt._apply_pit_quality_veto(
        [{"symbol": "AAA"}, {"symbol": "BBB"}, {"symbol": "CCC"}],
        threshold=-2.5,
        max_filing_age_days=180,
        min_positions_for_invest=2,
    )

    assert [row["symbol"] for row in out] == ["BBB", "CCC"]


def test_apply_pit_quality_veto_keeps_exempt_existing_holdings(monkeypatch):
    monkeypatch.setattr(bt, "_pit_symbol_bonus", lambda features, max_filing_age_days: {"AAA": -5.0, "BBB": -1.0})

    out = bt._apply_pit_quality_veto(
        [{"symbol": "AAA"}, {"symbol": "BBB"}, {"symbol": "CCC"}],
        threshold=-2.5,
        max_filing_age_days=180,
        min_positions_for_invest=2,
        exempt_symbols={"AAA"},
    )

    assert [row["symbol"] for row in out] == ["AAA", "BBB", "CCC"]


def test_pit_veto_enabled_for_regime_respects_explicit_gate():
    assert bt._pit_veto_enabled_for_regime(("neutral", "risk_off"), "neutral") is True
    assert bt._pit_veto_enabled_for_regime(("neutral", "risk_off"), "risk_off") is True
    assert bt._pit_veto_enabled_for_regime(("neutral", "risk_off"), "risk_on") is False


def test_breadth_new_entry_gate_freezes_new_names_when_neutral_breadth_is_weak():
    out = bt._apply_breadth_new_entry_gate(
        candidates=[{"symbol": "AAA"}, {"symbol": "BBB"}, {"symbol": "CCC"}],
        held_syms=["AAA"],
        market_regime="neutral",
        breadth={"up200": 0.40, "positive_63d": 0.40},
        neutral_min_up200=0.50,
        neutral_min_pos63=0.45,
        neutral_max_new_when_weak=0,
    )

    assert [row["symbol"] for row in out] == ["AAA"]


def test_breadth_new_entry_gate_is_noop_when_neutral_breadth_is_strong():
    out = bt._apply_breadth_new_entry_gate(
        candidates=[{"symbol": "AAA"}, {"symbol": "BBB"}],
        held_syms=["AAA"],
        market_regime="neutral",
        breadth={"up200": 0.60, "positive_63d": 0.55},
        neutral_min_up200=0.50,
        neutral_min_pos63=0.45,
        neutral_max_new_when_weak=0,
    )

    assert [row["symbol"] for row in out] == ["AAA", "BBB"]


def test_quality_momentum_portfolio_prefers_recent_quality_leaders():
    out = bt._quality_momentum_portfolio(
        candidates=[
            {
                "symbol": "NVDA",
                "sector": "Technology",
                "relative_strength_63d": 18.0,
                "relative_strength_21d": 7.0,
                "return_126d": 42.0,
                "ma200_gap": 15.0,
                "ma50_gap": 6.0,
                "vol_20": 0.020,
                "atr_pct": 2.1,
                "dd_252": -0.08,
                "pit_has_data": True,
                "pit_filing_age_days": 35,
                "pit_rev_yoy_pct": 26.0,
                "pit_ni_yoy_pct": 32.0,
                "pit_eps_yoy_pct": 34.0,
                "pit_op_margin_pct": 37.0,
                "pit_gross_margin_pct": 58.0,
                "pit_equity_ratio": 0.52,
                "pit_debt_to_assets": 0.25,
            },
            {
                "symbol": "META",
                "sector": "Communication",
                "relative_strength_63d": 15.0,
                "relative_strength_21d": 6.0,
                "return_126d": 30.0,
                "ma200_gap": 11.0,
                "ma50_gap": 5.0,
                "vol_20": 0.018,
                "atr_pct": 1.8,
                "dd_252": -0.09,
                "pit_has_data": True,
                "pit_filing_age_days": 48,
                "pit_rev_yoy_pct": 18.0,
                "pit_ni_yoy_pct": 20.0,
                "pit_eps_yoy_pct": 19.0,
                "pit_op_margin_pct": 31.0,
                "pit_gross_margin_pct": 42.0,
                "pit_equity_ratio": 0.41,
                "pit_debt_to_assets": 0.31,
            },
            {
                "symbol": "CSCO",
                "sector": "Technology",
                "relative_strength_63d": 8.0,
                "relative_strength_21d": 2.0,
                "return_126d": 12.0,
                "ma200_gap": 4.0,
                "ma50_gap": 1.0,
                "vol_20": 0.012,
                "atr_pct": 1.1,
                "dd_252": -0.12,
                "pit_has_data": True,
                "pit_filing_age_days": 44,
                "pit_rev_yoy_pct": 3.0,
                "pit_ni_yoy_pct": 2.0,
                "pit_eps_yoy_pct": 1.0,
                "pit_op_margin_pct": 20.0,
                "pit_gross_margin_pct": 29.0,
                "pit_equity_ratio": 0.23,
                "pit_debt_to_assets": 0.58,
            },
        ],
        top_k=2,
        weight_mode="equal",
        min_positions_for_invest=2,
        max_per_sector=2,
        sector_bonus_mult=0.0,
        sector_scores={},
        max_filing_age_days=240,
        quality_weight=0.65,
        momentum_weight=0.35,
        min_quality_score=-1.5,
        min_momentum_score=0.0,
        require_trend_template=False,
    )

    assert out["_quality_momo_mode"] is True
    assert out["_sit_out"] is False
    assert out["positions"] == [
        {"symbol": "NVDA", "weight_pct": 50.0},
        {"symbol": "META", "weight_pct": 50.0},
    ]


def test_select_regime_defensive_allocation_prefers_trending_safe_asset():
    alloc, reason = bt._select_regime_defensive_allocation(
        by_symbol={
            "GLD": {"return_21d": 2.5, "return_63d": 7.0, "ma200_gap": 6.0, "vol_20": 0.012, "dd_252": -0.03},
            "IEF": {"return_21d": 0.8, "return_63d": 1.4, "ma200_gap": 1.0, "vol_20": 0.009, "dd_252": -0.02},
            "TLT": {"return_21d": -1.2, "return_63d": -3.5, "ma200_gap": -4.0, "vol_20": 0.016, "dd_252": -0.12},
        },
        pool_symbols=["GLD", "IEF", "TLT"],
        top_n=1,
        fallback_alloc={"BIL": 1.0},
        min_ma_gap=0.0,
        min_ret21=0.0,
        min_ret63=0.0,
        max_vol=0.05,
        min_dd252=-0.20,
        weight_mode="inv_vol",
    )

    assert alloc == {"GLD": 1.0}
    assert reason == "dynamic:GLD"


def test_select_regime_defensive_allocation_falls_back_when_pool_is_weak():
    alloc, reason = bt._select_regime_defensive_allocation(
        by_symbol={
            "GLD": {"return_21d": -0.5, "return_63d": -1.0, "ma200_gap": -0.2, "vol_20": 0.015, "dd_252": -0.06},
            "IEF": {"return_21d": -0.1, "return_63d": -0.4, "ma200_gap": -0.1, "vol_20": 0.010, "dd_252": -0.04},
        },
        pool_symbols=["GLD", "IEF"],
        top_n=1,
        fallback_alloc={"BIL": 1.0},
        min_ma_gap=0.0,
        min_ret21=0.0,
        min_ret63=0.0,
        max_vol=0.05,
        min_dd252=-0.20,
        weight_mode="inv_vol",
    )

    assert alloc == {"BIL": 1.0}
    assert reason == "fallback"


def test_regime_portfolio_can_use_dynamic_defensive_sleeve():
    out = bt._regime_portfolio_from_features(
        by_symbol={
            "QQQ": {
                "symbol": "QQQ",
                "close": 95.0,
                "ma100": 98.0,
                "ma200": 100.0,
                "return_21d": -0.4,
                "vol_20": 0.033,
                "dd_252": -0.08,
            },
            "GLD": {
                "symbol": "GLD",
                "return_21d": 2.5,
                "return_63d": 7.0,
                "ma200_gap": 6.0,
                "vol_20": 0.012,
                "dd_252": -0.03,
            },
            "IEF": {
                "symbol": "IEF",
                "return_21d": 0.4,
                "return_63d": 1.3,
                "ma200_gap": 1.5,
                "vol_20": 0.009,
                "dd_252": -0.02,
            },
        },
        prev_state=None,
        regime_source="QQQ",
        ma_fast=100,
        ma_slow=200,
        mom_lb=21,
        mom_thr=0.0,
        risk_on_alloc={"TQQQ": 1.0},
        risk_on_alt_alloc={"QLD": 1.0},
        neutral_alloc={"QLD": 1.0},
        recovery_alloc={"QQQ": 1.0},
        risk_off_alloc={"GLD": 1.0},
        crash_alloc={"GLD": 1.0},
        vol_cap=0.05,
        vol_low=0.035,
        vol_mid=0.04,
        mom_strong=0.06,
        crash_vol=0.06,
        crash_dd=-0.2,
        hysteresis=0.0,
        recovery_slow_buffer=0.03,
        recovery_min_mom=0.015,
        recovery_max_vol=0.045,
        recovery_dd_floor=-0.12,
        risk_on_filter_asset=None,
        risk_on_filter_ma=0,
        risk_on_filter_safe_alloc={"BIL": 1.0},
        risk_off_dynamic=True,
        risk_off_pool_symbols=["GLD", "IEF"],
        risk_off_top_n=1,
        risk_off_min_ma_gap=0.0,
        risk_off_min_ret21=0.0,
        risk_off_min_ret63=0.0,
        risk_off_max_vol=0.05,
        risk_off_min_dd252=-0.20,
        risk_off_weight_mode="inv_vol",
        risk_off_fallback_alloc={"BIL": 1.0},
    )

    assert out["_regime_state"] == "risk_off"
    assert out["_regime_reason"] == "risk_off:dynamic:GLD"
    assert out["positions"] == [{"symbol": "GLD", "weight_pct": 100.0}]


def test_regime_portfolio_downshifts_levered_state_when_filter_asset_loses_ma():
    out = bt._regime_portfolio_from_features(
        by_symbol={
            "QQQ": {
                "symbol": "QQQ",
                "close": 110.0,
                "ma100": 104.0,
                "ma200": 100.0,
                "return_21d": 0.08,
                "vol_20": 0.028,
                "dd_252": -0.05,
            },
            "QLD": {
                "symbol": "QLD",
                "close": 74.0,
                "ma50": 75.0,
                "ma100": 72.0,
                "ma200": 68.0,
            },
        },
        prev_state=None,
        regime_source="QQQ",
        ma_fast=100,
        ma_slow=200,
        mom_lb=21,
        mom_thr=0.0,
        risk_on_alloc={"TQQQ": 0.8, "QQQ": 0.2},
        risk_on_alt_alloc={"QLD": 1.0},
        neutral_alloc={"QLD": 1.0},
        recovery_alloc={"QQQ": 1.0},
        risk_off_alloc={"GLD": 1.0},
        crash_alloc={"GLD": 1.0},
        vol_cap=0.05,
        vol_low=0.035,
        vol_mid=0.04,
        mom_strong=0.06,
        crash_vol=0.06,
        crash_dd=-0.2,
        hysteresis=0.0,
        recovery_slow_buffer=0.03,
        recovery_min_mom=0.015,
        recovery_max_vol=0.045,
        recovery_dd_floor=-0.12,
        risk_on_filter_asset="QLD",
        risk_on_filter_ma=50,
        risk_on_filter_safe_alloc={"QQQ": 1.0},
    )

    assert out["_regime_state"] == "neutral"
    assert out["_regime_reason"] == "risk_filter_safe"
    assert out["positions"] == [{"symbol": "QQQ", "weight_pct": 100.0}]


def test_apply_regime_exposure_respects_risk_on_alt_state():
    weights, cash = bt._apply_regime_exposure(
        weights_pct={"QLD": 100.0},
        market_ctx={"regime": "risk_on", "regime_state": "risk_on_alt", "vix_close": 18.0},
        on_exposure_pct=100.0,
        risk_on_alt_exposure_pct=82.0,
        neutral_exposure_pct=90.0,
        recovery_exposure_pct=95.0,
        risk_off_exposure_pct=80.0,
        crash_exposure_pct=60.0,
        risk_off_vix_threshold=30.0,
        risk_off_vix_hard_exposure_pct=50.0,
        risk_off_vix_extreme=34.0,
        risk_off_vix_extreme_exposure_pct=30.0,
    )

    assert weights == {"QLD": 82.0}
    assert cash == 18.0


def test_apply_regime_exposure_respects_crash_and_vix_caps():
    weights, cash = bt._apply_regime_exposure(
        weights_pct={"GLD": 100.0},
        market_ctx={"regime": "risk_off", "regime_state": "crash", "vix_close": 36.0},
        on_exposure_pct=100.0,
        risk_on_alt_exposure_pct=90.0,
        neutral_exposure_pct=90.0,
        recovery_exposure_pct=95.0,
        risk_off_exposure_pct=80.0,
        crash_exposure_pct=55.0,
        risk_off_vix_threshold=30.0,
        risk_off_vix_hard_exposure_pct=50.0,
        risk_off_vix_extreme=34.0,
        risk_off_vix_extreme_exposure_pct=30.0,
    )

    assert weights == {"GLD": 30.0}
    assert cash == 70.0
