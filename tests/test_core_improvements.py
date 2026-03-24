from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import numpy as np
import pandas as pd

from core.indicators import calculate_indicators
from core.scoring import (
    calculate_annual_edge_score,
    calculate_financial_score,
    calculate_risk_score,
    calculate_score,
)
from core.signals import _apply_relative_strength, _build_trade_plan, _fundamental_conviction_profile, check_entry_signal


def test_financial_coverage_excludes_missing_data_from_weighting():
    base = {
        "symbol": "TEST",
        "rsi": 55,
        "bb_position": 45,
        "position_52w": 60,
        "ma50_gap": 3,
        "change_5d": 1,
    }
    score_missing = calculate_score(base)
    score_full = calculate_score(
        {
            **base,
            "roe": 25,
            "profit_margin": 20,
            "revenue_growth": 12,
            "earnings_growth": 15,
            "current_ratio": 1.8,
            "free_cash_flow": 100,
        }
    )

    assert score_missing["total_score"] >= 0
    assert score_full["total_score"] >= score_missing["total_score"] - 15


def test_calculate_financial_score_has_coverage():
    result = calculate_financial_score({"roe": 22, "profit_margin": 12})
    assert "coverage" in result
    assert 0 <= result["coverage"] <= 1


def test_financial_score_reflects_forward_expectation_quality():
    strong = calculate_financial_score(
        {
            "target_upside_pct": 16,
            "recommendation_mean": 1.9,
            "analyst_count": 18,
            "forward_eps_growth_pct": 24,
        }
    )
    weak = calculate_financial_score(
        {
            "target_upside_pct": -15,
            "recommendation_mean": 3.6,
            "analyst_count": 12,
            "forward_eps_growth_pct": -28,
        }
    )
    assert strong["score"] > weak["score"]
    assert "expectation" in strong["details"]


def test_risk_score_penalizes_negative_forward_expectation():
    low_risk = calculate_risk_score(
        {
            "rsi": 54,
            "bb_position": 55,
            "position_52w": 65,
            "ma50_gap": 3,
            "change_5d": 1,
            "target_upside_pct": 12,
            "recommendation_mean": 2.0,
            "analyst_count": 12,
            "forward_eps_growth_pct": 18,
        }
    )
    high_risk = calculate_risk_score(
        {
            "rsi": 54,
            "bb_position": 55,
            "position_52w": 65,
            "ma50_gap": 3,
            "change_5d": 1,
            "target_upside_pct": -12,
            "recommendation_mean": 3.5,
            "analyst_count": 12,
            "forward_eps_growth_pct": -26,
        }
    )
    assert high_risk["score"] > low_risk["score"]


def test_fundamental_conviction_can_hard_block():
    profile = _fundamental_conviction_profile(
        {
            "target_upside_pct": -18,
            "recommendation_mean": 3.7,
            "analyst_count": 15,
            "forward_eps_growth_pct": -35,
            "revenue_growth": -0.1,
            "earnings_growth": -0.2,
        }
    )
    assert profile["hard_block"] is True
    assert profile["score"] < 50


@patch("core.signals.calculate_indicators")
@patch("core.signals.get_stock_data")
def test_entry_signal_uses_ma5_gap(mock_data, mock_indicators):
    mock_data.return_value = object()
    mock_indicators.return_value = {
        "price": 100,
        "rsi": 30,
        "bb_position": 15,
        "ma5_gap": -4,
        "ma50_gap": 2,
        "down_days": 3,
    }

    result = check_entry_signal("AAPL")
    assert result["conditions"]["below_ma5"] is True
    assert result["is_signal"] is True


def test_score_contains_confidence_and_label():
    result = calculate_score(
        {
            "symbol": "AAPL",
            "rsi": 52,
            "bb_position": 48,
            "ma50_gap": 4,
            "position_52w": 66,
            "change_5d": 2,
            "adx": 28,
            "volume_ratio": 1.2,
            "roe": 20,
            "profit_margin": 18,
            "revenue_growth": 10,
            "earnings_growth": 12,
            "current_ratio": 1.6,
            "free_cash_flow": 100,
        }
    )
    assert "confidence" in result
    assert 0 <= result["confidence"]["score"] <= 100
    assert result["confidence"]["label"] in {"높음", "보통", "낮음"}
    assert "annual_edge" in result
    assert 0 <= result["annual_edge"]["score"] <= 100


def test_annual_edge_prefers_reversion_over_extension():
    reversion_candidate = calculate_annual_edge_score(
        {
            "return_63d": -18,
            "relative_strength_63d": -12,
            "rsi": 43,
            "ma50_gap": -4,
            "position_52w": 40,
            "atr_pct": 4.5,
            "change_5d": -2,
        }
    )
    overextended_candidate = calculate_annual_edge_score(
        {
            "return_63d": 28,
            "relative_strength_63d": 14,
            "rsi": 74,
            "ma50_gap": 16,
            "position_52w": 96,
            "atr_pct": 11,
            "change_5d": 9,
        }
    )
    assert reversion_candidate["score"] > overextended_candidate["score"]
    assert overextended_candidate["stance"] in {"neutral", "negative"}


@patch("core.stock_data._get_ticker_info")
def test_stock_info_days_to_earnings_uses_future_only(mock_ticker):
    from core.stock_data import _days_until_ts, get_stock_info

    now = datetime.now(timezone.utc)
    future_ts = int((now + timedelta(days=5)).timestamp())
    past_ts = int((now - timedelta(days=2)).timestamp())

    mock_ticker.return_value = {
        "shortName": "Test",
        "earningsTimestamp": past_ts,
        "earningsTimestampStart": future_ts,
    }
    info = get_stock_info("TEST")
    assert info["days_to_earnings"] == _days_until_ts(future_ts)

    mock_ticker.return_value = {
        "shortName": "Test",
        "earningsTimestamp": past_ts,
        "earningsTimestampStart": past_ts,
        "earningsTimestampEnd": past_ts,
    }
    info2 = get_stock_info("TEST2")
    assert info2["days_to_earnings"] is None


def test_apply_relative_strength_adds_benchmark_relative_fields():
    out = _apply_relative_strength(
        {"return_21d": 4.4, "return_63d": 11.2},
        {"benchmark_return_21d": 1.1, "benchmark_return_63d": 7.2},
    )
    assert out["relative_strength_21d"] == 3.3
    assert out["relative_strength_63d"] == 4.0


def test_score_rewards_relative_strength_component():
    base = {
        "symbol": "AAPL",
        "rsi": 54,
        "bb_position": 52,
        "ma50_gap": 4,
        "ma200_gap": 6,
        "position_52w": 70,
        "change_5d": 2,
        "adx": 26,
        "volume_ratio": 1.1,
        "atr_pct": 3.2,
        "roe": 20,
        "profit_margin": 18,
        "revenue_growth": 10,
        "earnings_growth": 12,
        "current_ratio": 1.7,
        "free_cash_flow": 100,
    }
    weak = calculate_score({**base, "relative_strength_21d": -4, "relative_strength_63d": -10, "return_63d": -2})
    strong = calculate_score({**base, "relative_strength_21d": 6, "relative_strength_63d": 14, "return_63d": 18})

    assert strong["factor"]["score"] > weak["factor"]["score"]
    assert strong["factor"]["details"]["relative_strength"] > weak["factor"]["details"]["relative_strength"]


@patch("core.signals.scan_stocks")
def test_buy_plan_applies_event_sector_and_rs_filters(mock_scan):
    from trading.portfolio import Portfolio

    signals = [
        {"symbol": "AAA", "price": 100, "strength": "강함", "rsi": 31},
        {"symbol": "BBB", "price": 110, "strength": "보통", "rsi": 35},
        {"symbol": "CCC", "price": 90, "strength": "강함", "rsi": 29},
        {"symbol": "DDD", "price": 80, "strength": "보통", "rsi": 34},
        {"symbol": "EEE", "price": 70, "strength": "강함", "rsi": 33},
    ]
    mock_scan.return_value = {
        "results": [
            {
                "symbol": "AAA",
                "price": 100,
                "sector": "Technology",
                "liquidity_score": 70,
                "relative_strength_63d": 5,
                "trade_plan": {
                    "tradeable": True,
                    "execution": {"position_pct": 6},
                    "risk_reward": {"rr2": 1.8},
                    "positioning": {"stage": "right_knee"},
                    "event_risk": {"level": "distant"},
                },
                "investability_score": 80,
            },
            {
                "symbol": "BBB",
                "price": 110,
                "sector": "Technology",
                "liquidity_score": 68,
                "relative_strength_63d": 4,
                "trade_plan": {
                    "tradeable": True,
                    "execution": {"position_pct": 5},
                    "risk_reward": {"rr2": 1.7},
                    "positioning": {"stage": "mid_trend"},
                    "event_risk": {"level": "distant"},
                },
                "investability_score": 78,
            },
            {
                "symbol": "CCC",
                "price": 90,
                "sector": "Healthcare",
                "liquidity_score": 72,
                "relative_strength_63d": 6,
                "event_risk_level": "near",
                "trade_plan": {
                    "tradeable": True,
                    "execution": {"position_pct": 5},
                    "risk_reward": {"rr2": 1.6},
                    "positioning": {"stage": "right_knee"},
                    "event_risk": {"level": "near"},
                },
                "investability_score": 77,
            },
            {
                "symbol": "DDD",
                "price": 80,
                "sector": "Energy",
                "liquidity_score": 73,
                "relative_strength_63d": -7,
                "trade_plan": {
                    "tradeable": True,
                    "execution": {"position_pct": 6},
                    "risk_reward": {"rr2": 1.7},
                    "positioning": {"stage": "mid_trend"},
                    "event_risk": {"level": "distant"},
                },
                "investability_score": 76,
            },
            {
                "symbol": "EEE",
                "price": 70,
                "sector": "Healthcare",
                "liquidity_score": 75,
                "relative_strength_63d": 7,
                "trade_plan": {
                    "tradeable": True,
                    "execution": {"position_pct": 5},
                    "risk_reward": {"rr2": 1.5},
                    "positioning": {"stage": "mid_trend"},
                    "event_risk": {"level": "distant"},
                },
                "investability_score": 75,
            },
        ]
    }

    portfolio = Portfolio()
    with patch.object(Portfolio, "_build_sector_counts", return_value={"Technology": 1}):
        orders = portfolio._get_buy_plan(signals, available=1000, max_per_stock=300, holdings=[])

    symbols = [o["symbol"] for o in orders]
    assert "CCC" not in symbols
    assert "DDD" not in symbols
    assert sum(1 for s in symbols if s in {"AAA", "BBB"}) <= 1


def _build_indicator_inputs_for_trade_plan() -> dict:
    return {
        "price": 130.0,
        "atr": 2.2,
        "ma50_gap": 4.0,
        "rsi": 54.0,
        "position_52w": 72.0,
        "atr_pct": 2.0,
        "change_5d": 1.4,
        "relative_strength_21d": 4.0,
        "relative_strength_63d": 6.0,
        "support": [128.0, 126.0],
        "resistance": [136.0, 142.0],
        "ma50": 122.0,
        "ma150": 112.0,
        "ma200": 100.0,
        "ma200_30d_ago": 98.0,
        "high_52w": 138.0,
        "low_52w": 90.0,
    }


def test_calculate_indicators_adds_minervini_fields():
    days = 280
    idx = pd.date_range(end=datetime.now(), periods=days, freq="B")
    close = np.linspace(100, 180, days)
    frame = pd.DataFrame(
        {
            "Open": close * 0.997,
            "High": close * 1.01,
            "Low": close * 0.99,
            "Close": close,
            "Volume": np.full(days, 3_500_000),
        },
        index=idx,
    )

    out = calculate_indicators(frame)
    assert out is not None
    assert "ma150" in out
    assert "ma200_30d_ago" in out
    assert "ma200_trend_up_30d" in out
    assert out["ma50"] > out["ma150"] > out["ma200"]
    assert out["ma200"] >= out["ma200_30d_ago"]


def test_safe_mode_applies_trend_template_and_position_caps(monkeypatch):
    monkeypatch.setenv("AI_SAFE_MODE", "1")
    monkeypatch.setenv("AI_SAFE_REQUIRE_BULLISH", "1")
    monkeypatch.setenv("AI_SAFE_MAX_POSITION_PCT", "10")
    monkeypatch.setenv("AI_SAFE_MIN_LIQUIDITY_TIER", "medium")
    monkeypatch.setenv("AI_SAFE_EARNINGS_SKIP_DAYS", "7")
    monkeypatch.setenv("AI_SAFE_TREND_RS63_MIN", "0")

    plan = _build_trade_plan(
        _build_indicator_inputs_for_trade_plan(),
        {"risk": {"score": 44}, "confidence": {"score": 66}},
        "bullish",
        {"is_tradeable": True, "score": 82, "tier": "high", "avg_dollar_volume_m": 55.0},
        {"days_to_earnings": 14, "level": "distant", "penalty": 0.0},
        {"score": 60, "coverage": 0.8, "has_data": True, "hard_block": False, "reasons": []},
    )

    assert plan["gates"]["trend_template"] is True
    assert plan["tradeable"] is True
    assert plan["execution"]["max_position_pct"] <= 10.0
    assert plan["constraints"]["safe_mode"]["enabled"] is True


def test_safe_mode_sit_out_when_market_not_bullish(monkeypatch):
    monkeypatch.setenv("AI_SAFE_MODE", "1")
    monkeypatch.setenv("AI_SAFE_REQUIRE_BULLISH", "1")

    plan = _build_trade_plan(
        _build_indicator_inputs_for_trade_plan(),
        {"risk": {"score": 42}, "confidence": {"score": 64}},
        "neutral",
        {"is_tradeable": True, "score": 82, "tier": "high", "avg_dollar_volume_m": 55.0},
        {"days_to_earnings": 14, "level": "distant", "penalty": 0.0},
        {"score": 60, "coverage": 0.8, "has_data": True, "hard_block": False, "reasons": []},
    )

    assert plan["tradeable"] is False
    assert "market_regime" in plan["blockers"]


def test_safe_mode_blocks_near_earnings_and_thin_liquidity(monkeypatch):
    monkeypatch.setenv("AI_SAFE_MODE", "1")
    monkeypatch.setenv("AI_SAFE_REQUIRE_BULLISH", "0")
    monkeypatch.setenv("AI_SAFE_MIN_LIQUIDITY_TIER", "medium")
    monkeypatch.setenv("AI_SAFE_EARNINGS_SKIP_DAYS", "7")

    plan = _build_trade_plan(
        _build_indicator_inputs_for_trade_plan(),
        {"risk": {"score": 40}, "confidence": {"score": 68}},
        "bullish",
        {"is_tradeable": True, "score": 62, "tier": "thin", "avg_dollar_volume_m": 7.2},
        {"days_to_earnings": 3, "level": "near", "penalty": 8.0},
        {"score": 60, "coverage": 0.8, "has_data": True, "hard_block": False, "reasons": []},
    )

    assert plan["tradeable"] is False
    assert plan["gates"]["liquidity"] is False
    assert plan["gates"]["event"] is False
