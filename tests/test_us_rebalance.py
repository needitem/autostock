from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pandas as pd

from pipelines import us_rebalance as reb


def _fake_df(days: int = 260) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=days, freq="D")
    base = pd.Series(range(days), index=idx).astype(float) + 100.0
    frame = pd.DataFrame(
        {
            "Open": base,
            "High": base * 1.01,
            "Low": base * 0.99,
            "Close": base * 1.0,
            "Volume": 1_000_000,
        }
    )
    return frame


def test_build_orders_min_trade():
    prev = {"AAPL": 0.2, "MSFT": 0.1, "__CASH__": 0.7}
    target_pct = {"AAPL": 30.0, "NVDA": 20.0}
    orders = reb._build_orders(prev, target_pct, min_trade_pct=1.0)
    by_symbol = {o["symbol"]: o for o in orders}
    assert by_symbol["AAPL"]["action"] == "BUY"
    assert by_symbol["AAPL"]["delta_pct"] == 10.0
    assert by_symbol["MSFT"]["action"] == "SELL"
    assert by_symbol["NVDA"]["action"] == "BUY"


def test_portfolio_from_ai_returns_cash_only_when_no_allowed_weights():
    obj = {"cash_pct": 100.0, "positions": [{"symbol": "ZZZZ", "weight_pct": 50.0}]}
    weights, cash = reb._portfolio_from_ai(
        obj,
        allowed={"AAPL"},
        top_k=8,
        max_weight_pct=25.0,
        exposure_target_pct=65.0,
    )
    assert weights == {}
    assert cash == 100.0


def test_cap_desired_exposure_by_constraints_clamps_when_infeasible():
    eff, audit = reb._cap_desired_exposure_by_constraints(
        desired_exposure_pct=65.02,
        max_weight_pct=25.0,
        max_positions=2,
    )
    assert abs(eff - 50.0) < 1e-9
    assert audit["capped_by_constraints"] is True
    assert audit["feasible_max_exposure_pct"] == 50.0


def test_market_exposure_filter_levels():
    f_neutral = reb._market_exposure_filter({"price": 100.0, "ma50": 105.0, "ma200": 90.0})
    assert f_neutral["multiplier"] == 0.85

    f_risk = reb._market_exposure_filter({"price": 80.0, "ma50": 100.0, "ma200": 90.0})
    assert f_risk["multiplier"] == 0.7

    f_bull = reb._market_exposure_filter({"price": 110.0, "ma50": 100.0, "ma200": 90.0})
    assert f_bull["multiplier"] == 1.0


def test_apply_weight_multipliers_penalizes_overheat_entry_and_soft_volume():
    weights = {"AAPL": 20.0, "MSFT": 20.0}
    feat = {
        "AAPL": {"rsi": 74.0, "bb_position": 86.0, "entry_conviction": -0.3, "volume_ratio": 1.1},
        "MSFT": {"rsi": 58.0, "bb_position": 52.0, "entry_conviction": 0.6, "volume_ratio": 1.8},
    }
    out, audit = reb._apply_weight_multipliers(weights, feat, max_weight_pct=50.0)
    assert out["AAPL"] < out["MSFT"]
    assert audit["AAPL"]["multiplier"] < 0.2
    assert "overheat_dual" in audit["AAPL"]["flags"]
    assert "entry_negative" in audit["AAPL"]["flags"]


def test_apply_turnover_cap_skips_initial_build():
    prev = {"__CASH__": 1.0}
    target = {"AAPL": 40.0, "MSFT": 30.0}
    out, audit = reb._apply_turnover_cap(prev, target, turnover_target_pct=30.0, feature_by_symbol={})
    assert out == target
    assert audit["mode"] == "initial_build_no_constraint"
    assert audit["cap_applied"] is False


def test_apply_turnover_cap_limits_rebalance_turnover():
    prev = {"AAPL": 0.5, "MSFT": 0.5}
    target = {"NVDA": 60.0}
    feat = {
        "AAPL": {"selection_score": 20.0, "entry_conviction": -0.5},
        "MSFT": {"selection_score": 40.0, "entry_conviction": 0.0},
        "NVDA": {"selection_score": 80.0, "entry_conviction": 1.0},
    }
    out, audit = reb._apply_turnover_cap(prev, target, turnover_target_pct=30.0, feature_by_symbol=feat)
    assert audit["cap_applied"] is True
    assert audit["asset_scope"] == "equity_only_ex_cash"
    assert audit["after_pct"] <= 30.0
    assert reb._turnover_pct(prev, out) <= 30.0 + 1e-6


def test_cross_conviction_score_penalizes_dead_cross():
    crosses = [
        {"type": "MACD골든", "signal": "매수"},
        {"type": "MACD데드", "signal": "매도"},
        {"type": "골든크로스", "signal": "매수"},
    ]
    score = reb._cross_conviction_score(crosses)
    assert score > 0
    assert score < 1.5


def test_fill_to_target_exposure_boosts_clean_symbols():
    target = {"AAPL": 5.0, "MSFT": 5.0, "NVDA": 5.0}
    candidates = [
        {"symbol": "AAPL", "selection_score": 80.0, "warnings": [], "ma50_gap": 4.0, "ma200_gap": 8.0},
        {"symbol": "MSFT", "selection_score": 75.0, "warnings": ["overheat_dual"], "ma50_gap": 5.0, "ma200_gap": 9.0},
        {"symbol": "NVDA", "selection_score": 60.0, "warnings": [], "ma50_gap": 3.0, "ma200_gap": 7.0},
    ]
    feat = {row["symbol"]: row for row in candidates}
    out, audit = reb._fill_to_target_exposure(
        target_weights_pct=target,
        desired_exposure_pct=20.0,
        max_weight_pct=15.0,
        top_k=3,
        ordered_candidates=candidates,
        feature_by_symbol=feat,
    )
    assert sum(out.values()) >= 20.0 - 1e-3
    assert out["MSFT"] == 5.0
    assert "AAPL" in audit["boosted_symbols"] or "NVDA" in audit["boosted_symbols"]


def test_fill_to_target_exposure_respects_sector_cap():
    target = {"AAA": 20.0, "BBB": 20.0, "CCC": 5.0}
    candidates = [
        {"symbol": "AAA", "selection_score": 90.0, "warnings": [], "ma50_gap": 4.0, "ma200_gap": 8.0, "sector": "Tech"},
        {"symbol": "BBB", "selection_score": 85.0, "warnings": [], "ma50_gap": 4.0, "ma200_gap": 8.0, "sector": "Tech"},
        {"symbol": "CCC", "selection_score": 70.0, "warnings": [], "ma50_gap": 3.0, "ma200_gap": 6.0, "sector": "Health"},
    ]
    feat = {row["symbol"]: row for row in candidates}
    out, _ = reb._fill_to_target_exposure(
        target_weights_pct=target,
        desired_exposure_pct=65.0,
        max_weight_pct=30.0,
        top_k=3,
        ordered_candidates=candidates,
        feature_by_symbol=feat,
        sector_cap_pct=40.0,
    )
    tech_total = float(out.get("AAA", 0.0) + out.get("BBB", 0.0))
    assert tech_total <= 40.0 + 1e-6


def test_is_rebound_candidate_rule():
    cfg = {
        "enabled": True,
        "max_weight_pct": 4.0,
        "max_count": 2,
        "max_ma200_drawdown_pct": -15.0,
        "min_volume_ratio": 1.0,
    }
    ok = reb._is_rebound_candidate(
        rsi=30.0,
        bb_pos=18.0,
        entry_conviction=0.7,
        volume_ratio=1.2,
        ma200_gap=-7.0,
        rebound_cfg=cfg,
    )
    bad = reb._is_rebound_candidate(
        rsi=52.0,
        bb_pos=45.0,
        entry_conviction=0.1,
        volume_ratio=1.1,
        ma200_gap=-6.0,
        rebound_cfg=cfg,
    )
    assert ok is True
    assert bad is False


def test_apply_rebound_limits_caps_weight_and_count():
    weights = {"AAA": 8.0, "BBB": 6.0, "CCC": 10.0}
    feat = {
        "AAA": {"sleeve": "rebound", "selection_score": 90.0, "entry_conviction": 1.0},
        "BBB": {"sleeve": "rebound", "selection_score": 80.0, "entry_conviction": 0.9},
        "CCC": {"sleeve": "momentum", "selection_score": 95.0, "entry_conviction": 1.2},
    }
    out, audit = reb._apply_rebound_limits(weights, feat, rebound_max_weight_pct=4.0, rebound_max_count=1)
    assert "AAA" in out
    assert "BBB" not in out
    assert out["AAA"] <= 4.0 + 1e-9
    assert audit["enabled"] is True
    assert "AAA" in audit["capped_symbols"]


def test_build_orders_with_skips_records_below_min_trade():
    prev = {"AAPL": 0.0, "__CASH__": 1.0}
    target = {"AAPL": 0.5, "MSFT": 2.0}
    orders, skipped = reb._build_orders_with_skips(prev, target, min_trade_pct=1.0)
    syms = {o["symbol"] for o in orders}
    assert "MSFT" in syms
    assert any(s["symbol"] == "AAPL" and s["skip_reason"] == "below_min_trade" for s in skipped)


def test_reconcile_target_with_min_trade_refills_gap_and_keeps_orders_executable():
    prev = {"__CASH__": 1.0}
    target = {"AEP": 0.8, "MSFT": 10.0, "NVDA": 8.0}
    feat = {
        "AEP": {"selection_score": 20.0, "warnings": [], "ma50_gap": 2.0, "ma200_gap": 5.0},
        "MSFT": {"selection_score": 90.0, "warnings": [], "ma50_gap": 4.0, "ma200_gap": 8.0},
        "NVDA": {"selection_score": 80.0, "warnings": [], "ma50_gap": 5.0, "ma200_gap": 10.0},
    }
    out, audit = reb._reconcile_target_with_min_trade(
        prev_port=prev,
        target_weights_pct=target,
        min_trade_pct=1.0,
        desired_exposure_pct=18.8,
        max_weight_pct=30.0,
        feature_by_symbol=feat,
        allow_refill=True,
    )
    assert "AEP" not in out
    assert abs(sum(out.values()) - 18.8) < 1e-6
    assert audit["refill_used_pct"] > 0
    orders, skipped = reb._build_orders_with_skips(prev, out, min_trade_pct=1.0)
    assert len(orders) == 2
    assert skipped == []


def test_reconcile_target_with_min_trade_blocks_risky_refill_symbols():
    prev = {"__CASH__": 1.0}
    target = {"AEP": 3.0, "MSFT": 10.0}
    feat = {
        "AEP": {
            "selection_score": 40.0,
            "warnings": ["overheat_extreme", "entry_negative"],
            "ma50_gap": 2.0,
            "ma200_gap": 5.0,
        },
        "MSFT": {
            "selection_score": 90.0,
            "warnings": [],
            "ma50_gap": 4.0,
            "ma200_gap": 8.0,
        },
    }
    out, audit = reb._reconcile_target_with_min_trade(
        prev_port=prev,
        target_weights_pct=target,
        min_trade_pct=1.0,
        desired_exposure_pct=20.0,
        max_weight_pct=15.0,
        feature_by_symbol=feat,
        allow_refill=True,
    )
    assert out["AEP"] == 3.0
    assert out["MSFT"] > 10.0
    assert all(s != "AEP" for s in audit["refill_symbols"])
    assert any(b["symbol"] == "AEP" for b in audit["refill_blocked_symbols"])


def test_executed_portfolio_from_orders_matches_only_executed_orders():
    prev = {"AAPL": 0.02, "MSFT": 0.03, "__CASH__": 0.95}
    orders = [
        {"symbol": "MSFT", "target_weight_pct": 5.0},
        {"symbol": "NVDA", "target_weight_pct": 4.0},
    ]
    executed, cash, exposure = reb._executed_portfolio_from_orders(prev, orders)
    assert executed["MSFT"] == 5.0
    assert executed["NVDA"] == 4.0
    assert executed["AAPL"] == 2.0
    assert abs(exposure - 11.0) < 1e-9
    assert abs(cash - 89.0) < 1e-9


def test_run_us_rebalance_smoke(monkeypatch):
    base = Path("data") / "test_tmp" / f"us_rebalance_{uuid4().hex}"
    report_dir = base / "outputs" / "run_2026-02-24"
    report_dir.mkdir(parents=True)
    report_path = report_dir / "report.json"
    report_path.write_text(json.dumps({"module1_liquidity": {"risk_on_off": {"label": "risk_on"}}}))

    monkeypatch.setenv("AI_UNIVERSE", "custom")
    monkeypatch.setenv("AI_SYMBOLS", "AAPL,MSFT")
    monkeypatch.setenv("AI_REBALANCE_MAX_SYMBOLS", "2")
    monkeypatch.setenv("AI_PORTFOLIO_MAX_WEIGHT_PCT", "60")
    monkeypatch.setenv("AI_CURRENT_PORTFOLIO_JSON", str(base / "current_portfolio.json"))

    def fake_get_stock_data(symbol: str, period: str = "15mo"):
        return _fake_df()

    def fake_calculate_indicators(df):
        return {
            "price": 150.0,
            "return_63d": 12.0,
            "return_21d": 4.0,
            "rsi": 55.0,
            "adx": 20.0,
            "ma50_gap": 3.0,
            "ma200_gap": 5.0,
            "bb_position": 60.0,
            "atr_pct": 2.0,
            "volume_ratio": 1.2,
            "support": [140.0],
            "resistance": [160.0],
        }

    def fake_get_stock_info(symbol: str):
        return {"sector": "Tech", "price": 150.0}

    monkeypatch.setattr(reb, "get_stock_data", fake_get_stock_data)
    monkeypatch.setattr(reb, "calculate_indicators", fake_calculate_indicators)
    monkeypatch.setattr(reb, "get_stock_info", fake_get_stock_info)
    monkeypatch.setattr(
        reb,
        "get_market_condition",
        lambda: {"message": "neutral", "benchmark_return_21d": 0.0, "benchmark_return_63d": 0.0, "price": 100.0, "ma50": 105.0, "ma200": 90.0},
    )

    class DummyAnalyzer:
        has_api_access = True

        def _call(self, prompt: str, max_tokens: int = 2000):
            return json.dumps(
                {"cash_pct": 10, "positions": [{"symbol": "AAPL", "weight_pct": 50}, {"symbol": "MSFT", "weight_pct": 40}]}
            )

    monkeypatch.setattr(reb, "AIAnalyzer", lambda: DummyAnalyzer())

    result = reb.run_us_rebalance(report_dir=str(report_path))
    out_csv = Path(result["orders_csv"])
    assert out_csv.exists()
    # Minimal signal: CSV header + at least one line
    lines = out_csv.read_text(encoding="utf-8").strip().splitlines()
    assert lines and lines[0].startswith("symbol,action")
    assert "turnover_audit" in result["result"]
    assert result["result"].get("turnover_definition") in {"half_l1", "l1", "sum_abs"}


def test_turnover_pct_defaults_to_half_l1():
    prev = {"AAPL": 0.5, "MSFT": 0.5}
    target = {"AAPL": 20.0, "NVDA": 80.0}
    t_half = reb._turnover_pct(prev, target)
    t_l1 = reb._turnover_pct(prev, target, definition="l1")
    assert abs(t_l1 - (t_half * 2.0)) < 1e-9


def test_run_us_rebalance_fallback_sets_ai_error(monkeypatch):
    base = Path("data") / "test_tmp" / f"us_rebalance_{uuid4().hex}"
    report_dir = base / "outputs" / "run_2026-02-24"
    report_dir.mkdir(parents=True)
    report_path = report_dir / "report.json"
    report_path.write_text(json.dumps({"module1_liquidity": {"risk_on_off": {"label": "neutral"}}}))

    monkeypatch.setenv("AI_UNIVERSE", "custom")
    monkeypatch.setenv("AI_SYMBOLS", "AAPL,MSFT")
    monkeypatch.setenv("AI_REBALANCE_MAX_SYMBOLS", "2")
    monkeypatch.setenv("AI_CURRENT_PORTFOLIO_JSON", str(base / "current_portfolio.json"))

    def fake_get_stock_data(symbol: str, period: str = "15mo"):
        return _fake_df()

    def fake_calculate_indicators(df):
        return {
            "price": 150.0,
            "return_63d": 12.0,
            "return_21d": 4.0,
            "rsi": 55.0,
            "adx": 22.0,
            "ma50_gap": 3.0,
            "ma200_gap": 6.0,
            "bb_position": 60.0,
            "atr_pct": 2.0,
            "volume_ratio": 1.2,
            "support": [140.0],
            "resistance": [160.0],
        }

    monkeypatch.setattr(reb, "get_stock_data", fake_get_stock_data)
    monkeypatch.setattr(reb, "calculate_indicators", fake_calculate_indicators)
    monkeypatch.setattr(reb, "get_stock_info", lambda symbol: {"sector": "Tech", "price": 150.0})
    monkeypatch.setattr(
        reb,
        "get_market_condition",
        lambda: {"message": "neutral", "benchmark_return_21d": 0.0, "benchmark_return_63d": 0.0, "price": 100.0, "ma50": 100.0, "ma200": 95.0},
    )

    class DummyAnalyzerBadJSON:
        has_api_access = True

        def _call(self, prompt: str, max_tokens: int = 2000):
            return "not a json"

    monkeypatch.setattr(reb, "AIAnalyzer", lambda: DummyAnalyzerBadJSON())
    result = reb.run_us_rebalance(report_dir=str(report_path))
    payload = result["result"]
    assert payload.get("ai_fallback") is True
    assert isinstance(payload.get("ai_error"), str)
    assert "fallback" in payload.get("ai_error", "").lower()
