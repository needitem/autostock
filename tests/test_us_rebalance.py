from __future__ import annotations

import json
from pathlib import Path

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


def test_run_us_rebalance_smoke(tmp_path, monkeypatch):
    report_dir = tmp_path / "outputs" / "run_2026-02-24"
    report_dir.mkdir(parents=True)
    report_path = report_dir / "report.json"
    report_path.write_text(json.dumps({"module1_liquidity": {"risk_on_off": {"label": "risk_on"}}}))

    monkeypatch.setenv("AI_UNIVERSE", "custom")
    monkeypatch.setenv("AI_SYMBOLS", "AAPL,MSFT")
    monkeypatch.setenv("AI_REBALANCE_MAX_SYMBOLS", "2")
    monkeypatch.setenv("AI_PORTFOLIO_MAX_WEIGHT_PCT", "60")
    monkeypatch.setenv("AI_CURRENT_PORTFOLIO_JSON", str(tmp_path / "current_portfolio.json"))

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
