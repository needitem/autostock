from __future__ import annotations

import pandas as pd

from core import stock_data


def _sample_history() -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=5, freq="D")
    return pd.DataFrame(
        {
            "Open": [100, 101, 102, 103, 104],
            "High": [101, 102, 103, 104, 105],
            "Low": [99, 100, 101, 102, 103],
            "Close": [100, 101, 102, 103, 104],
            "Volume": [1_000_000] * 5,
        },
        index=idx,
    )


def test_get_stock_data_defaults_to_auto_adjust_true(monkeypatch):
    calls = []

    class DummyTicker:
        def history(self, **kwargs):
            calls.append(kwargs)
            return _sample_history()

    stock_data._get_stock_data_cached.cache_clear()
    monkeypatch.delenv("AI_YF_AUTO_ADJUST", raising=False)
    monkeypatch.setattr(stock_data.yf, "Ticker", lambda _symbol: DummyTicker())

    frame = stock_data.get_stock_data("AAPL", period="1mo")
    assert frame is not None
    assert calls
    assert calls[0]["auto_adjust"] is True


def test_get_stock_data_allows_auto_adjust_override(monkeypatch):
    calls = []

    class DummyTicker:
        def history(self, **kwargs):
            calls.append(kwargs)
            return _sample_history()

    stock_data._get_stock_data_cached.cache_clear()
    monkeypatch.setattr(stock_data.yf, "Ticker", lambda _symbol: DummyTicker())

    frame = stock_data.get_stock_data("AAPL", period="1mo", auto_adjust=False)
    assert frame is not None
    assert calls
    assert calls[0]["auto_adjust"] is False


def test_get_stock_data_cache_refreshes_when_bucket_changes(monkeypatch):
    call_count = {"n": 0}

    class DummyTicker:
        def history(self, **kwargs):
            call_count["n"] += 1
            return _sample_history()

    now = {"t": 60.0}

    stock_data._get_stock_data_cached.cache_clear()
    monkeypatch.setenv("AI_YF_CACHE_TTL_MINUTES", "1")
    monkeypatch.setattr(stock_data.yf, "Ticker", lambda _symbol: DummyTicker())
    monkeypatch.setattr(stock_data.time, "time", lambda: now["t"])

    stock_data.get_stock_data("AAPL", period="1mo")
    stock_data.get_stock_data("AAPL", period="1mo")
    assert call_count["n"] == 1

    now["t"] = 121.0
    stock_data.get_stock_data("AAPL", period="1mo")
    assert call_count["n"] == 2
