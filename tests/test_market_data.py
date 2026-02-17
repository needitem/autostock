"""Tests for src/market_data.py compatibility helpers."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from market_data import (
    get_comprehensive_stock_analysis,
    get_fear_greed_index,
    get_finviz_market_overview,
    get_finviz_sector_performance,
    get_finviz_stock_data,
    get_market_sentiment_summary,
    get_seeking_alpha_ratings,
    get_tipranks_rating,
)


class TestFearGreedIndex:
    @patch("market_data._core_fear_greed_index", return_value={"score": 55, "rating": "Neutral", "emoji": "🟢", "advice": "ok"})
    def test_returns_required_shape(self, _mock_fg):
        result = get_fear_greed_index()
        assert isinstance(result, dict)
        assert set(("score", "rating", "emoji", "advice")).issubset(result.keys())
        assert 0 <= result["score"] <= 100

    @patch("market_data._core_fear_greed_index", return_value={"score": 999, "emoji": "invalid"})
    def test_clamps_and_falls_back_emoji(self, _mock_fg):
        result = get_fear_greed_index()
        assert result["score"] == 100
        assert result["emoji"] in {"🔴", "🟠", "🟡", "🟢", "🔵", "⚪"}


class TestFinvizCompatibility:
    @patch("market_data._core_market_condition", return_value={"status": "bullish", "message": "uptrend", "price": 500, "ma50": 480, "ma200": 430})
    def test_market_overview(self, _mock_market):
        result = get_finviz_market_overview()
        assert result["status"] == "bullish"
        assert result["price"] == 500

    @patch("market_data.load_stock_categories", return_value={"tech": {"name": "Tech", "etf": "XLK"}})
    def test_sector_performance_shape(self, _mock_categories):
        result = get_finviz_sector_performance()
        assert isinstance(result, list)
        assert result
        assert result[0]["sector"] == "tech"
        assert result[0]["name"] == "Tech"

    @patch("market_data._core_finviz_data", return_value={"price": 190.0, "change": "+1.2%", "pe": 28.0, "rsi": 52, "target_price": 210.0})
    def test_stock_data_shape(self, _mock_finviz):
        result = get_finviz_stock_data("aapl")
        assert result["symbol"] == "AAPL"
        assert result["price"] == 190.0
        assert result["rsi"] == 52


class TestTipranksCompatibility:
    @patch("market_data._core_stock_info", return_value={"recommendation": "buy", "analyst_count": 18})
    def test_tipranks_buy_consensus(self, _mock_info):
        result = get_tipranks_rating("AAPL")
        assert result["symbol"] == "AAPL"
        assert result["consensus"] in {"Buy", "Hold", "Sell"}
        assert result["buy"] >= 1

    @patch("market_data._core_stock_info", return_value={"recommendation": "", "analyst_count": 0})
    def test_tipranks_empty_when_no_analysts(self, _mock_info):
        result = get_tipranks_rating("AAPL")
        assert result == {"symbol": "AAPL", "consensus": "N/A", "buy": 0, "hold": 0, "sell": 0}


class TestSeekingAlphaCompatibility:
    def test_seeking_alpha_placeholder(self):
        result = get_seeking_alpha_ratings("MSFT")
        assert result["symbol"] == "MSFT"
        assert "quant_rating" in result


class TestComprehensiveAnalysis:
    @patch("market_data.get_finviz_stock_data", return_value={"symbol": "TSLA", "price": 250})
    @patch("market_data.get_tipranks_rating", return_value={"symbol": "TSLA", "consensus": "Buy", "buy": 12, "hold": 3, "sell": 1})
    @patch("market_data.get_seeking_alpha_ratings", return_value={"symbol": "TSLA", "quant_rating": "N/A"})
    def test_comprehensive_has_sources(self, _mock_sa, _mock_tipranks, _mock_finviz):
        result = get_comprehensive_stock_analysis("tsla")
        assert result["symbol"] == "TSLA"
        assert "sources" in result
        assert set(("finviz", "tipranks", "seeking_alpha")).issubset(result["sources"].keys())


class TestMarketSentimentSummary:
    @patch("market_data.get_fear_greed_index", return_value={"score": 50, "rating": "Neutral", "emoji": "🟡", "advice": "ok"})
    @patch("market_data.get_finviz_sector_performance", return_value=[{"sector": "tech"}])
    @patch("market_data.get_finviz_market_overview", return_value={"status": "unknown"})
    def test_summary_shape(self, _mock_overview, _mock_sectors, _mock_fg):
        result = get_market_sentiment_summary()
        assert set(("fear_greed", "sectors", "market_overview")).issubset(result.keys())


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
