"""
Tests for `src/ai_analyzer.py` (legacy compatibility wrapper).

This project uses Codex CLI login (no manual API keys). The module provides a
thin wrapper around `ai.analyzer.AIAnalyzer` and returns structured dicts.
"""

import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ai_analyzer import analyze_news_with_ai, analyze_stock_with_ai, get_market_sentiment


class TestAnalyzeNewsWithAI:
    def test_returns_dict(self):
        news = [{"headline": "Apple reports record earnings"}]
        with patch("ai_analyzer._call_ai", return_value=None):
            result = analyze_news_with_ai("AAPL", news)
        assert isinstance(result, dict)

    def test_empty_news_returns_error(self):
        result = analyze_news_with_ai("AAPL", [])
        assert "error" in result

    def test_none_news_returns_error(self):
        result = analyze_news_with_ai("AAPL", None)
        assert "error" in result

    def test_no_login_returns_error(self):
        news = [{"headline": "Test news"}]
        with patch("ai_analyzer._call_ai", return_value=None):
            result = analyze_news_with_ai("AAPL", news)
        assert "error" in result


class TestAnalyzeStockWithAI:
    def test_returns_dict(self):
        stock_data = {
            "price": 150.0,
            "rsi": 55,
            "ma50_gap": 5.0,
            "relative_strength_21d": 1.0,
            "relative_strength_63d": 3.0,
        }
        with patch("ai_analyzer._call_ai", return_value=None):
            result = analyze_stock_with_ai("AAPL", stock_data)
        assert isinstance(result, dict)

    def test_invalid_stock_data_returns_error(self):
        result = analyze_stock_with_ai("AAPL", None)
        assert "error" in result

    def test_no_login_returns_error(self):
        with patch("ai_analyzer._call_ai", return_value=None):
            result = analyze_stock_with_ai("AAPL", {"price": 150.0})
        assert "error" in result

    def test_with_news_list(self):
        stock_data = {"price": 150.0, "rsi": 55}
        news = [{"headline": "Apple launches new product"}]
        with patch("ai_analyzer._call_ai", return_value=None):
            result = analyze_stock_with_ai("AAPL", stock_data, news)
        assert isinstance(result, dict)

    def test_with_market_data(self):
        stock_data = {"price": 150.0, "rsi": 55}
        market_data = {
            "market_condition": {"message": "Risk-on"},
            "sources": {
                "finviz": {"pe": "25", "target_price": "180"},
                "tipranks": {"consensus": "Buy", "buy": 20, "hold": 5, "sell": 2},
            },
        }
        with patch("ai_analyzer._call_ai", return_value=None):
            result = analyze_stock_with_ai("AAPL", stock_data, None, market_data)
        assert isinstance(result, dict)


class TestGetMarketSentiment:
    def test_returns_dict(self):
        news = [{"headline": "Markets rally on positive data"}]
        with patch("ai_analyzer._call_ai", return_value=None):
            result = get_market_sentiment(news)
        assert isinstance(result, dict)

    def test_empty_news_returns_error(self):
        result = get_market_sentiment([])
        assert "error" in result

    def test_with_fear_greed(self):
        news = [{"headline": "Market news"}]
        fear_greed = {"score": 45, "rating": "Neutral"}
        with patch("ai_analyzer._call_ai", return_value=None):
            result = get_market_sentiment(news, fear_greed)
        assert isinstance(result, dict)

    def test_no_login_returns_error(self):
        news = [{"headline": "Test"}]
        with patch("ai_analyzer._call_ai", return_value=None):
            result = get_market_sentiment(news)
        assert "error" in result


class TestAIAnalyzerIntegration:
    def test_analyze_news_calls_ai(self):
        news = [{"headline": "Test news"}]
        with patch("ai_analyzer._call_ai", return_value="AI analysis") as mock_ai:
            result = analyze_news_with_ai("AAPL", news)
        assert "analysis" in result
        mock_ai.assert_called_once()

    def test_analyze_stock_calls_ai(self):
        with patch("ai_analyzer._call_ai", return_value="AI analysis") as mock_ai:
            result = analyze_stock_with_ai("AAPL", {"price": 150.0})
        assert "analysis" in result
        mock_ai.assert_called_once()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))

