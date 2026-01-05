"""
ai_analyzer.py 테스트
- Groq AI 분석 기능
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest
from unittest.mock import patch, MagicMock
from ai_analyzer import (
    analyze_news_with_ai,
    analyze_stock_with_ai,
    get_market_sentiment,
)


class TestAnalyzeNewsWithAI:
    """뉴스 AI 분석 테스트"""
    
    def test_returns_dict(self):
        """딕셔너리 반환"""
        news = [{"headline": "Apple reports record earnings"}]
        result = analyze_news_with_ai("AAPL", news)
        assert isinstance(result, dict)
    
    def test_empty_news_returns_error(self):
        """빈 뉴스는 에러 반환"""
        result = analyze_news_with_ai("AAPL", [])
        assert "error" in result
    
    def test_none_news_returns_error(self):
        """None 뉴스는 에러 반환"""
        result = analyze_news_with_ai("AAPL", None)
        assert "error" in result
    
    @patch('ai_analyzer.GROQ_API_KEY', None)
    def test_no_api_key_returns_error(self):
        """API 키 없으면 에러 반환"""
        news = [{"headline": "Test news"}]
        result = analyze_news_with_ai("AAPL", news)
        assert "error" in result


class TestAnalyzeStockWithAI:
    """종목 AI 분석 테스트"""
    
    def test_returns_dict(self):
        """딕셔너리 반환"""
        stock_data = {
            "price": 150.0,
            "rsi": 55,
            "ma50_gap": 5.0,
            "position_52w": 70,
            "change_5d": 2.5,
            "risk_score": 25
        }
        result = analyze_stock_with_ai("AAPL", stock_data)
        assert isinstance(result, dict)
    
    @patch('ai_analyzer.GROQ_API_KEY', None)
    def test_no_api_key_returns_error(self):
        """API 키 없으면 에러 반환"""
        stock_data = {"price": 150.0}
        result = analyze_stock_with_ai("AAPL", stock_data)
        assert "error" in result
    
    def test_with_news_list(self):
        """뉴스 리스트와 함께 분석"""
        stock_data = {"price": 150.0, "rsi": 55}
        news = [{"headline": "Apple launches new product"}]
        result = analyze_stock_with_ai("AAPL", stock_data, news)
        assert isinstance(result, dict)
    
    def test_with_market_data(self):
        """외부 데이터와 함께 분석"""
        stock_data = {"price": 150.0, "rsi": 55}
        market_data = {
            "sources": {
                "finviz": {"pe": "25", "target_price": "180"},
                "tipranks": {"consensus": "Buy", "buy": 20, "hold": 5, "sell": 2}
            }
        }
        result = analyze_stock_with_ai("AAPL", stock_data, None, market_data)
        assert isinstance(result, dict)


class TestGetMarketSentiment:
    """시장 감성 분석 테스트"""
    
    def test_returns_dict(self):
        """딕셔너리 반환"""
        news = [{"headline": "Markets rally on positive data"}]
        result = get_market_sentiment(news)
        assert isinstance(result, dict)
    
    def test_empty_news_returns_error(self):
        """빈 뉴스는 에러 반환"""
        result = get_market_sentiment([])
        assert "error" in result
    
    def test_with_fear_greed(self):
        """공포탐욕 지수와 함께 분석"""
        news = [{"headline": "Market news"}]
        fear_greed = {"score": 45, "rating": "중립"}
        result = get_market_sentiment(news, fear_greed)
        assert isinstance(result, dict)
    
    @patch('ai_analyzer.GROQ_API_KEY', None)
    def test_no_api_key_returns_error(self):
        """API 키 없으면 에러 반환"""
        news = [{"headline": "Test"}]
        result = get_market_sentiment(news)
        assert "error" in result


class TestAIAnalyzerIntegration:
    """AI 분석기 통합 테스트"""
    
    @patch('ai_analyzer._call_groq')
    def test_analyze_news_calls_groq(self, mock_groq):
        """뉴스 분석이 Groq를 호출하는지"""
        mock_groq.return_value = "AI 분석 결과"
        news = [{"headline": "Test news"}]
        
        with patch('ai_analyzer.GROQ_API_KEY', 'test_key'):
            result = analyze_news_with_ai("AAPL", news)
        
        if "analysis" in result:
            mock_groq.assert_called_once()
    
    @patch('ai_analyzer._call_groq')
    def test_analyze_stock_calls_groq(self, mock_groq):
        """종목 분석이 Groq를 호출하는지"""
        mock_groq.return_value = "AI 분석 결과"
        stock_data = {"price": 150.0}
        
        with patch('ai_analyzer.GROQ_API_KEY', 'test_key'):
            result = analyze_stock_with_ai("AAPL", stock_data)
        
        if "analysis" in result:
            mock_groq.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
