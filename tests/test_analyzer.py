"""
analyzer.py 테스트
- 종목 분석
- 전체 스캔
- 추천 종목
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
from datetime import datetime


class TestAnalyzeStockAllStrategies:
    """개별 종목 전략 분석 테스트"""
    
    @patch('analyzer.get_stock_data')
    @patch('analyzer.add_all_indicators')
    def test_returns_list(self, mock_indicators, mock_data):
        """리스트 반환"""
        from analyzer import analyze_stock_all_strategies
        
        # 모의 데이터 설정
        mock_df = create_mock_df()
        mock_data.return_value = mock_df
        mock_indicators.return_value = mock_df
        
        result = analyze_stock_all_strategies("AAPL")
        assert isinstance(result, list)
    
    @patch('analyzer.get_stock_data')
    def test_returns_empty_for_no_data(self, mock_data):
        """데이터 없으면 빈 리스트"""
        from analyzer import analyze_stock_all_strategies
        
        mock_data.return_value = None
        result = analyze_stock_all_strategies("INVALID")
        assert result == []


class TestAnalyzeSingleStock:
    """단일 종목 상세 분석 테스트"""
    
    @patch('analyzer.get_stock_data')
    @patch('analyzer.add_all_indicators')
    def test_returns_dict_or_none(self, mock_indicators, mock_data):
        """딕셔너리 또는 None 반환"""
        from analyzer import analyze_single_stock
        
        mock_df = create_mock_df()
        mock_data.return_value = mock_df
        mock_indicators.return_value = mock_df
        
        result = analyze_single_stock("AAPL")
        assert result is None or isinstance(result, dict)
    
    @patch('analyzer.get_stock_data')
    def test_returns_none_for_no_data(self, mock_data):
        """데이터 없으면 None"""
        from analyzer import analyze_single_stock
        
        mock_data.return_value = None
        result = analyze_single_stock("INVALID")
        assert result is None


class TestScanAllStocks:
    """전체 스캔 테스트"""
    
    @patch('analyzer.check_market_condition')
    @patch('analyzer.analyze_stock_all_strategies')
    @patch('analyzer.NASDAQ_100', ['AAPL', 'MSFT'])
    def test_returns_dict(self, mock_analyze, mock_market):
        """딕셔너리 반환"""
        from analyzer import scan_all_stocks
        
        mock_market.return_value = {
            "status": "bullish",
            "emoji": "🟢",
            "message": "상승 추세",
            "price": 400,
            "ma50": 380,
            "ma200": 350
        }
        mock_analyze.return_value = []
        
        with patch('market_data.get_fear_greed_index', return_value={"score": 50}):
            result = scan_all_stocks()
        
        assert isinstance(result, dict)
    
    @patch('analyzer.check_market_condition')
    @patch('analyzer.analyze_stock_all_strategies')
    @patch('analyzer.NASDAQ_100', ['AAPL'])
    def test_has_market_info(self, mock_analyze, mock_market):
        """시장 정보 포함"""
        from analyzer import scan_all_stocks
        
        mock_market.return_value = {"status": "bullish", "emoji": "🟢", "message": "상승", "price": 400, "ma50": 380, "ma200": 350}
        mock_analyze.return_value = []
        
        with patch('market_data.get_fear_greed_index', return_value={"score": 50}):
            result = scan_all_stocks()
        
        assert "market" in result
    
    @patch('analyzer.check_market_condition')
    @patch('analyzer.analyze_stock_all_strategies')
    @patch('analyzer.NASDAQ_100', ['AAPL'])
    def test_has_strategy_results(self, mock_analyze, mock_market):
        """전략 결과 포함"""
        from analyzer import scan_all_stocks
        
        mock_market.return_value = {"status": "bullish", "emoji": "🟢", "message": "상승", "price": 400, "ma50": 380, "ma200": 350}
        mock_analyze.return_value = []
        
        with patch('market_data.get_fear_greed_index', return_value={"score": 50}):
            result = scan_all_stocks()
        
        assert "strategy_results" in result
    
    @patch('analyzer.check_market_condition')
    @patch('analyzer.analyze_stock_all_strategies')
    @patch('analyzer.NASDAQ_100', ['AAPL'])
    def test_has_fear_greed(self, mock_analyze, mock_market):
        """공포탐욕 지수 포함"""
        from analyzer import scan_all_stocks
        
        mock_market.return_value = {"status": "bullish", "emoji": "🟢", "message": "상승", "price": 400, "ma50": 380, "ma200": 350}
        mock_analyze.return_value = []
        
        with patch('market_data.get_fear_greed_index', return_value={"score": 50, "rating": "중립"}):
            result = scan_all_stocks()
        
        assert "fear_greed" in result


class TestGetRecommendations:
    """추천 종목 테스트"""
    
    @patch('analyzer.analyze_single_stock')
    @patch('analyzer.NASDAQ_100', ['AAPL', 'MSFT'])
    def test_returns_dict(self, mock_analyze):
        """딕셔너리 반환"""
        from analyzer import get_recommendations
        
        mock_analyze.return_value = {
            "strategies_matched": ["🎯 보수적 모멘텀"],
            "risk_score": 20,
            "price": 150,
            "risk_grade": "🟢 양호",
            "rsi": 50,
            "ma50_gap": 5,
            "change_5d": 2
        }
        
        result = get_recommendations()
        assert isinstance(result, dict)
    
    @patch('analyzer.analyze_single_stock')
    @patch('analyzer.NASDAQ_100', ['AAPL'])
    def test_has_recommendations_list(self, mock_analyze):
        """추천 리스트 포함"""
        from analyzer import get_recommendations
        
        mock_analyze.return_value = None
        result = get_recommendations()
        
        assert "recommendations" in result
        assert isinstance(result["recommendations"], list)
    
    @patch('analyzer.analyze_single_stock')
    @patch('analyzer.NASDAQ_100', ['AAPL'])
    def test_has_total_analyzed(self, mock_analyze):
        """분석 총 개수 포함"""
        from analyzer import get_recommendations
        
        mock_analyze.return_value = None
        result = get_recommendations()
        
        assert "total_analyzed" in result


def create_mock_df(days=250):
    """테스트용 모의 데이터 생성"""
    dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
    prices = 100 + np.cumsum(np.random.randn(days) * 0.5)
    prices = np.maximum(prices, 10)
    
    df = pd.DataFrame({
        'Open': prices,
        'High': prices * 1.02,
        'Low': prices * 0.98,
        'Close': prices,
        'Volume': np.random.randint(1000000, 10000000, days),
        'MA5': prices,
        'MA20': prices,
        'MA50': prices * 0.95,
        'MA200': prices * 0.90,
        'RSI': np.random.uniform(30, 70, days),
        'MACD': np.random.randn(days),
        'MACD_Signal': np.random.randn(days),
        'BB_Upper': prices * 1.05,
        'BB_Lower': prices * 0.95,
        'BB_Mid': prices,
        'Volume_Avg': np.random.randint(1000000, 5000000, days),
        'High_52w': prices.max(),
        'Low_52w': prices.min(),
    }, index=dates)
    
    return df


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
