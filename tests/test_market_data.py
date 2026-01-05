"""
market_data.py 테스트
- CNN Fear & Greed Index
- Finviz 데이터
- TipRanks 데이터
- Seeking Alpha 데이터
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest
from market_data import (
    get_fear_greed_index,
    get_finviz_market_overview,
    get_finviz_sector_performance,
    get_finviz_stock_data,
    get_tipranks_rating,
    get_seeking_alpha_ratings,
    get_comprehensive_stock_analysis,
    get_market_sentiment_summary,
)


class TestFearGreedIndex:
    """CNN 공포탐욕 지수 테스트"""
    
    def test_get_fear_greed_index_returns_dict(self):
        """공포탐욕 지수가 딕셔너리를 반환하는지"""
        result = get_fear_greed_index()
        assert isinstance(result, dict)
    
    def test_fear_greed_has_required_keys(self):
        """필수 키가 있는지"""
        result = get_fear_greed_index()
        required_keys = ["score", "rating", "emoji", "advice"]
        for key in required_keys:
            assert key in result, f"Missing key: {key}"
    
    def test_fear_greed_score_range(self):
        """점수가 0-100 범위인지"""
        result = get_fear_greed_index()
        score = result.get("score", 0)
        assert 0 <= score <= 100, f"Score out of range: {score}"
    
    def test_fear_greed_emoji_valid(self):
        """이모지가 유효한지"""
        result = get_fear_greed_index()
        valid_emojis = ["🔴", "🟠", "🟡", "🟢", "🔵", "⚪"]
        assert result.get("emoji") in valid_emojis


class TestFinviz:
    """Finviz 데이터 테스트"""
    
    def test_market_overview_returns_dict(self):
        """시장 개요가 딕셔너리를 반환하는지"""
        result = get_finviz_market_overview()
        assert isinstance(result, dict)
    
    def test_sector_performance_returns_list(self):
        """섹터 성과가 리스트를 반환하는지"""
        result = get_finviz_sector_performance()
        assert isinstance(result, list)
    
    def test_stock_data_returns_dict(self):
        """종목 데이터가 딕셔너리를 반환하는지"""
        result = get_finviz_stock_data("AAPL")
        assert isinstance(result, dict)
    
    def test_stock_data_has_symbol(self):
        """종목 데이터에 심볼이 있는지"""
        result = get_finviz_stock_data("MSFT")
        if result:  # 데이터가 있을 때만
            assert result.get("symbol") == "MSFT"
    
    def test_stock_data_has_price_info(self):
        """종목 데이터에 가격 정보가 있는지"""
        result = get_finviz_stock_data("GOOGL")
        if result:
            price_keys = ["price", "change", "pe", "rsi"]
            for key in price_keys:
                assert key in result, f"Missing key: {key}"


class TestTipRanks:
    """TipRanks 데이터 테스트"""
    
    def test_tipranks_returns_dict(self):
        """TipRanks가 딕셔너리를 반환하는지"""
        result = get_tipranks_rating("AAPL")
        assert isinstance(result, dict)
    
    def test_tipranks_has_consensus(self):
        """컨센서스 정보가 있는지"""
        result = get_tipranks_rating("NVDA")
        if result:
            assert "consensus" in result or "buy" in result


class TestSeekingAlpha:
    """Seeking Alpha 데이터 테스트"""
    
    def test_seeking_alpha_returns_dict(self):
        """Seeking Alpha가 딕셔너리를 반환하는지"""
        result = get_seeking_alpha_ratings("AAPL")
        assert isinstance(result, dict)


class TestComprehensiveAnalysis:
    """종합 분석 테스트"""
    
    def test_comprehensive_returns_dict(self):
        """종합 분석이 딕셔너리를 반환하는지"""
        result = get_comprehensive_stock_analysis("AAPL")
        assert isinstance(result, dict)
    
    def test_comprehensive_has_symbol(self):
        """종합 분석에 심볼이 있는지"""
        result = get_comprehensive_stock_analysis("TSLA")
        assert result.get("symbol") == "TSLA"
    
    def test_comprehensive_has_sources(self):
        """종합 분석에 sources가 있는지"""
        result = get_comprehensive_stock_analysis("META")
        assert "sources" in result
        assert isinstance(result["sources"], dict)


class TestMarketSentiment:
    """시장 심리 종합 테스트"""
    
    def test_market_sentiment_returns_dict(self):
        """시장 심리가 딕셔너리를 반환하는지"""
        result = get_market_sentiment_summary()
        assert isinstance(result, dict)
    
    def test_market_sentiment_has_fear_greed(self):
        """시장 심리에 공포탐욕이 있는지"""
        result = get_market_sentiment_summary()
        assert "fear_greed" in result
    
    def test_market_sentiment_has_sectors(self):
        """시장 심리에 섹터가 있는지"""
        result = get_market_sentiment_summary()
        assert "sectors" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
