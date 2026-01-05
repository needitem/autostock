"""
news_fetcher.py 테스트
- Finnhub API 뉴스/이벤트 수집
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest
from news_fetcher import (
    get_company_news,
    get_earnings_calendar,
    get_insider_transactions,
    get_recommendation_trends,
    get_price_target,
    get_ipo_calendar,
    get_market_news,
    EVENT_DESCRIPTIONS,
)


class TestCompanyNews:
    """종목별 뉴스 테스트"""
    
    def test_returns_list(self):
        """리스트 반환"""
        result = get_company_news("AAPL")
        assert isinstance(result, list)
    
    def test_news_item_has_headline(self):
        """뉴스 항목에 헤드라인이 있는지"""
        result = get_company_news("MSFT", days=7)
        if result:
            assert "headline" in result[0]
    
    def test_news_item_has_url(self):
        """뉴스 항목에 URL이 있는지"""
        result = get_company_news("GOOGL", days=7)
        if result:
            assert "url" in result[0]


class TestEarningsCalendar:
    """실적 발표 일정 테스트"""
    
    def test_returns_list(self):
        """리스트 반환"""
        result = get_earnings_calendar()
        assert isinstance(result, list)
    
    def test_earnings_item_has_symbol(self):
        """실적 항목에 심볼이 있는지"""
        result = get_earnings_calendar()
        if result:
            assert "symbol" in result[0]
    
    def test_earnings_item_has_date(self):
        """실적 항목에 날짜가 있는지"""
        result = get_earnings_calendar()
        if result:
            assert "date" in result[0]


class TestInsiderTransactions:
    """내부자 거래 테스트"""
    
    def test_returns_list(self):
        """리스트 반환"""
        result = get_insider_transactions("AAPL")
        assert isinstance(result, list)
    
    def test_transaction_has_name(self):
        """거래 항목에 이름이 있는지"""
        result = get_insider_transactions("TSLA")
        if result:
            assert "name" in result[0]


class TestRecommendationTrends:
    """애널리스트 추천 테스트"""
    
    def test_returns_dict_or_none(self):
        """딕셔너리 또는 None 반환"""
        result = get_recommendation_trends("AAPL")
        assert result is None or isinstance(result, dict)
    
    def test_has_buy_sell_hold(self):
        """매수/매도/보유 정보가 있는지"""
        result = get_recommendation_trends("NVDA")
        if result:
            assert "buy" in result
            assert "hold" in result
            assert "sell" in result


class TestPriceTarget:
    """목표 주가 테스트"""
    
    def test_returns_dict_or_none(self):
        """딕셔너리 또는 None 반환"""
        result = get_price_target("AAPL")
        assert result is None or isinstance(result, dict)
    
    def test_has_target_prices(self):
        """목표가 정보가 있는지"""
        result = get_price_target("META")
        if result:
            assert "target_high" in result
            assert "target_low" in result
            assert "target_mean" in result


class TestIPOCalendar:
    """IPO 일정 테스트"""
    
    def test_returns_list(self):
        """리스트 반환"""
        result = get_ipo_calendar()
        assert isinstance(result, list)


class TestMarketNews:
    """시장 뉴스 테스트"""
    
    def test_returns_list(self):
        """리스트 반환"""
        result = get_market_news()
        assert isinstance(result, list)
    
    def test_news_has_headline(self):
        """뉴스에 헤드라인이 있는지"""
        result = get_market_news()
        if result:
            assert "headline" in result[0]


class TestEventDescriptions:
    """이벤트 설명 테스트"""
    
    def test_has_major_events(self):
        """주요 이벤트가 있는지"""
        major_events = ["FOMC", "CPI", "NFP", "earnings"]
        for event in major_events:
            assert event in EVENT_DESCRIPTIONS
    
    def test_event_has_description(self):
        """이벤트에 설명이 있는지"""
        for event, info in EVENT_DESCRIPTIONS.items():
            assert "name" in info
            assert "impact" in info
            assert "desc" in info


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
