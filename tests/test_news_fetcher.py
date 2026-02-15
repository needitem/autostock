"""
news_fetcher.py tests

This module is intentionally lightweight and keyless.
It is kept mostly for compatibility with older tests and public interfaces.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from news_fetcher import (  # noqa: E402
    EVENT_DESCRIPTIONS,
    get_company_news,
    get_earnings_calendar,
    get_insider_transactions,
    get_ipo_calendar,
    get_market_news,
    get_price_target,
    get_recommendation_trends,
)


class TestCompanyNews:
    def test_returns_list(self):
        result = get_company_news("AAPL")
        assert isinstance(result, list)

    def test_news_item_has_headline(self):
        result = get_company_news("MSFT", days=7)
        if result:
            assert "headline" in result[0]

    def test_news_item_has_url(self):
        result = get_company_news("GOOGL", days=7)
        if result:
            assert "url" in result[0]


class TestEarningsCalendar:
    def test_returns_list(self):
        result = get_earnings_calendar()
        assert isinstance(result, list)

    def test_earnings_item_has_symbol(self):
        result = get_earnings_calendar()
        if result:
            assert "symbol" in result[0]

    def test_earnings_item_has_date(self):
        result = get_earnings_calendar()
        if result:
            assert "date" in result[0]


class TestInsiderTransactions:
    def test_returns_list(self):
        result = get_insider_transactions("AAPL")
        assert isinstance(result, list)

    def test_transaction_has_name(self):
        result = get_insider_transactions("TSLA")
        if result:
            assert "name" in result[0]


class TestRecommendationTrends:
    def test_returns_dict_or_none(self):
        result = get_recommendation_trends("AAPL")
        assert result is None or isinstance(result, dict)

    def test_has_buy_sell_hold(self):
        result = get_recommendation_trends("NVDA")
        if result:
            assert "buy" in result
            assert "hold" in result
            assert "sell" in result


class TestPriceTarget:
    def test_returns_dict_or_none(self):
        result = get_price_target("AAPL")
        assert result is None or isinstance(result, dict)

    def test_has_target_prices(self):
        result = get_price_target("META")
        if result:
            assert "target_high" in result
            assert "target_low" in result
            assert "target_mean" in result


class TestIPOCalendar:
    def test_returns_list(self):
        result = get_ipo_calendar()
        assert isinstance(result, list)


class TestMarketNews:
    def test_returns_list(self):
        result = get_market_news()
        assert isinstance(result, list)

    def test_news_has_headline(self):
        result = get_market_news()
        if result:
            assert "headline" in result[0]


class TestEventDescriptions:
    def test_has_major_events(self):
        major_events = ["FOMC", "CPI", "NFP", "earnings"]
        for event in major_events:
            assert event in EVENT_DESCRIPTIONS

    def test_event_has_description(self):
        for _event, info in EVENT_DESCRIPTIONS.items():
            assert "name" in info
            assert "impact" in info
            assert "desc" in info


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

