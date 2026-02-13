import sys
import os
from datetime import datetime
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.scoring import calculate_score, calculate_financial_score
from core.news import get_company_news, get_market_news
from core.signals import check_entry_signal


def test_financial_coverage_excludes_missing_data_from_weighting():
    base = {
        "symbol": "TEST",
        "rsi": 55,
        "bb_position": 45,
        "position_52w": 60,
        "ma50_gap": 3,
        "change_5d": 1,
    }
    score_missing = calculate_score(base)
    score_full = calculate_score({
        **base,
        "roe": 25,
        "profit_margin": 20,
        "revenue_growth": 12,
        "earnings_growth": 15,
        "current_ratio": 1.8,
        "free_cash_flow": 100,
    })

    # 재무가 없다고 무조건 불리하지 않아야 함 (가중치 정규화)
    assert score_missing["total_score"] >= 0
    assert score_full["total_score"] >= score_missing["total_score"] - 15


def test_calculate_financial_score_has_coverage():
    result = calculate_financial_score({"roe": 22, "profit_margin": 12})
    assert "coverage" in result
    assert 0 <= result["coverage"] <= 1


@patch('core.news._request')
def test_company_news_sorted_by_latest(mock_request):
    now = int(datetime.now().timestamp())
    mock_request.return_value = [
        {"headline": "old", "summary": "s", "source": "x", "datetime": now - 3600 * 5, "url": "u1"},
        {"headline": "new", "summary": "s", "source": "x", "datetime": now - 60, "url": "u2"},
    ]

    result = get_company_news("AAPL", days=1)
    assert result[0]["headline"] == "new"
    assert "age_hours" in result[0]
    assert "url" in result[0]


@patch('core.news._request')
def test_market_news_sorted_by_latest(mock_request):
    now = int(datetime.now().timestamp())
    mock_request.return_value = [
        {"headline": "m1", "summary": "s", "source": "x", "datetime": now - 3600 * 4, "url": "u1"},
        {"headline": "m2", "summary": "s", "source": "x", "datetime": now - 120, "url": "u2"},
    ]

    result = get_market_news()
    assert result[0]["headline"] == "m2"
    assert "published_ts" in result[0]


@patch('core.signals.calculate_indicators')
@patch('core.signals.get_stock_data')
def test_entry_signal_uses_ma5_gap(mock_data, mock_indicators):
    mock_data.return_value = object()
    mock_indicators.return_value = {
        "price": 100,
        "rsi": 30,
        "bb_position": 15,
        "ma5_gap": -4,
        "ma50_gap": 2,
        "down_days": 3,
    }

    result = check_entry_signal("AAPL")
    assert result["conditions"]["below_ma5"] is True
    assert result["is_signal"] is True


def test_score_contains_confidence_and_label():
    result = calculate_score({
        "symbol": "AAPL",
        "rsi": 52,
        "bb_position": 48,
        "ma50_gap": 4,
        "position_52w": 66,
        "change_5d": 2,
        "adx": 28,
        "volume_ratio": 1.2,
        "roe": 20,
        "profit_margin": 18,
        "revenue_growth": 10,
        "earnings_growth": 12,
        "current_ratio": 1.6,
        "free_cash_flow": 100,
    })
    assert "confidence" in result
    assert 0 <= result["confidence"]["score"] <= 100
    assert result["confidence"]["label"] in {"높음", "보통", "낮음"}
