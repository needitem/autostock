"""
pytest 설정 파일
"""
import sys
import os

# src 디렉토리를 path에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest


@pytest.fixture
def mock_stock_data():
    """모의 주식 데이터"""
    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    days = 250
    dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
    prices = 100 + np.cumsum(np.random.randn(days) * 0.5)
    prices = np.maximum(prices, 10)
    
    return pd.DataFrame({
        'Open': prices,
        'High': prices * 1.02,
        'Low': prices * 0.98,
        'Close': prices,
        'Volume': np.random.randint(1000000, 10000000, days),
    }, index=dates)


@pytest.fixture
def mock_news_list():
    """모의 뉴스 리스트"""
    return [
        {"headline": "Apple reports record Q4 earnings", "url": "https://example.com/1", "source": "Reuters", "datetime": "2025-01-05 10:00"},
        {"headline": "Tech stocks rally on positive outlook", "url": "https://example.com/2", "source": "Bloomberg", "datetime": "2025-01-05 09:00"},
        {"headline": "Fed signals potential rate cuts", "url": "https://example.com/3", "source": "CNBC", "datetime": "2025-01-04 15:00"},
    ]


@pytest.fixture
def mock_fear_greed():
    """모의 공포탐욕 지수"""
    return {
        "score": 55,
        "rating": "탐욕 😏",
        "emoji": "🟢",
        "advice": "탐욕 구간 - 추격 매수 주의",
        "timestamp": "2025-01-05 12:00"
    }


@pytest.fixture
def mock_stock_analysis():
    """모의 종목 분석 결과"""
    return {
        "symbol": "AAPL",
        "price": 185.50,
        "risk_score": 25,
        "risk_grade": "🟢 양호",
        "recommendation": "매수 고려 가능",
        "warnings": [],
        "rsi": 52.3,
        "bb_position": 65,
        "position_52w": 78,
        "ma50_gap": 5.2,
        "change_5d": 2.1,
        "strategies_matched": ["🎯 보수적 모멘텀"]
    }
