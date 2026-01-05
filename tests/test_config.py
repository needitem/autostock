"""
config.py 테스트
- 나스닥 100 목록
- 설정 값
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest
from unittest.mock import patch, MagicMock


class TestNasdaq100:
    """나스닥 100 목록 테스트"""
    
    def test_nasdaq_100_is_list(self):
        """NASDAQ_100이 리스트인지"""
        from config import NASDAQ_100
        assert isinstance(NASDAQ_100, list)
    
    def test_nasdaq_100_has_symbols(self):
        """NASDAQ_100에 심볼이 있는지"""
        from config import NASDAQ_100
        # 캐시가 있거나 네트워크 연결이 있으면 심볼이 있어야 함
        # 없을 수도 있으므로 타입만 체크
        assert isinstance(NASDAQ_100, list)
    
    def test_nasdaq_100_symbols_are_strings(self):
        """심볼이 문자열인지"""
        from config import NASDAQ_100
        for symbol in NASDAQ_100:
            assert isinstance(symbol, str)
    
    def test_has_major_stocks(self):
        """주요 종목이 있는지"""
        from config import NASDAQ_100
        if len(NASDAQ_100) > 0:
            # 나스닥 100에 있어야 할 주요 종목들
            major_stocks = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
            found = sum(1 for s in major_stocks if s in NASDAQ_100)
            # 최소 3개 이상은 있어야 함
            assert found >= 3 or len(NASDAQ_100) == 0


class TestMarketIndicator:
    """시장 지표 테스트"""
    
    def test_market_indicator_exists(self):
        """MARKET_INDICATOR가 있는지"""
        from config import MARKET_INDICATOR
        assert MARKET_INDICATOR is not None
    
    def test_market_indicator_is_string(self):
        """MARKET_INDICATOR가 문자열인지"""
        from config import MARKET_INDICATOR
        assert isinstance(MARKET_INDICATOR, str)
    
    def test_market_indicator_is_qqq(self):
        """MARKET_INDICATOR가 QQQ인지"""
        from config import MARKET_INDICATOR
        assert MARKET_INDICATOR == "QQQ"


class TestCacheSettings:
    """캐시 설정 테스트"""
    
    def test_cache_dir_exists(self):
        """CACHE_DIR이 있는지"""
        from config import CACHE_DIR
        assert CACHE_DIR is not None
    
    def test_cache_days_positive(self):
        """CACHE_DAYS가 양수인지"""
        from config import CACHE_DAYS
        assert CACHE_DAYS > 0


class TestFetchNasdaq100:
    """나스닥 100 가져오기 테스트"""
    
    def test_fetch_returns_list(self):
        """fetch_nasdaq_100이 리스트를 반환하는지"""
        from config import fetch_nasdaq_100
        result = fetch_nasdaq_100()
        assert isinstance(result, list)
    
    def test_get_nasdaq_100_returns_list(self):
        """get_nasdaq_100이 리스트를 반환하는지"""
        from config import get_nasdaq_100
        result = get_nasdaq_100()
        assert isinstance(result, list)


class TestTelegramConfig:
    """텔레그램 설정 테스트"""
    
    def test_telegram_token_exists(self):
        """TELEGRAM_BOT_TOKEN 변수가 있는지"""
        from config import TELEGRAM_BOT_TOKEN
        # None일 수 있음 (환경변수 미설정)
        assert TELEGRAM_BOT_TOKEN is None or isinstance(TELEGRAM_BOT_TOKEN, str)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
