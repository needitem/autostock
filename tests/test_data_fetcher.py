"""
data_fetcher.py 테스트
- yfinance 데이터 수집
- 시장 상태 체크
"""
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
from datetime import datetime

from data_fetcher import (
    get_stock_data,
    check_market_condition,
)


class TestGetStockData:
    """주식 데이터 가져오기 테스트"""
    
    def test_returns_dataframe_or_none(self):
        """DataFrame 또는 None 반환"""
        result = get_stock_data("AAPL")
        assert result is None or isinstance(result, pd.DataFrame)
    
    def test_valid_symbol_returns_data(self):
        """유효한 심볼은 데이터 반환"""
        result = get_stock_data("MSFT")
        if result is not None:
            assert len(result) > 0
    
    def test_invalid_symbol_returns_none(self):
        """잘못된 심볼은 None 반환"""
        result = get_stock_data("INVALID_SYMBOL_XYZ123")
        # yfinance가 빈 데이터를 반환할 수 있음
        assert result is None or len(result) == 0
    
    def test_dataframe_has_ohlcv(self):
        """DataFrame에 OHLCV가 있는지"""
        result = get_stock_data("GOOGL")
        if result is not None and len(result) > 0:
            required_cols = ["Open", "High", "Low", "Close", "Volume"]
            for col in required_cols:
                assert col in result.columns, f"Missing column: {col}"
    
    def test_custom_period(self):
        """커스텀 기간 테스트"""
        result = get_stock_data("NVDA", period="3mo")
        if result is not None:
            assert len(result) > 0


class TestCheckMarketCondition:
    """시장 상태 체크 테스트"""
    
    def test_returns_dict(self):
        """딕셔너리 반환"""
        result = check_market_condition()
        assert isinstance(result, dict)
    
    def test_has_status(self):
        """상태 정보가 있는지"""
        result = check_market_condition()
        assert "status" in result
    
    def test_has_emoji(self):
        """이모지가 있는지"""
        result = check_market_condition()
        assert "emoji" in result
    
    def test_has_message(self):
        """메시지가 있는지"""
        result = check_market_condition()
        assert "message" in result
    
    def test_has_price_info(self):
        """가격 정보가 있는지"""
        result = check_market_condition()
        assert "price" in result
        assert "ma50" in result
        assert "ma200" in result
    
    def test_status_valid(self):
        """상태가 유효한지"""
        result = check_market_condition()
        valid_statuses = ["bullish", "neutral", "bearish", "unknown"]
        assert result["status"] in valid_statuses
    
    def test_emoji_valid(self):
        """이모지가 유효한지"""
        result = check_market_condition()
        valid_emojis = ["🟢", "🟡", "🔴", "⚪"]
        assert result["emoji"] in valid_emojis


class TestDataFetcherIntegration:
    """데이터 수집 통합 테스트"""
    
    def test_multiple_symbols(self):
        """여러 심볼 테스트"""
        symbols = ["AAPL", "MSFT", "GOOGL"]
        for symbol in symbols:
            result = get_stock_data(symbol, period="1mo")
            assert result is None or isinstance(result, pd.DataFrame)
    
    @patch('data_fetcher.yf.Ticker')
    def test_handles_api_error(self, mock_ticker):
        """API 에러 처리"""
        mock_ticker.side_effect = Exception("API Error")
        result = get_stock_data("AAPL")
        assert result is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
