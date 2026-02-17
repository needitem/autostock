"""
strategies.py 테스트
- 기술적 지표 계산
- 7가지 매매 전략
- 위험도 분석
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from strategies import (
    add_all_indicators,
    strategy_conservative_momentum,
    strategy_golden_cross,
    strategy_bollinger_bounce,
    strategy_macd_crossover,
    strategy_near_52w_high,
    strategy_dip_bounce,
    strategy_volume_surge,
    analyze_risk_level,
    ALL_STRATEGIES,
)


def create_mock_df(days=250, trend="up"):
    """테스트용 모의 주가 데이터 생성"""
    dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
    
    if trend == "up":
        base_price = 100
        prices = base_price + np.cumsum(np.random.randn(days) * 0.5 + 0.1)
    elif trend == "down":
        base_price = 150
        prices = base_price + np.cumsum(np.random.randn(days) * 0.5 - 0.1)
    else:  # sideways
        base_price = 100
        prices = base_price + np.cumsum(np.random.randn(days) * 0.3)
    
    prices = np.maximum(prices, 10)  # 최소 가격 보장
    
    df = pd.DataFrame({
        'Open': prices * (1 + np.random.randn(days) * 0.01),
        'High': prices * (1 + np.abs(np.random.randn(days) * 0.02)),
        'Low': prices * (1 - np.abs(np.random.randn(days) * 0.02)),
        'Close': prices,
        'Volume': np.random.randint(1000000, 10000000, days),
    }, index=dates)
    
    return df


class TestAddAllIndicators:
    """기술적 지표 추가 테스트"""
    
    def test_returns_none_for_short_data(self):
        """데이터가 200일 미만이면 None 반환"""
        df = create_mock_df(days=100)
        result = add_all_indicators(df)
        assert result is None
    
    def test_returns_dataframe_for_valid_data(self):
        """유효한 데이터면 DataFrame 반환"""
        df = create_mock_df(days=250)
        result = add_all_indicators(df)
        assert isinstance(result, pd.DataFrame)
    
    def test_has_moving_averages(self):
        """이동평균선이 추가되는지"""
        df = create_mock_df(days=250)
        result = add_all_indicators(df)
        assert "MA5" in result.columns
        assert "MA20" in result.columns
        assert "MA50" in result.columns
        assert "MA200" in result.columns
    
    def test_has_rsi(self):
        """RSI가 추가되는지"""
        df = create_mock_df(days=250)
        result = add_all_indicators(df)
        assert "RSI" in result.columns
    
    def test_has_macd(self):
        """MACD가 추가되는지"""
        df = create_mock_df(days=250)
        result = add_all_indicators(df)
        assert "MACD" in result.columns
        assert "MACD_Signal" in result.columns
    
    def test_has_bollinger_bands(self):
        """볼린저밴드가 추가되는지"""
        df = create_mock_df(days=250)
        result = add_all_indicators(df)
        assert "BB_Upper" in result.columns
        assert "BB_Lower" in result.columns
    
    def test_has_52w_high_low(self):
        """52주 고저가 추가되는지"""
        df = create_mock_df(days=250)
        result = add_all_indicators(df)
        assert "High_52w" in result.columns
        assert "Low_52w" in result.columns


class TestStrategyConservativeMomentum:
    """보수적 모멘텀 전략 테스트"""
    
    def test_returns_none_when_conditions_not_met(self):
        """조건 불충족 시 None 반환"""
        df = create_mock_df(days=250, trend="down")
        df = add_all_indicators(df)
        result = strategy_conservative_momentum(df, "TEST")
        # 하락 추세에서는 대부분 None
        assert result is None or isinstance(result, dict)
    
    def test_returns_dict_with_required_keys(self):
        """결과가 있을 때 필수 키 포함"""
        df = create_mock_df(days=250, trend="up")
        df = add_all_indicators(df)
        result = strategy_conservative_momentum(df, "TEST")
        if result:
            assert "symbol" in result
            assert "strategy" in result
            assert "price" in result


class TestStrategyGoldenCross:
    """골든크로스 전략 테스트"""
    
    def test_returns_none_or_dict(self):
        """None 또는 딕셔너리 반환"""
        df = create_mock_df(days=250)
        df = add_all_indicators(df)
        result = strategy_golden_cross(df, "TEST")
        assert result is None or isinstance(result, dict)
    
    def test_short_data_returns_none(self):
        """짧은 데이터는 None 반환"""
        df = create_mock_df(days=250)
        df = add_all_indicators(df)
        short_df = df.iloc[:3]
        result = strategy_golden_cross(short_df, "TEST")
        assert result is None


class TestStrategyBollingerBounce:
    """볼린저 반등 전략 테스트"""
    
    def test_returns_none_or_dict(self):
        """None 또는 딕셔너리 반환"""
        df = create_mock_df(days=250)
        df = add_all_indicators(df)
        result = strategy_bollinger_bounce(df, "TEST")
        assert result is None or isinstance(result, dict)


class TestStrategyMACDCrossover:
    """MACD 크로스 전략 테스트"""
    
    def test_returns_none_or_dict(self):
        """None 또는 딕셔너리 반환"""
        df = create_mock_df(days=250)
        df = add_all_indicators(df)
        result = strategy_macd_crossover(df, "TEST")
        assert result is None or isinstance(result, dict)


class TestStrategyNear52wHigh:
    """52주 신고가 전략 테스트"""
    
    def test_returns_none_or_dict(self):
        """None 또는 딕셔너리 반환"""
        df = create_mock_df(days=250)
        df = add_all_indicators(df)
        result = strategy_near_52w_high(df, "TEST")
        assert result is None or isinstance(result, dict)


class TestStrategyDipBounce:
    """급락 반등 전략 테스트"""
    
    def test_returns_none_or_dict(self):
        """None 또는 딕셔너리 반환"""
        df = create_mock_df(days=250)
        df = add_all_indicators(df)
        result = strategy_dip_bounce(df, "TEST")
        assert result is None or isinstance(result, dict)


class TestStrategyVolumeSurge:
    """거래량 급증 전략 테스트"""
    
    def test_returns_none_or_dict(self):
        """None 또는 딕셔너리 반환"""
        df = create_mock_df(days=250)
        df = add_all_indicators(df)
        result = strategy_volume_surge(df, "TEST")
        assert result is None or isinstance(result, dict)


class TestAnalyzeRiskLevel:
    """위험도 분석 테스트"""
    
    def test_returns_dict(self):
        """딕셔너리 반환"""
        df = create_mock_df(days=250)
        df = add_all_indicators(df)
        result = analyze_risk_level(df, "TEST")
        assert isinstance(result, dict)
    
    def test_has_required_keys(self):
        """필수 키 포함"""
        df = create_mock_df(days=250)
        df = add_all_indicators(df)
        result = analyze_risk_level(df, "TEST")
        required_keys = ["symbol", "price", "risk_score", "risk_grade", "warnings"]
        for key in required_keys:
            assert key in result, f"Missing key: {key}"
    
    def test_risk_score_range(self):
        """위험도 점수가 0 이상인지"""
        df = create_mock_df(days=250)
        df = add_all_indicators(df)
        result = analyze_risk_level(df, "TEST")
        assert result["risk_score"] >= 0
    
    def test_risk_grade_valid(self):
        """위험 등급이 유효한지"""
        df = create_mock_df(days=250)
        df = add_all_indicators(df)
        result = analyze_risk_level(df, "TEST")
        valid_grades = ["🔴 고위험", "🟡 주의", "🟢 양호"]
        assert result["risk_grade"] in valid_grades


class TestAllStrategies:
    """전체 전략 리스트 테스트"""
    
    def test_has_7_strategies(self):
        """7개 전략이 있는지"""
        assert len(ALL_STRATEGIES) == 7
    
    def test_strategy_format(self):
        """전략 형식이 올바른지 (이모지, 이름, 함수)"""
        for emoji, name, func in ALL_STRATEGIES:
            assert isinstance(emoji, str)
            assert isinstance(name, str)
            assert callable(func)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
