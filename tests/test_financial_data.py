"""
재무제표 데이터 모듈 테스트
"""
import pytest
from unittest.mock import patch, MagicMock


class TestGetFinancialData:
    """get_financial_data 함수 테스트"""
    
    def test_returns_dict(self):
        """딕셔너리 반환 확인"""
        from src.financial_data import get_financial_data
        result = get_financial_data("AAPL")
        assert isinstance(result, dict)
    
    def test_has_symbol(self):
        """심볼 포함 확인"""
        from src.financial_data import get_financial_data
        result = get_financial_data("AAPL")
        assert "symbol" in result
        assert result["symbol"] == "AAPL"
    
    def test_has_profitability_metrics(self):
        """수익성 지표 포함 확인"""
        from src.financial_data import get_financial_data
        result = get_financial_data("MSFT")
        assert "roe" in result
        assert "roa" in result
        assert "profit_margin" in result
    
    def test_has_valuation_metrics(self):
        """밸류에이션 지표 포함 확인"""
        from src.financial_data import get_financial_data
        result = get_financial_data("GOOGL")
        assert "pe_trailing" in result
        assert "pb" in result
        assert "peg" in result
    
    def test_has_growth_metrics(self):
        """성장성 지표 포함 확인"""
        from src.financial_data import get_financial_data
        result = get_financial_data("NVDA")
        assert "revenue_growth" in result
        assert "earnings_growth" in result
    
    def test_has_financial_health_metrics(self):
        """재무 건전성 지표 포함 확인"""
        from src.financial_data import get_financial_data
        result = get_financial_data("AMZN")
        assert "debt_to_equity" in result
        assert "current_ratio" in result
        assert "free_cash_flow" in result
    
    def test_invalid_symbol_returns_error(self):
        """잘못된 심볼 에러 처리"""
        from src.financial_data import get_financial_data
        result = get_financial_data("INVALID_SYMBOL_XYZ123")
        # 에러가 있거나 기본값 반환
        assert isinstance(result, dict)


class TestCalculateFinancialScore:
    """calculate_financial_score 함수 테스트"""
    
    def test_returns_dict(self):
        """딕셔너리 반환 확인"""
        from src.financial_data import calculate_financial_score
        data = {
            "symbol": "TEST",
            "roe": 0.20,
            "roa": 0.10,
            "profit_margin": 0.15,
            "pe_trailing": 20,
            "pb": 3,
            "peg": 1.5,
            "revenue_growth": 0.10,
            "earnings_growth": 0.15,
            "debt_to_equity": 50,
            "current_ratio": 1.5,
            "free_cash_flow": 1000000000,
            "dividend_yield": 0.02,
            "payout_ratio": 0.30,
        }
        result = calculate_financial_score(data)
        assert isinstance(result, dict)
    
    def test_has_financial_score(self):
        """종합 점수 포함 확인"""
        from src.financial_data import calculate_financial_score
        data = {"symbol": "TEST", "roe": 0.20, "pe_trailing": 15}
        result = calculate_financial_score(data)
        assert "financial_score" in result
        assert 0 <= result["financial_score"] <= 100
    
    def test_has_financial_grade(self):
        """등급 포함 확인"""
        from src.financial_data import calculate_financial_score
        data = {"symbol": "TEST", "roe": 0.25, "pe_trailing": 12}
        result = calculate_financial_score(data)
        assert "financial_grade" in result
        assert result["financial_grade"] in ["A", "B", "C", "D", "F"]
    
    def test_has_scores_breakdown(self):
        """세부 점수 포함 확인"""
        from src.financial_data import calculate_financial_score
        data = {"symbol": "TEST"}
        result = calculate_financial_score(data)
        assert "scores" in result
        scores = result["scores"]
        assert "profitability" in scores
        assert "valuation" in scores
        assert "growth" in scores
        assert "financial_health" in scores
        assert "dividend" in scores
    
    def test_high_roe_increases_score(self):
        """높은 ROE가 점수 증가시키는지 확인"""
        from src.financial_data import calculate_financial_score
        low_roe = calculate_financial_score({"symbol": "TEST", "roe": 0.05})
        high_roe = calculate_financial_score({"symbol": "TEST", "roe": 0.30})
        assert high_roe["scores"]["profitability"] > low_roe["scores"]["profitability"]
    
    def test_low_pe_increases_score(self):
        """낮은 P/E가 점수 증가시키는지 확인"""
        from src.financial_data import calculate_financial_score
        high_pe = calculate_financial_score({"symbol": "TEST", "pe_trailing": 50})
        low_pe = calculate_financial_score({"symbol": "TEST", "pe_trailing": 10})
        assert low_pe["scores"]["valuation"] > high_pe["scores"]["valuation"]


class TestGetFinancialSummary:
    """get_financial_summary 함수 테스트"""
    
    def test_returns_dict(self):
        """딕셔너리 반환 확인"""
        from src.financial_data import get_financial_summary
        result = get_financial_summary("AAPL")
        assert isinstance(result, dict)
    
    def test_has_both_data_and_score(self):
        """데이터와 점수 모두 포함 확인"""
        from src.financial_data import get_financial_summary
        result = get_financial_summary("MSFT")
        # 재무 데이터
        assert "symbol" in result
        assert "roe" in result
        # 점수
        assert "financial_score" in result or "error" in result


class TestFormatFinancialReport:
    """format_financial_report 함수 테스트"""
    
    def test_returns_string(self):
        """문자열 반환 확인"""
        from src.financial_data import format_financial_report
        data = {
            "symbol": "TEST",
            "name": "Test Corp",
            "financial_score": 65,
            "financial_grade": "B",
            "key_metrics": {
                "roe": "20.0%",
                "pe": "15.0",
                "pb": "3.0",
                "peg": "1.5",
                "debt_equity": "50%",
                "revenue_growth": "10.0%",
                "dividend_yield": "2.0%",
            },
            "scores": {
                "profitability": 70,
                "valuation": 60,
                "growth": 65,
                "financial_health": 55,
                "dividend": 60,
            },
            "profit_margin": 0.15,
            "operating_margin": 0.20,
            "current_ratio": 1.5,
            "free_cash_flow": 5000000000,
            "earnings_growth": 0.12,
            "payout_ratio": 0.30,
        }
        result = format_financial_report(data)
        assert isinstance(result, str)
        assert "TEST" in result
    
    def test_error_data_returns_error_message(self):
        """에러 데이터 처리 확인"""
        from src.financial_data import format_financial_report
        data = {"symbol": "TEST", "error": "API Error"}
        result = format_financial_report(data)
        assert "❌" in result
        assert "TEST" in result


class TestFinancialDataIntegration:
    """통합 테스트"""
    
    def test_real_stock_analysis(self):
        """실제 종목 분석 테스트"""
        from src.financial_data import get_financial_summary
        # 대형주 테스트
        for symbol in ["AAPL", "MSFT", "GOOGL"]:
            result = get_financial_summary(symbol)
            assert isinstance(result, dict)
            assert "symbol" in result
    
    def test_grade_distribution(self):
        """등급 분포 테스트"""
        from src.financial_data import calculate_financial_score
        
        # 우량주 데이터 (높은 점수 예상)
        excellent = calculate_financial_score({
            "symbol": "EXCELLENT",
            "roe": 0.30,
            "roa": 0.15,
            "profit_margin": 0.25,
            "pe_trailing": 12,
            "pb": 1.2,
            "peg": 0.8,
            "revenue_growth": 0.20,
            "earnings_growth": 0.25,
            "debt_to_equity": 20,
            "current_ratio": 2.5,
            "free_cash_flow": 10000000000,
            "dividend_yield": 0.03,
            "payout_ratio": 0.40,
        })
        
        # 저조한 데이터 (낮은 점수 예상)
        poor = calculate_financial_score({
            "symbol": "POOR",
            "roe": -0.05,
            "roa": -0.02,
            "profit_margin": -0.10,
            "pe_trailing": 0,  # 적자
            "pb": 8,
            "peg": 5,
            "revenue_growth": -0.15,
            "earnings_growth": -0.20,
            "debt_to_equity": 300,
            "current_ratio": 0.5,
            "free_cash_flow": -5000000000,
            "dividend_yield": 0,
            "payout_ratio": 0,
        })
        
        assert excellent["financial_score"] > poor["financial_score"]
        assert excellent["financial_grade"] in ["A", "B"]
        assert poor["financial_grade"] in ["D", "F"]
