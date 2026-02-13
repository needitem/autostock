import os
import sys
from unittest.mock import patch, Mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ai.analyzer import AIAnalyzer


def test_ai_analyzer_defaults_to_openai(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    analyzer = AIAnalyzer()

    assert analyzer.provider == "openai"
    assert analyzer.model == os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    assert analyzer.base_url.endswith("/chat/completions")


@patch("ai.analyzer.requests.post")
def test_ai_call_uses_openai_request_schema(mock_post, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-4o-mini")
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "choices": [{"message": {"content": "분석 결과"}}]
    }
    mock_post.return_value = mock_response

    analyzer = AIAnalyzer()
    result = analyzer._call("테스트 프롬프트", max_tokens=123)

    assert result == "분석 결과"
    kwargs = mock_post.call_args.kwargs
    assert kwargs["json"]["model"] == "gpt-4o-mini"
    assert kwargs["json"]["max_tokens"] == 123
    assert kwargs["headers"]["Authorization"] == "Bearer test-key"


def test_ai_call_returns_none_without_api_key(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    analyzer = AIAnalyzer()

    assert analyzer._call("테스트") is None


def test_analyze_stock_falls_back_to_rule_based_without_api_key(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    analyzer = AIAnalyzer()

    result = analyzer.analyze_stock("AAPL", {
        "rsi": 28,
        "adx": 27,
        "ma50_gap": 2.1,
        "volume_ratio": 1.5,
        "support": [180.0],
        "resistance": [191.0],
    })

    assert "analysis" in result
    assert result.get("mode") == "rule-based"
    assert "룰기반" in result["analysis"]


def test_analyze_full_market_returns_rule_based_without_api_key(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    analyzer = AIAnalyzer()

    stocks = [
        {"symbol": "AAPL", "price": 190.0, "rsi": 45, "score": {"total_score": 82, "grade": "A"}},
        {"symbol": "MSFT", "price": 420.0, "rsi": 55, "score": {"total_score": 78, "grade": "B"}},
    ]
    categories = {"tech": {"name": "테크", "emoji": "💻", "stocks": ["AAPL", "MSFT"]}}

    result = analyzer.analyze_full_market(stocks, {}, {}, categories)

    assert result.get("mode") == "rule-based"
    assert result.get("total") == 2
    assert "analysis" in result
