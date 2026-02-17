from unittest.mock import PropertyMock, patch

from ai.analyzer import AIAnalyzer


def test_ai_analyzer_defaults_to_codex_cli():
    analyzer = AIAnalyzer()
    assert analyzer.provider == "codex-cli"
    assert analyzer.model == "gpt-5.2"
    assert analyzer.base_url is None


@patch.object(AIAnalyzer, "has_api_access", new_callable=PropertyMock, return_value=False)
def test_ai_call_returns_none_without_login(_mock_access):
    analyzer = AIAnalyzer()
    assert analyzer._call("test prompt", max_tokens=123) is None


@patch.object(AIAnalyzer, "has_api_access", new_callable=PropertyMock, return_value=False)
def test_analyze_stock_returns_error_without_login(_mock_access):
    analyzer = AIAnalyzer()
    result = analyzer.analyze_stock(
        "AAPL",
        {
            "rsi": 28,
            "adx": 27,
            "ma50_gap": 2.1,
            "volume_ratio": 1.5,
            "support": [180.0],
            "resistance": [191.0],
        },
    )
    assert "error" in result
    assert result.get("mode") == "codex-cli"


@patch.object(AIAnalyzer, "has_api_access", new_callable=PropertyMock, return_value=True)
@patch.object(AIAnalyzer, "_call", return_value="ai analysis text")
def test_analyze_full_market_uses_ai_call(_mock_call, _mock_access):
    analyzer = AIAnalyzer()
    stocks = [
        {"symbol": "AAPL", "price": 190.0, "rsi": 45, "adx": 30, "score": {"total_score": 82, "grade": "A"}},
        {"symbol": "MSFT", "price": 420.0, "rsi": 55, "adx": 26, "score": {"total_score": 78, "grade": "B"}},
    ]
    categories = {"tech": {"name": "Tech", "emoji": "T", "stocks": ["AAPL", "MSFT"]}}
    result = analyzer.analyze_full_market(stocks, {}, {}, categories)

    assert result.get("mode") == "codex-cli"
    assert result.get("total") == 2
    assert result.get("analysis") == "ai analysis text"
