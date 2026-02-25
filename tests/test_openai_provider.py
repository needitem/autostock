from unittest.mock import PropertyMock, patch
import subprocess
from pathlib import Path

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


@patch.object(AIAnalyzer, "has_api_access", new_callable=PropertyMock, return_value=True)
def test_ai_call_enforces_token_budget_prompt_and_truncation(_mock_access):
    analyzer = AIAnalyzer()
    long_text = "X" * 3000

    def fake_run(cmd, prompt, max_tokens, timeout=240):
        assert "Response length budget" in prompt
        assert max_tokens == 100
        out_path = Path(cmd[cmd.index("--output-last-message") + 1])
        out_path.write_text(long_text, encoding="utf-8")
        return True, subprocess.CompletedProcess(cmd, 0, stdout="", stderr=""), analyzer.model

    with patch.object(analyzer, "_run_codex", side_effect=fake_run):
        result = analyzer._call("test prompt", max_tokens=100)

    assert result is not None
    assert "[output truncated to token budget]" in result
