"""
Legacy compatibility wrapper for AI analysis helpers.

This project standardizes on Codex CLI authentication (ChatGPT login) and does
not require manual API keys. These functions remain for older modules/tests.
"""

from __future__ import annotations

import os
from typing import Any

from ai.analyzer import AIAnalyzer


_DEFAULT_MODEL = os.getenv("AI_MODEL", "gpt-5.2")


def _call_ai(prompt: str, max_tokens: int = 800) -> str | None:
    # Keep as a module-level function so tests can patch it without invoking
    # the external `codex` binary.
    return AIAnalyzer()._call(prompt, max_tokens=max_tokens)


def _no_login_error(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "error": "Codex login is required. Run: codex login",
        "mode": "codex-cli",
        "model": _DEFAULT_MODEL,
    }
    if extra:
        out.update(extra)
    return out


def analyze_news_with_ai(symbol: str, news: list[dict] | None) -> dict[str, Any]:
    if not news:
        return {"error": "No news provided", "symbol": symbol, "mode": "codex-cli", "model": _DEFAULT_MODEL}

    headlines = []
    for item in (news or [])[:10]:
        headline = str((item or {}).get("headline", "")).strip()
        if headline:
            headlines.append(f"- {headline[:200]}")
    headline_text = "\n".join(headlines) if headlines else "- none"

    prompt = (
        "You are a cautious equity analyst.\n"
        "Write in Korean and keep it concise.\n"
        "Output plain text only.\n\n"
        f"Symbol: {symbol}\n"
        "Recent headlines:\n"
        f"{headline_text}\n\n"
        "Return:\n"
        "1) what changed (facts)\n"
        "2) bull vs bear points\n"
        "3) likely 1-4 week implications\n"
        "4) key watch items\n"
    )
    text = _call_ai(prompt, max_tokens=700)
    if not text:
        return _no_login_error({"symbol": symbol})
    return {"symbol": symbol, "analysis": text, "mode": "codex-cli", "model": _DEFAULT_MODEL}


def analyze_stock_with_ai(
    symbol: str,
    stock_data: dict | None,
    news: list[dict] | None = None,
    market_data: dict | None = None,
) -> dict[str, Any]:
    if not isinstance(stock_data, dict):
        return {"error": "Invalid stock data", "symbol": symbol, "mode": "codex-cli", "model": _DEFAULT_MODEL}

    price = stock_data.get("price")
    rsi = stock_data.get("rsi")
    adx = stock_data.get("adx")
    ma50_gap = stock_data.get("ma50_gap")
    rs21 = stock_data.get("relative_strength_21d", stock_data.get("rs21"))
    rs63 = stock_data.get("relative_strength_63d", stock_data.get("rs63"))

    headlines = []
    for item in (news or [])[:6]:
        headline = str((item or {}).get("headline", "")).strip()
        if headline:
            headlines.append(f"- {headline[:200]}")
    headline_text = "\n".join(headlines) if headlines else "- none"

    market_ctx = market_data or {}
    market_note = ""
    try:
        mc = market_ctx.get("market_condition") or {}
        if isinstance(mc, dict) and mc.get("message"):
            market_note = str(mc.get("message"))
    except Exception:
        market_note = ""

    prompt = (
        "You are a cautious equity analyst.\n"
        "Write in Korean and keep it concise.\n"
        "Output plain text only.\n\n"
        f"Symbol: {symbol}\n"
        f"Price: {price}\n"
        f"RSI: {rsi}\n"
        f"ADX: {adx}\n"
        f"MA50 gap(%): {ma50_gap}\n"
        f"Relative strength 21d vs QQQ(%p): {rs21}\n"
        f"Relative strength 63d vs QQQ(%p): {rs63}\n"
        f"Market note: {market_note or 'N/A'}\n"
        "Recent headlines:\n"
        f"{headline_text}\n\n"
        "Return:\n"
        "1) trend read\n"
        "2) entry plan (if any)\n"
        "3) exit plan (if any)\n"
        "4) key risk\n"
        "5) confidence level (high/medium/low)\n"
    )

    text = _call_ai(prompt, max_tokens=900)
    if not text:
        return _no_login_error({"symbol": symbol})
    return {"symbol": symbol, "analysis": text, "mode": "codex-cli", "model": _DEFAULT_MODEL}


def get_market_sentiment(news: list[dict] | None, fear_greed: dict | None = None) -> dict[str, Any]:
    if not news:
        return {
            "error": "No market news",
            "fear_greed": fear_greed or {},
            "mode": "codex-cli",
            "model": _DEFAULT_MODEL,
        }

    headlines = []
    for item in (news or [])[:10]:
        headline = str((item or {}).get("headline", "")).strip()
        if headline:
            headlines.append(f"- {headline[:200]}")
    headline_text = "\n".join(headlines) if headlines else "- none"

    fg = fear_greed or {}
    prompt = (
        "You are a cautious market strategist.\n"
        "Write in Korean and keep it concise.\n"
        "Output plain text only.\n\n"
        f"Fear&Greed: {fg.get('score')} ({fg.get('rating')})\n"
        "Headlines:\n"
        f"{headline_text}\n\n"
        "Return:\n"
        "1) market regime (risk-on/off)\n"
        "2) what is driving sentiment\n"
        "3) near-term risks to watch\n"
    )
    text = _call_ai(prompt, max_tokens=700)
    if not text:
        return _no_login_error({"fear_greed": fg})
    return {"analysis": text, "fear_greed": fg, "mode": "codex-cli", "model": _DEFAULT_MODEL}

