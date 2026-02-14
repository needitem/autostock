"""
Compatibility AI analyzer module used by legacy tests.
"""
from __future__ import annotations

import os
import requests

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")
OPENROUTER_URL = os.getenv("OPENROUTER_URL", "https://openrouter.ai/api/v1/chat/completions")


def _call_ai(prompt: str, max_tokens: int = 800) -> str | None:
    if not OPENROUTER_API_KEY:
        return None
    try:
        response = requests.post(
            OPENROUTER_URL,
            headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": OPENROUTER_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "temperature": 0.2,
            },
            timeout=30,
        )
        if response.status_code != 200:
            return None
        return response.json().get("choices", [{}])[0].get("message", {}).get("content")
    except Exception:
        return None


def analyze_news_with_ai(symbol: str, news: list[dict] | None) -> dict:
    if not news:
        return {"error": "No news provided", "symbol": symbol}
    if not OPENROUTER_API_KEY:
        return {"error": "OPENROUTER_API_KEY missing", "symbol": symbol}

    headlines = "\n".join(f"- {item.get('headline', '')}" for item in news[:10])
    prompt = f"{symbol} 뉴스 분석:\n{headlines}\n\n요약, 투자심리, 리스크를 알려주세요."
    result = _call_ai(prompt, max_tokens=700)
    if not result:
        return {"error": "AI call failed", "symbol": symbol}
    return {"symbol": symbol, "analysis": result}


def analyze_stock_with_ai(
    symbol: str,
    stock_data: dict | None,
    news: list[dict] | None = None,
    market_data: dict | None = None,
) -> dict:
    if not isinstance(stock_data, dict):
        return {"error": "Invalid stock data", "symbol": symbol}
    if not OPENROUTER_API_KEY:
        return {"error": "OPENROUTER_API_KEY missing", "symbol": symbol}

    prompt = (
        f"{symbol} 종목 분석\n"
        f"price={stock_data.get('price')}, rsi={stock_data.get('rsi')}, ma50_gap={stock_data.get('ma50_gap')}\n"
        f"news_count={len(news or [])}, has_market_data={bool(market_data)}"
    )
    result = _call_ai(prompt, max_tokens=900)
    if not result:
        return {"error": "AI call failed", "symbol": symbol}
    return {"symbol": symbol, "analysis": result}


def get_market_sentiment(news: list[dict] | None, fear_greed: dict | None = None) -> dict:
    if not news:
        return {"error": "No market news", "fear_greed": fear_greed or {}}
    if not OPENROUTER_API_KEY:
        return {"error": "OPENROUTER_API_KEY missing", "fear_greed": fear_greed or {}}

    headlines = "\n".join(f"- {item.get('headline', '')}" for item in news[:10])
    fg = fear_greed or {}
    prompt = (
        "시장 심리 분석\n"
        f"fear_greed={fg.get('score')} ({fg.get('rating')})\n"
        f"{headlines}"
    )
    result = _call_ai(prompt, max_tokens=700)
    if not result:
        return {"error": "AI call failed", "fear_greed": fg}
    return {"analysis": result, "fear_greed": fg}

