# -*- coding: utf-8 -*-
"""
LLM-based analyzer using Codex CLI login (no manual API key required).
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from collections import Counter
from typing import Any


class AIAnalyzer:
    def __init__(self, model: str | None = None):
        self.provider = os.getenv("AI_PROVIDER", "codex-cli")
        self.codex_bin = os.getenv("CODEX_BIN", "codex")
        self.base_url = None
        self.model = model or os.getenv("AI_MODEL", "gpt-5.2")

    @property
    def has_api_access(self) -> bool:
        if self.provider != "codex-cli":
            return False
        try:
            proc = subprocess.run(
                [self.codex_bin, "login", "status"],
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
            )
            if proc.returncode != 0:
                return False
            out = (proc.stdout or "") + (proc.stderr or "")
            return "Logged in using ChatGPT" in out or "Logged in" in out
        except Exception:
            return False

    def _call(self, prompt: str, max_tokens: int = 1400) -> str | None:
        if not self.has_api_access:
            return None

        out_file: tempfile.NamedTemporaryFile | None = None
        try:
            out_file = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
            out_file.close()

            cmd = [
                self.codex_bin,
                "exec",
                "-m",
                self.model,
                "-c",
                'model_reasoning_effort="medium"',
                "--ephemeral",
                "--skip-git-repo-check",
                "-s",
                "read-only",
                "--output-last-message",
                out_file.name,
                "-",
            ]
            proc = subprocess.run(
                cmd,
                input=prompt,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                timeout=240,
                check=False,
            )
            if proc.returncode != 0:
                return None

            with open(out_file.name, "r", encoding="utf-8", errors="ignore") as fh:
                text = fh.read().strip()
            return text or None
        except Exception:
            return None
        finally:
            if out_file and os.path.exists(out_file.name):
                try:
                    os.remove(out_file.name)
                except Exception:
                    pass

    def _trim_news(self, news_items: list[dict[str, Any]] | None, limit: int = 8) -> list[str]:
        if not news_items:
            return []
        lines: list[str] = []
        for item in news_items[:limit]:
            headline = str(item.get("headline", "")).strip()
            source = str(item.get("source", "")).strip()
            if headline:
                lines.append(f"- {headline[:140]} ({source})".strip())
        return lines

    def select_news_symbols(self, stocks: list[dict[str, Any]], limit: int = 60) -> list[str]:
        """Pick symbols that are most likely to influence final AI output."""
        if not stocks:
            return []
        capped = max(1, min(limit, len(stocks)))

        ranked = sorted(
            stocks,
            key=lambda s: -float(
                s.get(
                    "investability_score",
                    s.get("quality_score", (s.get("score") or {}).get("total_score", 0)),
                )
            ),
        )
        seen: set[str] = set()
        symbols: list[str] = []
        for stock in ranked:
            symbol = str(stock.get("symbol", "")).upper().strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            symbols.append(symbol)
            if len(symbols) >= capped:
                break
        return symbols

    def analyze_stock(self, symbol: str, data: dict[str, Any]) -> dict[str, Any]:
        if not self.has_api_access:
            return {
                "error": "Codex login is required. Run: codex login",
                "mode": "codex-cli",
                "model": self.model,
                "symbol": symbol,
            }

        news_lines = self._trim_news(data.get("news"))
        prompt = (
            "You are a cautious equity analyst.\n"
            "Write in Korean and keep it concise.\n"
            "Output plain text only.\n\n"
            f"Symbol: {symbol}\n"
            f"Price: {data.get('price', 0)}\n"
            f"Score: {data.get('total_score') or data.get('score', {}).get('total_score', 0)}\n"
            f"RSI: {data.get('rsi', 50)}\n"
            f"ADX: {data.get('adx', 0)}\n"
            f"MA50 gap(%): {data.get('ma50_gap', 0)}\n"
            f"Volume ratio: {data.get('volume_ratio', 1)}\n"
            f"Support: {data.get('support', [])}\n"
            f"Resistance: {data.get('resistance', [])}\n"
            f"Liquidity score: {data.get('liquidity_score', (data.get('trade_plan') or {}).get('liquidity', {}).get('score', 0))}\n"
            f"Suggested position(%): {(data.get('trade_plan') or {}).get('execution', {}).get('position_pct', 0)}\n"
            f"RR2: {(data.get('trade_plan') or {}).get('risk_reward', {}).get('rr2', 0)}\n"
            f"Stage: {(data.get('trade_plan') or {}).get('positioning', {}).get('stage', '')}\n"
            f"Days to earnings: {data.get('days_to_earnings', 'N/A')}\n"
            "Recent news:\n"
            f"{chr(10).join(news_lines) if news_lines else '- none'}\n\n"
            "Give:\n"
            "1) trend read\n"
            "2) entry/exit plan\n"
            "3) key risk\n"
            "4) confidence level (high/medium/low)\n"
            "No markdown tables."
        )

        text = self._call(prompt, max_tokens=900)
        if not text:
            return {"error": "AI call failed", "mode": "codex-cli", "model": self.model, "symbol": symbol}
        return {"analysis": text, "mode": "codex-cli", "model": self.model}

    def analyze_full_market(
        self,
        stocks: list[dict[str, Any]],
        news_data: dict[str, list[dict[str, Any]]],
        market_data: dict[str, Any],
        categories: dict[str, Any],
    ) -> dict[str, Any]:
        if not stocks:
            return {"error": "No stocks"}
        if not self.has_api_access:
            return {"error": "Codex login is required. Run: codex login", "total": len(stocks), "mode": "codex-cli"}

        n = len(stocks)
        get_score = lambda s: float((s.get("score") or {}).get("total_score", 0))
        get_grade = lambda s: (s.get("score") or {}).get("grade", "C")
        get_rsi = lambda s: float(s.get("rsi", 50))
        get_quality = lambda s: float(s.get("quality_score", get_score(s)))
        get_inv = lambda s: float(s.get("investability_score", get_quality(s)))
        get_adx = lambda s: float(s.get("adx", 0))
        get_setup = lambda s: float((s.get("trade_plan") or {}).get("positioning", {}).get("setup_score", 0))
        get_rr2 = lambda s: float((s.get("trade_plan") or {}).get("risk_reward", {}).get("rr2", 0))
        get_liq = lambda s: float(s.get("liquidity_score", (s.get("trade_plan") or {}).get("liquidity", {}).get("score", 0)))
        get_pos = lambda s: float((s.get("trade_plan") or {}).get("execution", {}).get("position_pct", s.get("position_size_pct", 0)))

        avg_rsi = sum(get_rsi(s) for s in stocks) / n
        avg_score = sum(get_score(s) for s in stocks) / n
        avg_quality = sum(get_quality(s) for s in stocks) / n
        avg_inv = sum(get_inv(s) for s in stocks) / n
        avg_setup = sum(get_setup(s) for s in stocks) / n
        avg_liq = sum(get_liq(s) for s in stocks) / n
        avg_pos = sum(get_pos(s) for s in stocks) / n
        grades = Counter(get_grade(s) for s in stocks)
        oversold = sum(1 for s in stocks if get_rsi(s) < 30)
        overbought = sum(1 for s in stocks if get_rsi(s) > 70)
        strong_trend = sum(1 for s in stocks if get_adx(s) >= 25)
        tradeable_count = sum(1 for s in stocks if bool((s.get("trade_plan") or {}).get("tradeable")))

        top = sorted(stocks, key=lambda s: -get_inv(s))[:12]
        top_lines = [
            f"- {s['symbol']} price={float(s.get('price', 0)):.2f} "
            f"score={get_score(s):.1f} inv={get_inv(s):.1f} setup={get_setup(s):.1f} "
            f"rr2={get_rr2(s):.2f} liq={get_liq(s):.1f} pos={get_pos(s):.1f}% "
            f"dte={s.get('days_to_earnings', 'NA')} rsi={get_rsi(s):.1f}"
            for s in top
        ]

        cat_lines: list[str] = []
        for key, info in (categories or {}).items():
            symbols = set(info.get("stocks", []))
            if not symbols:
                continue
            pool = [s for s in stocks if s["symbol"] in symbols]
            if not pool:
                continue
            best = max(pool, key=get_inv)
            cat_lines.append(
                f"- {info.get('name', key)}: {best['symbol']} "
                f"inv={get_inv(best):.1f} setup={get_setup(best):.1f} rr2={get_rr2(best):.2f} "
                f"liq={get_liq(best):.1f}"
            )

        market_condition = (market_data or {}).get("market_condition", {})
        fear_greed = (market_data or {}).get("fear_greed", {})
        market_news = self._trim_news((market_data or {}).get("market_news", []), limit=8)
        sample_news: list[str] = []
        for symbol in [s["symbol"] for s in top[:8]]:
            sample_news.extend(self._trim_news((news_data or {}).get(symbol, []), limit=2))
        sample_news = sample_news[:12]

        prompt = (
            "You are a professional portfolio strategist.\n"
            "Write in Korean. Be practical and specific.\n"
            "Output plain text only.\n\n"
            f"Universe size: {n}\n"
            f"Market condition: {market_condition.get('message', 'N/A')}\n"
            f"Fear&Greed: {fear_greed.get('score', 'N/A')} ({fear_greed.get('rating', 'N/A')})\n"
            f"Avg RSI: {avg_rsi:.1f}\n"
            f"Avg Score: {avg_score:.1f}\n"
            f"Avg Quality: {avg_quality:.1f}\n"
            f"Avg Investability: {avg_inv:.1f}\n"
            f"Avg Setup Score: {avg_setup:.1f}\n"
            f"Avg Liquidity Score: {avg_liq:.1f}\n"
            f"Avg Suggested Position(%): {avg_pos:.1f}\n"
            f"Oversold count: {oversold}\n"
            f"Overbought count: {overbought}\n"
            f"Strong-trend count: {strong_trend}\n"
            f"Tradeable count: {tradeable_count}\n"
            "Grade distribution:\n"
            f"- A {grades.get('A', 0)} / B {grades.get('B', 0)} / C {grades.get('C', 0)} / D {grades.get('D', 0)} / F {grades.get('F', 0)}\n\n"
            "Top candidates:\n"
            f"{chr(10).join(top_lines)}\n\n"
            "Sector leaders:\n"
            f"{chr(10).join(cat_lines) if cat_lines else '- none'}\n\n"
            "Market headlines:\n"
            f"{chr(10).join(market_news) if market_news else '- none'}\n\n"
            "Company headline samples:\n"
            f"{chr(10).join(sample_news) if sample_news else '- none'}\n\n"
            "Return:\n"
            "1) one-paragraph market regime\n"
            "2) top 7 picks with why each matters\n"
            "3) risk controls for next 1-2 weeks\n"
            "4) what to avoid now\n"
            "Keep it under 45 lines."
        )

        analysis = self._call(prompt, max_tokens=1800)
        if not analysis:
            return {"error": "AI call failed", "total": n, "mode": "codex-cli", "model": self.model}

        stats = {
            "avg_rsi": avg_rsi,
            "avg_score": avg_score,
            "avg_quality": avg_quality,
            "avg_investability": avg_inv,
            "avg_setup_score": avg_setup,
            "avg_liquidity": avg_liq,
            "avg_position_pct": avg_pos,
            "grade_dist": {
                "A": grades.get("A", 0),
                "B": grades.get("B", 0),
                "C": grades.get("C", 0),
                "D": grades.get("D", 0),
                "F": grades.get("F", 0),
            },
            "oversold": oversold,
            "overbought": overbought,
            "strong_trend": strong_trend,
            "tradeable_count": tradeable_count,
        }
        return {"analysis": analysis, "total": n, "stats": stats, "mode": "codex-cli", "model": self.model}

ai = AIAnalyzer()
