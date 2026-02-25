# -*- coding: utf-8 -*-
"""
LLM-based analyzer using Codex CLI login (no manual API key required).
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
import subprocess
import tempfile
import shutil
from collections import Counter
from pathlib import Path
from typing import Any


class AIAnalyzer:
    def __init__(self, model: str | None = None):
        self.provider = os.getenv("AI_PROVIDER", "codex-cli")
        # Keep CLI auth path stable across shells. Without CODEX_HOME, some sessions
        # fail to find existing login state and report false "Not logged in".
        if not os.getenv("CODEX_HOME"):
            home = Path.home() / ".codex"
            if home.exists():
                os.environ["CODEX_HOME"] = str(home)
        configured = os.getenv("CODEX_BIN", "codex")
        if os.name == "nt" and (configured or "").strip().lower() == "codex":
            configured = shutil.which("codex.cmd") or configured
        self.codex_bin = configured
        self.base_url = None
        self.model = model or os.getenv("AI_MODEL", "gpt-5.2")
        self.reasoning_effort = os.getenv("AI_REASONING_EFFORT", "medium").strip().lower() or "medium"
        if self.reasoning_effort not in {"low", "medium", "high"}:
            self.reasoning_effort = "medium"
        self.no_proxy_mode = str(os.getenv("AI_DISABLE_PROXY", "0")).strip().lower() in {"1", "true", "yes", "on"}
        self.cli_retries = self._i_env("AI_CLI_RETRIES", 1)
        self.cli_retry_delay_sec = self._f_env("AI_CLI_RETRY_DELAY_SEC", 1.5)
        fallback_models = os.getenv("AI_MODEL_FALLBACKS", "")
        self.fallback_models = [m.strip() for m in fallback_models.split(",") if m.strip()]
        if self.model not in self.fallback_models:
            self.fallback_models.append(self.model)

    @contextmanager
    def _temporary_proxy_env(self):
        keys = (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "GIT_HTTP_PROXY",
            "GIT_HTTPS_PROXY",
        )
        backup = {}
        try:
            for key in keys:
                val = os.getenv(key)
                if self.no_proxy_mode or (val and "127.0.0.1:9" in val):
                    backup[key] = os.environ.get(key)
                    os.environ[key] = ""
            yield
        finally:
            for key, old in backup.items():
                if old is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old

    def _i_env(self, k: str, d: int) -> int:
        try:
            return int(os.getenv(k, str(d)))
        except Exception:
            return d

    def _f_env(self, k: str, d: float) -> float:
        try:
            return float(os.getenv(k, str(d)))
        except Exception:
            return d

    @property
    def has_api_access(self) -> bool:
        if self.provider != "codex-cli":
            return False
        try:
            with self._temporary_proxy_env():
                proc = subprocess.run(
                    [self.codex_bin, "login", "status"],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="ignore",
                    timeout=8,
                    check=False,
                )
            if proc.returncode != 0:
                return False
            out = (proc.stdout or "") + (proc.stderr or "")
            return "Logged in using ChatGPT" in out or "Logged in" in out
        except Exception:
            return False

    def _run_codex(self, cmd: list[str], prompt: str, max_tokens: int, timeout: int = 240) -> tuple[bool, subprocess.CompletedProcess[Any, Any], str | None]:
        last_error: str | None = None
        for model in self.fallback_models:
            for attempt in range(1, max(1, self.cli_retries) + 1):
                if "-m" in cmd:
                    run_cmd = list(cmd)
                    mi = run_cmd.index("-m")
                    if mi + 1 < len(run_cmd):
                        run_cmd[mi + 1] = model
                    else:
                        run_cmd.append(model)
                else:
                    run_cmd = [*cmd, "-m", model]
                try:
                    with self._temporary_proxy_env():
                        proc = subprocess.run(
                            run_cmd,
                            input=prompt,
                            capture_output=True,
                            text=True,
                            encoding="utf-8",
                            errors="ignore",
                            timeout=timeout,
                            check=False,
                        )
                except Exception as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    proc = None
                else:
                    if proc.returncode == 0:
                        return True, proc, model
                    last_error = f"rc={proc.returncode} stdout={proc.stdout!r} stderr={proc.stderr!r}"

                if attempt < max(1, self.cli_retries):
                    time.sleep(self.cli_retry_delay_sec)

        return False, proc if "proc" in locals() and proc is not None else subprocess.CompletedProcess(cmd, 1), last_error

    def _truncate_to_token_budget(self, text: str, max_tokens: int) -> str:
        if not text:
            return text
        budget = max(64, int(max_tokens))
        chars_per_token = max(1.0, self._f_env("AI_CHAR_PER_TOKEN", 4.0))
        max_chars = int(budget * chars_per_token)
        if len(text) <= max_chars:
            return text
        cut = text[:max_chars]
        for sep in ("\n\n", "\n", ". ", " "):
            idx = cut.rfind(sep)
            if idx >= int(max_chars * 0.6):
                cut = cut[:idx]
                break
        return cut.rstrip() + "\n\n[output truncated to token budget]"

    def _call(self, prompt: str, max_tokens: int = 1400) -> str | None:
        if not self.has_api_access:
            return None

        out_file: tempfile.NamedTemporaryFile | None = None
        try:
            token_budget = max(64, int(max_tokens))
            prompt_with_budget = (
                f"Response length budget: about {token_budget} tokens maximum.\n"
                "If needed, prioritize key actions and omit low-priority detail.\n\n"
                f"{prompt}"
            )
            out_file = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
            out_file.close()

            cmd = [
                self.codex_bin,
                "exec",
                "-m",
                self.model,
                "-c",
                f'model_reasoning_effort="{self.reasoning_effort}"',
                "--ephemeral",
                "--skip-git-repo-check",
                "-s",
                "read-only",
                "--output-last-message",
                out_file.name,
                "-",
            ]
            ok, proc, used_model = self._run_codex(cmd, prompt_with_budget, token_budget, timeout=240)
            if not ok:
                return None

            with open(out_file.name, "r", encoding="utf-8", errors="ignore") as fh:
                text = fh.read().strip()
            if not text and (proc.stdout or proc.stderr):
                text = (proc.stdout or "").strip() or (proc.stderr or "").strip()
            if not text:
                return None
            return self._truncate_to_token_budget(text, token_budget)
        except Exception:
            return None
        finally:
            if out_file and os.path.exists(out_file.name):
                try:
                    os.remove(out_file.name)
                except Exception:
                    pass

    def analyze_stock(self, symbol: str, data: dict[str, Any]) -> dict[str, Any]:
        if not self.has_api_access:
            return {
                "error": "Codex login is required. Run: codex login",
                "mode": "codex-cli",
                "model": self.model,
                "symbol": symbol,
            }

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
            f"Return 21d(%): {data.get('return_21d', 0)}\n"
            f"Return 63d(%): {data.get('return_63d', 0)}\n"
            f"Relative strength 21d vs QQQ(%p): {data.get('relative_strength_21d', 0)}\n"
            f"Relative strength 63d vs QQQ(%p): {data.get('relative_strength_63d', 0)}\n"
            f"Volume ratio: {data.get('volume_ratio', 1)}\n"
            f"Support: {data.get('support', [])}\n"
            f"Resistance: {data.get('resistance', [])}\n"
            f"Liquidity score: {data.get('liquidity_score', (data.get('trade_plan') or {}).get('liquidity', {}).get('score', 0))}\n"
            f"Suggested position(%): {(data.get('trade_plan') or {}).get('execution', {}).get('position_pct', 0)}\n"
            f"RR2: {(data.get('trade_plan') or {}).get('risk_reward', {}).get('rr2', 0)}\n"
            f"Stage: {(data.get('trade_plan') or {}).get('positioning', {}).get('stage', '')}\n"
            f"Days to earnings: {data.get('days_to_earnings', 'N/A')}\n"
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
        _legacy_context: dict[str, list[dict[str, Any]]],
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
        get_rs63 = lambda s: float(s.get("relative_strength_63d", (s.get("trade_plan") or {}).get("positioning", {}).get("relative_strength_63d", 0)))
        get_liq = lambda s: float(s.get("liquidity_score", (s.get("trade_plan") or {}).get("liquidity", {}).get("score", 0)))
        get_pos = lambda s: float((s.get("trade_plan") or {}).get("execution", {}).get("position_pct", s.get("position_size_pct", 0)))

        avg_rsi = sum(get_rsi(s) for s in stocks) / n
        avg_score = sum(get_score(s) for s in stocks) / n
        avg_quality = sum(get_quality(s) for s in stocks) / n
        avg_inv = sum(get_inv(s) for s in stocks) / n
        avg_setup = sum(get_setup(s) for s in stocks) / n
        avg_rs63 = sum(get_rs63(s) for s in stocks) / n
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
            f"rr2={get_rr2(s):.2f} rs63={get_rs63(s):.1f} liq={get_liq(s):.1f} pos={get_pos(s):.1f}% "
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
            f"Avg RS63 vs QQQ(%p): {avg_rs63:.1f}\n"
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
            "avg_rs63_vs_qqq": avg_rs63,
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

    def analyze_research_report(self, report: dict[str, Any]) -> dict[str, Any]:
        if not self.has_api_access:
            return {"error": "Codex login is required. Run: codex login", "mode": "codex-cli", "model": self.model}

        prompt = (
            "You are a cautious macro strategist.\n"
            "Write in Korean. Be concise and specific.\n"
            "Output plain text only.\n\n"
            "Given the following structured research report (JSON-like), return:\n"
            "1) Risk-on/off judgment\n"
            "2) Favored asset classes\n"
            "3) US market tactical stance (next 1-3 months)\n"
            "4) Top 5 risks to watch\n"
            "Respect module confidence: if confidence is low or data_gaps mention errors,\n"
            "avoid strong claims and mark items as tentative or exclude them.\n"
            "Keep it under 40 lines.\n\n"
            f"Report: {report}\n"
        )
        text = self._call(prompt, max_tokens=1200)
        if not text:
            return {"error": "AI call failed", "mode": "codex-cli", "model": self.model}
        return {"analysis": text, "mode": "codex-cli", "model": self.model}

ai = AIAnalyzer()
