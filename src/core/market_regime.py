from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Callable

import pandas as pd

from core.chart_structure import ChartStructureCollector
from core.indicators import calculate_indicators
from core.stock_data import get_stock_data


def _s(value: Any) -> str:
    return str(value or "").strip()


def _f(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    if out != out or out in {float("inf"), float("-inf")}:
        return default
    return out


def _pct_gap(current: float, reference: float) -> float | None:
    if current <= 0 or reference <= 0:
        return None
    return round((current / reference - 1.0) * 100.0, 2)


class MarketRegimeCollector:
    """Collect market-wide regime evidence for crash, stress, and risk-on/risk-off context."""

    def __init__(
        self,
        *,
        get_stock_data_fn: Callable[..., pd.DataFrame | None] | None = None,
        chart_structure: ChartStructureCollector | None = None,
    ) -> None:
        self.get_stock_data = get_stock_data_fn or get_stock_data
        self.chart_structure = chart_structure or ChartStructureCollector()

    def collect(
        self,
        *,
        market_condition: dict[str, Any] | None = None,
        fear_greed: dict[str, Any] | None = None,
        macro: dict[str, Any] | None = None,
        options_market: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        symbols = self._benchmark_symbols()
        benchmarks: dict[str, dict[str, Any]] = {}
        for symbol in symbols:
            row = self._benchmark_snapshot(symbol)
            if row:
                benchmarks[symbol] = row

        evidence = self._regime_evidence(
            benchmarks=benchmarks,
            market_condition=market_condition or {},
            fear_greed=fear_greed or {},
            macro=macro or {},
            options_market=options_market or {},
        )
        return {
            "status": "ok" if benchmarks else "unavailable",
            "source": "benchmark_chart_structure",
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "benchmarkSymbols": symbols,
            "primaryBenchmark": symbols[0] if symbols else "",
            "benchmarks": benchmarks,
            "evidence": evidence,
            "regimeLabel": self._regime_label(evidence),
            "notes": "This is structured market evidence, not a mechanical trade gate.",
        }

    def _benchmark_symbols(self) -> list[str]:
        raw = _s(os.getenv("MARKET_REGIME_BENCHMARKS") or os.getenv("AI_MARKET_INDICATOR") or "QQQ,SPY,IWM")
        seen: set[str] = set()
        out: list[str] = []
        for part in raw.replace(";", ",").split(","):
            symbol = _s(part).upper().replace(".", "-")
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            out.append(symbol)
        return out or ["QQQ", "SPY", "IWM"]

    def _benchmark_snapshot(self, symbol: str) -> dict[str, Any]:
        try:
            bars = self.get_stock_data(symbol, period="15mo", auto_adjust=False)
        except Exception:
            bars = None
        if bars is None or bars.empty:
            return {}
        indicators = calculate_indicators(bars)
        if indicators is None:
            return {}

        structure_payload = self.chart_structure.analyze_daily(symbol, bars, indicators)
        structure = structure_payload.get("chartStructure") if isinstance(structure_payload.get("chartStructure"), dict) else {}
        close = _f(indicators.get("price"), _f(bars["Close"].iloc[-1]))
        previous_close = _f(indicators.get("price_prev"), close)
        price_5d_ago = _f(bars["Close"].iloc[-6], close) if len(bars) >= 6 else close
        return {
            "symbol": symbol,
            "latestClosePrice": round(close, 2),
            "latestCloseAsOf": bars.tail(1).index[0].isoformat() if len(bars.index) else "",
            "dayReturnPct": _pct_gap(close, previous_close),
            "return5dPct": _pct_gap(close, price_5d_ago),
            "return21dPct": round(_f(indicators.get("return_21d"), 0.0), 2),
            "return63dPct": round(_f(indicators.get("return_63d"), 0.0), 2),
            "atrPct": round(_f(indicators.get("atr_pct"), 0.0), 2),
            "ma20GapPct": round(_f(indicators.get("ma20_gap"), 0.0), 2),
            "ma50GapPct": round(_f(indicators.get("ma50_gap"), 0.0), 2),
            "ma200GapPct": round(_f(indicators.get("ma200_gap"), 0.0), 2),
            "position52wPct": round(_f(indicators.get("position_52w"), 0.0), 2),
            "rsi": round(_f(indicators.get("rsi"), 50.0), 1),
            "volumeRatio": round(_f(indicators.get("volume_ratio"), 1.0), 2),
            "structureState": _s(structure.get("state")),
            "nearestSupportZone": structure.get("nearestSupportZone") or {},
            "nearestResistanceZone": structure.get("nearestResistanceZone") or {},
            "breakdowns": structure.get("breakdowns") or [],
            "retests": structure.get("retests") or [],
            "movingAverages": structure.get("movingAverages") or {},
            "swingStructure": structure.get("swingStructure") or {},
        }

    def _regime_evidence(
        self,
        *,
        benchmarks: dict[str, dict[str, Any]],
        market_condition: dict[str, Any],
        fear_greed: dict[str, Any],
        macro: dict[str, Any],
        options_market: dict[str, Any],
    ) -> dict[str, Any]:
        trend_evidence: list[str] = []
        stress_evidence: list[str] = []
        support_evidence: list[str] = []
        for symbol, row in benchmarks.items():
            state = _s(row.get("structureState"))
            if state:
                trend_evidence.append(f"{symbol} chart structure: {state}")
            if row.get("ma200GapPct") is not None:
                trend_evidence.append(f"{symbol} vs 200DMA: {_f(row.get('ma200GapPct')):.2f}%")
            if row.get("return5dPct") is not None:
                trend_evidence.append(f"{symbol} 5d return: {_f(row.get('return5dPct')):.2f}%")
            if state in {"breakdown_or_distribution", "downtrend"}:
                stress_evidence.append(f"{symbol} structure is {state}")
            if row.get("nearestSupportZone"):
                support_evidence.append(f"{symbol} nearest support {row.get('nearestSupportZone')}")

        fear_score = fear_greed.get("score")
        if fear_score is not None:
            stress_evidence.append(f"Fear/Greed score {fear_score}")
        market_message = _s(market_condition.get("message"))
        if market_message:
            trend_evidence.append(f"Market condition: {market_message}")

        series = macro.get("series") if isinstance(macro.get("series"), dict) else {}
        vix = series.get("VIXCLS") if isinstance(series.get("VIXCLS"), dict) else {}
        if vix.get("value") is not None:
            stress_evidence.append(f"VIX {vix.get('value')} change {vix.get('change')}")
        dgs10 = series.get("DGS10") if isinstance(series.get("DGS10"), dict) else {}
        spread = series.get("T10Y2Y") if isinstance(series.get("T10Y2Y"), dict) else {}
        rate_evidence = []
        if dgs10.get("value") is not None:
            rate_evidence.append(f"10Y {dgs10.get('value')} change {dgs10.get('change')}")
        if spread.get("value") is not None:
            rate_evidence.append(f"10Y-2Y {spread.get('value')} change {spread.get('change')}")

        ratios = options_market.get("ratios") if isinstance(options_market.get("ratios"), dict) else {}
        options_evidence = []
        for key in ("total_put_call_ratio", "equity_put_call_ratio", "index_put_call_ratio"):
            if ratios.get(key) is not None:
                options_evidence.append(f"{key} {ratios.get(key)}")

        return {
            "trendEvidence": trend_evidence[:12],
            "stressEvidence": stress_evidence[:8],
            "supportEvidence": support_evidence[:6],
            "rateEvidence": rate_evidence,
            "optionsEvidence": options_evidence,
        }

    def _regime_label(self, evidence: dict[str, Any]) -> str:
        trend_text = " | ".join(str(item) for item in evidence.get("trendEvidence", []))
        stress_text = " | ".join(str(item) for item in evidence.get("stressEvidence", []))
        if "breakdown_or_distribution" in stress_text:
            return "market_stress_or_breakdown"
        if "downtrend" in stress_text:
            return "risk_off_downtrend"
        if "bullish_stack" in trend_text or "uptrend" in trend_text:
            return "risk_on_or_constructive"
        return "mixed_or_transition"
