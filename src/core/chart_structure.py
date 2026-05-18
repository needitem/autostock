from __future__ import annotations

from typing import Any

import pandas as pd


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


def _round(value: Any, digits: int = 2, default: float = 0.0) -> float:
    return round(_f(value, default), digits)


def _pct_gap(current: float, reference: float) -> float | None:
    if current <= 0 or reference <= 0:
        return None
    return round((current / reference - 1.0) * 100.0, 2)


class ChartStructureCollector:
    """Extract chart structure evidence such as zones, trend, breakdowns and retests."""

    def analyze_daily(
        self,
        symbol: str,
        bars: pd.DataFrame,
        indicators: dict[str, Any],
    ) -> dict[str, Any]:
        if bars is None or bars.empty or not indicators:
            return {"chartState": "structure_unavailable", "chartStructure": {"status": "unavailable"}}

        frame = self._clean_frame(bars)
        if len(frame) < 60:
            return {"chartState": "structure_unavailable", "chartStructure": {"status": "insufficient_history"}}

        latest_price = _f(indicators.get("price"), _f(frame["Close"].iloc[-1]))
        atr_pct = _f(indicators.get("atr_pct"), 0.0)
        zone_pct = self._zone_width_pct(atr_pct)
        swing_points = self._swing_points(frame)
        support_zones = self._zones(
            [
                *[_f(point.get("price")) for point in swing_points.get("lows", [])],
                *[_f(value) for value in indicators.get("support", []) if _f(value) > 0],
            ],
            latest_price=latest_price,
            role="support",
            zone_pct=zone_pct,
        )
        resistance_zones = self._zones(
            [
                *[_f(point.get("price")) for point in swing_points.get("highs", [])],
                *[_f(point.get("price")) for point in swing_points.get("lows", [])],
                *[_f(value) for value in indicators.get("resistance", []) if _f(value) > 0],
            ],
            latest_price=latest_price,
            role="resistance",
            zone_pct=zone_pct,
        )

        moving_averages = self._moving_averages(indicators, latest_price)
        swing_structure = self._swing_structure(swing_points)
        breakdowns = self._breakdown_evidence(frame, indicators, support_zones)
        retests = self._retest_evidence(frame, support_zones, resistance_zones)
        trend_lines = self._trend_lines(swing_points, latest_price)
        state = self._structure_state(
            latest_price=latest_price,
            moving_averages=moving_averages,
            swing_structure=swing_structure,
            breakdowns=breakdowns,
            resistance_zones=resistance_zones,
        )
        nearest_support = support_zones[0] if support_zones else {}
        nearest_resistance = resistance_zones[0] if resistance_zones else {}

        chart_structure = {
            "status": "ok",
            "symbol": _s(symbol).upper(),
            "state": state,
            "latestPrice": round(latest_price, 2),
            "asOf": str(frame.index[-1]) if len(frame.index) else "",
            "movingAverages": moving_averages,
            "swingStructure": swing_structure,
            "supportZones": support_zones[:5],
            "resistanceZones": resistance_zones[:5],
            "nearestSupportZone": nearest_support,
            "nearestResistanceZone": nearest_resistance,
            "trendLines": trend_lines,
            "breakdowns": breakdowns,
            "retests": retests,
            "riskMap": self._risk_map(latest_price, nearest_support, nearest_resistance),
        }
        return {
            "chartState": state,
            "support": [zone["mid"] for zone in support_zones[:3]],
            "resistance": [zone["mid"] for zone in resistance_zones[:3]],
            "nearestSupportZone": nearest_support,
            "nearestResistanceZone": nearest_resistance,
            "chartStructure": chart_structure,
        }

    def _clean_frame(self, bars: pd.DataFrame) -> pd.DataFrame:
        required = ["Open", "High", "Low", "Close", "Volume"]
        frame = bars[[col for col in required if col in bars.columns]].copy()
        for col in required:
            if col not in frame.columns:
                frame[col] = 0.0
        frame = frame.dropna(subset=["Open", "High", "Low", "Close"])
        for col in required:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame.ffill().bfill()

    def _zone_width_pct(self, atr_pct: float) -> float:
        if atr_pct <= 0:
            return 1.0
        return max(0.6, min(3.0, atr_pct * 0.6))

    def _swing_points(self, frame: pd.DataFrame, lookback: int = 180, window: int = 3) -> dict[str, list[dict[str, Any]]]:
        recent = frame.tail(lookback)
        highs = recent["High"].tolist()
        lows = recent["Low"].tolist()
        index = list(recent.index)
        swing_highs: list[dict[str, Any]] = []
        swing_lows: list[dict[str, Any]] = []
        for idx in range(window, len(recent) - window):
            high = _f(highs[idx])
            low = _f(lows[idx])
            high_slice = [_f(value) for value in highs[idx - window : idx + window + 1]]
            low_slice = [_f(value) for value in lows[idx - window : idx + window + 1]]
            if high > 0 and high >= max(high_slice):
                swing_highs.append({"date": str(index[idx]), "price": round(high, 2)})
            if low > 0 and low <= min(low_slice):
                swing_lows.append({"date": str(index[idx]), "price": round(low, 2)})
        return {"highs": swing_highs[-12:], "lows": swing_lows[-12:]}

    def _zones(self, levels: list[float], *, latest_price: float, role: str, zone_pct: float) -> list[dict[str, Any]]:
        clean_levels = sorted({round(_f(level), 2) for level in levels if _f(level) > 0})
        clusters: list[list[float]] = []
        for level in clean_levels:
            if not clusters:
                clusters.append([level])
                continue
            midpoint = sum(clusters[-1]) / len(clusters[-1])
            distance_pct = abs(level / midpoint - 1.0) * 100.0 if midpoint > 0 else 999.0
            if distance_pct <= zone_pct:
                clusters[-1].append(level)
            else:
                clusters.append([level])

        zones: list[dict[str, Any]] = []
        for cluster in clusters:
            mid = round(sum(cluster) / len(cluster), 2)
            if role == "support" and mid >= latest_price:
                continue
            if role == "resistance" and mid <= latest_price:
                continue
            lower = round(min(cluster) * (1.0 - zone_pct / 200.0), 2)
            upper = round(max(cluster) * (1.0 + zone_pct / 200.0), 2)
            zones.append(
                {
                    "role": role,
                    "lower": lower,
                    "upper": upper,
                    "mid": mid,
                    "touchCount": len(cluster),
                    "distancePct": _pct_gap(mid, latest_price),
                }
            )

        if role == "support":
            zones.sort(key=lambda item: abs(_f(item.get("distancePct"), -999.0)))
        else:
            zones.sort(key=lambda item: abs(_f(item.get("distancePct"), 999.0)))
        return zones

    def _moving_averages(self, indicators: dict[str, Any], latest_price: float) -> dict[str, Any]:
        ma20 = _f(indicators.get("ma20"), 0.0)
        ma50 = _f(indicators.get("ma50"), 0.0)
        ma150 = _f(indicators.get("ma150"), 0.0)
        ma200 = _f(indicators.get("ma200"), 0.0)
        stack = "mixed"
        if latest_price > ma20 > ma50 > ma200:
            stack = "bullish_stack"
        elif latest_price < ma20 < ma50 < ma200:
            stack = "bearish_stack"
        elif latest_price > ma200 and ma50 >= ma200:
            stack = "constructive"
        elif latest_price < ma200:
            stack = "below_long_term_trend"
        return {
            "ma20": round(ma20, 2) if ma20 > 0 else None,
            "ma50": round(ma50, 2) if ma50 > 0 else None,
            "ma150": round(ma150, 2) if ma150 > 0 else None,
            "ma200": round(ma200, 2) if ma200 > 0 else None,
            "ma20GapPct": _pct_gap(latest_price, ma20),
            "ma50GapPct": _pct_gap(latest_price, ma50),
            "ma200GapPct": _pct_gap(latest_price, ma200),
            "ma200Slope30d": _round(indicators.get("ma200_slope_30d"), 4),
            "ma200TrendUp30d": bool(indicators.get("ma200_trend_up_30d")),
            "stack": stack,
        }

    def _swing_structure(self, swing_points: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        highs = swing_points.get("highs", [])
        lows = swing_points.get("lows", [])
        recent_highs = highs[-3:]
        recent_lows = lows[-3:]
        high_pattern = "insufficient"
        low_pattern = "insufficient"
        if len(recent_highs) >= 2:
            high_pattern = "higher_highs" if _f(recent_highs[-1].get("price")) > _f(recent_highs[-2].get("price")) else "lower_highs"
        if len(recent_lows) >= 2:
            low_pattern = "higher_lows" if _f(recent_lows[-1].get("price")) > _f(recent_lows[-2].get("price")) else "lower_lows"
        if high_pattern == "higher_highs" and low_pattern == "higher_lows":
            phase = "advancing"
        elif high_pattern == "lower_highs" and low_pattern == "lower_lows":
            phase = "declining"
        elif high_pattern == "lower_highs" and low_pattern == "higher_lows":
            phase = "compression"
        else:
            phase = "range_or_transition"
        return {
            "phase": phase,
            "highPattern": high_pattern,
            "lowPattern": low_pattern,
            "recentHighs": recent_highs,
            "recentLows": recent_lows,
        }

    def _breakdown_evidence(
        self,
        frame: pd.DataFrame,
        indicators: dict[str, Any],
        support_zones: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        latest_close = _f(frame["Close"].iloc[-1])
        previous_close = _f(frame["Close"].iloc[-2], latest_close) if len(frame) >= 2 else latest_close
        evidence: list[dict[str, Any]] = []
        for label in ("ma20", "ma50", "ma200"):
            ma = _f(indicators.get(label), 0.0)
            if ma > 0 and previous_close >= ma and latest_close < ma:
                evidence.append({"type": "moving_average_break", "level": label, "price": round(ma, 2)})
            elif ma > 0 and latest_close < ma:
                evidence.append({"type": "below_moving_average", "level": label, "price": round(ma, 2)})
        for zone in support_zones[:3]:
            lower = _f(zone.get("lower"), 0.0)
            if lower > 0 and previous_close >= lower and latest_close < lower:
                evidence.append({"type": "support_zone_break", "zone": zone})
        return evidence[:6]

    def _retest_evidence(
        self,
        frame: pd.DataFrame,
        support_zones: list[dict[str, Any]],
        resistance_zones: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        recent = frame.tail(8)
        if recent.empty:
            return []
        latest_close = _f(recent["Close"].iloc[-1])
        recent_high = _f(recent["High"].max())
        recent_low = _f(recent["Low"].min())
        evidence: list[dict[str, Any]] = []
        for zone in support_zones[:2]:
            upper = _f(zone.get("upper"))
            if upper > 0 and recent_low <= upper and latest_close >= upper:
                evidence.append({"type": "support_hold_retest", "zone": zone})
        for zone in resistance_zones[:2]:
            lower = _f(zone.get("lower"))
            if lower > 0 and recent_high >= lower and latest_close < lower:
                evidence.append({"type": "resistance_rejection_retest", "zone": zone})
        return evidence[:4]

    def _trend_lines(self, swing_points: dict[str, list[dict[str, Any]]], latest_price: float) -> dict[str, Any]:
        return {
            "risingSupport": self._line_from_points(swing_points.get("lows", [])[-2:], latest_price),
            "fallingResistance": self._line_from_points(swing_points.get("highs", [])[-2:], latest_price),
        }

    def _line_from_points(self, points: list[dict[str, Any]], latest_price: float) -> dict[str, Any]:
        if len(points) < 2:
            return {}
        p1 = _f(points[0].get("price"))
        p2 = _f(points[1].get("price"))
        slope_pct = _pct_gap(p2, p1)
        return {
            "from": points[0],
            "to": points[1],
            "slopePct": slope_pct,
            "latestDistancePct": _pct_gap(latest_price, p2),
        }

    def _structure_state(
        self,
        *,
        latest_price: float,
        moving_averages: dict[str, Any],
        swing_structure: dict[str, Any],
        breakdowns: list[dict[str, Any]],
        resistance_zones: list[dict[str, Any]],
    ) -> str:
        ma_stack = _s(moving_averages.get("stack"))
        phase = _s(swing_structure.get("phase"))
        hard_breaks = {item.get("type") for item in breakdowns}
        nearest_resistance = resistance_zones[0] if resistance_zones else {}
        if "support_zone_break" in hard_breaks or (
            ma_stack == "below_long_term_trend" and phase in {"declining", "range_or_transition"}
        ):
            return "breakdown_or_distribution"
        if ma_stack == "bearish_stack" or phase == "declining":
            return "downtrend"
        if ma_stack in {"bullish_stack", "constructive"} and phase == "advancing":
            return "uptrend"
        if ma_stack in {"bullish_stack", "constructive"} and phase in {"compression", "range_or_transition"}:
            return "constructive_pullback_or_base"
        resistance_mid = _f(nearest_resistance.get("mid"))
        if resistance_mid > 0 and latest_price > resistance_mid:
            return "breakout_attempt"
        return "range_or_transition"

    def _risk_map(
        self,
        latest_price: float,
        nearest_support: dict[str, Any],
        nearest_resistance: dict[str, Any],
    ) -> dict[str, Any]:
        support_mid = _f(nearest_support.get("mid"))
        resistance_mid = _f(nearest_resistance.get("mid"))
        return {
            "nearestSupportPct": _pct_gap(support_mid, latest_price),
            "nearestResistancePct": _pct_gap(resistance_mid, latest_price),
            "supportInvalidation": nearest_support,
            "firstResistance": nearest_resistance,
        }
