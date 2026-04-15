from __future__ import annotations

from typing import Any


def _f(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if out != out or out in {float("inf"), float("-inf")}:
            return default
        return out
    except Exception:
        return default


def daily_defense_state(
    *,
    ma50_gap_pct: Any,
    ma200_gap_pct: Any,
    return21d_pct: Any,
    vix_close: Any,
    soft_exposure_pct: float,
    hard_exposure_pct: float,
    vix_soft: float,
    vix_hard: float,
    return21d_soft: float,
) -> dict[str, Any]:
    ma50_gap = _f(ma50_gap_pct, 0.0)
    ma200_gap = _f(ma200_gap_pct, 0.0)
    return21d = _f(return21d_pct, 0.0)
    vix = _f(vix_close, 0.0)

    hard_reasons: list[str] = []
    soft_reasons: list[str] = []

    if ma200_gap < 0.0:
        hard_reasons.append("qqq_below_ma200")
    if vix_hard > 0 and vix >= float(vix_hard):
        hard_reasons.append("vix_extreme")

    if ma50_gap < 0.0:
        soft_reasons.append("qqq_below_ma50")
    if return21d <= float(return21d_soft):
        soft_reasons.append("bench_21d_weak")
    if vix_soft > 0 and vix >= float(vix_soft):
        soft_reasons.append("vix_elevated")

    if hard_reasons:
        return {
            "state": "hard_defense",
            "target_exposure_pct": float(hard_exposure_pct),
            "reasons": hard_reasons,
        }
    if soft_reasons:
        return {
            "state": "soft_defense",
            "target_exposure_pct": float(soft_exposure_pct),
            "reasons": soft_reasons,
        }
    return {
        "state": "normal",
        "target_exposure_pct": 100.0,
        "reasons": [],
    }
