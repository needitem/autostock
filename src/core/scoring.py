"""
Scoring engine for stock recommendations.

Score output contract is kept backward-compatible:
- total_score, grade, recommendation
- factor, financial, risk, confidence
"""

from __future__ import annotations

from typing import Any


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().upper() not in {"", "-", "N/A", "NONE", "NULL", "NAN"}
    return True


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
        return float(value)
    except Exception:
        return default


def _pct(value: Any, default: float = 0.0) -> float:
    out = _num(value, default)
    # normalize ratio-like values (0.15 -> 15)
    if abs(out) <= 1.5:
        return out * 100
    return out


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _has_numeric(value: Any) -> bool:
    if not _has_value(value):
        return False
    try:
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
        float(value)
        return True
    except Exception:
        return False


def _expectation_component(data: dict[str, Any]) -> tuple[float, int]:
    """
    Score forward-looking expectation quality.

    Returns:
        (score, check_count)
    """
    score = 50.0
    checks = 0
    analyst_count = _num(data.get("analyst_count"), 0)
    recommendation_mean = _num(data.get("recommendation_mean"), 0)

    target_upside = _num(data.get("target_upside_pct"), 0)
    if _has_numeric(data.get("target_upside_pct")) and analyst_count > 0:
        checks += 1
        if target_upside >= 20:
            score += 18
        elif target_upside >= 10:
            score += 12
        elif target_upside >= 5:
            score += 7
        elif target_upside <= -8:
            score -= 20
        elif target_upside < 0:
            score -= 10

    if recommendation_mean > 0 and analyst_count > 0:
        checks += 1
        if recommendation_mean <= 1.8:
            score += 18
        elif recommendation_mean <= 2.2:
            score += 12
        elif recommendation_mean <= 2.8:
            score += 4
        elif recommendation_mean <= 3.3:
            score -= 10
        else:
            score -= 18

    if _has_numeric(data.get("analyst_count")) and analyst_count > 0:
        checks += 1
        if analyst_count >= 20:
            score += 10
        elif analyst_count >= 10:
            score += 6
        elif 0 < analyst_count < 4:
            score -= 6

    forward_eps = _num(data.get("forward_eps"), 0)
    trailing_eps = _num(data.get("trailing_eps"), 0)
    has_eps_context = abs(forward_eps) > 0.0001 or abs(trailing_eps) > 0.0001
    forward_eps_growth = _num(data.get("forward_eps_growth_pct"), 0)
    if _has_numeric(data.get("forward_eps_growth_pct")) and has_eps_context:
        checks += 1
        if forward_eps_growth >= 20:
            score += 16
        elif forward_eps_growth >= 10:
            score += 10
        elif forward_eps_growth >= 0:
            score += 4
        elif forward_eps_growth <= -20:
            score -= 18
        else:
            score -= 10

    return _clamp(score), checks


def calculate_annual_edge_score(data: dict[str, Any]) -> dict[str, Any]:
    """
    Estimate 6-12 month edge using a contrarian/reversion-aware profile.

    Rationale:
    - Current swing setup is short-horizon friendly.
    - For annual horizon, heavily extended winners often mean-revert,
      while deeply lagging names can rebound if risk isn't extreme.
    """
    score = 50.0

    ret_63d = _num(data.get("return_63d"), 0)
    rs_63d = _num(data.get("relative_strength_63d"), 0)
    rsi = _num(data.get("rsi"), 50)
    ma50_gap = _num(data.get("ma50_gap"), 0)
    pos_52w = _num(data.get("position_52w"), 50)
    atr_pct = _num(data.get("atr_pct"), 0)
    change_5d = _num(data.get("change_5d"), 0)

    # Contrarian core: favor laggards over recent overextension.
    if ret_63d <= -30:
        score += 22
    elif ret_63d <= -18:
        score += 16
    elif ret_63d <= -8:
        score += 10
    elif ret_63d <= 5:
        score += 4
    elif ret_63d <= 18:
        score -= 8
    else:
        score -= 16

    if rs_63d <= -20:
        score += 14
    elif rs_63d <= -10:
        score += 8
    elif rs_63d <= -3:
        score += 4
    elif rs_63d >= 12:
        score -= 10
    elif rs_63d >= 6:
        score -= 5

    if 38 <= rsi <= 54:
        score += 12
    elif 30 <= rsi < 38:
        score += 5
    elif rsi > 68:
        score -= 14
    elif rsi > 60:
        score -= 7
    elif rsi < 24:
        score -= 8

    if -10 <= ma50_gap <= 2:
        score += 10
    elif 2 < ma50_gap <= 10:
        score -= 5
    elif ma50_gap > 10:
        score -= 10
    elif ma50_gap < -18:
        score -= 6

    if 25 <= pos_52w <= 72:
        score += 8
    elif pos_52w > 90:
        score -= 12
    elif pos_52w < 8:
        score -= 6

    if 2 <= atr_pct <= 8:
        score += 4
    elif atr_pct > 10:
        score -= 6

    if abs(change_5d) >= 15:
        score -= 4

    # Forward/fundamental overlays when available.
    analyst_count = _num(data.get("analyst_count"), 0)
    recommendation_mean = _num(data.get("recommendation_mean"), 0)
    if analyst_count >= 8 and recommendation_mean > 0:
        if recommendation_mean <= 2.2:
            score += 6
        elif recommendation_mean >= 3.3:
            score -= 8

    if _has_numeric(data.get("forward_eps_growth_pct")):
        fwd_eps_growth = _num(data.get("forward_eps_growth_pct"), 0)
        if fwd_eps_growth >= 10:
            score += 6
        elif fwd_eps_growth < 0:
            score -= 6

    if _has_numeric(data.get("target_upside_pct")) and analyst_count > 0:
        upside = _num(data.get("target_upside_pct"), 0)
        if upside >= 10:
            score += 5
        elif upside <= -5:
            score -= 6

    score = round(_clamp(score, 0.0, 95.0), 1)
    if score >= 65:
        stance = "positive"
    elif score <= 40:
        stance = "negative"
    else:
        stance = "neutral"
    return {"score": score, "stance": stance}


def _coverage_ratio(data: dict[str, Any], keys: list[str]) -> float:
    if not keys:
        return 0.0
    valid = sum(1 for key in keys if _has_value(data.get(key)))
    return valid / len(keys)


def _score_confidence(data: dict[str, Any], financial_coverage: float) -> dict[str, Any]:
    technical_keys = [
        "rsi",
        "bb_position",
        "ma50_gap",
        "position_52w",
        "change_5d",
        "adx",
        "volume_ratio",
        "atr_pct",
        "relative_strength_63d",
    ]
    technical_coverage = _coverage_ratio(data, technical_keys)

    confidence = technical_coverage * 0.6 + financial_coverage * 0.4
    confidence_score = round(_clamp(confidence * 100), 1)

    if confidence_score >= 80:
        label = "높음"
    elif confidence_score >= 60:
        label = "보통"
    else:
        label = "낮음"

    return {
        "score": confidence_score,
        "label": label,
        "technical_coverage": round(technical_coverage, 2),
        "financial_coverage": round(financial_coverage, 2),
    }


def calculate_score(data: dict[str, Any]) -> dict[str, Any]:
    """Calculate final recommendation score (0..100)."""
    factor = calculate_factor_score(data)
    financial = calculate_financial_score(data)
    risk = calculate_risk_score(data)
    annual_edge = calculate_annual_edge_score(data)

    components = [
        {"score": factor["score"], "weight": 0.47, "available": True},
        {"score": financial["score"], "weight": 0.25, "available": financial.get("coverage", 0) > 0},
        {"score": 100 - risk["score"], "weight": 0.20, "available": True},
        {"score": annual_edge["score"], "weight": 0.08, "available": True},
    ]
    active_weight = sum(item["weight"] for item in components if item["available"])
    if active_weight <= 0:
        total = 50.0
    else:
        total = sum(item["score"] * (item["weight"] / active_weight) for item in components if item["available"])
    total = round(_clamp(total), 1)

    confidence = _score_confidence(data, financial.get("coverage", 0))
    grade, recommendation = _grade_and_reco(total, risk["score"])
    if confidence["score"] < 60 and grade in {"A", "B"}:
        recommendation = f"{recommendation} (데이터 보강 필요)"

    return {
        "symbol": data.get("symbol", ""),
        "total_score": total,
        "grade": grade,
        "recommendation": recommendation,
        "factor": factor,
        "financial": financial,
        "risk": risk,
        "confidence": confidence,
        "annual_edge": annual_edge,
    }


def _grade_and_reco(total: float, risk_score: float) -> tuple[str, str]:
    if total >= 75:
        grade, reco = "A", "적극 매수"
    elif total >= 65:
        grade, reco = "B", "매수"
    elif total >= 52:
        grade, reco = "C", "관망"
    elif total >= 42:
        grade, reco = "D", "매도 고려"
    else:
        grade, reco = "F", "매도"

    if risk_score >= 70 and grade in {"A", "B"}:
        reco = "관망 (리스크 높음)"
        if grade == "A":
            grade = "B"
    return grade, reco


def calculate_factor_score(data: dict[str, Any]) -> dict[str, Any]:
    """Technical+valuation blend focused on recommendation ranking."""
    scores: dict[str, float] = {}

    # Trend / momentum
    trend = 50.0
    ma50_gap = _num(data.get("ma50_gap"), 0)
    ma200_gap = _num(data.get("ma200_gap"), 0)
    pos_52w = _num(data.get("position_52w"), 50)
    rsi = _num(data.get("rsi"), 50)
    adx = _num(data.get("adx"), 20)
    change_5d = _num(data.get("change_5d"), 0)

    if 0 <= ma50_gap <= 12:
        trend += 15
    elif ma50_gap > 20:
        trend -= 10
    elif ma50_gap < -10:
        trend -= 15

    if ma200_gap > 0:
        trend += 8
    elif ma200_gap < -8:
        trend -= 10

    if 55 <= pos_52w <= 88:
        trend += 12
    elif pos_52w >= 95:
        trend -= 10
    elif pos_52w <= 20:
        trend -= 8

    if 45 <= rsi <= 62:
        trend += 8
    elif rsi > 72:
        trend -= 14
    elif rsi < 28:
        trend -= 12

    if adx >= 25:
        trend += 5
    if abs(change_5d) > 15:
        trend -= 10

    scores["momentum"] = _clamp(trend)

    # Relative strength vs benchmark (QQQ by default)
    rs = 50.0
    rs_21d = _num(data.get("relative_strength_21d"), 0)
    rs_63d = _num(data.get("relative_strength_63d"), 0)
    rs_combo = rs_21d * 0.4 + rs_63d * 0.6
    ret_63d = _num(data.get("return_63d"), 0)

    if rs_combo >= 12:
        rs += 22
    elif rs_combo >= 7:
        rs += 14
    elif rs_combo >= 3:
        rs += 8
    elif rs_combo <= -12:
        rs -= 22
    elif rs_combo <= -7:
        rs -= 14
    elif rs_combo <= -3:
        rs -= 8

    if rs_63d > 0 and ret_63d > 0:
        rs += 4
    elif rs_63d < 0 and ret_63d < 0:
        rs -= 4
    scores["relative_strength"] = _clamp(rs)

    # Value
    value = 50.0
    pe = _num(data.get("pe"), 0)
    peg = _num(data.get("peg"), 0)
    pb = _num(data.get("pb"), 0)

    if 0 < pe <= 15:
        value += 18
    elif 15 < pe <= 25:
        value += 10
    elif pe > 40:
        value -= 14

    if 0 < peg <= 1.2:
        value += 15
    elif 1.2 < peg <= 2.0:
        value += 8
    elif peg > 3.0:
        value -= 12

    if 0 < pb <= 4:
        value += 7
    elif pb > 10:
        value -= 8

    scores["value"] = _clamp(value)

    # Quality
    quality = 50.0
    roe = _pct(data.get("roe"), 0)
    margin = _pct(data.get("profit_margin"), 0)
    rev_growth = _pct(data.get("revenue_growth"), 0)
    earn_growth = _pct(data.get("earnings_growth"), 0)
    debt = _num(data.get("debt_to_equity"), 0)

    if roe >= 20:
        quality += 18
    elif roe >= 12:
        quality += 10
    elif roe < 0:
        quality -= 15

    if margin >= 18:
        quality += 10
    elif margin >= 8:
        quality += 6
    elif margin < 0:
        quality -= 10

    if rev_growth >= 15:
        quality += 8
    elif rev_growth < -5:
        quality -= 8

    if earn_growth >= 15:
        quality += 8
    elif earn_growth < -10:
        quality -= 10

    if debt > 0:
        if debt <= 80:
            quality += 6
        elif debt >= 250:
            quality -= 10

    scores["quality"] = _clamp(quality)

    # Stability
    stability = 50.0
    bb_pos = _num(data.get("bb_position"), 50)
    vol_ratio = _num(data.get("volume_ratio"), 1)

    if 30 <= bb_pos <= 70:
        stability += 12
    elif bb_pos < 10 or bb_pos > 90:
        stability -= 12

    if abs(change_5d) <= 3:
        stability += 12
    elif abs(change_5d) >= 12:
        stability -= 10

    if 0.8 <= vol_ratio <= 2.2:
        stability += 6
    elif vol_ratio > 4:
        stability -= 8

    scores["stability"] = _clamp(stability)

    total = (
        scores["momentum"] * 0.30
        + scores["relative_strength"] * 0.20
        + scores["value"] * 0.20
        + scores["quality"] * 0.20
        + scores["stability"] * 0.10
    )
    return {"score": round(_clamp(total), 1), "details": scores}


def calculate_financial_score(data: dict[str, Any]) -> dict[str, Any]:
    """Financial health score with explicit data-coverage output."""
    scores: dict[str, float] = {}
    checks = {"profitability": 0, "growth": 0, "health": 0, "expectation": 0}

    # Profitability
    profitability = 50.0
    roe = _pct(data.get("roe"), 0)
    if _has_value(data.get("roe")):
        checks["profitability"] += 1
        if roe >= 20:
            profitability += 25
        elif roe >= 15:
            profitability += 15
        elif roe < 0:
            profitability -= 20

    margin = _pct(data.get("profit_margin"), 0)
    if _has_value(data.get("profit_margin")):
        checks["profitability"] += 1
        if margin >= 20:
            profitability += 15
        elif margin >= 10:
            profitability += 10
        elif margin < 0:
            profitability -= 10
    scores["profitability"] = _clamp(profitability)

    # Growth
    growth = 50.0
    rev_growth = _pct(data.get("revenue_growth"), 0)
    if _has_value(data.get("revenue_growth")):
        checks["growth"] += 1
        if rev_growth >= 20:
            growth += 20
        elif rev_growth >= 10:
            growth += 10
        elif rev_growth < 0:
            growth -= 10

    earn_growth = _pct(data.get("earnings_growth"), 0)
    if _has_value(data.get("earnings_growth")):
        checks["growth"] += 1
        if earn_growth >= 20:
            growth += 20
        elif earn_growth >= 10:
            growth += 10
        elif earn_growth < -10:
            growth -= 15
    scores["growth"] = _clamp(growth)

    # Balance-sheet health
    health = 50.0
    current_ratio = _num(data.get("current_ratio"), 0)
    if _has_value(data.get("current_ratio")):
        checks["health"] += 1
        if current_ratio >= 2:
            health += 15
        elif current_ratio >= 1.5:
            health += 10
        elif current_ratio < 1:
            health -= 10

    fcf = _num(data.get("free_cash_flow"), 0)
    if _has_value(data.get("free_cash_flow")):
        checks["health"] += 1
        if fcf > 0:
            health += 15
        else:
            health -= 10
    scores["health"] = _clamp(health)

    expectation, expectation_checks = _expectation_component(data)
    checks["expectation"] = expectation_checks
    scores["expectation"] = expectation

    total = (
        scores["profitability"] * 0.30
        + scores["growth"] * 0.24
        + scores["health"] * 0.20
        + scores["expectation"] * 0.26
    )
    coverage = sum(checks.values()) / 10
    return {"score": round(_clamp(total), 1), "details": scores, "coverage": round(coverage, 2)}


def calculate_risk_score(data: dict[str, Any]) -> dict[str, Any]:
    """Risk score (0..100, higher means riskier)."""
    risk = 0.0
    warnings: list[str] = []

    rsi = _num(data.get("rsi"), 50)
    bb_pos = _num(data.get("bb_position"), 50)
    pos_52w = _num(data.get("position_52w"), 50)
    ma50_gap = _num(data.get("ma50_gap"), 0)
    change_5d = _num(data.get("change_5d"), 0)
    vol_ratio = _num(data.get("volume_ratio"), 1)
    atr_pct = _num(data.get("atr_pct"), 0)
    beta = _num(data.get("beta"), 1)
    rs_63d = _num(data.get("relative_strength_63d"), 0)
    days_to_earnings = int(_num(data.get("days_to_earnings"), 999))
    avg_dollar_volume_m = _num(data.get("avg_dollar_volume_m"), 0)
    target_upside_pct = _num(data.get("target_upside_pct"), 0)
    recommendation_mean = _num(data.get("recommendation_mean"), 0)
    analyst_count = _num(data.get("analyst_count"), 0)
    forward_eps_growth = _num(data.get("forward_eps_growth_pct"), 0)

    if rsi >= 70:
        risk += 25
        warnings.append(f"RSI {rsi:.0f} 과매수")
    elif rsi >= 62:
        risk += 10

    if rsi <= 30:
        risk += 15
        warnings.append(f"RSI {rsi:.0f} 과매도")

    if bb_pos >= 95:
        risk += 20
        warnings.append("볼린저밴드 상단 과열")
    elif bb_pos >= 80:
        risk += 10

    if pos_52w >= 95:
        risk += 18
        warnings.append("52주 최고가 근접")
    elif pos_52w <= 10:
        risk += 14
        warnings.append("52주 최저가 근접")

    if ma50_gap >= 20:
        risk += 18
        warnings.append(f"50일선 대비 +{ma50_gap:.0f}% 이격")
    elif ma50_gap <= -20:
        risk += 22
        warnings.append(f"50일선 대비 {ma50_gap:.0f}% 이격")

    if change_5d >= 20:
        risk += 14
        warnings.append(f"5일 +{change_5d:.0f}% 급등")
    elif change_5d <= -15:
        risk += 18
        warnings.append(f"5일 {change_5d:.0f}% 급락")

    if vol_ratio >= 4:
        risk += 8
        warnings.append("거래량 급증")

    if atr_pct >= 8:
        risk += 18
        warnings.append(f"ATR 변동성 높음 ({atr_pct:.1f}%)")
    elif atr_pct >= 6:
        risk += 10

    if beta >= 2.0:
        risk += 10
        warnings.append(f"베타 높음 ({beta:.2f})")
    elif beta >= 1.5:
        risk += 5

    if rs_63d <= -15:
        risk += 12
        warnings.append(f"QQQ 대비 3개월 상대 성과 약세 ({rs_63d:.1f}%p)")
    elif rs_63d <= -8:
        risk += 7
    elif rs_63d >= 10:
        risk -= 4

    if 0 <= days_to_earnings <= 1:
        risk += 18
        warnings.append("실적 발표 임박")
    elif 2 <= days_to_earnings <= 3:
        risk += 12
        warnings.append("실적 발표 3일 이내")
    elif 4 <= days_to_earnings <= 7:
        risk += 6

    if 0 < avg_dollar_volume_m < 3:
        risk += 12
        warnings.append(f"유동성 낮음 ({avg_dollar_volume_m:.1f}M)")
    elif 3 <= avg_dollar_volume_m < 8:
        risk += 6

    if _has_numeric(data.get("target_upside_pct")):
        if target_upside_pct <= -8:
            risk += 14
            warnings.append("Street target implies downside")
        elif target_upside_pct < 0:
            risk += 8
        elif target_upside_pct >= 20:
            risk -= 3

    if recommendation_mean > 0:
        if recommendation_mean >= 3.3:
            risk += 14
            warnings.append(f"Weak analyst consensus ({recommendation_mean:.2f})")
        elif recommendation_mean >= 2.8:
            risk += 8
        elif recommendation_mean <= 2.0 and analyst_count >= 8:
            risk -= 4
        if 0 < analyst_count < 4:
            risk += 4

    if _has_numeric(data.get("forward_eps_growth_pct")):
        if forward_eps_growth <= -20:
            risk += 14
            warnings.append("Forward EPS trend deteriorating")
        elif forward_eps_growth < 0:
            risk += 8
        elif forward_eps_growth >= 15:
            risk -= 3

    risk = round(_clamp(risk), 1)
    if risk >= 50:
        grade = "🔴 고위험"
    elif risk >= 30:
        grade = "🟡 주의"
    else:
        grade = "🟢 양호"
    return {"score": risk, "grade": grade, "warnings": warnings[:4]}


__all__ = [
    "calculate_score",
    "calculate_annual_edge_score",
    "calculate_factor_score",
    "calculate_financial_score",
    "calculate_risk_score",
]
