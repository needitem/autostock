"""
Signal generation and bulk scanning.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from core.indicators import calculate_indicators
from core.stock_data import get_market_condition, get_stock_data, get_stock_info


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


_SCORING_FINANCIAL_KEYS = (
    "pe",
    "peg",
    "pb",
    "roe",
    "profit_margin",
    "revenue_growth",
    "earnings_growth",
    "debt_to_equity",
    "current_ratio",
    "free_cash_flow",
    "beta",
    "days_to_earnings",
    "avg_volume",
    "market_cap",
    "sector",
)


def _build_score_payload(
    symbol: str,
    ind: dict[str, Any],
    fundamentals: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"symbol": symbol, **ind}
    if fundamentals:
        for key in _SCORING_FINANCIAL_KEYS:
            if key in fundamentals:
                payload[key] = fundamentals.get(key)
    return payload


def _market_profile(status: str) -> dict[str, float]:
    profile = {
        "risk_budget_pct": 0.60,
        "max_position_pct": 12.0,
        "min_rr2": 1.25,
        "max_risk_pct": 9.0,
    }
    if status == "bullish":
        profile.update(
            {
                "risk_budget_pct": 0.95,
                "max_position_pct": 18.0,
                "min_rr2": 1.15,
                "max_risk_pct": 10.5,
            }
        )
    elif status == "neutral":
        profile.update(
            {
                "risk_budget_pct": 0.75,
                "max_position_pct": 14.0,
                "min_rr2": 1.25,
                "max_risk_pct": 9.5,
            }
        )
    elif status == "bearish":
        profile.update(
            {
                "risk_budget_pct": 0.45,
                "max_position_pct": 9.0,
                "min_rr2": 1.35,
                "max_risk_pct": 8.0,
            }
        )
    return profile


def _liquidity_profile(ind: dict[str, Any]) -> dict[str, Any]:
    price = max(_safe_float(ind.get("price"), 0.0), 0.0)
    volume = max(_safe_float(ind.get("volume"), 0.0), 0.0)
    volume_avg = max(_safe_float(ind.get("volume_avg"), 0.0), 0.0)

    avg_dollar_volume = price * volume_avg
    today_dollar_volume = price * volume
    adv_m = avg_dollar_volume / 1_000_000 if avg_dollar_volume > 0 else 0.0

    if adv_m >= 120:
        score, tier = 95.0, "institutional"
    elif adv_m >= 40:
        score, tier = 85.0, "high"
    elif adv_m >= 15:
        score, tier = 72.0, "medium"
    elif adv_m >= 6:
        score, tier = 58.0, "thin"
    elif adv_m > 0:
        score, tier = 40.0, "illiquid"
    else:
        score, tier = 25.0, "unknown"

    tradeable = adv_m >= 6 and volume_avg >= 200_000
    return {
        "score": round(score, 1),
        "tier": tier,
        "avg_dollar_volume": round(avg_dollar_volume, 2),
        "avg_dollar_volume_m": round(adv_m, 2),
        "today_dollar_volume_m": round(today_dollar_volume / 1_000_000, 2),
        "is_tradeable": tradeable,
    }


def _event_risk_profile(fundamentals: dict[str, Any] | None = None) -> dict[str, Any]:
    if not fundamentals:
        return {"days_to_earnings": None, "level": "unknown", "penalty": 0.0}

    raw = fundamentals.get("days_to_earnings")
    if raw in {None, ""}:
        return {"days_to_earnings": None, "level": "unknown", "penalty": 0.0}

    days = int(_safe_float(raw, 9999))
    if days < 0:
        return {"days_to_earnings": days, "level": "post_earnings", "penalty": 0.0}
    if days <= 1:
        return {"days_to_earnings": days, "level": "imminent", "penalty": 12.0}
    if days <= 3:
        return {"days_to_earnings": days, "level": "near", "penalty": 8.0}
    if days <= 7:
        return {"days_to_earnings": days, "level": "upcoming", "penalty": 4.0}
    return {"days_to_earnings": days, "level": "distant", "penalty": 0.0}


def _allocation_label(position_pct: float) -> str:
    if position_pct <= 0:
        return "skip"
    if position_pct < 3:
        return "pilot"
    if position_pct < 7:
        return "small"
    if position_pct < 12:
        return "standard"
    return "aggressive"


def _execution_profile(
    trade_plan: dict[str, Any],
    score: dict[str, Any],
    market_profile: dict[str, float],
    liquidity: dict[str, Any],
    event_risk: dict[str, Any],
) -> dict[str, Any]:
    risk_pct = _safe_float(trade_plan.get("risk_reward", {}).get("risk_pct"), 0.0)
    setup_score = _safe_float(trade_plan.get("positioning", {}).get("setup_score"), 50.0)
    confidence = _safe_float(score.get("confidence", {}).get("score"), 60.0)
    risk_score = _safe_float(score.get("risk", {}).get("score"), 50.0)
    liquidity_score = _safe_float(liquidity.get("score"), 60.0)
    event_penalty = _safe_float(event_risk.get("penalty"), 0.0)

    conviction = _clamp(setup_score * 0.5 + confidence * 0.3 + (100 - risk_score) * 0.2)
    conviction_mult = _clamp(0.72 + conviction / 180, 0.65, 1.28)
    liquidity_mult = 1.0 if liquidity_score >= 60 else 0.82 if liquidity_score >= 50 else 0.65
    event_mult = 1.0 if event_penalty == 0 else 0.88 if event_penalty <= 4 else 0.72 if event_penalty <= 8 else 0.55

    risk_budget_pct = market_profile["risk_budget_pct"] * conviction_mult * event_mult
    if risk_pct <= 0.25:
        raw_position_pct = 0.0
    else:
        raw_position_pct = (risk_budget_pct / risk_pct) * 100.0

    capped_position_pct = min(market_profile["max_position_pct"], raw_position_pct)
    position_pct = _clamp(capped_position_pct * liquidity_mult, 0.0, market_profile["max_position_pct"])

    return {
        "risk_budget_pct": round(risk_budget_pct, 2),
        "max_position_pct": round(market_profile["max_position_pct"], 2),
        "position_pct": round(position_pct, 2),
        "allocation": _allocation_label(position_pct),
        "conviction": round(conviction, 1),
    }


def _build_trade_plan(
    ind: dict[str, Any],
    score: dict[str, Any],
    market_status: str,
    liquidity: dict[str, Any],
    event_risk: dict[str, Any],
) -> dict[str, Any]:
    """
    Build an actionable trade plan.

    Goal: prefer right-knee entries (pullback in up-trend), avoid late chase,
    and size position by volatility/liquidity/regime.
    """
    price = _safe_float(ind.get("price"), 0.0)
    atr = max(_safe_float(ind.get("atr"), 0.0), price * 0.01 if price > 0 else 0.5)
    ma50_gap = _safe_float(ind.get("ma50_gap"), 0.0)
    rsi = _safe_float(ind.get("rsi"), 50.0)
    pos_52w = _safe_float(ind.get("position_52w"), 50.0)
    atr_pct = abs(_safe_float(ind.get("atr_pct"), 0.0))
    change_5d = abs(_safe_float(ind.get("change_5d"), 0.0))
    market_profile = _market_profile(market_status)

    supports = [float(x) for x in (ind.get("support") or [])[:3] if _safe_float(x, -1) > 0]
    resistances = [float(x) for x in (ind.get("resistance") or [])[:3] if _safe_float(x, -1) > 0]

    s1 = supports[0] if len(supports) > 0 else max(0.01, price - atr * 1.1)
    s2 = supports[1] if len(supports) > 1 else max(0.01, s1 - atr * 0.8)
    r1 = resistances[0] if len(resistances) > 0 else price + atr * 1.8
    r2 = resistances[1] if len(resistances) > 1 else r1 + atr * 1.2

    entry1 = min(price * 0.998, s1 * 1.005)
    entry2 = s1
    entry3 = min(s2, entry2 - atr * 0.7)
    if entry3 <= 0:
        entry3 = max(0.01, entry2 * 0.97)

    stop_a = entry3 - atr * 0.6
    stop_b = entry2 * 0.92
    stop_loss = max(0.01, min(stop_a, stop_b))

    risk = max(0.01, entry2 - stop_loss)
    target1 = max(r1, entry2 + risk * 1.4)
    target2 = max(r2, entry2 + risk * 2.2)

    rr1 = (target1 - entry2) / risk
    rr2 = (target2 - entry2) / risk
    risk_pct = risk / max(0.01, entry2) * 100
    room_to_r1_pct = ((r1 - price) / max(0.01, price)) * 100 if price > 0 else 0

    rr_score = _clamp((rr2 / 2.6) * 100)
    risk_score = _clamp(100 - (risk_pct / 10) * 100)
    trend_score = _clamp(72 - abs(ma50_gap - 4) * 6)
    rsi_score = 100 if 42 <= rsi <= 60 else 75 if 35 <= rsi <= 67 else 40
    location_score = _clamp(100 - abs(pos_52w - 65) * 2.2)
    liquidity_score = _safe_float(liquidity.get("score"), 55.0)

    volatility_score = 100.0
    if atr_pct >= 8:
        volatility_score -= 35
    elif atr_pct >= 6:
        volatility_score -= 22
    elif atr_pct <= 1:
        volatility_score -= 8
    if change_5d >= 12:
        volatility_score -= 18
    elif change_5d >= 8:
        volatility_score -= 10
    volatility_score = _clamp(volatility_score)

    event_penalty = _safe_float(event_risk.get("penalty"), 0.0)
    setup_score = _clamp(
        rr_score * 0.30
        + risk_score * 0.18
        + trend_score * 0.12
        + rsi_score * 0.10
        + location_score * 0.10
        + volatility_score * 0.10
        + liquidity_score * 0.10
        - event_penalty * 1.8
    )

    near_resistance = r1 > 0 and price >= r1 * 0.985
    deep_pullback = price <= entry1 * 1.01
    if near_resistance or (pos_52w >= 84 and rsi >= 64):
        stage = "right_shoulder"
    elif deep_pullback and rr2 >= 1.4 and ma50_gap > -3:
        stage = "right_knee"
    else:
        stage = "mid_trend"

    max_risk_gate = market_profile["max_risk_pct"]
    min_rr_gate = market_profile["min_rr2"]
    risk_ceiling = 62 if market_status == "bearish" else 68
    tradeable = (
        liquidity.get("is_tradeable", False)
        and rr2 >= min_rr_gate
        and risk_pct <= max_risk_gate
        and setup_score >= (60 if market_status == "bearish" else 55)
        and stage != "right_shoulder"
        and _safe_float(score.get("risk", {}).get("score"), 50) < risk_ceiling
        and event_penalty < 12
    )

    trade_plan = {
        "entry": {
            "buy1": round(entry1, 2),
            "buy2": round(entry2, 2),
            "buy3": round(entry3, 2),
            "split": [0.5, 0.3, 0.2],
        },
        "stop_loss": round(stop_loss, 2),
        "targets": {"target1": round(target1, 2), "target2": round(target2, 2)},
        "risk_reward": {"rr1": round(rr1, 2), "rr2": round(rr2, 2), "risk_pct": round(risk_pct, 2)},
        "positioning": {
            "stage": stage,
            "room_to_r1_pct": round(room_to_r1_pct, 2),
            "setup_score": round(setup_score, 1),
            "volatility_score": round(volatility_score, 1),
        },
        "liquidity": {
            "score": round(liquidity_score, 1),
            "tier": str(liquidity.get("tier", "unknown")),
            "avg_dollar_volume_m": _safe_float(liquidity.get("avg_dollar_volume_m"), 0.0),
        },
        "event_risk": event_risk,
        "constraints": {
            "market": market_status,
            "min_rr2": round(min_rr_gate, 2),
            "max_risk_pct": round(max_risk_gate, 2),
            "risk_ceiling": risk_ceiling,
        },
        "tradeable": tradeable,
    }
    execution = _execution_profile(trade_plan, score, market_profile, liquidity, event_risk)
    if not tradeable:
        execution["position_pct"] = 0.0
        execution["allocation"] = "skip"
    trade_plan["execution"] = execution
    return trade_plan


def _strategy_conservative_momentum(ind: dict[str, Any]) -> dict[str, Any] | None:
    if (
        ind["price"] > ind["ma50"]
        and ind["price"] > ind["ma200"]
        and 40 <= ind["rsi"] <= 60
        and ind["volume"] >= ind["volume_avg"] * 0.8
    ):
        return {
            "strategy": "보수적 모멘텀",
            "emoji": "🛡",
            "reason": f"추세 우상향 + RSI {ind['rsi']:.0f}",
            "risk": "낮음",
        }
    return None


def _strategy_golden_cross(ind: dict[str, Any]) -> dict[str, Any] | None:
    if ind["ma5_prev"] <= ind["ma20_prev"] and ind["ma5"] > ind["ma20"]:
        return {
            "strategy": "골든크로스",
            "emoji": "✨",
            "reason": "5일선이 20일선을 상향 돌파",
            "risk": "중간",
        }
    return None


def _strategy_bollinger_bounce(ind: dict[str, Any]) -> dict[str, Any] | None:
    if ind["price_prev"] <= ind["bb_lower_prev"] * 1.01 and ind["price"] > ind["price_prev"] and ind["rsi"] < 35:
        return {
            "strategy": "볼린저 반등",
            "emoji": "📈",
            "reason": f"하단 이탈 후 반등, RSI {ind['rsi']:.0f}",
            "risk": "중간",
        }
    return None


def _strategy_macd_cross(ind: dict[str, Any]) -> dict[str, Any] | None:
    if ind["macd_prev"] <= ind["macd_signal_prev"] and ind["macd"] > ind["macd_signal"]:
        return {
            "strategy": "MACD 크로스",
            "emoji": "📊",
            "reason": "MACD가 시그널선을 상향 돌파",
            "risk": "중간",
        }
    return None


def _strategy_near_52w_high(ind: dict[str, Any]) -> dict[str, Any] | None:
    if ind["high_52w"] <= 0:
        return None
    gap_52w = (ind["high_52w"] - ind["price"]) / ind["high_52w"] * 100
    if 0 < gap_52w <= 5 and ind["price"] > ind["ma50"]:
        return {
            "strategy": "52주 신고가 접근",
            "emoji": "🎯",
            "reason": f"신고가 대비 -{gap_52w:.1f}%",
            "risk": "높음",
        }
    return None


def _strategy_volume_surge(ind: dict[str, Any]) -> dict[str, Any] | None:
    if ind["volume_avg"] <= 0 or ind["price_prev"] <= 0:
        return None
    vol_ratio = ind["volume"] / ind["volume_avg"]
    price_change = (ind["price"] - ind["price_prev"]) / ind["price_prev"] * 100
    if vol_ratio >= 2 and price_change > 0 and ind["price"] > ind["ma50"]:
        return {
            "strategy": "거래량 급증",
            "emoji": "🚀",
            "reason": f"거래량 {vol_ratio:.1f}배 +{price_change:.1f}%",
            "risk": "중간",
        }
    return None


_STRATEGY_PIPELINE = [
    _strategy_conservative_momentum,
    _strategy_golden_cross,
    _strategy_bollinger_bounce,
    _strategy_macd_cross,
    _strategy_near_52w_high,
    _strategy_volume_surge,
]


def _evaluate_strategies(ind: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for strategy in _STRATEGY_PIPELINE:
        hit = strategy(ind)
        if hit:
            out.append(hit)
    return out


def check_entry_signal(symbol: str, target_price: float = 0) -> dict[str, Any]:
    """
    Entry signal checker.

    Signal is valid when >=3 conditions are met or explicit target is reached.
    """
    df = get_stock_data(symbol)
    if df is None:
        return {"error": "데이터 없음"}

    ind = calculate_indicators(df)
    if ind is None:
        return {"error": "지표 계산 실패"}

    ma5_gap = ind.get("ma5_gap", ind.get("ma50_gap", 0))
    conditions = {
        "rsi_oversold": ind["rsi"] <= 35,
        "bb_lower": ind["bb_position"] <= 20,
        "below_ma5": ma5_gap <= -3,
        "consecutive_down": ind["down_days"] >= 3,
        "target_reached": ind["price"] <= target_price if target_price > 0 else False,
    }

    met_count = sum(bool(v) for v in conditions.values())
    is_signal = met_count >= 3 or conditions["target_reached"]

    if met_count >= 4:
        strength = "강함"
    elif met_count >= 3:
        strength = "보통"
    else:
        strength = "약함"

    return {
        "symbol": symbol,
        "price": ind["price"],
        "is_signal": is_signal,
        "strength": strength,
        "met_count": met_count,
        "conditions": conditions,
        "rsi": ind["rsi"],
        "bb_position": ind["bb_position"],
        "ma5_gap": ma5_gap,
        "ma50_gap": ind["ma50_gap"],
        "down_days": ind["down_days"],
    }


def check_exit_signal(symbol: str, buy_price: float, stop_loss: float = -7, take_profit: float = 15) -> dict[str, Any]:
    """Exit signal checker for risk protection and profit capture."""
    df = get_stock_data(symbol)
    if df is None:
        return {"error": "데이터 없음"}

    ind = calculate_indicators(df)
    if ind is None:
        return {"error": "지표 계산 실패"}

    price = ind["price"]
    pnl_pct = ((price - buy_price) / buy_price * 100) if buy_price else 0

    signals = {
        "stop_loss": pnl_pct <= stop_loss,
        "take_profit": pnl_pct >= take_profit,
        "rsi_overbought": ind["rsi"] >= 72,
        "below_ma50": ind["ma50_gap"] < -5,
    }
    is_exit = any(signals.values())

    if signals["stop_loss"]:
        reason, urgency = f"손절 ({pnl_pct:.1f}%)", "즉시"
    elif signals["take_profit"]:
        reason, urgency = f"익절 ({pnl_pct:.1f}%)", "권장"
    elif signals["rsi_overbought"]:
        reason, urgency = f"RSI 과매수 ({ind['rsi']:.0f})", "고려"
    elif signals["below_ma50"]:
        reason, urgency = "50일선 하향 이탈", "고려"
    else:
        reason, urgency = "없음", "없음"

    return {
        "symbol": symbol,
        "price": price,
        "buy_price": buy_price,
        "pnl_pct": round(pnl_pct, 1),
        "is_exit": is_exit,
        "reason": reason,
        "urgency": urgency,
        "signals": signals,
        "rsi": ind["rsi"],
        "ma50_gap": ind["ma50_gap"],
    }


def check_strategies(symbol: str) -> list[dict[str, Any]]:
    """Evaluate all strategies for a single symbol."""
    df = get_stock_data(symbol)
    if df is None:
        return []

    ind = calculate_indicators(df)
    if ind is None:
        return []
    return _evaluate_strategies(ind)


def _quality_score(score: dict[str, Any], strategy_count: int, regime_adjustment: float) -> float:
    base = float(score.get("total_score", 0))
    risk_score = float(score.get("risk", {}).get("score", 50))
    confidence = float(score.get("confidence", {}).get("score", 60))

    strategy_bonus = min(15.0, strategy_count * 4.0)
    risk_penalty = max(0.0, risk_score - 45.0) * 0.35
    confidence_bonus = (confidence - 60.0) * 0.12

    raw = base + strategy_bonus - risk_penalty + confidence_bonus + regime_adjustment
    return round(max(0.0, min(100.0, raw)), 1)


def _investability_score(
    quality_score: float,
    score: dict[str, Any],
    trade_plan: dict[str, Any],
    strategy_count: int,
) -> float:
    setup_score = _safe_float(trade_plan.get("positioning", {}).get("setup_score"), 50)
    rr2 = _safe_float(trade_plan.get("risk_reward", {}).get("rr2"), 0.8)
    risk_score = _safe_float(score.get("risk", {}).get("score"), 50)
    stage = str(trade_plan.get("positioning", {}).get("stage", "mid_trend"))
    tradeable = bool(trade_plan.get("tradeable"))
    liquidity_score = _safe_float(trade_plan.get("liquidity", {}).get("score"), 55)
    position_pct = _safe_float(trade_plan.get("execution", {}).get("position_pct"), 0)
    event_penalty = _safe_float(trade_plan.get("event_risk", {}).get("penalty"), 0)
    market = str(trade_plan.get("constraints", {}).get("market", "neutral"))

    stage_bonus = 6 if stage == "right_knee" else -12 if stage == "right_shoulder" else 0
    rr_bonus = _clamp((rr2 - 1.0) * 11, -12, 14)
    risk_penalty = max(0.0, risk_score - 45.0) * 0.28
    strategy_bonus = min(8.0, strategy_count * 1.8)
    tradeable_bonus = 7 if tradeable else -8
    liquidity_bonus = (liquidity_score - 55.0) * 0.14
    position_bonus = min(10.0, position_pct * 0.65)
    regime_penalty = 5.0 if market == "bearish" and stage != "right_knee" else 0.0

    value = (
        quality_score * 0.52
        + setup_score * 0.28
        + stage_bonus
        + rr_bonus
        + strategy_bonus
        + tradeable_bonus
        + liquidity_bonus
        + position_bonus
        - risk_penalty
        - event_penalty
        - regime_penalty
    )
    return round(_clamp(value), 1)


def scan_stocks(symbols: list[str], fundamental_limit: int | None = None) -> dict[str, Any]:
    """Bulk scan symbols in parallel and return enriched ranking records."""
    from core.scoring import calculate_score

    if not symbols:
        return {"results": [], "total": 0}

    if fundamental_limit is None:
        n = len(symbols)
        if n <= 80:
            fundamental_limit = n
        elif n <= 220:
            fundamental_limit = max(80, n // 2)
        else:
            fundamental_limit = max(120, n // 3)
    fundamental_limit = max(0, min(fundamental_limit, len(symbols)))

    market = get_market_condition()
    regime_adjustment = {"bullish": 1.5, "neutral": 0.0, "bearish": -2.5}.get(market.get("status"), 0.0)

    def analyze(symbol: str) -> dict[str, Any] | None:
        try:
            df = get_stock_data(symbol)
            if df is None:
                return None

            ind = calculate_indicators(df)
            if ind is None:
                return None

            strategies = _evaluate_strategies(ind)
            liquidity = _liquidity_profile(ind)
            event_risk = _event_risk_profile(None)
            payload = _build_score_payload(symbol, ind)
            payload["avg_dollar_volume_m"] = liquidity.get("avg_dollar_volume_m", 0.0)
            score = calculate_score(payload)
            quality = _quality_score(score, len(strategies), regime_adjustment)
            trade_plan = _build_trade_plan(ind, score, market.get("status", "unknown"), liquidity, event_risk)
            investability = _investability_score(quality, score, trade_plan, len(strategies))

            return {
                "symbol": symbol,
                "price": ind["price"],
                "rsi": ind["rsi"],
                "adx": ind["adx"],
                "volume_ratio": ind["volume_ratio"],
                "ma50_gap": ind["ma50_gap"],
                "position_52w": ind["position_52w"],
                "strategies": strategies,
                "strategy_count": len(strategies),
                "quality_score": quality,
                "investability_score": investability,
                "liquidity_score": liquidity.get("score", 0.0),
                "liquidity_tier": liquidity.get("tier", "unknown"),
                "avg_dollar_volume_m": liquidity.get("avg_dollar_volume_m", 0.0),
                "position_size_pct": trade_plan.get("execution", {}).get("position_pct", 0.0),
                "days_to_earnings": event_risk.get("days_to_earnings"),
                "event_risk_level": event_risk.get("level", "unknown"),
                "financial_coverage": float(score.get("financial", {}).get("coverage", 0)),
                "fundamentals_used": False,
                "trade_plan": trade_plan,
                "score": score,
                "market_regime": market.get("status", "unknown"),
                "_ind": ind,
            }
        except Exception:
            return None

    results: list[dict[str, Any]] = []
    workers = min(16, max(4, len(symbols) // 8))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(analyze, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    enriched_count = 0
    if results and fundamental_limit > 0:
        top_candidates = sorted(results, key=lambda r: -r.get("investability_score", 0))[:fundamental_limit]
        candidate_symbols = {row["symbol"] for row in top_candidates}
        by_symbol = {row["symbol"]: row for row in results}
        updated_rows: dict[str, dict[str, Any]] = {}

        def enrich(symbol: str) -> dict[str, Any] | None:
            row = by_symbol.get(symbol)
            if not row:
                return None

            fundamentals = get_stock_info(symbol)
            if not fundamentals:
                return None

            ind = row.get("_ind")
            if not isinstance(ind, dict):
                return None

            liquidity = _liquidity_profile(ind)
            event_risk = _event_risk_profile(fundamentals)
            payload = _build_score_payload(symbol, ind, fundamentals)
            payload["avg_dollar_volume_m"] = liquidity.get("avg_dollar_volume_m", 0.0)
            score = calculate_score(payload)
            strategy_count = int(row.get("strategy_count", 0))
            quality = _quality_score(score, strategy_count, regime_adjustment)
            trade_plan = _build_trade_plan(ind, score, market.get("status", "unknown"), liquidity, event_risk)
            investability = _investability_score(quality, score, trade_plan, strategy_count)

            new_row = dict(row)
            new_row["score"] = score
            new_row["quality_score"] = quality
            new_row["investability_score"] = investability
            new_row["trade_plan"] = trade_plan
            new_row["financial_coverage"] = float(score.get("financial", {}).get("coverage", 0))
            new_row["fundamentals_used"] = True
            new_row["liquidity_score"] = liquidity.get("score", 0.0)
            new_row["liquidity_tier"] = liquidity.get("tier", "unknown")
            new_row["avg_dollar_volume_m"] = liquidity.get("avg_dollar_volume_m", 0.0)
            new_row["position_size_pct"] = trade_plan.get("execution", {}).get("position_pct", 0.0)
            new_row["days_to_earnings"] = event_risk.get("days_to_earnings")
            new_row["event_risk_level"] = event_risk.get("level", "unknown")
            new_row["beta"] = fundamentals.get("beta")
            new_row["sector"] = fundamentals.get("sector")
            new_row["market_cap"] = fundamentals.get("market_cap")
            return new_row

        enrich_workers = min(12, max(4, len(candidate_symbols) // 15))
        with ThreadPoolExecutor(max_workers=enrich_workers) as executor:
            futures = {executor.submit(enrich, symbol): symbol for symbol in candidate_symbols}
            for future in as_completed(futures):
                symbol = futures[future]
                updated = future.result()
                if not updated:
                    continue
                updated_rows[symbol] = updated

        if updated_rows:
            enriched_count = len(updated_rows)
            for idx, row in enumerate(results):
                replacement = updated_rows.get(row["symbol"])
                if replacement:
                    results[idx] = replacement

    for row in results:
        row.pop("_ind", None)

    return {"results": results, "total": len(results), "fundamentals_enriched": enriched_count}


__all__ = [
    "check_entry_signal",
    "check_exit_signal",
    "check_strategies",
    "scan_stocks",
]
