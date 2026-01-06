"""
종목 점수화 모듈
- 팩터 점수 (모멘텀, 가치, 퀄리티 등)
- 재무 점수
- 위험도 점수
"""


def calculate_score(data: dict) -> dict:
    """종합 점수 계산"""
    factor = calculate_factor_score(data)
    financial = calculate_financial_score(data)
    risk = calculate_risk_score(data)
    
    # 종합 점수 (팩터 50% + 재무 30% + 안전성 20%)
    total = factor["score"] * 0.5 + financial["score"] * 0.3 + (100 - risk["score"]) * 0.2
    
    if total >= 70:
        grade, recommendation = "A", "적극 매수"
    elif total >= 60:
        grade, recommendation = "B", "매수"
    elif total >= 50:
        grade, recommendation = "C", "관망"
    elif total >= 40:
        grade, recommendation = "D", "매도 고려"
    else:
        grade, recommendation = "F", "매도"
    
    return {
        "symbol": data.get("symbol", ""),
        "total_score": round(total, 1),
        "grade": grade,
        "recommendation": recommendation,
        "factor": factor,
        "financial": financial,
        "risk": risk,
    }


def calculate_factor_score(data: dict) -> dict:
    """팩터 점수 (모멘텀, 가치, 퀄리티, 저변동성)"""
    scores = {}
    
    # 모멘텀 (30%)
    momentum = 50
    pos_52w = data.get("position_52w", 50)
    if 60 <= pos_52w <= 85:
        momentum += 25
    elif 50 <= pos_52w < 60:
        momentum += 15
    elif pos_52w > 95:
        momentum -= 10
    elif pos_52w < 30:
        momentum -= 15
    
    ma50_gap = data.get("ma50_gap", 0)
    if 0 < ma50_gap <= 10:
        momentum += 15
    elif ma50_gap > 20:
        momentum -= 10
    elif ma50_gap < -10:
        momentum -= 15
    
    rsi = data.get("rsi", 50)
    if 50 <= rsi <= 65:
        momentum += 10
    elif rsi > 70:
        momentum -= 15
    elif rsi < 30:
        momentum -= 10
    
    scores["momentum"] = max(0, min(100, momentum))
    
    # 가치 (25%)
    value = 50
    pe = _parse_num(data.get("pe", 0))
    if 0 < pe <= 15:
        value += 20
    elif 15 < pe <= 25:
        value += 10
    elif pe > 35:
        value -= 15
    
    peg = _parse_num(data.get("peg", 0))
    if 0 < peg <= 1:
        value += 15
    elif 1 < peg <= 2:
        value += 5
    elif peg > 3:
        value -= 10
    
    scores["value"] = max(0, min(100, value))
    
    # 퀄리티 (25%)
    quality = 50
    roe = _parse_pct(data.get("roe", 0))
    if roe > 20:
        quality += 20
    elif roe > 15:
        quality += 15
    elif roe > 10:
        quality += 10
    elif roe < 0:
        quality -= 15
    
    debt = _parse_num(data.get("debt_to_equity", 0)) / 100
    if debt <= 0.5:
        quality += 15
    elif debt <= 1:
        quality += 10
    elif debt > 2:
        quality -= 15
    
    scores["quality"] = max(0, min(100, quality))
    
    # 저변동성 (20%)
    volatility = 50
    bb_pos = data.get("bb_position", 50)
    if 30 <= bb_pos <= 70:
        volatility += 20
    elif bb_pos < 10 or bb_pos > 90:
        volatility -= 15
    
    change_5d = abs(data.get("change_5d", 0))
    if change_5d <= 3:
        volatility += 15
    elif change_5d > 15:
        volatility -= 15
    
    scores["low_volatility"] = max(0, min(100, volatility))
    
    # 종합 (가중 평균)
    total = (scores["momentum"] * 0.30 + scores["value"] * 0.25 + 
             scores["quality"] * 0.25 + scores["low_volatility"] * 0.20)
    
    return {"score": round(total, 1), "details": scores}


def calculate_financial_score(data: dict) -> dict:
    """재무 점수"""
    scores = {}
    
    # 수익성
    profitability = 50
    roe = _parse_pct(data.get("roe", 0))
    if roe >= 20:
        profitability += 25
    elif roe >= 15:
        profitability += 15
    elif roe < 0:
        profitability -= 20
    
    margin = _parse_pct(data.get("profit_margin", 0))
    if margin >= 20:
        profitability += 15
    elif margin >= 10:
        profitability += 10
    elif margin < 0:
        profitability -= 10
    
    scores["profitability"] = max(0, min(100, profitability))
    
    # 성장성
    growth = 50
    rev_growth = _parse_pct(data.get("revenue_growth", 0))
    if rev_growth >= 20:
        growth += 20
    elif rev_growth >= 10:
        growth += 10
    elif rev_growth < 0:
        growth -= 10
    
    earn_growth = _parse_pct(data.get("earnings_growth", 0))
    if earn_growth >= 20:
        growth += 20
    elif earn_growth >= 10:
        growth += 10
    elif earn_growth < -10:
        growth -= 15
    
    scores["growth"] = max(0, min(100, growth))
    
    # 재무건전성
    health = 50
    current = data.get("current_ratio", 0) or 0
    if current >= 2:
        health += 15
    elif current >= 1.5:
        health += 10
    elif current < 1:
        health -= 10
    
    fcf = data.get("free_cash_flow", 0) or 0
    if fcf > 0:
        health += 15
    else:
        health -= 10
    
    scores["health"] = max(0, min(100, health))
    
    # 종합
    total = (scores["profitability"] * 0.4 + scores["growth"] * 0.35 + scores["health"] * 0.25)
    
    return {"score": round(total, 1), "details": scores}


def calculate_risk_score(data: dict) -> dict:
    """위험도 점수 (0-100, 높을수록 위험)"""
    risk = 0
    warnings = []
    
    # RSI 과매수
    rsi = data.get("rsi", 50)
    if rsi >= 70:
        risk += 25
        warnings.append(f"RSI {rsi:.0f} 과매수")
    elif rsi >= 60:
        risk += 10
    
    # RSI 과매도 (반등 가능하나 위험)
    if rsi <= 30:
        risk += 15
        warnings.append(f"RSI {rsi:.0f} 과매도")
    
    # 볼린저 상단
    bb_pos = data.get("bb_position", 50)
    if bb_pos >= 95:
        risk += 20
        warnings.append("볼린저 상단 돌파")
    elif bb_pos >= 80:
        risk += 10
    
    # 52주 고점
    pos_52w = data.get("position_52w", 50)
    if pos_52w >= 95:
        risk += 20
        warnings.append("52주 최고점 근접")
    
    # 52주 저점
    if pos_52w <= 10:
        risk += 15
        warnings.append("52주 최저점 근접")
    
    # 50일선 괴리
    ma50_gap = data.get("ma50_gap", 0)
    if ma50_gap >= 20:
        risk += 20
        warnings.append(f"50일선 대비 +{ma50_gap:.0f}%")
    elif ma50_gap <= -20:
        risk += 25
        warnings.append(f"50일선 대비 {ma50_gap:.0f}%")
    
    # 5일 급등/급락
    change_5d = data.get("change_5d", 0)
    if change_5d >= 20:
        risk += 15
        warnings.append(f"5일간 +{change_5d:.0f}% 급등")
    elif change_5d <= -15:
        risk += 20
        warnings.append(f"5일간 {change_5d:.0f}% 급락")
    
    risk = min(100, risk)
    
    if risk >= 50:
        grade = "🔴 고위험"
    elif risk >= 30:
        grade = "🟡 주의"
    else:
        grade = "🟢 양호"
    
    return {"score": risk, "grade": grade, "warnings": warnings}


def _parse_pct(value) -> float:
    """퍼센트 값 파싱"""
    if isinstance(value, str):
        value = value.replace("%", "").replace(",", "")
    try:
        v = float(value or 0)
        return v * 100 if abs(v) < 1 else v  # 0.15 → 15%
    except:
        return 0


def _parse_num(value) -> float:
    """숫자 파싱"""
    if isinstance(value, str):
        value = value.replace(",", "").replace("N/A", "0").replace("-", "0")
    try:
        return float(value or 0)
    except:
        return 0
