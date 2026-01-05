"""
학술 연구 기반 멀티팩터 모델

참고 연구 및 데이터:
[팩터 프리미엄 - Swedroe & Berkin (2016) "Your Complete Guide to Factor-Based Investing"]
- 기간: 1927-2015 (88년)
- Momentum: 연 9.6%, Sharpe 0.61 (가장 높은 프리미엄)
- Value: 연 4.8%, Sharpe 0.34
- Profitability: 연 3.1%, Sharpe 0.33
- Size: 연 3.3%, Sharpe 0.24

[Quality - Alpha Architect (2024)]
- 기간: 1964-2023 (60년)
- Quality (QMJ): 연 4.7%, Sharpe 0.47

[Low Volatility - Robeco (2024)]
- 저변동성 주식이 고변동성 주식보다 높은 위험조정수익률
- CAPM 반박하는 anomaly

[최신 연구]
- López de Prado (2023): Causal Factor Investing
- Blitz, van Vliet, Hanauer (2024): FF5 비판 - 모멘텀/저변동성 누락

가중치 근거:
- Sharpe Ratio 기반 배분 (위험조정수익률 높은 팩터에 더 많은 가중치)
- Momentum (0.61) > Quality (0.47) > Market (0.40) > Value (0.34) > Profitability (0.33)
"""
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional


@dataclass
class FactorWeights:
    """
    팩터별 가중치 (Sharpe Ratio 기반)
    
    근거: Swedroe & Berkin (2016), Alpha Architect (2024)
    
    Sharpe Ratio 순위 (높을수록 위험조정수익률 좋음):
    1. Momentum: 0.61 → 30%
    2. Quality: 0.47 → 25%
    3. Value: 0.34 → 20%
    4. Profitability: 0.33 → 15%
    5. Low Volatility: Robeco 연구 → 10%
    
    총합: 100%
    """
    # Sharpe Ratio 기반 가중치
    momentum: float = 0.30           # Sharpe 0.61 (최고)
    quality: float = 0.25            # Sharpe 0.47
    value: float = 0.20              # Sharpe 0.34
    profitability: float = 0.15      # Sharpe 0.33
    low_volatility: float = 0.10     # Robeco anomaly
    
    # 사용하지 않음 (근거 부족)
    size: float = 0.0                # Sharpe 0.24 (낮음)
    investment: float = 0.0          # FF5에서만 유의미
    
    def normalize(self):
        """가중치 합이 1이 되도록 정규화"""
        total = (self.profitability + self.momentum + self.value + 
                 self.quality + self.low_volatility + self.size + self.investment)
        return FactorWeights(
            profitability=self.profitability / total,
            momentum=self.momentum / total,
            value=self.value / total,
            quality=self.quality / total,
            low_volatility=self.low_volatility / total,
            size=self.size / total,
            investment=self.investment / total,
        )


# 기본 가중치 (연구 기반)
DEFAULT_WEIGHTS = FactorWeights()

# 공격적 가중치 (모멘텀 강조)
AGGRESSIVE_WEIGHTS = FactorWeights(
    profitability=0.20,
    momentum=0.35,
    value=0.10,
    quality=0.10,
    low_volatility=0.05,
    size=0.10,
    investment=0.10,
)

# 보수적 가중치 (안정성 강조)
CONSERVATIVE_WEIGHTS = FactorWeights(
    profitability=0.25,
    momentum=0.10,
    value=0.20,
    quality=0.20,
    low_volatility=0.15,
    size=0.05,
    investment=0.05,
)


def calculate_profitability_score(data: dict) -> float:
    """
    수익성 점수 (0-100)
    
    지표:
    - ROE (Return on Equity): 높을수록 좋음
    - Gross Profit Margin: 높을수록 좋음
    - ROA (Return on Assets): 높을수록 좋음
    - 순이익률, 영업이익률 (재무제표 데이터)
    
    연구: Novy-Marx (2013) - 수익성이 가장 강력한 예측 팩터
    """
    score = 50  # 기본값
    
    roe = _parse_percent(data.get("roe", "0"))
    if roe > 30:
        score += 30
    elif roe > 20:
        score += 20
    elif roe > 15:
        score += 10
    elif roe > 10:
        score += 5
    elif roe < 0:
        score -= 20
    
    # ROA
    roa = _parse_percent(data.get("roa", "0"))
    if roa > 15:
        score += 15
    elif roa > 10:
        score += 10
    elif roa > 5:
        score += 5
    elif roa < 0:
        score -= 10
    
    # 재무제표 데이터가 있으면 추가 점수
    profit_margin = data.get("profit_margin", 0)
    if isinstance(profit_margin, (int, float)) and profit_margin:
        pm_pct = profit_margin * 100
        if pm_pct > 20:
            score += 10
        elif pm_pct > 10:
            score += 5
        elif pm_pct < 0:
            score -= 5
    
    operating_margin = data.get("operating_margin", 0)
    if isinstance(operating_margin, (int, float)) and operating_margin:
        om_pct = operating_margin * 100
        if om_pct > 25:
            score += 5
        elif om_pct > 15:
            score += 3
    
    return max(0, min(100, score))


def calculate_momentum_score(data: dict) -> float:
    """
    모멘텀 점수 (0-100)
    
    지표:
    - 52주 위치: 고점 근처면 강한 모멘텀
    - 50일선 대비: 위에 있으면 상승 추세
    - RSI: 50-70 구간이 이상적
    
    연구: Jegadeesh & Titman (1993) - 12-1개월 모멘텀 유효
    """
    score = 50
    
    # 52주 위치 (중간~상위가 좋음, 너무 높으면 과열)
    pos_52w = data.get("position_52w", 50)
    if 60 <= pos_52w <= 85:
        score += 25  # 이상적 구간
    elif 50 <= pos_52w < 60:
        score += 15
    elif 85 < pos_52w <= 95:
        score += 10  # 고점 근처 주의
    elif pos_52w > 95:
        score -= 10  # 과열
    elif pos_52w < 30:
        score -= 15  # 약세
    
    # 50일선 대비
    ma50_gap = data.get("ma50_gap", 0)
    if 0 < ma50_gap <= 10:
        score += 15  # 적당히 위
    elif 10 < ma50_gap <= 20:
        score += 5   # 많이 위 (과열 주의)
    elif ma50_gap > 20:
        score -= 10  # 과열
    elif -5 <= ma50_gap <= 0:
        score += 5   # 지지선 근처
    elif ma50_gap < -10:
        score -= 15  # 하락 추세
    
    # RSI
    rsi = data.get("rsi", 50)
    if 50 <= rsi <= 65:
        score += 10  # 이상적
    elif 40 <= rsi < 50:
        score += 5
    elif rsi > 70:
        score -= 15  # 과매수
    elif rsi < 30:
        score -= 10  # 과매도 (반등 가능하나 위험)
    
    return max(0, min(100, score))


def calculate_value_score(data: dict) -> float:
    """
    가치 점수 (0-100)
    
    지표:
    - P/E: 낮을수록 저평가
    - Forward P/E: 미래 실적 반영
    - PEG: 1 이하면 저평가
    - P/B: 낮을수록 저평가
    
    연구: Fama & French (1992) - Value Premium
    """
    score = 50
    
    # P/E
    pe = _parse_number(data.get("pe", "0"))
    if 0 < pe <= 15:
        score += 20  # 저평가
    elif 15 < pe <= 25:
        score += 10  # 적정
    elif 25 < pe <= 35:
        score += 0   # 약간 고평가
    elif pe > 35:
        score -= 15  # 고평가
    elif pe <= 0:
        score -= 10  # 적자
    
    # Forward P/E
    fpe = _parse_number(data.get("forward_pe", "0"))
    if 0 < fpe <= 15:
        score += 15
    elif 15 < fpe <= 25:
        score += 5
    elif fpe > 30:
        score -= 10
    
    # PEG
    peg = _parse_number(data.get("peg", "0"))
    if 0 < peg <= 1:
        score += 15  # 저평가
    elif 1 < peg <= 2:
        score += 5   # 적정
    elif peg > 3:
        score -= 10  # 고평가
    
    return max(0, min(100, score))


def calculate_quality_score(data: dict) -> float:
    """
    퀄리티 점수 (0-100)
    
    지표:
    - 부채비율 (Debt/Equity): 낮을수록 좋음
    - 이익 안정성
    - 배당 지속성
    - 유동비율, 잉여현금흐름 (재무제표 데이터)
    
    연구: Asness et al. (2019) - Quality Minus Junk
    """
    score = 50
    
    # 부채비율
    debt_eq = _parse_number(data.get("debt_eq", "0"))
    if debt_eq <= 0.3:
        score += 20  # 매우 낮음
    elif debt_eq <= 0.5:
        score += 15
    elif debt_eq <= 1.0:
        score += 5
    elif debt_eq > 2.0:
        score -= 15  # 높은 부채
    
    # 배당 (있으면 가산)
    dividend = _parse_percent(data.get("dividend", "0"))
    if dividend > 0:
        score += 10
    if dividend > 2:
        score += 5
    
    # 재무제표 데이터가 있으면 추가 점수
    current_ratio = data.get("current_ratio", 0)
    if isinstance(current_ratio, (int, float)) and current_ratio:
        if current_ratio >= 2:
            score += 10  # 그레이엄 기준
        elif current_ratio >= 1.5:
            score += 5
        elif current_ratio < 1:
            score -= 10  # 유동성 위험
    
    free_cash_flow = data.get("free_cash_flow", 0)
    if isinstance(free_cash_flow, (int, float)):
        if free_cash_flow > 0:
            score += 5  # 양의 잉여현금흐름
        elif free_cash_flow < 0:
            score -= 5
    
    return max(0, min(100, score))


def calculate_volatility_score(data: dict) -> float:
    """
    저변동성 점수 (0-100)
    
    지표:
    - 볼린저밴드 위치: 중간이 안정적
    - 5일 변화율: 작을수록 안정적
    
    연구: Ang et al. (2006) - Low Volatility Anomaly
    """
    score = 50
    
    # 볼린저밴드 위치 (중간이 안정적)
    bb_pos = data.get("bb_position", 50)
    if 30 <= bb_pos <= 70:
        score += 20  # 안정적
    elif 20 <= bb_pos < 30 or 70 < bb_pos <= 80:
        score += 10
    elif bb_pos < 10 or bb_pos > 90:
        score -= 15  # 극단적
    
    # 5일 변화율 (작을수록 안정적)
    change_5d = abs(data.get("change_5d", 0))
    if change_5d <= 3:
        score += 15  # 안정적
    elif change_5d <= 5:
        score += 10
    elif change_5d <= 10:
        score += 0
    elif change_5d > 15:
        score -= 15  # 변동성 큼
    
    return max(0, min(100, score))


def calculate_composite_score(data: dict, weights: FactorWeights = None) -> dict:
    """
    종합 팩터 점수 계산
    
    Returns:
        dict: 각 팩터 점수 및 종합 점수
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS
    
    # 각 팩터 점수 계산
    profitability = calculate_profitability_score(data)
    momentum = calculate_momentum_score(data)
    value = calculate_value_score(data)
    quality = calculate_quality_score(data)
    volatility = calculate_volatility_score(data)
    
    # 종합 점수 (Sharpe Ratio 기반 가중 평균)
    # Momentum 30% + Quality 25% + Value 20% + Profitability 15% + Low Vol 10%
    composite = (
        momentum * weights.momentum +
        quality * weights.quality +
        value * weights.value +
        profitability * weights.profitability +
        volatility * weights.low_volatility
    )
    
    # 등급 결정
    if composite >= 70:
        grade = "A"
        recommendation = "적극 매수"
    elif composite >= 60:
        grade = "B"
        recommendation = "매수"
    elif composite >= 50:
        grade = "C"
        recommendation = "보유/관망"
    elif composite >= 40:
        grade = "D"
        recommendation = "매도 고려"
    else:
        grade = "F"
        recommendation = "매도"
    
    return {
        "symbol": data.get("symbol", ""),
        "composite_score": round(composite, 1),
        "grade": grade,
        "recommendation": recommendation,
        "factors": {
            "momentum": round(momentum, 1),
            "quality": round(quality, 1),
            "value": round(value, 1),
            "profitability": round(profitability, 1),
            "low_volatility": round(volatility, 1),
        },
        "weights_used": {
            "momentum": f"{weights.momentum*100:.0f}% (Sharpe 0.61)",
            "quality": f"{weights.quality*100:.0f}% (Sharpe 0.47)",
            "value": f"{weights.value*100:.0f}% (Sharpe 0.34)",
            "profitability": f"{weights.profitability*100:.0f}% (Sharpe 0.33)",
            "low_volatility": f"{weights.low_volatility*100:.0f}% (Robeco)",
        }
    }


def _parse_percent(value: str) -> float:
    """퍼센트 문자열을 숫자로 변환"""
    if not value or value == "N/A":
        return 0
    try:
        return float(str(value).replace("%", "").replace(",", ""))
    except:
        return 0


def _parse_number(value: str) -> float:
    """숫자 문자열을 float로 변환"""
    if not value or value == "N/A" or value == "-":
        return 0
    try:
        return float(str(value).replace(",", ""))
    except:
        return 0


def rank_stocks_by_factor(stocks_data: list[dict], weights: FactorWeights = None) -> list[dict]:
    """
    종목들을 팩터 점수로 랭킹
    
    Args:
        stocks_data: 종목 데이터 리스트
        weights: 팩터 가중치
    
    Returns:
        점수순 정렬된 종목 리스트
    """
    results = []
    
    for stock in stocks_data:
        score = calculate_composite_score(stock, weights)
        score["price"] = stock.get("price", 0)
        score["risk_score"] = stock.get("risk_score", 50)
        results.append(score)
    
    # 종합 점수 내림차순 정렬
    results.sort(key=lambda x: x["composite_score"], reverse=True)
    
    return results


# 테스트
if __name__ == "__main__":
    # 테스트 데이터
    test_stock = {
        "symbol": "AAPL",
        "price": 185.0,
        "roe": "150%",
        "roa": "25%",
        "pe": "28",
        "forward_pe": "25",
        "peg": "2.5",
        "debt_eq": "1.5",
        "dividend": "0.5%",
        "position_52w": 75,
        "ma50_gap": 5,
        "rsi": 55,
        "bb_position": 60,
        "change_5d": 2,
        "risk_score": 20,
    }
    
    print("=" * 50)
    print("팩터 모델 테스트")
    print("=" * 50)
    
    # 기본 가중치
    result = calculate_composite_score(test_stock, DEFAULT_WEIGHTS)
    print(f"\n종목: {result['symbol']}")
    print(f"종합 점수: {result['composite_score']}/100")
    print(f"등급: {result['grade']}")
    print(f"추천: {result['recommendation']}")
    print(f"\n팩터별 점수:")
    for factor, score in result['factors'].items():
        print(f"  - {factor}: {score}")
    print(f"\n가중치:")
    for factor, weight in result['weights_used'].items():
        print(f"  - {factor}: {weight}%")
