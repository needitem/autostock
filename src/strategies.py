"""
다양한 매매 전략 모듈
"""
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import SMAIndicator, MACD
from ta.volatility import BollingerBands


def add_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """모든 기술적 지표 추가"""
    if df is None or len(df) < 200:
        return None
    
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    
    # 이동평균선
    df["MA5"] = SMAIndicator(close, window=5).sma_indicator()
    df["MA20"] = SMAIndicator(close, window=20).sma_indicator()
    df["MA50"] = SMAIndicator(close, window=50).sma_indicator()
    df["MA200"] = SMAIndicator(close, window=200).sma_indicator()
    
    # RSI
    df["RSI"] = RSIIndicator(close, window=14).rsi()
    
    # MACD
    macd = MACD(close)
    df["MACD"] = macd.macd()
    df["MACD_Signal"] = macd.macd_signal()
    df["MACD_Hist"] = macd.macd_diff()
    
    # 볼린저밴드
    bb = BollingerBands(close, window=20, window_dev=2)
    df["BB_Upper"] = bb.bollinger_hband()
    df["BB_Lower"] = bb.bollinger_lband()
    df["BB_Mid"] = bb.bollinger_mavg()
    
    # 거래량 평균
    df["Volume_Avg"] = df["Volume"].rolling(window=20).mean()
    
    # 52주 고가/저가 (데이터가 252일 미만이면 전체 기간 사용)
    window_52w = min(252, len(df) - 1)
    df["High_52w"] = high.rolling(window=window_52w).max()
    df["Low_52w"] = low.rolling(window=window_52w).min()
    
    return df


def strategy_conservative_momentum(df: pd.DataFrame, symbol: str) -> dict | None:
    """전략 1: 보수적 모멘텀 (기존 전략)"""
    latest = df.iloc[-1]
    
    price = latest["Close"]
    ma50 = latest["MA50"]
    ma200 = latest["MA200"]
    rsi = latest["RSI"]
    volume = latest["Volume"]
    volume_avg = latest["Volume_Avg"]
    
    conditions = {
        "above_ma50": price > ma50,
        "above_ma200": price > ma200,
        "rsi_ok": 40 <= rsi <= 60,
        "volume_ok": volume > volume_avg * 0.8,
    }
    
    if all(conditions.values()):
        return {
            "symbol": symbol,
            "strategy": "보수적 모멘텀",
            "price": round(price, 2),
            "rsi": round(rsi, 1),
            "reason": f"RSI {rsi:.0f}, 50일선/200일선 위",
        }
    return None


def strategy_golden_cross(df: pd.DataFrame, symbol: str) -> dict | None:
    """전략 2: 골든크로스 (임박 또는 발생)"""
    if len(df) < 5:
        return None
    
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    
    ma5 = latest["MA5"]
    ma20 = latest["MA20"]
    ma5_prev = prev["MA5"]
    ma20_prev = prev["MA20"]
    price = latest["Close"]
    
    # 골든크로스 발생 (5일선이 20일선 돌파)
    if ma5_prev <= ma20_prev and ma5 > ma20:
        return {
            "symbol": symbol,
            "strategy": "골든크로스",
            "price": round(price, 2),
            "reason": "5일선이 20일선 상향 돌파",
        }
    
    # 골든크로스 임박 (5일선이 20일선에 근접)
    gap_pct = (ma20 - ma5) / ma20 * 100
    if 0 < gap_pct < 1 and ma5 > ma5_prev:  # 1% 이내 + 상승 중
        return {
            "symbol": symbol,
            "strategy": "골든크로스 임박",
            "price": round(price, 2),
            "reason": f"5일선이 20일선까지 {gap_pct:.1f}% 남음",
        }
    
    return None


def strategy_bollinger_bounce(df: pd.DataFrame, symbol: str) -> dict | None:
    """전략 3: 볼린저밴드 하단 반등"""
    if len(df) < 3:
        return None
    
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    
    price = latest["Close"]
    bb_lower = latest["BB_Lower"]
    bb_mid = latest["BB_Mid"]
    rsi = latest["RSI"]
    
    prev_price = prev["Close"]
    prev_bb_lower = prev["BB_Lower"]
    
    # 어제 하단 터치 + 오늘 반등 + RSI 과매도
    if prev_price <= prev_bb_lower * 1.01 and price > prev_price and rsi < 35:
        return {
            "symbol": symbol,
            "strategy": "볼린저 반등",
            "price": round(price, 2),
            "rsi": round(rsi, 1),
            "target": round(bb_mid, 2),
            "reason": f"하단 터치 후 반등, RSI {rsi:.0f}",
        }
    
    return None


def strategy_macd_crossover(df: pd.DataFrame, symbol: str) -> dict | None:
    """전략 4: MACD 골든크로스"""
    if len(df) < 3:
        return None
    
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    
    macd = latest["MACD"]
    signal = latest["MACD_Signal"]
    macd_prev = prev["MACD"]
    signal_prev = prev["MACD_Signal"]
    price = latest["Close"]
    
    # MACD가 시그널선 상향 돌파
    if macd_prev <= signal_prev and macd > signal:
        return {
            "symbol": symbol,
            "strategy": "MACD 크로스",
            "price": round(price, 2),
            "reason": "MACD 시그널선 상향 돌파",
        }
    
    return None


def strategy_near_52w_high(df: pd.DataFrame, symbol: str) -> dict | None:
    """전략 5: 52주 신고가 근접"""
    latest = df.iloc[-1]
    
    price = latest["Close"]
    high_52w = latest["High_52w"]
    ma50 = latest["MA50"]
    
    # 신고가 대비 -5% 이내 + 50일선 위
    gap_pct = (high_52w - price) / high_52w * 100
    
    if 0 < gap_pct <= 5 and price > ma50:
        return {
            "symbol": symbol,
            "strategy": "52주 신고가 근접",
            "price": round(price, 2),
            "high_52w": round(high_52w, 2),
            "reason": f"신고가 대비 -{gap_pct:.1f}%",
        }
    
    return None


def strategy_dip_bounce(df: pd.DataFrame, symbol: str) -> dict | None:
    """전략 6: 급락 후 반등"""
    if len(df) < 10:
        return None
    
    latest = df.iloc[-1]
    price = latest["Close"]
    rsi = latest["RSI"]
    
    # 최근 10일 고점
    recent_high = df["High"].iloc[-10:-1].max()
    drop_pct = (price - recent_high) / recent_high * 100
    
    # 최근 3일 추세 (반등 중인지)
    prices_3d = df["Close"].iloc[-3:].tolist()
    is_bouncing = prices_3d[-1] > prices_3d[-2] > prices_3d[-3]
    
    # -10% 이상 하락 + 반등 시작 + RSI 과매도 탈출 중
    if drop_pct <= -10 and is_bouncing and 30 < rsi < 45:
        return {
            "symbol": symbol,
            "strategy": "급락 반등",
            "price": round(price, 2),
            "drop_pct": round(drop_pct, 1),
            "rsi": round(rsi, 1),
            "reason": f"고점 대비 {drop_pct:.0f}% 후 반등 중",
        }
    
    return None


def strategy_volume_surge(df: pd.DataFrame, symbol: str) -> dict | None:
    """전략 7: 거래량 급증"""
    latest = df.iloc[-1]
    
    price = latest["Close"]
    volume = latest["Volume"]
    volume_avg = latest["Volume_Avg"]
    ma50 = latest["MA50"]
    
    if volume_avg == 0:
        return None
    
    volume_ratio = volume / volume_avg
    
    # 거래량 2배 이상 + 가격 상승 + 50일선 위
    prev_price = df["Close"].iloc[-2]
    price_change = (price - prev_price) / prev_price * 100
    
    if volume_ratio >= 2 and price_change > 0 and price > ma50:
        return {
            "symbol": symbol,
            "strategy": "거래량 급증",
            "price": round(price, 2),
            "volume_ratio": round(volume_ratio, 1),
            "price_change": round(price_change, 1),
            "reason": f"거래량 {volume_ratio:.1f}배, +{price_change:.1f}%",
        }
    
    return None


def analyze_risk_level(df: pd.DataFrame, symbol: str) -> dict:
    """종목 위험도 분석 (고점 + 하락 위험 모두 체크)"""
    latest = df.iloc[-1]
    
    price = latest["Close"]
    ma50 = latest["MA50"]
    ma200 = latest["MA200"]
    rsi = latest["RSI"]
    bb_upper = latest["BB_Upper"]
    bb_lower = latest["BB_Lower"]
    high_52w = latest["High_52w"]
    low_52w = latest["Low_52w"]
    
    warnings = []
    risk_score = 0  # 0~100, 높을수록 위험
    
    # === 고점 위험 (과매수) ===
    # 1. RSI 과매수 체크
    if rsi >= 70:
        warnings.append(f"⚠️ RSI {rsi:.0f} 과매수 (70 이상)")
        risk_score += 25
    elif rsi >= 60:
        warnings.append(f"🟡 RSI {rsi:.0f} 높음")
        risk_score += 10
    
    # 2. 볼린저밴드 상단 근접
    bb_position = (price - bb_lower) / (bb_upper - bb_lower) * 100 if (bb_upper - bb_lower) > 0 else 50
    if bb_position >= 95:
        warnings.append(f"⚠️ 볼린저 상단 돌파 (과열)")
        risk_score += 20
    elif bb_position >= 80:
        warnings.append(f"🟡 볼린저 상단 근접 ({bb_position:.0f}%)")
        risk_score += 10
    
    # 3. 52주 고점 대비 위치
    range_52w = high_52w - low_52w
    position_52w = (price - low_52w) / range_52w * 100 if range_52w > 0 else 50
    if position_52w >= 95:
        warnings.append(f"⚠️ 52주 최고점 근접 ({position_52w:.0f}%)")
        risk_score += 20
    elif position_52w >= 85:
        warnings.append(f"🟡 52주 고점권 ({position_52w:.0f}%)")
        risk_score += 10
    
    # 4. 이동평균선 괴리율 (상방)
    ma50_gap = (price - ma50) / ma50 * 100 if ma50 > 0 else 0
    if ma50_gap >= 20:
        warnings.append(f"⚠️ 50일선 대비 +{ma50_gap:.0f}% (과열)")
        risk_score += 20
    elif ma50_gap >= 10:
        warnings.append(f"🟡 50일선 대비 +{ma50_gap:.0f}%")
        risk_score += 10
    
    # 5. 최근 급등 체크 (5일간)
    price_5d_ago = df["Close"].iloc[-6] if len(df) >= 6 else price
    change_5d = (price - price_5d_ago) / price_5d_ago * 100
    if change_5d >= 20:
        warnings.append(f"⚠️ 5일간 +{change_5d:.0f}% 급등")
        risk_score += 15
    elif change_5d >= 10:
        warnings.append(f"🟡 5일간 +{change_5d:.0f}% 상승")
        risk_score += 5
    
    # === 하락 위험 (추세 약세) ===
    # 6. RSI 과매도
    if rsi <= 30:
        warnings.append(f"📉 RSI {rsi:.0f} 과매도 (바닥일 수도, 더 빠질 수도)")
        risk_score += 15
    elif rsi <= 40:
        warnings.append(f"📉 RSI {rsi:.0f} 낮음 (약세)")
        risk_score += 5
    
    # 7. 50일선 아래
    if ma50_gap <= -20:
        warnings.append(f"📉 50일선 대비 {ma50_gap:.0f}% (강한 하락)")
        risk_score += 25
    elif ma50_gap <= -10:
        warnings.append(f"📉 50일선 대비 {ma50_gap:.0f}% (하락 추세)")
        risk_score += 15
    elif ma50_gap < 0:
        warnings.append(f"📉 50일선 아래 ({ma50_gap:.0f}%)")
        risk_score += 5
    
    # 8. 200일선 아래 (장기 하락)
    ma200_gap = (price - ma200) / ma200 * 100 if ma200 > 0 else 0
    if ma200_gap <= -20:
        warnings.append(f"📉 200일선 대비 {ma200_gap:.0f}% (장기 약세)")
        risk_score += 20
    elif ma200_gap < 0:
        warnings.append(f"📉 200일선 아래 (장기 추세 약세)")
        risk_score += 10
    
    # 9. 최근 급락 (5일간)
    if change_5d <= -15:
        warnings.append(f"📉 5일간 {change_5d:.0f}% 급락")
        risk_score += 20
    elif change_5d <= -7:
        warnings.append(f"📉 5일간 {change_5d:.0f}% 하락")
        risk_score += 10
    
    # 10. 52주 저점 근접
    if position_52w <= 10:
        warnings.append(f"📉 52주 최저점 근접 ({position_52w:.0f}%)")
        risk_score += 15
    elif position_52w <= 20:
        warnings.append(f"📉 52주 저점권 ({position_52w:.0f}%)")
        risk_score += 5
    
    # 위험 등급 결정
    if risk_score >= 50:
        risk_grade = "🔴 고위험"
        recommendation = "매수 자제, 변동성 큼"
    elif risk_score >= 30:
        risk_grade = "🟡 주의"
        recommendation = "분할 매수 권장, 손절 철저히"
    else:
        risk_grade = "🟢 양호"
        recommendation = "매수 고려 가능"
    
    return {
        "symbol": symbol,
        "price": round(price, 2),
        "risk_score": risk_score,
        "risk_grade": risk_grade,
        "recommendation": recommendation,
        "warnings": warnings,
        "rsi": round(rsi, 1),
        "bb_position": round(bb_position, 0),
        "position_52w": round(position_52w, 0),
        "ma50_gap": round(ma50_gap, 1),
        "change_5d": round(change_5d, 1),
    }


# 모든 전략 리스트
ALL_STRATEGIES = [
    ("🎯", "보수적 모멘텀", strategy_conservative_momentum),
    ("✨", "골든크로스", strategy_golden_cross),
    ("📊", "볼린저 반등", strategy_bollinger_bounce),
    ("📈", "MACD 크로스", strategy_macd_crossover),
    ("🏆", "52주 신고가", strategy_near_52w_high),
    ("📉", "급락 반등", strategy_dip_bounce),
    ("🔥", "거래량 급증", strategy_volume_surge),
]
