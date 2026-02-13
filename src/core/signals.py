"""
매매 신호 모듈
- 진입 신호 (저점, 전략)
- 청산 신호 (손절, 익절)
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.stock_data import get_stock_data
from core.indicators import calculate_indicators


def check_entry_signal(symbol: str, target_price: float = 0) -> dict:
    """
    진입(매수) 신호 체크
    
    조건 (3개 이상 충족 시 신호):
    1. RSI 35 이하 (과매도)
    2. 볼린저 하단 근처 (20% 이하)
    3. 5일선 대비 -3% 이하
    4. 3일 이상 연속 하락
    5. 목표가 도달 (설정 시)
    """
    df = get_stock_data(symbol)
    if df is None:
        return {"error": "데이터 없음"}
    
    ind = calculate_indicators(df)
    if ind is None:
        return {"error": "지표 계산 실패"}
    
    conditions = {
        "rsi_oversold": ind["rsi"] <= 35,
        "bb_lower": ind["bb_position"] <= 20,
        "below_ma5": ind.get("ma5_gap", ind["ma50_gap"]) <= -3,
        "consecutive_down": ind["down_days"] >= 3,
        "target_reached": ind["price"] <= target_price if target_price > 0 else False,
    }
    
    met_count = sum(conditions.values())
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
        "ma5_gap": ind.get("ma5_gap", ind["ma50_gap"]),
        "ma50_gap": ind["ma50_gap"],
        "down_days": ind["down_days"],
    }


def check_exit_signal(symbol: str, buy_price: float, stop_loss: float = -7, take_profit: float = 15) -> dict:
    """
    청산(매도) 신호 체크
    
    조건:
    1. 손절: 매수가 대비 -7% (기본)
    2. 익절: 매수가 대비 +15% (기본)
    3. 추세 이탈: RSI 70+ 또는 50일선 이탈
    """
    df = get_stock_data(symbol)
    if df is None:
        return {"error": "데이터 없음"}
    
    ind = calculate_indicators(df)
    if ind is None:
        return {"error": "지표 계산 실패"}
    
    price = ind["price"]
    pnl_pct = (price - buy_price) / buy_price * 100
    
    signals = {
        "stop_loss": pnl_pct <= stop_loss,
        "take_profit": pnl_pct >= take_profit,
        "rsi_overbought": ind["rsi"] >= 70,
        "below_ma50": ind["ma50_gap"] < -5,
    }
    
    is_exit = any(signals.values())
    
    if signals["stop_loss"]:
        reason = f"손절 ({pnl_pct:.1f}%)"
        urgency = "즉시"
    elif signals["take_profit"]:
        reason = f"익절 ({pnl_pct:.1f}%)"
        urgency = "권장"
    elif signals["rsi_overbought"]:
        reason = f"RSI 과매수 ({ind['rsi']:.0f})"
        urgency = "고려"
    elif signals["below_ma50"]:
        reason = "50일선 이탈"
        urgency = "고려"
    else:
        reason = "없음"
        urgency = "없음"
    
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


def check_strategies(symbol: str) -> list[dict]:
    """전략별 매수 신호 체크"""
    df = get_stock_data(symbol)
    if df is None:
        return []
    
    ind = calculate_indicators(df)
    if ind is None:
        return []
    
    signals = []
    
    # 1. 보수적 모멘텀
    if (ind["price"] > ind["ma50"] and ind["price"] > ind["ma200"] and 
        40 <= ind["rsi"] <= 60 and ind["volume"] > ind["volume_avg"] * 0.8):
        signals.append({
            "strategy": "보수적 모멘텀",
            "emoji": "🎯",
            "reason": f"RSI {ind['rsi']:.0f}, 이평선 위",
            "risk": "낮음"
        })
    
    # 2. 골든크로스
    if ind["ma5_prev"] <= ind["ma20_prev"] and ind["ma5"] > ind["ma20"]:
        signals.append({
            "strategy": "골든크로스",
            "emoji": "✨",
            "reason": "5일선이 20일선 돌파",
            "risk": "중간"
        })
    
    # 3. 볼린저 반등
    if (ind["price_prev"] <= ind["bb_lower_prev"] * 1.01 and 
        ind["price"] > ind["price_prev"] and ind["rsi"] < 35):
        signals.append({
            "strategy": "볼린저 반등",
            "emoji": "📊",
            "reason": f"하단 터치 후 반등, RSI {ind['rsi']:.0f}",
            "risk": "중간"
        })
    
    # 4. MACD 크로스
    if ind["macd_prev"] <= ind["macd_signal_prev"] and ind["macd"] > ind["macd_signal"]:
        signals.append({
            "strategy": "MACD 크로스",
            "emoji": "📈",
            "reason": "MACD 시그널선 돌파",
            "risk": "중간"
        })
    
    # 5. 52주 신고가 근접
    gap_52w = (ind["high_52w"] - ind["price"]) / ind["high_52w"] * 100
    if 0 < gap_52w <= 5 and ind["price"] > ind["ma50"]:
        signals.append({
            "strategy": "52주 신고가",
            "emoji": "🏆",
            "reason": f"신고가 대비 -{gap_52w:.1f}%",
            "risk": "높음"
        })
    
    # 6. 거래량 급증
    if ind["volume_avg"] > 0:
        vol_ratio = ind["volume"] / ind["volume_avg"]
        price_change = (ind["price"] - ind["price_prev"]) / ind["price_prev"] * 100
        if vol_ratio >= 2 and price_change > 0 and ind["price"] > ind["ma50"]:
            signals.append({
                "strategy": "거래량 급증",
                "emoji": "🔥",
                "reason": f"거래량 {vol_ratio:.1f}배, +{price_change:.1f}%",
                "risk": "중간"
            })
    
    return signals


def scan_stocks(symbols: list[str]) -> dict:
    """여러 종목 스캔"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from core.scoring import calculate_score
    
    results = []
    
    def analyze(symbol):
        try:
            df = get_stock_data(symbol)
            if df is None:
                return None
            
            ind = calculate_indicators(df)
            if ind is None:
                return None
            
            strategies = check_strategies(symbol)
            score = calculate_score({"symbol": symbol, **ind})
            
            strategy_count = len(strategies)
            risk_score = score.get("risk", {}).get("score", 50)
            quality_score = score.get("total_score", 0) + min(12, strategy_count * 4) - max(0, risk_score - 45) * 0.2

            return {
                "symbol": symbol,
                "price": ind["price"],
                "rsi": ind["rsi"],
                "ma50_gap": ind["ma50_gap"],
                "position_52w": ind["position_52w"],
                "strategies": strategies,
                "strategy_count": strategy_count,
                "quality_score": round(quality_score, 1),
                "score": score,
            }
        except:
            return None
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze, s): s for s in symbols}
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
    
    return {
        "results": results,
        "total": len(results),
    }
