"""
기술적 지표 계산 모듈 (확장판)
- 캔들 패턴, 거래량 분석, 지지/저항, 추가 지표
"""
import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import SMAIndicator, EMAIndicator, MACD, ADXIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator, VolumeWeightedAveragePrice


def calculate_indicators(df: pd.DataFrame) -> dict | None:
    """모든 기술적 지표 계산"""
    if df is None or len(df) < 200:
        return None
    
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    open_ = df["Open"]
    volume = df["Volume"]
    
    # === 이동평균선 ===
    ma5 = SMAIndicator(close, window=5).sma_indicator()
    ma20 = SMAIndicator(close, window=20).sma_indicator()
    ma50 = SMAIndicator(close, window=50).sma_indicator()
    ma200 = SMAIndicator(close, window=200).sma_indicator()
    ema12 = EMAIndicator(close, window=12).ema_indicator()
    ema26 = EMAIndicator(close, window=26).ema_indicator()
    
    # === RSI ===
    rsi = RSIIndicator(close, window=14).rsi()
    
    # === 스토캐스틱 ===
    stoch = StochasticOscillator(high, low, close, window=14, smooth_window=3)
    stoch_k = stoch.stoch()
    stoch_d = stoch.stoch_signal()
    
    # === MACD ===
    macd_ind = MACD(close)
    macd = macd_ind.macd()
    macd_signal = macd_ind.macd_signal()
    macd_hist = macd_ind.macd_diff()
    
    # === 볼린저밴드 ===
    bb = BollingerBands(close, window=20, window_dev=2)
    bb_upper = bb.bollinger_hband()
    bb_lower = bb.bollinger_lband()
    bb_mid = bb.bollinger_mavg()
    
    # === ATR (변동성) ===
    atr = AverageTrueRange(high, low, close, window=14).average_true_range()
    
    # === ADX (추세 강도) ===
    adx = ADXIndicator(high, low, close, window=14).adx()
    
    # === OBV (거래량) ===
    obv = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
    
    # === 거래량 분석 ===
    volume_avg = volume.rolling(window=20).mean()
    volume_avg_5 = volume.rolling(window=5).mean()
    
    # === 52주 고가/저가 ===
    window_52w = min(252, len(df) - 1)
    high_52w = high.rolling(window=window_52w).max()
    low_52w = low.rolling(window=window_52w).min()
    
    # === 최신 값 추출 ===
    latest = df.iloc[-1]
    price = float(latest["Close"])
    
    # 볼린저 위치 (0-100)
    bb_range = float(bb_upper.iloc[-1]) - float(bb_lower.iloc[-1])
    bb_pos = (price - float(bb_lower.iloc[-1])) / bb_range * 100 if bb_range > 0 else 50
    
    # 52주 위치 (0-100)
    range_52w = float(high_52w.iloc[-1]) - float(low_52w.iloc[-1])
    pos_52w = (price - float(low_52w.iloc[-1])) / range_52w * 100 if range_52w > 0 else 50
    
    # 5일 변화율
    price_5d_ago = float(df["Close"].iloc[-6]) if len(df) >= 6 else price
    change_5d = (price - price_5d_ago) / price_5d_ago * 100
    
    # 최근 5일 하락일 수
    down_days = int(sum(df["Close"].tail(5).diff().dropna() < 0))
    
    # === 캔들 패턴 분석 ===
    candle_patterns = detect_candle_patterns(df)
    
    # === 지지/저항선 ===
    support, resistance = find_support_resistance(df)
    
    # === 크로스 신호 ===
    crosses = detect_crosses(df, ma5, ma20, ma50, ma200, macd, macd_signal)
    
    # === 거래량 신호 ===
    vol_signal = analyze_volume(df, volume_avg)
    
    # === 피보나치 되돌림 ===
    fib_levels = calculate_fibonacci(float(high_52w.iloc[-1]), float(low_52w.iloc[-1]))
    
    return {
        "price": round(price, 2),
        # 이동평균선
        "ma5": round(float(ma5.iloc[-1]), 2),
        "ma20": round(float(ma20.iloc[-1]), 2),
        "ma50": round(float(ma50.iloc[-1]), 2),
        "ma200": round(float(ma200.iloc[-1]), 2),
        "ema12": round(float(ema12.iloc[-1]), 2),
        "ema26": round(float(ema26.iloc[-1]), 2),
        # RSI & 스토캐스틱
        "rsi": round(float(rsi.iloc[-1]), 1),
        "stoch_k": round(float(stoch_k.iloc[-1]), 1),
        "stoch_d": round(float(stoch_d.iloc[-1]), 1),
        # MACD
        "macd": round(float(macd.iloc[-1]), 3),
        "macd_signal": round(float(macd_signal.iloc[-1]), 3),
        "macd_hist": round(float(macd_hist.iloc[-1]), 3),
        # 볼린저밴드
        "bb_upper": round(float(bb_upper.iloc[-1]), 2),
        "bb_lower": round(float(bb_lower.iloc[-1]), 2),
        "bb_mid": round(float(bb_mid.iloc[-1]), 2),
        "bb_position": round(bb_pos, 1),
        # 변동성 & 추세
        "atr": round(float(atr.iloc[-1]), 2),
        "atr_pct": round(float(atr.iloc[-1]) / price * 100, 2),
        "adx": round(float(adx.iloc[-1]), 1),
        # 거래량
        "volume": int(latest["Volume"]),
        "volume_avg": int(volume_avg.iloc[-1]),
        "volume_ratio": round(float(latest["Volume"]) / float(volume_avg.iloc[-1]), 2) if volume_avg.iloc[-1] > 0 else 1,
        "obv": int(obv.iloc[-1]),
        "obv_change": round((float(obv.iloc[-1]) - float(obv.iloc[-6])) / abs(float(obv.iloc[-6])) * 100, 1) if obv.iloc[-6] != 0 else 0,
        # 52주
        "high_52w": round(float(high_52w.iloc[-1]), 2),
        "low_52w": round(float(low_52w.iloc[-1]), 2),
        "position_52w": round(pos_52w, 1),
        # 갭
        "ma50_gap": round((price - float(ma50.iloc[-1])) / float(ma50.iloc[-1]) * 100, 1),
        "ma200_gap": round((price - float(ma200.iloc[-1])) / float(ma200.iloc[-1]) * 100, 1),
        "change_5d": round(change_5d, 1),
        "down_days": down_days,
        # 이전 값 (크로스 체크용)
        "ma5_prev": round(float(ma5.iloc[-2]), 2),
        "ma20_prev": round(float(ma20.iloc[-2]), 2),
        "macd_prev": round(float(macd.iloc[-2]), 3),
        "macd_signal_prev": round(float(macd_signal.iloc[-2]), 3),
        "price_prev": round(float(df["Close"].iloc[-2]), 2),
        "bb_lower_prev": round(float(bb_lower.iloc[-2]), 2),
        # 캔들 패턴
        "candle_patterns": candle_patterns,
        # 지지/저항
        "support": support,
        "resistance": resistance,
        # 크로스 신호
        "crosses": crosses,
        # 거래량 신호
        "volume_signal": vol_signal,
        # 피보나치
        "fib_levels": fib_levels,
    }


def detect_candle_patterns(df: pd.DataFrame) -> list[dict]:
    """캔들스틱 패턴 감지"""
    patterns = []
    
    if len(df) < 5:
        return patterns
    
    # 최근 5일 데이터
    recent = df.tail(5)
    
    for i in range(len(recent)):
        row = recent.iloc[i]
        o, h, l, c = row["Open"], row["High"], row["Low"], row["Close"]
        body = abs(c - o)
        upper_shadow = h - max(o, c)
        lower_shadow = min(o, c) - l
        total_range = h - l
        
        if total_range == 0:
            continue
        
        date = recent.index[i].strftime("%m/%d") if hasattr(recent.index[i], 'strftime') else str(i)
        
        # 도지 (Doji) - 몸통이 매우 작음
        if body / total_range < 0.1:
            patterns.append({"date": date, "pattern": "도지", "signal": "중립", "desc": "추세 전환 가능성"})
        
        # 망치형 (Hammer) - 하락 추세에서 긴 아래꼬리
        elif lower_shadow > body * 2 and upper_shadow < body * 0.5 and c > o:
            patterns.append({"date": date, "pattern": "망치형", "signal": "매수", "desc": "하락 추세 반전 신호"})
        
        # 역망치형 (Inverted Hammer)
        elif upper_shadow > body * 2 and lower_shadow < body * 0.5 and c > o:
            patterns.append({"date": date, "pattern": "역망치형", "signal": "매수", "desc": "상승 반전 가능"})
        
        # 교수형 (Hanging Man) - 상승 추세에서 긴 아래꼬리
        elif lower_shadow > body * 2 and upper_shadow < body * 0.5 and c < o:
            patterns.append({"date": date, "pattern": "교수형", "signal": "매도", "desc": "상승 추세 약화"})
        
        # 유성형 (Shooting Star)
        elif upper_shadow > body * 2 and lower_shadow < body * 0.5 and c < o:
            patterns.append({"date": date, "pattern": "유성형", "signal": "매도", "desc": "하락 반전 신호"})
        
        # 장대양봉 (Bullish Marubozu)
        elif c > o and body / total_range > 0.8:
            patterns.append({"date": date, "pattern": "장대양봉", "signal": "강한매수", "desc": "강한 매수세"})
        
        # 장대음봉 (Bearish Marubozu)
        elif c < o and body / total_range > 0.8:
            patterns.append({"date": date, "pattern": "장대음봉", "signal": "강한매도", "desc": "강한 매도세"})
    
    # 2일 패턴
    if len(recent) >= 2:
        prev, curr = recent.iloc[-2], recent.iloc[-1]
        
        # 상승장악형 (Bullish Engulfing)
        if prev["Close"] < prev["Open"] and curr["Close"] > curr["Open"]:
            if curr["Open"] < prev["Close"] and curr["Close"] > prev["Open"]:
                patterns.append({"date": "최근", "pattern": "상승장악형", "signal": "강한매수", "desc": "강력한 상승 반전"})
        
        # 하락장악형 (Bearish Engulfing)
        if prev["Close"] > prev["Open"] and curr["Close"] < curr["Open"]:
            if curr["Open"] > prev["Close"] and curr["Close"] < prev["Open"]:
                patterns.append({"date": "최근", "pattern": "하락장악형", "signal": "강한매도", "desc": "강력한 하락 반전"})
    
    # 갭 분석
    if len(recent) >= 2:
        prev_close = recent.iloc[-2]["Close"]
        curr_open = recent.iloc[-1]["Open"]
        gap_pct = (curr_open - prev_close) / prev_close * 100
        
        if gap_pct > 2:
            patterns.append({"date": "오늘", "pattern": "갭상승", "signal": "매수", "desc": f"+{gap_pct:.1f}% 갭"})
        elif gap_pct < -2:
            patterns.append({"date": "오늘", "pattern": "갭하락", "signal": "매도", "desc": f"{gap_pct:.1f}% 갭"})
    
    return patterns[-5:]  # 최근 5개만


def find_support_resistance(df: pd.DataFrame, window: int = 20) -> tuple:
    """지지선/저항선 계산"""
    if len(df) < window:
        return [], []
    
    recent = df.tail(60)
    highs = recent["High"].values
    lows = recent["Low"].values
    close = float(df["Close"].iloc[-1])
    
    # 피벗 포인트 찾기
    supports = []
    resistances = []
    
    for i in range(2, len(recent) - 2):
        # 저항선 (로컬 고점)
        if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
            if highs[i] > close:
                resistances.append(round(highs[i], 2))
        
        # 지지선 (로컬 저점)
        if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
            if lows[i] < close:
                supports.append(round(lows[i], 2))
    
    # 중복 제거 및 정렬
    supports = sorted(list(set(supports)), reverse=True)[:3]
    resistances = sorted(list(set(resistances)))[:3]
    
    return supports, resistances


def detect_crosses(df, ma5, ma20, ma50, ma200, macd, macd_signal) -> list[dict]:
    """크로스 신호 감지"""
    crosses = []
    
    # 골든크로스 (5일선 > 20일선)
    if ma5.iloc[-2] <= ma20.iloc[-2] and ma5.iloc[-1] > ma20.iloc[-1]:
        crosses.append({"type": "골든크로스", "detail": "5일선↗20일선", "signal": "매수"})
    
    # 데드크로스 (5일선 < 20일선)
    if ma5.iloc[-2] >= ma20.iloc[-2] and ma5.iloc[-1] < ma20.iloc[-1]:
        crosses.append({"type": "데드크로스", "detail": "5일선↘20일선", "signal": "매도"})
    
    # 장기 골든크로스 (50일선 > 200일선)
    if ma50.iloc[-2] <= ma200.iloc[-2] and ma50.iloc[-1] > ma200.iloc[-1]:
        crosses.append({"type": "장기골든크로스", "detail": "50일선↗200일선", "signal": "강한매수"})
    
    # 장기 데드크로스 (50일선 < 200일선)
    if ma50.iloc[-2] >= ma200.iloc[-2] and ma50.iloc[-1] < ma200.iloc[-1]:
        crosses.append({"type": "장기데드크로스", "detail": "50일선↘200일선", "signal": "강한매도"})
    
    # MACD 크로스
    if macd.iloc[-2] <= macd_signal.iloc[-2] and macd.iloc[-1] > macd_signal.iloc[-1]:
        crosses.append({"type": "MACD골든", "detail": "MACD↗시그널", "signal": "매수"})
    
    if macd.iloc[-2] >= macd_signal.iloc[-2] and macd.iloc[-1] < macd_signal.iloc[-1]:
        crosses.append({"type": "MACD데드", "detail": "MACD↘시그널", "signal": "매도"})
    
    return crosses


def analyze_volume(df: pd.DataFrame, volume_avg) -> dict:
    """거래량 분석"""
    curr_vol = df["Volume"].iloc[-1]
    avg_vol = volume_avg.iloc[-1]
    
    if avg_vol == 0:
        return {"signal": "중립", "ratio": 1, "desc": "데이터 부족"}
    
    ratio = curr_vol / avg_vol
    price_change = (df["Close"].iloc[-1] - df["Close"].iloc[-2]) / df["Close"].iloc[-2] * 100
    
    if ratio >= 3:
        if price_change > 0:
            return {"signal": "강한매수", "ratio": round(ratio, 1), "desc": f"거래량 {ratio:.1f}배 폭증 + 상승"}
        else:
            return {"signal": "강한매도", "ratio": round(ratio, 1), "desc": f"거래량 {ratio:.1f}배 폭증 + 하락"}
    elif ratio >= 2:
        if price_change > 0:
            return {"signal": "매수", "ratio": round(ratio, 1), "desc": f"거래량 {ratio:.1f}배 급증 + 상승"}
        else:
            return {"signal": "매도", "ratio": round(ratio, 1), "desc": f"거래량 {ratio:.1f}배 급증 + 하락"}
    elif ratio >= 1.5:
        return {"signal": "관심", "ratio": round(ratio, 1), "desc": f"거래량 {ratio:.1f}배 증가"}
    elif ratio <= 0.5:
        return {"signal": "중립", "ratio": round(ratio, 1), "desc": "거래량 감소 (관망)"}
    else:
        return {"signal": "중립", "ratio": round(ratio, 1), "desc": "평균 거래량"}


def calculate_fibonacci(high: float, low: float) -> dict:
    """피보나치 되돌림 레벨 계산"""
    diff = high - low
    return {
        "0.0": round(low, 2),
        "0.236": round(low + diff * 0.236, 2),
        "0.382": round(low + diff * 0.382, 2),
        "0.5": round(low + diff * 0.5, 2),
        "0.618": round(low + diff * 0.618, 2),
        "0.786": round(low + diff * 0.786, 2),
        "1.0": round(high, 2),
    }


def get_full_analysis(symbol: str) -> dict | None:
    """종목 전체 분석 (데이터 + 지표 + 정보)"""
    from core.stock_data import get_stock_data, get_stock_info, get_finviz_data
    
    df = get_stock_data(symbol)
    if df is None:
        return None
    
    indicators = calculate_indicators(df)
    if indicators is None:
        return None
    
    info = get_stock_info(symbol)
    finviz = get_finviz_data(symbol) or {}
    
    return {
        "symbol": symbol,
        **indicators,
        **{k: v for k, v in info.items() if k != "symbol"},
        "finviz": finviz,
    }
