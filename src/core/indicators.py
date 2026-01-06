"""
기술적 지표 계산 모듈
"""
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import SMAIndicator, MACD
from ta.volatility import BollingerBands


def calculate_indicators(df: pd.DataFrame) -> dict | None:
    """모든 기술적 지표 계산"""
    if df is None or len(df) < 200:
        return None
    
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    
    # 이동평균선
    ma5 = SMAIndicator(close, window=5).sma_indicator()
    ma20 = SMAIndicator(close, window=20).sma_indicator()
    ma50 = SMAIndicator(close, window=50).sma_indicator()
    ma200 = SMAIndicator(close, window=200).sma_indicator()
    
    # RSI
    rsi = RSIIndicator(close, window=14).rsi()
    
    # MACD
    macd_ind = MACD(close)
    macd = macd_ind.macd()
    macd_signal = macd_ind.macd_signal()
    macd_hist = macd_ind.macd_diff()
    
    # 볼린저밴드
    bb = BollingerBands(close, window=20, window_dev=2)
    bb_upper = bb.bollinger_hband()
    bb_lower = bb.bollinger_lband()
    bb_mid = bb.bollinger_mavg()
    
    # 거래량 평균
    volume_avg = df["Volume"].rolling(window=20).mean()
    
    # 52주 고가/저가
    window_52w = min(252, len(df) - 1)
    high_52w = high.rolling(window=window_52w).max()
    low_52w = low.rolling(window=window_52w).min()
    
    # 최신 값 추출
    latest = df.iloc[-1]
    price = float(latest["Close"])
    
    # 볼린저 위치 (0-100)
    bb_pos = (price - float(bb_lower.iloc[-1])) / (float(bb_upper.iloc[-1]) - float(bb_lower.iloc[-1])) * 100
    
    # 52주 위치 (0-100)
    range_52w = float(high_52w.iloc[-1]) - float(low_52w.iloc[-1])
    pos_52w = (price - float(low_52w.iloc[-1])) / range_52w * 100 if range_52w > 0 else 50
    
    # 5일 변화율
    price_5d_ago = float(df["Close"].iloc[-6]) if len(df) >= 6 else price
    change_5d = (price - price_5d_ago) / price_5d_ago * 100
    
    # 최근 5일 하락일 수
    down_days = int(sum(df["Close"].tail(5).diff().dropna() < 0))
    
    return {
        "price": round(price, 2),
        "ma5": round(float(ma5.iloc[-1]), 2),
        "ma20": round(float(ma20.iloc[-1]), 2),
        "ma50": round(float(ma50.iloc[-1]), 2),
        "ma200": round(float(ma200.iloc[-1]), 2),
        "rsi": round(float(rsi.iloc[-1]), 1),
        "macd": round(float(macd.iloc[-1]), 3),
        "macd_signal": round(float(macd_signal.iloc[-1]), 3),
        "macd_hist": round(float(macd_hist.iloc[-1]), 3),
        "bb_upper": round(float(bb_upper.iloc[-1]), 2),
        "bb_lower": round(float(bb_lower.iloc[-1]), 2),
        "bb_mid": round(float(bb_mid.iloc[-1]), 2),
        "bb_position": round(bb_pos, 1),
        "volume": int(latest["Volume"]),
        "volume_avg": int(volume_avg.iloc[-1]),
        "high_52w": round(float(high_52w.iloc[-1]), 2),
        "low_52w": round(float(low_52w.iloc[-1]), 2),
        "position_52w": round(pos_52w, 1),
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
