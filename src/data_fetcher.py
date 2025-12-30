import yfinance as yf
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import SMAIndicator
from config import MARKET_INDICATOR


def get_stock_data(symbol: str, period: str = "1y") -> pd.DataFrame:
    """개별 종목 데이터 가져오기"""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period)
        if df.empty:
            return None
        return df
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None


def check_market_condition() -> dict:
    """시장 전체 상태 체크 (QQQ 기준)"""
    from strategies import add_all_indicators
    
    df = get_stock_data(MARKET_INDICATOR)
    if df is None:
        return {"status": "unknown", "emoji": "⚪", "message": "데이터 없음", "price": 0, "ma50": 0, "ma200": 0}
    
    df = add_all_indicators(df)
    if df is None:
        return {"status": "unknown", "emoji": "⚪", "message": "지표 계산 실패", "price": 0, "ma50": 0, "ma200": 0}
    
    latest = df.iloc[-1]
    price = latest["Close"]
    ma50 = latest["MA50"]
    ma200 = latest["MA200"]
    
    if price > ma50 and price > ma200:
        status = "bullish"
        emoji = "🟢"
        msg = "상승 추세 - 매수 가능"
    elif price > ma50:
        status = "neutral"
        emoji = "🟡"
        msg = "중립 - 신중하게"
    else:
        status = "bearish"
        emoji = "🔴"
        msg = "하락 추세 - 매수 자제"
    
    return {
        "status": status,
        "emoji": emoji,
        "message": msg,
        "price": round(price, 2),
        "ma50": round(ma50, 2),
        "ma200": round(ma200, 2),
    }
