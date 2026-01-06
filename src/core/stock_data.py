"""
주식 데이터 수집 모듈
- yfinance: 가격, 재무제표
- finviz: 밸류에이션, 애널리스트
"""
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from functools import lru_cache
from datetime import datetime, timedelta
import pandas as pd


@lru_cache(maxsize=200)
def get_stock_data(symbol: str, period: str = "15mo") -> pd.DataFrame | None:
    """주가 데이터 가져오기 (캐시됨)"""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period)
        return df if not df.empty else None
    except Exception as e:
        print(f"[{symbol}] 데이터 오류: {e}")
        return None


def get_stock_info(symbol: str) -> dict:
    """종목 기본 정보 + 재무제표"""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        return {
            "symbol": symbol,
            "name": info.get("shortName", symbol),
            "sector": info.get("sector", "N/A"),
            "industry": info.get("industry", "N/A"),
            "price": info.get("currentPrice") or info.get("regularMarketPrice", 0),
            "market_cap": info.get("marketCap", 0),
            # 수익성
            "roe": info.get("returnOnEquity", 0),
            "roa": info.get("returnOnAssets", 0),
            "profit_margin": info.get("profitMargins", 0),
            "operating_margin": info.get("operatingMargins", 0),
            # 밸류에이션
            "pe": info.get("trailingPE", 0),
            "forward_pe": info.get("forwardPE", 0),
            "peg": info.get("pegRatio", 0),
            "pb": info.get("priceToBook", 0),
            # 성장성
            "revenue_growth": info.get("revenueGrowth", 0),
            "earnings_growth": info.get("earningsGrowth", 0),
            # 재무건전성
            "debt_to_equity": info.get("debtToEquity", 0),
            "current_ratio": info.get("currentRatio", 0),
            "free_cash_flow": info.get("freeCashflow", 0),
            # 배당
            "dividend_yield": info.get("dividendYield", 0),
            # 애널리스트
            "target_price": info.get("targetMeanPrice", 0),
            "recommendation": info.get("recommendationKey", "N/A"),
            # 변동성
            "beta": info.get("beta", 1),
            "52w_high": info.get("fiftyTwoWeekHigh", 0),
            "52w_low": info.get("fiftyTwoWeekLow", 0),
        }
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}


def get_finviz_data(symbol: str) -> dict | None:
    """Finviz에서 추가 데이터"""
    try:
        url = f"https://finviz.com/quote.ashx?t={symbol}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
        data = {}
        
        table = soup.find("table", class_="snapshot-table2")
        if table:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                for i in range(0, len(cells) - 1, 2):
                    key = cells[i].text.strip()
                    value = cells[i + 1].text.strip()
                    data[key] = value
        
        return {
            "symbol": symbol,
            "pe": data.get("P/E", "N/A"),
            "forward_pe": data.get("Forward P/E", "N/A"),
            "peg": data.get("PEG", "N/A"),
            "pb": data.get("P/B", "N/A"),
            "ps": data.get("P/S", "N/A"),
            "roe": data.get("ROE", "N/A"),
            "roa": data.get("ROA", "N/A"),
            "debt_eq": data.get("Debt/Eq", "N/A"),
            "eps": data.get("EPS (ttm)", "N/A"),
            "dividend": data.get("Dividend %", "N/A"),
            "rsi": data.get("RSI (14)", "N/A"),
            "target_price": data.get("Target Price", "N/A"),
            "price": data.get("Price", "N/A"),
            "change": data.get("Change", "N/A"),
            "volume": data.get("Volume", "N/A"),
            "rel_volume": data.get("Rel Volume", "N/A"),
            "short_float": data.get("Short Float", "N/A"),
            "sector": data.get("Sector", "N/A"),
            "industry": data.get("Industry", "N/A"),
        }
    except Exception as e:
        return None


def get_market_condition() -> dict:
    """시장 전체 상태 (QQQ 기준)"""
    from core.indicators import calculate_indicators
    
    df = get_stock_data("QQQ")
    if df is None:
        return {"status": "unknown", "emoji": "⚪", "message": "데이터 없음"}
    
    indicators = calculate_indicators(df)
    if indicators is None:
        return {"status": "unknown", "emoji": "⚪", "message": "지표 계산 실패"}
    
    price = indicators["price"]
    ma50 = indicators["ma50"]
    ma200 = indicators["ma200"]
    
    if price > ma50 and price > ma200:
        return {"status": "bullish", "emoji": "🟢", "message": "상승 추세", "price": price, "ma50": ma50, "ma200": ma200}
    elif price > ma50:
        return {"status": "neutral", "emoji": "🟡", "message": "중립", "price": price, "ma50": ma50, "ma200": ma200}
    else:
        return {"status": "bearish", "emoji": "🔴", "message": "하락 추세", "price": price, "ma50": ma50, "ma200": ma200}


def get_fear_greed_index() -> dict:
    """공포탐욕 지수 (Alternative API)"""
    try:
        # Alternative Fear & Greed API
        url = "https://api.alternative.me/fng/?limit=1"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            item = data.get("data", [{}])[0]
            score = int(item.get("value", 50))
            classification = item.get("value_classification", "Neutral")
            
            # 한글 변환
            rating_map = {
                "Extreme Fear": "극단적 공포",
                "Fear": "공포", 
                "Neutral": "중립",
                "Greed": "탐욕",
                "Extreme Greed": "극단적 탐욕"
            }
            rating = rating_map.get(classification, classification)
            
            if score <= 25:
                emoji, advice = "😱", "극단적 공포 - 매수 기회?"
            elif score <= 45:
                emoji, advice = "😰", "공포 - 신중한 매수"
            elif score <= 55:
                emoji, advice = "😐", "중립 - 관망"
            elif score <= 75:
                emoji, advice = "😊", "탐욕 - 신중하게"
            else:
                emoji, advice = "🤑", "극단적 탐욕 - 주의!"
            
            return {"score": score, "rating": rating, "emoji": emoji, "advice": advice}
    except Exception as e:
        print(f"Fear & Greed API 실패: {e}")
    
    return {"score": 50, "rating": "N/A", "emoji": "😐", "advice": "데이터 없음"}
