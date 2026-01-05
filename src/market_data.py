"""
외부 사이트 데이터 수집 모듈
- CNN Fear & Greed Index
- Finviz (섹터 맵, 스크리너)
- TipRanks (애널리스트 의견)
- Seeking Alpha (기업 분석)
- Marketscreener (사업부별 매출)
- ETF.com (ETF 정보)
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


# ============================================
# CNN Fear & Greed Index
# ============================================
def get_fear_greed_index() -> dict:
    """CNN Fear & Greed Index 가져오기"""
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        response = requests.get(url, headers=HEADERS, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            score = data.get("fear_and_greed", {}).get("score", 0)
            rating = data.get("fear_and_greed", {}).get("rating", "")
            
            # 한글 변환
            rating_kr = {
                "extreme fear": "극단적 공포 😱",
                "fear": "공포 😰",
                "neutral": "중립 😐",
                "greed": "탐욕 😏",
                "extreme greed": "극단적 탐욕 🤑"
            }.get(rating.lower(), rating)
            
            # 이모지 결정
            if score <= 25:
                emoji = "🔴"
                advice = "극단적 공포 구간 - 역발상 매수 기회일 수 있음"
            elif score <= 45:
                emoji = "🟠"
                advice = "공포 구간 - 신중하게 분할 매수 고려"
            elif score <= 55:
                emoji = "🟡"
                advice = "중립 구간 - 관망 또는 기존 전략 유지"
            elif score <= 75:
                emoji = "🟢"
                advice = "탐욕 구간 - 추격 매수 주의"
            else:
                emoji = "🔵"
                advice = "극단적 탐욕 - 차익 실현 고려, 신규 매수 자제"
            
            return {
                "score": round(score),
                "rating": rating_kr,
                "emoji": emoji,
                "advice": advice,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")
            }
    except Exception as e:
        print(f"Fear & Greed 가져오기 실패: {e}")
    
    return {"score": 0, "rating": "데이터 없음", "emoji": "⚪", "advice": ""}


# ============================================
# Finviz - 시장 개요 및 섹터 성과
# ============================================
def get_finviz_market_overview() -> dict:
    """Finviz에서 시장 개요 가져오기"""
    try:
        url = "https://finviz.com/"
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 주요 지수
        indices = {}
        for row in soup.select("table.t-home-table tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                name = cells[0].get_text(strip=True)
                change = cells[1].get_text(strip=True)
                if name in ["S&P 500", "DOW", "NASDAQ"]:
                    indices[name] = change
        
        return {"indices": indices, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")}
    except Exception as e:
        print(f"Finviz 시장 개요 실패: {e}")
        return {"indices": {}}


def get_finviz_sector_performance() -> list:
    """Finviz에서 섹터별 성과 가져오기"""
    try:
        url = "https://finviz.com/groups.ashx?g=sector&v=110&o=name"
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        
        sectors = []
        table = soup.find("table", {"class": "table-light"})
        if table:
            rows = table.find_all("tr")[1:]  # 헤더 제외
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 10:
                    name = cells[1].get_text(strip=True)
                    change = cells[3].get_text(strip=True)
                    volume = cells[8].get_text(strip=True)
                    
                    # 이모지 결정
                    try:
                        change_val = float(change.replace("%", ""))
                        emoji = "🟢" if change_val > 0 else "🔴" if change_val < 0 else "⚪"
                    except:
                        emoji = "⚪"
                    
                    sectors.append({
                        "name": name,
                        "change": change,
                        "volume": volume,
                        "emoji": emoji
                    })
        
        return sectors
    except Exception as e:
        print(f"Finviz 섹터 성과 실패: {e}")
        return []


def get_finviz_stock_data(symbol: str) -> dict:
    """Finviz에서 개별 종목 데이터 가져오기"""
    try:
        url = f"https://finviz.com/quote.ashx?t={symbol}"
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        
        data = {}
        table = soup.find("table", {"class": "snapshot-table2"})
        if table:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                for i in range(0, len(cells) - 1, 2):
                    key = cells[i].get_text(strip=True)
                    value = cells[i + 1].get_text(strip=True)
                    data[key] = value
        
        # 주요 지표 추출
        return {
            "symbol": symbol,
            "price": data.get("Price", "N/A"),
            "change": data.get("Change", "N/A"),
            "pe": data.get("P/E", "N/A"),
            "forward_pe": data.get("Forward P/E", "N/A"),
            "peg": data.get("PEG", "N/A"),
            "ps": data.get("P/S", "N/A"),
            "pb": data.get("P/B", "N/A"),
            "eps": data.get("EPS (ttm)", "N/A"),
            "eps_next_y": data.get("EPS next Y", "N/A"),
            "dividend": data.get("Dividend %", "N/A"),
            "roe": data.get("ROE", "N/A"),
            "roa": data.get("ROA", "N/A"),
            "debt_eq": data.get("Debt/Eq", "N/A"),
            "short_float": data.get("Short Float", "N/A"),
            "target_price": data.get("Target Price", "N/A"),
            "52w_high": data.get("52W High", "N/A"),
            "52w_low": data.get("52W Low", "N/A"),
            "rsi": data.get("RSI (14)", "N/A"),
            "rel_volume": data.get("Rel Volume", "N/A"),
            "avg_volume": data.get("Avg Volume", "N/A"),
            "sector": data.get("Sector", "N/A"),
            "industry": data.get("Industry", "N/A"),
        }
    except Exception as e:
        print(f"Finviz 종목 데이터 실패 ({symbol}): {e}")
        return {}


# ============================================
# TipRanks - 애널리스트 의견
# ============================================
def get_tipranks_rating(symbol: str) -> dict:
    """TipRanks 애널리스트 평점 (API 방식)"""
    try:
        url = f"https://www.tipranks.com/api/stocks/getData/?name={symbol}"
        response = requests.get(url, headers=HEADERS, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            consensus = data.get("analystConsensus", {})
            price_target = data.get("priceTarget", {})
            
            # 컨센서스 해석
            consensus_rating = consensus.get("consensus", "")
            rating_kr = {
                "Strong Buy": "적극 매수 🟢",
                "Moderate Buy": "매수 🟢",
                "Hold": "보유 🟡",
                "Moderate Sell": "매도 🟠",
                "Strong Sell": "적극 매도 🔴"
            }.get(consensus_rating, consensus_rating)
            
            return {
                "symbol": symbol,
                "consensus": rating_kr,
                "buy": consensus.get("buy", 0),
                "hold": consensus.get("hold", 0),
                "sell": consensus.get("sell", 0),
                "price_target_avg": price_target.get("mean", 0),
                "price_target_high": price_target.get("high", 0),
                "price_target_low": price_target.get("low", 0),
                "num_analysts": consensus.get("buy", 0) + consensus.get("hold", 0) + consensus.get("sell", 0),
            }
    except Exception as e:
        print(f"TipRanks 데이터 실패 ({symbol}): {e}")
    
    return {}


# ============================================
# Seeking Alpha - 기업 분석 요약
# ============================================
def get_seeking_alpha_ratings(symbol: str) -> dict:
    """Seeking Alpha 퀀트 레이팅"""
    try:
        url = f"https://seekingalpha.com/api/v3/symbols/{symbol}/rating"
        response = requests.get(url, headers={
            **HEADERS,
            "Accept": "application/json"
        }, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            ratings = data.get("data", {}).get("attributes", {})
            
            # 레이팅 해석
            quant_rating = ratings.get("quantRating", 0)
            if quant_rating >= 4:
                rating_text = "Strong Buy 🟢"
            elif quant_rating >= 3:
                rating_text = "Buy 🟢"
            elif quant_rating >= 2:
                rating_text = "Hold 🟡"
            else:
                rating_text = "Sell 🔴"
            
            return {
                "symbol": symbol,
                "quant_rating": quant_rating,
                "rating_text": rating_text,
                "authors_rating": ratings.get("authorsRating", 0),
                "sell_side_rating": ratings.get("sellSideRating", 0),
            }
    except Exception as e:
        print(f"Seeking Alpha 데이터 실패 ({symbol}): {e}")
    
    return {}


# ============================================
# ETF.com - ETF 정보
# ============================================
def get_etf_info(symbol: str) -> dict:
    """ETF.com에서 ETF 정보 가져오기"""
    try:
        url = f"https://www.etf.com/{symbol}"
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        
        data = {"symbol": symbol}
        
        # 기본 정보 추출
        info_table = soup.find("div", {"class": "fundHeader"})
        if info_table:
            data["name"] = info_table.find("h1").get_text(strip=True) if info_table.find("h1") else ""
        
        # 비용 비율
        expense = soup.find("span", string=re.compile("Expense Ratio"))
        if expense:
            data["expense_ratio"] = expense.find_next("span").get_text(strip=True)
        
        # AUM
        aum = soup.find("span", string=re.compile("AUM"))
        if aum:
            data["aum"] = aum.find_next("span").get_text(strip=True)
        
        return data
    except Exception as e:
        print(f"ETF.com 데이터 실패 ({symbol}): {e}")
    
    return {}


# ============================================
# 종합 분석 함수
# ============================================
def get_comprehensive_stock_analysis(symbol: str) -> dict:
    """여러 소스에서 종합 분석 데이터 수집"""
    result = {
        "symbol": symbol,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "sources": {}
    }
    
    # Finviz 데이터
    finviz_data = get_finviz_stock_data(symbol)
    if finviz_data:
        result["sources"]["finviz"] = finviz_data
    
    # TipRanks 데이터
    tipranks_data = get_tipranks_rating(symbol)
    if tipranks_data:
        result["sources"]["tipranks"] = tipranks_data
    
    # Seeking Alpha 데이터
    sa_data = get_seeking_alpha_ratings(symbol)
    if sa_data:
        result["sources"]["seeking_alpha"] = sa_data
    
    return result


def get_market_sentiment_summary() -> dict:
    """시장 심리 종합 요약"""
    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")
    }
    
    # Fear & Greed Index
    fg = get_fear_greed_index()
    result["fear_greed"] = fg
    
    # 섹터 성과
    sectors = get_finviz_sector_performance()
    result["sectors"] = sectors
    
    # 시장 개요
    overview = get_finviz_market_overview()
    result["market_overview"] = overview
    
    return result
