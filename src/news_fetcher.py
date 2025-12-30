"""
뉴스 및 이벤트 수집 모듈 (Finnhub API)
"""
import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1"


# 이벤트 설명 사전
EVENT_DESCRIPTIONS = {
    # 경제 지표
    "FOMC": {
        "name": "FOMC 금리 결정",
        "impact": "🔴 매우 큼",
        "desc": "미국 기준금리 결정. 금리 인상→주가 하락, 금리 인하→주가 상승 경향"
    },
    "CPI": {
        "name": "소비자물가지수 (CPI)",
        "impact": "🔴 매우 큼",
        "desc": "인플레이션 지표. 예상보다 높으면 금리 인상 우려로 주가 하락"
    },
    "NFP": {
        "name": "비농업 고용지표",
        "impact": "🔴 매우 큼",
        "desc": "매월 첫째 금요일 발표. 고용 강하면 금리 인상 우려"
    },
    "GDP": {
        "name": "GDP 성장률",
        "impact": "🟡 큼",
        "desc": "경제 성장률. 예상보다 높으면 호재"
    },
    
    # 기업 이벤트
    "earnings": {
        "name": "실적 발표",
        "impact": "🔴 매우 큼",
        "desc": "분기 실적 발표. EPS/매출이 예상치 상회하면 급등, 하회하면 급락"
    },
    "dividend": {
        "name": "배당",
        "impact": "🟡 중간",
        "desc": "배당 발표/인상은 호재, 삭감은 악재"
    },
    "insider_buy": {
        "name": "내부자 매수",
        "impact": "🟡 호재",
        "desc": "CEO/임원이 자기 돈으로 주식 매수. 회사에 자신감 있다는 신호"
    },
    "insider_sell": {
        "name": "내부자 매도",
        "impact": "🟡 주의",
        "desc": "내부자 대량 매도는 주의 필요. 단, 세금/개인사정일 수도"
    },
    "upgrade": {
        "name": "투자의견 상향",
        "impact": "🟡 호재",
        "desc": "애널리스트가 Buy로 상향. 목표가도 같이 보기"
    },
    "downgrade": {
        "name": "투자의견 하향",
        "impact": "🟡 악재",
        "desc": "애널리스트가 Sell로 하향. 이유 확인 필요"
    },
    "ipo": {
        "name": "IPO (신규상장)",
        "impact": "🟡 중간",
        "desc": "신규 상장. 락업 해제일(보통 90~180일 후) 주의"
    },
}


def _request(endpoint: str, params: dict = None) -> dict | None:
    """Finnhub API 요청"""
    if not FINNHUB_API_KEY:
        print("FINNHUB_API_KEY가 설정되지 않았습니다.")
        return None
    
    params = params or {}
    params["token"] = FINNHUB_API_KEY
    
    try:
        response = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Finnhub API 오류: {e}")
        return None


def get_company_news(symbol: str, days: int = 7) -> list[dict]:
    """종목별 뉴스 가져오기"""
    today = datetime.now()
    from_date = (today - timedelta(days=days)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")
    
    data = _request("/company-news", {
        "symbol": symbol,
        "from": from_date,
        "to": to_date
    })
    
    if not data:
        return []
    
    news = []
    for item in data[:10]:  # 최대 10개
        news.append({
            "headline": item.get("headline", ""),
            "summary": item.get("summary", "")[:200],
            "url": item.get("url", ""),
            "source": item.get("source", ""),
            "datetime": datetime.fromtimestamp(item.get("datetime", 0)).strftime("%Y-%m-%d %H:%M"),
        })
    
    return news


def get_earnings_calendar(from_date: str = None, to_date: str = None) -> list[dict]:
    """실적 발표 일정"""
    if not from_date:
        from_date = datetime.now().strftime("%Y-%m-%d")
    if not to_date:
        to_date = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%d")
    
    data = _request("/calendar/earnings", {"from": from_date, "to": to_date})
    
    if not data or "earningsCalendar" not in data:
        return []
    
    earnings = []
    for item in data["earningsCalendar"]:
        symbol = item.get("symbol", "")
        # 나스닥 100 종목만 필터링 (선택사항)
        earnings.append({
            "symbol": symbol,
            "date": item.get("date", ""),
            "hour": "장전" if item.get("hour") == "bmo" else "장후" if item.get("hour") == "amc" else "미정",
            "eps_estimate": item.get("epsEstimate"),
            "eps_actual": item.get("epsActual"),
            "revenue_estimate": item.get("revenueEstimate"),
            "event_type": "earnings",
            "description": EVENT_DESCRIPTIONS["earnings"],
        })
    
    return earnings


def get_insider_transactions(symbol: str) -> list[dict]:
    """내부자 거래"""
    data = _request("/stock/insider-transactions", {"symbol": symbol})
    
    if not data or "data" not in data:
        return []
    
    transactions = []
    for item in data["data"][:10]:
        change = item.get("change", 0)
        tx_type = "insider_buy" if change > 0 else "insider_sell"
        
        transactions.append({
            "symbol": symbol,
            "name": item.get("name", ""),
            "share": abs(change),
            "transaction_type": "매수" if change > 0 else "매도",
            "date": item.get("transactionDate", ""),
            "event_type": tx_type,
            "description": EVENT_DESCRIPTIONS[tx_type],
        })
    
    return transactions


def get_recommendation_trends(symbol: str) -> dict | None:
    """애널리스트 추천"""
    data = _request("/stock/recommendation", {"symbol": symbol})
    
    if not data or len(data) == 0:
        return None
    
    latest = data[0]
    return {
        "symbol": symbol,
        "period": latest.get("period", ""),
        "strong_buy": latest.get("strongBuy", 0),
        "buy": latest.get("buy", 0),
        "hold": latest.get("hold", 0),
        "sell": latest.get("sell", 0),
        "strong_sell": latest.get("strongSell", 0),
    }


def get_price_target(symbol: str) -> dict | None:
    """목표 주가"""
    data = _request("/stock/price-target", {"symbol": symbol})
    
    if not data:
        return None
    
    return {
        "symbol": symbol,
        "target_high": data.get("targetHigh"),
        "target_low": data.get("targetLow"),
        "target_mean": data.get("targetMean"),
        "target_median": data.get("targetMedian"),
        "last_updated": data.get("lastUpdated", ""),
    }


def get_ipo_calendar() -> list[dict]:
    """IPO 일정"""
    from_date = datetime.now().strftime("%Y-%m-%d")
    to_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
    
    data = _request("/calendar/ipo", {"from": from_date, "to": to_date})
    
    if not data or "ipoCalendar" not in data:
        return []
    
    ipos = []
    for item in data["ipoCalendar"]:
        ipos.append({
            "symbol": item.get("symbol", ""),
            "name": item.get("name", ""),
            "date": item.get("date", ""),
            "exchange": item.get("exchange", ""),
            "price_range": f"${item.get('priceRangeLow', '?')}-${item.get('priceRangeHigh', '?')}",
            "shares": item.get("numberOfShares"),
            "event_type": "ipo",
            "description": EVENT_DESCRIPTIONS["ipo"],
        })
    
    return ipos


def get_market_news(category: str = "general") -> list[dict]:
    """시장 전체 뉴스"""
    data = _request("/news", {"category": category})
    
    if not data:
        return []
    
    news = []
    for item in data[:10]:
        news.append({
            "headline": item.get("headline", ""),
            "summary": item.get("summary", "")[:200],
            "url": item.get("url", ""),
            "source": item.get("source", ""),
            "datetime": datetime.fromtimestamp(item.get("datetime", 0)).strftime("%Y-%m-%d %H:%M"),
        })
    
    return news
