"""
뉴스 및 이벤트 수집 모듈 (Finnhub API)
"""
import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1"


def _request(endpoint: str, params: dict = None) -> dict | None:
    """Finnhub API 요청"""
    if not FINNHUB_API_KEY:
        return None

    params = params or {}
    params["token"] = FINNHUB_API_KEY

    try:
        response = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
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
    for item in data[:5]:  # 종목당 최대 5개
        news.append({
            "headline": item.get("headline", ""),
            "summary": item.get("summary", "")[:150],
            "source": item.get("source", ""),
            "datetime": datetime.fromtimestamp(item.get("datetime", 0)).strftime("%m/%d"),
        })

    return news


def get_bulk_news(symbols: list[str], days: int = 7) -> dict[str, list[dict]]:
    """여러 종목 뉴스 병렬 수집"""
    results = {}
    
    def fetch(symbol):
        return symbol, get_company_news(symbol, days)
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch, s): s for s in symbols}
        for future in as_completed(futures):
            symbol, news = future.result()
            if news:
                results[symbol] = news
    
    return results


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
            "source": item.get("source", ""),
            "datetime": datetime.fromtimestamp(item.get("datetime", 0)).strftime("%m/%d %H:%M"),
        })

    return news


def get_earnings_calendar(days: int = 14) -> list[dict]:
    """실적 발표 일정"""
    from_date = datetime.now().strftime("%Y-%m-%d")
    to_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")

    data = _request("/calendar/earnings", {"from": from_date, "to": to_date})

    if not data or "earningsCalendar" not in data:
        return []

    earnings = []
    for item in data["earningsCalendar"]:
        earnings.append({
            "symbol": item.get("symbol", ""),
            "date": item.get("date", ""),
            "hour": "장전" if item.get("hour") == "bmo" else "장후" if item.get("hour") == "amc" else "미정",
            "eps_estimate": item.get("epsEstimate"),
        })

    return earnings


def get_recommendation_trends(symbol: str) -> dict | None:
    """애널리스트 추천"""
    data = _request("/stock/recommendation", {"symbol": symbol})

    if not data or len(data) == 0:
        return None

    latest = data[0]
    total = (latest.get("strongBuy", 0) + latest.get("buy", 0) + 
             latest.get("hold", 0) + latest.get("sell", 0) + latest.get("strongSell", 0))
    
    if total == 0:
        return None
    
    buy_pct = (latest.get("strongBuy", 0) + latest.get("buy", 0)) / total * 100
    
    return {
        "symbol": symbol,
        "strong_buy": latest.get("strongBuy", 0),
        "buy": latest.get("buy", 0),
        "hold": latest.get("hold", 0),
        "sell": latest.get("sell", 0),
        "strong_sell": latest.get("strongSell", 0),
        "buy_pct": buy_pct,
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
    }
