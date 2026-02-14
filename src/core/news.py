"""
뉴스 및 이벤트 수집 모듈 (Finnhub API)
"""
import os
import re
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1"


def _safe_dt(timestamp: int):
    """유닉스 타임스탬프를 datetime으로 변환"""
    try:
        if not timestamp:
            return None
        return datetime.fromtimestamp(int(timestamp))
    except Exception:
        return None


def _parse_pub_date(value: str) -> datetime | None:
    """RSS pubDate 파싱"""
    try:
        if not value:
            return None
        dt = parsedate_to_datetime(value)
        if dt.tzinfo:
            return dt.astimezone().replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _to_news_item(
    headline: str,
    summary: str,
    source: str,
    published: datetime | None,
    url: str,
    summary_limit: int = 200,
) -> dict | None:
    """내부 표준 뉴스 포맷 변환"""
    headline = (headline or "").strip()
    if not headline:
        return None

    clean_summary = re.sub(r"<[^>]+>", "", summary or "")
    clean_summary = re.sub(r"\s+", " ", clean_summary).strip()

    if published and published.tzinfo:
        published = published.astimezone().replace(tzinfo=None)

    now = datetime.now()
    ts = int(published.timestamp()) if published else 0
    age_hours = max(0, int((now - published).total_seconds() // 3600)) if published else 0

    return {
        "headline": headline,
        "summary": clean_summary[:summary_limit],
        "source": (source or "").strip(),
        "datetime": published.strftime("%m/%d %H:%M") if published else "",
        "published_ts": ts,
        "age_hours": age_hours,
        "url": (url or "").strip(),
    }


def _merge_news(news_lists: list[list[dict]], limit: int) -> list[dict]:
    """중복 제거 후 합치기"""
    merged = []
    seen = set()
    for items in news_lists:
        for item in items:
            key = item.get("url") or item.get("headline")
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(item)
            if len(merged) >= limit:
                return merged
    return merged


def _fetch_google_news_rss(
    query: str,
    days: int = 7,
    limit: int = 10,
    summary_limit: int = 200,
) -> list[dict]:
    """Google News RSS (API 키 불필요)"""
    try:
        q = quote_plus(f"{query} when:{max(1, days)}d")
        url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        response = requests.get(
            url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (autostock-news-fetcher)"},
        )
        response.raise_for_status()

        root = ET.fromstring(response.content)
        min_ts = int((datetime.now() - timedelta(days=max(1, days))).timestamp())
        news = []

        for item in root.findall(".//item"):
            published = _parse_pub_date(item.findtext("pubDate", ""))
            if published and int(published.timestamp()) < min_ts:
                continue

            entry = _to_news_item(
                headline=item.findtext("title", ""),
                summary=item.findtext("description", ""),
                source=item.findtext("source", "Google News"),
                published=published,
                url=item.findtext("link", ""),
                summary_limit=summary_limit,
            )
            if not entry:
                continue
            news.append(entry)
            if len(news) >= limit:
                break

        return news
    except Exception:
        return []


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
    except Exception:
        return None


def get_company_news(symbol: str, days: int = 7) -> list[dict]:
    """종목별 뉴스 가져오기 (최신순)"""
    now = datetime.now()
    from_date = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")

    data = _request("/company-news", {
        "symbol": symbol,
        "from": from_date,
        "to": to_date
    })

    if not data:
        return _merge_news(
            [
                _fetch_google_news_rss(f"{symbol} stock", days=days, limit=5, summary_limit=180),
                _fetch_google_news_rss(f"{symbol} earnings guidance", days=days, limit=5, summary_limit=180),
            ],
            limit=5,
        )

    # 최신순 정렬 + 조회 기간 밖 데이터 제외
    sorted_data = sorted(data, key=lambda x: x.get("datetime", 0), reverse=True)

    news = []
    min_ts = int((now - timedelta(days=days)).timestamp())
    for item in sorted_data:
        published = _safe_dt(item.get("datetime", 0))
        if published is None:
            continue
        if int(item.get("datetime", 0)) < min_ts:
            continue

        entry = _to_news_item(
            headline=item.get("headline", ""),
            summary=item.get("summary", ""),
            source=item.get("source", ""),
            published=published,
            url=item.get("url", ""),
            summary_limit=180,
        )
        if not entry:
            continue
        news.append(entry)
        if len(news) >= 5:
            break

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
    """시장 전체 뉴스 (최신순)"""
    data = _request("/news", {"category": category})

    if not data:
        queries = [
            "US stock market",
            "S&P 500",
            "Nasdaq",
            "Federal Reserve rates",
        ]
        if category and category != "general":
            queries.insert(0, f"{category} market news")

        news_lists = [
            _fetch_google_news_rss(q, days=3, limit=5, summary_limit=200)
            for q in queries
        ]
        return _merge_news(news_lists, limit=10)

    news = []
    for item in sorted(data, key=lambda x: x.get("datetime", 0), reverse=True):
        published = _safe_dt(item.get("datetime", 0))
        if published is None:
            continue
        entry = _to_news_item(
            headline=item.get("headline", ""),
            summary=item.get("summary", ""),
            source=item.get("source", ""),
            published=published,
            url=item.get("url", ""),
            summary_limit=200,
        )
        if not entry:
            continue
        news.append(entry)
        if len(news) >= 10:
            break

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
