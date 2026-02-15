"""
Keyless news and event collection module.

Design goals:
- No mandatory API key
- Reasonable defaults via Google News RSS + yfinance
- Keep legacy public function signatures stable
"""

from __future__ import annotations

import math
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote_plus

import requests
import yfinance as yf


_TITLE_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "into",
    "after",
    "amid",
    "stock",
    "stocks",
    "market",
    "markets",
    "today",
    "news",
    "update",
    "live",
}

_SOURCE_SCORE_RULES: list[tuple[str, float]] = [
    ("reuters", 88.0),
    ("bloomberg", 86.0),
    ("wall street journal", 85.0),
    ("financial times", 84.0),
    ("associated press", 82.0),
    ("cnbc", 78.0),
    ("yahoo finance", 76.0),
    ("marketwatch", 74.0),
    ("investor's business daily", 72.0),
    ("barron's", 74.0),
    ("seeking alpha", 62.0),
    ("motley fool", 58.0),
    ("marketbeat", 52.0),
    ("aol.com", 45.0),
]

_EVENT_RULES: list[tuple[str, str, float, bool]] = [
    ("earnings", r"\bearnings?\b|\beps\b|guidance|revenue", 60.0, True),
    ("fomc", r"\bfomc\b|federal reserve|fed rate|interest rates? unchanged|rate decision", 72.0, True),
    ("inflation", r"\bcpi\b|\bpce\b|inflation", 66.0, True),
    ("jobs", r"\bnfp\b|non-farm payrolls|jobless|unemployment", 60.0, True),
    ("analyst", r"upgrad|downgrad|price target|initiates? coverage", 42.0, False),
    ("mna", r"acquisition|merger|takeover|buyout", 56.0, True),
    ("regulatory", r"\bsec\b|doj|lawsuit|antitrust|investigation", 52.0, True),
    ("dividend_buyback", r"dividend|buyback|repurchase", 38.0, False),
]

_TICKER_RE = re.compile(r"\(([A-Z]{1,5}(?:\.[A-Z])?)\)")


def _safe_dt(timestamp: int):
    """Convert a Unix timestamp to a datetime object."""
    try:
        if not timestamp:
            return None
        return datetime.fromtimestamp(int(timestamp))
    except Exception:
        return None


def _parse_pub_date(value: str) -> datetime | None:
    """Parse an RSS pubDate string."""
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
    """Normalize raw news into the internal standard format."""
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
    """Merge feeds by newest timestamp after removing duplicates."""
    by_key: dict[str, dict] = {}
    for items in news_lists:
        for item in items:
            key = item.get("url") or item.get("headline")
            if not key:
                continue
            prev = by_key.get(key)
            prev_ts = int((prev or {}).get("published_ts", 0) or 0)
            curr_ts = int(item.get("published_ts", 0) or 0)
            if prev is None or curr_ts > prev_ts:
                by_key[key] = item

    merged = sorted(by_key.values(), key=lambda x: int(x.get("published_ts", 0) or 0), reverse=True)
    return merged[: max(0, limit)]


def _source_score(source: str) -> float:
    src = (source or "").strip().lower()
    if not src:
        return 45.0
    for name, score in _SOURCE_SCORE_RULES:
        if name in src:
            return score
    return 50.0


def _event_profile(headline: str, summary: str) -> dict[str, Any]:
    text = f"{headline or ''} {summary or ''}".lower()
    tags: list[str] = []
    event_score = 0.0
    hard_event = False

    for tag, pattern, score, is_hard in _EVENT_RULES:
        if re.search(pattern, text):
            tags.append(tag)
            event_score = max(event_score, score)
            hard_event = hard_event or is_hard

    # Light penalty for purely promotional/opinion-like headlines.
    if re.search(r"\b(is .*stock|millionaire|set you up for life|best .* to buy)\b", text):
        event_score = max(0.0, event_score - 14.0)

    if event_score >= 65:
        impact = "high"
    elif event_score >= 40:
        impact = "medium"
    else:
        impact = "low"

    return {
        "tags": tags,
        "event_score": round(event_score, 1),
        "hard_event": hard_event,
        "impact": impact,
    }


def _recency_score(age_hours: float, hard_event: bool) -> float:
    age = max(0.0, float(age_hours))
    half_life = 60.0 if hard_event else 22.0
    return round((math.exp(-age / half_life) * 100.0), 1)


def _canonical_story_key(headline: str) -> str:
    text = (headline or "").strip().lower()
    text = re.sub(r"\s+-\s+[^-]{1,50}$", "", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    parts = [p for p in text.split() if len(p) > 2 and p not in _TITLE_STOPWORDS]
    return " ".join(parts[:12])


def _company_relevance(item: dict, symbol: str) -> float:
    sym = _normalize_symbol(symbol)
    if not sym:
        return 0.0

    headline = str(item.get("headline", ""))
    summary = str(item.get("summary", ""))
    text = f"{headline} {summary}"
    text_low = text.lower()

    score = 0.0
    if f"${sym.lower()}" in text_low:
        score += 2.0
    if re.search(rf"\b{re.escape(sym.lower())}\b", text_low):
        score += 1.6
    if f"({sym})" in text or f"({sym}.US)" in text:
        score += 1.4

    mentioned = {m.upper() for m in _TICKER_RE.findall(text)}
    if mentioned and sym not in mentioned and f"${sym.lower()}" not in text_low:
        score -= 1.0

    return score


def _rank_and_select_news(items: list[dict], limit: int, symbol: str | None = None) -> list[dict]:
    if not items or limit <= 0:
        return []

    now = datetime.now()
    annotated: list[dict] = []
    for item in items:
        row = dict(item)
        age_h = row.get("age_hours")
        if age_h is None:
            ts = int(row.get("published_ts", 0) or 0)
            if ts > 0:
                dt = _safe_dt(ts)
                if dt is not None:
                    age_h = max(0, int((now - dt).total_seconds() // 3600))
                else:
                    age_h = 999
            else:
                age_h = 999

        profile = _event_profile(str(row.get("headline", "")), str(row.get("summary", "")))
        src_score = _source_score(str(row.get("source", "")))
        rec_score = _recency_score(float(age_h), bool(profile["hard_event"]))
        rel_score = _company_relevance(row, symbol) if symbol else 0.0

        total_score = profile["event_score"] * 0.44 + src_score * 0.22 + rec_score * 0.26 + rel_score * 8.0
        if float(age_h) > 96 and not profile["hard_event"]:
            total_score -= 10
        if float(age_h) > 168:
            total_score -= 16

        row["age_hours"] = int(age_h)
        row["event_tags"] = profile["tags"]
        row["is_hard_event"] = profile["hard_event"]
        row["event_score"] = profile["event_score"]
        row["source_score"] = round(src_score, 1)
        row["recency_score"] = rec_score
        row["relevance_score"] = round(rel_score, 2)
        row["impact"] = profile["impact"]
        row["news_score"] = round(max(0.0, min(100.0, total_score)), 1)
        annotated.append(row)

    # Select by score first, then ensure story diversity.
    ranked = sorted(
        annotated,
        key=lambda x: (
            -float(x.get("news_score", 0)),
            -int(x.get("published_ts", 0) or 0),
        ),
    )

    selected: list[dict] = []
    seen_keys: set[str] = set()
    for row in ranked:
        story_key = _canonical_story_key(str(row.get("headline", "")))
        if story_key and story_key in seen_keys:
            continue
        if story_key:
            seen_keys.add(story_key)
        selected.append(row)
        if len(selected) >= limit:
            break

    # Presentation remains newest-first for UI consistency.
    if symbol:
        relevant = [x for x in selected if float(x.get("relevance_score", 0)) >= 0.8]
        if relevant:
            extras = [x for x in selected if x not in relevant]
            min_relevant = max(2, (limit + 1) // 2)
            if len(relevant) >= min_relevant:
                selected = relevant[:limit]
            else:
                selected = (relevant + extras[: max(0, limit - len(relevant))])[:limit]

    selected.sort(key=lambda x: int(x.get("published_ts", 0) or 0), reverse=True)
    return selected


def _fetch_google_news_rss(
    query: str,
    days: int = 7,
    limit: int = 10,
    summary_limit: int = 200,
) -> list[dict]:
    """Google News RSS feed (no API key required)."""
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

        news.sort(key=lambda x: int(x.get("published_ts", 0) or 0), reverse=True)
        return news[: max(0, limit)]
    except Exception:
        return []


def _request(endpoint: str, params: dict | None = None):
    """
    Legacy external adapter.

    This project is now keyless by default, so the built-in implementation
    intentionally does not call paid provider endpoints. The function remains
    to preserve monkeypatch compatibility in existing tests.
    """
    _ = endpoint, params
    return None


def get_company_news(symbol: str, days: int = 7) -> list[dict]:
    """Fetch symbol-specific news (newest first)."""
    now = datetime.now()
    from_date = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")

    data = _request(
        "/company-news",
        {
            "symbol": symbol,
            "from": from_date,
            "to": to_date,
        },
    )

    if not data:
        merged = _merge_news(
            [
                _fetch_google_news_rss(f"\"{symbol}\" stock", days=days, limit=14, summary_limit=180),
                _fetch_google_news_rss(f"${symbol} earnings guidance", days=days, limit=14, summary_limit=180),
                _fetch_google_news_rss(f"\"{symbol}\" earnings", days=days, limit=14, summary_limit=180),
                _fetch_google_news_rss(f"${symbol} analyst target", days=days, limit=12, summary_limit=180),
            ],
            limit=48,
        )
        return _rank_and_select_news(merged, limit=5, symbol=symbol)

    # Legacy patched path: tests can inject provider-like payloads via _request.
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
    return _rank_and_select_news(news, limit=5, symbol=symbol)


def get_bulk_news(symbols: list[str], days: int = 7) -> dict[str, list[dict]]:
    """Collect news for multiple symbols in parallel."""
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
    """Fetch broad market news (newest first)."""
    data = _request("/news", {"category": category})

    if not data:
        queries = [
            "US stock market",
            "S&P 500",
            "Nasdaq",
            "Federal Reserve rates",
            "US CPI inflation",
            "US jobs report",
            "earnings surprise",
        ]
        if category and category != "general":
            queries.insert(0, f"{category} market news")

        news_lists = [
            _fetch_google_news_rss(q, days=3, limit=15, summary_limit=200)
            for q in queries
        ]
        merged = _merge_news(news_lists, limit=120)
        return _rank_and_select_news(merged, limit=10)

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

    return _rank_and_select_news(news, limit=10)


def _to_float(value, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _to_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _parse_earnings_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        for item in value:
            parsed = _parse_earnings_date(item)
            if parsed is not None:
                return parsed
        return None
    if hasattr(value, "to_pydatetime"):
        try:
            value = value.to_pydatetime()
        except Exception:
            pass
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return datetime.fromisoformat(value[:10]).date()
        except Exception:
            return None
    return None


def _calendar_to_dict(calendar) -> dict:
    if isinstance(calendar, dict):
        return calendar
    if hasattr(calendar, "to_dict"):
        try:
            raw = calendar.to_dict()
            if isinstance(raw, dict):
                if "Value" in raw and isinstance(raw["Value"], dict):
                    return raw["Value"]
                out = {}
                for key, value in raw.items():
                    if isinstance(value, dict) and value:
                        out[key] = next(iter(value.values()))
                    else:
                        out[key] = value
                return out
        except Exception:
            return {}
    return {}


def _fetch_earnings_row(symbol: str, start: date, end: date) -> dict | None:
    try:
        ticker = yf.Ticker(symbol)
        cal = _calendar_to_dict(ticker.calendar)
    except Exception:
        return None

    earnings_date = _parse_earnings_date(cal.get("Earnings Date"))
    if earnings_date is None:
        return None
    if earnings_date < start or earnings_date > end:
        return None

    eps_estimate = cal.get("Earnings Average")
    eps_value = _to_float(eps_estimate, float("nan"))
    if math.isnan(eps_value):
        eps_value = None

    return {
        "symbol": symbol,
        "date": earnings_date.isoformat(),
        "hour": "TBD",
        "eps_estimate": eps_value,
    }


def get_earnings_calendar(days: int = 14) -> list[dict]:
    """Fetch upcoming earnings calendar data without provider API keys."""
    window = max(1, int(days))
    start = datetime.now().date()
    end = start + timedelta(days=window)

    symbols: list[str]
    try:
        from config import load_nasdaq_100

        symbols = load_nasdaq_100()[:80]
    except Exception:
        symbols = []

    if not symbols:
        symbols = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "META", "AVGO", "COST", "AMD", "TSLA"]

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_earnings_row, sym, start, end): sym for sym in symbols}
        for future in as_completed(futures):
            row = future.result()
            if row:
                results.append(row)

    results.sort(key=lambda x: (x.get("date", ""), x.get("symbol", "")))
    return results[:60]


def get_recommendation_trends(symbol: str) -> dict | None:
    """Fetch analyst recommendation trends from yfinance."""
    symbol = _normalize_symbol(symbol)
    if not symbol:
        return None

    try:
        summary = yf.Ticker(symbol).get_recommendations_summary()
        if summary is not None and len(summary) > 0:
            row = summary.iloc[0].to_dict()
            strong_buy = _to_int(row.get("strongBuy", 0))
            buy = _to_int(row.get("buy", 0))
            hold = _to_int(row.get("hold", 0))
            sell = _to_int(row.get("sell", 0))
            strong_sell = _to_int(row.get("strongSell", 0))

            total = strong_buy + buy + hold + sell + strong_sell
            if total <= 0:
                return None

            buy_pct = (strong_buy + buy) / total * 100
            return {
                "symbol": symbol,
                "strong_buy": strong_buy,
                "buy": buy,
                "hold": hold,
                "sell": sell,
                "strong_sell": strong_sell,
                "buy_pct": buy_pct,
            }
    except Exception:
        pass

    # Fallback from coarse recommendation fields in quote info.
    try:
        info = yf.Ticker(symbol).info or {}
        opinions = max(0, _to_int(info.get("numberOfAnalystOpinions", 0)))
        if opinions <= 0:
            return None

        rec_key = str(info.get("recommendationKey", "")).lower()
        if rec_key in {"strong_buy", "buy"}:
            strong_buy = opinions // 3 if rec_key == "strong_buy" else opinions // 6
            buy = max(1, opinions - strong_buy - opinions // 6)
            hold = max(0, opinions - strong_buy - buy)
            sell = 0
            strong_sell = 0
        elif rec_key == "hold":
            strong_buy = 0
            buy = opinions // 4
            hold = opinions - buy
            sell = 0
            strong_sell = 0
        else:
            strong_buy = 0
            buy = opinions // 6
            hold = opinions // 3
            sell = max(0, opinions - buy - hold)
            strong_sell = 0

        total = strong_buy + buy + hold + sell + strong_sell
        if total <= 0:
            return None
        buy_pct = (strong_buy + buy) / total * 100
        return {
            "symbol": symbol,
            "strong_buy": strong_buy,
            "buy": buy,
            "hold": hold,
            "sell": sell,
            "strong_sell": strong_sell,
            "buy_pct": buy_pct,
        }
    except Exception:
        return None


def get_price_target(symbol: str) -> dict | None:
    """Fetch analyst price targets from yfinance."""
    symbol = _normalize_symbol(symbol)
    if not symbol:
        return None

    try:
        targets = yf.Ticker(symbol).get_analyst_price_targets() or {}
        if targets:
            return {
                "symbol": symbol,
                "target_high": targets.get("high"),
                "target_low": targets.get("low"),
                "target_mean": targets.get("mean"),
            }
    except Exception:
        pass

    try:
        info = yf.Ticker(symbol).info or {}
        high = info.get("targetHighPrice")
        low = info.get("targetLowPrice")
        mean = info.get("targetMeanPrice")
        if high is None and low is None and mean is None:
            return None
        return {
            "symbol": symbol,
            "target_high": high,
            "target_low": low,
            "target_mean": mean,
        }
    except Exception:
        return None
