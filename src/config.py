# -*- coding: utf-8 -*-
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# 캐시 파일 경로
CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
NASDAQ_CACHE_FILE = os.path.join(CACHE_DIR, "nasdaq100_cache.json")
SP500_CACHE_FILE = os.path.join(CACHE_DIR, "sp500_cache.json")
ALL_STOCKS_CACHE_FILE = os.path.join(CACHE_DIR, "all_stocks_cache.json")
SECTOR_CACHE_FILE = os.path.join(CACHE_DIR, "sector_cache.json")
CACHE_DAYS = 7  # 7일마다 갱신


def fetch_nasdaq_100() -> list[str]:
    """위키피디아에서 나스닥 100 종목 가져오기"""
    import pandas as pd
    import requests
    from io import StringIO
    
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        tables = pd.read_html(StringIO(response.text))
        
        for table in tables:
            if "Ticker" in table.columns:
                symbols = table["Ticker"].tolist()
                return [s for s in symbols if isinstance(s, str)]
        return []
    except Exception as e:
        print(f"나스닥 100 가져오기 실패: {e}")
        return []


def fetch_sp500() -> list[str]:
    """위키피디아에서 S&P 500 종목 가져오기"""
    import pandas as pd
    import requests
    from io import StringIO
    
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        tables = pd.read_html(StringIO(response.text))
        
        # 첫 번째 테이블이 S&P 500 목록
        if tables and "Symbol" in tables[0].columns:
            symbols = tables[0]["Symbol"].tolist()
            # BRK.B -> BRK-B 형식 변환 (yfinance 호환)
            return [s.replace(".", "-") for s in symbols if isinstance(s, str)]
        return []
    except Exception as e:
        print(f"S&P 500 가져오기 실패: {e}")
        return []


def get_cached_list(cache_file: str, fetch_func, name: str) -> list[str]:
    """캐시된 종목 목록 반환"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r") as f:
                cache = json.load(f)
                cached_date = datetime.fromisoformat(cache["date"])
                
                if datetime.now() - cached_date < timedelta(days=CACHE_DAYS):
                    return cache["symbols"]
        except:
            pass
    
    print(f"{name} 목록 가져오는 중...")
    symbols = fetch_func()
    
    if symbols:
        with open(cache_file, "w") as f:
            json.dump({"date": datetime.now().isoformat(), "symbols": symbols}, f)
        print(f"  ✅ {len(symbols)}개 종목")
        return symbols
    
    # 실패 시 기존 캐시 사용
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            return json.load(f).get("symbols", [])
    
    return []


def get_nasdaq_100() -> list[str]:
    return get_cached_list(NASDAQ_CACHE_FILE, fetch_nasdaq_100, "나스닥 100")


def get_sp500() -> list[str]:
    return get_cached_list(SP500_CACHE_FILE, fetch_sp500, "S&P 500")


def get_all_us_stocks() -> list[str]:
    """나스닥 100 + S&P 500 통합 (중복 제거)"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    if os.path.exists(ALL_STOCKS_CACHE_FILE):
        try:
            with open(ALL_STOCKS_CACHE_FILE, "r") as f:
                cache = json.load(f)
                cached_date = datetime.fromisoformat(cache["date"])
                
                if datetime.now() - cached_date < timedelta(days=CACHE_DAYS):
                    return cache["symbols"]
        except:
            pass
    
    nasdaq = get_nasdaq_100()
    sp500 = get_sp500()
    
    # 중복 제거 후 합치기
    all_stocks = list(set(nasdaq + sp500))
    all_stocks.sort()
    
    with open(ALL_STOCKS_CACHE_FILE, "w") as f:
        json.dump({"date": datetime.now().isoformat(), "symbols": all_stocks}, f)
    
    print(f"📊 전체 종목: {len(all_stocks)}개 (나스닥100: {len(nasdaq)}, S&P500: {len(sp500)}, 중복제거)")
    return all_stocks


def fetch_stock_sector(symbol: str) -> dict:
    """yfinance에서 종목의 섹터/산업 정보 가져오기"""
    import yfinance as yf
    
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        return {
            "symbol": symbol,
            "sector": info.get("sector", "Unknown"),
            "industry": info.get("industry", "Unknown"),
            "name": info.get("shortName", symbol),
        }
    except:
        return {
            "symbol": symbol,
            "sector": "Unknown",
            "industry": "Unknown",
            "name": symbol,
        }


def fetch_all_sectors(symbols: list[str], max_workers: int = 10) -> dict:
    """모든 종목의 섹터 정보 병렬 수집"""
    results = {}
    
    print(f"섹터 정보 수집 중... ({len(symbols)}개 종목)")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_stock_sector, s): s for s in symbols}
        
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results[result["symbol"]] = result
            if i % 20 == 0:
                print(f"  {i}/{len(symbols)} 완료...")
    
    return results


def get_sector_data(symbols: list[str] = None) -> dict:
    """캐시된 섹터 데이터 반환 (없으면 수집)"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    if symbols is None:
        symbols = get_all_us_stocks()
    
    if os.path.exists(SECTOR_CACHE_FILE):
        try:
            with open(SECTOR_CACHE_FILE, "r", encoding="utf-8") as f:
                cache = json.load(f)
                cached_date = datetime.fromisoformat(cache["date"])
                cached_symbols = set(cache["data"].keys())
                
                # 캐시가 유효하고 모든 종목이 포함되어 있으면 사용
                if datetime.now() - cached_date < timedelta(days=CACHE_DAYS):
                    missing = set(symbols) - cached_symbols
                    if not missing:
                        return cache["data"]
                    # 누락된 종목만 추가 수집
                    print(f"누락된 {len(missing)}개 종목 섹터 정보 수집 중...")
                    new_data = fetch_all_sectors(list(missing))
                    cache["data"].update(new_data)
                    with open(SECTOR_CACHE_FILE, "w", encoding="utf-8") as f:
                        json.dump(cache, f, ensure_ascii=False, indent=2)
                    return cache["data"]
        except:
            pass
    
    # 새로 수집
    data = fetch_all_sectors(symbols)
    
    if data:
        with open(SECTOR_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "date": datetime.now().isoformat(),
                "data": data
            }, f, ensure_ascii=False, indent=2)
    
    return data


def build_stock_categories(symbols: list[str] = None) -> dict:
    """섹터별 종목 카테고리 동적 생성"""
    if symbols is None:
        symbols = get_all_us_stocks()
    
    sector_data = get_sector_data(symbols)
    
    # 섹터별 이모지 및 ETF 매핑
    SECTOR_INFO = {
        "Technology": {"emoji": "💻", "etf": "XLK", "name": "기술"},
        "Communication Services": {"emoji": "📡", "etf": "XLC", "name": "통신서비스"},
        "Consumer Cyclical": {"emoji": "🛒", "etf": "XLY", "name": "경기소비재"},
        "Consumer Defensive": {"emoji": "🏪", "etf": "XLP", "name": "필수소비재"},
        "Healthcare": {"emoji": "🏥", "etf": "XLV", "name": "헬스케어"},
        "Financial Services": {"emoji": "💳", "etf": "XLF", "name": "금융"},
        "Industrials": {"emoji": "🏭", "etf": "XLI", "name": "산업재"},
        "Energy": {"emoji": "⛽", "etf": "XLE", "name": "에너지"},
        "Utilities": {"emoji": "💡", "etf": "XLU", "name": "유틸리티"},
        "Real Estate": {"emoji": "🏠", "etf": "XLRE", "name": "부동산"},
        "Basic Materials": {"emoji": "🧱", "etf": "XLB", "name": "소재"},
        "Unknown": {"emoji": "❓", "etf": "SPY", "name": "기타"},
    }
    
    # 섹터별로 종목 그룹화
    categories = {}
    
    for symbol in symbols:
        info = sector_data.get(symbol, {"sector": "Unknown", "industry": "Unknown"})
        sector = info.get("sector", "Unknown")
        
        if sector not in categories:
            sector_info = SECTOR_INFO.get(sector, SECTOR_INFO["Unknown"])
            categories[sector] = {
                "emoji": sector_info["emoji"],
                "etf": sector_info["etf"],
                "name": sector_info["name"],
                "stocks": [],
                "industries": {},
            }
        
        categories[sector]["stocks"].append(symbol)
        
        # 산업별로도 그룹화
        industry = info.get("industry", "Unknown")
        if industry not in categories[sector]["industries"]:
            categories[sector]["industries"][industry] = []
        categories[sector]["industries"][industry].append(symbol)
    
    # 종목 수 기준 정렬
    for sector in categories:
        categories[sector]["stocks"].sort()
        categories[sector]["description"] = f"{len(categories[sector]['stocks'])}개 종목"
    
    return categories


# ===== 전역 변수 =====

# 나스닥 100 종목
NASDAQ_100 = get_nasdaq_100()

# S&P 500 종목
SP500 = get_sp500()

# 전체 미국 주식 (나스닥 100 + S&P 500)
ALL_US_STOCKS = get_all_us_stocks()

# 시장 지표
MARKET_INDICATOR = "QQQ"

# 섹터별 카테고리 (동적 생성, 전체 종목 기준)
try:
    STOCK_CATEGORIES = build_stock_categories(ALL_US_STOCKS)
except Exception as e:
    print(f"섹터 카테고리 생성 실패: {e}")
    STOCK_CATEGORIES = {}

# 전체 카테고리 종목
ALL_CATEGORY_STOCKS = ALL_US_STOCKS


def get_category_summary() -> str:
    """카테고리 요약 출력"""
    lines = ["📂 섹터별 종목 분류:"]
    for sector, info in sorted(STOCK_CATEGORIES.items(), key=lambda x: -len(x[1]["stocks"])):
        lines.append(f"  {info['emoji']} {info['name']} ({sector}): {len(info['stocks'])}개")
    return "\n".join(lines)


if __name__ == "__main__":
    print(get_category_summary())
    print(f"\n총 {len(ALL_CATEGORY_STOCKS)}개 종목")
