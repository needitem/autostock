import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# 캐시 파일 경로
CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
NASDAQ_CACHE_FILE = os.path.join(CACHE_DIR, "nasdaq100_cache.json")
CACHE_DAYS = 7  # 7일마다 갱신


def fetch_nasdaq_100() -> list[str]:
    """위키피디아에서 나스닥 100 종목 가져오기"""
    import pandas as pd
    import requests
    from io import StringIO
    
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        tables = pd.read_html(StringIO(response.text))
        
        # 종목 테이블 찾기 (Ticker 컬럼이 있는 테이블)
        for table in tables:
            if "Ticker" in table.columns:
                symbols = table["Ticker"].tolist()
                return [s for s in symbols if isinstance(s, str)]
        
        print("나스닥 100 테이블을 찾을 수 없습니다.")
        return []
    except Exception as e:
        print(f"나스닥 100 목록 가져오기 실패: {e}")
        return []


def get_nasdaq_100() -> list[str]:
    """캐시된 나스닥 100 목록 반환 (없거나 오래되면 갱신)"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    # 캐시 확인
    if os.path.exists(NASDAQ_CACHE_FILE):
        try:
            with open(NASDAQ_CACHE_FILE, "r") as f:
                cache = json.load(f)
                cached_date = datetime.fromisoformat(cache["date"])
                
                if datetime.now() - cached_date < timedelta(days=CACHE_DAYS):
                    return cache["symbols"]
        except:
            pass
    
    # 새로 가져오기
    symbols = fetch_nasdaq_100()
    
    if symbols:
        with open(NASDAQ_CACHE_FILE, "w") as f:
            json.dump({
                "date": datetime.now().isoformat(),
                "symbols": symbols
            }, f)
        return symbols
    
    # 실패 시 기존 캐시 사용
    if os.path.exists(NASDAQ_CACHE_FILE):
        with open(NASDAQ_CACHE_FILE, "r") as f:
            return json.load(f).get("symbols", [])
    
    return []


# 나스닥 100 종목 (동적으로 가져옴)
NASDAQ_100 = get_nasdaq_100()

# 시장 지표 (QQQ)
MARKET_INDICATOR = "QQQ"
