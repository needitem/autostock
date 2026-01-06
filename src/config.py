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


# ===== 카테고리별 종목 =====
STOCK_CATEGORIES = {
    "AI/반도체": {
        "emoji": "🤖",
        "etf": "SMH",  # VanEck Semiconductor ETF
        "stocks": [
            "NVDA", "AMD", "AVGO", "QCOM", "INTC", "MU", "MRVL", "LRCX", "KLAC", "AMAT",
            "TSM", "ASML", "ARM", "SMCI", "PLTR", "AI", "PATH", "SNOW", "DDOG", "CRWD",
            "PANW", "ZS", "NET", "S", "FTNT", "ORCL", "IBM", "NOW", "CRM", "ADBE"
        ],
        "description": "AI 인프라, GPU, 반도체 장비, AI 소프트웨어"
    },
    "인터넷/플랫폼": {
        "emoji": "🌐",
        "etf": "SKYY",  # First Trust Cloud Computing ETF
        "stocks": [
            "GOOGL", "GOOG", "META", "AMZN", "NFLX", "SPOT", "UBER", "LYFT", "ABNB", "BKNG",
            "DASH", "PINS", "SNAP", "RBLX", "U", "TTWO", "EA", "MTCH", "BMBL", "ZG",
            "ETSY", "EBAY", "SHOP", "MELI", "SE", "PDD", "JD", "BABA", "CPNG", "GRAB"
        ],
        "description": "검색, SNS, 이커머스, 스트리밍, 게임"
    },
    "헬스케어/바이오": {
        "emoji": "🏥",
        "etf": "XBI",  # SPDR S&P Biotech ETF
        "stocks": [
            "LLY", "UNH", "JNJ", "MRK", "ABBV", "PFE", "TMO", "ABT", "DHR", "BMY",
            "AMGN", "GILD", "VRTX", "REGN", "MRNA", "BIIB", "ILMN", "DXCM", "ISRG", "IDXX",
            "ZTS", "EW", "SYK", "BDX", "MDT", "BSX", "HCA", "CI", "ELV", "CVS"
        ],
        "description": "제약, 바이오텍, 의료기기, 헬스케어 서비스"
    },
    "핀테크/금융": {
        "emoji": "💳",
        "etf": "FINX",  # Global X FinTech ETF
        "stocks": [
            "V", "MA", "PYPL", "SQ", "COIN", "HOOD", "SOFI", "AFRM", "UPST", "NU",
            "INTU", "FIS", "FISV", "GPN", "AXP", "COF", "DFS", "SYF", "ALLY", "LC",
            "BLK", "SCHW", "MS", "GS", "JPM", "BAC", "WFC", "C", "USB", "PNC"
        ],
        "description": "결제, 핀테크, 암호화폐, 전통 금융"
    },
    "전기차/클린에너지": {
        "emoji": "⚡",
        "etf": "QCLN",  # First Trust NASDAQ Clean Edge Green Energy
        "stocks": [
            "TSLA", "RIVN", "LCID", "NIO", "XPEV", "LI", "GM", "F", "TM", "HMC",
            "ENPH", "SEDG", "FSLR", "RUN", "NOVA", "PLUG", "BE", "CHPT", "BLNK", "EVGO",
            "ALB", "LAC", "LTHM", "MP", "QS", "PTRA", "LEA", "APT", "BWA", "APTV"
        ],
        "description": "전기차, 배터리, 태양광, 충전 인프라"
    },
    "소비재/리테일": {
        "emoji": "🛒",
        "etf": "XRT",  # SPDR S&P Retail ETF
        "stocks": [
            "AMZN", "WMT", "COST", "TGT", "HD", "LOW", "TJX", "ROST", "DG", "DLTR",
            "NKE", "LULU", "DECK", "CROX", "SKX", "UAA", "VFC", "PVH", "RL", "TPR",
            "SBUX", "MCD", "CMG", "DPZ", "YUM", "QSR", "WING", "SHAK", "CAVA", "BROS"
        ],
        "description": "이커머스, 리테일, 의류, 외식"
    },
    "통신/미디어": {
        "emoji": "📡",
        "etf": "XLC",  # Communication Services Select Sector SPDR
        "stocks": [
            "GOOGL", "META", "NFLX", "DIS", "CMCSA", "T", "VZ", "TMUS", "CHTR", "PARA",
            "WBD", "FOX", "FOXA", "NWSA", "NWS", "LYV", "SIRI", "ROKU", "FUBO", "ATUS",
            "LUMN", "FYBR", "USM", "LBRDK", "LBRDA", "CABO", "SBGI", "GTN", "NXST", "SSP"
        ],
        "description": "통신사, 미디어, 엔터테인먼트"
    },
    "산업재/방산": {
        "emoji": "🏭",
        "etf": "XLI",  # Industrial Select Sector SPDR
        "stocks": [
            "CAT", "DE", "HON", "UNP", "UPS", "FDX", "BA", "LMT", "RTX", "NOC",
            "GD", "GE", "MMM", "EMR", "ETN", "ITW", "PH", "ROK", "CMI", "PCAR",
            "WM", "RSG", "FAST", "ODFL", "JBHT", "XPO", "CHRW", "EXPD", "LSTR", "SAIA"
        ],
        "description": "항공우주, 방산, 물류, 산업장비"
    },
}

# 전체 카테고리 종목 (중복 제거)
ALL_CATEGORY_STOCKS = list(set(
    stock for cat in STOCK_CATEGORIES.values() for stock in cat["stocks"]
))
