"""
경제 지표 캘린더 (웹 스크래핑)
"""
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# 주요 경제 이벤트 설명
ECONOMIC_EVENTS = {
    "FOMC": {
        "name": "FOMC 금리 결정",
        "impact": "🔴 매우 큼",
        "desc": "미국 기준금리 결정. 금리 인상→주가 하락, 금리 인하→주가 상승 경향.\n시장은 금리 자체보다 '예상 대비' 결과에 반응."
    },
    "CPI": {
        "name": "소비자물가지수",
        "impact": "🔴 매우 큼",
        "desc": "인플레이션 핵심 지표. 예상보다 높으면 금리 인상 우려로 주가 하락.\nCore CPI(식품/에너지 제외)가 더 중요."
    },
    "PPI": {
        "name": "생산자물가지수",
        "impact": "🟡 큼",
        "desc": "기업 입장의 물가. CPI 선행지표로 활용."
    },
    "NFP": {
        "name": "비농업 고용지표",
        "impact": "🔴 매우 큼",
        "desc": "매월 첫째 금요일 발표. 고용 강하면 경기 좋지만 금리 인상 우려.\n실업률과 함께 발표."
    },
    "Unemployment": {
        "name": "실업률",
        "impact": "🟡 큼",
        "desc": "낮을수록 경기 좋음. 하지만 너무 낮으면 임금 인플레 우려."
    },
    "GDP": {
        "name": "GDP 성장률",
        "impact": "🟡 큼",
        "desc": "분기별 경제 성장률. 예상 상회하면 호재.\n속보치→잠정치→확정치 순으로 발표."
    },
    "Retail Sales": {
        "name": "소매판매",
        "impact": "🟡 중간",
        "desc": "소비 지출 지표. 미국 GDP의 70%가 소비."
    },
    "ISM Manufacturing": {
        "name": "ISM 제조업지수",
        "impact": "🟡 중간",
        "desc": "50 이상이면 경기 확장, 50 미만이면 수축."
    },
    "ISM Services": {
        "name": "ISM 서비스업지수",
        "impact": "🟡 중간",
        "desc": "서비스업 경기 지표. 제조업보다 비중 큼."
    },
    "Fed Chair Speech": {
        "name": "연준 의장 연설",
        "impact": "🔴 매우 큼",
        "desc": "파월 의장 발언. 향후 금리 방향 힌트 제공.\n'매파적'(금리↑)/'비둘기파적'(금리↓) 발언 주목."
    },
    "Initial Jobless Claims": {
        "name": "신규 실업수당 청구",
        "impact": "🟢 작음",
        "desc": "매주 목요일 발표. 고용시장 실시간 지표."
    },
    "PCE": {
        "name": "개인소비지출 물가",
        "impact": "🔴 매우 큼",
        "desc": "연준이 가장 중시하는 인플레 지표. CPI보다 중요."
    },
}

# 2025년 주요 경제 일정 (하드코딩 - 실제로는 API나 스크래핑 필요)
ECONOMIC_CALENDAR_2025 = [
    # 1월
    {"date": "2025-01-10", "event": "NFP", "time": "22:30"},
    {"date": "2025-01-15", "event": "CPI", "time": "22:30"},
    {"date": "2025-01-29", "event": "FOMC", "time": "04:00"},
    # 2월
    {"date": "2025-02-07", "event": "NFP", "time": "22:30"},
    {"date": "2025-02-12", "event": "CPI", "time": "22:30"},
    # 3월
    {"date": "2025-03-07", "event": "NFP", "time": "22:30"},
    {"date": "2025-03-12", "event": "CPI", "time": "22:30"},
    {"date": "2025-03-19", "event": "FOMC", "time": "04:00"},
    # ... 나머지 월도 추가 가능
]


def get_upcoming_events(days: int = 14) -> list[dict]:
    """다가오는 경제 이벤트"""
    today = datetime.now()
    end_date = today + timedelta(days=days)
    
    events = []
    for item in ECONOMIC_CALENDAR_2025:
        event_date = datetime.strptime(item["date"], "%Y-%m-%d")
        if today <= event_date <= end_date:
            event_info = ECONOMIC_EVENTS.get(item["event"], {})
            events.append({
                "date": item["date"],
                "time": item.get("time", ""),
                "event": item["event"],
                "name": event_info.get("name", item["event"]),
                "impact": event_info.get("impact", ""),
                "description": event_info.get("desc", ""),
            })
    
    return sorted(events, key=lambda x: x["date"])


def get_event_description(event_name: str) -> dict:
    """이벤트 설명 가져오기"""
    for key, value in ECONOMIC_EVENTS.items():
        if key.lower() in event_name.lower() or event_name.lower() in key.lower():
            return value
    return {"name": event_name, "impact": "🟡 중간", "desc": "경제 지표"}


def fetch_investing_calendar() -> list[dict]:
    """Investing.com에서 경제 캘린더 스크래핑 (백업용)"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        # 실제 구현 시 Investing.com API 또는 스크래핑 필요
        # 여기서는 하드코딩된 데이터 사용
        return get_upcoming_events()
    except Exception as e:
        print(f"캘린더 가져오기 실패: {e}")
        return []
