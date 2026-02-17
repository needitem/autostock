"""
economic_calendar.py 테스트
- 경제 지표 캘린더
"""
import pytest
from economic_calendar import (
    get_upcoming_events,
    get_event_description,
    fetch_investing_calendar,
    ECONOMIC_EVENTS,
    ECONOMIC_CALENDAR_2025,
)


class TestGetUpcomingEvents:
    """다가오는 이벤트 테스트"""
    
    def test_returns_list(self):
        """리스트 반환"""
        result = get_upcoming_events()
        assert isinstance(result, list)
    
    def test_events_have_required_keys(self):
        """이벤트에 필수 키가 있는지"""
        result = get_upcoming_events(days=365)  # 1년 범위로 확장
        if result:
            required_keys = ["date", "event", "name"]
            for key in required_keys:
                assert key in result[0], f"Missing key: {key}"
    
    def test_events_sorted_by_date(self):
        """이벤트가 날짜순 정렬인지"""
        result = get_upcoming_events(days=365)
        if len(result) > 1:
            dates = [e["date"] for e in result]
            assert dates == sorted(dates)


class TestGetEventDescription:
    """이벤트 설명 테스트"""
    
    def test_returns_dict(self):
        """딕셔너리 반환"""
        result = get_event_description("FOMC")
        assert isinstance(result, dict)
    
    def test_fomc_description(self):
        """FOMC 설명이 있는지"""
        result = get_event_description("FOMC")
        assert "name" in result
        assert "FOMC" in result["name"] or "금리" in result["name"]
    
    def test_cpi_description(self):
        """CPI 설명이 있는지"""
        result = get_event_description("CPI")
        assert "name" in result
    
    def test_unknown_event_returns_default(self):
        """알 수 없는 이벤트는 기본값 반환"""
        result = get_event_description("UNKNOWN_EVENT_XYZ")
        assert "name" in result
        assert "impact" in result


class TestFetchInvestingCalendar:
    """Investing.com 캘린더 테스트"""
    
    def test_returns_list(self):
        """리스트 반환"""
        result = fetch_investing_calendar()
        assert isinstance(result, list)


class TestEconomicEvents:
    """경제 이벤트 상수 테스트"""
    
    def test_has_major_events(self):
        """주요 이벤트가 있는지"""
        major_events = ["FOMC", "CPI", "NFP", "GDP"]
        for event in major_events:
            assert event in ECONOMIC_EVENTS, f"Missing event: {event}"
    
    def test_event_has_required_fields(self):
        """이벤트에 필수 필드가 있는지"""
        for event, info in ECONOMIC_EVENTS.items():
            assert "name" in info, f"{event} missing name"
            assert "impact" in info, f"{event} missing impact"
            assert "desc" in info, f"{event} missing desc"
    
    def test_impact_has_emoji(self):
        """영향도에 이모지가 있는지"""
        for event, info in ECONOMIC_EVENTS.items():
            impact = info["impact"]
            assert any(emoji in impact for emoji in ["🔴", "🟡", "🟢"]), f"{event} impact missing emoji"


class TestEconomicCalendar2025:
    """2025년 캘린더 테스트"""
    
    def test_is_list(self):
        """리스트인지"""
        assert isinstance(ECONOMIC_CALENDAR_2025, list)
    
    def test_has_events(self):
        """이벤트가 있는지"""
        assert len(ECONOMIC_CALENDAR_2025) > 0
    
    def test_event_has_date(self):
        """이벤트에 날짜가 있는지"""
        for item in ECONOMIC_CALENDAR_2025:
            assert "date" in item
            assert "event" in item
    
    def test_date_format(self):
        """날짜 형식이 올바른지"""
        from datetime import datetime
        for item in ECONOMIC_CALENDAR_2025:
            try:
                datetime.strptime(item["date"], "%Y-%m-%d")
            except ValueError:
                pytest.fail(f"Invalid date format: {item['date']}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
