# -*- coding: utf-8 -*-
"""Telegram inline keyboard builders."""

from __future__ import annotations

import os
import sys

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TOP_STOCKS = [
    "NVDA",
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "TSLA",
    "AMD",
    "NFLX",
    "AVGO",
    "SPY",
    "QQQ",
]


def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text, callback_data=data)


def _looks_real(value: str | None) -> bool:
    if not value:
        return False
    v = value.strip()
    if not v:
        return False
    return not v.lower().startswith("your_")


def trading_enabled() -> bool:
    """Return True only when KIS credentials look configured."""
    return all(
        _looks_real(os.getenv(key))
        for key in ("KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO")
    )


def grid(items: list[str], prefix: str, cols: int = 4) -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for item in items:
        row.append(btn(item, f"{prefix}{item}"))
        if len(row) == cols:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


def main_menu() -> InlineKeyboardMarkup:
    rows = [
        [btn("🚀 오늘 뭐 살까", "recommend"), btn("🔎 시장 훑어보기", "scan")],
        [btn("📊 종목 쉽게 보기", "analyze_menu"), btn("🤖 AI 요약", "ai_recommend")],
        [btn("👀 관심종목", "watchlist_main"), btn("😱 시장 분위기", "fear_greed")],
        [btn("⚙️ 초보/표준 설정", "display_settings")],
    ]
    if trading_enabled():
        rows.append([btn("💰 트레이딩", "trading_menu")])
    return InlineKeyboardMarkup(rows)


def back(to: str = "main", label: str = "메인") -> InlineKeyboardMarkup:
    if to == "main":
        return InlineKeyboardMarkup([[btn("🏠 메인", "main")]])
    return InlineKeyboardMarkup([[btn(f"◀ {label}", to), btn("🏠 메인", "main")]])


def analyze_menu() -> InlineKeyboardMarkup:
    kb = grid(TOP_STOCKS, "a_", cols=4)
    kb.append([btn("⌨️ 티커 직접 입력", "analyze_input")])
    kb.append([btn("🏠 메인", "main")])
    return InlineKeyboardMarkup(kb)


def stock_detail(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [btn("🤖 AI 요약", f"ai_{symbol}"), btn("➕ 관심등록", f"watchadd_{symbol}")],
            [btn("📊 다른 종목", "analyze_menu"), btn("📈 추천 보기", "recommend")],
            [btn("🏠 메인", "main")],
        ]
    )


def trading_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [btn("💵 잔고", "balance"), btn("📋 미체결", "orders")],
            [btn("⚙️ 자동매매 설정", "auto_settings")],
            [btn("🔌 API 상태", "api_status")],
            [btn("🏠 메인", "main")],
        ]
    )


def watchlist_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [btn("📋 목록", "watchlist_status"), btn("⚡ 지금 체크", "watchlist_check_now")],
            [btn("➕ 종목 추가", "watchlist_add"), btn("➖ 종목 삭제", "watchlist_remove_menu")],
            [btn("⚙️ 알림 설정", "watchlist_alert_settings")],
            [btn("🏠 메인", "main")],
        ]
    )


def watchlist_remove_menu(stocks: list[str]) -> InlineKeyboardMarkup:
    kb = [[btn(f"➖ {symbol}", f"watchdel_{symbol}")] for symbol in stocks]
    kb.append([btn("◀ 관심종목", "watchlist_main")])
    return InlineKeyboardMarkup(kb)


def watchlist_alert_settings(settings: dict) -> InlineKeyboardMarkup:
    monitor_on = settings.get("monitor_enabled", True)
    interval = settings.get("monitor_interval", 30)
    monitor_status = "🟢 ON" if monitor_on else "🔴 OFF"

    return InlineKeyboardMarkup(
        [
            [btn(f"📡 모니터링: {monitor_status}", "toggle_monitor")],
            [btn(f"⏱ 체크 간격: {interval}분", "change_interval")],
            [btn("◀ 관심종목", "watchlist_main")],
        ]
    )


def watchlist_add() -> InlineKeyboardMarkup:
    kb = grid(TOP_STOCKS, "watchadd_", cols=4)
    kb.append([btn("◀ 관심종목", "watchlist_main")])
    return InlineKeyboardMarkup(kb)


def auto_settings_menu(auto_buy: bool, auto_sell: bool) -> InlineKeyboardMarkup:
    buy_status = "🟢 ON" if auto_buy else "🔴 OFF"
    sell_status = "🟢 ON" if auto_sell else "🔴 OFF"
    return InlineKeyboardMarkup(
        [
            [btn(f"🤖 자동매수: {buy_status}", "toggle_auto_buy")],
            [btn(f"🛑 자동손절: {sell_status}", "toggle_auto_sell")],
            [btn("💵 잔고", "balance"), btn("🔌 API", "api_status")],
            [btn("👀 관심종목", "watchlist_main"), btn("💰 트레이딩", "trading_menu")],
            [btn("🏠 메인", "main")],
        ]
    )


def display_settings_menu(current_style: str) -> InlineKeyboardMarkup:
    current = (current_style or "beginner").strip().lower()
    if current == "compact":
        current = "beginner"

    def style_btn(label: str, key: str) -> InlineKeyboardButton:
        mark = "✅ " if current == key else ""
        return btn(f"{mark}{label}", f"style_{key}")

    return InlineKeyboardMarkup(
        [
            [style_btn("초보(권장)", "beginner"), style_btn("표준", "standard"), style_btn("상세", "detail")],
            [btn("🏠 메인", "main")],
        ]
    )
