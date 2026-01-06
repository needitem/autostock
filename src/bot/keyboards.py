# -*- coding: utf-8 -*-
"""
텔레그램 키보드 모듈
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

TOP_STOCKS = ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "AMD", "NFLX", "AVGO"]


def btn(text, data):
    return InlineKeyboardButton(text, callback_data=data)


def grid(items, prefix, cols=5):
    rows = []
    row = []
    for item in items:
        row.append(btn(item, f"{prefix}{item}"))
        if len(row) == cols:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


def main_menu():
    return InlineKeyboardMarkup([
        [btn("📈 추천종목", "recommend"), btn("🔍 전체스캔", "scan")],
        [btn("🤖 AI 분석", "ai_recommend")],
        [btn("📊 종목분석", "analyze_menu"), btn("📂 카테고리", "category_menu")],
        [btn("�  시장심리", "fear_greed")],
        [btn("💰 트레이딩", "trading_menu")],
    ])


def back(to="main", label="메인"):
    return InlineKeyboardMarkup([[btn(f"← {label}", to)]])


def analyze_menu():
    kb = grid(TOP_STOCKS, "a_")
    kb.append([btn("✏️ 직접입력", "analyze_input")])
    kb.append([btn("← 메인", "main")])
    return InlineKeyboardMarkup(kb)


def stock_detail(symbol):
    return InlineKeyboardMarkup([
        [btn("🤖 AI분석", f"ai_{symbol}"), btn("👀 관심등록", f"watchadd_{symbol}")],
        [btn("← 메인", "main")],
    ])


def category_menu():
    from config import STOCK_CATEGORIES
    kb = []
    items = list(STOCK_CATEGORIES.items())
    for i in range(0, len(items), 2):
        row = []
        for name, info in items[i:i+2]:
            row.append(btn(f"{info['emoji']} {name}", f"cat_{name}"))
        kb.append(row)
    kb.append([btn("📊 전체요약", "cat_all")])
    kb.append([btn("← 메인", "main")])
    return InlineKeyboardMarkup(kb)


def trading_menu():
    return InlineKeyboardMarkup([
        [btn("📊 잔고조회", "balance"), btn("📋 미체결", "orders")],
        [btn("🤖 자동매매 설정", "auto_settings")],
        [btn("⚙️ API상태", "api_status")],
        [btn("← 메인", "main")],
    ])


def watchlist_menu():
    return InlineKeyboardMarkup([
        [btn("📋 현황보기", "watchlist_status")],
        [btn("➕ 종목추가", "watchlist_add")],
        [btn("← 자동매매", "auto_settings")],
    ])


def watchlist_add():
    kb = grid(TOP_STOCKS, "watchadd_")
    kb.append([btn("← 관심종목", "watchlist")])
    return InlineKeyboardMarkup(kb)


def auto_settings_menu(auto_buy: bool, auto_sell: bool):
    """자동매매 설정 메뉴"""
    buy_status = "✅ ON" if auto_buy else "❌ OFF"
    sell_status = "✅ ON" if auto_sell else "❌ OFF"
    return InlineKeyboardMarkup([
        [btn(f"🤖 자동매수: {buy_status}", "toggle_auto_buy")],
        [btn(f"🛑 자동손절: {sell_status}", "toggle_auto_sell")],
        [btn("👀 관심종목 관리", "watchlist")],
        [btn("📜 매매 기록", "trade_history")],
        [btn("← 트레이딩", "trading_menu")],
    ])



