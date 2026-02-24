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
        [btn("?? ?¤ëŠ˜ ë­??´ê¹Œ", "recommend"), btn("?” ?œì¥ ?‘ì–´ë³´ê¸°", "scan")],
        [btn("?“Š ì¢…ëª© ?½ê²Œ ë³´ê¸°", "analyze_menu"), btn("?¤– AI ?”ì•½", "ai_recommend")],
        [btn("?? ê´€?¬ì¢…ëª?, "watchlist_main"), btn("?˜± ?œì¥ ë¶„ìœ„ê¸?, "fear_greed")],
        [btn("?™ï¸ ì´ˆë³´/?œì? ?¤ì •", "display_settings")],
    ]
    if trading_enabled():
        rows.append([btn("?’° ?¸ë ˆ?´ë”©", "trading_menu")])
    return InlineKeyboardMarkup(rows)


def back(to: str = "main", label: str = "ë©”ì¸") -> InlineKeyboardMarkup:
    if to == "main":
        return InlineKeyboardMarkup([[btn("?  ë©”ì¸", "main")]])
    return InlineKeyboardMarkup([[btn(f"?€ {label}", to), btn("?  ë©”ì¸", "main")]])


def analyze_menu() -> InlineKeyboardMarkup:
    kb = grid(TOP_STOCKS, "a_", cols=4)
    kb.append([btn("?¨ï¸ ?°ì»¤ ì§ì ‘ ?…ë ¥", "analyze_input")])
    kb.append([btn("?  ë©”ì¸", "main")])
    return InlineKeyboardMarkup(kb)


def stock_detail(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [btn("?¤– AI ?”ì•½", f"ai_{symbol}"), btn("??ê´€?¬ë“±ë¡?, f"watchadd_{symbol}")],
            [btn("?“Š ?¤ë¥¸ ì¢…ëª©", "analyze_menu"), btn("?“ˆ ì¶”ì²œ ë³´ê¸°", "recommend")],
            [btn("?  ë©”ì¸", "main")],
        ]
    )


def trading_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [btn("?’µ ?”ê³ ", "balance"), btn("?“‹ ë¯¸ì²´ê²?, "orders")],
            [btn("?™ï¸ ?ë™ë§¤ë§¤ ?¤ì •", "auto_settings")],
            [btn("?”Œ API ?íƒœ", "api_status")],
            [btn("?  ë©”ì¸", "main")],
        ]
    )


def watchlist_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [btn("?“‹ ëª©ë¡", "watchlist_status"), btn("??ì§€ê¸?ì²´í¬", "watchlist_check_now")],
            [btn("??ì¢…ëª© ì¶”ê?", "watchlist_add"), btn("??ì¢…ëª© ?? œ", "watchlist_remove_menu")],
            [btn("?™ï¸ ?Œë¦¼ ?¤ì •", "watchlist_alert_settings")],
            [btn("?  ë©”ì¸", "main")],
        ]
    )


def watchlist_remove_menu(stocks: list[str]) -> InlineKeyboardMarkup:
    kb = [[btn(f"??{symbol}", f"watchdel_{symbol}")] for symbol in stocks]
    kb.append([btn("?€ ê´€?¬ì¢…ëª?, "watchlist_main")])
    return InlineKeyboardMarkup(kb)


def watchlist_alert_settings(settings: dict) -> InlineKeyboardMarkup:
    monitor_on = settings.get("monitor_enabled", True)
    interval = settings.get("monitor_interval", 30)
    monitor_status = "?Ÿ¢ ON" if monitor_on else "?”´ OFF"

    return InlineKeyboardMarkup(
        [
            [btn(f"?“¡ ëª¨ë‹ˆ?°ë§: {monitor_status}", "toggle_monitor")],
            [btn(f"??ì²´í¬ ê°„ê²©: {interval}ë¶?, "change_interval")],
            [btn("?€ ê´€?¬ì¢…ëª?, "watchlist_main")],
        ]
    )


def watchlist_add() -> InlineKeyboardMarkup:
    kb = grid(TOP_STOCKS, "watchadd_", cols=4)
    kb.append([btn("?€ ê´€?¬ì¢…ëª?, "watchlist_main")])
    return InlineKeyboardMarkup(kb)


def auto_settings_menu(auto_buy: bool, auto_sell: bool) -> InlineKeyboardMarkup:
    buy_status = "?Ÿ¢ ON" if auto_buy else "?”´ OFF"
    sell_status = "?Ÿ¢ ON" if auto_sell else "?”´ OFF"
    return InlineKeyboardMarkup(
        [
            [btn(f"?¤– ?ë™ë§¤ìˆ˜: {buy_status}", "toggle_auto_buy")],
            [btn(f"?›‘ ?ë™?ì ˆ: {sell_status}", "toggle_auto_sell")],
            [btn("?’µ ?”ê³ ", "balance"), btn("?”Œ API", "api_status")],
            [btn("?? ê´€?¬ì¢…ëª?, "watchlist_main"), btn("?’° ?¸ë ˆ?´ë”©", "trading_menu")],
            [btn("?  ë©”ì¸", "main")],
        ]
    )


def display_settings_menu(current_style: str) -> InlineKeyboardMarkup:
    current = (current_style or "beginner").strip().lower()
    if current == "compact":
        current = "beginner"

    def style_btn(label: str, key: str) -> InlineKeyboardButton:
        mark = "??" if current == key else ""
        return btn(f"{mark}{label}", f"style_{key}")

    return InlineKeyboardMarkup(
        [
            [style_btn("ì´ˆë³´(ê¶Œì¥)", "beginner"), style_btn("?œì?", "standard"), style_btn("?ì„¸", "detail")],
            [btn("?  ë©”ì¸", "main")],
        ]
    )
