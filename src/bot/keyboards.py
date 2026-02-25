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
        [btn("Today Picks", "recommend"), btn("Market Scan", "scan")],
        [btn("Stock Analyze", "analyze_menu"), btn("AI Report", "ai_recommend")],
        [btn("Fear/Greed", "fear_greed")],
        [btn("Display Settings", "display_settings")],
    ]
    if trading_enabled():
        rows.append([btn("Trading", "trading_menu")])
    return InlineKeyboardMarkup(rows)


def back(to: str = "main", label: str = "Back") -> InlineKeyboardMarkup:
    if to == "main":
        return InlineKeyboardMarkup([[btn("Main", "main")]])
    return InlineKeyboardMarkup([[btn(label, to), btn("Main", "main")]])


def analyze_menu() -> InlineKeyboardMarkup:
    keyboard = grid(TOP_STOCKS, "a_", cols=4)
    keyboard.append([btn("Type ticker manually", "analyze_input")])
    keyboard.append([btn("Main", "main")])
    return InlineKeyboardMarkup(keyboard)


def stock_detail(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [btn("AI Summary", f"ai_{symbol}")],
            [btn("Other Ticker", "analyze_menu"), btn("Recommendations", "recommend")],
            [btn("Main", "main")],
        ]
    )


def trading_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [btn("Balance", "balance"), btn("Open Orders", "orders")],
            [btn("API Status", "api_status")],
            [btn("Main", "main")],
        ]
    )


def display_settings_menu(current_style: str) -> InlineKeyboardMarkup:
    current = (current_style or "beginner").strip().lower()
    if current == "compact":
        current = "beginner"

    def style_btn(label: str, key: str) -> InlineKeyboardButton:
        mark = "* " if current == key else ""
        return btn(f"{mark}{label}", f"style_{key}")

    return InlineKeyboardMarkup(
        [
            [
                style_btn("Beginner", "beginner"),
                style_btn("Standard", "standard"),
                style_btn("Detail", "detail"),
            ],
            [btn("Main", "main")],
        ]
    )
