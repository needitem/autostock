# -*- coding: utf-8 -*-
"""Telegram inline keyboard builders."""

from __future__ import annotations

import os
import sys

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.strategy_support import iter_strategy_specs, latest_action_key, run_action_key


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


def inventory_enabled() -> bool:
    raw = str(os.getenv("INVENTORY_MODE_ENABLED", "1")).strip().lower()
    return raw in {"1", "true", "yes", "on", "y"}


def _strategy_action_rows() -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    for spec in iter_strategy_specs():
        rows.append(
            [
                btn(f"Run {spec.label}", run_action_key(spec.key)),
                btn(f"Latest {spec.label}", latest_action_key(spec.key)),
            ]
        )
    rows.extend(
        [
            [btn("Run US Rebalance", "run_us_rebalance")],
            [btn("Latest Rebalance", "latest_rebalance")],
        ]
    )
    return rows


def main_menu() -> InlineKeyboardMarkup:
    strategy_rows = _strategy_action_rows()
    rows = strategy_rows[:]
    rows.insert(len(list(iter_strategy_specs())), [btn("Run US Report", "run_us_report")])
    if inventory_enabled():
        rows.append([btn("Inventory Report (Beta)", "run_inventory_report")])
    rows.append([btn("Display Settings", "display_settings")])
    if trading_enabled():
        rows.append([btn("Trading", "trading_menu")])
    return InlineKeyboardMarkup(rows)


def back(to: str = "main", label: str = "Back") -> InlineKeyboardMarkup:
    if to == "main":
        return InlineKeyboardMarkup([[btn("Main", "main")]])
    return InlineKeyboardMarkup([[btn(label, to), btn("Main", "main")]])


def stock_detail(symbol: str) -> InlineKeyboardMarkup:  # noqa: ARG001 - kept for call-site compatibility
    return InlineKeyboardMarkup(_strategy_action_rows() + [[btn("Main", "main")]])


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
