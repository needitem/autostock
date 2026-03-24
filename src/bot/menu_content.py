from __future__ import annotations

from bot.user_prefs import style_label


def build_main_menu_text(*, style: str, trading_enabled: bool, inventory_enabled: bool) -> str:
    text = "<b>AutoStock Rebalance Hub</b>\n" + ("-" * 26) + "\n\n"
    text += "Quick start:\n"
    text += "1) Run Strategy V2 baseline (regime ETF validation)\n"
    text += "2) Run Strategy V14 dynamic defense (challenger)\n"
    text += "3) Run US rebalance (portfolio recommendation)\n"
    text += "4) Check latest rebalance snapshot\n"
    if inventory_enabled:
        text += "5) Run inventory report (beta)\n"
    text += "\n"
    text += f"Display mode: <b>{style_label(style)}</b>\n"
    text += f"Trading: {'ON' if trading_enabled else 'OFF'}\n"
    text += "You can also type a ticker directly for chart analysis.\n"
    text += "Example: <code>AAPL</code>, <code>TSLA</code>"
    return text


def build_display_settings_text(style: str) -> str:
    text = "<b>Display Settings</b>\n" + ("-" * 26) + "\n\n"
    text += f"Current: <b>{style_label(style)}</b>\n\n"
    text += "Beginner: concise actions\n"
    text += "Standard: balanced detail\n"
    text += "Detail: full metrics"
    return text
