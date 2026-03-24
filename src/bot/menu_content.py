from __future__ import annotations

from bot.strategy_support import iter_strategy_specs
from bot.user_prefs import style_label


def build_main_menu_text(*, style: str, trading_enabled: bool, inventory_enabled: bool) -> str:
    text = "<b>AutoStock Rebalance Hub</b>\n" + ("-" * 26) + "\n\n"
    text += "Quick start:\n"
    step = 1
    for spec in iter_strategy_specs():
        text += f"{step}) Run {spec.label}\n"
        step += 1
    text += f"{step}) Run US rebalance (portfolio recommendation)\n"
    step += 1
    text += f"{step}) Check latest rebalance snapshot\n"
    if inventory_enabled:
        step += 1
        text += f"{step}) Run inventory report (beta)\n"
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
