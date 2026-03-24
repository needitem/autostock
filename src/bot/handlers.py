# -*- coding: utf-8 -*-
"""Telegram callback handlers (strategy-first UI)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from telegram import Update
from telegram.ext import ContextTypes

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot import formatters as fmt
from bot import keyboards as kb
from bot.menu_content import build_display_settings_text, build_main_menu_text
from bot.scheduler_config import (
    format_inventory_report_message,
    format_rebalance_snapshot,
    format_us_rebalance_message,
    format_us_report_message,
)
from bot.strategy_support import (
    format_strategy_snapshot_from_result,
    get_strategy_spec,
    iter_strategy_specs,
    latest_action_key,
    latest_strategy_missing_text,
    load_latest_strategy,
    run_action_key,
    run_strategy,
)
from bot.user_prefs import get_chat_style, normalize_style, set_chat_style, style_label
from trading.kis_api import kis
from trading.portfolio import portfolio

ROOT_DIR = Path(__file__).resolve().parents[2]
REBALANCE_DIR = ROOT_DIR / "data" / "rebalance"


def _chat_id_from_query(query) -> str:
    if getattr(query, "message", None) and getattr(query.message, "chat_id", None) is not None:
        return str(query.message.chat_id)
    if getattr(query, "from_user", None) and getattr(query.from_user, "id", None) is not None:
        return str(query.from_user.id)
    return ""


def _style_from_query(query) -> str:
    return get_chat_style(_chat_id_from_query(query))


async def _guard_trading_ready(query) -> bool:
    if kb.trading_enabled():
        return True

    text = "<b>Trading is disabled</b>\n" + ("-" * 26) + "\n\n"
    text += "KIS API credentials are not configured.\n\n"
    text += "Required env vars: <code>KIS_APP_KEY</code>, <code>KIS_APP_SECRET</code>, <code>KIS_ACCOUNT_NO</code>"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    return False


def _latest_rebalance_json_path() -> Path | None:
    if not REBALANCE_DIR.exists():
        return None
    files = sorted(REBALANCE_DIR.glob("rebalance_recommendation_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


async def handle_main(query) -> None:
    style = _style_from_query(query)
    text = build_main_menu_text(
        style=style,
        trading_enabled=kb.trading_enabled(),
        inventory_enabled=kb.inventory_enabled(),
    )
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.main_menu())


async def handle_display_settings(query) -> None:
    style = _style_from_query(query)
    text = build_display_settings_text(style)
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.display_settings_menu(style))


async def handle_set_style(query, data: str) -> None:
    chat_id = _chat_id_from_query(query)
    requested = data[6:]
    style = normalize_style(requested)

    if not chat_id:
        await query.answer("Chat context missing")
        return

    saved = set_chat_style(chat_id, style)
    await query.answer(f"Display mode: {style_label(saved)}")
    await handle_display_settings(query)


async def handle_run_us_report(query) -> None:
    await query.edit_message_text("US report started. This can take a few minutes.")

    try:
        from pipelines.us_orchestrator import run_all_us_engines

        result = await asyncio.to_thread(run_all_us_engines)
        text = format_us_report_message(result)
        await query.edit_message_text(text, reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"US report failed: {exc}", reply_markup=kb.back())


async def _handle_run_strategy(query, strategy_key: str) -> None:
    spec = get_strategy_spec(strategy_key)
    await query.edit_message_text(f"{spec.label} baseline + verification started. This can take a few minutes.")
    try:
        result = await run_strategy(strategy_key, True)
        text = format_strategy_snapshot_from_result(strategy_key, result if isinstance(result, dict) else {})
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"{spec.label} run failed: {exc}", reply_markup=kb.back())


async def _handle_latest_strategy(query, strategy_key: str) -> None:
    spec = get_strategy_spec(strategy_key)
    try:
        result = await load_latest_strategy(strategy_key)
    except Exception as exc:
        await query.edit_message_text(f"Latest {spec.label} load failed: {exc}", reply_markup=kb.back())
        return

    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    if not summary:
        await query.edit_message_text(
            latest_strategy_missing_text(strategy_key),
            reply_markup=kb.back(),
        )
        return

    text = format_strategy_snapshot_from_result(strategy_key, result)
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())


def _make_run_strategy_handler(strategy_key: str):
    async def _handler(query) -> None:
        await _handle_run_strategy(query, strategy_key)

    return _handler


def _make_latest_strategy_handler(strategy_key: str):
    async def _handler(query) -> None:
        await _handle_latest_strategy(query, strategy_key)

    return _handler


async def handle_run_us_rebalance(query) -> None:
    await query.edit_message_text("US report + rebalance started. This can take a few minutes.")

    try:
        from pipelines.us_orchestrator import run_all_us_engines
        from pipelines.us_rebalance import run_us_rebalance

        report_result = await asyncio.to_thread(run_all_us_engines)
        report_path = str(report_result.get("report_path", "") or "")
        result = await asyncio.to_thread(run_us_rebalance, report_path if report_path else None)
        payload = result.get("result") if isinstance(result, dict) else {}
        if not isinstance(payload, dict):
            payload = {}

        base_msg = format_us_rebalance_message(result if isinstance(result, dict) else {})
        snapshot = format_rebalance_snapshot(payload)
        text = f"{base_msg}\n\n{snapshot}" if base_msg else snapshot
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"US rebalance failed: {exc}", reply_markup=kb.back())


async def handle_run_inventory_report(query) -> None:
    await query.edit_message_text("Inventory report started (beta).")

    try:
        from pipelines.inventory_report import run_inventory_report

        result = await asyncio.to_thread(run_inventory_report)
        text = format_inventory_report_message(result if isinstance(result, dict) else {})
        await query.edit_message_text(text, reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"Inventory report failed: {exc}", reply_markup=kb.back())


async def handle_latest_rebalance(query) -> None:
    path = _latest_rebalance_json_path()
    if path is None:
        await query.edit_message_text(
            "No rebalance output found yet.\nRun 'Run US Rebalance' first.",
            reply_markup=kb.back(),
        )
        return

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        await query.edit_message_text(f"Failed to read latest rebalance JSON: {exc}", reply_markup=kb.back())
        return

    if not isinstance(payload, dict):
        await query.edit_message_text("Latest rebalance JSON format is invalid.", reply_markup=kb.back())
        return

    text = format_rebalance_snapshot(payload, src_path=str(path))
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())


async def handle_trading_menu(query) -> None:
    if not await _guard_trading_ready(query):
        return

    text = "<b>Trading</b>\n" + ("-" * 26) + "\n\n"
    text += "Order and account utilities via KIS API.\n"
    text += "Use paper mode first before live account usage."
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())


async def handle_balance(query) -> None:
    if not await _guard_trading_ready(query):
        return

    await query.edit_message_text("Loading balance...")
    try:
        style = _style_from_query(query)
        result = portfolio.get_status()
        if "error" in result:
            await query.edit_message_text(f"Error: {result['error']}", reply_markup=kb.trading_menu())
            return

        text = fmt.format_balance(result, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())
    except Exception as exc:
        await query.edit_message_text(f"Balance fetch failed: {exc}", reply_markup=kb.trading_menu())


async def handle_orders(query) -> None:
    if not await _guard_trading_ready(query):
        return

    await query.edit_message_text("Loading open orders...")
    try:
        style = _style_from_query(query)
        result = kis.get_orders()
        if "error" in result:
            await query.edit_message_text(f"Error: {result['error']}", reply_markup=kb.trading_menu())
            return

        text = fmt.format_orders(result.get("orders", []), style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())
    except Exception as exc:
        await query.edit_message_text(f"Order fetch failed: {exc}", reply_markup=kb.trading_menu())


async def handle_api_status(query) -> None:
    if not await _guard_trading_ready(query):
        return

    try:
        style = _style_from_query(query)
        status = kis.check_status()
        text = fmt.format_api_status(status, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())
    except Exception as exc:
        await query.edit_message_text(f"Status check failed: {exc}", reply_markup=kb.trading_menu())


_STRATEGY_CALLBACK_HANDLERS = {
    spec.key: (_make_run_strategy_handler(spec.key), _make_latest_strategy_handler(spec.key))
    for spec in iter_strategy_specs()
}


EXACT_HANDLERS = {
    "main": handle_main,
    "display_settings": handle_display_settings,
    "run_us_report": handle_run_us_report,
    "run_us_rebalance": handle_run_us_rebalance,
    "run_inventory_report": handle_run_inventory_report,
    "latest_rebalance": handle_latest_rebalance,
    "trading_menu": handle_trading_menu,
    "balance": handle_balance,
    "orders": handle_orders,
    "api_status": handle_api_status,
}
for spec in iter_strategy_specs():
    run_handler, latest_handler = _STRATEGY_CALLBACK_HANDLERS[spec.key]
    EXACT_HANDLERS[run_action_key(spec.key)] = run_handler
    EXACT_HANDLERS[latest_action_key(spec.key)] = latest_handler

PREFIX_HANDLERS = [
    ("style_", handle_set_style),
]


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Route callback queries by exact/prefix handlers."""
    query = update.callback_query
    await query.answer()
    data = query.data

    if data in EXACT_HANDLERS:
        await EXACT_HANDLERS[data](query)
        return

    for prefix, handler in PREFIX_HANDLERS:
        if data.startswith(prefix):
            await handler(query, data)
            return

    await query.edit_message_text("Unknown menu action.", reply_markup=kb.main_menu())
