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
from bot.scheduler_config import (
    format_inventory_report_message,
    format_rebalance_snapshot,
    format_strategy_v2_message,
    format_strategy_v2_snapshot,
    format_us_rebalance_message,
    format_us_report_message,
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

    text = "<b>AutoStock Rebalance Hub</b>\n" + ("-" * 26) + "\n\n"
    text += "Quick start:\n"
    text += "1) Run Strategy V2 baseline (regime ETF validation)\n"
    text += "2) Check latest Strategy V2 verification\n"
    text += "3) Run US rebalance (portfolio recommendation)\n"
    text += "4) Check latest rebalance snapshot\n"
    if kb.inventory_enabled():
        text += "5) Run inventory report (beta)\n"
    text += "\n"
    text += f"Display mode: <b>{style_label(style)}</b>\n"
    text += f"Trading: {'ON' if kb.trading_enabled() else 'OFF'}\n\n"
    text += "You can still type a ticker directly for chart analysis."
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.main_menu())


async def handle_display_settings(query) -> None:
    style = _style_from_query(query)
    text = "<b>Display Settings</b>\n" + ("-" * 26) + "\n\n"
    text += f"Current: <b>{style_label(style)}</b>\n\n"
    text += "Beginner: concise actions\n"
    text += "Standard: balanced detail\n"
    text += "Detail: full metrics"
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


async def handle_run_strategy_v2(query) -> None:
    await query.edit_message_text("Strategy V2 baseline + verification started. This can take a few minutes.")

    try:
        from pipelines.strategy_v2_pipeline import run_strategy_v2_pipeline

        result = await asyncio.to_thread(run_strategy_v2_pipeline, True)
        text = format_strategy_v2_message(result if isinstance(result, dict) else {})
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"Strategy V2 run failed: {exc}", reply_markup=kb.back())


async def handle_latest_strategy_v2(query) -> None:
    try:
        from pipelines.strategy_v2_pipeline import latest_strategy_v2_snapshot

        result = await asyncio.to_thread(latest_strategy_v2_snapshot)
    except Exception as exc:
        await query.edit_message_text(f"Latest Strategy V2 load failed: {exc}", reply_markup=kb.back())
        return

    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    if not summary:
        await query.edit_message_text(
            "No Strategy V2 baseline output found yet.\nRun 'Run Strategy V2' first.",
            reply_markup=kb.back(),
        )
        return

    verification = result.get("verification") if isinstance(result.get("verification"), dict) else {}
    text = format_strategy_v2_snapshot(
        summary,
        verification,
        summary_path=str(result.get("summary_path", "") or ""),
        verification_path=str(result.get("verification_json_path", "") or ""),
    )
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())


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


EXACT_HANDLERS = {
    "main": handle_main,
    "display_settings": handle_display_settings,
    "run_strategy_v2": handle_run_strategy_v2,
    "latest_strategy_v2": handle_latest_strategy_v2,
    "run_us_report": handle_run_us_report,
    "run_us_rebalance": handle_run_us_rebalance,
    "run_inventory_report": handle_run_inventory_report,
    "latest_rebalance": handle_latest_rebalance,
    "trading_menu": handle_trading_menu,
    "balance": handle_balance,
    "orders": handle_orders,
    "api_status": handle_api_status,
}

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
