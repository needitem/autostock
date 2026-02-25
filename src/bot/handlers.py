# -*- coding: utf-8 -*-
"""Telegram callback handlers (rebalance-first UI)."""

from __future__ import annotations

import asyncio
import html
import json
import os
import sys
from pathlib import Path

from telegram import Update
from telegram.ext import ContextTypes

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot import formatters as fmt
from bot import keyboards as kb
from bot.scheduler_config import format_us_rebalance_message, format_us_report_message
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


def _format_rebalance_snapshot(payload: dict, src_path: Path | None = None) -> str:
    generated_at = str(payload.get("generated_at", "-") or "-")
    risk = payload.get("risk_on_off", {}) if isinstance(payload.get("risk_on_off"), dict) else {}
    label = str(risk.get("label", "neutral") or "neutral")
    score = risk.get("score")
    score_txt = "-"
    try:
        score_txt = f"{float(score):.2f}"
    except Exception:
        pass

    desired_exposure = float(payload.get("desired_exposure_pct", 0.0) or 0.0)
    achieved_exposure = float(payload.get("achieved_exposure_after_execution_pct", payload.get("achieved_exposure_pct", 0.0)) or 0.0)
    cash_pct = float(payload.get("executed_cash_pct", payload.get("cash_pct", 0.0)) or 0.0)

    weights = payload.get("executed_weights_pct")
    if not isinstance(weights, dict):
        weights = payload.get("weights_pct", {})
    if not isinstance(weights, dict):
        weights = {}

    top_rows = sorted(
        ((str(sym), float(w)) for sym, w in weights.items() if str(sym) and float(w) > 0),
        key=lambda x: x[1],
        reverse=True,
    )[:5]

    lines = [
        "<b>Latest Rebalance Snapshot</b>",
        "--------------------------",
        f"Generated: <code>{html.escape(generated_at)}</code>",
        f"Risk regime: <b>{html.escape(label)}</b> (score {html.escape(score_txt)})",
        f"Exposure target: <b>{desired_exposure:.2f}%</b>",
        f"Exposure achieved: <b>{achieved_exposure:.2f}%</b>",
        f"Cash: <b>{cash_pct:.2f}%</b>",
    ]

    if top_rows:
        lines.append("\nTop positions:")
        for sym, w in top_rows:
            lines.append(f"- <b>{html.escape(sym)}</b>: {w:.2f}%")

    if src_path is not None:
        lines.append(f"\nJSON: <code>{html.escape(str(src_path))}</code>")

    return "\n".join(lines)


async def handle_main(query) -> None:
    style = _style_from_query(query)

    text = "<b>AutoStock Rebalance Hub</b>\n" + ("-" * 26) + "\n\n"
    text += "Quick start:\n"
    text += "1) Run US report (daily data refresh)\n"
    text += "2) Run US rebalance (portfolio recommendation)\n"
    text += "3) Check latest rebalance snapshot\n\n"
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


async def handle_run_us_rebalance(query) -> None:
    await query.edit_message_text("US rebalance started. This can take a few minutes.")

    try:
        from pipelines.us_rebalance import run_us_rebalance

        result = await asyncio.to_thread(run_us_rebalance)
        payload = result.get("result") if isinstance(result, dict) else {}
        if not isinstance(payload, dict):
            payload = {}

        base_msg = format_us_rebalance_message(result if isinstance(result, dict) else {})
        snapshot = _format_rebalance_snapshot(payload)
        text = f"{base_msg}\n\n{snapshot}" if base_msg else snapshot
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"US rebalance failed: {exc}", reply_markup=kb.back())


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

    text = _format_rebalance_snapshot(payload, src_path=path)
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
    "run_us_report": handle_run_us_report,
    "run_us_rebalance": handle_run_us_rebalance,
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
