# -*- coding: utf-8 -*-
"""Telegram callback handlers."""

from __future__ import annotations

import os
import sys

from telegram import Update
from telegram.ext import ContextTypes

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ai.analyzer import ai
from bot import formatters as fmt
from bot import keyboards as kb
from bot.scan_cache import get_scan_result
from bot.user_prefs import get_chat_style, normalize_style, set_chat_style, style_label
from core.indicators import get_full_analysis
from core.scoring import calculate_score
from core.stock_data import get_fear_greed_index, get_market_condition
from trading.kis_api import kis
from trading.portfolio import portfolio


def _chat_id_from_query(query) -> str:
    if getattr(query, "message", None) and getattr(query.message, "chat_id", None) is not None:
        return str(query.message.chat_id)
    if getattr(query, "from_user", None) and getattr(query.from_user, "id", None) is not None:
        return str(query.from_user.id)
    return ""


def _style_from_query(query) -> str:
    return get_chat_style(_chat_id_from_query(query))


async def send_long_message(query, text: str, max_len: int = 4000, reply_markup=None) -> None:
    """Send long HTML text by splitting on newline boundaries."""
    final_markup = reply_markup if reply_markup is not None else kb.back()

    if len(text) <= max_len:
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=final_markup)
        return

    parts: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= max_len:
            parts.append(remaining)
            break
        cut_pos = remaining.rfind("\n", 0, max_len)
        if cut_pos == -1:
            cut_pos = max_len
        parts.append(remaining[:cut_pos])
        remaining = remaining[cut_pos:].lstrip("\n")

    await query.edit_message_text(parts[0], parse_mode="HTML")
    for idx, part in enumerate(parts[1:], 1):
        if idx == len(parts) - 1:
            await query.message.reply_text(part, parse_mode="HTML", reply_markup=final_markup)
        else:
            await query.message.reply_text(part, parse_mode="HTML")


async def _guard_trading_ready(query) -> bool:
    if kb.trading_enabled():
        return True

    text = "<b>Trading is disabled</b>\n" + ("-" * 26) + "\n\n"
    text += "KIS API credentials are not configured.\n\n"
    text += "Required env vars: <code>KIS_APP_KEY</code>, <code>KIS_APP_SECRET</code>, <code>KIS_ACCOUNT_NO</code>"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    return False


def _diversify_by_sector(stocks: list[dict], limit: int = 10, max_per_sector: int = 3) -> list[dict]:
    if not stocks:
        return []

    picked: list[dict] = []
    counts: dict[str, int] = {}

    for stock in stocks:
        sector = str(stock.get("sector") or "Unknown")
        if counts.get(sector, 0) >= max_per_sector:
            continue
        picked.append(stock)
        counts[sector] = counts.get(sector, 0) + 1
        if len(picked) >= limit:
            return picked

    seen = {s.get("symbol") for s in picked}
    for stock in stocks:
        symbol = stock.get("symbol")
        if symbol in seen:
            continue
        picked.append(stock)
        seen.add(symbol)
        if len(picked) >= limit:
            break
    return picked


async def handle_main(query) -> None:
    style = _style_from_query(query)

    text = "<b>AutoStock</b>\n" + ("-" * 26) + "\n\n"
    text += "Quick start:\n"
    text += "1) See recommendations\n"
    text += "2) Analyze a ticker\n"
    text += "3) Check risk sentiment\n\n"
    text += f"Display mode: <b>{style_label(style)}</b>\n"
    text += f"Trading: {'ON' if kb.trading_enabled() else 'OFF'}\n\n"
    text += "Type a ticker directly for instant analysis. Example: <code>AAPL</code>"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.main_menu())


async def handle_help(query) -> None:
    text = "<b>Beginner Guide</b>\n" + ("-" * 26) + "\n\n"
    text += "1) <b>Recommendations</b>: quick buy/wait candidates\n"
    text += "2) <b>Analyze</b>: full score + entry/stop/target\n"
    text += "3) <b>Fear/Greed</b>: check current risk sentiment\n\n"
    text += "If unsure, use <b>Beginner</b> display mode."
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())


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


async def handle_recommend(query) -> None:
    await query.edit_message_text("Building recommendations... (can take 5-10 min)")

    try:
        from config import load_all_us_stocks

        style = _style_from_query(query)
        result, used_cache = get_scan_result(load_all_us_stocks(), max_age_sec=300)

        filtered: list[dict] = []
        for stock in result["results"]:
            score = stock.get("score", {})
            plan = stock.get("trade_plan", {})

            rr2 = float(plan.get("risk_reward", {}).get("rr2", 0) or 0)
            stage = str(plan.get("positioning", {}).get("stage", ""))
            position_pct = float(plan.get("execution", {}).get("position_pct", 0) or 0)
            liq = float(stock.get("liquidity_score", 0) or 0)
            rs63 = float(stock.get("relative_strength_63d", plan.get("positioning", {}).get("relative_strength_63d", 0)) or 0)
            event_level = stock.get("event_risk_level", "unknown")

            if score.get("risk", {}).get("score", 100) >= 68:
                continue
            if score.get("confidence", {}).get("score", 0) < 55:
                continue
            if not plan.get("tradeable", False):
                continue
            if rr2 < 1.25:
                continue
            if stage == "right_shoulder":
                continue
            if liq < 50:
                continue
            if rs63 < -4:
                continue
            if position_pct < 1.2:
                continue
            if event_level == "imminent":
                continue
            filtered.append(stock)

        if len(filtered) < 8:
            relaxed: list[dict] = []
            for stock in result["results"]:
                score = stock.get("score", {})
                plan = stock.get("trade_plan", {})
                rr2 = float(plan.get("risk_reward", {}).get("rr2", 0) or 0)
                stage = str(plan.get("positioning", {}).get("stage", ""))
                liq = float(stock.get("liquidity_score", 0) or 0)
                rs63 = float(stock.get("relative_strength_63d", plan.get("positioning", {}).get("relative_strength_63d", 0)) or 0)
                event_level = stock.get("event_risk_level", "unknown")

                if score.get("risk", {}).get("score", 100) >= 72:
                    continue
                if score.get("confidence", {}).get("score", 0) < 50:
                    continue
                if stage == "right_shoulder":
                    continue
                if rr2 < 1.1:
                    continue
                if liq < 45:
                    continue
                if rs63 < -8:
                    continue
                if event_level == "imminent":
                    continue
                relaxed.append(stock)
            filtered = relaxed

        ranked = sorted(
            filtered,
            key=lambda x: -x.get(
                "investability_score",
                x.get("quality_score", x.get("score", {}).get("total_score", 0)),
            ),
        )

        picks = _diversify_by_sector(ranked, limit=10, max_per_sector=3)
        text = fmt.format_recommendations(picks, result["total"], style=style)
        if used_cache:
            text += "\n\n<i>Using recent cached scan.</i>"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"Recommendation failed: {exc}", reply_markup=kb.back())


async def handle_scan(query) -> None:
    await query.edit_message_text("Running full scan... (can take 5-10 min)")

    try:
        from config import load_all_us_stocks

        style = _style_from_query(query)
        result, used_cache = get_scan_result(load_all_us_stocks(), max_age_sec=240)
        text = fmt.format_scan_brief(result["results"], result["total"], top_n=10, style=style)
        if used_cache:
            text += "\n\n<i>Using recent cached scan.</i>"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"Scan failed: {exc}", reply_markup=kb.back())


async def handle_ai_recommend(query) -> None:
    await query.edit_message_text("AI report generating...\n1/3 scan")

    try:
        from config import load_all_us_stocks, load_stock_categories

        style = _style_from_query(query)
        universe = load_all_us_stocks()
        categories = load_stock_categories()

        result, used_cache = get_scan_result(universe, max_age_sec=300)
        stocks = result["results"]

        cache_note = " (cache)" if used_cache else ""
        await query.edit_message_text(
            f"AI report generating...\n1/3 scan done{cache_note} ({len(stocks)})\n2/3 market context"
        )

        market_data = {
            "fear_greed": get_fear_greed_index(),
            "market_condition": get_market_condition(),
        }

        await query.edit_message_text(
            "AI report generating...\n"
            "1/3 scan done\n"
            "2/3 market context done\n"
            "3/3 AI analysis"
        )

        ai_result = ai.analyze_full_market(stocks, {}, market_data, categories)
        if "error" in ai_result:
            await query.edit_message_text(f"AI error: {ai_result['error']}", reply_markup=kb.back())
            return

        stats = ai_result.get("stats", {})
        header = f"<b>AI Report</b> ({ai_result.get('total', 0)} symbols)\n"
        header += f"avg RSI {stats.get('avg_rsi', 0):.0f} | avg score {stats.get('avg_score', 0):.0f}\n"
        if "avg_rs63_vs_qqq" in stats:
            header += f"avg RS63(vs QQQ) {stats.get('avg_rs63_vs_qqq', 0):+.1f}%p\n"
        header += f"oversold {stats.get('oversold', 0)} | overbought {stats.get('overbought', 0)}\n"
        header += f"strong trend {stats.get('strong_trend', 0)} | tradeable {stats.get('tradeable_count', 0)}\n"
        header += f"display mode: {style_label(style)}\n\n"

        await send_long_message(query, header + ai_result["analysis"], reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"AI analysis failed: {exc}", reply_markup=kb.back())


async def handle_analyze_menu(query) -> None:
    text = "<b>Analyze Ticker</b>\n" + ("-" * 26) + "\n\n"
    text += "Use buttons below or type ticker directly.\n"
    text += "Examples: <code>AAPL</code>, <code>TSLA</code>"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.analyze_menu())


async def handle_analyze_input(query) -> None:
    text = "<b>Manual Input Mode</b>\n" + ("-" * 26) + "\n\n"
    text += "Send just a ticker in chat.\n"
    text += "Examples: <code>NVDA</code>, <code>MSFT</code>"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back("analyze_menu", "Analyze"))


async def handle_fear_greed(query) -> None:
    await query.edit_message_text("Loading Fear & Greed...")
    try:
        style = _style_from_query(query)
        fg = get_fear_greed_index()
        text = fmt.format_fear_greed(fg, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"Fetch failed: {exc}", reply_markup=kb.back())


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


async def handle_analyze_stock(query, data: str) -> None:
    symbol = data[2:]
    await query.edit_message_text(f"Analyzing {symbol}...")

    try:
        style = _style_from_query(query)
        analysis = get_full_analysis(symbol)
        if analysis is None:
            await query.edit_message_text(f"No data for '{symbol}'", reply_markup=kb.back())
            return

        analysis["score"] = calculate_score(analysis)
        text = fmt.format_analysis(analysis, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.stock_detail(symbol))
    except Exception as exc:
        await query.edit_message_text(f"Analysis failed: {exc}", reply_markup=kb.back())


async def handle_ai_stock(query, data: str) -> None:
    symbol = data[3:]
    await query.edit_message_text(f"AI summary for {symbol}...")

    try:
        analysis = get_full_analysis(symbol)
        if analysis is None:
            await query.edit_message_text(f"'{symbol}' data not found", reply_markup=kb.back())
            return

        score = calculate_score(analysis)
        analysis["score"] = score
        analysis["total_score"] = score.get("total_score", 0)

        result = ai.analyze_stock(symbol, analysis)
        if "error" in result:
            await query.edit_message_text(f"AI error: {result['error']}", reply_markup=kb.stock_detail(symbol))
            return

        header = f"<b>{symbol} AI Summary</b>\n\n"
        await send_long_message(query, header + result["analysis"], reply_markup=kb.stock_detail(symbol))
    except Exception as exc:
        await query.edit_message_text(f"AI analysis failed: {exc}", reply_markup=kb.back())


EXACT_HANDLERS = {
    "main": handle_main,
    "help": handle_help,
    "display_settings": handle_display_settings,
    "recommend": handle_recommend,
    "scan": handle_scan,
    "ai_recommend": handle_ai_recommend,
    "analyze_menu": handle_analyze_menu,
    "analyze_input": handle_analyze_input,
    "fear_greed": handle_fear_greed,
    "trading_menu": handle_trading_menu,
    "balance": handle_balance,
    "orders": handle_orders,
    "api_status": handle_api_status,
}

PREFIX_HANDLERS = [
    ("style_", handle_set_style),
    ("ai_", handle_ai_stock),
    ("a_", handle_analyze_stock),
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
