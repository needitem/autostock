"""Telegram bot entry module."""

from __future__ import annotations

import asyncio
import json
import os
import sys

from dotenv import load_dotenv
from telegram import Update
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot import formatters as fmt
from bot import keyboards as kb
from bot.handlers import button_callback
from bot.scheduler_config import (
    format_us_rebalance_message,
    format_us_report_message,
    schedule_settings,
)
from bot.user_prefs import get_chat_style, normalize_style, set_chat_style, style_label

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "chat_id.json")


def get_saved_chat_id() -> str | None:
    try:
        if os.path.exists(CHAT_ID_FILE):
            with open(CHAT_ID_FILE, "r", encoding="utf-8") as f:
                return json.load(f).get("chat_id")
    except Exception:
        pass
    return None


def save_chat_id(chat_id: str) -> None:
    os.makedirs(os.path.dirname(CHAT_ID_FILE), exist_ok=True)
    with open(CHAT_ID_FILE, "w", encoding="utf-8") as f:
        json.dump({"chat_id": chat_id}, f)


async def send_long_message_bot(bot, chat_id: str, text: str, max_len: int = 4000) -> None:
    """Send long HTML text by chunking on newline boundaries."""
    if len(text) <= max_len:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        return

    parts: list[str] = []
    while text:
        if len(text) <= max_len:
            parts.append(text)
            break
        cut_pos = text.rfind("\n", 0, max_len)
        if cut_pos == -1:
            cut_pos = max_len
        parts.append(text[:cut_pos])
        text = text[cut_pos:].lstrip("\n")

    for part in parts:
        await bot.send_message(chat_id=chat_id, text=part, parse_mode="HTML")


async def app_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prevent silent crashes and handle malformed HTML parse errors gracefully."""
    err = context.error
    print(f"[bot] handler error: {err}")

    if isinstance(err, BadRequest) and "Can't parse entities" in str(err):
        try:
            cb = getattr(update, "callback_query", None)
            if cb is not None:
                await cb.answer("Message format error. Please try again.")
                await cb.message.reply_text("Message format error occurred. Please retry.")
                return
            msg = getattr(update, "message", None)
            if msg is not None:
                await msg.reply_text("Message format error occurred. Please retry.")
        except Exception as inner:
            print(f"[bot] error handler fallback failed: {inner}")


async def _send_main_menu(update: Update) -> None:
    if update.effective_chat is None or update.message is None:
        return

    chat_id = str(update.effective_chat.id)
    save_chat_id(chat_id)
    style = get_chat_style(chat_id)

    text = "<b>AutoStock Rebalance Hub</b>\n" + ("-" * 26) + "\n\n"
    text += "Quick start:\n"
    text += "1) Run US report (daily data refresh)\n"
    text += "2) Run US rebalance (portfolio recommendation)\n"
    text += "3) Check latest rebalance snapshot\n\n"
    text += f"Display mode: <b>{style_label(style)}</b>\n"
    text += "You can also type a ticker directly for chart analysis.\n"
    text += "Example: <code>AAPL</code>, <code>TSLA</code>"
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb.main_menu())


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_main_menu(update)


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_main_menu(update)


async def style_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.message is None:
        return

    chat_id = str(update.effective_chat.id)
    save_chat_id(chat_id)

    if context.args:
        requested = normalize_style(context.args[0])
        saved = set_chat_style(chat_id, requested)
        text = f"Display mode set to <b>{style_label(saved)}</b>."
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb.display_settings_menu(saved))
        return

    current = get_chat_style(chat_id)
    text = "<b>Display Mode</b>\n" + ("-" * 26) + "\n\n"
    text += f"Current: <b>{style_label(current)}</b>\n\n"
    text += "Recommended: beginner\n"
    text += "Command: <code>/style beginner</code> | <code>/style standard</code> | <code>/style detail</code>"
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb.display_settings_menu(current))


async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None or update.effective_chat is None:
        return

    if not context.args:
        await update.message.reply_text("Please enter a ticker. Example: /analyze AAPL", reply_markup=kb.back())
        return

    symbol = context.args[0].upper()
    await update.message.reply_text(f"Analyzing {symbol}...")

    try:
        from core.indicators import get_full_analysis
        from core.scoring import calculate_score

        chat_id = str(update.effective_chat.id)
        style = get_chat_style(chat_id)

        analysis = get_full_analysis(symbol)
        if analysis is None:
            await update.message.reply_text(f"No data found for '{symbol}'.")
            return

        analysis["score"] = calculate_score(analysis)
        text = fmt.format_analysis(analysis, style=style)
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb.stock_detail(symbol))
    except Exception as e:
        await update.message.reply_text(f"Analysis failed: {e}")


async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None or update.effective_chat is None:
        return

    text = update.message.text.strip().upper()
    if not text.isalpha() or len(text) > 5:
        return

    symbol = text
    await update.message.reply_text(f"Analyzing {symbol}...")

    try:
        from core.indicators import get_full_analysis
        from core.scoring import calculate_score

        chat_id = str(update.effective_chat.id)
        style = get_chat_style(chat_id)

        analysis = get_full_analysis(symbol)
        if analysis is None:
            await update.message.reply_text(
                f"No data found for '{symbol}'.\n\nPlease send a valid US ticker.",
                reply_markup=kb.back(),
            )
            return

        analysis["score"] = calculate_score(analysis)
        msg = fmt.format_analysis(analysis, style=style)
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=kb.stock_detail(symbol))
    except Exception as e:
        await update.message.reply_text(f"Analysis failed: {e}", reply_markup=kb.back())


async def _run_us_report_pipeline() -> dict:
    from pipelines.us_orchestrator import run_all_us_engines

    return await asyncio.to_thread(run_all_us_engines)


async def _run_us_rebalance_pipeline() -> dict:
    from pipelines.us_rebalance import run_us_rebalance

    return await asyncio.to_thread(run_us_rebalance)


async def us_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None or update.effective_chat is None:
        return
    chat_id = str(update.effective_chat.id)
    save_chat_id(chat_id)
    await update.message.reply_text("US report started. This can take a few minutes.")

    try:
        result = await _run_us_report_pipeline()
        await update.message.reply_text(format_us_report_message(result))
    except Exception as e:
        await update.message.reply_text(f"US report failed: {e}")


async def us_rebalance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None or update.effective_chat is None:
        return
    chat_id = str(update.effective_chat.id)
    save_chat_id(chat_id)
    await update.message.reply_text("US rebalance started. This can take a few minutes.")

    try:
        result = await _run_us_rebalance_pipeline()
        await update.message.reply_text(format_us_rebalance_message(result))
    except Exception as e:
        await update.message.reply_text(f"US rebalance failed: {e}")


async def scheduled_us_report(context) -> None:
    print("[scheduler] daily us report started")
    try:
        result = await _run_us_report_pipeline()
        report_path = result.get("report_path", "")
        print(f"[scheduler] daily us report saved: {report_path}")

        chat_id = get_saved_chat_id()
        if chat_id:
            msg = format_us_report_message(result)
            await context.bot.send_message(chat_id=chat_id, text=msg)
    except Exception as e:
        print(f"[scheduler] daily us report failed: {e}")


async def scheduled_us_rebalance(context) -> None:
    print("[scheduler] weekly us rebalance started")
    try:
        result = await _run_us_rebalance_pipeline()
        print("[scheduler] weekly us rebalance saved")

        chat_id = get_saved_chat_id()
        if chat_id:
            msg = format_us_rebalance_message(result)
            await context.bot.send_message(chat_id=chat_id, text=msg)
    except Exception as e:
        print(f"[scheduler] weekly us rebalance failed: {e}")


def run_bot(with_scheduler: bool = True) -> None:
    from datetime import time as dt_time

    import pytz

    if not TELEGRAM_BOT_TOKEN:
        print("TELEGRAM_BOT_TOKEN not set")
        return

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("menu", menu_command))
    app.add_handler(CommandHandler("style", style_command))
    app.add_handler(CommandHandler("analyze", analyze_command))
    app.add_handler(CommandHandler("us_report", us_report_command))
    app.add_handler(CommandHandler("us_rebalance", us_rebalance_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_handler))
    app.add_error_handler(app_error_handler)

    if with_scheduler:
        settings = schedule_settings()
        tz_name = str(settings["timezone"])
        bot_tz = pytz.timezone(tz_name)

        app.job_queue.run_daily(
            scheduled_us_report,
            time=dt_time(
                hour=int(settings["report_hour"]),
                minute=int(settings["report_minute"]),
                tzinfo=bot_tz,
            ),
            name="daily_us_report",
        )
        app.job_queue.run_daily(
            scheduled_us_rebalance,
            time=dt_time(
                hour=int(settings["rebalance_hour"]),
                minute=int(settings["rebalance_minute"]),
                tzinfo=bot_tz,
            ),
            days=(int(settings["rebalance_weekday"]),),
            name="weekly_us_rebalance",
        )

        print("=" * 52)
        print("Scheduler enabled")
        print("=" * 52)
        print("Daily 00:00 - US report")
        print("Weekly 00:10 - US rebalance")
        print("=" * 52)
    else:
        print("Bot started without scheduler")

    print("Use /start or /menu")
    app.run_polling()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--no-schedule":
        run_bot(with_scheduler=False)
    else:
        run_bot(with_scheduler=True)
