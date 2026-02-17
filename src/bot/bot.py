"""Telegram bot entry module."""

from __future__ import annotations

import json
import os
import sys

from dotenv import load_dotenv
from telegram import Update
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
from bot.scan_cache import get_scan_result
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


async def _send_main_menu(update: Update) -> None:
    chat_id = str(update.effective_chat.id)
    save_chat_id(chat_id)
    style = get_chat_style(chat_id)

    text = "👋 <b>AutoStock 시작</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "처음이면 아래 순서로 보세요.\n"
    text += "1) 🚀 오늘 뭐 살까\n"
    text += "2) 📊 종목 쉽게 보기\n"
    text += "3) 👀 관심종목 등록\n\n"
    text += f"현재 화면 모드: <b>{style_label(style)}</b>\n"
    text += "티커를 직접 보내도 바로 분석됩니다.\n"
    text += "예: <code>AAPL</code>, <code>TSLA</code>"
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb.main_menu())


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_main_menu(update)


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_main_menu(update)


async def style_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = str(update.effective_chat.id)
    save_chat_id(chat_id)

    if context.args:
        requested = normalize_style(context.args[0])
        saved = set_chat_style(chat_id, requested)
        text = f"⚙️ 화면 모드를 <b>{style_label(saved)}</b>로 설정했습니다."
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb.display_settings_menu(saved))
        return

    current = get_chat_style(chat_id)
    text = "⚙️ <b>화면 모드 설정</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += f"현재: <b>{style_label(current)}</b>\n\n"
    text += "추천: beginner(초보)\n"
    text += "명령 예시: <code>/style beginner</code> | <code>/style standard</code> | <code>/style detail</code>"
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb.display_settings_menu(current))


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🔍 스캔 중...")

    try:
        from config import load_all_us_stocks

        chat_id = str(update.effective_chat.id)
        style = get_chat_style(chat_id)
        result, used_cache = get_scan_result(load_all_us_stocks(), max_age_sec=240)

        text = fmt.format_scan_brief(result["results"], result["total"], top_n=10, style=style)
        if used_cache:
            text += "\n\n<i>최근 스캔 캐시 사용</i>"
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as e:
        await update.message.reply_text(f"스캔 실패: {e}", reply_markup=kb.back())


async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("종목 선택:", reply_markup=kb.analyze_menu())
        return

    symbol = context.args[0].upper()
    await update.message.reply_text(f"🔍 {symbol} 분석 중...")

    try:
        from core.indicators import get_full_analysis
        from core.scoring import calculate_score

        chat_id = str(update.effective_chat.id)
        style = get_chat_style(chat_id)

        analysis = get_full_analysis(symbol)
        if analysis is None:
            await update.message.reply_text(f"'{symbol}' 데이터 없음")
            return

        analysis["score"] = calculate_score(analysis)
        text = fmt.format_analysis(analysis, style=style)
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb.stock_detail(symbol))
    except Exception as e:
        await update.message.reply_text(f"분석 실패: {e}")


async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text.strip().upper()
    if not text.isalpha() or len(text) > 5:
        return

    symbol = text
    await update.message.reply_text(f"🔍 {symbol} 분석 중...")

    try:
        from core.indicators import get_full_analysis
        from core.scoring import calculate_score

        chat_id = str(update.effective_chat.id)
        style = get_chat_style(chat_id)

        analysis = get_full_analysis(symbol)
        if analysis is None:
            await update.message.reply_text(
                f"❌ '{symbol}' 데이터를 찾을 수 없습니다.\n\n유효한 미국 주식 심볼인지 확인해주세요.",
                reply_markup=kb.back("analyze_menu", "종목분석"),
            )
            return

        analysis["score"] = calculate_score(analysis)
        msg = fmt.format_analysis(analysis, style=style)
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=kb.stock_detail(symbol))
    except Exception as e:
        await update.message.reply_text(f"분석 실패: {e}", reply_markup=kb.back())


async def scheduled_daily_scan(context) -> None:
    chat_id = get_saved_chat_id()
    if not chat_id:
        print("[scheduler] chat_id missing; skip daily scan")
        return

    print("[scheduler] daily scan started")
    try:
        from config import load_all_us_stocks

        style = get_chat_style(chat_id)
        result, used_cache = get_scan_result(load_all_us_stocks(), max_age_sec=3600)
        text = fmt.format_scan_brief(result["results"], result["total"], top_n=10, style=style)
        if used_cache:
            text += "\n\n<i>최근 스캔 캐시 사용</i>"

        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        print("[scheduler] daily scan sent")
    except Exception as e:
        print(f"[scheduler] daily scan failed: {e}")


async def scheduled_ai_recommendation(context) -> None:
    chat_id = get_saved_chat_id()
    if not chat_id:
        print("[scheduler] chat_id missing; skip ai recommendation")
        return

    print("[scheduler] ai recommendation started")
    try:
        from ai.analyzer import ai
        from config import load_all_us_stocks, load_stock_categories
        from core.stock_data import get_fear_greed_index, get_market_condition

        style = get_chat_style(chat_id)
        stocks = get_scan_result(load_all_us_stocks(), max_age_sec=5400)[0]["results"]
        market_data = {
            "fear_greed": get_fear_greed_index(),
            "market_condition": get_market_condition(),
        }
        ai_result = ai.analyze_full_market(stocks, {}, market_data, load_stock_categories())

        if "error" in ai_result:
            print(f"[scheduler] ai failed: {ai_result['error']}")
            return

        stats = ai_result.get("stats", {})
        text = f"🤖 <b>AI 리포트</b> ({ai_result.get('total', 0)}개 분석)\n"
        text += f"평균 RSI {stats.get('avg_rsi', 0):.0f} | 평균 점수 {stats.get('avg_score', 0):.0f}\n"
        text += f"표시 스타일: {style_label(style)}\n"
        text += "━━━━━━━━━━━━━━━━━━\n\n"
        text += ai_result["analysis"]
        await send_long_message_bot(context.bot, chat_id, text)
        print("[scheduler] ai recommendation sent")
    except Exception as e:
        print(f"[scheduler] ai recommendation failed: {e}")


async def scheduled_watchlist_scan(context) -> None:
    chat_id = get_saved_chat_id()
    if not chat_id:
        return

    print("[scheduler] watchlist scan started")
    try:
        from trading.portfolio import portfolio
        from trading.watchlist import watchlist

        style = get_chat_style(chat_id)
        auto_sell = watchlist._load()["settings"].get("auto_sell", False)
        if auto_sell:
            sell_results = portfolio.auto_sell_losers()
            if sell_results and not any("message" in r for r in sell_results):
                text = fmt.format_trade_result("손절", sell_results, style=style)
                await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")

        signals = watchlist.scan_signals()
        total = len(watchlist.get_all().get("stocks", {}))
        if signals:
            signal_text = fmt.format_watchlist_signals(signals, total, style=style)
            await context.bot.send_message(chat_id=chat_id, text=signal_text, parse_mode="HTML")
        else:
            print("[scheduler] no watchlist signals")

        if signals and watchlist.is_auto_buy():
            results = portfolio.auto_buy_signals()
            result_text = fmt.format_trade_result("매수", results, style=style)
            await context.bot.send_message(chat_id=chat_id, text=result_text, parse_mode="HTML")

        print("[scheduler] watchlist scan done")
    except Exception as e:
        print(f"[scheduler] watchlist scan failed: {e}")


async def scheduled_watchlist_monitor(context) -> None:
    chat_id = get_saved_chat_id()
    if not chat_id:
        return

    try:
        from trading.monitor import monitor
        from trading.watchlist import watchlist

        data = watchlist._load()
        settings = data.get("settings", {})
        if not settings.get("monitor_enabled", True):
            return

        results = monitor.check_all_watchlist()
        if results:
            text = monitor.format_alert_message(results)
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
            print(f"[monitor] alerts sent: {len(results)} symbols")
    except Exception as e:
        print(f"[monitor] failed: {e}")


def run_bot(with_scheduler: bool = True) -> None:
    from datetime import time as dt_time

    import pytz

    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN이 설정되지 않았습니다")
        return

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("menu", menu_command))
    app.add_handler(CommandHandler("style", style_command))
    app.add_handler(CommandHandler("scan", scan_command))
    app.add_handler(CommandHandler("analyze", analyze_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_handler))

    if with_scheduler:
        kst = pytz.timezone("Asia/Seoul")

        app.job_queue.run_repeating(
            scheduled_watchlist_monitor,
            interval=1800,
            first=10,
            name="watchlist_monitor",
        )
        app.job_queue.run_daily(
            scheduled_watchlist_scan,
            time=dt_time(hour=21, minute=0, tzinfo=kst),
            name="watchlist_scan",
        )
        app.job_queue.run_daily(
            scheduled_daily_scan,
            time=dt_time(hour=22, minute=0, tzinfo=kst),
            name="daily_scan",
        )
        app.job_queue.run_daily(
            scheduled_ai_recommendation,
            time=dt_time(hour=23, minute=0, tzinfo=kst),
            name="ai_recommendation",
        )

        print("=" * 52)
        print("🤟 스케줄러 포함 봇 실행 중...")
        print("=" * 52)
        print("🕒 30분마다 - 관심종목 모니터링")
        print("🕘 21:00 - 자동매매 (신호매수/손절매도)")
        print("🕙 22:00 - 일일 스캔")
        print("🕚 23:00 - AI 추천")
        print("=" * 52)
    else:
        print("봇 실행 중... (스케줄러 없음)")

    print("/start 또는 /menu 로 시작")
    app.run_polling()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--no-schedule":
        run_bot(with_scheduler=False)
    else:
        run_bot(with_scheduler=True)
