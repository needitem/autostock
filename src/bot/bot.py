"""
텔레그램 봇 메인 모듈
"""
import os
import sys
import json
import asyncio

# src 폴더를 path에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv

load_dotenv()

from bot.handlers import button_callback
from bot import keyboards as kb
from bot import formatters as fmt

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "chat_id.json")


def get_saved_chat_id() -> str | None:
    try:
        if os.path.exists(CHAT_ID_FILE):
            with open(CHAT_ID_FILE, "r") as f:
                return json.load(f).get("chat_id")
    except:
        pass
    return None


def save_chat_id(chat_id: str):
    os.makedirs(os.path.dirname(CHAT_ID_FILE), exist_ok=True)
    with open(CHAT_ID_FILE, "w") as f:
        json.dump({"chat_id": chat_id}, f)


async def send_long_message_bot(bot, chat_id: str, text: str, max_len: int = 4000):
    """긴 메시지 분할 전송 (Bot 객체용)"""
    if len(text) <= max_len:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        return
    
    parts = []
    while text:
        if len(text) <= max_len:
            parts.append(text)
            break
        cut_pos = text.rfind('\n', 0, max_len)
        if cut_pos == -1:
            cut_pos = max_len
        parts.append(text[:cut_pos])
        text = text[cut_pos:].lstrip('\n')
    
    for part in parts:
        await bot.send_message(chat_id=chat_id, text=part, parse_mode="HTML")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """시작 명령어"""
    save_chat_id(str(update.effective_chat.id))
    await update.message.reply_text(
        "✅ <b>등록 완료!</b>\n\n버튼을 눌러서 사용하세요 👇", 
        parse_mode="HTML", 
        reply_markup=kb.main_menu()
    )


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """스캔 명령어"""
    await update.message.reply_text("🔍 스캔 중...")
    try:
        from core.signals import scan_stocks
        from config import ALL_US_STOCKS
        
        result = scan_stocks(ALL_US_STOCKS)  # 전체 스캔
        text = f"🔍 <b>스캔 결과</b>\n분석: {result['total']}개"
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as e:
        await update.message.reply_text(f"스캔 실패: {e}", reply_markup=kb.back())


async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """분석 명령어"""
    if not context.args:
        await update.message.reply_text("종목 선택:", reply_markup=kb.analyze_menu())
        return
    
    symbol = context.args[0].upper()
    await update.message.reply_text(f"🔍 {symbol} 분석 중...")
    
    try:
        from core.indicators import get_full_analysis
        from core.scoring import calculate_score
        
        analysis = get_full_analysis(symbol)
        if analysis is None:
            await update.message.reply_text(f"'{symbol}' 데이터 없음")
            return
        
        score = calculate_score(analysis)
        analysis["score"] = score
        
        text = fmt.format_analysis(analysis)
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb.stock_detail(symbol))
    except Exception as e:
        await update.message.reply_text(f"분석 실패: {e}")


async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """텍스트 메시지 핸들러 - 종목 심볼 직접 입력"""
    text = update.message.text.strip().upper()
    
    # 1-5글자 영문만 종목으로 인식
    if not text.isalpha() or len(text) > 5:
        return
    
    symbol = text
    await update.message.reply_text(f"🔍 {symbol} 분석 중...")
    
    try:
        from core.indicators import get_full_analysis
        from core.scoring import calculate_score
        
        analysis = get_full_analysis(symbol)
        if analysis is None:
            await update.message.reply_text(
                f"❌ '{symbol}' 데이터를 찾을 수 없습니다.\n\n유효한 미국 주식 심볼인지 확인해주세요.",
                reply_markup=kb.back("analyze_menu", "종목분석")
            )
            return
        
        score = calculate_score(analysis)
        analysis["score"] = score
        
        text = fmt.format_analysis(analysis)
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb.stock_detail(symbol))
    except Exception as e:
        await update.message.reply_text(f"분석 실패: {e}", reply_markup=kb.back())


async def send_message(text: str) -> bool:
    """메시지 전송"""
    chat_id = get_saved_chat_id()
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return False
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        return True
    except:
        return False


def send_sync(text: str) -> bool:
    """동기 메시지 전송"""
    return asyncio.run(send_message(text))


async def scheduled_daily_scan(context):
    """스케줄된 일일 스캔 (22:00)"""
    chat_id = get_saved_chat_id()
    if not chat_id:
        print("[스케줄] Chat ID 없음, 스킵")
        return
    
    print("[스케줄] 일일 스캔 시작...")
    try:
        from core.signals import scan_stocks
        from config import ALL_US_STOCKS
        
        result = scan_stocks(ALL_US_STOCKS)
        
        text = f"📊 <b>일일 스캔</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        text += f"분석: {result['total']}개\n\n"
        
        # 상위 10개 종목
        top_stocks = sorted(result["results"], key=lambda x: -x.get("score", {}).get("total_score", 0))[:10]
        for r in top_stocks:
            score = r.get("score", {}).get("total_score", 0)
            text += f"• {r['symbol']} ${r['price']:.2f} | 점수: {score:.0f}\n"
        
        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        print("[스케줄] 일일 스캔 전송 완료")
    except Exception as e:
        print(f"[스케줄] 일일 스캔 실패: {e}")


async def scheduled_ai_recommendation(context):
    """스케줄된 AI 추천 (23:00)"""
    chat_id = get_saved_chat_id()
    if not chat_id:
        print("[스케줄] Chat ID 없음, 스킵")
        return
    
    print("[스케줄] AI 추천 분석 시작...")
    try:
        from core.signals import scan_stocks
        from ai.analyzer import ai
        from config import ALL_US_STOCKS, STOCK_CATEGORIES
        
        result = scan_stocks(ALL_US_STOCKS)  # 전체 스캔
        ai_result = ai.analyze_recommendations(result["results"])
        
        if "error" in ai_result:
            print(f"[스케줄] AI 분석 실패: {ai_result['error']}")
            return
        
        text = f"🤖 <b>AI 추천</b> ({ai_result.get('total', 0)}개 분석)\n━━━━━━━━━━━━━━━━━━\n\n{ai_result['analysis']}"
        
        # 긴 메시지 분할 전송
        await send_long_message_bot(context.bot, chat_id, text)
        print("[스케줄] AI 추천 전송 완료")
    except Exception as e:
        print(f"[스케줄] AI 추천 실패: {e}")


async def scheduled_watchlist_scan(context):
    """스케줄된 관심종목 스캔 및 자동매매 (21:00)"""
    chat_id = get_saved_chat_id()
    if not chat_id:
        return
    
    print("[스케줄] 자동매매 스캔 시작...")
    try:
        from trading.watchlist import watchlist
        from trading.portfolio import portfolio
        
        # 1. 자동손절 체크
        auto_sell = watchlist._load()["settings"].get("auto_sell", False)
        if auto_sell:
            sell_results = portfolio.auto_sell_losers()
            if sell_results and not any("message" in r for r in sell_results):
                text = fmt.format_trade_result("손절", sell_results)
                await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
                print("[스케줄] 자동손절 실행 완료")
        
        # 2. 저점 신호 스캔
        signals = watchlist.scan_signals()
        if not signals:
            print("[스케줄] 저점 신호 없음")
            return
        
        text = f"🚨 <b>저점 신호 발생!</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        for s in signals:
            text += f"<b>{s['symbol']}</b> - {s.get('strength', '보통')}\n"
            text += f"   현재: ${s.get('price', 0)} | RSI: {s.get('rsi', 50):.0f}\n\n"
        
        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        
        # 3. 자동매수 실행
        if watchlist.is_auto_buy():
            results = portfolio.auto_buy_signals()
            result_text = fmt.format_trade_result("매수", results)
            await context.bot.send_message(chat_id=chat_id, text=result_text, parse_mode="HTML")
            print("[스케줄] 자동매수 실행 완료")
        
        print("[스케줄] 자동매매 스캔 완료")
    except Exception as e:
        print(f"[스케줄] 자동매매 스캔 실패: {e}")


async def scheduled_watchlist_monitor(context):
    """관심종목 30분 모니터링"""
    chat_id = get_saved_chat_id()
    if not chat_id:
        return
    
    try:
        from trading.watchlist import watchlist
        from trading.monitor import monitor
        
        # 모니터링 활성화 체크
        data = watchlist._load()
        if not data["settings"].get("monitor_enabled", True):
            return
        
        # 관심종목 체크
        results = monitor.check_all_watchlist()
        
        if results:
            text = monitor.format_alert_message(results)
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
            print(f"[모니터] 알림 전송: {len(results)}개 종목")
    except Exception as e:
        print(f"[모니터] 체크 실패: {e}")


def run_bot(with_scheduler: bool = True):
    """봇 실행"""
    from datetime import time as dt_time
    import pytz
    
    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN이 설정되지 않았습니다.")
        return
    
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("scan", scan_command))
    app.add_handler(CommandHandler("analyze", analyze_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_handler))
    
    if with_scheduler:
        kst = pytz.timezone("Asia/Seoul")
        
        # 30분마다 관심종목 모니터링 (미국장 시간: 한국 23:30 ~ 06:00)
        app.job_queue.run_repeating(
            scheduled_watchlist_monitor,
            interval=1800,  # 30분 = 1800초
            first=10,  # 시작 후 10초 뒤 첫 실행
            name="watchlist_monitor"
        )
        
        app.job_queue.run_daily(
            scheduled_watchlist_scan,
            time=dt_time(hour=21, minute=0, tzinfo=kst),
            name="watchlist_scan"
        )
        
        app.job_queue.run_daily(
            scheduled_daily_scan,
            time=dt_time(hour=22, minute=0, tzinfo=kst),
            name="daily_scan"
        )
        
        app.job_queue.run_daily(
            scheduled_ai_recommendation,
            time=dt_time(hour=23, minute=0, tzinfo=kst),
            name="ai_recommendation"
        )
        
        print("=" * 50)
        print("📅 스케줄러 포함 봇 실행 중...")
        print("=" * 50)
        print("• 30분마다 - 관심종목 모니터링 🔔")
        print("• 21:00 - 자동매매 (저점매수/손절매도)")
        print("• 22:00 - 일일 스캔")
        print("• 23:00 - AI 매수/매도 추천")
        print("=" * 50)
    else:
        print("봇 실행 중... (스케줄러 없음)")
    
    print("/start 로 시작")
    app.run_polling()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--no-schedule":
        run_bot(with_scheduler=False)
    else:
        run_bot(with_scheduler=True)
