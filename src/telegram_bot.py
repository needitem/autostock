import asyncio
import json
import os
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from config import TELEGRAM_BOT_TOKEN

CHAT_ID_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "chat_id.json")
POPULAR_STOCKS = ["AAPL", "MSFT", "NVDA", "TSLA", "GOOGL", "META", "AMZN", "AMD", "NFLX", "COST"]

STRATEGY_INFO = {
    "보수적 모멘텀": {"emoji": "🎯", "risk": "⭐ 낮음", "summary": "이미 잘 오르는 안전한 종목",
        "description": "이미 상승 중인 종목만 고르는 가장 안전한 전략.\n\n<b>조건:</b>\n• 주가가 50일선, 200일선 모두 위\n• RSI 40~60\n• 거래량 평균 이상\n\n<b>추천:</b> 초보자에게 가장 추천!"},
    "골든크로스": {"emoji": "✨", "risk": "⭐⭐ 중간", "summary": "상승 전환 신호",
        "description": "단기선(5일)이 장기선(20일)을 뚫고 올라가는 순간.\n\n<b>발생:</b> 5일선이 20일선 돌파 → 매수 가능\n<b>임박:</b> 1% 이내 → 곧 돌파할 수도\n\n<b>주의:</b> 임박은 아직 안 된 거라 확인 후 매수"},
    "볼린저 반등": {"emoji": "📊", "risk": "⭐⭐ 중간", "summary": "많이 떨어져서 반등",
        "description": "볼린저밴드 하단에서 반등하는 종목.\n\n• 상단: 비쌈 (과매수)\n• 하단: 쌈 (과매도) ← 여기서 반등하면 매수!\n\n<b>의미:</b> 너무 떨어져서 반등할 타이밍"},
    "MACD 크로스": {"emoji": "📈", "risk": "⭐⭐ 중간", "summary": "상승 힘 붙기 시작",
        "description": "MACD선이 시그널선을 위로 뚫으면 매수 신호.\n\n<b>의미:</b> 상승 힘이 붙기 시작함\n<b>활용:</b> 다른 전략과 함께 보면 신뢰도 UP"},
    "52주 신고가": {"emoji": "🏆", "risk": "⭐⭐⭐ 높음", "summary": "가장 강한 종목",
        "description": "52주 최고가 대비 -5% 이내 종목.\n\n<b>의미:</b> 강한 종목. 신고가 뚫으면 더 오를 수 있음\n\n<b>주의:</b> 고점에서 사는 거라 위험! 손절 철저히"},
    "급락 반등": {"emoji": "📉", "risk": "⭐⭐⭐ 높음", "summary": "바닥 찍고 반등",
        "description": "최근 10일 고점 대비 -10% 이상 하락 후 반등.\n\n<b>의미:</b> 바닥 찍고 올라오는 중\n\n<b>주의:</b> 진짜 반등인지 확인 필요! 가장 위험한 전략"},
    "거래량 급증": {"emoji": "🔥", "risk": "⭐⭐ 중간", "summary": "큰 손 유입 가능성",
        "description": "거래량이 평균의 2배 이상 + 주가 상승.\n\n<b>의미:</b> 큰 손이 사고 있을 수도\n<b>활용:</b> 왜 거래량이 터졌는지 뉴스 확인"},
}


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


def get_main_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 스캔 실행", callback_data="scan")],
        [InlineKeyboardButton("📊 종목 분석", callback_data="analyze_menu")],
        [InlineKeyboardButton("📚 전략 목록", callback_data="strategies"),
         InlineKeyboardButton("⚠️ 위험도", callback_data="risk")],
    ])


def get_analyze_keyboard():
    keyboard = []
    row = []
    for symbol in POPULAR_STOCKS:
        row.append(InlineKeyboardButton(symbol, callback_data=f"a_{symbol}"))
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔙 메인", callback_data="main")])
    return InlineKeyboardMarkup(keyboard)


def get_strategies_keyboard():
    keyboard = [[InlineKeyboardButton(f"{v['emoji']} {k}", callback_data=f"e_{k}")] for k, v in STRATEGY_INFO.items()]
    keyboard.append([InlineKeyboardButton("🔙 메인", callback_data="main")])
    return InlineKeyboardMarkup(keyboard)


def get_back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 메인", callback_data="main")]])


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_chat_id(str(update.effective_chat.id))
    await update.message.reply_text("✅ <b>등록 완료!</b>\n\n버튼을 눌러서 사용하세요 👇", 
                                     parse_mode="HTML", reply_markup=get_main_keyboard())


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    
    if data == "main":
        await query.edit_message_text("메인 메뉴 👇", reply_markup=get_main_keyboard())
    
    elif data == "scan":
        await query.edit_message_text("🔍 스캔 중...")
        try:
            from analyzer import scan_all_stocks
            result = scan_all_stocks()
            report = format_daily_report(result)
            await query.edit_message_text(report, parse_mode="HTML", reply_markup=get_back_keyboard())
        except Exception as e:
            await query.edit_message_text(f"스캔 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data == "analyze_menu":
        await query.edit_message_text("📊 분석할 종목 선택:", reply_markup=get_analyze_keyboard())
    
    elif data.startswith("a_"):
        symbol = data[2:]
        await query.edit_message_text(f"🔍 {symbol} 분석 중...")
        try:
            from analyzer import analyze_single_stock
            result = analyze_single_stock(symbol)
            if result:
                await query.edit_message_text(format_analysis(result), parse_mode="HTML", reply_markup=get_back_keyboard())
            else:
                await query.edit_message_text(f"'{symbol}' 데이터 없음", reply_markup=get_back_keyboard())
        except Exception as e:
            await query.edit_message_text(f"분석 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data == "strategies":
        await query.edit_message_text("📚 전략 선택:", reply_markup=get_strategies_keyboard())
    
    elif data.startswith("e_"):
        name = data[2:]
        info = STRATEGY_INFO.get(name, {})
        if info:
            text = f"{info['emoji']} <b>{name}</b>\n━━━━━━━━━━━━━━━━━━\n<b>위험도:</b> {info['risk']}\n\n{info['description']}"
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_strategies_keyboard())
    
    elif data == "risk":
        text = "⚠️ <b>위험도 가이드</b>\n━━━━━━━━━━━━━━━━━━\n\n⭐ 낮음: 🎯 보수적 모멘텀\n⭐⭐ 중간: ✨골든 📊볼린저 📈MACD 🔥거래량\n⭐⭐⭐ 높음: 🏆52주신고가 📉급락반등\n\n💡 손절 -7% 무조건 지키기!"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_back_keyboard())


def format_analysis(r: dict) -> str:
    text = f"📊 <b>{r['symbol']}</b>\n━━━━━━━━━━━━━━━━━━\n💰 ${r['price']}\n\n{r['risk_grade']} 위험도: {r['risk_score']}/100\n📝 {r['recommendation']}\n\n"
    text += f"• RSI: {r['rsi']}\n• 볼린저: {r['bb_position']}%\n• 52주: {r['position_52w']}%\n• 50일선: {r['ma50_gap']:+.1f}%\n• 5일: {r['change_5d']:+.1f}%\n\n"
    if r['warnings']:
        text += "<b>⚠️ 주의:</b>\n" + "\n".join(r['warnings']) + "\n\n"
    if r['strategies_matched']:
        text += "<b>✅ 매칭:</b> " + ", ".join(r['strategies_matched'])
    else:
        text += "❌ 매칭 전략 없음"
    return text


def format_daily_report(scan_result: dict) -> str:
    from strategies import ALL_STRATEGIES
    market = scan_result["market"]
    strategy_results = scan_result["strategy_results"]
    
    report = f"📊 <b>일일 리포트</b>\n━━━━━━━━━━━━━━━━━━\n\n🚦 {market['emoji']} {market['message']}\nQQQ: ${market['price']} (50일선: ${market['ma50']})\n\n"
    
    has_signals = False
    for emoji, name, _ in ALL_STRATEGIES:
        stocks = strategy_results.get(name, [])
        if stocks:
            has_signals = True
            info = STRATEGY_INFO.get(name, {})
            report += f"{emoji} <b>[{name}]</b> {info.get('risk', '')}\n"
            for s in stocks:
                report += f"  • {s['symbol']} ${s['price']} {s.get('risk_grade', '')}\n"
            report += "\n"
    
    if not has_signals:
        report += "📭 오늘은 신호 없음\n\n"
    report += f"━━━━━━━━━━━━━━━━━━\n📌 스캔: {scan_result['total_scanned']}개"
    return report


async def send_message(text: str) -> bool:
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
    return asyncio.run(send_message(text))


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 스캔 중...")
    from analyzer import scan_all_stocks
    result = scan_all_stocks()
    await update.message.reply_text(format_daily_report(result), parse_mode="HTML", reply_markup=get_back_keyboard())


async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("종목 선택:", reply_markup=get_analyze_keyboard())
        return
    symbol = context.args[0].upper()
    from analyzer import analyze_single_stock
    result = analyze_single_stock(symbol)
    if result:
        await update.message.reply_text(format_analysis(result), parse_mode="HTML", reply_markup=get_back_keyboard())
    else:
        await update.message.reply_text(f"'{symbol}' 데이터 없음")


def run_bot():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("scan", scan_command))
    app.add_handler(CommandHandler("analyze", analyze_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    print("봇 실행 중... /start 로 시작")
    app.run_polling()


if __name__ == "__main__":
    run_bot()
