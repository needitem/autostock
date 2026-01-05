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
        [InlineKeyboardButton("🌟 추천", callback_data="recommend"),
         InlineKeyboardButton("🔍 스캔", callback_data="scan")],
        [InlineKeyboardButton("🤖 AI추천", callback_data="ai_recommend"),
         InlineKeyboardButton("📊 종목분석", callback_data="analyze_menu")],
        [InlineKeyboardButton("📰 뉴스", callback_data="news_menu"),
         InlineKeyboardButton("😱 공포탐욕", callback_data="fear_greed")],
        [InlineKeyboardButton("🏭 섹터", callback_data="sectors"),
         InlineKeyboardButton("📅 일정", callback_data="calendar")],
        [InlineKeyboardButton("🔬 종합분석", callback_data="comprehensive_menu"),
         InlineKeyboardButton("📚 전략", callback_data="strategies")],
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


def get_news_keyboard():
    keyboard = []
    row = []
    for symbol in POPULAR_STOCKS:
        row.append(InlineKeyboardButton(symbol, callback_data=f"n_{symbol}"))
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🌍 시장뉴스", callback_data="market_news")])
    keyboard.append([InlineKeyboardButton("🔙 메인", callback_data="main")])
    return InlineKeyboardMarkup(keyboard)


def get_strategies_keyboard():
    keyboard = [[InlineKeyboardButton(f"{v['emoji']} {k}", callback_data=f"e_{k}")] for k, v in STRATEGY_INFO.items()]
    keyboard.append([InlineKeyboardButton("🔙 메인", callback_data="main")])
    return InlineKeyboardMarkup(keyboard)


def get_back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 메인", callback_data="main")]])


def get_comprehensive_keyboard():
    keyboard = []
    row = []
    for symbol in POPULAR_STOCKS:
        row.append(InlineKeyboardButton(symbol, callback_data=f"comp_{symbol}"))
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🌡️ 시장심리 종합", callback_data="market_sentiment")])
    keyboard.append([InlineKeyboardButton("🔙 메인", callback_data="main")])
    return InlineKeyboardMarkup(keyboard)


def get_stock_detail_keyboard(symbol: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 AI분석", callback_data=f"ai_{symbol}"),
         InlineKeyboardButton("📰 뉴스", callback_data=f"n_{symbol}")],
        [InlineKeyboardButton("👔 내부자", callback_data=f"insider_{symbol}"),
         InlineKeyboardButton("🎯 목표가", callback_data=f"target_{symbol}")],
        [InlineKeyboardButton("🔬 종합분석", callback_data=f"comp_{symbol}"),
         InlineKeyboardButton("📈 Finviz", callback_data=f"fv_{symbol}")],
        [InlineKeyboardButton("🔙 메인", callback_data="main")],
    ])


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
    
    elif data == "recommend":
        await query.edit_message_text("🌟 추천 종목 분석 중... (1~2분 소요)")
        try:
            from analyzer import get_recommendations
            result = get_recommendations()
            report = format_recommendations(result)
            await query.edit_message_text(report, parse_mode="HTML", reply_markup=get_back_keyboard())
        except Exception as e:
            await query.edit_message_text(f"추천 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data == "analyze_menu":
        await query.edit_message_text("📊 분석할 종목 선택:", reply_markup=get_analyze_keyboard())
    
    elif data.startswith("a_"):
        symbol = data[2:]
        await query.edit_message_text(f"🔍 {symbol} 분석 중...")
        try:
            from analyzer import analyze_single_stock
            result = analyze_single_stock(symbol)
            if result:
                text = format_analysis(result)
                await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_stock_detail_keyboard(symbol))
            else:
                await query.edit_message_text(f"'{symbol}' 데이터 없음", reply_markup=get_back_keyboard())
        except Exception as e:
            await query.edit_message_text(f"분석 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data == "news_menu":
        await query.edit_message_text("📰 뉴스 볼 종목 선택:", reply_markup=get_news_keyboard())
    
    elif data.startswith("n_"):
        symbol = data[2:]
        await query.edit_message_text(f"📰 {symbol} 뉴스 로딩...")
        try:
            from news_fetcher import get_company_news
            news = get_company_news(symbol)
            text = format_news(symbol, news)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🤖 AI 뉴스분석", callback_data=f"ainews_{symbol}")],
                [InlineKeyboardButton("🔙 메인", callback_data="main")]
            ]), disable_web_page_preview=True)
        except Exception as e:
            await query.edit_message_text(f"뉴스 로딩 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data == "market_news":
        await query.edit_message_text("🌍 시장 뉴스 로딩...")
        try:
            from news_fetcher import get_market_news
            news = get_market_news()
            text = format_market_news(news)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🤖 AI 시장분석", callback_data="ai_market")],
                [InlineKeyboardButton("🔙 메인", callback_data="main")]
            ]), disable_web_page_preview=True)
        except Exception as e:
            await query.edit_message_text(f"뉴스 로딩 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data.startswith("ai_"):
        target = data[3:]
        await query.edit_message_text(f"🤖 AI 분석 중... (10초 정도 걸려요)")
        try:
            if target == "market":
                from news_fetcher import get_market_news
                from ai_analyzer import get_market_sentiment
                from market_data import get_fear_greed_index
                news = get_market_news()
                fg = get_fear_greed_index()
                result = get_market_sentiment(news, fg)
                if "error" in result:
                    text = f"❌ {result['error']}"
                else:
                    text = f"🤖 <b>AI 시장 분석</b>\n━━━━━━━━━━━━━━━━━━\n\n{result['analysis']}"
            else:
                symbol = target
                from analyzer import analyze_single_stock
                from news_fetcher import get_company_news
                from ai_analyzer import analyze_stock_with_ai
                from market_data import get_comprehensive_stock_analysis
                stock_data = analyze_single_stock(symbol)
                news = get_company_news(symbol, days=3)
                market_data = get_comprehensive_stock_analysis(symbol)
                result = analyze_stock_with_ai(symbol, stock_data, news, market_data)
                if "error" in result:
                    text = f"❌ {result['error']}"
                else:
                    text = f"🤖 <b>{symbol} AI 분석</b>\n━━━━━━━━━━━━━━━━━━\n\n{result['analysis']}"
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_back_keyboard())
        except Exception as e:
            await query.edit_message_text(f"AI 분석 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data.startswith("ainews_"):
        symbol = data[7:]
        await query.edit_message_text(f"🤖 {symbol} 뉴스 AI 분석 중...")
        try:
            from news_fetcher import get_company_news
            from ai_analyzer import analyze_news_with_ai
            news = get_company_news(symbol, days=7)
            result = analyze_news_with_ai(symbol, news)
            if "error" in result:
                text = f"❌ {result['error']}"
            else:
                text = f"🤖 <b>{symbol} 뉴스 AI 분석</b>\n━━━━━━━━━━━━━━━━━━\n\n{result['analysis']}"
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_back_keyboard())
        except Exception as e:
            await query.edit_message_text(f"AI 분석 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data.startswith("insider_"):
        symbol = data[8:]
        await query.edit_message_text(f"👔 {symbol} 내부자 거래 로딩...")
        try:
            from news_fetcher import get_insider_transactions
            transactions = get_insider_transactions(symbol)
            text = format_insider(symbol, transactions)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_stock_detail_keyboard(symbol))
        except Exception as e:
            await query.edit_message_text(f"로딩 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data.startswith("target_"):
        symbol = data[7:]
        await query.edit_message_text(f"🎯 {symbol} 목표가 로딩...")
        try:
            from news_fetcher import get_price_target, get_recommendation_trends
            target = get_price_target(symbol)
            rec = get_recommendation_trends(symbol)
            text = format_target(symbol, target, rec)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_stock_detail_keyboard(symbol))
        except Exception as e:
            await query.edit_message_text(f"로딩 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data.startswith("earnings_"):
        symbol = data[9:]
        await query.edit_message_text(f"📊 {symbol} 실적 로딩...")
        try:
            from news_fetcher import get_earnings_calendar
            earnings = get_earnings_calendar()
            stock_earnings = [e for e in earnings if e["symbol"] == symbol]
            text = format_earnings(symbol, stock_earnings)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_stock_detail_keyboard(symbol))
        except Exception as e:
            await query.edit_message_text(f"로딩 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data == "calendar":
        await query.edit_message_text("📅 경제 일정 로딩...")
        try:
            from economic_calendar import get_upcoming_events, ECONOMIC_EVENTS
            from news_fetcher import get_earnings_calendar
            events = get_upcoming_events()
            earnings = get_earnings_calendar()
            text = format_calendar(events, earnings)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_back_keyboard())
        except Exception as e:
            await query.edit_message_text(f"로딩 실패: {e}", reply_markup=get_back_keyboard())
    
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
    
    # ===== 새로운 기능들 =====
    elif data == "fear_greed":
        await query.edit_message_text("😱 공포탐욕 지수 로딩...")
        try:
            from market_data import get_fear_greed_index
            fg = get_fear_greed_index()
            text = format_fear_greed(fg)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_back_keyboard())
        except Exception as e:
            await query.edit_message_text(f"로딩 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data == "sectors":
        await query.edit_message_text("🏭 섹터 성과 로딩...")
        try:
            from market_data import get_finviz_sector_performance
            sectors = get_finviz_sector_performance()
            text = format_sectors(sectors)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_back_keyboard())
        except Exception as e:
            await query.edit_message_text(f"로딩 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data == "comprehensive_menu":
        await query.edit_message_text("🔬 종합분석할 종목 선택:", reply_markup=get_comprehensive_keyboard())
    
    elif data.startswith("comp_"):
        symbol = data[5:]
        await query.edit_message_text(f"🔬 {symbol} 종합분석 중... (여러 사이트 조회)")
        try:
            from market_data import get_comprehensive_stock_analysis
            result = get_comprehensive_stock_analysis(symbol)
            text = format_comprehensive(result)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_stock_detail_keyboard(symbol))
        except Exception as e:
            await query.edit_message_text(f"분석 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data.startswith("fv_"):
        symbol = data[3:]
        await query.edit_message_text(f"📈 {symbol} Finviz 데이터 로딩...")
        try:
            from market_data import get_finviz_stock_data
            result = get_finviz_stock_data(symbol)
            text = format_finviz(result)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_stock_detail_keyboard(symbol))
        except Exception as e:
            await query.edit_message_text(f"로딩 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data == "market_sentiment":
        await query.edit_message_text("🌡️ 시장 심리 종합 분석 중...")
        try:
            from market_data import get_market_sentiment_summary
            result = get_market_sentiment_summary()
            text = format_market_sentiment(result)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_back_keyboard())
        except Exception as e:
            await query.edit_message_text(f"분석 실패: {e}", reply_markup=get_back_keyboard())
    
    elif data == "ai_recommend":
        await query.edit_message_text("🤖 AI 매수/매도 추천 분석 중...\n(나스닥 100 전체 분석, 2~3분 소요)")
        try:
            from groq_analyzer import run_full_analysis
            result = run_full_analysis()
            if "error" in result:
                text = f"❌ {result['error']}"
            else:
                text = format_ai_recommendation(result)
            # 텔레그램 메시지 길이 제한 (4096자)
            if len(text) > 4000:
                text = text[:3900] + "\n\n... (메시지가 너무 길어 일부 생략)"
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=get_back_keyboard())
        except Exception as e:
            await query.edit_message_text(f"AI 분석 실패: {e}", reply_markup=get_back_keyboard())


# 포맷팅 함수들
def format_analysis(r: dict) -> str:
    text = f"📊 <b>{r['symbol']}</b> ${r['price']}\n━━━━━━━━━━━━━━━━━━\n\n"
    text += f"⚠️ <b>위험도: {r['risk_score']}/100</b> (높을수록 위험)\n\n"
    text += f"<b>지표:</b>\n"
    text += f"• RSI: {r['rsi']} (30↓과매도 70↑과매수)\n"
    text += f"• 볼린저: {r['bb_position']}% (0=하단 100=상단)\n"
    text += f"• 52주: {r['position_52w']}% (0=저점 100=고점)\n"
    text += f"• 50일선: {r['ma50_gap']:+.1f}% (+위 -아래)\n"
    text += f"• 5일변화: {r['change_5d']:+.1f}%\n\n"
    if r['warnings']:
        text += "<b>⚠️ 경고:</b>\n" + "\n".join(r['warnings']) + "\n\n"
    if r['strategies_matched']:
        text += "<b>✅ 매칭:</b> " + ", ".join(r['strategies_matched'])
    else:
        text += "❌ 매칭 전략 없음"
    return text


def format_news(symbol: str, news: list) -> str:
    text = f"📰 <b>{symbol} 최근 뉴스</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    if not news:
        text += "최근 뉴스가 없습니다."
        return text
    for i, n in enumerate(news[:5], 1):
        text += f"<b>{i}. {n['headline'][:60]}...</b>\n"
        text += f"   📅 {n['datetime']} | {n['source']}\n"
        text += f"   <a href='{n['url']}'>기사 보기</a>\n\n"
    text += "\n💡 <b>뉴스 해석 팁:</b>\n• 실적 관련 → 예상치 대비 확인\n• 애널리스트 → 목표가 변경 확인\n• CEO 발언 → 가이던스 확인"
    return text


def format_market_news(news: list) -> str:
    text = "🌍 <b>시장 뉴스</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    if not news:
        text += "뉴스가 없습니다."
        return text
    for i, n in enumerate(news[:7], 1):
        text += f"<b>{i}. {n['headline'][:50]}...</b>\n"
        text += f"   📅 {n['datetime']}\n\n"
    return text


def format_insider(symbol: str, transactions: list) -> str:
    text = f"👔 <b>{symbol} 내부자 거래</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    if not transactions:
        text += "최근 내부자 거래가 없습니다."
        return text
    for t in transactions[:5]:
        emoji = "🟢" if t['transaction_type'] == "매수" else "🔴"
        text += f"{emoji} <b>{t['name']}</b>\n"
        text += f"   {t['transaction_type']} {t['share']:,}주 ({t['date']})\n\n"
    text += "\n💡 <b>해석:</b>\n• 내부자 매수 → 회사에 자신감 (호재)\n• 내부자 매도 → 주의 필요 (세금/개인사정일 수도)"
    return text


def format_target(symbol: str, target: dict, rec: dict) -> str:
    text = f"🎯 <b>{symbol} 애널리스트 의견</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    if target:
        text += f"<b>목표 주가:</b>\n"
        text += f"• 최고: ${target['target_high']}\n"
        text += f"• 평균: ${target['target_mean']}\n"
        text += f"• 최저: ${target['target_low']}\n\n"
    if rec:
        total = rec['strong_buy'] + rec['buy'] + rec['hold'] + rec['sell'] + rec['strong_sell']
        text += f"<b>투자의견 ({rec['period']}):</b>\n"
        text += f"🟢 적극매수: {rec['strong_buy']} | 매수: {rec['buy']}\n"
        text += f"🟡 보유: {rec['hold']}\n"
        text += f"🔴 매도: {rec['sell']} | 적극매도: {rec['strong_sell']}\n"
    text += "\n💡 <b>해석:</b>\n• 현재가 < 목표가 → 상승 여력\n• 매수 의견 많으면 긍정적\n• 단, 애널리스트도 틀릴 수 있음"
    return text


def format_earnings(symbol: str, earnings: list) -> str:
    text = f"📊 <b>{symbol} 실적 일정</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    if not earnings:
        text += "예정된 실적 발표가 없습니다.\n"
    else:
        for e in earnings[:3]:
            text += f"📅 {e['date']} ({e['hour']})\n"
            if e['eps_estimate']:
                text += f"   예상 EPS: ${e['eps_estimate']}\n"
    text += "\n💡 <b>실적 발표 팁:</b>\n• EPS가 예상치 상회 → 급등 가능\n• 가이던스(전망)가 더 중요할 때도\n• 발표 전후 변동성 큼 → 주의"
    return text


def format_calendar(events: list, earnings: list) -> str:
    text = "📅 <b>다가오는 일정</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    
    if events:
        text += "<b>🏛 경제 지표</b>\n"
        for e in events[:5]:
            text += f"• {e['date']} {e['name']} {e['impact']}\n"
            text += f"  └ {e['description'][:50]}...\n"
        text += "\n"
    
    if earnings:
        text += "<b>📊 실적 발표 (나스닥100)</b>\n"
        from config import NASDAQ_100
        nasdaq_earnings = [e for e in earnings if e['symbol'] in NASDAQ_100][:10]
        for e in nasdaq_earnings:
            text += f"• {e['date']} {e['symbol']} ({e['hour']})\n"
    
    if not events and not earnings:
        text += "예정된 일정이 없습니다."
    
    return text


def format_daily_report(scan_result: dict) -> str:
    from strategies import ALL_STRATEGIES
    market = scan_result["market"]
    strategy_results = scan_result["strategy_results"]
    fear_greed = scan_result.get("fear_greed", {})
    
    report = f"📊 <b>일일 리포트</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    
    # 공포탐욕 지수
    if fear_greed:
        report += f"😱 공포탐욕: {fear_greed.get('emoji', '')} {fear_greed.get('score', 'N/A')}/100 ({fear_greed.get('rating', '')})\n"
    
    report += f"🚦 {market['emoji']} {market['message']}\nQQQ: ${market['price']} (50일선: ${market['ma50']})\n\n"
    
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


def format_recommendations(result: dict) -> str:
    recs = result["recommendations"]
    
    report = "🌟 <b>추천 종목</b> (위험도 30 이하 + 전략 매칭)\n━━━━━━━━━━━━━━━━━━\n\n"
    
    if not recs:
        report += "😢 조건 맞는 종목 없음\n"
        return report
    
    for i, r in enumerate(recs, 1):
        strategies_short = ", ".join([s.split()[0] for s in r['strategies']])  # 이모지만
        report += f"{i}. <b>{r['symbol']}</b> ${r['price']} ⚠️{r['risk_score']} | {strategies_short}\n"
    
    report += f"\n━━━━━━━━━━━━━━━━━━\n"
    report += f"📌 {result['total_analyzed']}개 중 {len(recs)}개\n"
    report += "위험도 0~100 (낮을수록 좋음)"
    
    return report


# ===== 새로운 포맷팅 함수들 =====
def format_fear_greed(fg: dict) -> str:
    """공포탐욕 지수 포맷팅"""
    text = "😱 <b>CNN 공포탐욕 지수</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += f"{fg['emoji']} <b>현재: {fg['score']}/100</b>\n"
    text += f"📊 상태: {fg['rating']}\n\n"
    text += f"💡 <b>해석:</b>\n{fg['advice']}\n\n"
    text += "<b>지수 구간:</b>\n"
    text += "• 0-25: 극단적 공포 🔴 (매수 기회?)\n"
    text += "• 25-45: 공포 🟠\n"
    text += "• 45-55: 중립 🟡\n"
    text += "• 55-75: 탐욕 🟢\n"
    text += "• 75-100: 극단적 탐욕 🔵 (주의!)\n\n"
    text += f"⏰ {fg['timestamp']}"
    return text


def format_sectors(sectors: list) -> str:
    """섹터 성과 포맷팅"""
    text = "🏭 <b>섹터별 성과</b> (Finviz)\n━━━━━━━━━━━━━━━━━━\n\n"
    
    if not sectors:
        text += "데이터를 가져올 수 없습니다."
        return text
    
    # 성과순 정렬
    try:
        sectors_sorted = sorted(sectors, key=lambda x: float(x['change'].replace('%', '').replace('+', '')), reverse=True)
    except:
        sectors_sorted = sectors
    
    for s in sectors_sorted:
        text += f"{s['emoji']} <b>{s['name']}</b>: {s['change']}\n"
    
    text += "\n💡 <b>활용법:</b>\n"
    text += "• 강한 섹터의 대장주 매수\n"
    text += "• 약한 섹터는 피하거나 반등 노림"
    return text


def format_comprehensive(result: dict) -> str:
    """종합 분석 포맷팅"""
    symbol = result["symbol"]
    text = f"🔬 <b>{symbol} 종합분석</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    
    sources = result.get("sources", {})
    
    # Finviz 데이터
    fv = sources.get("finviz", {})
    if fv:
        text += "<b>📈 Finviz</b>\n"
        text += f"• 가격: ${fv.get('price', 'N/A')} ({fv.get('change', 'N/A')})\n"
        text += f"• P/E: {fv.get('pe', 'N/A')} | Forward P/E: {fv.get('forward_pe', 'N/A')}\n"
        text += f"• RSI: {fv.get('rsi', 'N/A')}\n"
        text += f"• 목표가: ${fv.get('target_price', 'N/A')}\n"
        text += f"• 섹터: {fv.get('sector', 'N/A')}\n\n"
    
    # TipRanks 데이터
    tr = sources.get("tipranks", {})
    if tr:
        text += "<b>🎯 TipRanks</b>\n"
        text += f"• 컨센서스: {tr.get('consensus', 'N/A')}\n"
        text += f"• 매수/보유/매도: {tr.get('buy', 0)}/{tr.get('hold', 0)}/{tr.get('sell', 0)}\n"
        text += f"• 목표가: ${tr.get('price_target_avg', 0):.2f}\n"
        text += f"• 애널리스트 수: {tr.get('num_analysts', 0)}명\n\n"
    
    # Seeking Alpha 데이터
    sa = sources.get("seeking_alpha", {})
    if sa:
        text += "<b>📊 Seeking Alpha</b>\n"
        text += f"• 퀀트 레이팅: {sa.get('rating_text', 'N/A')}\n\n"
    
    if not sources:
        text += "데이터를 가져올 수 없습니다.\n"
    
    text += f"⏰ {result['timestamp']}"
    return text


def format_finviz(data: dict) -> str:
    """Finviz 상세 데이터 포맷팅"""
    if not data:
        return "데이터를 가져올 수 없습니다."
    
    symbol = data.get("symbol", "")
    text = f"📈 <b>{symbol} Finviz 데이터</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    
    text += f"<b>💰 가격</b>\n"
    text += f"• 현재가: ${data.get('price', 'N/A')} ({data.get('change', 'N/A')})\n"
    text += f"• 52주 고가: {data.get('52w_high', 'N/A')}\n"
    text += f"• 52주 저가: {data.get('52w_low', 'N/A')}\n"
    text += f"• 목표가: ${data.get('target_price', 'N/A')}\n\n"
    
    text += f"<b>📊 밸류에이션</b>\n"
    text += f"• P/E: {data.get('pe', 'N/A')}\n"
    text += f"• Forward P/E: {data.get('forward_pe', 'N/A')}\n"
    text += f"• PEG: {data.get('peg', 'N/A')}\n"
    text += f"• P/S: {data.get('ps', 'N/A')}\n"
    text += f"• P/B: {data.get('pb', 'N/A')}\n\n"
    
    text += f"<b>💵 수익성</b>\n"
    text += f"• EPS: {data.get('eps', 'N/A')}\n"
    text += f"• EPS 예상(내년): {data.get('eps_next_y', 'N/A')}\n"
    text += f"• ROE: {data.get('roe', 'N/A')}\n"
    text += f"• ROA: {data.get('roa', 'N/A')}\n"
    text += f"• 배당률: {data.get('dividend', 'N/A')}\n\n"
    
    text += f"<b>📉 기술적</b>\n"
    text += f"• RSI(14): {data.get('rsi', 'N/A')}\n"
    text += f"• 상대거래량: {data.get('rel_volume', 'N/A')}\n"
    text += f"• 공매도비율: {data.get('short_float', 'N/A')}\n\n"
    
    text += f"<b>🏢 기업정보</b>\n"
    text += f"• 섹터: {data.get('sector', 'N/A')}\n"
    text += f"• 산업: {data.get('industry', 'N/A')}"
    
    return text


def format_market_sentiment(result: dict) -> str:
    """시장 심리 종합 포맷팅"""
    text = "🌡️ <b>시장 심리 종합</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    
    # Fear & Greed
    fg = result.get("fear_greed", {})
    if fg:
        text += f"<b>😱 공포탐욕 지수</b>\n"
        text += f"{fg['emoji']} {fg['score']}/100 - {fg['rating']}\n"
        text += f"💡 {fg['advice']}\n\n"
    
    # 시장 개요
    overview = result.get("market_overview", {})
    indices = overview.get("indices", {})
    if indices:
        text += "<b>📊 주요 지수</b>\n"
        for name, change in indices.items():
            emoji = "🟢" if "+" in change else "🔴" if "-" in change else "⚪"
            text += f"{emoji} {name}: {change}\n"
        text += "\n"
    
    # 섹터 성과 (상위 3개, 하위 3개)
    sectors = result.get("sectors", [])
    if sectors:
        try:
            sectors_sorted = sorted(sectors, key=lambda x: float(x['change'].replace('%', '').replace('+', '')), reverse=True)
            text += "<b>🏭 섹터 (상위/하위)</b>\n"
            for s in sectors_sorted[:3]:
                text += f"🟢 {s['name']}: {s['change']}\n"
            text += "...\n"
            for s in sectors_sorted[-3:]:
                text += f"🔴 {s['name']}: {s['change']}\n"
        except:
            pass
    
    text += f"\n⏰ {result['timestamp']}"
    return text


def format_ai_recommendation(result: dict) -> str:
    """AI 매수/매도 추천 포맷팅 (재무 데이터 포함)"""
    text = "🤖 <b>AI 매수/매도 추천</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    
    # 시장 심리
    fg = result.get("fear_greed", {})
    if fg:
        text += f"😱 공포탐욕: {fg.get('emoji', '')} {fg.get('score', 'N/A')}/100 ({fg.get('rating', '')})\n"
        text += f"💡 {fg.get('advice', '')}\n\n"
        text += f"📊 분석 종목: {result.get('total_stocks', 0)}개\n"
        text += f"🧠 모델: {result.get('model', 'llama4-maverick')}\n\n"
    
    # 점수 체계 설명
    text += "<b>📐 점수 체계</b>\n"
    text += "• 종합(T) = 팩터 60% + 재무 40%\n"
    text += "• 팩터(F): 수익성/모멘텀/가치/퀄리티/변동성\n"
    text += "• 재무(FIN): ROE/P·E/성장률/부채/배당\n"
    text += "• 등급: A(70+) B(60+) C(50+) D(40+) F\n\n"
    
    # AI 분석 결과
    analysis = result.get("analysis", "")
    if analysis:
        # HTML 태그 변환 (마크다운 -> HTML)
        analysis = analysis.replace("**", "")
        analysis = analysis.replace("##", "📌")
        text += analysis
    
    # 매수 추천 TOP 5 재무 요약 추가
    top_stocks = result.get("top_buy_stocks", [])
    if top_stocks:
        text += "\n\n<b>📊 매수 TOP 5 재무 요약</b>\n"
        for s in top_stocks[:5]:
            text += f"• {s['symbol']}: ROE {s.get('roe', 'N/A')} | P/E {s.get('pe', 'N/A')} | 성장 {s.get('growth', 'N/A')}\n"
    
    return text


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
        await update.message.reply_text(format_analysis(result), parse_mode="HTML", reply_markup=get_stock_detail_keyboard(symbol))
    else:
        await update.message.reply_text(f"'{symbol}' 데이터 없음")


async def news_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("종목 선택:", reply_markup=get_news_keyboard())
        return
    symbol = context.args[0].upper()
    from news_fetcher import get_company_news
    news = get_company_news(symbol)
    await update.message.reply_text(format_news(symbol, news), parse_mode="HTML", reply_markup=get_back_keyboard(), disable_web_page_preview=True)


def run_bot():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("scan", scan_command))
    app.add_handler(CommandHandler("analyze", analyze_command))
    app.add_handler(CommandHandler("news", news_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    print("봇 실행 중... /start 로 시작")
    app.run_polling()


if __name__ == "__main__":
    run_bot()
