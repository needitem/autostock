"""
텔레그램 콜백 핸들러 모듈
"""
import os
import sys

# src 폴더를 path에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from telegram import Update
from telegram.ext import ContextTypes

from bot import keyboards as kb
from bot import formatters as fmt
from core.stock_data import get_stock_data, get_stock_info, get_market_condition, get_fear_greed_index
from core.indicators import calculate_indicators, get_full_analysis
from core.scoring import calculate_score
from core.signals import check_entry_signal, scan_stocks
from trading.kis_api import kis
from trading.watchlist import watchlist
from trading.portfolio import portfolio
from trading.monitor import monitor
from ai.analyzer import ai


# ===== 헬퍼 함수 =====
async def send_long_message(query, text: str, max_len: int = 4000):
    """긴 메시지 분할 전송"""
    if len(text) <= max_len:
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
        return
    
    # 첫 메시지는 edit, 나머지는 reply
    parts = []
    while text:
        if len(text) <= max_len:
            parts.append(text)
            break
        # 줄바꿈 기준으로 자르기
        cut_pos = text.rfind('\n', 0, max_len)
        if cut_pos == -1:
            cut_pos = max_len
        parts.append(text[:cut_pos])
        text = text[cut_pos:].lstrip('\n')
    
    # 첫 파트는 edit
    await query.edit_message_text(parts[0], parse_mode="HTML")
    
    # 나머지는 새 메시지로 전송
    for i, part in enumerate(parts[1:], 2):
        if i == len(parts):  # 마지막 메시지에만 키보드 추가
            await query.message.reply_text(part, parse_mode="HTML", reply_markup=kb.back())
        else:
            await query.message.reply_text(part, parse_mode="HTML")


# ===== 메인 메뉴 핸들러 =====
async def handle_main(query):
    await query.edit_message_text("메인 메뉴 👇", reply_markup=kb.main_menu())


async def handle_recommend(query):
    """추천 종목"""
    await query.edit_message_text("🌟 추천 종목 분석 중... (5~10분 소요)")
    try:
        from config import ALL_US_STOCKS
        result = scan_stocks(ALL_US_STOCKS)  # 전체 스캔
        
        # 추천 품질 필터: 기술 신호 또는 높은 점수 + 과도한 위험 제외
        filtered = [
            s for s in result["results"]
            if s.get("strategy_count", 0) > 0 or s.get("score", {}).get("total_score", 0) >= 65
        ]
        filtered = [
            s for s in filtered
            if s.get("score", {}).get("risk", {}).get("score", 100) < 70
        ]
        filtered = [
            s for s in filtered
            if s.get("score", {}).get("confidence", {}).get("score", 0) >= 55
        ]

        # 품질 점수 기준 정렬, 상위 20개
        stocks = sorted(filtered, key=lambda x: -x.get("quality_score", x["score"]["total_score"]))[:20]
        text = fmt.format_recommendations(stocks, result["total"])
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as e:
        await query.edit_message_text(f"추천 실패: {e}", reply_markup=kb.back())


async def handle_scan(query):
    """전체 스캔"""
    await query.edit_message_text("🔍 전체 스캔 중... (5~10분 소요)")
    try:
        from config import ALL_US_STOCKS
        result = scan_stocks(ALL_US_STOCKS)  # 전체 스캔

        text = f"🔍 <b>스캔 결과</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        text += f"분석: {result['total']}개\n\n"
        
        for r in result["results"][:10]:
            if r.get("strategies"):
                strats = ", ".join([s["emoji"] for s in r["strategies"]])
                text += f"• {r['symbol']} ${r['price']} | {strats}\n"
        
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as e:
        await query.edit_message_text(f"스캔 실패: {e}", reply_markup=kb.back())


async def handle_ai_recommend(query):
    """AI 추천 (전체 종목 + 모든 뉴스 통합)"""
    await query.edit_message_text("🤖 AI 분석 중... (10~15분 소요)\n\n1️⃣ 전체 종목 스캔...")
    try:
        from config import ALL_US_STOCKS, STOCK_CATEGORIES
        from core.news import get_bulk_news, get_market_news
        
        # 1. 전체 종목 스캔
        result = scan_stocks(ALL_US_STOCKS)
        stocks = result["results"]
        
        await query.edit_message_text(f"🤖 AI 분석 중...\n\n1️⃣ 스캔 완료 ({len(stocks)}개)\n2️⃣ 시장 데이터 수집...")
        
        # 2. 시장 데이터
        market_data = {
            "fear_greed": get_fear_greed_index(),
            "market_condition": get_market_condition(),
            "market_news": get_market_news(),
        }
        
        await query.edit_message_text(f"🤖 AI 분석 중...\n\n1️⃣ 스캔 완료 ({len(stocks)}개)\n2️⃣ 시장 데이터 완료\n3️⃣ 주요 종목 뉴스 수집...")
        
        # 3. 상위 100개 종목 뉴스 수집
        top_stocks = sorted(stocks, key=lambda x: -x.get("score", {}).get("total_score", 0))[:100]
        top_symbols = [s['symbol'] for s in top_stocks]
        news_data = get_bulk_news(top_symbols, days=3)
        
        await query.edit_message_text(f"🤖 AI 분석 중...\n\n1️⃣ 스캔 완료 ({len(stocks)}개)\n2️⃣ 시장 데이터 완료\n3️⃣ 뉴스 수집 완료 ({len(news_data)}개)\n4️⃣ AI 종합 분석 중...")
        
        # 4. AI 분석
        ai_result = ai.analyze_full_market(stocks, news_data, market_data, STOCK_CATEGORIES)
        
        if "error" in ai_result:
            text = f"❌ {ai_result['error']}"
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
        else:
            stats = ai_result.get("stats", {})
            header = f"🤖 <b>AI 종합 추천</b> ({ai_result['total']}개 분석)\n"
            header += f"📊 평균RSI: {stats.get('avg_rsi', 0):.0f} | 평균점수: {stats.get('avg_score', 0):.0f}\n"
            header += f"📉 과매도: {stats.get('oversold', 0)}개 | 과매수: {stats.get('overbought', 0)}개\n"
            header += "━━━━━━━━━━━━━━━━━━\n\n"
            text = header + ai_result['analysis']
            await send_long_message(query, text)
    except Exception as e:
        await query.edit_message_text(f"AI 분석 실패: {e}", reply_markup=kb.back())


async def handle_analyze_menu(query):
    await query.edit_message_text("📊 분석할 종목 선택:", reply_markup=kb.analyze_menu())


async def handle_analyze_input(query):
    """직접 입력 모드"""
    text = "✏️ <b>종목 직접 입력</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "분석할 종목 심볼을 입력하세요.\n\n"
    text += "예시: <code>AAPL</code>, <code>TSLA</code>, <code>NVDA</code>\n\n"
    text += "💡 그냥 심볼만 입력하면 됩니다!"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back("analyze_menu", "종목분석"))


async def handle_fear_greed(query):
    """공포탐욕 지수"""
    await query.edit_message_text("😱 공포탐욕 지수 로딩...")
    try:
        fg = get_fear_greed_index()
        text = fmt.format_fear_greed(fg)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as e:
        await query.edit_message_text(f"로딩 실패: {e}", reply_markup=kb.back())


async def handle_category_menu(query):
    await query.edit_message_text("📂 카테고리별 추천 - 선택하세요:", reply_markup=kb.category_menu())


# ===== 자동매매 핸들러 =====
async def handle_trading_menu(query):
    text = "💰 <b>자동매매</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "한국투자증권 API를 통한 해외주식 자동매매\n\n"
    text += "⚠️ 자동매매 활성화 시 실제 주문이 실행됩니다!"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())


async def handle_auto_settings(query):
    """자동매매 설정"""
    auto_buy = watchlist.is_auto_buy()
    auto_sell = watchlist._load()["settings"].get("auto_sell", False)
    
    text = fmt.header("자동매매 설정", "⚙️")
    text += "\n<b>🤖 자동매수</b>\n"
    text += f"상태: {'✅ 활성화' if auto_buy else '❌ 비활성화'}\n"
    text += "관심종목 저점 신호 시 자동 매수\n"
    text += "\n<b>🛑 자동손절</b>\n"
    text += f"상태: {'✅ 활성화' if auto_sell else '❌ 비활성화'}\n"
    text += "보유종목 -7% 이하 시 자동 매도\n"
    text += "\n💡 스케줄: 매일 21:00 자동 실행"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.auto_settings_menu(auto_buy, auto_sell))


async def handle_toggle_auto_buy(query):
    """자동매수 토글"""
    current = watchlist.is_auto_buy()
    watchlist.set_auto_buy(not current)
    new_status = "활성화" if not current else "비활성화"
    await query.answer(f"자동매수 {new_status}됨")
    await handle_auto_settings(query)


async def handle_toggle_auto_sell(query):
    """자동손절 토글"""
    data = watchlist._load()
    current = data["settings"].get("auto_sell", False)
    data["settings"]["auto_sell"] = not current
    watchlist._save()
    new_status = "활성화" if not current else "비활성화"
    await query.answer(f"자동손절 {new_status}됨")
    await handle_auto_settings(query)


async def handle_trade_history(query):
    """매매 기록"""
    text = fmt.header("매매 기록", "📜")
    text += "\n최근 자동매매 기록이 여기에 표시됩니다.\n"
    text += "(추후 구현 예정)"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back("auto_settings", "자동매매"))


async def handle_balance(query):
    """잔고 조회"""
    await query.edit_message_text("📊 잔고 조회 중...")
    try:
        result = portfolio.get_status()
        if "error" in result:
            await query.edit_message_text(f"❌ {result['error']}", reply_markup=kb.trading_menu())
            return
        text = fmt.format_balance(result)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())
    except Exception as e:
        await query.edit_message_text(f"잔고 조회 실패: {e}", reply_markup=kb.trading_menu())


async def handle_orders(query):
    """미체결 주문"""
    await query.edit_message_text("📋 미체결 주문 조회 중...")
    try:
        result = kis.get_orders()
        if "error" in result:
            await query.edit_message_text(f"❌ {result['error']}", reply_markup=kb.trading_menu())
            return
        text = fmt.format_orders(result.get("orders", []))
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())
    except Exception as e:
        await query.edit_message_text(f"주문 조회 실패: {e}", reply_markup=kb.trading_menu())


async def handle_api_status(query):
    """API 상태"""
    try:
        status = kis.check_status()
        text = fmt.format_api_status(status)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())
    except Exception as e:
        await query.edit_message_text(f"상태 확인 실패: {e}", reply_markup=kb.trading_menu())


# ===== 관심종목 핸들러 =====
async def handle_watchlist_main(query):
    """관심종목 메인"""
    data = watchlist.get_all()
    stock_count = len(data.get("stocks", {}))
    settings = data.get("settings", {})
    monitor_on = settings.get("monitor_enabled", True)
    interval = settings.get("monitor_interval", 30)
    
    text = "👀 <b>관심종목</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += f"📌 등록 종목: {stock_count}개\n"
    text += f"🔔 모니터링: {'ON' if monitor_on else 'OFF'}\n"
    text += f"⏱️ 체크 간격: {interval}분\n\n"
    text += "<b>알림 조건:</b>\n"
    text += "• 가격 ±3% 변동\n"
    text += "• RSI 과매도(30↓) / 과매수(70↑)\n"
    text += "• 지지선/저항선 돌파\n"
    text += "• 골든크로스/데드크로스\n"
    text += "• 거래량 급증 (2배↑)\n"
    text += "• 캔들 패턴 감지"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_main_menu())


async def handle_watchlist(query):
    """관심종목 (자동매매 메뉴에서)"""
    await handle_watchlist_main(query)


async def handle_watchlist_status(query):
    """관심종목 현황"""
    await query.edit_message_text("📋 관심종목 현황 조회 중...")
    try:
        stocks = watchlist.get_status()
        if not stocks:
            text = "👀 <b>관심종목 현황</b>\n━━━━━━━━━━━━━━━━━━\n\n"
            text += "등록된 종목이 없습니다.\n\n➕ 종목추가 버튼으로 추가하세요!"
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_main_menu())
            return
        
        text = "👀 <b>관심종목 현황</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        
        for s in stocks:
            # 상태 이모지
            if s["is_signal"]:
                status_emoji = "🟢"  # 매수 신호
            elif s.get("rsi", 50) > 70:
                status_emoji = "🔴"  # 과매수
            else:
                status_emoji = "⚪"  # 중립
            
            text += f"{status_emoji} <b>{s['symbol']}</b>\n"
            text += f"   현재: ${s['price']:.2f} ({s['change_pct']:+.1f}%)\n"
            text += f"   RSI: {s['rsi']:.0f} | BB: {s['bb_position']:.0f}%\n"
            
            if s["is_signal"]:
                text += f"   🚨 <b>저점 신호! ({s['strength']})</b>\n"
            text += "\n"
        
        text += f"━━━━━━━━━━━━━━━━━━\n"
        text += f"🟢 매수신호 | ⚪ 중립 | 🔴 과매수"
        
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_main_menu())
    except Exception as e:
        await query.edit_message_text(f"조회 실패: {e}", reply_markup=kb.watchlist_main_menu())


async def handle_watchlist_check_now(query):
    """지금 바로 체크"""
    await query.edit_message_text("🔍 관심종목 체크 중...")
    try:
        results = monitor.check_all_watchlist()
        
        if not results:
            # 알림 없으면 현황 표시
            stocks = watchlist.get_status()
            if not stocks:
                text = "👀 등록된 관심종목이 없습니다."
            else:
                text = "✅ <b>체크 완료</b>\n━━━━━━━━━━━━━━━━━━\n\n"
                text += "특별한 알림 조건 없음\n\n"
                for s in stocks:
                    summary = monitor.get_summary(s['symbol'])
                    text += summary + "\n"
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_main_menu())
            return
        
        # 알림 있으면 표시
        text = monitor.format_alert_message(results)
        await send_long_message(query, text)
    except Exception as e:
        await query.edit_message_text(f"체크 실패: {e}", reply_markup=kb.watchlist_main_menu())


async def handle_watchlist_add_menu(query):
    """종목 추가 메뉴"""
    text = "➕ <b>관심종목 추가</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "추가할 종목을 선택하거나\n심볼을 직접 입력하세요.\n\n"
    text += "예: <code>AAPL</code>, <code>TSLA</code>"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_add())


async def handle_watchlist_remove_menu(query):
    """종목 삭제 메뉴"""
    data = watchlist.get_all()
    stocks = list(data.get("stocks", {}).keys())
    
    if not stocks:
        text = "❌ 삭제할 종목이 없습니다."
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_main_menu())
        return
    
    text = "➖ <b>관심종목 삭제</b>\n━━━━━━━━━━━━━━━━━━\n\n삭제할 종목을 선택하세요."
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_remove_menu(stocks))


async def handle_watchlist_remove(query, data):
    """종목 삭제 실행"""
    symbol = data[9:]  # watchdel_ 제거
    result = watchlist.remove(symbol)
    
    if result.get("success"):
        text = f"✅ {symbol} 삭제 완료"
    else:
        text = f"❌ 삭제 실패: {result.get('error')}"
    
    await query.answer(text)
    await handle_watchlist_remove_menu(query)


async def handle_watchlist_alert_settings(query):
    """알림 설정"""
    data = watchlist.get_all()
    settings = data.get("settings", {})
    
    text = "⚙️ <b>알림 설정</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "<b>모니터링</b>\n"
    text += "ON: 30분마다 자동 체크 후 알림\n"
    text += "OFF: 수동 체크만 가능\n\n"
    text += "<b>체크 간격</b>\n"
    text += "관심종목을 체크하는 주기\n"
    text += "(미국장 개장 시간에만 작동)"
    
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_alert_settings(settings))


async def handle_toggle_monitor(query):
    """모니터링 토글"""
    data = watchlist._load()
    current = data["settings"].get("monitor_enabled", True)
    data["settings"]["monitor_enabled"] = not current
    watchlist._save()
    
    status = "활성화" if not current else "비활성화"
    await query.answer(f"모니터링 {status}됨")
    await handle_watchlist_alert_settings(query)


async def handle_change_interval(query):
    """체크 간격 변경 (30 → 60 → 15 → 30)"""
    data = watchlist._load()
    current = data["settings"].get("monitor_interval", 30)
    
    intervals = [15, 30, 60]
    idx = intervals.index(current) if current in intervals else 0
    new_interval = intervals[(idx + 1) % len(intervals)]
    
    data["settings"]["monitor_interval"] = new_interval
    watchlist._save()
    
    await query.answer(f"체크 간격: {new_interval}분")
    await handle_watchlist_alert_settings(query)


# ===== Prefix 핸들러 =====
async def handle_analyze_stock(query, data):
    symbol = data[2:]
    await query.edit_message_text(f"🔍 {symbol} 분석 중...")
    try:
        analysis = get_full_analysis(symbol)
        if analysis is None:
            await query.edit_message_text(f"'{symbol}' 데이터 없음", reply_markup=kb.back())
            return
        score = calculate_score(analysis)
        analysis["score"] = score
        text = fmt.format_analysis(analysis)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.stock_detail(symbol))
    except Exception as e:
        await query.edit_message_text(f"분석 실패: {e}", reply_markup=kb.back())


async def handle_ai_stock(query, data):
    symbol = data[3:]
    await query.edit_message_text(f"🤖 {symbol} AI 분석 중... (뉴스 포함)")
    try:
        from core.news import get_company_news
        
        analysis = get_full_analysis(symbol)
        if analysis is None:
            await query.edit_message_text(f"'{symbol}' 데이터 없음", reply_markup=kb.back())
            return
        
        # 뉴스 수집
        news = get_company_news(symbol, days=3)
        analysis["news"] = news
        
        score = calculate_score(analysis)
        analysis["total_score"] = score["total_score"]
        result = ai.analyze_stock(symbol, analysis)
        if "error" in result:
            text = f"❌ {result['error']}"
        else:
            news_count = len(news)
            text = f"🤖 <b>{symbol} AI 분석</b> (뉴스 {news_count}건 반영)\n━━━━━━━━━━━━━━━━━━\n\n{result['analysis']}"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.stock_detail(symbol))
    except Exception as e:
        await query.edit_message_text(f"AI 분석 실패: {e}", reply_markup=kb.back())


async def handle_watchlist_add(query, data):
    symbol = data[9:]
    await query.edit_message_text(f"➕ {symbol} 추가 중...")
    try:
        result = watchlist.add(symbol)
        if result.get("success"):
            text = f"✅ <b>관심종목 추가 완료</b>\n\n종목: {symbol}\n현재가: ${result['price']}\n목표가: ${result['target_price']}"
        else:
            text = f"❌ 추가 실패: {result.get('error')}"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_menu())
    except Exception as e:
        await query.edit_message_text(f"추가 실패: {e}", reply_markup=kb.watchlist_menu())


async def handle_category(query, data):
    category = data[4:]
    await query.edit_message_text(f"📊 {category} 분석 중... (1~2분 소요)")
    try:
        from config import STOCK_CATEGORIES
        cat_info = STOCK_CATEGORIES.get(category)
        if not cat_info:
            await query.edit_message_text(f"❌ 카테고리 없음: {category}", reply_markup=kb.category_menu())
            return
        result = scan_stocks(cat_info["stocks"][:20])
        stocks = sorted(result["results"], key=lambda x: -x["score"]["total_score"])[:10]
        text = f"{cat_info['emoji']} <b>{category} 추천</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        text += f"📌 {cat_info['description']}\n📈 대표 ETF: {cat_info['etf']}\n\n"
        if stocks:
            text += f"<b>🌟 추천 종목 ({len(stocks)}개)</b>\n"
            for i, s in enumerate(stocks, 1):
                score = s.get("score", {})
                text += f"{i}. <b>{s['symbol']}</b> ${s.get('price', 0)} | 점수: {score.get('total_score', 0):.0f}\n"
        else:
            text += "😢 추천 종목 없음\n"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.category_menu())
    except Exception as e:
        await query.edit_message_text(f"분석 실패: {e}", reply_markup=kb.category_menu())


async def handle_category_all(query):
    await query.edit_message_text("📊 전체 카테고리 분석 중... (3~5분 소요)")
    try:
        from config import STOCK_CATEGORIES
        text = "📂 <b>전체 카테고리 추천 요약</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        for cat_name, cat_info in STOCK_CATEGORIES.items():
            result = scan_stocks(cat_info["stocks"][:10])
            top = sorted(result["results"], key=lambda x: -x["score"]["total_score"])[:2]
            text += f"{cat_info['emoji']} <b>{cat_name}</b>\n"
            if top:
                for s in top:
                    text += f"  • {s['symbol']} ${s.get('price', 0)} (점수: {s['score']['total_score']:.0f})\n"
            else:
                text += "  • 추천 없음\n"
            text += "\n"
        await send_long_message_category(query, text)
    except Exception as e:
        await query.edit_message_text(f"분석 실패: {e}", reply_markup=kb.category_menu())


async def send_long_message_category(query, text: str, max_len: int = 4000):
    """카테고리용 긴 메시지 분할 전송"""
    if len(text) <= max_len:
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.category_menu())
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
    
    await query.edit_message_text(parts[0], parse_mode="HTML")
    for i, part in enumerate(parts[1:], 2):
        if i == len(parts):
            await query.message.reply_text(part, parse_mode="HTML", reply_markup=kb.category_menu())
        else:
            await query.message.reply_text(part, parse_mode="HTML")


# ===== 핸들러 매핑 =====
EXACT_HANDLERS = {
    "main": handle_main,
    "recommend": handle_recommend,
    "scan": handle_scan,
    "ai_recommend": handle_ai_recommend,
    "analyze_menu": handle_analyze_menu,
    "analyze_input": handle_analyze_input,
    "fear_greed": handle_fear_greed,
    "category_menu": handle_category_menu,
    "cat_all": handle_category_all,
    "trading_menu": handle_trading_menu,
    "auto_settings": handle_auto_settings,
    "toggle_auto_buy": handle_toggle_auto_buy,
    "toggle_auto_sell": handle_toggle_auto_sell,
    "trade_history": handle_trade_history,
    "balance": handle_balance,
    "orders": handle_orders,
    "api_status": handle_api_status,
    "watchlist": handle_watchlist,
    "watchlist_main": handle_watchlist_main,
    "watchlist_status": handle_watchlist_status,
    "watchlist_add": handle_watchlist_add_menu,
    "watchlist_check_now": handle_watchlist_check_now,
    "watchlist_remove_menu": handle_watchlist_remove_menu,
    "watchlist_alert_settings": handle_watchlist_alert_settings,
    "toggle_monitor": handle_toggle_monitor,
    "change_interval": handle_change_interval,
}

PREFIX_HANDLERS = [
    ("watchdel_", handle_watchlist_remove),
    ("watchadd_", handle_watchlist_add),
    ("cat_", handle_category),
    ("ai_", handle_ai_stock),
    ("a_", handle_analyze_stock),
]


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """콜백 쿼리 핸들러"""
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
