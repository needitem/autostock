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
from trading.monitor import monitor
from trading.portfolio import portfolio
from trading.watchlist import watchlist


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

    text = "❌ <b>트레이딩 비활성화</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "KIS API 키가 설정되지 않았습니다.\n\n"
    text += "필수 환경변수: <code>KIS_APP_KEY</code>, <code>KIS_APP_SECRET</code>, <code>KIS_ACCOUNT_NO</code>"
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
    data = watchlist.get_all()
    settings = data.get("settings", {})

    stock_count = len(data.get("stocks", {}))
    monitor_on = settings.get("monitor_enabled", True)
    auto_buy = settings.get("auto_buy", False)
    auto_sell = settings.get("auto_sell", False)
    style = _style_from_query(query)

    text = "🏠 <b>AutoStock</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "<b>처음이라면 이렇게 시작하세요</b>\n"
    text += "1) 🚀 오늘 뭐 살까\n"
    text += "2) 📊 종목 쉽게 보기\n"
    text += "3) 👀 관심종목 등록\n\n"
    text += f"관심종목: <b>{stock_count}</b>개 | 모니터링 {'ON' if monitor_on else 'OFF'}\n"
    text += f"자동매수/손절: {'ON' if auto_buy else 'OFF'} / {'ON' if auto_sell else 'OFF'}\n"
    text += f"화면 모드: <b>{style_label(style)}</b>\n"
    text += f"트레이딩: {'사용 가능' if kb.trading_enabled() else '비활성'}\n\n"
    text += "티커만 보내도 바로 분석됩니다. (예: <code>AAPL</code>)"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.main_menu())


async def handle_help(query) -> None:
    text = "📌 <b>초보 가이드</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "1) <b>오늘 뭐 살까</b>: 지금 매수/대기만 빠르게 확인\n"
    text += "2) <b>종목 쉽게 보기</b>: 한 종목을 실행안(매수/손절/목표)으로 확인\n"
    text += "3) <b>관심종목</b>: 알림 켜두고 신호 올 때만 체크\n\n"
    text += "용어가 어렵다면 <b>초보(권장)</b> 모드를 사용하세요."
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())


async def handle_display_settings(query) -> None:
    style = _style_from_query(query)
    text = "⚙️ <b>표시 설정</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += f"현재 스타일: <b>{style_label(style)}</b>\n\n"
    text += "• 초보(권장): 지금 할 일 중심\n"
    text += "• 표준: 기본 지표 + 매매 정보\n"
    text += "• 상세: 보조 지표/경고까지 전체 표시"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.display_settings_menu(style))


async def handle_set_style(query, data: str) -> None:
    chat_id = _chat_id_from_query(query)
    requested = data[6:]
    style = normalize_style(requested)

    if not chat_id:
        await query.answer("채팅 정보를 찾을 수 없습니다.")
        return

    saved = set_chat_style(chat_id, style)
    await query.answer(f"표시 스타일: {style_label(saved)}")
    await handle_display_settings(query)


async def handle_recommend(query) -> None:
    await query.edit_message_text("📈 추천 종목 생성 중... (약 5~10분)")

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
            text += "\n\n<i>최근 스캔 캐시 사용</i>"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"추천 생성 실패: {exc}", reply_markup=kb.back())


async def handle_scan(query) -> None:
    await query.edit_message_text("🔎 전체 스캔 중... (약 5~10분)")

    try:
        from config import load_all_us_stocks

        style = _style_from_query(query)
        result, used_cache = get_scan_result(load_all_us_stocks(), max_age_sec=240)
        text = fmt.format_scan_brief(result["results"], result["total"], top_n=10, style=style)
        if used_cache:
            text += "\n\n<i>최근 스캔 캐시 사용</i>"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"스캔 실패: {exc}", reply_markup=kb.back())


async def handle_ai_recommend(query) -> None:
    await query.edit_message_text("🤖 AI 리포트 생성 중...\n1/4 스캔")

    try:
        from config import load_all_us_stocks, load_stock_categories
        from core.news import get_bulk_news, get_market_news

        style = _style_from_query(query)
        universe = load_all_us_stocks()
        categories = load_stock_categories()

        result, used_cache = get_scan_result(universe, max_age_sec=300)
        stocks = result["results"]

        cache_note = " (캐시)" if used_cache else ""
        await query.edit_message_text(
            f"🤖 AI 리포트 생성 중...\n1/4 스캔 완료{cache_note} ({len(stocks)}개)\n2/4 시장 데이터"
        )

        market_data = {
            "fear_greed": get_fear_greed_index(),
            "market_condition": get_market_condition(),
            "market_news": get_market_news(),
        }

        await query.edit_message_text(
            "🤖 AI 리포트 생성 중...\n1/4 스캔 완료\n2/4 시장 데이터 완료\n3/4 뉴스 수집"
        )

        news_symbols = ai.select_news_symbols(stocks, limit=60)
        news_data = get_bulk_news(news_symbols, days=3)

        await query.edit_message_text(
            "🤖 AI 리포트 생성 중...\n"
            "1/4 스캔 완료\n"
            "2/4 시장 데이터 완료\n"
            f"3/4 뉴스 수집 완료 ({len(news_data)}/{len(news_symbols)})\n"
            "4/4 AI 분석"
        )

        ai_result = ai.analyze_full_market(stocks, news_data, market_data, categories)
        if "error" in ai_result:
            await query.edit_message_text(f"❌ {ai_result['error']}", reply_markup=kb.back())
            return

        stats = ai_result.get("stats", {})
        header = f"🤖 <b>AI 리포트</b> ({ai_result.get('total', 0)}개 분석)\n"
        header += f"평균 RSI {stats.get('avg_rsi', 0):.0f} │ 평균 점수 {stats.get('avg_score', 0):.0f}\n"
        if "avg_rs63_vs_qqq" in stats:
            header += f"평균 RS63(vs QQQ) {stats.get('avg_rs63_vs_qqq', 0):+.1f}%p\n"
        header += f"과매도 {stats.get('oversold', 0)} │ 과매수 {stats.get('overbought', 0)}\n"
        header += f"강한 추세 {stats.get('strong_trend', 0)} │ 트레이드 가능 {stats.get('tradeable_count', 0)}\n"
        header += f"표시 스타일: {style_label(style)}\n"
        header += "━━━━━━━━━━━━━━━━━━\n\n"

        await send_long_message(query, header + ai_result["analysis"], reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"AI 분석 실패: {exc}", reply_markup=kb.back())


async def handle_analyze_menu(query) -> None:
    text = "📊 <b>종목 쉽게 보기</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "버튼을 누르거나 티커를 직접 입력하세요.\n"
    text += "예: <code>AAPL</code>, <code>TSLA</code>"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.analyze_menu())


async def handle_analyze_input(query) -> None:
    text = "⌨️ <b>직접 입력 모드</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "채팅창에 티커만 입력하면 됩니다.\n"
    text += "예: <code>NVDA</code>, <code>MSFT</code>"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back("analyze_menu", "종목 쉽게 보기"))


async def handle_fear_greed(query) -> None:
    await query.edit_message_text("😱 시장 심리 불러오는 중...")
    try:
        style = _style_from_query(query)
        fg = get_fear_greed_index()
        text = fmt.format_fear_greed(fg, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"조회 실패: {exc}", reply_markup=kb.back())


async def handle_trading_menu(query) -> None:
    if not await _guard_trading_ready(query):
        return

    text = "💰 <b>트레이딩</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "KIS API를 통한 주문/자동매매 설정 메뉴입니다.\n"
    text += "자동매매는 반드시 모의계좌로 먼저 점검하세요."
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())


async def handle_auto_settings(query) -> None:
    if not await _guard_trading_ready(query):
        return

    auto_buy = watchlist.is_auto_buy()
    auto_sell = watchlist._load().get("settings", {}).get("auto_sell", False)

    text = fmt.header("자동매매 설정", "⚙️")
    text += f"\n자동매수: {'ON' if auto_buy else 'OFF'}"
    text += f"\n자동손절: {'ON' if auto_sell else 'OFF'}\n"
    text += "\n• 자동매수: 관심종목 저점 신호 기반"
    text += "\n• 자동손절: 보유종목 -7% 이하"
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=kb.auto_settings_menu(auto_buy, auto_sell),
    )


async def handle_toggle_auto_buy(query) -> None:
    if not await _guard_trading_ready(query):
        return

    current = watchlist.is_auto_buy()
    watchlist.set_auto_buy(not current)
    await query.answer(f"자동매수 {'ON' if not current else 'OFF'}")
    await handle_auto_settings(query)


async def handle_toggle_auto_sell(query) -> None:
    if not await _guard_trading_ready(query):
        return

    data = watchlist._load()
    current = data.get("settings", {}).get("auto_sell", False)
    data.setdefault("settings", {})["auto_sell"] = not current
    watchlist._save()

    await query.answer(f"자동손절 {'ON' if not current else 'OFF'}")
    await handle_auto_settings(query)


async def handle_balance(query) -> None:
    if not await _guard_trading_ready(query):
        return

    await query.edit_message_text("💵 잔고 조회 중...")
    try:
        style = _style_from_query(query)
        result = portfolio.get_status()
        if "error" in result:
            await query.edit_message_text(f"❌ {result['error']}", reply_markup=kb.trading_menu())
            return

        text = fmt.format_balance(result, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())
    except Exception as exc:
        await query.edit_message_text(f"잔고 조회 실패: {exc}", reply_markup=kb.trading_menu())


async def handle_orders(query) -> None:
    if not await _guard_trading_ready(query):
        return

    await query.edit_message_text("📋 미체결 주문 조회 중...")
    try:
        style = _style_from_query(query)
        result = kis.get_orders()
        if "error" in result:
            await query.edit_message_text(f"❌ {result['error']}", reply_markup=kb.trading_menu())
            return

        text = fmt.format_orders(result.get("orders", []), style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())
    except Exception as exc:
        await query.edit_message_text(f"주문 조회 실패: {exc}", reply_markup=kb.trading_menu())


async def handle_api_status(query) -> None:
    if not await _guard_trading_ready(query):
        return

    try:
        style = _style_from_query(query)
        status = kis.check_status()
        text = fmt.format_api_status(status, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())
    except Exception as exc:
        await query.edit_message_text(f"상태 확인 실패: {exc}", reply_markup=kb.trading_menu())


async def handle_watchlist_main(query) -> None:
    data = watchlist.get_all()
    settings = data.get("settings", {})

    stock_count = len(data.get("stocks", {}))
    monitor_on = settings.get("monitor_enabled", True)
    interval = settings.get("monitor_interval", 30)
    auto_buy = settings.get("auto_buy", False)

    text = "👀 <b>관심종목</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += f"등록 종목: {stock_count}개\n"
    text += f"모니터링: {'ON' if monitor_on else 'OFF'} ({interval}분)\n"
    text += f"자동매수: {'ON' if auto_buy else 'OFF'}\n\n"
    text += "신호 기준: RSI, BB 위치, 단기 하락, 이평 괴리"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_main_menu())


async def handle_watchlist_status(query) -> None:
    await query.edit_message_text("📋 관심종목 상태 불러오는 중...")

    try:
        style = _style_from_query(query)
        stocks = watchlist.get_status()
        text = fmt.format_watchlist(stocks, watchlist.is_auto_buy(), style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_main_menu())
    except Exception as exc:
        await query.edit_message_text(f"조회 실패: {exc}", reply_markup=kb.watchlist_main_menu())


async def handle_watchlist_check_now(query) -> None:
    await query.edit_message_text("⚡ 관심종목 즉시 체크 중...")

    try:
        style = _style_from_query(query)
        results = monitor.check_all_watchlist()
        if results:
            text = monitor.format_alert_message(results)
            await send_long_message(query, text, reply_markup=kb.watchlist_main_menu())
            return

        total = len(watchlist.get_all().get("stocks", {}))
        text = fmt.format_watchlist_signals([], total, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_main_menu())
    except Exception as exc:
        await query.edit_message_text(f"체크 실패: {exc}", reply_markup=kb.watchlist_main_menu())


async def handle_watchlist_add_menu(query) -> None:
    text = "➕ <b>관심종목 추가</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    text += "버튼으로 선택하거나, 심볼을 직접 입력해도 됩니다."
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_add())


async def handle_watchlist_remove_menu(query) -> None:
    stocks = list(watchlist.get_all().get("stocks", {}).keys())
    if not stocks:
        await query.edit_message_text("삭제할 종목이 없습니다.", reply_markup=kb.watchlist_main_menu())
        return

    text = "➖ <b>관심종목 삭제</b>\n━━━━━━━━━━━━━━━━━━\n\n삭제할 종목을 선택하세요."
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_remove_menu(stocks))


async def handle_watchlist_remove(query, data: str) -> None:
    symbol = data[9:]
    result = watchlist.remove(symbol)

    if result.get("success"):
        await query.answer(f"{symbol} 삭제 완료")
    else:
        await query.answer(f"삭제 실패: {result.get('error', 'unknown')}"[:180])

    await handle_watchlist_remove_menu(query)


async def handle_watchlist_alert_settings(query) -> None:
    settings = watchlist.get_all().get("settings", {})
    monitor_on = settings.get("monitor_enabled", True)
    interval = settings.get("monitor_interval", 30)

    text = fmt.header("관심종목 알림 설정", "⚙️")
    text += f"\n모니터링: {'ON' if monitor_on else 'OFF'}"
    text += f"\n체크 간격: {interval}분\n"
    text += "\n간격 변경은 15 → 30 → 60분 순환입니다."

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=kb.watchlist_alert_settings(settings),
    )


async def handle_toggle_monitor(query) -> None:
    data = watchlist._load()
    settings = data.setdefault("settings", {})

    current = settings.get("monitor_enabled", True)
    settings["monitor_enabled"] = not current
    watchlist._save()

    await query.answer(f"모니터링 {'ON' if not current else 'OFF'}")
    await handle_watchlist_alert_settings(query)


async def handle_change_interval(query) -> None:
    data = watchlist._load()
    settings = data.setdefault("settings", {})

    current = settings.get("monitor_interval", 30)
    intervals = [15, 30, 60]
    idx = intervals.index(current) if current in intervals else 1
    new_interval = intervals[(idx + 1) % len(intervals)]

    settings["monitor_interval"] = new_interval
    watchlist._save()

    await query.answer(f"체크 간격 {new_interval}분")
    await handle_watchlist_alert_settings(query)


async def handle_analyze_stock(query, data: str) -> None:
    symbol = data[2:]
    await query.edit_message_text(f"🔎 {symbol} 분석 중...")

    try:
        style = _style_from_query(query)
        analysis = get_full_analysis(symbol)
        if analysis is None:
            await query.edit_message_text(f"'{symbol}' 데이터 없음", reply_markup=kb.back())
            return

        analysis["score"] = calculate_score(analysis)
        text = fmt.format_analysis(analysis, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.stock_detail(symbol))
    except Exception as exc:
        await query.edit_message_text(f"분석 실패: {exc}", reply_markup=kb.back())


async def handle_ai_stock(query, data: str) -> None:
    symbol = data[3:]
    await query.edit_message_text(f"🤖 {symbol} AI 요약 생성 중...")

    try:
        from core.news import get_company_news

        analysis = get_full_analysis(symbol)
        if analysis is None:
            await query.edit_message_text(f"'{symbol}' 데이터 없음", reply_markup=kb.back())
            return

        news = get_company_news(symbol, days=3)
        score = calculate_score(analysis)
        analysis["score"] = score
        analysis["total_score"] = score.get("total_score", 0)
        analysis["news"] = news

        result = ai.analyze_stock(symbol, analysis)
        if "error" in result:
            await query.edit_message_text(f"❌ {result['error']}", reply_markup=kb.stock_detail(symbol))
            return

        header = f"🤖 <b>{symbol} AI 요약</b>\n━━━━━━━━━━━━━━━━━━\n"
        header += f"뉴스 반영: {len(news)}건\n\n"
        await send_long_message(query, header + result["analysis"], reply_markup=kb.stock_detail(symbol))
    except Exception as exc:
        await query.edit_message_text(f"AI 분석 실패: {exc}", reply_markup=kb.back())


async def handle_watchlist_add(query, data: str) -> None:
    symbol = data[9:]
    await query.edit_message_text(f"➕ {symbol} 추가 중...")

    try:
        result = watchlist.add(symbol)
        if result.get("success"):
            text = "✅ <b>관심종목 추가 완료</b>\n\n"
            text += f"종목: {symbol}\n"
            text += f"현재가: ${result['price']}\n"
            text += f"목표가: ${result['target_price']}"
        else:
            text = f"❌ 추가 실패: {result.get('error', 'unknown')}"

        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_main_menu())
    except Exception as exc:
        await query.edit_message_text(f"추가 실패: {exc}", reply_markup=kb.watchlist_main_menu())


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
    "auto_settings": handle_auto_settings,
    "toggle_auto_buy": handle_toggle_auto_buy,
    "toggle_auto_sell": handle_toggle_auto_sell,
    "balance": handle_balance,
    "orders": handle_orders,
    "api_status": handle_api_status,
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

    await query.edit_message_text("알 수 없는 메뉴입니다.", reply_markup=kb.main_menu())
