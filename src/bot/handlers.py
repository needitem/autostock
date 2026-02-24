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

    text = "??<b>?몃젅?대뵫 鍮꾪솢?깊솕</b>\n?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺\n\n"
    text += "KIS API ?ㅺ? ?ㅼ젙?섏? ?딆븯?듬땲??\n\n"
    text += "?꾩닔 ?섍꼍蹂?? <code>KIS_APP_KEY</code>, <code>KIS_APP_SECRET</code>, <code>KIS_ACCOUNT_NO</code>"
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

    text = "?룧 <b>AutoStock</b>\n?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺\n\n"
    text += "<b>泥섏쓬?대씪硫??대젃寃??쒖옉?섏꽭??/b>\n"
    text += "1) ?? ?ㅻ뒛 萸??닿퉴\n"
    text += "2) ?뱤 醫낅ぉ ?쎄쾶 蹂닿린\n"
    text += "3) ?? 愿?ъ쥌紐??깅줉\n\n"
    text += f"愿?ъ쥌紐? <b>{stock_count}</b>媛?| 紐⑤땲?곕쭅 {'ON' if monitor_on else 'OFF'}\n"
    text += f"?먮룞留ㅼ닔/?먯젅: {'ON' if auto_buy else 'OFF'} / {'ON' if auto_sell else 'OFF'}\n"
    text += f"?붾㈃ 紐⑤뱶: <b>{style_label(style)}</b>\n"
    text += f"트레이딩: {'ON' if kb.trading_enabled() else 'OFF'}\n\n"
    text += "?곗빱留?蹂대궡??諛붾줈 遺꾩꽍?⑸땲?? (?? <code>AAPL</code>)"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.main_menu())


async def handle_help(query) -> None:
    text = "?뱦 <b>珥덈낫 媛?대뱶</b>\n?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺\n\n"
    text += "1) <b>?ㅻ뒛 萸??닿퉴</b>: 吏湲?留ㅼ닔/?湲곕쭔 鍮좊Ⅴ寃??뺤씤\n"
    text += "2) <b>醫낅ぉ ?쎄쾶 蹂닿린</b>: ??醫낅ぉ???ㅽ뻾??留ㅼ닔/?먯젅/紐⑺몴)?쇰줈 ?뺤씤\n"
    text += "3) <b>愿?ъ쥌紐?/b>: ?뚮┝ 耳쒕몢怨??좏샇 ???뚮쭔 泥댄겕\n\n"
    text += "?⑹뼱媛 ?대졄?ㅻ㈃ <b>珥덈낫(沅뚯옣)</b> 紐⑤뱶瑜??ъ슜?섏꽭??"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())


async def handle_display_settings(query) -> None:
    style = _style_from_query(query)
    text = "?숋툘 <b>?쒖떆 ?ㅼ젙</b>\n?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺\n\n"
    text += f"?꾩옱 ?ㅽ??? <b>{style_label(style)}</b>\n\n"
    text += "??珥덈낫(沅뚯옣): 吏湲?????以묒떖\n"
    text += "???쒖?: 湲곕낯 吏??+ 留ㅻℓ ?뺣낫\n"
    text += "???곸꽭: 蹂댁“ 吏??寃쎄퀬源뚯? ?꾩껜 ?쒖떆"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.display_settings_menu(style))


async def handle_set_style(query, data: str) -> None:
    chat_id = _chat_id_from_query(query)
    requested = data[6:]
    style = normalize_style(requested)

    if not chat_id:
        await query.answer("梨꾪똿 ?뺣낫瑜?李얠쓣 ???놁뒿?덈떎.")
        return

    saved = set_chat_style(chat_id, style)
    await query.answer(f"?쒖떆 ?ㅽ??? {style_label(saved)}")
    await handle_display_settings(query)


async def handle_recommend(query) -> None:
    await query.edit_message_text("?뱢 異붿쿇 醫낅ぉ ?앹꽦 以?.. (??5~10遺?")

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
            text += "\n\n<i>理쒓렐 ?ㅼ틪 罹먯떆 ?ъ슜</i>"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"異붿쿇 ?앹꽦 ?ㅽ뙣: {exc}", reply_markup=kb.back())


async def handle_scan(query) -> None:
    await query.edit_message_text("?뵊 ?꾩껜 ?ㅼ틪 以?.. (??5~10遺?")

    try:
        from config import load_all_us_stocks

        style = _style_from_query(query)
        result, used_cache = get_scan_result(load_all_us_stocks(), max_age_sec=240)
        text = fmt.format_scan_brief(result["results"], result["total"], top_n=10, style=style)
        if used_cache:
            text += "\n\n<i>理쒓렐 ?ㅼ틪 罹먯떆 ?ъ슜</i>"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"?ㅼ틪 ?ㅽ뙣: {exc}", reply_markup=kb.back())


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
    text = "?뱤 <b>醫낅ぉ ?쎄쾶 蹂닿린</b>\n?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺\n\n"
    text += "踰꾪듉???꾨Ⅴ嫄곕굹 ?곗빱瑜?吏곸젒 ?낅젰?섏꽭??\n"
    text += "?? <code>AAPL</code>, <code>TSLA</code>"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.analyze_menu())


async def handle_analyze_input(query) -> None:
    text = "?⑨툘 <b>吏곸젒 ?낅젰 紐⑤뱶</b>\n?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺\n\n"
    text += "梨꾪똿李쎌뿉 ?곗빱留??낅젰?섎㈃ ?⑸땲??\n"
    text += "?? <code>NVDA</code>, <code>MSFT</code>"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back("analyze_menu", "醫낅ぉ ?쎄쾶 蹂닿린"))


async def handle_fear_greed(query) -> None:
    await query.edit_message_text("?삺 ?쒖옣 ?щ━ 遺덈윭?ㅻ뒗 以?..")
    try:
        style = _style_from_query(query)
        fg = get_fear_greed_index()
        text = fmt.format_fear_greed(fg, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.back())
    except Exception as exc:
        await query.edit_message_text(f"議고쉶 ?ㅽ뙣: {exc}", reply_markup=kb.back())


async def handle_trading_menu(query) -> None:
    if not await _guard_trading_ready(query):
        return

    text = "?뮥 <b>?몃젅?대뵫</b>\n?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺\n\n"
    text += "KIS API瑜??듯븳 二쇰Ц/?먮룞留ㅻℓ ?ㅼ젙 硫붾돱?낅땲??\n"
    text += "?먮룞留ㅻℓ??諛섎뱶??紐⑥쓽怨꾩쥖濡?癒쇱? ?먭??섏꽭??"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())


async def handle_auto_settings(query) -> None:
    await query.answer("자동매매는 현재 비활성화 상태입니다.")
    await query.edit_message_text("자동매매는 현재 비활성화 상태입니다.")
    return
    if not await _guard_trading_ready(query):
        return

    auto_buy = watchlist.is_auto_buy()
    auto_sell = watchlist._load().get("settings", {}).get("auto_sell", False)

    text = fmt.header("?먮룞留ㅻℓ ?ㅼ젙", "?숋툘")
    text += f"\n?먮룞留ㅼ닔: {'ON' if auto_buy else 'OFF'}"
    text += f"\n?먮룞?먯젅: {'ON' if auto_sell else 'OFF'}\n"
    text += "\n???먮룞留ㅼ닔: 愿?ъ쥌紐?????좏샇 湲곕컲"
    text += "\n???먮룞?먯젅: 蹂댁쑀醫낅ぉ -7% ?댄븯"
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=kb.auto_settings_menu(auto_buy, auto_sell),
    )


async def handle_toggle_auto_buy(query) -> None:
    await query.answer("자동매매는 현재 비활성화 상태입니다.")
    await query.edit_message_text("자동매매는 현재 비활성화 상태입니다.")
    return
    if not await _guard_trading_ready(query):
        return

    current = watchlist.is_auto_buy()
    watchlist.set_auto_buy(not current)
    await query.answer(f"?먮룞留ㅼ닔 {'ON' if not current else 'OFF'}")
    await handle_auto_settings(query)


async def handle_toggle_auto_sell(query) -> None:
    await query.answer("자동매매는 현재 비활성화 상태입니다.")
    await query.edit_message_text("자동매매는 현재 비활성화 상태입니다.")
    return
    if not await _guard_trading_ready(query):
        return

    data = watchlist._load()
    current = data.get("settings", {}).get("auto_sell", False)
    data.setdefault("settings", {})["auto_sell"] = not current
    watchlist._save()

    await query.answer(f"?먮룞?먯젅 {'ON' if not current else 'OFF'}")
    await handle_auto_settings(query)


async def handle_balance(query) -> None:
    if not await _guard_trading_ready(query):
        return

    await query.edit_message_text("?뮫 ?붽퀬 議고쉶 以?..")
    try:
        style = _style_from_query(query)
        result = portfolio.get_status()
        if "error" in result:
            await query.edit_message_text(f"??{result['error']}", reply_markup=kb.trading_menu())
            return

        text = fmt.format_balance(result, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())
    except Exception as exc:
        await query.edit_message_text(f"?붽퀬 議고쉶 ?ㅽ뙣: {exc}", reply_markup=kb.trading_menu())


async def handle_orders(query) -> None:
    if not await _guard_trading_ready(query):
        return

    await query.edit_message_text("?뱥 誘몄껜寃?二쇰Ц 議고쉶 以?..")
    try:
        style = _style_from_query(query)
        result = kis.get_orders()
        if "error" in result:
            await query.edit_message_text(f"??{result['error']}", reply_markup=kb.trading_menu())
            return

        text = fmt.format_orders(result.get("orders", []), style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())
    except Exception as exc:
        await query.edit_message_text(f"二쇰Ц 議고쉶 ?ㅽ뙣: {exc}", reply_markup=kb.trading_menu())


async def handle_api_status(query) -> None:
    if not await _guard_trading_ready(query):
        return

    try:
        style = _style_from_query(query)
        status = kis.check_status()
        text = fmt.format_api_status(status, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.trading_menu())
    except Exception as exc:
        await query.edit_message_text(f"?곹깭 ?뺤씤 ?ㅽ뙣: {exc}", reply_markup=kb.trading_menu())


async def handle_watchlist_main(query) -> None:
    data = watchlist.get_all()
    settings = data.get("settings", {})

    stock_count = len(data.get("stocks", {}))
    monitor_on = settings.get("monitor_enabled", True)
    interval = settings.get("monitor_interval", 30)
    auto_buy = settings.get("auto_buy", False)

    text = "?? <b>愿?ъ쥌紐?/b>\n?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺\n\n"
    text += f"?깅줉 醫낅ぉ: {stock_count}媛?n"
    text += f"紐⑤땲?곕쭅: {'ON' if monitor_on else 'OFF'} ({interval}遺?\n"
    text += f"?먮룞留ㅼ닔: {'ON' if auto_buy else 'OFF'}\n\n"
    text += "?좏샇 湲곗?: RSI, BB ?꾩튂, ?④린 ?섎씫, ?댄룊 愿대━"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_main_menu())


async def handle_watchlist_status(query) -> None:
    await query.edit_message_text("?뱥 愿?ъ쥌紐??곹깭 遺덈윭?ㅻ뒗 以?..")

    try:
        style = _style_from_query(query)
        stocks = watchlist.get_status()
        text = fmt.format_watchlist(stocks, watchlist.is_auto_buy(), style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_main_menu())
    except Exception as exc:
        await query.edit_message_text(f"議고쉶 ?ㅽ뙣: {exc}", reply_markup=kb.watchlist_main_menu())


async def handle_watchlist_check_now(query) -> None:
    await query.edit_message_text("??愿?ъ쥌紐?利됱떆 泥댄겕 以?..")

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
        await query.edit_message_text(f"泥댄겕 ?ㅽ뙣: {exc}", reply_markup=kb.watchlist_main_menu())


async def handle_watchlist_add_menu(query) -> None:
    text = "??<b>愿?ъ쥌紐?異붽?</b>\n?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺\n\n"
    text += "踰꾪듉?쇰줈 ?좏깮?섍굅?? ?щ낵??吏곸젒 ?낅젰?대룄 ?⑸땲??"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_add())


async def handle_watchlist_remove_menu(query) -> None:
    stocks = list(watchlist.get_all().get("stocks", {}).keys())
    if not stocks:
        await query.edit_message_text("??젣??醫낅ぉ???놁뒿?덈떎.", reply_markup=kb.watchlist_main_menu())
        return

    text = "??<b>愿?ъ쥌紐???젣</b>\n?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺?곣봺\n\n??젣??醫낅ぉ???좏깮?섏꽭??"
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_remove_menu(stocks))


async def handle_watchlist_remove(query, data: str) -> None:
    symbol = data[9:]
    result = watchlist.remove(symbol)

    if result.get("success"):
        await query.answer(f"{symbol} ??젣 ?꾨즺")
    else:
        await query.answer(f"??젣 ?ㅽ뙣: {result.get('error', 'unknown')}"[:180])

    await handle_watchlist_remove_menu(query)


async def handle_watchlist_alert_settings(query) -> None:
    settings = watchlist.get_all().get("settings", {})
    monitor_on = settings.get("monitor_enabled", True)
    interval = settings.get("monitor_interval", 30)

    text = fmt.header("愿?ъ쥌紐??뚮┝ ?ㅼ젙", "?숋툘")
    text += f"\n紐⑤땲?곕쭅: {'ON' if monitor_on else 'OFF'}"
    text += f"\n泥댄겕 媛꾧꺽: {interval}遺?n"
    text += "\n媛꾧꺽 蹂寃쎌? 15 ??30 ??60遺??쒗솚?낅땲??"

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

    await query.answer(f"紐⑤땲?곕쭅 {'ON' if not current else 'OFF'}")
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
    await query.edit_message_text(f"?뵊 {symbol} 遺꾩꽍 以?..")

    try:
        style = _style_from_query(query)
        analysis = get_full_analysis(symbol)
        if analysis is None:
            await query.edit_message_text(f"'{symbol}' ?곗씠???놁쓬", reply_markup=kb.back())
            return

        analysis["score"] = calculate_score(analysis)
        text = fmt.format_analysis(analysis, style=style)
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.stock_detail(symbol))
    except Exception as exc:
        await query.edit_message_text(f"遺꾩꽍 ?ㅽ뙣: {exc}", reply_markup=kb.back())


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


async def handle_watchlist_add(query, data: str) -> None:
    symbol = data[9:]
    await query.edit_message_text(f"??{symbol} 異붽? 以?..")

    try:
        result = watchlist.add(symbol)
        if result.get("success"):
            text = "??<b>愿?ъ쥌紐?異붽? ?꾨즺</b>\n\n"
            text += f"醫낅ぉ: {symbol}\n"
            text += f"?꾩옱媛: ${result['price']}\n"
            text += f"紐⑺몴媛: ${result['target_price']}"
        else:
            text = f"??異붽? ?ㅽ뙣: {result.get('error', 'unknown')}"

        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb.watchlist_main_menu())
    except Exception as exc:
        await query.edit_message_text(f"異붽? ?ㅽ뙣: {exc}", reply_markup=kb.watchlist_main_menu())


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

    await query.edit_message_text("?????녿뒗 硫붾돱?낅땲??", reply_markup=kb.main_menu())


