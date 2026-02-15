"""Telegram message formatter module with beginner-friendly style levels."""

from __future__ import annotations

from typing import Any


LINE = "━" * 18


def header(title: str, emoji: str = "") -> str:
    return f"{emoji} <b>{title}</b>\n{LINE}"


def section(title: str) -> str:
    return f"\n<b>{title}</b>"


def item(label: str, value: Any, suffix: str = "") -> str:
    return f"• {label}: {value}{suffix}"


def pct(value: float, with_sign: bool = True) -> str:
    if with_sign:
        return f"{value:+.1f}%"
    return f"{value:.1f}%"


def usd(value: float, decimals: int = 2) -> str:
    if decimals == 0:
        return f"${value:,.0f}"
    return f"${value:,.{decimals}f}"


def emoji_pnl(value: float) -> str:
    return "🟢" if value >= 0 else "🔴"


def grade_emoji(grade: str) -> str:
    return {"A": "🏆", "B": "🥇", "C": "🥈", "D": "⚠️", "F": "❌"}.get(grade, "")


def _stage_label(stage: str) -> str:
    return {
        "right_knee": "눌림 구간(우측무릎)",
        "mid_trend": "추세 진행 구간",
        "right_shoulder": "고점 근처(우측어깨)",
    }.get(stage, stage or "-")


def _blocker_label(blocker: str) -> str:
    mapping = {
        "liquidity": "거래량 부족",
        "rr2": "수익/손실 비율 부족",
        "risk_pct": "손절폭 과다",
        "setup": "세팅 점수 부족",
        "stage": "고점 근처",
        "relative_strength": "시장 대비 약세",
        "risk_model": "리스크 높음",
        "event": "이벤트 리스크(실적 등)",
        "fundamental": "펀더멘털 확신 부족",
    }
    return mapping.get(blocker, blocker)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _style_level(style: str) -> int:
    s = (style or "beginner").strip().lower()
    if s == "compact":
        s = "beginner"
    if s == "detail":
        return 2
    if s == "standard":
        return 1
    return 0


def _confidence_text(score: float) -> str:
    if score >= 80:
        return "높음"
    if score >= 60:
        return "보통"
    return "낮음"


def _risk_text(score: float) -> str:
    if score >= 70:
        return "높음"
    if score >= 45:
        return "중간"
    return "낮음"


def _decision_text(total_score: float, grade: str, risk_score: float) -> tuple[str, str]:
    if risk_score >= 70:
        return "⛔ 지금은 쉬어가기", "리스크가 높아 초보 매매에 불리"
    if grade in {"A", "B"} and total_score >= 68:
        return "✅ 분할매수 후보", "점수와 추세가 상대적으로 양호"
    if grade == "C":
        return "⏳ 관망", "확신이 충분하지 않아 추세 확인 필요"
    return "⚠️ 보수 접근", "하락 변동성 또는 데이터 확신 부족"


def _entry_plan_for_beginner(data: dict[str, Any]) -> tuple[float, float, float, float]:
    price = _safe_float(data.get("price"), 0.0)
    support = (data.get("support") or [None])[0]
    resistance = (data.get("resistance") or [None])[0]
    support_v = _safe_float(support, 0.0)
    resistance_v = _safe_float(resistance, 0.0)

    if support_v > 0:
        buy_low = support_v * 0.995
        buy_high = min(price, support_v * 1.01) if price > 0 else support_v * 1.01
    else:
        buy_low = price * 0.98 if price > 0 else 0
        buy_high = price * 1.0 if price > 0 else 0

    if buy_high <= 0:
        buy_high = price
    if buy_low <= 0:
        buy_low = buy_high * 0.98 if buy_high > 0 else 0

    stop = buy_low * 0.93 if buy_low > 0 else 0
    target = resistance_v if resistance_v > buy_high else buy_high * 1.08
    return buy_low, buy_high, stop, target


# ===== Analysis =====
def format_analysis(data: dict, style: str = "beginner") -> str:
    level = _style_level(style)

    symbol = str(data.get("symbol", "종목"))
    price = _safe_float(data.get("price"), 0.0)
    score = data.get("score") or {}
    risk = score.get("risk") or {}
    confidence = score.get("confidence") or {}
    warnings = [w for w in (risk.get("warnings") or []) if isinstance(w, str) and w.strip()]

    total_score = _safe_float(score.get("total_score"), 0.0)
    grade = str(score.get("grade", "C"))
    recommendation = str(score.get("recommendation", "")).strip()
    risk_score = _safe_float(risk.get("score"), 50.0)
    confidence_score = _safe_float(confidence.get("score"), 60.0)

    if level == 0:
        decision, reason = _decision_text(total_score, grade, risk_score)
        buy_low, buy_high, stop, target = _entry_plan_for_beginner(data)

        text = header(f"{symbol} 한눈에 보기", "📊")
        text += f"\n\n현재가: <b>{usd(price)}</b>"
        text += f"\n판단: <b>{decision}</b>"
        text += f"\n근거: {reason}"
        text += f"\n신뢰도: {_confidence_text(confidence_score)} | 리스크: {_risk_text(risk_score)}"
        text += f"\n점수: {total_score:.0f}/100 ({grade})"
        text += "\n\n<b>초보 실행안</b>"
        text += f"\n매수 구간: {usd(buy_low)} ~ {usd(buy_high)}"
        text += f"\n손절 기준: {usd(stop)}"
        text += f"\n1차 목표: {usd(target)}"
        if warnings:
            text += f"\n주의: {warnings[0]}"
        text += "\n\n<i>팁: 한 번에 전액 매수하지 말고 3회 분할 진입</i>"
        return text

    text = header(symbol, "📊") + "\n"
    text += f"<b>{usd(price)}</b>\n"
    text += f"{grade_emoji(grade)} 점수 {total_score:.0f}/100({grade}) | 신뢰 {confidence_score:.0f} | 위험 {risk_score:.0f}"
    if recommendation:
        text += f"\n의견: {recommendation}"
    text += (
        f"\nRSI {_safe_float(data.get('rsi', 50)):.0f} | "
        f"BB {_safe_float(data.get('bb_position', 50)):.0f}% | "
        f"52주 {_safe_float(data.get('position_52w', 50)):.0f}% | "
        f"MA50 {pct(_safe_float(data.get('ma50_gap', 0)))}"
    )

    if level >= 2:
        adx = _safe_float(data.get("adx", 0))
        stoch = _safe_float(data.get("stoch_k", 0))
        vol_ratio = _safe_float(data.get("volume_ratio", 1))
        text += f"\nADX {adx:.0f} | Stoch {stoch:.0f} | 거래량 {vol_ratio:.2f}배"

    if warnings:
        show_n = 3 if level == 1 else 4
        text += f"\n주의: {' / '.join(warnings[:show_n])}"

    return text


def _recommend_row_beginner(index: int, row: dict[str, Any]) -> str:
    plan = row.get("trade_plan", {})
    positioning = plan.get("positioning", {})
    rr = plan.get("risk_reward", {})
    entry = plan.get("entry", {})
    targets = plan.get("targets", {})

    symbol = str(row.get("symbol", "-"))
    price = _safe_float(row.get("price"), 0.0)
    tradeable = bool(plan.get("tradeable", False))
    stage = _stage_label(str(positioning.get("stage", "")))
    buy1 = _safe_float(entry.get("buy1"), 0.0)
    buy2 = _safe_float(entry.get("buy2"), 0.0)
    stop = _safe_float(plan.get("stop_loss"), 0.0)
    target1 = _safe_float(targets.get("target1"), 0.0)
    rr2 = _safe_float(rr.get("rr2"), 0.0)
    blockers = [_blocker_label(x) for x in (plan.get("blockers") or []) if isinstance(x, str)]

    line = f"\n\n<b>{index}. {symbol}</b> {usd(price)}"
    if tradeable:
        line += "\n✅ 지금은 매수 후보"
        if buy1 > 0 and buy2 > 0:
            lo, hi = sorted((buy1, buy2))
            line += f"\n매수 구간: {usd(lo)} ~ {usd(hi)}"
        if stop > 0 and target1 > 0:
            line += f"\n손절: {usd(stop)} | 1차 목표: {usd(target1)}"
        line += f"\n상태: {stage} | 기대비율(RR2): {rr2:.2f}"
    else:
        line += "\n⏳ 지금은 대기"
        if blockers:
            line += f"\n대기 이유: {', '.join(blockers[:2])}"
        else:
            line += f"\n대기 이유: 조건 미충족 ({stage})"
    return line


# ===== Recommendations =====
def format_recommendations(stocks: list, total: int, style: str = "beginner") -> str:
    level = _style_level(style)
    text = header("추천 종목", "🚀") + "\n"
    text += f"전체 분석: {total}개"

    if not stocks:
        text += "\n\n조건을 만족하는 종목이 없습니다."
        text += "\n<i>초보 팁: 시장 분위기가 약할 땐 쉬어가는 것도 전략입니다.</i>"
        return text

    shown = stocks[:10]
    if level == 0:
        text += "\n\n<b>초보용 요약</b>: \"지금 매수\" 또는 \"대기\"만 확인하세요."
        for i, row in enumerate(shown, 1):
            text += _recommend_row_beginner(i, row)
        text += f"\n\n{LINE}\n표시 {len(shown)}개"
        return text

    for i, row in enumerate(shown, 1):
        score = row.get("score", {})
        plan = row.get("trade_plan", {})

        grade = str(score.get("grade", "C"))
        total_score = _safe_float(score.get("total_score", 0))
        rr2 = _safe_float(plan.get("risk_reward", {}).get("rr2", 0))
        stage = _stage_label(str(plan.get("positioning", {}).get("stage", "")))
        entry = _safe_float(plan.get("entry", {}).get("buy2", 0))
        stop = _safe_float(plan.get("stop_loss", 0))
        target2 = _safe_float(plan.get("targets", {}).get("target2", 0))
        pos_pct = _safe_float(plan.get("execution", {}).get("position_pct", row.get("position_size_pct", 0)))
        rs63 = _safe_float(row.get("relative_strength_63d", plan.get("positioning", {}).get("relative_strength_63d", 0)))

        text += (
            f"\n\n<b>{i}. {row.get('symbol', '-')}</b> {usd(_safe_float(row.get('price', 0)))}"
            f" | {grade_emoji(grade)}{total_score:.0f} | RR2 {rr2:.2f} | RS63 {rs63:+.1f}"
        )
        if level == 1:
            text += f"\n{stage} | 진입 {usd(entry)} 손절 {usd(stop)} 목표 {usd(target2)} | 비중 {pos_pct:.1f}%"
        else:
            inv = _safe_float(row.get("investability_score", row.get("quality_score", 0)))
            liq = _safe_float(row.get("liquidity_score", plan.get("liquidity", {}).get("score", 0)))
            text += (
                f"\n{stage} | 진입 {usd(entry)} 손절 {usd(stop)} 목표 {usd(target2)}"
                f" | 비중 {pos_pct:.1f}% | 유동성 {liq:.0f} | 투자가능 {inv:.1f}"
            )

    text += f"\n\n{LINE}\n상위 {len(shown)}개 표시"
    return text


def format_scan_brief(results: list, total: int, top_n: int = 12, style: str = "beginner") -> str:
    level = _style_level(style)
    text = header("빠른 스캔", "🔎") + "\n"
    text += f"분석: {total}개"

    if not results:
        return text + "\n\n데이터가 없습니다."

    ranked = sorted(
        results,
        key=lambda x: -x.get("investability_score", x.get("score", {}).get("total_score", 0)),
    )[: max(1, top_n)]

    if level == 0:
        text += "\n\n<b>초보용 상위 후보</b>"
        for i, row in enumerate(ranked, 1):
            plan = row.get("trade_plan", {})
            tradeable = bool(plan.get("tradeable", False))
            stage = _stage_label(str(plan.get("positioning", {}).get("stage", "")))
            blockers = [_blocker_label(x) for x in (plan.get("blockers") or []) if isinstance(x, str)]
            state = "✅ 매수 후보" if tradeable else "⏳ 대기"
            reason = stage if tradeable else (blockers[0] if blockers else "조건 미충족")
            text += f"\n\n<b>{i}. {row.get('symbol', '-')}</b> {usd(_safe_float(row.get('price', 0)))}"
            text += f"\n{state} | {reason}"
        return text

    for i, row in enumerate(ranked, 1):
        score = row.get("score", {})
        plan = row.get("trade_plan", {})
        rr2 = _safe_float(plan.get("risk_reward", {}).get("rr2", 0))
        stage = _stage_label(str(plan.get("positioning", {}).get("stage", "")))
        rs63 = _safe_float(row.get("relative_strength_63d", plan.get("positioning", {}).get("relative_strength_63d", 0)))

        text += (
            f"\n\n<b>{i}. {row.get('symbol', '-')}</b> {usd(_safe_float(row.get('price', 0)))}"
            f" | 점수 {_safe_float(score.get('total_score', 0)):.0f} | RR2 {rr2:.2f} | RS63 {rs63:+.1f}"
        )
        if level == 1:
            pos_pct = _safe_float(plan.get("execution", {}).get("position_pct", row.get("position_size_pct", 0)))
            text += f"\n{stage} | 비중 {pos_pct:.1f}%"
        else:
            inv = _safe_float(row.get("investability_score", 0))
            liq = _safe_float(row.get("liquidity_score", plan.get("liquidity", {}).get("score", 0)))
            text += f"\n투자가능 {inv:.1f} | 유동성 {liq:.0f} | {stage}"

    return text


# ===== Balance =====
def format_balance(balance: dict, style: str = "beginner") -> str:
    level = _style_level(style)
    available = _safe_float(balance.get("available_cash", 0))
    holdings = balance.get("holdings", []) or []

    text = header("보유 현황", "💰") + "\n"
    text += f"주문가능 현금: {usd(available)}"

    if not holdings:
        return text + "\n\n보유 종목이 없습니다."

    total_pnl = 0.0
    for row in holdings:
        qty = int(_safe_float(row.get("qty", 0)))
        pnl_pct = _safe_float(row.get("pnl_pct", 0))
        eval_amt = _safe_float(row.get("eval_amt", 0))
        avg_price = _safe_float(row.get("avg_price", 0))
        cur_price = eval_amt / qty if qty > 0 else 0.0

        text += (
            f"\n\n{emoji_pnl(pnl_pct)} <b>{row.get('symbol', '-')}</b> {qty}주"
            f" | 손익 {pct(pnl_pct)} | 평가 {usd(eval_amt)}"
        )
        if level >= 1:
            text += f"\n평단 {usd(avg_price)} | 현재 {usd(cur_price)}"
        if level >= 2 and row.get("exit_signal"):
            reason = str(row.get("exit_reason", "")).strip()
            if reason:
                text += f"\n주의: {reason}"

        total_pnl += _safe_float(row.get("pnl", 0))

    text += f"\n\n{LINE}\n총손익 {emoji_pnl(total_pnl)} <b>{usd(total_pnl)}</b>"
    return text


# ===== Open Orders =====
def format_orders(orders: list, style: str = "beginner") -> str:
    _ = _style_level(style)
    text = header("미체결 주문", "📋")
    if not orders:
        return text + "\n\n미체결 주문이 없습니다."

    for row in orders:
        side = str(row.get("side", ""))
        emoji = "🟢" if side == "매수" else "🔴"
        text += (
            f"\n\n{emoji} <b>{row.get('symbol', '-')}</b> {side}"
            f" | {row.get('qty', 0)}주 @ {usd(_safe_float(row.get('price', 0)))}"
            f" | 체결 {row.get('filled', 0)}"
        )
    return text


# ===== Watchlist =====
def format_watchlist(stocks: list, auto_buy: bool, style: str = "beginner") -> str:
    level = _style_level(style)
    text = header(f"관심종목 {len(stocks)}개", "👀")

    if not stocks:
        return text + "\n\n등록된 종목이 없습니다."

    for row in stocks:
        price = _safe_float(row.get("price", 0))
        change_pct = _safe_float(row.get("change_pct", 0))

        if row.get("is_signal"):
            icon = "🟢"
            state = "매수 신호 감지"
        elif row.get("status") == "bought":
            icon = "💼"
            state = "보유중"
        else:
            icon = "⚪"
            state = "관찰중"

        text += f"\n\n{icon} <b>{row.get('symbol', '-')}</b> {usd(price)} ({pct(change_pct)})"
        if level == 0:
            text += f"\n상태: {state}"
        else:
            text += (
                f"\n상태: {state} | RSI {_safe_float(row.get('rsi', 50)):.0f}"
                f" | 조건 {int(_safe_float(row.get('met_count', 0)))}/4"
            )

    text += f"\n\n{LINE}\n자동매수 {'🟢 ON' if auto_buy else '🔴 OFF'}"
    return text


def format_watchlist_signals(signals: list, total: int, style: str = "beginner") -> str:
    level = _style_level(style)
    text = header("관심종목 신호", "🚨") + "\n"
    text += f"점검 종목: {total}개"

    if not signals:
        text += "\n\n오늘은 새 매수 신호가 없습니다."
        if level >= 1:
            text += "\n기준: RSI≤35, BB 하단, 5일선 이격, 3일 하락"
        return text

    text += f"\n\n신호 {len(signals)}개"
    for row in signals:
        text += (
            f"\n\n<b>{row.get('symbol', '-')}</b> | {usd(_safe_float(row.get('price', 0)))}"
            f"\n강도: {row.get('strength', '보통')} | 조건 {int(_safe_float(row.get('met_count', 0)))}/4"
        )
    return text


# ===== Trade Results =====
def format_trade_result(action: str, results: list, style: str = "beginner") -> str:
    level = _style_level(style)
    text = header(f"자동{action} 결과", "📦")

    success = 0
    fail = 0
    for row in results:
        if row.get("success"):
            success += 1
            text += (
                f"\n\n✅ <b>{row.get('symbol', '?')}</b>"
                f" | {row.get('qty', 0)}주 @ {usd(_safe_float(row.get('price', 0)))}"
            )
            reason = str(row.get("reason", "")).strip()
            if reason and level >= 1:
                text += f"\n{reason[:120]}"
        elif row.get("error"):
            fail += 1
            text += f"\n\n❌ {row.get('symbol', '?')}: {str(row['error'])[:120]}"
        elif row.get("message"):
            text += f"\n\nℹ️ {str(row['message'])[:140]}"

    text += f"\n\n{LINE}\n성공 {success} | 실패 {fail}"
    return text


# ===== API Status =====
def format_api_status(status: dict, style: str = "beginner") -> str:
    _ = _style_level(style)
    text = header("API 상태", "🔌")
    if status.get("connected"):
        mode = "모의투자" if status.get("is_paper") else "실전투자"
        text += "\n\n✅ 연결됨"
        text += f"\n모드: {mode}"
        text += f"\n계좌: {status.get('account', '미설정')}"
        return text

    text += "\n\n❌ 연결 안됨"
    text += f"\n오류: {status.get('error', '알 수 없음')}"
    text += "\n설정값 확인: KIS_APP_KEY / KIS_APP_SECRET / KIS_ACCOUNT_NO"
    return text


def _fear_greed_action(score: int) -> str:
    if score <= 25:
        return "공포 구간입니다. 급하게 추격하지 말고 분할 접근하세요."
    if score <= 45:
        return "약한 공포 구간입니다. 종목별로 신호가 갈릴 수 있습니다."
    if score <= 55:
        return "중립 구간입니다. 무리한 베팅보다 기준 매매가 유리합니다."
    if score <= 75:
        return "탐욕 구간입니다. 이익보호(분할매도/손절선) 관리가 중요합니다."
    return "과열 구간입니다. 초보는 신규 진입을 보수적으로 보세요."


# ===== Fear & Greed =====
def format_fear_greed(fg: dict, style: str = "beginner") -> str:
    level = _style_level(style)
    score = int(_safe_float(fg.get("score", 50), 50))
    text = header("시장 심리", "😱")
    text += f"\n\n{fg.get('emoji', '😐')} <b>{score}</b>/100 ({fg.get('rating', 'N/A')})"

    advice = str(fg.get("advice", "")).strip()
    if level == 0:
        text += f"\n{_fear_greed_action(score)}"
    elif advice:
        text += f"\n{advice}"
    else:
        text += f"\n{_fear_greed_action(score)}"

    if level >= 1:
        text += "\n\n구간: 0-25 공포 | 25-45 약공포 | 45-55 중립 | 55-75 탐욕 | 75-100 과열"
    return text
