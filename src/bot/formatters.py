"""Telegram message formatter module with style levels."""

from __future__ import annotations

LINE = "━" * 18


def header(title: str, emoji: str = "") -> str:
    return f"{emoji} <b>{title}</b>\n{LINE}"


def section(title: str) -> str:
    return f"\n<b>{title}</b>"


def item(label: str, value, suffix: str = "") -> str:
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
        "right_knee": "우측무릎",
        "mid_trend": "추세중간",
        "right_shoulder": "우측어깨",
    }.get(stage, stage or "-")


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _style_level(style: str) -> int:
    s = (style or "compact").strip().lower()
    if s == "detail":
        return 2
    if s == "standard":
        return 1
    return 0


# ===== 종목 분석 =====
def format_analysis(data: dict, style: str = "compact") -> str:
    level = _style_level(style)

    symbol = str(data.get("symbol", ""))
    price = _safe_float(data.get("price", 0))
    score = data.get("score") or {}
    risk = score.get("risk") or {}
    confidence = score.get("confidence") or {}

    total = _safe_float(score.get("total_score", 0))
    grade = str(score.get("grade", "C"))
    rec = str(score.get("recommendation", "")).strip()

    risk_score = int(_safe_float(risk.get("score", 0)))
    conf_score = _safe_float(confidence.get("score", 0))
    conf_label = str(confidence.get("label", "보통"))
    warnings = [w for w in (risk.get("warnings") or []) if isinstance(w, str) and w.strip()][:4]

    text = header(symbol or "종목", "📊") + "\n"
    text += f"<b>{usd(price)}</b>\n"
    text += f"{grade_emoji(grade)} 점수 {total:.0f}/100({grade}) | 신뢰 {conf_score:.0f} | 위험 {risk_score}"
    text += "\n"
    text += (
        f"RSI {data.get('rsi', 50):.0f} | BB {data.get('bb_position', 50):.0f}% | "
        f"52주 {data.get('position_52w', 50):.0f}% | MA50 {pct(_safe_float(data.get('ma50_gap', 0)))}"
    )

    if level >= 1:
        if rec:
            text += f"\n의견: {rec}"
        text += f"\n신뢰도 등급: {conf_label}"

    if level >= 2:
        adx = _safe_float(data.get("adx", 0))
        stoch = _safe_float(data.get("stoch_k", 0))
        vol_ratio = _safe_float(data.get("volume_ratio", 1))
        support = (data.get("support") or [None])[0]
        resistance = (data.get("resistance") or [None])[0]

        text += f"\nADX {adx:.0f} | Stoch {stoch:.0f} | 거래량 {vol_ratio:.2f}배"
        if isinstance(support, (int, float)):
            text += f"\n지지 {usd(float(support))}"
        if isinstance(resistance, (int, float)):
            text += f" | 저항 {usd(float(resistance))}"

    if warnings:
        if level == 0:
            text += f"\n주의: {' / '.join(warnings[:2])}"
        elif level == 1:
            text += f"\n주의: {' / '.join(warnings[:3])}"
        else:
            text += f"\n주의: {' / '.join(warnings[:4])}"

    return text


# ===== 추천 종목 =====
def format_recommendations(stocks: list, total: int, style: str = "compact") -> str:
    level = _style_level(style)
    text = header("추천 종목", "📈") + "\n"
    text += f"분석 {total}개"

    if not stocks:
        text += "\n\n❌ 조건을 만족하는 종목이 없습니다."
        return text

    shown = stocks[:10]
    for i, s in enumerate(shown, 1):
        score = s.get("score", {})
        plan = s.get("trade_plan", {})

        grade = score.get("grade", "C")
        total_score = _safe_float(score.get("total_score", 0))
        rr2 = _safe_float(plan.get("risk_reward", {}).get("rr2", 0))
        stage = _stage_label(str(plan.get("positioning", {}).get("stage", "")))
        entry = _safe_float(plan.get("entry", {}).get("buy2", 0))
        stop = _safe_float(plan.get("stop_loss", 0))
        target2 = _safe_float(plan.get("targets", {}).get("target2", 0))
        pos_pct = _safe_float(plan.get("execution", {}).get("position_pct", s.get("position_size_pct", 0)))
        liq = _safe_float(s.get("liquidity_score", plan.get("liquidity", {}).get("score", 0)))
        inv = _safe_float(s.get("investability_score", s.get("quality_score", 0)))
        dte = s.get("days_to_earnings")

        text += (
            f"\n\n<b>{i}. {s.get('symbol', '-')}</b> {usd(_safe_float(s.get('price', 0)))}"
            f" | {grade_emoji(grade)}{total_score:.0f} | RSI {_safe_float(s.get('rsi', 50)):.0f} | RR2 {rr2:.2f}"
        )

        if level == 0:
            text += f"\n{stage} | 진입 {usd(entry)} 손절 {usd(stop)}"
        elif level == 1:
            text += f"\n{stage} | 진입 {usd(entry)} 손절 {usd(stop)} 목표 {usd(target2)} | 비중 {pos_pct:.1f}%"
        else:
            text += (
                f"\n{stage} | 진입 {usd(entry)} 손절 {usd(stop)} 목표 {usd(target2)}"
                f" | 비중 {pos_pct:.1f}% | 유동성 {liq:.0f} | 투자가능 {inv:.1f}"
            )

        if dte is not None and _safe_float(dte, -1) >= 0 and _safe_float(dte, 999) <= 7:
            text += f" | D-{int(_safe_float(dte, 0))}"

    text += f"\n\n{LINE}\n상위 {len(shown)}개 표시"
    return text


def format_scan_brief(results: list, total: int, top_n: int = 12, style: str = "compact") -> str:
    level = _style_level(style)
    text = header("빠른 스캔", "🔎") + "\n"
    text += f"분석 {total}개"

    if not results:
        return text + "\n\n데이터 없음"

    ranked = sorted(
        results,
        key=lambda x: -x.get("investability_score", x.get("score", {}).get("total_score", 0)),
    )[: max(1, top_n)]

    for i, r in enumerate(ranked, 1):
        score = r.get("score", {})
        plan = r.get("trade_plan", {})

        rr2 = _safe_float(plan.get("risk_reward", {}).get("rr2", 0))
        stage = _stage_label(str(plan.get("positioning", {}).get("stage", "")))
        inv = _safe_float(r.get("investability_score", 0))
        liq = _safe_float(r.get("liquidity_score", plan.get("liquidity", {}).get("score", 0)))
        pos_pct = _safe_float(plan.get("execution", {}).get("position_pct", r.get("position_size_pct", 0)))

        text += (
            f"\n\n<b>{i}. {r.get('symbol', '-')}</b> {usd(_safe_float(r.get('price', 0)))}"
            f" | 점수 {_safe_float(score.get('total_score', 0)):.0f} | RSI {_safe_float(r.get('rsi', 50)):.0f} | RR2 {rr2:.2f}"
        )

        if level == 0:
            text += f"\n{stage} | 비중 {pos_pct:.1f}%"
        elif level == 1:
            text += f"\n투자가능 {inv:.1f} | 유동성 {liq:.0f} | 비중 {pos_pct:.1f}% | {stage}"
        else:
            qual = _safe_float(r.get("quality_score", 0))
            text += (
                f"\n투자가능 {inv:.1f} | 퀄리티 {qual:.1f} | 유동성 {liq:.0f}"
                f" | 비중 {pos_pct:.1f}% | {stage}"
            )

    return text


# ===== 잔고 =====
def format_balance(balance: dict, style: str = "compact") -> str:
    level = _style_level(style)
    available = _safe_float(balance.get("available_cash", 0))
    holdings = balance.get("holdings", []) or []

    text = header("보유 현황", "💰") + "\n"
    text += f"주문가능 {usd(available)}"

    if not holdings:
        return text + "\n\n보유 종목 없음"

    total_pnl = 0.0
    for h in holdings:
        qty = int(_safe_float(h.get("qty", 0)))
        pnl_pct = _safe_float(h.get("pnl_pct", 0))
        eval_amt = _safe_float(h.get("eval_amt", 0))
        avg_price = _safe_float(h.get("avg_price", 0))
        cur_price = eval_amt / qty if qty > 0 else 0.0

        text += (
            f"\n\n{emoji_pnl(pnl_pct)} <b>{h.get('symbol', '-')}</b> {qty}주"
            f" | {pct(pnl_pct)} | 평가 {usd(eval_amt)}"
        )

        if level >= 1:
            text += f"\n평단 {usd(avg_price)} | 현재 {usd(cur_price)}"

        if level >= 2 and h.get("exit_signal"):
            reason = str(h.get("exit_reason", "")).strip()
            if reason:
                text += f"\n❗ {reason}"

        total_pnl += _safe_float(h.get("pnl", 0))

    text += f"\n\n{LINE}\n총손익 {emoji_pnl(total_pnl)} <b>{usd(total_pnl)}</b>"
    return text


# ===== 미체결 주문 =====
def format_orders(orders: list, style: str = "compact") -> str:
    _ = _style_level(style)
    text = header("미체결 주문", "📋")

    if not orders:
        return text + "\n\n미체결 없음"

    for o in orders:
        side = str(o.get("side", ""))
        emoji = "🟢" if side == "매수" else "🔴"
        text += (
            f"\n\n{emoji} <b>{o.get('symbol', '-')}</b> {side}"
            f" | {o.get('qty', 0)}주 @ {usd(_safe_float(o.get('price', 0)))}"
            f" | 체결 {o.get('filled', 0)}"
        )

    return text


# ===== 관심종목 =====
def format_watchlist(stocks: list, auto_buy: bool, style: str = "compact") -> str:
    level = _style_level(style)
    text = header(f"관심종목 {len(stocks)}개", "👀")

    if not stocks:
        text += "\n\n등록된 종목이 없습니다."
        return text

    for s in stocks:
        if s.get("is_signal"):
            icon = "🟢"
        elif s.get("status") == "bought":
            icon = "💼"
        else:
            icon = "⚪"

        text += (
            f"\n\n{icon} <b>{s.get('symbol', '-')}</b> {usd(_safe_float(s.get('price', 0)))}"
            f" ({pct(_safe_float(s.get('change_pct', 0)))})"
        )

        if level == 0:
            text += f"\nRSI {_safe_float(s.get('rsi', 50)):.0f} | 조건 {int(_safe_float(s.get('met_count', 0)))}/4"
        else:
            text += (
                f"\nRSI {_safe_float(s.get('rsi', 50)):.0f} | BB {_safe_float(s.get('bb_position', 50)):.0f}%"
                f" | 조건 {int(_safe_float(s.get('met_count', 0)))}/4 | 목표 {usd(_safe_float(s.get('target_price', 0)))}"
            )

    text += f"\n\n{LINE}\n자동매수 {'🟢 ON' if auto_buy else '🔴 OFF'}"
    return text


def format_watchlist_signals(signals: list, total: int, style: str = "compact") -> str:
    level = _style_level(style)
    text = header("저점 신호", "🚨") + "\n"
    text += f"스캔 {total}개"

    if not signals:
        text += "\n\n신호 없음"
        if level >= 1:
            text += "\n기준: RSI≤35, BB 하단, 5일선 이격, 3일 하락"
        return text

    text += f"\n\n신호 {len(signals)}개"
    for s in signals:
        text += (
            f"\n\n<b>{s.get('symbol', '-')}</b> {s.get('strength', '보통')}"
            f" | {usd(_safe_float(s.get('price', 0)))} | RSI {_safe_float(s.get('rsi', 50)):.0f}"
            f" | 조건 {int(_safe_float(s.get('met_count', 0)))}/4"
        )

    return text


# ===== 매매 결과 =====
def format_trade_result(action: str, results: list, style: str = "compact") -> str:
    level = _style_level(style)
    text = header(f"자동{action} 결과", "📦")

    success = 0
    fail = 0

    for r in results:
        if r.get("success"):
            success += 1
            text += (
                f"\n\n✅ <b>{r.get('symbol', '?')}</b>"
                f" | {r.get('qty', 0)}주 @ {usd(_safe_float(r.get('price', 0)))}"
            )
            reason = str(r.get("reason", "")).strip()
            if reason and level >= 1:
                text += f"\n{reason[:120]}"
        elif r.get("error"):
            fail += 1
            text += f"\n\n❌ {r.get('symbol', '?')}: {str(r['error'])[:120]}"
        elif r.get("message"):
            text += f"\n\nℹ️ {str(r['message'])[:140]}"

    text += f"\n\n{LINE}\n성공 {success} | 실패 {fail}"
    return text


# ===== API 상태 =====
def format_api_status(status: dict, style: str = "compact") -> str:
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
    text += "\n설정: .env의 KIS_APP_KEY / KIS_APP_SECRET / KIS_ACCOUNT_NO"
    return text


# ===== 공포탐욕 =====
def format_fear_greed(fg: dict, style: str = "compact") -> str:
    level = _style_level(style)
    score = int(_safe_float(fg.get("score", 50), 50))

    text = header("시장 심리", "😱")
    text += f"\n\n{fg.get('emoji', '😐')} <b>{score}</b>/100 ({fg.get('rating', 'N/A')})"

    advice = str(fg.get("advice", "")).strip()
    if advice:
        text += f"\n{advice}"

    if level >= 1:
        text += "\n\n구간: 0-25 공포 | 25-45 약공포 | 45-55 중립 | 55-75 탐욕 | 75-100 과열"

    return text
