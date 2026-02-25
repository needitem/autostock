"""Telegram message formatter module with style levels."""

from __future__ import annotations

from typing import Any

LINE = "-" * 26


def header(title: str, emoji: str = "") -> str:
    prefix = f"{emoji} " if emoji else ""
    return f"{prefix}<b>{title}</b>\n{LINE}"


def section(title: str) -> str:
    return f"\n<b>{title}</b>"


def item(label: str, value: Any, suffix: str = "") -> str:
    return f"- {label}: {value}{suffix}"


def pct(value: float, with_sign: bool = True) -> str:
    if with_sign:
        return f"{value:+.1f}%"
    return f"{value:.1f}%"


def usd(value: float, decimals: int = 2) -> str:
    if decimals == 0:
        return f"${value:,.0f}"
    return f"${value:,.{decimals}f}"


def emoji_pnl(value: float) -> str:
    return "+" if value >= 0 else "-"


def grade_emoji(grade: str) -> str:
    return {"A": "A", "B": "B", "C": "C", "D": "D", "F": "F"}.get(grade, "")


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


def _stage_label(stage: str) -> str:
    mapping = {
        "right_knee": "Early uptrend",
        "mid_trend": "Trend continuation",
        "right_shoulder": "Late trend",
    }
    return mapping.get(stage, stage or "-")


def _decision_text(total_score: float, grade: str, risk_score: float) -> tuple[str, str]:
    if risk_score >= 70:
        return "WAIT", "Risk score is high."
    if grade in {"A", "B"} and total_score >= 68:
        return "BUY (SCALED)", "Trend and score are supportive."
    if grade == "C":
        return "WATCH", "Need more confirmation."
    return "DEFENSIVE", "Weak setup / lower conviction."


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
        buy_high = price if price > 0 else 0

    if buy_high <= 0:
        buy_high = price
    if buy_low <= 0:
        buy_low = buy_high * 0.98 if buy_high > 0 else 0

    stop = buy_low * 0.93 if buy_low > 0 else 0
    target = resistance_v if resistance_v > buy_high else buy_high * 1.08
    return buy_low, buy_high, stop, target


def format_analysis(data: dict, style: str = "beginner") -> str:
    level = _style_level(style)

    symbol = str(data.get("symbol", "-"))
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

        text = header(f"{symbol} Snapshot", "INFO")
        text += f"\n\nPrice: <b>{usd(price)}</b>"
        text += f"\nDecision: <b>{decision}</b>"
        text += f"\nReason: {reason}"
        text += f"\nConfidence: {confidence_score:.0f} | Risk: {risk_score:.0f}"
        text += f"\nScore: {total_score:.0f}/100 ({grade})"
        text += "\n\n<b>Execution Plan</b>"
        text += f"\nBuy zone: {usd(buy_low)} ~ {usd(buy_high)}"
        text += f"\nStop: {usd(stop)}"
        text += f"\nTarget: {usd(target)}"
        if warnings:
            text += f"\nWarning: {warnings[0]}"
        text += "\n\n<i>Use scaled entries (not one-shot buy).</i>"
        return text

    text = header(symbol, "ANALYSIS") + "\n"
    text += f"<b>{usd(price)}</b>\n"
    text += f"{grade_emoji(grade)} Score {total_score:.0f}/100 ({grade}) | Confidence {confidence_score:.0f} | Risk {risk_score:.0f}"
    if recommendation:
        text += f"\nCall: {recommendation}"
    text += (
        f"\nRSI {_safe_float(data.get('rsi', 50)):.0f} | "
        f"BB {_safe_float(data.get('bb_position', 50)):.0f}% | "
        f"52W {_safe_float(data.get('position_52w', 50)):.0f}% | "
        f"MA50 gap {pct(_safe_float(data.get('ma50_gap', 0)))}"
    )

    if level >= 2:
        adx = _safe_float(data.get("adx", 0))
        stoch = _safe_float(data.get("stoch_k", 0))
        vol_ratio = _safe_float(data.get("volume_ratio", 1))
        text += f"\nADX {adx:.0f} | Stoch {stoch:.0f} | Volume ratio {vol_ratio:.2f}x"

    if warnings:
        show_n = 3 if level == 1 else 4
        text += f"\nWarnings: {' / '.join(warnings[:show_n])}"

    return text


def format_recommendations(stocks: list, total: int, style: str = "beginner") -> str:
    level = _style_level(style)
    text = header("Recommendations", "TOP") + "\n"
    text += f"Universe analyzed: {total}\n"

    if not stocks:
        text += "\nNo candidates passed current filters."
        return text

    shown = stocks[:10]
    for i, row in enumerate(shown, 1):
        score = row.get("score", {})
        plan = row.get("trade_plan", {})

        symbol = str(row.get("symbol", "-"))
        price = _safe_float(row.get("price", 0))
        total_score = _safe_float(score.get("total_score", row.get("investability_score", 0)))
        grade = str(score.get("grade", "C"))
        tradeable = bool(plan.get("tradeable", False))
        stage = _stage_label(str(plan.get("positioning", {}).get("stage", "")))
        rr2 = _safe_float(plan.get("risk_reward", {}).get("rr2", 0))

        text += f"\n\n<b>{i}. {symbol}</b> {usd(price)}"
        text += f"\nScore {total_score:.0f} ({grade}) | Stage {stage} | RR2 {rr2:.2f}"

        if level == 0:
            text += "\nAction: BUY candidate" if tradeable else "\nAction: WAIT"
        else:
            entry = _safe_float(plan.get("entry", {}).get("buy2", 0))
            stop = _safe_float(plan.get("stop_loss", 0))
            target2 = _safe_float(plan.get("targets", {}).get("target2", 0))
            if entry > 0:
                text += f"\nEntry {usd(entry)}"
            if stop > 0:
                text += f" | Stop {usd(stop)}"
            if target2 > 0:
                text += f" | Target2 {usd(target2)}"

    text += f"\n\n{LINE}\nShown: {len(shown)}"
    return text


def format_scan_brief(results: list, total: int, top_n: int = 12, style: str = "beginner") -> str:
    level = _style_level(style)
    ranked = sorted(
        results,
        key=lambda x: -_safe_float(
            x.get("investability_score", x.get("quality_score", (x.get("score", {}) or {}).get("total_score", 0)))
        ),
    )

    text = header("Market Scan", "SCAN")
    text += f"\nTotal analyzed: {total}"

    if not ranked:
        return text + "\n\nNo scan results."

    for i, row in enumerate(ranked[:top_n], 1):
        score = row.get("score", {})
        total_score = _safe_float(score.get("total_score", row.get("investability_score", 0)))
        grade = str(score.get("grade", "C"))
        symbol = str(row.get("symbol", "-"))
        price = _safe_float(row.get("price", 0))
        rsi = _safe_float(row.get("rsi", 50))
        rs63 = _safe_float(row.get("relative_strength_63d", 0))
        text += f"\n\n<b>{i}. {symbol}</b> {usd(price)}"
        text += f"\nScore {total_score:.0f} ({grade}) | RSI {rsi:.0f} | RS63 {rs63:+.1f}%"
        if level >= 2:
            vol_ratio = _safe_float(row.get("volume_ratio", 1.0))
            bb = _safe_float(row.get("bb_position", 50))
            text += f"\nBB {bb:.0f}% | Volume {vol_ratio:.2f}x"

    return text


def format_balance(balance: dict, style: str = "beginner") -> str:
    level = _style_level(style)
    holdings = balance.get("holdings", []) or []
    available_cash = _safe_float(balance.get("available_cash", 0))
    total_eval = _safe_float(balance.get("total_eval", 0))

    text = header("Portfolio Balance", "BAL")
    text += f"\nCash: {usd(available_cash)}"
    text += f"\nTotal Eval: {usd(total_eval)}"
    text += f"\nHoldings: {len(holdings)}"

    if not holdings:
        return text + "\n\nNo holdings."

    for row in holdings[:15]:
        symbol = str(row.get("symbol", "-"))
        qty = _safe_float(row.get("qty", 0))
        current = _safe_float(row.get("current_price", row.get("price", 0)))
        pnl_pct = _safe_float(row.get("pnl_pct", 0))
        pnl = _safe_float(row.get("pnl", 0))
        text += f"\n\n<b>{symbol}</b> {qty:.0f} sh @ {usd(current)}"
        text += f"\nPnL {emoji_pnl(pnl)} {usd(pnl)} ({pct(pnl_pct)})"
        if level >= 2:
            avg = _safe_float(row.get("avg_price", 0))
            text += f"\nAvg {usd(avg)}"

    return text


def format_orders(orders: list, style: str = "beginner") -> str:
    text = header("Open Orders", "ORD")
    if not orders:
        return text + "\n\nNo open orders."

    for row in orders[:20]:
        symbol = str(row.get("symbol") or row.get("pdno") or "-")
        side = str(row.get("side") or row.get("sll_buy_dvsn_cd") or "-")
        qty = row.get("qty") or row.get("ord_qty") or row.get("order_qty") or "-"
        price = row.get("price") or row.get("ord_unpr") or row.get("order_price") or "-"
        status = row.get("status") or row.get("ord_stts") or row.get("order_status") or "-"
        text += f"\n\n<b>{symbol}</b> {side}"
        text += f"\nQty {qty} | Price {price} | Status {status}"

    return text


def format_api_status(status: dict, style: str = "beginner") -> str:
    connected = bool(status.get("connected", False))
    text = header("API Status", "API")
    text += f"\nConnected: {'YES' if connected else 'NO'}"

    if connected:
        text += f"\nMode: {status.get('mode', '-') }"
        text += f"\nPaper: {status.get('is_paper', '-') }"
        text += f"\nAccount: {status.get('account', '-') }"
    else:
        text += f"\nError: {status.get('error', '-') }"

    return text


def format_fear_greed(fg: dict, style: str = "beginner") -> str:
    score = _safe_float(fg.get("score", 0))
    rating = str(fg.get("rating", "N/A"))
    text = header("Fear & Greed", "FG")
    text += f"\nScore: <b>{score:.0f}</b>/100"
    text += f"\nRating: {rating}"

    if score <= 25:
        text += "\nContext: Extreme fear (high volatility regime)."
    elif score <= 45:
        text += "\nContext: Fear (risk-sensitive)."
    elif score < 55:
        text += "\nContext: Neutral."
    elif score < 75:
        text += "\nContext: Greed (risk-on bias)."
    else:
        text += "\nContext: Extreme greed (overheat risk)."

    return text
