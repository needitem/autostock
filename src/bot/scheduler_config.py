"""Scheduler and notification formatting helpers for Telegram bot."""

from __future__ import annotations

import html
import os
from typing import Any

DEFAULT_BOT_TIMEZONE = "Asia/Seoul"
DEFAULT_US_REPORT_TIME = "00:00"
DEFAULT_US_REBALANCE_TIME = "00:10"
DEFAULT_US_REBALANCE_WEEKDAY = 0  # Monday (Python weekday convention)
DEFAULT_INVENTORY_REPORT_TIME = "00:20"


def parse_bool(raw: str | None, fallback: bool = False) -> bool:
    value = (raw or "").strip().lower()
    if not value:
        return fallback
    return value in {"1", "true", "yes", "on", "y"}


def parse_hhmm(raw: str | None, fallback: tuple[int, int]) -> tuple[int, int]:
    value = (raw or "").strip()
    if not value:
        return fallback
    parts = value.split(":")
    if len(parts) != 2:
        return fallback
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except Exception:
        return fallback
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return fallback
    return hour, minute


def parse_weekday(raw: str | None, fallback: int = DEFAULT_US_REBALANCE_WEEKDAY) -> int:
    value = (raw or "").strip()
    if not value:
        return fallback
    try:
        day = int(value)
    except Exception:
        return fallback
    if day < 0:
        return 0
    if day > 6:
        return 6
    return day


def schedule_settings() -> dict[str, int | str]:
    tz_name = (os.getenv("BOT_TIMEZONE") or DEFAULT_BOT_TIMEZONE).strip() or DEFAULT_BOT_TIMEZONE
    report_hour, report_minute = parse_hhmm(os.getenv("US_REPORT_TIME"), parse_hhmm(DEFAULT_US_REPORT_TIME, (0, 0)))
    rebalance_hour, rebalance_minute = parse_hhmm(
        os.getenv("US_REBALANCE_TIME"),
        parse_hhmm(DEFAULT_US_REBALANCE_TIME, (0, 10)),
    )
    inventory_hour, inventory_minute = parse_hhmm(
        os.getenv("INVENTORY_REPORT_TIME"),
        parse_hhmm(DEFAULT_INVENTORY_REPORT_TIME, (0, 20)),
    )
    rebalance_weekday = parse_weekday(os.getenv("US_REBALANCE_WEEKDAY"), DEFAULT_US_REBALANCE_WEEKDAY)
    inventory_enabled = parse_bool(os.getenv("INVENTORY_MODE_ENABLED"), True)
    return {
        "timezone": tz_name,
        "report_hour": report_hour,
        "report_minute": report_minute,
        "rebalance_hour": rebalance_hour,
        "rebalance_minute": rebalance_minute,
        "rebalance_weekday": rebalance_weekday,
        "inventory_enabled": inventory_enabled,
        "inventory_report_hour": inventory_hour,
        "inventory_report_minute": inventory_minute,
    }


def format_us_report_message(result: dict[str, Any]) -> str:
    report_path = str(result.get("report_path", "") or "")
    lines = ["US daily report saved."]
    if report_path:
        lines.append(f"report_json: {report_path}")
    return "\n".join(lines)


def format_us_rebalance_message(result: dict[str, Any]) -> str:
    result_json = str(result.get("result_json", "") or "")
    orders_csv = str(result.get("orders_csv", "") or "")
    report_path = ""
    payload = result.get("result")
    if isinstance(payload, dict):
        report_path = str(payload.get("report_path", "") or "")

    lines = ["US weekly rebalance saved."]
    if report_path:
        lines.append(f"source_report_json: {report_path}")
    if result_json:
        lines.append(f"rebalance_json: {result_json}")
    if orders_csv:
        lines.append(f"orders_csv: {orders_csv}")
    return "\n".join(lines)


def _metric_from_summary_or_verify(summary: dict[str, Any], verification: dict[str, Any], bucket: str) -> dict[str, Any]:
    verify_metrics = verification.get("metrics") if isinstance(verification.get("metrics"), dict) else {}
    if isinstance(verify_metrics.get(bucket), dict):
        return verify_metrics[bucket]
    summary_metrics = summary.get("portfolio_metrics") if isinstance(summary.get("portfolio_metrics"), dict) else {}
    if isinstance(summary_metrics.get(bucket), dict):
        return summary_metrics[bucket]
    return {}


def format_strategy_v2_snapshot(
    summary: dict[str, Any],
    verification: dict[str, Any] | None = None,
    summary_path: str | None = None,
    verification_path: str | None = None,
) -> str:
    verification = verification if isinstance(verification, dict) else {}
    summary = summary if isinstance(summary, dict) else {}
    ai = _metric_from_summary_or_verify(summary, verification, "ai_portfolio")
    qqq = _metric_from_summary_or_verify(summary, verification, "benchmark")
    alpha = verification.get("alpha") if isinstance(verification.get("alpha"), dict) else {}
    turnover = verification.get("turnover") if isinstance(verification.get("turnover"), dict) else {}
    turnover_ai = turnover.get("ai") if isinstance(turnover.get("ai"), dict) else {}

    ai_cagr = float(ai.get("cagr_pct", 0.0) or 0.0)
    qqq_cagr = float(qqq.get("cagr_pct", 0.0) or 0.0)
    ai_mdd = float(ai.get("max_drawdown_pct", 0.0) or 0.0)
    qqq_mdd = float(qqq.get("max_drawdown_pct", 0.0) or 0.0)

    lines = [
        "<b>Strategy V2 Baseline</b>",
        "--------------------------",
        f"Run: <code>{html.escape(str(summary.get('run_tag', '-') or '-'))}</code>",
        (
            f"Window: <b>{html.escape(str(summary.get('start_date', '-') or '-'))}</b> to "
            f"<b>{html.escape(str(summary.get('end_date', '-') or '-'))}</b> "
            f"({html.escape(str(summary.get('snapshot_freq', '-') or '-'))})"
        ),
        f"Engine: <b>{html.escape(str(summary.get('decision_engine', '-') or '-'))}</b>",
        f"CAGR: <b>{ai_cagr:.2f}%</b> vs QQQ <b>{qqq_cagr:.2f}%</b>",
        (
            f"Sharpe: <b>{float(ai.get('sharpe', 0.0) or 0.0):.2f}</b> vs "
            f"QQQ <b>{float(qqq.get('sharpe', 0.0) or 0.0):.2f}</b>"
        ),
        f"MDD: <b>{ai_mdd:.2f}%</b> vs QQQ <b>{qqq_mdd:.2f}%</b>",
        f"Alpha CAGR diff: <b>{ai_cagr - qqq_cagr:.2f}%p</b>",
    ]

    if alpha:
        lines.append(
            "Validation: "
            f"p(two-sided)=<b>{float(alpha.get('nw_p_two_sided', 1.0) or 1.0):.3f}</b> | "
            f"P(alpha&gt;0)=<b>{float(alpha.get('nw_p_gt0', 0.5) or 0.5):.3f}</b>"
        )
    if turnover_ai:
        lines.append(f"Avg turnover: <b>{float(turnover_ai.get('mean', 0.0) or 0.0):.3f}</b>")

    if summary_path:
        lines.append(f"summary_json: <code>{html.escape(str(summary_path))}</code>")
    if verification_path:
        lines.append(f"verification_json: <code>{html.escape(str(verification_path))}</code>")

    return "\n".join(lines)


def format_strategy_v2_message(result: dict[str, Any]) -> str:
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    verification = result.get("verification") if isinstance(result.get("verification"), dict) else {}
    return format_strategy_v2_snapshot(
        summary,
        verification,
        summary_path=str(result.get("summary_path", "") or ""),
        verification_path=str(result.get("verification_json_path", "") or ""),
    )


def format_inventory_report_message(result: dict[str, Any]) -> str:
    report_path = str(result.get("report_path", "") or "")
    md_path = str(result.get("md_path", "") or "")
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    movement_count = int(summary.get("movement_count", 0) or 0)
    balance_count = int(summary.get("balance_count", 0) or 0)
    low_stock_count = int(summary.get("low_stock_count", 0) or 0)
    mismatch_count = int(summary.get("mismatch_count", 0) or 0)

    lines = [
        "Inventory report saved.",
        f"movements: {movement_count}",
        f"balances: {balance_count}",
        f"low_stock_candidates: {low_stock_count}",
        f"reconcile_mismatches: {mismatch_count}",
    ]
    if report_path:
        lines.append(f"report_json: {report_path}")
    if md_path:
        lines.append(f"report_md: {md_path}")
    return "\n".join(lines)


def _positive_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if out <= 0:
        return None
    return out


def _price_map(payload: dict[str, Any]) -> dict[str, float]:
    prices: dict[str, float] = {}

    plans = payload.get("execution_plans")
    if isinstance(plans, dict):
        for sym, plan in plans.items():
            symbol = str(sym or "").strip().upper()
            if not symbol or not isinstance(plan, dict):
                continue
            anchors = plan.get("anchors")
            raw_price = anchors.get("price") if isinstance(anchors, dict) else plan.get("price")
            price = _positive_float(raw_price)
            if price is not None:
                prices[symbol] = price

    candidates = payload.get("candidates")
    if isinstance(candidates, list):
        for row in candidates:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol", "") or "").strip().upper()
            if not symbol or symbol in prices:
                continue
            price = _positive_float(row.get("price"))
            if price is not None:
                prices[symbol] = price

    return prices


def format_rebalance_snapshot(payload: dict[str, Any], src_path: str | None = None) -> str:
    generated_at = str(payload.get("generated_at", "-") or "-")
    risk = payload.get("risk_on_off", {}) if isinstance(payload.get("risk_on_off"), dict) else {}
    label = str(risk.get("label", "neutral") or "neutral")
    score = risk.get("score")
    score_txt = "-"
    try:
        score_txt = f"{float(score):.2f}"
    except Exception:
        pass

    desired_exposure = float(payload.get("desired_exposure_pct", 0.0) or 0.0)
    achieved_exposure = float(
        payload.get("achieved_exposure_after_execution_pct", payload.get("achieved_exposure_pct", 0.0)) or 0.0
    )
    cash_pct = float(payload.get("executed_cash_pct", payload.get("cash_pct", 0.0)) or 0.0)

    weights = payload.get("executed_weights_pct")
    if not isinstance(weights, dict):
        weights = payload.get("weights_pct", {})
    if not isinstance(weights, dict):
        weights = {}

    top_rows = sorted(
        ((str(sym), float(w)) for sym, w in weights.items() if str(sym) and float(w) > 0),
        key=lambda x: x[1],
        reverse=True,
    )[:5]
    prices = _price_map(payload)

    lines = [
        "<b>Latest Rebalance Snapshot</b>",
        "--------------------------",
        f"Generated: <code>{html.escape(generated_at)}</code>",
        f"Risk regime: <b>{html.escape(label)}</b> (score {html.escape(score_txt)})",
        f"Exposure target: <b>{desired_exposure:.2f}%</b>",
        f"Exposure achieved: <b>{achieved_exposure:.2f}%</b>",
        f"Cash: <b>{cash_pct:.2f}%</b>",
    ]

    if top_rows:
        lines.append("\nTop positions:")
        for sym, w in top_rows:
            p = prices.get(sym.upper())
            if p is not None:
                lines.append(f"- <b>{html.escape(sym)}</b>: {w:.2f}% @ <b>${p:,.2f}</b>")
            else:
                lines.append(f"- <b>{html.escape(sym)}</b>: {w:.2f}%")

    if src_path is not None:
        lines.append(f"\nJSON: <code>{html.escape(str(src_path))}</code>")

    return "\n".join(lines)
