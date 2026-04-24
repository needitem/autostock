from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

import pandas as pd

from core.stock_data import get_stock_data


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "outputs" / "telegram"
JOURNAL_PATH = OUTPUT_ROOT / "shadow_journal.jsonl"
EVAL_PATH = OUTPUT_ROOT / "shadow_journal_eval.json"
SCHEMA_VERSION = "shadow-journal-v1"


def _s(value: Any) -> str:
    return str(value or "").strip()


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _pct(current: float, reference: float) -> float | None:
    if current <= 0 or reference <= 0:
        return None
    return round((current / reference - 1.0) * 100.0, 2)


def _parse_date(value: Any) -> date | None:
    raw = _s(value)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except Exception:
        pass
    try:
        return date.fromisoformat(raw[:10])
    except Exception:
        return None


def _today_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _journal_horizon_days() -> int:
    try:
        return max(1, int(os.getenv("TELEGRAM_JOURNAL_HORIZON_DAYS", "10")))
    except Exception:
        return 10


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _stable_run_id(mode: str, payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    symbols = [
        _s(row.get("symbol")).upper()
        for key in ("actionableNow", "waitPullback")
        for row in (payload.get(key) or [])[:12]
        if isinstance(row, dict)
    ]
    basis = {
        "mode": mode,
        "generatedAt": _s(payload.get("generatedAt")),
        "schemaVersion": _s(payload.get("schemaVersion")),
        "newsAnalysisLimit": payload.get("newsAnalysisLimit"),
        "universeScannedCount": payload.get("universeScannedCount"),
        "summary": summary,
        "symbols": symbols,
    }
    digest = hashlib.sha1(json.dumps(basis, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    return digest[:16]


def _recommendation_rows(payload: dict[str, Any], max_wait: int = 10) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = []
    for row in payload.get("actionableNow") or []:
        if isinstance(row, dict):
            out.append(("actionable_now", row))
    for row in (payload.get("waitPullback") or [])[:max_wait]:
        if isinstance(row, dict):
            out.append(("wait_pullback", row))
    return out


def _condition_flags(row: dict[str, Any]) -> dict[str, bool]:
    warnings = {str(item) for item in (row.get("warnings") or [])}
    news_signal = _s(row.get("newsSignal")).lower()
    return {
        "entry_near_1pct": abs(_f(row.get("currentVsEntryPct"), 999.0)) <= 1.0,
        "entry_near_2pct": abs(_f(row.get("currentVsEntryPct"), 999.0)) <= 2.0,
        "tp1_reward_ge_4pct": _f(row.get("rewardToTp1Pct"), 0.0) >= 4.0,
        "rr1_ge_1": _f(row.get("rrToTp1"), -99.0) >= 1.0,
        "rr1_ge_1_2": _f(row.get("rrToTp1"), -99.0) >= 1.2,
        "rsi_42_70": 42.0 <= _f(row.get("rsi"), 50.0) <= 70.0,
        "volume_ge_0_9": _f(row.get("volumeRatio"), 0.0) >= 0.9,
        "no_severe_warning": not bool({"entry_negative", "overheat_extreme", "overheat_dual"} & warnings),
        "constructive_chart": _s(row.get("chartState")) in {"constructive", "confirmed_breakout"},
        "bullish_news": news_signal == "bullish",
    }


def record_recommendation_run(mode: str, payload: dict[str, Any], *, trigger: str = "telegram") -> dict[str, Any]:
    if not bool(payload.get("available")):
        return {"recorded": 0, "skipped": "payload_unavailable"}

    rows = _recommendation_rows(payload)
    if not rows:
        return {"recorded": 0, "skipped": "no_recommendations"}

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    run_id = _stable_run_id(mode, payload)
    existing_run_ids = {
        _s(row.get("runId"))
        for row in _load_jsonl(JOURNAL_PATH)
        if _s(row.get("eventType")) == "recommendation"
    }
    if run_id in existing_run_ids:
        return {"recorded": 0, "skipped": "already_recorded", "runId": run_id}

    recorded_at = _today_utc()
    records = []
    for recommendation_type, row in rows:
        symbol = _s(row.get("symbol")).upper()
        if not symbol:
            continue
        recommendation_id = f"{run_id}:{recommendation_type}:{symbol}"
        records.append(
            {
                "schemaVersion": SCHEMA_VERSION,
                "eventType": "recommendation",
                "runId": run_id,
                "recommendationId": recommendation_id,
                "recordedAt": recorded_at,
                "trigger": trigger,
                "mode": mode,
                "recommendationType": recommendation_type,
                "symbol": symbol,
                "generatedAt": _s(payload.get("generatedAt")),
                "latestCloseAsOf": _s(row.get("latestCloseAsOf") or payload.get("latestCloseMax")),
                "latestClosePrice": round(_f(row.get("latestClosePrice"), 0.0), 2),
                "averageEntryPrice": round(_f(row.get("averageEntryPrice"), 0.0), 2),
                "closeStopPrice": round(_f(row.get("closeStopPrice"), 0.0), 2),
                "hardStopPrice": round(_f(row.get("hardStopPrice"), 0.0), 2),
                "tp1Price": round(_f(row.get("tp1Price"), 0.0), 2),
                "tp2Price": round(_f(row.get("tp2Price"), 0.0), 2),
                "portfolioWeightPct": round(_f(row.get("portfolioWeightPct"), 0.0), 2),
                "riskToStopPct": row.get("riskToStopPct"),
                "rewardToTp1Pct": row.get("rewardToTp1Pct"),
                "rrToTp1": row.get("rrToTp1"),
                "currentVsEntryPct": row.get("currentVsEntryPct"),
                "chartState": _s(row.get("chartState")),
                "rsi": row.get("rsi"),
                "adx": row.get("adx"),
                "volumeRatio": row.get("volumeRatio"),
                "tradeVerdict": _s(row.get("tradeVerdict")),
                "tradeReason": _s(row.get("tradeReason")),
                "actionBucket": _s(row.get("actionBucket")),
                "actionReason": _s(row.get("actionReason")),
                "newsSignal": _s(row.get("newsSignal")),
                "newsStrength": _s(row.get("newsStrength")),
                "newsHeadline": _s(row.get("newsHeadline")),
                "gateReasons": [str(item) for item in (row.get("chartGateReasons") or [])[:10]]
                if isinstance(row.get("chartGateReasons"), list)
                else [],
                "conditionFlags": _condition_flags(row),
                "warnings": [str(item) for item in (row.get("warnings") or [])[:10]]
                if isinstance(row.get("warnings"), list)
                else [],
            }
        )

    with JOURNAL_PATH.open("a", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    return {"recorded": len(records), "runId": run_id, "path": str(JOURNAL_PATH)}


def _bar_date(index_value: Any) -> date | None:
    try:
        if isinstance(index_value, pd.Timestamp):
            return index_value.date()
        if hasattr(index_value, "date"):
            return index_value.date()
        return date.fromisoformat(str(index_value)[:10])
    except Exception:
        return None


def _evaluate_recommendation(record: dict[str, Any], horizon_days: int) -> dict[str, Any]:
    symbol = _s(record.get("symbol")).upper()
    entry = _f(record.get("averageEntryPrice"), 0.0)
    stop = _f(record.get("closeStopPrice"), 0.0)
    tp1 = _f(record.get("tp1Price"), 0.0)
    if not symbol or entry <= 0 or stop <= 0 or tp1 <= 0:
        return {**record, "evalStatus": "invalid_plan"}

    basis_date = _parse_date(record.get("latestCloseAsOf")) or _parse_date(record.get("generatedAt"))
    if basis_date is None:
        return {**record, "evalStatus": "invalid_date"}

    bars = get_stock_data(symbol, period="6mo", auto_adjust=False)
    if bars is None or bars.empty:
        return {**record, "evalStatus": "no_price_data"}

    future_rows: list[dict[str, Any]] = []
    for idx, bar in bars.iterrows():
        row_date = _bar_date(idx)
        if row_date is None or row_date <= basis_date:
            continue
        future_rows.append(
            {
                "date": row_date.isoformat(),
                "open": _f(bar.get("Open"), 0.0),
                "high": _f(bar.get("High"), 0.0),
                "low": _f(bar.get("Low"), 0.0),
                "close": _f(bar.get("Close"), 0.0),
            }
        )
        if len(future_rows) >= horizon_days:
            break

    if not future_rows:
        return {**record, "evalStatus": "pending_no_future_bars", "barsEvaluated": 0}

    positive_lows = [row["low"] for row in future_rows if row["low"] > 0]
    positive_highs = [row["high"] for row in future_rows if row["high"] > 0]
    if not positive_lows or not positive_highs:
        return {**record, "evalStatus": "no_price_data"}
    lowest = min(positive_lows)
    highest = max(positive_highs)
    last_close = future_rows[-1]["close"]
    fill_row = next((row for row in future_rows if row["low"] <= entry), None)
    if fill_row is None:
        return {
            **record,
            "evalStatus": "unfilled",
            "barsEvaluated": len(future_rows),
            "minLow": round(lowest, 2),
            "maxHigh": round(highest, 2),
            "lastClose": round(last_close, 2),
            "distanceToEntryPct": _pct(lowest, entry),
        }

    after_fill = future_rows[future_rows.index(fill_row) :]
    max_favorable_pct = _pct(max(row["high"] for row in after_fill), entry)
    max_adverse_pct = _pct(min(row["low"] for row in after_fill), entry)
    for row in after_fill:
        hit_stop = row["low"] <= stop
        hit_tp1 = row["high"] >= tp1
        if hit_stop and hit_tp1:
            return {
                **record,
                "evalStatus": "stopped_conservative_same_day",
                "barsEvaluated": len(future_rows),
                "fillDate": fill_row["date"],
                "exitDate": row["date"],
                "fillPrice": entry,
                "exitPrice": stop,
                "realizedPct": _pct(stop, entry),
                "maxFavorablePct": max_favorable_pct,
                "maxAdversePct": max_adverse_pct,
            }
        if hit_stop:
            return {
                **record,
                "evalStatus": "stopped",
                "barsEvaluated": len(future_rows),
                "fillDate": fill_row["date"],
                "exitDate": row["date"],
                "fillPrice": entry,
                "exitPrice": stop,
                "realizedPct": _pct(stop, entry),
                "maxFavorablePct": max_favorable_pct,
                "maxAdversePct": max_adverse_pct,
            }
        if hit_tp1:
            return {
                **record,
                "evalStatus": "tp1_hit",
                "barsEvaluated": len(future_rows),
                "fillDate": fill_row["date"],
                "exitDate": row["date"],
                "fillPrice": entry,
                "exitPrice": tp1,
                "realizedPct": _pct(tp1, entry),
                "maxFavorablePct": max_favorable_pct,
                "maxAdversePct": max_adverse_pct,
            }

    return {
        **record,
        "evalStatus": "filled_open",
        "barsEvaluated": len(future_rows),
        "fillDate": fill_row["date"],
        "fillPrice": entry,
        "lastClose": round(last_close, 2),
        "unrealizedPct": _pct(last_close, entry),
        "maxFavorablePct": max_favorable_pct,
        "maxAdversePct": max_adverse_pct,
    }


def _condition_stats(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = {}
    terminal_statuses = {"tp1_hit", "stopped", "stopped_conservative_same_day", "filled_open"}
    for row in rows:
        flags = row.get("conditionFlags") if isinstance(row.get("conditionFlags"), dict) else _condition_flags(row)
        for key, enabled in flags.items():
            if not enabled:
                continue
            bucket = stats.setdefault(
                str(key),
                {
                    "count": 0,
                    "filled": 0,
                    "tp1": 0,
                    "stopped": 0,
                    "pending": 0,
                    "unfilled": 0,
                },
            )
            status = _s(row.get("evalStatus"))
            bucket["count"] += 1
            if status in terminal_statuses:
                bucket["filled"] += 1
            if status == "tp1_hit":
                bucket["tp1"] += 1
            if status in {"stopped", "stopped_conservative_same_day"}:
                bucket["stopped"] += 1
            if status == "pending_no_future_bars":
                bucket["pending"] += 1
            if status == "unfilled":
                bucket["unfilled"] += 1
    for bucket in stats.values():
        count = max(1, int(bucket["count"]))
        filled = max(1, int(bucket["filled"]))
        bucket["fillRatePct"] = round(bucket["filled"] / count * 100.0, 2)
        bucket["tp1RateFilledPct"] = round(bucket["tp1"] / filled * 100.0, 2) if bucket["filled"] else 0.0
        bucket["stopRateFilledPct"] = round(bucket["stopped"] / filled * 100.0, 2) if bucket["filled"] else 0.0
    return stats


def evaluate_shadow_journal(horizon_days: int | None = None) -> dict[str, Any]:
    horizon_days = _journal_horizon_days() if horizon_days is None else max(1, int(horizon_days))
    records = [
        row
        for row in _load_jsonl(JOURNAL_PATH)
        if _s(row.get("eventType")) == "recommendation" and _s(row.get("schemaVersion")) == SCHEMA_VERSION
    ]
    latest_by_id: dict[str, dict[str, Any]] = {}
    for row in records:
        rid = _s(row.get("recommendationId"))
        if rid:
            latest_by_id[rid] = row
    evaluated = [_evaluate_recommendation(row, horizon_days) for row in latest_by_id.values()]
    status_counts: dict[str, int] = {}
    for row in evaluated:
        status = _s(row.get("evalStatus") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    filled_count = sum(1 for row in evaluated if _s(row.get("evalStatus")) in {"tp1_hit", "stopped", "stopped_conservative_same_day", "filled_open"})
    closed_count = sum(1 for row in evaluated if _s(row.get("evalStatus")) in {"tp1_hit", "stopped", "stopped_conservative_same_day"})
    tp1_count = status_counts.get("tp1_hit", 0)
    stopped_count = status_counts.get("stopped", 0) + status_counts.get("stopped_conservative_same_day", 0)
    payload = {
        "generatedAt": _today_utc(),
        "schemaVersion": SCHEMA_VERSION,
        "journalPath": str(JOURNAL_PATH),
        "horizonDays": horizon_days,
        "summary": {
            "recommendationCount": len(evaluated),
            "filledCount": filled_count,
            "closedCount": closed_count,
            "tp1Count": tp1_count,
            "stoppedCount": stopped_count,
            "unfilledCount": status_counts.get("unfilled", 0),
            "pendingCount": status_counts.get("pending_no_future_bars", 0),
            "fillRatePct": round(filled_count / len(evaluated) * 100.0, 2) if evaluated else 0.0,
            "tp1RateFilledPct": round(tp1_count / filled_count * 100.0, 2) if filled_count else 0.0,
            "stopRateFilledPct": round(stopped_count / filled_count * 100.0, 2) if filled_count else 0.0,
            "statusCounts": status_counts,
        },
        "conditionStats": _condition_stats(evaluated),
        "recent": sorted(evaluated, key=lambda row: _s(row.get("recordedAt")), reverse=True)[:30],
        "all": evaluated,
    }
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    EVAL_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def _format_price(value: Any) -> str:
    if value is None:
        return "-"
    return f"{_f(value):.2f}"


def _format_pct(value: Any) -> str:
    if value is None:
        return "-"
    return f"{_f(value):.2f}%"


def _status_ko(status: str) -> str:
    return {
        "tp1_hit": "TP1 도달",
        "stopped": "손절",
        "stopped_conservative_same_day": "동일일 손절 우선",
        "filled_open": "체결 후 진행중",
        "unfilled": "미체결",
        "pending_no_future_bars": "평가 대기",
        "invalid_plan": "계획 오류",
        "invalid_date": "날짜 오류",
        "no_price_data": "가격 없음",
    }.get(status, status or "-")


def _condition_ko(key: str) -> str:
    return {
        "entry_near_1pct": "진입가 ±1%",
        "entry_near_2pct": "진입가 ±2%",
        "tp1_reward_ge_4pct": "TP1 여력 4%+",
        "rr1_ge_1": "RR1 1.0+",
        "rr1_ge_1_2": "RR1 1.2+",
        "rsi_42_70": "RSI 42~70",
        "volume_ge_0_9": "거래량 0.9x+",
        "no_severe_warning": "심각 경고 없음",
        "constructive_chart": "constructive 차트",
        "bullish_news": "뉴스 bullish",
    }.get(key, key)


def render_journal_html(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    lines = [
        "<b>Shadow Journal</b>",
        f"<code>{escape(_s(payload.get('generatedAt')))}</code>",
        f"평가기간 {payload.get('horizonDays', '-')}거래일 | 기록 {summary.get('recommendationCount', 0)}",
        f"체결률 {_format_pct(summary.get('fillRatePct'))} | TP1/체결 {_format_pct(summary.get('tp1RateFilledPct'))} | 손절/체결 {_format_pct(summary.get('stopRateFilledPct'))}",
        f"TP1 {summary.get('tp1Count', 0)} | 손절 {summary.get('stoppedCount', 0)} | 미체결 {summary.get('unfilledCount', 0)} | 대기 {summary.get('pendingCount', 0)}",
        "",
        "<b>조건별 누적</b>",
    ]
    condition_stats = payload.get("conditionStats") if isinstance(payload.get("conditionStats"), dict) else {}
    populated = [
        (key, row)
        for key, row in condition_stats.items()
        if isinstance(row, dict) and _f(row.get("count"), 0.0) > 0
    ]
    if populated:
        for key, row in sorted(populated, key=lambda item: (-_f(item[1].get("count")), item[0]))[:6]:
            lines.append(
                f"{escape(_condition_ko(key))}: n={int(_f(row.get('count')))} | 체결 {_format_pct(row.get('fillRatePct'))} | TP1/체결 {_format_pct(row.get('tp1RateFilledPct'))} | 손절/체결 {_format_pct(row.get('stopRateFilledPct'))}"
            )
    else:
        lines.append("조건 통계는 기록이 쌓이면 표시됩니다.")
    lines.extend(
        [
            "",
        "<b>최근 기록</b>",
        ]
    )
    recent = payload.get("recent") if isinstance(payload.get("recent"), list) else []
    if not recent:
        lines.append("아직 기록이 없습니다. /chart 또는 /trade를 먼저 실행하세요.")
        return "\n".join(lines)
    for row in recent[:12]:
        status = _status_ko(_s(row.get("evalStatus")))
        symbol = escape(_s(row.get("symbol")))
        rec_type = "즉시" if _s(row.get("recommendationType")) == "actionable_now" else "대기"
        lines.append(
            f"<b>{symbol}</b> {rec_type} | {status} | 진입 {_format_price(row.get('averageEntryPrice'))} | 손절 {_format_price(row.get('closeStopPrice'))} | 1차 {_format_price(row.get('tp1Price'))}"
        )
        if row.get("realizedPct") is not None:
            lines.append(f"실현 {_format_pct(row.get('realizedPct'))} | 체결일 {_s(row.get('fillDate'))} | 종료일 {_s(row.get('exitDate'))}")
        elif row.get("unrealizedPct") is not None:
            lines.append(f"미실현 {_format_pct(row.get('unrealizedPct'))} | 체결일 {_s(row.get('fillDate'))}")
        elif row.get("distanceToEntryPct") is not None:
            lines.append(f"최저가 기준 진입가 대비 {_format_pct(row.get('distanceToEntryPct'))}")
        lines.append("")
    return "\n".join(lines)


__all__ = [
    "EVAL_PATH",
    "JOURNAL_PATH",
    "evaluate_shadow_journal",
    "record_recommendation_run",
    "render_journal_html",
]
