from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from nautilus_v2.models import NautilusEnvelope, SignalSnapshot, to_timestamp_ns


def _s(value: Any) -> str:
    return str(value or "").strip()


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _now_iso() -> str:
    ts = pd.Timestamp.utcnow()
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat()


def _safe_iso(value: str | None, fallback: str) -> str:
    raw = _s(value)
    if not raw:
        return fallback
    ts = pd.Timestamp(raw)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat()


def _news_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = dict(event)
    payload.pop("symbol", None)
    payload.pop("published_at", None)
    return payload


def build_news_events(payload: dict[str, Any], symbol: str = "TSLA") -> list[NautilusEnvelope]:
    recommendations = payload.get("recommendations", []) if isinstance(payload, dict) else []
    row = next(
        (
            item for item in recommendations
            if isinstance(item, dict) and _s(item.get("symbol")).upper() == _s(symbol).upper()
        ),
        None,
    )
    if not isinstance(row, dict):
        return []
    generated_at = _safe_iso(_s(payload.get("generated_at")), _now_iso())
    raw_events = row.get("raw_events", [])
    out: list[NautilusEnvelope] = []
    for event in raw_events if isinstance(raw_events, list) else []:
        if not isinstance(event, dict):
            continue
        ts_event = _safe_iso(_s(event.get("published_at")), generated_at)
        out.append(
            NautilusEnvelope(
                schema="autostock.nautilus_v2.news.v1",
                event_type="news",
                symbol=_s(symbol).upper(),
                ts_event=ts_event,
                ts_event_ns=to_timestamp_ns(ts_event),
                payload=_news_payload(event),
            )
        )
    return out


def build_macro_events(payload: dict[str, Any], symbol: str = "TSLA") -> list[NautilusEnvelope]:
    macro = payload.get("macro_overlay", {}) if isinstance(payload, dict) else {}
    market_ctx = payload.get("market_ctx", {}) if isinstance(payload, dict) else {}
    fear_greed = payload.get("fear_greed", {}) if isinstance(payload, dict) else {}
    generated_at = _safe_iso(_s(payload.get("generated_at")), _now_iso())

    event_payload = {
        "macro_mode": _s(macro.get("mode")),
        "macro_reason": _s(macro.get("reason")),
        "position_scale": _f(macro.get("position_scale"), 0.0),
        "allow_new_longs": bool(macro.get("allow_new_longs", False)),
        "fear_greed_score": int(_f(fear_greed.get("score"), 50.0)),
        "market_status": _s(market_ctx.get("status")),
        "benchmark": _s(market_ctx.get("benchmark")),
        "benchmark_return_21d": _f(market_ctx.get("benchmark_return_21d"), 0.0),
        "benchmark_return_63d": _f(market_ctx.get("benchmark_return_63d"), 0.0),
        "market_events": macro.get("market_events", []),
    }
    return [
        NautilusEnvelope(
            schema="autostock.nautilus_v2.macro.v1",
            event_type="macro",
            symbol=_s(symbol).upper(),
            ts_event=generated_at,
            ts_event_ns=to_timestamp_ns(generated_at),
            payload=event_payload,
        )
    ]


def build_signal_snapshot(payload: dict[str, Any], symbol: str = "TSLA") -> SignalSnapshot | None:
    recommendations = payload.get("recommendations", []) if isinstance(payload, dict) else []
    row = next(
        (
            item for item in recommendations
            if isinstance(item, dict) and _s(item.get("symbol")).upper() == _s(symbol).upper()
        ),
        None,
    )
    if not isinstance(row, dict):
        return None
    chart_gate = row.get("chart_gate", {}) if isinstance(row.get("chart_gate"), dict) else {}
    return SignalSnapshot(
        symbol=_s(symbol).upper(),
        generated_at=_safe_iso(_s(payload.get("generated_at")), _now_iso()),
        action=_s(row.get("action")),
        confidence=round(_f(row.get("confidence"), 0.0), 4),
        event_signal=_s(row.get("event_signal")) or "neutral",
        event_strength=_s(row.get("event_strength")) or "none",
        chart_state=_s(chart_gate.get("state")),
        volume_ratio=round(_f(chart_gate.get("volume_ratio"), 0.0), 4),
        macro_mode=_s(row.get("macro_mode")),
        price=round(_f(row.get("price"), 0.0), 4),
        rationale=_s(row.get("rationale")),
        reason_lines=[str(item) for item in row.get("reason_lines", [])[:5]] if isinstance(row.get("reason_lines"), list) else [],
    )


def build_bars_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts_event", "open", "high", "low", "close", "volume"])
    frame = df.copy()
    if frame.index.tz is None:
        frame.index = frame.index.tz_localize("UTC")
    else:
        frame.index = frame.index.tz_convert("UTC")
    out = pd.DataFrame(
        {
            "ts_event": frame.index.astype("int64"),
            "open": frame["Open"].astype(float),
            "high": frame["High"].astype(float),
            "low": frame["Low"].astype(float),
            "close": frame["Close"].astype(float),
            "volume": frame["Volume"].astype(float),
        }
    )
    return out.reset_index(drop=True)


def export_tsla_bundle(
    *,
    payload: dict[str, Any],
    bars_df: pd.DataFrame,
    output_dir: str | Path,
    symbol: str = "TSLA",
) -> dict[str, str]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    news_events = build_news_events(payload, symbol=symbol)
    macro_events = build_macro_events(payload, symbol=symbol)
    signal_snapshot = build_signal_snapshot(payload, symbol=symbol)
    bars = build_bars_frame(bars_df)

    news_path = out_dir / f"{_s(symbol).lower()}_news_events.jsonl"
    macro_path = out_dir / f"{_s(symbol).lower()}_macro_events.jsonl"
    signal_path = out_dir / f"{_s(symbol).lower()}_signal_snapshot.json"
    bars_path = out_dir / f"{_s(symbol).lower()}_bars.csv"

    news_path.write_text(
        "\n".join(json.dumps(row.to_dict(), ensure_ascii=False) for row in news_events) + ("\n" if news_events else ""),
        encoding="utf-8",
    )
    macro_path.write_text(
        "\n".join(json.dumps(row.to_dict(), ensure_ascii=False) for row in macro_events) + ("\n" if macro_events else ""),
        encoding="utf-8",
    )
    signal_path.write_text(
        json.dumps(signal_snapshot.to_dict() if signal_snapshot else {}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    bars.to_csv(bars_path, index=False)

    return {
        "news_events": str(news_path),
        "macro_events": str(macro_path),
        "signal_snapshot": str(signal_path),
        "bars_csv": str(bars_path),
    }


def export_symbol_bundle(
    *,
    payload: dict[str, Any],
    bars_df: pd.DataFrame,
    output_dir: str | Path,
    symbol: str,
) -> dict[str, str]:
    return export_tsla_bundle(payload=payload, bars_df=bars_df, output_dir=output_dir, symbol=symbol)
