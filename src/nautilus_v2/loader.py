from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from nautilus_v2.strategy import TslaMacroEvent, TslaNewsEvent


def _load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    file_path = Path(path)
    if not file_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in file_path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            obj = json.loads(text)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def load_tsla_news_events(path: str | Path) -> list[TslaNewsEvent]:
    out: list[TslaNewsEvent] = []
    for row in _load_jsonl(path):
        payload = row.get("payload", {}) if isinstance(row.get("payload"), dict) else {}
        ts_event = int(row.get("ts_event_ns", 0) or 0)
        kwargs = {
            "headline": str(payload.get("headline", "")),
            "category": str(payload.get("category", "")),
            "sentiment": str(payload.get("sentiment", "neutral")),
            "source": str(payload.get("source", "")),
            "magnitude": float(payload.get("magnitude", 0.0) or 0.0),
        }
        try:
            item = TslaNewsEvent(ts_event=ts_event, ts_init=ts_event, **kwargs)
        except TypeError:
            item = TslaNewsEvent(**kwargs)
            setattr(item, "_ts_event", ts_event)
            setattr(item, "_ts_init", ts_event)
        out.append(item)
    return out


def load_tsla_macro_events(path: str | Path) -> list[TslaMacroEvent]:
    out: list[TslaMacroEvent] = []
    for row in _load_jsonl(path):
        payload = row.get("payload", {}) if isinstance(row.get("payload"), dict) else {}
        ts_event = int(row.get("ts_event_ns", 0) or 0)
        kwargs = {
            "macro_mode": str(payload.get("macro_mode", "neutral")),
            "macro_reason": str(payload.get("macro_reason", "")),
            "position_scale": float(payload.get("position_scale", 1.0) or 1.0),
            "allow_new_longs": bool(payload.get("allow_new_longs", True)),
            "fear_greed_score": int(payload.get("fear_greed_score", 50) or 50),
        }
        try:
            item = TslaMacroEvent(ts_event=ts_event, ts_init=ts_event, **kwargs)
        except TypeError:
            item = TslaMacroEvent(**kwargs)
            setattr(item, "_ts_event", ts_event)
            setattr(item, "_ts_init", ts_event)
        out.append(item)
    return out


def load_tsla_bars_csv(path: str | Path) -> pd.DataFrame:
    file_path = Path(path)
    if not file_path.exists():
        return pd.DataFrame(columns=["ts_event", "open", "high", "low", "close", "volume"])
    df = pd.read_csv(file_path)
    if "ts_event" in df.columns:
        df["ts_event"] = pd.to_numeric(df["ts_event"], errors="coerce").astype("Int64")
    return df
