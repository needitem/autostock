from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def load_signal_snapshot(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_tsla_bars_csv(path: str | Path) -> pd.DataFrame:
    file_path = Path(path)
    if not file_path.exists():
        return pd.DataFrame(columns=["ts_event", "open", "high", "low", "close", "volume"])
    df = pd.read_csv(file_path)
    if "ts_event" in df.columns:
        df["ts_event"] = pd.to_numeric(df["ts_event"], errors="coerce").astype("Int64")
    return df
