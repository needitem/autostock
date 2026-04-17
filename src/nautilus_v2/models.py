from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd


def to_timestamp_ns(value: str | pd.Timestamp) -> int:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.value)


@dataclass(frozen=True)
class NautilusEnvelope:
    schema: str
    event_type: str
    symbol: str
    ts_event: str
    ts_event_ns: int
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SignalSnapshot:
    symbol: str
    generated_at: str
    action: str
    confidence: float
    event_signal: str
    event_strength: str
    chart_state: str
    volume_ratio: float
    macro_mode: str
    price: float
    rationale: str
    reason_lines: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
