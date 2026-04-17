from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class RuntimeState:
    profile: str
    last_run_at: str | None = None
    last_actions: dict[str, str] = field(default_factory=dict)
    seen_event_keys: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RuntimeNotification:
    profile: str
    kind: str
    symbol: str
    created_at: str
    title: str
    body: str
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

