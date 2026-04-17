from __future__ import annotations

import json
from pathlib import Path

from event_runtime.models import RuntimeState


def load_runtime_state(path: str | Path, profile: str) -> RuntimeState:
    file_path = Path(path)
    if not file_path.exists():
        return RuntimeState(profile=profile)
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return RuntimeState(profile=profile)
    if not isinstance(payload, dict):
        return RuntimeState(profile=profile)
    return RuntimeState(
        profile=str(payload.get("profile") or profile),
        last_run_at=payload.get("last_run_at"),
        last_actions={
            str(key): str(value)
            for key, value in (payload.get("last_actions") or {}).items()
            if str(key).strip()
        },
        seen_event_keys=[
            str(item)
            for item in (payload.get("seen_event_keys") or [])
            if str(item).strip()
        ],
    )


def save_runtime_state(path: str | Path, state: RuntimeState) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(
        json.dumps(state.to_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

