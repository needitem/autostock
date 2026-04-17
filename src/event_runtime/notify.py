from __future__ import annotations

import json
from pathlib import Path

from event_runtime.models import RuntimeNotification


def append_notifications(path: str | Path, notifications: list[RuntimeNotification]) -> None:
    if not notifications:
        return
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("a", encoding="utf-8") as fh:
        for item in notifications:
            fh.write(json.dumps(item.to_dict(), ensure_ascii=False) + "\n")

