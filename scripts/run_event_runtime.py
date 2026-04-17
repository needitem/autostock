from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from event_runtime.engine import run_runtime_cycle, run_runtime_loop


def _configure_console_output() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream and hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(errors="replace")
            except Exception:
                pass


def main() -> None:
    _configure_console_output()
    print(f"[{datetime.now()}] event runtime started...")
    profile = str(os.getenv("AI_EVENT_PROFILE", "tsla") or "tsla").strip() or "tsla"
    once = str(os.getenv("AI_EVENT_RUNTIME_ONCE", "1")).strip().lower() in {"1", "true", "yes", "on"}
    interval = int(float(str(os.getenv("AI_EVENT_RUNTIME_INTERVAL_SECONDS", "60")).strip() or "60"))
    max_cycles_raw = str(os.getenv("AI_EVENT_RUNTIME_MAX_CYCLES", "")).strip()
    max_cycles = int(max_cycles_raw) if max_cycles_raw else None

    result = (
        run_runtime_cycle(profile_name=profile)
        if once
        else run_runtime_loop(profile_name=profile, interval_seconds=interval, max_cycles=max_cycles)
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
