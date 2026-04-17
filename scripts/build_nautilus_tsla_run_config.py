from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from event_profile import load_event_profile
from nautilus_v2.config_builders import build_symbol_backtest_stub

OUTPUT_ROOT = ROOT / "data" / "nautilus_v2"


def main() -> None:
    profile = load_event_profile()
    date_tag = datetime.now().strftime("%Y-%m-%d")
    input_dir = OUTPUT_ROOT / str(profile.get("name", profile.get("primary_symbol", "profile").lower())) / date_tag
    symbol = str(profile.get("primary_symbol", "TSLA")).lower()
    output_path = input_dir / f"{symbol}_nautilus_run_config.json"
    payload = build_symbol_backtest_stub(input_dir, profile)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"run_config: {output_path}")


if __name__ == "__main__":
    main()
