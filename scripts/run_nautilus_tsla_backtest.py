from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from event_profile import load_event_profile
from nautilus_v2.backtest import import_tsla_bundle_to_catalog
from nautilus_v2.backtest import run_tsla_backtest_in_memory


DATA_ROOT = ROOT / "data" / "nautilus_v2"


def _latest_bundle_dir(profile_name: str) -> Path:
    base_dir = DATA_ROOT / profile_name
    candidates = [p for p in base_dir.glob("*") if p.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No data/nautilus_v2/{profile_name}/<date> bundle directories found")
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def main() -> None:
    print(f"[{datetime.now()}] run nautilus tsla backtest started...")
    profile = load_event_profile()
    profile_name = str(profile.get("name", profile.get("primary_symbol", "profile").lower()))
    bundle_dir = _latest_bundle_dir(profile_name)
    catalog_dir = bundle_dir / "catalog"
    import_info = import_tsla_bundle_to_catalog(bundle_dir, catalog_dir)
    summary = run_tsla_backtest_in_memory(bundle_dir)
    symbol = str(profile.get("primary_symbol", "TSLA")).lower()
    out_path = bundle_dir / f"{symbol}_nautilus_backtest_summary.json"
    payload = {
        "profile": profile_name,
        "bundle_dir": str(bundle_dir),
        "catalog_import": import_info,
        "backtest_summary": summary,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"bundle_dir: {bundle_dir}")
    print(f"catalog_path: {catalog_dir}")
    print(f"summary_json: {out_path}")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
