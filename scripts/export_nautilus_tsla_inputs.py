from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from event_profile import load_event_profile
from core.stock_data import get_stock_data
from nautilus_v2.bridge import export_symbol_bundle
from pipelines.autostock_v2_pipeline import run_autostock_v2

OUTPUT_ROOT = ROOT / "data" / "nautilus_v2"


def main() -> None:
    print(f"[{datetime.now()}] export nautilus tsla inputs started...")
    profile = load_event_profile()
    symbol = str(profile.get("primary_symbol", "TSLA")).upper()
    rss_raw = str(os.getenv("AI_V2_RSS_URLS", "") or "")
    rss_urls = [part.strip() for part in rss_raw.replace("|", ",").split(",") if part.strip()] or list(profile.get("rss_urls", []))
    result = run_autostock_v2(
        profile=profile,
        watchlist_override=list(profile.get("symbols", [symbol])),
        event_feed_path=(str(os.getenv("AI_V2_EVENT_FILE", "") or "").strip() or profile.get("event_file") or None),
        rss_urls=rss_urls or None,
    )
    payload = result.get("payload", {}) if isinstance(result, dict) else {}
    bars_df = get_stock_data(symbol, period=str(os.getenv("AI_V2_TSLA_BARS_PERIOD", "15mo") or "15mo"), auto_adjust=False)
    date_tag = datetime.now().strftime("%Y-%m-%d")
    output_dir = OUTPUT_ROOT / str(profile.get("name", symbol.lower())) / date_tag
    paths = export_symbol_bundle(
        payload=payload if isinstance(payload, dict) else {},
        bars_df=bars_df,
        output_dir=output_dir,
        symbol=symbol,
    )
    print(f"profile: {profile.get('name')}")
    print(f"autostock_v2_json: {result.get('report_path')}")
    print(f"autostock_v2_md: {result.get('md_path')}")
    for key, value in paths.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
