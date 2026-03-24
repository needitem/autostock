from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent


DEFAULT_ENV: dict[str, str] = {
    "CCP_CHAMPION": "weekly_baseline_v4",
    "CCP_FIXED_OOS_START_YEAR": "2016",
    "CCP_RUN_VERIFY": "1",
    "HYP_START_DATE": "2011-03-01",
    "HYP_END_DATE": "2026-03-01",
    "HYP_OOS_START_YEAR": "2016",
    "WF_START_DATE": "2006-03-01",
    "WF_END_DATE": "2026-03-11",
    "WF_TRAIN_YEARS": "5",
    "WF_TEST_YEARS": "1",
    "WF_MIN_TEST_WEEKS": "40",
    "WF_TRADE_COST_BPS": "20",
}


def _apply_defaults() -> None:
    for key, value in DEFAULT_ENV.items():
        os.environ[key] = value
    os.environ.setdefault(
        "CCP_RUN_TAG",
        f"champion_challenger_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
    )


def main() -> None:
    _apply_defaults()
    sys.path.insert(0, str(ROOT_DIR / "src"))

    from pipelines.champion_challenger_pipeline import run_champion_challenger_pipeline

    print("Running champion/challenger validation pipeline...")
    for key in (
        "CCP_RUN_TAG",
        "CCP_CHAMPION",
        "CCP_FIXED_OOS_START_YEAR",
        "HYP_START_DATE",
        "HYP_END_DATE",
        "WF_START_DATE",
        "WF_END_DATE",
        "WF_TRAIN_YEARS",
        "WF_TEST_YEARS",
    ):
        print(f"  {key}={os.environ.get(key, '')}")

    out = run_champion_challenger_pipeline(run_verify=os.getenv("CCP_RUN_VERIFY", "1") != "0")
    print(f"Saved: {Path(out['summary_json_path']).relative_to(ROOT_DIR)}")
    print(f"Saved: {Path(out['summary_md_path']).relative_to(ROOT_DIR)}")
    print(f"Saved: {Path(out['sensitivity_csv_path']).relative_to(ROOT_DIR)}")
    print(
        "Decision -> "
        f"champion_retained={out['champion_retained']} "
        f"| best_candidate={out['best_candidate_name']}"
    )


if __name__ == "__main__":
    main()
