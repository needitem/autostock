"""Helpers to run and inspect the checked-in Strategy V2 baseline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pipelines.strategy_pipeline_utils import latest_strategy_snapshot, run_strategy_pipeline


ROOT_DIR = Path(__file__).resolve().parents[2]
RUNNER_SCRIPT = ROOT_DIR / "scripts" / "run_strategy_v2_baseline.py"


def latest_strategy_v2_snapshot(run_tag: str | None = None) -> dict[str, Any]:
    return latest_strategy_snapshot(run_tag)


def run_strategy_v2_pipeline(run_verify: bool = True, run_tag: str | None = None) -> dict[str, Any]:
    return run_strategy_pipeline(
        runner_script=RUNNER_SCRIPT,
        default_run_tag_prefix="strategy_v2_baseline",
        run_verify=run_verify,
        run_tag=run_tag,
    )
