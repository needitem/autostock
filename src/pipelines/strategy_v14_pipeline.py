"""Helpers to run and inspect the checked-in Strategy V14 dynamic-defense candidate."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pipelines.strategy_pipeline_utils import (
    VERIFY_SCRIPT,
    artifact_paths,
    latest_strategy_snapshot,
    run_strategy_pipeline,
)


ROOT_DIR = Path(__file__).resolve().parents[2]
RUNNER_SCRIPT = ROOT_DIR / "scripts" / "run_strategy_v14_regime_gld_dynamic_defense.py"


def _artifact_paths(run_tag: str) -> dict[str, Path]:
    return artifact_paths(run_tag)


def latest_strategy_v14_snapshot(run_tag: str | None = None) -> dict[str, Any]:
    return latest_strategy_snapshot(run_tag)


def run_strategy_v14_pipeline(run_verify: bool = True, run_tag: str | None = None) -> dict[str, Any]:
    return run_strategy_pipeline(
        runner_script=RUNNER_SCRIPT,
        default_run_tag_prefix="strategy_v14_regime_gld_dynamic_defense",
        run_verify=run_verify,
        run_tag=run_tag,
    )
