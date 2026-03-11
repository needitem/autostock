"""Helpers to run and inspect the checked-in Strategy V4 stock-momentum baseline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
SUMMARY_JSON = DATA_DIR / "ai_portfolio_backtest_summary.json"
RESULTS_CSV = DATA_DIR / "ai_portfolio_backtest_results.csv"
VERIFY_JSON = DATA_DIR / "ai_portfolio_backtest_verification.json"
VERIFY_MD = DATA_DIR / "ai_portfolio_backtest_verification.md"
RUNNER_SCRIPT = ROOT_DIR / "scripts" / "run_strategy_v4_stock_momentum.py"
VERIFY_SCRIPT = ROOT_DIR / "scripts" / "verify_ai_portfolio_backtest.py"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _run_python_script(script_path: Path) -> dict[str, Any]:
    proc = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        check=False,
    )
    result = {
        "script": str(script_path),
        "returncode": int(proc.returncode),
        "stdout": str(proc.stdout or "").strip(),
        "stderr": str(proc.stderr or "").strip(),
    }
    if proc.returncode != 0:
        detail = result["stderr"] or result["stdout"] or f"exit={proc.returncode}"
        raise RuntimeError(f"{script_path.name} failed: {detail}")
    return result


def latest_strategy_v4_stock_snapshot() -> dict[str, Any]:
    summary = _read_json(SUMMARY_JSON) if SUMMARY_JSON.exists() else {}
    verification = _read_json(VERIFY_JSON) if VERIFY_JSON.exists() else {}
    return {
        "summary_path": str(SUMMARY_JSON),
        "results_path": str(RESULTS_CSV),
        "verification_json_path": str(VERIFY_JSON),
        "verification_md_path": str(VERIFY_MD),
        "summary": summary,
        "verification": verification,
    }


def run_strategy_v4_stock_pipeline(run_verify: bool = True) -> dict[str, Any]:
    baseline_run = _run_python_script(RUNNER_SCRIPT)
    verify_run: dict[str, Any] | None = None
    if run_verify:
        verify_run = _run_python_script(VERIFY_SCRIPT)

    payload = latest_strategy_v4_stock_snapshot()
    payload["baseline_run"] = baseline_run
    payload["verification_run"] = verify_run
    payload["verified"] = bool(run_verify)
    return payload
