"""Shared helpers for deterministic strategy runner pipelines."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
RUNS_DIR = DATA_DIR / "runs"
SUMMARY_JSON = DATA_DIR / "ai_portfolio_backtest_summary.json"
RESULTS_CSV = DATA_DIR / "ai_portfolio_backtest_results.csv"
VERIFY_JSON = DATA_DIR / "ai_portfolio_backtest_verification.json"
VERIFY_MD = DATA_DIR / "ai_portfolio_backtest_verification.md"
VERIFY_SCRIPT = ROOT_DIR / "scripts" / "verify_ai_portfolio_backtest.py"


def read_json(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def run_python_script(script_path: Path, env_updates: dict[str, str] | None = None) -> dict[str, Any]:
    env = os.environ.copy()
    if env_updates:
        env.update({k: str(v) for k, v in env_updates.items()})
    proc = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
        env=env,
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


def resolve_run_tag(default_prefix: str, env_key: str = "AI_RUN_TAG") -> str:
    raw = (os.getenv(env_key) or "").strip()
    if raw:
        return raw
    run_tag = f"{default_prefix}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    os.environ[env_key] = run_tag
    return run_tag


def artifact_paths(run_tag: str) -> dict[str, Path]:
    return {
        "summary_json": RUNS_DIR / f"ai_portfolio_backtest_summary_{run_tag}.json",
        "results_csv": RUNS_DIR / f"ai_portfolio_backtest_results_{run_tag}.csv",
        "verification_json": RUNS_DIR / f"ai_portfolio_backtest_verification_{run_tag}.json",
        "verification_md": RUNS_DIR / f"ai_portfolio_backtest_verification_{run_tag}.md",
    }


def latest_strategy_snapshot(run_tag: str | None = None) -> dict[str, Any]:
    paths = artifact_paths(run_tag) if run_tag else {}
    summary_path = paths.get("summary_json", SUMMARY_JSON)
    results_path = paths.get("results_csv", RESULTS_CSV)
    verify_json_path = paths.get("verification_json", VERIFY_JSON)
    verify_md_path = paths.get("verification_md", VERIFY_MD)

    if run_tag and not summary_path.exists():
        summary_path = SUMMARY_JSON
    if run_tag and not results_path.exists():
        results_path = RESULTS_CSV
    if run_tag and not verify_json_path.exists():
        verify_json_path = VERIFY_JSON
    if run_tag and not verify_md_path.exists():
        verify_md_path = VERIFY_MD

    summary = read_json(summary_path) if summary_path.exists() else {}
    verification = read_json(verify_json_path) if verify_json_path.exists() else {}
    return {
        "summary_path": str(summary_path),
        "results_path": str(results_path),
        "verification_json_path": str(verify_json_path),
        "verification_md_path": str(verify_md_path),
        "summary": summary,
        "verification": verification,
    }


def run_strategy_pipeline(
    *,
    runner_script: Path,
    default_run_tag_prefix: str,
    run_verify: bool = True,
    run_tag: str | None = None,
    env_key: str = "AI_RUN_TAG",
) -> dict[str, Any]:
    resolved_run_tag = run_tag or resolve_run_tag(default_run_tag_prefix, env_key=env_key)
    baseline_run = run_python_script(runner_script, {env_key: resolved_run_tag})

    verify_run: dict[str, Any] | None = None
    if run_verify:
        paths = artifact_paths(resolved_run_tag)
        verify_run = run_python_script(
            VERIFY_SCRIPT,
            {
                "AI_PORTFOLIO_RESULTS_CSV": str(paths["results_csv"]),
                "AI_PORTFOLIO_SUMMARY_JSON": str(paths["summary_json"]),
                "AI_PORTFOLIO_VERIFY_JSON": str(paths["verification_json"]),
                "AI_PORTFOLIO_VERIFY_MD": str(paths["verification_md"]),
            },
        )

    payload = latest_strategy_snapshot(resolved_run_tag)
    payload["baseline_run"] = baseline_run
    payload["verification_run"] = verify_run
    payload["verified"] = bool(run_verify)
    payload["run_tag"] = resolved_run_tag
    return payload
