from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "runs"
VERIFY_SCRIPT = ROOT / "scripts" / "verify_ai_portfolio_backtest.py"
PROMOTION_SCRIPT = ROOT / "scripts" / "run_strategy_promotion_check.py"


@dataclass(frozen=True)
class SweepArtifacts:
    ai_run_tag: str
    promotion_tag: str
    results_csv: Path
    summary_json: Path
    verify_json: Path
    verify_md: Path
    promotion_json: Path


def load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def slug(text: str) -> str:
    out = "".join(ch if ch.isalnum() else "_" for ch in str(text).strip().lower()).strip("_")
    return out or "run"


def run_python_script(script_path: Path, env_updates: dict[str, str]) -> dict[str, Any]:
    env = os.environ.copy()
    env.update({k: str(v) for k, v in env_updates.items()})
    proc = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    out = {
        "script": str(script_path),
        "returncode": int(proc.returncode),
        "stdout": str(proc.stdout or "").strip(),
        "stderr": str(proc.stderr or "").strip(),
    }
    if proc.returncode != 0:
        detail = out["stderr"] or out["stdout"] or f"exit={proc.returncode}"
        raise RuntimeError(f"{script_path.name} failed: {detail}")
    return out


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def metric(report: dict[str, Any], *keys: str, default: float = 0.0) -> float:
    cur: Any = report
    for key in keys:
        if not isinstance(cur, dict):
            return float(default)
        cur = cur.get(key)
    try:
        return float(cur)
    except Exception:
        return float(default)


def build_artifacts(ai_run_tag: str, promotion_tag: str) -> SweepArtifacts:
    return SweepArtifacts(
        ai_run_tag=ai_run_tag,
        promotion_tag=promotion_tag,
        results_csv=RUNS_DIR / f"ai_portfolio_backtest_results_{ai_run_tag}.csv",
        summary_json=RUNS_DIR / f"ai_portfolio_backtest_summary_{ai_run_tag}.json",
        verify_json=RUNS_DIR / f"ai_portfolio_backtest_verification_{ai_run_tag}.json",
        verify_md=RUNS_DIR / f"ai_portfolio_backtest_verification_{ai_run_tag}.md",
        promotion_json=RUNS_DIR / f"{promotion_tag}.json",
    )


def run_backtest_verify_promotion(
    *,
    runner_script: Path,
    env_updates: dict[str, str],
    ai_run_tag: str,
    promotion_tag: str,
) -> tuple[SweepArtifacts, dict[str, Any]]:
    artifacts = build_artifacts(ai_run_tag, promotion_tag)
    run_python_script(runner_script, env_updates)
    run_python_script(
        VERIFY_SCRIPT,
        {
            "AI_PORTFOLIO_RESULTS_CSV": str(artifacts.results_csv),
            "AI_PORTFOLIO_SUMMARY_JSON": str(artifacts.summary_json),
            "AI_PORTFOLIO_VERIFY_JSON": str(artifacts.verify_json),
            "AI_PORTFOLIO_VERIFY_MD": str(artifacts.verify_md),
        },
    )
    run_python_script(
        PROMOTION_SCRIPT,
        {
            "PROMOTION_VERIFY_JSON": str(artifacts.verify_json),
            "PROMOTION_RESULTS_CSV": str(artifacts.results_csv),
            "PROMOTION_RUN_TAG": promotion_tag,
        },
    )
    return artifacts, load_json(artifacts.promotion_json)
