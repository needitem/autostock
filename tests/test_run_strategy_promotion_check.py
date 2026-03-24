from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_strategy_promotion_check.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_horizon_check_requires_positive_cagr_diff_and_low_p_value():
    runner = _load_module(SCRIPT_PATH, "run_strategy_promotion_check")
    net = [1.2 if i % 2 == 0 else 0.8 for i in range(260)]
    bench = [0.2 if i % 2 == 0 else 0.1 for i in range(260)]
    df = runner.pd.DataFrame(
        {
            "entry_day": runner.pd.date_range("2020-01-03", periods=260, freq="W-FRI"),
            "net_return_pct": net,
            "benchmark_return_pct": bench,
        }
    )

    out = runner._horizon_check(df, years=3, periods_per_year=52)

    assert out["cagr_diff_pct"] > 0
    assert out["passes"] is True
