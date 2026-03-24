from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_stock_hypothesis_strict_compare.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_load_hypotheses_can_find_ro2_variants():
    runner = _load_module(SCRIPT_PATH, "run_stock_hypothesis_strict_compare")
    research = runner._load_research_module()

    hypotheses = runner._load_hypotheses(
        research,
        [
            "weekly_baseline_v4",
            "weekly_veto_recentq_newonly_neutral_soft_bonus_ro2",
            "weekly_veto_recentq_newonly_nrisk_soft_bonus_ro2",
        ],
    )

    assert [h.name for h in hypotheses] == [
        "weekly_baseline_v4",
        "weekly_veto_recentq_newonly_neutral_soft_bonus_ro2",
        "weekly_veto_recentq_newonly_nrisk_soft_bonus_ro2",
    ]
    assert hypotheses[1].top_k_risk_off == 2
    assert hypotheses[2].pit_veto_regimes == ("neutral", "risk_off")


def test_walkforward_folds_require_train_and_test_coverage():
    runner = _load_module(SCRIPT_PATH, "run_stock_hypothesis_strict_compare_folds")
    df = runner.pd.DataFrame(
        {
            "entry_year": [2006] * 52 + [2007] * 52 + [2008] * 52 + [2009] * 52 + [2010] * 52 + [2011] * 30 + [2012] * 52
        }
    )

    folds = runner._walkforward_folds(df, train_years=5, test_years=1, min_test_weeks=40)

    assert folds == [(2005, 2009, 2010, 2010), (2007, 2011, 2012, 2012)]
