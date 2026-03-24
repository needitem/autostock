from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = ROOT / "src" / "main.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_main_parser_supports_v14_backtest_flag():
    main = _load_module(MAIN_PATH, "main_v14_entry")

    args = main._parse_args(["--v14-backtest"])

    assert args.v14_backtest is True
    assert args.backtest is False
    assert args.stock_backtest is False
