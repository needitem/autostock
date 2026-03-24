from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def apply_default_env(
    default_env: dict[str, str],
    *,
    run_tag_prefix: str,
    overwrite_existing: bool = False,
    env_key: str = "AI_RUN_TAG",
) -> None:
    for key, value in default_env.items():
        if overwrite_existing:
            os.environ[key] = value
        else:
            os.environ.setdefault(key, value)
    os.environ.setdefault(
        env_key,
        f"{run_tag_prefix}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
    )


def run_backtest_selector(
    *,
    script_dir: Path,
    heading: str,
    echo_keys: Iterable[str],
) -> None:
    sys.path.insert(0, str(script_dir))

    import backtest_ai_portfolio_selector as selector

    print(heading)
    for key in echo_keys:
        print(f"  {key}={os.environ.get(key, '')}")

    selector.run()
