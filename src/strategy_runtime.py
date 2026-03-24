from __future__ import annotations

from importlib import import_module
from typing import Any, Callable

from strategy_catalog import StrategyDefinition, get_strategy_definition


def _load_pipeline_callable(defn: StrategyDefinition, attr_name: str) -> Callable[..., Any]:
    module = import_module(defn.pipeline_module)
    fn = getattr(module, attr_name, None)
    if not callable(fn):
        raise AttributeError(f"{defn.pipeline_module}.{attr_name} is not callable")
    return fn


def run_strategy_by_key(strategy_key: str, verify: bool = True) -> dict[str, Any]:
    definition = get_strategy_definition(strategy_key)
    fn = _load_pipeline_callable(definition, definition.run_fn_name)
    return fn(verify)


def load_latest_strategy_by_key(strategy_key: str) -> dict[str, Any]:
    definition = get_strategy_definition(strategy_key)
    fn = _load_pipeline_callable(definition, definition.latest_fn_name)
    return fn()
