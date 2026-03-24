from __future__ import annotations

import asyncio
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Callable

from bot.scheduler_config import (
    format_strategy_v14_message,
    format_strategy_v14_snapshot,
    format_strategy_v2_message,
    format_strategy_v2_snapshot,
)


SnapshotFormatter = Callable[[dict[str, Any], dict[str, Any], str | None, str | None], str]
MessageFormatter = Callable[[dict[str, Any]], str]


@dataclass(frozen=True)
class StrategySpec:
    key: str
    label: str
    pipeline_module: str
    run_fn_name: str
    latest_fn_name: str
    snapshot_formatter: SnapshotFormatter
    message_formatter: MessageFormatter


STRATEGY_SPECS: dict[str, StrategySpec] = {
    "v2": StrategySpec(
        key="v2",
        label="Strategy V2",
        pipeline_module="pipelines.strategy_v2_pipeline",
        run_fn_name="run_strategy_v2_pipeline",
        latest_fn_name="latest_strategy_v2_snapshot",
        snapshot_formatter=format_strategy_v2_snapshot,
        message_formatter=format_strategy_v2_message,
    ),
    "v14": StrategySpec(
        key="v14",
        label="Strategy V14",
        pipeline_module="pipelines.strategy_v14_pipeline",
        run_fn_name="run_strategy_v14_pipeline",
        latest_fn_name="latest_strategy_v14_snapshot",
        snapshot_formatter=format_strategy_v14_snapshot,
        message_formatter=format_strategy_v14_message,
    ),
}


def iter_strategy_specs() -> list[StrategySpec]:
    return [STRATEGY_SPECS[key] for key in sorted(STRATEGY_SPECS.keys())]


def get_strategy_spec(key: str) -> StrategySpec:
    spec = STRATEGY_SPECS.get(str(key).strip().lower())
    if spec is None:
        raise KeyError(f"Unknown strategy key: {key}")
    return spec


def _load_pipeline_callable(spec: StrategySpec, attr_name: str) -> Callable[..., Any]:
    module = import_module(spec.pipeline_module)
    fn = getattr(module, attr_name, None)
    if not callable(fn):
        raise AttributeError(f"{spec.pipeline_module}.{attr_name} is not callable")
    return fn


async def run_strategy(spec_key: str, verify: bool = True) -> dict[str, Any]:
    spec = get_strategy_spec(spec_key)
    fn = _load_pipeline_callable(spec, spec.run_fn_name)
    return await asyncio.to_thread(fn, verify)


async def load_latest_strategy(spec_key: str) -> dict[str, Any]:
    spec = get_strategy_spec(spec_key)
    fn = _load_pipeline_callable(spec, spec.latest_fn_name)
    return await asyncio.to_thread(fn)


def format_strategy_snapshot_from_result(spec_key: str, result: dict[str, Any]) -> str:
    spec = get_strategy_spec(spec_key)
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    verification = result.get("verification") if isinstance(result.get("verification"), dict) else {}
    return spec.snapshot_formatter(
        summary,
        verification,
        str(result.get("summary_path", "") or ""),
        str(result.get("verification_json_path", "") or ""),
    )


def latest_strategy_snapshot_text(spec_key: str, result: dict[str, Any]) -> str:
    spec = get_strategy_spec(spec_key)
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    verification = result.get("verification") if isinstance(result.get("verification"), dict) else {}
    return spec.snapshot_formatter(
        summary,
        verification,
        str(result.get("summary_path", "") or ""),
        str(result.get("verification_json_path", "") or ""),
    )


def latest_strategy_missing_text(spec_key: str) -> str:
    spec = get_strategy_spec(spec_key)
    return f"No {spec.label} output found yet.\nRun {spec.label} first."


def run_action_key(spec_key: str) -> str:
    return f"run_strategy_{get_strategy_spec(spec_key).key}"


def latest_action_key(spec_key: str) -> str:
    return f"latest_strategy_{get_strategy_spec(spec_key).key}"


def command_name(spec_key: str) -> str:
    return f"strategy_{get_strategy_spec(spec_key).key}"


def latest_command_name(spec_key: str) -> str:
    return f"strategy_{get_strategy_spec(spec_key).key}_latest"
