from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable

from bot.scheduler_config import (
    format_strategy_v14_message,
    format_strategy_v14_snapshot,
    format_strategy_v2_message,
    format_strategy_v2_snapshot,
)
from strategy_catalog import get_strategy_definition, iter_strategy_definitions
from strategy_runtime import load_latest_strategy_by_key, run_strategy_by_key


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
        label=get_strategy_definition("v2").label,
        pipeline_module=get_strategy_definition("v2").pipeline_module,
        run_fn_name=get_strategy_definition("v2").run_fn_name,
        latest_fn_name=get_strategy_definition("v2").latest_fn_name,
        snapshot_formatter=format_strategy_v2_snapshot,
        message_formatter=format_strategy_v2_message,
    ),
    "v14": StrategySpec(
        key="v14",
        label=get_strategy_definition("v14").label,
        pipeline_module=get_strategy_definition("v14").pipeline_module,
        run_fn_name=get_strategy_definition("v14").run_fn_name,
        latest_fn_name=get_strategy_definition("v14").latest_fn_name,
        snapshot_formatter=format_strategy_v14_snapshot,
        message_formatter=format_strategy_v14_message,
    ),
}


def iter_strategy_specs() -> list[StrategySpec]:
    return [STRATEGY_SPECS[item.key] for item in iter_strategy_definitions(bot_only=True)]


def get_strategy_spec(key: str) -> StrategySpec:
    spec = STRATEGY_SPECS.get(str(key).strip().lower())
    if spec is None:
        raise KeyError(f"Unknown strategy key: {key}")
    return spec

async def run_strategy(spec_key: str, verify: bool = True) -> dict[str, Any]:
    return await asyncio.to_thread(run_strategy_by_key, spec_key, verify)


async def load_latest_strategy(spec_key: str) -> dict[str, Any]:
    return await asyncio.to_thread(load_latest_strategy_by_key, spec_key)


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


def latest_strategy_missing_text(spec_key: str) -> str:
    spec = get_strategy_spec(spec_key)
    return f"No {spec.label} output found yet.\nRun {spec.label} first."


def run_action_key(spec_key: str) -> str:
    return f"run_strategy_{get_strategy_spec(spec_key).key}"


def latest_action_key(spec_key: str) -> str:
    return f"latest_strategy_{get_strategy_spec(spec_key).key}"


def command_name(spec_key: str) -> str:
    return get_strategy_definition(spec_key).command_name


def latest_command_name(spec_key: str) -> str:
    return get_strategy_definition(spec_key).latest_command_name
