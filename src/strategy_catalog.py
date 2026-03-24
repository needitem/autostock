from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyDefinition:
    key: str
    label: str
    pipeline_module: str
    run_fn_name: str
    latest_fn_name: str
    summary_title: str
    command_name: str
    latest_command_name: str
    bot_enabled: bool = True


STRATEGY_DEFINITIONS: dict[str, StrategyDefinition] = {
    "v2": StrategyDefinition(
        key="v2",
        label="Strategy V2",
        pipeline_module="pipelines.strategy_v2_pipeline",
        run_fn_name="run_strategy_v2_pipeline",
        latest_fn_name="latest_strategy_v2_snapshot",
        summary_title="strategy v2 baseline",
        command_name="strategy_v2",
        latest_command_name="strategy_v2_latest",
        bot_enabled=True,
    ),
    "v4": StrategyDefinition(
        key="v4",
        label="Strategy V4",
        pipeline_module="pipelines.strategy_v4_stock_pipeline",
        run_fn_name="run_strategy_v4_stock_pipeline",
        latest_fn_name="latest_strategy_v4_stock_snapshot",
        summary_title="strategy v4 stock-momentum baseline",
        command_name="strategy_v4",
        latest_command_name="strategy_v4_latest",
        bot_enabled=False,
    ),
    "v14": StrategyDefinition(
        key="v14",
        label="Strategy V14",
        pipeline_module="pipelines.strategy_v14_pipeline",
        run_fn_name="run_strategy_v14_pipeline",
        latest_fn_name="latest_strategy_v14_snapshot",
        summary_title="strategy v14 regime GLD dynamic defense",
        command_name="strategy_v14",
        latest_command_name="strategy_v14_latest",
        bot_enabled=True,
    ),
}


def get_strategy_definition(key: str) -> StrategyDefinition:
    strategy = STRATEGY_DEFINITIONS.get(str(key).strip().lower())
    if strategy is None:
        raise KeyError(f"Unknown strategy key: {key}")
    return strategy


def iter_strategy_definitions(*, bot_only: bool = False) -> list[StrategyDefinition]:
    items = list(STRATEGY_DEFINITIONS.values())
    if bot_only:
        items = [item for item in items if item.bot_enabled]
    return items
