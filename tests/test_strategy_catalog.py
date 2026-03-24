from __future__ import annotations

from strategy_catalog import get_strategy_definition, iter_strategy_definitions


def test_strategy_catalog_exposes_expected_active_keys() -> None:
    defs = iter_strategy_definitions()
    keys = [item.key for item in defs]

    assert keys == ["v2", "v4", "v14"]


def test_strategy_catalog_v14_command_names() -> None:
    item = get_strategy_definition("v14")

    assert item.command_name == "strategy_v14"
    assert item.latest_command_name == "strategy_v14_latest"
