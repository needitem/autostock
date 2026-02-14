"""In-process scan cache for bot request paths."""

from __future__ import annotations

import copy
import threading
import time
from typing import Any


_LOCK = threading.Lock()
_CACHE: dict[str, Any] = {
    "key": None,
    "at": 0.0,
    "result": None,
}


def _universe_key(symbols: list[str]) -> str:
    if not symbols:
        return "0"
    return f"{len(symbols)}:{symbols[0]}:{symbols[-1]}:{hash(tuple(symbols[:50]))}"


def get_scan_result(symbols: list[str], max_age_sec: int = 300, force_refresh: bool = False) -> tuple[dict[str, Any], bool]:
    """Return scan result and whether cache was used."""
    from core.signals import scan_stocks

    key = _universe_key(symbols)
    now = time.time()

    with _LOCK:
        if (
            not force_refresh
            and _CACHE["result"] is not None
            and _CACHE["key"] == key
            and now - float(_CACHE["at"] or 0.0) <= max(1, int(max_age_sec))
        ):
            return copy.deepcopy(_CACHE["result"]), True

    result = scan_stocks(symbols)

    with _LOCK:
        _CACHE["key"] = key
        _CACHE["at"] = time.time()
        _CACHE["result"] = copy.deepcopy(result)

    return result, False


def clear_scan_cache() -> None:
    with _LOCK:
        _CACHE["key"] = None
        _CACHE["at"] = 0.0
        _CACHE["result"] = None
