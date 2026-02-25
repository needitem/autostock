"""Per-chat bot preference storage."""

from __future__ import annotations

import json
import os
from typing import Any


VALID_STYLES = {"beginner", "standard", "detail"}
DEFAULT_STYLE = os.getenv("BOT_MESSAGE_STYLE", "beginner").strip().lower() or "beginner"
if DEFAULT_STYLE not in VALID_STYLES:
    DEFAULT_STYLE = "beginner"

SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "chat_settings.json")


def _load_all() -> dict[str, Any]:
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {"chats": {}}


def _save_all(data: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)
    with open(SETTINGS_FILE, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)


def normalize_style(style: str | None) -> str:
    value = (style or "").strip().lower()
    if value == "compact":
        value = "beginner"
    if value in VALID_STYLES:
        return value

    aliases = {
        "c": "beginner",
        "b": "beginner",
        "s": "standard",
        "d": "detail",
        "simple": "beginner",
        "begin": "beginner",
        "easy": "beginner",
        "basic": "beginner",
        "normal": "standard",
        "detailed": "detail",
    }
    return aliases.get(value, DEFAULT_STYLE)


def get_chat_style(chat_id: str | int | None) -> str:
    if chat_id is None:
        return DEFAULT_STYLE
    data = _load_all()
    chats = data.get("chats", {})
    style = chats.get(str(chat_id), {}).get("style", DEFAULT_STYLE)
    return normalize_style(style)


def set_chat_style(chat_id: str | int, style: str) -> str:
    normalized = normalize_style(style)
    data = _load_all()
    chats = data.setdefault("chats", {})
    row = chats.setdefault(str(chat_id), {})
    row["style"] = normalized
    _save_all(data)
    return normalized


def style_label(style: str) -> str:
    mapping = {
        "beginner": "Beginner",
        "standard": "Standard",
        "detail": "Detail",
    }
    return mapping.get(normalize_style(style), "Beginner")
