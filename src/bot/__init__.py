"""Telegram bot package exports (lazy to avoid side-effects on import)."""


def run_bot(*args, **kwargs):
    from .bot import run_bot as _run_bot

    return _run_bot(*args, **kwargs)


__all__ = ["run_bot"]
