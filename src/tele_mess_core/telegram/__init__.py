"""Telegram account runtime, ingestion, and management services.

The services are loaded on first access so that importing a submodule such
as telegram.runtime does not pull in Telethon.
"""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "TelegramArchiveService": ".ingest",
    "TelegramAuthService": ".auth",
    "TelegramDiscoveryService": ".discovery",
    "TelegramRuntimeManager": ".manager",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name], __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *_EXPORTS})
