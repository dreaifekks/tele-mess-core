from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class StorageConfig:
    data_dir: Path = Path("./data")
    database: Path = Path("./data/archive.db")


@dataclass(slots=True)
class TelegramChatConfig:
    id: int
    name: str | None = None


@dataclass(slots=True)
class BackfillConfig:
    enabled: bool = True
    initial_limit: int = 1000
    catch_up_limit: int = 1000


@dataclass(slots=True)
class MediaDownloadConfig:
    retries: int = 2
    retry_delay_seconds: float = 1.0


@dataclass(slots=True)
class TelegramConfig:
    accounts: list["TelegramAccountConfig"] = field(default_factory=list)
    backfill: BackfillConfig = field(default_factory=BackfillConfig)
    media_download: MediaDownloadConfig = field(default_factory=MediaDownloadConfig)


@dataclass(slots=True)
class TelegramAccountConfig:
    account_id: str
    api_id: int
    api_hash: str
    session_name: str
    session_dir: Path = Path("./data/sessions")
    timezone: str = "Asia/Tokyo"
    chats: list[TelegramChatConfig] = field(default_factory=list)


@dataclass(slots=True)
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 8765
    token: str = ""


@dataclass(slots=True)
class LoggingConfig:
    level: str = "INFO"
    file: Path | None = Path("./logs/tele-mess-core.log")


@dataclass(slots=True)
class AppConfig:
    storage: StorageConfig
    telegram: TelegramConfig
    server: ServerConfig
    logging: LoggingConfig


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    base_dir = config_path.parent
    storage_raw = raw.get("storage", {})
    telegram_raw = raw.get("telegram", {})
    server_raw = raw.get("server", {})
    logging_raw = raw.get("logging", {})

    telegram = TelegramConfig(
        accounts=_parse_accounts(base_dir, telegram_raw),
        backfill=_parse_backfill(telegram_raw.get("backfill", {})),
        media_download=_parse_media_download(telegram_raw.get("media_download", {})),
    )

    storage = StorageConfig(
        data_dir=_resolve_path(base_dir, storage_raw.get("data_dir", "./data")),
        database=_resolve_path(base_dir, storage_raw.get("database", "./data/archive.db")),
    )
    server = ServerConfig(
        host=str(server_raw.get("host", "127.0.0.1")),
        port=int(server_raw.get("port", 8765)),
        token=str(server_raw.get("token", "") or ""),
    )
    log_file = logging_raw.get("file", "./logs/tele-mess-core.log")
    logging_config = LoggingConfig(
        level=str(logging_raw.get("level", "INFO")),
        file=_resolve_path(base_dir, log_file) if log_file else None,
    )
    return AppConfig(storage=storage, telegram=telegram, server=server, logging=logging_config)


def _parse_chat(item: Any) -> TelegramChatConfig:
    if isinstance(item, dict):
        return TelegramChatConfig(id=int(item["id"]), name=item.get("name"))
    return TelegramChatConfig(id=int(item))


def _parse_accounts(base_dir: Path, telegram_raw: dict[str, Any]) -> list[TelegramAccountConfig]:
    raw_accounts = telegram_raw.get("accounts")
    if raw_accounts:
        return [_parse_account(base_dir, item, index) for index, item in enumerate(raw_accounts)]
    return [
        TelegramAccountConfig(
            account_id=str(telegram_raw.get("account_id", "default")),
            api_id=int(_required(telegram_raw, "api_id", "telegram.api_id")),
            api_hash=str(_required(telegram_raw, "api_hash", "telegram.api_hash")),
            session_name=str(telegram_raw.get("session_name", "tele_mess_core")),
            session_dir=_resolve_path(base_dir, telegram_raw.get("session_dir", "./data/sessions")),
            timezone=str(telegram_raw.get("timezone", "Asia/Tokyo")),
            chats=[_parse_chat(item) for item in telegram_raw.get("chats", [])],
        )
    ]


def _parse_account(base_dir: Path, item: dict[str, Any], index: int) -> TelegramAccountConfig:
    account_id = str(item.get("account_id") or item.get("id") or f"account_{index + 1}")
    return TelegramAccountConfig(
        account_id=account_id,
        api_id=int(_required(item, "api_id", f"telegram.accounts[{index}].api_id")),
        api_hash=str(_required(item, "api_hash", f"telegram.accounts[{index}].api_hash")),
        session_name=str(item.get("session_name") or account_id),
        session_dir=_resolve_path(base_dir, item.get("session_dir", "./data/sessions")),
        timezone=str(item.get("timezone", "Asia/Tokyo")),
        chats=[_parse_chat(chat) for chat in item.get("chats", [])],
    )


def _required(raw: dict[str, Any], key: str, label: str) -> Any:
    value = raw.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing required config value: {label}")
    return value


def _resolve_path(base_dir: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _parse_backfill(raw: Any) -> BackfillConfig:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("telegram.backfill must be a mapping")
    return BackfillConfig(
        enabled=_parse_bool(raw.get("enabled", True)),
        initial_limit=int(raw.get("initial_limit", 1000)),
        catch_up_limit=int(raw.get("catch_up_limit", 1000)),
    )


def _parse_media_download(raw: Any) -> MediaDownloadConfig:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("telegram.media_download must be a mapping")
    return MediaDownloadConfig(
        retries=max(0, int(raw.get("retries", 2))),
        retry_delay_seconds=max(0.0, float(raw.get("retry_delay_seconds", 1.0))),
    )


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)
