from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class StorageConfig:
    data_dir: Path = Path("./data")
    database: Path = Path("./data/archive.db")
    raw_json_retention_days: int = 7


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


@dataclass(slots=True)
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 8765
    token: str = ""
    allow_unauthenticated_localhost: bool = False


@dataclass(slots=True)
class LoggingConfig:
    level: str = "INFO"
    file: Path | None = Path("./logs/tele-mess-core.log")


@dataclass(slots=True)
class DailyAiConfig:
    provider: str = "codex-cli"
    command: list[str] = field(
        default_factory=lambda: [
            "codex",
            "-a",
            "never",
            "exec",
            "--skip-git-repo-check",
            "--output-last-message",
            "{output}",
            "{images}",
            "-",
        ]
    )
    timeout_seconds: int = 900


@dataclass(slots=True)
class DailyDeliveryConfig:
    enabled: bool = False
    account_id: str = ""
    origin_id: int | None = None
    topic_id: int = 0


@dataclass(slots=True)
class DailyPackagingConfig:
    output_dir: Path | None = None
    systemd_user_dir: Path | None = None
    cli_path: str = "tele-mess-core"
    ai: DailyAiConfig = field(default_factory=DailyAiConfig)
    delivery: DailyDeliveryConfig = field(default_factory=DailyDeliveryConfig)


@dataclass(slots=True)
class AppConfig:
    storage: StorageConfig
    telegram: TelegramConfig
    server: ServerConfig
    logging: LoggingConfig
    daily: DailyPackagingConfig = field(default_factory=DailyPackagingConfig)
    config_path: Path | None = None


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    base_dir = config_path.parent
    storage_raw = raw.get("storage", {})
    telegram_raw = raw.get("telegram", {})
    server_raw = raw.get("server", {})
    logging_raw = raw.get("logging", {})
    daily_raw = raw.get("daily", {})

    telegram = TelegramConfig(
        accounts=_parse_accounts(base_dir, telegram_raw),
        backfill=_parse_backfill(telegram_raw.get("backfill", {})),
        media_download=_parse_media_download(telegram_raw.get("media_download", {})),
    )

    storage = StorageConfig(
        data_dir=_resolve_path(base_dir, storage_raw.get("data_dir", "./data")),
        database=_resolve_path(base_dir, storage_raw.get("database", "./data/archive.db")),
        raw_json_retention_days=max(1, int(storage_raw.get("raw_json_retention_days", 7))),
    )
    server = ServerConfig(
        host=str(server_raw.get("host", "127.0.0.1")),
        port=int(server_raw.get("port", 8765)),
        token=str(server_raw.get("token", "") or ""),
        allow_unauthenticated_localhost=_parse_bool(
            server_raw.get("allow_unauthenticated_localhost", False)
        ),
    )
    log_file = logging_raw.get("file", "./logs/tele-mess-core.log")
    logging_config = LoggingConfig(
        level=str(logging_raw.get("level", "INFO")),
        file=_resolve_path(base_dir, log_file) if log_file else None,
    )
    daily = _parse_daily(base_dir, daily_raw)
    return AppConfig(
        storage=storage,
        telegram=telegram,
        server=server,
        logging=logging_config,
        daily=daily,
        config_path=config_path.resolve(),
    )


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


def _parse_daily(base_dir: Path, raw: Any) -> DailyPackagingConfig:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("daily must be a mapping")
    output_dir = raw.get("output_dir")
    systemd_user_dir = raw.get("systemd_user_dir")
    return DailyPackagingConfig(
        output_dir=_resolve_path(base_dir, output_dir) if output_dir else None,
        systemd_user_dir=_resolve_path(base_dir, systemd_user_dir) if systemd_user_dir else None,
        cli_path=str(raw.get("cli_path", "tele-mess-core") or "tele-mess-core"),
        ai=_parse_daily_ai(raw.get("ai", {})),
        delivery=_parse_daily_delivery(raw.get("delivery", {})),
    )


def _parse_daily_ai(raw: Any) -> DailyAiConfig:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("daily.ai must be a mapping")
    command = raw.get("command")
    if command is None:
        command_list = DailyAiConfig().command
    elif isinstance(command, list):
        command_list = [str(item) for item in command]
    elif isinstance(command, str):
        command_list = [command]
    else:
        raise ValueError("daily.ai.command must be a string or list")
    return DailyAiConfig(
        provider=str(raw.get("provider", "codex-cli") or "codex-cli"),
        command=command_list,
        timeout_seconds=max(1, int(raw.get("timeout_seconds", 900))),
    )


def _parse_daily_delivery(raw: Any) -> DailyDeliveryConfig:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("daily.delivery must be a mapping")
    enabled = _parse_bool(raw.get("enabled", False))
    account_id = str(raw.get("account_id") or "").strip()
    origin_id_raw = raw.get("origin_id")
    topic_id_raw = raw.get("topic_id", 0)
    origin_id = None if origin_id_raw in (None, "") else int(origin_id_raw)
    topic_id = 0 if topic_id_raw in (None, "") else int(topic_id_raw)
    if enabled:
        if not account_id:
            raise ValueError("daily.delivery.account_id is required when daily.delivery.enabled is true")
        if origin_id is None:
            raise ValueError("daily.delivery.origin_id is required when daily.delivery.enabled is true")
    return DailyDeliveryConfig(
        enabled=enabled,
        account_id=account_id,
        origin_id=origin_id,
        topic_id=topic_id,
    )


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)
