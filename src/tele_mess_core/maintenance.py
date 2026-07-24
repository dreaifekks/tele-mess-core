from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import shlex
import subprocess
from typing import TYPE_CHECKING, Any

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import app_workspace_dir
from tele_mess_core.models import utc_now_iso

if TYPE_CHECKING:
    from tele_mess_core.config import AppConfig


RAW_JSON_CLEANUP_SYSTEMD_BASENAME = "tele-mess-core-raw-json-cleanup"
RAW_JSON_CLEANUP_BATCH_SIZE = 10_000


def raw_json_cutoff_for_retention(retention_days: int) -> str:
    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")
    return (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()


def cleanup_message_raw_json(
    store: ArchiveStore,
    *,
    retention_days: int,
    cutoff_sent_at: str | None = None,
    dry_run: bool = False,
    vacuum: bool = False,
    batch_size: int = RAW_JSON_CLEANUP_BATCH_SIZE,
) -> dict[str, Any]:
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    cutoff = cutoff_sent_at or raw_json_cutoff_for_retention(retention_days)
    before_total = store.message_raw_json_stats()
    eligible = store.message_raw_json_stats(cutoff_sent_at=cutoff)
    removed = {"message_count": 0, "raw_json_bytes": 0}
    checkpoint = {"busy": 0, "log": 0, "checkpointed": 0}
    vacuumed = False
    if not dry_run:
        removed = store.clear_message_raw_json_before(cutoff, batch_size=batch_size)
        if vacuum:
            store.vacuum()
            vacuumed = True
        if removed["message_count"] or vacuum:
            checkpoint = store.wal_checkpoint_truncate()
    after_total = store.message_raw_json_stats()
    return {
        "retention_days": retention_days,
        "cutoff_sent_at": cutoff,
        "batch_size": batch_size,
        "dry_run": dry_run,
        "vacuum": vacuumed,
        "before": before_total,
        "eligible": eligible,
        "removed": removed,
        "checkpoint": checkpoint,
        "after": after_total,
    }


def install_raw_json_cleanup_timer(
    config: "AppConfig",
    *,
    retention_days: int | None = None,
    on_calendar: str = "weekly",
    cli_path: str | None = None,
    vacuum: bool = False,
    activate: bool = False,
) -> dict[str, Any]:
    if config.config_path is None:
        return {"installed": False, "last_installed_at": None, "last_error": "config_path is required"}
    systemd_dir = _systemd_user_dir(config)
    try:
        systemd_dir.mkdir(parents=True, exist_ok=True)
        service_path = systemd_dir / f"{RAW_JSON_CLEANUP_SYSTEMD_BASENAME}.service"
        timer_path = systemd_dir / f"{RAW_JSON_CLEANUP_SYSTEMD_BASENAME}.timer"
        service_path.write_text(
            _raw_json_cleanup_service(config, retention_days=retention_days, cli_path=cli_path, vacuum=vacuum),
            encoding="utf-8",
        )
        timer_path.write_text(_raw_json_cleanup_timer(on_calendar=on_calendar), encoding="utf-8")
        if activate:
            subprocess.run(["systemctl", "--user", "daemon-reload"], check=True, capture_output=True, text=True)
            subprocess.run(
                ["systemctl", "--user", "enable", "--now", f"{RAW_JSON_CLEANUP_SYSTEMD_BASENAME}.timer"],
                check=True,
                capture_output=True,
                text=True,
            )
        return {
            "installed": True,
            "activated": activate,
            "service_path": str(service_path),
            "timer_path": str(timer_path),
            "last_installed_at": utc_now_iso(),
            "last_error": None,
        }
    except Exception as exc:
        return {"installed": False, "activated": False, "last_installed_at": None, "last_error": str(exc)}


def remove_raw_json_cleanup_timer(config: "AppConfig", *, activate: bool = False) -> dict[str, Any]:
    systemd_dir = _systemd_user_dir(config)
    service_path = systemd_dir / f"{RAW_JSON_CLEANUP_SYSTEMD_BASENAME}.service"
    timer_path = systemd_dir / f"{RAW_JSON_CLEANUP_SYSTEMD_BASENAME}.timer"
    try:
        if activate:
            subprocess.run(["systemctl", "--user", "disable", "--now", f"{RAW_JSON_CLEANUP_SYSTEMD_BASENAME}.timer"], check=False, capture_output=True, text=True)
        for path in (service_path, timer_path):
            if path.exists():
                path.unlink()
        if activate:
            subprocess.run(["systemctl", "--user", "daemon-reload"], check=True, capture_output=True, text=True)
        return {
            "removed": True,
            "activated": activate,
            "service_path": str(service_path),
            "timer_path": str(timer_path),
            "last_error": None,
        }
    except Exception as exc:
        return {"removed": False, "activated": activate, "last_error": str(exc)}


def _systemd_user_dir(config: "AppConfig") -> Path:
    return config.daily.systemd_user_dir or (Path.home() / ".config" / "systemd" / "user")


def _raw_json_cleanup_service(config: "AppConfig", *, retention_days: int | None, cli_path: str | None, vacuum: bool) -> str:
    config_path = str(config.config_path)
    command = [
        "/usr/bin/env",
        cli_path or config.daily.cli_path,
        "--workspace",
        str(app_workspace_dir(config)),
        "--config",
        config_path,
        "cleanup-raw-json",
    ]
    if retention_days is not None:
        command.extend(["--retention-days", str(retention_days)])
    if vacuum:
        command.append("--vacuum")
    exec_start = shlex.join(command)
    return "\n".join(
        [
            "[Unit]",
            "Description=tele-mess-core raw JSON cleanup",
            "",
            "[Service]",
            "Type=oneshot",
            f"WorkingDirectory={shlex.quote(str(app_workspace_dir(config)))}",
            "Environment=PYTHONUNBUFFERED=1",
            f"ExecStart={exec_start}",
            "",
        ]
    )


def _raw_json_cleanup_timer(*, on_calendar: str) -> str:
    return "\n".join(
        [
            "[Unit]",
            "Description=Run tele-mess-core raw JSON cleanup",
            "",
            "[Timer]",
            f"OnCalendar={on_calendar}",
            "Persistent=true",
            f"Unit={RAW_JSON_CLEANUP_SYSTEMD_BASENAME}.service",
            "",
            "[Install]",
            "WantedBy=timers.target",
            "",
        ]
    )
