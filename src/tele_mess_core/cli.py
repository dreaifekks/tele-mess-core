from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
from pathlib import Path
from typing import TYPE_CHECKING

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.models import (
    AccountAuthRecord,
    AccountRecord,
    MessageRecord,
    SOURCE_TELEGRAM,
    utc_now_iso,
)
from tele_mess_core.server import SyncApiServer

if TYPE_CHECKING:
    from tele_mess_core.config import AppConfig


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Telegram message archive core")
    parser.add_argument("--config", default="config.yml", help="Path to config YAML")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_config_arg(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--config", default=argparse.SUPPRESS, help=argparse.SUPPRESS)

    add_config_arg(sub.add_parser("init-db", help="Initialize the SQLite archive"))
    add_config_arg(sub.add_parser("serve-api", help="Run HTTP API and durable daily worker without Telegram ingestion"))
    add_config_arg(sub.add_parser("run-telegram", help="Run only Telegram ingestion"))
    add_config_arg(sub.add_parser("run-server", help="Run Telegram ingestion and sync API"))
    smoke_parser = sub.add_parser("smoke-telegram", help="Check Telegram account auth and optional live discovery")
    add_config_arg(smoke_parser)
    smoke_parser.add_argument("--account-id", help="Configured Telegram account ID to check")
    smoke_parser.add_argument("--discover-origins", action="store_true", help="Run live origin discovery after auth status")
    smoke_parser.add_argument("--topic-limit", type=int, default=20, help="Max forum topics to inspect during discovery")

    daily_package_parser = sub.add_parser("daily-package", help="Generate a daily package from archived messages")
    add_config_arg(daily_package_parser)
    daily_package_parser.add_argument("--date", help="Local package date in YYYY-MM-DD. Defaults to yesterday in the target timezone.")
    daily_package_parser.add_argument("--timezone", help="Timezone for the local day window")
    daily_package_parser.add_argument("--account-id", help="Filter to one account")
    daily_package_parser.add_argument("--origin-id", type=int, help="Filter to one origin")
    daily_package_parser.add_argument("--topic-id", type=int, help="Filter to one topic")
    daily_package_parser.add_argument("--tags", help="Comma-separated tags that all selected origins must have")
    daily_package_parser.add_argument("--tag-group", action="append", default=[], help="Tag group such as 'web3 info'. Repeatable.")
    daily_package_parser.add_argument("--scheduled", action="store_true", help="Use saved daily package schedule scope/timezone")

    daily_summary_parser = sub.add_parser("daily-summary", help="Run AI analysis for a daily package")
    add_config_arg(daily_summary_parser)
    daily_summary_parser.add_argument("--package-run-id", help="Daily package run ID to summarize")
    daily_summary_parser.add_argument("--date", help="Local package date in YYYY-MM-DD when no package run is supplied")
    daily_summary_parser.add_argument("--timezone", help="Timezone for the local day window")
    daily_summary_parser.add_argument("--account-id", help="Filter to one account when building a package first")
    daily_summary_parser.add_argument("--origin-id", type=int, help="Filter to one origin when building a package first")
    daily_summary_parser.add_argument("--topic-id", type=int, help="Filter to one topic when building a package first")
    daily_summary_parser.add_argument("--tags", help="Comma-separated tags when building a package first")
    daily_summary_parser.add_argument("--tag-group", action="append", default=[], help="Tag group such as 'web3 info'. Repeatable.")
    daily_summary_parser.add_argument("--force", action="store_true", help="Run even when an identical completed job exists")

    daily_run_parser = sub.add_parser("daily-run", help="Generate a daily package and then run its AI summary")
    add_config_arg(daily_run_parser)
    daily_run_parser.add_argument("--date", help="Local package date in YYYY-MM-DD. Defaults to yesterday in the target timezone.")
    daily_run_parser.add_argument("--timezone", help="Timezone for the local day window")
    daily_run_parser.add_argument("--account-id", help="Filter to one account")
    daily_run_parser.add_argument("--origin-id", type=int, help="Filter to one origin")
    daily_run_parser.add_argument("--topic-id", type=int, help="Filter to one topic")
    daily_run_parser.add_argument("--tags", help="Comma-separated tags that all selected origins must have")
    daily_run_parser.add_argument("--tag-group", action="append", default=[], help="Tag group such as 'web3 info'. Repeatable.")
    daily_run_parser.add_argument("--scheduled", action="store_true", help="Use saved daily package schedule scope/timezone")
    daily_run_parser.add_argument("--force", action="store_true", help="Run even when an identical completed job exists")

    daily_schedule_parser = sub.add_parser("daily-schedule", help="Install or remove the system daily package timer")
    add_config_arg(daily_schedule_parser)
    daily_schedule_sub = daily_schedule_parser.add_subparsers(dest="daily_schedule_command", required=True)
    daily_schedule_install = daily_schedule_sub.add_parser("install", help="Write and optionally activate the systemd user timer")
    daily_schedule_install.add_argument("--activate-systemd", action="store_true", help="Run systemctl --user enable/disable after writing timer files")
    daily_schedule_remove = daily_schedule_sub.add_parser("remove", help="Disable the saved daily schedule and rewrite timer files")
    daily_schedule_remove.add_argument("--activate-systemd", action="store_true", help="Run systemctl --user disable after writing timer files")

    cleanup_parser = sub.add_parser("cleanup-raw-json", help="Clear old raw Telegram JSON payloads from message rows")
    add_config_arg(cleanup_parser)
    cleanup_parser.add_argument("--retention-days", type=int, help="Days of messages.raw_json to keep. Defaults to storage.raw_json_retention_days.")
    cleanup_parser.add_argument("--cutoff-sent-at", help="Explicit sent_at cutoff; raw_json older than this value is cleared.")
    cleanup_parser.add_argument("--dry-run", action="store_true", help="Report eligible rows without modifying the database")
    cleanup_parser.add_argument("--vacuum", action="store_true", help="Run VACUUM after cleanup so the database file shrinks immediately")

    cleanup_schedule_parser = sub.add_parser("raw-json-cleanup-schedule", help="Install or remove the system raw JSON cleanup timer")
    add_config_arg(cleanup_schedule_parser)
    cleanup_schedule_sub = cleanup_schedule_parser.add_subparsers(dest="raw_json_cleanup_schedule_command", required=True)
    cleanup_schedule_install = cleanup_schedule_sub.add_parser("install", help="Write and optionally activate the systemd user timer")
    cleanup_schedule_install.add_argument("--retention-days", type=int, help="Days of messages.raw_json to keep. Defaults to storage.raw_json_retention_days.")
    cleanup_schedule_install.add_argument("--on-calendar", default="weekly", help="systemd OnCalendar expression. Defaults to weekly.")
    cleanup_schedule_install.add_argument("--cli-path", help="Executable path to write into the systemd service. Defaults to daily.cli_path.")
    cleanup_schedule_install.add_argument("--vacuum", action="store_true", help="Run VACUUM after scheduled cleanup")
    cleanup_schedule_install.add_argument("--activate-systemd", action="store_true", help="Run systemctl --user enable --now after writing timer files")
    cleanup_schedule_remove = cleanup_schedule_sub.add_parser("remove", help="Remove the systemd user timer files")
    cleanup_schedule_remove.add_argument("--activate-systemd", action="store_true", help="Run systemctl --user disable --now before removing timer files")

    import_parser = sub.add_parser("import-ndjson", help="Import legacy .bak NDJSON messages")
    add_config_arg(import_parser)
    import_parser.add_argument("path", help="NDJSON backup file")
    import_parser.add_argument("--chat-id", type=int, required=True, help="Chat ID to attach imported messages to")
    import_parser.add_argument("--account-id", default="default", help="Account ID for imported messages")

    docs_parser = sub.add_parser("generate-api-docs", help="Generate static API reference files")
    docs_parser.add_argument("--output-dir", default="docs", help="Directory for generated docs")
    docs_parser.add_argument("--check", action="store_true", help="Fail if generated docs are not current")

    args = parser.parse_args(argv)
    if args.command == "generate-api-docs":
        return _generate_api_docs(Path(args.output_dir), args.check)

    from tele_mess_core.config import load_config
    from tele_mess_core.logging_setup import setup_logging

    config = load_config(args.config)
    logger = setup_logging(config.logging)

    store = ArchiveStore(config.storage.database)
    store.initialize()
    _sync_configured_accounts(store, config)

    try:
        if args.command == "init-db":
            logger.info("Initialized archive at %s", config.storage.database)
            return 0
        if args.command == "serve-api":
            return _serve_api(config, store)
        if args.command == "run-telegram":
            return _run_async(_run_telegram(config, store))
        if args.command == "run-server":
            return _run_async(_run_server(config, store))
        if args.command == "smoke-telegram":
            return _run_async(_smoke_telegram(config, store, args.account_id, args.discover_origins, args.topic_limit))
        if args.command == "daily-package":
            _daily_package(config, store, args)
            return 0
        if args.command == "daily-summary":
            _daily_summary(config, store, args)
            return 0
        if args.command == "daily-run":
            return _daily_run(config, store, args)
        if args.command == "daily-schedule":
            _daily_schedule(config, store, args)
            return 0
        if args.command == "cleanup-raw-json":
            _cleanup_raw_json(config, store, args)
            return 0
        if args.command == "raw-json-cleanup-schedule":
            _raw_json_cleanup_schedule(config, args)
            return 0
        if args.command == "import-ndjson":
            _import_ndjson(store, Path(args.path), args.chat_id, args.account_id)
            logger.info("Imported %s", args.path)
            return 0
    finally:
        store.close()

    parser.error(f"Unknown command: {args.command}")
    return 2


def _generate_api_docs(output_dir: Path, check: bool = False) -> int:
    from tele_mess_core.server.contracts import agent_markdown_document, markdown_document, openapi_json

    files = {
        output_dir / "api.md": markdown_document(),
        output_dir / "api-agent.md": agent_markdown_document(),
        output_dir / "openapi.json": openapi_json(),
    }
    if check:
        stale: list[Path] = []
        missing: list[Path] = []
        for path, expected in files.items():
            if not path.exists():
                missing.append(path)
                continue
            if path.read_text(encoding="utf-8") != expected:
                stale.append(path)
        if missing or stale:
            for path in missing:
                print(f"missing generated API doc: {path}")
            for path in stale:
                print(f"stale generated API doc: {path}")
            return 1
        print("API docs are current")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    for path, content in files.items():
        path.write_text(content, encoding="utf-8")
        print(f"wrote {path}")
    return 0


def _serve_api(config: AppConfig, store: ArchiveStore) -> int:
    from tele_mess_core.daily_jobs import DailyJobWorker

    daily_worker = DailyJobWorker(store, config)
    api = SyncApiServer(
        store,
        config.server.host,
        config.server.port,
        config.server.token,
        config,
        allow_unauthenticated_localhost=config.server.allow_unauthenticated_localhost,
        daily_worker=daily_worker,
    )
    daily_worker.start()
    try:
        api.serve_forever()
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Stopping API server")
    finally:
        api.stop()
        daily_worker.stop()
    return 0


def _sync_configured_accounts(store: ArchiveStore, config: AppConfig) -> None:
    now = utc_now_iso()
    existing = {
        (str(item.get("source") or ""), str(item.get("account_id") or "")): item
        for item in store.list_management_accounts()
    }
    for account in config.telegram.accounts:
        store.upsert_account(
            AccountRecord(
                source=SOURCE_TELEGRAM,
                account_id=account.account_id,
                display_name=account.account_id,
                kind="telegram",
                updated_at=now,
            )
        )
        current = existing.get((SOURCE_TELEGRAM, account.account_id))
        if current is None or current.get("auth_state") in {None, "unknown"}:
            session_present = _telethon_session_exists(account.session_dir, account.session_name)
            store.upsert_account_auth(
                AccountAuthRecord(
                    source=SOURCE_TELEGRAM,
                    account_id=account.account_id,
                    auth_state="session_present" if session_present else "needs_login",
                    session_name=account.session_name,
                    session_dir=str(account.session_dir),
                    updated_at=now,
                )
            )


def _telethon_session_exists(session_dir: Path, session_name: str) -> bool:
    base = session_dir / session_name
    return base.exists() or base.with_suffix(".session").exists()


async def _run_telegram(config: AppConfig, store: ArchiveStore) -> None:
    from tele_mess_core.telegram.manager import TelegramRuntimeManager

    if not config.telegram.accounts:
        raise RuntimeError("No telegram.accounts configured")
    runtime = TelegramRuntimeManager(config, store)
    stop_event = asyncio.Event()
    _install_stop_handlers(stop_event)
    await runtime.start()
    try:
        await stop_event.wait()
    finally:
        await runtime.stop()


async def _smoke_telegram(
    config: AppConfig,
    store: ArchiveStore,
    account_id: str | None,
    discover_origins: bool,
    topic_limit: int,
) -> int:
    from tele_mess_core.telegram.auth import TelegramAuthService
    from tele_mess_core.telegram.discovery import TelegramDiscoveryService

    if not config.telegram.accounts:
        raise RuntimeError("No telegram.accounts configured")
    account = config.telegram.accounts[0]
    if account_id:
        matches = [item for item in config.telegram.accounts if item.account_id == account_id]
        if not matches:
            raise RuntimeError(f"Unknown account_id: {account_id}")
        account = matches[0]

    status = await TelegramAuthService(account, store).status()
    result: dict[str, object] = {
        "account_id": account.account_id,
        "session_name": account.session_name,
        "session_dir": str(account.session_dir),
        "status": status,
    }
    if discover_origins and status.get("authorized"):
        result["discovery"] = await TelegramDiscoveryService(account, store).discover_origins(
            include_topics=True,
            topic_limit=topic_limit,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if status.get("authorized") else 1


async def _run_server(config: AppConfig, store: ArchiveStore) -> None:
    from tele_mess_core.daily_jobs import DailyJobWorker
    from tele_mess_core.telegram.manager import TelegramRuntimeManager

    runtime = TelegramRuntimeManager(config, store)
    daily_worker: DailyJobWorker | None = None
    api: SyncApiServer | None = None
    stop_event = asyncio.Event()
    _install_stop_handlers(stop_event)
    stop_task: asyncio.Task[bool] | None = None
    api_task: asyncio.Task[None] | None = None
    try:
        await runtime.start()
        daily_worker = DailyJobWorker(store, config, telegram_runtime=runtime)
        api = SyncApiServer(
            store,
            config.server.host,
            config.server.port,
            config.server.token,
            config,
            allow_unauthenticated_localhost=config.server.allow_unauthenticated_localhost,
            telegram_runtime=runtime,
            daily_worker=daily_worker,
        )
        daily_worker.start()
        api.start_background()
        stop_task = asyncio.create_task(stop_event.wait(), name="server-stop-signal")
        api_task = asyncio.create_task(api.wait_stopped(), name="sync-api-stopped")
        done, pending = await asyncio.wait({stop_task, api_task}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if api_task in done and not stop_event.is_set():
            raise RuntimeError("Sync API stopped unexpectedly")
    finally:
        if api is not None:
            api.stop()
        if daily_worker is not None:
            daily_worker.stop()
        await runtime.stop()
        for task in (stop_task, api_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (stop_task, api_task) if task is not None),
            return_exceptions=True,
        )


def _install_stop_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_stop)
        except NotImplementedError:
            pass


def _run_async(coro: object) -> int:
    try:
        result = asyncio.run(coro)  # type: ignore[arg-type]
        return result if isinstance(result, int) else 0
    except KeyboardInterrupt:
        return 130


def _daily_package(config: AppConfig, store: ArchiveStore, args: argparse.Namespace) -> None:
    from tele_mess_core.daily import build_daily_package

    schedule = store.get_daily_package_schedule() if args.scheduled else {}
    scope = dict(schedule.get("scope") or {}) if args.scheduled else {}
    scope.update(_daily_scope_from_args(args))
    timezone_name = args.timezone or schedule.get("timezone") or scope.get("timezone")
    item = build_daily_package(store, config, run_date=args.date, timezone_name=timezone_name, scope=scope)
    print(json.dumps(item, ensure_ascii=False, indent=2, default=str))


def _daily_summary(config: AppConfig, store: ArchiveStore, args: argparse.Namespace) -> None:
    from tele_mess_core.daily_jobs import DailyJobWorker

    scope = _daily_scope_from_args(args)
    worker = DailyJobWorker(store, config)
    job = worker.enqueue(
        package_run_id=args.package_run_id,
        run_date=args.date,
        timezone_name=args.timezone,
        scope=scope,
        force=bool(args.force),
    )
    terminal = worker.wait_for_terminal(str(job["job_id"]), timeout=max(300, config.daily.ai.timeout_seconds * 20))
    item = store.get_daily_summary_run(str(terminal.get("summary_run_id") or "")) or terminal
    print(json.dumps(item, ensure_ascii=False, indent=2, default=str))


def _daily_run(config: AppConfig, store: ArchiveStore, args: argparse.Namespace) -> int:
    from tele_mess_core.daily_jobs import DailyJobWorker

    schedule = store.get_daily_package_schedule() if args.scheduled else {}
    scope = dict(schedule.get("scope") or {}) if args.scheduled else {}
    scope.update(_daily_scope_from_args(args))
    timezone_name = args.timezone or schedule.get("timezone") or scope.get("timezone")
    worker = DailyJobWorker(store, config)
    job = worker.enqueue(
        run_date=args.date,
        timezone_name=timezone_name,
        scope=scope,
        force=bool(args.force),
    )
    terminal = worker.wait_for_terminal(str(job["job_id"]), timeout=max(300, config.daily.ai.timeout_seconds * 20))
    package = store.get_daily_package_run(str(terminal.get("package_run_id") or ""))
    summary = store.get_daily_summary_run(str(terminal.get("summary_run_id") or ""))
    item = {
        "status": terminal.get("status"),
        "job_id": terminal.get("job_id"),
        "package_run_id": terminal.get("package_run_id"),
        "summary_run_id": terminal.get("summary_run_id"),
        "package": package,
        "summary": summary,
        "error": terminal.get("error"),
    }
    print(json.dumps(item, ensure_ascii=False, indent=2, default=str))
    return 0 if item.get("status") == "completed" else 1


def _daily_schedule(config: AppConfig, store: ArchiveStore, args: argparse.Namespace) -> None:
    from tele_mess_core.daily import update_daily_package_schedule

    current = store.get_daily_package_schedule()
    payload = dict(current)
    payload["enabled"] = args.daily_schedule_command == "install"
    payload["activate_systemd"] = bool(args.activate_systemd)
    item = update_daily_package_schedule(store, config, payload)
    print(json.dumps(item, ensure_ascii=False, indent=2, default=str))


def _cleanup_raw_json(config: AppConfig, store: ArchiveStore, args: argparse.Namespace) -> None:
    from tele_mess_core.maintenance import cleanup_message_raw_json

    retention_days = args.retention_days or config.storage.raw_json_retention_days
    item = cleanup_message_raw_json(
        store,
        retention_days=retention_days,
        cutoff_sent_at=args.cutoff_sent_at,
        dry_run=bool(args.dry_run),
        vacuum=bool(args.vacuum),
    )
    print(json.dumps(item, ensure_ascii=False, indent=2, default=str))


def _raw_json_cleanup_schedule(config: AppConfig, args: argparse.Namespace) -> None:
    from tele_mess_core.maintenance import install_raw_json_cleanup_timer, remove_raw_json_cleanup_timer

    if args.raw_json_cleanup_schedule_command == "install":
        item = install_raw_json_cleanup_timer(
            config,
            retention_days=args.retention_days,
            on_calendar=args.on_calendar,
            cli_path=args.cli_path,
            vacuum=bool(args.vacuum),
            activate=bool(args.activate_systemd),
        )
    else:
        item = remove_raw_json_cleanup_timer(config, activate=bool(args.activate_systemd))
    print(json.dumps(item, ensure_ascii=False, indent=2, default=str))


def _daily_scope_from_args(args: argparse.Namespace) -> dict[str, object]:
    scope: dict[str, object] = {}
    if getattr(args, "account_id", None):
        scope["account_id"] = args.account_id
    if getattr(args, "origin_id", None) is not None:
        scope["origin_id"] = args.origin_id
    if getattr(args, "topic_id", None) is not None:
        scope["topic_id"] = args.topic_id
    if getattr(args, "tags", None):
        scope["tags"] = args.tags
    if getattr(args, "tag_group", None):
        scope["tag_groups"] = args.tag_group
    return scope


def _import_ndjson(store: ArchiveStore, path: Path, chat_id: int, account_id: str) -> None:
    now = utc_now_iso()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            raw = json.loads(line)
            message_id = int(raw["id"])
            record = MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id=account_id,
                chat_id=chat_id,
                message_id=message_id,
                sender_id=raw.get("sender_id"),
                sent_at=raw.get("date") or now,
                ingested_at=now,
                text=raw.get("text"),
                reply_to_message_id=raw.get("reply_to"),
                raw_json=json.dumps(raw, ensure_ascii=False, default=str),
            )
            store.upsert_message(record, event_type="import")


if __name__ == "__main__":
    raise SystemExit(main())
