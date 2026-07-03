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
    add_config_arg(sub.add_parser("serve-api", help="Run only the read-only sync API"))
    add_config_arg(sub.add_parser("run-telegram", help="Run only Telegram ingestion"))
    add_config_arg(sub.add_parser("run-server", help="Run Telegram ingestion and sync API"))
    smoke_parser = sub.add_parser("smoke-telegram", help="Check Telegram account auth and optional live discovery")
    add_config_arg(smoke_parser)
    smoke_parser.add_argument("--account-id", help="Configured Telegram account ID to check")
    smoke_parser.add_argument("--discover-origins", action="store_true", help="Run live origin discovery after auth status")
    smoke_parser.add_argument("--topic-limit", type=int, default=20, help="Max forum topics to inspect during discovery")

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
    api = SyncApiServer(store, config.server.host, config.server.port, config.server.token, config)
    try:
        api.serve_forever()
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Stopping API server")
    finally:
        api.stop()
    return 0


def _sync_configured_accounts(store: ArchiveStore, config: AppConfig) -> None:
    now = utc_now_iso()
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
    from tele_mess_core.telegram import TelegramArchiveService

    if not config.telegram.accounts:
        raise RuntimeError("No telegram.accounts configured")
    services = [
        TelegramArchiveService(account, store, config.telegram.backfill, config.telegram.media_download)
        for account in config.telegram.accounts
    ]
    await asyncio.gather(*(service.run() for service in services))


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
    api = SyncApiServer(store, config.server.host, config.server.port, config.server.token, config)
    api.start_background()
    stop_event = asyncio.Event()

    def request_stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_stop)
        except NotImplementedError:
            pass

    telegram_task = asyncio.create_task(_run_telegram(config, store))
    stop_task = asyncio.create_task(stop_event.wait())
    done, pending = await asyncio.wait(
        {telegram_task, stop_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    api.stop()
    for task in done:
        exc = task.exception()
        if exc:
            raise exc


def _run_async(coro: object) -> int:
    try:
        result = asyncio.run(coro)  # type: ignore[arg-type]
        return result if isinstance(result, int) else 0
    except KeyboardInterrupt:
        return 130


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
