from __future__ import annotations

import asyncio
import inspect
import json
import logging
from pathlib import Path
import threading
from typing import Any, Callable

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import AppConfig, TelegramAccountConfig
from tele_mess_core.models import AccountAuthRecord, OperationEventRecord, SOURCE_TELEGRAM, utc_now_iso
from tele_mess_core.telegram.auth import TelegramAuthService
from tele_mess_core.telegram.delivery import TelegramSummaryDeliveryService
from tele_mess_core.telegram.discovery import TelegramDiscoveryService
from tele_mess_core.telegram.ingest import TelegramArchiveService


ClientFactory = Callable[[TelegramAccountConfig], Any]


class TelegramAccountRuntime:
    """Own the single long-lived Telegram client for one configured account."""

    def __init__(
        self,
        config: TelegramAccountConfig,
        app_config: AppConfig,
        store: ArchiveStore,
        *,
        client_factory: ClientFactory,
    ) -> None:
        self.config = config
        self.app_config = app_config
        self.store = store
        self.client_factory = client_factory
        self.logger = logging.getLogger(__name__)
        self.client: Any | None = None
        self._command_lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._stopping = asyncio.Event()
        self._supervisor_task: asyncio.Task[None] | None = None
        self._ingest_task: asyncio.Task[None] | None = None
        self._ingest_service: TelegramArchiveService | None = None

    async def start(self) -> None:
        if self._supervisor_task and not self._supervisor_task.done():
            return
        self._supervisor_task = asyncio.create_task(
            self._supervise_connection(),
            name=f"telegram-account-{self.config.account_id}",
        )

    async def stop(self) -> None:
        self._stopping.set()
        await self._stop_ingestion()
        client = self.client
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                self.logger.exception("Failed to disconnect Telegram account %s", self.config.account_id)
        task = self._supervisor_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.client = None
        self._ready.clear()

    async def execute(self, operation: str, **kwargs: Any) -> dict[str, Any]:
        await self._ready.wait()
        async with self._command_lock:
            client = self.client
            if client is None:
                raise RuntimeError(f"Telegram account {self.config.account_id} is not connected")
            if operation == "auth_status":
                result = await TelegramAuthService(self.config, self.store).status(client=client)
                if result.get("authorized"):
                    self._schedule_ingestion(client)
                return result
            if operation == "request_code":
                return await TelegramAuthService(self.config, self.store).request_code(
                    str(kwargs["phone"]),
                    client=client,
                )
            if operation == "submit_code":
                result = await TelegramAuthService(self.config, self.store).submit_code(
                    str(kwargs["phone"]),
                    str(kwargs["code"]),
                    kwargs.get("password"),
                    client=client,
                )
                if result.get("authorized"):
                    self._schedule_ingestion(client)
                return result
            if operation == "discover_origins":
                return await TelegramDiscoveryService(self.config, self.store).discover_origins(
                    include_topics=bool(kwargs.get("include_topics", True)),
                    topic_limit=int(kwargs.get("topic_limit", 100)),
                    include_private=bool(kwargs.get("include_private", False)),
                    client=client,
                )
            if operation == "refresh_participants":
                return await TelegramDiscoveryService(self.config, self.store).refresh_participants(
                    int(kwargs["origin_id"]),
                    int(kwargs.get("limit", 500)),
                    client=client,
                )
            if operation == "deliver_summary":
                return await TelegramSummaryDeliveryService(self.config, self.store).send_summary(
                    kwargs["delivery"],
                    str(kwargs.get("content") or ""),
                    client=client,
                )
            if operation == "deliver_chunk":
                return await TelegramSummaryDeliveryService(self.config, self.store).send_summary(
                    kwargs["delivery"],
                    str(kwargs.get("content") or ""),
                    client=client,
                    split_content=False,
                )
            if operation == "refresh_capture":
                if not await client.is_user_authorized():
                    return {"account_id": self.config.account_id, "status": "needs_login"}
                was_attached = self._ingest_service is not None
                self._schedule_ingestion(client)
                task = self._ingest_task
                if task is not None:
                    await asyncio.shield(task)
                if was_attached and self._ingest_service is not None:
                    await self._ingest_service.refresh_capture_targets()
                return {"account_id": self.config.account_id, "status": "refreshed"}
            raise ValueError(f"Unknown Telegram runtime operation: {operation}")

    def status(self) -> dict[str, Any]:
        supervisor = self._supervisor_task
        return {
            "account_id": self.config.account_id,
            "connected": self._ready.is_set() and self.client is not None,
            "supervisor_running": bool(supervisor and not supervisor.done()),
            "ingest_running": self._ingest_service is not None,
        }

    async def _supervise_connection(self) -> None:
        delay = 1.0
        while not self._stopping.is_set():
            client = None
            try:
                client = self.client_factory(self.config)
                if inspect.isawaitable(client):
                    client = await client
                await client.connect()
                self.client = client
                self._ready.set()
                delay = 1.0
                authorized = await client.is_user_authorized()
                self._record_auth_state("authorized" if authorized else "needs_login")
                if authorized:
                    self._schedule_ingestion(client)
                disconnected = getattr(client, "disconnected", None)
                if inspect.isawaitable(disconnected):
                    await disconnected
                else:
                    await self._stopping.wait()
                if not self._stopping.is_set():
                    raise RuntimeError("Telegram client disconnected")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._record_runtime_failure(exc)
                if self._stopping.is_set():
                    break
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=delay)
                except TimeoutError:
                    pass
                delay = min(delay * 2, 60.0)
            finally:
                self._ready.clear()
                await self._stop_ingestion()
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        self.logger.debug("Telegram client disconnect cleanup failed", exc_info=True)
                if self.client is client:
                    self.client = None

    def _schedule_ingestion(self, client: Any) -> None:
        if self._ingest_service is not None:
            return
        service = TelegramArchiveService(
            self.config,
            self.store,
            self.app_config.telegram.backfill,
            self.app_config.telegram.media_download,
        )
        self._ingest_service = service
        self._ingest_task = asyncio.create_task(
            service.attach(client),
            name=f"telegram-ingest-attach-{self.config.account_id}",
        )
        self._ingest_task.add_done_callback(self._ingest_done)

    def _ingest_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled() or self._stopping.is_set():
            return
        exc = task.exception()
        if exc is not None:
            if self._ingest_task is task:
                self._ingest_task = None
            self._ingest_service = None
            self._record_runtime_failure(exc, operation="ingest_attach")

    async def _stop_ingestion(self) -> None:
        task = self._ingest_task
        self._ingest_task = None
        self._ingest_service = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def _record_auth_state(self, state: str, last_error: str | None = None) -> None:
        self.store.upsert_account_auth(
            AccountAuthRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.config.account_id,
                auth_state=state,
                session_name=self.config.session_name,
                session_dir=str(self.config.session_dir),
                last_error=last_error,
                updated_at=utc_now_iso(),
            )
        )

    def _record_runtime_failure(self, exc: Exception, operation: str = "account_runtime") -> None:
        message = str(exc) or exc.__class__.__name__
        self.logger.warning("Telegram runtime failure for account %s: %s", self.config.account_id, message)
        self._record_auth_state("error", last_error=message)
        self.store.add_operation_event(
            OperationEventRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.config.account_id,
                operation=operation,
                status="failed",
                error_code="telegram_runtime_failed",
                message=message,
                occurred_at=utc_now_iso(),
                raw_json=json.dumps({"error_type": exc.__class__.__name__}, ensure_ascii=False),
            )
        )


class TelegramRuntimeManager:
    """Bridge threaded HTTP callers to account runtimes on the main event loop."""

    def __init__(
        self,
        config: AppConfig,
        store: ArchiveStore,
        *,
        client_factory: ClientFactory | None = None,
        call_timeout: float = 300.0,
    ) -> None:
        self.config = config
        self.store = store
        self.client_factory = client_factory or _default_client_factory
        self.call_timeout = call_timeout
        self.logger = logging.getLogger(__name__)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread_id: int | None = None
        self._runtimes: dict[str, TelegramAccountRuntime] = {}
        self._configs = {item.account_id: item for item in config.telegram.accounts}

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._loop_thread_id = threading.get_ident()
        configs = dict(self._configs)
        for item in self.store.list_management_accounts():
            account_id = str(item.get("account_id") or "")
            if account_id and account_id not in configs:
                configs[account_id] = self._stored_account_config(item)
        for account in configs.values():
            await self._register_account(account)

    async def stop(self) -> None:
        runtimes = list(self._runtimes.values())
        self._runtimes.clear()
        await asyncio.gather(*(runtime.stop() for runtime in runtimes), return_exceptions=True)
        self._loop = None
        self._loop_thread_id = None

    def call(self, account_id: str, operation: str, **kwargs: Any) -> dict[str, Any]:
        loop = self._require_loop()
        if threading.get_ident() == self._loop_thread_id:
            raise RuntimeError("Use execute() when calling TelegramRuntimeManager from its event loop")
        future = asyncio.run_coroutine_threadsafe(self.execute(account_id, operation, **kwargs), loop)
        return self._wait_for_future(future)

    def notify(self, account_id: str, operation: str, **kwargs: Any) -> None:
        loop = self._require_loop()
        future = asyncio.run_coroutine_threadsafe(self.execute(account_id, operation, **kwargs), loop)

        def done(completed: Any) -> None:
            try:
                completed.result()
            except Exception:
                self.logger.exception(
                    "Background Telegram runtime operation failed: account=%s operation=%s",
                    account_id,
                    operation,
                )

        future.add_done_callback(done)

    async def execute(self, account_id: str, operation: str, **kwargs: Any) -> dict[str, Any]:
        runtime = self._runtimes.get(account_id)
        if runtime is None:
            runtime = await self._register_account(self._account_config(account_id))
        return await runtime.execute(operation, **kwargs)

    def register_account(self, config: TelegramAccountConfig) -> dict[str, Any]:
        loop = self._require_loop()
        future = asyncio.run_coroutine_threadsafe(self._register_account(config), loop)
        runtime = self._wait_for_future(future)
        return runtime.status()

    def unregister_account(self, account_id: str) -> dict[str, Any]:
        loop = self._require_loop()
        future = asyncio.run_coroutine_threadsafe(self._unregister_account(account_id), loop)
        return self._wait_for_future(future)

    def statuses(self) -> list[dict[str, Any]]:
        return [runtime.status() for runtime in self._runtimes.values()]

    async def _register_account(self, config: TelegramAccountConfig) -> TelegramAccountRuntime:
        existing = self._runtimes.get(config.account_id)
        if existing is not None:
            return existing
        runtime = TelegramAccountRuntime(
            config,
            self.config,
            self.store,
            client_factory=self.client_factory,
        )
        self._runtimes[config.account_id] = runtime
        await runtime.start()
        return runtime

    async def _unregister_account(self, account_id: str) -> dict[str, Any]:
        runtime = self._runtimes.pop(account_id, None)
        if runtime is None:
            return {"account_id": account_id, "stopped": False}
        await runtime.stop()
        return {"account_id": account_id, "stopped": True}

    def _account_config(self, account_id: str) -> TelegramAccountConfig:
        configured = self._configs.get(account_id)
        if configured is not None:
            return configured
        for item in self.store.list_management_accounts():
            if str(item.get("account_id") or "") == account_id:
                return self._stored_account_config(item)
        raise ValueError(f"Unknown account_id: {account_id}")

    def _stored_account_config(self, item: dict[str, Any]) -> TelegramAccountConfig:
        if not self.config.telegram.accounts:
            raise ValueError("At least one configured Telegram account is required as an API credential template")
        template = self.config.telegram.accounts[0]
        account_id = str(item["account_id"])
        session_dir_raw = item.get("session_dir")
        session_dir = Path(str(session_dir_raw)).expanduser() if session_dir_raw else template.session_dir
        return TelegramAccountConfig(
            account_id=account_id,
            api_id=template.api_id,
            api_hash=template.api_hash,
            session_name=str(item.get("session_name") or account_id),
            session_dir=session_dir,
            timezone=template.timezone,
        )

    def _require_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None or self._loop.is_closed():
            raise RuntimeError("Telegram runtime manager is not running")
        return self._loop

    def _wait_for_future(self, future: Any) -> Any:
        try:
            return future.result(timeout=self.call_timeout)
        except BaseException:
            future.cancel()
            raise


def _default_client_factory(config: TelegramAccountConfig) -> Any:
    from telethon import TelegramClient

    config.session_dir.mkdir(parents=True, exist_ok=True)
    session_file = config.session_dir / config.session_name
    return TelegramClient(str(session_file), config.api_id, config.api_hash)
