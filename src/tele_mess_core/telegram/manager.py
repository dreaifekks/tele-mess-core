from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError as FutureTimeoutError
import inspect
import json
import logging
from pathlib import Path
import threading
import time
from typing import Any, Callable

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import AppConfig, TelegramAccountConfig, resolve_account_config, stored_account_config
from tele_mess_core.models import AccountAuthRecord, OperationEventRecord, SOURCE_TELEGRAM, utc_now_iso
from tele_mess_core.telegram.auth import TelegramAuthService
from tele_mess_core.telegram.delivery import TelegramSummaryDeliveryService
from tele_mess_core.telegram.discovery import TelegramDiscoveryService
from tele_mess_core.telegram.ingest import TelegramArchiveService


ClientFactory = Callable[[TelegramAccountConfig], Any]
INGEST_RETRY_INITIAL_SECONDS = 1.0
CONNECTION_RETRY_INITIAL_SECONDS = 1.0
MANAGEMENT_OPERATIONS = frozenset(
    {"auth_status", "request_code", "submit_code", "discover_origins", "refresh_participants"}
)
DEFAULT_MANAGEMENT_TIMEOUT_SECONDS = 20.0
DEFAULT_DELIVERY_TIMEOUT_SECONDS = 60.0


class TelegramAccountRuntime:
    """Own the single long-lived Telegram client for one configured account."""

    def __init__(
        self,
        config: TelegramAccountConfig,
        app_config: AppConfig,
        store: ArchiveStore,
        *,
        client_factory: ClientFactory,
        ingest: bool = True,
        connect_wait_seconds: float = 5.0,
        management_timeout_seconds: float = DEFAULT_MANAGEMENT_TIMEOUT_SECONDS,
        delivery_timeout_seconds: float = DEFAULT_DELIVERY_TIMEOUT_SECONDS,
    ) -> None:
        self.config = config
        self.app_config = app_config
        self.store = store
        self.client_factory = client_factory
        self.ingest = ingest
        self._connect_wait_seconds = max(0.0, float(connect_wait_seconds))
        self._management_timeout_seconds = max(0.05, float(management_timeout_seconds))
        self._delivery_timeout_seconds = max(0.05, float(delivery_timeout_seconds))
        self.logger = logging.getLogger(__name__)
        self.client: Any | None = None
        self._command_lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._attempt_cv = asyncio.Condition()
        self._attempt_seq = 0
        self._reconnect_now = asyncio.Event()
        self._force_disconnect = asyncio.Event()
        self._stopping = asyncio.Event()
        self._last_connect_error: Exception | None = None
        self._supervisor_task: asyncio.Task[None] | None = None
        self._ingest_task: asyncio.Task[None] | None = None
        self._ingest_retry_task: asyncio.Task[None] | None = None
        self._ingest_service: TelegramArchiveService | None = None
        self._ingest_active = False
        self._ingest_retry_delay = INGEST_RETRY_INITIAL_SECONDS
        self._diagnostic_lock = threading.Lock()
        self._diagnostic_threads: set[threading.Thread] = set()

    async def start(self) -> None:
        if self._supervisor_task and not self._supervisor_task.done():
            return
        self._supervisor_task = asyncio.create_task(
            self._supervise_connection(),
            name=f"telegram-account-{self.config.account_id}",
        )

    async def stop(self) -> None:
        self._stopping.set()
        self._reconnect_now.set()
        await self._notify_attempt()
        await self._stop_ingestion()
        client = self.client
        if client is not None:
            try:
                await asyncio.wait_for(client.disconnect(), timeout=2.0)
            except Exception:
                self.logger.exception("Failed to disconnect Telegram account %s", self.config.account_id)
        task = self._supervisor_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.client = None
        self._ready.clear()
        with self._diagnostic_lock:
            threads = list(self._diagnostic_threads)
        for thread in threads:
            thread.join(timeout=2.0)

    async def execute(self, operation: str, **kwargs: Any) -> dict[str, Any]:
        if operation == "refresh_capture" and not self.ingest:
            return {"account_id": self.config.account_id, "status": "ingest_disabled"}
        budget = self._operation_timeout(operation)
        deadline = None if budget is None else time.monotonic() + budget
        connect_timeout = self._connect_wait_seconds
        if deadline is not None:
            connect_timeout = min(connect_timeout, max(0.0, deadline - time.monotonic()))
        await self._wait_for_connection(timeout=connect_timeout)
        if deadline is None:
            async with self._command_lock:
                return await self._execute_locked(operation, **kwargs)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            timeout_exc = TimeoutError(f"Telegram {operation} timed out after {budget:g} seconds")
            return self._offline_result(operation, error=timeout_exc, **kwargs)
        lock_acquired = False

        async def run_with_lock() -> dict[str, Any]:
            nonlocal lock_acquired
            async with self._command_lock:
                lock_acquired = True
                return await self._execute_locked(operation, **kwargs)

        try:
            return await asyncio.wait_for(run_with_lock(), timeout=remaining)
        except TimeoutError:
            timeout_exc = TimeoutError(f"Telegram {operation} timed out after {budget:g} seconds")
            if lock_acquired:
                self._trigger_reconnect_after_timeout(timeout_exc)
            return self._offline_result(operation, error=timeout_exc, **kwargs)

    def _operation_timeout(self, operation: str) -> float | None:
        if operation in MANAGEMENT_OPERATIONS:
            return self._management_timeout_seconds
        if operation == "deliver_chunk":
            return self._delivery_timeout_seconds
        return None

    def _trigger_reconnect_after_timeout(self, exc: Exception) -> None:
        self._last_connect_error = exc
        self._ready.clear()
        self._reconnect_now.set()
        self._force_disconnect.set()

    async def _execute_locked(self, operation: str, **kwargs: Any) -> dict[str, Any]:
        client = self.client
        if client is None or not self._ready.is_set():
            return self._offline_result(operation, **kwargs)
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
        if operation == "deliver_chunk":
            return await TelegramSummaryDeliveryService(self.config, self.store).send_chunk(
                kwargs["delivery"],
                str(kwargs.get("content") or ""),
                client=client,
                random_id=kwargs.get("random_id"),
            )
        if operation == "refresh_capture":
            if not await client.is_user_authorized():
                return {"account_id": self.config.account_id, "status": "needs_login"}
            was_attached = self._ingest_active
            self._schedule_ingestion(client)
            task = self._ingest_task
            if task is not None:
                await asyncio.shield(task)
            if was_attached and self._ingest_service is not None:
                await self._ingest_service.refresh_capture_targets()
            return {"account_id": self.config.account_id, "status": "refreshed"}
        raise ValueError(f"Unknown Telegram runtime operation: {operation}")

    async def _notify_attempt(self) -> None:
        async with self._attempt_cv:
            self._attempt_seq += 1
            self._attempt_cv.notify_all()

    async def _wait_for_connection(self, *, timeout: float | None = None) -> None:
        if self._ready.is_set() or self._stopping.is_set():
            return
        wait_seconds = self._connect_wait_seconds if timeout is None else max(0.0, float(timeout))
        if wait_seconds <= 0:
            self._reconnect_now.set()
            return
        async with self._attempt_cv:
            if self._ready.is_set() or self._stopping.is_set():
                return
            seen = self._attempt_seq
            if seen > 0:
                self._reconnect_now.set()
            try:
                await asyncio.wait_for(
                    self._attempt_cv.wait_for(
                        lambda: self._ready.is_set() or self._stopping.is_set() or self._attempt_seq > seen
                    ),
                    timeout=wait_seconds,
                )
            except TimeoutError:
                pass

    def _offline_result(
        self,
        operation: str,
        *,
        error: Exception | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        exc = error or self._last_connect_error or ConnectionError(
            f"Telegram account {self.config.account_id} is not connected"
        )
        if operation == "auth_status":
            service = TelegramAuthService(self.config, self.store)
            result = service.status_error_result(exc, persist=False)
            self._schedule_offline_diagnostic(lambda: service.status_error_result(exc, persist=True))
            return result
        if operation == "request_code":
            phone = str(kwargs["phone"])
            service = TelegramAuthService(self.config, self.store)
            result = service.request_code_error_result(phone, exc, persist=False)
            self._schedule_offline_diagnostic(lambda: service.request_code_error_result(phone, exc, persist=True))
            return result
        if operation == "submit_code":
            phone = str(kwargs["phone"])
            service = TelegramAuthService(self.config, self.store)
            result = service.submit_code_error_result(phone, exc, persist=False)
            self._schedule_offline_diagnostic(lambda: service.submit_code_error_result(phone, exc, persist=True))
            return result
        if operation == "discover_origins":
            service = TelegramDiscoveryService(self.config, self.store)
            result = service.discover_origins_error_result(exc, persist=False)
            self._schedule_offline_diagnostic(lambda: service.discover_origins_error_result(exc, persist=True))
            return result
        if operation == "refresh_participants":
            origin_id = int(kwargs["origin_id"])
            service = TelegramDiscoveryService(self.config, self.store)
            result = service.refresh_participants_error_result(origin_id, exc, persist=False)
            self._schedule_offline_diagnostic(
                lambda: service.refresh_participants_error_result(origin_id, exc, persist=True)
            )
            return result
        if operation == "deliver_chunk":
            raise TelegramSummaryDeliveryService(self.config, self.store).record_delivery_error(
                kwargs["delivery"],
                exc,
            ) from exc
        if operation == "refresh_capture":
            return {"account_id": self.config.account_id, "status": "offline"}
        raise ValueError(f"Unknown Telegram runtime operation: {operation}")

    def _schedule_offline_diagnostic(self, writer: Callable[[], Any]) -> None:
        """Persist offline diagnostic state off the HTTP response path so SQLite contention never blocks or fails it."""
        if self._stopping.is_set():
            return

        def run() -> None:
            try:
                conn = getattr(self.store, "_conn", None)
                if conn is not None:
                    conn.execute("PRAGMA busy_timeout = 200")
                writer()
            except Exception:
                self.logger.debug("Best-effort offline diagnostic write failed", exc_info=True)
            finally:
                try:
                    self.store.close_thread_connection()
                except Exception:
                    pass
                with self._diagnostic_lock:
                    self._diagnostic_threads.discard(threading.current_thread())

        thread = threading.Thread(
            target=run,
            name=f"telegram-offline-diag-{self.config.account_id}",
            daemon=True,
        )
        with self._diagnostic_lock:
            self._diagnostic_threads = {item for item in self._diagnostic_threads if item.is_alive()}
            self._diagnostic_threads.add(thread)
        thread.start()

    def status(self) -> dict[str, Any]:
        supervisor = self._supervisor_task
        return {
            "account_id": self.config.account_id,
            "connected": self._ready.is_set() and self.client is not None,
            "supervisor_running": bool(supervisor and not supervisor.done()),
            "ingest_running": self._ingest_active,
        }

    async def _supervise_connection(self) -> None:
        delay = CONNECTION_RETRY_INITIAL_SECONDS
        try:
            while not self._stopping.is_set():
                client = None
                try:
                    client = self.client_factory(self.config)
                    if inspect.isawaitable(client):
                        client = await client
                    if self.ingest and callable(getattr(client, "on", None)):
                        self._prepare_ingestion(client)
                    await self._await_supervised(
                        client.connect(),
                        timeout=max(10.0, self._connect_wait_seconds * 2.0),
                        label="connect",
                    )
                    self.client = client
                    self._last_connect_error = None
                    self._force_disconnect.clear()
                    self._ready.set()
                    await self._notify_attempt()
                    delay = CONNECTION_RETRY_INITIAL_SECONDS
                    authorized = await self._await_supervised(
                        client.is_user_authorized(),
                        timeout=self._management_timeout_seconds,
                        label="is_user_authorized",
                    )
                    self._record_auth_state("authorized" if authorized else "needs_login")
                    if authorized:
                        self._schedule_ingestion(client)
                    await self._wait_until_disconnected(client)
                    if not self._stopping.is_set():
                        if self._force_disconnect.is_set():
                            self._force_disconnect.clear()
                            raise self._last_connect_error or TimeoutError("Telegram client timed out")
                        raise RuntimeError("Telegram client disconnected")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._last_connect_error = exc
                    await self._notify_attempt()
                    self._record_runtime_failure(exc)
                    if self._stopping.is_set():
                        break
                    await self._wait_retry_delay(delay)
                    delay = min(delay * 2, 60.0)
                finally:
                    self._ready.clear()
                    try:
                        await self._stop_ingestion()
                    except Exception as exc:
                        self._record_runtime_failure(exc, operation="ingest_stop")
                    if client is not None:
                        try:
                            await asyncio.wait_for(client.disconnect(), timeout=2.0)
                        except Exception:
                            self.logger.debug("Telegram client disconnect cleanup failed", exc_info=True)
                    if self.client is client:
                        self.client = None
        finally:
            await self._notify_attempt()

    async def _await_supervised(self, awaitable: Any, *, timeout: float, label: str) -> Any:
        op_task = asyncio.ensure_future(awaitable)
        force_task = asyncio.create_task(self._force_disconnect.wait())
        stop_task = asyncio.create_task(self._stopping.wait())
        waiters: set[asyncio.Task[Any]] = {op_task, force_task, stop_task}
        try:
            done, pending = await asyncio.wait(
                waiters,
                timeout=max(0.05, float(timeout)),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if op_task in done:
                return op_task.result()
            if self._stopping.is_set():
                raise asyncio.CancelledError()
            if self._force_disconnect.is_set():
                self._force_disconnect.clear()
                raise self._last_connect_error or TimeoutError(f"Telegram {label} interrupted for reconnect")
            raise TimeoutError(f"Telegram {label} timed out after {timeout:g} seconds")
        except BaseException:
            for task in waiters:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
            raise

    async def _wait_until_disconnected(self, client: Any) -> None:
        force_task = asyncio.create_task(self._force_disconnect.wait())
        stop_task = asyncio.create_task(self._stopping.wait())
        waiters: set[asyncio.Task[Any]] = {force_task, stop_task}
        disconnected = getattr(client, "disconnected", None)
        if inspect.isawaitable(disconnected):
            waiters.add(asyncio.ensure_future(disconnected))
        try:
            _, pending = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        except BaseException:
            for task in waiters:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
            raise

    async def _wait_retry_delay(self, delay: float) -> None:
        stop_task = asyncio.create_task(self._stopping.wait())
        reconnect_task = asyncio.create_task(self._reconnect_now.wait())
        try:
            _, pending = await asyncio.wait(
                {stop_task, reconnect_task},
                timeout=delay,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        finally:
            self._reconnect_now.clear()

    def _prepare_ingestion(self, client: Any) -> TelegramArchiveService:
        if self._ingest_service is not None:
            return self._ingest_service
        service = TelegramArchiveService(
            self.config,
            self.store,
            self.app_config.telegram.backfill,
            self.app_config.telegram.media_download,
        )
        register_handlers = getattr(service, "register_handlers", None)
        if callable(register_handlers):
            register_handlers(client)
        self._ingest_service = service
        return service

    def _schedule_ingestion(self, client: Any) -> None:
        if not self.ingest or self._ingest_task is not None:
            return
        retry_task = self._ingest_retry_task
        if retry_task is not None and not retry_task.done():
            retry_task.cancel()
        self._ingest_retry_task = None
        service = self._prepare_ingestion(client)
        activate = getattr(service, "activate", None)
        operation = activate(client) if callable(activate) else service.attach(client)
        self._ingest_active = False
        self._ingest_task = asyncio.create_task(
            operation,
            name=f"telegram-ingest-attach-{self.config.account_id}",
        )
        self._ingest_task.add_done_callback(self._ingest_done)

    def _ingest_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled() or self._stopping.is_set():
            return
        exc = task.exception()
        if exc is None:
            self._ingest_active = True
            self._ingest_retry_delay = INGEST_RETRY_INITIAL_SECONDS
            return
        if self._ingest_task is task:
            self._ingest_task = None
        self._ingest_active = False
        # Keep the service because its handlers were registered before connect.
        # Retry activation on the same connected client without duplicating
        # handlers, even when no management client is open to trigger it.
        self._record_runtime_failure(exc, operation="ingest_attach")
        client = self.client
        if client is not None:
            delay = self._ingest_retry_delay
            self._ingest_retry_delay = min(max(delay * 2, INGEST_RETRY_INITIAL_SECONDS), 60.0)
            self._ingest_retry_task = asyncio.create_task(
                self._retry_ingestion_activation(client, delay),
                name=f"telegram-ingest-retry-{self.config.account_id}",
            )

    async def _retry_ingestion_activation(self, client: Any, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            if self._stopping.is_set() or self.client is not client:
                return
            self._ingest_retry_task = None
            self._schedule_ingestion(client)
        except asyncio.CancelledError:
            raise

    async def _stop_ingestion(self) -> None:
        task = self._ingest_task
        retry_task = self._ingest_retry_task
        service = self._ingest_service
        self._ingest_task = None
        self._ingest_retry_task = None
        self._ingest_service = None
        self._ingest_active = False
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if retry_task is not None and not retry_task.done():
            retry_task.cancel()
            await asyncio.gather(retry_task, return_exceptions=True)
        stop = getattr(service, "stop", None)
        if callable(stop):
            result = stop()
            if inspect.isawaitable(result):
                await result

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
        try:
            self._record_auth_state("error", last_error=message)
        except Exception:
            self.logger.exception(
                "Failed to persist Telegram runtime auth failure for account %s",
                self.config.account_id,
            )
        try:
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
        except Exception:
            self.logger.exception(
                "Failed to persist Telegram runtime operation failure for account %s",
                self.config.account_id,
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
        ingest: bool = True,
        connect_wait_seconds: float = 5.0,
        management_timeout_seconds: float = DEFAULT_MANAGEMENT_TIMEOUT_SECONDS,
        delivery_timeout_seconds: float = DEFAULT_DELIVERY_TIMEOUT_SECONDS,
        account_ids: set[str] | None = None,
    ) -> None:
        """Run one client per account; with ingest=False the clients serve commands without archiving."""
        self.config = config
        self.store = store
        self.client_factory = client_factory or _default_client_factory
        self.call_timeout = call_timeout
        self.ingest = ingest
        self.connect_wait_seconds = max(0.0, float(connect_wait_seconds))
        self.management_timeout_seconds = max(0.05, float(management_timeout_seconds))
        self.delivery_timeout_seconds = max(0.05, float(delivery_timeout_seconds))
        self._account_ids = set(account_ids) if account_ids is not None else None
        self.logger = logging.getLogger(__name__)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread_id: int | None = None
        self._runtimes: dict[str, TelegramAccountRuntime] = {}

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._loop_thread_id = threading.get_ident()
        configs = {item.account_id: item for item in self.config.telegram.accounts}
        for item in self.store.list_management_accounts():
            if item.get("source") == SOURCE_TELEGRAM and item["account_id"] not in configs:
                configs[item["account_id"]] = stored_account_config(self.config, item)
        for account in configs.values():
            if self._account_ids is not None and account.account_id not in self._account_ids:
                continue
            await self._register_account(account)

    async def stop(self) -> None:
        runtimes = list(self._runtimes.values())
        self._runtimes.clear()
        await asyncio.gather(*(runtime.stop() for runtime in runtimes), return_exceptions=True)
        self._loop = None
        self._loop_thread_id = None

    def call(
        self,
        account_id: str,
        operation: str,
        *,
        cancel_event: threading.Event | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        loop = self._require_loop()
        if threading.get_ident() == self._loop_thread_id:
            raise RuntimeError("Use execute() when calling TelegramRuntimeManager from its event loop")
        future = asyncio.run_coroutine_threadsafe(self.execute(account_id, operation, **kwargs), loop)
        try:
            return self._wait_for_future(future, operation=operation, cancel_event=cancel_event)
        except TimeoutError as exc:
            if operation in MANAGEMENT_OPERATIONS:
                runtime = self._runtimes.get(account_id)
                if runtime is not None:
                    return runtime._offline_result(operation, error=exc, **kwargs)
            raise

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
            runtime = await self._register_account(resolve_account_config(self.config, self.store, account_id))
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
            ingest=self.ingest,
            connect_wait_seconds=self.connect_wait_seconds,
            management_timeout_seconds=self.management_timeout_seconds,
            delivery_timeout_seconds=self.delivery_timeout_seconds,
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

    def _require_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None or self._loop.is_closed():
            raise RuntimeError("Telegram runtime manager is not running")
        return self._loop

    def _wait_for_future(
        self,
        future: Any,
        *,
        operation: str = "operation",
        cancel_event: threading.Event | None = None,
    ) -> Any:
        deadline = time.monotonic() + self.call_timeout
        try:
            while True:
                if future.done():
                    return future.result()
                if cancel_event is not None and cancel_event.is_set():
                    raise RuntimeError(f"Telegram runtime {operation} canceled while waiting")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Telegram runtime {operation} timed out after {self.call_timeout:g} seconds"
                    )
                try:
                    return future.result(timeout=min(0.1, remaining))
                except FutureTimeoutError:
                    continue
        except BaseException:
            future.cancel()
            raise


def _default_client_factory(config: TelegramAccountConfig) -> Any:
    from telethon import TelegramClient

    config.session_dir.mkdir(parents=True, exist_ok=True)
    session_file = config.session_dir / config.session_name
    return TelegramClient(
        str(session_file),
        config.api_id,
        config.api_hash,
        catch_up=True,
        sequential_updates=True,
    )
