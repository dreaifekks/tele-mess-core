from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import signal
import sqlite3
import subprocess
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None  # type: ignore[assignment]

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import AppConfig, DailyDeliveryConfig
from tele_mess_core.daily import (
    DailyJobCancelled,
    DailyJobInterrupted,
    DailyJobLeaseLost,
    _resolve_package_date,
    _terminate_process,
    _zoneinfo,
    build_daily_package,
    resolve_daily_summary_delivery,
    run_daily_summary,
)
from tele_mess_core.models import DEFAULT_TIMEZONE, DailySummaryJobRecord, utc_now_iso


TERMINAL_JOB_STATUSES = {"completed", "failed", "canceled"}
DELIVERY_MAX_ATTEMPTS = 30
DAILY_WORKER_LOCK_FILENAME = ".daily-worker.lock"
LOGGER = logging.getLogger(__name__)


class DailyJobWorkerStopping(DailyJobInterrupted):
    pass


class DailyJobCliInterrupted(Exception):
    def __init__(self, signum: int) -> None:
        self.signum = int(signum)
        super().__init__(f"Daily CLI interrupted by signal {self.signum}")


class DailyWorkerLock:
    """Process-level advisory file lock ensuring at most one daily job executor."""

    def __init__(self, path: Path, *, owner: str = "", mode: str = "service") -> None:
        self.path = path
        self.owner = owner
        self.mode = mode
        self._fd: int | None = None

    def try_acquire(self) -> bool:
        if self._fd is not None:
            return True
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.ftruncate(fd, 0)
            os.lseek(fd, 0, os.SEEK_SET)
            payload = json.dumps(
                {
                    "pid": os.getpid(),
                    "owner": self.owner,
                    "mode": self.mode,
                    "acquired_at": utc_now_iso(),
                },
                ensure_ascii=False,
            )
            os.write(fd, payload.encode("utf-8"))
            self._fd = fd
            return True
        except BlockingIOError:
            os.close(fd)
            return False
        except Exception:
            os.close(fd)
            raise

    def read_holder(self) -> dict[str, Any] | None:
        try:
            raw = self.path.read_text(encoding="utf-8").strip()
            if not raw:
                return None
            data = json.loads(raw)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def release(self) -> None:
        fd = self._fd
        self._fd = None
        if fd is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


class _ScopedTelegramRuntime:
    """Short-lived TelegramRuntimeManager(ingest=False) for standalone CLI delivery."""

    def __init__(self, config: AppConfig, store: ArchiveStore, account_ids: set[str]) -> None:
        self._config = config
        self._store = store
        self._account_ids = set(account_ids)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._runtime: Any | None = None

    def __enter__(self) -> Any:
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        ready = threading.Event()
        errors: list[BaseException] = []
        runtime = TelegramRuntimeManager(
            self._config,
            self._store,
            ingest=False,
            account_ids=self._account_ids,
        )
        self._runtime = runtime

        def run_loop() -> None:
            loop = asyncio.new_event_loop()
            self._loop = loop
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(runtime.start())
                ready.set()
                loop.run_forever()
            except BaseException as exc:
                errors.append(exc)
                ready.set()
            finally:
                try:
                    loop.run_until_complete(runtime.stop())
                finally:
                    loop.run_until_complete(loop.shutdown_asyncgens())
                    loop.close()

        self._thread = threading.Thread(target=run_loop, name="cli-daily-delivery-runtime", daemon=True)
        self._thread.start()
        ready.wait(timeout=15.0)
        if errors:
            self.__exit__(None, None, None)
            raise errors[0]
        return runtime

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        loop = self._loop
        thread = self._thread
        if loop is not None and not loop.is_closed():
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                pass
        if thread is not None:
            thread.join(timeout=15.0)
        self._loop = None
        self._thread = None
        self._runtime = None


def enqueue_daily_summary_job(
    store: ArchiveStore,
    config: AppConfig,
    *,
    package_run_id: str | None = None,
    run_date: str | None = None,
    timezone_name: str | None = None,
    scope: dict[str, Any] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    scope = dict(scope or {})
    package = store.get_daily_package_run(package_run_id) if package_run_id else None
    effective_timezone = str(
        timezone_name or scope.get("timezone") or (package or {}).get("timezone") or DEFAULT_TIMEZONE
    )
    effective_date = run_date or scope.get("date") or (package or {}).get("date")
    if not effective_date:
        effective_date = _resolve_package_date(None, _zoneinfo(effective_timezone)).isoformat()
    request = {
        "package_run_id": package_run_id,
        "date": str(effective_date),
        "timezone": effective_timezone,
        "scope": scope,
        "force": bool(force),
    }
    delivery = resolve_daily_summary_delivery(store, config)
    dedupe_payload = {
        **request,
        "pipeline_version": 3,
        "provider": config.daily.ai.provider,
        "model": config.daily.ai.model,
        "fallback": {
            "enabled": config.daily.ai.fallback.enabled,
            "provider": config.daily.ai.fallback.provider,
            "trigger": config.daily.ai.fallback.trigger,
            "base_url": config.daily.ai.fallback.base_url,
            "model": config.daily.ai.fallback.model,
            "retry_delay_seconds": config.daily.ai.fallback.retry_delay_seconds,
            "max_retries": config.daily.ai.fallback.max_retries,
            "supports_images": config.daily.ai.fallback.supports_images,
            "supports_json_schema": config.daily.ai.fallback.supports_json_schema,
        },
        "delivery": {
            "enabled": delivery.enabled,
            "account_id": delivery.account_id,
            "origin_id": delivery.origin_id,
            "topic_id": delivery.topic_id,
        },
    }
    canonical = json.dumps(dedupe_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    dedupe_key = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if force:
        dedupe_key = hashlib.sha256(f"{dedupe_key}:{uuid.uuid4().hex}".encode("utf-8")).hexdigest()
    existing = store.find_active_daily_summary_job(dedupe_key)
    if existing is not None:
        return existing
    if not force:
        completed = store.find_completed_daily_summary_job(dedupe_key)
        if completed is not None:
            return completed
    now = utc_now_iso()
    job = DailySummaryJobRecord(
        job_id=f"job_{uuid.uuid4().hex[:12]}",
        status="queued",
        date=str(effective_date),
        timezone=effective_timezone,
        scope_json=json.dumps(scope, ensure_ascii=False, sort_keys=True),
        package_run_id=package_run_id,
        provider=config.daily.ai.provider,
        progress_label="queued",
        progress_json=json.dumps({"stage": "queued", "phase": "queued", "current": 0, "total": 0, "label": "queued"}),
        request_json=json.dumps(request, ensure_ascii=False, sort_keys=True),
        dedupe_key=dedupe_key,
        started_at=now,
        updated_at=now,
    )
    try:
        return store.insert_daily_summary_job(job)
    except sqlite3.IntegrityError:
        raced = store.find_active_daily_summary_job(dedupe_key)
        if raced is None:
            raise
        return raced


def wait_for_daily_summary_job(
    store: ArchiveStore,
    job_id: str,
    *,
    timeout: float | None = None,
    poll_interval: float = 0.05,
) -> dict[str, Any]:
    deadline = None if timeout is None else time.monotonic() + timeout
    interval = max(0.01, float(poll_interval))
    while True:
        item = store.get_daily_summary_job(job_id)
        if item is None:
            raise ValueError("Unknown daily summary job")
        if item["status"] in TERMINAL_JOB_STATUSES:
            return item
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for daily summary job {job_id}")
        time.sleep(interval)


def _has_undelivered_outbox(
    store: ArchiveStore,
    summary_run_id: str | None,
    *,
    job_id: str | None = None,
) -> bool:
    if not summary_run_id and not job_id:
        return False
    return any(
        item.get("status") in {"pending", "retry", "sending"}
        for item in store.list_delivery_outbox(
            summary_run_id=str(summary_run_id) if summary_run_id else None,
            job_id=job_id,
        )
    )


def run_daily_summary_job_from_cli(
    store: ArchiveStore,
    config: AppConfig,
    job_id: str,
    *,
    timeout: float | None = None,
    poll_interval: float = 0.05,
    stop_event: threading.Event | None = None,
    delivery_runtime_factory: Callable[[AppConfig, ArchiveStore, set[str]], Any] | None = None,
) -> dict[str, Any]:
    """Wait for the running core service, or execute only this job locally when no service runs."""
    current = store.get_daily_summary_job(job_id)
    if current is None:
        raise ValueError("Unknown daily summary job")

    effective_stop = stop_event or threading.Event()
    interrupted_signal: list[int] = []
    active_worker: list[DailyJobWorker] = []
    previous_handlers: dict[int, Any] = {}
    install_signals = stop_event is None and threading.current_thread() is threading.main_thread()
    watcher_done = threading.Event()
    watcher_thread: threading.Thread | None = None

    if install_signals:
        def _handle_signal(signum: int, _frame: Any) -> None:
            # Signal handlers must not acquire threading locks (such as Event._cond);
            # record the signal and let the dedicated watcher thread stop the worker.
            interrupted_signal.append(signum)

        def _watch_signals() -> None:
            while not watcher_done.wait(0.02):
                if interrupted_signal:
                    effective_stop.set()
                    if active_worker:
                        active_worker[0].request_stop()
                    return

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                previous_handlers[int(sig)] = signal.getsignal(sig)
                signal.signal(sig, _handle_signal)
            except (OSError, ValueError):
                pass
        watcher_thread = threading.Thread(
            target=_watch_signals,
            name=f"cli-signal-watcher-{job_id}",
            daemon=True,
        )
        watcher_thread.start()

    lock = DailyWorkerLock(
        config.storage.data_dir / DAILY_WORKER_LOCK_FILENAME,
        owner=f"cli:{job_id}",
        mode="cli",
    )
    runtime_factory = delivery_runtime_factory or _ScopedTelegramRuntime
    deadline = None if timeout is None else time.monotonic() + timeout
    interval = max(0.01, float(poll_interval))
    worker: DailyJobWorker | None = None
    lock_held = False

    def _raise_if_interrupted() -> None:
        if interrupted_signal:
            effective_stop.set()
            raise DailyJobCliInterrupted(interrupted_signal[0])
        if effective_stop.is_set():
            raise DailyJobCliInterrupted(int(signal.SIGINT))

    try:
        while True:
            _raise_if_interrupted()

            item = store.get_daily_summary_job(job_id)
            if item is None:
                raise ValueError("Unknown daily summary job")
            status = str(item.get("status") or "")

            if status in {"failed", "canceled"}:
                return item

            if status == "completed":
                summary_run_id = str(item.get("summary_run_id") or "") or None
                if not _has_undelivered_outbox(store, summary_run_id, job_id=job_id):
                    return item
                if not lock_held:
                    lock_held = lock.try_acquire()
                if lock_held:
                    store.reset_retry_backoff_for_summary(summary_run_id, job_id=job_id)
                    if worker is None:
                        worker = DailyJobWorker(
                            store,
                            config,
                            telegram_runtime=None,
                            use_process_lock=False,
                            standalone_delivery_factory=runtime_factory,
                            stop_event=effective_stop,
                        )
                        active_worker[:] = [worker]
                    worker.drain_summary_delivery(summary_run_id, job_id=job_id)
                    _raise_if_interrupted()
                    return store.get_daily_summary_job(job_id) or item
                holder = lock.read_holder()
                if holder is not None and holder.get("mode") == "service":
                    # A running service holds the worker lock and owns background delivery retries.
                    return item
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out waiting for daily summary delivery for job {job_id}")
                if install_signals:
                    time.sleep(min(interval, 0.05))
                else:
                    effective_stop.wait(interval)
                continue

            if not lock_held:
                lock_held = lock.try_acquire()
            if lock_held:
                if worker is None:
                    worker = DailyJobWorker(
                        store,
                        config,
                        telegram_runtime=None,
                        use_process_lock=False,
                        standalone_delivery_factory=runtime_factory,
                        stop_event=effective_stop,
                    )
                    active_worker[:] = [worker]
                ran = worker.run_once(job_id=job_id)
                _raise_if_interrupted()
                if ran is not None and ran["status"] in TERMINAL_JOB_STATUSES:
                    return ran
                # Nothing is runnable right now (e.g. waiting on retry_at backoff) and
                # any scoped delivery runtime is already closed; release the lock so a
                # starting core service is not blocked while this CLI waits.
                lock.release()
                lock_held = False

            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for daily summary job {job_id}")
            if install_signals:
                time.sleep(min(interval, 0.05))
            else:
                effective_stop.wait(interval)
    finally:
        watcher_done.set()
        if watcher_thread is not None:
            watcher_thread.join(timeout=1.0)
        if install_signals:
            for signum, handler in previous_handlers.items():
                try:
                    signal.signal(signum, handler)
                except (OSError, ValueError):
                    pass
        lock.release()


class DailyJobWorker:
    def __init__(
        self,
        store: ArchiveStore,
        config: AppConfig,
        *,
        telegram_runtime: Any | None = None,
        poll_interval: float = 0.5,
        worker_id: str | None = None,
        use_process_lock: bool = True,
        heartbeat_interval: float | None = None,
        standalone_delivery_factory: Callable[[AppConfig, ArchiveStore, set[str]], Any] | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        self.store = store
        self.config = config
        self.telegram_runtime = telegram_runtime
        self.poll_interval = max(0.05, poll_interval)
        self.worker_id = worker_id or f"worker_{uuid.uuid4().hex[:12]}"
        self.use_process_lock = use_process_lock
        self._standalone_delivery_factory = standalone_delivery_factory
        self._stop_event = stop_event or threading.Event()
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._process_lock = threading.RLock()
        self._current_job_id: str | None = None
        self._current_process: subprocess.Popen[str] | None = None
        self._lease_seconds = max(300, int(config.daily.ai.timeout_seconds) + 120)
        self._heartbeat_interval = (
            max(0.05, float(heartbeat_interval))
            if heartbeat_interval is not None
            else max(5.0, min(60.0, self._lease_seconds / 3.0))
        )
        self._worker_lock = DailyWorkerLock(
            config.storage.data_dir / DAILY_WORKER_LOCK_FILENAME,
            owner=self.worker_id,
        )

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="daily-job-worker", daemon=False)
        self._thread.start()

    def request_stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        with self._process_lock:
            process = self._current_process
        if process is not None and process.poll() is None:
            _terminate_process(process)

    def stop(self, timeout: float = 15.0) -> None:
        self.request_stop()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
            if thread.is_alive():
                raise RuntimeError("Daily job worker did not stop before timeout")
        self._thread = None

    def enqueue(
        self,
        *,
        package_run_id: str | None = None,
        run_date: str | None = None,
        timezone_name: str | None = None,
        scope: dict[str, Any] | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        item = enqueue_daily_summary_job(
            self.store,
            self.config,
            package_run_id=package_run_id,
            run_date=run_date,
            timezone_name=timezone_name,
            scope=scope,
            force=force,
        )
        self._wake_event.set()
        return item

    def cancel(self, job_id: str) -> dict[str, Any]:
        item = self.store.request_daily_summary_job_cancel(job_id)
        if item is None:
            raise ValueError("Unknown daily summary job")
        with self._process_lock:
            process = self._current_process if self._current_job_id == job_id else None
        if process is not None and process.poll() is None:
            _terminate_process(process)
        self._wake_event.set()
        return self.store.get_daily_summary_job(job_id) or item

    def run_once(self, *, job_id: str | None = None) -> dict[str, Any] | None:
        now, lease_until = self._lease_window()
        job = self.store.claim_daily_summary_job(
            self.worker_id,
            now=now,
            lease_until=lease_until,
            job_id=job_id,
        )
        if job is None:
            return None
        self._execute(job)
        return self.store.get_daily_summary_job(str(job["job_id"]))

    def wait_for_terminal(self, job_id: str, timeout: float | None = None) -> dict[str, Any]:
        return wait_for_daily_summary_job(self.store, job_id, timeout=timeout)

    def _run_loop(self) -> None:
        retry_delay = max(0.25, self.poll_interval)
        lock_acquired = False
        try:
            while not self._stop_event.is_set():
                if self.use_process_lock and not lock_acquired:
                    lock_acquired = self._worker_lock.try_acquire()
                    if not lock_acquired:
                        self._wake_event.wait(self.poll_interval)
                        self._wake_event.clear()
                        continue
                try:
                    item = self.run_once()
                    delivered = self._deliver_pending_once() if item is None else False
                    retry_delay = max(0.25, self.poll_interval)
                    if item is None and not delivered:
                        self._wake_event.wait(self.poll_interval)
                        self._wake_event.clear()
                except Exception as exc:
                    if isinstance(exc, sqlite3.OperationalError) and _is_sqlite_lock_error(exc):
                        LOGGER.warning(
                            "Daily job worker hit a transient SQLite lock; retrying in %.2fs: %s",
                            retry_delay,
                            exc,
                        )
                    else:
                        LOGGER.exception(
                            "Daily job worker loop failed; resetting its database connection "
                            "and retrying in %.2fs",
                            retry_delay,
                        )
                    try:
                        self.store.close_thread_connection()
                    except Exception:
                        LOGGER.exception("Failed to reset the daily job worker database connection")
                    self._wake_event.wait(retry_delay)
                    self._wake_event.clear()
                    retry_delay = min(30.0, max(0.5, retry_delay * 2))
        finally:
            if lock_acquired:
                self._worker_lock.release()
            self.store.close_thread_connection()

    def _execute(self, job: dict[str, Any]) -> None:
        job_id = str(job["job_id"])
        attempt = int(job.get("attempt") or 1)
        job_owner = (job_id, self.worker_id, attempt)
        request = dict(job.get("request") or {})
        scope = dict(request.get("scope") or job.get("scope") or {})
        requested_package_id = request.get("package_run_id")
        package_run_id = requested_package_id or job.get("package_run_id")
        run_date = request.get("date") or job.get("date")
        timezone_name = request.get("timezone") or job.get("timezone")
        package: dict[str, Any] | None = None
        summary: dict[str, Any] | None = None
        lease_lost = threading.Event()
        heartbeat_stop = threading.Event()

        with self._process_lock:
            self._current_job_id = job_id

        def terminate_current_process() -> None:
            with self._process_lock:
                process = self._current_process if self._current_job_id == job_id else None
            if process is not None and process.poll() is None:
                _terminate_process(process)

        def heartbeat_loop() -> None:
            try:
                while not heartbeat_stop.wait(self._heartbeat_interval):
                    if self._stop_event.is_set() or lease_lost.is_set():
                        return
                    try:
                        renewed = self.store.renew_daily_summary_job_lease(
                            job_id,
                            self.worker_id,
                            attempt,
                            lease_seconds=self._lease_seconds,
                        )
                    except sqlite3.OperationalError:
                        continue
                    except Exception:
                        LOGGER.exception("Failed to renew lease for daily summary job %s", job_id)
                        continue
                    if not renewed:
                        lease_lost.set()
                        terminate_current_process()
                        return
            finally:
                self.store.close_thread_connection()

        heartbeat_thread = threading.Thread(
            target=heartbeat_loop,
            name=f"daily-job-heartbeat-{job_id}",
            daemon=True,
        )
        heartbeat_thread.start()

        def check_cancel() -> None:
            if self._stop_event.is_set():
                raise DailyJobWorkerStopping("worker stopping")
            if lease_lost.is_set():
                raise DailyJobLeaseLost(f"lost lease for job {job_id}")
            if self.store.daily_summary_job_cancel_requested(job_id):
                raise DailyJobCancelled("canceled")

        def update_job_progress(
            progress: dict[str, Any],
            *,
            package_id: str | None = None,
            summary_id: str | None = None,
        ) -> None:
            updated = self.store.update_daily_summary_job_progress(
                job_id,
                self.worker_id,
                attempt,
                progress=progress,
                lease_seconds=self._lease_seconds,
                package_run_id=package_id,
                summary_run_id=summary_id,
                date=str(progress.get("date") or run_date or "") or None,
                timezone=str(progress.get("timezone") or timezone_name or "") or None,
                provider=str(progress.get("provider") or "") or None,
            )
            if not updated:
                lease_lost.set()
                raise DailyJobLeaseLost(f"lost lease for job {job_id}")

        def finish_job(
            status: str,
            progress: dict[str, Any],
            *,
            package_id: str | None = None,
            summary_id: str | None = None,
            error: str | None = None,
            discard_unsent_outbox: bool = False,
        ) -> bool:
            updated = self.store.finish_daily_summary_job(
                job_id,
                self.worker_id,
                attempt,
                status=status,
                progress=progress,
                package_run_id=package_id,
                summary_run_id=summary_id,
                date=str(progress.get("date") or run_date or "") or None,
                timezone=str(progress.get("timezone") or timezone_name or "") or None,
                provider=str(progress.get("provider") or "") or None,
                error=error,
                discard_unsent_outbox=discard_unsent_outbox,
            )
            if not updated:
                lease_lost.set()
            return updated

        def progress_callback(progress: dict[str, Any]) -> None:
            check_cancel()
            update_job_progress(progress)

        def process_callback(process: subprocess.Popen[str] | None, task_name: str) -> None:
            with self._process_lock:
                self._current_process = process
            if process is None and (self._stop_event.is_set() or lease_lost.is_set()):
                return
            current_progress = (self.store.get_daily_summary_job(job_id) or {}).get("progress") or {}
            update_job_progress(
                {**current_progress, "task": task_name, "pid": process.pid if process is not None else None}
            )

        try:
            check_cancel()
            package = self.store.get_daily_package_run(str(package_run_id)) if package_run_id else None
            if package is None or (not requested_package_id and package.get("status") != "completed"):
                planned_package_id = (
                    str(package_run_id)
                    if (not requested_package_id and package_run_id)
                    else f"pkg_{uuid.uuid4().hex[:12]}"
                )
                update_job_progress(
                    {"stage": "package", "phase": "queued", "current": 0, "total": 0, "label": "package queued"},
                    package_id=planned_package_id,
                )
                package = build_daily_package(
                    self.store,
                    self.config,
                    run_date=str(run_date) if run_date else None,
                    timezone_name=str(timezone_name) if timezone_name else None,
                    scope=scope,
                    run_id=planned_package_id,
                    progress_callback=progress_callback,
                    cancel_check=check_cancel,
                    job_owner=job_owner,
                )
            if package.get("status") != "completed":
                if package.get("status") == "canceled":
                    raise DailyJobCancelled("canceled")
                raise RuntimeError(package.get("error") or "Daily package did not complete")
            check_cancel()
            planned_summary_id = str(job.get("summary_run_id") or f"sum_{uuid.uuid4().hex[:12]}")
            summary = self.store.get_daily_summary_run(planned_summary_id)
            if summary is None or summary.get("status") != "completed":
                update_job_progress(
                    {
                        "stage": "summary",
                        "phase": "queued",
                        "current": 0,
                        "total": 0,
                        "label": "summary queued",
                        "date": package.get("date"),
                        "timezone": package.get("timezone"),
                    },
                    package_id=str(package["run_id"]),
                    summary_id=planned_summary_id,
                )
                summary = run_daily_summary(
                    self.store,
                    self.config,
                    package_run_id=str(package["run_id"]),
                    scope=scope,
                    run_id=planned_summary_id,
                    progress_callback=progress_callback,
                    cancel_check=check_cancel,
                    process_callback=process_callback,
                    job_owner=job_owner,
                )
            if summary.get("status") != "completed":
                if summary.get("status") == "canceled":
                    raise DailyJobCancelled("canceled")
                summary_progress = dict(summary.get("progress") or {})
                fallback = getattr(self.config.daily.ai, "fallback", None)
                max_retries = max(0, int(getattr(fallback, "max_retries", 0) or 0))
                current_job = self.store.get_daily_summary_job(job_id) or job
                retry_count = int(current_job.get("retry_count") or 0)
                if bool(summary_progress.get("retryable")) and retry_count < max_retries:
                    retry_delay_seconds = max(
                        0,
                        int(getattr(fallback, "retry_delay_seconds", 0) or 0),
                    )
                    now_dt = datetime.now(timezone.utc)
                    retry_at = (now_dt + timedelta(seconds=retry_delay_seconds)).isoformat()
                    error_kind = str(summary_progress.get("error_kind") or "retryable_ai_failure")
                    retry_progress = {
                        **(current_job.get("progress") or {}),
                        **summary_progress,
                        "phase": "retry_wait",
                        "label": f"retry scheduled after {error_kind}",
                        "retry_at": retry_at,
                        "retry_count": retry_count + 1,
                    }
                    scheduled = self.store.schedule_daily_summary_job_retry(
                        job_id,
                        self.worker_id,
                        attempt,
                        retry_delay_seconds=retry_delay_seconds,
                        max_retries=max_retries,
                        progress_label=str(retry_progress["label"]),
                        progress_json=json.dumps(retry_progress, ensure_ascii=False, sort_keys=True),
                        error=str(summary.get("error") or error_kind),
                    )
                    if scheduled:
                        return
                    if self.store.daily_summary_job_cancel_requested(job_id):
                        raise DailyJobCancelled("canceled")
                    raise DailyJobLeaseLost(f"lost lease for job {job_id}")
                raise RuntimeError(summary.get("error") or "Daily summary did not complete")
            check_cancel()
            self._drain_summary_delivery(
                str(summary["run_id"]),
                job_id=job_id,
                cancel_check=check_cancel,
                job_owner=job_owner,
            )
            check_cancel()
            completed = finish_job(
                "completed",
                {
                    "stage": "summary",
                    "phase": "completed",
                    "current": int(summary.get("progress_current") or 0),
                    "total": int(summary.get("progress_total") or 0),
                    "label": "completed",
                    "date": summary.get("date"),
                    "timezone": summary.get("timezone"),
                    "provider": summary.get("provider"),
                },
                package_id=str(package["run_id"]),
                summary_id=str(summary["run_id"]),
            )
            if not completed and self.store.daily_summary_job_cancel_requested(job_id):
                raise DailyJobCancelled("canceled")
        except DailyJobLeaseLost:
            LOGGER.warning("Daily job %s attempt %s lost its lease; stopping stale worker", job_id, attempt)
        except DailyJobInterrupted:
            self.store.requeue_daily_summary_job(job_id, self.worker_id, attempt)
        except DailyJobCancelled as exc:
            current = self.store.get_daily_summary_job(job_id) or {}
            finish_job(
                "canceled",
                {**(current.get("progress") or {}), "phase": "canceled", "label": "canceled"},
                package_id=str((package or {}).get("run_id") or package_run_id or "") or None,
                summary_id=str((summary or {}).get("run_id") or "") or None,
                error=str(exc) or "canceled",
                discard_unsent_outbox=True,
            )
        except Exception as exc:
            current = self.store.get_daily_summary_job(job_id) or {}
            finish_job(
                "failed",
                {**(current.get("progress") or {}), "phase": "failed", "label": "failed"},
                package_id=str((package or {}).get("run_id") or package_run_id or "") or None,
                summary_id=str((summary or {}).get("run_id") or "") or None,
                error=str(exc),
            )
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=5.0)
            with self._process_lock:
                self._current_process = None
                self._current_job_id = None

    def _lease_window(self) -> tuple[str, str]:
        now = datetime.now(timezone.utc)
        return now.isoformat(), (now + timedelta(seconds=self._lease_seconds)).isoformat()

    def drain_summary_delivery(self, summary_run_id: str | None, *, job_id: str | None = None) -> None:
        self._drain_summary_delivery(summary_run_id, job_id=job_id)

    def _drain_summary_delivery(
        self,
        summary_run_id: str | None,
        *,
        job_id: str | None = None,
        cancel_check: Callable[[], None] | None = None,
        job_owner: tuple[str, str, int] | None = None,
    ) -> None:
        if self.telegram_runtime is not None:
            while not self._stop_event.is_set():
                if cancel_check:
                    cancel_check()
                if not self._deliver_pending_once(
                    summary_run_id=summary_run_id,
                    job_id=job_id,
                    job_owner=job_owner,
                ):
                    return
            return

        if self._standalone_delivery_factory is None:
            return
        pending = [
            item
            for item in self.store.list_delivery_outbox(summary_run_id=summary_run_id, job_id=job_id)
            if item.get("status") in {"pending", "retry", "sending"}
        ]
        if not pending:
            return
        account_ids = {str(item["account_id"]) for item in pending if item.get("account_id")}
        with self._standalone_delivery_factory(self.config, self.store, account_ids) as runtime:
            while not self._stop_event.is_set():
                if cancel_check:
                    cancel_check()
                if not self._deliver_pending_once(
                    summary_run_id=summary_run_id,
                    job_id=job_id,
                    telegram_runtime=runtime,
                    job_owner=job_owner,
                ):
                    return

    def _deliver_pending_once(
        self,
        summary_run_id: str | None = None,
        *,
        job_id: str | None = None,
        telegram_runtime: Any | None = None,
        job_owner: tuple[str, str, int] | None = None,
    ) -> bool:
        runtime = telegram_runtime if telegram_runtime is not None else self.telegram_runtime
        if runtime is None or self._stop_event.is_set():
            return False
        item = self.store.claim_delivery_outbox(
            stale_seconds=300.0,
            summary_run_id=summary_run_id,
            job_id=job_id,
            job_owner=job_owner,
        )
        if item is None:
            return False
        attempts = int(item.get("attempts") or 1)
        claim_token = int(item["claim_token"])
        send_random_id = item.get("send_random_id")
        delivery = DailyDeliveryConfig(
            enabled=True,
            account_id=str(item["account_id"]),
            origin_id=int(item["origin_id"]),
            topic_id=int(item.get("topic_id") or 0),
        )
        try:
            call_kwargs: dict[str, Any] = {
                "cancel_event": self._stop_event,
                "delivery": delivery,
                "content": str(item["content"]),
            }
            if send_random_id is not None:
                call_kwargs["random_id"] = int(send_random_id)
            result = runtime.call(
                delivery.account_id,
                "deliver_chunk",
                **call_kwargs,
            )
            message_ids = list(result.get("message_ids") or [])
            self.store.complete_delivery_outbox(
                str(item["outbox_id"]),
                message_id=int(message_ids[0]) if message_ids else None,
                now=utc_now_iso(),
                expected_claim_token=claim_token,
            )
            return True
        except Exception as exc:
            if self._stop_event.is_set():
                self.store.release_delivery_outbox(
                    str(item["outbox_id"]),
                    error=str(exc),
                    now=utc_now_iso(),
                    expected_claim_token=claim_token,
                )
                return False
            if _is_random_id_duplicate_error(exc):
                # Telegram reports RANDOM_ID_DUPLICATE while an earlier request with the
                # same random_id is still executing on the server; keep the same random_id
                # and retry after a short delay without failing trailing chunks.
                retry_at = datetime.now(timezone.utc) + timedelta(seconds=15)
                self.store.retry_delivery_outbox(
                    str(item["outbox_id"]),
                    error=str(exc),
                    next_attempt_at=retry_at.isoformat(),
                    now=utc_now_iso(),
                    expected_claim_token=claim_token,
                    preserve_attempt_budget=True,
                )
                return True
            if attempts >= DELIVERY_MAX_ATTEMPTS:
                self.store.fail_delivery_outbox(
                    str(item["outbox_id"]),
                    error=str(exc),
                    now=utc_now_iso(),
                    expected_claim_token=claim_token,
                )
                return True
            retry_at = datetime.now(timezone.utc) + timedelta(seconds=max(2, min(300, 2 ** min(attempts, 8))))
            self.store.retry_delivery_outbox(
                str(item["outbox_id"]),
                error=str(exc),
                next_attempt_at=retry_at.isoformat(),
                now=utc_now_iso(),
                expected_claim_token=claim_token,
            )
            return True


def _is_random_id_duplicate_error(exc: Exception) -> bool:
    if getattr(exc, "code", None) == "random_id_duplicate":
        return True
    name = exc.__class__.__name__.lower()
    message = str(exc).lower()
    return "randomidduplicate" in name or "random_id_duplicate" in message


def _is_sqlite_lock_error(exc: sqlite3.OperationalError) -> bool:
    error_code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(error_code, int) and (error_code & 0xFF) in {
        sqlite3.SQLITE_BUSY,
        sqlite3.SQLITE_LOCKED,
    }:
        return True
    message = str(exc).lower()
    return "database is locked" in message or "database is busy" in message
