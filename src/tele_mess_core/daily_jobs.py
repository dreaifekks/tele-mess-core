from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import subprocess
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import AppConfig, DailyDeliveryConfig
from tele_mess_core.daily import (
    DailyJobCancelled,
    _resolve_package_date,
    _terminate_process,
    _zoneinfo,
    build_daily_package,
    resolve_daily_summary_delivery,
    run_daily_summary,
)
from tele_mess_core.models import DailySummaryJobRecord, utc_now_iso
from tele_mess_core.telegram.delivery import TelegramSummaryDeliveryService


TERMINAL_JOB_STATUSES = {"completed", "failed", "canceled"}


class DailyJobWorkerStopping(RuntimeError):
    pass


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
        timezone_name or scope.get("timezone") or (package or {}).get("timezone") or "Asia/Tokyo"
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
        "provider": config.daily.ai.provider,
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
        return store.upsert_daily_summary_job(job)
    except sqlite3.IntegrityError:
        raced = store.find_active_daily_summary_job(dedupe_key)
        if raced is None:
            raise
        return raced


class DailyJobWorker:
    def __init__(
        self,
        store: ArchiveStore,
        config: AppConfig,
        *,
        telegram_runtime: Any | None = None,
        poll_interval: float = 0.5,
        worker_id: str | None = None,
    ) -> None:
        self.store = store
        self.config = config
        self.telegram_runtime = telegram_runtime
        self.poll_interval = max(0.05, poll_interval)
        self.worker_id = worker_id or f"worker_{uuid.uuid4().hex[:12]}"
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._process_lock = threading.RLock()
        self._current_job_id: str | None = None
        self._current_process: subprocess.Popen[str] | None = None
        self._lease_seconds = max(300, int(config.daily.ai.timeout_seconds) + 120)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="daily-job-worker", daemon=False)
        self._thread.start()

    def stop(self, timeout: float = 15.0) -> None:
        self._stop_event.set()
        self._wake_event.set()
        with self._process_lock:
            process = self._current_process
        if process is not None and process.poll() is None:
            _terminate_process(process)
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

    def run_once(self) -> dict[str, Any] | None:
        now, lease_until = self._lease_window()
        job = self.store.claim_daily_summary_job(self.worker_id, now=now, lease_until=lease_until)
        if job is None:
            return None
        self._execute(job)
        return self.store.get_daily_summary_job(str(job["job_id"]))

    def wait_for_terminal(self, job_id: str, timeout: float | None = None) -> dict[str, Any]:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            item = self.store.get_daily_summary_job(job_id)
            if item is None:
                raise ValueError("Unknown daily summary job")
            if item["status"] in TERMINAL_JOB_STATUSES:
                return item
            if self._thread is None or not self._thread.is_alive():
                self.run_once()
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for daily summary job {job_id}")
            time.sleep(0.05)

    def _run_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                item = self.run_once()
                delivered = self._deliver_pending_once() if item is None else False
                if item is None and not delivered:
                    self._wake_event.wait(self.poll_interval)
                    self._wake_event.clear()
        finally:
            self.store.close_thread_connection()

    def _execute(self, job: dict[str, Any]) -> None:
        job_id = str(job["job_id"])
        request = dict(job.get("request") or {})
        scope = dict(request.get("scope") or job.get("scope") or {})
        package_run_id = request.get("package_run_id") or job.get("package_run_id")
        run_date = request.get("date") or job.get("date")
        timezone_name = request.get("timezone") or job.get("timezone")
        package: dict[str, Any] | None = None
        summary: dict[str, Any] | None = None
        with self._process_lock:
            self._current_job_id = job_id

        def check_cancel() -> None:
            if self._stop_event.is_set():
                raise DailyJobWorkerStopping("worker stopping")
            if self.store.daily_summary_job_cancel_requested(job_id):
                raise DailyJobCancelled("canceled")

        def write_job(
            status: str,
            progress: dict[str, Any],
            *,
            package_id: str | None = None,
            summary_id: str | None = None,
            error: str | None = None,
            finished_at: str | None = None,
        ) -> dict[str, Any]:
            current = self.store.get_daily_summary_job(job_id) or job
            now, lease_until = self._lease_window()
            return self.store.upsert_daily_summary_job(
                DailySummaryJobRecord(
                    job_id=job_id,
                    status=status,
                    date=str(progress.get("date") or run_date or current.get("date") or "") or None,
                    timezone=str(progress.get("timezone") or timezone_name or current.get("timezone") or "") or None,
                    scope_json=json.dumps(scope, ensure_ascii=False, sort_keys=True),
                    package_run_id=package_id or current.get("package_run_id"),
                    summary_run_id=summary_id or current.get("summary_run_id"),
                    provider=self.config.daily.ai.provider,
                    progress_total=int(progress.get("total") or 0),
                    progress_current=int(progress.get("current") or 0),
                    progress_label=str(progress.get("label") or status),
                    progress_json=json.dumps(progress, ensure_ascii=False, sort_keys=True),
                    request_json=json.dumps(request, ensure_ascii=False, sort_keys=True),
                    dedupe_key=current.get("dedupe_key"),
                    worker_id=self.worker_id,
                    lease_until=lease_until,
                    heartbeat_at=now,
                    attempt=int(current.get("attempt") or 0),
                    cancel_requested_at=current.get("cancel_requested_at"),
                    error=error,
                    started_at=current.get("started_at"),
                    finished_at=finished_at,
                    updated_at=now,
                )
            )

        def progress_callback(progress: dict[str, Any]) -> None:
            check_cancel()
            write_job("running", progress)

        def process_callback(process: subprocess.Popen[str] | None, task_name: str) -> None:
            with self._process_lock:
                self._current_process = process
            current_progress = (self.store.get_daily_summary_job(job_id) or {}).get("progress") or {}
            write_job(
                "running",
                {**current_progress, "task": task_name, "pid": process.pid if process is not None else None},
            )

        try:
            check_cancel()
            package = self.store.get_daily_package_run(str(package_run_id)) if package_run_id else None
            if package is None:
                planned_package_id = f"pkg_{uuid.uuid4().hex[:12]}"
                write_job(
                    "running",
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
                )
            if package.get("status") != "completed":
                if package.get("status") == "canceled":
                    raise DailyJobCancelled("canceled")
                raise RuntimeError(package.get("error") or "Daily package did not complete")
            check_cancel()
            planned_summary_id = str(job.get("summary_run_id") or f"sum_{uuid.uuid4().hex[:12]}")
            summary = self.store.get_daily_summary_run(planned_summary_id)
            if summary is None or summary.get("status") != "completed":
                write_job(
                    "running",
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
                    telegram_runtime=self.telegram_runtime,
                    defer_delivery=True,
                    job_id=job_id,
                )
            if summary.get("status") != "completed":
                if summary.get("status") == "canceled":
                    raise DailyJobCancelled("canceled")
                raise RuntimeError(summary.get("error") or "Daily summary did not complete")
            self._drain_summary_delivery(str(summary["run_id"]))
            write_job(
                "completed",
                {
                    "stage": "summary",
                    "phase": "completed",
                    "current": int(summary.get("progress_current") or 0),
                    "total": int(summary.get("progress_total") or 0),
                    "label": "completed",
                    "date": summary.get("date"),
                    "timezone": summary.get("timezone"),
                },
                package_id=str(package["run_id"]),
                summary_id=str(summary["run_id"]),
                finished_at=utc_now_iso(),
            )
        except DailyJobWorkerStopping:
            self.store.requeue_daily_summary_job(job_id, self.worker_id, now=utc_now_iso())
        except DailyJobCancelled as exc:
            current = self.store.get_daily_summary_job(job_id) or {}
            write_job(
                "canceled",
                {**(current.get("progress") or {}), "phase": "canceled", "label": "canceled"},
                package_id=str((package or {}).get("run_id") or package_run_id or "") or None,
                summary_id=str((summary or {}).get("run_id") or "") or None,
                error=str(exc) or "canceled",
                finished_at=utc_now_iso(),
            )
        except Exception as exc:
            current = self.store.get_daily_summary_job(job_id) or {}
            write_job(
                "failed",
                {**(current.get("progress") or {}), "phase": "failed", "label": "failed"},
                package_id=str((package or {}).get("run_id") or package_run_id or "") or None,
                summary_id=str((summary or {}).get("run_id") or "") or None,
                error=str(exc),
                finished_at=utc_now_iso(),
            )
        finally:
            with self._process_lock:
                self._current_process = None
                self._current_job_id = None

    def _lease_window(self) -> tuple[str, str]:
        now = datetime.now(timezone.utc)
        return now.isoformat(), (now + timedelta(seconds=self._lease_seconds)).isoformat()

    def _drain_summary_delivery(self, summary_run_id: str) -> None:
        while not self._stop_event.is_set():
            if not self._deliver_pending_once(summary_run_id=summary_run_id):
                return

    def _deliver_pending_once(self, summary_run_id: str | None = None) -> bool:
        now_dt = datetime.now(timezone.utc)
        item = self.store.claim_delivery_outbox(
            now=now_dt.isoformat(),
            stale_before=(now_dt - timedelta(minutes=5)).isoformat(),
            summary_run_id=summary_run_id,
        )
        if item is None:
            return False
        delivery = DailyDeliveryConfig(
            enabled=True,
            account_id=str(item["account_id"]),
            origin_id=int(item["origin_id"]),
            topic_id=int(item.get("topic_id") or 0),
        )
        try:
            if self.telegram_runtime is not None:
                result = self.telegram_runtime.call(
                    delivery.account_id,
                    "deliver_chunk",
                    delivery=delivery,
                    content=str(item["content"]),
                )
            else:
                account = next(
                    (candidate for candidate in self.config.telegram.accounts if candidate.account_id == delivery.account_id),
                    None,
                )
                if account is None:
                    raise ValueError(f"Unknown delivery account_id: {delivery.account_id}")
                result = asyncio.run(
                    TelegramSummaryDeliveryService(account, self.store).send_summary(
                        delivery,
                        str(item["content"]),
                        split_content=False,
                    )
                )
            message_ids = list(result.get("message_ids") or [])
            self.store.complete_delivery_outbox(
                str(item["outbox_id"]),
                message_id=int(message_ids[0]) if message_ids else None,
                now=utc_now_iso(),
            )
            return True
        except Exception as exc:
            attempts = int(item.get("attempts") or 1)
            retry_at = datetime.now(timezone.utc) + timedelta(seconds=min(300, 2 ** min(attempts, 8)))
            self.store.retry_delivery_outbox(
                str(item["outbox_id"]),
                error=str(exc),
                next_attempt_at=retry_at.isoformat(),
                now=utc_now_iso(),
            )
            return False
