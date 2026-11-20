from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from telethon.extensions import markdown as telethon_markdown

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import (
    AppConfig,
    DailyAiConfig,
    DailyAiFallbackConfig,
    DailyDeliveryConfig,
    DailyPackagingConfig,
    LoggingConfig,
    ServerConfig,
    StorageConfig,
    TelegramAccountConfig,
    TelegramConfig,
)
from tele_mess_core.daily_jobs import (
    DELIVERY_MAX_ATTEMPTS,
    DailyJobWorker,
    DailyJobWorkerStopping,
    enqueue_daily_summary_job,
    run_daily_summary_job_from_cli,
)
from tele_mess_core.models import (
    AccountRecord,
    BackupPolicyRecord,
    DailyPackageRunRecord,
    DailySummaryDeliveryRecord,
    MessageRecord,
    OriginRecord,
    SOURCE_TELEGRAM,
    utc_now_iso,
)
from tele_mess_core.telegram.delivery import split_telegram_markdown


class DailyJobWorkerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = ArchiveStore(self.root / "archive.db")
        self.store.initialize()
        self.store.upsert_account(
            AccountRecord(source=SOURCE_TELEGRAM, account_id="main", display_name="Main", kind="telegram")
        )
        self.config = AppConfig(
            storage=StorageConfig(data_dir=self.root, database=self.root / "archive.db"),
            telegram=TelegramConfig(
                accounts=[
                    TelegramAccountConfig(
                        account_id="main",
                        api_id=1,
                        api_hash="test",
                        session_name="main",
                        session_dir=self.root / "sessions",
                    )
                ]
            ),
            server=ServerConfig(token="secret"),
            logging=LoggingConfig(file=None),
            daily=DailyPackagingConfig(
                output_dir=self.root / "daily-packages",
                ai=DailyAiConfig(provider="disabled"),
            ),
        )
        self.store.upsert_origin(
            OriginRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                origin_type="group",
                title="Daily Source",
            )
        )
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                enabled=True,
                tags="info",
            )
        )
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-1001,
                message_id=1,
                sent_at="2026-07-03T12:00:00+00:00",
                ingested_at=utc_now_iso(),
                text="durable worker payload",
            ),
            event_type="new",
        )

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_job_is_queued_when_the_delivery_account_was_removed(self) -> None:
        self.store.set_daily_summary_delivery(
            DailySummaryDeliveryRecord(enabled=True, account_id="removed-account", origin_id=-9001, updated_at=utc_now_iso())
        )

        job = enqueue_daily_summary_job(self.store, self.config, run_date="2026-07-03", timezone_name="UTC")

        self.assertEqual(job["status"], "queued")

    def test_enqueue_is_deduplicated_and_worker_completes_job(self) -> None:
        worker = DailyJobWorker(self.store, self.config, poll_interval=0.01)
        first = worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        duplicate = worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )

        self.assertEqual(first["job_id"], duplicate["job_id"])
        self.assertEqual(first["status"], "queued")

        completed = worker.run_once()

        self.assertIsNotNone(completed)
        assert completed is not None
        self.assertEqual(completed["status"], "completed")
        self.assertTrue(completed["package_run_id"].startswith("pkg_"))
        self.assertTrue(completed["summary_run_id"].startswith("sum_"))
        self.assertEqual(completed["attempt"], 1)

    def test_worker_recovers_after_transient_sqlite_lock(self) -> None:
        worker = DailyJobWorker(self.store, self.config, poll_interval=0.01)
        original_claim = self.store.claim_daily_summary_job
        recovered = threading.Event()
        attempts = 0

        def flaky_claim(*args: object, **kwargs: object) -> dict[str, object] | None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise sqlite3.OperationalError("database is locked")
            recovered.set()
            return original_claim(*args, **kwargs)

        with patch.object(self.store, "claim_daily_summary_job", side_effect=flaky_claim):
            worker.start()
            try:
                self.assertTrue(recovered.wait(2), "worker did not retry after the transient lock")
            finally:
                worker.stop()

        self.assertGreaterEqual(attempts, 2)

    def test_implicit_date_is_resolved_before_deduplication(self) -> None:
        worker = DailyJobWorker(self.store, self.config)
        with patch("tele_mess_core.daily_jobs._resolve_package_date", return_value=date(2026, 7, 3)):
            first = worker.enqueue(timezone_name="UTC", scope={"account_id": "main"})
        with patch("tele_mess_core.daily_jobs._resolve_package_date", return_value=date(2026, 7, 4)):
            second = worker.enqueue(timezone_name="UTC", scope={"account_id": "main"})

        self.assertEqual(first["date"], "2026-07-03")
        self.assertEqual(second["date"], "2026-07-04")
        self.assertNotEqual(first["job_id"], second["job_id"])

    def test_job_dedupe_changes_when_effective_model_changes(self) -> None:
        first = DailyJobWorker(self.store, self.config).enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        alternate_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=DailyAiConfig(provider="disabled", model="gpt-5.6-terra"),
            ),
        )

        second = DailyJobWorker(self.store, alternate_config).enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )

        self.assertNotEqual(first["job_id"], second["job_id"])
        self.assertNotEqual(first["dedupe_key"], second["dedupe_key"])

    def test_job_dedupe_changes_when_fallback_model_changes_without_key_material(self) -> None:
        key_path = self.root / "fallback-key"
        key_path.write_text("must-not-enter-dedupe", encoding="utf-8")
        first_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=DailyAiConfig(
                    provider="disabled",
                    fallback=DailyAiFallbackConfig(
                        enabled=True,
                        base_url="https://fallback.example/v1",
                        model="deepseek-v4-flash",
                        api_key_file=key_path,
                    ),
                ),
            ),
        )
        second_config = AppConfig(
            storage=first_config.storage,
            telegram=first_config.telegram,
            server=first_config.server,
            logging=first_config.logging,
            daily=DailyPackagingConfig(
                output_dir=first_config.daily.output_dir,
                ai=DailyAiConfig(
                    provider="disabled",
                    fallback=DailyAiFallbackConfig(
                        enabled=True,
                        base_url="https://fallback.example/v1",
                        model="deepseek-v4-pro",
                        api_key_file=key_path,
                    ),
                ),
            ),
        )
        first = DailyJobWorker(self.store, first_config).enqueue(
            run_date="2026-07-03", timezone_name="UTC", scope={"account_id": "main"}
        )
        second = DailyJobWorker(self.store, second_config).enqueue(
            run_date="2026-07-03", timezone_name="UTC", scope={"account_id": "main"}
        )

        self.assertNotEqual(first["dedupe_key"], second["dedupe_key"])
        self.assertNotIn("must-not-enter-dedupe", str(first))

    def test_expired_lease_is_reclaimed_after_restart(self) -> None:
        old_worker = DailyJobWorker(self.store, self.config, worker_id="old")
        job = old_worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        claimed = self.store.claim_daily_summary_job(
            "old",
            now="2000-01-01T00:00:00+00:00",
            lease_until="2000-01-01T00:01:00+00:00",
        )
        self.assertEqual(claimed["status"], "running")

        replacement = DailyJobWorker(self.store, self.config, worker_id="replacement")
        completed = replacement.run_once()

        self.assertEqual(completed["job_id"], job["job_id"])
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["attempt"], 2)

    def test_job_requeued_before_its_package_exists_rebuilds_in_new_attempt_dir(self) -> None:
        worker = DailyJobWorker(self.store, self.config, worker_id="stopping")
        job = worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )

        with patch(
            "tele_mess_core.daily_jobs.build_daily_package",
            side_effect=DailyJobWorkerStopping("worker stopping"),
        ):
            queued = worker.run_once()

        assert queued is not None
        planned_package_id = str(queued["package_run_id"])
        self.assertEqual(queued["job_id"], job["job_id"])
        self.assertEqual(queued["status"], "queued")
        self.assertIsNone(queued["worker_id"])
        self.assertIsNone(queued["lease_until"])
        self.assertEqual(queued["progress_label"], "queued after worker stop")

        completed = DailyJobWorker(
            self.store,
            self.config,
            worker_id="replacement",
        ).run_once()

        assert completed is not None
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["package_run_id"], planned_package_id)
        pkg = self.store.get_daily_package_run(planned_package_id)
        assert pkg is not None
        self.assertEqual(pkg["status"], "completed")
        self.assertIn("attempt-2", pkg["output_dir"])

    def test_lease_takeover_rebuilds_own_interrupted_package_in_isolated_attempt_dir(self) -> None:
        old_worker = DailyJobWorker(self.store, self.config, worker_id="old")
        job = old_worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        claimed = self.store.claim_daily_summary_job(
            "old",
            now="2000-01-01T00:00:00+00:00",
            lease_until="2000-01-01T00:01:00+00:00",
        )
        assert claimed is not None
        package_run_id = "pkg_expired_running"
        old_dir = self.config.daily.output_dir / "2026-07-03" / package_run_id / "attempt-1"
        old_dir.mkdir(parents=True, exist_ok=True)
        (old_dir / "package.json").write_text('{"stale": true}', encoding="utf-8")
        self.store.upsert_daily_package_run(
            DailyPackageRunRecord(
                run_id=package_run_id,
                status="running",
                date="2026-07-03",
                timezone="UTC",
                scope_json='{"account_id": "main"}',
                output_dir=str(old_dir),
                progress_label="packaging",
            )
        )
        with self.store._lock:
            self.store._conn.execute(
                "UPDATE daily_summary_jobs SET package_run_id = ? WHERE job_id = ?",
                (package_run_id, job["job_id"]),
            )
            self.store._conn.commit()

        recovered = DailyJobWorker(
            self.store,
            self.config,
            worker_id="replacement",
        ).run_once()

        assert recovered is not None
        self.assertEqual(recovered["status"], "completed")
        package = self.store.get_daily_package_run(package_run_id)
        assert package is not None
        self.assertEqual(package["status"], "completed")
        self.assertIn("attempt-2", package["output_dir"])
        self.assertEqual((old_dir / "package.json").read_text(encoding="utf-8"), '{"stale": true}')

    def test_requested_package_that_is_still_running_is_not_rebuilt(self) -> None:
        package_run_id = "pkg_other_job_running"
        self.store.upsert_daily_package_run(
            DailyPackageRunRecord(
                run_id=package_run_id,
                status="running",
                date="2026-07-03",
                timezone="UTC",
                scope_json='{"account_id": "main", "origin_id": -1001}',
                output_dir=str(self.config.daily.output_dir / "2026-07-03" / package_run_id),
                progress_label="packaging",
            )
        )
        worker = DailyJobWorker(self.store, self.config, worker_id="summary-only")
        worker.enqueue(package_run_id=package_run_id)

        finished = worker.run_once()

        assert finished is not None
        self.assertEqual(finished["status"], "failed")
        package = self.store.get_daily_package_run(package_run_id)
        assert package is not None
        self.assertEqual(package["status"], "running")
        self.assertEqual(package["message_count"], 0)

    def test_worker_interruption_with_cancel_request_finishes_canceled(self) -> None:
        worker = DailyJobWorker(self.store, self.config, worker_id="stopping")
        job = worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )

        def cancel_then_interrupt(*args: object, **kwargs: object) -> dict[str, object]:
            self.store.request_daily_summary_job_cancel(job["job_id"])
            raise DailyJobWorkerStopping("worker stopping")

        with patch(
            "tele_mess_core.daily_jobs.build_daily_package",
            side_effect=cancel_then_interrupt,
        ):
            canceled = worker.run_once()

        assert canceled is not None
        self.assertEqual(canceled["status"], "canceled")
        self.assertEqual(canceled["progress_label"], "canceled")
        self.assertIsNone(canceled["worker_id"])
        self.assertIsNone(canceled["lease_until"])
        self.assertIsNotNone(canceled["finished_at"])

    def test_cancel_and_stop_before_publish_leave_nothing_to_send(self) -> None:
        delivery_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=self.config.daily.ai,
                delivery=DailyDeliveryConfig(enabled=True, account_id="main", origin_id=-9001),
            ),
        )
        worker = DailyJobWorker(self.store, delivery_config, worker_id="stopping")
        job = worker.enqueue(run_date="2026-07-03", timezone_name="UTC", scope={"account_id": "main"})
        publish = self.store.publish_daily_summary

        def cancel_and_stop_then_publish(*args: object, **kwargs: object) -> object:
            self.store.request_daily_summary_job_cancel(job["job_id"])
            worker._stop_event.set()
            return publish(*args, **kwargs)

        with patch.object(self.store, "publish_daily_summary", side_effect=cancel_and_stop_then_publish):
            canceled = worker.run_once()

        assert canceled is not None
        self.assertEqual(canceled["status"], "canceled")
        outbox = self.store.list_delivery_outbox(summary_run_id=canceled["summary_run_id"])
        self.assertEqual([item for item in outbox if item["status"] != "sent"], [])
        summary = self.store.get_daily_summary_run(str(canceled["summary_run_id"]))
        assert summary is not None
        self.assertEqual(summary["status"], "canceled")

    def test_recovery_reuses_a_summary_completed_before_job_commit(self) -> None:
        first_worker = DailyJobWorker(self.store, self.config, worker_id="first")
        job = first_worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        first = first_worker.run_once()
        summary_run_id = first["summary_run_id"]

        with self.store._lock:
            self.store._conn.execute(
                """
                UPDATE daily_summary_jobs
                SET status = 'running', worker_id = 'crashed',
                    lease_until = '2000-01-01T00:00:00+00:00', finished_at = NULL
                WHERE job_id = ?
                """,
                (job["job_id"],),
            )
            self.store._conn.commit()

        replacement = DailyJobWorker(self.store, self.config, worker_id="replacement")
        recovered = replacement.run_once()

        self.assertEqual(recovered["status"], "completed")
        self.assertEqual(recovered["summary_run_id"], summary_run_id)
        self.assertEqual(len(self.store.list_daily_summary_runs(package_run_id=recovered["package_run_id"])), 1)

    def test_retryable_summary_is_durably_requeued_and_reuses_run_ids(self) -> None:
        retry_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=DailyAiConfig(
                    provider="disabled",
                    fallback=DailyAiFallbackConfig(retry_delay_seconds=0, max_retries=1),
                ),
            ),
        )
        summary_run_ids: list[str] = []

        def fake_summary(*args: object, **kwargs: object) -> dict[str, object]:
            run_id = str(kwargs["run_id"])
            summary_run_ids.append(run_id)
            if len(summary_run_ids) == 1:
                return {
                    "run_id": run_id,
                    "status": "failed",
                    "error": "fallback temporarily unavailable",
                    "progress": {"retryable": True, "error_kind": "fallback_failed"},
                }
            return {
                "run_id": run_id,
                "status": "completed",
                "date": "2026-07-03",
                "timezone": "UTC",
                "progress_current": 1,
                "progress_total": 1,
            }

        worker = DailyJobWorker(self.store, retry_config, worker_id="retry-worker")
        job = worker.enqueue(run_date="2026-07-03", timezone_name="UTC", scope={"account_id": "main"})
        with patch("tele_mess_core.daily_jobs.run_daily_summary", side_effect=fake_summary):
            waiting = worker.run_once()
            assert waiting is not None
            self.assertEqual(waiting["status"], "queued")
            self.assertEqual(waiting["retry_count"], 1)
            self.assertIsNotNone(waiting["retry_at"])
            self.assertIsNone(waiting["worker_id"])
            self.assertIsNone(waiting["lease_until"])
            self.assertEqual(waiting["progress"]["phase"], "retry_wait")

            completed = worker.run_once()

        assert completed is not None
        self.assertEqual(completed["job_id"], job["job_id"])
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["retry_count"], 1)
        self.assertEqual(completed["attempt"], 2)
        self.assertEqual(summary_run_ids, [completed["summary_run_id"], completed["summary_run_id"]])

    def test_retryable_summary_fails_after_retry_budget_is_exhausted(self) -> None:
        retry_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=DailyAiConfig(
                    provider="disabled",
                    fallback=DailyAiFallbackConfig(retry_delay_seconds=0, max_retries=1),
                ),
            ),
        )

        def failed_summary(*args: object, **kwargs: object) -> dict[str, object]:
            return {
                "run_id": str(kwargs["run_id"]),
                "status": "failed",
                "error": "fallback still unavailable",
                "progress": {"retryable": True, "error_kind": "fallback_failed"},
            }

        worker = DailyJobWorker(self.store, retry_config)
        worker.enqueue(run_date="2026-07-03", timezone_name="UTC", scope={"account_id": "main"})
        with patch("tele_mess_core.daily_jobs.run_daily_summary", side_effect=failed_summary) as provider:
            waiting = worker.run_once()
            assert waiting is not None
            self.assertEqual(waiting["status"], "queued")
            failed = worker.run_once()

        assert failed is not None
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["retry_count"], 1)
        self.assertEqual(failed["attempt"], 2)
        self.assertEqual(provider.call_count, 2)

    def test_canceling_scheduled_retry_prevents_another_provider_call(self) -> None:
        retry_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=DailyAiConfig(
                    provider="disabled",
                    fallback=DailyAiFallbackConfig(retry_delay_seconds=1200, max_retries=1),
                ),
            ),
        )

        def failed_summary(*args: object, **kwargs: object) -> dict[str, object]:
            return {
                "run_id": str(kwargs["run_id"]),
                "status": "failed",
                "error": "fallback temporarily unavailable",
                "progress": {"retryable": True, "error_kind": "fallback_failed"},
            }

        worker = DailyJobWorker(self.store, retry_config)
        job = worker.enqueue(run_date="2026-07-03", timezone_name="UTC", scope={"account_id": "main"})
        with patch("tele_mess_core.daily_jobs.run_daily_summary", side_effect=failed_summary) as provider:
            waiting = worker.run_once()
            assert waiting is not None
            self.assertEqual(waiting["status"], "queued")
            requested = worker.cancel(job["job_id"])
            self.assertEqual(requested["status"], "cancel_requested")
            self.assertIsNone(worker.run_once())

        canceled = self.store.get_daily_summary_job(job["job_id"])
        assert canceled is not None
        self.assertEqual(canceled["status"], "canceled")
        self.assertIsNone(canceled["retry_at"])
        self.assertEqual(provider.call_count, 1)

    def test_canceling_queued_job_reaches_terminal_state(self) -> None:
        worker = DailyJobWorker(self.store, self.config)
        job = worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )

        requested = worker.cancel(job["job_id"])
        self.assertEqual(requested["status"], "cancel_requested")
        self.assertIsNone(worker.run_once())
        canceled = self.store.get_daily_summary_job(job["job_id"])
        self.assertEqual(canceled["status"], "canceled")

    def test_delivery_outbox_is_idempotent_and_failure_does_not_fail_summary(self) -> None:
        delivery_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=self.config.daily.ai,
                delivery=DailyDeliveryConfig(
                    enabled=True,
                    account_id="main",
                    origin_id=-9001,
                    topic_id=42,
                ),
            ),
        )

        class FailingRuntime:
            def call(self, account_id: str, operation: str, **kwargs: object) -> dict[str, object]:
                raise RuntimeError("temporary delivery failure")

        worker = DailyJobWorker(self.store, delivery_config, telegram_runtime=FailingRuntime())
        job = worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )

        completed = worker.run_once()

        self.assertEqual(completed["job_id"], job["job_id"])
        self.assertEqual(completed["status"], "completed")
        outbox = self.store.list_delivery_outbox(summary_run_id=completed["summary_run_id"])
        self.assertEqual(len(outbox), 1)
        self.assertEqual(outbox[0]["status"], "retry")
        self.assertEqual(outbox[0]["attempts"], 1)
        self.assertIn("temporary delivery failure", outbox[0]["last_error"])

        records_before = self.store.list_daily_summary_records(run_id=completed["summary_run_id"])
        same = worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        self.assertEqual(same["job_id"], job["job_id"])
        # Re-persisting the same run/chunk is protected by the outbox unique key.
        summary_run = self.store.get_daily_summary_run(completed["summary_run_id"])
        self.assertIsNotNone(summary_run)
        self.assertEqual(
            len(self.store.list_daily_summary_records(run_id=completed["summary_run_id"])),
            len(records_before),
        )

    def test_delivery_outbox_records_sent_message_id(self) -> None:
        self.store.set_origin_important(SOURCE_TELEGRAM, "main", -1001, 0, True)
        delivery_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=self.config.daily.ai,
                delivery=DailyDeliveryConfig(enabled=True, account_id="main", origin_id=-9001),
            ),
        )

        class SuccessfulRuntime:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str, str]] = []

            def call(self, account_id: str, operation: str, **kwargs: object) -> dict[str, object]:
                self.calls.append((account_id, operation, str(kwargs.get("content") or "")))
                return {"status": "sent", "message_ids": [320 + len(self.calls)], "message_count": 1}

        runtime = SuccessfulRuntime()
        worker = DailyJobWorker(self.store, delivery_config, telegram_runtime=runtime)
        job = worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )

        with patch(
            "tele_mess_core.telegram.delivery.split_telegram_markdown",
            side_effect=lambda content: split_telegram_markdown(content, limit=100),
        ):
            completed = worker.run_once()

        self.assertEqual(completed["job_id"], job["job_id"])
        self.assertEqual(completed["status"], "completed")
        outbox = self.store.list_delivery_outbox(summary_run_id=completed["summary_run_id"])
        self.assertGreater(len(outbox), 2)
        self.assertTrue(all(item["status"] == "sent" for item in outbox))
        self.assertEqual(
            [item["message_id"] for item in outbox],
            list(range(321, 321 + len(outbox))),
        )
        by_kind: dict[str, list[dict[str, object]]] = {}
        for item in outbox:
            by_kind.setdefault(str(item["content_kind"]), []).append(item)
        self.assertEqual(set(by_kind), {"important_summary", "point_summary"})
        for chunks in by_kind.values():
            self.assertEqual(
                [item["chunk_index"] for item in chunks],
                list(range(1, len(chunks) + 1)),
            )
            self.assertTrue(all(item["chunk_count"] == len(chunks) for item in chunks))
        self.assertEqual(len(runtime.calls), len(outbox))
        self.assertTrue(all(item[0:2] == ("main", "deliver_chunk") for item in runtime.calls))
        self.assertTrue(any("**Important Daily Summary**" in item[2] for item in runtime.calls))
        self.assertTrue(any("**Daily Message Point Summary**" in item[2] for item in runtime.calls))
        parsed_calls = [telethon_markdown.parse(item[2]) for item in runtime.calls]
        self.assertTrue(any(entities for _, entities in parsed_calls))
        self.assertTrue(all("**" not in parsed and "```" not in parsed for parsed, _ in parsed_calls))

    def test_delivery_outbox_routes_database_managed_account_through_standalone_runtime(self) -> None:
        self.store.upsert_account(
            AccountRecord(
                source=SOURCE_TELEGRAM,
                account_id="dynamic",
                display_name="Dynamic",
                kind="telegram",
            )
        )
        template = TelegramAccountConfig(
            account_id="template",
            api_id=123,
            api_hash="hash",
            session_name="template",
            session_dir=self.root / "sessions",
        )
        delivery_config = AppConfig(
            storage=self.config.storage,
            telegram=TelegramConfig(accounts=[template]),
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=self.config.daily.ai,
                delivery=DailyDeliveryConfig(
                    enabled=True,
                    account_id="dynamic",
                    origin_id=-9001,
                ),
            ),
        )
        opened_accounts: list[set[str]] = []
        sent_chunks: list[tuple[str, str]] = []

        class FakeScopedRuntime:
            def __init__(self, config: AppConfig, store: ArchiveStore, account_ids: set[str]) -> None:
                opened_accounts.append(set(account_ids))

            def __enter__(self) -> FakeScopedRuntime:
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def call(self, account_id: str, operation: str, **kwargs: object) -> dict[str, object]:
                sent_chunks.append((account_id, operation))
                return {"status": "sent", "message_ids": [90210], "message_count": 1}

        job = enqueue_daily_summary_job(
            self.store,
            delivery_config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        completed = run_daily_summary_job_from_cli(
            self.store,
            delivery_config,
            job["job_id"],
            delivery_runtime_factory=FakeScopedRuntime,
        )

        self.assertEqual(completed["job_id"], job["job_id"])
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(opened_accounts, [{"dynamic"}])
        self.assertEqual(sent_chunks, [("dynamic", "deliver_chunk")])
        outbox = self.store.list_delivery_outbox(summary_run_id=completed["summary_run_id"])
        self.assertTrue(outbox)
        self.assertTrue(all(item["status"] == "sent" for item in outbox))

    def test_stop_interrupts_runtime_delivery_and_requeues_job(self) -> None:
        delivery_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=self.config.daily.ai,
                delivery=DailyDeliveryConfig(enabled=True, account_id="main", origin_id=-9001),
            ),
        )

        class BlockingRuntime:
            def __init__(self) -> None:
                self.entered = threading.Event()

            def call(
                self,
                account_id: str,
                operation: str,
                *,
                cancel_event: threading.Event,
                **kwargs: object,
            ) -> dict[str, object]:
                self.entered.set()
                if not cancel_event.wait(3):
                    raise TimeoutError("delivery was not interrupted")
                raise RuntimeError("delivery interrupted")

        runtime = BlockingRuntime()
        worker = DailyJobWorker(
            self.store,
            delivery_config,
            telegram_runtime=runtime,
            poll_interval=0.01,
        )
        job = worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        worker.start()
        try:
            self.assertTrue(runtime.entered.wait(3), "worker did not reach delivery")
        finally:
            worker.stop(timeout=2)

        current = self.store.get_daily_summary_job(job["job_id"])
        assert current is not None
        self.assertEqual(current["status"], "queued")
        outbox = self.store.list_delivery_outbox(summary_run_id=current["summary_run_id"])
        self.assertTrue(outbox)
        self.assertTrue(all(item["status"] == "retry" for item in outbox))

    def test_stopping_during_the_last_attempt_keeps_the_chunk_deliverable(self) -> None:
        self.store.persist_daily_summary_batch(
            [],
            [
                {
                    "summary_run_id": "sum_stop",
                    "account_id": "main",
                    "origin_id": -9001,
                    "content_kind": "point_summary",
                    "chunk_index": index,
                    "chunk_count": 2,
                    "content": f"chunk {index}",
                }
                for index in (1, 2)
            ],
        )
        with self.store._lock:
            self.store._conn.execute(
                "UPDATE delivery_outbox SET attempts = ? WHERE chunk_index = 1",
                (DELIVERY_MAX_ATTEMPTS - 1,),
            )
            self.store._conn.commit()

        delivery_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=self.config.daily.ai,
                delivery=DailyDeliveryConfig(enabled=True, account_id="main", origin_id=-9001),
            ),
        )
        worker = DailyJobWorker(self.store, delivery_config, telegram_runtime=None)

        class StoppedRuntime:
            def call(
                self,
                account_id: str,
                operation: str,
                *,
                cancel_event: threading.Event,
                **kwargs: object,
            ) -> dict[str, object]:
                worker._stop_event.set()
                raise RuntimeError("Telegram runtime deliver_chunk canceled while waiting")

        worker.telegram_runtime = StoppedRuntime()

        self.assertFalse(worker._deliver_pending_once())

        outbox = self.store.list_delivery_outbox(summary_run_id="sum_stop")
        self.assertEqual([item["status"] for item in outbox], ["retry", "pending"])
        self.assertEqual(outbox[0]["attempts"], DELIVERY_MAX_ATTEMPTS - 1)
        claimed = self.store.claim_delivery_outbox(now=utc_now_iso(), stale_before=utc_now_iso())
        assert claimed is not None
        self.assertEqual(claimed["chunk_index"], 1)

    def test_failed_report_does_not_block_other_report_and_stops_at_max_attempts(self) -> None:
        self.store.persist_daily_summary_batch(
            [],
            [
                {
                    "outbox_id": "out_imp_1",
                    "summary_run_id": "sum_split",
                    "account_id": "main",
                    "origin_id": -9001,
                    "content_kind": "important_summary",
                    "chunk_index": 1,
                    "chunk_count": 2,
                    "content": "important 1",
                },
                {
                    "outbox_id": "out_imp_2",
                    "summary_run_id": "sum_split",
                    "account_id": "main",
                    "origin_id": -9001,
                    "content_kind": "important_summary",
                    "chunk_index": 2,
                    "chunk_count": 2,
                    "content": "important 2",
                },
                {
                    "outbox_id": "out_pt_1",
                    "summary_run_id": "sum_split",
                    "account_id": "main",
                    "origin_id": -9001,
                    "content_kind": "point_summary",
                    "chunk_index": 1,
                    "chunk_count": 1,
                    "content": "point 1",
                },
            ],
        )
        with self.store._lock:
            self.store._conn.execute(
                "UPDATE delivery_outbox SET attempts = ? WHERE outbox_id = 'out_imp_1'",
                (DELIVERY_MAX_ATTEMPTS - 1,),
            )
            self.store._conn.commit()

        class SelectiveRuntime:
            def __init__(self) -> None:
                self.sent: list[str] = []

            def call(self, account_id: str, operation: str, **kwargs: object) -> dict[str, object]:
                content = str(kwargs["content"])
                if content.startswith("important"):
                    raise RuntimeError("channel unavailable")
                self.sent.append(content)
                return {"status": "sent", "message_ids": [55], "message_count": 1}

        runtime = SelectiveRuntime()
        worker = DailyJobWorker(self.store, self.config, telegram_runtime=runtime)
        self.assertTrue(worker._deliver_pending_once(summary_run_id="sum_split"))
        self.assertTrue(worker._deliver_pending_once(summary_run_id="sum_split"))
        self.assertFalse(worker._deliver_pending_once(summary_run_id="sum_split"))

        by_id = {row["outbox_id"]: row for row in self.store.list_delivery_outbox(summary_run_id="sum_split")}
        self.assertEqual(by_id["out_imp_1"]["status"], "failed")
        self.assertEqual(by_id["out_imp_2"]["status"], "failed")
        self.assertEqual(by_id["out_pt_1"]["status"], "sent")
        self.assertEqual(runtime.sent, ["point 1"])

    def test_pre_v20_outbox_rows_prevent_duplicate_queueing_on_rerun(self) -> None:
        with self.store._lock:
            self.store._conn.execute(
                """
                INSERT INTO delivery_outbox(
                  outbox_id, summary_run_id, job_id, account_id, origin_id, content_kind,
                  chunk_index, chunk_count, content, status, attempts, created_at, updated_at
                )
                VALUES ('out_legacy_1', 'sum_legacy', 'job_legacy', 'main', -9001, 'summary', 1, 1, 'legacy', 'sent', 1, ?, ?)
                """,
                (utc_now_iso(), utc_now_iso()),
            )
            self.store._conn.commit()

        self.store.persist_daily_summary_batch(
            [],
            [
                {
                    "outbox_id": "out_rerun_by_run",
                    "summary_run_id": "sum_legacy",
                    "job_id": "job_other",
                    "account_id": "main",
                    "origin_id": -9001,
                    "content_kind": "point_summary",
                    "chunk_index": 1,
                    "chunk_count": 1,
                    "content": "rerun same run",
                },
                {
                    "outbox_id": "out_rerun_by_job",
                    "summary_run_id": "sum_new_id",
                    "job_id": "job_legacy",
                    "account_id": "main",
                    "origin_id": -9001,
                    "content_kind": "point_summary",
                    "chunk_index": 1,
                    "chunk_count": 1,
                    "content": "rerun same job",
                },
            ],
        )

        self.assertEqual([row["outbox_id"] for row in self.store.list_delivery_outbox()], ["out_legacy_1"])

    def test_stale_worker_after_lease_takeover_cannot_overwrite_db_or_published_files(self) -> None:
        first_entered = threading.Event()
        release_first = threading.Event()
        job = enqueue_daily_summary_job(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        original_pipeline = __import__("tele_mess_core.daily", fromlist=["_run_ai_analysis_pipeline"])._run_ai_analysis_pipeline
        call_count = 0

        def gated_pipeline(*args: object, **kwargs: object) -> dict[str, object]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                first_entered.set()
                release_first.wait(5)
            return original_pipeline(*args, **kwargs)

        stale_worker = DailyJobWorker(self.store, self.config, worker_id="stale", heartbeat_interval=60.0)
        winner_worker = DailyJobWorker(self.store, self.config, worker_id="winner", heartbeat_interval=60.0)

        with patch("tele_mess_core.daily._run_ai_analysis_pipeline", side_effect=gated_pipeline):
            stale_thread = threading.Thread(target=stale_worker.run_once)
            stale_thread.start()
            self.assertTrue(first_entered.wait(3))

            # Expire stale worker's lease so winner takes over attempt 2.
            with self.store._lock:
                self.store._conn.execute(
                    "UPDATE daily_summary_jobs SET lease_until = '2000-01-01T00:00:00+00:00' WHERE job_id = ?",
                    (job["job_id"],),
                )
                self.store._conn.commit()

            winner_result = winner_worker.run_once()
            assert winner_result is not None
            self.assertEqual(winner_result["status"], "completed")
            self.assertEqual(winner_result["attempt"], 2)
            winning_summary = self.store.get_daily_summary_run(str(winner_result["summary_run_id"]))
            assert winning_summary is not None
            winning_path = Path(str(winning_summary["summary_path"]))
            self.assertIn("attempt-2", str(winning_path))
            winning_content = winning_path.read_text(encoding="utf-8")

            # Now let the stale worker continue and try to publish.
            release_first.set()
            stale_thread.join(timeout=5)

        final_job = self.store.get_daily_summary_job(job["job_id"])
        assert final_job is not None
        self.assertEqual(final_job["status"], "completed")
        self.assertEqual(final_job["worker_id"], "winner")
        self.assertEqual(final_job["attempt"], 2)
        final_summary = self.store.get_daily_summary_run(str(final_job["summary_run_id"]))
        assert final_summary is not None
        self.assertEqual(final_summary["status"], "completed")
        self.assertEqual(final_summary["summary_path"], str(winning_path))
        self.assertEqual(winning_path.read_text(encoding="utf-8"), winning_content)

    def test_cancel_during_delivery_discards_remaining_outbox_chunks(self) -> None:
        self.store.set_origin_important(SOURCE_TELEGRAM, "main", -1001, 0, True)
        delivery_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=self.config.daily.ai,
                delivery=DailyDeliveryConfig(enabled=True, account_id="main", origin_id=-9001),
            ),
        )

        class CancelOnFirstRuntime:
            def __init__(self) -> None:
                self.calls = 0
                self.worker: DailyJobWorker | None = None
                self.job_id = ""

            def call(self, account_id: str, operation: str, **kwargs: object) -> dict[str, object]:
                self.calls += 1
                assert self.worker is not None
                self.worker.cancel(self.job_id)
                return {"status": "sent", "message_ids": [777], "message_count": 1}

        runtime = CancelOnFirstRuntime()
        worker = DailyJobWorker(self.store, delivery_config, telegram_runtime=runtime)
        runtime.worker = worker
        job = worker.enqueue(
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        runtime.job_id = job["job_id"]

        canceled = worker.run_once()

        assert canceled is not None
        self.assertEqual(canceled["status"], "canceled")
        self.assertEqual(runtime.calls, 1)
        self.assertEqual(self.store.list_delivery_outbox(summary_run_id=canceled["summary_run_id"]), [])

    def test_standalone_cli_drains_other_report_on_failure_and_recovers_on_rerun(self) -> None:
        self.store.set_origin_important(SOURCE_TELEGRAM, "main", -1001, 0, True)
        delivery_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=self.config.daily.ai,
                delivery=DailyDeliveryConfig(enabled=True, account_id="main", origin_id=-9001),
            ),
        )
        fail_first = {"remaining": 1}
        sent_contents: list[tuple[str, int | None]] = []

        class FlakyRuntime:
            def __enter__(self) -> FlakyRuntime:
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def call(self, account_id: str, operation: str, **kwargs: object) -> dict[str, object]:
                content = str(kwargs["content"])
                random_id = kwargs.get("random_id")
                if fail_first["remaining"] > 0:
                    fail_first["remaining"] -= 1
                    raise RuntimeError("temporary delivery failure")
                sent_contents.append((content, int(random_id) if random_id is not None else None))
                return {"status": "sent", "message_ids": [len(sent_contents)], "message_count": 1}

        job = enqueue_daily_summary_job(
            self.store,
            delivery_config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        first = run_daily_summary_job_from_cli(
            self.store,
            delivery_config,
            job["job_id"],
            delivery_runtime_factory=lambda _cfg, _store, _ids: FlakyRuntime(),
        )
        self.assertEqual(first["status"], "completed")
        outbox_after_first = {
            row["content_kind"]: row
            for row in self.store.list_delivery_outbox(summary_run_id=first["summary_run_id"])
        }
        # Point summary must have been delivered even though important_summary entered retry.
        self.assertEqual(outbox_after_first["point_summary"]["status"], "sent")
        self.assertEqual(outbox_after_first["important_summary"]["status"], "retry")
        self.assertEqual(len(sent_contents), 1)

        # Re-running the same request recovers the remaining undelivered outbox without re-running AI.
        re_enqueued = enqueue_daily_summary_job(
            self.store,
            delivery_config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        self.assertEqual(re_enqueued["job_id"], first["job_id"])
        second = run_daily_summary_job_from_cli(
            self.store,
            delivery_config,
            re_enqueued["job_id"],
            delivery_runtime_factory=lambda _cfg, _store, _ids: FlakyRuntime(),
        )
        self.assertEqual(second["status"], "completed")
        outbox_after_second = {
            row["content_kind"]: row
            for row in self.store.list_delivery_outbox(summary_run_id=second["summary_run_id"])
        }
        self.assertEqual(outbox_after_second["important_summary"]["status"], "sent")
        self.assertEqual(outbox_after_second["point_summary"]["status"], "sent")
        self.assertEqual(len(sent_contents), 2)
        self.assertIsNotNone(sent_contents[1][1])

    def test_waiting_cli_takes_over_after_lock_holder_exits(self) -> None:
        import time
        from tele_mess_core.daily_jobs import DAILY_WORKER_LOCK_FILENAME, DailyWorkerLock

        holder = DailyWorkerLock(self.root / DAILY_WORKER_LOCK_FILENAME, owner="other-service")
        self.assertTrue(holder.try_acquire())
        job = enqueue_daily_summary_job(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )

        def release_soon() -> None:
            time.sleep(0.15)
            holder.release()

        releaser = threading.Thread(target=release_soon, daemon=True)
        releaser.start()
        try:
            finished = run_daily_summary_job_from_cli(
                self.store,
                self.config,
                job["job_id"],
                timeout=5.0,
                poll_interval=0.02,
            )
        finally:
            holder.release()
            releaser.join(timeout=2)

        self.assertEqual(finished["status"], "completed")
        self.assertEqual(finished["attempt"], 1)

    def test_standalone_cli_stop_event_interrupts_and_requeues_job(self) -> None:
        from tele_mess_core.daily_jobs import DailyJobCliInterrupted

        entered = threading.Event()
        stop_event = threading.Event()
        job = enqueue_daily_summary_job(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )

        def blocking_package(*args: object, cancel_check: object = None, **kwargs: object) -> dict[str, object]:
            entered.set()
            assert callable(cancel_check)
            while True:
                cancel_check()
                stop_event.wait(0.02)

        def trigger_stop() -> None:
            if entered.wait(2.0):
                stop_event.set()

        stopper = threading.Thread(target=trigger_stop, daemon=True)
        stopper.start()
        try:
            with patch("tele_mess_core.daily_jobs.build_daily_package", side_effect=blocking_package):
                with self.assertRaises(DailyJobCliInterrupted):
                    run_daily_summary_job_from_cli(
                        self.store,
                        self.config,
                        job["job_id"],
                        timeout=5.0,
                        poll_interval=0.02,
                        stop_event=stop_event,
                    )
        finally:
            stopper.join(timeout=2)

        after = self.store.get_daily_summary_job(job["job_id"])
        assert after is not None
        self.assertEqual(after["status"], "queued")
        self.assertIsNone(after["worker_id"])

    def test_recovering_v19_run_with_legacy_outbox_publishes_legacy_delivery_metadata(self) -> None:
        import json as json_mod

        delivery_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=self.config.daily.ai,
                delivery=DailyDeliveryConfig(enabled=True, account_id="main", origin_id=-9001),
            ),
        )
        job = enqueue_daily_summary_job(
            self.store,
            delivery_config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        with self.store._lock:
            self.store._conn.execute(
                """
                INSERT INTO delivery_outbox(
                  outbox_id, summary_run_id, job_id, account_id, origin_id, topic_id,
                  content_kind, chunk_index, chunk_count, content, status, attempts,
                  claim_token, send_random_id, created_at, updated_at
                )
                VALUES ('out_legacy_keep', 'sum_legacy_run', ?, 'main', -9001, 0,
                        'summary', 1, 1, 'legacy body', 'sent', 1, 1, 999, ?, ?)
                """,
                (job["job_id"], utc_now_iso(), utc_now_iso()),
            )
            self.store._conn.execute(
                "UPDATE daily_summary_jobs SET summary_run_id = 'sum_legacy_run' WHERE job_id = ?",
                (job["job_id"],),
            )
            self.store._conn.commit()

        worker = DailyJobWorker(self.store, delivery_config, telegram_runtime=None)
        completed = worker.run_once(job_id=job["job_id"])
        assert completed is not None
        self.assertEqual(completed["status"], "completed")
        summary_run = self.store.get_daily_summary_run("sum_legacy_run")
        assert summary_run is not None
        delivery_meta = summary_run["progress"]["delivery"]
        self.assertEqual(delivery_meta["outbox_ids"], ["out_legacy_keep"])
        self.assertEqual(delivery_meta["delivery_kinds"], ["summary"])
        self.assertEqual(delivery_meta["message_count"], 1)
        self.assertEqual(delivery_meta["status"], "sent")
        summary_json = json_mod.loads((Path(str(summary_run["output_dir"])) / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual(summary_json["delivery"]["outbox_ids"], ["out_legacy_keep"])
        self.assertEqual(
            [row["outbox_id"] for row in self.store.list_delivery_outbox(summary_run_id="sum_legacy_run")],
            ["out_legacy_keep"],
        )


if __name__ == "__main__":
    unittest.main()
