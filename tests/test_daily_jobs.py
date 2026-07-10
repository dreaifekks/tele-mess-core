from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import (
    AppConfig,
    DailyAiConfig,
    DailyDeliveryConfig,
    DailyPackagingConfig,
    LoggingConfig,
    ServerConfig,
    StorageConfig,
    TelegramConfig,
)
from tele_mess_core.daily_jobs import DailyJobWorker
from tele_mess_core.models import BackupPolicyRecord, MessageRecord, OriginRecord, SOURCE_TELEGRAM, utc_now_iso


class DailyJobWorkerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = ArchiveStore(self.root / "archive.db")
        self.store.initialize()
        self.config = AppConfig(
            storage=StorageConfig(data_dir=self.root, database=self.root / "archive.db"),
            telegram=TelegramConfig(),
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

        completed = worker.run_once()

        self.assertEqual(completed["job_id"], job["job_id"])
        self.assertEqual(completed["status"], "completed")
        outbox = self.store.list_delivery_outbox(summary_run_id=completed["summary_run_id"])
        self.assertEqual(len(outbox), 2)
        self.assertTrue(all(item["status"] == "sent" for item in outbox))
        self.assertEqual([item["message_id"] for item in outbox], [321, 322])
        self.assertEqual([item["chunk_index"] for item in outbox], [1, 2])
        self.assertEqual(len(runtime.calls), 2)
        self.assertTrue(all(item[0:2] == ("main", "deliver_chunk") for item in runtime.calls))
        self.assertIn("# Important Daily Summary", runtime.calls[0][2])
        self.assertNotIn("- Tags: #point", runtime.calls[0][2])
        self.assertIn("# Daily Message Point Summary", runtime.calls[1][2])
        self.assertIn("- Tags: #point", runtime.calls[1][2])

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


if __name__ == "__main__":
    unittest.main()
