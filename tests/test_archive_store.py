from __future__ import annotations

import tempfile
import threading
import unittest
import sqlite3
from pathlib import Path
from unittest.mock import patch

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.archive.migrations import MIGRATIONS, apply_migrations
from tele_mess_core.models import (
    AccountAuthRecord,
    AccountRecord,
    BackupPolicyRecord,
    CaptureCursorRecord,
    ChatRecord,
    DailySummaryJobRecord,
    DailySummaryRecord,
    MediaFileRecord,
    MessageRecord,
    OperationEventRecord,
    OriginRecord,
    ParticipantRecord,
    SOURCE_TELEGRAM,
    UserRecord,
    utc_now_iso,
)


class ArchiveStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = ArchiveStore(Path(self.tmp.name) / "archive.db")
        self.store.initialize()

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_message_upsert_and_sync_cursor(self) -> None:
        now = utc_now_iso()
        self.store.upsert_account(AccountRecord(source=SOURCE_TELEGRAM, account_id="main", display_name="Main"))
        self.store.upsert_chat(ChatRecord(source=SOURCE_TELEGRAM, chat_id=-1001, title="Source"))
        self.store.upsert_user(UserRecord(source=SOURCE_TELEGRAM, user_id=42, display_name="Alice"))
        seq = self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                chat_id=-1001,
                message_id=10,
                sender_id=42,
                sender_name="Alice",
                sent_at=now,
                ingested_at=now,
                text="hello archive",
            ),
            event_type="new",
        )

        self.assertEqual(seq, 1)
        state = self.store.state()
        self.assertEqual(state["last_event_seq"], 1)
        self.assertEqual(state["message_count"], 1)

        events = self.store.list_events(after=0)
        self.assertEqual(events["next_cursor"], 1)
        self.assertEqual(events["items"][0]["event_type"], "new")

        messages = self.store.list_messages_after(after_event_seq=0)
        self.assertEqual(messages["items"][0]["text"], "hello archive")
        self.assertEqual(messages["items"][0]["event_seq"], 1)
        self.assertEqual(messages["items"][0]["chat_title"], "Source")
        accounts = self.store.list_accounts()
        self.assertEqual(accounts[0]["account_id"], "main")

    def test_each_thread_uses_its_own_sqlite_connection(self) -> None:
        main_connection_id = id(self.store._conn)
        connection_ids: list[int] = []
        errors: list[BaseException] = []
        barrier = threading.Barrier(3)

        def write_account(account_id: str) -> None:
            try:
                barrier.wait()
                connection_ids.append(id(self.store._conn))
                self.store.upsert_account(
                    AccountRecord(source=SOURCE_TELEGRAM, account_id=account_id, display_name=account_id)
                )
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=write_account, args=(account_id,)) for account_id in ("one", "two")]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=5)

        self.assertEqual(errors, [])
        self.assertEqual(len(set(connection_ids)), 2)
        self.assertNotIn(main_connection_id, connection_ids)
        self.assertEqual({item["account_id"] for item in self.store.list_accounts()}, {"one", "two"})

    def test_delete_creates_tombstone(self) -> None:
        seqs = self.store.mark_deleted(SOURCE_TELEGRAM, -1002, [99], event_at=utc_now_iso())
        self.assertEqual(seqs, [1])

        messages = self.store.list_messages_after(after_event_seq=0)
        self.assertEqual(messages["items"][0]["message_id"], 99)
        self.assertIsNotNone(messages["items"][0]["deleted_at"])

    def test_search_messages(self) -> None:
        now = utc_now_iso()
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                chat_id=-1001,
                message_id=11,
                sent_at=now,
                ingested_at=now,
                text="indexed search payload",
            ),
            event_type="new",
        )
        results = self.store.search_messages("indexed")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["message_id"], 11)

    def test_clear_old_message_raw_json_keeps_structured_message_data(self) -> None:
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                chat_id=-1001,
                message_id=1,
                sent_at="2026-01-01T00:00:00+00:00",
                ingested_at="2026-01-01T00:00:00+00:00",
                text="old searchable payload",
                raw_json='{"message":"old searchable payload","media":{"_":"MessageMediaPhoto"}}',
            ),
            event_type="new",
        )
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                chat_id=-1001,
                message_id=2,
                sent_at="2026-01-10T00:00:00+00:00",
                ingested_at="2026-01-10T00:00:00+00:00",
                text="new searchable payload",
                raw_json='{"message":"new searchable payload"}',
            ),
            event_type="new",
        )

        stats = self.store.message_raw_json_stats(cutoff_sent_at="2026-01-07T00:00:00+00:00")
        removed = self.store.clear_message_raw_json_before("2026-01-07T00:00:00+00:00")
        messages = self.store.list_latest_messages(limit=2)["items"]

        self.assertEqual(stats["message_count"], 1)
        self.assertEqual(removed["message_count"], 1)
        by_id = {item["message_id"]: item for item in messages}
        self.assertIsNone(by_id[1]["raw_json"])
        self.assertEqual(by_id[1]["text"], "old searchable payload")
        self.assertIsNotNone(by_id[2]["raw_json"])
        self.assertEqual(self.store.search_messages("old")[0]["message_id"], 1)

    def test_reaction_creates_minimal_message_if_missing(self) -> None:
        self.store.update_reactions(
            SOURCE_TELEGRAM,
            -1003,
            12,
            reactions={"results": [{"reaction": "like", "count": 1}]},
            event_at=utc_now_iso(),
        )
        messages = self.store.list_messages_after(after_event_seq=0)
        self.assertEqual(messages["items"][0]["message_id"], 12)
        self.assertEqual(messages["items"][0]["reactions_json"]["results"][0]["reaction"], "like")

    def test_same_chat_message_can_exist_in_multiple_accounts(self) -> None:
        now = utc_now_iso()
        for account_id, text in (("main", "from main"), ("alt", "from alt")):
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id=account_id,
                    chat_id=-1001,
                    message_id=20,
                    sent_at=now,
                    ingested_at=now,
                    text=text,
                ),
                event_type="new",
            )

        messages = self.store.list_messages_after(after_event_seq=0)
        self.assertEqual(len(messages["items"]), 2)
        self.assertEqual({item["account_id"] for item in messages["items"]}, {"main", "alt"})

    def test_latest_messages_returns_newest_sent_messages_first(self) -> None:
        self.store.upsert_chat(ChatRecord(source=SOURCE_TELEGRAM, chat_id=-1001, title="Known Chat"))
        messages = (
            (1, "2026-01-01T00:00:00+00:00"),
            (2, "2026-01-03T00:00:00+00:00"),
            (3, "2026-01-02T00:00:00+00:00"),
        )
        for message_id, sent_at in messages:
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    chat_id=-1001,
                    message_id=message_id,
                    sent_at=sent_at,
                    ingested_at=utc_now_iso(),
                    text=f"message {message_id}",
                ),
                event_type="new",
            )

        latest = self.store.list_latest_messages(limit=2)

        self.assertEqual([item["message_id"] for item in latest["items"]], [2, 3])
        self.assertEqual([item["chat_title"] for item in latest["items"]], ["Known Chat", "Known Chat"])
        self.assertEqual(latest["next_cursor"], 3)
        self.assertTrue(latest["has_more"])

    def test_topic_message_latest_includes_origin_title(self) -> None:
        self.store.upsert_chat(ChatRecord(source=SOURCE_TELEGRAM, account_id="main", chat_id=-1001, title="Forum"))
        self.store.upsert_origin(
            OriginRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                topic_id=42,
                origin_type="topic",
                parent_origin_id=-1001,
                title="Topic One",
            )
        )
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-1001,
                topic_id=42,
                message_id=4,
                sent_at="2026-01-04T00:00:00+00:00",
                ingested_at=utc_now_iso(),
                text="topic payload",
            ),
            event_type="new",
        )

        latest = self.store.list_latest_messages(limit=1)

        self.assertEqual(latest["items"][0]["chat_title"], "Forum")
        self.assertEqual(latest["items"][0]["origin_title"], "Topic One")

    def test_management_objects_cover_account_origin_policy_and_participants(self) -> None:
        self.store.upsert_account(AccountRecord(source=SOURCE_TELEGRAM, account_id="main", display_name="Main"))
        self.store.upsert_account_auth(
            AccountAuthRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                auth_state="needs_login",
                session_name="main",
                session_dir="/tmp/sessions",
            )
        )
        self.store.upsert_origin(
            OriginRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                origin_type="group",
                title="Source Group",
                is_forum=True,
                last_message_at="2026-01-01T00:00:00+00:00",
            )
        )
        self.store.upsert_origin(
            OriginRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                topic_id=42,
                origin_type="topic",
                parent_origin_id=-1001,
                title="Important Topic",
            )
        )
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                topic_id=42,
                enabled=True,
                capture_text=True,
                capture_media_metadata=True,
                download_media=False,
                tags="ops,important",
            )
        )
        self.store.upsert_participant(
            ParticipantRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                user_id=7,
                username="alice",
                display_name="Alice",
                role="member",
            )
        )

        accounts = self.store.list_management_accounts()
        self.assertEqual(accounts[0]["auth_state"], "needs_login")
        self.assertEqual(accounts[0]["session_name"], "main")

        origins = self.store.list_origins(account_id="main")
        self.assertEqual({origin["origin_type"] for origin in origins}, {"group", "topic"})
        group = next(origin for origin in origins if origin["topic_id"] == 0)
        self.assertEqual(group["last_message_at"], "2026-01-01T00:00:00+00:00")
        self.assertFalse(group["important"])
        topic = next(origin for origin in origins if origin["topic_id"] == 42)
        self.assertTrue(topic["backup_policy"]["enabled"])
        self.assertTrue(topic["backup_policy"]["capture_media_metadata"])
        self.assertFalse(topic["backup_policy"]["download_media"])
        self.assertEqual(topic["backup_policy"]["tags"], "ops,important")

        self.store.set_origin_important(SOURCE_TELEGRAM, "main", -1001, 42, True)
        self.store.upsert_origin(
            OriginRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                topic_id=42,
                origin_type="topic",
                parent_origin_id=-1001,
                title="Important Topic",
            )
        )
        topic = next(origin for origin in self.store.list_origins(account_id="main") if origin["topic_id"] == 42)
        self.assertTrue(topic["important"])

        self.store.upsert_capture_cursor(
            CaptureCursorRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                last_message_id=20,
                last_message_at="2026-01-02T00:00:00+00:00",
            )
        )
        origins = self.store.list_origins(account_id="main")
        group = next(origin for origin in origins if origin["topic_id"] == 0)
        self.assertEqual(group["last_message_at"], "2026-01-02T00:00:00+00:00")

        changed = self.store.archive_origin(SOURCE_TELEGRAM, "main", -1001, archived=True)
        self.assertGreaterEqual(changed, 2)
        self.assertEqual(self.store.list_origins(account_id="main"), [])
        archived_origins = self.store.list_origins(account_id="main", include_archived=True)
        self.assertEqual(len(archived_origins), 2)
        self.assertTrue(all(origin["archived_at"] for origin in archived_origins))
        archived_topic = next(origin for origin in archived_origins if origin["topic_id"] == 42)
        self.assertFalse(archived_topic["backup_policy"]["enabled"])

        self.store.archive_origin(SOURCE_TELEGRAM, "main", -1001, archived=False)
        self.assertEqual(len(self.store.list_origins(account_id="main")), 2)

        participants = self.store.list_participants(account_id="main", origin_id=-1001)
        self.assertEqual(participants[0]["username"], "alice")
        self.assertFalse(participants[0]["is_bot"])

    def test_topic_inherits_archived_parent_state(self) -> None:
        self.store.upsert_origin(
            OriginRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                origin_type="group",
                title="Archived Forum",
                is_forum=True,
            )
        )
        self.store.archive_origin(SOURCE_TELEGRAM, "main", -1001, archived=True)

        self.store.upsert_origin(
            OriginRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                topic_id=42,
                origin_type="topic",
                parent_origin_id=-1001,
                title="Late Topic",
            )
        )

        self.assertEqual(self.store.list_origins(account_id="main"), [])
        archived = self.store.list_origins(account_id="main", include_archived=True)
        topic = next(origin for origin in archived if origin["topic_id"] == 42)
        self.assertTrue(topic["archived_at"])

    def test_backup_policy_and_capture_cursor_are_queryable(self) -> None:
        self.store.upsert_origin(
            OriginRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                origin_type="group",
                title="Cursor Chat",
            )
        )
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                enabled=True,
                capture_text=False,
                capture_media_metadata=False,
                download_media=False,
                tags="alpha,beta",
            )
        )
        policy = self.store.get_backup_policy(SOURCE_TELEGRAM, "main", -1001)
        self.assertIsNotNone(policy)
        assert policy is not None
        self.assertFalse(policy["capture_text"])
        self.assertFalse(policy["capture_media_metadata"])
        self.assertEqual(policy["tags"], "alpha,beta")

        self.store.upsert_capture_cursor(
            CaptureCursorRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                last_message_id=10,
                last_message_at="2026-01-01T00:00:00+00:00",
            )
        )
        self.store.upsert_capture_cursor(
            CaptureCursorRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                last_message_id=5,
                last_message_at="2026-01-01T00:00:01+00:00",
            )
        )
        cursor = self.store.get_capture_cursor(SOURCE_TELEGRAM, "main", -1001)
        self.assertIsNotNone(cursor)
        assert cursor is not None
        self.assertEqual(cursor["last_message_id"], 10)
        listed_cursor = self.store.list_capture_cursors(account_id="main")[0]
        self.assertEqual(listed_cursor["origin_id"], -1001)
        self.assertEqual(listed_cursor["origin_title"], "Cursor Chat")

    def test_media_files_include_chat_title(self) -> None:
        now = utc_now_iso()
        self.store.upsert_chat(ChatRecord(source=SOURCE_TELEGRAM, account_id="main", chat_id=-1002, title="Media Chat"))
        self.store.upsert_media_file(
            MediaFileRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-1002,
                message_id=22,
                file_path="/tmp/media.bin",
                media_kind="photo",
                downloaded_at=now,
            )
        )

        files = self.store.list_media_files(account_id="main")

        self.assertEqual(files[0]["chat_title"], "Media Chat")

    def test_media_files_can_be_grouped_by_message(self) -> None:
        now = utc_now_iso()
        self.store.upsert_media_file(
            MediaFileRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-1002,
                message_id=22,
                file_path="/tmp/media.bin",
                media_kind="document",
                downloaded_at=now,
            )
        )

        grouped = self.store.list_media_files_for_messages(
            [{"source": SOURCE_TELEGRAM, "account_id": "main", "chat_id": -1002, "message_id": 22}]
        )

        self.assertEqual(len(grouped[(SOURCE_TELEGRAM, "main", -1002, 22)]), 1)
        self.assertEqual(grouped[(SOURCE_TELEGRAM, "main", -1002, 22)][0]["file_path"], "/tmp/media.bin")

    def test_media_files_grouping_handles_large_message_batches(self) -> None:
        now = utc_now_iso()
        for message_id in (1, 500, 1100):
            self.store.upsert_media_file(
                MediaFileRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=-1002,
                    message_id=message_id,
                    file_path=f"/tmp/media-{message_id}.bin",
                    media_kind="document",
                    downloaded_at=now,
                )
            )

        grouped = self.store.list_media_files_for_messages(
            [
                {"source": SOURCE_TELEGRAM, "account_id": "main", "chat_id": -1002, "message_id": message_id}
                for message_id in range(1, 1101)
            ]
        )

        self.assertEqual(grouped[(SOURCE_TELEGRAM, "main", -1002, 1)][0]["file_path"], "/tmp/media-1.bin")
        self.assertEqual(grouped[(SOURCE_TELEGRAM, "main", -1002, 500)][0]["file_path"], "/tmp/media-500.bin")
        self.assertEqual(grouped[(SOURCE_TELEGRAM, "main", -1002, 1100)][0]["file_path"], "/tmp/media-1100.bin")

    def test_operation_events_are_queryable_and_counted_in_state(self) -> None:
        now = utc_now_iso()
        self.store.upsert_chat(ChatRecord(source=SOURCE_TELEGRAM, account_id="main", chat_id=-1001, title="Error Chat"))
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-1001,
                message_id=10,
                sent_at=now,
                ingested_at=now,
                text="failed media message",
            ),
            event_type="new",
        )
        self.store.add_operation_event(
            OperationEventRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                operation="media_download",
                status="failed",
                subject_type="message",
                subject_id="-1001/10",
                error_code="media_download_failed",
                message="network down",
            )
        )

        events = self.store.list_operation_events(account_id="main", status="failed")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["operation"], "media_download")
        self.assertEqual(events[0]["error_code"], "media_download_failed")
        self.assertEqual(self.store.state()["operation_error_count"], 1)
        self.assertEqual(events[0]["subject_chat_title"], "Error Chat")
        self.assertEqual(events[0]["subject_message_id"], 10)
        self.assertEqual(events[0]["subject_text"], "failed media message")

        deleted = self.store.delete_operation_events([events[0]["id"]])

        self.assertEqual(deleted, 1)
        self.assertEqual(self.store.list_operation_events(account_id="main", status="failed"), [])
        self.assertEqual(self.store.state()["operation_error_count"], 0)

    def test_daily_summary_records_are_queryable_by_tags_and_flags(self) -> None:
        self.store.upsert_daily_summary_record(
            DailySummaryRecord(
                summary_id="sum_a",
                run_id="sum_a",
                package_run_id="pkg_a",
                date="2026-07-03",
                timezone="UTC",
                tags_json='["web3", "info"]',
                important=True,
                provider="disabled",
                title="Daily Summary 2026-07-03",
                content_md="# Daily Summary\n\nweb3 alpha",
                content_json='{"source":"test"}',
                summary_path="/tmp/summary.md",
                origin_count=1,
                group_count=1,
            )
        )
        self.store.upsert_daily_summary_record(
            DailySummaryRecord(
                summary_id="sum_a_group_ai",
                run_id="sum_a",
                package_run_id="pkg_a",
                date="2026-07-03",
                timezone="UTC",
                tags_json='["ai", "info"]',
                important=False,
                provider="disabled",
                title="Daily Summary 2026-07-03 - ai,info",
                content_md="# Daily Summary\n\nai alpha",
                content_json='{"source":"test","record_type":"tag_group"}',
                summary_path="/tmp/summary-ai.md",
                origin_count=1,
                group_count=1,
            )
        )

        listed = self.store.list_daily_summary_records(tags=["web3", "info"], important=True)

        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0]["summary_id"], "sum_a")
        self.assertEqual(listed[0]["tags"], ["web3", "info"])
        self.assertEqual(listed[0]["tags_csv"], "web3,info")
        self.assertNotIn("content_md", listed[0])
        self.assertIn("web3 alpha", listed[0]["content_preview"])

        fetched = self.store.get_daily_summary_record(run_id="sum_a")
        assert fetched is not None
        self.assertIn(fetched["content_md"], {"# Daily Summary\n\nweb3 alpha", "# Daily Summary\n\nai alpha"})
        self.assertEqual(fetched["run_id"], "sum_a")

        run_records = self.store.list_daily_summary_records(run_id="sum_a")
        self.assertEqual(len(run_records), 2)

        self.assertEqual(self.store.list_daily_summary_records(tags=["web3", "ai"]), [])

        changed = self.store.set_daily_summary_records_deleted(["sum_a", "sum_a_group_ai"], deleted=True)
        self.assertEqual(changed, 2)
        self.assertEqual(self.store.list_daily_summary_records(run_id="sum_a"), [])
        deleted_records = self.store.list_daily_summary_records(run_id="sum_a", deleted=True)
        self.assertEqual(len(deleted_records), 2)
        self.assertTrue(all(item["deleted"] for item in deleted_records))
        self.assertIsNone(self.store.get_daily_summary_record(summary_id="sum_a"))
        deleted_record = self.store.get_daily_summary_record(summary_id="sum_a", include_deleted=True)
        assert deleted_record is not None
        self.assertTrue(deleted_record["deleted"])

        restored = self.store.set_daily_summary_records_deleted(["sum_a"], deleted=False)
        self.assertEqual(restored, 1)
        self.assertEqual(len(self.store.list_daily_summary_records(run_id="sum_a")), 1)

    def test_daily_summary_jobs_track_progress_and_cancel_request(self) -> None:
        self.store.upsert_daily_summary_job(
            DailySummaryJobRecord(
                job_id="job_a",
                status="running",
                date="2026-07-03",
                timezone="UTC",
                scope_json='{"tags":"web3"}',
                package_run_id="pkg_a",
                provider="disabled",
                progress_total=4,
                progress_current=1,
                progress_label="packaging",
                progress_json='{"stage":"package","current":1,"total":4}',
            )
        )

        job = self.store.get_daily_summary_job("job_a")
        assert job is not None
        self.assertEqual(job["progress"]["stage"], "package")
        self.assertEqual(job["scope"], {"tags": "web3"})

        canceled = self.store.request_daily_summary_job_cancel("job_a")
        assert canceled is not None
        self.assertEqual(canceled["status"], "cancel_requested")
        self.assertIsNotNone(canceled["cancel_requested_at"])


class ArchiveMigrationTest(unittest.TestCase):
    def test_v1_database_migrates_to_account_aware_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "archive.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO meta(key, value) VALUES('schema_version', '1');
                CREATE TABLE chats (
                  source TEXT NOT NULL, chat_id INTEGER NOT NULL, title TEXT, username TEXT,
                  kind TEXT, updated_at TEXT NOT NULL, raw_json TEXT, PRIMARY KEY(source, chat_id)
                );
                CREATE TABLE users (
                  source TEXT NOT NULL, user_id INTEGER NOT NULL, username TEXT, display_name TEXT,
                  is_bot INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL, raw_json TEXT,
                  PRIMARY KEY(source, user_id)
                );
                CREATE TABLE messages (
                  source TEXT NOT NULL, chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                  topic_id INTEGER, sender_id INTEGER, sender_name TEXT, sender_username TEXT,
                  sent_at TEXT NOT NULL, edited_at TEXT, ingested_at TEXT NOT NULL,
                  deleted_at TEXT, text TEXT, has_media INTEGER NOT NULL DEFAULT 0,
                  media_kind TEXT, grouped_id TEXT, reply_to_message_id INTEGER,
                  forward_from_id TEXT, forward_from_name TEXT, permalink TEXT,
                  reactions_json TEXT, raw_json TEXT, version INTEGER NOT NULL DEFAULT 1,
                  PRIMARY KEY(source, chat_id, message_id)
                );
                CREATE TABLE events (
                  seq INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL,
                  event_type TEXT NOT NULL, chat_id INTEGER NOT NULL, message_id INTEGER,
                  event_at TEXT NOT NULL, payload_json TEXT NOT NULL
                );
                INSERT INTO messages(source, chat_id, message_id, sent_at, ingested_at, text)
                VALUES('telegram', -1001, 1, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:01+00:00', 'old');
                INSERT INTO events(source, event_type, chat_id, message_id, event_at, payload_json)
                VALUES('telegram', 'new', -1001, 1, '2026-01-01T00:00:01+00:00', '{}');
                """
            )
            conn.commit()
            conn.close()

            store = ArchiveStore(db_path)
            try:
                store.initialize()
                self.assertEqual(store.state()["schema_version"], 14)
                messages = store.list_messages_after(after_event_seq=0)
                self.assertEqual(messages["items"][0]["account_id"], "default")
                self.assertEqual(store.list_accounts()[0]["account_id"], "default")
            finally:
                store.close()

    def test_v12_job_schema_migrates_to_durable_queue_and_outbox(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.executescript(
                """
                CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO meta(key, value) VALUES('schema_version', '12');
                PRAGMA user_version = 12;
                CREATE TABLE daily_summary_jobs (
                  job_id TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  date TEXT,
                  timezone TEXT,
                  scope_json TEXT,
                  package_run_id TEXT,
                  summary_run_id TEXT,
                  provider TEXT,
                  progress_total INTEGER NOT NULL DEFAULT 0,
                  progress_current INTEGER NOT NULL DEFAULT 0,
                  progress_label TEXT,
                  progress_json TEXT,
                  cancel_requested_at TEXT,
                  error TEXT,
                  started_at TEXT NOT NULL,
                  finished_at TEXT,
                  updated_at TEXT NOT NULL
                );
                INSERT INTO daily_summary_jobs(job_id, status, started_at, updated_at)
                VALUES('legacy_job', 'queued', '2026-07-01T00:00:00+00:00', '2026-07-01T00:00:00+00:00');
                """
            )

            apply_migrations(conn, 12, 14)

            columns = {row[1] for row in conn.execute("PRAGMA table_info(daily_summary_jobs)")}
            self.assertTrue({"request_json", "dedupe_key", "worker_id", "lease_until", "heartbeat_at", "attempt"} <= columns)
            self.assertIsNotNone(
                conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='delivery_outbox'").fetchone()
            )
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
            self.assertEqual(
                conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0],
                "14",
            )
            self.assertEqual(conn.execute("SELECT status FROM daily_summary_jobs WHERE job_id='legacy_job'").fetchone()[0], "queued")

            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO daily_summary_jobs(job_id, status, started_at, updated_at) VALUES(?, ?, ?, ?)",
                    ("bad_job", "unknown", "2026-07-01T00:00:00+00:00", "2026-07-01T00:00:00+00:00"),
                )
        finally:
            conn.close()

    def test_failed_versioned_migration_rolls_back_schema_and_version(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("INSERT INTO meta(key, value) VALUES('schema_version', '14')")
        conn.execute("PRAGMA user_version = 14")
        conn.commit()

        def failing_migration(connection: sqlite3.Connection) -> None:
            connection.execute("CREATE TABLE should_rollback(id INTEGER PRIMARY KEY)")
            raise RuntimeError("migration failed")

        try:
            with patch.dict(MIGRATIONS, {15: failing_migration}):
                with self.assertRaisesRegex(RuntimeError, "migration failed"):
                    apply_migrations(conn, 14, 15)
            self.assertIsNone(
                conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='should_rollback'").fetchone()
            )
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
            self.assertEqual(
                conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0],
                "14",
            )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
