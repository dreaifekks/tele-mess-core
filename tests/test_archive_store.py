from __future__ import annotations

import tempfile
import unittest
import sqlite3
from pathlib import Path

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.models import (
    AccountAuthRecord,
    AccountRecord,
    BackupPolicyRecord,
    CaptureCursorRecord,
    ChatRecord,
    MessageRecord,
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
        accounts = self.store.list_accounts()
        self.assertEqual(accounts[0]["account_id"], "main")

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
        topic = next(origin for origin in origins if origin["topic_id"] == 42)
        self.assertTrue(topic["backup_policy"]["enabled"])
        self.assertTrue(topic["backup_policy"]["capture_media_metadata"])
        self.assertFalse(topic["backup_policy"]["download_media"])

        participants = self.store.list_participants(account_id="main", origin_id=-1001)
        self.assertEqual(participants[0]["username"], "alice")
        self.assertFalse(participants[0]["is_bot"])


    def test_backup_policy_and_capture_cursor_are_queryable(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                enabled=True,
                capture_text=False,
                capture_media_metadata=False,
                download_media=False,
            )
        )
        policy = self.store.get_backup_policy(SOURCE_TELEGRAM, "main", -1001)
        self.assertIsNotNone(policy)
        assert policy is not None
        self.assertFalse(policy["capture_text"])
        self.assertFalse(policy["capture_media_metadata"])

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
        self.assertEqual(self.store.list_capture_cursors(account_id="main")[0]["origin_id"], -1001)


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
                self.assertEqual(store.state()["schema_version"], "5")
                messages = store.list_messages_after(after_event_seq=0)
                self.assertEqual(messages["items"][0]["account_id"], "default")
                self.assertEqual(store.list_accounts()[0]["account_id"], "default")
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
