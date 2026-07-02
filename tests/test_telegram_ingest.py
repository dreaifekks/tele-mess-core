from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import MediaDownloadConfig, TelegramAccountConfig
from tele_mess_core.models import BackupPolicyRecord, SOURCE_TELEGRAM
from tele_mess_core.telegram.ingest import TelegramArchiveService


class FakeEntity:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def to_dict(self):
        return dict(self.__dict__)


class FakeMessage:
    def __init__(
        self,
        message_id: int,
        chat_id: int,
        text: str = "hello",
        media: object | None = None,
        fail_download_times: int = 0,
        empty_download: bool = False,
    ):
        self.id = message_id
        self.chat_id = chat_id
        self.sender_id = 7
        self.date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.edit_date = None
        self.text = text
        self.media = media
        self.grouped_id = 99 if media else None
        self.reply_to_msg_id = None
        self.reply_to = None
        self.fwd_from = None
        self.reactions = None
        self.fail_download_times = fail_download_times
        self.empty_download = empty_download
        self.download_attempts = 0

    async def get_sender(self):
        return FakeEntity(id=7, username="alice", first_name="Alice", last_name="", bot=False)

    async def get_chat(self):
        return FakeEntity(id=self.chat_id, title="Source Group", username=None)

    async def download_media(self, file):
        self.download_attempts += 1
        if self.fail_download_times > 0:
            self.fail_download_times -= 1
            raise RuntimeError("temporary network error")
        if self.empty_download:
            return None
        target = Path(file) / f"{self.id}.bin"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"media")
        return str(target)

    def to_dict(self):
        return {"id": self.id, "chat_id": self.chat_id, "text": self.text, "has_media": self.media is not None}


class TelegramIngestPolicyTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = ArchiveStore(Path(self.tmp.name) / "archive.db")
        self.store.initialize()
        self.config = TelegramAccountConfig(
            account_id="main",
            api_id=1,
            api_hash="hash",
            session_name="main",
            chats=[],
        )
        self.service = TelegramArchiveService(self.config, self.store, media_download=MediaDownloadConfig(retries=2, retry_delay_seconds=0))

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    async def test_store_message_applies_capture_policy_and_cursor(self) -> None:
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

        stored = await self.service._store_message(FakeMessage(10, -1001, media=object()), event_type="new")
        self.assertTrue(stored)
        messages = self.store.list_messages_after(after_event_seq=0)["items"]
        self.assertIsNone(messages[0]["text"])
        self.assertEqual(messages[0]["has_media"], 0)
        self.assertIsNone(messages[0]["media_kind"])
        self.assertIsNone(messages[0]["raw_json"])
        cursor = self.store.get_capture_cursor(SOURCE_TELEGRAM, "main", -1001)
        self.assertIsNotNone(cursor)
        assert cursor is not None
        self.assertEqual(cursor["last_message_id"], 10)

    async def test_disabled_policy_skips_message(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1002,
                enabled=False,
            )
        )

        stored = await self.service._store_message(FakeMessage(11, -1002), event_type="new")
        self.assertFalse(stored)
        self.assertEqual(self.store.state()["message_count"], 0)

    async def test_download_media_policy_writes_media_file(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1003,
                enabled=True,
                capture_text=True,
                capture_media_metadata=True,
                download_media=True,
            )
        )

        stored = await self.service._store_message(FakeMessage(12, -1003, media=object()), event_type="new")
        self.assertTrue(stored)
        files = self.store.list_media_files(account_id="main", chat_id=-1003, message_id=12)
        self.assertEqual(len(files), 1)
        self.assertTrue(Path(files[0]["file_path"]).exists())
        self.assertEqual(files[0]["file_size"], 5)

    async def test_download_media_retries_and_records_recovered_attempt(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1004,
                enabled=True,
                download_media=True,
            )
        )
        message = FakeMessage(13, -1004, media=object(), fail_download_times=1)

        stored = await self.service._store_message(message, event_type="new")

        self.assertTrue(stored)
        self.assertEqual(message.download_attempts, 2)
        files = self.store.list_media_files(account_id="main", chat_id=-1004, message_id=13)
        self.assertEqual(len(files), 1)
        events = self.store.list_operation_events(account_id="main")
        self.assertEqual(events[0]["operation"], "media_download")
        self.assertEqual(events[0]["status"], "ok")

    async def test_download_media_failure_records_operation_event(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1005,
                enabled=True,
                download_media=True,
            )
        )
        message = FakeMessage(14, -1005, media=object(), fail_download_times=3)

        stored = await self.service._store_message(message, event_type="new")

        self.assertTrue(stored)
        self.assertEqual(message.download_attempts, 3)
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=-1005, message_id=14), [])
        events = self.store.list_operation_events(account_id="main", status="failed")
        self.assertEqual(events[0]["operation"], "media_download")
        self.assertEqual(events[0]["error_code"], "media_download_failed")


if __name__ == "__main__":
    unittest.main()
