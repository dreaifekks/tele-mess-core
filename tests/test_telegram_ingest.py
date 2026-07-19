from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import BackfillConfig, MediaDownloadConfig, TelegramAccountConfig
from tele_mess_core.models import BackupPolicyRecord, CaptureCursorRecord, SOURCE_TELEGRAM
from tele_mess_core.telegram.ingest import CaptureTarget, TelegramArchiveService


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
        reply_to: object | None = None,
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
        self.reply_to = reply_to
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


class MessageMediaWebPage:
    def __init__(self, webpage):
        self.webpage = webpage


class MessageMediaDocument:
    def __init__(self, document=None):
        self.document = document


class Document:
    def __init__(self, attributes=None):
        self.attributes = list(attributes or [])


class DocumentAttributeSticker:
    pass


class DocumentAttributeCustomEmoji:
    pass


class WebPage:
    def __init__(self, document=None, photo=None):
        self.document = document
        self.photo = photo


class WebPagePending:
    pass


class FloodWaitError(RuntimeError):
    def __init__(self, seconds: int):
        super().__init__(f"A wait of {seconds} seconds is required")
        self.seconds = seconds


class ChannelPrivateError(RuntimeError):
    pass


class FakeBackfillIterator:
    def __init__(
        self,
        messages: list[FakeMessage] | None = None,
        error: Exception | None = None,
        on_exhausted=None,
    ):
        self.messages = list(messages or [])
        self.error = error
        self.on_exhausted = on_exhausted
        self.exhausted = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.error:
            error = self.error
            self.error = None
            raise error
        if not self.messages:
            if not self.exhausted:
                self.exhausted = True
                if self.on_exhausted is not None:
                    self.on_exhausted()
            raise StopAsyncIteration
        return self.messages.pop(0)


class FakeBackfillClient:
    def __init__(self, results):
        self.results = results
        self.calls = []

    def iter_messages(self, chat_id, **kwargs):
        self.calls.append((chat_id, kwargs))
        result = self.results[chat_id]
        if isinstance(result, Exception):
            return FakeBackfillIterator(error=result)
        return FakeBackfillIterator(messages=result)


class FakePagedBackfillClient:
    """Small Telethon history model that enforces cursor, head, and page bounds."""

    def __init__(self, histories: dict[int, list[FakeMessage]]):
        self.histories = {chat_id: list(messages) for chat_id, messages in histories.items()}
        self.calls: list[tuple[int, dict[str, object]]] = []
        self.head_calls: list[tuple[int, dict[str, object]]] = []
        self.fail_next_get_messages: dict[int, int] = {}
        self.fail_next_reverse_pages: dict[int, int] = {}
        self.fail_on_reverse_call: dict[int, int] = {}
        self.reverse_call_counts: dict[int, int] = {}
        self.append_after_next_reverse_page: dict[int, FakeMessage] = {}

    async def get_messages(self, chat_id, **kwargs):
        self.head_calls.append((chat_id, kwargs))
        if self.fail_next_get_messages.get(chat_id, 0) > 0:
            self.fail_next_get_messages[chat_id] -= 1
            raise RuntimeError("temporary head failure")
        messages = self._filtered_messages(chat_id, kwargs, reverse=False)
        limit = kwargs.get("limit", 1)
        return messages if limit is None else messages[: int(limit)]

    def iter_messages(self, chat_id, **kwargs):
        self.calls.append((chat_id, dict(kwargs)))
        reverse = bool(kwargs.get("reverse", False))
        if reverse:
            reverse_call = self.reverse_call_counts.get(chat_id, 0) + 1
            self.reverse_call_counts[chat_id] = reverse_call
            if self.fail_on_reverse_call.get(chat_id) == reverse_call:
                self.fail_on_reverse_call.pop(chat_id, None)
                return FakeBackfillIterator(error=RuntimeError("temporary paged history failure"))
        if reverse and self.fail_next_reverse_pages.get(chat_id, 0) > 0:
            self.fail_next_reverse_pages[chat_id] -= 1
            return FakeBackfillIterator(error=RuntimeError("temporary history failure"))

        messages = self._filtered_messages(chat_id, kwargs, reverse=reverse)
        limit = kwargs.get("limit")
        if limit is not None:
            messages = messages[: int(limit)]

        append_message = self.append_after_next_reverse_page.pop(chat_id, None) if reverse else None

        def append_new_head() -> None:
            if append_message is not None:
                self.histories[chat_id].append(append_message)

        return FakeBackfillIterator(
            messages=messages,
            on_exhausted=append_new_head if append_message is not None else None,
        )

    def _filtered_messages(self, chat_id: int, kwargs: dict[str, object], *, reverse: bool) -> list[FakeMessage]:
        messages = list(self.histories[chat_id])
        reply_to = int(kwargs.get("reply_to") or 0)
        if reply_to:
            messages = [message for message in messages if _fake_topic_id(message) == reply_to]

        min_id = int(kwargs.get("min_id") or 0)
        max_id = int(kwargs.get("max_id") or 0)
        offset_id = int(kwargs.get("offset_id") or 0)
        messages = [message for message in messages if int(message.id) > min_id]
        if max_id:
            messages = [message for message in messages if int(message.id) < max_id]
        if offset_id:
            if reverse:
                messages = [message for message in messages if int(message.id) > offset_id]
            else:
                messages = [message for message in messages if int(message.id) < offset_id]
        return sorted(messages, key=lambda message: int(message.id), reverse=not reverse)


class BlockingPagedBackfillClient(FakePagedBackfillClient):
    def __init__(self, histories: dict[int, list[FakeMessage]]):
        super().__init__(histories)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.active_gets = 0
        self.max_active_gets = 0

    async def get_messages(self, chat_id, **kwargs):
        self.active_gets += 1
        self.max_active_gets = max(self.max_active_gets, self.active_gets)
        try:
            if not self.entered.is_set():
                self.entered.set()
                await self.release.wait()
            return await super().get_messages(chat_id, **kwargs)
        finally:
            self.active_gets -= 1


def _fake_topic_id(message: FakeMessage) -> int:
    reply_to = getattr(message, "reply_to", None)
    return int(getattr(reply_to, "reply_to_top_id", 0) or 0)


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

    async def test_missing_policy_skips_message_by_default(self) -> None:
        stored = await self.service._store_message(FakeMessage(15, -1006), event_type="new")

        self.assertFalse(stored)
        self.assertEqual(self.store.state()["message_count"], 0)

    async def test_topic_policy_captures_topic_message_without_parent_policy(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                topic_id=42,
                enabled=True,
            )
        )
        reply_to = SimpleNamespace(reply_to_top_id=42, reply_to_msg_id=100)

        stored = await self.service._store_message(FakeMessage(20, -1001, reply_to=reply_to), event_type="new")

        self.assertTrue(stored)
        latest = self.store.list_latest_messages()["items"]
        self.assertEqual(latest[0]["message_id"], 20)
        self.assertEqual(latest[0]["topic_id"], 42)
        cursor = self.store.get_capture_cursor(SOURCE_TELEGRAM, "main", -1001, 42)
        self.assertIsNotNone(cursor)
        assert cursor is not None
        self.assertEqual(cursor["last_message_id"], 20)

    def test_capture_targets_include_enabled_management_policies(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(source=SOURCE_TELEGRAM, account_id="main", origin_id=-1002, enabled=True)
        )
        self.store.set_backup_policy(
            BackupPolicyRecord(source=SOURCE_TELEGRAM, account_id="main", origin_id=-1003, topic_id=42, enabled=True)
        )
        self.store.set_backup_policy(
            BackupPolicyRecord(source=SOURCE_TELEGRAM, account_id="main", origin_id=-1004, enabled=False)
        )

        targets = self.service._capture_targets()

        self.assertEqual({(target.chat_id, target.topic_id) for target in targets}, {(-1002, 0), (-1003, 42)})

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

        stored = await self.service._store_message(FakeMessage(12, -1003, media=MessageMediaDocument()), event_type="new")
        self.assertTrue(stored)
        files = self.store.list_media_files(account_id="main", chat_id=-1003, message_id=12)
        self.assertEqual(len(files), 1)
        self.assertTrue(Path(files[0]["file_path"]).exists())
        self.assertEqual(files[0]["file_size"], 5)

    async def test_backfill_does_not_redownload_existing_media(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1019,
                enabled=True,
                download_media=True,
            )
        )
        message = FakeMessage(23, -1019, media=MessageMediaDocument())

        await self.service._store_message(message, event_type="new")
        await self.service._store_message(message, event_type="backfill")

        self.assertEqual(message.download_attempts, 1)
        files = self.store.list_media_files(account_id="main", chat_id=-1019, message_id=23)
        self.assertEqual(len(files), 1)

    async def test_sticker_document_metadata_does_not_download_file(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1010,
                enabled=True,
                capture_text=True,
                capture_media_metadata=True,
                download_media=True,
            )
        )
        message = FakeMessage(
            18,
            -1010,
            media=MessageMediaDocument(Document(attributes=[DocumentAttributeSticker()])),
        )

        stored = await self.service._store_message(message, event_type="new")

        self.assertTrue(stored)
        self.assertEqual(message.download_attempts, 0)
        files = self.store.list_media_files(account_id="main", chat_id=-1010, message_id=18)
        self.assertEqual(files, [])
        messages = self.store.list_messages_after(after_event_seq=0)["items"]
        self.assertEqual(messages[0]["has_media"], 1)
        self.assertEqual(messages[0]["media_kind"], "MessageMediaDocument")
        self.assertEqual(self.store.list_operation_events(account_id="main"), [])

    async def test_custom_emoji_document_metadata_does_not_download_file(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1011,
                enabled=True,
                download_media=True,
            )
        )
        message = FakeMessage(
            19,
            -1011,
            media=MessageMediaDocument(Document(attributes=[DocumentAttributeCustomEmoji()])),
        )

        stored = await self.service._store_message(message, event_type="new")

        self.assertTrue(stored)
        self.assertEqual(message.download_attempts, 0)
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=-1011, message_id=19), [])
        self.assertEqual(self.store.list_operation_events(account_id="main"), [])

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
        message = FakeMessage(13, -1004, media=MessageMediaDocument(), fail_download_times=1)

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
        message = FakeMessage(14, -1005, media=MessageMediaDocument(), fail_download_times=3)

        stored = await self.service._store_message(message, event_type="new")

        self.assertTrue(stored)
        self.assertEqual(message.download_attempts, 3)
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=-1005, message_id=14), [])
        events = self.store.list_operation_events(account_id="main", status="failed")
        self.assertEqual(events[0]["operation"], "media_download")
        self.assertEqual(events[0]["error_code"], "media_download_failed")

    async def test_pending_webpage_preview_does_not_attempt_media_download(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1006,
                enabled=True,
                download_media=True,
            )
        )
        message = FakeMessage(16, -1006, media=MessageMediaWebPage(WebPagePending()))

        stored = await self.service._store_message(message, event_type="new")

        self.assertTrue(stored)
        self.assertEqual(message.download_attempts, 0)
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=-1006, message_id=16), [])
        self.assertEqual(self.store.list_operation_events(account_id="main"), [])

    async def test_webpage_preview_with_file_media_still_downloads(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1007,
                enabled=True,
                download_media=True,
            )
        )
        message = FakeMessage(17, -1007, media=MessageMediaWebPage(WebPage(photo=object())))

        stored = await self.service._store_message(message, event_type="new")

        self.assertTrue(stored)
        self.assertEqual(message.download_attempts, 1)
        files = self.store.list_media_files(account_id="main", chat_id=-1007, message_id=17)
        self.assertEqual(len(files), 1)

    async def test_backfill_failure_records_operation_and_continues(self) -> None:
        for chat_id in (-1008, -1009):
            self.store.set_backup_policy(
                BackupPolicyRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    origin_id=chat_id,
                    enabled=True,
                )
            )
        self.service.backfill = BackfillConfig(enabled=True, initial_limit=100, catch_up_limit=100)
        self.service.client = FakeBackfillClient(
            {
                -1008: RuntimeError("private history"),
                -1009: [FakeMessage(21, -1009, text="still captured")],
            }
        )

        await self.service._backfill_capture_targets(self.service._capture_targets())

        self.assertEqual(self.store.state()["message_count"], 1)
        self.assertEqual(self.store.list_latest_messages()["items"][0]["text"], "still captured")
        events = self.store.list_operation_events(account_id="main", status="failed")
        self.assertEqual(events[0]["operation"], "backfill")
        self.assertEqual(events[0]["subject_id"], "-1008")

    async def test_backfill_retry_respects_flood_wait_and_skips_permanent_access_error(self) -> None:
        for chat_id in (-1023, -1024):
            self.store.set_backup_policy(
                BackupPolicyRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    origin_id=chat_id,
                    enabled=True,
                )
            )
        targets = self.service._capture_targets()
        self.service.client = FakeBackfillClient(
            {
                -1023: FloodWaitError(30),
                -1024: ChannelPrivateError("private channel"),
            }
        )

        failed_targets = await self.service._backfill_capture_targets(targets)

        self.assertEqual(failed_targets, [CaptureTarget(-1023, 0)])
        self.assertEqual(self.service._backfill_retry_wait_seconds, 30.0)

    async def test_topic_backfill_uses_topic_policy_target(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1012,
                topic_id=77,
                enabled=True,
            )
        )
        self.service.backfill = BackfillConfig(enabled=True, initial_limit=100, catch_up_limit=100)
        self.service.client = FakeBackfillClient(
            {-1012: [FakeMessage(22, -1012, text="topic backfill", reply_to=SimpleNamespace(reply_to_top_id=77))]}
        )

        await self.service._backfill_capture_targets(self.service._capture_targets())

        self.assertEqual(self.store.list_latest_messages()["items"][0]["text"], "topic backfill")
        self.assertEqual(self.service.client.calls[0][0], -1012)
        self.assertEqual(self.service.client.calls[0][1]["reply_to"], 77)

    async def test_live_high_watermarks_do_not_skip_offline_history_for_multiple_targets(self) -> None:
        histories = {
            -1013: [FakeMessage(message_id, -1013) for message_id in (1, 2, 3, 50)],
            -1014: [FakeMessage(message_id, -1014) for message_id in (11, 12, 13, 80)],
        }
        for chat_id in histories:
            self.store.set_backup_policy(
                BackupPolicyRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    origin_id=chat_id,
                    enabled=True,
                )
            )
        self.service.backfill = BackfillConfig(enabled=True, initial_limit=100, catch_up_limit=2)
        self.service.client = FakePagedBackfillClient(histories)

        # Event handlers are registered before startup backfill. A live event can
        # therefore advance the ingestion high-water mark for every target first.
        await self.service._store_message(histories[-1013][-1], event_type="new")
        await self.service._store_message(histories[-1014][-1], event_type="new")

        await self.service._backfill_capture_targets(self.service._capture_targets())

        stored = self.store.list_messages_after(after_event_seq=0, limit=100)["items"]
        ids_by_chat = {
            chat_id: {int(item["message_id"]) for item in stored if int(item["chat_id"]) == chat_id}
            for chat_id in histories
        }
        self.assertEqual(ids_by_chat[-1013], {1, 2, 3, 50})
        self.assertEqual(ids_by_chat[-1014], {11, 12, 13, 80})

    async def test_v18_migration_rescans_recent_window_for_legacy_cursor_gaps(self) -> None:
        chat_id = -1017
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=chat_id,
                enabled=True,
            )
        )
        self.store.upsert_capture_cursor(
            CaptureCursorRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=chat_id,
                last_message_id=2,
                observed_max_message_id=2,
                history_scanned_through_id=0,
                last_backfill_at="2026-07-18T00:00:00+00:00",
                backfill_status="migration_rescan",
            )
        )
        history = [FakeMessage(message_id, chat_id) for message_id in range(1, 11)]
        self.service.backfill = BackfillConfig(enabled=True, initial_limit=2, catch_up_limit=2)
        self.service.client = FakePagedBackfillClient({chat_id: history})

        await self.service._backfill_capture_targets(self.service._capture_targets())

        stored = self.store.list_messages_after(after_event_seq=0, limit=100)["items"]
        self.assertEqual({int(item["message_id"]) for item in stored}, set(range(1, 11)))
        cursor = self.store.get_capture_cursor(SOURCE_TELEGRAM, "main", chat_id)
        assert cursor is not None
        self.assertEqual(cursor["history_scanned_through_id"], 10)
        self.assertEqual(cursor["backfill_status"], "completed")
        self.assertEqual(cursor["backfill_error"], "")

    async def test_migration_rescan_marker_survives_failure_and_restart(self) -> None:
        chat_id = -1018
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=chat_id,
                enabled=True,
            )
        )
        self.store.upsert_capture_cursor(
            CaptureCursorRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=chat_id,
                last_message_id=50,
                observed_max_message_id=50,
                history_scanned_through_id=0,
                last_backfill_at="2026-07-18T00:00:00+00:00",
                backfill_status="migration_rescan_running",
            )
        )
        history = [FakeMessage(message_id, chat_id) for message_id in (47, 48, 49, 50)]
        client = FakePagedBackfillClient({chat_id: history})
        client.fail_next_get_messages[chat_id] = 1
        self.service.backfill = BackfillConfig(enabled=True, initial_limit=100, catch_up_limit=2)
        self.service.client = client
        targets = self.service._capture_targets()

        failed_targets = await self.service._backfill_capture_targets(targets)

        self.assertEqual(failed_targets, targets)
        cursor = self.store.get_capture_cursor(SOURCE_TELEGRAM, "main", chat_id)
        assert cursor is not None
        self.assertEqual(cursor["backfill_status"], "migration_rescan_failed")

        await self.service._backfill_capture_targets(targets)

        stored = self.store.list_messages_after(after_event_seq=0, limit=100)["items"]
        self.assertEqual({int(item["message_id"]) for item in stored}, {47, 48, 49, 50})
        cursor = self.store.get_capture_cursor(SOURCE_TELEGRAM, "main", chat_id)
        assert cursor is not None
        self.assertEqual(cursor["backfill_status"], "completed")

    async def test_migration_rescan_keeps_marker_after_completed_page(self) -> None:
        chat_id = -1021
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=chat_id,
                enabled=True,
            )
        )
        self.store.upsert_capture_cursor(
            CaptureCursorRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=chat_id,
                last_message_id=6,
                observed_max_message_id=6,
                history_scanned_through_id=0,
                last_backfill_at="2026-07-18T00:00:00+00:00",
                backfill_status="migration_rescan",
            )
        )
        client = FakePagedBackfillClient(
            {chat_id: [FakeMessage(message_id, chat_id) for message_id in range(1, 7)]}
        )
        client.fail_on_reverse_call[chat_id] = 2
        self.service.backfill = BackfillConfig(enabled=True, initial_limit=1, catch_up_limit=2)
        self.service.client = client
        targets = self.service._capture_targets()

        await self.service._backfill_capture_targets(targets)

        cursor = self.store.get_capture_cursor(SOURCE_TELEGRAM, "main", chat_id)
        assert cursor is not None
        self.assertEqual(cursor["history_scanned_through_id"], 2)
        self.assertEqual(cursor["backfill_status"], "migration_rescan_failed")

        await self.service._backfill_capture_targets(targets)

        stored = self.store.list_messages_after(after_event_seq=0, limit=100)["items"]
        self.assertEqual({int(item["message_id"]) for item in stored}, set(range(1, 7)))
        cursor = self.store.get_capture_cursor(SOURCE_TELEGRAM, "main", chat_id)
        assert cursor is not None
        self.assertEqual(cursor["backfill_status"], "completed")

    async def test_catch_up_limit_is_page_size_and_stops_at_fixed_head(self) -> None:
        chat_id = -1015
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=chat_id,
                enabled=True,
            )
        )
        client = FakePagedBackfillClient(
            {chat_id: [FakeMessage(message_id, chat_id) for message_id in (1, 2)]}
        )
        self.service.backfill = BackfillConfig(enabled=True, initial_limit=100, catch_up_limit=2)
        self.service.client = client
        targets = self.service._capture_targets()
        await self.service._backfill_capture_targets(targets)

        client.histories[chat_id].extend(FakeMessage(message_id, chat_id) for message_id in range(3, 9))
        client.calls.clear()
        client.head_calls.clear()
        client.append_after_next_reverse_page[chat_id] = FakeMessage(9, chat_id)

        await self.service._backfill_capture_targets(targets)

        stored = self.store.list_messages_after(after_event_seq=0, limit=100)["items"]
        self.assertEqual(
            {int(item["message_id"]) for item in stored if int(item["chat_id"]) == chat_id},
            set(range(1, 9)),
        )
        catch_up_pages = [
            kwargs
            for called_chat_id, kwargs in client.calls
            if called_chat_id == chat_id and kwargs.get("reverse") and kwargs.get("limit") == 2
        ]
        self.assertGreaterEqual(len(catch_up_pages), 3)
        self.assertTrue(all(int(page["limit"]) == 2 for page in catch_up_pages))

        # Message 9 appeared after the fixed catch-up head. It belongs to the next
        # pass instead of extending this pass indefinitely.
        await self.service._backfill_capture_targets(targets)
        stored = self.store.list_messages_after(after_event_seq=0, limit=100)["items"]
        self.assertEqual(
            {int(item["message_id"]) for item in stored if int(item["chat_id"]) == chat_id},
            set(range(1, 10)),
        )

    async def test_unlimited_initial_backfill_still_uses_bounded_pages(self) -> None:
        chat_id = -1022
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=chat_id,
                enabled=True,
            )
        )
        client = FakePagedBackfillClient(
            {chat_id: [FakeMessage(message_id, chat_id) for message_id in range(1, 6)]}
        )
        self.service.backfill = BackfillConfig(enabled=True, initial_limit=0, catch_up_limit=2)
        self.service.client = client

        await self.service._backfill_capture_targets(self.service._capture_targets())

        stored = self.store.list_messages_after(after_event_seq=0, limit=100)["items"]
        self.assertEqual({int(item["message_id"]) for item in stored}, set(range(1, 6)))
        pages = [kwargs for _, kwargs in client.calls if kwargs.get("reverse")]
        self.assertGreaterEqual(len(pages), 3)
        self.assertTrue(all(kwargs.get("limit") == 2 for kwargs in pages))

    async def test_failed_catch_up_can_retry_without_losing_offline_range(self) -> None:
        chat_id = -1016
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=chat_id,
                enabled=True,
            )
        )
        history = [FakeMessage(message_id, chat_id) for message_id in (1, 2)]
        client = FakePagedBackfillClient({chat_id: history})
        self.service.backfill = BackfillConfig(enabled=True, initial_limit=100, catch_up_limit=10)
        self.service.client = client
        targets = self.service._capture_targets()
        await self.service._backfill_capture_targets(targets)

        client.histories[chat_id].extend(FakeMessage(message_id, chat_id) for message_id in range(3, 11))
        await self.service._store_message(client.histories[chat_id][-1], event_type="new")
        client.fail_next_reverse_pages[chat_id] = 1

        await self.service._backfill_capture_targets(targets)
        failed = self.store.list_operation_events(account_id="main", status="failed")
        self.assertEqual(failed[0]["operation"], "backfill")
        self.assertEqual(failed[0]["subject_id"], str(chat_id))

        await self.service._backfill_capture_targets(targets)

        stored = self.store.list_messages_after(after_event_seq=0, limit=100)["items"]
        self.assertEqual(
            {int(item["message_id"]) for item in stored if int(item["chat_id"]) == chat_id},
            set(range(1, 11)),
        )
        cursor = self.store.get_capture_cursor(SOURCE_TELEGRAM, "main", chat_id)
        self.assertIsNotNone(cursor)
        assert cursor is not None
        self.assertEqual(cursor["last_message_id"], 10)
        self.assertIsNotNone(cursor["last_backfill_at"])

    async def test_concurrent_refreshes_serialize_backfill_passes(self) -> None:
        chat_id = -1020
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=chat_id,
                enabled=True,
            )
        )
        client = BlockingPagedBackfillClient(
            {chat_id: [FakeMessage(message_id, chat_id) for message_id in (1, 2, 3)]}
        )
        self.service.client = client
        targets = self.service._capture_targets()

        first = asyncio.create_task(self.service._backfill_capture_targets(targets))
        await asyncio.wait_for(client.entered.wait(), timeout=1)
        second = asyncio.create_task(self.service._backfill_capture_targets(targets))
        await asyncio.sleep(0)

        self.assertEqual(client.max_active_gets, 1)
        client.release.set()
        await asyncio.gather(first, second)
        self.assertEqual(client.max_active_gets, 1)


if __name__ == "__main__":
    unittest.main()
