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
    def __init__(self, alt: str = ""):
        self.alt = alt


class DocumentAttributeCustomEmoji:
    def __init__(self, alt: str = ""):
        self.alt = alt


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

    async def test_sticker_is_stored_as_emoji_when_download_media_is_enabled(self) -> None:
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
            text="",
            media=MessageMediaDocument(Document(attributes=[DocumentAttributeSticker("🙂")])),
        )

        stored = await self.service._store_message(message, event_type="new")

        self.assertTrue(stored)
        self.assertEqual(message.download_attempts, 0)
        files = self.store.list_media_files(account_id="main", chat_id=-1010, message_id=18)
        self.assertEqual(files, [])
        messages = self.store.list_messages_after(after_event_seq=0)["items"]
        self.assertEqual(messages[0]["text"], "🙂")
        self.assertEqual(messages[0]["has_media"], 0)
        self.assertIsNone(messages[0]["media_kind"])
        self.assertIsNone(messages[0]["grouped_id"])
        self.assertIsNone(messages[0]["raw_json"])
        self.assertEqual(self.store.list_operation_events(account_id="main"), [])

    async def test_download_stickers_downloads_file_independently_of_download_media(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1012,
                enabled=True,
                download_media=False,
                download_stickers=True,
            )
        )
        message = FakeMessage(
            24,
            -1012,
            text="",
            media=MessageMediaDocument(Document(attributes=[DocumentAttributeSticker("🙃")])),
        )

        stored = await self.service._store_message(message, event_type="new")

        self.assertTrue(stored)
        self.assertEqual(message.download_attempts, 1)
        files = self.store.list_media_files(account_id="main", chat_id=-1012, message_id=24)
        self.assertEqual(len(files), 1)
        self.assertTrue(Path(files[0]["file_path"]).exists())
        self.assertEqual(files[0]["file_size"], 5)
        messages = self.store.list_messages_after(after_event_seq=0)["items"]
        self.assertEqual(messages[0]["text"], "🙃")
        self.assertEqual(messages[0]["has_media"], 1)
        self.assertEqual(messages[0]["media_kind"], "MessageMediaDocument")
        self.assertEqual(messages[0]["grouped_id"], "99")
        self.assertIsNotNone(messages[0]["raw_json"])
        cursor = self.store.get_capture_cursor(SOURCE_TELEGRAM, "main", -1012)
        self.assertIsNotNone(cursor)
        assert cursor is not None
        self.assertEqual(cursor["last_message_id"], 24)

    async def test_custom_emoji_document_is_stored_from_attribute_alt(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1011,
                enabled=True,
                capture_text=False,
                download_media=True,
            )
        )
        message = FakeMessage(
            19,
            -1011,
            text="",
            media=MessageMediaDocument(Document(attributes=[DocumentAttributeCustomEmoji("🔥")])),
        )

        stored = await self.service._store_message(message, event_type="new")

        self.assertTrue(stored)
        self.assertEqual(message.download_attempts, 0)
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=-1011, message_id=19), [])
        messages = self.store.list_messages_after(after_event_seq=0)["items"]
        self.assertEqual(messages[0]["text"], "🔥")
        self.assertEqual(messages[0]["has_media"], 0)
        self.assertIsNone(messages[0]["media_kind"])
        self.assertIsNone(messages[0]["raw_json"])
        self.assertEqual(self.store.list_operation_events(account_id="main"), [])

    async def test_sticker_without_alt_uses_compact_fallback(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1013,
                enabled=True,
            )
        )
        message = FakeMessage(
            25,
            -1013,
            text="",
            media=MessageMediaDocument(Document(attributes=[DocumentAttributeSticker()])),
        )

        stored = await self.service._store_message(message, event_type="new")

        self.assertTrue(stored)
        messages = self.store.list_messages_after(after_event_seq=0)["items"]
        self.assertEqual(messages[0]["text"], "[sticker]")
        self.assertEqual(messages[0]["has_media"], 0)
        self.assertEqual(message.download_attempts, 0)

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

    async def test_archived_topic_and_archived_parent_are_not_ingested_or_backfilled(self) -> None:
        from tele_mess_core.models import OriginRecord

        self.store.upsert_origin(OriginRecord(SOURCE_TELEGRAM, "main", -2010, "group", title="Forum", is_forum=True))
        self.store.upsert_origin(
            OriginRecord(SOURCE_TELEGRAM, "main", -2010, "topic", topic_id=10, parent_origin_id=-2010, title="Active")
        )
        self.store.upsert_origin(
            OriginRecord(SOURCE_TELEGRAM, "main", -2010, "topic", topic_id=11, parent_origin_id=-2010, title="Archived")
        )
        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", -2010, enabled=True))
        self.store.archive_origin(SOURCE_TELEGRAM, "main", -2010, topic_id=11, archived=True)

        stored_active = await self.service._store_message(
            FakeMessage(1, -2010, text="active topic", reply_to=SimpleNamespace(reply_to_top_id=10)),
            event_type="new",
        )
        stored_archived = await self.service._store_message(
            FakeMessage(2, -2010, text="archived topic", reply_to=SimpleNamespace(reply_to_top_id=11)),
            event_type="new",
        )
        self.assertTrue(stored_active)
        self.assertFalse(stored_archived)

        # Archiving parent blocks even a child topic that has an explicit enabled=True policy.
        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", -2010, topic_id=10, enabled=True))
        self.store.archive_origin(SOURCE_TELEGRAM, "main", -2010, topic_id=0, archived=True)
        stored_under_archived_parent = await self.service._store_message(
            FakeMessage(3, -2010, text="parent archived", reply_to=SimpleNamespace(reply_to_top_id=10)),
            event_type="new",
        )
        self.assertFalse(stored_under_archived_parent)
        self.assertEqual(self.service._capture_targets(), [])

    async def test_delete_and_reaction_handlers_skip_unarchived_messages_and_update_archived_ones(self) -> None:
        from telethon.tl.types import PeerChannel, ReactionCount, ReactionEmoji, MessageReactions, UpdateMessageReactions

        handlers: list[tuple[object, object]] = []

        class HandlerClient:
            def on(self, event_builder: object):
                def decorator(fn: object):
                    handlers.append((event_builder, fn))
                    return fn
                return decorator

        self.service.register_handlers(HandlerClient())
        on_delete = handlers[2][1]
        on_raw = handlers[3][1]
        assert callable(on_delete) and callable(on_raw)

        # Unarchived messages in an enabled chat must NOT produce tombstone/skeleton rows or orphan events.
        chat_id = -1000000000555
        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True))
        await on_delete(SimpleNamespace(chat_id=chat_id, deleted_ids=[777]))
        reaction_update = UpdateMessageReactions(
            peer=PeerChannel(channel_id=555),
            msg_id=888,
            reactions=MessageReactions(results=[ReactionCount(reaction=ReactionEmoji(emoticon="👍"), count=1)]),
        )
        await on_raw(reaction_update)
        self.assertEqual(self.store.list_messages_after(after_event_seq=0)["items"], [])
        self.assertEqual(self.store.list_events(after=0)["items"], [])

        # Existing archived message IS updated by delete and reaction handlers.
        stored = await self.service._store_message(FakeMessage(888, chat_id, text="real msg"), event_type="new")
        self.assertTrue(stored)
        await on_raw(reaction_update)
        await on_delete(SimpleNamespace(chat_id=chat_id, deleted_ids=[888]))
        items = self.store.list_latest_messages()["items"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["message_id"], 888)
        self.assertIsNotNone(items[0]["deleted_at"])
        self.assertIsNotNone(items[0]["reactions_json"])

    async def test_positive_peer_user_reaction_does_not_corrupt_channel_message(self) -> None:
        from telethon.tl.types import MessageReactions, PeerUser, ReactionCount, ReactionEmoji, UpdateMessageReactions
        from tele_mess_core.models import MessageRecord, utc_now_iso

        handlers: list[tuple[object, object]] = []

        class HandlerClient:
            def on(self, event_builder: object):
                def decorator(fn: object):
                    handlers.append((event_builder, fn))
                    return fn
                return decorator

        self.service.register_handlers(HandlerClient())
        on_raw = handlers[3][1]
        assert callable(on_raw)

        # Channel -1001234567890 has archived message 88; DM PeerUser(1234567890) does not.
        now = utc_now_iso()
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-1001234567890,
                message_id=88,
                sent_at=now,
                ingested_at=now,
                text="channel message 88",
            ),
            event_type="new",
        )
        dm_reaction = UpdateMessageReactions(
            peer=PeerUser(user_id=1234567890),
            msg_id=88,
            reactions=MessageReactions(results=[ReactionCount(reaction=ReactionEmoji(emoticon="🔥"), count=1)]),
        )
        await on_raw(dm_reaction)

        items = self.store.list_latest_messages()["items"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["chat_id"], -1001234567890)
        self.assertIsNone(items[0]["reactions_json"])
        self.assertFalse(self.store._conn.in_transaction)

    async def test_inflight_store_message_aborts_on_concurrent_delete_or_archive_and_handles_chatless_delete(self) -> None:
        from tele_mess_core.models import OriginRecord

        handlers: list[tuple[object, object]] = []

        class HandlerClient:
            def on(self, event_builder: object):
                def decorator(fn: object):
                    handlers.append((event_builder, fn))
                    return fn
                return decorator

        self.service.register_handlers(HandlerClient())
        on_delete = handlers[2][1]
        assert callable(on_delete)

        self.store.upsert_origin(OriginRecord(SOURCE_TELEGRAM, "main", -2020, "group", title="Forum", is_forum=True))
        self.store.upsert_origin(
            OriginRecord(SOURCE_TELEGRAM, "main", -2020, "topic", topic_id=5, parent_origin_id=-2020, title="Topic 5")
        )
        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", -2020, enabled=True))

        # 1) Concurrent delete while _store_message is paused in await get_sender().
        entered_del = asyncio.Event()
        release_del = asyncio.Event()

        class PausingMessage(FakeMessage):
            def __init__(self, *args, entered: asyncio.Event, release: asyncio.Event, **kwargs):
                super().__init__(*args, **kwargs)
                self._entered = entered
                self._release = release

            async def get_sender(self):
                self._entered.set()
                await self._release.wait()
                return await super().get_sender()

        msg1 = PausingMessage(
            101,
            -2020,
            text="deleted while in flight",
            reply_to=SimpleNamespace(reply_to_top_id=5),
            entered=entered_del,
            release=release_del,
        )
        task1 = asyncio.create_task(self.service._store_message(msg1, event_type="backfill"))
        await asyncio.wait_for(entered_del.wait(), timeout=1.0)
        await on_delete(SimpleNamespace(chat_id=-2020, deleted_ids=[101]))
        release_del.set()
        self.assertFalse(await task1)
        self.assertEqual(self.store.list_latest_messages()["items"], [])

        # 2) Concurrent topic archive while _store_message is paused in await get_sender().
        entered_arch = asyncio.Event()
        release_arch = asyncio.Event()
        msg2 = PausingMessage(
            102,
            -2020,
            text="archived while in flight",
            reply_to=SimpleNamespace(reply_to_top_id=5),
            entered=entered_arch,
            release=release_arch,
        )
        task2 = asyncio.create_task(self.service._store_message(msg2, event_type="backfill"))
        await asyncio.wait_for(entered_arch.wait(), timeout=1.0)
        self.store.archive_origin(SOURCE_TELEGRAM, "main", -2020, topic_id=5, archived=True)
        release_arch.set()
        self.assertFalse(await task2)
        self.assertEqual(self.store.list_latest_messages()["items"], [])

        # 3) Chatless delete (UpdateDeleteMessages with chat_id=None) marks DM & basic group messages.
        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", 9001, enabled=True))
        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", -500, enabled=True))
        self.assertTrue(await self.service._store_message(FakeMessage(10, 9001, text="dm"), event_type="new"))
        self.assertTrue(await self.service._store_message(FakeMessage(11, -500, text="group"), event_type="new"))
        await on_delete(SimpleNamespace(chat_id=None, deleted_ids=[10, 11, 999]))
        by_id = {item["message_id"]: item for item in self.store.list_latest_messages()["items"]}
        self.assertEqual(set(by_id), {10, 11})
        self.assertIsNotNone(by_id[10]["deleted_at"])
        self.assertIsNotNone(by_id[11]["deleted_at"])

    async def test_backfill_page_delete_cancels_queued_message_before_its_turn(self) -> None:
        handlers: list[tuple[object, object]] = []

        class BackfillHandlerClient:
            def __init__(self, messages: list[FakeMessage]) -> None:
                self._messages = messages

            def on(self, event_builder: object):
                def decorator(fn: object):
                    handlers.append((event_builder, fn))
                    return fn
                return decorator

            async def get_messages(self, chat_id: int, **kwargs):
                if kwargs.get("limit") == 1:
                    return [self._messages[-1]]
                return list(self._messages)

        entered_60 = asyncio.Event()
        release_60 = asyncio.Event()

        class PausingMessage(FakeMessage):
            async def get_sender(self):
                entered_60.set()
                await release_60.wait()
                return await super().get_sender()

        channel_id = -1001234567890
        msg60 = PausingMessage(60, channel_id, text="keep 60")
        msg61 = FakeMessage(61, channel_id, text="delete 61 while 60 paused")
        client = BackfillHandlerClient([msg60, msg61])
        self.service.register_handlers(client)
        on_delete = handlers[2][1]
        assert callable(on_delete)
        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", channel_id, enabled=True))

        task = asyncio.create_task(self.service.refresh_capture_targets())
        await asyncio.wait_for(entered_60.wait(), timeout=1.0)
        await on_delete(SimpleNamespace(chat_id=channel_id, deleted_ids=[61]))
        release_60.set()
        await asyncio.wait_for(task, timeout=2.0)

        items = self.store.list_latest_messages()["items"]
        self.assertEqual([item["message_id"] for item in items], [60])
        self.assertIsNone(items[0]["deleted_at"])

    async def test_chatless_delete_merges_db_and_inflight_candidates_before_uniqueness_check(self) -> None:
        handlers: list[tuple[object, object]] = []

        class HandlerClient:
            def on(self, event_builder: object):
                def decorator(fn: object):
                    handlers.append((event_builder, fn))
                    return fn
                return decorator

        self.service.register_handlers(HandlerClient())
        on_delete = handlers[2][1]
        assert callable(on_delete)

        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", 9001, enabled=True))
        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", -501, enabled=True))
        self.assertTrue(await self.service._store_message(FakeMessage(71, 9001, text="dm 71"), event_type="new"))

        entered = asyncio.Event()
        release = asyncio.Event()

        class PausingMessage(FakeMessage):
            async def get_sender(self):
                entered.set()
                await release.wait()
                return await super().get_sender()

        group_msg = PausingMessage(71, -501, text="group 71")
        task = asyncio.create_task(self.service._store_message(group_msg, event_type="new"))
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        await on_delete(SimpleNamespace(chat_id=None, deleted_ids=[71]))
        release.set()
        self.assertTrue(await task)

        by_chat = {item["chat_id"]: item for item in self.store.list_latest_messages()["items"]}
        self.assertEqual(set(by_chat), {9001, -501})
        self.assertIsNone(by_chat[9001]["deleted_at"])
        self.assertIsNone(by_chat[-501]["deleted_at"])
        self.assertFalse(self.store._conn.in_transaction)

    async def test_inflight_policy_change_rebuilds_record_and_skips_media_download(self) -> None:
        self.store.set_backup_policy(
            BackupPolicyRecord(
                SOURCE_TELEGRAM,
                "main",
                -1099,
                enabled=True,
                capture_text=True,
                capture_media_metadata=True,
                download_media=True,
            )
        )
        entered = asyncio.Event()
        release = asyncio.Event()
        download_calls: list[str] = []

        class PausingMediaMessage(FakeMessage):
            async def get_sender(self):
                entered.set()
                await release.wait()
                return await super().get_sender()

            async def download_media(self, file: str) -> str:
                download_calls.append(file)
                path = Path(file) / "should-not-download.jpg"
                path.write_bytes(b"img")
                return str(path)

        msg = PausingMediaMessage(
            81,
            -1099,
            text="secret body",
            media=MessageMediaDocument(),
        )
        task = asyncio.create_task(self.service._store_message(msg, event_type="new"))
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        self.store.set_backup_policy(
            BackupPolicyRecord(
                SOURCE_TELEGRAM,
                "main",
                -1099,
                enabled=True,
                capture_text=False,
                capture_media_metadata=False,
                download_media=False,
            )
        )
        release.set()
        self.assertTrue(await task)

        items = self.store.list_latest_messages()["items"]
        self.assertEqual(len(items), 1)
        self.assertIsNone(items[0]["text"])
        self.assertFalse(items[0]["has_media"])
        self.assertIsNone(items[0]["media_kind"])
        self.assertIsNone(items[0]["raw_json"])
        self.assertEqual(download_calls, [])
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=-1099), [])

    async def test_backfill_iterator_registers_messages_as_yielded_before_next_batch(self) -> None:
        handlers: list[tuple[object, object]] = []
        yielded_first = asyncio.Event()
        release_second = asyncio.Event()
        channel_id = -1001234567891

        class PausingBatchIterator:
            def __init__(self, first: FakeMessage, second: FakeMessage) -> None:
                self._items = [first, second]
                self._index = 0

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self._index == 0:
                    self._index += 1
                    return self._items[0]
                if self._index == 1:
                    self._index += 1
                    yielded_first.set()
                    await release_second.wait()
                    return self._items[1]
                raise StopAsyncIteration

        class PagedIterClient:
            def on(self, event_builder: object):
                def decorator(fn: object):
                    handlers.append((event_builder, fn))
                    return fn
                return decorator

            async def get_messages(self, chat_id: int, **kwargs):
                return [FakeMessage(92, channel_id, text="keep 92")]

            def iter_messages(self, chat_id: int, **kwargs):
                return PausingBatchIterator(
                    FakeMessage(91, channel_id, text="delete 91 between batches"),
                    FakeMessage(92, channel_id, text="keep 92"),
                )

        self.service.register_handlers(PagedIterClient())
        on_delete = handlers[2][1]
        assert callable(on_delete)
        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", channel_id, enabled=True))

        task = asyncio.create_task(self.service.refresh_capture_targets())
        await asyncio.wait_for(yielded_first.wait(), timeout=1.0)
        await on_delete(SimpleNamespace(chat_id=channel_id, deleted_ids=[91]))
        release_second.set()
        await asyncio.wait_for(task, timeout=2.0)

        items = self.store.list_latest_messages()["items"]
        self.assertEqual([item["message_id"] for item in items], [92])
        self.assertEqual(self.service._inflight_fetches, {})

    async def test_duplicate_inflight_cleanup_uses_identity_and_preserves_other_task(self) -> None:
        handlers: list[tuple[object, object]] = []

        class HandlerClient:
            def on(self, event_builder: object):
                def decorator(fn: object):
                    handlers.append((event_builder, fn))
                    return fn
                return decorator

        self.service.register_handlers(HandlerClient())
        on_delete = handlers[2][1]
        assert callable(on_delete)

        channel_id = -1001234567892
        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", channel_id, enabled=True))

        entered = asyncio.Event()
        release = asyncio.Event()

        class PausingRealtimeMessage(FakeMessage):
            async def get_sender(self):
                entered.set()
                await release.wait()
                return await super().get_sender()

        live_msg = PausingRealtimeMessage(95, channel_id, text="live 95")
        live_task = asyncio.create_task(self.service._store_message(live_msg, event_type="new"))
        await asyncio.wait_for(entered.wait(), timeout=1.0)

        # Duplicate backfill skips while policy is temporarily disabled and cleans up its own state twice.
        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", channel_id, enabled=False))
        dup_msg = FakeMessage(95, channel_id, text="dup 95")
        dup_state = self.service._register_inflight_fetch(channel_id, 95)
        await self.service._store_backfill_page([(dup_msg, dup_state)], type("FakeService", (), {}))

        # Re-enable policy and deliver delete for 95; live task must still be fenced.
        self.store.set_backup_policy(BackupPolicyRecord(SOURCE_TELEGRAM, "main", channel_id, enabled=True))
        await on_delete(SimpleNamespace(chat_id=channel_id, deleted_ids=[95]))
        release.set()
        self.assertFalse(await live_task)
        self.assertEqual(self.store.list_latest_messages()["items"], [])
        self.assertEqual(self.service._inflight_fetches, {})

    async def test_media_download_retry_stops_when_policy_disabled_mid_attempt(self) -> None:
        chat_id = -1098
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        attempts: list[int] = []
        service = TelegramArchiveService(
            TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=Path(self.tmp.name),
            ),
            self.store,
            media_download=MediaDownloadConfig(retries=2, retry_delay_seconds=0),
        )

        class DisableOnFirstFailureMessage(FakeMessage):
            async def download_media(self, file: str) -> str:
                attempts.append(len(attempts) + 1)
                if len(attempts) == 1:
                    self.store_ref.set_backup_policy(
                        BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=False)
                    )
                    raise RuntimeError("transient media failure")
                path = Path(file) / "should-not-retry.bin"
                path.write_bytes(b"data")
                return str(path)

        msg = DisableOnFirstFailureMessage(96, chat_id, text="with media", media=MessageMediaDocument())
        msg.store_ref = self.store
        self.assertTrue(await service._store_message(msg, event_type="new"))
        self.assertEqual(attempts, [1])
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=96), [])


if __name__ == "__main__":
    unittest.main()
