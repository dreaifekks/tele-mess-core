from __future__ import annotations

import asyncio
import errno
import os
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


class FakeMediaClient:
    """Minimal client fake supporting message refresh for the decoupled media download worker."""

    def __init__(self, *messages: FakeMessage) -> None:
        self.messages: dict[tuple[int, int], FakeMessage] = {
            (int(msg.chat_id), int(msg.id)): msg for msg in messages
        }
        self.refresh_calls: list[tuple[int, int]] = []

    def add(self, message: FakeMessage) -> None:
        self.messages[(int(message.chat_id), int(message.id))] = message

    async def get_messages(self, chat_id: int, ids: int | None = None, **kwargs):
        if ids is not None:
            self.refresh_calls.append((int(chat_id), int(ids)))
            return self.messages.get((int(chat_id), int(ids)))
        return [msg for (cid, _mid), msg in self.messages.items() if cid == int(chat_id)]


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
        message = FakeMessage(12, -1003, media=MessageMediaDocument())
        client = FakeMediaClient(message)
        self.service.client = client

        stored = await self.service._store_message(message, event_type="new")
        self.assertTrue(stored)
        # Ingest only enqueues; download runs asynchronously in the media worker.
        self.assertEqual(message.download_attempts, 0)
        self.assertEqual(self.store.state()["media_download_pending"], 1)

        await self.service.process_pending_media_downloads()
        self.assertEqual(client.refresh_calls, [(-1003, 12)])
        files = self.store.list_media_files(account_id="main", chat_id=-1003, message_id=12)
        self.assertEqual(len(files), 1)
        self.assertTrue(Path(files[0]["file_path"]).exists())
        self.assertEqual(files[0]["file_size"], 5)
        self.assertEqual(self.store.state()["media_download_pending"], 0)

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
        self.service.client = FakeMediaClient(message)

        await self.service._store_message(message, event_type="new")
        await self.service.process_pending_media_downloads()
        await self.service._store_message(message, event_type="backfill")
        await self.service.process_pending_media_downloads()

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
        self.service.client = FakeMediaClient(message)

        stored = await self.service._store_message(message, event_type="new")
        await self.service.process_pending_media_downloads()

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
        self.service.client = FakeMediaClient(message)

        stored = await self.service._store_message(message, event_type="new")
        await self.service.process_pending_media_downloads()

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
        self.service.client = FakeMediaClient(message)

        stored = await self.service._store_message(message, event_type="new")
        await self.service.process_pending_media_downloads()

        self.assertTrue(stored)
        self.assertEqual(message.download_attempts, 3)
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=-1005, message_id=14), [])
        events = self.store.list_operation_events(account_id="main", status="failed")
        self.assertEqual(events[0]["operation"], "media_download")
        self.assertEqual(events[0]["error_code"], "media_download_failed")
        self.assertEqual(self.store.state()["media_download_failed"], 1)

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
        self.service.client = FakeMediaClient(message)

        stored = await self.service._store_message(message, event_type="new")
        await self.service.process_pending_media_downloads()

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
        service.client = FakeMediaClient(msg)
        self.assertTrue(await service._store_message(msg, event_type="new"))
        await service.process_pending_media_downloads()
        self.assertEqual(attempts, [1])
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=96), [])
        jobs = self.store.list_media_download_jobs(account_id="main")
        self.assertEqual(jobs[0]["status"], "canceled")

    async def test_failed_and_policy_rejected_media_downloads_leave_no_partial_or_published_files(self) -> None:
        chat_id = -1097
        target_dir = self.store.database_path.parent / "media" / "main" / str(chat_id)
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
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
        client = FakeMediaClient()
        service.client = client

        # 1) Partial write before failure leaves zero files or staging directories behind.
        class PartialFailMessage(FakeMessage):
            async def download_media(self, file: str) -> str:
                partial = Path(file) / "partial.bin"
                partial.write_bytes(b"half-written")
                self.store_ref.set_backup_policy(
                    BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=False)
                )
                raise RuntimeError("interrupted mid-transfer")

        msg1 = PartialFailMessage(97, chat_id, text="partial fail", media=MessageMediaDocument())
        msg1.store_ref = self.store
        client.add(msg1)
        self.assertTrue(await service._store_message(msg1, event_type="new"))
        await service.process_pending_media_downloads()
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=97), [])
        self.assertEqual(list(target_dir.iterdir()), [])

        # 2) Policy disabled concurrently right before atomic publish leaves zero files and zero media rows.
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )

        class DisableBeforePublishMessage(FakeMessage):
            async def download_media(self, file: str) -> str:
                staged = Path(file) / "finished.bin"
                staged.write_bytes(b"complete-bytes")
                self.store_ref.set_backup_policy(
                    BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=False)
                )
                return str(staged)

        msg2 = DisableBeforePublishMessage(98, chat_id, text="disabled before publish", media=MessageMediaDocument())
        msg2.store_ref = self.store
        client.add(msg2)
        self.assertTrue(await service._store_message(msg2, event_type="new"))
        await service.process_pending_media_downloads()
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=98), [])
        self.assertEqual(list(target_dir.iterdir()), [])
        self.assertFalse(self.store._conn.in_transaction)

        # 3) Fault during stat() before atomic replace leaves zero files in target_dir and zero DB rows.
        from unittest import mock

        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        orig_stat = Path.stat

        def failing_stat(path_self: Path, *args, **kwargs):
            if path_self.name == "stat-fail.bin":
                raise OSError(5, "Input/output error")
            return orig_stat(path_self, *args, **kwargs)

        class StatFailMessage(FakeMessage):
            async def download_media(self, file: str) -> str:
                staged = Path(file) / "stat-fail.bin"
                staged.write_bytes(b"complete-bytes")
                return str(staged)

        msg3 = StatFailMessage(99, chat_id, text="stat fail", media=MessageMediaDocument())
        client.add(msg3)
        self.assertTrue(await service._store_message(msg3, event_type="new"))
        with mock.patch.object(Path, "stat", failing_stat):
            await service.process_pending_media_downloads()
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=99), [])
        self.assertEqual(list(target_dir.iterdir()), [])
        self.assertFalse(self.store._conn.in_transaction)

        # 4) Fault during DB insert after atomic replace rolls back both DB row and published file.
        class DbFailMessage(FakeMessage):
            async def download_media(self, file: str) -> str:
                staged = Path(file) / "db-fail.bin"
                staged.write_bytes(b"complete-bytes")
                return str(staged)

        msg4 = DbFailMessage(100, chat_id, text="db fail", media=MessageMediaDocument())
        client.add(msg4)
        self.assertTrue(await service._store_message(msg4, event_type="new"))
        with mock.patch.object(
            self.store,
            "_upsert_media_file_row_unlocked",
            side_effect=RuntimeError("simulated sqlite insert error"),
        ):
            await service.process_pending_media_downloads()
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=100), [])
        self.assertEqual(list(target_dir.iterdir()), [])
        self.assertFalse(self.store._conn.in_transaction)

    async def test_backfill_cursor_advances_immediately_and_worker_recovers_via_message_refresh(self) -> None:
        chat_id = -1096
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        stale_msg = FakeMessage(
            110,
            chat_id,
            text="photo with expiring ref",
            media=MessageMediaDocument(),
            fail_download_times=99,
        )
        fresh_msg = FakeMessage(110, chat_id, text="photo with fresh ref", media=MessageMediaDocument())

        class BackfillAndRefreshClient:
            def __init__(self) -> None:
                self.refresh_calls: list[tuple[int, int]] = []
                self.serve_fresh = False

            async def get_messages(self, cid: int, ids: int | None = None, **kwargs):
                if ids is not None:
                    self.refresh_calls.append((int(cid), int(ids)))
                    return fresh_msg if self.serve_fresh else stale_msg
                return [stale_msg]

            def iter_messages(self, cid: int, **kwargs):
                return FakeBackfillIterator(messages=[stale_msg])

        client = BackfillAndRefreshClient()
        self.service.client = client
        await self.service.refresh_capture_targets()

        # Backfill cursor completed immediately without blocking on media download.
        cursor = self.store.get_capture_cursor(SOURCE_TELEGRAM, "main", chat_id)
        self.assertIsNotNone(cursor)
        assert cursor is not None
        self.assertEqual(cursor["backfill_status"], "completed")
        self.assertEqual(cursor["history_scanned_through_id"], 110)
        self.assertEqual(self.store.state()["media_download_pending"], 1)

        # First worker attempt sees failing stale_msg, schedules retry.
        self.assertEqual(await self.service.process_pending_media_downloads(max_jobs=1), 1)
        self.assertEqual(self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=110), [])
        jobs = self.store.list_media_download_jobs(account_id="main")
        self.assertEqual(jobs[0]["status"], "retry")
        self.assertEqual(jobs[0]["attempts"], 1)

        # Next attempt refreshes via client.get_messages(chat_id, ids=110) and gets fresh_msg.
        client.serve_fresh = True
        self.assertEqual(await self.service.process_pending_media_downloads(max_jobs=1), 1)
        self.assertEqual(client.refresh_calls, [(chat_id, 110), (chat_id, 110)])
        files = self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=110)
        self.assertEqual(len(files), 1)
        self.assertTrue(Path(files[0]["file_path"]).exists())
        self.assertEqual(self.store.state()["media_download_pending"], 0)

    async def test_media_download_flood_wait_preserves_attempt_budget_and_permanent_error_fails_immediately(self) -> None:
        class FloodWaitError(RuntimeError):
            def __init__(self, seconds: int) -> None:
                super().__init__(f"A wait of {seconds} seconds is required")
                self.seconds = seconds

        class ChannelPrivateError(RuntimeError):
            pass

        chat_id = -1095
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )

        class FloodWaitMessage(FakeMessage):
            async def download_media(self, file: str) -> str:
                self.download_attempts += 1
                raise FloodWaitError(25)

        class PrivateErrorMessage(FakeMessage):
            async def download_media(self, file: str) -> str:
                self.download_attempts += 1
                raise ChannelPrivateError("private channel")

        flood_msg = FloodWaitMessage(111, chat_id, media=MessageMediaDocument())
        priv_msg = PrivateErrorMessage(112, chat_id, media=MessageMediaDocument())
        self.service.client = FakeMediaClient(flood_msg, priv_msg)

        self.assertTrue(await self.service._store_message(flood_msg, event_type="new"))
        self.assertTrue(await self.service._store_message(priv_msg, event_type="new"))
        await self.service.process_pending_media_downloads()

        jobs_by_msg = {
            job["message_id"]: job
            for job in self.store.list_media_download_jobs(account_id="main")
        }
        self.assertEqual(jobs_by_msg[111]["status"], "retry")
        self.assertEqual(jobs_by_msg[111]["attempts"], 0)
        self.assertEqual(jobs_by_msg[111]["error_code"], "flood_wait")
        self.assertIsNotNone(jobs_by_msg[111]["next_attempt_at"])
        self.assertEqual(flood_msg.download_attempts, 1)

        self.assertEqual(jobs_by_msg[112]["status"], "failed")
        self.assertEqual(jobs_by_msg[112]["attempts"], 1)
        self.assertEqual(jobs_by_msg[112]["error_code"], "access_denied")
        self.assertEqual(priv_msg.download_attempts, 1)
        state = self.store.state()
        self.assertEqual(state["media_download_pending"], 1)
        self.assertEqual(state["media_download_failed"], 1)

    async def test_media_worker_graceful_shutdown_releases_job_without_spending_attempt_or_leaving_part_files(self) -> None:
        chat_id = -1094
        target_dir = self.store.database_path.parent / "media" / "main" / str(chat_id)
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        started = asyncio.Event()
        release_blocker = asyncio.Event()
        allow_complete = False

        class BlockingMediaMessage(FakeMessage):
            async def download_media(self, file: str) -> str:
                self.download_attempts += 1
                part = Path(file) / "video.mp4"
                part.write_bytes(b"partial-chunk")
                if not allow_complete:
                    started.set()
                    await release_blocker.wait()
                return str(part)

        msg = BlockingMediaMessage(113, chat_id, media=MessageMediaDocument())

        class ActivatableClient(FakeMediaClient):
            def on(self, event_builder: object):
                return lambda fn: fn

            async def is_user_authorized(self) -> bool:
                return True

            def is_connected(self) -> bool:
                return True

            def iter_messages(self, cid: int, **kwargs):
                return FakeBackfillIterator(messages=[])

            async def get_messages(self, cid: int, ids: int | None = None, **kwargs):
                if ids is not None:
                    return await super().get_messages(cid, ids=ids, **kwargs)
                return []

        client = ActivatableClient(msg)
        await self.service.activate(client)
        self.assertTrue(await self.service._store_message(msg, event_type="new"))
        await asyncio.wait_for(started.wait(), timeout=2.0)

        # Graceful shutdown cancels worker, removes .part-* dir, and releases job without spending attempt.
        await self.service.stop()
        self.assertEqual(list(target_dir.iterdir()), [])
        jobs = self.store.list_media_download_jobs(account_id="main")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["status"], "retry")
        self.assertEqual(jobs[0]["attempts"], 0)
        self.assertEqual(jobs[0]["claim_token"], 1)
        self.assertEqual(jobs[0]["error_code"], "worker_stopped")

        # Subsequent claim uses monotonic claim_token=2 and completes cleanly.
        allow_complete = True
        self.assertEqual(await self.service.process_pending_media_downloads(), 1)
        finished = self.store.get_media_download_job(jobs[0]["job_id"])
        assert finished is not None
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(finished["attempts"], 1)
        self.assertEqual(finished["claim_token"], 2)
        self.assertEqual(len(self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=113)), 1)

    async def test_media_download_cancels_when_message_deleted_or_topic_archived_while_queued(self) -> None:
        from tele_mess_core.models import OriginRecord

        forum_id = -1093
        self.store.upsert_origin(OriginRecord(SOURCE_TELEGRAM, "main", forum_id, "group", title="Forum", is_forum=True))
        self.store.upsert_origin(
            OriginRecord(SOURCE_TELEGRAM, "main", forum_id, "topic", topic_id=7, parent_origin_id=forum_id, title="T7")
        )
        self.store.upsert_origin(
            OriginRecord(SOURCE_TELEGRAM, "main", forum_id, "topic", topic_id=8, parent_origin_id=forum_id, title="T8")
        )
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", forum_id, enabled=True, download_media=True)
        )
        msg_del = FakeMessage(
            120,
            forum_id,
            media=MessageMediaDocument(),
            reply_to=SimpleNamespace(reply_to_top_id=7),
        )
        msg_arch = FakeMessage(
            121,
            forum_id,
            media=MessageMediaDocument(),
            reply_to=SimpleNamespace(reply_to_top_id=8),
        )
        client = FakeMediaClient(msg_del, msg_arch)
        self.service.client = client

        self.assertTrue(await self.service._store_message(msg_del, event_type="new"))
        self.assertTrue(await self.service._store_message(msg_arch, event_type="new"))
        self.assertEqual(self.store.state()["media_download_pending"], 2)

        # Deleting msg 120 immediately cancels its pending job in SQLite.
        self.store.mark_deleted(SOURCE_TELEGRAM, forum_id, [120], account_id="main")
        # Archiving topic 8 causes worker to cancel msg 121 without calling Telegram.
        self.store.archive_origin(SOURCE_TELEGRAM, "main", forum_id, topic_id=8, archived=True)
        await self.service.process_pending_media_downloads()

        jobs_by_msg = {
            job["message_id"]: job
            for job in self.store.list_media_download_jobs(account_id="main")
        }
        self.assertEqual(jobs_by_msg[120]["status"], "canceled")
        self.assertEqual(jobs_by_msg[120]["error_code"], "message_deleted")
        self.assertEqual(jobs_by_msg[121]["status"], "canceled")
        self.assertEqual(jobs_by_msg[121]["error_code"], "policy_disabled")
        self.assertEqual(client.refresh_calls, [])
        self.assertEqual(self.store.state()["media_download_pending"], 0)

    async def test_media_worker_survives_sqlite_lock_on_retry_write_and_recovers_after_unlock(self) -> None:
        import sqlite3
        from unittest import mock

        chat_id = -1092
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        service = TelegramArchiveService(
            TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=Path(self.tmp.name),
            ),
            self.store,
            media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0, stale_seconds=0.15),
        )
        msg1 = FakeMessage(201, chat_id, media=MessageMediaDocument(), fail_download_times=1)
        msg2 = FakeMessage(202, chat_id, media=MessageMediaDocument())

        class LiveClient(FakeMediaClient):
            def on(self, event_builder: object):
                return lambda fn: fn

            async def is_user_authorized(self) -> bool:
                return True

            def is_connected(self) -> bool:
                return True

            def iter_messages(self, cid: int, **kwargs):
                return FakeBackfillIterator(messages=[])

            async def get_messages(self, cid: int, ids: int | None = None, **kwargs):
                if ids is not None:
                    return await super().get_messages(cid, ids=ids, **kwargs)
                return []

        client = LiveClient(msg1, msg2)
        orig_retry = self.store.retry_media_download_job
        lock_once = {"raised": False}

        def flaky_retry(*args, **kwargs):
            if not lock_once["raised"]:
                lock_once["raised"] = True
                raise sqlite3.OperationalError("database is locked")
            return orig_retry(*args, **kwargs)

        try:
            with mock.patch.object(self.store, "retry_media_download_job", side_effect=flaky_retry):
                await service.activate(client)
                self.assertTrue(await service._store_message(msg1, event_type="new"))
                # Wait until the simulated SQLite lock error fires inside the worker.
                deadline = asyncio.get_running_loop().time() + 3.0
                while not lock_once["raised"] and asyncio.get_running_loop().time() < deadline:
                    await asyncio.sleep(0.02)
                self.assertTrue(lock_once["raised"])
                # Worker task must still be alive; enqueue msg2 and wake worker.
                self.assertIsNotNone(service._media_worker_task)
                assert service._media_worker_task is not None
                self.assertFalse(service._media_worker_task.done())
                self.assertTrue(await service._store_message(msg2, event_type="new"))

                # Wait for both msg2 (new) and msg1 (stale lease reclaim after 0.15s) to complete.
                deadline = asyncio.get_running_loop().time() + 4.0
                while asyncio.get_running_loop().time() < deadline:
                    files = self.store.list_media_files(account_id="main", chat_id=chat_id)
                    if {item["message_id"] for item in files} == {201, 202}:
                        break
                    await asyncio.sleep(0.05)
                files = self.store.list_media_files(account_id="main", chat_id=chat_id)
                self.assertEqual({item["message_id"] for item in files}, {201, 202})
        finally:
            await service.stop()

    async def test_edited_message_replaces_existing_media_and_removes_superseded_file(self) -> None:
        chat_id = -1091
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )

        class VersionedMediaMessage(FakeMessage):
            def __init__(self, *args, filename: str, payload: bytes, **kwargs):
                super().__init__(*args, **kwargs)
                self._filename = filename
                self._payload = payload

            async def download_media(self, file: str) -> str:
                self.download_attempts += 1
                path = Path(file) / self._filename
                path.write_bytes(self._payload)
                return str(path)

        v1 = VersionedMediaMessage(
            210,
            chat_id,
            text="original",
            media=MessageMediaDocument(),
            filename="photo.bin",
            payload=b"old-v1",
        )
        client = FakeMediaClient(v1)
        self.service.client = client

        self.assertTrue(await self.service._store_message(v1, event_type="new"))
        await self.service.process_pending_media_downloads()
        files_v1 = self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=210)
        self.assertEqual(len(files_v1), 1)
        old_path = Path(files_v1[0]["file_path"])
        self.assertEqual(old_path.read_bytes(), b"old-v1")

        # Edit message with a replacement attachment and newer edit_date.
        v2 = VersionedMediaMessage(
            210,
            chat_id,
            text="edited caption",
            media=MessageMediaDocument(),
            filename="photo.bin",
            payload=b"replaced-v2-bytes",
        )
        v2.edit_date = datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc)
        client.add(v2)
        self.assertTrue(await self.service._store_message(v2, event_type="edit"))
        await self.service.process_pending_media_downloads()

        self.assertEqual(v2.download_attempts, 1)
        files_v2 = self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=210)
        self.assertEqual(len(files_v2), 1)
        new_path = Path(files_v2[0]["file_path"])
        self.assertEqual(new_path.read_bytes(), b"replaced-v2-bytes")
        self.assertEqual(files_v2[0]["file_size"], len(b"replaced-v2-bytes"))
        if new_path != old_path:
            self.assertFalse(old_path.exists())

    async def test_crash_recovery_cleans_up_leftover_part_dir_and_uncommitted_published_file(self) -> None:
        chat_id = -1090
        target_dir = self.store.database_path.parent / "media" / "main" / str(chat_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        msg = FakeMessage(220, chat_id, media=MessageMediaDocument())
        self.service.client = FakeMediaClient(msg)

        self.assertTrue(await self.service._store_message(msg, event_type="new"))
        claimed = self.store.claim_media_download_job("main")
        assert claimed is not None
        job_id = claimed["job_id"]

        # Simulate crash leaving both a job-scoped staging dir AND an uncommitted os.replace target file.
        stale_part_dir = target_dir / f".part-{job_id}-t1"
        stale_part_dir.mkdir(parents=True, exist_ok=True)
        (stale_part_dir / "half.bin").write_bytes(b"half")
        orphan_dir = target_dir / f"{job_id}-t1"
        orphan_dir.mkdir(parents=True, exist_ok=True)
        orphan_published = orphan_dir / f"{msg.id}.bin"
        orphan_published.write_bytes(b"uncommitted-crash-file")
        self.assertEqual(
            self.store.record_media_download_pending_path(
                job_id,
                expected_claim_token=1,
                pending_file_path=str(orphan_published),
            ),
            "ok",
        )
        # Backdate lease to simulate restart after crash.
        self.store._conn.execute(
            "UPDATE media_download_jobs SET updated_at = '2020-01-01T00:00:00+00:00' WHERE job_id = ?",
            (job_id,),
        )
        self.store._conn.commit()

        await self.service.process_pending_media_downloads()

        self.assertFalse(stale_part_dir.exists())
        self.assertFalse(orphan_published.exists())
        self.assertFalse(orphan_dir.exists())
        files = self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=220)
        self.assertEqual(len(files), 1)
        published_path = Path(files[0]["file_path"])
        final_job = self.store.get_media_download_job(job_id)
        assert final_job is not None
        self.assertGreater(final_job["claim_token"], 1)
        self.assertEqual(published_path, target_dir / f"{job_id}-t{final_job['claim_token']}" / f"{msg.id}.bin")
        self.assertEqual(published_path.read_bytes(), b"media")
        # Exactly one exclusive publish dir remains on disk — zero leftover .part dirs or uncommitted crash files.
        self.assertEqual(list(target_dir.iterdir()), [published_path.parent])

    async def test_media_worker_heartbeats_during_slow_get_messages_preventing_stale_expiry(self) -> None:
        chat_id = -1089
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        service = TelegramArchiveService(
            TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=Path(self.tmp.name),
            ),
            self.store,
            media_download=MediaDownloadConfig(max_attempts=1, retry_delay_seconds=0, stale_seconds=0.3),
        )
        msg = FakeMessage(230, chat_id, media=MessageMediaDocument())
        entered_rpc = asyncio.Event()

        class SlowRefreshClient(FakeMediaClient):
            async def get_messages(self, cid: int, ids: int | None = None, **kwargs):
                if ids is not None:
                    entered_rpc.set()
                    await asyncio.sleep(0.55)
                return await super().get_messages(cid, ids=ids, **kwargs)

        service.client = SlowRefreshClient(msg)
        self.assertTrue(await service._store_message(msg, event_type="new"))

        second_store = ArchiveStore(self.store.database_path)
        second_store.initialize()
        try:
            drain_task = asyncio.create_task(service.process_pending_media_downloads())
            await asyncio.wait_for(entered_rpc.wait(), timeout=1.0)
            # Wait longer than stale_seconds while get_messages() is still running.
            await asyncio.sleep(0.42)
            # Second connection must NOT consider the active claim stale or transition it to failed.
            self.assertIsNone(second_store.claim_media_download_job("main", stale_seconds=0.3))
            jobs_mid = second_store.list_media_download_jobs(account_id="main")
            self.assertEqual(jobs_mid[0]["status"], "downloading")

            await asyncio.wait_for(drain_task, timeout=2.0)
            files = self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=230)
            self.assertEqual(len(files), 1)
            self.assertEqual(self.store.state()["media_download_failed"], 0)
        finally:
            second_store.close()

    async def test_media_completion_emits_incremental_sync_event_for_after_cursor(self) -> None:
        chat_id = -1088
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        msg = FakeMessage(240, chat_id, text="async photo", media=MessageMediaDocument())
        self.service.client = FakeMediaClient(msg)

        self.assertTrue(await self.service._store_message(msg, event_type="new"))
        first_page = self.store.list_messages_after(after_event_seq=0)
        self.assertEqual(len(first_page["items"]), 1)
        cursor_before_download = first_page["next_cursor"]
        self.assertEqual(self.store.state()["last_event_seq"], cursor_before_download)

        await self.service.process_pending_media_downloads()

        self.assertGreater(self.store.state()["last_event_seq"], cursor_before_download)
        next_page = self.store.list_messages_after(after_event_seq=cursor_before_download)
        self.assertEqual(len(next_page["items"]), 1)
        self.assertEqual(next_page["items"][0]["message_id"], 240)
        events = self.store.list_events(after=cursor_before_download)["items"]
        self.assertEqual(events[0]["event_type"], "media_ready")
        media_map = self.store.list_media_files_for_messages(next_page["items"])
        self.assertEqual(len(media_map[(SOURCE_TELEGRAM, "main", chat_id, 240)]), 1)

    async def test_concurrent_same_filename_downloads_publish_to_exclusive_paths_without_overwrite(self) -> None:
        chat_id = -1087
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        both_started = asyncio.Event()
        release_both = asyncio.Event()
        started_count = 0

        class CoordinatedMessage(FakeMessage):
            def __init__(self, msg_id: int, payload: bytes) -> None:
                super().__init__(msg_id, chat_id, text=f"msg {msg_id}", media=MessageMediaDocument())
                self._payload = payload

            async def download_media(self, file: str) -> str:
                nonlocal started_count
                started_count += 1
                if started_count == 2:
                    both_started.set()
                await asyncio.wait_for(release_both.wait(), timeout=2.0)
                path = Path(file) / "report.bin"
                path.write_bytes(self._payload)
                return str(path)

        msg1 = CoordinatedMessage(301, b"payload-301")
        msg2 = CoordinatedMessage(302, b"payload-302")
        self.assertTrue(await self.service._store_message(msg1, event_type="new"))
        self.assertTrue(await self.service._store_message(msg2, event_type="new"))

        store_a = ArchiveStore(self.store.database_path)
        store_b = ArchiveStore(self.store.database_path)
        store_a.initialize()
        store_b.initialize()
        try:
            svc_a = TelegramArchiveService(self.config, store_a, media_download=MediaDownloadConfig(retries=2, retry_delay_seconds=0))
            svc_b = TelegramArchiveService(self.config, store_b, media_download=MediaDownloadConfig(retries=2, retry_delay_seconds=0))
            svc_a.client = FakeMediaClient(msg1, msg2)
            svc_b.client = FakeMediaClient(msg1, msg2)

            task_a = asyncio.create_task(svc_a.process_pending_media_downloads(max_jobs=1))
            task_b = asyncio.create_task(svc_b.process_pending_media_downloads(max_jobs=1))
            await asyncio.wait_for(both_started.wait(), timeout=2.0)
            release_both.set()
            await asyncio.gather(task_a, task_b)

            files = self.store.list_media_files(account_id="main", chat_id=chat_id)
            by_msg = {item["message_id"]: Path(item["file_path"]) for item in files}
            self.assertEqual(set(by_msg), {301, 302})
            self.assertNotEqual(by_msg[301], by_msg[302])
            self.assertEqual(by_msg[301].read_bytes(), b"payload-301")
            self.assertEqual(by_msg[302].read_bytes(), b"payload-302")
        finally:
            store_a.close()
            store_b.close()

    async def test_late_older_edit_does_not_cancel_or_regress_newer_attachment_job(self) -> None:
        chat_id = -1086
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )

        class PayloadMessage(FakeMessage):
            def __init__(self, msg_id: int, *, payload: bytes | None, edit_dt: datetime | None) -> None:
                super().__init__(
                    msg_id,
                    chat_id,
                    text="body",
                    media=MessageMediaDocument() if payload is not None else None,
                )
                self.edit_date = edit_dt
                self._payload = payload

            async def download_media(self, file: str) -> str:
                assert self._payload is not None
                path = Path(file) / "doc.bin"
                path.write_bytes(self._payload)
                return str(path)

        v2 = PayloadMessage(310, payload=b"newest-v2", edit_dt=datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc))
        late_no_media = PayloadMessage(310, payload=None, edit_dt=datetime(2026, 4, 2, 11, 0, tzinfo=timezone.utc))
        late_with_media = PayloadMessage(310, payload=b"stale-v1", edit_dt=datetime(2026, 4, 2, 11, 30, tzinfo=timezone.utc))

        self.service.client = FakeMediaClient(v2)
        self.assertTrue(await self.service._store_message(v2, event_type="edit"))
        # Late edit without media must NOT cancel the newer job.
        self.assertTrue(await self.service._store_message(late_no_media, event_type="edit"))
        # Late edit with media must NOT regress target_edited_at to 11:30.
        self.assertTrue(await self.service._store_message(late_with_media, event_type="edit"))

        jobs = self.store.list_media_download_jobs(account_id="main")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["status"], "pending")
        self.assertEqual(jobs[0]["target_edited_at"], "2026-04-02T12:00:00+00:00")

        await self.service.process_pending_media_downloads()
        finished = self.store.get_media_download_job(jobs[0]["job_id"])
        assert finished is not None
        self.assertEqual(finished["status"], "completed")
        files = self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=310)
        self.assertEqual(len(files), 1)
        self.assertEqual(Path(files[0]["file_path"]).read_bytes(), b"newest-v2")

    async def test_exhausted_stale_crash_and_transient_unlink_failure_clean_up_on_retry(self) -> None:
        from unittest import mock

        chat_id = -1085
        target_dir = self.store.database_path.parent / "media" / "main" / str(chat_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        service = TelegramArchiveService(
            self.config,
            self.store,
            media_download=MediaDownloadConfig(max_attempts=1, retry_delay_seconds=0, stale_seconds=60.0),
        )
        msg = FakeMessage(320, chat_id, media=MessageMediaDocument())
        service.client = FakeMediaClient(msg)

        self.assertTrue(await service._store_message(msg, event_type="new"))
        claimed = self.store.claim_media_download_job("main", stale_seconds=60.0)
        assert claimed is not None
        job_id = claimed["job_id"]
        part_dir = target_dir / f".part-{job_id}-t1"
        part_dir.mkdir(parents=True, exist_ok=True)
        (part_dir / "part.bin").write_bytes(b"half")
        orphan_dir = target_dir / f"{job_id}-t1"
        orphan_dir.mkdir(parents=True, exist_ok=True)
        orphan_file = orphan_dir / "320.bin"
        orphan_file.write_bytes(b"uncommitted")
        self.assertEqual(
            self.store.record_media_download_pending_path(
                job_id,
                expected_claim_token=1,
                pending_file_path=str(orphan_file),
            ),
            "ok",
        )

        # Restart BEFORE lease expires: startup cleanup must NOT touch active lease artifacts.
        service._cleanup_orphan_media_artifacts()
        self.assertTrue(part_dir.exists())
        self.assertTrue(orphan_file.exists())

        # Now expire the lease on the final attempt (attempts == max_attempts == 1).
        self.store._conn.execute(
            "UPDATE media_download_jobs SET updated_at = '2020-01-01T00:00:00+00:00' WHERE job_id = ?",
            (job_id,),
        )
        self.store._conn.commit()

        # First cleanup pass hits transient unlink PermissionError -> pending_file_path stays recorded in DB!
        orig_unlink = Path.unlink

        def flaky_unlink(path_self: Path, *args, **kwargs):
            if path_self == orphan_file:
                raise PermissionError("busy file")
            return orig_unlink(path_self, *args, **kwargs)

        with mock.patch.object(Path, "unlink", flaky_unlink):
            self.assertEqual(await service.process_pending_media_downloads(), 0)
        job_after_err = self.store.get_media_download_job(job_id)
        assert job_after_err is not None
        self.assertEqual(job_after_err["status"], "failed")
        self.assertEqual(job_after_err["pending_file_path"], str(orphan_file))
        self.assertTrue(orphan_file.exists())
        self.assertFalse(part_dir.exists())

        # Next cleanup pass succeeds and removes the orphan file and clears pending_file_path.
        self.assertEqual(await service.process_pending_media_downloads(), 0)
        job_after_ok = self.store.get_media_download_job(job_id)
        assert job_after_ok is not None
        self.assertIsNone(job_after_ok["pending_file_path"])
        self.assertEqual(list(target_dir.iterdir()), [])

    async def test_backfill_newer_edited_version_advances_media_job_target_instead_of_canceling(self) -> None:
        chat_id = -1084
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )

        class EditedMediaMessage(FakeMessage):
            def __init__(self, msg_id: int, *, payload: bytes, edit_dt: datetime) -> None:
                super().__init__(msg_id, chat_id, text="edited", media=MessageMediaDocument())
                self.edit_date = edit_dt
                self._payload = payload

            async def download_media(self, file: str) -> str:
                self.download_attempts += 1
                path = Path(file) / "doc.bin"
                path.write_bytes(self._payload)
                return str(path)

        v1 = EditedMediaMessage(330, payload=b"v1-old", edit_dt=datetime(2026, 4, 2, 11, 0, tzinfo=timezone.utc))
        v2 = EditedMediaMessage(330, payload=b"v2-backfill-new", edit_dt=datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc))

        self.assertTrue(await self.service._store_message(v1, event_type="edit"))
        # Backfill delivers a newer edited version before v1 is downloaded.
        self.assertTrue(await self.service._store_message(v2, event_type="backfill"))
        self.service.client = FakeMediaClient(v2)

        jobs = self.store.list_media_download_jobs(account_id="main")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["target_edited_at"], "2026-04-02T12:00:00+00:00")

        await self.service.process_pending_media_downloads()
        finished = self.store.get_media_download_job(jobs[0]["job_id"])
        assert finished is not None
        self.assertEqual(finished["status"], "completed")
        files = self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=330)
        self.assertEqual(len(files), 1)
        self.assertEqual(Path(files[0]["file_path"]).read_bytes(), b"v2-backfill-new")

    async def test_idle_worker_retries_failed_cleanup_and_reclaim_preserves_uncleaned_pending_path(self) -> None:
        from unittest import mock

        chat_id = -1083
        target_dir = self.store.database_path.parent / "media" / "main" / str(chat_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        service = TelegramArchiveService(
            self.config,
            self.store,
            media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
        )
        msg1 = FakeMessage(340, chat_id, media=MessageMediaDocument())
        client = FakeMediaClient(msg1)
        service.client = client

        # Part A: Reclaimed job with uncleaned pending_file_path does NOT overwrite pending_file_path or download.
        self.assertTrue(await service._store_message(msg1, event_type="new"))
        claimed1 = self.store.claim_media_download_job("main", stale_seconds=60.0)
        assert claimed1 is not None
        job1_id = claimed1["job_id"]
        orphan1_dir = target_dir / f"{job1_id}-t1"
        orphan1_dir.mkdir(parents=True, exist_ok=True)
        orphan1 = orphan1_dir / "340.bin"
        orphan1.write_bytes(b"stale-uncleaned")
        self.assertEqual(
            self.store.record_media_download_pending_path(
                job1_id,
                expected_claim_token=1,
                pending_file_path=str(orphan1),
            ),
            "ok",
        )
        self.store._conn.execute(
            "UPDATE media_download_jobs SET updated_at = '2020-01-01T00:00:00+00:00' WHERE job_id = ?",
            (job1_id,),
        )
        self.store._conn.commit()

        orig_unlink = Path.unlink

        def fail_orphan1(path_self: Path, *args, **kwargs):
            if path_self == orphan1:
                raise PermissionError("locked file")
            return orig_unlink(path_self, *args, **kwargs)

        with mock.patch.object(Path, "unlink", fail_orphan1):
            await service.process_pending_media_downloads(max_jobs=1)
        job1_mid = self.store.get_media_download_job(job1_id)
        assert job1_mid is not None
        self.assertEqual(job1_mid["status"], "retry")
        self.assertEqual(job1_mid["pending_file_path"], str(orphan1))
        self.assertEqual(msg1.download_attempts, 0)
        self.assertTrue(orphan1.exists())

        # Once unlink error clears, reclaim cleans orphan1 and completes cleanly.
        self.store._conn.execute(
            "UPDATE media_download_jobs SET next_attempt_at = '2020-01-01T00:00:00+00:00' WHERE job_id = ?",
            (job1_id,),
        )
        self.store._conn.commit()
        await service.process_pending_media_downloads()
        self.assertFalse(orphan1.exists())
        self.assertEqual(len(self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=340)), 1)

        # Part B: Idle worker automatically retries failed cleanup on bounded backoff without external wakeup.
        msg2 = FakeMessage(341, chat_id, media=MessageMediaDocument())
        self.assertTrue(await service._store_message(msg2, event_type="new"))
        claimed2 = self.store.claim_media_download_job("main", stale_seconds=60.0)
        assert claimed2 is not None
        job2_id = claimed2["job_id"]
        orphan2_dir = target_dir / f"{job2_id}-t1"
        orphan2_dir.mkdir(parents=True, exist_ok=True)
        orphan2 = orphan2_dir / "341.bin"
        orphan2.write_bytes(b"exhausted-orphan")
        self.assertEqual(
            self.store.record_media_download_pending_path(
                job2_id,
                expected_claim_token=1,
                pending_file_path=str(orphan2),
            ),
            "ok",
        )
        self.store._conn.execute(
            "UPDATE media_download_jobs SET attempts = max_attempts, updated_at = '2020-01-01T00:00:00+00:00' WHERE job_id = ?",
            (job2_id,),
        )
        self.store._conn.commit()

        class IdleLiveClient(FakeMediaClient):
            def on(self, event_builder: object):
                return lambda fn: fn

            async def is_user_authorized(self) -> bool:
                return True

            def is_connected(self) -> bool:
                return True

            def iter_messages(self, cid: int, **kwargs):
                return FakeBackfillIterator(messages=[])

            async def get_messages(self, cid: int, ids: int | None = None, **kwargs):
                if ids is not None:
                    return await super().get_messages(cid, ids=ids, **kwargs)
                return []

        fail_counter = {"remaining": 2}

        def fail_twice_unlink(path_self: Path, *args, **kwargs):
            if path_self == orphan2 and fail_counter["remaining"] > 0:
                fail_counter["remaining"] -= 1
                raise PermissionError("temporary lock")
            return orig_unlink(path_self, *args, **kwargs)

        try:
            with mock.patch.object(Path, "unlink", fail_twice_unlink):
                await service.activate(IdleLiveClient())
                deadline = asyncio.get_running_loop().time() + 3.0
                while orphan2.exists() and asyncio.get_running_loop().time() < deadline:
                    await asyncio.sleep(0.03)
            self.assertFalse(orphan2.exists())
            job2_done = self.store.get_media_download_job(job2_id)
            assert job2_done is not None
            self.assertIsNone(job2_done["pending_file_path"])
        finally:
            await service.stop()

    async def test_long_250_byte_filename_and_legacy_file_collision_publish_cleanly_in_exclusive_subdir(self) -> None:
        chat_id = -1082
        target_dir = self.store.database_path.parent / "media" / "main" / str(chat_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        # Pre-populate legacy flat files to prove zero collision with per-claim publish subdirectories.
        (target_dir / "report.bin").write_bytes(b"legacy-report")
        (target_dir / "report_m351_t1.bin").write_bytes(b"legacy-m351")
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        long_name = ("a" * 246) + ".bin"
        self.assertEqual(len(long_name.encode("utf-8")), 250)

        class NamedMessage(FakeMessage):
            def __init__(self, msg_id: int, filename: str, payload: bytes) -> None:
                super().__init__(msg_id, chat_id, media=MessageMediaDocument())
                self._filename = filename
                self._payload = payload

            async def download_media(self, file: str) -> str:
                path = Path(file) / self._filename
                path.write_bytes(self._payload)
                return str(path)

        msg_long = NamedMessage(350, long_name, b"long-payload")
        msg_legacy = NamedMessage(351, "report.bin", b"new-report-payload")
        self.service.client = FakeMediaClient(msg_long, msg_legacy)

        self.assertTrue(await self.service._store_message(msg_long, event_type="new"))
        self.assertTrue(await self.service._store_message(msg_legacy, event_type="new"))
        await self.service.process_pending_media_downloads()

        files_by_msg = {
            item["message_id"]: Path(item["file_path"])
            for item in self.store.list_media_files(account_id="main", chat_id=chat_id)
        }
        self.assertEqual(set(files_by_msg), {350, 351})
        self.assertEqual(files_by_msg[350].name, long_name)
        self.assertEqual(files_by_msg[350].read_bytes(), b"long-payload")
        self.assertEqual(files_by_msg[351].name, "report.bin")
        self.assertEqual(files_by_msg[351].read_bytes(), b"new-report-payload")
        # Legacy flat files are completely untouched.
        self.assertEqual((target_dir / "report.bin").read_bytes(), b"legacy-report")
        self.assertEqual((target_dir / "report_m351_t1.bin").read_bytes(), b"legacy-m351")

    async def test_transient_cleanup_failure_preserves_download_attempt_budget(self) -> None:
        from unittest import mock

        chat_id = -1084
        target_dir = self.store.database_path.parent / "media" / "main" / str(chat_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        service = TelegramArchiveService(
            self.config,
            self.store,
            media_download=MediaDownloadConfig(max_attempts=2, retry_delay_seconds=0.01, stale_seconds=60.0),
        )
        msg = FakeMessage(360, chat_id, media=MessageMediaDocument())
        service.client = FakeMediaClient(msg)

        self.assertTrue(await service._store_message(msg, event_type="new"))
        claimed1 = self.store.claim_media_download_job("main", stale_seconds=60.0)
        assert claimed1 is not None
        job_id = claimed1["job_id"]
        orphan_dir = target_dir / f"{job_id}-t1"
        orphan_dir.mkdir(parents=True, exist_ok=True)
        orphan_file = orphan_dir / "360.bin"
        orphan_file.write_bytes(b"leftover")
        self.assertEqual(
            self.store.record_media_download_pending_path(
                job_id,
                expected_claim_token=1,
                pending_file_path=str(orphan_file),
            ),
            "ok",
        )
        self.store._conn.execute(
            "UPDATE media_download_jobs SET updated_at = '2020-01-01T00:00:00+00:00' WHERE job_id = ?",
            (job_id,),
        )
        self.store._conn.commit()

        # Fail ONLY the first unlink inside _execute_media_download_job on reclaim;
        # the second unlink inside retry_media_download_job succeeds.
        orig_unlink = Path.unlink
        fail_once = {"remaining": 1}

        def fail_first_unlink(path_self: Path, *args, **kwargs):
            if path_self == orphan_file and fail_once["remaining"] > 0:
                fail_once["remaining"] -= 1
                raise PermissionError("transient lock")
            return orig_unlink(path_self, *args, **kwargs)

        claimed2 = self.store.claim_media_download_job("main", stale_seconds=60.0)
        assert claimed2 is not None
        self.assertEqual(claimed2["attempts"], 2)
        with mock.patch.object(Path, "unlink", fail_first_unlink):
            await service._execute_media_download_job(claimed2)

        mid_job = self.store.get_media_download_job(job_id)
        assert mid_job is not None
        self.assertEqual(mid_job["status"], "retry")
        self.assertEqual(mid_job["attempts"], 1)
        self.assertIsNone(mid_job["pending_file_path"])
        self.assertEqual(msg.download_attempts, 0)
        self.assertFalse(orphan_file.exists())

        # Once due, the preserved final attempt downloads and completes cleanly.
        self.store._conn.execute(
            "UPDATE media_download_jobs SET next_attempt_at = '2020-01-01T00:00:00+00:00' WHERE job_id = ?",
            (job_id,),
        )
        self.store._conn.commit()
        await service.process_pending_media_downloads()

        final_job = self.store.get_media_download_job(job_id)
        assert final_job is not None
        self.assertEqual(final_job["status"], "completed")
        self.assertEqual(msg.download_attempts, 1)
        self.assertEqual(len(self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=360)), 1)

    async def test_concurrent_cleanup_does_not_remove_active_publish_directory_between_mkdir_and_replace(self) -> None:
        from unittest import mock

        chat_id = -1085
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        worker1 = TelegramArchiveService(
            self.config,
            self.store,
            media_download=MediaDownloadConfig(max_attempts=1, retry_delay_seconds=0.01, stale_seconds=60.0),
        )
        store2 = ArchiveStore(self.store.database_path)
        try:
            worker2 = TelegramArchiveService(
                self.config,
                store2,
                media_download=MediaDownloadConfig(max_attempts=1, retry_delay_seconds=0.01, stale_seconds=60.0),
            )
            msg = FakeMessage(361, chat_id, media=MessageMediaDocument())
            worker1.client = FakeMediaClient(msg)
            self.assertTrue(await worker1._store_message(msg, event_type="new"))

            orig_replace = os.replace
            ran_concurrent_cleanup = {"count": 0}

            def replace_with_concurrent_cleanup(src, dst):
                dst_path = Path(dst)
                self.assertTrue(dst_path.parent.is_dir())
                ran_concurrent_cleanup["count"] += 1
                worker2._cleanup_orphan_media_artifacts()
                self.assertTrue(dst_path.parent.is_dir())
                return orig_replace(src, dst)

            with mock.patch("tele_mess_core.telegram.ingest.os.replace", side_effect=replace_with_concurrent_cleanup):
                await worker1.process_pending_media_downloads()

            self.assertEqual(ran_concurrent_cleanup["count"], 1)
            files = self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=361)
            self.assertEqual(len(files), 1)
            self.assertTrue(Path(files[0]["file_path"]).exists())
            jobs = [
                j
                for j in self.store.list_media_download_jobs(account_id="main")
                if j["chat_id"] == chat_id and j["message_id"] == 361
            ]
            self.assertEqual(jobs[0]["status"], "completed")
        finally:
            store2.close()

    async def test_eacces_stat_and_unlink_preserves_pending_and_superseded_paths_for_retry(self) -> None:
        from unittest import mock

        chat_id = -1086
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        service = TelegramArchiveService(
            self.config,
            self.store,
            media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.01, stale_seconds=60.0),
        )

        # Part A: Both unlink and stat raising EACCES must NOT clear pending_file_path.
        msg_a = FakeMessage(370, chat_id, media=MessageMediaDocument())
        self.assertTrue(await service._store_message(msg_a, event_type="new"))
        claimed_a = self.store.claim_media_download_job("main", stale_seconds=60.0)
        assert claimed_a is not None
        job_a_id = claimed_a["job_id"]
        target_dir = self.store.database_path.parent / "media" / "main" / str(chat_id)
        orphan_a_dir = target_dir / f"{job_a_id}-t1"
        orphan_a_dir.mkdir(parents=True, exist_ok=True)
        orphan_a = orphan_a_dir / "370.bin"
        orphan_a.write_bytes(b"eacces-orphan")
        self.assertEqual(
            self.store.record_media_download_pending_path(
                job_a_id,
                expected_claim_token=1,
                pending_file_path=str(orphan_a),
            ),
            "ok",
        )
        self.store.fail_media_download_job(
            job_a_id,
            error="aborted",
            error_code="aborted",
            expected_claim_token=1,
        )
        # Re-attach orphan_a on the failed job to test cleanup_media_download_pending_path under EACCES on unlink+stat.
        orphan_a_dir.mkdir(parents=True, exist_ok=True)
        orphan_a.write_bytes(b"eacces-orphan")
        self.store._conn.execute(
            "UPDATE media_download_jobs SET pending_file_path = ? WHERE job_id = ?",
            (str(orphan_a), job_a_id),
        )
        self.store._conn.commit()

        orig_unlink = Path.unlink
        orig_stat = Path.stat

        def eacces_unlink(path_self: Path, *args, **kwargs):
            if path_self == orphan_a:
                raise PermissionError(errno.EACCES, "Permission denied", str(path_self))
            return orig_unlink(path_self, *args, **kwargs)

        def eacces_stat(path_self: Path, *args, **kwargs):
            if path_self == orphan_a:
                raise PermissionError(errno.EACCES, "Permission denied", str(path_self))
            return orig_stat(path_self, *args, **kwargs)

        with mock.patch.object(Path, "unlink", eacces_unlink), mock.patch.object(Path, "stat", eacces_stat):
            cleared, unlinked = self.store.cleanup_media_download_pending_path(
                job_a_id,
                expected_claim_token=1,
                require_downloading=False,
            )
            self.assertFalse(cleared)
            self.assertIsNone(unlinked)
        job_a_after = self.store.get_media_download_job(job_a_id)
        assert job_a_after is not None
        self.assertEqual(job_a_after["pending_file_path"], str(orphan_a))
        self.assertTrue(orphan_a.exists())

        # Once EACCES clears, orphan cleanup removes orphan_a and clears pending_file_path.
        self.assertFalse(service._cleanup_orphan_media_artifacts())
        self.assertFalse(orphan_a.exists())
        self.assertFalse(orphan_a_dir.exists())
        job_a_cleaned = self.store.get_media_download_job(job_a_id)
        assert job_a_cleaned is not None
        self.assertIsNone(job_a_cleaned["pending_file_path"])

        # Part B: Attachment replacement persists superseded path for retry when post-commit unlink fails with EACCES.
        class PayloadMessage(FakeMessage):
            def __init__(self, payload: bytes, edit_date: datetime | None = None) -> None:
                super().__init__(371, chat_id, media=MessageMediaDocument())
                self._payload = payload
                self.edit_date = edit_date

            async def download_media(self, file: str) -> str:
                path = Path(file) / "photo.bin"
                path.write_bytes(self._payload)
                return str(path)

        msg_v1 = PayloadMessage(b"v1-bytes")
        service.client = FakeMediaClient(msg_v1)
        self.assertTrue(await service._store_message(msg_v1, event_type="new"))
        await service.process_pending_media_downloads()
        v1_files = self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=371)
        self.assertEqual(len(v1_files), 1)
        v1_path = Path(v1_files[0]["file_path"])
        self.assertTrue(v1_path.exists())

        msg_v2 = PayloadMessage(b"v2-bytes", edit_date=datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc))
        service.client = FakeMediaClient(msg_v2)
        self.assertTrue(await service._store_message(msg_v2, event_type="edit"))

        def eacces_v1_unlink(path_self: Path, *args, **kwargs):
            if path_self == v1_path:
                raise PermissionError(errno.EACCES, "Permission denied", str(path_self))
            return orig_unlink(path_self, *args, **kwargs)

        def eacces_v1_stat(path_self: Path, *args, **kwargs):
            if path_self == v1_path:
                raise PermissionError(errno.EACCES, "Permission denied", str(path_self))
            return orig_stat(path_self, *args, **kwargs)

        with mock.patch.object(Path, "unlink", eacces_v1_unlink), mock.patch.object(Path, "stat", eacces_v1_stat):
            await service.process_pending_media_downloads(max_jobs=1)

        v2_files = self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=371)
        self.assertEqual(len(v2_files), 1)
        v2_path = Path(v2_files[0]["file_path"])
        self.assertEqual(v2_path.read_bytes(), b"v2-bytes")
        self.assertTrue(v1_path.exists())
        job_b = next(
            j
            for j in self.store.list_media_download_jobs(account_id="main")
            if j["chat_id"] == chat_id and j["message_id"] == 371
        )
        assert job_b is not None
        self.assertEqual(job_b["status"], "completed")
        self.assertEqual(job_b["pending_file_path"], str(v1_path))

        # Later cleanup pass retries and deletes the superseded v1 file and its empty directory.
        self.assertFalse(service._cleanup_orphan_media_artifacts())
        self.assertFalse(v1_path.exists())
        self.assertFalse(v1_path.parent.exists())
        job_b_cleaned = self.store.get_media_download_job(job_b["job_id"])
        assert job_b_cleaned is not None
        self.assertIsNone(job_b_cleaned["pending_file_path"])
        self.assertTrue(v2_path.exists())

    async def test_heartbeat_survives_transient_sqlite_lock_and_completes_publish(self) -> None:
        import sqlite3
        from unittest import mock

        chat_id = -1087
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        service = TelegramArchiveService(
            self.config,
            self.store,
            media_download=MediaDownloadConfig(max_attempts=1, retry_delay_seconds=0.05, stale_seconds=0.35),
        )
        store2 = ArchiveStore(self.store.database_path)
        try:
            other_claimed: list[dict | None] = []

            class SlowDownloadMessage(FakeMessage):
                async def download_media(self, file: str) -> str:
                    # Exceed stale_seconds so lease survival depends on heartbeat continuing after transient lock.
                    await asyncio.sleep(0.45)
                    other_claimed.append(store2.claim_media_download_job("main", stale_seconds=0.35))
                    await asyncio.sleep(0.10)
                    return await super().download_media(file)

            msg = SlowDownloadMessage(380, chat_id, media=MessageMediaDocument())
            service.client = FakeMediaClient(msg)
            self.assertTrue(await service._store_message(msg, event_type="new"))

            orig_heartbeat = self.store.heartbeat_media_download_job
            hb_calls = {"count": 0}

            def flaky_heartbeat(*args, **kwargs):
                hb_calls["count"] += 1
                if hb_calls["count"] == 1:
                    raise sqlite3.OperationalError("database is locked")
                return orig_heartbeat(*args, **kwargs)

            with mock.patch.object(self.store, "heartbeat_media_download_job", side_effect=flaky_heartbeat):
                await service.process_pending_media_downloads()

            self.assertGreaterEqual(hb_calls["count"], 2)
            self.assertEqual(other_claimed, [None])
            files = self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=380)
            self.assertEqual(len(files), 1)
            self.assertTrue(Path(files[0]["file_path"]).exists())
            job = next(
                j for j in self.store.list_media_download_jobs(account_id="main")
                if j["chat_id"] == chat_id and j["message_id"] == 380
            )
            self.assertEqual(job["status"], "completed")
        finally:
            store2.close()

    async def test_persistent_cleanup_failure_backs_off_exponentially_even_with_zero_retry_delay(self) -> None:
        from unittest import mock

        chat_id = -1088
        target_dir = self.store.database_path.parent / "media" / "main" / str(chat_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        service = TelegramArchiveService(
            self.config,
            self.store,
            media_download=MediaDownloadConfig(
                max_attempts=2,
                retry_delay_seconds=0.0,
                max_retry_delay_seconds=60.0,
                stale_seconds=60.0,
            ),
        )
        msg = FakeMessage(381, chat_id, media=MessageMediaDocument())
        service.client = FakeMediaClient(msg)
        self.assertTrue(await service._store_message(msg, event_type="new"))
        claimed1 = self.store.claim_media_download_job("main", stale_seconds=60.0)
        assert claimed1 is not None
        job_id = claimed1["job_id"]
        orphan_dir = target_dir / f"{job_id}-t1"
        orphan_dir.mkdir(parents=True, exist_ok=True)
        orphan = orphan_dir / "381.bin"
        orphan.write_bytes(b"stuck-file")
        self.assertEqual(
            self.store.record_media_download_pending_path(
                job_id,
                expected_claim_token=1,
                pending_file_path=str(orphan),
            ),
            "ok",
        )
        self.store._conn.execute(
            "UPDATE media_download_jobs SET updated_at = '2020-01-01T00:00:00+00:00' WHERE job_id = ?",
            (job_id,),
        )
        self.store._conn.commit()

        orig_unlink = Path.unlink

        def always_eacces(path_self: Path, *args, **kwargs):
            if path_self == orphan:
                raise PermissionError(errno.EACCES, "Permission denied", str(path_self))
            return orig_unlink(path_self, *args, **kwargs)

        delays: list[float] = []
        with mock.patch.object(Path, "unlink", always_eacces):
            for _ in range(4):
                claimed = self.store.claim_media_download_job("main", stale_seconds=60.0)
                assert claimed is not None
                before = datetime.now(timezone.utc)
                await service._execute_media_download_job(claimed)
                row = self.store.get_media_download_job(job_id)
                assert row is not None
                self.assertEqual(row["status"], "retry")
                self.assertEqual(row["attempts"], 1)
                self.assertEqual(row["pending_file_path"], str(orphan))
                due = datetime.fromisoformat(row["next_attempt_at"])
                delays.append((due - before).total_seconds())
                self.store._conn.execute(
                    "UPDATE media_download_jobs SET next_attempt_at = '2020-01-01T00:00:00+00:00' WHERE job_id = ?",
                    (job_id,),
                )
                self.store._conn.commit()

        self.assertEqual(msg.download_attempts, 0)
        self.assertGreater(delays[1], delays[0] * 1.5)
        self.assertGreater(delays[2], delays[1] * 1.5)
        self.assertGreater(delays[3], delays[2] * 1.5)
        self.assertGreaterEqual(delays[3], 0.35)

    async def test_idle_worker_picks_up_job_enqueued_by_separate_service_instance(self) -> None:
        chat_id = -1089
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        msg = FakeMessage(382, chat_id, media=MessageMediaDocument())

        class IdleCrossInstanceClient(FakeMediaClient):
            def on(self, event_builder: object):
                return lambda fn: fn

            async def is_user_authorized(self) -> bool:
                return True

            def is_connected(self) -> bool:
                return True

            def iter_messages(self, cid: int, **kwargs):
                return FakeBackfillIterator(messages=[])

            async def get_messages(self, cid: int, ids: int | None = None, **kwargs):
                if ids is not None:
                    return await super().get_messages(cid, ids=ids, **kwargs)
                return []

        service1 = TelegramArchiveService(
            self.config,
            self.store,
            media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
        )
        store2 = ArchiveStore(self.store.database_path)
        try:
            await service1.activate(IdleCrossInstanceClient(msg))
            # Let worker1 enter its idle wait loop with an empty queue.
            await asyncio.sleep(0.12)

            service2 = TelegramArchiveService(
                self.config,
                store2,
                media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
            )
            self.assertTrue(await service2._store_message(msg, event_type="new"))

            deadline = asyncio.get_running_loop().time() + 2.0
            while asyncio.get_running_loop().time() < deadline:
                if self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=382):
                    break
                await asyncio.sleep(0.03)

            files = self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=382)
            self.assertEqual(len(files), 1)
            self.assertTrue(Path(files[0]["file_path"]).exists())
            job = next(
                j for j in self.store.list_media_download_jobs(account_id="main")
                if j["chat_id"] == chat_id and j["message_id"] == 382
            )
            self.assertEqual(job["status"], "completed")
        finally:
            await service1.stop()
            store2.close()

    async def test_delete_management_account_preserves_uncleaned_pending_path_for_later_cleanup(self) -> None:
        from unittest import mock

        chat_id = -1090
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        service = TelegramArchiveService(
            self.config,
            self.store,
            media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
        )

        class ReplacementMessage(FakeMessage):
            def __init__(self, payload: bytes, edit_date: datetime | None = None) -> None:
                super().__init__(383, chat_id, media=MessageMediaDocument())
                self._payload = payload
                self.edit_date = edit_date

            async def download_media(self, file: str) -> str:
                path = Path(file) / "clip.bin"
                path.write_bytes(self._payload)
                return str(path)

        msg_v1 = ReplacementMessage(b"old-clip")
        service.client = FakeMediaClient(msg_v1)
        self.assertTrue(await service._store_message(msg_v1, event_type="new"))
        await service.process_pending_media_downloads()
        v1_path = Path(self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=383)[0]["file_path"])
        self.assertTrue(v1_path.exists())

        msg_v2 = ReplacementMessage(b"new-clip", edit_date=datetime(2026, 3, 3, 12, 0, tzinfo=timezone.utc))
        service.client = FakeMediaClient(msg_v2)
        self.assertTrue(await service._store_message(msg_v2, event_type="edit"))

        orig_unlink = Path.unlink
        orig_stat = Path.stat

        def eacces_v1_unlink(path_self: Path, *args, **kwargs):
            if path_self == v1_path:
                raise PermissionError(errno.EACCES, "Permission denied", str(path_self))
            return orig_unlink(path_self, *args, **kwargs)

        def eacces_v1_stat(path_self: Path, *args, **kwargs):
            if path_self == v1_path:
                raise PermissionError(errno.EACCES, "Permission denied", str(path_self))
            return orig_stat(path_self, *args, **kwargs)

        with mock.patch.object(Path, "unlink", eacces_v1_unlink), mock.patch.object(Path, "stat", eacces_v1_stat):
            await service.process_pending_media_downloads(max_jobs=1)
            # Deleting the management account while v1_path cannot be unlinked must preserve the pending cleanup record.
            self.store.delete_management_account(SOURCE_TELEGRAM, "main")

        self.assertTrue(v1_path.exists())
        self.assertEqual(self.store.state()["media_download_pending"], 0)
        self.assertEqual(self.store.state()["media_download_failed"], 0)
        preserved = self.store.list_media_jobs_with_pending_paths(account_id="alt", source=SOURCE_TELEGRAM)
        self.assertEqual(len(preserved), 1)
        self.assertEqual(preserved[0]["pending_file_path"], str(v1_path))
        self.assertEqual(preserved[0]["error_code"], "account_deleted")

        # Once permissions recover, another surviving account's orphan scan cleans v1_path and removes the tombstone row.
        alt_config = TelegramAccountConfig(
            account_id="alt",
            api_id=self.config.api_id,
            api_hash=self.config.api_hash,
            session_name="alt",
            session_dir=self.config.session_dir,
        )
        alt_service = TelegramArchiveService(alt_config, self.store)
        self.assertFalse(alt_service._cleanup_orphan_media_artifacts())
        self.assertFalse(v1_path.exists())
        self.assertFalse(v1_path.parent.exists())
        self.assertEqual(self.store.list_media_download_jobs(account_id="main"), [])

    async def test_cleanup_fences_stale_claim_atomically_and_never_deletes_renewed_staging_dir(self) -> None:
        chat_id = -1091
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        service = TelegramArchiveService(
            self.config,
            self.store,
            media_download=MediaDownloadConfig(max_attempts=1, retry_delay_seconds=0.05, stale_seconds=60.0),
        )
        msg = FakeMessage(390, chat_id, media=MessageMediaDocument())
        self.assertTrue(await service._store_message(msg, event_type="new"))
        claimed = self.store.claim_media_download_job("main", stale_seconds=60.0)
        assert claimed is not None
        job_id = claimed["job_id"]
        target_dir = self.store.database_path.parent / "media" / "main" / str(chat_id)
        staging_dir = target_dir / f".part-{job_id}-t1"
        staging_dir.mkdir(parents=True, exist_ok=True)
        staged_file = staging_dir / "390.bin"
        staged_file.write_bytes(b"in-progress")

        store2 = ArchiveStore(self.store.database_path)
        try:
            # Make lease briefly stale, then renew via heartbeat before connection 2 fences it.
            self.store._conn.execute(
                "UPDATE media_download_jobs SET updated_at = '2020-01-01T00:00:00+00:00' WHERE job_id = ?",
                (job_id,),
            )
            self.store._conn.commit()
            self.assertTrue(self.store.heartbeat_media_download_job(job_id, expected_claim_token=1))
            self.assertFalse(
                store2.cleanup_orphan_media_staging_dirs(
                    account_id="main",
                    source=SOURCE_TELEGRAM,
                    stale_seconds=60.0,
                )
            )
            self.assertTrue(staged_file.exists())

            # When lease is genuinely stale and cleanup wins the write lock, it fences claim_token BEFORE rmtree.
            self.store._conn.execute(
                "UPDATE media_download_jobs SET updated_at = '2020-01-01T00:00:00+00:00' WHERE job_id = ?",
                (job_id,),
            )
            self.store._conn.commit()
            self.assertFalse(
                store2.cleanup_orphan_media_staging_dirs(
                    account_id="main",
                    source=SOURCE_TELEGRAM,
                    stale_seconds=60.0,
                )
            )
            self.assertFalse(staging_dir.exists())
            self.assertFalse(self.store.heartbeat_media_download_job(job_id, expected_claim_token=1))
            fenced_job = self.store.get_media_download_job(job_id)
            assert fenced_job is not None
            self.assertEqual(fenced_job["status"], "failed")
            self.assertGreater(fenced_job["claim_token"], 1)
        finally:
            store2.close()

    async def test_idle_worker_decouples_sqlite_polling_from_filesystem_staging_scan(self) -> None:
        from unittest import mock

        chat_id = -1092
        target_dir = self.store.database_path.parent / "media" / "main" / str(chat_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        for idx in range(100):
            (target_dir / f"mdj_existing{idx:04d}-t1").mkdir(exist_ok=True)

        class IdleClient(FakeMediaClient):
            def on(self, event_builder: object):
                return lambda fn: fn

            async def is_user_authorized(self) -> bool:
                return True

            def is_connected(self) -> bool:
                return True

            def iter_messages(self, cid: int, **kwargs):
                return FakeBackfillIterator(messages=[])

            async def get_messages(self, cid: int, ids: int | None = None, **kwargs):
                return []

        service = TelegramArchiveService(
            self.config,
            self.store,
            media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
        )
        fs_calls = {"count": 0}
        claim_calls = {"count": 0}
        orig_fs = self.store.cleanup_orphan_media_staging_dirs
        orig_claim = self.store.claim_media_download_job

        def count_fs(*args, **kwargs):
            fs_calls["count"] += 1
            return orig_fs(*args, **kwargs)

        def count_claim(*args, **kwargs):
            claim_calls["count"] += 1
            return orig_claim(*args, **kwargs)

        try:
            with (
                mock.patch.object(self.store, "cleanup_orphan_media_staging_dirs", side_effect=count_fs),
                mock.patch.object(self.store, "claim_media_download_job", side_effect=count_claim),
            ):
                await service.activate(IdleClient())
                await asyncio.sleep(0.45)
        finally:
            await service.stop()

        self.assertGreaterEqual(claim_calls["count"], 4)
        self.assertEqual(fs_calls["count"], 1)

    async def test_deleting_last_account_cleans_up_crashed_part_dir_and_pending_path_via_runtime_manager(self) -> None:
        import shutil
        from unittest import mock

        from tele_mess_core.config import (
            AppConfig,
            LoggingConfig,
            ServerConfig,
            StorageConfig,
            TelegramConfig,
        )
        from tele_mess_core.models import AccountRecord
        from tele_mess_core.server.api import _delete_management_account
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        chat_id = -1093
        self.store.upsert_account(AccountRecord(SOURCE_TELEGRAM, "main", "Main Account", "user"))
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        app_config = AppConfig(
            storage=StorageConfig(database=self.store.database_path),
            server=ServerConfig(),
            logging=LoggingConfig(),
            telegram=TelegramConfig(
                accounts=[self.config],
                media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
            ),
        )

        # Create both a crashed .part-* staging dir AND a superseded pending_file_path under main.
        service = TelegramArchiveService(
            self.config,
            self.store,
            media_download=app_config.telegram.media_download,
        )

        class ReplaceMsg(FakeMessage):
            def __init__(self, payload: bytes, edit_date: datetime | None = None) -> None:
                super().__init__(391, chat_id, media=MessageMediaDocument())
                self._payload = payload
                self.edit_date = edit_date

            async def download_media(self, file: str) -> str:
                path = Path(file) / "doc.bin"
                path.write_bytes(self._payload)
                return str(path)

        msg_v1 = ReplaceMsg(b"old-doc")
        service.client = FakeMediaClient(msg_v1)
        self.assertTrue(await service._store_message(msg_v1, event_type="new"))
        await service.process_pending_media_downloads()
        v1_path = Path(self.store.list_media_files(account_id="main", chat_id=chat_id, message_id=391)[0]["file_path"])

        msg_v2 = ReplaceMsg(b"new-doc", edit_date=datetime(2026, 3, 4, 12, 0, tzinfo=timezone.utc))
        service.client = FakeMediaClient(msg_v2)
        self.assertTrue(await service._store_message(msg_v2, event_type="edit"))

        crashed_part_dir = self.store.database_path.parent / "media" / "main" / str(chat_id) / ".part-mdj_crashed0001-t1"
        crashed_part_dir.mkdir(parents=True, exist_ok=True)
        (crashed_part_dir / "partial.bin").write_bytes(b"partial-crash")

        class MinimalRuntimeClient:
            def __init__(self) -> None:
                self._connected = True

            async def connect(self) -> None:
                self._connected = True

            async def disconnect(self) -> None:
                self._connected = False

            def is_connected(self) -> bool:
                return self._connected

            async def is_user_authorized(self) -> bool:
                return False

            def on(self, event_builder: object):
                return lambda fn: fn

        manager = TelegramRuntimeManager(
            app_config,
            self.store,
            client_factory=lambda _cfg: MinimalRuntimeClient(),
            ingest=False,
        )
        orig_unlink = Path.unlink
        orig_stat = Path.stat
        orig_rmtree = shutil.rmtree
        block_cleanup = {"enabled": True}

        def guarded_unlink(path_self: Path, *args, **kwargs):
            if block_cleanup["enabled"] and path_self == v1_path:
                raise PermissionError(errno.EACCES, "Permission denied", str(path_self))
            return orig_unlink(path_self, *args, **kwargs)

        def guarded_stat(path_self: Path, *args, **kwargs):
            if block_cleanup["enabled"] and path_self == v1_path:
                raise PermissionError(errno.EACCES, "Permission denied", str(path_self))
            return orig_stat(path_self, *args, **kwargs)

        def guarded_rmtree(path_arg, *args, **kwargs):
            if block_cleanup["enabled"] and Path(path_arg) == crashed_part_dir:
                raise PermissionError(errno.EACCES, "Permission denied", str(path_arg))
            return orig_rmtree(path_arg, *args, **kwargs)

        await manager.start()
        try:
            with (
                mock.patch.object(Path, "unlink", guarded_unlink),
                mock.patch.object(Path, "stat", guarded_stat),
                mock.patch("tele_mess_core.archive.store.shutil.rmtree", side_effect=guarded_rmtree),
            ):
                await service.process_pending_media_downloads(max_jobs=1)
                # Delete the ONLY account through the management API helper (from a worker thread, like HTTP does).
                await asyncio.to_thread(
                    _delete_management_account,
                    self.store,
                    {"source": SOURCE_TELEGRAM, "account_id": "main"},
                    manager,
                )
                self.assertEqual(manager.statuses(), [])
                self.assertTrue(v1_path.exists())
                self.assertTrue(crashed_part_dir.exists())

                # Release EACCES: manager's account-independent cleanup task must clean BOTH artifacts with 0 accounts!
                block_cleanup["enabled"] = False
                deadline = asyncio.get_running_loop().time() + 3.0
                while asyncio.get_running_loop().time() < deadline:
                    if not v1_path.exists() and not crashed_part_dir.exists():
                        break
                    await asyncio.sleep(0.03)

            self.assertFalse(v1_path.exists())
            self.assertFalse(v1_path.parent.exists())
            self.assertFalse(crashed_part_dir.exists())
            self.assertEqual(self.store.list_media_download_jobs(account_id="main"), [])
        finally:
            await manager.stop()

    async def test_deleting_other_source_same_account_id_and_post_delete_claim_do_not_harm_live_telegram_download(
        self,
    ) -> None:
        from tele_mess_core.models import AccountRecord
        from tele_mess_core.server.api import _delete_management_account

        chat_id = -1094
        self.store.upsert_account(AccountRecord(SOURCE_TELEGRAM, "main", "TG Main", "user"))
        self.store.upsert_account(AccountRecord("other_source", "main", "Other Main", "user"))
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", chat_id, enabled=True, download_media=True)
        )
        service = TelegramArchiveService(
            self.config,
            self.store,
            media_download=MediaDownloadConfig(max_attempts=1, retry_delay_seconds=0.05, stale_seconds=60.0),
        )
        msg = FakeMessage(392, chat_id, media=MessageMediaDocument())
        self.assertTrue(await service._store_message(msg, event_type="new"))
        claimed = self.store.claim_media_download_job("main", stale_seconds=60.0)
        assert claimed is not None
        job_id = claimed["job_id"]
        target_dir = self.store.database_path.parent / "media" / "main" / str(chat_id)
        staging_dir = target_dir / f".part-{job_id}-t1"
        staging_dir.mkdir(parents=True, exist_ok=True)
        staged_file = staging_dir / "active.bin"
        staged_file.write_bytes(b"live-telegram-bytes")

        # Deleting other_source:main must NEVER fence or delete telegram:main's active download or staging dir.
        _delete_management_account(self.store, {"source": "other_source", "account_id": "main"}, None)
        job_after = self.store.get_media_download_job(job_id)
        assert job_after is not None
        self.assertEqual(job_after["status"], "downloading")
        self.assertEqual(job_after["claim_token"], 1)
        self.assertTrue(staged_file.exists())
        self.assertTrue(self.store.heartbeat_media_download_job(job_id, expected_claim_token=1))

        # Post-delete staging cleanup (allow_fencing_downloading=False) also protects any newly created claim.
        self.assertFalse(
            self.store.cleanup_orphan_media_staging_dirs(
                account_id="main",
                source=SOURCE_TELEGRAM,
                stale_seconds=0.0,
                include_deleted_accounts=False,
                allow_fencing_downloading=False,
            )
        )
        self.assertTrue(staged_file.exists())
        job_still_live = self.store.get_media_download_job(job_id)
        assert job_still_live is not None
        self.assertEqual(job_still_live["status"], "downloading")

    async def test_manager_stop_waits_for_inflight_cleanup_thread_before_returning(self) -> None:
        import threading
        from unittest import mock

        from tele_mess_core.config import (
            AppConfig,
            LoggingConfig,
            ServerConfig,
            StorageConfig,
            TelegramConfig,
        )
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "manager_stop_cleanup.db"
            local_store = ArchiveStore(db_path)
            local_store.initialize()
            app_config = AppConfig(
                storage=StorageConfig(database=db_path),
                server=ServerConfig(),
                logging=LoggingConfig(),
                telegram=TelegramConfig(
                    accounts=[],
                    media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
                ),
            )
            entered = threading.Event()
            release = threading.Event()
            thread_errors: list[BaseException] = []

            def blocking_cleanup(*args, **kwargs) -> bool:
                entered.set()
                release.wait(timeout=5.0)
                try:
                    # Access SQLite after being released to prove store is still open until stop() completes.
                    local_store.state()
                except BaseException as exc:
                    thread_errors.append(exc)
                    raise
                return False

            manager = TelegramRuntimeManager(
                app_config,
                local_store,
                client_factory=lambda _cfg: None,
                ingest=False,
            )
            with mock.patch.object(
                local_store,
                "cleanup_deleted_account_media_artifacts",
                side_effect=blocking_cleanup,
            ):
                await manager.start()
                self.assertTrue(await asyncio.to_thread(entered.wait, 2.0))
                stop_task = asyncio.create_task(manager.stop())
                await asyncio.sleep(0.05)
                self.assertFalse(stop_task.done())
                release.set()
                await asyncio.wait_for(stop_task, timeout=2.0)
            local_store.close()
            self.assertEqual(thread_errors, [])

    async def test_service_stop_waits_for_inflight_staging_scan_thread_before_returning(self) -> None:
        import threading
        from unittest import mock

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "service_stop_cleanup.db"
            local_store = ArchiveStore(db_path)
            local_store.initialize()
            service = TelegramArchiveService(
                self.config,
                local_store,
                media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
            )
            entered = threading.Event()
            release = threading.Event()
            thread_errors: list[BaseException] = []

            def blocking_fs_scan(*args, **kwargs) -> bool:
                entered.set()
                release.wait(timeout=5.0)
                try:
                    local_store.state()
                except BaseException as exc:
                    thread_errors.append(exc)
                    raise
                return False

            class IdleClient(FakeMediaClient):
                def on(self, event_builder: object):
                    return lambda fn: fn

                async def is_user_authorized(self) -> bool:
                    return True

                def is_connected(self) -> bool:
                    return True

                def iter_messages(self, cid: int, **kwargs):
                    return FakeBackfillIterator(messages=[])

                async def get_messages(self, cid: int, ids: int | None = None, **kwargs):
                    return []

            with mock.patch.object(
                local_store,
                "cleanup_orphan_media_staging_dirs",
                side_effect=blocking_fs_scan,
            ):
                await service.activate(IdleClient())
                self.assertTrue(await asyncio.to_thread(entered.wait, 2.0))
                stop_task = asyncio.create_task(service.stop())
                await asyncio.sleep(0.05)
                self.assertFalse(stop_task.done())
                release.set()
                await asyncio.wait_for(stop_task, timeout=2.0)
            local_store.close()
            self.assertEqual(thread_errors, [])

    async def test_cancelled_first_stop_still_blocks_second_stop_until_cleanup_threads_exit(self) -> None:
        import threading
        from unittest import mock

        from tele_mess_core.config import (
            AppConfig,
            LoggingConfig,
            ServerConfig,
            StorageConfig,
            TelegramConfig,
        )
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        # Part A: Manager stop() cancelled while deleted-account cleanup thread is paused;
        # second stop() MUST still wait for the thread to exit before returning.
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "manager_cancel_stop.db"
            local_store = ArchiveStore(db_path)
            local_store.initialize()
            app_config = AppConfig(
                storage=StorageConfig(database=db_path),
                server=ServerConfig(),
                logging=LoggingConfig(),
                telegram=TelegramConfig(
                    accounts=[],
                    media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
                ),
            )
            entered = threading.Event()
            release = threading.Event()
            thread_errors: list[BaseException] = []

            def blocking_cleanup(*args, **kwargs) -> bool:
                entered.set()
                release.wait(timeout=5.0)
                try:
                    local_store.state()
                except BaseException as exc:
                    thread_errors.append(exc)
                    raise
                return False

            manager = TelegramRuntimeManager(
                app_config,
                local_store,
                client_factory=lambda _cfg: None,
                ingest=False,
            )
            with mock.patch.object(
                local_store,
                "cleanup_deleted_account_media_artifacts",
                side_effect=blocking_cleanup,
            ):
                await manager.start()
                self.assertTrue(await asyncio.to_thread(entered.wait, 2.0))
                stop1 = asyncio.create_task(manager.stop())
                await asyncio.sleep(0.05)
                self.assertFalse(stop1.done())
                stop1.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await stop1

                stop2 = asyncio.create_task(manager.stop())
                await asyncio.sleep(0.05)
                self.assertFalse(stop2.done())
                release.set()
                await asyncio.wait_for(stop2, timeout=2.0)
            local_store.close()
            self.assertEqual(thread_errors, [])

        # Part B: Service stop() cancelled while staging-scan thread is paused;
        # second stop() MUST still wait for the thread to exit before returning.
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "service_cancel_stop.db"
            local_store = ArchiveStore(db_path)
            local_store.initialize()
            service = TelegramArchiveService(
                self.config,
                local_store,
                media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
            )
            entered = threading.Event()
            release = threading.Event()
            thread_errors = []

            def blocking_fs_scan(*args, **kwargs) -> bool:
                entered.set()
                release.wait(timeout=5.0)
                try:
                    local_store.state()
                except BaseException as exc:
                    thread_errors.append(exc)
                    raise
                return False

            class IdleClient(FakeMediaClient):
                def on(self, event_builder: object):
                    return lambda fn: fn

                async def is_user_authorized(self) -> bool:
                    return True

                def is_connected(self) -> bool:
                    return True

                def iter_messages(self, cid: int, **kwargs):
                    return FakeBackfillIterator(messages=[])

                async def get_messages(self, cid: int, ids: int | None = None, **kwargs):
                    return []

            with mock.patch.object(
                local_store,
                "cleanup_orphan_media_staging_dirs",
                side_effect=blocking_fs_scan,
            ):
                await service.activate(IdleClient())
                self.assertTrue(await asyncio.to_thread(entered.wait, 2.0))
                stop1 = asyncio.create_task(service.stop())
                await asyncio.sleep(0.05)
                self.assertFalse(stop1.done())
                stop1.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await stop1

                stop2 = asyncio.create_task(service.stop())
                await asyncio.sleep(0.05)
                self.assertFalse(stop2.done())
                release.set()
                await asyncio.wait_for(stop2, timeout=2.0)
            local_store.close()
            self.assertEqual(thread_errors, [])

    async def test_delete_management_account_retry_after_unregister_timeout_waits_for_old_runtime_to_stop(
        self,
    ) -> None:
        import threading
        from unittest import mock

        from tele_mess_core.config import (
            AppConfig,
            LoggingConfig,
            ServerConfig,
            StorageConfig,
            TelegramConfig,
        )
        from tele_mess_core.models import AccountRecord
        from tele_mess_core.server.api import _delete_management_account
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "unregister_retry.db"
            local_store = ArchiveStore(db_path)
            local_store.initialize()
            local_store.upsert_account(AccountRecord(SOURCE_TELEGRAM, "main", "Main", "user"))
            app_config = AppConfig(
                storage=StorageConfig(database=db_path),
                server=ServerConfig(),
                logging=LoggingConfig(),
                telegram=TelegramConfig(
                    accounts=[self.config],
                    media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
                ),
            )
            entered = threading.Event()
            release = threading.Event()
            thread_exited = threading.Event()

            class AuthorizedFakeClient:
                def __init__(self) -> None:
                    self._connected = False
                    self.disconnected = False

                async def connect(self) -> None:
                    self._connected = True

                async def disconnect(self) -> None:
                    self._connected = False
                    self.disconnected = True

                def is_connected(self) -> bool:
                    return self._connected

                async def is_user_authorized(self) -> bool:
                    return True

                async def get_me(self):
                    return SimpleNamespace(id=1, first_name="Main", last_name=None, username="main")

                def on(self, event_builder: object):
                    return lambda fn: fn

                def iter_messages(self, cid: int, **kwargs):
                    return FakeBackfillIterator(messages=[])

                async def get_messages(self, cid: int, ids: int | None = None, **kwargs):
                    return []

            client = AuthorizedFakeClient()
            manager = TelegramRuntimeManager(
                app_config,
                local_store,
                client_factory=lambda _cfg: client,
                call_timeout=0.12,
                ingest=True,
            )

            def blocking_fs_scan(*args, **kwargs) -> bool:
                if kwargs.get("deleted_accounts_only"):
                    return False
                entered.set()
                release.wait(timeout=5.0)
                thread_exited.set()
                return False

            with mock.patch.object(
                local_store,
                "cleanup_orphan_media_staging_dirs",
                side_effect=blocking_fs_scan,
            ):
                await manager.start()
                try:
                    self.assertTrue(await asyncio.to_thread(entered.wait, 2.0))
                    # First delete call times out while unregister_account waits for the paused cleanup thread.
                    with self.assertRaises(TimeoutError):
                        await asyncio.to_thread(
                            _delete_management_account,
                            local_store,
                            {"source": SOURCE_TELEGRAM, "account_id": "main"},
                            manager,
                        )
                    self.assertIsNotNone(local_store.get_management_account(SOURCE_TELEGRAM, "main"))
                    self.assertFalse(client.disconnected)
                    self.assertFalse(thread_exited.is_set())

                    # Retry delete with a normal timeout: MUST block until the old runtime finishes stopping.
                    manager.call_timeout = 5.0
                    retry_task = asyncio.create_task(
                        asyncio.to_thread(
                            _delete_management_account,
                            local_store,
                            {"source": SOURCE_TELEGRAM, "account_id": "main"},
                            manager,
                        )
                    )
                    await asyncio.sleep(0.08)
                    self.assertFalse(retry_task.done())
                    self.assertIsNotNone(local_store.get_management_account(SOURCE_TELEGRAM, "main"))
                    self.assertFalse(client.disconnected)

                    # Release the paused cleanup thread; old runtime stops cleanly and retry commits deletion.
                    release.set()
                    result = await asyncio.wait_for(retry_task, timeout=3.0)
                    self.assertEqual(result["account_id"], "main")
                    self.assertTrue(thread_exited.is_set())
                    self.assertTrue(client.disconnected)
                    self.assertIsNone(local_store.get_management_account(SOURCE_TELEGRAM, "main"))
                finally:
                    release.set()
                    await manager.stop()
            local_store.close()

    async def test_concurrent_create_and_delete_management_account_stay_consistent(self) -> None:
        import threading
        from unittest import mock

        from tele_mess_core.config import (
            AppConfig,
            LoggingConfig,
            ServerConfig,
            StorageConfig,
            TelegramConfig,
        )
        from tele_mess_core.models import AccountRecord
        from tele_mess_core.server.api import _create_management_account, _delete_management_account
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "concurrent_create_delete.db"
            local_store = ArchiveStore(db_path)
            local_store.initialize()
            local_store.upsert_account(AccountRecord(SOURCE_TELEGRAM, "main", "Main", "user"))
            app_config = AppConfig(
                storage=StorageConfig(database=db_path),
                server=ServerConfig(),
                logging=LoggingConfig(),
                telegram=TelegramConfig(
                    accounts=[self.config],
                    media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
                ),
            )
            entered = threading.Event()
            release = threading.Event()
            block_first_scan = {"active": True}

            class MinimalAuthClient:
                def __init__(self) -> None:
                    self._connected = False

                async def connect(self) -> None:
                    self._connected = True

                async def disconnect(self) -> None:
                    self._connected = False

                def is_connected(self) -> bool:
                    return self._connected

                async def is_user_authorized(self) -> bool:
                    return True

                async def get_me(self):
                    return SimpleNamespace(id=1, first_name="Main", last_name=None, username="main")

                def on(self, event_builder: object):
                    return lambda fn: fn

                def iter_messages(self, cid: int, **kwargs):
                    return FakeBackfillIterator(messages=[])

                async def get_messages(self, cid: int, ids: int | None = None, **kwargs):
                    return []

            def blocking_first_scan(*args, **kwargs) -> bool:
                if kwargs.get("deleted_accounts_only"):
                    return False
                if block_first_scan["active"]:
                    block_first_scan["active"] = False
                    entered.set()
                    release.wait(timeout=5.0)
                return False

            manager = TelegramRuntimeManager(
                app_config,
                local_store,
                client_factory=lambda _cfg: MinimalAuthClient(),
                ingest=True,
            )
            with mock.patch.object(
                local_store,
                "cleanup_orphan_media_staging_dirs",
                side_effect=blocking_first_scan,
            ):
                await manager.start()
                try:
                    self.assertTrue(await asyncio.to_thread(entered.wait, 2.0))
                    # Begin stopping old runtime via unregister, then run concurrent POST and DELETE for "main".
                    delete_task = asyncio.create_task(
                        asyncio.to_thread(
                            _delete_management_account,
                            local_store,
                            {"source": SOURCE_TELEGRAM, "account_id": "main"},
                            manager,
                        )
                    )
                    await asyncio.sleep(0.05)
                    create_task = asyncio.create_task(
                        asyncio.to_thread(
                            _create_management_account,
                            app_config,
                            local_store,
                            {
                                "source": SOURCE_TELEGRAM,
                                "account_id": "main",
                                "display_name": "Main Recreated",
                                "session_dir": str(self.config.session_dir),
                            },
                            manager,
                        )
                    )
                    await asyncio.sleep(0.05)
                    # Because mutations are serialized per (source, account_id), create cannot interleave mid-delete.
                    self.assertFalse(delete_task.done())
                    self.assertFalse(create_task.done())
                    release.set()
                    await asyncio.gather(delete_task, create_task)
                    db_exists = local_store.get_management_account(SOURCE_TELEGRAM, "main") is not None
                    runtime_exists = "main" in {s["account_id"] for s in manager.statuses()}
                    self.assertEqual(db_exists, runtime_exists)
                    self.assertTrue(db_exists)

                    # Also verify POST followed concurrently by DELETE leaves neither DB record nor running runtime.
                    c2 = asyncio.create_task(
                        asyncio.to_thread(
                            _create_management_account,
                            app_config,
                            local_store,
                            {
                                "source": SOURCE_TELEGRAM,
                                "account_id": "main",
                                "display_name": "Main Updated",
                                "session_dir": str(self.config.session_dir),
                            },
                            manager,
                        )
                    )
                    d2 = asyncio.create_task(
                        asyncio.to_thread(
                            _delete_management_account,
                            local_store,
                            {"source": SOURCE_TELEGRAM, "account_id": "main"},
                            manager,
                        )
                    )
                    await asyncio.gather(c2, d2)
                    db_exists_2 = local_store.get_management_account(SOURCE_TELEGRAM, "main") is not None
                    runtime_exists_2 = "main" in {s["account_id"] for s in manager.statuses()}
                    self.assertEqual(db_exists_2, runtime_exists_2)
                finally:
                    release.set()
                    await manager.stop()
            local_store.close()

    async def test_global_stop_rejects_concurrent_registration_and_waits_for_all_runtimes(self) -> None:
        import threading
        from unittest import mock

        from tele_mess_core.config import (
            AppConfig,
            LoggingConfig,
            ServerConfig,
            StorageConfig,
            TelegramConfig,
        )
        from tele_mess_core.models import AccountRecord
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "stop_rejects_register.db"
            local_store = ArchiveStore(db_path)
            local_store.initialize()
            local_store.upsert_account(AccountRecord(SOURCE_TELEGRAM, "main", "Main", "user"))
            app_config = AppConfig(
                storage=StorageConfig(database=db_path),
                server=ServerConfig(),
                logging=LoggingConfig(),
                telegram=TelegramConfig(
                    accounts=[self.config],
                    media_download=MediaDownloadConfig(max_attempts=3, retry_delay_seconds=0.05, stale_seconds=60.0),
                ),
            )
            entered = threading.Event()
            release = threading.Event()
            created_clients: list[object] = []

            class TrackedClient:
                def __init__(self) -> None:
                    self._connected = False
                    created_clients.append(self)

                async def connect(self) -> None:
                    self._connected = True

                async def disconnect(self) -> None:
                    self._connected = False

                def is_connected(self) -> bool:
                    return self._connected

                async def is_user_authorized(self) -> bool:
                    return True

                async def get_me(self):
                    return SimpleNamespace(id=1, first_name="Main", last_name=None, username="main")

                def on(self, event_builder: object):
                    return lambda fn: fn

                def iter_messages(self, cid: int, **kwargs):
                    return FakeBackfillIterator(messages=[])

                async def get_messages(self, cid: int, ids: int | None = None, **kwargs):
                    return []

            def blocking_fs_scan(*args, **kwargs) -> bool:
                if kwargs.get("deleted_accounts_only"):
                    return False
                entered.set()
                release.wait(timeout=5.0)
                return False

            manager = TelegramRuntimeManager(
                app_config,
                local_store,
                client_factory=lambda _cfg: TrackedClient(),
                ingest=True,
            )
            with mock.patch.object(
                local_store,
                "cleanup_orphan_media_staging_dirs",
                side_effect=blocking_fs_scan,
            ):
                await manager.start()
                self.assertTrue(await asyncio.to_thread(entered.wait, 2.0))
                # Start unregistering old runtime so a concurrent _register_account suspends awaiting its stop task.
                unreg_task = asyncio.create_task(manager._unregister_account("main"))
                await asyncio.sleep(0.03)
                suspended_reg = asyncio.create_task(manager._register_account(self.config))
                await asyncio.sleep(0.03)
                self.assertFalse(suspended_reg.done())

                # Now start global stop() while old runtime is still stopping and suspended_reg is waiting.
                stop_task = asyncio.create_task(manager.stop())
                await asyncio.sleep(0.03)
                # Direct registration during global stop is rejected immediately.
                with self.assertRaises(RuntimeError):
                    await asyncio.to_thread(manager.register_account, self.config)

                release.set()
                await asyncio.wait_for(stop_task, timeout=3.0)
                await asyncio.gather(unreg_task, return_exceptions=True)
                with self.assertRaises(RuntimeError):
                    await suspended_reg

            self.assertEqual(len(created_clients), 1)
            self.assertEqual(manager.statuses(), [])
            self.assertEqual(manager._runtimes, {})
            self.assertEqual(manager._stopping_runtimes, {})
            local_store.close()


if __name__ == "__main__":
    unittest.main()
