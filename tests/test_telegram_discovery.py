from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import TelegramAccountConfig
from tele_mess_core.telegram.discovery import TelegramDiscoveryService


class FakeEntity:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def to_dict(self):
        return dict(self.__dict__)


class ChatAdminRequiredError(Exception):
    pass


class FakeClient:
    def __init__(self):
        self.disconnected = False
        self.topic_calls = 0

    async def is_user_authorized(self):
        return True

    async def iter_dialogs(self):
        yield SimpleNamespace(
            id=-1001,
            title="Source Group",
            date=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
            is_user=False,
            is_group=True,
            is_channel=False,
            entity=FakeEntity(id=-1001, title="Source Group", username="source", megagroup=True, forum=True),
        )
        yield SimpleNamespace(
            id=-1002,
            title="News Channel",
            message=SimpleNamespace(date=datetime(2026, 1, 3, 3, 4, 5, tzinfo=timezone.utc)),
            is_user=False,
            is_group=False,
            is_channel=True,
            entity=FakeEntity(id=-1002, title="News Channel", username="news", broadcast=True),
        )
        yield SimpleNamespace(
            id=1003,
            title="Alice",
            is_user=True,
            is_group=False,
            is_channel=False,
            entity=FakeEntity(id=1003, first_name="Alice", username="alice"),
        )

    async def iter_participants(self, origin_id, limit=None):
        yield FakeEntity(id=7, username="alice", first_name="Alice", last_name="", bot=False)

    async def __call__(self, request):
        self.topic_calls += 1
        if self.topic_calls == 1:
            return SimpleNamespace(
                count=2,
                topics=[
                    FakeEntity(id=10, title="Topic One", top_message=100, date=datetime(2026, 1, 4, 3, 4, 5, tzinfo=timezone.utc)),
                    FakeEntity(id=11, title="Topic Two", top_message=101),
                ],
            )
        return SimpleNamespace(count=2, topics=[])

    async def disconnect(self):
        self.disconnected = True


class FakeDiscoveryService(TelegramDiscoveryService):
    def __init__(self, config, store):
        super().__init__(config, store)
        self.fake_client = FakeClient()

    async def _connected_client(self):
        return self.fake_client

    def _forum_topics_request(self, functions, entity, offset_id, offset_topic, limit):
        return SimpleNamespace(offset_id=offset_id, offset_topic=offset_topic, limit=limit)


class FailingParticipantClient(FakeClient):
    async def iter_participants(self, origin_id, limit=None):
        raise ChatAdminRequiredError("admin required")
        yield


class TelegramDiscoveryTest(unittest.IsolatedAsyncioTestCase):
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
        self.service = FakeDiscoveryService(self.config, self.store)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    async def test_discover_origins_and_refresh_participants(self) -> None:
        result = await self.service.discover_origins(include_topics=True, topic_limit=10)
        self.assertTrue(result["authorized"])
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["origins"], 2)
        self.assertEqual(result["topics"], 2)
        self.assertEqual(result["private_skipped"], 1)
        origins = self.store.list_origins(account_id="main")
        self.assertEqual({item["origin_type"] for item in origins}, {"group", "channel", "topic"})
        group = next(item for item in origins if item["origin_id"] == -1001 and item["topic_id"] == 0)
        self.assertTrue(group["is_forum"])
        self.assertEqual(group["last_message_at"], "2026-01-02T03:04:05+00:00")
        self.assertEqual(next(item for item in origins if item["origin_id"] == -1002)["last_message_at"], "2026-01-03T03:04:05+00:00")
        topics = [item for item in origins if item["origin_type"] == "topic"]
        self.assertEqual(len(topics), 2)
        self.assertEqual(next(item for item in topics if item["topic_id"] == 10)["last_message_at"], "2026-01-04T03:04:05+00:00")

        refresh = await self.service.refresh_participants(-1001)
        self.assertEqual(refresh["participants"], 1)
        self.assertEqual(refresh["status"], "ok")
        participants = self.store.list_participants(account_id="main", origin_id=-1001)
        self.assertEqual(participants[0]["username"], "alice")

    async def test_discover_topics_respects_limit_and_reports_truncation(self) -> None:
        result = await self.service.discover_origins(include_topics=True, topic_limit=1)

        self.assertEqual(result["topics"], 1)
        self.assertTrue(result["topics_truncated"])
        topics = [item for item in self.store.list_origins(account_id="main") if item["origin_type"] == "topic"]
        self.assertEqual(len(topics), 1)

    async def test_discover_origins_can_include_private_when_explicit(self) -> None:
        result = await self.service.discover_origins(include_topics=False, include_private=True)

        self.assertEqual(result["origins"], 3)
        self.assertEqual(result["private_skipped"], 0)
        origins = self.store.list_origins(account_id="main")
        self.assertIn("private", {item["origin_type"] for item in origins})

    async def test_refresh_participants_records_access_error(self) -> None:
        self.service.fake_client = FailingParticipantClient()

        result = await self.service.refresh_participants(-1001)

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["participants"], 0)
        self.assertEqual(result["errors"][0]["code"], "access_denied")
        events = self.store.list_operation_events(account_id="main", status="failed")
        self.assertEqual(events[0]["operation"], "refresh_participants")
        self.assertEqual(events[0]["error_code"], "access_denied")

    async def test_forum_topics_request_uses_messages_namespace(self) -> None:
        class Request:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        functions = SimpleNamespace(messages=SimpleNamespace(GetForumTopicsRequest=Request))
        entity = FakeEntity(id=-1001)

        request = TelegramDiscoveryService(self.config, self.store)._forum_topics_request(
            functions,
            entity,
            offset_id=12,
            offset_topic=34,
            limit=56,
        )

        self.assertIs(request.kwargs["peer"], entity)
        self.assertEqual(request.kwargs["offset_id"], 12)
        self.assertEqual(request.kwargs["offset_topic"], 34)
        self.assertEqual(request.kwargs["limit"], 56)


if __name__ == "__main__":
    unittest.main()
