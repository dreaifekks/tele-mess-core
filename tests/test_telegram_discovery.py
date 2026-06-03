from __future__ import annotations

import tempfile
import unittest
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
            is_user=False,
            is_group=True,
            is_channel=False,
            entity=FakeEntity(id=-1001, title="Source Group", username="source", megagroup=True, forum=True),
        )
        yield SimpleNamespace(
            id=-1002,
            title="News Channel",
            is_user=False,
            is_group=False,
            is_channel=True,
            entity=FakeEntity(id=-1002, title="News Channel", username="news", broadcast=True),
        )

    async def iter_participants(self, origin_id, limit=None):
        yield FakeEntity(id=7, username="alice", first_name="Alice", last_name="", bot=False)

    async def __call__(self, request):
        self.topic_calls += 1
        if self.topic_calls == 1:
            return SimpleNamespace(
                count=2,
                topics=[
                    FakeEntity(id=10, title="Topic One", top_message=100),
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
        self.assertEqual(result["origins"], 2)
        self.assertEqual(result["topics"], 2)
        origins = self.store.list_origins(account_id="main")
        self.assertEqual({item["origin_type"] for item in origins}, {"group", "channel", "topic"})
        self.assertTrue(next(item for item in origins if item["origin_id"] == -1001 and item["topic_id"] == 0)["is_forum"])
        self.assertEqual(len([item for item in origins if item["origin_type"] == "topic"]), 2)

        refresh = await self.service.refresh_participants(-1001)
        self.assertEqual(refresh["participants"], 1)
        participants = self.store.list_participants(account_id="main", origin_id=-1001)
        self.assertEqual(participants[0]["username"], "alice")


if __name__ == "__main__":
    unittest.main()
