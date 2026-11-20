from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import Mock

from telethon.extensions import markdown as telethon_markdown
from telethon.tl.types import MessageEntityBold, MessageEntityPre, MessageEntityTextUrl

from tele_mess_core.config import DailyDeliveryConfig, TelegramAccountConfig
from tele_mess_core.telegram.delivery import (
    TelegramSummaryDeliveryService,
    split_telegram_markdown,
    telegram_markdown_for_send,
)


class TelegramDeliveryTest(unittest.TestCase):
    def test_split_telegram_markdown_uses_empty_marker(self) -> None:
        self.assertEqual(split_telegram_markdown(""), ["Daily summary is empty."])

    def test_telegram_markdown_converts_headings_but_preserves_tags_and_code(self) -> None:
        content = "# Daily Summary\n\n#point\n\n```md\n# code heading\n```"

        rendered = telegram_markdown_for_send(content)

        self.assertTrue(rendered.startswith("**Daily Summary**"))
        self.assertIn("\n#point\n", rendered)
        self.assertIn("```md\n# code heading\n```", rendered)

    def test_split_telegram_markdown_reopens_entities_across_chunks(self) -> None:
        cases = [
            ("bold", "**" + ("bold content " * 30) + "**", MessageEntityBold),
            (
                "link",
                "[" + ("linked content " * 30) + "](https://example.com)",
                MessageEntityTextUrl,
            ),
            (
                "fenced code",
                "```python\n" + ("# code heading\nprint('chunk')\n" * 15) + "```",
                MessageEntityPre,
            ),
        ]

        for name, content, entity_type in cases:
            with self.subTest(name=name):
                chunks = split_telegram_markdown(content, limit=100)

                self.assertGreater(len(chunks), 1)
                for chunk in chunks:
                    parsed, entities = telethon_markdown.parse(chunk)
                    self.assertNotIn("**", parsed)
                    self.assertNotIn("```", parsed)
                    self.assertTrue(any(isinstance(entity, entity_type) for entity in entities))

    def test_split_telegram_markdown_counts_astral_characters_as_utf16(self) -> None:
        chunks = split_telegram_markdown("😀" * 120, limit=100)

        self.assertEqual(len(chunks), 3)
        for chunk in chunks:
            parsed, _ = telethon_markdown.parse(chunk)
            self.assertLessEqual(len(parsed.encode("utf-16-le")) // 2, 100)


class TelegramSummaryDeliveryServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_send_summary_uses_markdown_parse_mode(self) -> None:
        class SentMessage:
            id = 42

        class FakeClient:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            async def is_user_authorized(self) -> bool:
                return True

            async def get_entity(self, origin_id: int) -> str:
                return f"entity:{origin_id}"

            async def send_message(self, entity: object, body: str, **kwargs: object) -> SentMessage:
                self.calls.append({"entity": entity, "body": body, **kwargs})
                return SentMessage()

        store = Mock()
        account = TelegramAccountConfig(
            account_id="main",
            api_id=1,
            api_hash="test",
            session_name="main",
            session_dir=Path("/tmp/tele-mess-core-test-sessions"),
        )
        client = FakeClient()
        service = TelegramSummaryDeliveryService(account, store)

        result = await service.send_chunk(
            DailyDeliveryConfig(enabled=True, account_id="main", origin_id=-1001),
            "# Important Summary\n\n**Bold** and `code`\n\n#point",
            client=client,
        )

        self.assertEqual(result["status"], "sent")
        self.assertEqual(result["message_ids"], [42])
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0]["parse_mode"], "md")
        self.assertTrue(str(client.calls[0]["body"]).startswith("**Important Summary**"))
        self.assertIn("#point", str(client.calls[0]["body"]))

    async def test_send_chunk_preserves_markdown_on_split_chunks(self) -> None:
        class SentMessage:
            id = 42

        class FakeClient:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            async def is_user_authorized(self) -> bool:
                return True

            async def get_entity(self, origin_id: int) -> str:
                return f"entity:{origin_id}"

            async def send_message(self, entity: object, body: str, **kwargs: object) -> SentMessage:
                self.calls.append({"entity": entity, "body": body, **kwargs})
                return SentMessage()

        store = Mock()
        account = TelegramAccountConfig(
            account_id="main",
            api_id=1,
            api_hash="test",
            session_name="main",
            session_dir=Path("/tmp/tele-mess-core-test-sessions"),
        )
        client = FakeClient()
        service = TelegramSummaryDeliveryService(account, store)
        content = "**" + ("formatted content " * 300) + "**"
        chunks = split_telegram_markdown(content, limit=100)

        for chunk in chunks:
            result = await service.send_chunk(
                DailyDeliveryConfig(enabled=True, account_id="main", origin_id=-1001),
                chunk,
                client=client,
            )
            self.assertEqual(result["message_count"], 1)

        self.assertGreater(len(chunks), 1)
        self.assertEqual(len(client.calls), len(chunks))
        self.assertTrue(all(call["parse_mode"] == "md" for call in client.calls))
        for call in client.calls:
            parsed, entities = telethon_markdown.parse(str(call["body"]))
            self.assertNotIn("**", parsed)
            self.assertTrue(any(isinstance(entity, MessageEntityBold) for entity in entities))

    async def test_send_chunk_passes_persisted_random_id_via_send_message_request(self) -> None:
        from telethon.tl.functions.messages import SendMessageRequest

        class SentMessage:
            id = 91

        class TelethonStyleClient:
            def __init__(self) -> None:
                self.requests: list[object] = []

            async def is_user_authorized(self) -> bool:
                return True

            async def get_entity(self, origin_id: int) -> str:
                return f"entity:{origin_id}"

            async def get_input_entity(self, entity: object) -> str:
                return f"input:{entity}"

            async def __call__(self, request: object) -> object:
                self.requests.append(request)
                return {"raw": True}

            def _get_response_message(self, request: object, response: object, entity: object) -> SentMessage:
                return SentMessage()

        store = Mock()
        account = TelegramAccountConfig(
            account_id="main",
            api_id=1,
            api_hash="test",
            session_name="main",
            session_dir=Path("/tmp/tele-mess-core-test-sessions"),
        )
        client = TelethonStyleClient()
        service = TelegramSummaryDeliveryService(account, store)

        result = await service.send_chunk(
            DailyDeliveryConfig(enabled=True, account_id="main", origin_id=-1001, topic_id=7),
            "# Heading\n\n**Bold** body",
            client=client,
            random_id=1234567890123456789,
        )

        self.assertEqual(result["status"], "sent")
        self.assertEqual(result["message_ids"], [91])
        self.assertEqual(len(client.requests), 1)
        request = client.requests[0]
        self.assertIsInstance(request, SendMessageRequest)
        assert isinstance(request, SendMessageRequest)
        self.assertEqual(request.random_id, 1234567890123456789)
        self.assertEqual(request.peer, "input:entity:-1001")
        self.assertIsNotNone(request.reply_to)
        self.assertIn("Bold", request.message)


if __name__ == "__main__":
    unittest.main()
