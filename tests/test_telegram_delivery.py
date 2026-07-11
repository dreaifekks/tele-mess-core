from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import Mock

from tele_mess_core.config import DailyDeliveryConfig, TelegramAccountConfig
from tele_mess_core.telegram.delivery import (
    TelegramSummaryDeliveryService,
    split_telegram_message,
    telegram_markdown_for_send,
)


class TelegramDeliveryTest(unittest.TestCase):
    def test_split_telegram_message_prefers_paragraph_boundaries(self) -> None:
        text = "first paragraph\n\n" + ("second " * 80) + "\n\nthird"

        chunks = split_telegram_message(text, limit=140)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(chunks[0].startswith("first paragraph"))
        self.assertTrue(chunks[-1].endswith("third"))
        self.assertTrue(all(len(chunk) <= 140 for chunk in chunks))

    def test_split_telegram_message_uses_empty_marker(self) -> None:
        self.assertEqual(split_telegram_message(""), ["Daily summary is empty."])

    def test_telegram_markdown_converts_headings_but_preserves_tags_and_code(self) -> None:
        content = "# Daily Summary\n\n#point\n\n```md\n# code heading\n```"

        rendered = telegram_markdown_for_send(content)

        self.assertTrue(rendered.startswith("**Daily Summary**"))
        self.assertIn("\n#point\n", rendered)
        self.assertIn("```md\n# code heading\n```", rendered)


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

        result = await service.send_summary(
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


if __name__ == "__main__":
    unittest.main()
