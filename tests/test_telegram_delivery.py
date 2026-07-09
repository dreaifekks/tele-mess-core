from __future__ import annotations

import unittest

from tele_mess_core.telegram.delivery import split_telegram_message


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


if __name__ == "__main__":
    unittest.main()
