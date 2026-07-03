from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tele_mess_core.config import load_config


class ConfigTest(unittest.TestCase):
    def test_telegram_chats_are_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yml"
            config_path.write_text(
                """
storage:
  database: ./archive.db
telegram:
  accounts:
    - account_id: main
      api_id: 1
      api_hash: hash
      session_name: main
      chats:
        - id: -1001
          name: Legacy Chat
server:
  token: secret
""",
                encoding="utf-8",
            )

            config = load_config(config_path)

        account = config.telegram.accounts[0]
        self.assertEqual(account.account_id, "main")
        self.assertFalse(hasattr(account, "chats"))


if __name__ == "__main__":
    unittest.main()
