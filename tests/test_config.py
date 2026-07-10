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
  raw_json_retention_days: 10
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
        self.assertEqual(config.storage.raw_json_retention_days, 10)
        self.assertEqual(account.account_id, "main")
        self.assertFalse(hasattr(account, "chats"))
        self.assertFalse(config.server.allow_unauthenticated_localhost)

    def test_local_unauthenticated_server_opt_in_is_parsed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yml"
            config_path.write_text(
                """
telegram:
  api_id: 1
  api_hash: hash
server:
  host: 127.0.0.1
  token: ""
  allow_unauthenticated_localhost: true
""",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertTrue(config.server.allow_unauthenticated_localhost)

    def test_daily_packaging_config_is_parsed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yml"
            config_path.write_text(
                """
storage:
  database: ./archive.db
telegram:
  api_id: 1
  api_hash: hash
daily:
  output_dir: ./daily-output
  systemd_user_dir: ./systemd-user
  cli_path: ./bin/tele-mess-core
  ai:
    provider: disabled
    command: [python3, -c, pass]
    timeout_seconds: 12
  delivery:
    enabled: true
    account_id: main
    origin_id: -1001
    topic_id: 42
""",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.daily.output_dir, Path(tmp) / "daily-output")
        self.assertEqual(config.daily.systemd_user_dir, Path(tmp) / "systemd-user")
        self.assertEqual(config.daily.cli_path, "./bin/tele-mess-core")
        self.assertEqual(config.daily.ai.provider, "disabled")
        self.assertEqual(config.daily.ai.timeout_seconds, 12)
        self.assertTrue(config.daily.delivery.enabled)
        self.assertEqual(config.daily.delivery.account_id, "main")
        self.assertEqual(config.daily.delivery.origin_id, -1001)
        self.assertEqual(config.daily.delivery.topic_id, 42)

    def test_daily_delivery_requires_target_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yml"
            config_path.write_text(
                """
storage:
  database: ./archive.db
telegram:
  api_id: 1
  api_hash: hash
daily:
  delivery:
    enabled: true
    account_id: main
""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "daily.delivery.origin_id"):
                load_config(config_path)


if __name__ == "__main__":
    unittest.main()
