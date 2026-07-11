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
    model: gpt-5.6-terra
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
        self.assertEqual(config.daily.ai.model, "gpt-5.6-terra")
        self.assertEqual(config.daily.ai.timeout_seconds, 12)
        self.assertTrue(config.daily.delivery.enabled)
        self.assertEqual(config.daily.delivery.account_id, "main")
        self.assertEqual(config.daily.delivery.origin_id, -1001)
        self.assertEqual(config.daily.delivery.topic_id, 42)

    def test_daily_ai_defaults_to_gpt_5_6_sol_with_structured_output_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yml"
            config_path.write_text(
                """
storage:
  database: ./archive.db
telegram:
  api_id: 1
  api_hash: hash
""",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.daily.ai.provider, "codex-cli")
        self.assertEqual(config.daily.ai.model, "gpt-5.6-sol")
        self.assertIn("{model}", config.daily.ai.command)
        self.assertIn("{output_schema}", config.daily.ai.command)
        self.assertFalse(config.daily.ai.fallback.enabled)
        self.assertEqual(config.daily.ai.fallback.provider, "openai-compatible")
        self.assertEqual(config.daily.ai.fallback.trigger, "usage-limit")
        self.assertIsNone(config.daily.ai.fallback.api_key_file)
        self.assertEqual(config.daily.ai.fallback.retry_delay_seconds, 1200)
        self.assertEqual(config.daily.ai.fallback.max_retries, 1)

    def test_daily_ai_fallback_is_parsed_with_relative_api_key_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            key_path = Path(tmp) / ".secrets" / "fallback-api-key"
            key_path.parent.mkdir(parents=True)
            key_path.write_text("test-token-placeholder\n", encoding="utf-8")
            config_path = Path(tmp) / "config.yml"
            config_path.write_text(
                """
storage:
  database: ./archive.db
telegram:
  api_id: 1
  api_hash: hash
daily:
  ai:
    fallback:
      enabled: true
      provider: openai-compatible
      trigger: usage-limit
      base_url: https://gateway.example/v1/
      model: deepseek-v4-flash
      api_key_file: ./.secrets/fallback-api-key
      retry_delay_seconds: 1200
      max_retries: 1
      supports_images: true
      supports_json_schema: true
""",
                encoding="utf-8",
            )

            config = load_config(config_path)

        fallback = config.daily.ai.fallback
        self.assertTrue(fallback.enabled)
        self.assertEqual(fallback.provider, "openai-compatible")
        self.assertEqual(fallback.trigger, "usage-limit")
        self.assertEqual(fallback.base_url, "https://gateway.example/v1")
        self.assertEqual(fallback.model, "deepseek-v4-flash")
        self.assertEqual(fallback.api_key_file, key_path)
        self.assertEqual(fallback.retry_delay_seconds, 1200)
        self.assertEqual(fallback.max_retries, 1)
        self.assertTrue(fallback.supports_images)
        self.assertTrue(fallback.supports_json_schema)

    def test_daily_ai_fallback_rejects_invalid_settings(self) -> None:
        cases = (
            (
                "provider",
                "not-openai-compatible",
                "usage-limit",
                "https://gateway.example/v1",
                1200,
                1,
                "daily.ai.fallback.provider",
            ),
            (
                "trigger",
                "openai-compatible",
                "any-error",
                "https://gateway.example/v1",
                1200,
                1,
                "daily.ai.fallback.trigger",
            ),
            (
                "url",
                "openai-compatible",
                "usage-limit",
                "http://gateway.example/v1",
                1200,
                1,
                "daily.ai.fallback.base_url",
            ),
            (
                "retry delay",
                "openai-compatible",
                "usage-limit",
                "https://gateway.example/v1",
                -1,
                1,
                "daily.ai.fallback.retry_delay_seconds",
            ),
            (
                "retry count",
                "openai-compatible",
                "usage-limit",
                "https://gateway.example/v1",
                1200,
                2,
                "daily.ai.fallback.max_retries",
            ),
        )
        for name, provider, trigger, base_url, retry_delay, max_retries, error in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                key_path = Path(tmp) / ".secrets" / "fallback-api-key"
                key_path.parent.mkdir(parents=True)
                key_path.write_text("test-token-placeholder\n", encoding="utf-8")
                config_path = Path(tmp) / "config.yml"
                config_path.write_text(
                    f"""
storage:
  database: ./archive.db
telegram:
  api_id: 1
  api_hash: hash
daily:
  ai:
    fallback:
      enabled: true
      provider: {provider}
      trigger: {trigger}
      base_url: {base_url}
      model: deepseek-v4-flash
      api_key_file: ./.secrets/fallback-api-key
      retry_delay_seconds: {retry_delay}
      max_retries: {max_retries}
""",
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(ValueError, error):
                    load_config(config_path)

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
