from __future__ import annotations

import json
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.cli import main
from tele_mess_core.config import AppConfig, DailyAiConfig, DailyPackagingConfig, LoggingConfig, ServerConfig, StorageConfig, TelegramConfig
from tele_mess_core.daily import build_daily_package, run_daily_summary, update_daily_package_schedule
from tele_mess_core.models import BackupPolicyRecord, MediaFileRecord, MessageRecord, OriginRecord, SOURCE_TELEGRAM, utc_now_iso


class DailyPackagingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = ArchiveStore(self.root / "archive.db")
        self.store.initialize()
        self.config = AppConfig(
            storage=StorageConfig(data_dir=self.root, database=self.root / "archive.db"),
            telegram=TelegramConfig(),
            server=ServerConfig(),
            logging=LoggingConfig(file=None),
            daily=DailyPackagingConfig(
                output_dir=self.root / "daily-packages",
                systemd_user_dir=self.root / "systemd-user",
                ai=DailyAiConfig(provider="disabled"),
            ),
            config_path=self.root / "config.yml",
        )

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_tag_groups_assign_specific_groups_first_and_exclude_important_origins(self) -> None:
        self._origin(-1001, "Web3 Broad", "web3,info")
        self._origin(-1002, "Web3 Infra", "web3,it,info")
        self._origin(-1003, "AI Broad", "ai,info")
        self._origin(-1004, "Important AI", "ai,info", important=True)
        for chat_id, text in (
            (-1001, "web3 broad payload"),
            (-1002, "web3 infra payload"),
            (-1003, "ai broad payload"),
            (-1004, "important ai payload"),
        ):
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=chat_id,
                    message_id=1,
                    sent_at="2026-07-02T15:30:00+00:00",
                    ingested_at=utc_now_iso(),
                    text=text,
                ),
                event_type="new",
            )
        media_path = self.root / "important.png"
        media_path.write_bytes(b"fake-png")
        self.store.upsert_media_file(
            MediaFileRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-1004,
                message_id=1,
                file_path=str(media_path),
                media_kind="photo",
                mime_type="image/png",
                file_size=8,
            )
        )
        pdf_path = self.root / "important.pdf"
        pdf_path.write_bytes(b"fake-pdf")
        self.store.upsert_media_file(
            MediaFileRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-1004,
                message_id=1,
                file_index=1,
                file_path=str(pdf_path),
                media_kind="document",
                mime_type="application/pdf",
                file_size=8,
            )
        )

        package = build_daily_package(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="Asia/Tokyo",
            scope={"account_id": "main", "tag_groups": ["web3 info", "web3 it info", "ai info"]},
        )

        self.assertEqual(package["status"], "completed")
        payload = json.loads(Path(package["package_json_path"]).read_text(encoding="utf-8"))
        groups = {group["name"]: group for group in payload["normal_groups"]}
        self.assertEqual([origin["origin"]["origin_id"] for origin in groups["web3 it info"]["origins"]], [-1002])
        self.assertEqual([origin["origin"]["origin_id"] for origin in groups["web3 info"]["origins"]], [-1001])
        self.assertEqual([origin["origin"]["origin_id"] for origin in groups["ai info"]["origins"]], [-1003])
        self.assertEqual([origin["origin"]["origin_id"] for origin in payload["important_origins"]], [-1004])
        self.assertEqual(payload["stats"]["message_count"], 4)
        self.assertEqual(payload["stats"]["media_count"], 2)

        summary = run_daily_summary(self.store, self.config, package_run_id=package["run_id"])
        self.assertEqual(summary["status"], "completed")
        self.assertIn("AI provider is disabled", Path(summary["summary_path"]).read_text(encoding="utf-8"))
        records = self.store.list_daily_summary_records(package_run_id=package["run_id"], tags=["ai", "info"], important=True)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["summary_id"], summary["run_id"])
        self.assertIn("AI provider is disabled", records[0]["content_preview"])
        record = self.store.get_daily_summary_record(run_id=summary["run_id"])
        assert record is not None
        self.assertIn("AI provider is disabled", record["content_md"])
        self.assertEqual(record["tags"], ["web3", "info", "it", "ai"])

        fake_provider = self.root / "fake_provider.py"
        fake_log = self.root / "fake_provider_calls.jsonl"
        fake_provider.write_text(
            "from pathlib import Path\n"
            "import json\n"
            "import sys\n"
            "prompt = sys.stdin.read()\n"
            "output = Path(sys.argv[1])\n"
            "task = sys.argv[2]\n"
            "log_path = Path(sys.argv[3])\n"
            "extra = sys.argv[4:]\n"
            "output.write_text(f'# Fake {task}\\n' + '\\n'.join(extra), encoding='utf-8')\n"
            "with log_path.open('a', encoding='utf-8') as handle:\n"
            "    handle.write(json.dumps({'task': task, 'args': extra, 'prompt': prompt[:500]}, ensure_ascii=False) + '\\n')\n",
            encoding="utf-8",
        )
        fake_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                systemd_user_dir=self.config.daily.systemd_user_dir,
                ai=DailyAiConfig(
                    provider="fake",
                    command=[sys.executable, str(fake_provider), "{output}", "{task}", str(fake_log), "{images}"],
                ),
            ),
            config_path=self.config.config_path,
        )
        fake_summary = run_daily_summary(self.store, fake_config, package_run_id=package["run_id"])
        fake_text = Path(fake_summary["summary_path"]).read_text(encoding="utf-8")
        self.assertIn("# Fake final_daily_summary", fake_text)
        calls = [json.loads(line) for line in fake_log.read_text(encoding="utf-8").splitlines()]
        tasks = [call["task"] for call in calls]
        self.assertIn("media_image_analysis", tasks)
        self.assertIn("normal_origin_key_extraction", tasks)
        self.assertIn("normal_group_analysis", tasks)
        self.assertIn("important_origin_analysis", tasks)
        self.assertIn("final_daily_summary", tasks)
        media_call = next(call for call in calls if call["task"] == "media_image_analysis")
        self.assertIn("--image", media_call["args"])
        self.assertIn(str(media_path), media_call["args"])
        summary_payload = json.loads((Path(fake_summary["output_dir"]) / "summary.json").read_text(encoding="utf-8"))
        self.assertTrue(any(item["task"] == "media_file_reference" for item in summary_payload["analysis"]["media"]))
        self.assertIn(str(pdf_path), json.dumps(summary_payload["analysis"], ensure_ascii=False))
        fake_record = self.store.get_daily_summary_record(run_id=fake_summary["run_id"])
        assert fake_record is not None
        self.assertEqual(fake_record["content_json"]["analysis"]["final"]["task"], "final_daily_summary")

    def test_schedule_update_writes_systemd_user_timer_files(self) -> None:
        schedule = update_daily_package_schedule(
            self.store,
            self.config,
            {
                "enabled": True,
                "time_of_day": "09:15",
                "timezone": "UTC",
                "scope": {"tag_groups": ["ai info"]},
            },
        )

        self.assertTrue(schedule["installed"])
        self.assertEqual(schedule["last_error"], None)
        timer = self.root / "systemd-user" / "tele-mess-core-daily-package.timer"
        service = self.root / "systemd-user" / "tele-mess-core-daily-package.service"
        self.assertIn("OnCalendar=*-*-* 09:15:00 UTC", timer.read_text(encoding="utf-8"))
        self.assertIn("daily-package", service.read_text(encoding="utf-8"))

    def test_cli_daily_package_and_summary_commands_run(self) -> None:
        self._origin(-3001, "CLI Daily", "cli,info")
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-3001,
                message_id=1,
                sent_at="2026-07-03T01:00:00+00:00",
                ingested_at=utc_now_iso(),
                text="cli daily payload",
            ),
            event_type="new",
        )
        config_path = self.root / "config.yml"
        config_path.write_text(
            f"""
storage:
  data_dir: "{self.root}"
  database: "{self.root / "archive.db"}"
telegram:
  accounts:
    - account_id: main
      api_id: 1
      api_hash: hash
      session_name: main
logging:
  file: ""
daily:
  output_dir: "{self.root / "daily-packages"}"
  systemd_user_dir: "{self.root / "systemd-user"}"
  ai:
    provider: disabled
""",
            encoding="utf-8",
        )

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = main(["--config", str(config_path), "daily-package", "--date", "2026-07-03", "--timezone", "UTC", "--account-id", "main"])
        self.assertEqual(code, 0)
        package = json.loads(stdout.getvalue())
        self.assertEqual(package["status"], "completed")

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = main(["--config", str(config_path), "daily-summary", "--package-run-id", package["run_id"]])
        self.assertEqual(code, 0)
        summary = json.loads(stdout.getvalue())
        self.assertEqual(summary["status"], "completed")

    def _origin(self, origin_id: int, title: str, tags: str, important: bool = False) -> None:
        self.store.upsert_origin(
            OriginRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=origin_id,
                origin_type="group",
                title=title,
                important=important,
            )
        )
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=origin_id,
                enabled=True,
                tags=tags,
            )
        )


if __name__ == "__main__":
    unittest.main()
