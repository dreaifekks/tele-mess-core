from __future__ import annotations

import json
import io
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.cli import main
from tele_mess_core.config import AppConfig, DailyAiConfig, DailyPackagingConfig, LoggingConfig, ServerConfig, StorageConfig, TelegramConfig
from tele_mess_core.daily import build_daily_package, cancel_daily_summary_job, run_daily_summary, start_daily_summary_job, update_daily_package_schedule
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
        self.assertEqual(package["progress_current"], package["progress_total"])
        self.assertEqual(package["progress_label"], "completed")
        self.assertEqual(package["progress"]["normal_group_count"], 3)
        self.assertEqual(package["progress"]["important_origin_count"], 1)
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
        self.assertEqual(summary["progress_current"], summary["progress_total"])
        self.assertEqual(summary["progress_label"], "completed")
        self.assertEqual(summary["progress"]["normal_group_count"], 3)
        self.assertEqual(summary["progress"]["important_origin_count"], 1)
        self.assertIn("AI provider is disabled", Path(summary["summary_path"]).read_text(encoding="utf-8"))
        records = self.store.list_daily_summary_records(package_run_id=package["run_id"], tags=["ai", "info"], important=True)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["run_id"], summary["run_id"])
        self.assertIn("--important--", records[0]["summary_id"])
        self.assertIn("AI provider is disabled", records[0]["content_preview"])
        record = self.store.get_daily_summary_record(run_id=summary["run_id"])
        assert record is not None
        self.assertIn("AI provider is disabled", record["content_md"])
        self.assertTrue(set(record["tags"]).issubset({"web3", "info", "it", "ai"}))
        all_records = self.store.list_daily_summary_records(package_run_id=package["run_id"])
        self.assertEqual(len(all_records), 4)
        self.assertEqual(
            {tuple(item["tags"]) for item in all_records},
            {("web3", "it", "info"), ("web3", "info"), ("ai", "info")},
        )

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
        self.assertNotIn("media_image_analysis", tasks)
        self.assertIn("normal_origin_key_extraction", tasks)
        self.assertIn("normal_group_analysis", tasks)
        self.assertIn("important_origin_analysis", tasks)
        self.assertIn("final_daily_summary", tasks)
        important_call = next(call for call in calls if call["task"] == "important_origin_analysis")
        self.assertIn("--image", important_call["args"])
        self.assertIn(str(media_path), important_call["args"])
        summary_payload = json.loads((Path(fake_summary["output_dir"]) / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual(summary_payload["analysis"]["media"], [])
        important_prompt_path = Path(summary_payload["analysis"]["important_origins"][0]["prompt_path"])
        self.assertIn(str(pdf_path), important_prompt_path.read_text(encoding="utf-8"))
        fake_record = self.store.get_daily_summary_record(run_id=fake_summary["run_id"])
        assert fake_record is not None
        self.assertIn(fake_record["content_json"]["record_type"], {"tag_group", "important_origin"})

    def test_default_daily_package_groups_by_origin_tag_sets(self) -> None:
        self._origin(-5001, "Web3 One", "web3,info")
        self._origin(-5002, "Web3 Two", "web3,info")
        self._origin(-5003, "AI", "ai,info")
        for chat_id, text in (
            (-5001, "web3 one"),
            (-5002, "web3 two"),
            (-5003, "ai one"),
        ):
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=chat_id,
                    message_id=1,
                    sent_at="2026-07-03T01:00:00+00:00",
                    ingested_at=utc_now_iso(),
                    text=text,
                ),
                event_type="new",
            )

        package = build_daily_package(self.store, self.config, run_date="2026-07-03", timezone_name="UTC", scope={"account_id": "main"})
        payload = json.loads(Path(package["package_json_path"]).read_text(encoding="utf-8"))
        groups = {group["name"]: group for group in payload["normal_groups"]}
        self.assertTrue(payload["auto_tag_groups"])
        self.assertEqual(set(groups), {"web3,info", "ai,info"})
        self.assertEqual(groups["web3,info"]["origin_count"], 2)
        self.assertEqual(groups["ai,info"]["origin_count"], 1)

        summary = run_daily_summary(self.store, self.config, package_run_id=package["run_id"])
        records = self.store.list_daily_summary_records(package_run_id=package["run_id"])
        self.assertEqual(len(records), 2)
        self.assertEqual({tuple(item["tags"]) for item in records}, {("web3", "info"), ("ai", "info")})
        self.assertEqual({item["tags_csv"] for item in records}, {"web3,info", "ai,info"})
        group_prompt = Path(summary["output_dir"]) / "stages" / "normal-groups" / "web3-info.prompt.md"
        group_prompt_text = group_prompt.read_text(encoding="utf-8")
        self.assertIn("Tag-specific instruction for `info`", group_prompt_text)
        self.assertIn("不要把输入消息机械地逐条重排成消息列表", group_prompt_text)
        self.assertIn("### 主题标题 ([起始消息](telegram_deeplink 或 source_ref))", group_prompt_text)
        self.assertIn("不要把网页版 `https://t.me/...` 当作首选链接", group_prompt_text)

    def test_daily_package_adds_telegram_deeplinks_for_message_links(self) -> None:
        self._origin(-8001, "Public Link", "info")
        self._origin(-1001234567890, "Private Link", "info")
        for chat_id, message_id, permalink in (
            (-8001, 42, "https://t.me/example_channel/42"),
            (-1001234567890, 99, "https://t.me/c/1234567890/99"),
        ):
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=chat_id,
                    message_id=message_id,
                    sent_at="2026-07-03T01:00:00+00:00",
                    ingested_at=utc_now_iso(),
                    text=f"linked message {message_id}",
                    permalink=permalink,
                ),
                event_type="new",
            )

        package = build_daily_package(self.store, self.config, run_date="2026-07-03", timezone_name="UTC", scope={"account_id": "main"})
        payload = json.loads(Path(package["package_json_path"]).read_text(encoding="utf-8"))
        messages = {
            message["message_id"]: message
            for group in payload["normal_groups"]
            for origin in group["origins"]
            for message in origin["messages"]
        }

        self.assertEqual(messages[42]["telegram_deeplink"], "tg://resolve?domain=example_channel&post=42")
        self.assertEqual(messages[99]["telegram_deeplink"], "tg://privatepost?channel=1234567890&post=99")

    def test_important_origin_prompt_includes_all_messages_past_normal_limit(self) -> None:
        self._origin(-7001, "Important Full", "trade,info", important=True)
        for index in range(205):
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=-7001,
                    message_id=index + 1,
                    sent_at=f"2026-07-03T01:{index % 60:02d}:00+00:00",
                    ingested_at=utc_now_iso(),
                    text=f"important payload {index + 1}",
                ),
                event_type="new",
            )

        package = build_daily_package(self.store, self.config, run_date="2026-07-03", timezone_name="UTC", scope={"account_id": "main"})
        summary = run_daily_summary(self.store, self.config, package_run_id=package["run_id"])

        prompt_files = list((Path(summary["output_dir"]) / "stages" / "important-origins").glob("*.prompt.md"))
        self.assertEqual(len(prompt_files), 1)
        prompt_text = prompt_files[0].read_text(encoding="utf-8")
        self.assertIn("important origin 永远按全量消息处理", prompt_text)
        self.assertIn('"message_count": 205', prompt_text)
        self.assertIn('"truncated_message_count": 0', prompt_text)
        self.assertIn("important payload 205", prompt_text)
        self.assertIn("Segment Importance Scan", prompt_text)
        self.assertIn("但最终输出要按话题/事件/决策聚合", prompt_text)
        self.assertIn("## Important Topic / Event Summary", prompt_text)

    def test_daily_package_skips_origins_without_messages_in_window(self) -> None:
        self._origin(-6001, "Active Web3", "web3,info")
        self._origin(-6002, "Silent Web3", "web3,info")
        self._origin(-6003, "Silent Important", "trade,info", important=True)
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-6001,
                message_id=1,
                sent_at="2026-07-03T01:00:00+00:00",
                ingested_at=utc_now_iso(),
                text="active payload",
            ),
            event_type="new",
        )
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-6002,
                message_id=1,
                sent_at="2026-07-01T01:00:00+00:00",
                ingested_at=utc_now_iso(),
                text="old payload",
            ),
            event_type="new",
        )

        package = build_daily_package(self.store, self.config, run_date="2026-07-03", timezone_name="UTC", scope={"account_id": "main"})

        payload = json.loads(Path(package["package_json_path"]).read_text(encoding="utf-8"))
        self.assertEqual(payload["stats"]["origin_count"], 1)
        self.assertEqual(payload["stats"]["message_count"], 1)
        self.assertEqual(payload["important_origins"], [])
        groups = {group["name"]: group for group in payload["normal_groups"]}
        self.assertEqual(list(groups), ["web3,info"])
        self.assertEqual([origin["origin"]["origin_id"] for origin in groups["web3,info"]["origins"]], [-6001])

        summary = run_daily_summary(self.store, self.config, package_run_id=package["run_id"])
        self.assertEqual(summary["group_count"], 1)
        records = self.store.list_daily_summary_records(package_run_id=package["run_id"])
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["tags_csv"], "web3,info")

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
        service_text = service.read_text(encoding="utf-8")
        self.assertIn("daily-run", service_text)
        self.assertIn("TimeoutStartSec=0", service_text)

    def test_topics_group_with_parent_tags_unless_explicitly_different_or_important(self) -> None:
        self._origin(-2001, "Parent Group", "web3,info")
        for topic_id, title, tags, important in (
            (10, "Blank Topic", None, False),
            (11, "Same Topic", "web3,info", False),
            (12, "AI Topic", "ai,info", False),
            (13, "Important Topic", None, True),
        ):
            self.store.upsert_origin(
                OriginRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    origin_id=-2001,
                    topic_id=topic_id,
                    parent_origin_id=-2001,
                    origin_type="topic",
                    title=title,
                    important=important,
                )
            )
            self.store.set_backup_policy(
                BackupPolicyRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    origin_id=-2001,
                    topic_id=topic_id,
                    enabled=True,
                    tags=tags,
                )
            )
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=-2001,
                    topic_id=topic_id,
                    message_id=topic_id,
                    sent_at="2026-07-03T01:00:00+00:00",
                    ingested_at=utc_now_iso(),
                    text=f"{title} payload",
                ),
                event_type="new",
            )
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-2001,
                message_id=1,
                sent_at="2026-07-03T01:00:00+00:00",
                ingested_at=utc_now_iso(),
                text="parent payload",
            ),
            event_type="new",
        )

        package = build_daily_package(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main", "tag_groups": ["web3 info", "ai info"]},
        )

        payload = json.loads(Path(package["package_json_path"]).read_text(encoding="utf-8"))
        groups = {group["name"]: group for group in payload["normal_groups"]}
        web3_origins = {
            (origin["origin"]["origin_id"], origin["origin"]["topic_id"])
            for origin in groups["web3 info"]["origins"]
        }
        ai_origins = {
            (origin["origin"]["origin_id"], origin["origin"]["topic_id"])
            for origin in groups["ai info"]["origins"]
        }
        self.assertEqual(web3_origins, {(-2001, 0), (-2001, 10), (-2001, 11)})
        self.assertEqual(ai_origins, {(-2001, 12)})
        blank_topic = next(
            origin["origin"]
            for origin in groups["web3 info"]["origins"]
            if origin["origin"]["topic_id"] == 10
        )
        self.assertEqual(blank_topic["tags"], ["web3", "info"])
        self.assertEqual(blank_topic["local_tags"], [])
        self.assertEqual(blank_topic["tag_grouping"], "parent")
        self.assertEqual(
            [(origin["origin"]["origin_id"], origin["origin"]["topic_id"]) for origin in payload["important_origins"]],
            [(-2001, 13)],
        )

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

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = main(["--config", str(config_path), "daily-run", "--date", "2026-07-03", "--timezone", "UTC", "--account-id", "main"])
        self.assertEqual(code, 0)
        run = json.loads(stdout.getvalue())
        self.assertEqual(run["status"], "completed")
        self.assertEqual(run["package"]["status"], "completed")
        self.assertEqual(run["summary"]["status"], "completed")

    def test_daily_summary_job_can_cancel_running_provider_process(self) -> None:
        self._origin(-1001, "Cancelable Daily", "web3,info")
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-1001,
                message_id=1,
                sent_at="2026-07-03T01:00:00+00:00",
                ingested_at=utc_now_iso(),
                text="cancel me",
            ),
            event_type="new",
        )
        slow_provider = self.root / "slow_provider.py"
        slow_provider.write_text(
            "from pathlib import Path\n"
            "import sys\n"
            "import time\n"
            "sys.stdin.read()\n"
            "time.sleep(5)\n"
            "Path(sys.argv[1]).write_text('# done', encoding='utf-8')\n",
            encoding="utf-8",
        )
        slow_config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                systemd_user_dir=self.config.daily.systemd_user_dir,
                ai=DailyAiConfig(
                    provider="fake",
                    command=[sys.executable, str(slow_provider), "{output}", "{task}"],
                    timeout_seconds=10,
                ),
            ),
            config_path=self.config.config_path,
        )

        job = start_daily_summary_job(
            self.store,
            slow_config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        deadline = time.time() + 3
        current = job
        while time.time() < deadline:
            current = self.store.get_daily_summary_job(job["job_id"])
            assert current is not None
            if (current.get("progress") or {}).get("pid"):
                break
            time.sleep(0.05)

        canceled = cancel_daily_summary_job(self.store, job["job_id"])
        self.assertIn(canceled["status"], {"cancel_requested", "canceled"})
        deadline = time.time() + 3
        while time.time() < deadline:
            current = self.store.get_daily_summary_job(job["job_id"])
            assert current is not None
            if current["status"] == "canceled":
                break
            time.sleep(0.05)
        self.assertEqual(current["status"], "canceled")
        self.assertEqual(current["progress_label"], "canceled")

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
