from __future__ import annotations

import json
import io
import shlex
import sys
import tempfile
import time
import unittest
import uuid
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.cli import main
from tele_mess_core.config import (
    AppConfig,
    DailyAiConfig,
    DailyAiFallbackConfig,
    DailyDeliveryConfig,
    DailyPackagingConfig,
    LoggingConfig,
    ServerConfig,
    StorageConfig,
    TelegramConfig,
)
from tele_mess_core.daily import (
    AiProviderResult,
    AiProviderRuntimeState,
    _codex_provider_error,
    _expand_command,
    _run_summary_provider,
    build_daily_package,
    run_daily_summary,
    update_daily_package_schedule,
)
from tele_mess_core.daily_jobs import DailyJobWorker
from tele_mess_core.models import BackupPolicyRecord, DailySummaryDeliveryRecord, MediaFileRecord, MessageRecord, OriginRecord, SOURCE_TELEGRAM, utc_now_iso


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
        records = self.store.list_daily_summary_records(
            package_run_id=package["run_id"],
            tags=["ai", "info"],
            important=True,
        )
        self.assertEqual({item["record_type"] for item in records}, {"important_origin", "important_daily"})
        self.assertTrue(all(item["run_id"] == summary["run_id"] for item in records))
        self.assertTrue(all("AI provider is disabled" in item["content_preview"] for item in records))
        all_records = self.store.list_daily_summary_records(package_run_id=package["run_id"])
        self.assertEqual(
            {item["record_type"] for item in all_records},
            {"important_origin", "important_daily", "point_daily"},
        )
        point_daily = next(item for item in all_records if item["record_type"] == "point_daily")
        self.assertEqual(point_daily["tags"], ["point"])
        self.assertEqual(point_daily["tags_csv"], "point")
        self.assertEqual(self.store.list_daily_message_points(run_id=summary["run_id"]), [])

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
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["record_type"], "point_daily")
        self.assertEqual(records[0]["tags"], ["point"])
        self.assertEqual(records[0]["tags_csv"], "point")
        point_prompts = list((Path(summary["output_dir"]) / "stages" / "message-points").glob("*.prompt.md"))
        self.assertEqual(len(point_prompts), 3)
        prompt_text = "\n".join(path.read_text(encoding="utf-8") for path in point_prompts)
        self.assertIn("TASK: message_point_extraction", prompt_text)
        self.assertIn('"tags": [\n    "web3",\n    "info"', prompt_text)
        self.assertFalse((Path(summary["output_dir"]) / "stages" / "normal-groups").exists())

    def test_daily_summary_delivers_final_summary_when_configured(self) -> None:
        self._origin(-7001, "Delivery Source", "info")
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-7001,
                message_id=1,
                sent_at="2026-07-03T01:00:00+00:00",
                ingested_at=utc_now_iso(),
                text="delivery source",
            ),
            event_type="new",
        )
        package = build_daily_package(self.store, self.config, run_date="2026-07-03", timezone_name="UTC", scope={"account_id": "main"})
        self.store.set_daily_summary_delivery(
            DailySummaryDeliveryRecord(
                enabled=True,
                account_id="main",
                origin_id=-9001,
                topic_id=88,
            )
        )

        delivery_result = {
            "account_id": "main",
            "origin_id": -9001,
            "topic_id": 88,
            "status": "sent",
            "message_count": 1,
            "message_ids": [123],
        }
        with patch("tele_mess_core.daily.deliver_daily_summary", return_value=delivery_result) as deliver:
            summary = run_daily_summary(self.store, self.config, package_run_id=package["run_id"])

        deliver.assert_called_once()
        delivered_content = deliver.call_args.args[2]
        self.assertIn("# Daily Message Point Summary", delivered_content)
        self.assertIn("- Date: `2026-07-03`", delivered_content)
        self.assertIn("- Timezone: `UTC`", delivered_content)
        self.assertIn("- Tags: #point", delivered_content)
        self.assertIn("- Summary provider: `disabled`", delivered_content)
        self.assertIn("No message points were extracted", delivered_content)
        self.assertEqual(deliver.call_args.kwargs["delivery"].origin_id, -9001)
        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["progress_current"], summary["progress_total"])
        self.assertEqual(summary["progress"]["delivery_count"], 1)
        summary_payload = json.loads((Path(summary["output_dir"]) / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual(summary_payload["delivery"]["status"], "sent")
        self.assertEqual(summary_payload["delivery"]["deliveries"], [{"kind": "point_summary", "result": delivery_result}])

    def test_structured_points_are_persisted_for_normal_and_important_origins(self) -> None:
        self._origin(-9001, "Normal Source", "normal,info")
        self._origin(-9002, "Important Source", "alert,info", important=True)
        for chat_id, message_id, sent_at, permalink in (
            (-9001, 11, "2026-07-03T01:05:00+00:00", "https://t.me/normal_source/11"),
            (-9002, 22, "2026-07-03T02:15:00+00:00", "https://t.me/important_source/22"),
        ):
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=chat_id,
                    message_id=message_id,
                    sent_at=sent_at,
                    ingested_at=utc_now_iso(),
                    text=f"structured payload {message_id}",
                    permalink=permalink,
                ),
                event_type="new",
            )

        package = build_daily_package(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        fake_config, fake_log = self._structured_ai_config(delivery=True)
        delivery_result = {
            "account_id": "main",
            "origin_id": -9900,
            "topic_id": 0,
            "status": "sent",
            "message_count": 1,
            "message_ids": [321],
        }

        with patch("tele_mess_core.daily.deliver_daily_summary", return_value=delivery_result) as deliver:
            summary = run_daily_summary(self.store, fake_config, package_run_id=package["run_id"])

        self.assertEqual(summary["status"], "completed")
        calls = self._structured_provider_calls(fake_log)
        tasks = [item["task"] for item in calls]
        point_calls = [item for item in calls if item["task"] == "message_point_extraction"]
        self.assertEqual({item["origin"]["origin_id"] for item in point_calls}, {-9001, -9002})
        self.assertTrue(all("--output-schema" in item["args"] for item in point_calls))
        self.assertIn("important_origin_analysis", tasks)
        self.assertIn("important_daily_summary", tasks)
        self.assertIn("daily_point_summary", tasks)
        self.assertEqual(
            next(item for item in calls if item["task"] == "daily_point_summary")["persisted_point_count"],
            2,
        )
        self.assertNotIn("normal_origin_key_extraction", tasks)
        self.assertNotIn("normal_group_analysis", tasks)
        self.assertNotIn("final_daily_summary", tasks)

        points = self.store.list_daily_message_points(run_id=summary["run_id"], limit=20)
        self.assertEqual(len(points), 2)
        points_by_origin = {item["origin_id"]: item for item in points}
        normal_point = points_by_origin[-9001]
        important_point = points_by_origin[-9002]
        self.assertEqual(normal_point["occurred_at"], "2026-07-03T01:05:00+00:00")
        self.assertEqual(normal_point["telegram_deeplink"], "tg://resolve?domain=normal_source&post=11")
        self.assertNotEqual(normal_point["telegram_deeplink"], "tg://resolve?domain=forged&post=1")
        self.assertEqual(normal_point["message_id"], 11)
        self.assertEqual(normal_point["source_refs"], ["main/-9001/0/11"])
        self.assertEqual(normal_point["importance_score"], 4)
        self.assertEqual(normal_point["tags"], ["normal", "info", "derived"])
        self.assertFalse(normal_point["origin_important"])
        self.assertTrue(important_point["origin_important"])
        self.assertEqual(important_point["telegram_deeplink"], "tg://resolve?domain=important_source&post=22")

        records = self.store.list_daily_summary_records(run_id=summary["run_id"], limit=20)
        self.assertEqual(
            {item["record_type"] for item in records},
            {"important_origin", "important_daily", "point_daily"},
        )
        point_daily = next(item for item in records if item["record_type"] == "point_daily")
        self.assertEqual(point_daily["tags"], ["point"])
        self.assertIn("Fake daily_point_summary", point_daily["content_preview"])

        self.assertEqual(deliver.call_count, 2)
        delivered = [item.args[2] for item in deliver.call_args_list]
        important_delivery = next(content for content in delivered if content.startswith("# Important Daily Summary"))
        point_delivery = next(content for content in delivered if content.startswith("# Daily Message Point Summary"))
        self.assertNotIn("- Tags: #point", important_delivery)
        self.assertIn("- Tags: #point", point_delivery)
        self.assertIn("Fake daily_point_summary", point_delivery)
        self.assertEqual(summary["progress"]["delivery_count"], 2)

    def test_point_extraction_batches_all_unmatched_messages(self) -> None:
        self._origin(-9101, "Unmatched Source", "misc")
        for index in range(205):
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=-9101,
                    message_id=index + 1,
                    sent_at=f"2026-07-03T01:{index % 60:02d}:00+00:00",
                    ingested_at=utc_now_iso(),
                    text=f"unmatched payload {index + 1}",
                ),
                event_type="new",
            )

        package = build_daily_package(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main", "tag_groups": ["ai info"]},
        )
        package_payload = json.loads(Path(package["package_json_path"]).read_text(encoding="utf-8"))
        self.assertEqual([item["origin_id"] for item in package_payload["unmatched_origins"]], [-9101])
        self.assertEqual(package_payload["stats"]["origin_count"], 1)
        self.assertEqual(package_payload["stats"]["message_count"], 205)
        self.assertEqual(package_payload["stats"]["point_origin_count"], 1)
        self.assertEqual(package_payload["stats"]["point_message_count"], 205)

        fake_config, fake_log = self._structured_ai_config()
        summary = run_daily_summary(self.store, fake_config, package_run_id=package["run_id"])

        self.assertEqual(summary["status"], "completed")
        calls = [
            item
            for item in self._structured_provider_calls(fake_log)
            if item["task"] == "message_point_extraction"
        ]
        self.assertEqual(len(calls), 2)
        self.assertEqual([len(item["message_ids"]) for item in calls], [200, 5])
        all_message_ids = [message_id for item in calls for message_id in item["message_ids"]]
        self.assertEqual(sorted(all_message_ids), list(range(1, 206)))
        self.assertEqual(len(set(all_message_ids)), 205)
        self.assertEqual(summary["progress"]["point_extraction_count"], 2)
        all_calls = self._structured_provider_calls(fake_log)
        self.assertEqual(
            next(item for item in all_calls if item["task"] == "daily_point_summary")["persisted_point_count"],
            2,
        )
        points = self.store.list_daily_message_points(run_id=summary["run_id"], limit=20)
        self.assertEqual(
            {item["message_id"] for item in points},
            {int(item["message_ids"][0]) for item in calls},
        )

    def test_parent_topic_messages_are_canonicalized_once_for_points(self) -> None:
        self._origin(-9201, "Parent", "info")
        self.store.upsert_origin(
            OriginRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-9201,
                topic_id=42,
                parent_origin_id=-9201,
                origin_type="topic",
                title="Topic 42",
            )
        )
        self.store.set_backup_policy(
            BackupPolicyRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-9201,
                topic_id=42,
                enabled=True,
                tags="topic,info",
            )
        )
        for message_id, topic_id, text in (
            (1, None, "parent-only message"),
            (42, 42, "topic message"),
        ):
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=-9201,
                    topic_id=topic_id,
                    message_id=message_id,
                    sent_at="2026-07-03T03:00:00+00:00",
                    ingested_at=utc_now_iso(),
                    text=text,
                ),
                event_type="new",
            )

        package = build_daily_package(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main"},
        )
        payload = json.loads(Path(package["package_json_path"]).read_text(encoding="utf-8"))
        canonical_messages = {
            int(item["origin"]["topic_id"]): [message["message_id"] for message in item["messages"]]
            for item in payload["point_origins"]
        }
        self.assertEqual(canonical_messages, {0: [1], 42: [42]})

        fake_config, fake_log = self._structured_ai_config()
        summary = run_daily_summary(self.store, fake_config, package_run_id=package["run_id"])

        self.assertEqual(summary["status"], "completed")
        point_calls = [
            item
            for item in self._structured_provider_calls(fake_log)
            if item["task"] == "message_point_extraction"
        ]
        self.assertEqual(
            {(item["origin"]["topic_id"], tuple(item["message_ids"])) for item in point_calls},
            {(0, (1,)), (42, (42,))},
        )
        points = self.store.list_daily_message_points(run_id=summary["run_id"], limit=20)
        self.assertEqual({(item["topic_id"], item["message_id"]) for item in points}, {(0, 1), (42, 42)})

    def test_ai_command_placeholders_expand_model_schema_and_images(self) -> None:
        output_path = self.root / "point.json"
        schema_path = self.root / "point.schema.json"

        expanded = _expand_command(
            ["codex", "exec", "{model}", "{output_schema}", "{images}", "--task={task}", "{output}"],
            output_path,
            ["one.png", "two.png"],
            task_name="message_point_extraction",
            model="gpt-5.6-sol",
            output_schema_path=schema_path,
        )

        self.assertEqual(
            expanded,
            [
                "codex",
                "exec",
                "--model",
                "gpt-5.6-sol",
                "--output-schema",
                str(schema_path),
                "--image",
                "one.png",
                "--image",
                "two.png",
                "--task=message_point_extraction",
                str(output_path),
            ],
        )

        legacy = _expand_command(
            [
                "codex",
                "-a",
                "never",
                "exec",
                "--skip-git-repo-check",
                "--output-last-message",
                "{output}",
                "{images}",
                "-",
            ],
            output_path,
            [],
            task_name="message_point_extraction",
            model="gpt-5.6-sol",
            output_schema_path=schema_path,
        )
        exec_index = legacy.index("exec")
        self.assertEqual(
            legacy[exec_index + 1 : exec_index + 5],
            ["--model", "gpt-5.6-sol", "--output-schema", str(schema_path)],
        )
        self.assertEqual(legacy.count("--model"), 1)
        self.assertEqual(legacy.count("--output-schema"), 1)

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

    def test_daily_package_pages_through_complete_origin_history(self) -> None:
        self._origin(-8050, "Paged Origin", "info")
        for message_id in range(1, 6):
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=-8050,
                    message_id=message_id,
                    sent_at=f"2026-07-03T01:0{message_id}:00+00:00",
                    ingested_at=utc_now_iso(),
                    text=f"paged message {message_id}",
                ),
                event_type="new",
            )

        with patch("tele_mess_core.daily.PACKAGE_MESSAGE_PAGE_SIZE", 2):
            package = build_daily_package(
                self.store,
                self.config,
                run_date="2026-07-03",
                timezone_name="UTC",
                scope={"account_id": "main"},
            )

        payload = json.loads(Path(package["package_json_path"]).read_text(encoding="utf-8"))
        point_messages = payload["point_origins"][0]["messages"]
        self.assertEqual([item["message_id"] for item in point_messages], [1, 2, 3, 4, 5])
        self.assertEqual(payload["stats"]["message_count"], 5)

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
        self.assertEqual(records[0]["record_type"], "point_daily")
        self.assertEqual(records[0]["tags_csv"], "point")

    def test_schedule_update_writes_systemd_user_timer_files(self) -> None:
        workspace = self.root / "Mac Workspace"
        self.config.workspace_dir = workspace
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
        exec_start = next(line.removeprefix("ExecStart=") for line in service_text.splitlines() if line.startswith("ExecStart="))
        command = shlex.split(exec_start)
        self.assertEqual(command[command.index("--workspace") + 1], str(workspace))
        self.assertEqual(command[command.index("--config") + 1], str(self.config.config_path))
        self.assertIn("--enqueue-only", command)

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

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = main(
                [
                    "--config",
                    str(config_path),
                    "daily-run",
                    "--date",
                    "2026-07-04",
                    "--timezone",
                    "UTC",
                    "--account-id",
                    "main",
                    "--enqueue-only",
                ]
            )
        self.assertEqual(code, 0)
        queued = json.loads(stdout.getvalue())
        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["mode"], "enqueue-only")
        self.assertIsNone(queued["package_run_id"])
        self.assertIsNone(queued["summary_run_id"])

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

        worker = DailyJobWorker(self.store, slow_config, poll_interval=0.05)
        worker.start()
        try:
            job = worker.enqueue(
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

            canceled = worker.cancel(job["job_id"])
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
        finally:
            worker.stop()

    def test_codex_usage_limit_activates_run_local_fallback_circuit(self) -> None:
        counter_path = self.root / "codex-attempts.txt"
        provider_path = self.root / "limited_codex.py"
        provider_path.write_text(
            "from pathlib import Path\n"
            "import sys\n"
            "sys.stdin.read()\n"
            "path = Path(sys.argv[1])\n"
            "path.write_text(path.read_text() + 'attempt\\n' if path.exists() else 'attempt\\n')\n"
            "print(\"ERROR: You've hit your usage limit. Visit https://chatgpt.com/codex/settings/usage to purchase more credits or try again at 11:33 PM.\", file=sys.stderr)\n"
            "raise SystemExit(1)\n",
            encoding="utf-8",
        )
        key_path = self.root / "fallback-key"
        key_path.write_text("local-secret", encoding="utf-8")
        config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=DailyAiConfig(
                    provider="codex-cli",
                    command=[sys.executable, str(provider_path), str(counter_path)],
                    fallback=DailyAiFallbackConfig(
                        enabled=True,
                        base_url="https://fallback.example/v1",
                        api_key_file=key_path,
                    ),
                ),
            ),
        )
        state = AiProviderRuntimeState()

        def fake_fallback(
            _config: AppConfig,
            _prompt: str,
            output_path: Path,
            _images: list[str],
            **kwargs: object,
        ) -> AiProviderResult:
            runtime_state = kwargs["provider_state"]
            assert isinstance(runtime_state, AiProviderRuntimeState)
            runtime_state.register("openai-compatible:deepseek-v4-flash")
            output_path.write_text("fallback output", encoding="utf-8")
            return AiProviderResult(
                content="fallback output",
                provider="openai-compatible:deepseek-v4-flash",
            )

        with patch(
            "tele_mess_core.daily._run_openai_fallback_provider",
            side_effect=fake_fallback,
        ) as fallback:
            first = _run_summary_provider(
                config,
                "first task",
                self.root / "first.md",
                [],
                provider_state=state,
            )
            second = _run_summary_provider(
                config,
                "second task",
                self.root / "second.md",
                [],
                provider_state=state,
            )

        self.assertEqual(first.content, "fallback output")
        self.assertEqual(second.content, "fallback output")
        self.assertTrue(state.fallback_active)
        self.assertEqual(counter_path.read_text(encoding="utf-8").splitlines(), ["attempt"])
        self.assertEqual(fallback.call_count, 2)

    def test_codex_errors_are_classified_without_persisting_prompt_or_key(self) -> None:
        usage = _codex_provider_error(
            "private Telegram body\nERROR: You've hit your usage limit and try again at 11:33 PM.",
            returncode=1,
        )
        self.assertEqual(usage.kind, "codex_usage_limit")
        self.assertTrue(usage.retryable)
        self.assertNotIn("Telegram", str(usage))

        generic = _codex_provider_error(
            "prompt /home/user/private/image.png sk-private\nERROR: arbitrary upstream failure",
            returncode=7,
        )
        self.assertEqual(generic.kind, "codex_failed")
        self.assertNotIn("/home", str(generic))
        self.assertNotIn("sk-private", str(generic))

    def _structured_ai_config(self, *, delivery: bool = False) -> tuple[AppConfig, Path]:
        provider_path = self.root / "structured_provider.py"
        log_path = self.root / f"structured_provider_{uuid.uuid4().hex}.jsonl"
        provider_path.write_text(
            "from pathlib import Path\n"
            "import json\n"
            "import sqlite3\n"
            "import sys\n"
            "prompt = sys.stdin.read()\n"
            "output = Path(sys.argv[1])\n"
            "task = sys.argv[2]\n"
            "log_path = Path(sys.argv[3])\n"
            "extra = sys.argv[4:]\n"
            "origin = {}\n"
            "evidence = []\n"
            "persisted_point_count = None\n"
            "if task == 'message_point_extraction':\n"
            "    origin_text, evidence_text = prompt.split('Origin metadata:\\n', 1)[1].split('\\n\\nMessage evidence:\\n', 1)\n"
            "    origin = json.loads(origin_text)\n"
            "    evidence = json.loads(evidence_text)\n"
            "    first = evidence[0]\n"
            "    last = evidence[-1]\n"
            "    payload = {'points': [{'source_message_ids': [first['message_id']], 'tags': ['derived'], "
            "'content': f\"Point for {origin.get('origin_id')}/{origin.get('topic_id', 0)} messages {first['message_id']}-{last['message_id']}\", "
            "'importance_score': 4, 'importance_reason': 'fake material update', "
            "'occurred_at': '1900-01-01T00:00:00+00:00', "
            "'telegram_deeplink': 'tg://resolve?domain=forged&post=1'}]}\n"
            "    output.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')\n"
            "else:\n"
            "    if task == 'daily_point_summary':\n"
            "        with sqlite3.connect(log_path.parent / 'archive.db') as connection:\n"
            "            persisted_point_count = connection.execute('SELECT COUNT(*) FROM daily_message_points').fetchone()[0]\n"
            "    output.write_text(f'# Fake {task}\\n\\nGenerated from validated inputs.\\n', encoding='utf-8')\n"
            "with log_path.open('a', encoding='utf-8') as handle:\n"
            "    handle.write(json.dumps({'task': task, 'args': extra, 'origin': origin, "
            "'message_ids': [item.get('message_id') for item in evidence], "
            "'persisted_point_count': persisted_point_count}, ensure_ascii=False) + '\\n')\n",
            encoding="utf-8",
        )
        return (
            AppConfig(
                storage=self.config.storage,
                telegram=self.config.telegram,
                server=self.config.server,
                logging=self.config.logging,
                daily=DailyPackagingConfig(
                    output_dir=self.config.daily.output_dir,
                    systemd_user_dir=self.config.daily.systemd_user_dir,
                    ai=DailyAiConfig(
                        provider="fake-structured",
                        model="gpt-5.6-sol",
                        command=[
                            sys.executable,
                            str(provider_path),
                            "{output}",
                            "{task}",
                            str(log_path),
                            "{output_schema}",
                            "{images}",
                        ],
                    ),
                    delivery=DailyDeliveryConfig(
                        enabled=delivery,
                        account_id="main" if delivery else "",
                        origin_id=-9900 if delivery else None,
                    ),
                ),
                config_path=self.config.config_path,
            ),
            log_path,
        )

    def _structured_provider_calls(self, log_path: Path) -> list[dict[str, object]]:
        return [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]

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
