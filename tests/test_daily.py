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
from unittest.mock import AsyncMock, Mock, patch

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.cli import main
from tele_mess_core.codex_limits import CodexRateLimitStatus
from tele_mess_core.config import (
    AppConfig,
    DailyAiConfig,
    DailyAiFallbackConfig,
    DailyDeliveryConfig,
    DailyPackagingConfig,
    LoggingConfig,
    ServerConfig,
    StorageConfig,
    TelegramAccountConfig,
    TelegramConfig,
    resolve_account_config,
)
from tele_mess_core.daily import (
    AiProviderError,
    AiProviderResult,
    AiProviderRuntimeState,
    DailyJobInterrupted,
    _codex_provider_error,
    _collect_image_paths,
    _daily_ai_work_dir,
    _expand_command,
    _origin_image_paths,
    _run_summary_provider,
    build_daily_package,
    resolve_daily_summary_delivery,
    run_daily_summary,
    update_daily_package_schedule,
    update_daily_summary_delivery,
)
from tele_mess_core.daily_jobs import DailyJobWorker
from tele_mess_core.models import AccountAuthRecord, AccountRecord, BackupPolicyRecord, DailySummaryDeliveryRecord, MediaFileRecord, MessageRecord, OriginRecord, SOURCE_TELEGRAM, utc_now_iso


class DailyPackagingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = ArchiveStore(self.root / "archive.db")
        self.store.initialize()
        self.store.upsert_account(
            AccountRecord(source=SOURCE_TELEGRAM, account_id="main", display_name="Main", kind="telegram")
        )
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

    def test_worker_interruption_leaves_package_run_resumable(self) -> None:
        checks = 0

        def interrupt_after_start() -> None:
            nonlocal checks
            checks += 1
            if checks > 1:
                raise DailyJobInterrupted("worker stopping")

        run_id = "pkg_interrupted"
        with self.assertRaises(DailyJobInterrupted):
            build_daily_package(
                self.store,
                self.config,
                run_date="2026-07-03",
                timezone_name="UTC",
                run_id=run_id,
                cancel_check=interrupt_after_start,
            )

        package = self.store.get_daily_package_run(run_id)
        assert package is not None
        self.assertEqual(package["status"], "running")
        self.assertIsNone(package["finished_at"])

    def test_worker_interruption_does_not_fail_summary_run(self) -> None:
        package = build_daily_package(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
        )
        checks = 0

        def interrupt_after_start() -> None:
            nonlocal checks
            checks += 1
            if checks > 1:
                raise DailyJobInterrupted("worker stopping")

        run_id = "sum_interrupted"
        with self.assertRaises(DailyJobInterrupted):
            run_daily_summary(
                self.store,
                self.config,
                package_run_id=str(package["run_id"]),
                run_id=run_id,
                cancel_check=interrupt_after_start,
            )

        summary = self.store.get_daily_summary_run(run_id)
        assert summary is not None
        self.assertEqual(summary["status"], "running")
        self.assertIsNone(summary["error"])
        self.assertIsNone(summary["finished_at"])

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
        self.assertFalse((Path(package["output_dir"]) / "normal-groups").exists())
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
        self._configure_main_account()
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

        summary = run_daily_summary(self.store, self.config, package_run_id=package["run_id"])

        outbox = self.store.list_delivery_outbox(summary_run_id=summary["run_id"])
        self.assertEqual(len(outbox), 1)
        self.assertEqual(outbox[0]["content_kind"], "point_summary")
        self.assertEqual(outbox[0]["origin_id"], -9001)
        self.assertEqual(outbox[0]["topic_id"], 88)
        delivered_content = outbox[0]["content"]
        self.assertIn("**Daily Message Point Summary**", delivered_content)
        self.assertIn("- Date: `2026-07-03`", delivered_content)
        self.assertIn("- Timezone: `UTC`", delivered_content)
        self.assertIn("- Tags: #point", delivered_content)
        self.assertIn("- Summary provider: `disabled`", delivered_content)
        self.assertIn("No message points were extracted", delivered_content)

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["progress_current"], summary["progress_total"])
        self.assertEqual(summary["progress"]["delivery_count"], 1)
        summary_payload = json.loads((Path(summary["output_dir"]) / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual(summary_payload["delivery"]["status"], "queued")
        self.assertEqual(summary_payload["delivery"]["delivery_kinds"], ["point_summary"])

    def test_image_paths_follow_the_archive_media_classification(self) -> None:
        media_dir = self.root / "media"
        media_dir.mkdir()
        photo = media_dir / "photo"
        sent_as_file = media_dir / "chart.png"
        clip = media_dir / "clip.mp4"
        for path in (photo, sent_as_file, clip):
            path.write_bytes(b"x")
        origin_payload = {
            "messages": [
                {
                    "media_files": [
                        {"media_kind": "photo", "mime_type": None, "content_type": "application/octet-stream", "file_path": str(photo)},
                        {"media_kind": "document", "mime_type": "application/octet-stream", "content_type": "application/octet-stream", "file_path": str(sent_as_file)},
                        {"media_kind": "video", "mime_type": "video/mp4", "content_type": "video/mp4", "file_path": str(clip)},
                    ]
                }
            ]
        }

        expected = [str(photo), str(sent_as_file)]
        self.assertEqual(_origin_image_paths(origin_payload), expected)
        self.assertEqual(_collect_image_paths({"point_origins": [origin_payload]}), expected)

    def test_daily_delivery_accepts_sqlite_telegram_management_account(self) -> None:
        self.config.telegram.accounts.append(
            TelegramAccountConfig(
                account_id="credential-template",
                api_id=123,
                api_hash="template-hash",
                session_name="credential-template",
                session_dir=self.root / "template-sessions",
            )
        )
        self.store.upsert_account(
            AccountRecord(
                source=SOURCE_TELEGRAM,
                account_id="runtime-account",
                display_name="Runtime Account",
                kind="telegram",
            )
        )
        self.store.upsert_account_auth(
            AccountAuthRecord(
                source=SOURCE_TELEGRAM,
                account_id="runtime-account",
                auth_state="authorized",
                session_name="runtime-session",
                session_dir="runtime-sessions",
            )
        )

        stored = update_daily_summary_delivery(
            self.store,
            self.config,
            {
                "enabled": True,
                "account_id": "runtime-account",
                "origin_id": -9001,
                "topic_id": 42,
            },
        )
        resolved = resolve_daily_summary_delivery(self.store, self.config)

        self.assertTrue(stored["enabled"])
        self.assertEqual(resolved.account_id, "runtime-account")
        self.assertEqual(resolved.origin_id, -9001)
        self.assertEqual(resolved.topic_id, 42)

    def test_daily_delivery_rejects_non_telegram_management_account(self) -> None:
        self.store.upsert_account(
            AccountRecord(
                source="other",
                account_id="shared-id",
                display_name="Other Account",
                kind="other",
            )
        )

        with self.assertRaisesRegex(ValueError, "Unknown account_id: shared-id"):
            update_daily_summary_delivery(
                self.store,
                self.config,
                {"enabled": True, "account_id": "shared-id", "origin_id": -9001},
            )

    def test_daily_delivery_keeps_yaml_account_compatibility(self) -> None:
        self.config.telegram.accounts.append(
            TelegramAccountConfig(
                account_id="configured-account",
                api_id=1,
                api_hash="test",
                session_name="configured-account",
                session_dir=self.root / "sessions",
            )
        )

        update_daily_summary_delivery(
            self.store,
            self.config,
            {"enabled": True, "account_id": "configured-account", "origin_id": -9002},
        )

        self.assertEqual(
            resolve_daily_summary_delivery(self.store, self.config).account_id,
            "configured-account",
        )

    def test_structured_points_are_persisted_for_normal_and_important_origins(self) -> None:
        self._configure_main_account()
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
            0,
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

        outbox = self.store.list_delivery_outbox(summary_run_id=summary["run_id"])
        self.assertEqual(len(outbox), 2)
        by_kind = {item["content_kind"]: item for item in outbox}
        self.assertEqual(set(by_kind), {"important_summary", "point_summary"})
        self.assertEqual(by_kind["important_summary"]["chunk_index"], 1)
        self.assertEqual(by_kind["important_summary"]["chunk_count"], 1)
        self.assertEqual(by_kind["point_summary"]["chunk_index"], 1)
        self.assertEqual(by_kind["point_summary"]["chunk_count"], 1)
        important_delivery = by_kind["important_summary"]["content"]
        point_delivery = by_kind["point_summary"]["content"]
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
            0,
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
                "--ephemeral",
                "--sandbox",
                "read-only",
                "--disable",
                "hooks",
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
            legacy[exec_index + 1 : exec_index + 10],
            [
                "--ephemeral",
                "--sandbox",
                "read-only",
                "--disable",
                "hooks",
                "--model",
                "gpt-5.6-sol",
                "--output-schema",
                str(schema_path),
            ],
        )
        self.assertEqual(legacy.count("--ephemeral"), 1)
        self.assertEqual(legacy.count("--sandbox"), 1)
        self.assertEqual(legacy.count("--disable"), 1)
        self.assertEqual(legacy.count("--model"), 1)
        self.assertEqual(legacy.count("--output-schema"), 1)

        explicit_hooks = _expand_command(
            ["codex", "exec", "--enable", "hooks", "--ephemeral", "{output}"],
            output_path,
            [],
        )
        self.assertEqual(explicit_hooks.count("--ephemeral"), 1)
        self.assertNotIn("--disable", explicit_hooks)

        explicit_hooks_equals = _expand_command(
            ["codex", "--config=features.hooks=true", "exec", "{output}"],
            output_path,
            [],
        )
        self.assertNotIn("--disable", explicit_hooks_equals)

        explicit_sandbox = _expand_command(
            ["codex", "exec", "--sandbox", "workspace-write", "{output}"],
            output_path,
            [],
        )
        self.assertEqual(explicit_sandbox.count("--sandbox"), 1)
        self.assertIn("workspace-write", explicit_sandbox)

    def test_default_codex_command_uses_read_only_sandbox(self) -> None:
        command = DailyAiConfig().command

        sandbox_index = command.index("--sandbox")
        self.assertEqual(command[sandbox_index + 1], "read-only")
        self.assertEqual(_daily_ai_work_dir(self.config), self.root / "ai-work")

    def test_worker_interruption_terminates_running_provider_process(self) -> None:
        provider_path = self.root / "slow_interrupted_provider.py"
        provider_path.write_text(
            "import sys\n"
            "import time\n"
            "sys.stdin.read()\n"
            "time.sleep(30)\n",
            encoding="utf-8",
        )
        config = AppConfig(
            storage=self.config.storage,
            telegram=self.config.telegram,
            server=self.config.server,
            logging=self.config.logging,
            daily=DailyPackagingConfig(
                output_dir=self.config.daily.output_dir,
                ai=DailyAiConfig(
                    provider="fake",
                    command=[sys.executable, str(provider_path)],
                    timeout_seconds=60,
                ),
            ),
        )
        checks = 0
        processes = []

        def interrupt_after_process_start() -> None:
            nonlocal checks
            checks += 1
            if checks > 1:
                raise DailyJobInterrupted("worker stopping")

        def track_process(process: object | None, task_name: str) -> None:
            processes.append(process)

        with self.assertRaises(DailyJobInterrupted):
            _run_summary_provider(
                config,
                "prompt",
                self.root / "interrupted-output.md",
                [],
                cancel_check=interrupt_after_process_start,
                process_callback=track_process,
                provider_state=AiProviderRuntimeState(),
            )

        self.assertEqual(len(processes), 2)
        process = processes[0]
        self.assertIsNotNone(process)
        assert process is not None
        self.assertIsNotNone(process.poll())
        self.assertIsNone(processes[1])

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

    def test_daily_package_preserves_sticker_emoji_without_media_file(self) -> None:
        self._origin(-8040, "Sticker Source", "info")
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-8040,
                message_id=7,
                sent_at="2026-07-03T01:00:00+00:00",
                ingested_at=utc_now_iso(),
                text="🙂",
                has_media=False,
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
        message = payload["point_origins"][0]["messages"][0]
        self.assertEqual(message["text"], "🙂")
        self.assertFalse(message["has_media"])
        self.assertIsNone(message["media_kind"])
        self.assertEqual(message["media_files"], [])
        self.assertEqual(payload["stats"]["message_count"], 1)
        self.assertEqual(payload["stats"]["media_count"], 0)
        origin_markdown = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (Path(package["output_dir"]) / "point-origins").glob("*.md")
        )
        self.assertIn(": 🙂", origin_markdown)

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

    def test_daily_package_unified_topic_policy_excludes_disabled_archived_and_includes_important_only_topic(self) -> None:
        self._origin(-4001, "Forum Parent", "web3,info")
        for topic_id, title, important in (
            (10, "Normal Inherited", False),
            (11, "Important Only", True),
            (12, "Disabled Topic", False),
            (13, "Archived Topic", False),
            (14, "AI Topic", False),
        ):
            self.store.upsert_origin(
                OriginRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    origin_id=-4001,
                    topic_id=topic_id,
                    parent_origin_id=-4001,
                    origin_type="topic",
                    title=title,
                    important=important,
                )
            )
        # Topic 10 & 11 have NO own backup_policy (pure inheritance from parent).
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", -4001, topic_id=12, enabled=False)
        )
        self.store.archive_origin(SOURCE_TELEGRAM, "main", -4001, topic_id=13, archived=True)
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", -4001, topic_id=14, enabled=True, tags="ai,info")
        )
        # Topic 15 has no origins row, only a disabled backup_policy row.
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", -4001, topic_id=15, enabled=False)
        )

        for msg_id, tid, text in (
            (1, None, "parent general"),
            (10, 10, "inherited normal"),
            (11, 11, "important only msg"),
            (12, 12, "disabled topic msg"),
            (13, 13, "archived topic msg"),
            (14, 14, "ai topic msg"),
            (15, 15, "policy-only disabled msg"),
        ):
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=-4001,
                    topic_id=tid,
                    message_id=msg_id,
                    sent_at="2026-07-03T02:00:00+00:00",
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
            scope={"account_id": "main", "tag_groups": ["web3 info", "ai info"]},
        )
        payload = json.loads(Path(package["package_json_path"]).read_text(encoding="utf-8"))
        groups = {group["name"]: group for group in payload["normal_groups"]}

        # Parent rolls up general + normal inherited topic 10, and excludes topics 11, 12, 13, 14, 15.
        parent_pkg = next(o for o in groups["web3 info"]["origins"] if o["origin"]["topic_id"] == 0)
        self.assertEqual([m["message_id"] for m in parent_pkg["messages"]], [1, 10])
        self.assertEqual(len(groups["web3 info"]["origins"]), 1)

        # Important-only topic 11 (without its own backup_policy) is selected as important_origin.
        self.assertEqual(
            [(o["origin"]["origin_id"], o["origin"]["topic_id"]) for o in payload["important_origins"]],
            [(-4001, 11)],
        )
        self.assertEqual([m["message_id"] for m in payload["important_origins"][0]["messages"]], [11])

        # AI topic 14 is in ai info only.
        ai_pkg = groups["ai info"]["origins"][0]
        self.assertEqual(ai_pkg["origin"]["topic_id"], 14)
        self.assertEqual([m["message_id"] for m in ai_pkg["messages"]], [14])

        # Disabled/archived topics (12, 13, 15) must never appear in point_origins either.
        all_point_msg_ids = {
            m["message_id"]
            for origin_pkg in payload["point_origins"]
            for m in origin_pkg["messages"]
        }
        self.assertEqual(all_point_msg_ids, {1, 10, 11, 14})

        # Tag-filtered scope for web3 must NOT leak topic 14 (ai) into parent.
        web3_only = build_daily_package(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main", "tags": "web3"},
        )
        web3_payload = json.loads(Path(web3_only["package_json_path"]).read_text(encoding="utf-8"))
        web3_point_ids = {
            m["message_id"]
            for origin_pkg in web3_payload["point_origins"]
            for m in origin_pkg["messages"]
        }
        self.assertEqual(web3_point_ids, {1, 10, 11})

        # Explicit topic_id scope selects an inherited normal topic via its effective policy.
        topic10_only = build_daily_package(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main", "origin_id": -4001, "topic_id": 10},
        )
        topic10_payload = json.loads(Path(topic10_only["package_json_path"]).read_text(encoding="utf-8"))
        self.assertEqual(
            [m["message_id"] for o in topic10_payload["point_origins"] for m in o["messages"]],
            [10],
        )

    def test_daily_package_policy_only_topic_and_policy_only_parent_preserve_tag_isolation(self) -> None:
        # Case A: parent has origins + web3 policy; topic 20 has ONLY enabled ai policy (no origins row).
        self._origin(-4010, "Web3 Parent", "web3")
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", -4010, topic_id=20, enabled=True, tags="ai")
        )
        for mid, tid, text in ((1, None, "parent msg"), (20, 20, "ai-only topic msg")):
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=-4010,
                    topic_id=tid,
                    message_id=mid,
                    sent_at="2026-07-03T02:00:00+00:00",
                    ingested_at=utc_now_iso(),
                    text=text,
                ),
                event_type="new",
            )

        web3_pkg = build_daily_package(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main", "tags": "web3"},
        )
        web3_payload = json.loads(Path(web3_pkg["package_json_path"]).read_text(encoding="utf-8"))
        self.assertEqual(
            [m["message_id"] for o in web3_payload["point_origins"] for m in o["messages"]],
            [1],
        )

        ai_pkg = build_daily_package(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main", "tags": "ai"},
        )
        ai_payload = json.loads(Path(ai_pkg["package_json_path"]).read_text(encoding="utf-8"))
        self.assertEqual(
            [m["message_id"] for o in ai_payload["point_origins"] for m in o["messages"]],
            [20],
        )

        # Case B: parent has ONLY backup_policy (no origins row); child topic 30 has origins row inheriting parent.
        self.store.set_backup_policy(
            BackupPolicyRecord(SOURCE_TELEGRAM, "main", -4011, enabled=True, tags="web3")
        )
        self.store.upsert_origin(
            OriginRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-4011,
                topic_id=30,
                parent_origin_id=-4011,
                origin_type="topic",
                title="Inherited Without Parent Row",
            )
        )
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                chat_id=-4011,
                topic_id=30,
                message_id=30,
                sent_at="2026-07-03T02:00:00+00:00",
                ingested_at=utc_now_iso(),
                text="inherited topic msg",
            ),
            event_type="new",
        )
        topic30_pkg = build_daily_package(
            self.store,
            self.config,
            run_date="2026-07-03",
            timezone_name="UTC",
            scope={"account_id": "main", "origin_id": -4011, "topic_id": 30, "tags": "web3"},
        )
        topic30_payload = json.loads(Path(topic30_pkg["package_json_path"]).read_text(encoding="utf-8"))
        self.assertEqual(
            [m["message_id"] for o in topic30_payload["point_origins"] for m in o["messages"]],
            [30],
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

    def test_codex_limit_preflight_activates_fallback_without_starting_codex(self) -> None:
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
                    command=["codex", "exec", "-"],
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

        with (
            patch(
                "tele_mess_core.daily._read_codex_limit_status",
                return_value=CodexRateLimitStatus(True, "codex:rate_limit_reached"),
            ) as probe,
            patch("tele_mess_core.daily._run_codex_provider") as codex,
            patch(
                "tele_mess_core.daily._run_openai_fallback_provider",
                side_effect=fake_fallback,
            ) as fallback,
        ):
            first = _run_summary_provider(
                config,
                "first task",
                self.root / "preflight-first.md",
                [],
                provider_state=state,
            )
            second = _run_summary_provider(
                config,
                "second task",
                self.root / "preflight-second.md",
                [],
                provider_state=state,
            )

        self.assertEqual(first.content, "fallback output")
        self.assertEqual(second.content, "fallback output")
        self.assertTrue(state.fallback_active)
        self.assertIn("rate_limit_reached", str(state.fallback_reason))
        probe.assert_called_once()
        codex.assert_not_called()
        self.assertEqual(fallback.call_count, 2)

    def test_generic_codex_failure_uses_fresh_limit_snapshot_as_confirmation(self) -> None:
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
                    command=["codex", "exec", "-"],
                    fallback=DailyAiFallbackConfig(
                        enabled=True,
                        base_url="https://fallback.example/v1",
                        api_key_file=key_path,
                    ),
                ),
            ),
        )
        state = AiProviderRuntimeState()
        generic_error = AiProviderError(
            "Codex CLI failed with exit code 1",
            kind="codex_failed",
            provider="codex-cli",
        )

        with (
            patch(
                "tele_mess_core.daily._read_codex_limit_status",
                side_effect=[
                    CodexRateLimitStatus(False),
                    CodexRateLimitStatus(True, "codex:secondary_window_reached"),
                ],
            ) as probe,
            patch("tele_mess_core.daily._run_codex_provider", side_effect=generic_error),
            patch(
                "tele_mess_core.daily._run_openai_fallback_provider",
                return_value=AiProviderResult(
                    content="confirmed fallback",
                    provider="openai-compatible:deepseek-v4-flash",
                ),
            ) as fallback,
        ):
            result = _run_summary_provider(
                config,
                "task",
                self.root / "confirmed.md",
                [],
                provider_state=state,
            )

        self.assertEqual(result.content, "confirmed fallback")
        self.assertTrue(state.fallback_active)
        self.assertIn("secondary_window_reached", str(state.fallback_reason))
        self.assertEqual(probe.call_count, 2)
        fallback.assert_called_once()

    def test_codex_errors_are_classified_without_persisting_prompt_or_key(self) -> None:
        usage = _codex_provider_error(
            "private Telegram body\nERROR: You've hit your usage limit and try again at 11:33 PM.",
            returncode=1,
        )
        self.assertEqual(usage.kind, "codex_usage_limit")
        self.assertTrue(usage.retryable)
        self.assertNotIn("Telegram", str(usage))

        for detail in (
            "You've hit your usage limit for GPT-5.6 Sol. Switch to another model now.",
            "You've hit your usage limit.",
            "You’ve hit your usage limit.",
            '{"code":"usage_limit_exceeded"}',
            '{"rateLimitReachedType":"workspace_owner_credits_depleted"}',
        ):
            with self.subTest(detail=detail):
                variant = _codex_provider_error(detail, returncode=1)
                self.assertEqual(variant.kind, "codex_usage_limit")
                self.assertTrue(variant.retryable)

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

    def _configure_main_account(self) -> None:
        self.config.telegram.accounts.append(
            TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="test",
                session_name="main",
                session_dir=self.root / "sessions",
            )
        )

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
