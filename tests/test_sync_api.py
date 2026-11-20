from __future__ import annotations

import json
from http.client import HTTPConnection
import tempfile
import time
import unittest
from unittest.mock import Mock
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import (
    AppConfig,
    DailyAiConfig,
    DailyPackagingConfig,
    LoggingConfig,
    ServerConfig,
    StorageConfig,
    TelegramAccountConfig,
    TelegramConfig,
)
from tele_mess_core.models import (
    AccountRecord,
    CaptureCursorRecord,
    ChatRecord,
    DailyMessagePointRecord,
    DailyPackageRunRecord,
    DailySummaryRunRecord,
    MediaFileRecord,
    MessageRecord,
    OperationEventRecord,
    SOURCE_TELEGRAM,
    utc_now_iso,
)
from tele_mess_core.daily_jobs import DailyJobWorker
from tele_mess_core.server import SyncApiServer
from tele_mess_core.server.api import (
    MAX_JSON_BODY_BYTES,
    _account_config,
    _auth_status,
    _discover_origins,
    _refresh_participants,
    _request_auth_code,
    _submit_auth_code,
)
from tele_mess_core.server.contracts import API_CONTRACT_HASH, API_CONTRACT_VERSION, API_ENDPOINTS


class SyncApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = ArchiveStore(Path(self.tmp.name) / "archive.db")
        self.store.initialize()
        now = utc_now_iso()
        self.store.upsert_chat(ChatRecord(source=SOURCE_TELEGRAM, chat_id=-1001, title="API Chat"))
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                chat_id=-1001,
                message_id=1,
                sent_at=now,
                ingested_at=now,
                text="api payload",
            ),
            event_type="new",
        )
        self.config = AppConfig(
            storage=StorageConfig(data_dir=Path(self.tmp.name), database=Path(self.tmp.name) / "archive.db"),
            telegram=TelegramConfig(),
            server=ServerConfig(),
            logging=LoggingConfig(file=None),
            daily=DailyPackagingConfig(
                output_dir=Path(self.tmp.name) / "daily-packages",
                systemd_user_dir=Path(self.tmp.name) / "systemd-user",
                ai=DailyAiConfig(provider="disabled"),
            ),
            config_path=Path(self.tmp.name) / "config.yml",
        )
        self.daily_worker = DailyJobWorker(self.store, self.config, poll_interval=0.01)
        self.daily_worker.start()
        self.api = SyncApiServer(
            self.store,
            "127.0.0.1",
            0,
            token="secret",
            config=self.config,
            daily_worker=self.daily_worker,
        )
        self.api.start_background()
        deadline = time.time() + 2
        while self.api._httpd is None and time.time() < deadline:
            time.sleep(0.01)
        assert self.api._httpd is not None
        self.port = self.api._httpd.server_address[1]

    def tearDown(self) -> None:
        self.api.stop()
        self.daily_worker.stop()
        self.store.close()
        self.tmp.cleanup()

    def request_json(self, path: str, method: str = "GET", payload: dict | None = None) -> dict:
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
        req = Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method)
        req.add_header("Authorization", "Bearer secret")
        if payload is not None:
            req.add_header("Content-Type", "application/json")
        with urlopen(req, timeout=3) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def request_text(self, path: str) -> str:
        req = Request(f"http://127.0.0.1:{self.port}{path}")
        req.add_header("Authorization", "Bearer secret")
        with urlopen(req, timeout=3) as resp:
            return resp.read().decode("utf-8")

    def request_text_no_auth(self, path: str) -> str:
        req = Request(f"http://127.0.0.1:{self.port}{path}")
        with urlopen(req, timeout=3) as resp:
            return resp.read().decode("utf-8")

    def request_bytes(self, path: str) -> tuple[bytes, str]:
        req = Request(f"http://127.0.0.1:{self.port}{path}")
        req.add_header("Authorization", "Bearer secret")
        with urlopen(req, timeout=3) as resp:
            return resp.read(), resp.headers.get("Content-Type", "")

    def test_state_requires_token_and_returns_json(self) -> None:
        req = Request(f"http://127.0.0.1:{self.port}/sync/state")
        req.add_header("Authorization", "Bearer secret")
        with urlopen(req, timeout=3) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        self.assertEqual(payload["last_event_seq"], 1)

    def test_messages_endpoint(self) -> None:
        req = Request(f"http://127.0.0.1:{self.port}/sync/messages?after=0")
        req.add_header("X-Api-Token", "secret")
        with urlopen(req, timeout=3) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        self.assertEqual(payload["items"][0]["text"], "api payload")
        self.assertEqual(payload["items"][0]["chat_title"], "API Chat")

    def test_search_endpoint_treats_invalid_fts_syntax_as_literal_text(self) -> None:
        now = utc_now_iso()
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                chat_id=-1001,
                message_id=99,
                sent_at=now,
                ingested_at=now,
                text="release-notes contain hello OR",
            ),
            event_type="new",
        )

        result = self.request_json(f"/sync/search?q={quote('release-notes', safe='')}")
        empty = self.request_json(f"/sync/search?q={quote(chr(34), safe='')}")
        wildcard = self.request_json(f"/sync/search?q={quote('*', safe='')}")
        nul = self.request_json("/sync/search?q=%00")

        self.assertEqual(result["items"][0]["message_id"], 99)
        self.assertEqual(empty["items"], [])
        self.assertEqual(wildcard["items"], [])
        self.assertEqual(nul["items"], [])

    def test_latest_messages_endpoint(self) -> None:
        now = utc_now_iso()
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                chat_id=-1001,
                message_id=2,
                sent_at=now,
                ingested_at=now,
                text="newer payload",
            ),
            event_type="new",
        )

        req = Request(f"http://127.0.0.1:{self.port}/sync/messages?latest=true&limit=1")
        req.add_header("X-Api-Token", "secret")
        with urlopen(req, timeout=3) as resp:
            payload = json.loads(resp.read().decode("utf-8"))

        self.assertEqual(payload["items"][0]["text"], "newer payload")
        self.assertEqual(payload["items"][0]["chat_title"], "API Chat")

    def test_list_endpoints_apply_the_contract_default_limit(self) -> None:
        for index in range(120):
            self.store.upsert_daily_package_run(
                DailyPackageRunRecord(
                    run_id=f"pkg_{index:03d}",
                    status="completed",
                    date="2026-07-03",
                    timezone="UTC",
                    started_at=f"2026-07-03T00:{index // 60:02d}:{index % 60:02d}+00:00",
                )
            )
        endpoint = next(
            item for item in API_ENDPOINTS if (item.method, item.path) == ("GET", "/manage/daily-package-runs")
        )
        default_limit = next(item.default for item in endpoint.query if item.name == "limit")

        items = self.request_json("/manage/daily-package-runs")["items"]

        self.assertEqual(len(items), min(120, default_limit))

    def test_accounts_endpoint(self) -> None:
        req = Request(f"http://127.0.0.1:{self.port}/sync/accounts")
        req.add_header("Authorization", "Bearer secret")
        with urlopen(req, timeout=3) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        self.assertEqual(payload["items"], [])

    def test_console_endpoint(self) -> None:
        html = self.request_text_no_auth("/console")
        self.assertIn("tele-mess-core", html)
        self.assertIn("teleMessToken", html)
        self.assertIn('data-view="accounts"', html)
        self.assertIn('data-view="daily"', html)
        self.assertIn('data-view="people">Members', html)
        self.assertIn("/manage/accounts/auth/request-code", html)
        self.assertIn("/manage/accounts/auth/submit-code", html)
        self.assertIn("/manage/discover-origins", html)
        self.assertIn("/manage/backup-policies", html)
        self.assertIn("/manage/participants/refresh", html)
        self.assertIn("/manage/operation-events", html)
        self.assertIn("/sync/media-files", html)
        self.assertIn("/manage/api-manifest", html)
        self.assertIn("teleMessApiContractHash", html)
        self.assertIn("API contract", html)
        self.assertIn("API docs", html)
        self.assertIn("contract_hash", html)
        self.assertIn("sessionStorage.getItem('teleMessToken')", html)
        self.assertNotIn("localStorage.getItem('teleMessToken')", html)

        connection = HTTPConnection("127.0.0.1", self.port, timeout=3)
        connection.request("GET", "/console")
        response = connection.getresponse()
        response.read()
        self.assertEqual(response.getheader("X-Content-Type-Options"), "nosniff")
        self.assertIn("frame-ancestors 'none'", response.getheader("Content-Security-Policy") or "")
        connection.close()

        self.assertIn("Save policy", html)
        self.assertIn('name="download_stickers"', html)
        self.assertIn("Save stickers as media", html)
        self.assertIn("escapeHtml", html)
        self.assertIn("API token required", html)
        self.assertIn("Enter server.token", html)
        self.assertIn('data-action="delete-account"', html)
        self.assertIn("remove-origin", html)
        self.assertIn("restore-origin", html)
        self.assertIn('data-action="refresh-origins"', html)
        self.assertIn('data-action="reload-origins"', html)
        self.assertIn('data-action="toggle-origin-manage"', html)
        self.assertIn('data-action="bulk-remove-origins"', html)
        self.assertIn('data-action="bulk-restore-origins"', html)
        self.assertIn('data-action="bulk-clear-policies"', html)
        self.assertIn("originSelectionAnchor", html)
        self.assertIn("selectOriginRange", html)
        self.assertIn("toggleOriginRowKey", html)
        self.assertIn("orphanTopics", html)
        self.assertIn("origin-dragging", html)
        self.assertIn("event.ctrlKey || event.metaKey", html)
        self.assertIn("event?.shiftKey", html)
        self.assertIn('data-action="delete-policy"', html)
        self.assertIn('data-action="delete-participant"', html)
        self.assertIn('id="service-panel"', html)
        self.assertIn("messages-panel", html)
        self.assertIn("syncOverviewHeights", html)
        self.assertIn("/sync/messages?latest=true&limit=100&include_media=true", html)
        self.assertIn("/sync/media-files/content", html)
        self.assertIn("previewMedia", html)
        self.assertIn("mediaObjectUrl", html)
        self.assertIn("media_files", html)
        self.assertIn("startMessageAutoRefresh", html)
        self.assertIn("stopMessageAutoRefresh", html)
        self.assertIn("operationEvents", html)
        self.assertIn("renderOperationEvents", html)
        self.assertIn('data-action="delete-operation-event"', html)
        self.assertIn("messageChatLabel", html)
        self.assertIn("data-tag-editor", html)
        self.assertIn("data-tag-remove", html)
        self.assertIn('id="tag-suggestions"', html)
        self.assertIn("setupTagEditor", html)
        self.assertNotIn("Manual Origin", html)
        self.assertIn('id="origin-search"', html)
        self.assertIn('id="origin-type-filter"', html)
        self.assertIn('id="origin-backup-filter"', html)
        self.assertIn('id="origin-tag-filter"', html)
        self.assertIn('id="origin-sort"', html)
        self.assertIn("filteredOrigins", html)
        self.assertIn("Last message", html)
        self.assertIn("Removed", html)
        self.assertIn("Tags", html)
        self.assertIn("Important", html)
        self.assertIn("/manage/origins/important", html)
        self.assertIn("/manage/daily-package-schedule", html)
        self.assertIn("/manage/daily-summary-delivery", html)
        self.assertIn("/manage/daily-packages", html)
        self.assertIn("/manage/daily-summaries", html)
        self.assertIn("/manage/daily-summary-jobs", html)
        self.assertIn("/manage/daily-summary-jobs/cancel", html)
        self.assertIn("/manage/daily-summary-records", html)
        self.assertIn("daily-summary-jobs-body", html)
        self.assertIn("daily-summary-records-body", html)
        self.assertIn("startDailyAutoRefresh", html)
        self.assertIn("daily-schedule-form", html)
        self.assertIn("Daily Runs", html)
        self.assertIn("policy-row", html)
        self.assertIn("button.closest('tr').after(row)", html)
        self.assertIn(".raw-panel", html)
        self.assertIn("height: calc(100vh - 170px)", html)
        self.assertIn("<th>Actions</th>", html)
        self.assertIn(".table-wrap thead th", html)
        self.assertIn("top: 0", html)
        self.assertNotIn("top: 57px", html)

    def test_contract_route_registry_rejects_wrong_method(self) -> None:
        connection = HTTPConnection("127.0.0.1", self.port, timeout=3)
        connection.request("POST", "/healthz", headers={"Authorization": "Bearer secret"})
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
        self.assertEqual(response.status, 405)
        self.assertEqual(payload["error"], "method_not_allowed")
        connection.close()

    def test_contract_rejects_body_type_drift(self) -> None:
        connection = HTTPConnection("127.0.0.1", self.port, timeout=3)
        body = json.dumps({"account_id": 123})
        connection.request(
            "POST",
            "/manage/accounts",
            body=body,
            headers={"Authorization": "Bearer secret", "Content-Type": "application/json"},
        )
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
        self.assertEqual(response.status, 400)
        self.assertIn("body.account_id must be a string", payload["detail"])
        connection.close()

    def test_root_serves_console_without_token(self) -> None:
        html = self.request_text_no_auth("/")
        self.assertIn("tele-mess-core console", html)
        self.assertIn("API token", html)

    def test_runtime_contract_docs_endpoints(self) -> None:
        manifest = self.request_json("/manage/api-manifest")
        self.assertEqual(manifest["contract_version"], API_CONTRACT_VERSION)
        self.assertEqual(manifest["contract_hash"], API_CONTRACT_HASH)
        self.assertIn({"method": "GET", "path": "/sync/messages"}, [
            {"method": item["method"], "path": item["path"]} for item in manifest["endpoints"]
        ])
        self.assertEqual(manifest["openapi_url"], "/openapi.json")
        self.assertEqual(manifest["markdown_url"], "/docs/api.md")

        openapi_text = self.request_text_no_auth("/openapi.json")
        openapi = json.loads(openapi_text)
        self.assertEqual(openapi["info"]["x-contract-hash"], API_CONTRACT_HASH)
        self.assertIn("/manage/api-manifest", openapi["paths"])
        self.assertIn("/sync/messages", openapi["paths"])
        self.assertIn("/manage/daily-summary-delivery", openapi["paths"])

        markdown = self.request_text_no_auth("/docs/api.md")
        self.assertIn(f"Contract hash: `{API_CONTRACT_HASH}`", markdown)
        self.assertIn("GET /manage/api-manifest", markdown)
        self.assertIn("POST /manage/discover-origins", markdown)

    def test_daily_package_and_summary_endpoints(self) -> None:
        self.store.upsert_message(
            MessageRecord(
                source=SOURCE_TELEGRAM,
                account_id="default",
                chat_id=-2001,
                message_id=77,
                sent_at="2026-07-03T01:00:00+00:00",
                ingested_at=utc_now_iso(),
                text="daily api payload",
            ),
            event_type="new",
        )
        self.request_json(
            "/manage/origins",
            method="POST",
            payload={"account_id": "default", "origin_id": -2001, "origin_type": "group", "title": "Daily API"},
        )
        important = self.request_json(
            "/manage/origins/important",
            method="PATCH",
            payload={"account_id": "default", "origin_id": -2001, "important": True},
        )["item"]
        self.assertTrue(important["important"])
        self.request_json(
            "/manage/backup-policies",
            method="PATCH",
            payload={"account_id": "default", "origin_id": -2001, "enabled": True, "tags": "web3,info"},
        )

        schedule = self.request_json(
            "/manage/daily-package-schedule",
            method="PATCH",
            payload={
                "enabled": True,
                "time_of_day": "08:30",
                "timezone": "UTC",
                "scope": {"tag_groups": ["web3 info"]},
            },
        )["item"]
        self.assertTrue(schedule["installed"])
        self.assertEqual(schedule["time_of_day"], "08:30")

        package = self.request_json(
            "/manage/daily-packages",
            method="POST",
            payload={"date": "2026-07-03", "timezone": "UTC", "tag_groups": ["web3 info"]},
        )["item"]
        self.assertEqual(package["status"], "completed")
        self.assertEqual(package["message_count"], 1)
        self.assertEqual(package["progress_current"], package["progress_total"])
        self.assertEqual(package["progress_label"], "completed")
        package_md = self.request_text(f"/manage/daily-package-runs/content?run_id={package['run_id']}&format=md")
        self.assertIn("Daily Package 2026-07-03", package_md)

        summary = self.request_json(
            "/manage/daily-summaries",
            method="POST",
            payload={"package_run_id": package["run_id"], "background": False},
        )["item"]
        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["progress_current"], summary["progress_total"])
        self.assertEqual(summary["progress_label"], "completed")
        summary_md = self.request_text(f"/manage/daily-summary-runs/content?run_id={summary['run_id']}")
        self.assertIn("AI provider is disabled", summary_md)
        important_records = self.request_json(
            f"/manage/daily-summary-records?package_run_id={package['run_id']}&tag=web3&tags=info&important=true"
        )["items"]
        self.assertEqual(
            {item["record_type"] for item in important_records},
            {"important_origin", "important_daily"},
        )
        self.assertTrue(all(item["run_id"] == summary["run_id"] for item in important_records))
        self.assertTrue(all("AI provider is disabled" in item["content_preview"] for item in important_records))
        self.assertTrue(all("content_md" not in item for item in important_records))

        point_records = self.request_json(
            f"/manage/daily-summary-records?package_run_id={package['run_id']}&record_type=point_daily&tag=point"
        )["items"]
        self.assertEqual(len(point_records), 1)
        point_record = point_records[0]
        record = self.request_json(
            f"/manage/daily-summary-records/item?run_id={summary['run_id']}&record_type=point_daily"
        )["item"]
        self.assertEqual(record["record_type"], "point_daily")
        self.assertIn("No message points were extracted", record["content_md"])
        self.assertEqual(record["tags"], ["point"])

        deleted = self.request_json(
            "/manage/daily-summary-records",
            method="PATCH",
            payload={"summary_ids": [point_record["summary_id"]], "deleted": True},
        )["item"]
        self.assertEqual(deleted["changed_rows"], 1)
        hidden_records = self.request_json(
            f"/manage/daily-summary-records?summary_id={point_record['summary_id']}"
        )["items"]
        self.assertEqual(hidden_records, [])
        deleted_records = self.request_json(
            f"/manage/daily-summary-records?summary_id={point_record['summary_id']}&deleted=true"
        )["items"]
        self.assertEqual(len(deleted_records), 1)
        self.assertTrue(deleted_records[0]["deleted"])

        restored = self.request_json(
            "/manage/daily-summary-records",
            method="PATCH",
            payload={"summary_id": point_record["summary_id"], "deleted": False},
        )["item"]
        self.assertEqual(restored["changed_rows"], 1)

        job = self.request_json(
            "/manage/daily-summary-jobs",
            method="POST",
            payload={"date": "2026-07-03", "timezone": "UTC", "tag_groups": ["web3 info"]},
        )["item"]
        self.assertIn(job["status"], {"queued", "running", "completed"})
        deadline = time.time() + 3
        job_items = []
        while time.time() < deadline:
            job_items = self.request_json(f"/manage/daily-summary-jobs?job_id={job['job_id']}")["items"]
            if job_items and job_items[0]["status"] == "completed":
                break
            time.sleep(0.05)
        self.assertEqual(job_items[0]["status"], "completed")
        self.assertTrue(job_items[0]["package_run_id"].startswith("pkg_"))
        self.assertTrue(job_items[0]["summary_run_id"].startswith("sum_"))
        self.assertEqual(job_items[0]["progress_label"], "completed")

    def test_daily_summary_delivery_api_and_schedule_compatibility(self) -> None:
        initial = self.request_json("/manage/daily-summary-delivery")["item"]
        self.assertFalse(initial["enabled"])
        self.assertEqual(initial["source"], "config")
        self.config.telegram.accounts.append(
            TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="test",
                session_name="main",
                session_dir=Path(self.tmp.name) / "sessions",
            )
        )

        saved = self.request_json(
            "/manage/daily-summary-delivery",
            method="PATCH",
            payload={"enabled": True, "account_id": "main", "origin_id": -9001, "topic_id": 42},
        )["item"]
        self.assertTrue(saved["enabled"])
        self.assertEqual(saved["origin_id"], -9001)
        self.assertEqual(saved["topic_id"], 42)
        self.assertEqual(saved["source"], "database")
        self.assertEqual(self.request_json("/manage/daily-summary-delivery")["item"], saved)

        schedule = self.request_json(
            "/manage/daily-package-schedule",
            method="PATCH",
            payload={
                "enabled": True,
                "time_of_day": "08:00",
                "timezone": "UTC",
                "scope": {},
                "delivery": {"enabled": True, "account_id": "main", "origin_id": -9002, "topic_id": 99},
            },
        )["item"]
        self.assertEqual(schedule["delivery"]["origin_id"], -9002)
        self.assertEqual(schedule["delivery"]["topic_id"], 99)

        req = Request(
            f"http://127.0.0.1:{self.port}/manage/daily-package-schedule",
            data=json.dumps({"enabled": True, "unknown_delivery_field": "ignored-before"}).encode("utf-8"),
            method="PATCH",
        )
        req.add_header("Authorization", "Bearer secret")
        req.add_header("Content-Type", "application/json")
        with self.assertRaises(HTTPError) as raised:
            urlopen(req, timeout=3)
        self.assertEqual(raised.exception.code, 400)
        raised.exception.close()

    def test_daily_message_points_api_filters_and_item(self) -> None:
        self.store.upsert_daily_summary_run(
            DailySummaryRunRecord(
                run_id="sum_points_api",
                status="completed",
                package_run_id="pkg_points_api",
                date="2026-07-03",
                timezone="UTC",
            )
        )
        self.store.persist_daily_message_points(
            "sum_points_api",
            [
                DailyMessagePointRecord(
                    point_id="point_api",
                    run_id="sum_points_api",
                    package_run_id="pkg_points_api",
                    date="2026-07-03",
                    timezone="UTC",
                    source=SOURCE_TELEGRAM,
                    account_id="default",
                    origin_id=-2001,
                    topic_id=8,
                    origin_title="API Points",
                    message_id=77,
                    occurred_at="2026-07-03T01:00:00+00:00",
                    tags_json='["ai", "point"]',
                    content="API point payload",
                    telegram_deeplink="tg://privatepost?channel=2001&post=77",
                    importance_score=5,
                    importance_reason="API-visible",
                    origin_important=True,
                    source_refs_json='[{"message_id":77}]',
                    provider="fake",
                )
            ],
        )

        items = self.request_json(
            "/manage/daily-message-points?date=2026-07-03&account_id=default&origin_id=-2001"
            "&topic_id=8&tags=AI,point&importance_min=4&origin_important=true&q=payload"
        )["items"]
        self.assertEqual([item["point_id"] for item in items], ["point_api"])
        self.assertEqual(items[0]["source_refs"], [{"message_id": 77}])
        self.assertTrue(items[0]["origin_important"])
        self.assertEqual(items[0]["run_status"], "completed")

        item = self.request_json("/manage/daily-message-points/item?point_id=point_api")["item"]
        self.assertEqual(item["telegram_deeplink"], "tg://privatepost?channel=2001&post=77")

        self.store.upsert_daily_summary_run(
            DailySummaryRunRecord(
                run_id="sum_points_failed_api",
                status="failed",
                package_run_id="pkg_points_failed_api",
                date="2026-07-03",
                timezone="UTC",
            )
        )
        self.store.persist_daily_message_points(
            "sum_points_failed_api",
            [
                DailyMessagePointRecord(
                    point_id="point_failed_api",
                    run_id="sum_points_failed_api",
                    package_run_id="pkg_points_failed_api",
                    date="2026-07-03",
                    timezone="UTC",
                    source=SOURCE_TELEGRAM,
                    account_id="default",
                    origin_id=-2001,
                    occurred_at="2026-07-03T02:00:00+00:00",
                    content="failed API point",
                )
            ],
        )
        default_ids = {
            point["point_id"]
            for point in self.request_json("/manage/daily-message-points?date=2026-07-03")["items"]
        }
        self.assertNotIn("point_failed_api", default_ids)
        diagnostic_ids = {
            point["point_id"]
            for point in self.request_json(
                "/manage/daily-message-points?date=2026-07-03&include_incomplete=true"
            )["items"]
        }
        self.assertIn("point_failed_api", diagnostic_ids)

    def test_api_manifest_requires_token(self) -> None:
        req = Request(f"http://127.0.0.1:{self.port}/manage/api-manifest")
        with self.assertRaises(HTTPError) as caught:
            urlopen(req, timeout=3)
        self.assertEqual(caught.exception.code, 401)
        caught.exception.close()

    def test_media_files_endpoint(self) -> None:
        media_path = Path(self.tmp.name) / "api-media.jpg"
        media_path.write_bytes(b"fake-jpeg")
        self.store.upsert_media_file(
            MediaFileRecord(
                source=SOURCE_TELEGRAM,
                account_id="default",
                chat_id=-1001,
                message_id=1,
                file_path=str(media_path),
                media_kind="photo",
                mime_type="image/jpeg",
                file_size=9,
                downloaded_at=utc_now_iso(),
            )
        )

        payload = self.request_json("/sync/media-files")
        self.assertEqual(payload["items"][0]["chat_title"], "API Chat")
        self.assertEqual(payload["items"][0]["content_type"], "image/jpeg")
        self.assertEqual(payload["items"][0]["preview_kind"], "image")
        self.assertEqual(payload["items"][0]["filename"], "api-media.jpg")
        self.assertEqual(payload["items"][0]["file_extension"], "jpg")
        self.assertEqual(payload["items"][0]["media_type"], "image")
        self.assertIn("/sync/media-files/content?", payload["items"][0]["access_url"])
        self.assertIn("message_id=1", payload["items"][0]["access_url"])

    def test_media_files_endpoint_applies_server_side_filters(self) -> None:
        for message_id, filename in ((10, "Quarterly_Report.MD"), (11, "photo.jpg")):
            self.store.upsert_media_file(
                MediaFileRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="default",
                    chat_id=-1001,
                    message_id=message_id,
                    file_path=str(Path(self.tmp.name) / filename),
                    media_kind="MessageMediaDocument",
                    downloaded_at=utc_now_iso(),
                )
            )

        payload = self.request_json(
            "/sync/media-files?account_id=default&chat_id=-1001&message_id=10"
            "&filename_query=quarterly&media_type=text&file_extension=.MD&limit=500"
        )

        self.assertEqual([item["message_id"] for item in payload["items"]], [10])
        self.assertEqual(payload["items"][0]["filename"], "Quarterly_Report.MD")
        self.assertEqual(payload["items"][0]["file_extension"], "md")
        self.assertEqual(payload["items"][0]["media_type"], "text")

    def test_media_files_endpoint_rejects_invalid_media_type(self) -> None:
        req = Request(f"http://127.0.0.1:{self.port}/sync/media-files?media_type=archive")
        req.add_header("Authorization", "Bearer secret")

        with self.assertRaises(HTTPError) as caught:
            urlopen(req, timeout=3)

        self.assertEqual(caught.exception.code, 400)
        payload = json.loads(caught.exception.read().decode("utf-8"))
        caught.exception.close()
        self.assertEqual(payload["error"], "bad_request")
        self.assertIn("media_type must be one of", payload["detail"])

    def test_media_file_content_endpoint_serves_registered_file(self) -> None:
        media_path = Path(self.tmp.name) / "api-media.txt"
        media_path.write_bytes(b"registered media")
        self.store.upsert_media_file(
            MediaFileRecord(
                source=SOURCE_TELEGRAM,
                account_id="default",
                chat_id=-1001,
                message_id=1,
                file_path=str(media_path),
                media_kind="document",
                mime_type="text/plain",
                file_size=16,
                downloaded_at=utc_now_iso(),
            )
        )

        body, content_type = self.request_bytes(
            "/sync/media-files/content?source=telegram&account_id=default&chat_id=-1001&message_id=1&file_index=0"
        )

        self.assertEqual(body, b"registered media")
        self.assertEqual(content_type, "text/plain")

    def test_media_file_content_endpoint_supports_unicode_filename(self) -> None:
        media_path = Path(self.tmp.name) / "日本語 media.png"
        content = b"unicode filename media body"
        media_path.write_bytes(content)
        self.store.upsert_media_file(
            MediaFileRecord(
                source=SOURCE_TELEGRAM,
                account_id="default",
                chat_id=-1001,
                message_id=1,
                file_path=str(media_path),
                media_kind="photo",
                mime_type="image/png",
                file_size=len(content),
                downloaded_at=utc_now_iso(),
            )
        )

        req = Request(
            f"http://127.0.0.1:{self.port}/sync/media-files/content"
            "?source=telegram&account_id=default&chat_id=-1001&message_id=1&file_index=0"
        )
        req.add_header("Authorization", "Bearer secret")
        with urlopen(req, timeout=3) as resp:
            body = resp.read()
            disposition = resp.headers.get("Content-Disposition", "")
            status = resp.status

        self.assertEqual(status, 200)
        self.assertEqual(body, content)
        self.assertTrue(disposition.isascii())
        self.assertIn('filename="___ media.png"', disposition)
        self.assertIn(f"filename*=UTF-8''{quote(media_path.name, safe='')}", disposition)

    def test_media_file_content_endpoint_rejects_unsafe_mime_header_value(self) -> None:
        media_path = Path(self.tmp.name) / "safe-content.txt"
        content = b"safe media body"
        media_path.write_bytes(content)
        self.store.upsert_media_file(
            MediaFileRecord(
                source=SOURCE_TELEGRAM,
                account_id="default",
                chat_id=-1001,
                message_id=2,
                file_path=str(media_path),
                media_kind="document",
                mime_type="text/plain\r\nX-Injected: yes",
                file_size=len(content),
                downloaded_at=utc_now_iso(),
            )
        )

        req = Request(
            f"http://127.0.0.1:{self.port}/sync/media-files/content"
            "?source=telegram&account_id=default&chat_id=-1001&message_id=2&file_index=0"
        )
        req.add_header("Authorization", "Bearer secret")
        with urlopen(req, timeout=3) as resp:
            body = resp.read()
            content_type = resp.headers.get("Content-Type")
            injected = resp.headers.get("X-Injected")

        self.assertEqual(body, content)
        self.assertEqual(content_type, "text/plain")
        self.assertIsNone(injected)

    def test_media_file_content_endpoint_requires_token(self) -> None:
        req = Request(
            f"http://127.0.0.1:{self.port}/sync/media-files/content?source=telegram&account_id=default&chat_id=-1001&message_id=1&file_index=0"
        )
        with self.assertRaises(HTTPError) as caught:
            urlopen(req, timeout=3)
        self.assertEqual(caught.exception.code, 401)
        caught.exception.close()

    def test_messages_endpoint_can_include_media_files(self) -> None:
        media_path = Path(self.tmp.name) / "api-media.bin"
        media_path.write_bytes(b"payload")
        self.store.upsert_media_file(
            MediaFileRecord(
                source=SOURCE_TELEGRAM,
                account_id="default",
                chat_id=-1001,
                message_id=1,
                file_path=str(media_path),
                media_kind="document",
                file_size=7,
                downloaded_at=utc_now_iso(),
            )
        )

        payload = self.request_json("/sync/messages?latest=true&limit=1&include_media=true")

        self.assertEqual(payload["items"][0]["media_count"], 1)
        self.assertEqual(payload["items"][0]["media_files"][0]["message_id"], 1)
        self.assertEqual(payload["items"][0]["media_files"][0]["filename"], "api-media.bin")
        self.assertEqual(payload["items"][0]["media_files"][0]["file_extension"], "bin")
        self.assertEqual(payload["items"][0]["media_files"][0]["media_type"], "document")
        self.assertIn("/sync/media-files/content?", payload["items"][0]["media_files"][0]["access_url"])

    def test_operation_events_endpoint(self) -> None:
        event_id = self.store.add_operation_event(
            OperationEventRecord(
                source=SOURCE_TELEGRAM,
                account_id="default",
                operation="backfill",
                status="failed",
                subject_type="message",
                subject_id="-1001/1",
                error_code="access_denied",
                message="private history",
                raw_json=json.dumps(
                    {
                        "auth_state": "authorized",
                        "code": "access_denied",
                        "message": "private history",
                        "type": "ChannelPrivateError",
                    }
                ),
            )
        )

        payload = self.request_json("/manage/operation-events?account_id=default&status=failed")

        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["operation"], "backfill")
        self.assertEqual(payload["items"][0]["error_code"], "access_denied")
        self.assertEqual(payload["items"][0]["error"]["type"], "ChannelPrivateError")
        self.assertEqual(payload["items"][0]["auth_state"], "authorized")
        self.assertEqual(payload["items"][0]["subject"]["chat_title"], "API Chat")
        self.assertEqual(payload["items"][0]["subject"]["message_id"], 1)
        self.assertEqual(payload["items"][0]["subject"]["text"], "api payload")

        deleted = self.request_json("/manage/operation-events", method="DELETE", payload={"id": event_id})["item"]

        self.assertEqual(deleted["deleted"], 1)
        self.assertEqual(self.request_json("/manage/operation-events?account_id=default&status=failed")["items"], [])

    def test_start_background_reports_bind_failure(self) -> None:
        second = SyncApiServer(self.store, "127.0.0.1", self.port, token="secret")
        with self.assertRaises(RuntimeError):
            second.start_background(startup_timeout=1)
        second.stop()

    def test_empty_token_requires_explicit_loopback_opt_in(self) -> None:
        with self.assertRaisesRegex(ValueError, "server.token is required"):
            SyncApiServer(self.store, "127.0.0.1", 0, token="")

        local = SyncApiServer(
            self.store,
            "127.0.0.1",
            0,
            token="",
            allow_unauthenticated_localhost=True,
        )
        local.stop()

        with self.assertRaisesRegex(ValueError, "only allowed on a loopback"):
            SyncApiServer(
                self.store,
                "0.0.0.0",
                0,
                token="",
                allow_unauthenticated_localhost=True,
            )

    def test_write_endpoints_reject_oversized_or_non_json_bodies(self) -> None:
        connection = HTTPConnection("127.0.0.1", self.port, timeout=3)
        connection.putrequest("POST", "/manage/accounts")
        connection.putheader("Authorization", "Bearer secret")
        connection.putheader("Content-Type", "application/json")
        connection.putheader("Content-Length", str(MAX_JSON_BODY_BYTES + 1))
        connection.endheaders()
        response = connection.getresponse()
        oversized = json.loads(response.read().decode("utf-8"))
        self.assertEqual(response.status, 413)
        self.assertEqual(oversized["error"], "payload_too_large")
        connection.close()

        req = Request(
            f"http://127.0.0.1:{self.port}/manage/accounts",
            data=b"{}",
            method="POST",
            headers={"Authorization": "Bearer secret", "Content-Type": "text/plain"},
        )
        with self.assertRaises(HTTPError) as caught:
            urlopen(req, timeout=3)
        self.assertEqual(caught.exception.code, 415)
        caught.exception.close()

    def test_management_endpoints_cover_goal_objects(self) -> None:
        account = self.request_json(
            "/manage/accounts",
            method="POST",
            payload={
                "account_id": "main",
                "display_name": "Main Account",
                "phone": "+10000000000",
                "session_name": "main",
                "auth_state": "pending_auth",
                "api_hash": "should-not-be-returned",
            },
        )["item"]
        self.assertEqual(account["auth_state"], "pending_auth")
        self.assertNotIn("api_hash", account["raw_json"])

        auth = self.request_json(
            "/manage/accounts/auth",
            method="PATCH",
            payload={"account_id": "main", "auth_state": "code_sent", "session_name": "main"},
        )["item"]
        self.assertEqual(auth["auth_state"], "code_sent")

        self.request_json(
            "/manage/origins",
            method="POST",
            payload={
                "account_id": "main",
                "origin_id": -1001,
                "origin_type": "group",
                "title": "Source Group",
                "is_forum": True,
                "last_message_at": "2026-01-01T00:00:00+00:00",
            },
        )
        topic = self.request_json(
            "/manage/origins",
            method="POST",
            payload={
                "account_id": "main",
                "origin_id": -1001,
                "topic_id": 123,
                "origin_type": "topic",
                "parent_origin_id": -1001,
                "title": "Announcements",
            },
        )["item"]
        self.assertEqual(topic["origin_type"], "topic")

        policy = self.request_json(
            "/manage/backup-policies",
            method="PATCH",
            payload={
                "account_id": "main",
                "origin_id": -1001,
                "topic_id": 123,
                "enabled": True,
                "capture_text": True,
                "capture_media_metadata": True,
                "download_media": False,
                "download_stickers": True,
                "tags": "alpha,beta",
            },
        )["item"]
        self.assertTrue(policy["enabled"])
        self.assertFalse(policy["download_media"])
        self.assertTrue(policy["download_stickers"])
        self.assertEqual(policy["tags"], "alpha,beta")

        listed_policy = next(
            item
            for item in self.request_json("/manage/backup-policies?account_id=main")["items"]
            if item["origin_id"] == -1001 and item["topic_id"] == 123
        )
        self.assertTrue(listed_policy["download_stickers"])

        legacy_patch = self.request_json(
            "/manage/backup-policies",
            method="PATCH",
            payload={
                "account_id": "main",
                "origin_id": -1001,
                "topic_id": 123,
                "enabled": True,
                "capture_text": True,
                "capture_media_metadata": True,
                "download_media": False,
                "tags": "alpha,beta",
            },
        )["item"]
        self.assertTrue(legacy_patch["download_stickers"])

        participant = self.request_json(
            "/manage/participants",
            method="POST",
            payload={
                "account_id": "main",
                "origin_id": -1001,
                "user_id": 42,
                "username": "alice",
                "display_name": "Alice",
                "role": "member",
            },
        )["item"]
        self.assertEqual(participant["username"], "alice")

        origins = self.request_json("/manage/origins?account_id=main")["items"]
        self.assertEqual(next(item for item in origins if item["topic_id"] == 0)["last_message_at"], "2026-01-01T00:00:00+00:00")
        saved_topic = next(item for item in origins if item["topic_id"] == 123)
        self.assertTrue(saved_topic["backup_policy"]["enabled"])
        self.assertTrue(saved_topic["backup_policy"]["download_stickers"])
        self.assertEqual(saved_topic["backup_policy"]["tags"], "alpha,beta")
        participants = self.request_json("/manage/participants?account_id=main&origin_id=-1001")["items"]
        self.assertEqual(participants[0]["display_name"], "Alice")

        self.store.upsert_capture_cursor(
            CaptureCursorRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                origin_id=-1001,
                observed_max_message_id=12,
                history_scanned_through_id=10,
                backfill_head_message_id=12,
                backfill_status="running",
                backfill_count=10,
            )
        )
        cursors = self.request_json("/manage/capture-cursors?account_id=main")["items"]
        self.assertEqual(len(cursors), 1)
        self.assertEqual(cursors[0]["observed_max_message_id"], 12)
        self.assertEqual(cursors[0]["history_scanned_through_id"], 10)
        self.assertEqual(cursors[0]["backfill_head_message_id"], 12)
        self.assertEqual(cursors[0]["backfill_status"], "running")
        self.assertEqual(cursors[0]["backfill_count"], 10)

        capabilities = self.request_json("/manage/capabilities")
        self.assertIn("origin_registry", capabilities["management"])
        self.assertIn("capture_cursors", capabilities["management"])
        self.assertIn("operation_events", capabilities["management"])
        self.assertIn("operation_event_delete", capabilities["management"])
        self.assertIn("daily_message_points", capabilities["management"])

        archive = self.request_json(
            "/manage/origins/archive",
            method="PATCH",
            payload={"account_id": "main", "origin_id": -1001, "archived": True},
        )["item"]
        self.assertTrue(archive["archived"])
        self.assertEqual(self.request_json("/manage/origins?account_id=main")["items"], [])
        archived = self.request_json("/manage/origins?account_id=main&include_archived=true")["items"]
        self.assertEqual(len(archived), 2)
        self.assertTrue(all(item["archived_at"] for item in archived))
        archived_topic = next(item for item in archived if item["topic_id"] == 123)
        self.assertFalse(archived_topic["backup_policy"]["enabled"])

        restore = self.request_json(
            "/manage/origins/archive",
            method="PATCH",
            payload={"account_id": "main", "origin_id": -1001, "archived": False},
        )["item"]
        self.assertFalse(restore["archived"])
        self.assertEqual(len(self.request_json("/manage/origins?account_id=main")["items"]), 2)

    def test_management_writes_return_the_row_they_wrote(self) -> None:
        self.request_json(
            "/manage/origins",
            method="POST",
            payload={"account_id": "main", "origin_id": -3001, "origin_type": "group", "title": "Archived"},
        )
        self.request_json(
            "/manage/origins/archive",
            method="PATCH",
            payload={"account_id": "main", "origin_id": -3001, "archived": True},
        )
        important = self.request_json(
            "/manage/origins/important",
            method="PATCH",
            payload={"account_id": "main", "origin_id": -3001, "important": True},
        )["item"]
        self.assertTrue(important["important"])
        self.assertIsNotNone(important["archived_at"])

        for source, title, enabled, display_name in (
            ("telegram", "Alpha", True, "Telegram user"),
            ("other", "Beta", False, "Other user"),
        ):
            origin = self.request_json(
                "/manage/origins",
                method="POST",
                payload={"source": source, "account_id": "main", "origin_id": -3002, "origin_type": "group", "title": title},
            )["item"]
            policy = self.request_json(
                "/manage/backup-policies",
                method="POST",
                payload={"source": source, "account_id": "main", "origin_id": -3002, "enabled": enabled},
            )["item"]
            participant = self.request_json(
                "/manage/participants",
                method="POST",
                payload={"source": source, "account_id": "main", "origin_id": -3002, "user_id": 7, "display_name": display_name},
            )["item"]
            self.assertEqual((origin["source"], origin["title"]), (source, title))
            self.assertEqual((policy["source"], policy["enabled"]), (source, enabled))
            self.assertEqual((participant["source"], participant["display_name"]), (source, display_name))

    def test_account_auth_patch_creates_a_missing_account(self) -> None:
        created = self.request_json(
            "/manage/accounts/auth",
            method="PATCH",
            payload={"account_id": "patched-first", "auth_state": "code_sent", "session_name": "patched-session"},
        )["item"]

        self.assertEqual(created["account_id"], "patched-first")
        self.assertEqual(created["auth_state"], "code_sent")
        self.assertEqual(created["session_name"], "patched-session")

    def test_account_auth_patch_merges_omitted_fields_and_clears_explicit_empty_values(self) -> None:
        created = self.request_json(
            "/manage/accounts/auth",
            method="POST",
            payload={
                "account_id": "partial-auth",
                "auth_state": "code_sent",
                "phone": "+10000000000",
                "session_name": "stable-session",
                "session_dir": "managed-sessions",
                "last_error": "temporary failure",
            },
        )["item"]
        expected_session_dir = str((Path(self.tmp.name) / "managed-sessions").resolve())
        self.assertEqual(created["session_dir"], expected_session_dir)

        patched = self.request_json(
            "/manage/accounts/auth",
            method="PATCH",
            payload={"account_id": "partial-auth", "auth_state": "authorized"},
        )["item"]

        self.assertEqual(patched["auth_state"], "authorized")
        self.assertEqual(patched["phone"], "+10000000000")
        self.assertEqual(patched["session_name"], "stable-session")
        self.assertEqual(patched["session_dir"], expected_session_dir)
        self.assertEqual(patched["last_error"], "temporary failure")

        cleared = self.request_json(
            "/manage/accounts/auth",
            method="PATCH",
            payload={
                "account_id": "partial-auth",
                "phone": None,
                "session_name": "",
                "session_dir": None,
                "last_error": "",
            },
        )["item"]

        self.assertEqual(cleared["auth_state"], "authorized")
        self.assertIsNone(cleared["phone"])
        self.assertIsNone(cleared["session_name"])
        self.assertIsNone(cleared["session_dir"])
        self.assertIsNone(cleared["last_error"])

        defaults = self.request_json(
            "/manage/accounts/auth",
            method="POST",
            payload={"account_id": "post-auth-defaults"},
        )["item"]
        self.assertEqual(defaults["auth_state"], "pending_auth")
        self.assertIsNone(defaults["phone"])
        self.assertIsNone(defaults["session_name"])
        self.assertIsNone(defaults["session_dir"])

    def test_backup_policy_patch_merges_omitted_fields_and_clears_tags(self) -> None:
        created = self.request_json(
            "/manage/backup-policies",
            method="POST",
            payload={
                "account_id": "main",
                "origin_id": -2001,
                "enabled": True,
                "capture_text": False,
                "capture_media_metadata": False,
                "download_media": True,
                "download_stickers": True,
                "tags": "keep-me",
            },
        )["item"]
        self.assertTrue(created["enabled"])

        patched = self.request_json(
            "/manage/backup-policies",
            method="PATCH",
            payload={"account_id": "main", "origin_id": -2001, "enabled": False},
        )["item"]

        self.assertFalse(patched["enabled"])
        self.assertFalse(patched["capture_text"])
        self.assertFalse(patched["capture_media_metadata"])
        self.assertTrue(patched["download_media"])
        self.assertTrue(patched["download_stickers"])
        self.assertEqual(patched["tags"], "keep-me")

        cleared = self.request_json(
            "/manage/backup-policies",
            method="PATCH",
            payload={"account_id": "main", "origin_id": -2001, "tags": None},
        )["item"]
        self.assertIsNone(cleared["tags"])
        self.assertFalse(cleared["enabled"])
        self.assertFalse(cleared["capture_text"])
        self.assertFalse(cleared["capture_media_metadata"])
        self.assertTrue(cleared["download_media"])
        self.assertTrue(cleared["download_stickers"])

        defaults = self.request_json(
            "/manage/backup-policies",
            method="POST",
            payload={"account_id": "main", "origin_id": -2002},
        )["item"]
        self.assertFalse(defaults["enabled"])
        self.assertTrue(defaults["capture_text"])
        self.assertTrue(defaults["capture_media_metadata"])
        self.assertFalse(defaults["download_media"])
        self.assertFalse(defaults["download_stickers"])
        self.assertIsNone(defaults["tags"])

    def test_management_delete_endpoints_remove_console_records(self) -> None:
        self.request_json(
            "/manage/accounts",
            method="POST",
            payload={"account_id": "main", "display_name": "Main", "session_name": "main"},
        )
        self.request_json(
            "/manage/origins",
            method="POST",
            payload={"account_id": "main", "origin_id": -1001, "origin_type": "group", "title": "Source Group"},
        )
        default_policy = self.request_json(
            "/manage/backup-policies",
            method="PATCH",
            payload={"account_id": "main", "origin_id": -1001, "enabled": True},
        )["item"]
        self.assertFalse(default_policy["download_stickers"])
        self.request_json(
            "/manage/participants",
            method="POST",
            payload={"account_id": "main", "origin_id": -1001, "user_id": 42, "display_name": "Alice"},
        )

        policy = self.request_json(
            "/manage/backup-policies",
            method="DELETE",
            payload={"account_id": "main", "origin_id": -1001},
        )["item"]
        self.assertEqual(policy["deleted_rows"], 1)
        self.assertEqual(self.request_json("/manage/backup-policies?account_id=main")["items"], [])

        participant = self.request_json(
            "/manage/participants",
            method="DELETE",
            payload={"account_id": "main", "origin_id": -1001, "user_id": 42},
        )["item"]
        self.assertEqual(participant["deleted_rows"], 1)
        self.assertEqual(self.request_json("/manage/participants?account_id=main&origin_id=-1001")["items"], [])

        origin = self.request_json(
            "/manage/origins",
            method="DELETE",
            payload={"account_id": "main", "origin_id": -1001},
        )["item"]
        self.assertEqual(origin["deleted_rows"], 1)
        self.assertEqual(self.request_json("/manage/origins?account_id=main")["items"], [])

        account = self.request_json(
            "/manage/accounts",
            method="DELETE",
            payload={"account_id": "main"},
        )["item"]
        self.assertEqual(account["deleted_rows"], 2)
        self.assertEqual(self.request_json("/manage/accounts")["items"], [])

    def test_saved_management_account_can_supply_live_runtime_config(self) -> None:
        session_dir = Path(self.tmp.name) / "sessions"
        config = AppConfig(
            storage=StorageConfig(data_dir=Path(self.tmp.name), database=Path(self.tmp.name) / "archive.db"),
            telegram=TelegramConfig(
                accounts=[
                    TelegramAccountConfig(
                        account_id="default",
                        api_id=12345,
                        api_hash="hash",
                        session_name="tele_mess_core",
                        session_dir=session_dir,
                    )
                ]
            ),
            server=ServerConfig(),
            logging=LoggingConfig(),
        )
        self.request_json(
            "/manage/accounts",
            method="POST",
            payload={
                "account_id": "second",
                "display_name": "Second",
                "session_name": "second-main",
                "session_dir": "managed-sessions",
            },
        )

        account = _account_config(config, self.store, "second")

        self.assertEqual(account.account_id, "second")
        self.assertEqual(account.api_id, 12345)
        self.assertEqual(account.api_hash, "hash")
        self.assertEqual(account.session_name, "second-main")
        self.assertEqual(account.session_dir, Path(self.tmp.name) / "managed-sessions")


class TelegramOperationSourceTest(unittest.TestCase):
    def test_telegram_operations_reject_other_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = ArchiveStore(Path(tmp) / "archive.db")
            store.initialize()
            store.upsert_account(
                AccountRecord(source=SOURCE_TELEGRAM, account_id="shared-id", display_name="Shared", kind="telegram")
            )
            config = AppConfig(
                storage=StorageConfig(data_dir=Path(tmp), database=Path(tmp) / "archive.db"),
                telegram=TelegramConfig(
                    accounts=[
                        TelegramAccountConfig(
                            account_id="main", api_id=1, api_hash="hash", session_name="main", session_dir=Path(tmp)
                        )
                    ]
                ),
                server=ServerConfig(token="secret"),
                logging=LoggingConfig(file=None),
            )
            payload = {"source": "other", "account_id": "shared-id", "phone": "+1", "code": "1", "origin_id": -1}
            for operation in (_auth_status, _request_auth_code, _submit_auth_code, _discover_origins, _refresh_participants):
                with self.subTest(operation=operation.__name__):
                    runtime = Mock()
                    with self.assertRaisesRegex(ValueError, "source"):
                        operation(payload, runtime)
                    runtime.call.assert_not_called()
            store.close()

    def test_offline_runtime_returns_http_200_structured_errors(self) -> None:
        import asyncio
        import threading
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        class OfflineClient:
            async def connect(self) -> None:
                raise ConnectionError("network unreachable")

            async def disconnect(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root / "archive.db")
            store.initialize()
            account = TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=root / "sessions",
            )
            config = AppConfig(
                storage=StorageConfig(data_dir=root, database=root / "archive.db"),
                telegram=TelegramConfig(accounts=[account]),
                server=ServerConfig(token="secret"),
                logging=LoggingConfig(file=None),
            )
            runtime = TelegramRuntimeManager(
                config,
                store,
                client_factory=lambda _cfg: OfflineClient(),
                ingest=False,
                connect_wait_seconds=1.0,
            )
            loop = asyncio.new_event_loop()
            started = threading.Event()

            def run_loop() -> None:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(runtime.start())
                started.set()
                loop.run_forever()
                loop.run_until_complete(runtime.stop())
                loop.close()

            thread = threading.Thread(target=run_loop, daemon=True)
            thread.start()
            started.wait(2)
            api = SyncApiServer(store, "127.0.0.1", 0, token="secret", config=config, telegram_runtime=runtime)
            api.start_background()
            try:
                assert api._httpd is not None
                port = api._httpd.server_address[1]

                def post(path: str, body: dict) -> tuple[int, dict]:
                    req = Request(
                        f"http://127.0.0.1:{port}{path}",
                        data=json.dumps(body).encode("utf-8"),
                        method="POST",
                    )
                    req.add_header("Authorization", "Bearer secret")
                    req.add_header("Content-Type", "application/json")
                    with urlopen(req, timeout=3) as resp:
                        return resp.status, json.loads(resp.read().decode("utf-8"))

                status, auth_payload = post("/manage/accounts/auth/status", {"account_id": "main"})
                self.assertEqual(status, 200)
                self.assertFalse(auth_payload["item"]["authorized"])
                self.assertEqual(auth_payload["item"]["auth_state"], "auth_failed")
                self.assertEqual(auth_payload["item"]["error"]["code"], "auth_status_failed")

                status, discover_payload = post("/manage/discover-origins", {"account_id": "main"})
                self.assertEqual(status, 200)
                self.assertFalse(discover_payload["item"]["authorized"])
                self.assertEqual(discover_payload["item"]["status"], "failed")
                self.assertEqual(discover_payload["item"]["errors"][0]["code"], "discover_origins_failed")

                status, part_payload = post("/manage/participants/refresh", {"account_id": "main", "origin_id": -1001})
                self.assertEqual(status, 200)
                self.assertFalse(part_payload["item"]["authorized"])
                self.assertEqual(part_payload["item"]["status"], "failed")
                self.assertEqual(part_payload["item"]["errors"][0]["code"], "refresh_participants_failed")
            finally:
                api.stop()
                loop.call_soon_threadsafe(loop.stop)
                thread.join(timeout=3)
                store.close()

    def test_hung_rpc_returns_http_200_structured_errors_and_recovers_after_reconnect(self) -> None:
        import asyncio
        import threading
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        should_hang = {"value": True}

        class ToggleClient:
            async def connect(self) -> None:
                return None

            async def disconnect(self) -> None:
                return None

            async def is_user_authorized(self) -> bool:
                if should_hang["value"]:
                    await asyncio.Future()
                return True

            async def get_me(self) -> object:
                class Me:
                    id = 777
                    username = "recovered"
                    first_name = "Recovered"
                    last_name = None

                return Me()

            async def send_code_request(self, phone: str) -> object:
                await asyncio.Future()

            async def sign_in(self, *args: object, **kwargs: object) -> object:
                await asyncio.Future()

            async def iter_dialogs(self) -> object:
                if False:
                    yield None

            async def get_entity(self, origin_id: int) -> object:
                await asyncio.Future()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root / "archive.db")
            store.initialize()
            account = TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=root / "sessions",
            )
            config = AppConfig(
                storage=StorageConfig(data_dir=root, database=root / "archive.db"),
                telegram=TelegramConfig(accounts=[account]),
                server=ServerConfig(token="secret"),
                logging=LoggingConfig(file=None),
            )
            runtime = TelegramRuntimeManager(
                config,
                store,
                client_factory=lambda _cfg: ToggleClient(),
                ingest=False,
                connect_wait_seconds=0.3,
                management_timeout_seconds=0.15,
            )
            loop = asyncio.new_event_loop()
            started = threading.Event()

            def run_loop() -> None:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(runtime.start())
                started.set()
                loop.run_forever()
                loop.run_until_complete(runtime.stop())
                loop.close()

            thread = threading.Thread(target=run_loop, daemon=True)
            thread.start()
            started.wait(2)
            api = SyncApiServer(store, "127.0.0.1", 0, token="secret", config=config, telegram_runtime=runtime)
            api.start_background()
            try:
                assert api._httpd is not None
                port = api._httpd.server_address[1]

                def post(path: str, body: dict) -> tuple[int, dict]:
                    req = Request(
                        f"http://127.0.0.1:{port}{path}",
                        data=json.dumps(body).encode("utf-8"),
                        method="POST",
                    )
                    req.add_header("Authorization", "Bearer secret")
                    req.add_header("Content-Type", "application/json")
                    with urlopen(req, timeout=3) as resp:
                        return resp.status, json.loads(resp.read().decode("utf-8"))

                # All 5 management endpoints return HTTP 200 structured errors on hung RPC.
                status, auth_payload = post("/manage/accounts/auth/status", {"account_id": "main"})
                self.assertEqual(status, 200)
                self.assertEqual(auth_payload["item"]["error"]["code"], "auth_status_failed")

                status, req_payload = post("/manage/accounts/auth/request-code", {"account_id": "main", "phone": "+1000"})
                self.assertEqual(status, 200)
                self.assertEqual(req_payload["item"]["error"]["code"], "request_code_failed")

                status, sub_payload = post(
                    "/manage/accounts/auth/submit-code",
                    {"account_id": "main", "phone": "+1000", "code": "12345"},
                )
                self.assertEqual(status, 200)
                self.assertEqual(sub_payload["item"]["error"]["code"], "submit_code_failed")

                status, disc_payload = post("/manage/discover-origins", {"account_id": "main"})
                self.assertEqual(status, 200)
                self.assertEqual(disc_payload["item"]["errors"][0]["code"], "discover_origins_failed")

                status, part_payload = post("/manage/participants/refresh", {"account_id": "main", "origin_id": -1001})
                self.assertEqual(status, 200)
                self.assertEqual(part_payload["item"]["errors"][0]["code"], "refresh_participants_failed")

                # Once the network recovers, the same runtime reconnects and succeeds.
                should_hang["value"] = False
                status, recovered = post("/manage/accounts/auth/status", {"account_id": "main"})
                self.assertEqual(status, 200)
                self.assertTrue(recovered["item"]["authorized"])
                self.assertEqual(recovered["item"]["auth_state"], "authorized")
            finally:
                api.stop()
                loop.call_soon_threadsafe(loop.stop)
                thread.join(timeout=3)
                store.close()

    def test_offline_endpoints_return_http_200_even_when_sqlite_write_is_locked(self) -> None:
        import asyncio
        import sqlite3
        import threading
        from unittest.mock import patch
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        class OfflineClient:
            async def connect(self) -> None:
                raise ConnectionError("network unreachable")

            async def disconnect(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root / "archive.db")
            store.initialize()
            account = TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=root / "sessions",
            )
            config = AppConfig(
                storage=StorageConfig(data_dir=root, database=root / "archive.db"),
                telegram=TelegramConfig(accounts=[account]),
                server=ServerConfig(token="secret"),
                logging=LoggingConfig(file=None),
            )
            runtime = TelegramRuntimeManager(
                config,
                store,
                client_factory=lambda _cfg: OfflineClient(),
                ingest=False,
                connect_wait_seconds=0.2,
                management_timeout_seconds=0.5,
            )
            loop = asyncio.new_event_loop()
            started = threading.Event()

            def run_loop() -> None:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(runtime.start())
                started.set()
                loop.run_forever()
                loop.run_until_complete(runtime.stop())
                loop.close()

            thread = threading.Thread(target=run_loop, daemon=True)
            thread.start()
            started.wait(2)
            api = SyncApiServer(store, "127.0.0.1", 0, token="secret", config=config, telegram_runtime=runtime)
            api.start_background()
            try:
                assert api._httpd is not None
                port = api._httpd.server_address[1]

                def post(path: str, body: dict) -> tuple[int, dict]:
                    req = Request(
                        f"http://127.0.0.1:{port}{path}",
                        data=json.dumps(body).encode("utf-8"),
                        method="POST",
                    )
                    req.add_header("Authorization", "Bearer secret")
                    req.add_header("Content-Type", "application/json")
                    with urlopen(req, timeout=3) as resp:
                        return resp.status, json.loads(resp.read().decode("utf-8"))

                def raise_locked(*_args: object, **_kwargs: object) -> None:
                    raise sqlite3.OperationalError("database is locked")

                with (
                    patch.object(store, "upsert_account_auth", side_effect=raise_locked),
                    patch.object(store, "add_operation_event", side_effect=raise_locked),
                ):
                    for path, body, expected_key, expected_code in (
                        ("/manage/accounts/auth/status", {"account_id": "main"}, "error", "auth_status_failed"),
                        (
                            "/manage/accounts/auth/request-code",
                            {"account_id": "main", "phone": "+1000"},
                            "error",
                            "request_code_failed",
                        ),
                        (
                            "/manage/accounts/auth/submit-code",
                            {"account_id": "main", "phone": "+1000", "code": "12345"},
                            "error",
                            "submit_code_failed",
                        ),
                        ("/manage/discover-origins", {"account_id": "main"}, "errors", "discover_origins_failed"),
                        (
                            "/manage/participants/refresh",
                            {"account_id": "main", "origin_id": -1001},
                            "errors",
                            "refresh_participants_failed",
                        ),
                    ):
                        status, payload = post(path, body)
                        self.assertEqual(status, 200)
                        if expected_key == "error":
                            self.assertEqual(payload["item"]["error"]["code"], expected_code)
                        else:
                            self.assertEqual(payload["item"]["errors"][0]["code"], expected_code)
            finally:
                api.stop()
                loop.call_soon_threadsafe(loop.stop)
                thread.join(timeout=3)
                store.close()

    def test_offline_http_budget_not_blocked_by_slow_supervisor_sqlite_write(self) -> None:
        import asyncio
        import sqlite3
        import threading
        import time
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        class OfflineClient:
            async def connect(self) -> None:
                raise ConnectionError("network unreachable")

            async def disconnect(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "archive.db"
            store = ArchiveStore(db_path)
            store.initialize()
            account = TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=root / "sessions",
            )
            config = AppConfig(
                storage=StorageConfig(data_dir=root, database=db_path),
                telegram=TelegramConfig(accounts=[account]),
                server=ServerConfig(token="secret"),
                logging=LoggingConfig(file=None),
            )
            blocker = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False)
            blocker.execute("BEGIN IMMEDIATE")
            runtime = TelegramRuntimeManager(
                config,
                store,
                client_factory=lambda _cfg: OfflineClient(),
                ingest=False,
                connect_wait_seconds=0.06,
                management_timeout_seconds=0.1,
            )
            loop = asyncio.new_event_loop()
            started = threading.Event()

            def run_loop() -> None:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(runtime.start())
                started.set()
                loop.run_forever()
                loop.run_until_complete(runtime.stop())
                loop.close()

            thread = threading.Thread(target=run_loop, daemon=True)
            thread.start()
            started.wait(2)
            api = SyncApiServer(store, "127.0.0.1", 0, token="secret", config=config, telegram_runtime=runtime)
            api.start_background()
            try:
                assert api._httpd is not None
                port = api._httpd.server_address[1]
                req = Request(
                    f"http://127.0.0.1:{port}/manage/accounts/auth/status",
                    data=json.dumps({"account_id": "main"}).encode("utf-8"),
                    method="POST",
                )
                req.add_header("Authorization", "Bearer secret")
                req.add_header("Content-Type", "application/json")
                started_at = time.monotonic()
                with urlopen(req, timeout=2) as resp:
                    elapsed = time.monotonic() - started_at
                    status = resp.status
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(status, 200)
                self.assertEqual(payload["item"]["error"]["code"], "auth_status_failed")
                # Even while SQLite is write-locked by another connection, HTTP response stays fast.
                self.assertLess(elapsed, 0.25)
            finally:
                blocker.execute("ROLLBACK")
                blocker.close()
                api.stop()
                loop.call_soon_threadsafe(loop.stop)
                thread.join(timeout=3)
                store.close()

    def test_connected_rpc_disconnect_not_blocked_by_sqlite_write_lock(self) -> None:
        import asyncio
        import sqlite3
        import threading
        import time
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        auth_calls = {"count": 0}

        class DropOnRpcClient:
            async def connect(self) -> None:
                return None

            async def disconnect(self) -> None:
                return None

            async def is_user_authorized(self) -> bool:
                auth_calls["count"] += 1
                if auth_calls["count"] == 1:
                    return False
                raise ConnectionError("connection lost mid-rpc")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "archive.db"
            store = ArchiveStore(db_path)
            store.initialize()
            account = TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=root / "sessions",
            )
            config = AppConfig(
                storage=StorageConfig(data_dir=root, database=db_path),
                telegram=TelegramConfig(accounts=[account]),
                server=ServerConfig(token="secret"),
                logging=LoggingConfig(file=None),
            )
            runtime = TelegramRuntimeManager(
                config,
                store,
                client_factory=lambda _cfg: DropOnRpcClient(),
                ingest=False,
                connect_wait_seconds=0.5,
                management_timeout_seconds=0.1,
            )
            loop = asyncio.new_event_loop()
            started = threading.Event()

            def run_loop() -> None:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(runtime.start())
                started.set()
                loop.run_forever()
                loop.run_until_complete(runtime.stop())
                loop.close()

            thread = threading.Thread(target=run_loop, daemon=True)
            thread.start()
            started.wait(2)
            # Wait until client is connected and ready before locking SQLite.
            for _ in range(50):
                if runtime.statuses() and runtime.statuses()[0]["connected"] and auth_calls["count"] >= 1:
                    break
                time.sleep(0.01)
            blocker = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False)
            blocker.execute("BEGIN IMMEDIATE")
            api = SyncApiServer(store, "127.0.0.1", 0, token="secret", config=config, telegram_runtime=runtime)
            api.start_background()
            try:
                assert api._httpd is not None
                port = api._httpd.server_address[1]
                req = Request(
                    f"http://127.0.0.1:{port}/manage/accounts/auth/status",
                    data=json.dumps({"account_id": "main"}).encode("utf-8"),
                    method="POST",
                )
                req.add_header("Authorization", "Bearer secret")
                req.add_header("Content-Type", "application/json")
                started_at = time.monotonic()
                with urlopen(req, timeout=2) as resp:
                    elapsed = time.monotonic() - started_at
                    status = resp.status
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(status, 200)
                self.assertEqual(payload["item"]["error"]["code"], "auth_status_failed")
                self.assertLess(elapsed, 0.25)
            finally:
                blocker.execute("ROLLBACK")
                blocker.close()
                api.stop()
                loop.call_soon_threadsafe(loop.stop)
                thread.join(timeout=3)
                store.close()

    def test_discover_and_refresh_mid_pagination_disconnect_not_blocked_by_sqlite_write_lock(self) -> None:
        import asyncio
        import sqlite3
        import threading
        import time
        from types import SimpleNamespace
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "archive.db"
            store = ArchiveStore(db_path)
            store.initialize()
            blocker = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False)

            def lock_db() -> None:
                blocker.execute("BEGIN IMMEDIATE")

            def unlock_db() -> None:
                if blocker.in_transaction:
                    blocker.execute("ROLLBACK")

            class MidPageDisconnectClient:
                async def connect(self) -> None:
                    return None

                async def disconnect(self) -> None:
                    return None

                async def is_user_authorized(self) -> bool:
                    return True

                async def iter_dialogs(self):
                    yield SimpleNamespace(
                        id=-1001,
                        title="Group 1",
                        is_user=False,
                        is_group=True,
                        is_channel=False,
                        date=None,
                        entity=SimpleNamespace(megagroup=True, broadcast=False, forum=False, username="g1"),
                    )
                    lock_db()
                    raise ConnectionError("dialog page 2 disconnect")

                async def iter_participants(self, origin_id: int, limit: int = 500):
                    yield SimpleNamespace(id=42, username="alice", first_name="Alice", last_name=None, bot=False)
                    lock_db()
                    raise ConnectionError("participant page 2 disconnect")

            account = TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=root / "sessions",
            )
            config = AppConfig(
                storage=StorageConfig(data_dir=root, database=db_path),
                telegram=TelegramConfig(accounts=[account]),
                server=ServerConfig(token="secret"),
                logging=LoggingConfig(file=None),
            )
            runtime = TelegramRuntimeManager(
                config,
                store,
                client_factory=lambda _cfg: MidPageDisconnectClient(),
                ingest=False,
                connect_wait_seconds=0.5,
                management_timeout_seconds=1.0,
            )
            loop = asyncio.new_event_loop()
            started = threading.Event()

            def run_loop() -> None:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(runtime.start())
                started.set()
                loop.run_forever()
                loop.run_until_complete(runtime.stop())
                loop.close()

            thread = threading.Thread(target=run_loop, daemon=True)
            thread.start()
            started.wait(2)
            for _ in range(50):
                if runtime.statuses() and runtime.statuses()[0]["connected"]:
                    break
                time.sleep(0.01)

            api = SyncApiServer(store, "127.0.0.1", 0, token="secret", config=config, telegram_runtime=runtime)
            api.start_background()
            try:
                assert api._httpd is not None
                port = api._httpd.server_address[1]

                req1 = Request(
                    f"http://127.0.0.1:{port}/manage/discover-origins",
                    data=json.dumps({"account_id": "main", "include_topics": False}).encode("utf-8"),
                    method="POST",
                )
                req1.add_header("Authorization", "Bearer secret")
                req1.add_header("Content-Type", "application/json")
                t0 = time.monotonic()
                with urlopen(req1, timeout=2) as resp1:
                    elapsed1 = time.monotonic() - t0
                    body1 = json.loads(resp1.read().decode("utf-8"))
                unlock_db()
                self.assertEqual(body1["item"]["status"], "partial")
                self.assertEqual(body1["item"]["origins"], 1)
                self.assertEqual(body1["item"]["errors"][0]["code"], "dialog_discovery_failed")
                self.assertLess(elapsed1, 0.25)

                req2 = Request(
                    f"http://127.0.0.1:{port}/manage/participants/refresh",
                    data=json.dumps({"account_id": "main", "origin_id": -1001}).encode("utf-8"),
                    method="POST",
                )
                req2.add_header("Authorization", "Bearer secret")
                req2.add_header("Content-Type", "application/json")
                t1 = time.monotonic()
                with urlopen(req2, timeout=2) as resp2:
                    elapsed2 = time.monotonic() - t1
                    body2 = json.loads(resp2.read().decode("utf-8"))
                unlock_db()
                self.assertEqual(body2["item"]["status"], "partial")
                self.assertEqual(body2["item"]["participants"], 1)
                self.assertEqual(body2["item"]["errors"][0]["code"], "participant_refresh_failed")
                self.assertLess(elapsed2, 0.25)
            finally:
                unlock_db()
                blocker.close()
                api.stop()
                loop.call_soon_threadsafe(loop.stop)
                thread.join(timeout=3)
                store.close()

    def test_patch_account_auth_advances_seq_and_blocks_stale_supervisor_probe(self) -> None:
        import asyncio
        import threading
        import time
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "archive.db"
            store = ArchiveStore(db_path)
            store.initialize()
            probe_entered = threading.Event()
            release_probe = threading.Event()

            class BlockingProbeClient:
                async def connect(self) -> None:
                    return None

                async def disconnect(self) -> None:
                    return None

                async def is_user_authorized(self) -> bool:
                    probe_entered.set()
                    while not release_probe.is_set():
                        await asyncio.sleep(0.01)
                    return False

            account = TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=root / "sessions",
            )
            config = AppConfig(
                storage=StorageConfig(data_dir=root, database=db_path),
                telegram=TelegramConfig(accounts=[account]),
                server=ServerConfig(token="secret"),
                logging=LoggingConfig(file=None),
            )
            runtime = TelegramRuntimeManager(
                config,
                store,
                client_factory=lambda _cfg: BlockingProbeClient(),
                ingest=False,
                connect_wait_seconds=0.5,
                management_timeout_seconds=2.0,
            )
            loop = asyncio.new_event_loop()
            started = threading.Event()

            def run_loop() -> None:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(runtime.start())
                started.set()
                loop.run_forever()
                loop.run_until_complete(runtime.stop())
                loop.close()

            thread = threading.Thread(target=run_loop, daemon=True)
            thread.start()
            started.wait(2)
            self.assertTrue(probe_entered.wait(2))
            api = SyncApiServer(store, "127.0.0.1", 0, token="secret", config=config, telegram_runtime=runtime)
            api.start_background()
            try:
                assert api._httpd is not None
                port = api._httpd.server_address[1]
                req = Request(
                    f"http://127.0.0.1:{port}/manage/accounts/auth",
                    data=json.dumps({"account_id": "main", "auth_state": "authorized"}).encode("utf-8"),
                    method="PATCH",
                )
                req.add_header("Authorization", "Bearer secret")
                req.add_header("Content-Type", "application/json")
                with urlopen(req, timeout=2) as resp:
                    self.assertEqual(resp.status, 200)
                    body = json.loads(resp.read().decode("utf-8"))
                    self.assertEqual(body["item"]["auth_state"], "authorized")

                release_probe.set()
                time.sleep(0.08)
                accounts = {item["account_id"]: item for item in store.list_management_accounts()}
                self.assertEqual(accounts["main"]["auth_state"], "authorized")
            finally:
                release_probe.set()
                api.stop()
                loop.call_soon_threadsafe(loop.stop)
                thread.join(timeout=3)
                store.close()

    def test_discover_and_refresh_prelocked_sqlite_returns_within_budget(self) -> None:
        import asyncio
        import sqlite3
        import threading
        import time
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "archive.db"
            store = ArchiveStore(db_path)
            store.initialize()
            iter_calls = {"dialogs": 0, "participants": 0}

            class CachedAuthDisconnectedClient:
                async def connect(self) -> None:
                    return None

                async def disconnect(self) -> None:
                    return None

                async def is_user_authorized(self) -> bool:
                    return True

                async def iter_dialogs(self):
                    iter_calls["dialogs"] += 1
                    raise ConnectionError("network lost before first dialog page")
                    yield  # pragma: no cover

                async def iter_participants(self, origin_id: int, limit: int = 500):
                    iter_calls["participants"] += 1
                    raise ConnectionError("network lost before first participant page")
                    yield  # pragma: no cover

            account = TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=root / "sessions",
            )
            config = AppConfig(
                storage=StorageConfig(data_dir=root, database=db_path),
                telegram=TelegramConfig(accounts=[account]),
                server=ServerConfig(token="secret"),
                logging=LoggingConfig(file=None),
            )
            runtime = TelegramRuntimeManager(
                config,
                store,
                client_factory=lambda _cfg: CachedAuthDisconnectedClient(),
                ingest=False,
                connect_wait_seconds=0.5,
                management_timeout_seconds=0.2,
            )
            loop = asyncio.new_event_loop()
            started = threading.Event()

            def run_loop() -> None:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(runtime.start())
                started.set()
                loop.run_forever()
                loop.run_until_complete(runtime.stop())
                loop.close()

            thread = threading.Thread(target=run_loop, daemon=True)
            thread.start()
            started.wait(2)
            for _ in range(50):
                if runtime.statuses() and runtime.statuses()[0]["connected"]:
                    break
                time.sleep(0.01)

            # Lock SQLite BEFORE either HTTP request arrives.
            blocker = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False)
            blocker.execute("BEGIN IMMEDIATE")
            api = SyncApiServer(store, "127.0.0.1", 0, token="secret", config=config, telegram_runtime=runtime)
            api.start_background()
            try:
                assert api._httpd is not None
                port = api._httpd.server_address[1]

                req1 = Request(
                    f"http://127.0.0.1:{port}/manage/discover-origins",
                    data=json.dumps({"account_id": "main", "include_topics": False}).encode("utf-8"),
                    method="POST",
                )
                req1.add_header("Authorization", "Bearer secret")
                req1.add_header("Content-Type", "application/json")
                t0 = time.monotonic()
                with urlopen(req1, timeout=2) as resp1:
                    elapsed1 = time.monotonic() - t0
                    body1 = json.loads(resp1.read().decode("utf-8"))
                self.assertEqual(body1["item"]["status"], "failed")
                self.assertEqual(body1["item"]["errors"][0]["code"], "dialog_discovery_failed")
                self.assertEqual(iter_calls["dialogs"], 1)
                self.assertLess(elapsed1, 0.2)

                req2 = Request(
                    f"http://127.0.0.1:{port}/manage/participants/refresh",
                    data=json.dumps({"account_id": "main", "origin_id": -1001}).encode("utf-8"),
                    method="POST",
                )
                req2.add_header("Authorization", "Bearer secret")
                req2.add_header("Content-Type", "application/json")
                t1 = time.monotonic()
                with urlopen(req2, timeout=2) as resp2:
                    elapsed2 = time.monotonic() - t1
                    body2 = json.loads(resp2.read().decode("utf-8"))
                self.assertEqual(body2["item"]["status"], "failed")
                self.assertEqual(body2["item"]["errors"][0]["code"], "participant_refresh_failed")
                self.assertEqual(iter_calls["participants"], 1)
                self.assertLess(elapsed2, 0.2)
            finally:
                blocker.execute("ROLLBACK")
                blocker.close()
                api.stop()
                loop.call_soon_threadsafe(loop.stop)
                thread.join(timeout=3)
                store.close()

    def test_management_writes_off_event_loop_respect_budget_and_committed_counts(self) -> None:
        import asyncio
        import sqlite3
        import threading
        import time
        from types import SimpleNamespace
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "archive.db"
            store = ArchiveStore(db_path)
            store.initialize()

            class FirstPageThenDisconnectClient:
                async def connect(self) -> None:
                    return None

                async def disconnect(self) -> None:
                    return None

                async def is_user_authorized(self) -> bool:
                    return True

                async def iter_dialogs(self):
                    yield SimpleNamespace(
                        id=-1001,
                        title="Group 1",
                        is_user=False,
                        is_group=True,
                        is_channel=False,
                        date=None,
                        entity=SimpleNamespace(megagroup=True, broadcast=False, forum=False, username="g1"),
                    )
                    raise ConnectionError("page 2 disconnect")

                async def iter_participants(self, origin_id: int, limit: int = 500):
                    yield SimpleNamespace(id=42, username="alice", first_name="Alice", last_name=None, bot=False)
                    raise ConnectionError("page 2 disconnect")

            account = TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=root / "sessions",
            )
            config = AppConfig(
                storage=StorageConfig(data_dir=root, database=db_path),
                telegram=TelegramConfig(accounts=[account]),
                server=ServerConfig(token="secret"),
                logging=LoggingConfig(file=None),
            )
            runtime = TelegramRuntimeManager(
                config,
                store,
                client_factory=lambda _cfg: FirstPageThenDisconnectClient(),
                ingest=False,
                connect_wait_seconds=0.5,
                management_timeout_seconds=0.06,
            )
            loop = asyncio.new_event_loop()
            started = threading.Event()

            def run_loop() -> None:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(runtime.start())
                started.set()
                loop.run_forever()
                loop.run_until_complete(runtime.stop())
                loop.close()

            thread = threading.Thread(target=run_loop, daemon=True)
            thread.start()
            started.wait(2)
            for _ in range(50):
                if runtime.statuses() and runtime.statuses()[0]["connected"]:
                    break
                time.sleep(0.01)

            blocker = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False)
            blocker.execute("BEGIN IMMEDIATE")
            api = SyncApiServer(store, "127.0.0.1", 0, token="secret", config=config, telegram_runtime=runtime)
            api.start_background()
            try:
                assert api._httpd is not None
                port = api._httpd.server_address[1]

                # 1) All three management operations hit a locked SQLite write after receiving cached/first-page data;
                #    all must stay within the 60ms management budget (well under the 200ms SQLite busy_timeout) and
                #    never count uncommitted rows.
                for path, payload, count_key in (
                    ("/manage/accounts/auth/status", {"account_id": "main"}, None),
                    ("/manage/discover-origins", {"account_id": "main", "include_topics": False}, "origins"),
                    ("/manage/participants/refresh", {"account_id": "main", "origin_id": -1001}, "participants"),
                ):
                    for _ in range(50):
                        if runtime.statuses() and runtime.statuses()[0]["connected"]:
                            break
                        time.sleep(0.01)
                    req = Request(
                        f"http://127.0.0.1:{port}{path}",
                        data=json.dumps(payload).encode("utf-8"),
                        method="POST",
                    )
                    req.add_header("Authorization", "Bearer secret")
                    req.add_header("Content-Type", "application/json")
                    t0 = time.monotonic()
                    with urlopen(req, timeout=2) as resp:
                        elapsed = time.monotonic() - t0
                        body = json.loads(resp.read().decode("utf-8"))
                    self.assertLess(elapsed, 0.18, f"{path} exceeded budget: {elapsed:.3f}s")
                    if count_key is not None:
                        self.assertEqual(body["item"][count_key], 0)
                        self.assertEqual(body["item"]["status"], "failed")
                    else:
                        self.assertEqual(body["item"]["error"]["code"], "auth_status_failed")

                # 2) With a larger budget (>200ms busy_timeout), first-item DB lock fails cleanly without incrementing
                #    committed counts, and the subsequent page-2 disconnect is also captured.
                runtime._runtimes["main"]._management_timeout_seconds = 1.0
                for _ in range(50):
                    if runtime.statuses() and runtime.statuses()[0]["connected"]:
                        break
                    time.sleep(0.01)
                req_disc = Request(
                    f"http://127.0.0.1:{port}/manage/discover-origins",
                    data=json.dumps({"account_id": "main", "include_topics": False}).encode("utf-8"),
                    method="POST",
                )
                req_disc.add_header("Authorization", "Bearer secret")
                req_disc.add_header("Content-Type", "application/json")
                with urlopen(req_disc, timeout=2) as resp_disc:
                    disc_body = json.loads(resp_disc.read().decode("utf-8"))
                self.assertEqual(disc_body["item"]["origins"], 0)
                self.assertEqual(disc_body["item"]["status"], "failed")
                codes = [err["code"] for err in disc_body["item"]["errors"]]
                self.assertIn("origin_discovery_failed", codes)
                self.assertIn("dialog_discovery_failed", codes)
            finally:
                blocker.execute("ROLLBACK")
                blocker.close()
                api.stop()
                loop.call_soon_threadsafe(loop.stop)
                thread.join(timeout=3)
                store.close()


if __name__ == "__main__":
    unittest.main()
