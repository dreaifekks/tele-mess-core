from __future__ import annotations

import json
from http.client import HTTPConnection
import tempfile
import time
import unittest
from pathlib import Path
from urllib.error import HTTPError
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
    ChatRecord,
    MediaFileRecord,
    MessageRecord,
    OperationEventRecord,
    SOURCE_TELEGRAM,
    utc_now_iso,
)
from tele_mess_core.daily_jobs import DailyJobWorker
from tele_mess_core.server import SyncApiServer
from tele_mess_core.server.api import MAX_JSON_BODY_BYTES, _account_config
from tele_mess_core.server.contracts import API_CONTRACT_HASH, API_CONTRACT_VERSION


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
        records = self.request_json(
            f"/manage/daily-summary-records?package_run_id={package['run_id']}&tag=web3&tags=info&important=true"
        )["items"]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["run_id"], summary["run_id"])
        self.assertIn("AI provider is disabled", records[0]["content_preview"])
        self.assertNotIn("content_md", records[0])
        record = self.request_json(f"/manage/daily-summary-records/item?run_id={summary['run_id']}")["item"]
        self.assertIn("AI provider is disabled", record["content_md"])
        self.assertEqual(record["tags"], ["web3", "info"])

        deleted = self.request_json(
            "/manage/daily-summary-records",
            method="PATCH",
            payload={"summary_ids": [records[0]["summary_id"]], "deleted": True},
        )["item"]
        self.assertEqual(deleted["changed_rows"], 1)
        hidden_records = self.request_json(f"/manage/daily-summary-records?summary_id={records[0]['summary_id']}")["items"]
        self.assertEqual(hidden_records, [])
        deleted_records = self.request_json(
            f"/manage/daily-summary-records?summary_id={records[0]['summary_id']}&deleted=true"
        )["items"]
        self.assertEqual(len(deleted_records), 1)
        self.assertTrue(deleted_records[0]["deleted"])

        restored = self.request_json(
            "/manage/daily-summary-records",
            method="PATCH",
            payload={"summary_id": records[0]["summary_id"], "deleted": False},
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
        self.assertIn("/sync/media-files/content?", payload["items"][0]["access_url"])
        self.assertIn("message_id=1", payload["items"][0]["access_url"])

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
                "tags": "alpha,beta",
            },
        )["item"]
        self.assertTrue(policy["enabled"])
        self.assertFalse(policy["download_media"])
        self.assertEqual(policy["tags"], "alpha,beta")

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
        self.assertEqual(saved_topic["backup_policy"]["tags"], "alpha,beta")
        participants = self.request_json("/manage/participants?account_id=main&origin_id=-1001")["items"]
        self.assertEqual(participants[0]["display_name"], "Alice")

        cursors = self.request_json("/manage/capture-cursors?account_id=main")["items"]
        self.assertEqual(cursors, [])

        capabilities = self.request_json("/manage/capabilities")
        self.assertIn("origin_registry", capabilities["management"])
        self.assertIn("capture_cursors", capabilities["management"])
        self.assertIn("operation_events", capabilities["management"])
        self.assertIn("operation_event_delete", capabilities["management"])

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
        self.request_json(
            "/manage/backup-policies",
            method="PATCH",
            payload={"account_id": "main", "origin_id": -1001, "enabled": True},
        )
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
                        timezone="UTC",
                    )
                ]
            ),
            server=ServerConfig(),
            logging=LoggingConfig(),
        )
        self.request_json(
            "/manage/accounts",
            method="POST",
            payload={"account_id": "second", "display_name": "Second", "session_name": "second-main"},
        )

        account = _account_config(config, self.store, "second")

        self.assertEqual(account.account_id, "second")
        self.assertEqual(account.api_id, 12345)
        self.assertEqual(account.api_hash, "hash")
        self.assertEqual(account.session_name, "second-main")
        self.assertEqual(account.session_dir, session_dir)


if __name__ == "__main__":
    unittest.main()
