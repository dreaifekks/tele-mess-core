from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from urllib.request import Request, urlopen

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import (
    AppConfig,
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
from tele_mess_core.server import SyncApiServer
from tele_mess_core.server.api import _account_config


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
        self.api = SyncApiServer(self.store, "127.0.0.1", 0, token="secret")
        self.api.start_background()
        deadline = time.time() + 2
        while self.api._httpd is None and time.time() < deadline:
            time.sleep(0.01)
        assert self.api._httpd is not None
        self.port = self.api._httpd.server_address[1]

    def tearDown(self) -> None:
        self.api.stop()
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
        self.assertIn('data-view="people">Members', html)
        self.assertIn("/manage/accounts/auth/request-code", html)
        self.assertIn("/manage/accounts/auth/submit-code", html)
        self.assertIn("/manage/discover-origins", html)
        self.assertIn("/manage/backup-policies", html)
        self.assertIn("/manage/participants/refresh", html)
        self.assertIn("/manage/operation-events", html)
        self.assertIn("/sync/media-files", html)
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
        self.assertIn("/sync/messages?latest=true&limit=100", html)
        self.assertIn("startMessageAutoRefresh", html)
        self.assertIn("stopMessageAutoRefresh", html)
        self.assertIn("operationEvents", html)
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
        self.assertIn("policy-row", html)
        self.assertIn("button.closest('tr').after(row)", html)
        self.assertIn(".raw-panel", html)
        self.assertIn("height: calc(100vh - 170px)", html)
        self.assertIn("<th>Actions</th>", html)
        self.assertIn(".table-wrap thead th", html)
        self.assertIn("top: 0", html)
        self.assertNotIn("top: 57px", html)

    def test_root_serves_console_without_token(self) -> None:
        html = self.request_text_no_auth("/")
        self.assertIn("tele-mess-core console", html)
        self.assertIn("API token", html)

    def test_media_files_endpoint(self) -> None:
        self.store.upsert_media_file(
            MediaFileRecord(
                source=SOURCE_TELEGRAM,
                account_id="default",
                chat_id=-1001,
                message_id=1,
                file_path="/tmp/api-media.bin",
                media_kind="photo",
                downloaded_at=utc_now_iso(),
            )
        )

        payload = self.request_json("/sync/media-files")
        self.assertEqual(payload["items"][0]["chat_title"], "API Chat")

    def test_operation_events_endpoint(self) -> None:
        self.store.add_operation_event(
            OperationEventRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                operation="media_download",
                status="failed",
                error_code="media_download_failed",
                message="network down",
            )
        )

        payload = self.request_json("/manage/operation-events?account_id=main&status=failed")

        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["operation"], "media_download")
        self.assertEqual(payload["items"][0]["error_code"], "media_download_failed")

    def test_start_background_reports_bind_failure(self) -> None:
        second = SyncApiServer(self.store, "127.0.0.1", self.port, token="secret")
        with self.assertRaises(RuntimeError):
            second.start_background(startup_timeout=1)
        second.stop()

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
        self.assertEqual(account.chats, [])


if __name__ == "__main__":
    unittest.main()
