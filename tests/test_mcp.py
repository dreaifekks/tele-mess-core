from __future__ import annotations

import json
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from http.client import HTTPConnection
from pathlib import Path
from typing import Any

from tele_mess_core import __version__
from tele_mess_core.archive import ArchiveStore
from tele_mess_core.models import (
    AccountAuthRecord,
    AccountRecord,
    BackupPolicyRecord,
    DailyMessagePointRecord,
    DailySummaryRecord,
    DailySummaryRunRecord,
    MessageRecord,
    OriginRecord,
    SOURCE_TELEGRAM,
)
from tele_mess_core.server import SyncApiServer
from tele_mess_core.server.mcp import MCP_TOOLS

GROUP = -1001
CHANNEL = -1002
FORUM = -1003
UNWATCHED = -1004
TOOL_NAMES = [
    "overview",
    "list_sources",
    "search_messages",
    "read_messages",
    "list_daily_reports",
    "read_daily_report",
    "search_points",
]


class McpApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = ArchiveStore(Path(self.tmp.name) / "archive.db")
        self.store.initialize()
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        self._seed_sources()
        self._seed_messages()
        self._seed_daily_content()
        self.api = SyncApiServer(self.store, "127.0.0.1", 0, token="secret", mcp_token="agent-secret")
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

    def _seed_sources(self) -> None:
        self.store.upsert_account(AccountRecord(source=SOURCE_TELEGRAM, account_id="main", display_name="Main"))
        self.store.upsert_account_auth(
            AccountAuthRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                auth_state="authorized",
                phone="+10000000000",
                session_name="main-session",
                session_dir="/secret/sessions",
            )
        )
        for origin in (
            OriginRecord(SOURCE_TELEGRAM, "main", GROUP, "group", title="研发群", important=True),
            OriginRecord(SOURCE_TELEGRAM, "main", CHANNEL, "channel", title="News Channel", username="newsch"),
            OriginRecord(SOURCE_TELEGRAM, "main", FORUM, "group", title="论坛", is_forum=True),
            OriginRecord(SOURCE_TELEGRAM, "main", FORUM, "topic", topic_id=7, parent_origin_id=FORUM, title="模型讨论"),
            OriginRecord(SOURCE_TELEGRAM, "main", UNWATCHED, "group", title="Unwatched Group"),
        ):
            self.store.upsert_origin(origin)
        for origin_id, tags in ((GROUP, "ai,dev"), (CHANNEL, "news"), (FORUM, "ai")):
            self.store.set_backup_policy(
                BackupPolicyRecord(SOURCE_TELEGRAM, "main", origin_id, enabled=True, tags=tags)
            )

    def _seed_messages(self) -> None:
        def add(chat_id: int, message_id: int, age: timedelta, text: str, **fields: Any) -> None:
            sent_at = (self.now - age).isoformat()
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=chat_id,
                    message_id=message_id,
                    sent_at=sent_at,
                    ingested_at=sent_at,
                    text=text,
                    raw_json='{"secret": "raw"}',
                    reactions_json='[{"emoji": "x"}]',
                    **fields,
                ),
                event_type="new",
            )

        add(GROUP, 1, timedelta(hours=3), "今天发布了新模型，评测结果不错", sender_name="张三", sender_username="zhangsan")
        add(GROUP, 2, timedelta(hours=2), "我用claude写代码", sender_name="李四", reply_to_message_id=1)
        add(
            GROUP,
            3,
            timedelta(hours=1),
            "I use claude daily, gpt-5 too",
            sender_name="Alice",
            sender_username="alice",
            permalink="https://t.me/c/1001/3",
        )
        add(GROUP, 4, timedelta(minutes=30), "这条被删除的模型消息", sender_name="张三")
        self.store.mark_deleted(SOURCE_TELEGRAM, GROUP, [4], account_id="main")
        add(CHANNEL, 10, timedelta(days=200), "旧闻：模型价格下调 100%", forward_from_name="Wire")
        add(FORUM, 20, timedelta(minutes=10), "话题里的讨论", topic_id=7, has_media=True, media_kind="photo")

    def _seed_daily_content(self) -> None:
        self.store.upsert_daily_summary_run(
            DailySummaryRunRecord(
                run_id="run_0703", status="completed", package_run_id="pkg_0703", date="2026-07-03", timezone="UTC"
            )
        )
        for summary_id, record_type, date, title, content in (
            ("sum_daily", "important_daily", "2026-07-03", "重点日报", "# 重点日报\n" + "模型发布。" * 40),
            ("sum_origin_a", "important_origin", "2026-07-03", "研发群分析", "研发群的分析"),
            ("sum_origin_b", "important_origin", "2026-07-03", "论坛分析", "论坛的分析"),
            ("sum_points", "point_daily", "2026-07-04", "消息点日报", "消息点汇总"),
        ):
            self.store.upsert_daily_summary_record(
                DailySummaryRecord(
                    summary_id=summary_id,
                    run_id="run_0703",
                    content_md=content,
                    date=date,
                    timezone="UTC",
                    tags_json='["ai"]',
                    title=title,
                    record_type=record_type,
                )
            )
        self.store.persist_daily_message_points(
            "run_0703",
            [
                DailyMessagePointRecord(
                    point_id=point_id,
                    run_id="run_0703",
                    package_run_id="pkg_0703",
                    date="2026-07-03",
                    timezone="UTC",
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    origin_id=GROUP,
                    origin_title="研发群",
                    message_id=message_id,
                    occurred_at=occurred_at,
                    tags_json=tags_json,
                    content=content,
                    telegram_deeplink=f"tg://privatepost?channel=1001&post={message_id}",
                    importance_score=score,
                    importance_reason="影响选型" if score >= 4 else None,
                    origin_important=True,
                    source_refs_json=f'[{{"message_id": {message_id}}}]',
                )
                for point_id, message_id, occurred_at, tags_json, content, score in (
                    ("point_model", 1, "2026-07-03T10:00:00+09:00", '["ai", "model"]', "新模型发布，评测领先", 5),
                    ("point_chat", 3, "2026-07-03T09:00:00+09:00", '["ai"]', "Alice 每天使用 claude", 2),
                )
            ],
        )

    def post(self, body: Any, token: str | None = "agent-secret", raw: bytes | None = None) -> tuple[int, dict, Any]:
        connection = HTTPConnection("127.0.0.1", self.port, timeout=5)
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        connection.request("POST", "/mcp", body=raw if raw is not None else json.dumps(body).encode(), headers=headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, dict(response.getheaders()), json.loads(data) if data else None

    def rpc(self, method: str, params: dict | None = None) -> dict:
        status, _, payload = self.post({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}})
        self.assertEqual(status, 200)
        self.assertEqual(payload["jsonrpc"], "2.0")
        self.assertEqual(payload["id"], 1)
        return payload

    def call_raw(self, name: str, arguments: dict | None = None) -> dict:
        return self.rpc("tools/call", {"name": name, "arguments": arguments or {}})["result"]

    def call(self, name: str, arguments: dict | None = None) -> dict:
        result = self.call_raw(name, arguments)
        self.assertFalse(result["isError"], result["content"][0]["text"])
        self.assertEqual(result["content"][0]["type"], "text")
        return json.loads(result["content"][0]["text"])

    def call_error(self, name: str, arguments: dict | None = None) -> str:
        result = self.call_raw(name, arguments)
        self.assertTrue(result["isError"])
        return result["content"][0]["text"]

    def status_for(self, method: str, path: str, headers: dict[str, str]) -> int:
        connection = HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.request(method, path, headers=headers)
        response = connection.getresponse()
        response.read()
        connection.close()
        return response.status

    def test_initialize_negotiates_protocol_version(self) -> None:
        status, headers, payload = self.post(
            {"jsonrpc": "2.0", "id": "init-1", "method": "initialize", "params": {"protocolVersion": "2025-03-26"}}
        )
        self.assertEqual(status, 200)
        self.assertTrue(headers["Content-Type"].startswith("application/json"))
        self.assertNotIn("mcp-session-id", {name.lower() for name in headers})
        self.assertEqual(payload["id"], "init-1")
        result = payload["result"]
        self.assertEqual(result["protocolVersion"], "2025-03-26")
        self.assertEqual(result["capabilities"], {"tools": {}})
        self.assertEqual(result["serverInfo"], {"name": "tele-mess-core", "version": __version__})
        self.assertIn("overview", result["instructions"])

        for requested in ("2025-06-18", "2024-11-05"):
            echoed = self.rpc("initialize", {"protocolVersion": requested})["result"]
            self.assertEqual(echoed["protocolVersion"], requested)
        for requested in ("1999-01-01", None):
            fallback = self.rpc("initialize", {"protocolVersion": requested})["result"]
            self.assertEqual(fallback["protocolVersion"], "2025-06-18")

    def test_notifications_return_202_without_body(self) -> None:
        for method in ("notifications/initialized", "notifications/cancelled", "notifications/unknown"):
            status, headers, payload = self.post({"jsonrpc": "2.0", "method": method})
            self.assertEqual(status, 202)
            self.assertEqual(headers["Content-Length"], "0")
            self.assertIsNone(payload)

    def test_ping_and_tools_list(self) -> None:
        self.assertEqual(self.rpc("ping")["result"], {})

        tools = self.rpc("tools/list")["result"]["tools"]
        self.assertEqual([tool["name"] for tool in tools], TOOL_NAMES)
        self.assertEqual([tool.name for tool in MCP_TOOLS], TOOL_NAMES)
        for tool in tools:
            self.assertEqual(tool["annotations"], {"readOnlyHint": True})
            self.assertEqual(tool["inputSchema"]["type"], "object")
            self.assertTrue(tool["description"])
            if tool["name"] not in {"overview", "read_daily_report"}:
                self.assertIn("limit", tool["inputSchema"]["properties"])
        search = next(tool for tool in tools if tool["name"] == "search_messages")
        self.assertEqual(search["inputSchema"]["required"], ["query"])

    def test_protocol_errors(self) -> None:
        unknown_method = self.rpc("resources/list")
        self.assertEqual(unknown_method["error"]["code"], -32601)
        self.assertNotIn("result", unknown_method)

        unknown_tool = self.rpc("tools/call", {"name": "delete_everything", "arguments": {}})
        self.assertEqual(unknown_tool["error"]["code"], -32602)
        self.assertIn("delete_everything", unknown_tool["error"]["message"])

        bad_params = self.post({"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": ["overview"]})[2]
        self.assertEqual(bad_params["error"]["code"], -32602)

        status, _, parse_error = self.post(None, raw=b"{not json")
        self.assertEqual(status, 400)
        self.assertEqual(parse_error, {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}})

        status, _, batch = self.post([{"jsonrpc": "2.0", "id": 1, "method": "ping"}])
        self.assertEqual(status, 400)
        self.assertEqual(batch["error"]["code"], -32600)

    def test_tool_argument_errors_are_tool_results(self) -> None:
        self.assertIn("arguments.limit must be an integer", self.call_error("search_messages", {"query": "模型", "limit": "ten"}))
        self.assertIn("Unknown field: arguments.chat_id", self.call_error("read_messages", {"chat_id": GROUP}))
        self.assertIn("arguments.query", self.call_error("search_messages"))
        self.assertIn("at least one keyword", self.call_error("search_messages", {"query": "   "}))
        self.assertIn("origin_id requires account_id", self.call_error("search_messages", {"query": "模型", "origin_id": GROUP}))
        self.assertIn("Invalid date", self.call_error("read_messages", {"date_from": "yesterday"}))
        self.assertIn("Invalid cursor", self.call_error("read_messages", {"cursor": "garbage"}))
        self.assertIn("importance_min", self.call_error("search_points", {"importance_min": 9}))

    def test_auth_scopes_mcp_token_to_mcp_endpoint(self) -> None:
        ping = {"jsonrpc": "2.0", "id": 1, "method": "ping"}
        self.assertEqual(self.post(ping, token=None)[0], 401)
        self.assertEqual(self.post(ping, token="wrong")[0], 401)
        self.assertEqual(self.post(ping, token="agent-secret")[0], 200)
        self.assertEqual(self.post(ping, token="secret")[0], 200)

        connection = HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.request(
            "POST",
            "/mcp",
            body=json.dumps(ping),
            headers={"Content-Type": "application/json", "X-Api-Token": "agent-secret"},
        )
        response = connection.getresponse()
        response.read()
        connection.close()
        self.assertEqual(response.status, 200)

        agent = {"Authorization": "Bearer agent-secret"}
        for path in ("/manage/accounts", "/manage/origins", "/manage/api-manifest", "/sync/state", "/sync/messages", "/healthz"):
            self.assertEqual(self.status_for("GET", path, agent), 401, path)
            self.assertEqual(self.status_for("GET", path, {"X-Api-Token": "agent-secret"}), 401, path)
        self.assertEqual(self.status_for("DELETE", "/manage/origins", agent), 401)
        self.assertEqual(self.status_for("GET", "/manage/accounts", {"Authorization": "Bearer secret"}), 200)

        for method in ("GET", "DELETE"):
            self.assertEqual(self.status_for(method, "/mcp", agent), 405)
            self.assertEqual(self.status_for(method, "/mcp", {}), 401)

    def test_mcp_token_is_optional(self) -> None:
        api = SyncApiServer(self.store, "127.0.0.1", 0, token="secret")
        api.start_background()
        try:
            port = api._httpd.server_address[1]
            for token, expected in (("secret", 200), ("", 401), ("agent-secret", 401)):
                connection = HTTPConnection("127.0.0.1", port, timeout=5)
                connection.request(
                    "POST",
                    "/mcp",
                    body=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ping"}),
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
                )
                response = connection.getresponse()
                response.read()
                connection.close()
                self.assertEqual(response.status, expected, token)
        finally:
            api.stop()

    def test_capabilities_advertise_mcp(self) -> None:
        connection = HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.request("GET", "/manage/capabilities", headers={"Authorization": "Bearer secret"})
        payload = json.loads(connection.getresponse().read())
        connection.close()
        self.assertEqual(payload["mcp"]["endpoint"], "/mcp")
        self.assertEqual(payload["mcp"]["tools"], TOOL_NAMES)

    def test_overview(self) -> None:
        text = self.call_raw("overview")["content"][0]["text"]
        for secret in ("+10000000000", "main-session", "/secret/sessions", "authorized"):
            self.assertNotIn(secret, text)
        overview = json.loads(text)

        self.assertEqual(overview["timezone"], "Asia/Tokyo")
        self.assertTrue(overview["now"].endswith("+09:00"))
        self.assertEqual(overview["accounts"], [{"account_id": "main", "display_name": "Main"}])
        self.assertEqual(overview["messages"]["count"], 6)
        self.assertTrue(overview["messages"]["first_sent_at"] < overview["messages"]["last_sent_at"])
        self.assertEqual(overview["sources"], {"known": 5, "enabled": 3})
        self.assertEqual(overview["tags"], ["ai", "dev", "news"])
        self.assertEqual(overview["daily_reports"]["count"], 4)
        self.assertEqual(overview["daily_reports"]["date_from"], "2026-07-03")
        self.assertEqual(overview["daily_reports"]["date_to"], "2026-07-04")
        self.assertEqual(
            overview["daily_reports"]["record_types"]["important_origin"],
            {"count": 2, "date_from": "2026-07-03", "date_to": "2026-07-03"},
        )
        self.assertEqual(overview["message_points"], {"count": 2, "date_from": "2026-07-03", "date_to": "2026-07-03"})

    def test_list_sources(self) -> None:
        enabled = self.call("list_sources")
        self.assertEqual(enabled["total"], 4)
        self.assertFalse(enabled["has_more"])
        titles = [item["title"] for item in enabled["sources"]]
        self.assertEqual(set(titles), {"研发群", "News Channel", "论坛", "论坛 / 模型讨论"})
        topic = next(item for item in enabled["sources"] if item["title"] == "论坛 / 模型讨论")
        self.assertEqual(
            topic,
            {
                "account_id": "main",
                "origin_id": FORUM,
                "topic_id": 7,
                "title": "论坛 / 模型讨论",
                "origin_type": "topic",
                "tags": ["ai"],
                "important": False,
            },
        )
        group = next(item for item in enabled["sources"] if item["origin_id"] == GROUP)
        self.assertNotIn("topic_id", group)
        self.assertNotIn("enabled", group)
        self.assertEqual(group["tags"], ["ai", "dev"])
        self.assertTrue(group["important"])

        self.assertEqual([item["origin_id"] for item in self.call("list_sources", {"important_only": True})["sources"]], [GROUP])
        self.assertEqual([item["origin_id"] for item in self.call("list_sources", {"tag": "NEWS"})["sources"]], [CHANNEL])
        self.assertEqual([item["origin_id"] for item in self.call("list_sources", {"query": "NEWSCH"})["sources"]], [CHANNEL])
        self.assertEqual(self.call("list_sources", {"query": "模型讨论"})["sources"][0]["topic_id"], 7)
        self.assertEqual(self.call("list_sources", {"account_id": "other"})["total"], 0)

        everything = self.call("list_sources", {"enabled_only": False})
        self.assertEqual(everything["total"], 5)
        unwatched = next(item for item in everything["sources"] if item["origin_id"] == UNWATCHED)
        self.assertFalse(unwatched["enabled"])

        limited = self.call("list_sources", {"limit": 2})
        self.assertEqual(len(limited["sources"]), 2)
        self.assertTrue(limited["has_more"])
        self.assertEqual(limited["total"], 4)
        self.assertIn("limit", limited["note"])

    def test_search_messages_matches_chinese_substrings(self) -> None:
        found = self.call("search_messages", {"query": "模型"})
        self.assertEqual(found["match"], "substring")
        self.assertEqual([item["message_id"] for item in found["messages"]], [1])
        self.assertFalse(found["has_more"])
        self.assertIn("90 days", found["note"])
        self.assertEqual(found["searched"]["date_to"], "now")
        self.assertTrue(found["searched"]["date_from"].endswith("+09:00"))
        message = found["messages"][0]
        self.assertEqual(
            set(message),
            {"account_id", "origin_id", "message_id", "origin_title", "sender_name", "sender_username", "sent_at", "text"},
        )
        self.assertEqual(message["origin_id"], GROUP)
        self.assertEqual(message["origin_title"], "研发群")
        self.assertEqual(message["text"], "今天发布了新模型，评测结果不错")
        self.assertTrue(message["sent_at"].endswith("+09:00"))

        self.assertEqual([m["message_id"] for m in self.call("search_messages", {"query": "模型 评测"})["messages"]], [1])
        self.assertEqual(self.call("search_messages", {"query": "模型 不存在"})["messages"], [])
        self.assertEqual([m["message_id"] for m in self.call("search_messages", {"query": "claude写"})["messages"]], [2])

        wide = self.call("search_messages", {"query": "模型", "date_from": "2020-01-01"})
        self.assertEqual([item["message_id"] for item in wide["messages"]], [1, 10])
        self.assertNotIn("note", wide)
        self.assertEqual(wide["messages"][1]["forward_from_name"], "Wire")

        in_origin = self.call("search_messages", {"query": "模型", "account_id": "main", "origin_id": CHANNEL})
        self.assertEqual([item["message_id"] for item in in_origin["messages"]], [10])
        self.assertEqual(in_origin["searched"]["date_from"], "archive start")

    def test_search_messages_excludes_deleted_by_default(self) -> None:
        self.assertEqual(self.call("search_messages", {"query": "被删除"})["messages"], [])
        included = self.call("search_messages", {"query": "被删除", "include_deleted": True})["messages"]
        self.assertEqual([item["message_id"] for item in included], [4])
        self.assertIn("deleted_at", included[0])

    def test_search_messages_uses_fts_for_ascii_queries(self) -> None:
        words = self.call("search_messages", {"query": "CLAUDE"})
        self.assertEqual(words["match"], "word")
        self.assertEqual(words["searched"], {"date_from": "archive start", "date_to": "now"})
        self.assertEqual([item["message_id"] for item in words["messages"]], [3])
        self.assertEqual(words["messages"][0]["permalink"], "https://t.me/c/1001/3")
        self.assertIn("substring=true", words["note"])

        self.assertEqual([m["message_id"] for m in self.call("search_messages", {"query": "claude gpt-5"})["messages"]], [3])
        self.assertEqual([m["message_id"] for m in self.call("search_messages", {"query": "100%"})["messages"]], [10])
        self.assertEqual(self.call("search_messages", {"query": "alice"})["messages"], [])

        substring = self.call("search_messages", {"query": "claude", "substring": True})
        self.assertEqual(substring["match"], "substring")
        self.assertEqual([item["message_id"] for item in substring["messages"]], [3, 2])

        for query in ('"claude" AND (', "NEAR(claude", "claude*", "text:claude", '"', "-claude", "claude OR", "a^2 {b}", "++ --"):
            result = self.call("search_messages", {"query": query})
            self.assertIsInstance(result["messages"], list, query)
        self.assertEqual(self.call("search_messages", {"query": "++ --"})["match"], "substring")

    def test_search_messages_escapes_like_wildcards(self) -> None:
        everything = {"date_from": "2020-01-01", "substring": True}
        self.assertEqual([m["message_id"] for m in self.call("search_messages", {"query": "0%", **everything})["messages"]], [10])
        self.assertEqual(self.call("search_messages", {"query": "%", "date_from": "2020-01-01"})["messages"][0]["message_id"], 10)
        self.assertEqual(self.call("search_messages", {"query": "_", **everything})["messages"], [])

    def test_search_messages_filters_and_pages(self) -> None:
        by_sender = self.call("search_messages", {"query": "claude", "substring": True, "sender": "ALICE"})
        self.assertEqual([item["message_id"] for item in by_sender["messages"]], [3])
        by_name = self.call("search_messages", {"query": "模型", "sender": "张"})
        self.assertEqual([item["message_id"] for item in by_name["messages"]], [1])
        in_topic = self.call(
            "search_messages", {"query": "讨论", "account_id": "main", "origin_id": FORUM, "topic_id": 7}
        )["messages"]
        self.assertEqual([item["message_id"] for item in in_topic], [20])
        self.assertEqual(in_topic[0]["origin_title"], "论坛 / 模型讨论")
        self.assertEqual(in_topic[0]["media_kind"], "photo")
        self.assertEqual(self.call("search_messages", {"query": "讨论", "account_id": "other"})["messages"], [])
        bounded = self.call(
            "search_messages",
            {"query": "claude", "substring": True, "date_to": (self.now - timedelta(minutes=90)).isoformat()},
        )
        self.assertEqual([item["message_id"] for item in bounded["messages"]], [2])

        first = self.call("search_messages", {"query": "claude", "substring": True, "limit": 1})
        self.assertEqual([item["message_id"] for item in first["messages"]], [3])
        self.assertTrue(first["has_more"])
        second = self.call("search_messages", {"query": "claude", "substring": True, "limit": 1, "cursor": first["next_cursor"]})
        self.assertEqual([item["message_id"] for item in second["messages"]], [2])
        self.assertFalse(second["has_more"])
        self.assertNotIn("next_cursor", second)

    def test_read_messages_time_window(self) -> None:
        recent = self.call("read_messages", {"hours": 5})
        self.assertEqual([item["message_id"] for item in recent["messages"]], [1, 2, 3, 20])
        self.assertFalse(recent["has_more"])
        self.assertEqual(recent["window"]["date_to"], "now")
        self.assertEqual(recent["messages"][1]["reply_to_message_id"], 1)
        self.assertEqual(recent["messages"][3]["topic_id"], 7)
        text = self.call_raw("read_messages", {"hours": 5})["content"][0]["text"]
        for field in ("raw_json", "reactions", "ingested_at", "version", "secret"):
            self.assertNotIn(field, text)
        self.assertIn("今天发布了新模型", text)

        self.assertEqual([m["message_id"] for m in self.call("read_messages")["messages"]], [1, 2, 3, 20])
        self.assertEqual([m["message_id"] for m in self.call("read_messages", {"hours": 1.5})["messages"]], [3, 20])
        newest_first = self.call("read_messages", {"hours": 5, "order": "desc"})
        self.assertEqual([item["message_id"] for item in newest_first["messages"]], [20, 3, 2, 1])
        with_deleted = self.call("read_messages", {"hours": 5, "include_deleted": True})
        self.assertEqual([item["message_id"] for item in with_deleted["messages"]], [1, 2, 3, 4, 20])

        one_origin = self.call("read_messages", {"hours": 5, "account_id": "main", "origin_id": GROUP})
        self.assertEqual([item["message_id"] for item in one_origin["messages"]], [1, 2, 3])
        one_topic = self.call("read_messages", {"hours": 5, "account_id": "main", "origin_id": FORUM, "topic_id": 7})
        self.assertEqual([item["message_id"] for item in one_topic["messages"]], [20])

        old_day = (self.now - timedelta(days=200)).astimezone(timezone(timedelta(hours=9))).date().isoformat()
        by_date = self.call("read_messages", {"date_from": old_day, "date_to": old_day})
        self.assertEqual([item["message_id"] for item in by_date["messages"]], [10])
        self.assertEqual(by_date["window"]["date_from"], f"{old_day}T00:00:00+09:00")
        self.assertIn("either hours or date_from", self.call_error("read_messages", {"hours": 1, "date_from": old_day}))

        seen: list[int] = []
        arguments: dict[str, Any] = {"hours": 5, "limit": 3}
        for _ in range(3):
            page = self.call("read_messages", arguments)
            seen.extend(item["message_id"] for item in page["messages"])
            if not page["has_more"]:
                break
            arguments["cursor"] = page["next_cursor"]
        self.assertEqual(seen, [1, 2, 3, 20])

    def test_read_messages_pages_stay_within_size_budget(self) -> None:
        for message_id in range(100, 110):
            sent_at = (self.now - timedelta(minutes=message_id)).isoformat()
            self.store.upsert_message(
                MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    chat_id=CHANNEL,
                    message_id=message_id,
                    sent_at=sent_at,
                    ingested_at=sent_at,
                    text="长" * 4000,
                ),
                event_type="new",
            )
        arguments: dict[str, Any] = {"hours": 2, "account_id": "main", "origin_id": CHANNEL, "limit": 500}
        seen: list[int] = []
        pages = 0
        while True:
            result = self.call_raw("read_messages", arguments)
            self.assertLess(len(result["content"][0]["text"]), 30000)
            page = json.loads(result["content"][0]["text"])
            seen.extend(item["message_id"] for item in page["messages"])
            pages += 1
            if not page["has_more"]:
                break
            arguments["cursor"] = page["next_cursor"]
        self.assertEqual(seen, list(range(109, 99, -1)))
        self.assertGreater(pages, 1)

    def test_read_messages_context(self) -> None:
        context = self.call(
            "read_messages",
            {"account_id": "main", "origin_id": GROUP, "around_message_id": 2, "context_messages": 1},
        )
        self.assertEqual([item["message_id"] for item in context["messages"]], [1, 2, 3])
        wider = self.call("read_messages", {"account_id": "main", "origin_id": GROUP, "around_message_id": 3})
        self.assertEqual([item["message_id"] for item in wider["messages"]], [1, 2, 3])
        with_deleted = self.call(
            "read_messages",
            {"account_id": "main", "origin_id": GROUP, "around_message_id": 3, "include_deleted": True},
        )
        self.assertEqual([item["message_id"] for item in with_deleted["messages"]], [1, 2, 3, 4])

        self.assertIn("was not found", self.call_error("read_messages", {"account_id": "main", "origin_id": GROUP, "around_message_id": 999}))
        self.assertIn("requires account_id and origin_id", self.call_error("read_messages", {"around_message_id": 2}))

    def test_daily_reports(self) -> None:
        listed = self.call("list_daily_reports")
        self.assertEqual({item["summary_id"] for item in listed["reports"]}, {"sum_daily", "sum_origin_a", "sum_origin_b", "sum_points"})
        self.assertFalse(listed["has_more"])
        daily = next(item for item in listed["reports"] if item["summary_id"] == "sum_daily")
        self.assertEqual(
            daily,
            {
                "summary_id": "sum_daily",
                "date": "2026-07-03",
                "record_type": "important_daily",
                "title": "重点日报",
                "tags": ["ai"],
                "content_length": 207,
            },
        )
        by_type = self.call("list_daily_reports", {"record_type": "important_origin"})
        self.assertEqual({item["summary_id"] for item in by_type["reports"]}, {"sum_origin_a", "sum_origin_b"})
        self.assertEqual([r["summary_id"] for r in self.call("list_daily_reports", {"date": "2026-07-04"})["reports"]], ["sum_points"])
        self.assertEqual([r["summary_id"] for r in self.call("list_daily_reports", {"date_from": "2026-07-04T12:00:00+09:00"})["reports"]], ["sum_points"])
        self.assertEqual(len(self.call("list_daily_reports", {"date_to": "2026-07-03"})["reports"]), 3)
        self.assertEqual([r["summary_id"] for r in self.call("list_daily_reports", {"query": "论坛"})["reports"]], ["sum_origin_b"])
        limited = self.call("list_daily_reports", {"limit": 1})
        self.assertEqual(len(limited["reports"]), 1)
        self.assertTrue(limited["has_more"])

        report = self.call("read_daily_report", {"summary_id": "sum_origin_a"})
        self.assertEqual(report["content"], "研发群的分析")
        self.assertEqual(report["record_type"], "important_origin")
        self.assertNotIn("truncated", report)

        by_date = self.call("read_daily_report", {"date": "2026-07-03", "record_type": "important_daily"})
        self.assertEqual(by_date["summary_id"], "sum_daily")
        self.assertTrue(by_date["content"].startswith("# 重点日报"))

        choices = self.call("read_daily_report", {"date": "2026-07-03", "record_type": "important_origin"})
        self.assertEqual({item["summary_id"] for item in choices["reports"]}, {"sum_origin_a", "sum_origin_b"})
        self.assertNotIn("content", choices)
        self.assertIn("summary_id", choices["note"])
        self.assertEqual(len(self.call("read_daily_report", {"date": "2026-07-03"})["reports"]), 3)

        head = self.call("read_daily_report", {"summary_id": "sum_daily", "max_chars": 100})
        self.assertEqual(len(head["content"]), 100)
        self.assertTrue(head["truncated"])
        self.assertEqual(head["next_offset"], 100)
        self.assertEqual(head["content_length"], 207)
        tail = self.call("read_daily_report", {"summary_id": "sum_daily", "max_chars": 200, "offset": head["next_offset"]})
        self.assertEqual(head["content"] + tail["content"], "# 重点日报\n" + "模型发布。" * 40)
        self.assertNotIn("truncated", tail)

        self.assertIn("Unknown summary_id", self.call_error("read_daily_report", {"summary_id": "missing"}))
        self.assertIn("No daily report found for 2026-01-01", self.call_error("read_daily_report", {"date": "2026-01-01"}))
        self.assertIn("Pass summary_id", self.call_error("read_daily_report"))

    def test_search_points(self) -> None:
        everything = self.call("search_points")
        self.assertEqual([item["point_id"] for item in everything["points"]], ["point_model", "point_chat"])
        self.assertFalse(everything["has_more"])
        self.assertEqual(
            everything["points"][0],
            {
                "point_id": "point_model",
                "date": "2026-07-03",
                "occurred_at": "2026-07-03T10:00:00+09:00",
                "origin_title": "研发群",
                "content": "新模型发布，评测领先",
                "tags": ["ai", "model"],
                "importance_score": 5,
                "importance_reason": "影响选型",
                "telegram_deeplink": "tg://privatepost?channel=1001&post=1",
                "account_id": "main",
                "origin_id": GROUP,
                "message_id": 1,
            },
        )
        self.assertNotIn("importance_reason", everything["points"][1])

        def ids(arguments: dict[str, Any]) -> list[str]:
            return [item["point_id"] for item in self.call("search_points", arguments)["points"]]

        self.assertEqual(ids({"query": "评测"}), ["point_model"])
        self.assertEqual(ids({"tags": ["AI", "model"]}), ["point_model"])
        self.assertEqual(ids({"importance_min": 4}), ["point_model"])
        self.assertEqual(ids({"date": "2026-07-03", "account_id": "main", "origin_id": GROUP, "origin_important": True}), ["point_model", "point_chat"])
        self.assertEqual(ids({"date_from": "2026-07-04"}), [])
        self.assertEqual(ids({"date_to": "2026-07-02"}), [])
        self.assertEqual(ids({"origin_important": False}), [])

        first = self.call("search_points", {"limit": 1})
        self.assertEqual([item["point_id"] for item in first["points"]], ["point_model"])
        self.assertTrue(first["has_more"])
        self.assertEqual(first["next_offset"], 1)
        second = self.call("search_points", {"limit": 1, "offset": first["next_offset"]})
        self.assertEqual([item["point_id"] for item in second["points"]], ["point_chat"])
        self.assertFalse(second["has_more"])

        point = everything["points"][0]
        context = self.call(
            "read_messages",
            {
                "account_id": point["account_id"],
                "origin_id": point["origin_id"],
                "around_message_id": point["message_id"],
                "context_messages": 1,
            },
        )
        self.assertEqual([item["message_id"] for item in context["messages"]], [1, 2])


if __name__ == "__main__":
    unittest.main()
