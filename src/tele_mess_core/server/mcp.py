from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date as date_type
from datetime import datetime, time, timedelta, timezone
from http import HTTPStatus
from typing import Any, Callable
from zoneinfo import ZoneInfo

from tele_mess_core import __version__
from tele_mess_core.archive import ArchiveStore
from tele_mess_core.daily import normalize_tag, parse_tags
from tele_mess_core.server.contracts import validate_schema_value


MCP_PROTOCOL_VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")
MCP_SERVER_NAME = "tele-mess-core"
MCP_INSTRUCTIONS = (
    "tele-mess-core is a read-only archive of the user's Telegram chats (groups, channels, forum topics, "
    "private chats) across their accounts, plus AI-written daily reports and message points distilled from "
    "those chats. Content is mostly Chinese.\n"
    "Typical flow: call `overview` first to learn the accounts, date ranges, tags, and timezone; use "
    "`list_sources` to turn a chat name into account_id + origin_id. For questions about what happened, "
    "prefer the distilled layers: `list_daily_reports` and `read_daily_report` for a day's narrative, "
    "`search_points` for individual facts, events, decisions, resources, and risks with source links. Use "
    "`search_messages` and `read_messages` for the raw messages: exact wording, who said what, or the "
    "conversation around a point (pass its account_id, origin_id, and message_id as around_message_id).\n"
    "Timestamps are returned in the archive timezone, and plain YYYY-MM-DD dates are interpreted in it. "
    "Results are paged: when has_more is true, follow next_cursor or next_offset."
)

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602

# Keeps one tool result within what MCP clients accept as a single response.
MAX_PAGE_CHARS = 24000
DEFAULT_SEARCH_DAYS = 90
DEFAULT_READ_HOURS = 24

_FTS_WORD = re.compile(r"[A-Za-z0-9]")


@dataclass(frozen=True, slots=True)
class McpTool:
    name: str
    description: str
    input_schema: dict[str, Any]
    handler: Callable[[ArchiveStore, dict[str, Any]], dict[str, Any]]

    def listing(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
            "annotations": {"readOnlyHint": True},
        }


def handle_mcp_request(store: ArchiveStore, body: bytes) -> tuple[HTTPStatus, dict[str, Any] | None]:
    """Handle one JSON-RPC message; a None payload means 202 Accepted with no body."""
    try:
        message = json.loads(body)
    except ValueError:
        return HTTPStatus.BAD_REQUEST, _rpc_error(None, PARSE_ERROR, "Parse error")
    if not isinstance(message, dict):
        return HTTPStatus.BAD_REQUEST, _rpc_error(None, INVALID_REQUEST, "Invalid Request")
    if "method" not in message or "id" not in message:
        return HTTPStatus.ACCEPTED, None

    request_id = message["id"]
    method = message["method"]
    params = message.get("params") or {}
    if not isinstance(params, dict):
        return HTTPStatus.OK, _rpc_error(request_id, INVALID_PARAMS, "params must be an object")
    if method == "initialize":
        result = _initialize(params)
    elif method == "ping":
        result = {}
    elif method == "tools/list":
        result = {"tools": [tool.listing() for tool in MCP_TOOLS]}
    elif method == "tools/call":
        name = params.get("name")
        if not isinstance(name, str) or name not in _TOOLS_BY_NAME:
            return HTTPStatus.OK, _rpc_error(request_id, INVALID_PARAMS, f"Unknown tool: {name}")
        result = _call_tool(store, _TOOLS_BY_NAME[name], params.get("arguments") or {})
    else:
        return HTTPStatus.OK, _rpc_error(request_id, METHOD_NOT_FOUND, f"Method not found: {method}")
    return HTTPStatus.OK, {"jsonrpc": "2.0", "id": request_id, "result": result}


def _rpc_error(request_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def _initialize(params: dict[str, Any]) -> dict[str, Any]:
    requested = params.get("protocolVersion")
    return {
        "protocolVersion": requested if requested in MCP_PROTOCOL_VERSIONS else MCP_PROTOCOL_VERSIONS[0],
        "capabilities": {"tools": {}},
        "serverInfo": {"name": MCP_SERVER_NAME, "version": __version__},
        "instructions": MCP_INSTRUCTIONS,
    }


def _call_tool(store: ArchiveStore, tool: McpTool, arguments: Any) -> dict[str, Any]:
    try:
        validate_schema_value(arguments, tool.input_schema, "arguments")
        payload = tool.handler(store, arguments)
    except ValueError as exc:
        return {"content": [{"type": "text", "text": str(exc)}], "isError": True}
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return {"content": [{"type": "text", "text": text}], "isError": False}


def _overview(store: ArchiveStore, args: dict[str, Any]) -> dict[str, Any]:
    tz = _zone(store)
    content = store.content_overview()
    origins = store.list_origins()
    reports = content["daily_reports"]
    tags = parse_tags([tag for policy in store.list_backup_policies() for tag in parse_tags(policy["tags"])])
    return {
        "timezone": tz.key,
        "now": datetime.now(tz).isoformat(timespec="seconds"),
        "accounts": [
            _compact({"account_id": account["account_id"], "display_name": account["display_name"]})
            for account in store.list_accounts()
        ],
        "messages": _compact(
            {
                "count": store.state()["message_count"],
                "first_sent_at": _local_iso(content["messages"]["first_sent_at"], tz),
                "last_sent_at": _local_iso(content["messages"]["last_sent_at"], tz),
            }
        ),
        "sources": {
            "known": len(origins),
            "enabled": sum(1 for origin in origins if (origin["backup_policy"] or {}).get("enabled")),
        },
        "tags": sorted(tags, key=str.lower),
        "daily_reports": _compact(
            {
                "count": sum(item["count"] for item in reports),
                "date_from": min((item["date_from"] for item in reports if item["date_from"]), default=None),
                "date_to": max((item["date_to"] for item in reports if item["date_to"]), default=None),
                "record_types": {
                    item["record_type"]: _compact(
                        {"count": item["count"], "date_from": item["date_from"], "date_to": item["date_to"]}
                    )
                    for item in reports
                },
            }
        ),
        "message_points": _compact(content["message_points"]),
    }


def _list_sources(store: ArchiveStore, args: dict[str, Any]) -> dict[str, Any]:
    tz = _zone(store)
    origins = store.list_origins(account_id=args.get("account_id"))
    parents = {(origin["account_id"], origin["origin_id"]): origin for origin in origins if not origin["topic_id"]}
    enabled_only = args.get("enabled_only", True)
    query = (args.get("query") or "").lower()
    tag = normalize_tag(args.get("tag") or "")
    matches: list[dict[str, Any]] = []
    for origin in origins:
        title = origin["title"]
        policy = origin["backup_policy"]
        if origin["topic_id"]:
            parent = parents.get((origin["account_id"], origin["origin_id"]))
            if parent is not None:
                # A topic without its own policy is captured under its group's policy.
                policy = policy or parent["backup_policy"]
                if parent["title"]:
                    title = f"{parent['title']} / {title or origin['topic_id']}"
        policy = policy or {}
        tags = parse_tags(policy.get("tags"))
        if enabled_only and not policy.get("enabled"):
            continue
        if args.get("important_only") and not origin["important"]:
            continue
        if tag and tag not in {normalize_tag(item) for item in tags}:
            continue
        if query and query not in f"{title or ''}\n{origin['username'] or ''}".lower():
            continue
        matches.append(
            {
                "account_id": origin["account_id"],
                "origin_id": origin["origin_id"],
                "topic_id": origin["topic_id"] or None,
                "title": title,
                "origin_type": origin["origin_type"],
                "username": origin["username"],
                "tags": tags,
                "important": origin["important"],
                "enabled": None if enabled_only else bool(policy.get("enabled")),
                "last_message_at": origin["last_message_at"],
            }
        )
    matches.sort(key=lambda item: item["last_message_at"] or "", reverse=True)
    limit = _bounded(args, "limit", 100, 500)
    result: dict[str, Any] = {
        "total": len(matches),
        "sources": [
            _compact({**item, "last_message_at": _local_iso(item["last_message_at"], tz)})
            for item in matches[:limit]
        ],
        "has_more": len(matches) > limit,
    }
    if result["has_more"]:
        result["note"] = "More sources match; raise limit or narrow with query, tag, or account_id."
    return result


def _search_messages(store: ArchiveStore, args: dict[str, Any]) -> dict[str, Any]:
    tz = _zone(store)
    terms = args["query"].split()
    if not terms:
        raise ValueError("query must contain at least one keyword")
    account_id, origin_id, topic_id = _origin_scope(args)
    sent_from = _utc_bound(args.get("date_from"), tz)
    sent_before = _utc_bound(args.get("date_to"), tz, end=True)
    notes: list[str] = []
    word_match = not args.get("substring") and all(term.isascii() and _FTS_WORD.search(term) for term in terms)
    if word_match:
        notes.append(
            "ASCII keywords matched as whole words; pass substring=true to also find a word written "
            "without spaces next to Chinese text."
        )
    elif sent_from is None and origin_id is None:
        sent_from = _utc_iso(datetime.now(timezone.utc) - timedelta(days=DEFAULT_SEARCH_DAYS))
        notes.append(
            f"No date_from was given, so only the last {DEFAULT_SEARCH_DAYS} days were searched; "
            "pass date_from to search further back."
        )
    limit = _bounded(args, "limit", 30, 100)
    rows = store.query_messages(
        fts_query="text : (" + " ".join(_fts_phrase(term) for term in terms) + ")" if word_match else None,
        text_terms=None if word_match else terms,
        account_id=account_id,
        origin_id=origin_id,
        topic_id=topic_id,
        sender=args.get("sender"),
        sent_from=sent_from,
        sent_before=sent_before,
        include_deleted=args.get("include_deleted", False),
        after=_parse_cursor(args.get("cursor")),
        limit=limit + 1,
    )
    return {
        "match": "word" if word_match else "substring",
        "searched": {
            "date_from": _local_iso(sent_from, tz) or "archive start",
            "date_to": _local_iso(sent_before, tz) or "now",
        },
        **_message_page(rows, limit, tz),
        **({"note": " ".join(notes)} if notes else {}),
    }


def _read_messages(store: ArchiveStore, args: dict[str, Any]) -> dict[str, Any]:
    tz = _zone(store)
    account_id, origin_id, topic_id = _origin_scope(args)
    include_deleted = args.get("include_deleted", False)
    if "around_message_id" in args:
        if origin_id is None:
            raise ValueError("around_message_id requires account_id and origin_id")
        context = _bounded(args, "context_messages", 10, 50)
        rows = store.list_message_context(
            account_id,
            origin_id,
            args["around_message_id"],
            before=context,
            after=context,
            include_deleted=include_deleted,
        )
        if not rows:
            raise ValueError(
                f"Message {args['around_message_id']} was not found in origin {origin_id} of account {account_id}"
            )
        return {"messages": [_message_item(row, tz) for row in rows]}

    if "hours" in args and "date_from" in args:
        raise ValueError("Pass either hours or date_from, not both")
    if "date_from" in args:
        sent_from = _utc_bound(args["date_from"], tz)
    else:
        hours = args.get("hours", DEFAULT_READ_HOURS)
        sent_from = _utc_iso(datetime.now(timezone.utc) - timedelta(hours=hours))
    sent_before = _utc_bound(args.get("date_to"), tz, end=True)
    limit = _bounded(args, "limit", 100, 500)
    rows = store.query_messages(
        account_id=account_id,
        origin_id=origin_id,
        topic_id=topic_id,
        sent_from=sent_from,
        sent_before=sent_before,
        include_deleted=include_deleted,
        descending=args.get("order") == "desc",
        after=_parse_cursor(args.get("cursor")),
        limit=limit + 1,
    )
    return {
        "window": {"date_from": _local_iso(sent_from, tz), "date_to": _local_iso(sent_before, tz) or "now"},
        **_message_page(rows, limit, tz),
    }


def _list_daily_reports(store: ArchiveStore, args: dict[str, Any]) -> dict[str, Any]:
    tz = _zone(store)
    limit = _bounded(args, "limit", 50, 200)
    records = store.list_daily_summary_records(
        date=_local_date(args.get("date"), tz),
        date_from=_local_date(args.get("date_from"), tz),
        date_to=_local_date(args.get("date_to"), tz),
        record_type=args.get("record_type"),
        q=args.get("query"),
        limit=limit + 1,
    )
    result: dict[str, Any] = {
        "reports": [_report_item(record) for record in records[:limit]],
        "has_more": len(records) > limit,
    }
    if result["has_more"]:
        result["note"] = "More reports match; narrow date_from/date_to or record_type, or raise limit."
    return result


def _read_daily_report(store: ArchiveStore, args: dict[str, Any]) -> dict[str, Any]:
    if "summary_id" in args:
        record = store.get_daily_summary_record(summary_id=args["summary_id"])
        if record is None:
            raise ValueError(f"Unknown summary_id: {args['summary_id']}")
    elif "date" in args:
        report_date = _local_date(args["date"], _zone(store))
        records = store.list_daily_summary_records(
            date=report_date,
            record_type=args.get("record_type"),
            include_content=True,
            limit=50,
        )
        if not records:
            raise ValueError(f"No daily report found for {report_date}; check list_daily_reports")
        if len(records) > 1:
            return {
                "reports": [_report_item(item) for item in records],
                "note": "Several reports match; call read_daily_report again with one summary_id.",
            }
        record = records[0]
    else:
        raise ValueError("Pass summary_id, or date (optionally with record_type)")
    content = record["content_md"]
    offset = max(0, args.get("offset", 0))
    chunk = content[offset : offset + _bounded(args, "max_chars", 12000, 50000)]
    result = {**_report_item(record), "content": chunk}
    if offset + len(chunk) < len(content):
        result["truncated"] = True
        result["next_offset"] = offset + len(chunk)
    return result


def _search_points(store: ArchiveStore, args: dict[str, Any]) -> dict[str, Any]:
    tz = _zone(store)
    limit = _bounded(args, "limit", 50, 200)
    offset = max(0, args.get("offset", 0))
    points = store.list_daily_message_points(
        date=_local_date(args.get("date"), tz),
        date_from=_local_date(args.get("date_from"), tz),
        date_to=_local_date(args.get("date_to"), tz),
        account_id=args.get("account_id"),
        origin_id=args.get("origin_id"),
        tags=args.get("tags"),
        importance_min=args.get("importance_min"),
        origin_important=args.get("origin_important"),
        q=args.get("query"),
        limit=limit + 1,
        offset=offset,
    )
    page = _fit_page(
        [
            _compact(
                {
                    "point_id": point["point_id"],
                    "date": point["date"],
                    "occurred_at": point["occurred_at"],
                    "origin_title": point["origin_title"],
                    "content": point["content"],
                    "tags": point["tags"],
                    "importance_score": point["importance_score"],
                    "importance_reason": point["importance_reason"],
                    "permalink": point["permalink"],
                    "telegram_deeplink": None if point["permalink"] else point["telegram_deeplink"],
                    "account_id": point["account_id"],
                    "origin_id": point["origin_id"],
                    "topic_id": point["topic_id"] or None,
                    "message_id": point["message_id"],
                }
            )
            for point in points
        ],
        limit,
    )
    result: dict[str, Any] = {"points": page, "has_more": len(page) < len(points)}
    if result["has_more"]:
        result["next_offset"] = offset + len(page)
    return result


def _zone(store: ArchiveStore) -> ZoneInfo:
    return ZoneInfo(store.get_daily_package_schedule()["timezone"])


def _compact(item: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in item.items() if value not in (None, "", [], {})}


def _bounded(args: dict[str, Any], key: str, default: int, maximum: int) -> int:
    return min(max(args.get(key, default), 1), maximum)


def _origin_scope(args: dict[str, Any]) -> tuple[str | None, int | None, int | None]:
    account_id = args.get("account_id")
    origin_id = args.get("origin_id")
    topic_id = args.get("topic_id")
    if origin_id is not None and account_id is None:
        raise ValueError("origin_id requires account_id; list_sources returns both")
    if topic_id is not None and origin_id is None:
        raise ValueError("topic_id requires account_id and origin_id")
    return account_id, origin_id, topic_id


def _parse_moment(value: str, tz: ZoneInfo, *, end: bool = False) -> datetime:
    """Parse YYYY-MM-DD or ISO 8601; a plain date used as an end bound covers that whole day."""
    try:
        day = date_type.fromisoformat(value)
    except ValueError:
        day = None
    if day is not None:
        return datetime.combine(day + timedelta(days=1 if end else 0), time.min, tz)
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid date {value!r}; use YYYY-MM-DD or an ISO 8601 date-time") from exc
    return moment if moment.tzinfo else moment.replace(tzinfo=tz)


def _utc_iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="seconds")


def _utc_bound(value: str | None, tz: ZoneInfo, *, end: bool = False) -> str | None:
    return _utc_iso(_parse_moment(value, tz, end=end)) if value else None


def _local_date(value: str | None, tz: ZoneInfo) -> str | None:
    return _parse_moment(value, tz).astimezone(tz).date().isoformat() if value else None


def _local_iso(value: str | None, tz: ZoneInfo) -> str | None:
    if not value:
        return None
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(tz).isoformat(timespec="seconds")


def _fts_phrase(term: str) -> str:
    return '"' + term.replace('"', '""') + '"'


def _parse_cursor(value: str | None) -> tuple[str, int] | None:
    if not value:
        return None
    sent_at, _, row_id = value.rpartition("|")
    if not sent_at or not row_id.isdigit():
        raise ValueError("Invalid cursor; pass next_cursor from the previous page unchanged")
    return sent_at, int(row_id)


def _fit_page(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Trim a page to `limit` items and to the response size budget, keeping at least one item."""
    page: list[dict[str, Any]] = []
    size = 0
    for item in items[:limit]:
        size += len(json.dumps(item, ensure_ascii=False))
        if page and size > MAX_PAGE_CHARS:
            break
        page.append(item)
    return page


def _message_item(row: dict[str, Any], tz: ZoneInfo) -> dict[str, Any]:
    return _compact(
        {
            "account_id": row["account_id"],
            "origin_id": row["chat_id"],
            "message_id": row["message_id"],
            "topic_id": row["topic_id"],
            "origin_title": row["origin_title"],
            "sender_name": row["sender_name"],
            "sender_username": row["sender_username"],
            "sent_at": _local_iso(row["sent_at"], tz),
            "text": row["text"],
            "media_kind": row["media_kind"],
            "reply_to_message_id": row["reply_to_message_id"],
            "forward_from_name": row["forward_from_name"],
            "permalink": row["permalink"],
            "edited_at": _local_iso(row["edited_at"], tz),
            "deleted_at": _local_iso(row["deleted_at"], tz),
        }
    )


def _message_page(rows: list[dict[str, Any]], limit: int, tz: ZoneInfo) -> dict[str, Any]:
    page = _fit_page([_message_item(row, tz) for row in rows], limit)
    result: dict[str, Any] = {"messages": page, "has_more": len(page) < len(rows)}
    if result["has_more"]:
        last = rows[len(page) - 1]
        result["next_cursor"] = f"{last['sent_at']}|{last['row_id']}"
    return result


def _report_item(record: dict[str, Any]) -> dict[str, Any]:
    return _compact(
        {
            "summary_id": record["summary_id"],
            "date": record["date"],
            "record_type": record["record_type"],
            "title": record["title"],
            "tags": record["tags"],
            "content_length": record["content_length"],
        }
    )


def _schema(properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "object", "properties": properties, "additionalProperties": False}
    if required:
        schema["required"] = required
    return schema


_DATE_NOTE = "YYYY-MM-DD or ISO 8601 date-time, read in the archive timezone unless it carries an offset."
_ACCOUNT_ID = {"type": "string", "description": "Local account ID, as returned by overview and list_sources."}
_ORIGIN_ID = {
    "type": "integer",
    "description": "Chat ID of one group, channel, or private chat (origin_id from list_sources). Requires account_id.",
}
_TOPIC_ID = {"type": "integer", "description": "Forum topic ID inside origin_id (topic_id from list_sources)."}
_INCLUDE_DELETED = {
    "type": "boolean",
    "default": False,
    "description": "Also return messages that were deleted on Telegram; they carry deleted_at.",
}
_MESSAGE_SHAPE = (
    "Each message has account_id, origin_id, message_id, origin_title, sender_name, sent_at, and text, plus "
    "topic_id, sender_username, media_kind, reply_to_message_id, forward_from_name, permalink, and edited_at "
    "when present."
)


MCP_TOOLS: tuple[McpTool, ...] = (
    McpTool(
        "overview",
        "Start here. Returns what this Telegram archive contains: accounts, total messages and their time "
        "range, how many chats are known and actively archived, every tag used to group chats, the dates and "
        "types of AI-written daily reports, the date range of message points, and the archive timezone with "
        "the current local time. Takes no arguments.",
        _schema({}),
        _overview,
    ),
    McpTool(
        "list_sources",
        "List the archived chats (groups, channels, forum topics, private chats) to find the account_id and "
        "origin_id that other tools filter on. Returns account_id, origin_id, topic_id (topics only), title "
        "(topics are shown as 'Group / Topic'), origin_type, username, tags, important, and last_message_at, "
        "most recently active first, with the total match count.",
        _schema(
            {
                "query": {
                    "type": "string",
                    "description": "Case-insensitive substring of the chat title or @username.",
                },
                "tag": {"type": "string", "description": "Only chats carrying this tag (see overview for tags)."},
                "important_only": {
                    "type": "boolean",
                    "default": False,
                    "description": "Only chats the user marked as important.",
                },
                "enabled_only": {
                    "type": "boolean",
                    "default": True,
                    "description": "Only chats that are actively archived. Set false to include every chat the "
                    "accounts know about; each item then carries an enabled flag.",
                },
                "account_id": _ACCOUNT_ID,
                "limit": {"type": "integer", "default": 100, "minimum": 1, "maximum": 500},
            }
        ),
        _list_sources,
    ),
    McpTool(
        "search_messages",
        "Search raw message text by keyword, newest first. Split the query on whitespace into keywords; a "
        "message must contain all of them. Keywords with non-ASCII characters (Chinese etc.) match as "
        f"substrings and, unless date_from or origin_id is given, only the last {DEFAULT_SEARCH_DAYS} days are "
        "searched. ASCII-only queries match whole words (case-insensitive) across the whole archive. The "
        "result states the match mode and the time range actually searched. " + _MESSAGE_SHAPE + " When "
        "has_more is true, repeat the call with cursor set to next_cursor. Use read_messages with "
        "around_message_id to see the conversation around a hit.",
        _schema(
            {
                "query": {"type": "string", "description": "One or more keywords separated by spaces (AND)."},
                "account_id": _ACCOUNT_ID,
                "origin_id": _ORIGIN_ID,
                "topic_id": _TOPIC_ID,
                "sender": {
                    "type": "string",
                    "description": "Substring of the sender's display name or username.",
                },
                "date_from": {"type": "string", "description": "Earliest send time, inclusive. " + _DATE_NOTE},
                "date_to": {
                    "type": "string",
                    "description": "Latest send time; a plain date includes that whole day. " + _DATE_NOTE,
                },
                "substring": {
                    "type": "boolean",
                    "default": False,
                    "description": "Match ASCII keywords as substrings instead of whole words. Needed to find "
                    "a Latin word written without spaces next to Chinese text (e.g. 用claude写); applies the "
                    f"{DEFAULT_SEARCH_DAYS}-day default window.",
                },
                "include_deleted": _INCLUDE_DELETED,
                "limit": {"type": "integer", "default": 30, "minimum": 1, "maximum": 100},
                "cursor": {"type": "string", "description": "next_cursor from the previous page."},
            },
            required=["query"],
        ),
        _search_messages,
    ),
    McpTool(
        "read_messages",
        "Read raw messages in time order. Two modes. (1) Time window: pass hours (the last N hours) or "
        "date_from/date_to; without either, the last "
        f"{DEFAULT_READ_HOURS} hours are read. Leave out origin_id to read across every chat, e.g. to see what "
        "was discussed recently, or pass account_id + origin_id (and topic_id) for one chat. (2) Context: pass "
        "account_id + origin_id + around_message_id to get that message with context_messages before and "
        "after it in the same chat and topic. " + _MESSAGE_SHAPE + " When has_more is true, repeat the call "
        "with cursor set to next_cursor; a page can end before limit when the messages are long.",
        _schema(
            {
                "hours": {"type": "number", "description": "Read the last N hours up to now."},
                "date_from": {"type": "string", "description": "Window start, inclusive. " + _DATE_NOTE},
                "date_to": {
                    "type": "string",
                    "description": "Window end; a plain date includes that whole day. " + _DATE_NOTE,
                },
                "account_id": _ACCOUNT_ID,
                "origin_id": _ORIGIN_ID,
                "topic_id": _TOPIC_ID,
                "around_message_id": {
                    "type": "integer",
                    "description": "Context mode: message_id to centre on, from search_messages or search_points.",
                },
                "context_messages": {
                    "type": "integer",
                    "default": 10,
                    "minimum": 1,
                    "maximum": 50,
                    "description": "Context mode: messages to return on each side of around_message_id.",
                },
                "order": {
                    "type": "string",
                    "enum": ["asc", "desc"],
                    "default": "asc",
                    "description": "Window mode: asc reads oldest first; desc reads newest first, which is the "
                    "way to get the latest messages of a long window.",
                },
                "include_deleted": _INCLUDE_DELETED,
                "limit": {"type": "integer", "default": 100, "minimum": 1, "maximum": 500},
                "cursor": {"type": "string", "description": "Window mode: next_cursor from the previous page."},
            }
        ),
        _read_messages,
    ),
    McpTool(
        "list_daily_reports",
        "List AI-written daily reports without their text, newest first. record_type is one of "
        "important_daily (the day's digest of important chats), important_origin (one deep report per "
        "important chat, several per day), point_daily (the day's digest built from message points), or the "
        "older tag_group. Returns summary_id, date, record_type, title, tags, and content_length; pass a "
        "summary_id to read_daily_report for the text.",
        _schema(
            {
                "date": {"type": "string", "description": "Reports for this local date (YYYY-MM-DD)."},
                "date_from": {"type": "string", "description": "Reports on or after this local date."},
                "date_to": {"type": "string", "description": "Reports on or before this local date."},
                "record_type": {"type": "string", "description": "Only this report type."},
                "query": {"type": "string", "description": "Substring of the report title or text."},
                "limit": {"type": "integer", "default": 50, "minimum": 1, "maximum": 200},
            }
        ),
        _list_daily_reports,
    ),
    McpTool(
        "read_daily_report",
        "Read the Markdown text of one daily report. Pass summary_id from list_daily_reports, or pass date "
        "(optionally with record_type) to read that day's report; when several reports match, their list is "
        "returned instead so you can pick a summary_id. Long reports are cut at max_chars: the result then has "
        "truncated=true and next_offset, which you pass back as offset to continue.",
        _schema(
            {
                "summary_id": {"type": "string", "description": "Report ID from list_daily_reports."},
                "date": {"type": "string", "description": "Local report date (YYYY-MM-DD)."},
                "record_type": {
                    "type": "string",
                    "description": "With date: important_daily, important_origin, or point_daily.",
                },
                "max_chars": {"type": "integer", "default": 12000, "minimum": 1, "maximum": 50000},
                "offset": {
                    "type": "integer",
                    "default": 0,
                    "minimum": 0,
                    "description": "Character offset to continue from (next_offset of the previous call).",
                },
            }
        ),
        _read_daily_report,
    ),
    McpTool(
        "search_points",
        "Search message points: short AI-extracted statements of facts, events, decisions, resources, and "
        "risks, each tied to a source message. Faster and denser than raw messages for finding out what "
        "happened. Returns point_id, date, occurred_at, origin_title, content, tags, importance_score (1-5), "
        "importance_reason, permalink or telegram_deeplink, and account_id + origin_id + message_id, which "
        "read_messages accepts as around_message_id to show the original conversation. Newest first; when "
        "has_more is true, repeat the call with offset set to next_offset.",
        _schema(
            {
                "query": {
                    "type": "string",
                    "description": "One substring matched against point content, chat title, or importance "
                    "reason (a phrase, not a list of keywords).",
                },
                "date": {"type": "string", "description": "Points for this local date (YYYY-MM-DD)."},
                "date_from": {"type": "string", "description": "Points on or after this local date."},
                "date_to": {"type": "string", "description": "Points on or before this local date."},
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Only points carrying all of these tags.",
                },
                "importance_min": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 5,
                    "description": "Minimum importance score; 4 keeps only the notable points.",
                },
                "account_id": _ACCOUNT_ID,
                "origin_id": {"type": "integer", "description": "Only points from this chat (origin_id)."},
                "origin_important": {
                    "type": "boolean",
                    "description": "Only points from chats marked important (true) or not (false).",
                },
                "limit": {"type": "integer", "default": 50, "minimum": 1, "maximum": 200},
                "offset": {"type": "integer", "default": 0, "minimum": 0},
            }
        ),
        _search_points,
    ),
)

_TOOLS_BY_NAME = {tool.name: tool for tool in MCP_TOOLS}
