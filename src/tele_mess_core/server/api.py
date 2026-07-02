from __future__ import annotations

import asyncio
import json
import logging
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.models import (
    AccountAuthRecord,
    AccountRecord,
    BackupPolicyRecord,
    OriginRecord,
    ParticipantRecord,
    SOURCE_TELEGRAM,
    utc_now_iso,
)
from tele_mess_core.telegram.runtime import TelegramOperationError

if TYPE_CHECKING:
    from tele_mess_core.config import AppConfig, TelegramAccountConfig


class SyncApiServer:
    def __init__(
        self,
        store: ArchiveStore,
        host: str,
        port: int,
        token: str = "",
        config: "AppConfig | None" = None,
    ):
        self.store = store
        self.host = host
        self.port = port
        self.token = token
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._startup_event: threading.Event | None = None
        self._startup_error: BaseException | None = None

    def serve_forever(self) -> None:
        handler = self._make_handler()
        self._httpd = ThreadingHTTPServer((self.host, self.port), handler)
        if self._startup_event:
            self._startup_event.set()
        self.logger.info("Sync API listening on http://%s:%s", self.host, self.port)
        self._httpd.serve_forever()

    def start_background(self, startup_timeout: float = 5.0) -> None:
        startup_event = threading.Event()
        self._startup_event = startup_event
        self._startup_error = None

        def target() -> None:
            try:
                self.serve_forever()
            except BaseException as exc:
                self._startup_error = exc
                startup_event.set()
                self.logger.error("Sync API failed to start: %s", exc)
            finally:
                self._startup_event = None

        self._thread = threading.Thread(target=target, name="sync-api", daemon=True)
        self._thread.start()
        if not startup_event.wait(startup_timeout):
            raise RuntimeError("Sync API did not report startup before timeout")
        if self._startup_error is not None:
            raise RuntimeError(f"Sync API failed to start: {self._startup_error}") from self._startup_error

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()
        if self._thread:
            self._thread.join(timeout=5)

    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        store = self.store
        token = self.token
        config = self.config

        class Handler(BaseHTTPRequestHandler):
            server_version = "tele-mess-core/0.1"

            def log_message(self, fmt: str, *args: Any) -> None:
                logging.getLogger("tele_mess_core.server").info(fmt, *args)

            def do_GET(self) -> None:
                self._handle("GET")

            def do_POST(self) -> None:
                self._handle("POST")

            def do_PATCH(self) -> None:
                self._handle("PATCH")

            def do_DELETE(self) -> None:
                self._handle("DELETE")

            def _handle(self, method: str) -> None:
                parsed = urlparse(self.path)
                params = parse_qs(parsed.query)
                if token and not (method == "GET" and parsed.path in {"/", "/console"}) and not self._authorized(token):
                    self._json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                    return

                try:
                    if method == "GET":
                        self._handle_get(parsed.path, params)
                    elif method in {"POST", "PATCH", "DELETE"}:
                        self._handle_write(method, parsed.path)
                    else:
                        self._json({"error": "method_not_allowed"}, status=HTTPStatus.METHOD_NOT_ALLOWED)
                except ValueError as exc:
                    self._json({"error": "bad_request", "detail": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                except TelegramOperationError as exc:
                    self._json({"error": exc.code, "detail": exc.message, **exc.to_public_dict()}, status=exc.http_status)
                except Exception as exc:
                    logging.getLogger("tele_mess_core.server").exception("API request failed")
                    self._json({"error": "internal_error", "detail": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

            def _handle_get(self, path: str, params: dict[str, list[str]]) -> None:
                if path == "/" or path == "/console":
                    self._html(_console_html())
                elif path == "/healthz":
                    self._json({"ok": True, **store.state()})
                elif path == "/sync/state":
                    self._json(store.state())
                elif path == "/sync/events":
                    self._json(store.list_events(after=_int_param(params, "after", 0), limit=_int_param(params, "limit", 500)))
                elif path == "/sync/messages":
                    self._json(
                        store.list_messages_after(
                            after_event_seq=_int_param(params, "after", 0),
                            limit=_int_param(params, "limit", 500),
                        )
                    )
                elif path == "/sync/chats":
                    self._json({"items": store.list_chats()})
                elif path == "/sync/accounts":
                    self._json({"items": store.list_accounts()})
                elif path == "/sync/search":
                    query = _str_param(params, "q", "")
                    if not query:
                        self._json({"items": []})
                    else:
                        self._json({"items": store.search_messages(query, limit=_int_param(params, "limit", 50))})
                elif path == "/sync/media-files":
                    self._json(
                        {
                            "items": store.list_media_files(
                                account_id=_optional_str_param(params, "account_id"),
                                chat_id=_optional_int_param(params, "chat_id"),
                                message_id=_optional_int_param(params, "message_id"),
                                limit=_int_param(params, "limit", 500),
                            )
                        }
                    )
                elif path == "/manage/capabilities":
                    self._json(_capabilities())
                elif path == "/manage/accounts":
                    self._json({"items": store.list_management_accounts()})
                elif path == "/manage/origins":
                    self._json(
                        {
                            "items": store.list_origins(
                                account_id=_optional_str_param(params, "account_id"),
                                include_archived=_bool_param(params, "include_archived", False),
                            )
                        }
                    )
                elif path == "/manage/backup-policies":
                    self._json({"items": store.list_backup_policies(account_id=_optional_str_param(params, "account_id"))})
                elif path == "/manage/participants":
                    self._json(
                        {
                            "items": store.list_participants(
                                account_id=_optional_str_param(params, "account_id"),
                                origin_id=_optional_int_param(params, "origin_id"),
                            )
                        }
                    )
                elif path == "/manage/capture-cursors":
                    self._json({"items": store.list_capture_cursors(account_id=_optional_str_param(params, "account_id"))})
                elif path == "/manage/operation-events":
                    self._json(
                        {
                            "items": store.list_operation_events(
                                account_id=_optional_str_param(params, "account_id"),
                                status=_optional_str_param(params, "status"),
                                limit=_int_param(params, "limit", 100),
                            )
                        }
                    )
                else:
                    self._json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

            def _handle_write(self, method: str, path: str) -> None:
                payload = self._read_json()
                if path == "/manage/accounts" and method == "POST":
                    item = _create_management_account(store, payload)
                    self._json({"item": item}, status=HTTPStatus.CREATED)
                elif path == "/manage/accounts" and method == "DELETE":
                    item = _delete_management_account(store, payload)
                    self._json({"item": item})
                elif path == "/manage/accounts/auth" and method in {"POST", "PATCH"}:
                    item = _update_account_auth(store, payload)
                    self._json({"item": item})
                elif path == "/manage/accounts/auth/status" and method == "POST":
                    item = _auth_status(config, store, payload)
                    self._json({"item": item})
                elif path == "/manage/accounts/auth/request-code" and method == "POST":
                    item = _request_auth_code(config, store, payload)
                    self._json({"item": item})
                elif path == "/manage/accounts/auth/submit-code" and method == "POST":
                    item = _submit_auth_code(config, store, payload)
                    self._json({"item": item})
                elif path == "/manage/origins" and method == "POST":
                    item = _create_origin(store, payload)
                    self._json({"item": item}, status=HTTPStatus.CREATED)
                elif path == "/manage/origins/archive" and method == "PATCH":
                    item = _archive_origin(store, payload)
                    self._json({"item": item})
                elif path == "/manage/backup-policies" and method in {"POST", "PATCH"}:
                    item = _set_backup_policy(store, payload)
                    self._json({"item": item})
                elif path == "/manage/backup-policies" and method == "DELETE":
                    item = _delete_backup_policy(store, payload)
                    self._json({"item": item})
                elif path == "/manage/participants" and method == "POST":
                    item = _create_participant(store, payload)
                    self._json({"item": item}, status=HTTPStatus.CREATED)
                elif path == "/manage/participants" and method == "DELETE":
                    item = _delete_participant(store, payload)
                    self._json({"item": item})
                elif path == "/manage/origins" and method == "DELETE":
                    item = _delete_origin(store, payload)
                    self._json({"item": item})
                elif path == "/manage/discover-origins" and method == "POST":
                    item = _discover_origins(config, store, payload)
                    self._json({"item": item})
                elif path == "/manage/participants/refresh" and method == "POST":
                    item = _refresh_participants(config, store, payload)
                    self._json({"item": item})
                else:
                    self._json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

            def _authorized(self, expected: str) -> bool:
                auth = self.headers.get("Authorization", "")
                if auth == f"Bearer {expected}":
                    return True
                return self.headers.get("X-Api-Token") == expected

            def _read_json(self) -> dict[str, Any]:
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length <= 0:
                    return {}
                body = self.rfile.read(length).decode("utf-8")
                data = json.loads(body)
                if not isinstance(data, dict):
                    raise ValueError("JSON body must be an object")
                return data

            def _json(self, data: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
                body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
                self.send_response(int(status))
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _html(self, html: str, status: HTTPStatus = HTTPStatus.OK) -> None:
                body = html.encode("utf-8")
                self.send_response(int(status))
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler


def _capabilities() -> dict[str, Any]:
    return {
        "mode": "single-user-multi-telegram-account",
        "sync": ["state", "events", "messages", "accounts", "chats", "search", "media_files"],
        "management": [
            "account_status",
            "account_auth_state",
            "origin_registry",
            "backup_policy",
            "participant_registry",
            "capture_cursors",
            "live_origin_discovery",
            "live_participant_refresh",
            "live_account_auth",
            "operation_events",
            "web_console",
        ],
        "auth_flow": {
            "status": "implemented",
            "note": "Remote clients can request a Telegram login code, submit it, and provide a 2FA password when Telegram requires one.",
        },
    }


def _create_management_account(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    source = _source(payload)
    account_id = _required_str(payload, "account_id")
    display_name = str(payload.get("display_name") or account_id)
    now = utc_now_iso()
    raw = _public_raw_json(payload, hidden={"api_hash", "password", "code"})
    store.upsert_account(
        AccountRecord(
            source=source,
            account_id=account_id,
            display_name=display_name,
            kind=str(payload.get("kind") or "telegram"),
            updated_at=now,
            raw_json=raw,
        )
    )
    store.upsert_account_auth(
        AccountAuthRecord(
            source=source,
            account_id=account_id,
            auth_state=str(payload.get("auth_state") or "pending_auth"),
            phone=_optional_payload_str(payload, "phone"),
            session_name=_optional_payload_str(payload, "session_name") or account_id,
            session_dir=_optional_payload_str(payload, "session_dir"),
            updated_at=now,
            raw_json=raw,
        )
    )
    return _find_account(store, source, account_id)


def _update_account_auth(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    source = _source(payload)
    account_id = _required_str(payload, "account_id")
    now = utc_now_iso()
    store.upsert_account_auth(
        AccountAuthRecord(
            source=source,
            account_id=account_id,
            auth_state=str(payload.get("auth_state") or payload.get("status") or "pending_auth"),
            phone=_optional_payload_str(payload, "phone"),
            session_name=_optional_payload_str(payload, "session_name"),
            session_dir=_optional_payload_str(payload, "session_dir"),
            last_error=_optional_payload_str(payload, "last_error"),
            updated_at=now,
            raw_json=_public_raw_json(payload, hidden={"api_hash", "password", "code"}),
        )
    )
    return _find_account(store, source, account_id)


def _delete_management_account(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    source = _source(payload)
    account_id = _required_str(payload, "account_id")
    deleted_rows = store.delete_management_account(source, account_id)
    return {"source": source, "account_id": account_id, "deleted_rows": deleted_rows}


def _create_origin(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    source = _source(payload)
    account_id = _required_str(payload, "account_id")
    origin_id = _required_int(payload, "origin_id")
    topic_id = _payload_int(payload, "topic_id", 0)
    store.upsert_origin(
        OriginRecord(
            source=source,
            account_id=account_id,
            origin_id=origin_id,
            topic_id=topic_id,
            origin_type=_required_str(payload, "origin_type"),
            parent_origin_id=_optional_payload_int(payload, "parent_origin_id"),
            title=_optional_payload_str(payload, "title"),
            username=_optional_payload_str(payload, "username"),
            is_forum=_payload_bool(payload, "is_forum", False),
            updated_at=utc_now_iso(),
            raw_json=_public_raw_json(payload),
        )
    )
    return _find_origin(store, account_id, origin_id, topic_id)


def _delete_origin(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    source = _source(payload)
    account_id = _required_str(payload, "account_id")
    origin_id = _required_int(payload, "origin_id")
    topic_id = _payload_int(payload, "topic_id", 0)
    deleted_rows = store.delete_origin(source, account_id, origin_id, topic_id)
    return {
        "source": source,
        "account_id": account_id,
        "origin_id": origin_id,
        "topic_id": topic_id,
        "deleted_rows": deleted_rows,
    }


def _archive_origin(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    source = _source(payload)
    account_id = _required_str(payload, "account_id")
    origin_id = _required_int(payload, "origin_id")
    topic_id = _payload_int(payload, "topic_id", 0)
    archived = _payload_bool(payload, "archived", True)
    changed_rows = store.archive_origin(source, account_id, origin_id, topic_id, archived)
    return {
        "source": source,
        "account_id": account_id,
        "origin_id": origin_id,
        "topic_id": topic_id,
        "archived": archived,
        "changed_rows": changed_rows,
    }


def _set_backup_policy(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    source = _source(payload)
    account_id = _required_str(payload, "account_id")
    origin_id = _required_int(payload, "origin_id")
    topic_id = _payload_int(payload, "topic_id", 0)
    store.set_backup_policy(
        BackupPolicyRecord(
            source=source,
            account_id=account_id,
            origin_id=origin_id,
            topic_id=topic_id,
            enabled=_payload_bool(payload, "enabled", False),
            capture_text=_payload_bool(payload, "capture_text", True),
            capture_media_metadata=_payload_bool(payload, "capture_media_metadata", True),
            download_media=_payload_bool(payload, "download_media", False),
            tags=_optional_payload_str(payload, "tags"),
            updated_at=utc_now_iso(),
        )
    )
    return _find_policy(store, account_id, origin_id, topic_id)


def _delete_backup_policy(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    source = _source(payload)
    account_id = _required_str(payload, "account_id")
    origin_id = _required_int(payload, "origin_id")
    topic_id = _payload_int(payload, "topic_id", 0)
    deleted_rows = store.delete_backup_policy(source, account_id, origin_id, topic_id)
    return {
        "source": source,
        "account_id": account_id,
        "origin_id": origin_id,
        "topic_id": topic_id,
        "deleted_rows": deleted_rows,
    }


def _create_participant(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    source = _source(payload)
    account_id = _required_str(payload, "account_id")
    origin_id = _required_int(payload, "origin_id")
    user_id = _required_int(payload, "user_id")
    store.upsert_participant(
        ParticipantRecord(
            source=source,
            account_id=account_id,
            origin_id=origin_id,
            user_id=user_id,
            username=_optional_payload_str(payload, "username"),
            display_name=_optional_payload_str(payload, "display_name"),
            is_bot=_payload_bool(payload, "is_bot", False),
            role=_optional_payload_str(payload, "role"),
            last_seen_at=_optional_payload_str(payload, "last_seen_at"),
            updated_at=utc_now_iso(),
            raw_json=_public_raw_json(payload),
        )
    )
    return _find_participant(store, account_id, origin_id, user_id)


def _delete_participant(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    source = _source(payload)
    account_id = _required_str(payload, "account_id")
    origin_id = _required_int(payload, "origin_id")
    user_id = _required_int(payload, "user_id")
    deleted_rows = store.delete_participant(source, account_id, origin_id, user_id)
    return {
        "source": source,
        "account_id": account_id,
        "origin_id": origin_id,
        "user_id": user_id,
        "deleted_rows": deleted_rows,
    }


def _auth_status(config: "AppConfig | None", store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    account = _account_config(config, store, _required_str(payload, "account_id"), _source(payload))
    from tele_mess_core.telegram.auth import TelegramAuthService

    return asyncio.run(TelegramAuthService(account, store).status())


def _request_auth_code(config: "AppConfig | None", store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    account = _account_config(config, store, _required_str(payload, "account_id"), _source(payload))
    phone = _required_str(payload, "phone")
    from tele_mess_core.telegram.auth import TelegramAuthService

    return asyncio.run(TelegramAuthService(account, store).request_code(phone))


def _submit_auth_code(config: "AppConfig | None", store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    account = _account_config(config, store, _required_str(payload, "account_id"), _source(payload))
    phone = _required_str(payload, "phone")
    code = _required_str(payload, "code")
    password = _optional_payload_str(payload, "password")
    from tele_mess_core.telegram.auth import TelegramAuthService

    return asyncio.run(TelegramAuthService(account, store).submit_code(phone, code, password))


def _discover_origins(config: "AppConfig | None", store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    account = _account_config(config, store, _required_str(payload, "account_id"), _source(payload))
    include_topics = _payload_bool(payload, "include_topics", True)
    topic_limit = _payload_int(payload, "topic_limit", 100)
    from tele_mess_core.telegram.discovery import TelegramDiscoveryService

    return asyncio.run(TelegramDiscoveryService(account, store).discover_origins(include_topics, topic_limit))


def _refresh_participants(config: "AppConfig | None", store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    account = _account_config(config, store, _required_str(payload, "account_id"), _source(payload))
    origin_id = _required_int(payload, "origin_id")
    limit = _payload_int(payload, "limit", 500)
    from tele_mess_core.telegram.discovery import TelegramDiscoveryService

    return asyncio.run(TelegramDiscoveryService(account, store).refresh_participants(origin_id, limit))


def _account_config(
    config: "AppConfig | None",
    store: ArchiveStore,
    account_id: str,
    source: str = SOURCE_TELEGRAM,
) -> "TelegramAccountConfig":
    if config is None:
        raise ValueError("Server config is required for live Telegram management actions")
    for account in config.telegram.accounts:
        if account.account_id == account_id:
            return account
    if not config.telegram.accounts:
        raise ValueError("At least one configured Telegram account is required to authenticate a new account")
    stored = None
    for item in store.list_management_accounts():
        if item["source"] == source and item["account_id"] == account_id:
            stored = item
            break
    if stored is None:
        raise ValueError(f"Unknown account_id: {account_id}")

    from tele_mess_core.config import TelegramAccountConfig

    template = config.telegram.accounts[0]
    session_dir_raw = stored.get("session_dir")
    session_dir = Path(session_dir_raw).expanduser() if session_dir_raw else template.session_dir
    return TelegramAccountConfig(
        account_id=account_id,
        api_id=template.api_id,
        api_hash=template.api_hash,
        session_name=str(stored.get("session_name") or account_id),
        session_dir=session_dir,
        timezone=template.timezone,
        chats=[],
    )


def _find_account(store: ArchiveStore, source: str, account_id: str) -> dict[str, Any]:
    for item in store.list_management_accounts():
        if item["source"] == source and item["account_id"] == account_id:
            return item
    raise ValueError("account was not persisted")


def _find_origin(store: ArchiveStore, account_id: str, origin_id: int, topic_id: int) -> dict[str, Any]:
    for item in store.list_origins(account_id=account_id):
        if item["origin_id"] == origin_id and item["topic_id"] == topic_id:
            return item
    raise ValueError("origin was not persisted")


def _find_policy(store: ArchiveStore, account_id: str, origin_id: int, topic_id: int) -> dict[str, Any]:
    for item in store.list_backup_policies(account_id=account_id):
        if item["origin_id"] == origin_id and item["topic_id"] == topic_id:
            return item
    raise ValueError("backup policy was not persisted")


def _find_participant(store: ArchiveStore, account_id: str, origin_id: int, user_id: int) -> dict[str, Any]:
    for item in store.list_participants(account_id=account_id, origin_id=origin_id):
        if item["user_id"] == user_id:
            return item
    raise ValueError("participant was not persisted")


def _source(payload: dict[str, Any]) -> str:
    return str(payload.get("source") or SOURCE_TELEGRAM)


def _public_raw_json(payload: dict[str, Any], hidden: set[str] | None = None) -> str:
    hidden = hidden or set()
    public = {key: value for key, value in payload.items() if key not in hidden}
    return json.dumps(public, ensure_ascii=False, default=str)


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing required field: {key}")
    return str(value)


def _required_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing required field: {key}")
    return int(value)


def _optional_payload_str(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None or value == "":
        return None
    return str(value)


def _optional_payload_int(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if value is None or value == "":
        return None
    return int(value)


def _payload_int(payload: dict[str, Any], key: str, default: int) -> int:
    value = payload.get(key, default)
    if value is None or value == "":
        return default
    return int(value)


def _payload_bool(payload: dict[str, Any], key: str, default: bool) -> bool:
    value = payload.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _int_param(params: dict[str, list[str]], key: str, default: int) -> int:
    try:
        return int(params.get(key, [str(default)])[0])
    except (TypeError, ValueError):
        return default


def _optional_int_param(params: dict[str, list[str]], key: str) -> int | None:
    if key not in params:
        return None
    try:
        return int(params[key][0])
    except (TypeError, ValueError):
        return None


def _bool_param(params: dict[str, list[str]], key: str, default: bool) -> bool:
    if key not in params:
        return default
    value = params[key][0]
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _str_param(params: dict[str, list[str]], key: str, default: str) -> str:
    return params.get(key, [default])[0]


def _optional_str_param(params: dict[str, list[str]], key: str) -> str | None:
    value = params.get(key, [None])[0]
    return value if value else None


def _console_html() -> str:
    return """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>tele-mess-core console</title>
  <style>
    :root {
      color-scheme: light;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif;
      --bg: #f4f6f8;
      --surface: #ffffff;
      --surface-2: #eef2f6;
      --text: #151922;
      --muted: #647084;
      --line: #d8dee8;
      --line-strong: #b9c2d0;
      --primary: #1f6feb;
      --primary-2: #1859be;
      --danger: #b42318;
      --ok: #166534;
      --warn: #9a6700;
      --radius: 8px;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--text); }
    header { position: sticky; top: 0; z-index: 5; background: var(--surface); border-bottom: 1px solid var(--line); }
    .topbar { max-width: 1440px; margin: 0 auto; padding: 14px 20px; display: grid; grid-template-columns: 220px minmax(0, 1fr) auto; gap: 14px; align-items: center; }
    h1 { margin: 0; font-size: 19px; font-weight: 680; letter-spacing: 0; }
    h2 { margin: 0; font-size: 15px; font-weight: 680; letter-spacing: 0; }
    h3 { margin: 0 0 8px; font-size: 13px; font-weight: 680; letter-spacing: 0; color: var(--muted); }
    main { max-width: 1440px; margin: 0 auto; padding: 18px 20px 42px; display: grid; gap: 16px; }
    .token-row { display: grid; grid-template-columns: minmax(160px, 360px) auto auto; gap: 8px; align-items: center; }
    .tabs { display: flex; flex-wrap: wrap; gap: 6px; }
    .tab { background: transparent; color: var(--text); border-color: transparent; }
    .tab.active { background: #dbeafe; color: #0f3f8f; border-color: #bfdbfe; }
    .grid { display: grid; grid-template-columns: minmax(280px, 380px) minmax(0, 1fr); gap: 16px; align-items: start; }
    .panel { background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius); padding: 14px; }
    summary { cursor: pointer; }
    summary h2 { display: inline; }
    details .form-grid { margin-top: 12px; }
    .panel-head { display: flex; justify-content: space-between; gap: 12px; align-items: center; margin-bottom: 12px; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
    .form-grid { display: grid; gap: 10px; }
    .two { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    label { display: grid; gap: 5px; font-size: 12px; color: var(--muted); }
    input, select, button { font: inherit; min-height: 34px; }
    input, select { width: 100%; border: 1px solid var(--line-strong); border-radius: 6px; padding: 7px 9px; background: #fff; color: var(--text); }
    input[type=checkbox] { width: 16px; min-height: 16px; }
    .check { display: flex; gap: 8px; align-items: center; color: var(--text); }
    button { border: 1px solid var(--line-strong); border-radius: 6px; padding: 7px 10px; background: var(--surface-2); color: var(--text); cursor: pointer; white-space: nowrap; }
    button.primary { background: var(--primary); border-color: var(--primary); color: #fff; }
    button.primary:hover { background: var(--primary-2); }
    button.danger { color: var(--danger); }
    button:disabled { opacity: .55; cursor: not-allowed; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { border-bottom: 1px solid var(--line); padding: 8px 7px; text-align: left; vertical-align: top; }
    th { color: var(--muted); font-weight: 650; background: #fafbfc; }
    td.actions { width: 1%; white-space: nowrap; }
    td.actions button + button { margin-left: 6px; }
    tr.archived-row { color: var(--muted); }
    .tree-cell { display: flex; align-items: center; gap: 7px; }
    .tree-toggle, .tree-spacer { width: 28px; min-width: 28px; text-align: center; padding-left: 0; padding-right: 0; }
    .topic-row .tree-cell { padding-left: 28px; }
    .tag-list { color: var(--muted); font-size: 12px; }
    .status { min-height: 28px; border: 1px solid var(--line); border-radius: 6px; background: #fbfcfe; padding: 7px 9px; font-size: 13px; color: var(--muted); }
    .status.ok { color: var(--ok); border-color: #bbdfc5; background: #f0fdf4; }
    .status.error { color: var(--danger); border-color: #f1b7b2; background: #fff5f5; }
    .pill { display: inline-flex; align-items: center; border: 1px solid var(--line-strong); border-radius: 999px; padding: 2px 7px; color: var(--muted); font-size: 12px; }
    .pill.ok { color: var(--ok); border-color: #86c79a; }
    .pill.warn { color: var(--warn); border-color: #dfc16d; }
    .muted { color: var(--muted); }
    .hidden { display: none; }
    .stack { display: grid; gap: 12px; }
    .table-wrap { overflow: auto; max-height: calc(100vh - 240px); border: 1px solid var(--line); border-radius: 6px; }
    .table-wrap table { min-width: 680px; }
    .table-wrap thead th { position: sticky; top: 0; z-index: 2; box-shadow: 0 1px 0 var(--line); }
    pre { margin: 0; overflow: auto; background: #101828; color: #e5e7eb; border-radius: 6px; padding: 10px; max-height: 280px; font-size: 12px; }
    @media (max-width: 920px) { .topbar, .grid, .two { grid-template-columns: 1fr; } .token-row { grid-template-columns: 1fr; } th { position: static; } }
  </style>
</head>
<body>
<header>
  <div class=\"topbar\">
    <h1>tele-mess-core</h1>
    <nav class=\"tabs\" aria-label=\"Views\">
      <button class=\"tab active\" data-view=\"overview\">Overview</button>
      <button class=\"tab\" data-view=\"accounts\">Accounts</button>
      <button class=\"tab\" data-view=\"origins\">Origins</button>
      <button class=\"tab\" data-view=\"people\">People</button>
      <button class=\"tab\" data-view=\"files\">Files</button>
      <button class=\"tab\" data-view=\"raw\">Raw</button>
    </nav>
    <div class=\"token-row\">
      <input id=\"token\" type=\"password\" autocomplete=\"current-password\" placeholder=\"API token\">
      <button id=\"save-token\">Save</button>
      <button class=\"primary\" id=\"refresh\">Refresh</button>
    </div>
  </div>
</header>
<main>
  <div id=\"status\" class=\"status\">Ready</div>

  <section id=\"view-overview\" class=\"view stack\">
    <div class=\"grid\">
      <div class=\"panel\">
        <div class=\"panel-head\"><h2>Service</h2><button data-action=\"load\">Refresh</button></div>
        <div id=\"summary\" class=\"stack\"></div>
      </div>
      <div class=\"panel\">
        <div class=\"panel-head\"><h2>Recent Messages</h2><button data-action=\"load-messages\">Load</button></div>
        <div class=\"table-wrap\"><table><thead><tr><th>Seq</th><th>Account</th><th>Chat</th><th>Message</th><th>Text</th></tr></thead><tbody id=\"messages-body\"></tbody></table></div>
      </div>
    </div>
  </section>

  <section id=\"view-accounts\" class=\"view grid hidden\">
    <div class=\"panel\">
      <div class=\"panel-head\"><h2>Account Login</h2></div>
      <form id=\"account-form\" class=\"form-grid\">
        <label>Account ID<input name=\"account_id\" required placeholder=\"main\"></label>
        <label>Display name<input name=\"display_name\" placeholder=\"Main\"></label>
        <label>Phone<input name=\"phone\" placeholder=\"+10000000000\"></label>
        <label>Session name<input name=\"session_name\" placeholder=\"main\"></label>
        <div class=\"toolbar\">
          <button class=\"primary\" type=\"submit\">Save account</button>
          <button type=\"button\" data-action=\"auth-status\">Status</button>
          <button type=\"button\" data-action=\"request-code\">Request code</button>
        </div>
        <div class=\"two form-grid\">
          <label>Login code<input name=\"code\" inputmode=\"numeric\"></label>
          <label>2FA password<input name=\"password\" type=\"password\"></label>
        </div>
        <button type=\"button\" data-action=\"submit-code\">Submit code</button>
      </form>
    </div>
    <div class=\"panel\">
      <div class=\"panel-head\"><h2>Accounts</h2><button data-action=\"discover-selected\">Discover origins</button></div>
      <div class=\"table-wrap\"><table><thead><tr><th>Account</th><th>State</th><th>Session</th><th>Updated</th><th>Actions</th></tr></thead><tbody id=\"accounts-body\"></tbody></table></div>
    </div>
  </section>

  <section id=\"view-origins\" class=\"view grid hidden\">
    <details class=\"panel\">
      <summary><h2>Manual Origin</h2></summary>
      <form id=\"origin-form\" class=\"form-grid\">
        <label>Account ID<input name=\"account_id\" required></label>
        <div class=\"two form-grid\">
          <label>Origin ID<input name=\"origin_id\" required></label>
          <label>Topic ID<input name=\"topic_id\" value=\"0\"></label>
        </div>
        <div class=\"two form-grid\">
          <label>Type<select name=\"origin_type\"><option>group</option><option>channel</option><option>private</option><option>topic</option><option>configured_chat</option></select></label>
          <label>Parent origin<input name=\"parent_origin_id\"></label>
        </div>
        <label>Title<input name=\"title\"></label>
        <label>Username<input name=\"username\"></label>
        <label class=\"check\"><input type=\"checkbox\" name=\"is_forum\"> Forum</label>
        <button class=\"primary\" type=\"submit\">Save origin</button>
      </form>
    </details>
    <div class=\"panel\">
      <div class=\"panel-head\"><h2>Origins</h2><div class=\"toolbar\"><input id=\"origin-filter\" placeholder=\"Account filter\"><label class=\"check\"><input id=\"show-archived\" type=\"checkbox\"> Archived</label><button data-action=\"reload-origins\">Reload</button><button data-action=\"refresh-origins\">Refresh Telegram</button></div></div>
      <div class=\"table-wrap\"><table><thead><tr><th>Account</th><th>Origin</th><th>Type</th><th>Title</th><th>Tags</th><th>Backup</th><th>Actions</th></tr></thead><tbody id=\"origins-body\"></tbody></table></div>
    </div>
  </section>

  <section id=\"view-people\" class=\"view grid hidden\">
    <div class=\"panel\">
      <div class=\"panel-head\"><h2>Refresh Participants</h2></div>
      <form id=\"participant-refresh-form\" class=\"form-grid\">
        <label>Account ID<input name=\"account_id\" required></label>
        <label>Origin ID<input name=\"origin_id\" required></label>
        <label>Limit<input name=\"limit\" value=\"500\"></label>
        <button class=\"primary\" type=\"submit\">Refresh participants</button>
      </form>
      <h3>Manual participant</h3>
      <form id=\"participant-form\" class=\"form-grid\">
        <label>Account ID<input name=\"account_id\" required></label>
        <label>Origin ID<input name=\"origin_id\" required></label>
        <label>User ID<input name=\"user_id\" required></label>
        <label>Username<input name=\"username\"></label>
        <label>Display name<input name=\"display_name\"></label>
        <button type=\"submit\">Save participant</button>
      </form>
    </div>
    <div class=\"panel\">
      <div class=\"panel-head\"><h2>Participants</h2><button data-action=\"load-participants\">Load</button></div>
      <div class=\"table-wrap\"><table><thead><tr><th>Account</th><th>Origin</th><th>User</th><th>Name</th><th>Role</th><th>Actions</th></tr></thead><tbody id=\"participants-body\"></tbody></table></div>
    </div>
  </section>

  <section id=\"view-files\" class=\"view grid hidden\">
    <div class=\"panel\">
      <div class=\"panel-head\"><h2>Capture Cursors</h2><button data-action=\"load-cursors\">Load</button></div>
      <div class=\"table-wrap\"><table><thead><tr><th>Account</th><th>Origin</th><th>Topic</th><th>Last message</th><th>Backfill</th></tr></thead><tbody id=\"cursors-body\"></tbody></table></div>
    </div>
    <div class=\"panel\">
      <div class=\"panel-head\"><h2>Media Files</h2><button data-action=\"load-media\">Load</button></div>
      <div class=\"table-wrap\"><table><thead><tr><th>Account</th><th>Chat</th><th>Message</th><th>Kind</th><th>Path</th></tr></thead><tbody id=\"media-body\"></tbody></table></div>
    </div>
  </section>

  <section id=\"view-raw\" class=\"view hidden\">
    <div class=\"panel\"><div class=\"panel-head\"><h2>Raw Snapshot</h2><button data-action=\"load-raw\">Refresh</button></div><pre id=\"raw\"></pre></div>
  </section>
</main>

<template id=\"policy-template\">
  <form class=\"form-grid policy-form\">
    <input type=\"hidden\" name=\"account_id\">
    <input type=\"hidden\" name=\"origin_id\">
    <input type=\"hidden\" name=\"topic_id\">
    <label class=\"check\"><input type=\"checkbox\" name=\"enabled\"> Enabled</label>
    <label class=\"check\"><input type=\"checkbox\" name=\"capture_text\"> Text</label>
    <label class=\"check\"><input type=\"checkbox\" name=\"capture_media_metadata\"> Media metadata</label>
    <label class=\"check\"><input type=\"checkbox\" name=\"download_media\"> Download media</label>
    <label>Tags<input name=\"tags\" placeholder=\"tag1, tag2\"></label>
    <button type=\"submit\">Save policy</button>
  </form>
</template>

<script>
const state = { accounts: [], origins: [], policies: [], participants: [], cursors: [], media: [], service: null, messages: [], expandedOrigins: {} };
const $ = (id) => document.getElementById(id);
const tokenInput = $('token');
tokenInput.value = localStorage.getItem('teleMessToken') || '';
function tokenValue() {
  return tokenInput.value.trim();
}
function headers() {
  const token = tokenValue();
  return token ? { 'Authorization': `Bearer ${token}` } : {};
}
function requireToken() {
  if (tokenValue()) return;
  throw new Error('API token required: enter server.token from config.yml, then click Save');
}
function setStatus(text, kind='') {
  const node = $('status');
  node.className = `status ${kind}`.trim();
  node.textContent = text;
}
async function api(path, options={}) {
  requireToken();
  const opts = { ...options, headers: { ...headers(), ...(options.headers || {}) } };
  if (opts.body && !opts.headers['Content-Type']) opts.headers['Content-Type'] = 'application/json';
  const response = await fetch(path, opts);
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text}`);
  return text ? JSON.parse(text) : {};
}
function formData(form) {
  const data = {};
  for (const [key, value] of new FormData(form).entries()) data[key] = value;
  for (const input of form.querySelectorAll('input[type=checkbox]')) data[input.name] = input.checked;
  return data;
}
function numberFields(data, fields) {
  for (const field of fields) if (data[field] !== undefined && data[field] !== '') data[field] = Number(data[field]);
  return data;
}
function rawText(value) { return value === null || value === undefined || value === '' ? '-' : String(value); }
function escapeHtml(value) {
  return rawText(value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
}
function text(value) { return escapeHtml(value); }
function attr(value) { return escapeHtml(value); }
function pill(value) {
  const cls = value === 'authorized' || value === true ? 'ok' : value === 'needs_login' || value === 'pending_auth' || value === 'code_sent' ? 'warn' : '';
  return `<span class=\"pill ${cls}\">${text(value)}</span>`;
}
function fillTable(id, rows, emptyCols) {
  $(id).innerHTML = rows.length ? rows.join('') : `<tr><td colspan=\"${emptyCols}\" class=\"muted\">No rows</td></tr>`;
}
function originPath() {
  const params = new URLSearchParams();
  const q = $('origin-filter')?.value.trim();
  if (q) params.set('account_id', q);
  if ($('show-archived')?.checked) params.set('include_archived', 'true');
  const suffix = params.toString();
  return suffix ? `/manage/origins?${suffix}` : '/manage/origins';
}
async function loadOrigins() {
  const data = await api(originPath());
  state.origins = data.items || [];
  renderOrigins();
  renderSummary();
  renderRaw();
}
async function loadAll() {
  try {
    setStatus('Loading');
    const [service, accounts, origins, policies, participants, cursors, media] = await Promise.all([
      api('/sync/state'), api('/manage/accounts'), api(originPath()), api('/manage/backup-policies'),
      api('/manage/participants'), api('/manage/capture-cursors'), api('/manage/operation-events'), api('/sync/media-files')
    ]);
    state.service = service;
    state.accounts = accounts.items || [];
    state.origins = origins.items || [];
    state.policies = policies.items || [];
    state.participants = participants.items || [];
    state.cursors = cursors.items || [];
    state.media = media.items || [];
    renderAll();
    setStatus('Loaded', 'ok');
  } catch (error) { setStatus(String(error), 'error'); }
}
async function loadMessages() {
  const data = await api('/sync/messages?after=0&limit=50');
  state.messages = data.items || [];
  renderMessages();
}
function renderAll() {
  renderSummary(); renderAccounts(); renderOrigins(); renderParticipants(); renderCursors(); renderMedia(); renderRaw();
}
function renderSummary() {
  const html = [
    `<div><span class=\"muted\">Schema</span> ${text(state.service?.schema_version)}</div>`,
    `<div><span class=\"muted\">Messages</span> ${text(state.service?.message_count)}</div>`,
    `<div><span class=\"muted\">Last event</span> ${text(state.service?.last_event_seq)}</div>`,
    `<div><span class=\"muted\">Accounts</span> ${state.accounts.length}</div>`,
    `<div><span class=\"muted\">Origins</span> ${state.origins.length}</div>`,
    `<div><span class=\"muted\">Participants</span> ${state.participants.length}</div>`
  ].join('');
  $('summary').innerHTML = html;
}
function renderAccounts() {
  fillTable('accounts-body', state.accounts.map(item => `<tr>
    <td>${text(item.account_id)}</td><td>${pill(item.auth_state)}</td><td>${text(item.session_name)}</td><td>${text(item.auth_updated_at || item.updated_at)}</td>
    <td class=\"actions\"><button data-account=\"${attr(item.account_id)}\" data-action=\"select-account\">Select</button><button class=\"danger\" data-account=\"${attr(item.account_id)}\" data-action=\"delete-account\">Delete</button></td>
  </tr>`), 5);
}
function renderOrigins() {
  const topicsByParent = {};
  const parentKeys = new Set();
  const parents = [];
  for (const item of state.origins) {
    const topicId = item.topic_id ?? 0;
    const key = `${item.account_id}:${item.origin_id}:0`;
    if (!topicId) {
      parents.push(item);
      parentKeys.add(key);
    } else {
      (topicsByParent[key] ||= []).push(item);
    }
  }
  for (const item of state.origins) {
    const topicId = item.topic_id ?? 0;
    const key = `${item.account_id}:${item.origin_id}:0`;
    if (topicId && !parentKeys.has(key)) {
      parents.push(item);
    }
  }
  const rows = [];
  for (const item of parents) {
    const key = `${item.account_id}:${item.origin_id}:0`;
    const children = topicsByParent[key] || [];
    rows.push(originRow(item, children.length, false));
    if (children.length && state.expandedOrigins[key]) {
      for (const child of children) rows.push(originRow(child, 0, true));
    }
  }
  fillTable('origins-body', rows, 7);
}
function originRow(item, childCount, isTopic) {
  const policy = item.backup_policy;
  const topicId = item.topic_id ?? 0;
  const key = `${item.account_id}:${item.origin_id}:0`;
  const expanded = Boolean(state.expandedOrigins[key]);
  const toggle = childCount ? `<button class=\"tree-toggle\" data-action=\"toggle-origin\" data-key=\"${attr(key)}\">${expanded ? '-' : '+'}</button>` : '<span class=\"tree-spacer\"></span>';
  const policyTags = policy?.tags || '';
  const backup = item.archived_at ? pill('archived') : policy ? pill(policy.enabled ? 'on' : 'off') : pill('off');
  const archiveLabel = item.archived_at ? 'Restore' : 'Archive';
  const archiveAction = item.archived_at ? 'restore-origin' : 'archive-origin';
  const rowClass = `${item.archived_at ? 'archived-row ' : ''}${isTopic ? 'topic-row' : ''}`.trim();
  return `<tr class=\"${attr(rowClass)}\"><td>${text(item.account_id)}</td><td><div class=\"tree-cell\">${toggle}<span>${text(item.origin_id)}${topicId ? `/${text(topicId)}` : ''}</span></div></td><td>${text(item.origin_type)}</td><td>${text(item.title)}${childCount ? ` <span class=\"muted\">(${text(childCount)} topics)</span>` : ''}</td><td class=\"tag-list\">${text(policyTags)}</td><td>${backup}</td><td class=\"actions\"><button data-origin=\"${attr(item.origin_id)}\" data-topic=\"${attr(topicId)}\" data-account=\"${attr(item.account_id)}\" data-action=\"edit-policy\">Policy</button><button data-origin=\"${attr(item.origin_id)}\" data-topic=\"${attr(topicId)}\" data-account=\"${attr(item.account_id)}\" data-action=\"select-origin\">Select</button><button data-origin=\"${attr(item.origin_id)}\" data-topic=\"${attr(topicId)}\" data-account=\"${attr(item.account_id)}\" data-action=\"delete-policy\">Clear policy</button><button class=\"danger\" data-origin=\"${attr(item.origin_id)}\" data-topic=\"${attr(topicId)}\" data-account=\"${attr(item.account_id)}\" data-action=\"${archiveAction}\">${archiveLabel}</button></td></tr>`;
}
function renderParticipants() {
  fillTable('participants-body', state.participants.map(item => `<tr><td>${text(item.account_id)}</td><td>${text(item.origin_id)}</td><td>${text(item.username || item.user_id)}</td><td>${text(item.display_name)}</td><td>${text(item.role)}</td><td class=\"actions\"><button class=\"danger\" data-account=\"${attr(item.account_id)}\" data-origin=\"${attr(item.origin_id)}\" data-user=\"${attr(item.user_id)}\" data-action=\"delete-participant\">Delete</button></td></tr>`), 6);
}
function renderCursors() {
  fillTable('cursors-body', state.cursors.map(item => `<tr><td>${text(item.account_id)}</td><td>${text(item.origin_id)}</td><td>${text(item.topic_id)}</td><td>${text(item.last_message_id)}</td><td>${text(item.last_backfill_at)}</td></tr>`), 5);
}
function renderMedia() {
  fillTable('media-body', state.media.map(item => `<tr><td>${text(item.account_id)}</td><td>${text(item.chat_id)}</td><td>${text(item.message_id)}</td><td>${text(item.media_kind)}</td><td>${text(item.file_path)}</td></tr>`), 5);
}
function renderMessages() {
  fillTable('messages-body', state.messages.map(item => `<tr><td>${text(item.event_seq)}</td><td>${text(item.account_id)}</td><td>${text(item.chat_id)}</td><td>${text(item.message_id)}</td><td>${text((item.text || '').slice(0, 120))}</td></tr>`), 5);
}
function renderRaw() { $('raw').textContent = JSON.stringify(state, null, 2); }
async function post(path, data, method='POST') {
  setStatus('Saving');
  const result = await api(path, { method, body: JSON.stringify(data) });
  await loadAll();
  setStatus('Saved', 'ok');
  return result;
}
async function removeRecord(path, data) {
  setStatus('Deleting');
  const result = await api(path, { method: 'DELETE', body: JSON.stringify(data) });
  await loadAll();
  setStatus('Deleted', 'ok');
  return result;
}
function selectedAccount() { return document.querySelector('#account-form [name=account_id]').value.trim(); }
function selectedOriginAccount() {
  const filtered = $('origin-filter').value.trim() || selectedAccount();
  if (filtered) return filtered;
  return state.accounts.length === 1 ? state.accounts[0].account_id : '';
}
document.addEventListener('click', async (event) => {
  const target = event.target.closest('button');
  if (!target) return;
  const action = target.dataset.action;
  try {
    if (target.id === 'save-token') {
      localStorage.setItem('teleMessToken', tokenValue());
      setStatus(tokenValue() ? 'Token saved' : 'Token cleared', tokenValue() ? 'ok' : 'warn');
      if (tokenValue()) await loadAll();
    }
    else if (target.id === 'refresh' || action === 'load') await loadAll();
    else if (action === 'load-messages') await loadMessages();
    else if (action === 'load-participants') { const data = await api('/manage/participants'); state.participants = data.items || []; renderParticipants(); }
    else if (action === 'load-cursors') { const data = await api('/manage/capture-cursors'); state.cursors = data.items || []; renderCursors(); }
    else if (action === 'load-media') { const data = await api('/sync/media-files'); state.media = data.items || []; renderMedia(); }
    else if (action === 'load-raw') renderRaw();
    else if (action === 'select-account') { document.querySelector('#account-form [name=account_id]').value = target.dataset.account; document.querySelector('#origin-filter').value = target.dataset.account; await loadOrigins(); }
    else if (action === 'select-origin') { document.querySelector('#participant-refresh-form [name=account_id]').value = target.dataset.account; document.querySelector('#participant-refresh-form [name=origin_id]').value = target.dataset.origin; }
    else if (action === 'delete-account') { if (confirm(`Delete account ${target.dataset.account}? Archived messages are kept.`)) await removeRecord('/manage/accounts', { account_id: target.dataset.account }); }
    else if (action === 'archive-origin' || action === 'restore-origin') { const archived = action === 'archive-origin'; if (confirm(`${archived ? 'Archive' : 'Restore'} origin ${target.dataset.origin}/${target.dataset.topic || 0}?`)) await post('/manage/origins/archive', { account_id: target.dataset.account, origin_id: Number(target.dataset.origin), topic_id: Number(target.dataset.topic || 0), archived }, 'PATCH'); }
    else if (action === 'delete-policy') { if (confirm(`Clear backup policy for ${target.dataset.origin}/${target.dataset.topic || 0}?`)) await removeRecord('/manage/backup-policies', { account_id: target.dataset.account, origin_id: Number(target.dataset.origin), topic_id: Number(target.dataset.topic || 0) }); }
    else if (action === 'delete-participant') { if (confirm(`Delete participant ${target.dataset.user}?`)) await removeRecord('/manage/participants', { account_id: target.dataset.account, origin_id: Number(target.dataset.origin), user_id: Number(target.dataset.user) }); }
    else if (action === 'auth-status') await post('/manage/accounts/auth/status', { account_id: selectedAccount() });
    else if (action === 'request-code') { const f = $('account-form'); await post('/manage/accounts/auth/request-code', { account_id: f.account_id.value, phone: f.phone.value }); }
    else if (action === 'submit-code') { const f = $('account-form'); await post('/manage/accounts/auth/submit-code', { account_id: f.account_id.value, phone: f.phone.value, code: f.code.value, password: f.password.value }); }
    else if (action === 'discover-selected') await post('/manage/discover-origins', { account_id: selectedAccount(), include_topics: true, topic_limit: 500 });
    else if (action === 'filter-origins' || action === 'reload-origins') { await loadOrigins(); setStatus('Origins loaded', 'ok'); }
    else if (action === 'refresh-origins') { const accountId = selectedOriginAccount(); if (!accountId) throw new Error('Select or enter an account first'); await post('/manage/discover-origins', { account_id: accountId, include_topics: true, topic_limit: 500 }); }
    else if (action === 'toggle-origin') { state.expandedOrigins[target.dataset.key] = !state.expandedOrigins[target.dataset.key]; renderOrigins(); }
    else if (action === 'edit-policy') openPolicy(target.dataset.account, Number(target.dataset.origin), Number(target.dataset.topic || 0));
  } catch (error) { setStatus(String(error), 'error'); }
});
document.querySelectorAll('.tab').forEach(button => button.addEventListener('click', () => {
  document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
  button.classList.add('active');
  document.querySelectorAll('.view').forEach(view => view.classList.add('hidden'));
  $(`view-${button.dataset.view}`).classList.remove('hidden');
}));
$('account-form').addEventListener('submit', async (event) => { event.preventDefault(); try { await post('/manage/accounts', formData(event.target)); } catch (error) { setStatus(String(error), 'error'); } });
$('origin-form').addEventListener('submit', async (event) => { event.preventDefault(); try { await post('/manage/origins', numberFields(formData(event.target), ['origin_id','topic_id','parent_origin_id'])); } catch (error) { setStatus(String(error), 'error'); } });
$('participant-refresh-form').addEventListener('submit', async (event) => { event.preventDefault(); try { await post('/manage/participants/refresh', numberFields(formData(event.target), ['origin_id','limit'])); } catch (error) { setStatus(String(error), 'error'); } });
$('participant-form').addEventListener('submit', async (event) => { event.preventDefault(); try { await post('/manage/participants', numberFields(formData(event.target), ['origin_id','user_id'])); } catch (error) { setStatus(String(error), 'error'); } });
$('show-archived').addEventListener('change', async () => { try { await loadOrigins(); setStatus('Origins loaded', 'ok'); } catch (error) { setStatus(String(error), 'error'); } });
function openPolicy(accountId, originId, topicId) {
  const policy = state.policies.find(item => item.account_id === accountId && item.origin_id === originId && item.topic_id === topicId) || {};
  const origin = state.origins.find(item => item.account_id === accountId && item.origin_id === originId && item.topic_id === topicId);
  const row = document.createElement('tr');
  const cell = document.createElement('td');
  cell.colSpan = 7;
  const form = $('policy-template').content.firstElementChild.cloneNode(true);
  form.account_id.value = accountId; form.origin_id.value = originId; form.topic_id.value = topicId;
  form.enabled.checked = policy.enabled ?? origin?.backup_policy?.enabled ?? false;
  form.capture_text.checked = policy.capture_text ?? origin?.backup_policy?.capture_text ?? true;
  form.capture_media_metadata.checked = policy.capture_media_metadata ?? origin?.backup_policy?.capture_media_metadata ?? true;
  form.download_media.checked = policy.download_media ?? origin?.backup_policy?.download_media ?? false;
  form.tags.value = policy.tags ?? origin?.backup_policy?.tags ?? '';
  form.addEventListener('submit', async (event) => { event.preventDefault(); try { await post('/manage/backup-policies', numberFields(formData(form), ['origin_id','topic_id']), 'PATCH'); row.remove(); } catch (error) { setStatus(String(error), 'error'); } });
  cell.appendChild(form); row.appendChild(cell); $('origins-body').prepend(row);
}
if (tokenValue()) loadAll();
else setStatus('Enter server.token from config.yml, then click Save', 'warn');
</script>
</body>
</html>"""
