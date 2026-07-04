from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlencode, urlparse

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
from tele_mess_core.server.contracts import (
    API_CONTRACT_HASH,
    API_CONTRACT_VERSION,
    API_MANIFEST_PATH,
    MARKDOWN_API_DOC_PATH,
    OPENAPI_PATH,
    api_manifest,
    markdown_document,
    openapi_document,
)
from tele_mess_core.telegram.runtime import TelegramOperationError

if TYPE_CHECKING:
    from tele_mess_core.config import AppConfig, TelegramAccountConfig


PUBLIC_GET_PATHS = {"/", "/console", OPENAPI_PATH, MARKDOWN_API_DOC_PATH}


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
            server_version = "tele-mess-core/0.2.3"

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
                if token and not (method == "GET" and parsed.path in PUBLIC_GET_PATHS) and not self._authorized(token):
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
                elif path == OPENAPI_PATH:
                    self._json(openapi_document())
                elif path == MARKDOWN_API_DOC_PATH:
                    self._text(markdown_document(), content_type="text/markdown; charset=utf-8")
                elif path == "/healthz":
                    self._json({"ok": True, **store.state()})
                elif path == "/sync/state":
                    self._json(store.state())
                elif path == "/sync/events":
                    self._json(store.list_events(after=_int_param(params, "after", 0), limit=_int_param(params, "limit", 500)))
                elif path == "/sync/messages":
                    include_media = _bool_param(params, "include_media", False)
                    if _bool_param(params, "latest", False):
                        payload = store.list_latest_messages(limit=_int_param(params, "limit", 50))
                    else:
                        payload = store.list_messages_after(
                            after_event_seq=_int_param(params, "after", 0),
                            limit=_int_param(params, "limit", 500),
                        )
                    if include_media:
                        _attach_message_media(store, payload)
                    self._json(payload)
                elif path == "/sync/chats":
                    self._json({"items": store.list_chats()})
                elif path == "/sync/accounts":
                    self._json({"items": store.list_accounts()})
                elif path == "/sync/search":
                    query = _str_param(params, "q", "")
                    if not query:
                        self._json({"items": []})
                    else:
                        payload = {"items": store.search_messages(query, limit=_int_param(params, "limit", 50))}
                        if _bool_param(params, "include_media", False):
                            _attach_message_media(store, payload)
                        self._json(payload)
                elif path == "/sync/media-files/content":
                    item = store.get_media_file(
                        source=_str_param(params, "source", SOURCE_TELEGRAM),
                        account_id=_str_param(params, "account_id", ""),
                        chat_id=_int_param(params, "chat_id", 0),
                        message_id=_int_param(params, "message_id", 0),
                        file_index=_int_param(params, "file_index", 0),
                    )
                    self._media_file(item)
                elif path == "/sync/media-files":
                    items = store.list_media_files(
                        account_id=_optional_str_param(params, "account_id"),
                        chat_id=_optional_int_param(params, "chat_id"),
                        message_id=_optional_int_param(params, "message_id"),
                        limit=_int_param(params, "limit", 500),
                    )
                    self._json(
                        {
                            "items": [_media_public_item(item) for item in items]
                        }
                    )
                elif path == "/manage/capabilities":
                    self._json(_capabilities())
                elif path == API_MANIFEST_PATH:
                    self._json(api_manifest())
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
                    items = store.list_operation_events(
                        account_id=_optional_str_param(params, "account_id"),
                        status=_optional_str_param(params, "status"),
                        limit=_int_param(params, "limit", 100),
                    )
                    self._json(
                        {
                            "items": [_operation_event_public_item(item) for item in items]
                        }
                    )
                elif path == "/manage/daily-package-schedule":
                    self._json({"item": store.get_daily_package_schedule()})
                elif path == "/manage/daily-package-runs":
                    self._json(
                        {
                            "items": store.list_daily_package_runs(
                                status=_optional_str_param(params, "status"),
                                limit=_int_param(params, "limit", 100),
                            )
                        }
                    )
                elif path == "/manage/daily-package-runs/content":
                    from tele_mess_core.daily import read_run_content

                    body, content_type = read_run_content(
                        store,
                        "package",
                        _str_param(params, "run_id", ""),
                        _str_param(params, "format", "md"),
                    )
                    self._text(body, content_type=content_type)
                elif path == "/manage/daily-summary-runs":
                    self._json(
                        {
                            "items": store.list_daily_summary_runs(
                                package_run_id=_optional_str_param(params, "package_run_id"),
                                status=_optional_str_param(params, "status"),
                                limit=_int_param(params, "limit", 100),
                            )
                        }
                    )
                elif path == "/manage/daily-summary-jobs":
                    self._json(
                        {
                            "items": store.list_daily_summary_jobs(
                                job_id=_optional_str_param(params, "job_id"),
                                status=_optional_str_param(params, "status"),
                                limit=_int_param(params, "limit", 100),
                            )
                        }
                    )
                elif path == "/manage/daily-summary-runs/content":
                    from tele_mess_core.daily import read_run_content

                    body, content_type = read_run_content(
                        store,
                        "summary",
                        _str_param(params, "run_id", ""),
                        "md",
                    )
                    self._text(body, content_type=content_type)
                elif path == "/manage/daily-summary-records":
                    self._json(
                        {
                            "items": store.list_daily_summary_records(
                                summary_id=_optional_str_param(params, "summary_id"),
                                run_id=_optional_str_param(params, "run_id"),
                                package_run_id=_optional_str_param(params, "package_run_id"),
                                date=_optional_str_param(params, "date"),
                                date_from=_optional_str_param(params, "date_from"),
                                date_to=_optional_str_param(params, "date_to"),
                                provider=_optional_str_param(params, "provider"),
                                important=_optional_bool_param(params, "important"),
                                tags=_tags_param(params),
                                q=_optional_str_param(params, "q"),
                                include_deleted=_bool_param(params, "include_deleted", False),
                                deleted=_optional_bool_param(params, "deleted"),
                                include_content=_bool_param(params, "include_content", False),
                                limit=_int_param(params, "limit", 100),
                            )
                        }
                    )
                elif path == "/manage/daily-summary-records/item":
                    item = store.get_daily_summary_record(
                        summary_id=_optional_str_param(params, "summary_id"),
                        run_id=_optional_str_param(params, "run_id"),
                        include_deleted=_bool_param(params, "include_deleted", False),
                    )
                    if item is None:
                        raise ValueError("Unknown daily summary record")
                    self._json({"item": item})
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
                elif path == "/manage/origins/important" and method == "PATCH":
                    item = _set_origin_important(store, payload)
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
                elif path == "/manage/operation-events" and method == "DELETE":
                    item = _delete_operation_events(store, payload)
                    self._json({"item": item})
                elif path == "/manage/daily-package-schedule" and method == "PATCH":
                    item = _update_daily_package_schedule(config, store, payload)
                    self._json({"item": item})
                elif path == "/manage/daily-packages" and method == "POST":
                    item = _create_daily_package(config, store, payload)
                    self._json({"item": item}, status=HTTPStatus.CREATED)
                elif path == "/manage/daily-summaries" and method == "POST":
                    item = _create_daily_summary(config, store, payload)
                    self._json({"item": item}, status=HTTPStatus.CREATED)
                elif path == "/manage/daily-summary-jobs" and method == "POST":
                    item = _create_daily_summary_job(config, store, payload)
                    self._json({"item": item}, status=HTTPStatus.CREATED)
                elif path == "/manage/daily-summary-jobs/cancel" and method == "PATCH":
                    item = _cancel_daily_summary_job(store, payload)
                    self._json({"item": item})
                elif path == "/manage/daily-summary-records" and method == "PATCH":
                    item = _set_daily_summary_records_deleted(store, payload)
                    self._json({"item": item})
                elif path == "/manage/daily-summary-records" and method == "DELETE":
                    item = _set_daily_summary_records_deleted(store, {**payload, "deleted": True})
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

            def _text(self, text: str, content_type: str, status: HTTPStatus = HTTPStatus.OK) -> None:
                body = text.encode("utf-8")
                self.send_response(int(status))
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _media_file(self, item: dict[str, Any] | None) -> None:
                if item is None:
                    self._json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
                    return
                path = Path(str(item["file_path"])).expanduser()
                if not path.is_file():
                    self._json({"error": "not_found", "detail": "media file is missing"}, status=HTTPStatus.NOT_FOUND)
                    return
                content_type = _media_content_type(item)
                disposition = _content_disposition_filename(path.name)
                self.send_response(int(HTTPStatus.OK))
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(path.stat().st_size))
                self.send_header("Content-Disposition", f'inline; filename="{disposition}"')
                self.send_header("Cache-Control", "private, max-age=3600")
                self.end_headers()
                with path.open("rb") as f:
                    while True:
                        chunk = f.read(1024 * 256)
                        if not chunk:
                            break
                        self.wfile.write(chunk)

        return Handler


def _capabilities() -> dict[str, Any]:
    return {
        "mode": "single-user-multi-telegram-account",
        "sync": [
            "state",
            "events",
            "messages",
            "accounts",
            "chats",
            "search",
            "media_files",
            "media_file_content",
            "message_media_files",
        ],
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
            "operation_event_delete",
            "detailed_operation_events",
            "daily_package_schedule",
            "daily_package_runs",
            "daily_summary_runs",
            "daily_summary_records",
            "web_console",
        ],
        "auth_flow": {
            "status": "implemented",
            "note": "Remote clients can request a Telegram login code, submit it, and provide a 2FA password when Telegram requires one.",
        },
        "api_contract": {
            "version": API_CONTRACT_VERSION,
            "hash": API_CONTRACT_HASH,
            "manifest_url": API_MANIFEST_PATH,
            "openapi_url": OPENAPI_PATH,
            "markdown_url": MARKDOWN_API_DOC_PATH,
        },
    }


def _attach_message_media(store: ArchiveStore, payload: dict[str, Any]) -> None:
    items = payload.get("items") or []
    media_by_message = store.list_media_files_for_messages(items)
    for item in items:
        key = (
            item.get("source"),
            item.get("account_id"),
            item.get("chat_id"),
            item.get("message_id"),
        )
        media = [_media_public_item(media_item) for media_item in media_by_message.get(key, [])]
        item["media_files"] = media
        item["media_count"] = len(media)


def _media_public_item(item: dict[str, Any]) -> dict[str, Any]:
    public = dict(item)
    content_type = _media_content_type(public)
    access_url = _media_access_url(public)
    public["content_type"] = content_type
    public["preview_kind"] = _media_preview_kind(content_type)
    public["access_url"] = access_url
    public["download_url"] = access_url
    return public


def _media_access_url(item: dict[str, Any]) -> str:
    return "/sync/media-files/content?" + urlencode(
        {
            "source": item.get("source") or SOURCE_TELEGRAM,
            "account_id": item.get("account_id") or "",
            "chat_id": item.get("chat_id") or "",
            "message_id": item.get("message_id") or "",
            "file_index": item.get("file_index", 0),
        }
    )


def _media_content_type(item: dict[str, Any]) -> str:
    mime_type = item.get("mime_type")
    if isinstance(mime_type, str) and mime_type:
        return mime_type
    guessed, _ = mimetypes.guess_type(str(item.get("file_path") or ""))
    return guessed or "application/octet-stream"


def _media_preview_kind(content_type: str) -> str:
    if content_type.startswith("image/"):
        return "image"
    if content_type.startswith("video/"):
        return "video"
    if content_type.startswith("audio/"):
        return "audio"
    return "file"


def _content_disposition_filename(filename: str) -> str:
    cleaned = "".join("_" if char in {'"', "\\", "/", "\r", "\n"} else char for char in filename)
    return cleaned or "media"


def _operation_event_public_item(item: dict[str, Any]) -> dict[str, Any]:
    public = dict(item)
    error = _operation_event_error(public)
    subject = _operation_event_subject(public)
    public["error"] = error
    public["error_type"] = error.get("type")
    public["auth_state"] = error.get("auth_state")
    public["subject"] = subject
    public["subject_label"] = subject.get("label")
    return public


def _operation_event_error(item: dict[str, Any]) -> dict[str, Any]:
    raw = item.get("raw_json")
    error = dict(raw) if isinstance(raw, dict) else {}
    if item.get("error_code") and not error.get("code"):
        error["code"] = item["error_code"]
    if item.get("message") and not error.get("message"):
        error["message"] = item["message"]
    if item.get("retry_after") is not None and not error.get("retry_after"):
        error["retry_after"] = item["retry_after"]
    return error


def _operation_event_subject(item: dict[str, Any]) -> dict[str, Any]:
    subject_type = item.get("subject_type")
    subject_id = item.get("subject_id")
    subject: dict[str, Any] = {
        "type": subject_type,
        "id": subject_id,
        "account_id": item.get("account_id"),
    }
    if subject_type == "message":
        chat_id, message_id = _parse_message_subject_id(subject_id)
        subject.update(
            {
                "chat_id": item.get("subject_chat_id") or chat_id,
                "message_id": item.get("subject_message_id") or message_id,
                "topic_id": item.get("subject_topic_id"),
                "chat_title": item.get("subject_chat_title"),
                "sent_at": item.get("subject_sent_at"),
                "media_kind": item.get("subject_media_kind"),
                "text": item.get("subject_text"),
            }
        )
        chat_label = item.get("subject_chat_title") or chat_id or subject_id
        subject["label"] = f"{chat_label}/{message_id}" if message_id is not None else str(subject_id or "-")
    elif subject_type == "origin":
        origin_id = item.get("subject_origin_id") or _optional_int_value(subject_id)
        subject.update(
            {
                "origin_id": origin_id,
                "topic_id": item.get("subject_origin_topic_id"),
                "origin_title": item.get("subject_origin_title"),
                "origin_type": item.get("subject_origin_type"),
            }
        )
        subject["label"] = str(item.get("subject_origin_title") or origin_id or subject_id or "-")
    else:
        subject["label"] = str(subject_id or "-")
    return subject


def _parse_message_subject_id(subject_id: Any) -> tuple[int | None, int | None]:
    if not subject_id:
        return None, None
    left, sep, right = str(subject_id).partition("/")
    if not sep:
        return None, _optional_int_value(left)
    return _optional_int_value(left), _optional_int_value(right)


def _optional_int_value(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _delete_operation_events(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    raw_ids = payload.get("ids")
    if raw_ids is None:
        raw_ids = [payload.get("id")]
    if not isinstance(raw_ids, list):
        raise ValueError("ids must be a list")
    ids = [int(item) for item in raw_ids if item is not None and item != ""]
    if not ids:
        raise ValueError("Missing required field: id")
    deleted = store.delete_operation_events(ids)
    return {"ids": ids, "deleted": deleted}


def _summary_record_ids(payload: dict[str, Any]) -> list[str]:
    raw_ids = payload.get("summary_ids")
    if raw_ids is None:
        raw_ids = payload.get("ids")
    if raw_ids is None:
        raw_ids = [payload.get("summary_id")]
    if not isinstance(raw_ids, list):
        raise ValueError("summary_ids must be a list")
    ids: list[str] = []
    seen: set[str] = set()
    for item in raw_ids:
        summary_id = str(item or "").strip()
        if not summary_id or summary_id in seen:
            continue
        seen.add(summary_id)
        ids.append(summary_id)
    if not ids:
        raise ValueError("Missing required field: summary_id")
    return ids


def _set_daily_summary_records_deleted(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    summary_ids = _summary_record_ids(payload)
    deleted = _payload_bool(payload, "deleted", True)
    changed_rows = store.set_daily_summary_records_deleted(summary_ids, deleted=deleted)
    return {"summary_ids": summary_ids, "deleted": deleted, "changed_rows": changed_rows}


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
            important=_payload_bool(payload, "important", False),
            last_message_at=_optional_payload_str(payload, "last_message_at"),
            updated_at=utc_now_iso(),
            raw_json=_public_raw_json(payload),
        )
    )
    return _find_origin(store, account_id, origin_id, topic_id)


def _set_origin_important(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    source = _source(payload)
    account_id = _required_str(payload, "account_id")
    origin_id = _required_int(payload, "origin_id")
    topic_id = _payload_int(payload, "topic_id", 0)
    important = _payload_bool(payload, "important", True)
    changed = store.set_origin_important(source, account_id, origin_id, topic_id, important)
    if changed == 0:
        raise ValueError("origin was not found")
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
    include_private = _payload_bool(payload, "include_private", False)
    topic_limit = _payload_int(payload, "topic_limit", 100)
    from tele_mess_core.telegram.discovery import TelegramDiscoveryService

    return asyncio.run(
        TelegramDiscoveryService(account, store).discover_origins(
            include_topics=include_topics,
            topic_limit=topic_limit,
            include_private=include_private,
        )
    )


def _refresh_participants(config: "AppConfig | None", store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    account = _account_config(config, store, _required_str(payload, "account_id"), _source(payload))
    origin_id = _required_int(payload, "origin_id")
    limit = _payload_int(payload, "limit", 500)
    from tele_mess_core.telegram.discovery import TelegramDiscoveryService

    return asyncio.run(TelegramDiscoveryService(account, store).refresh_participants(origin_id, limit))


def _update_daily_package_schedule(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
) -> dict[str, Any]:
    if config is None:
        raise ValueError("Server config is required for daily package scheduling")
    from tele_mess_core.daily import update_daily_package_schedule

    return update_daily_package_schedule(store, config, payload)


def _create_daily_package(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
) -> dict[str, Any]:
    if config is None:
        raise ValueError("Server config is required for daily package runs")
    from tele_mess_core.daily import build_daily_package

    scope = _daily_scope(payload)
    return build_daily_package(
        store,
        config,
        run_date=_optional_payload_str(payload, "date"),
        timezone_name=_optional_payload_str(payload, "timezone"),
        scope=scope,
    )


def _create_daily_summary(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
) -> dict[str, Any]:
    if config is None:
        raise ValueError("Server config is required for daily summary runs")
    from tele_mess_core.daily import run_daily_summary, start_daily_summary_thread

    scope = _daily_scope(payload)
    if _payload_bool(payload, "background", True):
        return start_daily_summary_thread(
            store,
            config,
            package_run_id=_optional_payload_str(payload, "package_run_id"),
            run_date=_optional_payload_str(payload, "date"),
            timezone_name=_optional_payload_str(payload, "timezone"),
            scope=scope,
        )
    return run_daily_summary(
        store,
        config,
        package_run_id=_optional_payload_str(payload, "package_run_id"),
        run_date=_optional_payload_str(payload, "date"),
        timezone_name=_optional_payload_str(payload, "timezone"),
        scope=scope,
    )


def _create_daily_summary_job(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
) -> dict[str, Any]:
    if config is None:
        raise ValueError("Server config is required for daily summary jobs")
    from tele_mess_core.daily import start_daily_summary_job

    scope = _daily_scope(payload)
    return start_daily_summary_job(
        store,
        config,
        package_run_id=_optional_payload_str(payload, "package_run_id"),
        run_date=_optional_payload_str(payload, "date"),
        timezone_name=_optional_payload_str(payload, "timezone"),
        scope=scope,
    )


def _cancel_daily_summary_job(store: ArchiveStore, payload: dict[str, Any]) -> dict[str, Any]:
    from tele_mess_core.daily import cancel_daily_summary_job

    return cancel_daily_summary_job(store, _required_str(payload, "job_id"))


def _daily_scope(payload: dict[str, Any]) -> dict[str, Any]:
    raw_scope = payload.get("scope") or {}
    if not isinstance(raw_scope, dict):
        raise ValueError("scope must be an object")
    scope = dict(raw_scope)
    for key in ("account_id", "origin_id", "topic_id", "tags", "tag_groups"):
        if key in payload and payload[key] not in (None, ""):
            scope[key] = payload[key]
    return scope


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


def _optional_bool_param(params: dict[str, list[str]], key: str) -> bool | None:
    if key not in params:
        return None
    return _bool_param(params, key, False)


def _str_param(params: dict[str, list[str]], key: str, default: str) -> str:
    return params.get(key, [default])[0]


def _optional_str_param(params: dict[str, list[str]], key: str) -> str | None:
    value = params.get(key, [None])[0]
    return value if value else None


def _tags_param(params: dict[str, list[str]]) -> list[str]:
    values: list[str] = []
    for item in params.get("tag", []):
        values.append(item)
    for item in params.get("tags", []):
        values.extend(part for part in item.split(",") if part.strip())
    tags: list[str] = []
    seen: set[str] = set()
    for value in values:
        tag = value.strip()
        key = tag.lower()
        if tag and key not in seen:
            seen.add(key)
            tags.append(tag)
    return tags


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
    .overview-grid { align-items: start; }
    .panel { background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius); padding: 14px; }
    .messages-panel { display: grid; grid-template-rows: auto minmax(0, 1fr); overflow: hidden; }
    .messages-panel .table-wrap { max-height: none; min-height: 0; }
    summary { cursor: pointer; }
    summary h2 { display: inline; }
    details .form-grid { margin-top: 12px; }
    .panel-head { display: flex; justify-content: space-between; gap: 12px; align-items: center; margin-bottom: 12px; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
    .property-bar { display: grid; grid-template-columns: minmax(160px, 1.2fr) repeat(5, minmax(120px, .8fr)) auto; gap: 8px; align-items: end; margin-bottom: 12px; }
    .property-bar label { min-width: 0; }
    .form-grid { display: grid; gap: 10px; }
    .two { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    label { display: grid; gap: 5px; font-size: 12px; color: var(--muted); }
    input, select, button, textarea { font: inherit; min-height: 34px; }
    input, select, textarea { width: 100%; border: 1px solid var(--line-strong); border-radius: 6px; padding: 7px 9px; background: #fff; color: var(--text); }
    textarea { resize: vertical; min-height: 92px; }
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
    tr.removed-row, tr.deleted-row { color: var(--muted); }
    .origin-table .origin-manage-row { user-select: none; }
    .origin-table .origin-manage-row td { cursor: pointer; }
    .origin-table .origin-manage-row td.actions, .origin-table .origin-manage-row td:has(.origin-select) { cursor: auto; }
    .origin-table tr.origin-selected td { background: #eff6ff; }
    body.origin-dragging, body.origin-dragging * { user-select: none; }
    .tree-cell { display: flex; align-items: center; gap: 7px; }
    .tree-toggle { width: 22px; min-width: 22px; height: 22px; min-height: 22px; line-height: 20px; padding: 0; border-radius: 4px; font-size: 13px; font-weight: 650; text-align: center; }
    .tree-spacer { width: 22px; min-width: 22px; height: 22px; }
    .topic-row .tree-cell { padding-left: 28px; }
    .tag-list, .tag-chips { display: flex; flex-wrap: wrap; gap: 5px; align-items: center; }
    .tag-chip { display: inline-flex; align-items: center; gap: 5px; border: 1px solid var(--line-strong); border-radius: 999px; padding: 2px 7px; color: var(--muted); background: #f8fafc; font-size: 12px; line-height: 1.4; }
    .tag-chip.pending-remove { color: var(--danger); border-color: #f1b7b2; background: #fff5f5; }
    .tag-remove { display: inline-grid; place-items: center; width: 16px; min-width: 16px; height: 16px; min-height: 16px; padding: 0; border-radius: 999px; border: 0; background: transparent; color: inherit; font-size: 11px; line-height: 1; }
    .tag-editor { display: flex; flex-wrap: wrap; gap: 5px; align-items: center; min-height: 34px; border: 1px solid var(--line-strong); border-radius: 6px; padding: 4px 6px; background: #fff; }
    .tag-editor input[data-tag-input] { flex: 1 1 120px; min-width: 120px; width: auto; border: 0; min-height: 24px; padding: 2px; background: transparent; }
    .tag-editor input[data-tag-input]:focus { outline: none; }
    .origin-select { min-height: 16px; }
    .bulk-toolbar { border-left: 1px solid var(--line); padding-left: 8px; }
    .policy-row td { background: #fbfcfe; }
    .media-cell { min-width: 170px; }
    .media-actions { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }
    .media-preview { display: grid; gap: 6px; align-items: start; }
    .media-preview img, .media-preview video { display: block; max-width: 220px; max-height: 140px; border: 1px solid var(--line); border-radius: 6px; background: #f8fafc; }
    .media-preview audio { width: 220px; max-width: 100%; }
    .media-name { max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .status { min-height: 28px; border: 1px solid var(--line); border-radius: 6px; background: #fbfcfe; padding: 7px 9px; font-size: 13px; color: var(--muted); }
    .status.ok { color: var(--ok); border-color: #bbdfc5; background: #f0fdf4; }
    .status.warn { color: var(--warn); border-color: #dfc16d; background: #fffbea; }
    .status.error { color: var(--danger); border-color: #f1b7b2; background: #fff5f5; }
    .pill { display: inline-flex; align-items: center; border: 1px solid var(--line-strong); border-radius: 999px; padding: 2px 7px; color: var(--muted); font-size: 12px; }
    .pill.ok { color: var(--ok); border-color: #86c79a; }
    .pill.warn { color: var(--warn); border-color: #dfc16d; }
    .muted { color: var(--muted); }
    .hidden { display: none; }
    .stack { display: grid; gap: 12px; }
    .table-wrap { overflow: auto; max-height: calc(100vh - 240px); border: 1px solid var(--line); border-radius: 6px; }
    .table-wrap table { min-width: 680px; }
    .origin-table table { min-width: 1080px; }
    .table-wrap thead th { position: sticky; top: 0; z-index: 2; box-shadow: 0 1px 0 var(--line); }
    pre { margin: 0; overflow: auto; background: #101828; color: #e5e7eb; border-radius: 6px; padding: 10px; max-height: 280px; font-size: 12px; }
    .raw-panel { display: grid; grid-template-rows: auto minmax(0, 1fr); height: calc(100vh - 170px); min-height: 360px; }
    #raw { max-height: none; min-height: 0; height: 100%; }
    @media (max-width: 920px) { .topbar, .grid, .two, .property-bar { grid-template-columns: 1fr; } .token-row { grid-template-columns: 1fr; } .messages-panel { height: auto !important; } th { position: static; } }
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
      <button class=\"tab\" data-view=\"daily\">Daily</button>
      <button class=\"tab\" data-view=\"people\">Members</button>
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
	    <div class=\"grid overview-grid\">
	      <div class=\"panel\" id=\"service-panel\">
	        <div class=\"panel-head\"><h2>Service</h2><button data-action=\"load\">Refresh</button></div>
	        <div id=\"summary\" class=\"stack\"></div>
      </div>
      <div class=\"panel messages-panel\" id=\"recent-panel\">
        <div class=\"panel-head\"><h2>Recent Messages</h2><button data-action=\"load-messages\">Load</button></div>
	        <div class=\"table-wrap\"><table><thead><tr><th>Seq</th><th>Account</th><th>Chat</th><th>Message</th><th>Text</th><th>Media</th></tr></thead><tbody id=\"messages-body\"></tbody></table></div>
	      </div>
	    </div>
	    <div class=\"panel\">
	      <div class=\"panel-head\"><h2>Operation Events</h2><button data-action=\"load-operation-events\">Load</button></div>
	      <div class=\"table-wrap\"><table><thead><tr><th>Time</th><th>Account</th><th>Operation</th><th>Status</th><th>Subject</th><th>Error</th><th>Actions</th></tr></thead><tbody id=\"operation-events-body\"></tbody></table></div>
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

  <section id=\"view-origins\" class=\"view hidden\">
    <div class=\"panel\">
      <div class=\"panel-head\"><h2>Origins</h2><div class=\"toolbar\"><button data-action=\"reload-origins\">Reload</button><button data-action=\"refresh-origins\">Refresh Telegram</button><button data-action=\"toggle-origin-manage\">Manage</button><span id=\"origin-bulk\" class=\"toolbar bulk-toolbar hidden\"><button data-action=\"select-visible-origins\">Select visible</button><button data-action=\"clear-origin-selection\">Clear</button><button data-action=\"bulk-remove-origins\">Remove selected</button><button data-action=\"bulk-restore-origins\">Restore selected</button><button data-action=\"bulk-clear-policies\">Clear policies</button><span id=\"origin-selection\" class=\"muted\">0 selected</span></span></div></div>
      <div class=\"property-bar\">
        <label>Search<input id=\"origin-search\" placeholder=\"Title, id, username\"></label>
        <label>Account<input id=\"origin-filter\" placeholder=\"Account filter\"></label>
        <label>Type<select id=\"origin-type-filter\"><option value=\"\">Any type</option><option value=\"group\">Group</option><option value=\"channel\">Channel</option><option value=\"topic\">Topic</option><option value=\"private\">Private</option><option value=\"unknown\">Unknown</option></select></label>
        <label>Backup<select id=\"origin-backup-filter\"><option value=\"\">Any backup</option><option value=\"on\">On</option><option value=\"off\">Off</option></select></label>
        <label>Tags<input id=\"origin-tag-filter\" list=\"tag-suggestions\" placeholder=\"Tag filter\"></label>
        <label>Sort<select id=\"origin-sort\"><option value=\"last_desc\">Last message desc</option><option value=\"last_asc\">Last message asc</option><option value=\"title_asc\">Title A-Z</option><option value=\"account_asc\">Account A-Z</option><option value=\"type_asc\">Type A-Z</option><option value=\"backup_desc\">Backup first</option></select></label>
        <label class=\"check\"><input id=\"show-archived\" type=\"checkbox\"> Removed</label>
      </div>
      <div class=\"table-wrap origin-table\"><table><thead id=\"origins-head\"></thead><tbody id=\"origins-body\"></tbody></table></div>
    </div>
  </section>

  <section id=\"view-daily\" class=\"view grid hidden\">
    <div class=\"panel\">
      <div class=\"panel-head\"><h2>Daily Packaging</h2><button data-action=\"load-daily\">Load</button></div>
      <form id=\"daily-schedule-form\" class=\"form-grid\">
        <label class=\"check\"><input type=\"checkbox\" name=\"enabled\"> Enabled</label>
        <label>Time of day<input name=\"time_of_day\" value=\"08:00\" pattern=\"[0-9]{2}:[0-9]{2}\"></label>
        <label>Timezone<input name=\"timezone\" value=\"Asia/Tokyo\"></label>
        <label>Scope JSON<textarea name=\"scope\" rows=\"6\" placeholder='{\"tag_groups\":[\"web3 it info\",\"web3 info\",\"ai info\"]}'></textarea></label>
        <label class=\"check\"><input type=\"checkbox\" name=\"activate_systemd\"> Activate systemd user timer</label>
        <button class=\"primary\" type=\"submit\">Save schedule</button>
      </form>
      <h3>Run package</h3>
      <form id=\"daily-package-form\" class=\"form-grid\">
        <label>Date<input name=\"date\" placeholder=\"YYYY-MM-DD\"></label>
        <label>Timezone<input name=\"timezone\" placeholder=\"Asia/Tokyo\"></label>
        <label>Account<input name=\"account_id\"></label>
        <label>Tags<input name=\"tags\" placeholder=\"web3,info\"></label>
        <label>Tag groups<input name=\"tag_groups\" placeholder=\"web3 it info; web3 info; ai info\"></label>
        <button type=\"submit\">Generate package</button>
      </form>
      <h3>Run summary</h3>
      <form id=\"daily-summary-form\" class=\"form-grid\">
        <label>Package run ID<input name=\"package_run_id\"></label>
        <label class=\"check\"><input type=\"checkbox\" name=\"background\" checked> Background</label>
        <button type=\"submit\">Run summary</button>
      </form>
      <h3>Run package + summary job</h3>
      <form id=\"daily-summary-job-form\" class=\"form-grid\">
        <label>Date<input name=\"date\" placeholder=\"YYYY-MM-DD\"></label>
        <label>Timezone<input name=\"timezone\" placeholder=\"Asia/Tokyo\"></label>
        <label>Account<input name=\"account_id\"></label>
        <label>Tags<input name=\"tags\" placeholder=\"web3,info\"></label>
        <label>Tag groups<input name=\"tag_groups\" placeholder=\"web3 it info; web3 info; ai info\"></label>
        <button class=\"primary\" type=\"submit\">Start job</button>
      </form>
    </div>
    <div class=\"panel\">
      <div class=\"panel-head\"><h2>Daily Runs</h2><button data-action=\"load-daily\">Refresh</button></div>
      <h3>Schedule</h3>
      <pre id=\"daily-schedule-raw\"></pre>
      <h3>Summary jobs</h3>
      <div class=\"table-wrap\"><table><thead><tr><th>Job</th><th>Status</th><th>Progress</th><th>Package</th><th>Summary</th><th>Actions</th></tr></thead><tbody id=\"daily-summary-jobs-body\"></tbody></table></div>
      <h3>Package runs</h3>
      <div class=\"table-wrap\"><table><thead><tr><th>Run</th><th>Status</th><th>Date</th><th>Progress</th><th>Origins</th><th>Messages</th><th>Output</th></tr></thead><tbody id=\"daily-package-runs-body\"></tbody></table></div>
      <h3>Summary runs</h3>
      <div class=\"table-wrap\"><table><thead><tr><th>Run</th><th>Status</th><th>Package</th><th>Provider</th><th>Progress</th><th>Groups</th><th>Images</th><th>Output</th></tr></thead><tbody id=\"daily-summary-runs-body\"></tbody></table></div>
      <div class=\"panel-head\"><h3>Summary records</h3><div class=\"toolbar\"><label class=\"check\"><input id=\"show-deleted-summaries\" type=\"checkbox\"> Deleted</label><button data-action=\"toggle-summary-manage\">Manage</button><span id=\"summary-bulk\" class=\"toolbar bulk-toolbar hidden\"><button data-action=\"select-visible-summaries\">Select visible</button><button data-action=\"clear-summary-selection\">Clear</button><button data-action=\"bulk-delete-summaries\">Delete selected</button><button data-action=\"bulk-restore-summaries\">Restore selected</button><span id=\"summary-selection\" class=\"muted\">0 selected</span></span></div></div>
      <div class=\"table-wrap\"><table><thead id=\"daily-summary-records-head\"></thead><tbody id=\"daily-summary-records-body\"></tbody></table></div>
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
      <div class=\"table-wrap\"><table><thead><tr><th>Account</th><th>Chat</th><th>Message</th><th>Preview</th><th>Kind</th><th>Size</th><th>Path</th></tr></thead><tbody id=\"media-body\"></tbody></table></div>
    </div>
  </section>

  <section id=\"view-raw\" class=\"view hidden\">
    <div class=\"panel raw-panel\"><div class=\"panel-head\"><h2>Raw Snapshot</h2><button data-action=\"load-raw\">Refresh</button></div><pre id=\"raw\"></pre></div>
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
    <label>Tags
      <div class=\"tag-editor\" data-tag-editor>
        <div class=\"tag-chips\" data-tag-chips></div>
        <input data-tag-input list=\"tag-suggestions\" placeholder=\"Add tag\">
        <input type=\"hidden\" name=\"tags\">
      </div>
    </label>
    <button type=\"submit\">Save policy</button>
  </form>
</template>
<datalist id=\"tag-suggestions\"></datalist>

<script>
const state = { apiManifest: null, accounts: [], origins: [], policies: [], participants: [], cursors: [], media: [], operationEvents: [], service: null, messages: [], dailySchedule: null, dailySummaryJobs: [], dailyPackageRuns: [], dailySummaryRuns: [], dailySummaryRecords: [], expandedOrigins: {}, manageOrigins: false, selectedOrigins: {}, manageSummaries: false, selectedSummaries: {} };
let messageRefreshTimer = null;
let dailyRefreshTimer = null;
let messageRefreshInFlight = false;
let originSelectionAnchor = null;
let originDragState = null;
let ignoreNextOriginClick = false;
const MEDIA_CONTENT_PATH = '/sync/media-files/content';
const CONTRACT_HASH_KEY = 'teleMessApiContractHash';
const mediaObjectUrls = new Map();
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
function updateApiManifest(manifest) {
  state.apiManifest = manifest || null;
  const nextHash = manifest?.contract_hash;
  if (!nextHash) return '';
  const previousHash = localStorage.getItem(CONTRACT_HASH_KEY);
  localStorage.setItem(CONTRACT_HASH_KEY, nextHash);
  if (previousHash && previousHash !== nextHash) {
    return `API contract updated ${previousHash.slice(0, 8)} -> ${nextHash.slice(0, 8)}; refresh clients before writes`;
  }
  return '';
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
function originKey(accountId, originId, topicId=0) {
  return `${accountId}:${originId}:${topicId || 0}`;
}
function originPayload(accountId, originId, topicId=0) {
  return { account_id: accountId, origin_id: Number(originId), topic_id: Number(topicId || 0) };
}
function originEntryFromRow(row) {
  if (!row?.dataset?.key) return null;
  return {
    key: row.dataset.key,
    payload: originPayload(row.dataset.account, row.dataset.origin, row.dataset.topic),
  };
}
function visibleOriginEntries() {
  return [...document.querySelectorAll('#origins-body tr[data-key]')].map(originEntryFromRow).filter(Boolean);
}
function originRangeEntries(fromKey, toKey) {
  const entries = visibleOriginEntries();
  const fromIndex = entries.findIndex(entry => entry.key === fromKey);
  const toIndex = entries.findIndex(entry => entry.key === toKey);
  if (fromIndex < 0 || toIndex < 0) return entries.filter(entry => entry.key === toKey);
  const start = Math.min(fromIndex, toIndex);
  const end = Math.max(fromIndex, toIndex);
  return entries.slice(start, end + 1);
}
function selectionFromEntries(entries, base={}) {
  const selected = { ...base };
  for (const entry of entries) selected[entry.key] = entry.payload;
  return selected;
}
function selectOriginRange(fromKey, toKey, additive=false) {
  const base = additive ? state.selectedOrigins : {};
  state.selectedOrigins = selectionFromEntries(originRangeEntries(fromKey, toKey), base);
}
function selectOriginRowKey(key, event) {
  const entry = visibleOriginEntries().find(item => item.key === key);
  if (!entry) return;
  const additive = event?.ctrlKey || event?.metaKey;
  if (event?.shiftKey && originSelectionAnchor) {
    selectOriginRange(originSelectionAnchor, key, additive);
  } else if (additive) {
    state.selectedOrigins = { ...state.selectedOrigins };
    if (state.selectedOrigins[key]) delete state.selectedOrigins[key];
    else state.selectedOrigins[key] = entry.payload;
    originSelectionAnchor = key;
  } else if (state.selectedOrigins[key]) {
    state.selectedOrigins = { ...state.selectedOrigins };
    delete state.selectedOrigins[key];
    if (!Object.keys(state.selectedOrigins).length) originSelectionAnchor = null;
  } else {
    state.selectedOrigins = { [key]: entry.payload };
    originSelectionAnchor = key;
  }
  renderOrigins();
}
function toggleOriginRowKey(key, event) {
  const entry = visibleOriginEntries().find(item => item.key === key);
  if (!entry) return;
  if (event?.shiftKey && originSelectionAnchor) {
    selectOriginRange(originSelectionAnchor, key, true);
  } else {
    state.selectedOrigins = { ...state.selectedOrigins };
    if (state.selectedOrigins[key]) delete state.selectedOrigins[key];
    else state.selectedOrigins[key] = entry.payload;
    originSelectionAnchor = key;
  }
  renderOrigins();
}
function clearOriginSelection() {
  state.selectedOrigins = {};
  originSelectionAnchor = null;
  originDragState = null;
  ignoreNextOriginClick = false;
  document.body?.classList.remove('origin-dragging');
}
function normalized(value) {
  return value === null || value === undefined ? '' : String(value).toLowerCase();
}
function originTags(item) {
  return item.backup_policy?.tags || '';
}
function splitTags(value) {
  return String(value || '').split(',').map(tag => tag.trim()).filter(Boolean);
}
function uniqueTags(tags) {
  const seen = new Set();
  const result = [];
  for (const tag of tags) {
    const key = tag.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(tag);
  }
  return result;
}
function allTags() {
  return uniqueTags([
    ...state.policies.flatMap(item => splitTags(item.tags)),
    ...state.origins.flatMap(item => splitTags(originTags(item))),
  ]).sort((a, b) => a.localeCompare(b));
}
function refreshTagSuggestions() {
  const list = $('tag-suggestions');
  if (!list) return;
  list.innerHTML = allTags().map(tag => `<option value=\"${attr(tag)}\"></option>`).join('');
}
function renderTags(value, removable=false) {
  const tags = Array.isArray(value) ? value : splitTags(value);
  if (!tags.length) return '<span class=\"muted\">-</span>';
  return tags.map((tag, index) => `<span class=\"tag-chip\" data-tag-index=\"${index}\"><span>${text(tag)}</span>${removable ? `<button type=\"button\" class=\"tag-remove\" data-tag-remove=\"${index}\" aria-label=\"Remove ${attr(tag)}\">x</button>` : ''}</span>`).join('');
}
function originBackupState(item) {
  return item.backup_policy?.enabled ? 'on' : 'off';
}
function originLastMessageValue(item) {
  return item.last_message_at || '';
}
function messageChatLabel(item) {
  if (item.topic_id) return item.origin_title || item.chat_title || item.chat_id;
  return item.chat_title || item.chat_id;
}
function originTitle(accountId, originId, topicId=0) {
  const origin = state.origins.find(item => item.account_id === accountId && item.origin_id === originId && (item.topic_id ?? 0) === (topicId || 0));
  return origin?.title || originId;
}
function cursorOriginLabel(item) {
  return item.origin_title || originTitle(item.account_id, item.origin_id, item.topic_id);
}
function mediaChatLabel(item) {
  return item.chat_title || originTitle(item.account_id, item.chat_id, 0);
}
function operationSubjectLabel(item) {
  return item.subject?.label || item.subject_label || item.subject_id || '-';
}
function operationSubjectDetail(item) {
  const subject = item.subject || {};
  const details = [];
  if (subject.type) details.push(subject.type);
  if (subject.id) details.push(subject.id);
  if (subject.chat_id && subject.message_id) details.push(`${subject.chat_id}/${subject.message_id}`);
  if (subject.origin_type) details.push(subject.origin_type);
  const textSnippet = subject.text ? String(subject.text).slice(0, 120) : '';
  return `${details.map(text).join(' · ')}${textSnippet ? `<div class=\"muted\">${text(textSnippet)}</div>` : ''}`;
}
function operationErrorSummary(item) {
  const error = item.error || {};
  const parts = [error.code || item.error_code, error.type || item.error_type, error.auth_state || item.auth_state].filter(Boolean);
  return parts.length ? parts.join(' / ') : item.message || '-';
}
function operationErrorDetail(item) {
  const error = item.error || item.raw_json || {};
  const message = error.message || item.message || '';
  const summary = operationErrorSummary(item);
  const raw = JSON.stringify(error, null, 2);
  return `<details><summary>${text(summary)}</summary><div>${text(message)}</div><pre>${text(raw)}</pre></details>`;
}
function mediaKey(item) {
  return [item.source, item.account_id, item.chat_id, item.message_id, item.file_index ?? 0].map(value => encodeURIComponent(String(value ?? ''))).join('|');
}
function allMediaItems() {
  return [...state.media, ...state.messages.flatMap(item => item.media_files || [])];
}
function findMediaItem(key) {
  return allMediaItems().find(item => mediaKey(item) === key);
}
function mediaFilename(item) {
  const path = rawText(item.file_path);
  const name = path.split(/[\\/]/).filter(Boolean).pop();
  return name && name !== '-' ? name : `media-${item.message_id}-${item.file_index ?? 0}`;
}
function formatBytes(value) {
  const size = Number(value);
  if (!Number.isFinite(size) || size < 0) return '-';
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  if (size < 1024 * 1024 * 1024) return `${(size / 1024 / 1024).toFixed(1)} MB`;
  return `${(size / 1024 / 1024 / 1024).toFixed(1)} GB`;
}
function mediaPreviewCell(item) {
  const key = mediaKey(item);
  const label = item.preview_kind === 'file' ? 'Open' : 'Preview';
  return `<div class=\"media-preview\" data-media-preview=\"${attr(key)}\"><div class=\"media-actions\"><button data-action=\"preview-media\" data-media-key=\"${attr(key)}\">${label}</button><span class=\"muted\">${text(item.preview_kind || 'file')}</span></div></div>`;
}
function mediaAccessUrl(item) {
  if (item.access_url) return item.access_url;
  const params = new URLSearchParams({
    source: item.source || 'telegram',
    account_id: item.account_id || '',
    chat_id: item.chat_id ?? '',
    message_id: item.message_id ?? '',
    file_index: item.file_index ?? 0,
  });
  return `${MEDIA_CONTENT_PATH}?${params.toString()}`;
}
function messageMediaCell(item) {
  const media = item.media_files || [];
  if (!media.length) return '<span class=\"muted\">-</span>';
  return `<div class=\"media-actions\">${media.map(mediaPreviewCell).join('')}</div>`;
}
async function mediaObjectUrl(item) {
  const key = mediaKey(item);
  if (mediaObjectUrls.has(key)) return mediaObjectUrls.get(key);
  const response = await fetch(mediaAccessUrl(item), { headers: headers() });
  if (!response.ok) throw new Error(`${response.status} ${await response.text()}`);
  const url = URL.createObjectURL(await response.blob());
  mediaObjectUrls.set(key, url);
  return url;
}
function mediaElementHtml(item, url) {
  const name = mediaFilename(item);
  if (item.preview_kind === 'image') return `<img src=\"${attr(url)}\" alt=\"${attr(name)}\" loading=\"lazy\"><div class=\"media-name muted\" title=\"${attr(name)}\">${text(name)}</div>`;
  if (item.preview_kind === 'video') return `<video src=\"${attr(url)}\" controls preload=\"metadata\"></video><div class=\"media-name muted\" title=\"${attr(name)}\">${text(name)}</div>`;
  if (item.preview_kind === 'audio') return `<audio src=\"${attr(url)}\" controls preload=\"metadata\"></audio><div class=\"media-name muted\" title=\"${attr(name)}\">${text(name)}</div>`;
  return `<div class=\"media-actions\"><a href=\"${attr(url)}\" target=\"_blank\" rel=\"noopener\">Open</a><span class=\"media-name muted\" title=\"${attr(name)}\">${text(name)}</span></div>`;
}
async function previewMedia(button) {
  const item = findMediaItem(button.dataset.mediaKey);
  if (!item) throw new Error('Media record not found');
  const container = button.closest('[data-media-preview]');
  if (!container) throw new Error('Media preview container not found');
  button.disabled = true;
  button.textContent = 'Loading';
  const url = await mediaObjectUrl(item);
  container.innerHTML = mediaElementHtml(item, url);
}
async function openMedia(button) {
  const item = findMediaItem(button.dataset.mediaKey);
  if (!item) throw new Error('Media record not found');
  const url = await mediaObjectUrl(item);
  window.open(url, '_blank', 'noopener');
}
function syncOverviewHeights() {
  const service = $('service-panel');
  const recent = $('recent-panel');
  if (!service || !recent) return;
  const wide = window.matchMedia('(min-width: 921px)').matches;
  if (!wide || service.offsetParent === null || recent.offsetParent === null) {
    recent.style.height = '';
    return;
  }
  const height = Math.ceil(service.getBoundingClientRect().height);
  if (height > 0) recent.style.height = `${height}px`;
}
function originPath() {
  const params = new URLSearchParams();
  const q = $('origin-filter')?.value.trim();
  if (q) params.set('account_id', q);
  if ($('show-archived')?.checked) params.set('include_archived', 'true');
  const suffix = params.toString();
  return suffix ? `/manage/origins?${suffix}` : '/manage/origins';
}
function summaryRecordsPath() {
  const params = new URLSearchParams();
  if ($('show-deleted-summaries')?.checked) params.set('include_deleted', 'true');
  const suffix = params.toString();
  return suffix ? `/manage/daily-summary-records?${suffix}` : '/manage/daily-summary-records';
}
async function loadOrigins() {
  const data = await api(originPath());
  state.origins = data.items || [];
  clearOriginSelection();
  refreshTagSuggestions();
  renderOrigins();
  renderSummary();
  renderRaw();
}
async function loadAll() {
  try {
    setStatus('Loading');
    const [apiManifest, service, accounts, origins, policies, participants, cursors, operationEvents, media, messages, dailySchedule, summaryJobs, packageRuns, summaryRuns, summaryRecords] = await Promise.all([
      api('/manage/api-manifest'), api('/sync/state'), api('/manage/accounts'), api(originPath()), api('/manage/backup-policies'),
      api('/manage/participants'), api('/manage/capture-cursors'), api('/manage/operation-events'), api('/sync/media-files'),
      api('/sync/messages?latest=true&limit=100&include_media=true'), api('/manage/daily-package-schedule'),
      api('/manage/daily-summary-jobs'), api('/manage/daily-package-runs'), api('/manage/daily-summary-runs'), api(summaryRecordsPath())
    ]);
    const contractWarning = updateApiManifest(apiManifest);
    state.service = service;
    state.accounts = accounts.items || [];
    state.origins = origins.items || [];
    clearOriginSelection();
    state.policies = policies.items || [];
    state.participants = participants.items || [];
    state.cursors = cursors.items || [];
    state.operationEvents = operationEvents.items || [];
    state.media = media.items || [];
    state.messages = messages.items || [];
    state.dailySchedule = dailySchedule.item || null;
    state.dailySummaryJobs = summaryJobs.items || [];
    state.dailyPackageRuns = packageRuns.items || [];
    state.dailySummaryRuns = summaryRuns.items || [];
    state.dailySummaryRecords = summaryRecords.items || [];
    clearSummarySelection();
    refreshTagSuggestions();
    renderAll();
    startMessageAutoRefresh();
    startDailyAutoRefresh();
    setStatus(contractWarning || 'Loaded', contractWarning ? 'warn' : 'ok');
  } catch (error) { setStatus(String(error), 'error'); }
}
async function loadMessages(options={}) {
  if (messageRefreshInFlight) return;
  messageRefreshInFlight = true;
  const previousTop = state.messages[0]?.event_seq;
  try {
    const data = await api('/sync/messages?latest=true&limit=100&include_media=true');
    state.messages = data.items || [];
    renderMessages(previousTop);
    if (!options.silent) setStatus('Messages loaded', 'ok');
  } finally {
    messageRefreshInFlight = false;
  }
}
async function loadDaily() {
  const [schedule, summaryJobs, packageRuns, summaryRuns, summaryRecords] = await Promise.all([
    api('/manage/daily-package-schedule'),
    api('/manage/daily-summary-jobs'),
    api('/manage/daily-package-runs'),
    api('/manage/daily-summary-runs'),
    api(summaryRecordsPath()),
  ]);
  state.dailySchedule = schedule.item || null;
  state.dailySummaryJobs = summaryJobs.items || [];
  state.dailyPackageRuns = packageRuns.items || [];
  state.dailySummaryRuns = summaryRuns.items || [];
  state.dailySummaryRecords = summaryRecords.items || [];
  clearSummarySelection();
  renderDaily();
  renderRaw();
  startDailyAutoRefresh();
  setStatus('Daily runs loaded', 'ok');
}
function startMessageAutoRefresh() {
  if (messageRefreshTimer || !tokenValue()) return;
  messageRefreshTimer = window.setInterval(() => {
    if (!tokenValue()) return;
    loadMessages({ silent: true }).catch(error => setStatus(String(error), 'error'));
  }, 10000);
}
function stopMessageAutoRefresh() {
  if (!messageRefreshTimer) return;
  window.clearInterval(messageRefreshTimer);
  messageRefreshTimer = null;
}
function startDailyAutoRefresh() {
  const active = (state.dailySummaryJobs || []).some(item => ['running', 'queued', 'cancel_requested'].includes(item.status));
  if (!active || !tokenValue()) {
    stopDailyAutoRefresh();
    return;
  }
  if (dailyRefreshTimer) return;
  dailyRefreshTimer = window.setInterval(() => {
    if (!tokenValue()) return;
    loadDaily().catch(error => setStatus(String(error), 'error'));
  }, 2000);
}
function stopDailyAutoRefresh() {
  if (!dailyRefreshTimer) return;
  window.clearInterval(dailyRefreshTimer);
  dailyRefreshTimer = null;
}
function renderAll() {
  renderSummary(); renderMessages(); renderAccounts(); renderOrigins(); renderDaily(); renderParticipants(); renderCursors(); renderOperationEvents(); renderMedia(); renderRaw();
}
function renderSummary() {
  const manifest = state.apiManifest || {};
  const contract = manifest.contract_hash ? `${manifest.contract_version} / ${manifest.contract_hash}` : '-';
  const docs = manifest.markdown_url ? `<a href=\"${attr(manifest.markdown_url)}\" target=\"_blank\" rel=\"noopener\">API</a> <a href=\"${attr(manifest.openapi_url)}\" target=\"_blank\" rel=\"noopener\">OpenAPI</a>` : '-';
  const html = [
    `<div><span class=\"muted\">Schema</span> ${text(state.service?.schema_version)}</div>`,
    `<div><span class=\"muted\">API contract</span> ${text(contract)}</div>`,
    `<div><span class=\"muted\">API docs</span> ${docs}</div>`,
    `<div><span class=\"muted\">Messages</span> ${text(state.service?.message_count)}</div>`,
    `<div><span class=\"muted\">Last event</span> ${text(state.service?.last_event_seq)}</div>`,
    `<div><span class=\"muted\">Accounts</span> ${state.accounts.length}</div>`,
    `<div><span class=\"muted\">Origins</span> ${state.origins.length}</div>`,
    `<div><span class=\"muted\">Participants</span> ${state.participants.length}</div>`
  ].join('');
  $('summary').innerHTML = html;
  requestAnimationFrame(syncOverviewHeights);
}
function renderAccounts() {
  fillTable('accounts-body', state.accounts.map(item => `<tr>
    <td>${text(item.account_id)}</td><td>${pill(item.auth_state)}</td><td>${text(item.session_name)}</td><td>${text(item.auth_updated_at || item.updated_at)}</td>
    <td class=\"actions\"><button data-account=\"${attr(item.account_id)}\" data-action=\"select-account\">Select</button><button class=\"danger\" data-account=\"${attr(item.account_id)}\" data-action=\"delete-account\">Delete</button></td>
  </tr>`), 5);
}
function renderOrigins() {
  renderOriginHead();
  const visibleOrigins = filteredOrigins();
  const topicsByParent = {};
  const parentKeys = new Set();
  const parents = [];
  const orphanTopics = [];
  for (const item of visibleOrigins) {
    const topicId = item.topic_id ?? 0;
    const key = `${item.account_id}:${item.origin_id}:0`;
    if (!topicId) {
      parents.push(item);
      parentKeys.add(key);
    } else {
      (topicsByParent[key] ||= []).push(item);
    }
  }
  for (const item of visibleOrigins) {
    const topicId = item.topic_id ?? 0;
    const key = `${item.account_id}:${item.origin_id}:0`;
    if (topicId && !parentKeys.has(key)) {
      orphanTopics.push(item);
    }
  }
  const rows = [];
  for (const item of parents) {
    const key = originKey(item.account_id, item.origin_id, 0);
    const children = topicsByParent[key] || [];
    rows.push(originRow(item, children.length, Boolean(item.topic_id ?? 0)));
    if (children.length && state.expandedOrigins[key]) {
      for (const child of children) rows.push(originRow(child, 0, true));
    }
  }
  for (const item of orphanTopics) rows.push(originRow(item, 0, true));
  fillTable('origins-body', rows, state.manageOrigins ? 10 : 9);
  updateOriginBulk();
}
function filteredOrigins() {
  const search = normalized($('origin-search')?.value);
  const type = $('origin-type-filter')?.value || '';
  const backup = $('origin-backup-filter')?.value || '';
  const tag = normalized($('origin-tag-filter')?.value);
  const sort = $('origin-sort')?.value || 'last_desc';
  const items = state.origins.filter(item => {
    if (type && item.origin_type !== type) return false;
    if (backup && originBackupState(item) !== backup) return false;
    if (tag && !normalized(originTags(item)).includes(tag)) return false;
    if (search) {
      const haystack = [
        item.account_id,
        item.origin_id,
        item.topic_id,
        item.title,
        item.username,
        item.origin_type,
        originTags(item),
      ].map(normalized).join(' ');
      if (!haystack.includes(search)) return false;
    }
    return true;
  });
  return items.sort((a, b) => compareOrigins(a, b, sort));
}
function compareOrigins(a, b, sort) {
  const textCompare = (left, right) => normalized(left).localeCompare(normalized(right));
  if (sort === 'last_asc') return textCompare(originLastMessageValue(a), originLastMessageValue(b)) || textCompare(a.title, b.title);
  if (sort === 'title_asc') return textCompare(a.title, b.title) || Number(a.origin_id) - Number(b.origin_id);
  if (sort === 'account_asc') return textCompare(a.account_id, b.account_id) || textCompare(a.title, b.title);
  if (sort === 'type_asc') return textCompare(a.origin_type, b.origin_type) || textCompare(a.title, b.title);
  if (sort === 'backup_desc') return textCompare(originBackupState(b), originBackupState(a)) || textCompare(a.title, b.title);
  return textCompare(originLastMessageValue(b), originLastMessageValue(a)) || textCompare(a.title, b.title);
}
function renderOriginHead() {
  $('origins-head').innerHTML = `<tr>${state.manageOrigins ? '<th>Select</th>' : ''}<th>Account</th><th>Origin</th><th>Type</th><th>Title</th><th>Important</th><th>Last message</th><th>Tags</th><th>Backup</th><th>Actions</th></tr>`;
}
function originRow(item, childCount, isTopic) {
  const policy = item.backup_policy;
  const topicId = item.topic_id ?? 0;
  const parentKey = originKey(item.account_id, item.origin_id, 0);
  const rowKey = originKey(item.account_id, item.origin_id, topicId);
  const expanded = Boolean(state.expandedOrigins[parentKey]);
  const toggle = childCount ? `<button class=\"tree-toggle\" data-action=\"toggle-origin\" data-key=\"${attr(parentKey)}\">${expanded ? '-' : '+'}</button>` : '<span class=\"tree-spacer\"></span>';
  const policyTags = policy?.tags || '';
  const backup = item.archived_at ? pill('removed') : policy ? pill(policy.enabled ? 'on' : 'off') : pill('off');
  const removeLabel = item.archived_at ? 'Restore' : 'Remove';
  const removeAction = item.archived_at ? 'restore-origin' : 'remove-origin';
  const rowClass = [
    item.archived_at ? 'removed-row' : '',
    isTopic ? 'topic-row' : '',
    state.manageOrigins ? 'origin-manage-row' : '',
    state.selectedOrigins[rowKey] ? 'origin-selected' : '',
  ].filter(Boolean).join(' ');
  const selectCell = state.manageOrigins ? `<td><input class=\"origin-select\" type=\"checkbox\" data-action=\"select-origin-row\" data-key=\"${attr(rowKey)}\" data-account=\"${attr(item.account_id)}\" data-origin=\"${attr(item.origin_id)}\" data-topic=\"${attr(topicId)}\" ${state.selectedOrigins[rowKey] ? 'checked' : ''}></td>` : '';
  const importantLabel = item.important ? pill('important') : '<span class=\"muted\">-</span>';
  const importantAction = item.important ? 'Uncheck important' : 'Check important';
  return `<tr class=\"${attr(rowClass)}\" data-key=\"${attr(rowKey)}\" data-account=\"${attr(item.account_id)}\" data-origin=\"${attr(item.origin_id)}\" data-topic=\"${attr(topicId)}\">${selectCell}<td>${text(item.account_id)}</td><td><div class=\"tree-cell\">${toggle}<span>${text(item.origin_id)}${topicId ? `/${text(topicId)}` : ''}</span></div></td><td>${text(item.origin_type)}</td><td>${text(item.title)}${childCount ? ` <span class=\"muted\">(${text(childCount)} topics)</span>` : ''}</td><td>${importantLabel}</td><td>${text(item.last_message_at)}</td><td><div class=\"tag-list\">${renderTags(policyTags)}</div></td><td>${backup}</td><td class=\"actions\"><button data-origin=\"${attr(item.origin_id)}\" data-topic=\"${attr(topicId)}\" data-account=\"${attr(item.account_id)}\" data-important=\"${attr(!item.important)}\" data-action=\"toggle-important\">${importantAction}</button><button data-origin=\"${attr(item.origin_id)}\" data-topic=\"${attr(topicId)}\" data-account=\"${attr(item.account_id)}\" data-action=\"edit-policy\">Policy</button><button data-origin=\"${attr(item.origin_id)}\" data-topic=\"${attr(topicId)}\" data-account=\"${attr(item.account_id)}\" data-action=\"select-origin\">Select</button><button data-origin=\"${attr(item.origin_id)}\" data-topic=\"${attr(topicId)}\" data-account=\"${attr(item.account_id)}\" data-action=\"delete-policy\">Clear policy</button><button class=\"danger\" data-origin=\"${attr(item.origin_id)}\" data-topic=\"${attr(topicId)}\" data-account=\"${attr(item.account_id)}\" data-action=\"${removeAction}\">${removeLabel}</button></td></tr>`;
}
function updateOriginBulk() {
  const bulk = $('origin-bulk');
  if (!bulk) return;
  bulk.classList.toggle('hidden', !state.manageOrigins);
  const manageButton = document.querySelector('[data-action=\"toggle-origin-manage\"]');
  if (manageButton) manageButton.textContent = state.manageOrigins ? 'Done' : 'Manage';
  const count = Object.keys(state.selectedOrigins).length;
  $('origin-selection').textContent = `${count} selected`;
}
function visibleOriginPayloads() {
  return visibleOriginEntries().map(entry => entry.payload);
}
function selectedOriginPayloads() {
  return Object.values(state.selectedOrigins);
}
function renderParticipants() {
  fillTable('participants-body', state.participants.map(item => `<tr><td>${text(item.account_id)}</td><td>${text(item.origin_id)}</td><td>${text(item.username || item.user_id)}</td><td>${text(item.display_name)}</td><td>${text(item.role)}</td><td class=\"actions\"><button class=\"danger\" data-account=\"${attr(item.account_id)}\" data-origin=\"${attr(item.origin_id)}\" data-user=\"${attr(item.user_id)}\" data-action=\"delete-participant\">Delete</button></td></tr>`), 6);
}
function renderCursors() {
  fillTable('cursors-body', state.cursors.map(item => `<tr><td>${text(item.account_id)}</td><td title=\"${attr(item.origin_id)}\">${text(cursorOriginLabel(item))}</td><td>${text(item.topic_id)}</td><td>${text(item.last_message_id)}</td><td>${text(item.last_backfill_at)}</td></tr>`), 5);
}
function renderOperationEvents() {
  fillTable('operation-events-body', state.operationEvents.map(item => `<tr><td>${text(item.occurred_at)}</td><td>${text(item.account_id)}</td><td>${text(item.operation)}</td><td>${pill(item.status)}</td><td title=\"${attr(item.subject_id)}\"><div>${text(operationSubjectLabel(item))}</div><div class=\"muted\">${operationSubjectDetail(item)}</div></td><td>${operationErrorDetail(item)}</td><td class=\"actions\"><button class=\"danger\" data-action=\"delete-operation-event\" data-event-id=\"${attr(item.id)}\">Delete</button></td></tr>`), 7);
}
function renderMedia() {
  fillTable('media-body', state.media.map(item => `<tr><td>${text(item.account_id)}</td><td title=\"${attr(item.chat_id)}\">${text(mediaChatLabel(item))}</td><td>${text(item.message_id)}</td><td class=\"media-cell\">${mediaPreviewCell(item)}</td><td>${text(item.media_kind)}</td><td>${text(formatBytes(item.file_size))}</td><td title=\"${attr(item.file_path)}\">${text(item.file_path)}</td></tr>`), 7);
}
function renderMessages(previousTop) {
  fillTable('messages-body', state.messages.map(item => `<tr><td>${text(item.event_seq)}</td><td>${text(item.account_id)}</td><td title=\"${attr(item.chat_id)}\">${text(messageChatLabel(item))}</td><td>${text(item.message_id)}</td><td>${text((item.text || '').slice(0, 120))}</td><td class=\"media-cell\">${messageMediaCell(item)}</td></tr>`), 6);
  const nextTop = state.messages[0]?.event_seq;
  if (previousTop && nextTop && previousTop !== nextTop) {
    const wrap = $('messages-body').closest('.table-wrap');
    if (wrap) wrap.scrollTop = 0;
  }
  requestAnimationFrame(syncOverviewHeights);
}
function renderDaily() {
  const schedule = state.dailySchedule || {};
  const form = $('daily-schedule-form');
  if (form) {
    form.enabled.checked = Boolean(schedule.enabled);
    form.time_of_day.value = schedule.time_of_day || '08:00';
    form.timezone.value = schedule.timezone || 'Asia/Tokyo';
    form.scope.value = JSON.stringify(schedule.scope || {}, null, 2);
    form.activate_systemd.checked = false;
  }
  const raw = $('daily-schedule-raw');
  if (raw) raw.textContent = JSON.stringify(schedule, null, 2);
  renderSummaryRecordHead();
  fillTable('daily-summary-jobs-body', (state.dailySummaryJobs || []).map(summaryJobRow), 6);
  fillTable('daily-package-runs-body', (state.dailyPackageRuns || []).map(item => `<tr><td>${text(item.run_id)}</td><td>${pill(item.status)}</td><td>${text(item.date)}<div class=\"muted\">${text(item.timezone)}</div></td><td>${progressCell(item)}</td><td>${text(item.origin_count)}</td><td>${text(item.message_count)}</td><td title=\"${attr(item.output_dir)}\">${text(item.package_md_path || item.output_dir)}</td></tr>`), 7);
  fillTable('daily-summary-runs-body', (state.dailySummaryRuns || []).map(item => `<tr><td>${text(item.run_id)}</td><td>${pill(item.status)}</td><td>${text(item.package_run_id)}</td><td>${text(item.provider)}</td><td>${progressCell(item)}</td><td>${text(item.group_count)}</td><td>${text(item.image_count)}</td><td title=\"${attr(item.output_dir)}\">${text(item.summary_path || item.output_dir)}</td></tr>`), 8);
  fillTable('daily-summary-records-body', (state.dailySummaryRecords || []).map(summaryRecordRow), state.manageSummaries ? 8 : 7);
  updateSummaryBulk();
}
function summaryJobRow(item) {
  const active = ['running', 'queued', 'cancel_requested'].includes(item.status);
  const cancelButton = active ? `<button class=\"danger\" data-action=\"cancel-summary-job\" data-job=\"${attr(item.job_id)}\">Cancel</button>` : '<span class=\"muted\">-</span>';
  return `<tr><td>${text(item.job_id)}<div class=\"muted\">${text(item.started_at)}</div></td><td>${pill(item.status)}</td><td>${progressCell(item)}</td><td>${text(item.package_run_id)}</td><td>${text(item.summary_run_id)}</td><td class=\"actions\">${cancelButton}</td></tr>`;
}
function progressCell(item) {
  const total = Number(item.progress_total || 0);
  const current = Number(item.progress_current || 0);
  const label = item.progress_label || item.progress?.label || '-';
  const counts = total ? `${current}/${total}` : '-';
  return `<div>${text(counts)}</div><div class=\"muted\">${text(label)}</div>`;
}
function renderSummaryRecordHead() {
  $('daily-summary-records-head').innerHTML = `<tr>${state.manageSummaries ? '<th>Select</th>' : ''}<th>Summary</th><th>Date</th><th>Tags</th><th>State</th><th>Content</th><th>Path</th><th>Actions</th></tr>`;
}
function summaryRecordRow(item) {
  const selected = Boolean(state.selectedSummaries[item.summary_id]);
  const selectCell = state.manageSummaries ? `<td><input class=\"summary-select\" type=\"checkbox\" data-action=\"select-summary-row\" data-summary=\"${attr(item.summary_id)}\" ${selected ? 'checked' : ''}></td>` : '';
  const statePill = item.deleted ? pill('deleted') : pill(item.important ? 'important' : 'normal');
  const action = item.deleted ? 'restore-summary-record' : 'delete-summary-record';
  const actionLabel = item.deleted ? 'Restore' : 'Delete';
  const rowClass = item.deleted ? 'deleted-row' : '';
  return `<tr class=\"${attr(rowClass)}\" data-summary=\"${attr(item.summary_id)}\">${selectCell}<td>${text(item.summary_id)}<div class=\"muted\">${text(item.provider)}</div></td><td>${text(item.date)}<div class=\"muted\">${text(item.timezone)}</div></td><td>${text((item.tags || []).join(', '))}</td><td>${statePill}</td><td>${text(item.content_preview)}</td><td title=\"${attr(item.summary_path)}\">${text(item.summary_path)}</td><td class=\"actions\"><button class=\"${item.deleted ? '' : 'danger'}\" data-action=\"${action}\" data-summary=\"${attr(item.summary_id)}\">${actionLabel}</button></td></tr>`;
}
function updateSummaryBulk() {
  const bulk = $('summary-bulk');
  if (!bulk) return;
  bulk.classList.toggle('hidden', !state.manageSummaries);
  const manageButton = document.querySelector('[data-action=\"toggle-summary-manage\"]');
  if (manageButton) manageButton.textContent = state.manageSummaries ? 'Done' : 'Manage';
  $('summary-selection').textContent = `${Object.keys(state.selectedSummaries).length} selected`;
}
function clearSummarySelection() {
  state.selectedSummaries = {};
}
function visibleSummaryIds() {
  return (state.dailySummaryRecords || []).map(item => item.summary_id).filter(Boolean);
}
function selectedSummaryIds() {
  return Object.keys(state.selectedSummaries);
}
function renderRaw() { $('raw').textContent = JSON.stringify(state, null, 2); }
function parseScopeJson(value) {
  const textValue = String(value || '').trim();
  if (!textValue) return {};
  const parsed = JSON.parse(textValue);
  if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') throw new Error('Scope JSON must be an object');
  return parsed;
}
function dailyRunPayload(form) {
  const data = formData(form);
  const payload = {};
  for (const key of ['date', 'timezone', 'account_id', 'tags', 'package_run_id']) {
    if (data[key]) payload[key] = data[key];
  }
  if (data.origin_id) payload.origin_id = Number(data.origin_id);
  if (data.topic_id) payload.topic_id = Number(data.topic_id);
  if (data.tag_groups) payload.tag_groups = String(data.tag_groups).split(';').map(item => item.trim()).filter(Boolean);
  if (data.background !== undefined) payload.background = Boolean(data.background);
  return payload;
}
function setupTagEditor(editor, initialTags='') {
  if (!editor) return;
  const chips = editor.querySelector('[data-tag-chips]');
  const input = editor.querySelector('[data-tag-input]');
  const hidden = editor.querySelector('input[name=\"tags\"]');
  let tags = uniqueTags(splitTags(initialTags));
  const clearPending = () => {
    editor.dataset.backspaceArmed = '';
    chips.querySelectorAll('.pending-remove').forEach(chip => chip.classList.remove('pending-remove'));
  };
  const render = () => {
    hidden.value = tags.join(',');
    chips.innerHTML = renderTags(tags, true);
  };
  const markPending = () => {
    const last = chips.querySelector('.tag-chip:last-child');
    chips.querySelectorAll('.pending-remove').forEach(chip => chip.classList.remove('pending-remove'));
    if (last) last.classList.add('pending-remove');
  };
  const addFromInput = () => {
    const next = splitTags(input.value);
    if (!next.length) return false;
    tags = uniqueTags([...tags, ...next]);
    input.value = '';
    clearPending();
    render();
    return true;
  };
  editor.addEventListener('click', () => input.focus());
  chips.addEventListener('click', event => {
    const button = event.target.closest('[data-tag-remove]');
    if (!button) return;
    tags.splice(Number(button.dataset.tagRemove), 1);
    clearPending();
    render();
    input.focus();
  });
  input.addEventListener('keydown', event => {
    if (event.key === 'Enter') {
      event.preventDefault();
      addFromInput();
      return;
    }
    if (event.key === 'Backspace' && input.value === '') {
      event.preventDefault();
      if (editor.dataset.backspaceArmed === '1') {
        tags.pop();
        clearPending();
        render();
      } else if (tags.length) {
        editor.dataset.backspaceArmed = '1';
        markPending();
      }
      return;
    }
    clearPending();
  });
  input.addEventListener('input', clearPending);
  input.addEventListener('blur', addFromInput);
  render();
}
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
async function setOriginRemoved(data, removed) {
  setStatus(removed ? 'Removing' : 'Restoring');
  const result = await api('/manage/origins/archive', { method: 'PATCH', body: JSON.stringify({ ...data, archived: removed }) });
  await loadAll();
  setStatus(removed ? 'Removed' : 'Restored', 'ok');
  return result;
}
async function bulkSetOriginsRemoved(removed) {
  const items = selectedOriginPayloads();
  if (!items.length) throw new Error('Select at least one origin first');
  if (!confirm(`${removed ? 'Remove' : 'Restore'} ${items.length} selected origins?`)) return;
  setStatus(removed ? 'Removing selected origins' : 'Restoring selected origins');
  for (const item of items) {
    await api('/manage/origins/archive', { method: 'PATCH', body: JSON.stringify({ ...item, archived: removed }) });
  }
  clearOriginSelection();
  await loadAll();
  setStatus(removed ? 'Selected origins removed' : 'Selected origins restored', 'ok');
}
async function bulkClearPolicies() {
  const items = selectedOriginPayloads();
  if (!items.length) throw new Error('Select at least one origin first');
  if (!confirm(`Clear backup policies for ${items.length} selected origins?`)) return;
  setStatus('Clearing selected policies');
  for (const item of items) {
    await api('/manage/backup-policies', { method: 'DELETE', body: JSON.stringify(item) });
  }
  clearOriginSelection();
  await loadAll();
  setStatus('Selected policies cleared', 'ok');
}
async function setSummaryRecordsDeleted(summaryIds, deleted) {
  if (!summaryIds.length) throw new Error('Select at least one summary first');
  setStatus(deleted ? 'Deleting summaries' : 'Restoring summaries');
  const result = await api('/manage/daily-summary-records', {
    method: 'PATCH',
    body: JSON.stringify({ summary_ids: summaryIds, deleted }),
  });
  await loadDaily();
  setStatus(`${result.item?.changed_rows || 0} summaries ${deleted ? 'deleted' : 'restored'}`, 'ok');
  return result;
}
async function bulkSetSummariesDeleted(deleted) {
  const ids = selectedSummaryIds();
  if (!ids.length) throw new Error('Select at least one summary first');
  if (!confirm(`${deleted ? 'Delete' : 'Restore'} ${ids.length} selected summaries?`)) return;
  await setSummaryRecordsDeleted(ids, deleted);
}
function selectedAccount() { return document.querySelector('#account-form [name=account_id]').value.trim(); }
function selectedOriginAccount() {
  const filtered = $('origin-filter').value.trim() || selectedAccount();
  if (filtered) return filtered;
  return state.accounts.length === 1 ? state.accounts[0].account_id : '';
}
function isOriginSelectionTarget(target) {
  return target.closest('button, a, input:not(.origin-select), select, textarea, label, .actions, .policy-row');
}
function applyOriginDragSelection(toKey) {
  if (!originDragState) return;
  selectOriginRange(originDragState.startKey, toKey, false);
  if (originDragState.additive) {
    state.selectedOrigins = { ...originDragState.baseSelection, ...state.selectedOrigins };
  }
  renderOrigins();
}
document.addEventListener('mousedown', (event) => {
  if (!state.manageOrigins || event.button !== 0) return;
  const row = event.target.closest('#origins-body tr[data-key]');
  if (!row || isOriginSelectionTarget(event.target)) return;
  const entry = originEntryFromRow(row);
  if (!entry) return;
  originDragState = {
    startKey: entry.key,
    baseSelection: { ...state.selectedOrigins },
    additive: event.ctrlKey || event.metaKey,
    dragged: false,
    lastKey: entry.key,
  };
});
document.addEventListener('mouseover', (event) => {
  if (!originDragState) return;
  const row = event.target.closest('#origins-body tr[data-key]');
  if (!row) return;
  const entry = originEntryFromRow(row);
  if (!entry || entry.key === originDragState.lastKey) return;
  originDragState.lastKey = entry.key;
  originDragState.dragged = true;
  ignoreNextOriginClick = true;
  document.body.classList.add('origin-dragging');
  applyOriginDragSelection(entry.key);
});
document.addEventListener('mouseup', () => {
  if (!originDragState) return;
  originSelectionAnchor = originDragState.startKey;
  originDragState = null;
  document.body.classList.remove('origin-dragging');
});
document.addEventListener('click', (event) => {
  if (!state.manageOrigins) return;
  const checkbox = event.target.closest('[data-action=\"select-origin-row\"]');
  if (checkbox) {
    event.preventDefault();
    event.stopPropagation();
    toggleOriginRowKey(checkbox.dataset.key, event);
    return;
  }
  const row = event.target.closest('#origins-body tr[data-key]');
  if (!row || isOriginSelectionTarget(event.target)) return;
  if (ignoreNextOriginClick) {
    ignoreNextOriginClick = false;
    event.preventDefault();
    return;
  }
  selectOriginRowKey(row.dataset.key, event);
});
document.addEventListener('click', (event) => {
  const checkbox = event.target.closest('[data-action=\"select-summary-row\"]');
  if (!checkbox) return;
  const summaryId = checkbox.dataset.summary;
  if (!summaryId) return;
  if (checkbox.checked) state.selectedSummaries[summaryId] = true;
  else delete state.selectedSummaries[summaryId];
  updateSummaryBulk();
});
document.addEventListener('click', async (event) => {
  const target = event.target.closest('button');
  if (!target) return;
  const action = target.dataset.action;
  try {
    if (target.id === 'save-token') {
      localStorage.setItem('teleMessToken', tokenValue());
      setStatus(tokenValue() ? 'Token saved' : 'Token cleared', tokenValue() ? 'ok' : 'warn');
      if (tokenValue()) await loadAll();
      else { stopMessageAutoRefresh(); stopDailyAutoRefresh(); }
    }
    else if (target.id === 'refresh' || action === 'load') await loadAll();
    else if (action === 'load-messages') await loadMessages();
    else if (action === 'load-participants') { const data = await api('/manage/participants'); state.participants = data.items || []; renderParticipants(); }
    else if (action === 'load-cursors') { const data = await api('/manage/capture-cursors'); state.cursors = data.items || []; renderCursors(); }
    else if (action === 'load-operation-events') { const data = await api('/manage/operation-events'); state.operationEvents = data.items || []; renderOperationEvents(); }
    else if (action === 'load-media') { const data = await api('/sync/media-files'); state.media = data.items || []; renderMedia(); }
    else if (action === 'load-daily') await loadDaily();
    else if (action === 'load-raw') renderRaw();
    else if (action === 'preview-media') await previewMedia(target);
    else if (action === 'open-media') await openMedia(target);
    else if (action === 'delete-operation-event') { if (confirm(`Delete operation event ${target.dataset.eventId}?`)) await removeRecord('/manage/operation-events', { id: Number(target.dataset.eventId) }); }
    else if (action === 'select-account') { document.querySelector('#account-form [name=account_id]').value = target.dataset.account; document.querySelector('#origin-filter').value = target.dataset.account; await loadOrigins(); }
    else if (action === 'select-origin') { document.querySelector('#participant-refresh-form [name=account_id]').value = target.dataset.account; document.querySelector('#participant-refresh-form [name=origin_id]').value = target.dataset.origin; }
    else if (action === 'delete-account') { if (confirm(`Delete account ${target.dataset.account}? Stored messages are kept.`)) await removeRecord('/manage/accounts', { account_id: target.dataset.account }); }
    else if (action === 'remove-origin' || action === 'restore-origin') { const removed = action === 'remove-origin'; if (confirm(`${removed ? 'Remove' : 'Restore'} origin ${target.dataset.origin}/${target.dataset.topic || 0}?`)) await setOriginRemoved(originPayload(target.dataset.account, target.dataset.origin, target.dataset.topic), removed); }
    else if (action === 'toggle-important') { await post('/manage/origins/important', { account_id: target.dataset.account, origin_id: Number(target.dataset.origin), topic_id: Number(target.dataset.topic || 0), important: target.dataset.important === 'true' }, 'PATCH'); }
    else if (action === 'delete-policy') { if (confirm(`Clear backup policy for ${target.dataset.origin}/${target.dataset.topic || 0}?`)) await removeRecord('/manage/backup-policies', { account_id: target.dataset.account, origin_id: Number(target.dataset.origin), topic_id: Number(target.dataset.topic || 0) }); }
    else if (action === 'delete-participant') { if (confirm(`Delete participant ${target.dataset.user}?`)) await removeRecord('/manage/participants', { account_id: target.dataset.account, origin_id: Number(target.dataset.origin), user_id: Number(target.dataset.user) }); }
    else if (action === 'auth-status') await post('/manage/accounts/auth/status', { account_id: selectedAccount() });
    else if (action === 'request-code') { const f = $('account-form'); await post('/manage/accounts/auth/request-code', { account_id: f.account_id.value, phone: f.phone.value }); }
    else if (action === 'submit-code') { const f = $('account-form'); await post('/manage/accounts/auth/submit-code', { account_id: f.account_id.value, phone: f.phone.value, code: f.code.value, password: f.password.value }); }
    else if (action === 'discover-selected') await post('/manage/discover-origins', { account_id: selectedAccount(), include_topics: true, topic_limit: 500 });
    else if (action === 'filter-origins' || action === 'reload-origins') { await loadOrigins(); setStatus('Origins loaded', 'ok'); }
    else if (action === 'refresh-origins') { const accountId = selectedOriginAccount(); if (!accountId) throw new Error('Select or enter an account first'); await post('/manage/discover-origins', { account_id: accountId, include_topics: true, topic_limit: 500 }); }
    else if (action === 'toggle-origin') { state.expandedOrigins[target.dataset.key] = !state.expandedOrigins[target.dataset.key]; renderOrigins(); }
    else if (action === 'toggle-origin-manage') { state.manageOrigins = !state.manageOrigins; clearOriginSelection(); renderOrigins(); }
    else if (action === 'select-visible-origins') { for (const item of visibleOriginPayloads()) state.selectedOrigins[originKey(item.account_id, item.origin_id, item.topic_id)] = item; renderOrigins(); }
    else if (action === 'clear-origin-selection') { clearOriginSelection(); renderOrigins(); }
    else if (action === 'bulk-remove-origins') await bulkSetOriginsRemoved(true);
    else if (action === 'bulk-restore-origins') await bulkSetOriginsRemoved(false);
    else if (action === 'bulk-clear-policies') await bulkClearPolicies();
    else if (action === 'toggle-summary-manage') { state.manageSummaries = !state.manageSummaries; clearSummarySelection(); renderDaily(); }
    else if (action === 'select-visible-summaries') { for (const id of visibleSummaryIds()) state.selectedSummaries[id] = true; renderDaily(); }
    else if (action === 'clear-summary-selection') { clearSummarySelection(); renderDaily(); }
    else if (action === 'bulk-delete-summaries') await bulkSetSummariesDeleted(true);
    else if (action === 'bulk-restore-summaries') await bulkSetSummariesDeleted(false);
    else if (action === 'delete-summary-record') { if (confirm(`Delete summary ${target.dataset.summary}?`)) await setSummaryRecordsDeleted([target.dataset.summary], true); }
    else if (action === 'restore-summary-record') { await setSummaryRecordsDeleted([target.dataset.summary], false); }
    else if (action === 'cancel-summary-job') { if (confirm(`Cancel summary job ${target.dataset.job}?`)) { await api('/manage/daily-summary-jobs/cancel', { method: 'PATCH', body: JSON.stringify({ job_id: target.dataset.job }) }); await loadDaily(); setStatus('Summary job cancel requested', 'warn'); } }
    else if (action === 'edit-policy') openPolicy(target);
  } catch (error) { setStatus(String(error), 'error'); }
});
document.querySelectorAll('.tab').forEach(button => button.addEventListener('click', () => {
  document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
  button.classList.add('active');
  document.querySelectorAll('.view').forEach(view => view.classList.add('hidden'));
  $(`view-${button.dataset.view}`).classList.remove('hidden');
  requestAnimationFrame(syncOverviewHeights);
}));
window.addEventListener('resize', syncOverviewHeights);
window.addEventListener('beforeunload', () => {
  stopDailyAutoRefresh();
  for (const url of mediaObjectUrls.values()) URL.revokeObjectURL(url);
});
$('account-form').addEventListener('submit', async (event) => { event.preventDefault(); try { await post('/manage/accounts', formData(event.target)); } catch (error) { setStatus(String(error), 'error'); } });
$('daily-schedule-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    const data = formData(event.target);
    const payload = { enabled: Boolean(data.enabled), time_of_day: data.time_of_day, timezone: data.timezone, scope: parseScopeJson(data.scope), activate_systemd: Boolean(data.activate_systemd) };
    const result = await api('/manage/daily-package-schedule', { method: 'PATCH', body: JSON.stringify(payload) });
    state.dailySchedule = result.item;
    await loadDaily();
    setStatus('Daily schedule saved', result.item?.last_error ? 'warn' : 'ok');
  } catch (error) { setStatus(String(error), 'error'); }
});
$('daily-package-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    const result = await api('/manage/daily-packages', { method: 'POST', body: JSON.stringify(dailyRunPayload(event.target)) });
    await loadDaily();
    setStatus(`Package ${result.item?.run_id || ''} ${result.item?.status || ''}`, result.item?.status === 'failed' ? 'error' : 'ok');
  } catch (error) { setStatus(String(error), 'error'); }
});
$('daily-summary-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    const result = await api('/manage/daily-summaries', { method: 'POST', body: JSON.stringify(dailyRunPayload(event.target)) });
    await loadDaily();
    setStatus(`Summary ${result.item?.run_id || ''} ${result.item?.status || ''}`, result.item?.status === 'failed' ? 'error' : 'ok');
  } catch (error) { setStatus(String(error), 'error'); }
});
$('daily-summary-job-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    const result = await api('/manage/daily-summary-jobs', { method: 'POST', body: JSON.stringify(dailyRunPayload(event.target)) });
    await loadDaily();
    setStatus(`Summary job ${result.item?.job_id || ''} ${result.item?.status || ''}`, result.item?.status === 'failed' ? 'error' : 'ok');
  } catch (error) { setStatus(String(error), 'error'); }
});
$('participant-refresh-form').addEventListener('submit', async (event) => { event.preventDefault(); try { await post('/manage/participants/refresh', numberFields(formData(event.target), ['origin_id','limit'])); } catch (error) { setStatus(String(error), 'error'); } });
$('participant-form').addEventListener('submit', async (event) => { event.preventDefault(); try { await post('/manage/participants', numberFields(formData(event.target), ['origin_id','user_id'])); } catch (error) { setStatus(String(error), 'error'); } });
$('show-archived').addEventListener('change', async () => { try { await loadOrigins(); setStatus('Origins loaded', 'ok'); } catch (error) { setStatus(String(error), 'error'); } });
$('show-deleted-summaries').addEventListener('change', async () => { try { await loadDaily(); } catch (error) { setStatus(String(error), 'error'); } });
['origin-search', 'origin-type-filter', 'origin-backup-filter', 'origin-tag-filter', 'origin-sort'].forEach(id => {
  const input = $(id);
  if (input) input.addEventListener('input', renderOrigins);
  if (input) input.addEventListener('change', renderOrigins);
});
function openPolicy(button) {
  refreshTagSuggestions();
  const accountId = button.dataset.account;
  const originId = Number(button.dataset.origin);
  const topicId = Number(button.dataset.topic || 0);
  document.querySelectorAll('.policy-row').forEach(row => row.remove());
  const policy = state.policies.find(item => item.account_id === accountId && item.origin_id === originId && item.topic_id === topicId) || {};
  const origin = state.origins.find(item => item.account_id === accountId && item.origin_id === originId && item.topic_id === topicId);
  const row = document.createElement('tr');
  row.className = 'policy-row';
  const cell = document.createElement('td');
  cell.colSpan = state.manageOrigins ? 10 : 9;
  const form = $('policy-template').content.firstElementChild.cloneNode(true);
  form.account_id.value = accountId; form.origin_id.value = originId; form.topic_id.value = topicId;
  form.enabled.checked = policy.enabled ?? origin?.backup_policy?.enabled ?? false;
  form.capture_text.checked = policy.capture_text ?? origin?.backup_policy?.capture_text ?? true;
  form.capture_media_metadata.checked = policy.capture_media_metadata ?? origin?.backup_policy?.capture_media_metadata ?? true;
  form.download_media.checked = policy.download_media ?? origin?.backup_policy?.download_media ?? false;
  setupTagEditor(form.querySelector('[data-tag-editor]'), policy.tags ?? origin?.backup_policy?.tags ?? '');
  form.addEventListener('submit', async (event) => { event.preventDefault(); try { await post('/manage/backup-policies', numberFields(formData(form), ['origin_id','topic_id']), 'PATCH'); row.remove(); } catch (error) { setStatus(String(error), 'error'); } });
  cell.appendChild(form); row.appendChild(cell); button.closest('tr').after(row);
}
if (tokenValue()) loadAll();
else setStatus('Enter server.token from config.yml, then click Save', 'warn');
</script>
</body>
</html>"""
