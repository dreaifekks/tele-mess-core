from __future__ import annotations

import asyncio
import hmac
import ipaddress
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
    API_ENDPOINTS,
    API_CONTRACT_HASH,
    API_CONTRACT_VERSION,
    API_MANIFEST_PATH,
    MARKDOWN_API_DOC_PATH,
    OPENAPI_PATH,
    api_manifest,
    markdown_document,
    openapi_document,
    validate_query_params,
    validate_request_payload,
)
from tele_mess_core.server.console import console_html
from tele_mess_core.telegram.runtime import TelegramOperationError

if TYPE_CHECKING:
    from tele_mess_core.config import AppConfig, TelegramAccountConfig
    from tele_mess_core.daily_jobs import DailyJobWorker
    from tele_mess_core.telegram.manager import TelegramRuntimeManager


MAX_JSON_BODY_BYTES = 1024 * 1024
ENDPOINTS_BY_ROUTE = {(endpoint.method, endpoint.path): endpoint for endpoint in API_ENDPOINTS}
METHODS_BY_PATH = {
    path: {endpoint.method for endpoint in API_ENDPOINTS if endpoint.path == path}
    for path in {endpoint.path for endpoint in API_ENDPOINTS}
}


class ApiError(Exception):
    def __init__(self, status: HTTPStatus, code: str, detail: str):
        super().__init__(detail)
        self.status = status
        self.code = code
        self.detail = detail


class SyncApiServer:
    def __init__(
        self,
        store: ArchiveStore,
        host: str,
        port: int,
        token: str = "",
        config: "AppConfig | None" = None,
        allow_unauthenticated_localhost: bool = False,
        telegram_runtime: "TelegramRuntimeManager | None" = None,
        daily_worker: "DailyJobWorker | None" = None,
    ):
        _validate_server_auth(host, token, allow_unauthenticated_localhost)
        self.store = store
        self.host = host
        self.port = port
        self.token = token
        self.config = config
        self.telegram_runtime = telegram_runtime
        self.daily_worker = daily_worker
        self.logger = logging.getLogger(__name__)
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._startup_event: threading.Event | None = None
        self._startup_error: BaseException | None = None
        self._stopped_event = threading.Event()

    def serve_forever(self) -> None:
        self._stopped_event.clear()
        try:
            handler = self._make_handler()
            self._httpd = ThreadingHTTPServer((self.host, self.port), handler)
            if self._startup_event:
                self._startup_event.set()
            self.logger.info("Sync API listening on http://%s:%s", self.host, self.port)
            self._httpd.serve_forever()
        finally:
            self._stopped_event.set()

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

        self._thread = threading.Thread(target=target, name="sync-api", daemon=False)
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

    async def wait_stopped(self) -> None:
        await asyncio.to_thread(self._stopped_event.wait)

    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        store = self.store
        token = self.token
        config = self.config
        telegram_runtime = self.telegram_runtime
        daily_worker = self.daily_worker

        class Handler(BaseHTTPRequestHandler):
            server_version = "tele-mess-core/0.2.5"

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
                endpoint = ENDPOINTS_BY_ROUTE.get((method, parsed.path))
                if token and (endpoint is None or endpoint.auth) and not self._authorized(token):
                    self._json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                    return
                if endpoint is None:
                    status = HTTPStatus.METHOD_NOT_ALLOWED if parsed.path in METHODS_BY_PATH else HTTPStatus.NOT_FOUND
                    self._json(
                        {"error": "method_not_allowed" if status == HTTPStatus.METHOD_NOT_ALLOWED else "not_found"},
                        status=status,
                    )
                    return

                try:
                    validate_query_params(endpoint, params)
                    if method == "GET":
                        self._handle_get(parsed.path, params)
                    elif method in {"POST", "PATCH", "DELETE"}:
                        self._handle_write(method, parsed.path, endpoint)
                    else:
                        self._json({"error": "method_not_allowed"}, status=HTTPStatus.METHOD_NOT_ALLOWED)
                except ValueError as exc:
                    self._json({"error": "bad_request", "detail": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                except ApiError as exc:
                    self._json({"error": exc.code, "detail": exc.detail}, status=exc.status)
                except TelegramOperationError as exc:
                    self._json({"error": exc.code, "detail": exc.message, **exc.to_public_dict()}, status=exc.http_status)
                except Exception as exc:
                    logging.getLogger("tele_mess_core.server").exception("API request failed")
                    self._json({"error": "internal_error", "detail": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
                finally:
                    store.close_thread_connection()

            def _handle_get(self, path: str, params: dict[str, list[str]]) -> None:
                if path == "/" or path == "/console":
                    self._html(console_html())
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
                    self._json({"item": _daily_package_schedule_item(config, store)})
                elif path == "/manage/daily-summary-delivery":
                    self._json({"item": _daily_summary_delivery(config, store)})
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

            def _handle_write(self, method: str, path: str, endpoint: Any) -> None:
                payload = self._read_json()
                validate_request_payload(endpoint, payload)
                if path == "/manage/accounts" and method == "POST":
                    item = _create_management_account(config, store, payload, telegram_runtime)
                    self._json({"item": item}, status=HTTPStatus.CREATED)
                elif path == "/manage/accounts" and method == "DELETE":
                    item = _delete_management_account(store, payload, telegram_runtime)
                    self._json({"item": item})
                elif path == "/manage/accounts/auth" and method in {"POST", "PATCH"}:
                    item = _update_account_auth(store, payload)
                    self._json({"item": item})
                elif path == "/manage/accounts/auth/status" and method == "POST":
                    item = _auth_status(config, store, payload, telegram_runtime)
                    self._json({"item": item})
                elif path == "/manage/accounts/auth/request-code" and method == "POST":
                    item = _request_auth_code(config, store, payload, telegram_runtime)
                    self._json({"item": item})
                elif path == "/manage/accounts/auth/submit-code" and method == "POST":
                    item = _submit_auth_code(config, store, payload, telegram_runtime)
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
                    item = _set_backup_policy(store, payload, telegram_runtime)
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
                    item = _discover_origins(config, store, payload, telegram_runtime)
                    self._json({"item": item})
                elif path == "/manage/participants/refresh" and method == "POST":
                    item = _refresh_participants(config, store, payload, telegram_runtime)
                    self._json({"item": item})
                elif path == "/manage/operation-events" and method == "DELETE":
                    item = _delete_operation_events(store, payload)
                    self._json({"item": item})
                elif path == "/manage/daily-package-schedule" and method == "PATCH":
                    item = _update_daily_package_schedule(config, store, payload)
                    self._json({"item": item})
                elif path == "/manage/daily-summary-delivery" and method == "PATCH":
                    item = _update_daily_summary_delivery(config, store, payload)
                    self._json({"item": item})
                elif path == "/manage/daily-packages" and method == "POST":
                    item = _create_daily_package(config, store, payload)
                    self._json({"item": item}, status=HTTPStatus.CREATED)
                elif path == "/manage/daily-summaries" and method == "POST":
                    item = _create_daily_summary(config, store, payload, telegram_runtime, daily_worker)
                    self._json({"item": item}, status=HTTPStatus.CREATED)
                elif path == "/manage/daily-summary-jobs" and method == "POST":
                    item = _create_daily_summary_job(config, store, payload, daily_worker)
                    self._json({"item": item}, status=HTTPStatus.CREATED)
                elif path == "/manage/daily-summary-jobs/cancel" and method == "PATCH":
                    item = _cancel_daily_summary_job(store, payload, daily_worker)
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
                if hmac.compare_digest(auth, f"Bearer {expected}"):
                    return True
                supplied = self.headers.get("X-Api-Token", "")
                return hmac.compare_digest(supplied, expected)

            def _read_json(self) -> dict[str, Any]:
                raw_length = self.headers.get("Content-Length", "0") or "0"
                try:
                    length = int(raw_length)
                except ValueError as exc:
                    raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_content_length", "Content-Length must be an integer") from exc
                if length < 0:
                    raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_content_length", "Content-Length must not be negative")
                if length <= 0:
                    return {}
                if length > MAX_JSON_BODY_BYTES:
                    raise ApiError(
                        HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                        "payload_too_large",
                        f"JSON body exceeds {MAX_JSON_BODY_BYTES} bytes",
                    )
                content_type = self.headers.get_content_type()
                if content_type != "application/json":
                    raise ApiError(
                        HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                        "unsupported_media_type",
                        "Content-Type must be application/json",
                    )
                body = self.rfile.read(length).decode("utf-8")
                data = json.loads(body)
                if not isinstance(data, dict):
                    raise ValueError("JSON body must be an object")
                return data

            def _json(self, data: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
                body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
                self.send_response(int(status))
                self._security_headers()
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _html(self, html: str, status: HTTPStatus = HTTPStatus.OK) -> None:
                body = html.encode("utf-8")
                self.send_response(int(status))
                self._security_headers()
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _text(self, text: str, content_type: str, status: HTTPStatus = HTTPStatus.OK) -> None:
                body = text.encode("utf-8")
                self.send_response(int(status))
                self._security_headers()
                self.send_header("Content-Type", content_type)
                self.send_header("Cache-Control", "no-store")
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
                self._security_headers()
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

            def _security_headers(self) -> None:
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Referrer-Policy", "no-referrer")
                self.send_header("X-Frame-Options", "DENY")
                self.send_header(
                    "Content-Security-Policy",
                    "default-src 'self'; base-uri 'none'; connect-src 'self'; "
                    "form-action 'self'; frame-ancestors 'none'; img-src 'self' data:; "
                    "script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'",
                )

        return Handler


def _validate_server_auth(host: str, token: str, allow_unauthenticated_localhost: bool) -> None:
    if token:
        return
    if not allow_unauthenticated_localhost:
        raise ValueError(
            "server.token is required; set server.allow_unauthenticated_localhost=true only for isolated local development"
        )
    if not _is_loopback_host(host):
        raise ValueError("Unauthenticated server mode is only allowed on a loopback host")


def _is_loopback_host(host: str) -> bool:
    normalized = str(host or "").strip().lower()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


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
            "daily_summary_delivery",
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


def _create_management_account(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
    telegram_runtime: "TelegramRuntimeManager | None" = None,
) -> dict[str, Any]:
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
    item = _find_account(store, source, account_id)
    if telegram_runtime is not None and source == SOURCE_TELEGRAM:
        telegram_runtime.register_account(_account_config(config, store, account_id, source))
    return item


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


def _delete_management_account(
    store: ArchiveStore,
    payload: dict[str, Any],
    telegram_runtime: "TelegramRuntimeManager | None" = None,
) -> dict[str, Any]:
    source = _source(payload)
    account_id = _required_str(payload, "account_id")
    if telegram_runtime is not None and source == SOURCE_TELEGRAM:
        telegram_runtime.unregister_account(account_id)
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


def _set_backup_policy(
    store: ArchiveStore,
    payload: dict[str, Any],
    telegram_runtime: "TelegramRuntimeManager | None" = None,
) -> dict[str, Any]:
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
    item = _find_policy(store, account_id, origin_id, topic_id)
    if telegram_runtime is not None and source == SOURCE_TELEGRAM:
        telegram_runtime.notify(account_id, "refresh_capture")
    return item


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


def _auth_status(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
    telegram_runtime: "TelegramRuntimeManager | None" = None,
) -> dict[str, Any]:
    account_id = _required_str(payload, "account_id")
    source = _source(payload)
    if telegram_runtime is not None and source == SOURCE_TELEGRAM:
        return telegram_runtime.call(account_id, "auth_status")
    account = _account_config(config, store, account_id, source)
    from tele_mess_core.telegram.auth import TelegramAuthService

    return asyncio.run(TelegramAuthService(account, store).status())


def _request_auth_code(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
    telegram_runtime: "TelegramRuntimeManager | None" = None,
) -> dict[str, Any]:
    account_id = _required_str(payload, "account_id")
    source = _source(payload)
    phone = _required_str(payload, "phone")
    if telegram_runtime is not None and source == SOURCE_TELEGRAM:
        return telegram_runtime.call(account_id, "request_code", phone=phone)
    account = _account_config(config, store, account_id, source)
    from tele_mess_core.telegram.auth import TelegramAuthService

    return asyncio.run(TelegramAuthService(account, store).request_code(phone))


def _submit_auth_code(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
    telegram_runtime: "TelegramRuntimeManager | None" = None,
) -> dict[str, Any]:
    account_id = _required_str(payload, "account_id")
    source = _source(payload)
    phone = _required_str(payload, "phone")
    code = _required_str(payload, "code")
    password = _optional_payload_str(payload, "password")
    if telegram_runtime is not None and source == SOURCE_TELEGRAM:
        return telegram_runtime.call(account_id, "submit_code", phone=phone, code=code, password=password)
    account = _account_config(config, store, account_id, source)
    from tele_mess_core.telegram.auth import TelegramAuthService

    return asyncio.run(TelegramAuthService(account, store).submit_code(phone, code, password))


def _discover_origins(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
    telegram_runtime: "TelegramRuntimeManager | None" = None,
) -> dict[str, Any]:
    account_id = _required_str(payload, "account_id")
    source = _source(payload)
    include_topics = _payload_bool(payload, "include_topics", True)
    include_private = _payload_bool(payload, "include_private", False)
    topic_limit = _payload_int(payload, "topic_limit", 100)
    if telegram_runtime is not None and source == SOURCE_TELEGRAM:
        return telegram_runtime.call(
            account_id,
            "discover_origins",
            include_topics=include_topics,
            topic_limit=topic_limit,
            include_private=include_private,
        )
    account = _account_config(config, store, account_id, source)
    from tele_mess_core.telegram.discovery import TelegramDiscoveryService

    return asyncio.run(
        TelegramDiscoveryService(account, store).discover_origins(
            include_topics=include_topics,
            topic_limit=topic_limit,
            include_private=include_private,
        )
    )


def _refresh_participants(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
    telegram_runtime: "TelegramRuntimeManager | None" = None,
) -> dict[str, Any]:
    account_id = _required_str(payload, "account_id")
    source = _source(payload)
    origin_id = _required_int(payload, "origin_id")
    limit = _payload_int(payload, "limit", 500)
    if telegram_runtime is not None and source == SOURCE_TELEGRAM:
        return telegram_runtime.call(
            account_id,
            "refresh_participants",
            origin_id=origin_id,
            limit=limit,
        )
    account = _account_config(config, store, account_id, source)
    from tele_mess_core.telegram.discovery import TelegramDiscoveryService

    return asyncio.run(TelegramDiscoveryService(account, store).refresh_participants(origin_id, limit))


def _update_daily_package_schedule(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
) -> dict[str, Any]:
    if config is None:
        raise ValueError("Server config is required for daily package scheduling")
    from tele_mess_core.daily import update_daily_package_schedule, update_daily_summary_delivery

    if "delivery" in payload:
        update_daily_summary_delivery(store, config, dict(payload.get("delivery") or {}))
    schedule_payload = {key: value for key, value in payload.items() if key != "delivery"}
    update_daily_package_schedule(store, config, schedule_payload)
    return _daily_package_schedule_item(config, store)


def _daily_package_schedule_item(
    config: "AppConfig | None",
    store: ArchiveStore,
) -> dict[str, Any]:
    item = store.get_daily_package_schedule()
    if config is not None:
        item["delivery"] = _daily_summary_delivery(config, store)
    return item


def _daily_summary_delivery(
    config: "AppConfig | None",
    store: ArchiveStore,
) -> dict[str, Any]:
    if config is None:
        raise ValueError("Server config is required for daily summary delivery")
    from tele_mess_core.daily import daily_summary_delivery_state

    return daily_summary_delivery_state(store, config)


def _update_daily_summary_delivery(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
) -> dict[str, Any]:
    if config is None:
        raise ValueError("Server config is required for daily summary delivery")
    from tele_mess_core.daily import update_daily_summary_delivery

    return update_daily_summary_delivery(store, config, payload)


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
    telegram_runtime: "TelegramRuntimeManager | None" = None,
    daily_worker: "DailyJobWorker | None" = None,
) -> dict[str, Any]:
    if config is None:
        raise ValueError("Server config is required for daily summary runs")
    from tele_mess_core.daily import run_daily_summary

    scope = _daily_scope(payload)
    if daily_worker is not None:
        job = daily_worker.enqueue(
            package_run_id=_optional_payload_str(payload, "package_run_id"),
            run_date=_optional_payload_str(payload, "date"),
            timezone_name=_optional_payload_str(payload, "timezone"),
            scope=scope,
            force=_payload_bool(payload, "force", False),
        )
        if _payload_bool(payload, "background", True):
            return job
        terminal = daily_worker.wait_for_terminal(
            str(job["job_id"]),
            timeout=max(300, config.daily.ai.timeout_seconds * 20),
        )
        if terminal.get("summary_run_id"):
            summary = store.get_daily_summary_run(str(terminal["summary_run_id"]))
            if summary is not None:
                return summary
        return terminal
    if _payload_bool(payload, "background", True):
        return _create_daily_summary_job(
            config,
            store,
            payload,
            daily_worker=daily_worker,
        )
    return run_daily_summary(
        store,
        config,
        package_run_id=_optional_payload_str(payload, "package_run_id"),
        run_date=_optional_payload_str(payload, "date"),
        timezone_name=_optional_payload_str(payload, "timezone"),
        scope=scope,
        telegram_runtime=telegram_runtime,
    )


def _create_daily_summary_job(
    config: "AppConfig | None",
    store: ArchiveStore,
    payload: dict[str, Any],
    daily_worker: "DailyJobWorker | None" = None,
) -> dict[str, Any]:
    if config is None:
        raise ValueError("Server config is required for daily summary jobs")
    scope = _daily_scope(payload)
    if daily_worker is not None:
        return daily_worker.enqueue(
            package_run_id=_optional_payload_str(payload, "package_run_id"),
            run_date=_optional_payload_str(payload, "date"),
            timezone_name=_optional_payload_str(payload, "timezone"),
            scope=scope,
            force=_payload_bool(payload, "force", False),
        )
    from tele_mess_core.daily_jobs import enqueue_daily_summary_job

    return enqueue_daily_summary_job(
        store,
        config,
        package_run_id=_optional_payload_str(payload, "package_run_id"),
        run_date=_optional_payload_str(payload, "date"),
        timezone_name=_optional_payload_str(payload, "timezone"),
        scope=scope,
        force=_payload_bool(payload, "force", False),
    )


def _cancel_daily_summary_job(
    store: ArchiveStore,
    payload: dict[str, Any],
    daily_worker: "DailyJobWorker | None" = None,
) -> dict[str, Any]:
    job_id = _required_str(payload, "job_id")
    if daily_worker is not None:
        return daily_worker.cancel(job_id)
    item = store.request_daily_summary_job_cancel(job_id)
    if item is None:
        raise ValueError("Unknown daily summary job")
    return item


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
