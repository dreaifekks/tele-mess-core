from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any


API_CONTRACT_VERSION = "2026-07-19.1"
API_MANIFEST_PATH = "/manage/api-manifest"
OPENAPI_PATH = "/openapi.json"
MARKDOWN_API_DOC_PATH = "/docs/api.md"


@dataclass(frozen=True, slots=True)
class ApiParam:
    name: str
    type: str = "string"
    required: bool = False
    description: str = ""
    default: Any | None = None


@dataclass(frozen=True, slots=True)
class ApiEndpoint:
    method: str
    path: str
    tag: str
    summary: str
    auth: bool = True
    query: tuple[ApiParam, ...] = ()
    body_schema: str | None = None
    response_schema: str | None = None
    response_content_type: str = "application/json"
    status: int = 200
    notes: str = ""
    operation_id: str | None = None

    def id(self) -> str:
        if self.operation_id:
            return self.operation_id
        cleaned = self.path.strip("/").replace("/", "_").replace("-", "_") or "root"
        return f"{self.method.lower()}_{cleaned}"


def _object(
    properties: dict[str, Any],
    *,
    required: list[str] | None = None,
    description: str = "",
    additional_properties: bool | None = None,
) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    if description:
        schema["description"] = description
    if additional_properties is not None:
        schema["additionalProperties"] = additional_properties
    return schema


def _array(item_ref: str) -> dict[str, Any]:
    return {"type": "array", "items": {"$ref": f"#/components/schemas/{item_ref}"}}


def _items_response(item_ref: str) -> dict[str, Any]:
    return _object({"items": _array(item_ref)}, required=["items"])


def _page_response(item_ref: str) -> dict[str, Any]:
    return _object(
        {
            "items": _array(item_ref),
            "next_cursor": {"type": "integer"},
            "has_more": {"type": "boolean"},
        },
        required=["items", "next_cursor", "has_more"],
    )


def _item_response(item_ref: str) -> dict[str, Any]:
    return _object({"item": {"$ref": f"#/components/schemas/{item_ref}"}}, required=["item"])


def _delete_response(extra: dict[str, Any]) -> dict[str, Any]:
    return _object({"deleted_rows": {"type": "integer"}, **extra})


SCHEMAS: dict[str, dict[str, Any]] = {
    "ErrorResponse": _object(
        {
            "error": {"type": "string"},
            "detail": {"type": "string"},
        },
        required=["error"],
    ),
    "HealthResponse": _object(
        {
            "ok": {"type": "boolean"},
            "database_id": {"type": "string"},
            "schema_version": {"type": "integer"},
            "message_count": {"type": "integer"},
            "last_event_seq": {"type": "integer"},
            "operation_error_count": {"type": "integer"},
            "server_time": {"type": "string"},
        },
        required=["ok", "schema_version", "message_count", "last_event_seq"],
    ),
    "StateResponse": _object(
        {
            "database_id": {"type": "string"},
            "schema_version": {"type": "integer"},
            "message_count": {"type": "integer"},
            "last_event_seq": {"type": "integer"},
            "operation_error_count": {"type": "integer"},
            "server_time": {"type": "string"},
        },
        required=["schema_version", "message_count", "last_event_seq"],
    ),
    "CapabilitiesResponse": _object(
        {
            "mode": {"type": "string"},
            "sync": {"type": "array", "items": {"type": "string"}},
            "management": {"type": "array", "items": {"type": "string"}},
            "auth_flow": {"type": "object"},
            "api_contract": {"type": "object"},
        },
        required=["mode", "sync", "management"],
    ),
    "ApiManifest": _object(
        {
            "name": {"type": "string"},
            "contract_version": {"type": "string"},
            "contract_hash": {"type": "string"},
            "openapi_url": {"type": "string"},
            "markdown_url": {"type": "string"},
            "agent_doc": {"type": "string"},
            "endpoints": {"type": "array", "items": {"type": "object"}},
            "schemas": {"type": "object"},
        },
        required=["contract_version", "contract_hash", "endpoints"],
    ),
    "Account": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
            "display_name": {"type": "string", "nullable": True},
            "kind": {"type": "string", "nullable": True},
            "auth_state": {"type": "string", "nullable": True},
            "phone": {"type": "string", "nullable": True},
            "session_name": {"type": "string", "nullable": True},
            "session_dir": {"type": "string", "nullable": True},
            "last_error": {"type": "string", "nullable": True},
            "updated_at": {"type": "string", "nullable": True},
            "auth_updated_at": {"type": "string", "nullable": True},
            "raw_json": {"type": "object", "nullable": True},
        },
        required=["source", "account_id"],
    ),
    "AccountInput": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
            "display_name": {"type": "string"},
            "kind": {"type": "string", "default": "telegram"},
            "auth_state": {"type": "string", "default": "pending_auth"},
            "phone": {"type": "string"},
            "session_name": {"type": "string"},
            "session_dir": {"type": "string"},
        },
        required=["account_id"],
    ),
    "AccountAuthInput": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
            "auth_state": {"type": "string"},
            "status": {"type": "string"},
            "phone": {"type": "string"},
            "session_name": {"type": "string"},
            "session_dir": {"type": "string"},
            "last_error": {"type": "string"},
        },
        required=["account_id"],
    ),
    "AuthStatusInput": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
        },
        required=["account_id"],
    ),
    "RequestCodeInput": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
            "phone": {"type": "string"},
        },
        required=["account_id", "phone"],
    ),
    "SubmitCodeInput": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
            "phone": {"type": "string"},
            "code": {"type": "string"},
            "password": {"type": "string"},
        },
        required=["account_id", "phone", "code"],
    ),
    "AuthResult": _object(
        {
            "account_id": {"type": "string"},
            "authorized": {"type": "boolean"},
            "auth_state": {"type": "string"},
            "phone": {"type": "string", "nullable": True},
            "message": {"type": "string", "nullable": True},
            "requires_password": {"type": "boolean"},
            "phone_code_hash": {"type": "string", "nullable": True},
        },
    ),
    "Origin": _object(
        {
            "source": {"type": "string"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "topic_id": {"type": "integer", "default": 0},
            "origin_type": {"type": "string"},
            "parent_origin_id": {"type": "integer", "nullable": True},
            "title": {"type": "string", "nullable": True},
            "username": {"type": "string", "nullable": True},
            "is_forum": {"type": "boolean"},
            "important": {"type": "boolean"},
            "archived_at": {"type": "string", "nullable": True},
            "last_message_at": {"type": "string", "nullable": True},
            "discovered_at": {"type": "string", "nullable": True},
            "updated_at": {"type": "string", "nullable": True},
            "parent_title": {"type": "string", "nullable": True},
            "backup_policy": {"$ref": "#/components/schemas/BackupPolicy", "nullable": True},
            "raw_json": {"type": "object", "nullable": True},
        },
        required=["source", "account_id", "origin_id", "topic_id", "origin_type"],
    ),
    "OriginInput": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "topic_id": {"type": "integer", "default": 0},
            "origin_type": {"type": "string"},
            "parent_origin_id": {"type": "integer"},
            "title": {"type": "string"},
            "username": {"type": "string"},
            "is_forum": {"type": "boolean", "default": False},
            "important": {"type": "boolean", "default": False},
            "last_message_at": {"type": "string"},
        },
        required=["account_id", "origin_id", "origin_type"],
    ),
    "OriginImportantInput": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "topic_id": {"type": "integer", "default": 0},
            "important": {"type": "boolean", "default": True},
        },
        required=["account_id", "origin_id"],
    ),
    "OriginArchiveInput": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "topic_id": {"type": "integer", "default": 0},
            "archived": {"type": "boolean", "default": True},
        },
        required=["account_id", "origin_id"],
    ),
    "BackupPolicy": _object(
        {
            "source": {"type": "string"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "topic_id": {"type": "integer", "default": 0},
            "enabled": {"type": "boolean"},
            "capture_text": {"type": "boolean"},
            "capture_media_metadata": {"type": "boolean"},
            "download_media": {"type": "boolean"},
            "tags": {"type": "string", "nullable": True},
            "updated_at": {"type": "string", "nullable": True},
        },
        required=["source", "account_id", "origin_id", "topic_id"],
    ),
    "BackupPolicyInput": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "topic_id": {"type": "integer", "default": 0},
            "enabled": {"type": "boolean", "default": False},
            "capture_text": {"type": "boolean", "default": True},
            "capture_media_metadata": {"type": "boolean", "default": True},
            "download_media": {"type": "boolean", "default": False},
            "tags": {"type": "string"},
        },
        required=["account_id", "origin_id"],
    ),
    "Participant": _object(
        {
            "source": {"type": "string"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "user_id": {"type": "integer"},
            "username": {"type": "string", "nullable": True},
            "display_name": {"type": "string", "nullable": True},
            "is_bot": {"type": "boolean"},
            "role": {"type": "string", "nullable": True},
            "last_seen_at": {"type": "string", "nullable": True},
            "updated_at": {"type": "string", "nullable": True},
            "raw_json": {"type": "object", "nullable": True},
        },
        required=["source", "account_id", "origin_id", "user_id"],
    ),
    "ParticipantInput": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "user_id": {"type": "integer"},
            "username": {"type": "string"},
            "display_name": {"type": "string"},
            "is_bot": {"type": "boolean", "default": False},
            "role": {"type": "string"},
            "last_seen_at": {"type": "string"},
        },
        required=["account_id", "origin_id", "user_id"],
    ),
    "ParticipantRefreshInput": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "limit": {"type": "integer", "default": 500},
        },
        required=["account_id", "origin_id"],
    ),
    "DiscoveryInput": _object(
        {
            "source": {"type": "string", "default": "telegram"},
            "account_id": {"type": "string"},
            "include_topics": {"type": "boolean", "default": True},
            "include_private": {"type": "boolean", "default": False},
            "topic_limit": {"type": "integer", "default": 100},
        },
        required=["account_id"],
    ),
    "DiscoveryResult": _object(
        {
            "account_id": {"type": "string"},
            "authorized": {"type": "boolean"},
            "status": {"type": "string"},
            "origins": {"type": "integer"},
            "topics": {"type": "integer"},
            "private_skipped": {"type": "integer"},
            "errors": {"type": "array", "items": {"type": "object"}},
            "topics_truncated": {"type": "boolean"},
            "topic_limit": {"type": "integer"},
            "include_private": {"type": "boolean"},
        },
    ),
    "ParticipantRefreshResult": _object(
        {
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "authorized": {"type": "boolean"},
            "status": {"type": "string"},
            "participants": {"type": "integer"},
            "errors": {"type": "array", "items": {"type": "object"}},
            "participants_truncated": {"type": "boolean"},
            "limit": {"type": "integer"},
        },
    ),
    "CaptureCursor": _object(
        {
            "source": {"type": "string"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "topic_id": {"type": "integer"},
            "last_message_id": {"type": "integer"},
            "history_scanned_through_id": {"type": "integer"},
            "observed_max_message_id": {"type": "integer"},
            "last_message_at": {"type": "string", "nullable": True},
            "last_backfill_at": {"type": "string", "nullable": True},
            "backfill_head_message_id": {"type": "integer", "nullable": True},
            "backfill_status": {"type": "string", "nullable": True},
            "backfill_error": {"type": "string", "nullable": True},
            "backfill_count": {"type": "integer", "nullable": True},
            "updated_at": {"type": "string", "nullable": True},
            "origin_title": {"type": "string", "nullable": True},
            "raw_json": {"type": "object", "nullable": True},
        },
        required=["source", "account_id", "origin_id", "topic_id"],
    ),
    "OperationEvent": _object(
        {
            "id": {"type": "integer"},
            "source": {"type": "string"},
            "account_id": {"type": "string"},
            "operation": {"type": "string"},
            "status": {"type": "string"},
            "subject_type": {"type": "string", "nullable": True},
            "subject_id": {"type": "string", "nullable": True},
            "error_code": {"type": "string", "nullable": True},
            "message": {"type": "string", "nullable": True},
            "retry_after": {"type": "integer", "nullable": True},
            "occurred_at": {"type": "string", "nullable": True},
            "error": {"type": "object"},
            "error_type": {"type": "string", "nullable": True},
            "auth_state": {"type": "string", "nullable": True},
            "subject": {"type": "object"},
            "subject_label": {"type": "string", "nullable": True},
            "raw_json": {"type": "object", "nullable": True},
        },
        required=["id", "source", "account_id", "operation", "status"],
    ),
    "OperationEventDeleteInput": _object(
        {
            "id": {"type": "integer"},
            "ids": {"type": "array", "items": {"type": "integer"}},
        },
    ),
    "OperationEventDeleteResult": _object(
        {
            "ids": {"type": "array", "items": {"type": "integer"}},
            "deleted": {"type": "integer"},
        },
        required=["ids", "deleted"],
    ),
    "Chat": _object(
        {
            "source": {"type": "string"},
            "account_id": {"type": "string"},
            "chat_id": {"type": "integer"},
            "title": {"type": "string", "nullable": True},
            "username": {"type": "string", "nullable": True},
            "kind": {"type": "string", "nullable": True},
            "updated_at": {"type": "string", "nullable": True},
            "raw_json": {"type": "object", "nullable": True},
        },
        required=["source", "account_id", "chat_id"],
    ),
    "Message": _object(
        {
            "event_seq": {"type": "integer"},
            "source": {"type": "string"},
            "account_id": {"type": "string"},
            "chat_id": {"type": "integer"},
            "message_id": {"type": "integer"},
            "topic_id": {"type": "integer", "nullable": True},
            "sender_id": {"type": "integer", "nullable": True},
            "sender_name": {"type": "string", "nullable": True},
            "sender_username": {"type": "string", "nullable": True},
            "sent_at": {"type": "string"},
            "edited_at": {"type": "string", "nullable": True},
            "ingested_at": {"type": "string", "nullable": True},
            "deleted_at": {"type": "string", "nullable": True},
            "text": {"type": "string", "nullable": True},
            "has_media": {"type": "boolean"},
            "media_kind": {"type": "string", "nullable": True},
            "media_count": {"type": "integer"},
            "media_files": _array("MediaFile"),
            "chat_title": {"type": "string", "nullable": True},
            "origin_title": {"type": "string", "nullable": True},
            "raw_json": {"type": "object", "nullable": True},
        },
        required=["source", "account_id", "chat_id", "message_id"],
    ),
    "Event": _object(
        {
            "seq": {"type": "integer"},
            "source": {"type": "string"},
            "account_id": {"type": "string"},
            "event_type": {"type": "string"},
            "chat_id": {"type": "integer"},
            "message_id": {"type": "integer"},
            "event_at": {"type": "string"},
            "payload_json": {"type": "object", "nullable": True},
        },
        required=["seq", "source", "account_id", "event_type"],
    ),
    "MediaFile": _object(
        {
            "source": {"type": "string"},
            "account_id": {"type": "string"},
            "chat_id": {"type": "integer"},
            "message_id": {"type": "integer"},
            "file_index": {"type": "integer"},
            "file_path": {"type": "string"},
            "media_kind": {"type": "string", "nullable": True},
            "mime_type": {"type": "string", "nullable": True},
            "file_size": {"type": "integer", "nullable": True},
            "downloaded_at": {"type": "string", "nullable": True},
            "chat_title": {"type": "string", "nullable": True},
            "origin_title": {"type": "string", "nullable": True},
            "content_type": {"type": "string"},
            "preview_kind": {"type": "string"},
            "access_url": {"type": "string"},
            "download_url": {"type": "string"},
            "raw_json": {"type": "object", "nullable": True},
        },
        required=["source", "account_id", "chat_id", "message_id", "file_index"],
    ),
    "DailySummaryDelivery": _object(
        {
            "enabled": {"type": "boolean"},
            "account_id": {"type": "string", "nullable": True},
            "origin_id": {"type": "integer", "nullable": True},
            "topic_id": {"type": "integer", "default": 0},
            "source": {"type": "string", "enum": ["config", "database"]},
            "updated_at": {"type": "string", "nullable": True},
        },
        required=["enabled", "account_id", "origin_id", "topic_id", "source"],
    ),
    "DailySummaryDeliveryInput": _object(
        {
            "enabled": {"type": "boolean", "default": False},
            "account_id": {"type": "string", "nullable": True},
            "origin_id": {"type": "integer", "nullable": True},
            "topic_id": {"type": "integer", "default": 0},
        },
        additional_properties=False,
    ),
    "DailyPackageSchedule": _object(
        {
            "enabled": {"type": "boolean"},
            "time_of_day": {"type": "string"},
            "timezone": {"type": "string"},
            "scope": {"type": "object"},
            "delivery": {"$ref": "#/components/schemas/DailySummaryDelivery"},
            "system_manager": {"type": "string"},
            "installed": {"type": "boolean"},
            "last_installed_at": {"type": "string", "nullable": True},
            "last_error": {"type": "string", "nullable": True},
            "updated_at": {"type": "string", "nullable": True},
        },
        required=["enabled", "time_of_day", "timezone", "scope", "system_manager", "installed"],
    ),
    "DailyPackageScheduleInput": _object(
        {
            "enabled": {"type": "boolean", "default": False},
            "time_of_day": {"type": "string", "default": "08:00"},
            "timezone": {"type": "string", "default": "Asia/Tokyo"},
            "scope": {"type": "object"},
            "delivery": {"$ref": "#/components/schemas/DailySummaryDeliveryInput"},
            "system_manager": {"type": "string", "default": "systemd-user"},
            "activate_systemd": {"type": "boolean", "default": False},
        },
        additional_properties=False,
    ),
    "DailyPackageRunInput": _object(
        {
            "date": {"type": "string"},
            "timezone": {"type": "string", "default": "Asia/Tokyo"},
            "scope": {"type": "object"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "topic_id": {"type": "integer"},
            "tags": {"type": "string"},
            "tag_groups": {"type": "array", "items": {"type": "string"}},
        },
    ),
    "DailyPackageRun": _object(
        {
            "run_id": {"type": "string"},
            "status": {"type": "string"},
            "date": {"type": "string"},
            "timezone": {"type": "string"},
            "scope": {"type": "object"},
            "output_dir": {"type": "string", "nullable": True},
            "package_json_path": {"type": "string", "nullable": True},
            "package_md_path": {"type": "string", "nullable": True},
            "origin_count": {"type": "integer"},
            "message_count": {"type": "integer"},
            "media_count": {"type": "integer"},
            "important_origin_count": {"type": "integer"},
            "progress_total": {"type": "integer"},
            "progress_current": {"type": "integer"},
            "progress_label": {"type": "string", "nullable": True},
            "progress": {"type": "object"},
            "error": {"type": "string", "nullable": True},
            "started_at": {"type": "string", "nullable": True},
            "finished_at": {"type": "string", "nullable": True},
        },
        required=["run_id", "status", "date", "timezone"],
    ),
    "DailySummaryRunInput": _object(
        {
            "package_run_id": {"type": "string"},
            "date": {"type": "string"},
            "timezone": {"type": "string", "default": "Asia/Tokyo"},
            "scope": {"type": "object"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "topic_id": {"type": "integer"},
            "tags": {"type": "string"},
            "tag_groups": {"type": "array", "items": {"type": "string"}},
            "background": {"type": "boolean", "default": True},
            "force": {"type": "boolean", "default": False},
        },
    ),
    "DailySummaryRun": _object(
        {
            "run_id": {"type": "string"},
            "status": {"type": "string"},
            "package_run_id": {"type": "string", "nullable": True},
            "date": {"type": "string", "nullable": True},
            "timezone": {"type": "string", "nullable": True},
            "scope": {"type": "object"},
            "output_dir": {"type": "string", "nullable": True},
            "summary_path": {"type": "string", "nullable": True},
            "provider": {"type": "string", "nullable": True},
            "origin_count": {"type": "integer"},
            "group_count": {"type": "integer"},
            "image_count": {"type": "integer"},
            "progress_total": {"type": "integer"},
            "progress_current": {"type": "integer"},
            "progress_label": {"type": "string", "nullable": True},
            "progress": {"type": "object"},
            "error": {"type": "string", "nullable": True},
            "started_at": {"type": "string", "nullable": True},
            "finished_at": {"type": "string", "nullable": True},
        },
        required=["run_id", "status"],
    ),
    "DailySummaryJob": _object(
        {
            "job_id": {"type": "string"},
            "status": {"type": "string"},
            "date": {"type": "string", "nullable": True},
            "timezone": {"type": "string", "nullable": True},
            "scope": {"type": "object"},
            "package_run_id": {"type": "string", "nullable": True},
            "summary_run_id": {"type": "string", "nullable": True},
            "provider": {"type": "string", "nullable": True},
            "progress_total": {"type": "integer"},
            "progress_current": {"type": "integer"},
            "progress_label": {"type": "string", "nullable": True},
            "progress": {"type": "object"},
            "request": {"type": "object"},
            "dedupe_key": {"type": "string", "nullable": True},
            "worker_id": {"type": "string", "nullable": True},
            "lease_until": {"type": "string", "nullable": True},
            "heartbeat_at": {"type": "string", "nullable": True},
            "attempt": {"type": "integer"},
            "retry_at": {"type": "string", "nullable": True},
            "retry_count": {"type": "integer"},
            "cancel_requested_at": {"type": "string", "nullable": True},
            "error": {"type": "string", "nullable": True},
            "started_at": {"type": "string", "nullable": True},
            "finished_at": {"type": "string", "nullable": True},
            "updated_at": {"type": "string", "nullable": True},
        },
        required=["job_id", "status"],
    ),
    "DailySummaryJobCancelInput": _object(
        {
            "job_id": {"type": "string"},
        },
        required=["job_id"],
    ),
    "DailySummaryRecord": _object(
        {
            "summary_id": {"type": "string"},
            "run_id": {"type": "string"},
            "package_run_id": {"type": "string", "nullable": True},
            "date": {"type": "string", "nullable": True},
            "timezone": {"type": "string", "nullable": True},
            "scope": {"type": "object"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "tags_csv": {"type": "string", "nullable": True},
            "important": {"type": "boolean"},
            "record_type": {"type": "string"},
            "provider": {"type": "string", "nullable": True},
            "title": {"type": "string", "nullable": True},
            "content_preview": {"type": "string"},
            "content_md": {"type": "string"},
            "content_json": {"type": "object", "nullable": True},
            "summary_path": {"type": "string", "nullable": True},
            "origin_count": {"type": "integer"},
            "group_count": {"type": "integer"},
            "image_count": {"type": "integer"},
            "content_length": {"type": "integer"},
            "deleted": {"type": "boolean"},
            "deleted_at": {"type": "string", "nullable": True},
            "created_at": {"type": "string"},
            "updated_at": {"type": "string"},
        },
        required=["summary_id", "run_id", "record_type", "content_preview"],
    ),
    "DailyMessagePoint": _object(
        {
            "point_id": {"type": "string"},
            "run_id": {"type": "string"},
            "package_run_id": {"type": "string"},
            "date": {"type": "string"},
            "timezone": {"type": "string"},
            "source": {"type": "string"},
            "account_id": {"type": "string"},
            "origin_id": {"type": "integer"},
            "topic_id": {"type": "integer"},
            "origin_title": {"type": "string", "nullable": True},
            "message_id": {"type": "integer", "nullable": True},
            "occurred_at": {"type": "string"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "tags_csv": {"type": "string", "nullable": True},
            "content": {"type": "string"},
            "telegram_deeplink": {"type": "string", "nullable": True},
            "permalink": {"type": "string", "nullable": True},
            "importance_score": {"type": "integer", "minimum": 1, "maximum": 5},
            "importance_reason": {"type": "string", "nullable": True},
            "origin_important": {"type": "boolean"},
            "source_refs": {
                "type": "array",
                "items": {"oneOf": [{"type": "string"}, {"type": "object"}]},
            },
            "provider": {"type": "string", "nullable": True},
            "run_status": {"type": "string", "nullable": True},
            "job_status": {"type": "string", "nullable": True},
            "created_at": {"type": "string"},
            "updated_at": {"type": "string"},
        },
        required=[
            "point_id",
            "run_id",
            "package_run_id",
            "date",
            "timezone",
            "source",
            "account_id",
            "origin_id",
            "topic_id",
            "occurred_at",
            "tags",
            "content",
            "importance_score",
            "origin_important",
            "source_refs",
        ],
    ),
    "DailySummaryRecordDeleteInput": _object(
        {
            "summary_id": {"type": "string"},
            "summary_ids": {"type": "array", "items": {"type": "string"}},
            "ids": {"type": "array", "items": {"type": "string"}},
            "deleted": {"type": "boolean", "default": True},
        },
    ),
    "DailySummaryRecordDeleteResponse": _item_response("DailySummaryRecordDeleteResult"),
    "DailySummaryRecordDeleteResult": _object(
        {
            "summary_ids": {"type": "array", "items": {"type": "string"}},
            "deleted": {"type": "boolean"},
            "changed_rows": {"type": "integer"},
        },
        required=["summary_ids", "deleted", "changed_rows"],
    ),
    "AccountListResponse": _items_response("Account"),
    "OriginListResponse": _items_response("Origin"),
    "BackupPolicyListResponse": _items_response("BackupPolicy"),
    "ParticipantListResponse": _items_response("Participant"),
    "CaptureCursorListResponse": _items_response("CaptureCursor"),
    "OperationEventListResponse": _items_response("OperationEvent"),
    "DailySummaryDeliveryResponse": _item_response("DailySummaryDelivery"),
    "DailyPackageScheduleResponse": _item_response("DailyPackageSchedule"),
    "DailyPackageRunListResponse": _items_response("DailyPackageRun"),
    "DailyPackageRunResponse": _item_response("DailyPackageRun"),
    "DailySummaryRunListResponse": _items_response("DailySummaryRun"),
    "DailySummaryRunResponse": _item_response("DailySummaryRun"),
    "DailySummarySubmitResponse": _object(
        {
            "item": {
                "oneOf": [
                    {"$ref": "#/components/schemas/DailySummaryRun"},
                    {"$ref": "#/components/schemas/DailySummaryJob"},
                ]
            }
        },
        required=["item"],
    ),
    "DailySummaryJobListResponse": _items_response("DailySummaryJob"),
    "DailySummaryJobResponse": _item_response("DailySummaryJob"),
    "DailySummaryRecordListResponse": _items_response("DailySummaryRecord"),
    "DailySummaryRecordResponse": _item_response("DailySummaryRecord"),
    "DailyMessagePointListResponse": _items_response("DailyMessagePoint"),
    "DailyMessagePointResponse": _item_response("DailyMessagePoint"),
    "ChatListResponse": _items_response("Chat"),
    "MediaFileListResponse": _items_response("MediaFile"),
    "EventPage": _page_response("Event"),
    "MessagePage": _page_response("Message"),
    "MessageListResponse": _items_response("Message"),
    "AccountItemResponse": _item_response("Account"),
    "OriginItemResponse": _item_response("Origin"),
    "BackupPolicyItemResponse": _item_response("BackupPolicy"),
    "ParticipantItemResponse": _item_response("Participant"),
    "AuthResultResponse": _item_response("AuthResult"),
    "DiscoveryResultResponse": _item_response("DiscoveryResult"),
    "ParticipantRefreshResultResponse": _item_response("ParticipantRefreshResult"),
    "OperationEventDeleteResponse": _object(
        {"item": {"$ref": "#/components/schemas/OperationEventDeleteResult"}},
        required=["item"],
    ),
    "AccountDeleteResponse": _object(
        {
            "item": _delete_response(
                {
                    "source": {"type": "string"},
                    "account_id": {"type": "string"},
                }
            )
        },
        required=["item"],
    ),
    "OriginDeleteResponse": _object(
        {
            "item": _delete_response(
                {
                    "source": {"type": "string"},
                    "account_id": {"type": "string"},
                    "origin_id": {"type": "integer"},
                    "topic_id": {"type": "integer"},
                }
            )
        },
        required=["item"],
    ),
    "OriginArchiveResponse": _object(
        {
            "item": _object(
                {
                    "source": {"type": "string"},
                    "account_id": {"type": "string"},
                    "origin_id": {"type": "integer"},
                    "topic_id": {"type": "integer"},
                    "archived": {"type": "boolean"},
                    "changed_rows": {"type": "integer"},
                }
            )
        },
        required=["item"],
    ),
    "BackupPolicyDeleteResponse": _object(
        {
            "item": _delete_response(
                {
                    "source": {"type": "string"},
                    "account_id": {"type": "string"},
                    "origin_id": {"type": "integer"},
                    "topic_id": {"type": "integer"},
                }
            )
        },
        required=["item"],
    ),
    "ParticipantDeleteResponse": _object(
        {
            "item": _delete_response(
                {
                    "source": {"type": "string"},
                    "account_id": {"type": "string"},
                    "origin_id": {"type": "integer"},
                    "user_id": {"type": "integer"},
                }
            )
        },
        required=["item"],
    ),
}


COMMON_ACCOUNT_PARAM = ApiParam("account_id", description="Local Telegram account ID.")
COMMON_LIMIT_PARAM = ApiParam("limit", "integer", default=500, description="Maximum rows to return.")


API_ENDPOINTS: tuple[ApiEndpoint, ...] = (
    ApiEndpoint("GET", "/", "console", "Serve the built-in management console.", auth=False, response_content_type="text/html"),
    ApiEndpoint("GET", "/console", "console", "Serve the built-in management console.", auth=False, response_content_type="text/html"),
    ApiEndpoint("GET", OPENAPI_PATH, "docs", "Return the current OpenAPI document.", auth=False, response_schema=None),
    ApiEndpoint("GET", MARKDOWN_API_DOC_PATH, "docs", "Return the generated Markdown API reference.", auth=False, response_content_type="text/markdown"),
    ApiEndpoint("GET", "/healthz", "sync", "Return health and archive state.", response_schema="HealthResponse"),
    ApiEndpoint("GET", "/sync/state", "sync", "Return archive sync state.", response_schema="StateResponse"),
    ApiEndpoint(
        "GET",
        "/sync/events",
        "sync",
        "Return raw event rows after a cursor.",
        query=(
            ApiParam("after", "integer", default=0, description="Last consumed event sequence."),
            COMMON_LIMIT_PARAM,
        ),
        response_schema="EventPage",
    ),
    ApiEndpoint(
        "GET",
        "/sync/messages",
        "sync",
        "Return message rows after a cursor or the latest messages.",
        query=(
            ApiParam("after", "integer", default=0, description="Last consumed event sequence."),
            COMMON_LIMIT_PARAM,
            ApiParam("latest", "boolean", default=False, description="Return latest messages instead of cursor page."),
            ApiParam("include_media", "boolean", default=False, description="Attach media_files to each message."),
        ),
        response_schema="MessagePage",
    ),
    ApiEndpoint("GET", "/sync/accounts", "sync", "Return archive account metadata.", response_schema="AccountListResponse"),
    ApiEndpoint("GET", "/sync/chats", "sync", "Return archived chat metadata.", response_schema="ChatListResponse"),
    ApiEndpoint(
        "GET",
        "/sync/search",
        "sync",
        "Search archived message text.",
        query=(
            ApiParam("q", description="Search text."),
            ApiParam("limit", "integer", default=50, description="Maximum messages to return."),
            ApiParam("include_media", "boolean", default=False, description="Attach media_files to each message."),
        ),
        response_schema="MessageListResponse",
    ),
    ApiEndpoint(
        "GET",
        "/sync/media-files",
        "sync",
        "Return downloaded media file records.",
        query=(
            COMMON_ACCOUNT_PARAM,
            ApiParam("chat_id", "integer", description="Filter to a Telegram chat or origin ID."),
            ApiParam("message_id", "integer", description="Filter to a message ID."),
            COMMON_LIMIT_PARAM,
        ),
        response_schema="MediaFileListResponse",
    ),
    ApiEndpoint(
        "GET",
        "/sync/media-files/content",
        "sync",
        "Return the binary contents of a registered media file.",
        query=(
            ApiParam("source", default="telegram", description="Source system."),
            ApiParam("account_id", required=True, description="Local Telegram account ID."),
            ApiParam("chat_id", "integer", required=True, description="Telegram chat or origin ID."),
            ApiParam("message_id", "integer", required=True, description="Telegram message ID."),
            ApiParam("file_index", "integer", default=0, description="Media index on the message."),
        ),
        response_content_type="application/octet-stream",
    ),
    ApiEndpoint("GET", "/manage/capabilities", "management", "Return supported management capabilities.", response_schema="CapabilitiesResponse"),
    ApiEndpoint("GET", API_MANIFEST_PATH, "management", "Return the machine-readable API contract manifest.", response_schema="ApiManifest"),
    ApiEndpoint("GET", "/manage/accounts", "management", "List management account state.", response_schema="AccountListResponse"),
    ApiEndpoint("POST", "/manage/accounts", "management", "Create or update account metadata.", body_schema="AccountInput", response_schema="AccountItemResponse", status=201),
    ApiEndpoint("DELETE", "/manage/accounts", "management", "Delete management account metadata.", body_schema="AuthStatusInput", response_schema="AccountDeleteResponse"),
    ApiEndpoint("POST", "/manage/accounts/auth", "management", "Create or update account auth state.", body_schema="AccountAuthInput", response_schema="AccountItemResponse"),
    ApiEndpoint("PATCH", "/manage/accounts/auth", "management", "Patch account auth state.", body_schema="AccountAuthInput", response_schema="AccountItemResponse"),
    ApiEndpoint("POST", "/manage/accounts/auth/status", "management", "Check live Telegram auth status.", body_schema="AuthStatusInput", response_schema="AuthResultResponse"),
    ApiEndpoint("POST", "/manage/accounts/auth/request-code", "management", "Request a Telegram login code.", body_schema="RequestCodeInput", response_schema="AuthResultResponse"),
    ApiEndpoint("POST", "/manage/accounts/auth/submit-code", "management", "Submit a Telegram login code and optional 2FA password.", body_schema="SubmitCodeInput", response_schema="AuthResultResponse"),
    ApiEndpoint(
        "GET",
        "/manage/origins",
        "management",
        "List known origins and topics.",
        query=(
            COMMON_ACCOUNT_PARAM,
            ApiParam("include_archived", "boolean", default=False, description="Include removed origins."),
        ),
        response_schema="OriginListResponse",
    ),
    ApiEndpoint("POST", "/manage/origins", "management", "Create or update origin metadata.", body_schema="OriginInput", response_schema="OriginItemResponse", status=201),
    ApiEndpoint("DELETE", "/manage/origins", "management", "Delete an origin and related management metadata.", body_schema="OriginArchiveInput", response_schema="OriginDeleteResponse"),
    ApiEndpoint("PATCH", "/manage/origins/archive", "management", "Archive or restore an origin.", body_schema="OriginArchiveInput", response_schema="OriginArchiveResponse"),
    ApiEndpoint("PATCH", "/manage/origins/important", "management", "Mark or unmark an origin as important.", body_schema="OriginImportantInput", response_schema="OriginItemResponse"),
    ApiEndpoint(
        "GET",
        "/manage/backup-policies",
        "management",
        "List origin backup policies.",
        query=(COMMON_ACCOUNT_PARAM,),
        response_schema="BackupPolicyListResponse",
    ),
    ApiEndpoint("POST", "/manage/backup-policies", "management", "Create or update an origin backup policy.", body_schema="BackupPolicyInput", response_schema="BackupPolicyItemResponse"),
    ApiEndpoint("PATCH", "/manage/backup-policies", "management", "Patch an origin backup policy.", body_schema="BackupPolicyInput", response_schema="BackupPolicyItemResponse"),
    ApiEndpoint("DELETE", "/manage/backup-policies", "management", "Delete an origin backup policy.", body_schema="BackupPolicyInput", response_schema="BackupPolicyDeleteResponse"),
    ApiEndpoint(
        "GET",
        "/manage/participants",
        "management",
        "List participant profiles.",
        query=(
            COMMON_ACCOUNT_PARAM,
            ApiParam("origin_id", "integer", description="Filter to an origin."),
        ),
        response_schema="ParticipantListResponse",
    ),
    ApiEndpoint("POST", "/manage/participants", "management", "Create or update a participant profile.", body_schema="ParticipantInput", response_schema="ParticipantItemResponse", status=201),
    ApiEndpoint("DELETE", "/manage/participants", "management", "Delete a participant profile.", body_schema="ParticipantInput", response_schema="ParticipantDeleteResponse"),
    ApiEndpoint(
        "GET",
        "/manage/capture-cursors",
        "management",
        "List capture cursors.",
        query=(COMMON_ACCOUNT_PARAM,),
        response_schema="CaptureCursorListResponse",
    ),
    ApiEndpoint(
        "GET",
        "/manage/operation-events",
        "management",
        "List structured operation events.",
        query=(
            COMMON_ACCOUNT_PARAM,
            ApiParam("status", description="Filter by status, for example failed."),
            ApiParam("limit", "integer", default=100, description="Maximum events to return."),
        ),
        response_schema="OperationEventListResponse",
    ),
    ApiEndpoint("DELETE", "/manage/operation-events", "management", "Delete one or more operation events.", body_schema="OperationEventDeleteInput", response_schema="OperationEventDeleteResponse"),
    ApiEndpoint("GET", "/manage/daily-package-schedule", "management", "Return the daily package system schedule.", response_schema="DailyPackageScheduleResponse"),
    ApiEndpoint("PATCH", "/manage/daily-package-schedule", "management", "Update the daily package system schedule.", body_schema="DailyPackageScheduleInput", response_schema="DailyPackageScheduleResponse"),
    ApiEndpoint("GET", "/manage/daily-summary-delivery", "management", "Return the effective daily summary Telegram delivery target.", response_schema="DailySummaryDeliveryResponse"),
    ApiEndpoint("PATCH", "/manage/daily-summary-delivery", "management", "Persist the daily summary Telegram delivery target.", body_schema="DailySummaryDeliveryInput", response_schema="DailySummaryDeliveryResponse"),
    ApiEndpoint("POST", "/manage/daily-packages", "management", "Generate a daily package immediately.", body_schema="DailyPackageRunInput", response_schema="DailyPackageRunResponse", status=201),
    ApiEndpoint(
        "GET",
        "/manage/daily-package-runs",
        "management",
        "List daily package runs.",
        query=(
            ApiParam("status", description="Filter by run status."),
            COMMON_LIMIT_PARAM,
        ),
        response_schema="DailyPackageRunListResponse",
    ),
    ApiEndpoint(
        "GET",
        "/manage/daily-package-runs/content",
        "management",
        "Return daily package run content.",
        query=(
            ApiParam("run_id", required=True, description="Daily package run ID."),
            ApiParam("format", default="md", description="Content format: md or json."),
        ),
        response_schema=None,
        response_content_type="text/markdown",
    ),
    ApiEndpoint(
        "POST",
        "/manage/daily-summaries",
        "management",
        "Run or enqueue a daily summary.",
        body_schema="DailySummaryRunInput",
        response_schema="DailySummarySubmitResponse",
        status=201,
        notes="Returns a daily summary job when background=true, otherwise waits and returns the resulting summary run.",
    ),
    ApiEndpoint(
        "POST",
        "/manage/daily-summary-jobs",
        "management",
        "Start a background daily package and summary job.",
        body_schema="DailySummaryRunInput",
        response_schema="DailySummaryJobResponse",
        status=201,
    ),
    ApiEndpoint(
        "GET",
        "/manage/daily-summary-jobs",
        "management",
        "List background daily package and summary jobs.",
        query=(
            ApiParam("job_id", description="Filter by job ID."),
            ApiParam("status", description="Filter by job status."),
            COMMON_LIMIT_PARAM,
        ),
        response_schema="DailySummaryJobListResponse",
    ),
    ApiEndpoint(
        "PATCH",
        "/manage/daily-summary-jobs/cancel",
        "management",
        "Request cancellation of a running daily summary job.",
        body_schema="DailySummaryJobCancelInput",
        response_schema="DailySummaryJobResponse",
    ),
    ApiEndpoint(
        "GET",
        "/manage/daily-summary-runs",
        "management",
        "List daily summary runs.",
        query=(
            ApiParam("package_run_id", description="Filter by package run ID."),
            ApiParam("status", description="Filter by run status."),
            COMMON_LIMIT_PARAM,
        ),
        response_schema="DailySummaryRunListResponse",
    ),
    ApiEndpoint(
        "GET",
        "/manage/daily-summary-runs/content",
        "management",
        "Return daily summary run content.",
        query=(ApiParam("run_id", required=True, description="Daily summary run ID."),),
        response_schema=None,
        response_content_type="text/markdown",
    ),
    ApiEndpoint(
        "GET",
        "/manage/daily-summary-records",
        "management",
        "List stored daily summary contents.",
        query=(
            ApiParam("summary_id", description="Filter by summary content ID."),
            ApiParam("run_id", description="Filter by summary run ID."),
            ApiParam("package_run_id", description="Filter by package run ID."),
            ApiParam("date", description="Filter by local summary date."),
            ApiParam("date_from", description="Filter summaries on or after this local date."),
            ApiParam("date_to", description="Filter summaries on or before this local date."),
            ApiParam("provider", description="Filter by AI provider."),
            ApiParam("record_type", description="Filter by stored artifact type, such as important_daily or point_daily."),
            ApiParam("important", type="boolean", description="Filter summaries that include important origins."),
            ApiParam("tag", description="Required tag. Repeatable; all tags must match."),
            ApiParam("tags", description="Comma-separated required tags; all tags must match."),
            ApiParam("q", description="Filter by title or Markdown content substring."),
            ApiParam("include_deleted", type="boolean", default=False, description="Include soft-deleted summaries."),
            ApiParam("deleted", type="boolean", description="Filter by soft-deleted state."),
            ApiParam("include_content", type="boolean", default=False, description="Include full Markdown content in list items."),
            COMMON_LIMIT_PARAM,
        ),
        response_schema="DailySummaryRecordListResponse",
    ),
    ApiEndpoint(
        "GET",
        "/manage/daily-summary-records/item",
        "management",
        "Return one stored daily summary content record.",
        query=(
            ApiParam("summary_id", description="Summary content ID."),
            ApiParam("run_id", description="Summary run ID."),
            ApiParam("record_type", description="Stored artifact type when a run has multiple records."),
            ApiParam("include_deleted", type="boolean", default=False, description="Allow returning a soft-deleted record."),
        ),
        response_schema="DailySummaryRecordResponse",
    ),
    ApiEndpoint(
        "PATCH",
        "/manage/daily-summary-records",
        "management",
        "Soft-delete or restore one or more stored daily summary records.",
        body_schema="DailySummaryRecordDeleteInput",
        response_schema="DailySummaryRecordDeleteResponse",
    ),
    ApiEndpoint(
        "DELETE",
        "/manage/daily-summary-records",
        "management",
        "Soft-delete one or more stored daily summary records.",
        body_schema="DailySummaryRecordDeleteInput",
        response_schema="DailySummaryRecordDeleteResponse",
    ),
    ApiEndpoint(
        "GET",
        "/manage/daily-message-points",
        "management",
        "List stored daily message points.",
        query=(
            ApiParam("point_id", description="Filter by message point ID."),
            ApiParam("run_id", description="Filter by summary run ID."),
            ApiParam("package_run_id", description="Filter by package run ID."),
            ApiParam("date", description="Filter by local package date."),
            ApiParam("date_from", description="Filter points on or after this local date."),
            ApiParam("date_to", description="Filter points on or before this local date."),
            ApiParam("source", description="Filter by source system."),
            COMMON_ACCOUNT_PARAM,
            ApiParam("origin_id", "integer", description="Filter by Telegram origin ID."),
            ApiParam("topic_id", "integer", description="Filter by Telegram topic ID."),
            ApiParam("message_id", "integer", description="Filter by primary source message ID."),
            ApiParam("tag", description="Required tag. Repeatable; all tags must match."),
            ApiParam("tags", description="Comma-separated required tags; all tags must match."),
            ApiParam("importance_min", "integer", description="Minimum importance score from 1 to 5."),
            ApiParam("importance_max", "integer", description="Maximum importance score from 1 to 5."),
            ApiParam("origin_important", "boolean", description="Filter by the origin's important flag."),
            ApiParam("q", description="Filter by point content, origin title, or importance reason substring."),
            ApiParam(
                "include_incomplete",
                "boolean",
                default=False,
                description="Include points from running, failed, or canceled summary runs.",
            ),
            ApiParam("limit", "integer", default=100, description="Maximum message points to return."),
        ),
        response_schema="DailyMessagePointListResponse",
    ),
    ApiEndpoint(
        "GET",
        "/manage/daily-message-points/item",
        "management",
        "Return one stored daily message point.",
        query=(ApiParam("point_id", required=True, description="Message point ID."),),
        response_schema="DailyMessagePointResponse",
    ),
    ApiEndpoint("POST", "/manage/discover-origins", "management", "Discover Telegram dialogs and topics for an authenticated account.", body_schema="DiscoveryInput", response_schema="DiscoveryResultResponse"),
    ApiEndpoint("POST", "/manage/participants/refresh", "management", "Refresh participants for a Telegram origin.", body_schema="ParticipantRefreshInput", response_schema="ParticipantRefreshResultResponse"),
)


def _contract_payload() -> dict[str, Any]:
    return {
        "version": API_CONTRACT_VERSION,
        "endpoints": [asdict(endpoint) for endpoint in API_ENDPOINTS],
        "schemas": SCHEMAS,
    }


def _contract_hash() -> str:
    body = json.dumps(_contract_payload(), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]


API_CONTRACT_HASH = _contract_hash()


def validate_request_payload(endpoint: ApiEndpoint, payload: dict[str, Any]) -> None:
    if endpoint.body_schema is None:
        return
    schema = SCHEMAS.get(endpoint.body_schema)
    if schema is None:
        raise RuntimeError(f"Unknown request schema: {endpoint.body_schema}")
    _validate_schema_value(payload, schema, "body")


def validate_query_params(endpoint: ApiEndpoint, params: dict[str, list[str]]) -> None:
    for item in endpoint.query:
        values = params.get(item.name)
        if item.required and (not values or not values[0]):
            raise ValueError(f"Missing required query parameter: {item.name}")
        if not values:
            continue
        for value in values:
            if item.type == "integer":
                try:
                    int(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"Query parameter {item.name} must be an integer") from exc
            elif item.type == "boolean" and value.strip().lower() not in {
                "1",
                "0",
                "true",
                "false",
                "yes",
                "no",
                "on",
                "off",
            }:
                raise ValueError(f"Query parameter {item.name} must be a boolean")


def _validate_schema_value(value: Any, schema: dict[str, Any], path: str) -> None:
    if value is None and schema.get("nullable"):
        return
    reference = schema.get("$ref")
    if reference:
        name = str(reference).rsplit("/", 1)[-1]
        target = SCHEMAS.get(name)
        if target is None:
            raise RuntimeError(f"Unknown schema reference: {reference}")
        _validate_schema_value(value, target, path)
        return
    value_type = schema.get("type")
    if value_type == "object":
        if not isinstance(value, dict):
            raise ValueError(f"{path} must be an object")
        for key in schema.get("required") or []:
            if key not in value or value[key] is None or value[key] == "":
                raise ValueError(f"Missing required field: {path}.{key}")
        properties = schema.get("properties") or {}
        if schema.get("additionalProperties") is False:
            unknown = sorted(set(value) - set(properties))
            if unknown:
                raise ValueError(f"Unknown field: {path}.{unknown[0]}")
        for key, item in value.items():
            if key in properties:
                _validate_schema_value(item, properties[key], f"{path}.{key}")
    elif value_type == "array":
        if not isinstance(value, list):
            raise ValueError(f"{path} must be an array")
        item_schema = schema.get("items")
        if item_schema:
            for index, item in enumerate(value):
                _validate_schema_value(item, item_schema, f"{path}[{index}]")
    elif value_type == "string":
        if not isinstance(value, str):
            raise ValueError(f"{path} must be a string")
    elif value_type == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{path} must be an integer")
    elif value_type == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{path} must be a number")
    elif value_type == "boolean":
        if not isinstance(value, bool):
            raise ValueError(f"{path} must be a boolean")
    if "enum" in schema and value not in schema["enum"]:
        raise ValueError(f"{path} must be one of {schema['enum']}")


def api_manifest() -> dict[str, Any]:
    return {
        "name": "tele-mess-core",
        "contract_version": API_CONTRACT_VERSION,
        "contract_hash": API_CONTRACT_HASH,
        "openapi_url": OPENAPI_PATH,
        "markdown_url": MARKDOWN_API_DOC_PATH,
        "agent_doc": "AGENT.md",
        "endpoints": [_endpoint_manifest(endpoint) for endpoint in API_ENDPOINTS],
        "schemas": SCHEMAS,
    }


def _endpoint_manifest(endpoint: ApiEndpoint) -> dict[str, Any]:
    return {
        "method": endpoint.method,
        "path": endpoint.path,
        "tag": endpoint.tag,
        "summary": endpoint.summary,
        "auth": endpoint.auth,
        "query": [asdict(item) for item in endpoint.query],
        "body_schema": endpoint.body_schema,
        "response_schema": endpoint.response_schema,
        "response_content_type": endpoint.response_content_type,
        "status": endpoint.status,
        "operation_id": endpoint.id(),
        "notes": endpoint.notes,
    }


def openapi_document() -> dict[str, Any]:
    paths: dict[str, Any] = {}
    for endpoint in API_ENDPOINTS:
        path_item = paths.setdefault(endpoint.path, {})
        path_item[endpoint.method.lower()] = _openapi_operation(endpoint)
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "tele-mess-core API",
            "version": API_CONTRACT_VERSION,
            "x-contract-hash": API_CONTRACT_HASH,
        },
        "servers": [{"url": "http://127.0.0.1:8765"}],
        "paths": paths,
        "components": {
            "securitySchemes": {
                "BearerAuth": {"type": "http", "scheme": "bearer"},
                "ApiTokenAuth": {"type": "apiKey", "in": "header", "name": "X-Api-Token"},
            },
            "schemas": SCHEMAS,
        },
    }


def _openapi_operation(endpoint: ApiEndpoint) -> dict[str, Any]:
    operation: dict[str, Any] = {
        "operationId": endpoint.id(),
        "summary": endpoint.summary,
        "tags": [endpoint.tag],
        "responses": _openapi_responses(endpoint),
    }
    if endpoint.auth:
        operation["security"] = [{"BearerAuth": []}, {"ApiTokenAuth": []}]
    if endpoint.query:
        operation["parameters"] = [_openapi_param(param) for param in endpoint.query]
    if endpoint.body_schema:
        operation["requestBody"] = {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {"$ref": f"#/components/schemas/{endpoint.body_schema}"}
                }
            },
        }
    if endpoint.notes:
        operation["description"] = endpoint.notes
    return operation


def _openapi_param(param: ApiParam) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": param.type}
    if param.default is not None:
        schema["default"] = param.default
    return {
        "name": param.name,
        "in": "query",
        "required": param.required,
        "description": param.description,
        "schema": schema,
    }


def _openapi_responses(endpoint: ApiEndpoint) -> dict[str, Any]:
    responses: dict[str, Any] = {
        str(endpoint.status): {
            "description": "OK",
        },
        "400": {
            "description": "Bad request",
            "content": {
                "application/json": {"schema": {"$ref": "#/components/schemas/ErrorResponse"}}
            },
        },
        "500": {
            "description": "Internal error",
            "content": {
                "application/json": {"schema": {"$ref": "#/components/schemas/ErrorResponse"}}
            },
        },
    }
    success = responses[str(endpoint.status)]
    if endpoint.response_schema:
        success["content"] = {
            endpoint.response_content_type: {
                "schema": {"$ref": f"#/components/schemas/{endpoint.response_schema}"}
            }
        }
    elif endpoint.response_content_type == "application/json":
        success["content"] = {
            "application/json": {"schema": {"type": "object"}}
        }
    else:
        success["content"] = {
            endpoint.response_content_type: {
                "schema": {"type": "string", "format": "binary"}
            }
        }
    if endpoint.auth:
        responses["401"] = {
            "description": "Unauthorized",
            "content": {
                "application/json": {"schema": {"$ref": "#/components/schemas/ErrorResponse"}}
            },
        }
    return responses


def markdown_document() -> str:
    lines = [
        "# tele-mess-core API",
        "",
        "This file is generated from `tele_mess_core.server.contracts`.",
        "",
        f"- Contract version: `{API_CONTRACT_VERSION}`",
        f"- Contract hash: `{API_CONTRACT_HASH}`",
        f"- Runtime manifest: `{API_MANIFEST_PATH}`",
        f"- OpenAPI: `{OPENAPI_PATH}`",
        "",
        "## Authentication",
        "",
        "Token-protected endpoints accept either `Authorization: Bearer <token>` or `X-Api-Token: <token>`.",
        "The built-in console and generated documentation endpoints are public on the local server.",
        "",
        "## Endpoint Index",
        "",
    ]
    for endpoint in API_ENDPOINTS:
        auth = "token" if endpoint.auth else "public"
        lines.append(f"- `{endpoint.method} {endpoint.path}` ({endpoint.tag}, {auth}) - {endpoint.summary}")
    lines.extend(["", "## Endpoints", ""])
    for endpoint in API_ENDPOINTS:
        lines.extend(_markdown_endpoint(endpoint))
    lines.extend(["## Schemas", ""])
    for name in sorted(SCHEMAS):
        schema = SCHEMAS[name]
        lines.append(f"### {name}")
        if schema.get("description"):
            lines.extend(["", str(schema["description"])])
        properties = schema.get("properties", {})
        if properties:
            lines.extend(["", "| Field | Type | Required |", "| --- | --- | --- |"])
            required = set(schema.get("required", []))
            for field_name, field_schema in properties.items():
                lines.append(f"| `{field_name}` | `{_schema_type(field_schema)}` | {'yes' if field_name in required else 'no'} |")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _markdown_endpoint(endpoint: ApiEndpoint) -> list[str]:
    lines = [
        f"### {endpoint.method} {endpoint.path}",
        "",
        endpoint.summary,
        "",
        f"- Tag: `{endpoint.tag}`",
        f"- Auth: `{'required' if endpoint.auth else 'public'}`",
        f"- Success: `{endpoint.status}`",
    ]
    if endpoint.query:
        lines.extend(["", "Query parameters:", ""])
        for param in endpoint.query:
            default = f", default `{param.default}`" if param.default is not None else ""
            required = "required" if param.required else "optional"
            description = f" - {param.description}" if param.description else ""
            lines.append(f"- `{param.name}` (`{param.type}`, {required}{default}){description}")
    if endpoint.body_schema:
        lines.extend(["", f"Request body: `{endpoint.body_schema}`"])
    else:
        lines.extend(["", "Request body: none"])
    response = endpoint.response_schema or endpoint.response_content_type
    lines.extend(["", f"Response: `{response}`", ""])
    return lines


def _schema_type(schema: dict[str, Any]) -> str:
    if "$ref" in schema:
        return str(schema["$ref"]).removeprefix("#/components/schemas/")
    if schema.get("type") == "array":
        item = schema.get("items", {})
        if isinstance(item, dict) and "$ref" in item:
            return f"array<{str(item['$ref']).removeprefix('#/components/schemas/')}>"
        return "array"
    return str(schema.get("type", "object"))


def agent_markdown_document() -> str:
    write_endpoints = [endpoint for endpoint in API_ENDPOINTS if endpoint.method in {"POST", "PATCH", "DELETE"}]
    lines = [
        "# tele-mess-core Agent API Notes",
        "",
        "This file is generated from `tele_mess_core.server.contracts` for quick agent lookup.",
        "",
        f"- Contract version: `{API_CONTRACT_VERSION}`",
        f"- Contract hash: `{API_CONTRACT_HASH}`",
        f"- Full reference: `docs/api.md`",
        f"- OpenAPI snapshot: `docs/openapi.json`",
        f"- Runtime manifest: `{API_MANIFEST_PATH}`",
        "",
        "## Agent Rules",
        "",
        "- Treat `contracts.py` as the source of truth for endpoint shape.",
        "- When changing an API handler, update the contract and regenerate docs in the same change.",
        "- Do not read or commit local secrets such as `config.yml`, Telegram `.session` files, SQLite archives, media files, tokens, phone numbers, login codes, or 2FA passwords unless the user explicitly asks and the data is needed.",
        "- Token-protected endpoints accept `Authorization: Bearer <token>` or `X-Api-Token: <token>`.",
        "",
        "## Write Endpoints",
        "",
    ]
    for endpoint in write_endpoints:
        body = endpoint.body_schema or "none"
        lines.append(f"- `{endpoint.method} {endpoint.path}` body `{body}` - {endpoint.summary}")
    lines.extend(
        [
            "",
            "## Required Checks",
            "",
            "```bash",
            "tele-mess-core generate-api-docs --check",
            "python -m unittest discover -s tests -v",
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def openapi_json() -> str:
    return json.dumps(openapi_document(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
