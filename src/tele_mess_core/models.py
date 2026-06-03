from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


SOURCE_TELEGRAM = "telegram"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


@dataclass(slots=True)
class AccountRecord:
    source: str
    account_id: str
    display_name: str | None = None
    kind: str | None = None
    updated_at: str | None = None
    raw_json: str | None = None


@dataclass(slots=True)
class ChatRecord:
    source: str
    chat_id: int
    account_id: str = "default"
    title: str | None = None
    username: str | None = None
    kind: str | None = None
    updated_at: str | None = None
    raw_json: str | None = None


@dataclass(slots=True)
class UserRecord:
    source: str
    user_id: int
    account_id: str = "default"
    username: str | None = None
    display_name: str | None = None
    is_bot: bool = False
    updated_at: str | None = None
    raw_json: str | None = None


@dataclass(slots=True)
class AccountAuthRecord:
    source: str
    account_id: str
    auth_state: str = "unknown"
    phone: str | None = None
    session_name: str | None = None
    session_dir: str | None = None
    last_error: str | None = None
    updated_at: str | None = None
    raw_json: str | None = None


@dataclass(slots=True)
class OriginRecord:
    source: str
    account_id: str
    origin_id: int
    origin_type: str
    topic_id: int = 0
    parent_origin_id: int | None = None
    title: str | None = None
    username: str | None = None
    is_forum: bool = False
    discovered_at: str | None = None
    updated_at: str | None = None
    raw_json: str | None = None


@dataclass(slots=True)
class BackupPolicyRecord:
    source: str
    account_id: str
    origin_id: int
    topic_id: int = 0
    enabled: bool = False
    capture_text: bool = True
    capture_media_metadata: bool = True
    download_media: bool = False
    updated_at: str | None = None


@dataclass(slots=True)
class ParticipantRecord:
    source: str
    account_id: str
    origin_id: int
    user_id: int
    username: str | None = None
    display_name: str | None = None
    is_bot: bool = False
    role: str | None = None
    last_seen_at: str | None = None
    updated_at: str | None = None
    raw_json: str | None = None


@dataclass(slots=True)
class CaptureCursorRecord:
    source: str
    account_id: str
    origin_id: int
    topic_id: int = 0
    last_message_id: int = 0
    last_message_at: str | None = None
    last_backfill_at: str | None = None
    updated_at: str | None = None
    raw_json: str | None = None


@dataclass(slots=True)
class MediaFileRecord:
    source: str
    account_id: str
    chat_id: int
    message_id: int
    file_path: str
    file_index: int = 0
    media_kind: str | None = None
    mime_type: str | None = None
    file_size: int | None = None
    downloaded_at: str | None = None
    raw_json: str | None = None


@dataclass(slots=True)
class MessageRecord:
    source: str
    chat_id: int
    message_id: int
    sent_at: str
    account_id: str = "default"
    topic_id: int | None = None
    sender_id: int | None = None
    sender_name: str | None = None
    sender_username: str | None = None
    edited_at: str | None = None
    ingested_at: str | None = None
    deleted_at: str | None = None
    text: str | None = None
    has_media: bool = False
    media_kind: str | None = None
    grouped_id: str | None = None
    reply_to_message_id: int | None = None
    forward_from_id: str | None = None
    forward_from_name: str | None = None
    permalink: str | None = None
    reactions_json: str | None = None
    raw_json: str | None = None
