from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from importlib import resources
from pathlib import Path
from typing import Any

from tele_mess_core.models import (
    AccountAuthRecord,
    AccountRecord,
    BackupPolicyRecord,
    CaptureCursorRecord,
    ChatRecord,
    MediaFileRecord,
    MessageRecord,
    OperationEventRecord,
    OriginRecord,
    ParticipantRecord,
    UserRecord,
    utc_now_iso,
)


SCHEMA_VERSION = "8"


class ArchiveStore:
    """SQLite-backed message archive."""

    def __init__(self, database_path: str | Path):
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.database_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def initialize(self) -> None:
        with self._lock:
            if self._has_table("messages") and not self._has_column("messages", "account_id"):
                self._migrate_v1_to_v2()
            else:
                self._conn.executescript(_schema_sql())
            self._ensure_current_schema()
            if self.get_meta("database_id") is None:
                self.set_meta("database_id", str(uuid.uuid4()))
            self.set_meta("schema_version", SCHEMA_VERSION)
            self._conn.commit()

    def get_meta(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def upsert_account(self, account: AccountRecord) -> None:
        now = account.updated_at or utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO accounts(source, account_id, display_name, kind, updated_at, raw_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, account_id) DO UPDATE SET
                  display_name = excluded.display_name,
                  kind = excluded.kind,
                  updated_at = excluded.updated_at,
                  raw_json = excluded.raw_json
                """,
                (account.source, account.account_id, account.display_name, account.kind, now, account.raw_json),
            )
            self._conn.commit()


    def upsert_account_auth(self, auth: AccountAuthRecord) -> None:
        now = auth.updated_at or utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO account_auth(
                  source, account_id, auth_state, phone, session_name, session_dir,
                  last_error, updated_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, account_id) DO UPDATE SET
                  auth_state = excluded.auth_state,
                  phone = excluded.phone,
                  session_name = excluded.session_name,
                  session_dir = excluded.session_dir,
                  last_error = excluded.last_error,
                  updated_at = excluded.updated_at,
                  raw_json = excluded.raw_json
                """,
                (
                    auth.source,
                    auth.account_id,
                    auth.auth_state,
                    auth.phone,
                    auth.session_name,
                    auth.session_dir,
                    auth.last_error,
                    now,
                    auth.raw_json,
                ),
            )
            self._conn.commit()

    def delete_management_account(self, source: str, account_id: str) -> int:
        deleted = 0
        with self._lock:
            for sql in (
                "DELETE FROM account_auth WHERE source = ? AND account_id = ?",
                "DELETE FROM origins WHERE source = ? AND account_id = ?",
                "DELETE FROM backup_policies WHERE source = ? AND account_id = ?",
                "DELETE FROM participants WHERE source = ? AND account_id = ?",
                "DELETE FROM capture_cursors WHERE source = ? AND account_id = ?",
                "DELETE FROM operation_events WHERE source = ? AND account_id = ?",
                "DELETE FROM accounts WHERE source = ? AND account_id = ?",
            ):
                cur = self._conn.execute(sql, (source, account_id))
                deleted += max(cur.rowcount, 0)
            self._conn.commit()
        return deleted

    def list_management_accounts(self) -> list[dict[str, Any]]:
        with self._lock:
            account_rows = self._conn.execute(
                """
                SELECT source, account_id, display_name, kind, updated_at, raw_json
                FROM accounts
                ORDER BY source, account_id
                """
            ).fetchall()
            auth_rows = self._conn.execute(
                """
                SELECT source, account_id, auth_state, phone, session_name, session_dir,
                       last_error, updated_at AS auth_updated_at, raw_json AS auth_raw_json
                FROM account_auth
                ORDER BY source, account_id
                """
            ).fetchall()

        items: dict[tuple[str, str], dict[str, Any]] = {}
        for row in account_rows:
            data = _row_to_dict(row, json_fields={"raw_json"})
            data.update(
                {
                    "auth_state": "unknown",
                    "phone": None,
                    "session_name": None,
                    "session_dir": None,
                    "last_error": None,
                    "auth_updated_at": None,
                    "auth_raw_json": None,
                }
            )
            items[(data["source"], data["account_id"])] = data
        for row in auth_rows:
            data = _row_to_dict(row, json_fields={"auth_raw_json"})
            key = (data["source"], data["account_id"])
            item = items.setdefault(
                key,
                {
                    "source": data["source"],
                    "account_id": data["account_id"],
                    "display_name": data["account_id"],
                    "kind": None,
                    "updated_at": data["auth_updated_at"],
                    "raw_json": None,
                },
            )
            item.update(
                {
                    "auth_state": data["auth_state"],
                    "phone": data["phone"],
                    "session_name": data["session_name"],
                    "session_dir": data["session_dir"],
                    "last_error": data["last_error"],
                    "auth_updated_at": data["auth_updated_at"],
                    "auth_raw_json": data["auth_raw_json"],
                }
            )
        return sorted(items.values(), key=lambda item: (item["source"], item["account_id"]))

    def upsert_chat(self, chat: ChatRecord) -> None:
        now = chat.updated_at or utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO chats(source, account_id, chat_id, title, username, kind, updated_at, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, account_id, chat_id) DO UPDATE SET
                  title = excluded.title,
                  username = excluded.username,
                  kind = excluded.kind,
                  updated_at = excluded.updated_at,
                  raw_json = excluded.raw_json
                """,
                (chat.source, chat.account_id, chat.chat_id, chat.title, chat.username, chat.kind, now, chat.raw_json),
            )
            self._conn.commit()

    def upsert_user(self, user: UserRecord) -> None:
        now = user.updated_at or utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO users(source, account_id, user_id, username, display_name, is_bot, updated_at, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, account_id, user_id) DO UPDATE SET
                  username = excluded.username,
                  display_name = excluded.display_name,
                  is_bot = excluded.is_bot,
                  updated_at = excluded.updated_at,
                  raw_json = excluded.raw_json
                """,
                (
                    user.source,
                    user.account_id,
                    user.user_id,
                    user.username,
                    user.display_name,
                    int(user.is_bot),
                    now,
                    user.raw_json,
                ),
            )
            self._conn.commit()


    def upsert_origin(self, origin: OriginRecord) -> None:
        now = origin.updated_at or utc_now_iso()
        discovered_at = origin.discovered_at or now
        with self._lock:
            archived_at = origin.archived_at
            if origin.topic_id and archived_at is None:
                row = self._conn.execute(
                    """
                    SELECT archived_at
                    FROM origins
                    WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = 0
                    """,
                    (origin.source, origin.account_id, origin.origin_id),
                ).fetchone()
                if row and row["archived_at"]:
                    archived_at = row["archived_at"]
            self._conn.execute(
                """
                INSERT INTO origins(
                  source, account_id, origin_id, topic_id, origin_type, parent_origin_id,
                  title, username, is_forum, archived_at, last_message_at, discovered_at, updated_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, account_id, origin_id, topic_id) DO UPDATE SET
                  origin_type = excluded.origin_type,
                  parent_origin_id = excluded.parent_origin_id,
                  title = excluded.title,
                  username = excluded.username,
                  is_forum = excluded.is_forum,
                  last_message_at = COALESCE(excluded.last_message_at, origins.last_message_at),
                  updated_at = excluded.updated_at,
                  raw_json = excluded.raw_json
                """,
                (
                    origin.source,
                    origin.account_id,
                    origin.origin_id,
                    origin.topic_id,
                    origin.origin_type,
                    origin.parent_origin_id,
                    origin.title,
                    origin.username,
                    int(origin.is_forum),
                    archived_at,
                    origin.last_message_at,
                    discovered_at,
                    now,
                    origin.raw_json,
                ),
            )
            self._conn.commit()

    def archive_origin(
        self,
        source: str,
        account_id: str,
        origin_id: int,
        topic_id: int = 0,
        archived: bool = True,
    ) -> int:
        at = utc_now_iso() if archived else None
        changed = 0
        with self._lock:
            if topic_id == 0:
                cur = self._conn.execute(
                    """
                    UPDATE origins
                    SET archived_at = ?, updated_at = ?
                    WHERE source = ? AND account_id = ? AND origin_id = ?
                    """,
                    (at, utc_now_iso(), source, account_id, origin_id),
                )
                changed += max(cur.rowcount, 0)
                if archived:
                    cur = self._conn.execute(
                        """
                        UPDATE backup_policies
                        SET enabled = 0, updated_at = ?
                        WHERE source = ? AND account_id = ? AND origin_id = ?
                        """,
                        (utc_now_iso(), source, account_id, origin_id),
                    )
                    changed += max(cur.rowcount, 0)
            else:
                cur = self._conn.execute(
                    """
                    UPDATE origins
                    SET archived_at = ?, updated_at = ?
                    WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = ?
                    """,
                    (at, utc_now_iso(), source, account_id, origin_id, topic_id),
                )
                changed += max(cur.rowcount, 0)
                if archived:
                    cur = self._conn.execute(
                        """
                        UPDATE backup_policies
                        SET enabled = 0, updated_at = ?
                        WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = ?
                        """,
                        (utc_now_iso(), source, account_id, origin_id, topic_id),
                    )
                    changed += max(cur.rowcount, 0)
            self._conn.commit()
        return changed

    def delete_origin(self, source: str, account_id: str, origin_id: int, topic_id: int = 0) -> int:
        deleted = 0
        with self._lock:
            for sql in (
                """
                DELETE FROM backup_policies
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = ?
                """,
                """
                DELETE FROM capture_cursors
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = ?
                """,
                """
                DELETE FROM origins
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = ?
                """,
            ):
                cur = self._conn.execute(sql, (source, account_id, origin_id, topic_id))
                deleted += max(cur.rowcount, 0)
            if topic_id == 0:
                cur = self._conn.execute(
                    "DELETE FROM participants WHERE source = ? AND account_id = ? AND origin_id = ?",
                    (source, account_id, origin_id),
                )
                deleted += max(cur.rowcount, 0)
            self._conn.commit()
        return deleted

    def list_origins(self, account_id: str | None = None, include_archived: bool = False) -> list[dict[str, Any]]:
        sql = """
            SELECT
              o.source, o.account_id, o.origin_id, o.topic_id, o.origin_type,
              o.parent_origin_id, o.title, o.username, o.is_forum,
              o.archived_at, o.last_message_at, o.discovered_at, o.updated_at, o.raw_json,
              p.enabled AS backup_enabled,
              p.capture_text,
              p.capture_media_metadata,
              p.download_media,
              p.tags,
              p.updated_at AS policy_updated_at
            FROM origins o
            LEFT JOIN origins parent
              ON parent.source = o.source
             AND parent.account_id = o.account_id
             AND parent.origin_id = o.origin_id
             AND parent.topic_id = 0
            LEFT JOIN backup_policies p
              ON p.source = o.source
             AND p.account_id = o.account_id
             AND p.origin_id = o.origin_id
             AND p.topic_id = o.topic_id
        """
        clauses: list[str] = []
        params: list[Any] = []
        if account_id is not None:
            clauses.append("o.account_id = ?")
            params.append(account_id)
        if not include_archived:
            clauses.append("o.archived_at IS NULL")
            clauses.append("(o.topic_id = 0 OR parent.archived_at IS NULL)")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY o.account_id, o.origin_type, o.title, o.origin_id, o.topic_id"
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        items = []
        for row in rows:
            data = _row_to_dict(
                row,
                json_fields={"raw_json"},
                bool_fields={"is_forum", "backup_enabled", "capture_text", "capture_media_metadata", "download_media"},
            )
            backup_enabled = data.pop("backup_enabled")
            if backup_enabled is None:
                data["backup_policy"] = None
                data.pop("capture_text", None)
                data.pop("capture_media_metadata", None)
                data.pop("download_media", None)
                data.pop("tags", None)
                data.pop("policy_updated_at", None)
            else:
                data["backup_policy"] = {
                    "enabled": backup_enabled,
                    "capture_text": data.pop("capture_text"),
                    "capture_media_metadata": data.pop("capture_media_metadata"),
                    "download_media": data.pop("download_media"),
                    "tags": data.pop("tags"),
                    "updated_at": data.pop("policy_updated_at"),
                }
            items.append(data)
        return items

    def set_backup_policy(self, policy: BackupPolicyRecord) -> None:
        now = policy.updated_at or utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO backup_policies(
                  source, account_id, origin_id, topic_id, enabled,
                  capture_text, capture_media_metadata, download_media, tags, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, account_id, origin_id, topic_id) DO UPDATE SET
                  enabled = excluded.enabled,
                  capture_text = excluded.capture_text,
                  capture_media_metadata = excluded.capture_media_metadata,
                  download_media = excluded.download_media,
                  tags = excluded.tags,
                  updated_at = excluded.updated_at
                """,
                (
                    policy.source,
                    policy.account_id,
                    policy.origin_id,
                    policy.topic_id,
                    int(policy.enabled),
                    int(policy.capture_text),
                    int(policy.capture_media_metadata),
                    int(policy.download_media),
                    policy.tags,
                    now,
                ),
            )
            self._conn.commit()

    def delete_backup_policy(self, source: str, account_id: str, origin_id: int, topic_id: int = 0) -> int:
        with self._lock:
            cur = self._conn.execute(
                """
                DELETE FROM backup_policies
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = ?
                """,
                (source, account_id, origin_id, topic_id),
            )
            self._conn.commit()
            return max(cur.rowcount, 0)

    def list_backup_policies(self, account_id: str | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT source, account_id, origin_id, topic_id, enabled,
                   capture_text, capture_media_metadata, download_media, tags, updated_at
            FROM backup_policies
        """
        params: tuple[Any, ...] = ()
        if account_id is not None:
            sql += " WHERE account_id = ?"
            params = (account_id,)
        sql += " ORDER BY account_id, origin_id, topic_id"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [
            _row_to_dict(
                row,
                bool_fields={"enabled", "capture_text", "capture_media_metadata", "download_media"},
            )
            for row in rows
        ]


    def get_backup_policy(
        self,
        source: str,
        account_id: str,
        origin_id: int,
        topic_id: int = 0,
    ) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT source, account_id, origin_id, topic_id, enabled,
                       capture_text, capture_media_metadata, download_media, tags, updated_at
                FROM backup_policies
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = ?
                """,
                (source, account_id, origin_id, topic_id),
            ).fetchone()
        if row is None:
            return None
        return _row_to_dict(
            row,
            bool_fields={"enabled", "capture_text", "capture_media_metadata", "download_media"},
        )

    def get_capture_cursor(
        self,
        source: str,
        account_id: str,
        origin_id: int,
        topic_id: int = 0,
    ) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT source, account_id, origin_id, topic_id, last_message_id,
                       last_message_at, last_backfill_at, updated_at, raw_json
                FROM capture_cursors
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = ?
                """,
                (source, account_id, origin_id, topic_id),
            ).fetchone()
        return _row_to_dict(row, json_fields={"raw_json"}) if row else None

    def upsert_capture_cursor(self, cursor: CaptureCursorRecord) -> None:
        now = cursor.updated_at or utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO capture_cursors(
                  source, account_id, origin_id, topic_id, last_message_id,
                  last_message_at, last_backfill_at, updated_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, account_id, origin_id, topic_id) DO UPDATE SET
                  last_message_id = MAX(capture_cursors.last_message_id, excluded.last_message_id),
                  last_message_at = COALESCE(excluded.last_message_at, capture_cursors.last_message_at),
                  last_backfill_at = excluded.last_backfill_at,
                  updated_at = excluded.updated_at,
                  raw_json = excluded.raw_json
                """,
                (
                    cursor.source,
                    cursor.account_id,
                    cursor.origin_id,
                    cursor.topic_id,
                    cursor.last_message_id,
                    cursor.last_message_at,
                    cursor.last_backfill_at,
                    now,
                    cursor.raw_json,
                ),
            )
            if cursor.last_message_at:
                self._conn.execute(
                    """
                    UPDATE origins
                    SET last_message_at = CASE
                          WHEN last_message_at IS NULL OR last_message_at < ? THEN ?
                          ELSE last_message_at
                        END,
                        updated_at = ?
                    WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = ?
                    """,
                    (
                        cursor.last_message_at,
                        cursor.last_message_at,
                        now,
                        cursor.source,
                        cursor.account_id,
                        cursor.origin_id,
                        cursor.topic_id,
                    ),
                )
            self._conn.commit()

    def list_capture_cursors(self, account_id: str | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT c.source, c.account_id, c.origin_id, c.topic_id, c.last_message_id,
                   c.last_message_at, c.last_backfill_at, c.updated_at, c.raw_json,
                   o.title AS origin_title
            FROM capture_cursors c
            LEFT JOIN origins o
              ON o.source = c.source
             AND o.account_id = c.account_id
             AND o.origin_id = c.origin_id
             AND o.topic_id = c.topic_id
        """
        params: tuple[Any, ...] = ()
        if account_id is not None:
            sql += " WHERE c.account_id = ?"
            params = (account_id,)
        sql += " ORDER BY c.account_id, c.origin_id, c.topic_id"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_dict(row, json_fields={"raw_json"}) for row in rows]

    def upsert_participant(self, participant: ParticipantRecord) -> None:
        now = participant.updated_at or utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO participants(
                  source, account_id, origin_id, user_id, username, display_name,
                  is_bot, role, last_seen_at, updated_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, account_id, origin_id, user_id) DO UPDATE SET
                  username = excluded.username,
                  display_name = excluded.display_name,
                  is_bot = excluded.is_bot,
                  role = excluded.role,
                  last_seen_at = excluded.last_seen_at,
                  updated_at = excluded.updated_at,
                  raw_json = excluded.raw_json
                """,
                (
                    participant.source,
                    participant.account_id,
                    participant.origin_id,
                    participant.user_id,
                    participant.username,
                    participant.display_name,
                    int(participant.is_bot),
                    participant.role,
                    participant.last_seen_at,
                    now,
                    participant.raw_json,
                ),
            )
            self._conn.commit()

    def delete_participant(self, source: str, account_id: str, origin_id: int, user_id: int) -> int:
        with self._lock:
            cur = self._conn.execute(
                """
                DELETE FROM participants
                WHERE source = ? AND account_id = ? AND origin_id = ? AND user_id = ?
                """,
                (source, account_id, origin_id, user_id),
            )
            self._conn.commit()
            return max(cur.rowcount, 0)

    def list_participants(self, account_id: str | None = None, origin_id: int | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT source, account_id, origin_id, user_id, username, display_name,
                   is_bot, role, last_seen_at, updated_at, raw_json
            FROM participants
        """
        clauses: list[str] = []
        params: list[Any] = []
        if account_id is not None:
            clauses.append("account_id = ?")
            params.append(account_id)
        if origin_id is not None:
            clauses.append("origin_id = ?")
            params.append(origin_id)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY account_id, origin_id, display_name, user_id"
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [
            _row_to_dict(row, json_fields={"raw_json"}, bool_fields={"is_bot"})
            for row in rows
        ]

    def upsert_message(self, message: MessageRecord, event_type: str) -> int:
        payload = _message_payload(message)
        now = message.ingested_at or utc_now_iso()
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO messages(
                  source, account_id, chat_id, message_id, topic_id, sender_id, sender_name, sender_username,
                  sent_at, edited_at, ingested_at, deleted_at, text, has_media, media_kind,
                  grouped_id, reply_to_message_id, forward_from_id, forward_from_name, permalink,
                  reactions_json, raw_json, version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(source, account_id, chat_id, message_id) DO UPDATE SET
                  topic_id = excluded.topic_id,
                  sender_id = excluded.sender_id,
                  sender_name = excluded.sender_name,
                  sender_username = excluded.sender_username,
                  sent_at = excluded.sent_at,
                  edited_at = excluded.edited_at,
                  ingested_at = excluded.ingested_at,
                  deleted_at = excluded.deleted_at,
                  text = excluded.text,
                  has_media = excluded.has_media,
                  media_kind = excluded.media_kind,
                  grouped_id = excluded.grouped_id,
                  reply_to_message_id = excluded.reply_to_message_id,
                  forward_from_id = excluded.forward_from_id,
                  forward_from_name = excluded.forward_from_name,
                  permalink = excluded.permalink,
                  reactions_json = excluded.reactions_json,
                  raw_json = excluded.raw_json,
                  version = messages.version + 1
                """,
                (
                    message.source,
                    message.account_id,
                    message.chat_id,
                    message.message_id,
                    message.topic_id,
                    message.sender_id,
                    message.sender_name,
                    message.sender_username,
                    message.sent_at,
                    message.edited_at,
                    now,
                    message.deleted_at,
                    message.text,
                    int(message.has_media),
                    message.media_kind,
                    message.grouped_id,
                    message.reply_to_message_id,
                    message.forward_from_id,
                    message.forward_from_name,
                    message.permalink,
                    message.reactions_json,
                    message.raw_json,
                ),
            )
            event_seq = self._insert_event(
                source=message.source,
                account_id=message.account_id,
                event_type=event_type,
                chat_id=message.chat_id,
                message_id=message.message_id,
                event_at=now,
                payload=payload,
            )
            self._conn.commit()
            return event_seq

    def mark_deleted(
        self,
        source: str,
        chat_id: int,
        message_ids: list[int],
        event_at: str | None = None,
        raw_payload: dict[str, Any] | None = None,
        account_id: str = "default",
    ) -> list[int]:
        at = event_at or utc_now_iso()
        seqs: list[int] = []
        with self._lock:
            for message_id in message_ids:
                existing = self._conn.execute(
                    "SELECT source FROM messages WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?",
                    (source, account_id, chat_id, message_id),
                ).fetchone()
                if existing:
                    self._conn.execute(
                        """
                        UPDATE messages
                        SET deleted_at = ?, ingested_at = ?, version = version + 1
                        WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
                        """,
                        (at, at, source, account_id, chat_id, message_id),
                    )
                else:
                    self._conn.execute(
                        """
                        INSERT INTO messages(source, account_id, chat_id, message_id, sent_at, ingested_at, deleted_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (source, account_id, chat_id, message_id, at, at, at),
                    )
                seqs.append(
                    self._insert_event(
                        source=source,
                        account_id=account_id,
                        event_type="delete",
                        chat_id=chat_id,
                        message_id=message_id,
                        event_at=at,
                        payload=raw_payload or {"message_id": message_id},
                    )
                )
            self._conn.commit()
        return seqs

    def update_reactions(
        self,
        source: str,
        chat_id: int,
        message_id: int,
        reactions: Any,
        event_at: str | None = None,
        raw_payload: dict[str, Any] | None = None,
        account_id: str = "default",
    ) -> int:
        at = event_at or utc_now_iso()
        reactions_json = json.dumps(reactions, ensure_ascii=False, default=str)
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE messages
                SET reactions_json = ?, ingested_at = ?, version = version + 1
                WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
                """,
                (reactions_json, at, source, account_id, chat_id, message_id),
            )
            if cur.rowcount == 0:
                self._conn.execute(
                    """
                    INSERT INTO messages(source, account_id, chat_id, message_id, sent_at, ingested_at, reactions_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (source, account_id, chat_id, message_id, at, at, reactions_json),
                )
            seq = self._insert_event(
                source=source,
                account_id=account_id,
                event_type="reaction",
                chat_id=chat_id,
                message_id=message_id,
                event_at=at,
                payload=raw_payload or {"reactions": reactions},
            )
            self._conn.commit()
            return seq


    def upsert_media_file(self, media_file: MediaFileRecord) -> None:
        downloaded_at = media_file.downloaded_at or utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO media_files(
                  source, account_id, chat_id, message_id, file_index, file_path,
                  media_kind, mime_type, file_size, downloaded_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, account_id, chat_id, message_id, file_index) DO UPDATE SET
                  file_path = excluded.file_path,
                  media_kind = excluded.media_kind,
                  mime_type = excluded.mime_type,
                  file_size = excluded.file_size,
                  downloaded_at = excluded.downloaded_at,
                  raw_json = excluded.raw_json
                """,
                (
                    media_file.source,
                    media_file.account_id,
                    media_file.chat_id,
                    media_file.message_id,
                    media_file.file_index,
                    media_file.file_path,
                    media_file.media_kind,
                    media_file.mime_type,
                    media_file.file_size,
                    downloaded_at,
                    media_file.raw_json,
                ),
            )
            self._conn.commit()

    def list_media_files(
        self,
        account_id: str | None = None,
        chat_id: int | None = None,
        message_id: int | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT m.source, m.account_id, m.chat_id, m.message_id, m.file_index, m.file_path,
                   m.media_kind, m.mime_type, m.file_size, m.downloaded_at, m.raw_json,
                   COALESCE(c.title, o.title) AS chat_title
            FROM media_files m
            LEFT JOIN chats c
              ON c.source = m.source
             AND c.account_id = m.account_id
             AND c.chat_id = m.chat_id
            LEFT JOIN origins o
              ON o.source = m.source
             AND o.account_id = m.account_id
             AND o.origin_id = m.chat_id
             AND o.topic_id = 0
        """
        clauses: list[str] = []
        params: list[Any] = []
        if account_id is not None:
            clauses.append("m.account_id = ?")
            params.append(account_id)
        if chat_id is not None:
            clauses.append("m.chat_id = ?")
            params.append(chat_id)
        if message_id is not None:
            clauses.append("m.message_id = ?")
            params.append(message_id)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY m.downloaded_at DESC, m.account_id, m.chat_id, m.message_id, m.file_index LIMIT ?"
        params.append(_bounded_limit(limit))
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [_row_to_dict(row, json_fields={"raw_json"}) for row in rows]

    def add_operation_event(self, event: OperationEventRecord) -> int:
        occurred_at = event.occurred_at or utc_now_iso()
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO operation_events(
                  source, account_id, operation, status, subject_type, subject_id,
                  error_code, message, retry_after, occurred_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.source,
                    event.account_id,
                    event.operation,
                    event.status,
                    event.subject_type,
                    event.subject_id,
                    event.error_code,
                    event.message,
                    event.retry_after,
                    occurred_at,
                    event.raw_json,
                ),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def list_operation_events(
        self,
        account_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT id, source, account_id, operation, status, subject_type, subject_id,
                   error_code, message, retry_after, occurred_at, raw_json
            FROM operation_events
        """
        clauses: list[str] = []
        params: list[Any] = []
        if account_id is not None:
            clauses.append("account_id = ?")
            params.append(account_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(_bounded_limit(limit, max_limit=500))
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [_row_to_dict(row, json_fields={"raw_json"}) for row in rows]

    def state(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute("SELECT COALESCE(MAX(seq), 0) AS seq FROM events").fetchone()
            message_count = self._conn.execute("SELECT COUNT(*) AS count FROM messages").fetchone()
            operation_error_count = self._conn.execute(
                "SELECT COUNT(*) AS count FROM operation_events WHERE status IN ('failed', 'partial', 'rate_limited')"
            ).fetchone()
            return {
                "database_id": self.get_meta("database_id"),
                "schema_version": self.get_meta("schema_version"),
                "last_event_seq": row["seq"],
                "message_count": message_count["count"],
                "operation_error_count": operation_error_count["count"],
                "server_time": utc_now_iso(),
            }

    def list_events(self, after: int = 0, limit: int = 500) -> dict[str, Any]:
        limit = _bounded_limit(limit)
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT seq, source, account_id, event_type, chat_id, message_id, event_at, payload_json
                FROM events
                WHERE seq > ?
                ORDER BY seq ASC
                LIMIT ?
                """,
                (after, limit),
            ).fetchall()
        events = [_row_to_dict(row, json_fields={"payload_json"}) for row in rows]
        next_cursor = events[-1]["seq"] if events else after
        return {"items": events, "next_cursor": next_cursor, "has_more": len(events) == limit}

    def list_messages_after(self, after_event_seq: int = 0, limit: int = 500) -> dict[str, Any]:
        limit = _bounded_limit(limit)
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                  e.seq AS event_seq,
                  m.source, m.account_id, m.chat_id, m.message_id, m.topic_id, m.sender_id, m.sender_name,
                  m.sender_username, m.sent_at, m.edited_at, m.ingested_at, m.deleted_at,
                  m.text, m.has_media, m.media_kind, m.grouped_id, m.reply_to_message_id,
                  m.forward_from_id, m.forward_from_name, m.permalink, m.reactions_json,
                  m.raw_json, m.version,
                  COALESCE(c.title, o.title) AS chat_title
                FROM events e
                JOIN messages m
                  ON m.source = e.source
                 AND m.chat_id = e.chat_id
                 AND m.account_id = e.account_id
                 AND m.message_id = e.message_id
                LEFT JOIN chats c
                  ON c.source = m.source
                 AND c.account_id = m.account_id
                 AND c.chat_id = m.chat_id
                LEFT JOIN origins o
                  ON o.source = m.source
                 AND o.account_id = m.account_id
                 AND o.origin_id = m.chat_id
                 AND o.topic_id = COALESCE(m.topic_id, 0)
                WHERE e.seq > ?
                  AND e.message_id IS NOT NULL
                ORDER BY e.seq ASC
                LIMIT ?
                """,
                (after_event_seq, limit),
            ).fetchall()
        messages = [_row_to_dict(row, json_fields={"reactions_json", "raw_json"}) for row in rows]
        next_cursor = messages[-1]["event_seq"] if messages else after_event_seq
        return {"items": messages, "next_cursor": next_cursor, "has_more": len(messages) == limit}

    def list_latest_messages(self, limit: int = 50) -> dict[str, Any]:
        limit = _bounded_limit(limit, max_limit=100)
        with self._lock:
            rows = self._conn.execute(
                """
                WITH latest_events AS (
                  SELECT source, account_id, chat_id, message_id, MAX(seq) AS event_seq
                  FROM events
                  WHERE message_id IS NOT NULL
                  GROUP BY source, account_id, chat_id, message_id
                )
                SELECT
                  e.event_seq AS event_seq,
                  m.source, m.account_id, m.chat_id, m.message_id, m.topic_id, m.sender_id, m.sender_name,
                  m.sender_username, m.sent_at, m.edited_at, m.ingested_at, m.deleted_at,
                  m.text, m.has_media, m.media_kind, m.grouped_id, m.reply_to_message_id,
                  m.forward_from_id, m.forward_from_name, m.permalink, m.reactions_json,
                  m.raw_json, m.version,
                  COALESCE(c.title, o.title) AS chat_title
                FROM messages m
                JOIN latest_events e
                  ON e.source = m.source
                 AND e.chat_id = m.chat_id
                 AND e.account_id = m.account_id
                 AND e.message_id = m.message_id
                LEFT JOIN chats c
                  ON c.source = m.source
                 AND c.account_id = m.account_id
                 AND c.chat_id = m.chat_id
                LEFT JOIN origins o
                  ON o.source = m.source
                 AND o.account_id = m.account_id
                 AND o.origin_id = m.chat_id
                 AND o.topic_id = COALESCE(m.topic_id, 0)
                ORDER BY m.sent_at DESC, e.event_seq DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        messages = [_row_to_dict(row, json_fields={"reactions_json", "raw_json"}) for row in rows]
        next_cursor = max((item["event_seq"] for item in messages), default=0)
        return {"items": messages, "next_cursor": next_cursor, "has_more": len(messages) == limit}

    def list_chats(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT source, account_id, chat_id, title, username, kind, updated_at, raw_json FROM chats ORDER BY account_id, title"
            ).fetchall()
        return [_row_to_dict(row, json_fields={"raw_json"}) for row in rows]

    def list_accounts(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT source, account_id, display_name, kind, updated_at, raw_json FROM accounts ORDER BY source, account_id"
            ).fetchall()
        return [_row_to_dict(row, json_fields={"raw_json"}) for row in rows]

    def search_messages(self, query: str, limit: int = 50) -> list[dict[str, Any]]:
        limit = _bounded_limit(limit, max_limit=100)
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT m.*
                FROM message_fts f
                JOIN messages m ON m.rowid = f.rowid
                WHERE message_fts MATCH ?
                ORDER BY m.sent_at DESC
                LIMIT ?
                """,
                (query, limit),
            ).fetchall()
        return [_row_to_dict(row, json_fields={"reactions_json", "raw_json"}) for row in rows]

    def _insert_event(
        self,
        source: str,
        account_id: str,
        event_type: str,
        chat_id: int,
        message_id: int | None,
        event_at: str,
        payload: dict[str, Any],
    ) -> int:
        cur = self._conn.execute(
            """
            INSERT INTO events(source, account_id, event_type, chat_id, message_id, event_at, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source,
                account_id,
                event_type,
                chat_id,
                message_id,
                event_at,
                json.dumps(payload, ensure_ascii=False, default=str),
            ),
        )
        return int(cur.lastrowid)

    def _has_table(self, name: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        return row is not None

    def _has_column(self, table: str, column: str) -> bool:
        if not self._has_table(table):
            return False
        rows = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(row["name"] == column for row in rows)

    def _ensure_current_schema(self) -> None:
        if self._has_table("origins") and not self._has_column("origins", "archived_at"):
            self._conn.execute("ALTER TABLE origins ADD COLUMN archived_at TEXT")
        if self._has_table("origins") and not self._has_column("origins", "last_message_at"):
            self._conn.execute("ALTER TABLE origins ADD COLUMN last_message_at TEXT")
        if self._has_table("backup_policies") and not self._has_column("backup_policies", "tags"):
            self._conn.execute("ALTER TABLE backup_policies ADD COLUMN tags TEXT")

    def _migrate_v1_to_v2(self) -> None:
        self._conn.executescript(
            """
            DROP TRIGGER IF EXISTS messages_ai;
            DROP TRIGGER IF EXISTS messages_ad;
            DROP TRIGGER IF EXISTS messages_au;
            DROP TABLE IF EXISTS message_fts;
            ALTER TABLE chats RENAME TO chats_v1;
            ALTER TABLE users RENAME TO users_v1;
            ALTER TABLE messages RENAME TO messages_v1;
            ALTER TABLE events RENAME TO events_v1;
            """
        )
        self._conn.executescript(_schema_sql())
        self._conn.execute(
            """
            INSERT INTO accounts(source, account_id, display_name, kind, updated_at, raw_json)
            VALUES('telegram', 'default', 'default', 'telegram', datetime('now'), NULL)
            """
        )
        self._conn.execute(
            """
            INSERT INTO chats(source, account_id, chat_id, title, username, kind, updated_at, raw_json)
            SELECT source, 'default', chat_id, title, username, kind, updated_at, raw_json FROM chats_v1
            """
        )
        self._conn.execute(
            """
            INSERT INTO users(source, account_id, user_id, username, display_name, is_bot, updated_at, raw_json)
            SELECT source, 'default', user_id, username, display_name, is_bot, updated_at, raw_json FROM users_v1
            """
        )
        self._conn.execute(
            """
            INSERT INTO messages(
              source, account_id, chat_id, message_id, topic_id, sender_id, sender_name,
              sender_username, sent_at, edited_at, ingested_at, deleted_at, text, has_media,
              media_kind, grouped_id, reply_to_message_id, forward_from_id, forward_from_name,
              permalink, reactions_json, raw_json, version
            )
            SELECT
              source, 'default', chat_id, message_id, topic_id, sender_id, sender_name,
              sender_username, sent_at, edited_at, ingested_at, deleted_at, text, has_media,
              media_kind, grouped_id, reply_to_message_id, forward_from_id, forward_from_name,
              permalink, reactions_json, raw_json, version
            FROM messages_v1
            """
        )
        self._conn.execute(
            """
            INSERT INTO events(seq, source, account_id, event_type, chat_id, message_id, event_at, payload_json)
            SELECT seq, source, 'default', event_type, chat_id, message_id, event_at, payload_json FROM events_v1
            """
        )
        self._conn.executescript(
            """
            DROP TABLE chats_v1;
            DROP TABLE users_v1;
            DROP TABLE messages_v1;
            DROP TABLE events_v1;
            """
        )


def _message_payload(message: MessageRecord) -> dict[str, Any]:
    return {
        "source": message.source,
        "account_id": message.account_id,
        "chat_id": message.chat_id,
        "message_id": message.message_id,
        "topic_id": message.topic_id,
        "sender_id": message.sender_id,
        "sent_at": message.sent_at,
        "edited_at": message.edited_at,
        "deleted_at": message.deleted_at,
        "text": message.text,
        "has_media": message.has_media,
        "media_kind": message.media_kind,
    }


def _schema_sql() -> str:
    return resources.files("tele_mess_core.archive").joinpath("schema.sql").read_text()


def _row_to_dict(
    row: sqlite3.Row,
    json_fields: set[str] | None = None,
    bool_fields: set[str] | None = None,
) -> dict[str, Any]:
    json_fields = json_fields or set()
    bool_fields = bool_fields or set()
    data = dict(row)
    for key in json_fields:
        value = data.get(key)
        if isinstance(value, str) and value:
            try:
                data[key] = json.loads(value)
            except json.JSONDecodeError:
                pass
    for key in bool_fields:
        if key in data and data[key] is not None:
            data[key] = bool(data[key])
    return data


def _bounded_limit(limit: int, max_limit: int = 1000) -> int:
    if limit <= 0:
        return 1
    return min(limit, max_limit)
