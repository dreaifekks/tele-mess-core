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
    DailyPackageRunRecord,
    DailyPackageScheduleRecord,
    DailySummaryDeliveryRecord,
    DailySummaryJobRecord,
    DailySummaryRecord,
    DailySummaryRunRecord,
    MediaFileRecord,
    MessageRecord,
    OperationEventRecord,
    OriginRecord,
    ParticipantRecord,
    UserRecord,
    utc_now_iso,
)
from tele_mess_core.archive.migrations import apply_migrations


SCHEMA_VERSION = 15
LEGACY_SCHEMA_BASELINE = 12


class ArchiveStore:
    """SQLite-backed message archive."""

    def __init__(self, database_path: str | Path):
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._state_lock = threading.RLock()
        self._connections: set[sqlite3.Connection] = set()
        self._closed = False
        self._conn

    @property
    def _lock(self) -> threading.RLock:
        lock = getattr(self._local, "lock", None)
        if lock is None:
            lock = threading.RLock()
            self._local.lock = lock
        return lock

    @property
    def _conn(self) -> sqlite3.Connection:
        if self._closed:
            raise RuntimeError("ArchiveStore is closed")
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            return connection
        connection = sqlite3.connect(str(self.database_path), check_same_thread=False, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA busy_timeout = 30000")
        self._local.connection = connection
        with self._state_lock:
            if self._closed:
                connection.close()
                raise RuntimeError("ArchiveStore is closed")
            self._connections.add(connection)
        return connection

    def close(self) -> None:
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            connections = list(self._connections)
            self._connections.clear()
        for connection in connections:
            connection.close()

    def close_thread_connection(self) -> None:
        connection = getattr(self._local, "connection", None)
        if connection is None:
            return
        try:
            if connection.in_transaction:
                connection.rollback()
        finally:
            connection.close()
            del self._local.connection
            with self._state_lock:
                self._connections.discard(connection)

    def initialize(self) -> None:
        with self._lock:
            try:
                if self._has_table("messages") and not self._has_column("messages", "account_id"):
                    self._migrate_v1_to_v2()
                else:
                    self._conn.executescript(_schema_sql())
                self._ensure_current_schema()
                meta_version = self.get_meta("schema_version")
                current_version = int(meta_version) if meta_version else LEGACY_SCHEMA_BASELINE
                current_version = max(current_version, LEGACY_SCHEMA_BASELINE)
                apply_migrations(self._conn, current_version, SCHEMA_VERSION)
                if self.get_meta("database_id") is None:
                    self.set_meta("database_id", str(uuid.uuid4()))
                self.set_meta("schema_version", str(SCHEMA_VERSION))
                self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

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
                  title, username, is_forum, important, archived_at, last_message_at, discovered_at, updated_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, account_id, origin_id, topic_id) DO UPDATE SET
                  origin_type = excluded.origin_type,
                  parent_origin_id = excluded.parent_origin_id,
                  title = excluded.title,
                  username = excluded.username,
                  is_forum = excluded.is_forum,
                  important = CASE WHEN excluded.important = 1 THEN 1 ELSE origins.important END,
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
                    int(origin.important),
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
              o.important, o.archived_at, o.last_message_at, o.discovered_at, o.updated_at, o.raw_json,
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
                bool_fields={"is_forum", "important", "backup_enabled", "capture_text", "capture_media_metadata", "download_media"},
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

    def set_origin_important(
        self,
        source: str,
        account_id: str,
        origin_id: int,
        topic_id: int = 0,
        important: bool = True,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE origins
                SET important = ?, updated_at = ?
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = ?
                """,
                (int(important), utc_now_iso(), source, account_id, origin_id, topic_id),
            )
            self._conn.commit()
            return max(cur.rowcount, 0)

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

    def get_media_file(
        self,
        source: str,
        account_id: str,
        chat_id: int,
        message_id: int,
        file_index: int = 0,
    ) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
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
                WHERE m.source = ?
                  AND m.account_id = ?
                  AND m.chat_id = ?
                  AND m.message_id = ?
                  AND m.file_index = ?
                """,
                (source, account_id, chat_id, message_id, file_index),
            ).fetchone()
        return _row_to_dict(row, json_fields={"raw_json"}) if row else None

    def list_media_files_for_messages(
        self,
        messages: list[dict[str, Any]],
    ) -> dict[tuple[str, str, int, int], list[dict[str, Any]]]:
        keys: list[tuple[str, str, int, int]] = []
        seen: set[tuple[str, str, int, int]] = set()
        for message in messages:
            try:
                key = (
                    str(message["source"]),
                    str(message["account_id"]),
                    int(message["chat_id"]),
                    int(message["message_id"]),
                )
            except (KeyError, TypeError, ValueError):
                continue
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)
        if not keys:
            return {}

        lookup_sql = """
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
            WHERE {where_clause}
            ORDER BY m.source, m.account_id, m.chat_id, m.message_id, m.file_index
        """
        rows: list[sqlite3.Row] = []
        with self._lock:
            for batch_start in range(0, len(keys), 200):
                clauses: list[str] = []
                params: list[Any] = []
                for source, account_id, chat_id, message_id in keys[batch_start : batch_start + 200]:
                    clauses.append("(m.source = ? AND m.account_id = ? AND m.chat_id = ? AND m.message_id = ?)")
                    params.extend([source, account_id, chat_id, message_id])
                rows.extend(
                    self._conn.execute(
                        lookup_sql.format(where_clause=" OR ".join(clauses)),
                        tuple(params),
                    ).fetchall()
                )

        grouped: dict[tuple[str, str, int, int], list[dict[str, Any]]] = {key: [] for key in keys}
        for row in rows:
            item = _row_to_dict(row, json_fields={"raw_json"})
            key = (item["source"], item["account_id"], item["chat_id"], item["message_id"])
            grouped.setdefault(key, []).append(item)
        return grouped

    def list_messages_for_origin_window(
        self,
        source: str,
        account_id: str,
        origin_id: int,
        topic_id: int = 0,
        window_start: str = "",
        window_end: str = "",
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        clauses = [
            "m.source = ?",
            "m.account_id = ?",
            "m.chat_id = ?",
            "m.sent_at >= ?",
            "m.sent_at < ?",
        ]
        params: list[Any] = [source, account_id, origin_id, window_start, window_end]
        if topic_id:
            clauses.append("COALESCE(m.topic_id, 0) = ?")
            params.append(topic_id)
        sql = f"""
            SELECT
              m.source, m.account_id, m.chat_id, m.message_id, m.topic_id, m.sender_id, m.sender_name,
              m.sender_username, m.sent_at, m.edited_at, m.ingested_at, m.deleted_at,
              m.text, m.has_media, m.media_kind, m.grouped_id, m.reply_to_message_id,
              m.forward_from_id, m.forward_from_name, m.permalink, m.reactions_json,
              m.raw_json, m.version,
              COALESCE(c.title, o.title) AS chat_title,
              o.title AS origin_title
            FROM messages m
            LEFT JOIN chats c
              ON c.source = m.source
             AND c.account_id = m.account_id
             AND c.chat_id = m.chat_id
            LEFT JOIN origins o
              ON o.source = m.source
             AND o.account_id = m.account_id
             AND o.origin_id = m.chat_id
             AND o.topic_id = COALESCE(m.topic_id, 0)
            WHERE {" AND ".join(clauses)}
            ORDER BY m.sent_at ASC, m.message_id ASC
            LIMIT ?
        """
        params.append(_bounded_limit(limit, max_limit=50000))
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [_row_to_dict(row, json_fields={"reactions_json", "raw_json"}, bool_fields={"has_media"}) for row in rows]

    def get_daily_package_schedule(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT enabled, time_of_day, timezone, scope_json, system_manager,
                       installed, last_installed_at, last_error, updated_at
                FROM daily_package_schedule
                WHERE id = 1
                """
            ).fetchone()
        if row is None:
            return {
                "enabled": False,
                "time_of_day": "08:00",
                "timezone": "Asia/Tokyo",
                "scope": {},
                "system_manager": "systemd-user",
                "installed": False,
                "last_installed_at": None,
                "last_error": None,
                "updated_at": None,
            }
        data = _row_to_dict(row, json_fields={"scope_json"}, bool_fields={"enabled", "installed"})
        data["scope"] = data.pop("scope_json") or {}
        return data

    def set_daily_package_schedule(self, schedule: DailyPackageScheduleRecord) -> dict[str, Any]:
        now = schedule.updated_at or utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO daily_package_schedule(
                  id, enabled, time_of_day, timezone, scope_json, system_manager,
                  installed, last_installed_at, last_error, updated_at
                )
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  enabled = excluded.enabled,
                  time_of_day = excluded.time_of_day,
                  timezone = excluded.timezone,
                  scope_json = excluded.scope_json,
                  system_manager = excluded.system_manager,
                  installed = excluded.installed,
                  last_installed_at = excluded.last_installed_at,
                  last_error = excluded.last_error,
                  updated_at = excluded.updated_at
                """,
                (
                    int(schedule.enabled),
                    schedule.time_of_day,
                    schedule.timezone,
                    schedule.scope_json,
                    schedule.system_manager,
                    int(schedule.installed),
                    schedule.last_installed_at,
                    schedule.last_error,
                    now,
                ),
            )
            self._conn.commit()
        return self.get_daily_package_schedule()

    def get_daily_summary_delivery(self) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT enabled, account_id, origin_id, topic_id, updated_at
                FROM daily_summary_delivery
                WHERE id = 1
                """
            ).fetchone()
        if row is None:
            return None
        return _row_to_dict(row, bool_fields={"enabled"})

    def set_daily_summary_delivery(self, delivery: DailySummaryDeliveryRecord) -> dict[str, Any]:
        now = delivery.updated_at or utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO daily_summary_delivery(
                  id, enabled, account_id, origin_id, topic_id, updated_at
                )
                VALUES (1, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  enabled = excluded.enabled,
                  account_id = excluded.account_id,
                  origin_id = excluded.origin_id,
                  topic_id = excluded.topic_id,
                  updated_at = excluded.updated_at
                """,
                (
                    int(delivery.enabled),
                    delivery.account_id,
                    delivery.origin_id,
                    int(delivery.topic_id),
                    now,
                ),
            )
            self._conn.commit()
        item = self.get_daily_summary_delivery()
        if item is None:
            raise ValueError("daily summary delivery was not persisted")
        return item

    def upsert_daily_package_run(self, run: DailyPackageRunRecord) -> dict[str, Any]:
        started_at = run.started_at or utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO daily_package_runs(
                  run_id, status, date, timezone, scope_json, output_dir,
                  package_json_path, package_md_path, origin_count, message_count,
                  media_count, important_origin_count, progress_total, progress_current,
                  progress_label, progress_json, error, started_at, finished_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                  status = excluded.status,
                  date = excluded.date,
                  timezone = excluded.timezone,
                  scope_json = excluded.scope_json,
                  output_dir = excluded.output_dir,
                  package_json_path = excluded.package_json_path,
                  package_md_path = excluded.package_md_path,
                  origin_count = excluded.origin_count,
                  message_count = excluded.message_count,
                  media_count = excluded.media_count,
                  important_origin_count = excluded.important_origin_count,
                  progress_total = excluded.progress_total,
                  progress_current = excluded.progress_current,
                  progress_label = excluded.progress_label,
                  progress_json = excluded.progress_json,
                  error = excluded.error,
                  finished_at = excluded.finished_at
                """,
                (
                    run.run_id,
                    run.status,
                    run.date,
                    run.timezone,
                    run.scope_json,
                    run.output_dir,
                    run.package_json_path,
                    run.package_md_path,
                    int(run.origin_count),
                    int(run.message_count),
                    int(run.media_count),
                    int(run.important_origin_count),
                    int(run.progress_total),
                    int(run.progress_current),
                    run.progress_label,
                    run.progress_json,
                    run.error,
                    started_at,
                    run.finished_at,
                ),
            )
            self._conn.commit()
        item = self.get_daily_package_run(run.run_id)
        if item is None:
            raise ValueError("daily package run was not persisted")
        return item

    def get_daily_package_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT run_id, status, date, timezone, scope_json, output_dir,
                       package_json_path, package_md_path, origin_count, message_count,
                       media_count, important_origin_count, progress_total, progress_current,
                       progress_label, progress_json, error, started_at, finished_at
                FROM daily_package_runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        data = _row_to_dict(row, json_fields={"scope_json", "progress_json"})
        data["scope"] = data.pop("scope_json") or {}
        data["progress"] = data.pop("progress_json") or {}
        return data

    def list_daily_package_runs(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        sql = """
            SELECT run_id, status, date, timezone, scope_json, output_dir,
                   package_json_path, package_md_path, origin_count, message_count,
                   media_count, important_origin_count, progress_total, progress_current,
                   progress_label, progress_json, error, started_at, finished_at
            FROM daily_package_runs
        """
        params: list[Any] = []
        if status:
            sql += " WHERE status = ?"
            params.append(status)
        sql += " ORDER BY started_at DESC LIMIT ?"
        params.append(_bounded_limit(limit, max_limit=500))
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        items = []
        for row in rows:
            data = _row_to_dict(row, json_fields={"scope_json", "progress_json"})
            data["scope"] = data.pop("scope_json") or {}
            data["progress"] = data.pop("progress_json") or {}
            items.append(data)
        return items

    def upsert_daily_summary_run(self, run: DailySummaryRunRecord) -> dict[str, Any]:
        started_at = run.started_at or utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO daily_summary_runs(
                  run_id, status, package_run_id, date, timezone, scope_json,
                  output_dir, summary_path, provider, origin_count, group_count,
                  image_count, progress_total, progress_current, progress_label,
                  progress_json, error, started_at, finished_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                  status = excluded.status,
                  package_run_id = excluded.package_run_id,
                  date = excluded.date,
                  timezone = excluded.timezone,
                  scope_json = excluded.scope_json,
                  output_dir = excluded.output_dir,
                  summary_path = excluded.summary_path,
                  provider = excluded.provider,
                  origin_count = excluded.origin_count,
                  group_count = excluded.group_count,
                  image_count = excluded.image_count,
                  progress_total = excluded.progress_total,
                  progress_current = excluded.progress_current,
                  progress_label = excluded.progress_label,
                  progress_json = excluded.progress_json,
                  error = excluded.error,
                  finished_at = excluded.finished_at
                """,
                (
                    run.run_id,
                    run.status,
                    run.package_run_id,
                    run.date,
                    run.timezone,
                    run.scope_json,
                    run.output_dir,
                    run.summary_path,
                    run.provider,
                    int(run.origin_count),
                    int(run.group_count),
                    int(run.image_count),
                    int(run.progress_total),
                    int(run.progress_current),
                    run.progress_label,
                    run.progress_json,
                    run.error,
                    started_at,
                    run.finished_at,
                ),
            )
            self._conn.commit()
        item = self.get_daily_summary_run(run.run_id)
        if item is None:
            raise ValueError("daily summary run was not persisted")
        return item

    def get_daily_summary_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT run_id, status, package_run_id, date, timezone, scope_json,
                       output_dir, summary_path, provider, origin_count, group_count,
                       image_count, progress_total, progress_current, progress_label,
                       progress_json, error, started_at, finished_at
                FROM daily_summary_runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        data = _row_to_dict(row, json_fields={"scope_json", "progress_json"})
        data["scope"] = data.pop("scope_json") or {}
        data["progress"] = data.pop("progress_json") or {}
        return data

    def list_daily_summary_runs(
        self,
        package_run_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT run_id, status, package_run_id, date, timezone, scope_json,
                   output_dir, summary_path, provider, origin_count, group_count,
                   image_count, progress_total, progress_current, progress_label,
                   progress_json, error, started_at, finished_at
            FROM daily_summary_runs
        """
        clauses: list[str] = []
        params: list[Any] = []
        if package_run_id:
            clauses.append("package_run_id = ?")
            params.append(package_run_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY started_at DESC LIMIT ?"
        params.append(_bounded_limit(limit, max_limit=500))
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        items = []
        for row in rows:
            data = _row_to_dict(row, json_fields={"scope_json", "progress_json"})
            data["scope"] = data.pop("scope_json") or {}
            data["progress"] = data.pop("progress_json") or {}
            items.append(data)
        return items

    def upsert_daily_summary_job(self, job: DailySummaryJobRecord) -> dict[str, Any]:
        now = job.updated_at or utc_now_iso()
        started_at = job.started_at or now
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO daily_summary_jobs(
                  job_id, status, date, timezone, scope_json, package_run_id,
                  summary_run_id, provider, progress_total, progress_current,
                  progress_label, progress_json, request_json, dedupe_key, worker_id,
                  lease_until, heartbeat_at, attempt, cancel_requested_at, error,
                  started_at, finished_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                  status = CASE
                    WHEN daily_summary_jobs.status = 'cancel_requested' AND excluded.status = 'running'
                    THEN 'cancel_requested'
                    ELSE excluded.status
                  END,
                  date = excluded.date,
                  timezone = excluded.timezone,
                  scope_json = excluded.scope_json,
                  package_run_id = excluded.package_run_id,
                  summary_run_id = excluded.summary_run_id,
                  provider = excluded.provider,
                  progress_total = excluded.progress_total,
                  progress_current = excluded.progress_current,
                  progress_label = excluded.progress_label,
                  progress_json = excluded.progress_json,
                  request_json = COALESCE(excluded.request_json, daily_summary_jobs.request_json),
                  dedupe_key = COALESCE(excluded.dedupe_key, daily_summary_jobs.dedupe_key),
                  worker_id = COALESCE(excluded.worker_id, daily_summary_jobs.worker_id),
                  lease_until = COALESCE(excluded.lease_until, daily_summary_jobs.lease_until),
                  heartbeat_at = COALESCE(excluded.heartbeat_at, daily_summary_jobs.heartbeat_at),
                  attempt = MAX(excluded.attempt, daily_summary_jobs.attempt),
                  cancel_requested_at = COALESCE(excluded.cancel_requested_at, daily_summary_jobs.cancel_requested_at),
                  error = excluded.error,
                  finished_at = excluded.finished_at,
                  updated_at = excluded.updated_at
                """,
                (
                    job.job_id,
                    job.status,
                    job.date,
                    job.timezone,
                    job.scope_json,
                    job.package_run_id,
                    job.summary_run_id,
                    job.provider,
                    int(job.progress_total),
                    int(job.progress_current),
                    job.progress_label,
                    job.progress_json,
                    job.request_json,
                    job.dedupe_key,
                    job.worker_id,
                    job.lease_until,
                    job.heartbeat_at,
                    int(job.attempt),
                    job.cancel_requested_at,
                    job.error,
                    started_at,
                    job.finished_at,
                    now,
                ),
            )
            self._conn.commit()
        item = self.get_daily_summary_job(job.job_id)
        if item is None:
            raise ValueError("daily summary job was not persisted")
        return item

    def get_daily_summary_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT job_id, status, date, timezone, scope_json, package_run_id,
                       summary_run_id, provider, progress_total, progress_current,
                       progress_label, progress_json, request_json, dedupe_key, worker_id,
                       lease_until, heartbeat_at, attempt, cancel_requested_at, error,
                       started_at, finished_at, updated_at
                FROM daily_summary_jobs
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        return _summary_job_from_row(row)

    def list_daily_summary_jobs(
        self,
        *,
        job_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT job_id, status, date, timezone, scope_json, package_run_id,
                   summary_run_id, provider, progress_total, progress_current,
                   progress_label, progress_json, request_json, dedupe_key, worker_id,
                   lease_until, heartbeat_at, attempt, cancel_requested_at, error,
                   started_at, finished_at, updated_at
            FROM daily_summary_jobs
        """
        clauses: list[str] = []
        params: list[Any] = []
        if job_id:
            clauses.append("job_id = ?")
            params.append(job_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY started_at DESC LIMIT ?"
        params.append(_bounded_limit(limit, max_limit=500))
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [_summary_job_from_row(row) for row in rows]

    def find_active_daily_summary_job(self, dedupe_key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT job_id, status, date, timezone, scope_json, package_run_id,
                       summary_run_id, provider, progress_total, progress_current,
                       progress_label, progress_json, request_json, dedupe_key, worker_id,
                       lease_until, heartbeat_at, attempt, cancel_requested_at, error,
                       started_at, finished_at, updated_at
                FROM daily_summary_jobs
                WHERE dedupe_key = ? AND status IN ('queued', 'running', 'cancel_requested')
                ORDER BY started_at ASC
                LIMIT 1
                """,
                (dedupe_key,),
            ).fetchone()
        return _summary_job_from_row(row) if row is not None else None

    def find_completed_daily_summary_job(self, dedupe_key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT job_id, status, date, timezone, scope_json, package_run_id,
                       summary_run_id, provider, progress_total, progress_current,
                       progress_label, progress_json, request_json, dedupe_key, worker_id,
                       lease_until, heartbeat_at, attempt, cancel_requested_at, error,
                       started_at, finished_at, updated_at
                FROM daily_summary_jobs
                WHERE dedupe_key = ? AND status = 'completed'
                ORDER BY finished_at DESC
                LIMIT 1
                """,
                (dedupe_key,),
            ).fetchone()
        return _summary_job_from_row(row) if row is not None else None

    def claim_daily_summary_job(
        self,
        worker_id: str,
        *,
        now: str,
        lease_until: str,
    ) -> dict[str, Any] | None:
        job_id: str | None = None
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    UPDATE daily_summary_jobs
                    SET status = 'canceled', finished_at = COALESCE(finished_at, ?),
                        updated_at = ?, worker_id = NULL, lease_until = NULL
                    WHERE status = 'cancel_requested'
                      AND (worker_id IS NULL OR lease_until IS NULL OR lease_until < ?)
                    """,
                    (now, now, now),
                )
                row = self._conn.execute(
                    """
                    SELECT job_id
                    FROM daily_summary_jobs
                    WHERE status = 'queued'
                       OR (status = 'running' AND (lease_until IS NULL OR lease_until < ?))
                    ORDER BY started_at ASC
                    LIMIT 1
                    """,
                    (now,),
                ).fetchone()
                if row is not None:
                    job_id = str(row["job_id"])
                    self._conn.execute(
                        """
                        UPDATE daily_summary_jobs
                        SET status = 'running', worker_id = ?, lease_until = ?, heartbeat_at = ?,
                            attempt = attempt + 1, updated_at = ?
                        WHERE job_id = ?
                        """,
                        (worker_id, lease_until, now, now, job_id),
                    )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return self.get_daily_summary_job(job_id) if job_id else None

    def renew_daily_summary_job_lease(
        self,
        job_id: str,
        worker_id: str,
        *,
        now: str,
        lease_until: str,
    ) -> bool:
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE daily_summary_jobs
                SET heartbeat_at = ?, lease_until = ?, updated_at = ?
                WHERE job_id = ? AND worker_id = ? AND status IN ('running', 'cancel_requested')
                """,
                (now, lease_until, now, job_id, worker_id),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def daily_summary_job_cancel_requested(self, job_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT status, cancel_requested_at FROM daily_summary_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return bool(row and (row["status"] == "cancel_requested" or row["cancel_requested_at"]))

    def requeue_daily_summary_job(self, job_id: str, worker_id: str, *, now: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE daily_summary_jobs
                SET status = 'queued', worker_id = NULL, lease_until = NULL,
                    heartbeat_at = NULL, progress_label = 'queued after worker stop', updated_at = ?
                WHERE job_id = ? AND worker_id = ? AND status = 'running'
                """,
                (now, job_id, worker_id),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def request_daily_summary_job_cancel(self, job_id: str) -> dict[str, Any] | None:
        now = utc_now_iso()
        with self._lock:
            row = self._conn.execute("SELECT status FROM daily_summary_jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is None:
                return None
            status = str(row["status"])
            if status not in {"completed", "failed", "canceled"}:
                self._conn.execute(
                    """
                    UPDATE daily_summary_jobs
                    SET status = 'cancel_requested',
                        cancel_requested_at = COALESCE(cancel_requested_at, ?),
                        updated_at = ?
                    WHERE job_id = ?
                    """,
                    (now, now, job_id),
                )
                self._conn.commit()
        return self.get_daily_summary_job(job_id)

    def upsert_daily_summary_record(
        self,
        record: DailySummaryRecord,
        *,
        commit: bool = True,
    ) -> dict[str, Any]:
        now = record.updated_at or utc_now_iso()
        created_at = record.created_at or now
        content_length = record.content_length or len(record.content_md)
        tags_csv = record.tags_csv if record.tags_csv is not None else _tags_csv_from_json(record.tags_json)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO daily_summary_records(
                  summary_id, run_id, package_run_id, date, timezone, scope_json,
                  tags_json, tags_csv, important, provider, title, content_md, content_json,
                  summary_path, origin_count, group_count, image_count, content_length,
                  deleted_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(summary_id) DO UPDATE SET
                  run_id = excluded.run_id,
                  package_run_id = excluded.package_run_id,
                  date = excluded.date,
                  timezone = excluded.timezone,
                  scope_json = excluded.scope_json,
                  tags_json = excluded.tags_json,
                  tags_csv = excluded.tags_csv,
                  important = excluded.important,
                  provider = excluded.provider,
                  title = excluded.title,
                  content_md = excluded.content_md,
                  content_json = excluded.content_json,
                  summary_path = excluded.summary_path,
                  origin_count = excluded.origin_count,
                  group_count = excluded.group_count,
                  image_count = excluded.image_count,
                  content_length = excluded.content_length,
                  deleted_at = excluded.deleted_at,
                  updated_at = excluded.updated_at
                """,
                (
                    record.summary_id,
                    record.run_id,
                    record.package_run_id,
                    record.date,
                    record.timezone,
                    record.scope_json,
                    record.tags_json,
                    tags_csv,
                    int(record.important),
                    record.provider,
                    record.title,
                    record.content_md,
                    record.content_json,
                    record.summary_path,
                    int(record.origin_count),
                    int(record.group_count),
                    int(record.image_count),
                    int(content_length),
                    record.deleted_at,
                    created_at,
                    now,
                ),
            )
            if commit:
                self._conn.commit()
        item = self.get_daily_summary_record(summary_id=record.summary_id, include_deleted=True)
        if item is None:
            raise ValueError("daily summary record was not persisted")
        return item

    def persist_daily_summary_batch(
        self,
        records: list[DailySummaryRecord],
        outbox_items: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        persisted: list[dict[str, Any]] = []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for record in records:
                    persisted.append(self.upsert_daily_summary_record(record, commit=False))
                for item in outbox_items or []:
                    now = str(item.get("created_at") or utc_now_iso())
                    self._conn.execute(
                        """
                        INSERT INTO delivery_outbox(
                          outbox_id, summary_run_id, job_id, account_id, origin_id, topic_id,
                          chunk_index, chunk_count, content, status, attempts, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)
                        ON CONFLICT(summary_run_id, account_id, origin_id, topic_id, chunk_index)
                        DO NOTHING
                        """,
                        (
                            str(item.get("outbox_id") or f"out_{uuid.uuid4().hex[:16]}"),
                            str(item["summary_run_id"]),
                            item.get("job_id"),
                            str(item["account_id"]),
                            int(item["origin_id"]),
                            int(item.get("topic_id") or 0),
                            int(item["chunk_index"]),
                            int(item["chunk_count"]),
                            str(item["content"]),
                            now,
                            now,
                        ),
                    )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return persisted

    def list_delivery_outbox(
        self,
        *,
        summary_run_id: str | None = None,
        status: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM delivery_outbox"
        clauses: list[str] = []
        params: list[Any] = []
        if summary_run_id:
            clauses.append("summary_run_id = ?")
            params.append(summary_run_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at, chunk_index LIMIT ?"
        params.append(_bounded_limit(limit, max_limit=2000))
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def claim_delivery_outbox(
        self,
        *,
        now: str,
        stale_before: str,
        summary_run_id: str | None = None,
    ) -> dict[str, Any] | None:
        outbox_id: str | None = None
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                clauses = [
                    "(o.status IN ('pending', 'retry') AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= ?))",
                    "(o.status = 'sending' AND o.updated_at < ?)",
                ]
                params: list[Any] = [now, stale_before]
                summary_clause = ""
                if summary_run_id:
                    summary_clause = " AND o.summary_run_id = ?"
                    params.append(summary_run_id)
                row = self._conn.execute(
                    f"""
                    SELECT o.outbox_id
                    FROM delivery_outbox o
                    WHERE ({' OR '.join(clauses)}){summary_clause}
                      AND NOT EXISTS (
                        SELECT 1 FROM delivery_outbox previous
                        WHERE previous.summary_run_id = o.summary_run_id
                          AND previous.account_id = o.account_id
                          AND previous.origin_id = o.origin_id
                          AND previous.topic_id = o.topic_id
                          AND previous.chunk_index < o.chunk_index
                          AND previous.status != 'sent'
                      )
                    ORDER BY o.created_at, o.chunk_index
                    LIMIT 1
                    """,
                    tuple(params),
                ).fetchone()
                if row is not None:
                    outbox_id = str(row["outbox_id"])
                    self._conn.execute(
                        """
                        UPDATE delivery_outbox
                        SET status = 'sending', attempts = attempts + 1, updated_at = ?
                        WHERE outbox_id = ?
                        """,
                        (now, outbox_id),
                    )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        if outbox_id is None:
            return None
        with self._lock:
            row = self._conn.execute("SELECT * FROM delivery_outbox WHERE outbox_id = ?", (outbox_id,)).fetchone()
        return dict(row) if row is not None else None

    def complete_delivery_outbox(self, outbox_id: str, *, message_id: int | None, now: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE delivery_outbox
                SET status = 'sent', message_id = ?, last_error = NULL,
                    next_attempt_at = NULL, sent_at = ?, updated_at = ?
                WHERE outbox_id = ?
                """,
                (message_id, now, now, outbox_id),
            )
            self._conn.commit()

    def retry_delivery_outbox(
        self,
        outbox_id: str,
        *,
        error: str,
        next_attempt_at: str,
        now: str,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE delivery_outbox
                SET status = 'retry', last_error = ?, next_attempt_at = ?, updated_at = ?
                WHERE outbox_id = ?
                """,
                (error, next_attempt_at, now, outbox_id),
            )
            self._conn.commit()

    def get_daily_summary_record(
        self,
        *,
        summary_id: str | None = None,
        run_id: str | None = None,
        include_deleted: bool = False,
    ) -> dict[str, Any] | None:
        clauses: list[str] = []
        params: list[Any] = []
        if summary_id:
            clauses.append("summary_id = ?")
            params.append(summary_id)
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        if not clauses:
            return None
        if not include_deleted:
            clauses.append("deleted_at IS NULL")
        with self._lock:
            row = self._conn.execute(
                """
                SELECT summary_id, run_id, package_run_id, date, timezone, scope_json,
                       tags_json, tags_csv, important, provider, title, content_md, content_json,
                       summary_path, origin_count, group_count, image_count, content_length,
                       deleted_at, created_at, updated_at
                FROM daily_summary_records
                WHERE """ + " AND ".join(clauses) + """
                ORDER BY created_at DESC, summary_id DESC
                LIMIT 1
                """,
                tuple(params),
            ).fetchone()
        if row is None:
            return None
        return _summary_record_from_row(row, include_content=True)

    def list_daily_summary_records(
        self,
        *,
        summary_id: str | None = None,
        run_id: str | None = None,
        package_run_id: str | None = None,
        date: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        provider: str | None = None,
        important: bool | None = None,
        tags: list[str] | None = None,
        q: str | None = None,
        include_deleted: bool = False,
        deleted: bool | None = None,
        include_content: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT summary_id, run_id, package_run_id, date, timezone, scope_json,
                   tags_json, tags_csv, important, provider, title, content_md, content_json,
                   summary_path, origin_count, group_count, image_count, content_length,
                   deleted_at, created_at, updated_at
            FROM daily_summary_records
        """
        clauses: list[str] = []
        params: list[Any] = []
        if summary_id:
            clauses.append("summary_id = ?")
            params.append(summary_id)
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        if package_run_id:
            clauses.append("package_run_id = ?")
            params.append(package_run_id)
        if date:
            clauses.append("date = ?")
            params.append(date)
        if date_from:
            clauses.append("date >= ?")
            params.append(date_from)
        if date_to:
            clauses.append("date <= ?")
            params.append(date_to)
        if provider:
            clauses.append("provider = ?")
            params.append(provider)
        if important is not None:
            clauses.append("important = ?")
            params.append(int(important))
        if q:
            clauses.append("(title LIKE ? OR content_md LIKE ?)")
            params.extend((f"%{q}%", f"%{q}%"))
        if deleted is not None:
            clauses.append("deleted_at IS NOT NULL" if deleted else "deleted_at IS NULL")
        elif not include_deleted:
            clauses.append("deleted_at IS NULL")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT ?"
        requested_limit = _bounded_limit(limit, max_limit=500)
        params.append(500 if tags else requested_limit)
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        required_tags = {_normalize_tag(tag) for tag in (tags or []) if _normalize_tag(tag)}
        items: list[dict[str, Any]] = []
        for row in rows:
            item = _summary_record_from_row(row, include_content=include_content)
            if required_tags:
                item_tags = {_normalize_tag(tag) for tag in item.get("tags") or []}
                if not required_tags.issubset(item_tags):
                    continue
            items.append(item)
            if len(items) >= requested_limit:
                break
        return items

    def set_daily_summary_records_deleted(self, summary_ids: list[str], deleted: bool = True) -> int:
        clean_ids: list[str] = []
        seen: set[str] = set()
        for item in summary_ids:
            summary_id = str(item or "").strip()
            if not summary_id or summary_id in seen:
                continue
            seen.add(summary_id)
            clean_ids.append(summary_id)
        if not clean_ids:
            return 0
        placeholders = ",".join("?" for _ in clean_ids)
        deleted_at = utc_now_iso() if deleted else None
        with self._lock:
            cur = self._conn.execute(
                f"""
                UPDATE daily_summary_records
                SET deleted_at = ?, updated_at = ?
                WHERE summary_id IN ({placeholders})
                """,
                (deleted_at, utc_now_iso(), *clean_ids),
            )
            self._conn.commit()
            return max(cur.rowcount, 0)

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
            SELECT e.id, e.source, e.account_id, e.operation, e.status, e.subject_type, e.subject_id,
                   e.error_code, e.message, e.retry_after, e.occurred_at, e.raw_json,
                   m.chat_id AS subject_chat_id,
                   m.message_id AS subject_message_id,
                   m.topic_id AS subject_topic_id,
                   m.sent_at AS subject_sent_at,
                   m.text AS subject_text,
                   m.media_kind AS subject_media_kind,
                   COALESCE(mc.title, mo.title) AS subject_chat_title,
                   o.origin_id AS subject_origin_id,
                   o.topic_id AS subject_origin_topic_id,
                   o.title AS subject_origin_title,
                   o.origin_type AS subject_origin_type
            FROM operation_events e
            LEFT JOIN messages m
              ON e.subject_type = 'message'
             AND instr(e.subject_id, '/') > 0
             AND m.source = e.source
             AND m.account_id = e.account_id
             AND m.chat_id = CAST(substr(e.subject_id, 1, instr(e.subject_id, '/') - 1) AS INTEGER)
             AND m.message_id = CAST(substr(e.subject_id, instr(e.subject_id, '/') + 1) AS INTEGER)
            LEFT JOIN chats mc
              ON mc.source = m.source
             AND mc.account_id = m.account_id
             AND mc.chat_id = m.chat_id
            LEFT JOIN origins mo
              ON mo.source = m.source
             AND mo.account_id = m.account_id
             AND mo.origin_id = m.chat_id
             AND mo.topic_id = COALESCE(m.topic_id, 0)
            LEFT JOIN origins o
              ON e.subject_type = 'origin'
             AND o.source = e.source
             AND o.account_id = e.account_id
             AND o.origin_id = CAST(e.subject_id AS INTEGER)
             AND o.topic_id = 0
        """
        clauses: list[str] = []
        params: list[Any] = []
        if account_id is not None:
            clauses.append("e.account_id = ?")
            params.append(account_id)
        if status is not None:
            clauses.append("e.status = ?")
            params.append(status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY e.id DESC LIMIT ?"
        params.append(_bounded_limit(limit, max_limit=500))
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [_row_to_dict(row, json_fields={"raw_json"}) for row in rows]

    def delete_operation_events(self, ids: list[int]) -> int:
        clean_ids: list[int] = []
        for item in ids:
            try:
                event_id = int(item)
            except (TypeError, ValueError):
                continue
            if event_id > 0:
                clean_ids.append(event_id)
        if not clean_ids:
            return 0
        placeholders = ",".join("?" for _ in clean_ids)
        with self._lock:
            cur = self._conn.execute(f"DELETE FROM operation_events WHERE id IN ({placeholders})", tuple(clean_ids))
            self._conn.commit()
            return max(cur.rowcount, 0)

    def state(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute("SELECT COALESCE(MAX(seq), 0) AS seq FROM events").fetchone()
            message_count = self._conn.execute("SELECT COUNT(*) AS count FROM messages").fetchone()
            operation_error_count = self._conn.execute(
                "SELECT COUNT(*) AS count FROM operation_events WHERE status IN ('failed', 'partial', 'rate_limited')"
            ).fetchone()
            return {
                "database_id": self.get_meta("database_id"),
                "schema_version": int(self.get_meta("schema_version") or 0),
                "last_event_seq": row["seq"],
                "message_count": message_count["count"],
                "operation_error_count": operation_error_count["count"],
                "server_time": utc_now_iso(),
            }

    def message_raw_json_stats(self, cutoff_sent_at: str | None = None) -> dict[str, int]:
        clauses = ["raw_json IS NOT NULL"]
        params: list[Any] = []
        if cutoff_sent_at is not None:
            clauses.append("sent_at < ?")
            params.append(cutoff_sent_at)
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT
                  COUNT(*) AS message_count,
                  COALESCE(SUM(LENGTH(CAST(raw_json AS BLOB))), 0) AS raw_json_bytes
                FROM messages
                WHERE {" AND ".join(clauses)}
                """,
                tuple(params),
            ).fetchone()
        return {
            "message_count": int(row["message_count"] if row else 0),
            "raw_json_bytes": int(row["raw_json_bytes"] if row else 0),
        }

    def clear_message_raw_json_before(self, cutoff_sent_at: str) -> dict[str, int]:
        before = self.message_raw_json_stats(cutoff_sent_at=cutoff_sent_at)
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE messages
                SET raw_json = NULL
                WHERE raw_json IS NOT NULL
                  AND sent_at < ?
                """,
                (cutoff_sent_at,),
            )
            self._conn.commit()
        return {
            "message_count": int(max(cur.rowcount, 0)),
            "raw_json_bytes": before["raw_json_bytes"],
        }

    def vacuum(self) -> None:
        with self._lock:
            self._conn.commit()
            self._conn.execute("VACUUM")

    def wal_checkpoint_truncate(self) -> dict[str, int]:
        with self._lock:
            row = self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        return {
            "busy": int(row[0]) if row else 0,
            "log": int(row[1]) if row else 0,
            "checkpointed": int(row[2]) if row else 0,
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
                  COALESCE(c.title, o.title) AS chat_title,
                  o.title AS origin_title
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
                  COALESCE(c.title, o.title) AS chat_title,
                  o.title AS origin_title
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
        if self._has_table("origins") and not self._has_column("origins", "important"):
            self._conn.execute("ALTER TABLE origins ADD COLUMN important INTEGER NOT NULL DEFAULT 0")
        if self._has_table("backup_policies") and not self._has_column("backup_policies", "tags"):
            self._conn.execute("ALTER TABLE backup_policies ADD COLUMN tags TEXT")
        self._ensure_column("daily_package_runs", "progress_total", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("daily_package_runs", "progress_current", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("daily_package_runs", "progress_label", "TEXT")
        self._ensure_column("daily_package_runs", "progress_json", "TEXT")
        self._ensure_column("daily_summary_runs", "progress_total", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("daily_summary_runs", "progress_current", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("daily_summary_runs", "progress_label", "TEXT")
        self._ensure_column("daily_summary_runs", "progress_json", "TEXT")
        if not self._has_table("daily_summary_jobs"):
            self._conn.execute(
                """
                CREATE TABLE daily_summary_jobs (
                  job_id TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  date TEXT,
                  timezone TEXT,
                  scope_json TEXT,
                  package_run_id TEXT,
                  summary_run_id TEXT,
                  provider TEXT,
                  progress_total INTEGER NOT NULL DEFAULT 0,
                  progress_current INTEGER NOT NULL DEFAULT 0,
                  progress_label TEXT,
                  progress_json TEXT,
                  cancel_requested_at TEXT,
                  error TEXT,
                  started_at TEXT NOT NULL,
                  finished_at TEXT,
                  updated_at TEXT NOT NULL
                )
                """
            )
        if self._daily_summary_records_run_id_is_unique():
            self._migrate_daily_summary_records_run_id_unique()
        if self._has_table("daily_summary_records") and not self._has_column("daily_summary_records", "tags_csv"):
            self._conn.execute("ALTER TABLE daily_summary_records ADD COLUMN tags_csv TEXT")
            self._conn.execute(
                """
                UPDATE daily_summary_records
                SET tags_csv = (
                  SELECT group_concat(value, ',')
                  FROM json_each(daily_summary_records.tags_json)
                )
                WHERE tags_json IS NOT NULL AND tags_json != '' AND json_valid(tags_json)
                """
            )
        self._ensure_column("daily_summary_records", "deleted_at", "TEXT")
        if self._has_table("messages"):
            self._ensure_message_fts_triggers()

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        if self._has_table(table) and not self._has_column(table, column):
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _daily_summary_records_run_id_is_unique(self) -> bool:
        if not self._has_table("daily_summary_records"):
            return False
        row = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'daily_summary_records'"
        ).fetchone()
        return bool(row and row["sql"] and "run_id TEXT NOT NULL UNIQUE" in str(row["sql"]))

    def _migrate_daily_summary_records_run_id_unique(self) -> None:
        self._conn.executescript(
            """
            BEGIN IMMEDIATE;
            ALTER TABLE daily_summary_records RENAME TO daily_summary_records_old_unique;
            CREATE TABLE daily_summary_records (
              summary_id TEXT PRIMARY KEY,
              run_id TEXT NOT NULL,
              package_run_id TEXT,
              date TEXT,
              timezone TEXT,
              scope_json TEXT,
              tags_json TEXT,
              tags_csv TEXT,
              important INTEGER NOT NULL DEFAULT 0,
              provider TEXT,
              title TEXT,
              content_md TEXT NOT NULL,
              content_json TEXT,
              summary_path TEXT,
              origin_count INTEGER NOT NULL DEFAULT 0,
              group_count INTEGER NOT NULL DEFAULT 0,
              image_count INTEGER NOT NULL DEFAULT 0,
              content_length INTEGER NOT NULL DEFAULT 0,
              deleted_at TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            INSERT INTO daily_summary_records(
              summary_id, run_id, package_run_id, date, timezone, scope_json,
              tags_json, tags_csv, important, provider, title, content_md, content_json,
              summary_path, origin_count, group_count, image_count, content_length,
              deleted_at, created_at, updated_at
            )
            SELECT
              summary_id, run_id, package_run_id, date, timezone, scope_json,
              tags_json,
              (
                SELECT CASE
                  WHEN json_valid(daily_summary_records_old_unique.tags_json)
                  THEN (SELECT group_concat(value, ',') FROM json_each(daily_summary_records_old_unique.tags_json))
                  ELSE NULL
                END
              ),
              important, provider, title, content_md, content_json,
              summary_path, origin_count, group_count, image_count, content_length,
              NULL, created_at, updated_at
            FROM daily_summary_records_old_unique;
            DROP TABLE daily_summary_records_old_unique;
            CREATE INDEX IF NOT EXISTS idx_daily_summary_records_date ON daily_summary_records(date, created_at);
            CREATE INDEX IF NOT EXISTS idx_daily_summary_records_package ON daily_summary_records(package_run_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_daily_summary_records_important ON daily_summary_records(important, created_at);
            COMMIT;
            """
        )

    def _ensure_message_fts_triggers(self) -> None:
        self._conn.executescript(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS message_fts USING fts5(
              text,
              sender_name,
              content='messages',
              content_rowid='rowid'
            );
            DROP TRIGGER IF EXISTS messages_ai;
            DROP TRIGGER IF EXISTS messages_ad;
            DROP TRIGGER IF EXISTS messages_au;
            CREATE TRIGGER messages_ai AFTER INSERT ON messages BEGIN
              INSERT INTO message_fts(rowid, text, sender_name)
              VALUES (new.rowid, COALESCE(new.text, ''), COALESCE(new.sender_name, ''));
            END;
            CREATE TRIGGER messages_ad AFTER DELETE ON messages BEGIN
              INSERT INTO message_fts(message_fts, rowid, text, sender_name)
              VALUES ('delete', old.rowid, COALESCE(old.text, ''), COALESCE(old.sender_name, ''));
            END;
            CREATE TRIGGER messages_au AFTER UPDATE ON messages
            WHEN old.text IS NOT new.text OR old.sender_name IS NOT new.sender_name
            BEGIN
              INSERT INTO message_fts(message_fts, rowid, text, sender_name)
              VALUES ('delete', old.rowid, COALESCE(old.text, ''), COALESCE(old.sender_name, ''));
              INSERT INTO message_fts(rowid, text, sender_name)
              VALUES (new.rowid, COALESCE(new.text, ''), COALESCE(new.sender_name, ''));
            END;
            """
        )

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


def _summary_record_from_row(row: sqlite3.Row, *, include_content: bool) -> dict[str, Any]:
    data = _row_to_dict(row, json_fields={"scope_json", "tags_json", "content_json"}, bool_fields={"important"})
    data["scope"] = data.pop("scope_json") or {}
    data["tags"] = data.pop("tags_json") or []
    if not data.get("tags_csv"):
        data["tags_csv"] = ",".join(str(tag) for tag in data["tags"])
    content = str(data.get("content_md") or "")
    data["content_preview"] = content[:240]
    data["content_length"] = int(data.get("content_length") or len(content))
    data["deleted"] = bool(data.get("deleted_at"))
    if not include_content:
        data.pop("content_md", None)
        data.pop("content_json", None)
    return data


def _summary_job_from_row(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row, json_fields={"scope_json", "progress_json", "request_json"})
    data["scope"] = data.pop("scope_json") or {}
    data["progress"] = data.pop("progress_json") or {}
    data["request"] = data.pop("request_json") or {}
    return data


def _tags_csv_from_json(tags_json: str | None) -> str | None:
    if not tags_json:
        return None
    try:
        value = json.loads(tags_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(value, list):
        return None
    return ",".join(str(tag).strip() for tag in value if str(tag).strip())


def _normalize_tag(tag: Any) -> str:
    return str(tag or "").strip().lower()


def _bounded_limit(limit: int, max_limit: int = 1000) -> int:
    if limit <= 0:
        return 1
    return min(limit, max_limit)
