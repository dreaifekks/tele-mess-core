from __future__ import annotations

from datetime import datetime, timedelta, timezone as dt_timezone
import functools
import json
import re
import shutil
import sqlite3
import threading
import uuid
from importlib import resources
from pathlib import Path
from typing import Any, Callable, Iterable

from tele_mess_core.models import (
    DEFAULT_TIMEZONE,
    AccountAuthRecord,
    AccountRecord,
    BackupPolicyRecord,
    CaptureCursorRecord,
    ChatRecord,
    DailyMessagePointRecord,
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
    SOURCE_TELEGRAM,
    UserRecord,
    utc_now_iso,
)
from tele_mess_core.archive.migrations import _migration_20, _migration_21, apply_migrations, outbox_send_random_id
from tele_mess_core.media import (
    classify_media_type,
    media_file_extension,
    media_filename,
    media_filename_contains,
    normalize_file_extension_filter,
    normalize_media_type_filter,
)


SCHEMA_VERSION = 21
UPGRADE_PENDING_META = "upgrade_pending"
LEGACY_SCHEMA_BASELINE = 12

# Compact message projection shared by the read-only message queries; a topic's
# origin_title is prefixed with its parent group title.
_MESSAGE_VIEW_COLUMNS = """
    m.rowid AS row_id, m.account_id, m.chat_id, m.message_id, m.topic_id,
    m.sender_name, m.sender_username, m.sent_at, m.edited_at, m.deleted_at, m.text,
    m.media_kind, m.reply_to_message_id, m.forward_from_name, m.permalink,
    COALESCE(parent.title || ' / ' || o.title, o.title, c.title) AS origin_title
"""
_MESSAGE_VIEW_JOINS = """
    LEFT JOIN chats c
      ON c.source = m.source
     AND c.account_id = m.account_id
     AND c.chat_id = m.chat_id
    LEFT JOIN origins o
      ON o.source = m.source
     AND o.account_id = m.account_id
     AND o.origin_id = m.chat_id
     AND o.topic_id = COALESCE(m.topic_id, 0)
    LEFT JOIN origins parent
      ON parent.source = o.source
     AND parent.account_id = o.account_id
     AND parent.origin_id = o.origin_id
     AND parent.topic_id = 0
     AND o.topic_id != 0
"""


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

    def _open_connection(self, *, timeout: float = 30.0, busy_timeout_ms: int = 30000) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.database_path), check_same_thread=False, timeout=timeout)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
        connection.create_function("tmc_media_filename", 1, media_filename, deterministic=True)
        connection.create_function("tmc_media_file_extension", 1, media_file_extension, deterministic=True)
        connection.create_function("tmc_media_type", 3, classify_media_type, deterministic=True)
        connection.create_function("tmc_media_filename_contains", 2, media_filename_contains, deterministic=True)
        return connection

    @property
    def _conn(self) -> sqlite3.Connection:
        if self._closed:
            raise RuntimeError("ArchiveStore is closed")
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            return connection
        connection = self._open_connection()
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
        """Create or upgrade the archive schema; a current database is left untouched."""
        with self._lock:
            existing_schema_version = self._validate_existing_schema_version()
            self._conn.execute("PRAGMA journal_mode = WAL")
            if (
                existing_schema_version == SCHEMA_VERSION
                and self.get_meta(UPGRADE_PENDING_META) is None
                and self._has_schema_objects()
            ):
                # An open of an earlier release interrupted after its last
                # migration can leave the archive without its ID.
                if self.get_meta("database_id") is None:
                    self.set_meta("database_id", str(uuid.uuid4()))
                    self._conn.commit()
                return
            is_empty = self._conn.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone() is None
            if is_empty:
                try:
                    self._conn.executescript(_schema_sql())
                    if self.get_meta("database_id") is None:
                        self.set_meta("database_id", str(uuid.uuid4()))
                    self.set_meta("schema_version", str(SCHEMA_VERSION))
                    self._conn.execute("DELETE FROM meta WHERE key = ?", (UPGRADE_PENDING_META,))
                    self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                    self._conn.commit()
                except Exception:
                    self._conn.rollback()
                    raise
                return

            if str(self.database_path) != ":memory:":
                self._initialize_exclusive_upgrade(existing_schema_version)
                return

            try:
                self._upgrade_schema(existing_schema_version)
                if self.get_meta("database_id") is None:
                    self.set_meta("database_id", str(uuid.uuid4()))
                self.set_meta("schema_version", str(SCHEMA_VERSION))
                self._conn.execute("DELETE FROM meta WHERE key = ?", (UPGRADE_PENDING_META,))
                self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def _initialize_exclusive_upgrade(self, existing_schema_version: int) -> None:
        """Hold an exclusive SQLite lock across a schema upgrade so active older services are never broken."""
        conn = self._conn
        conn.execute("PRAGMA busy_timeout = 1500")
        conn.execute("PRAGMA locking_mode = EXCLUSIVE")
        try:
            conn.execute("BEGIN EXCLUSIVE")
        except sqlite3.OperationalError as exc:
            conn.execute("PRAGMA locking_mode = NORMAL")
            conn.execute("PRAGMA busy_timeout = 30000")
            raise RuntimeError(
                f"Cannot upgrade archive schema from v{existing_schema_version} to v{SCHEMA_VERSION} "
                "while another process has the database open; stop running services before upgrading."
            ) from exc
        try:
            self._upgrade_schema(existing_schema_version)
            if self.get_meta("database_id") is None:
                self.set_meta("database_id", str(uuid.uuid4()))
            self.set_meta("schema_version", str(SCHEMA_VERSION))
            conn.execute("DELETE FROM meta WHERE key = ?", (UPGRADE_PENDING_META,))
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            self.close_thread_connection()

    def _upgrade_schema(self, existing_schema_version: int) -> None:
        tables_sql, object_statements = _schema_sql_parts()
        message_fts_existed = self._has_table("message_fts")
        if self._has_table("messages") and not self._has_column("messages", "account_id"):
            self._migrate_v1_to_v2()
        else:
            self._conn.executescript(tables_sql)
        # Migrations commit one by one; the marker keeps the fast path closed
        # until the triggers and the full-text index are rebuilt as well.
        self.set_meta(UPGRADE_PENDING_META, "1")
        self._conn.commit()
        self._ensure_current_schema()
        current_version = self.get_meta("schema_version")
        apply_migrations(
            self._conn,
            max(int(current_version) if current_version else existing_schema_version, LEGACY_SCHEMA_BASELINE),
            SCHEMA_VERSION,
        )
        # Replacing the triggers and checking the index share one write
        # transaction with the final version stamp in initialize(), so no
        # message can be written while the full-text triggers are missing.
        self._conn.execute("BEGIN IMMEDIATE")
        for trigger in ("messages_ai", "messages_ad", "messages_au"):
            self._conn.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        for statement in object_statements:
            self._conn.execute(statement)
        if not message_fts_existed or not self._message_fts_integrity_is_valid():
            self._rebuild_message_fts()

    def _has_schema_objects(self) -> bool:
        """Whether every table, index, and trigger of the snapshot exists; an older
        release recreated them on each open, so an interrupted open can leave one missing."""
        existing = {str(row[0]) for row in self._conn.execute("SELECT name FROM sqlite_master")}
        if not (
            _schema_object_names() <= existing
            and self._has_column("delivery_outbox", "claim_token")
            and self._has_column("delivery_outbox", "send_random_id")
            and self._has_column("media_download_jobs", "replace_existing")
            and self._has_column("media_download_jobs", "target_edited_at")
            and self._has_column("media_download_jobs", "pending_file_path")
        ):
            return False
        missing_random = self._conn.execute(
            "SELECT 1 FROM delivery_outbox WHERE send_random_id IS NULL LIMIT 1"
        ).fetchone()
        return missing_random is None

    def _rebuild_message_fts(self) -> None:
        self._conn.execute("INSERT INTO message_fts(message_fts) VALUES('rebuild')")

    def _validate_existing_schema_version(self) -> int:
        user_version = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
        meta_version = 0
        if self._has_table("meta"):
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is not None:
                try:
                    meta_version = int(row["value"])
                except (TypeError, ValueError) as exc:
                    raise RuntimeError("Database schema_version metadata is invalid") from exc
        newest = max(user_version, meta_version)
        if newest > SCHEMA_VERSION:
            raise RuntimeError(
                f"Database schema version {newest} is newer than supported version {SCHEMA_VERSION}"
            )
        if user_version and meta_version and user_version != meta_version:
            raise RuntimeError(
                "Database schema version metadata is inconsistent: "
                f"meta={meta_version}, user_version={user_version}"
            )
        return newest

    def _message_fts_integrity_is_valid(self) -> bool:
        try:
            self._conn.execute(
                "INSERT INTO message_fts(message_fts, rank) VALUES('integrity-check', 1)"
            )
        except sqlite3.DatabaseError:
            return False
        return True

    def get_meta(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def _upsert_account_unlocked(self, account: AccountRecord, now: str) -> None:
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

    def upsert_account(self, account: AccountRecord) -> None:
        now = account.updated_at or utc_now_iso()
        seq_key = f"auth_state_seq:{account.source}:{account.account_id}"
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT value FROM meta WHERE key = ?",
                    (seq_key,),
                ).fetchone()
                existing_seq, existing_owner, _existing_epoch = _parse_auth_state_meta(
                    row["value"] if row is not None else None
                )
                if existing_owner == "__deleted__":
                    self._conn.execute(
                        """
                        INSERT INTO meta(key, value) VALUES (?, ?)
                        ON CONFLICT(key) DO UPDATE SET value = excluded.value
                        """,
                        (
                            seq_key,
                            json.dumps(
                                {"seq": existing_seq + 1, "owner": "", "epoch": 0},
                                separators=(",", ":"),
                            ),
                        ),
                    )
                self._upsert_account_unlocked(account, now)
                self._conn.commit()
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def get_auth_state_seq(self, source: str, account_id: str) -> int:
        seq_key = f"auth_state_seq:{source}:{account_id}"
        seq, _owner, _epoch = _parse_auth_state_meta(self.get_meta(seq_key))
        return seq

    def upsert_account_auth(
        self,
        auth: AccountAuthRecord,
        *,
        expected_seq: int | None = None,
        owner_id: str | None = None,
        owner_epoch: int | None = None,
        extra_meta: dict[str, str] | None = None,
        account_record: AccountRecord | None = None,
    ) -> bool:
        now = auth.updated_at or utc_now_iso()
        seq_key = f"auth_state_seq:{auth.source}:{auth.account_id}"
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT value FROM meta WHERE key = ?",
                    (seq_key,),
                ).fetchone()
                existing_seq, existing_owner, existing_epoch = _parse_auth_state_meta(
                    row["value"] if row is not None else None
                )
                if owner_id is not None and existing_owner == "__deleted__":
                    self._conn.rollback()
                    return False
                if expected_seq is not None:
                    same_owner_newer = (
                        owner_id is not None
                        and existing_owner == owner_id
                        and owner_epoch is not None
                        and int(owner_epoch) > existing_epoch
                    )
                    if existing_seq != int(expected_seq) and not same_owner_newer:
                        self._conn.rollback()
                        return False
                elif (
                    owner_id is not None
                    and owner_epoch is not None
                    and existing_owner == owner_id
                    and int(owner_epoch) <= existing_epoch
                ):
                    self._conn.rollback()
                    return False
                target_seq = existing_seq + 1
                meta_value = json.dumps(
                    {
                        "seq": target_seq,
                        "owner": owner_id or "",
                        "epoch": int(owner_epoch) if owner_epoch is not None else 0,
                    },
                    separators=(",", ":"),
                )
                self._conn.execute(
                    """
                    INSERT INTO meta(key, value) VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (seq_key, meta_value),
                )
                if extra_meta:
                    for key, value in extra_meta.items():
                        self._conn.execute(
                            """
                            INSERT INTO meta(key, value) VALUES (?, ?)
                            ON CONFLICT(key) DO UPDATE SET value = excluded.value
                            """,
                            (str(key), str(value)),
                        )
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
                if account_record is not None:
                    self._upsert_account_unlocked(account_record, account_record.updated_at or now)
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def delete_management_account(self, source: str, account_id: str) -> int:
        deleted = 0
        deleted_job_ids: set[str] = set()
        seq_key = f"auth_state_seq:{source}:{account_id}"
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now = utc_now_iso()
                seq_row = self._conn.execute(
                    "SELECT value FROM meta WHERE key = ?",
                    (seq_key,),
                ).fetchone()
                existing_seq, _existing_owner, _existing_epoch = _parse_auth_state_meta(
                    seq_row["value"] if seq_row is not None else None
                )
                self._conn.execute(
                    """
                    INSERT INTO meta(key, value) VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (
                        seq_key,
                        json.dumps(
                            {"seq": existing_seq + 1, "owner": "__deleted__", "epoch": 0},
                            separators=(",", ":"),
                        ),
                    ),
                )
                deleted_job_ids = {
                    str(row["job_id"])
                    for row in self._conn.execute(
                        "SELECT job_id FROM media_download_jobs WHERE source = ? AND account_id = ?",
                        (source, account_id),
                    ).fetchall()
                }
                pending_rows = self._conn.execute(
                    """
                    SELECT job_id, status, pending_file_path
                    FROM media_download_jobs
                    WHERE source = ? AND account_id = ?
                      AND pending_file_path IS NOT NULL
                    """,
                    (source, account_id),
                ).fetchall()
                for row in pending_rows:
                    remaining = self._clear_pending_path_value_unlocked(row["pending_file_path"])
                    if remaining is None:
                        self._conn.execute(
                            "UPDATE media_download_jobs SET pending_file_path = NULL WHERE job_id = ?",
                            (row["job_id"],),
                        )
                    else:
                        self._conn.execute(
                            """
                            UPDATE media_download_jobs
                            SET status = 'canceled',
                                replace_existing = 0,
                                claim_token = CASE WHEN status = 'downloading' THEN claim_token + 1 ELSE claim_token END,
                                pending_file_path = ?,
                                next_attempt_at = NULL,
                                last_error = 'account deleted with pending file cleanup',
                                error_code = 'account_deleted',
                                updated_at = ?,
                                completed_at = COALESCE(completed_at, ?)
                            WHERE job_id = ?
                            """,
                            (remaining, now, now, row["job_id"]),
                        )
                for sql in (
                    "DELETE FROM account_auth WHERE source = ? AND account_id = ?",
                    "DELETE FROM origins WHERE source = ? AND account_id = ?",
                    "DELETE FROM backup_policies WHERE source = ? AND account_id = ?",
                    "DELETE FROM participants WHERE source = ? AND account_id = ?",
                    "DELETE FROM capture_cursors WHERE source = ? AND account_id = ?",
                    "DELETE FROM media_download_jobs WHERE source = ? AND account_id = ? AND pending_file_path IS NULL",
                    "DELETE FROM operation_events WHERE source = ? AND account_id = ?",
                    "DELETE FROM accounts WHERE source = ? AND account_id = ?",
                ):
                    cur = self._conn.execute(sql, (source, account_id))
                    deleted += max(cur.rowcount, 0)
                self._conn.commit()
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise
        if source == SOURCE_TELEGRAM:
            self.cleanup_orphan_media_staging_dirs(
                account_id=account_id,
                source=source,
                stale_seconds=0.0,
                include_deleted_accounts=False,
                allow_fencing_downloading=False,
            )
        elif deleted_job_ids:
            self.cleanup_orphan_media_staging_dirs(
                account_id=account_id,
                source=source,
                stale_seconds=0.0,
                include_deleted_accounts=False,
                allow_fencing_downloading=False,
                only_job_ids=deleted_job_ids,
            )
        return deleted

    def list_management_accounts(self) -> list[dict[str, Any]]:
        return self._management_accounts("", ())

    def get_management_account(self, source: str, account_id: str) -> dict[str, Any] | None:
        items = self._management_accounts("WHERE source = ? AND account_id = ?", (source, account_id))
        return items[0] if items else None

    def _management_accounts(self, where: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._lock:
            account_rows = self._conn.execute(
                f"""
                SELECT source, account_id, display_name, kind, updated_at, raw_json
                FROM accounts
                {where}
                ORDER BY source, account_id
                """,
                params,
            ).fetchall()
            auth_rows = self._conn.execute(
                f"""
                SELECT source, account_id, auth_state, phone, session_name, session_dir,
                       last_error, updated_at AS auth_updated_at, raw_json AS auth_raw_json
                FROM account_auth
                {where}
                ORDER BY source, account_id
                """,
                params,
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
        clauses: list[str] = []
        params: list[Any] = []
        if account_id is not None:
            clauses.append("o.account_id = ?")
            params.append(account_id)
        if not include_archived:
            clauses.append("o.archived_at IS NULL")
            clauses.append("(o.topic_id = 0 OR parent.archived_at IS NULL)")
        return self._origins(clauses, params)

    def get_origin(self, source: str, account_id: str, origin_id: int, topic_id: int = 0) -> dict[str, Any] | None:
        items = self._origins(
            ["o.source = ?", "o.account_id = ?", "o.origin_id = ?", "o.topic_id = ?"],
            [source, account_id, origin_id, topic_id],
        )
        return items[0] if items else None

    def _origins(self, clauses: list[str], params: list[Any]) -> list[dict[str, Any]]:
        sql = """
            SELECT
              o.source, o.account_id, o.origin_id, o.topic_id, o.origin_type,
              o.parent_origin_id, o.title, o.username, o.is_forum,
              o.important, o.archived_at, o.last_message_at, o.discovered_at, o.updated_at, o.raw_json,
              parent.archived_at AS parent_archived_at,
              p.enabled AS backup_enabled,
              p.capture_text,
              p.capture_media_metadata,
              p.download_media,
              p.download_stickers,
              p.tags,
              p.updated_at AS policy_updated_at,
              pp.enabled AS parent_backup_enabled,
              pp.capture_text AS parent_capture_text,
              pp.capture_media_metadata AS parent_capture_media_metadata,
              pp.download_media AS parent_download_media,
              pp.download_stickers AS parent_download_stickers,
              pp.tags AS parent_tags,
              pp.updated_at AS parent_policy_updated_at
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
            LEFT JOIN backup_policies pp
              ON o.topic_id != 0
             AND pp.source = o.source
             AND pp.account_id = o.account_id
             AND pp.origin_id = o.origin_id
             AND pp.topic_id = 0
        """
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
                bool_fields={
                    "is_forum",
                    "important",
                    "backup_enabled",
                    "capture_text",
                    "capture_media_metadata",
                    "download_media",
                    "download_stickers",
                    "parent_backup_enabled",
                    "parent_capture_text",
                    "parent_capture_media_metadata",
                    "parent_download_media",
                    "parent_download_stickers",
                },
            )
            parent_archived_at = data.pop("parent_archived_at", None)
            backup_enabled = data.pop("backup_enabled")
            if backup_enabled is None:
                self_policy: dict[str, Any] | None = None
                data.pop("capture_text", None)
                data.pop("capture_media_metadata", None)
                data.pop("download_media", None)
                data.pop("download_stickers", None)
                data.pop("tags", None)
                data.pop("policy_updated_at", None)
            else:
                self_policy = {
                    "enabled": backup_enabled,
                    "capture_text": data.pop("capture_text"),
                    "capture_media_metadata": data.pop("capture_media_metadata"),
                    "download_media": data.pop("download_media"),
                    "download_stickers": data.pop("download_stickers"),
                    "tags": data.pop("tags"),
                    "updated_at": data.pop("policy_updated_at"),
                }
            parent_backup_enabled = data.pop("parent_backup_enabled")
            if parent_backup_enabled is None:
                parent_policy: dict[str, Any] | None = None
                data.pop("parent_capture_text", None)
                data.pop("parent_capture_media_metadata", None)
                data.pop("parent_download_media", None)
                data.pop("parent_download_stickers", None)
                data.pop("parent_tags", None)
                data.pop("parent_policy_updated_at", None)
            else:
                parent_policy = {
                    "enabled": parent_backup_enabled,
                    "capture_text": data.pop("parent_capture_text"),
                    "capture_media_metadata": data.pop("parent_capture_media_metadata"),
                    "download_media": data.pop("parent_download_media"),
                    "download_stickers": data.pop("parent_download_stickers"),
                    "tags": data.pop("parent_tags"),
                    "updated_at": data.pop("parent_policy_updated_at"),
                }
            data["backup_policy"] = self_policy
            effective_policy, policy_source = resolve_effective_policy(
                self_policy,
                parent_policy,
                topic_id=int(data.get("topic_id") or 0),
                self_archived=data.get("archived_at") is not None,
                parent_archived=parent_archived_at is not None,
            )
            data["effective_backup_policy"] = effective_policy
            data["policy_source"] = policy_source
            items.append(data)
        return items

    def _resolve_effective_backup_policy_unlocked(
        self,
        source: str,
        account_id: str,
        origin_id: int,
        topic_id: int = 0,
    ) -> tuple[dict[str, Any], str]:
        tid = int(topic_id or 0)
        oid = int(origin_id)
        if tid:
            origin_rows = self._conn.execute(
                """
                SELECT topic_id, archived_at
                FROM origins
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id IN (0, ?)
                """,
                (source, account_id, oid, tid),
            ).fetchall()
            policy_rows = self._conn.execute(
                """
                SELECT topic_id, enabled, capture_text, capture_media_metadata,
                       download_media, download_stickers, tags, updated_at
                FROM backup_policies
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id IN (0, ?)
                """,
                (source, account_id, oid, tid),
            ).fetchall()
        else:
            origin_rows = self._conn.execute(
                """
                SELECT topic_id, archived_at
                FROM origins
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = 0
                """,
                (source, account_id, oid),
            ).fetchall()
            policy_rows = self._conn.execute(
                """
                SELECT topic_id, enabled, capture_text, capture_media_metadata,
                       download_media, download_stickers, tags, updated_at
                FROM backup_policies
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = 0
                """,
                (source, account_id, oid),
            ).fetchall()
        archived_by_topic = {int(row["topic_id"]): row["archived_at"] is not None for row in origin_rows}
        policy_by_topic: dict[int, dict[str, Any]] = {}
        for row in policy_rows:
            row_tid = int(row["topic_id"])
            policy_by_topic[row_tid] = {
                "enabled": bool(row["enabled"]),
                "capture_text": bool(row["capture_text"]),
                "capture_media_metadata": bool(row["capture_media_metadata"]),
                "download_media": bool(row["download_media"]),
                "download_stickers": bool(row["download_stickers"]),
                "tags": row["tags"],
                "updated_at": row["updated_at"],
            }
        return resolve_effective_policy(
            policy_by_topic.get(tid),
            policy_by_topic.get(0) if tid else None,
            topic_id=tid,
            self_archived=archived_by_topic.get(tid, False),
            parent_archived=archived_by_topic.get(0, False) if tid else False,
        )

    def resolve_effective_backup_policy_with_source(
        self,
        source: str,
        account_id: str,
        origin_id: int,
        topic_id: int = 0,
    ) -> tuple[dict[str, Any], str]:
        with self._lock:
            return self._resolve_effective_backup_policy_unlocked(
                source,
                account_id,
                origin_id,
                topic_id=topic_id,
            )

    def resolve_effective_backup_policy(
        self,
        source: str,
        account_id: str,
        origin_id: int,
        topic_id: int = 0,
    ) -> dict[str, Any]:
        policy, _policy_source = self.resolve_effective_backup_policy_with_source(
            source,
            account_id,
            origin_id,
            topic_id=topic_id,
        )
        return policy

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
                  capture_text, capture_media_metadata, download_media,
                  download_stickers, tags, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, account_id, origin_id, topic_id) DO UPDATE SET
                  enabled = excluded.enabled,
                  capture_text = excluded.capture_text,
                  capture_media_metadata = excluded.capture_media_metadata,
                  download_media = excluded.download_media,
                  download_stickers = excluded.download_stickers,
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
                    int(policy.download_stickers),
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
                   capture_text, capture_media_metadata, download_media,
                   download_stickers, tags, updated_at
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
                bool_fields={
                    "enabled",
                    "capture_text",
                    "capture_media_metadata",
                    "download_media",
                    "download_stickers",
                },
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
                       capture_text, capture_media_metadata, download_media,
                       download_stickers, tags, updated_at
                FROM backup_policies
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = ?
                """,
                (source, account_id, origin_id, topic_id),
            ).fetchone()
        if row is None:
            return None
        return _row_to_dict(
            row,
            bool_fields={
                "enabled",
                "capture_text",
                "capture_media_metadata",
                "download_media",
                "download_stickers",
            },
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
                       history_scanned_through_id, observed_max_message_id,
                       last_message_at, last_backfill_at, backfill_head_message_id,
                       backfill_status, backfill_error, backfill_count, updated_at, raw_json
                FROM capture_cursors
                WHERE source = ? AND account_id = ? AND origin_id = ? AND topic_id = ?
                """,
                (source, account_id, origin_id, topic_id),
            ).fetchone()
        return _row_to_dict(row, json_fields={"raw_json"}) if row else None

    def upsert_capture_cursor(self, cursor: CaptureCursorRecord) -> None:
        now = cursor.updated_at or utc_now_iso()
        observed_max_message_id = max(
            cursor.last_message_id,
            cursor.observed_max_message_id or 0,
        )
        history_scanned_through_id = cursor.history_scanned_through_id or 0
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO capture_cursors(
                  source, account_id, origin_id, topic_id, last_message_id,
                  history_scanned_through_id, observed_max_message_id,
                  last_message_at, last_backfill_at, backfill_head_message_id,
                  backfill_status, backfill_error, backfill_count, updated_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, account_id, origin_id, topic_id) DO UPDATE SET
                  last_message_id = MAX(
                    capture_cursors.last_message_id,
                    capture_cursors.observed_max_message_id,
                    excluded.last_message_id,
                    excluded.observed_max_message_id
                  ),
                  history_scanned_through_id = MAX(
                    capture_cursors.history_scanned_through_id,
                    excluded.history_scanned_through_id
                  ),
                  observed_max_message_id = MAX(
                    capture_cursors.last_message_id,
                    capture_cursors.observed_max_message_id,
                    excluded.last_message_id,
                    excluded.observed_max_message_id
                  ),
                  last_message_at = CASE
                    WHEN excluded.last_message_at IS NULL THEN capture_cursors.last_message_at
                    WHEN capture_cursors.last_message_at IS NULL
                      OR excluded.last_message_at > capture_cursors.last_message_at
                    THEN excluded.last_message_at
                    ELSE capture_cursors.last_message_at
                  END,
                  last_backfill_at = CASE
                    WHEN excluded.last_backfill_at IS NULL THEN capture_cursors.last_backfill_at
                    WHEN capture_cursors.last_backfill_at IS NULL
                      OR excluded.last_backfill_at > capture_cursors.last_backfill_at
                    THEN excluded.last_backfill_at
                    ELSE capture_cursors.last_backfill_at
                  END,
                  backfill_head_message_id = CASE
                    WHEN excluded.backfill_head_message_id IS NULL
                    THEN capture_cursors.backfill_head_message_id
                    WHEN capture_cursors.backfill_head_message_id IS NULL
                      OR excluded.backfill_head_message_id > capture_cursors.backfill_head_message_id
                    THEN excluded.backfill_head_message_id
                    ELSE capture_cursors.backfill_head_message_id
                  END,
                  backfill_status = COALESCE(excluded.backfill_status, capture_cursors.backfill_status),
                  backfill_error = COALESCE(excluded.backfill_error, capture_cursors.backfill_error),
                  backfill_count = COALESCE(excluded.backfill_count, capture_cursors.backfill_count),
                  updated_at = excluded.updated_at,
                  raw_json = COALESCE(excluded.raw_json, capture_cursors.raw_json)
                """,
                (
                    cursor.source,
                    cursor.account_id,
                    cursor.origin_id,
                    cursor.topic_id,
                    observed_max_message_id,
                    history_scanned_through_id,
                    observed_max_message_id,
                    cursor.last_message_at,
                    cursor.last_backfill_at,
                    cursor.backfill_head_message_id,
                    cursor.backfill_status,
                    cursor.backfill_error,
                    cursor.backfill_count,
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
                   c.history_scanned_through_id, c.observed_max_message_id,
                   c.last_message_at, c.last_backfill_at, c.backfill_head_message_id,
                   c.backfill_status, c.backfill_error, c.backfill_count,
                   c.updated_at, c.raw_json,
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

    def get_participant(self, source: str, account_id: str, origin_id: int, user_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT source, account_id, origin_id, user_id, username, display_name,
                       is_bot, role, last_seen_at, updated_at, raw_json
                FROM participants
                WHERE source = ? AND account_id = ? AND origin_id = ? AND user_id = ?
                """,
                (source, account_id, origin_id, user_id),
            ).fetchone()
        if row is None:
            return None
        return _row_to_dict(row, json_fields={"raw_json"}, bool_fields={"is_bot"})

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
            self._upsert_message_row_unlocked(message, event_type, now)
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

    def upsert_message_if_policy_enabled(
        self,
        *,
        source: str,
        account_id: str,
        chat_id: int,
        topic_id: int,
        event_type: str,
        build_record: Callable[[dict[str, Any]], MessageRecord],
        build_media_job: Callable[[dict[str, Any], MessageRecord], dict[str, Any] | None] | None = None,
    ) -> tuple[int, dict[str, Any]] | None:
        """Atomically resolve effective backup policy, build the message from it, and insert/update in one write transaction."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                effective_policy, _source = self._resolve_effective_backup_policy_unlocked(
                    source,
                    account_id,
                    chat_id,
                    topic_id=int(topic_id or 0),
                )
                if not effective_policy.get("enabled"):
                    self._conn.rollback()
                    return None
                message = build_record(effective_policy)
                payload = _message_payload(message)
                now = message.ingested_at or utc_now_iso()
                prev_row = self._conn.execute(
                    """
                    SELECT edited_at, deleted_at
                    FROM messages
                    WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
                    """,
                    (message.source, message.account_id, message.chat_id, message.message_id),
                ).fetchone()
                prev_edited_at = str(prev_row["edited_at"]) if (prev_row is not None and prev_row["edited_at"]) else None
                prev_deleted = prev_row is not None and prev_row["deleted_at"] is not None
                has_newer_edit = (
                    prev_row is not None
                    and message.edited_at is not None
                    and (prev_edited_at is None or str(message.edited_at) > prev_edited_at)
                )
                is_stale_edit = (
                    prev_edited_at is not None
                    and (message.edited_at is None or str(message.edited_at) < prev_edited_at)
                )
                accepted_edited_at = prev_edited_at if is_stale_edit else (message.edited_at or prev_edited_at)
                self._upsert_message_row_unlocked(message, event_type, now)
                event_seq = self._insert_event(
                    source=message.source,
                    account_id=message.account_id,
                    event_type=event_type,
                    chat_id=message.chat_id,
                    message_id=message.message_id,
                    event_at=now,
                    payload=payload,
                )
                if build_media_job is not None and not is_stale_edit and not prev_deleted:
                    job_spec = build_media_job(effective_policy, message)
                    should_replace = has_newer_edit or (event_type == "edit")
                    if job_spec is not None:
                        self._enqueue_media_download_job_unlocked(
                            source=message.source,
                            account_id=message.account_id,
                            chat_id=message.chat_id,
                            message_id=message.message_id,
                            topic_id=int(message.topic_id or 0),
                            media_kind=job_spec.get("media_kind"),
                            is_sticker_like=bool(job_spec.get("is_sticker_like", False)),
                            max_attempts=int(job_spec.get("max_attempts", 8)),
                            replace_existing=bool(job_spec.get("replace_existing", False)) or should_replace,
                            target_edited_at=accepted_edited_at,
                            now=now,
                        )
                    elif should_replace:
                        self._invalidate_media_jobs_on_edit_unlocked(
                            source=message.source,
                            account_id=message.account_id,
                            chat_id=message.chat_id,
                            message_id=message.message_id,
                            reason="media_removed_on_edit",
                            now=now,
                        )
                self._conn.commit()
                return event_seq, effective_policy
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def get_message(
        self,
        source: str,
        account_id: str,
        chat_id: int,
        message_id: int,
    ) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT source, account_id, chat_id, message_id, topic_id, sender_id, sender_name,
                       sender_username, sent_at, edited_at, ingested_at, deleted_at, text,
                       has_media, media_kind, grouped_id, reply_to_message_id, forward_from_id,
                       forward_from_name, permalink, reactions_json, raw_json, version
                FROM messages
                WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
                """,
                (source, account_id, chat_id, message_id),
            ).fetchone()
        if row is None:
            return None
        return _row_to_dict(row, json_fields={"reactions_json", "raw_json"}, bool_fields={"has_media"})

    def _upsert_message_row_unlocked(self, message: MessageRecord, event_type: str, now: str) -> None:
        self._conn.execute(
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
              edited_at = CASE
                WHEN excluded.edited_at IS NULL THEN messages.edited_at
                WHEN messages.edited_at IS NULL OR excluded.edited_at >= messages.edited_at
                THEN excluded.edited_at
                ELSE messages.edited_at
              END,
              ingested_at = CASE
                WHEN excluded.ingested_at >= messages.ingested_at THEN excluded.ingested_at
                ELSE messages.ingested_at
              END,
              deleted_at = CASE
                WHEN excluded.deleted_at IS NULL THEN messages.deleted_at
                WHEN messages.deleted_at IS NULL OR excluded.deleted_at >= messages.deleted_at
                THEN excluded.deleted_at
                ELSE messages.deleted_at
              END,
              text = CASE
                WHEN messages.edited_at IS NULL
                  OR (excluded.edited_at IS NOT NULL AND excluded.edited_at >= messages.edited_at)
                THEN excluded.text ELSE messages.text
              END,
              has_media = CASE
                WHEN messages.edited_at IS NULL
                  OR (excluded.edited_at IS NOT NULL AND excluded.edited_at >= messages.edited_at)
                THEN excluded.has_media ELSE messages.has_media
              END,
              media_kind = CASE
                WHEN messages.edited_at IS NULL
                  OR (excluded.edited_at IS NOT NULL AND excluded.edited_at >= messages.edited_at)
                THEN excluded.media_kind ELSE messages.media_kind
              END,
              grouped_id = CASE
                WHEN messages.edited_at IS NULL
                  OR (excluded.edited_at IS NOT NULL AND excluded.edited_at >= messages.edited_at)
                THEN excluded.grouped_id ELSE messages.grouped_id
              END,
              reply_to_message_id = CASE
                WHEN messages.edited_at IS NULL
                  OR (excluded.edited_at IS NOT NULL AND excluded.edited_at >= messages.edited_at)
                THEN excluded.reply_to_message_id ELSE messages.reply_to_message_id
              END,
              forward_from_id = CASE
                WHEN messages.edited_at IS NULL
                  OR (excluded.edited_at IS NOT NULL AND excluded.edited_at >= messages.edited_at)
                THEN excluded.forward_from_id ELSE messages.forward_from_id
              END,
              forward_from_name = CASE
                WHEN messages.edited_at IS NULL
                  OR (excluded.edited_at IS NOT NULL AND excluded.edited_at >= messages.edited_at)
                THEN excluded.forward_from_name ELSE messages.forward_from_name
              END,
              permalink = COALESCE(excluded.permalink, messages.permalink),
              reactions_json = CASE
                WHEN ? = 'backfill'
                THEN COALESCE(messages.reactions_json, excluded.reactions_json)
                ELSE COALESCE(excluded.reactions_json, messages.reactions_json)
              END,
              raw_json = CASE
                WHEN messages.edited_at IS NULL
                  OR (excluded.edited_at IS NOT NULL AND excluded.edited_at >= messages.edited_at)
                THEN excluded.raw_json ELSE messages.raw_json
              END,
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
                event_type,
            ),
        )

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
            try:
                for message_id in message_ids:
                    cur = self._conn.execute(
                        """
                        UPDATE messages
                        SET deleted_at = ?, ingested_at = ?, version = version + 1
                        WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
                        """,
                        (at, at, source, account_id, chat_id, message_id),
                    )
                    if cur.rowcount == 0:
                        continue
                    self._cancel_pending_media_jobs_for_message_unlocked(
                        source=source,
                        account_id=account_id,
                        chat_id=chat_id,
                        message_id=message_id,
                        reason="message_deleted",
                        now=at,
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
                if seqs:
                    self._conn.commit()
                elif self._conn.in_transaction:
                    self._conn.rollback()
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise
        return seqs

    def mark_deleted_without_chat(
        self,
        source: str,
        account_id: str,
        message_ids: list[int],
        event_at: str | None = None,
        raw_payload: dict[str, Any] | None = None,
        inflight_chat_ids_by_message: dict[int, set[int]] | None = None,
    ) -> list[int]:
        """Mark deleted non-channel messages (DM / basic group UpdateDeleteMessages with chat_id=None) when unique."""
        seqs, _resolved = self.resolve_and_mark_deleted_without_chat(
            source=source,
            account_id=account_id,
            message_ids=message_ids,
            inflight_chat_ids_by_message=inflight_chat_ids_by_message,
            event_at=event_at,
            raw_payload=raw_payload,
        )
        return seqs

    def resolve_and_mark_deleted_without_chat(
        self,
        source: str,
        account_id: str,
        message_ids: list[int],
        *,
        inflight_chat_ids_by_message: dict[int, set[int]] | None = None,
        event_at: str | None = None,
        raw_payload: dict[str, Any] | None = None,
    ) -> tuple[list[int], dict[int, int]]:
        """Atomically merge DB and in-flight non-channel candidates and mark/return only unique chats."""
        at = event_at or utc_now_iso()
        seqs: list[int] = []
        resolved: dict[int, int] = {}
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for message_id in message_ids:
                    matches = self._conn.execute(
                        """
                        SELECT DISTINCT chat_id
                        FROM messages
                        WHERE source = ? AND account_id = ? AND message_id = ? AND chat_id > -1000000000000
                        LIMIT 2
                        """,
                        (source, account_id, message_id),
                    ).fetchall()
                    db_chat_ids = {int(row["chat_id"]) for row in matches}
                    mem_chat_ids = {
                        int(cid)
                        for cid in (inflight_chat_ids_by_message or {}).get(message_id, ())
                        if int(cid) > -1000000000000
                    }
                    combined_chat_ids = db_chat_ids | mem_chat_ids
                    if len(combined_chat_ids) != 1:
                        continue
                    chat_id = next(iter(combined_chat_ids))
                    resolved[message_id] = chat_id
                    if chat_id in db_chat_ids:
                        cur = self._conn.execute(
                            """
                            UPDATE messages
                            SET deleted_at = ?, ingested_at = ?, version = version + 1
                            WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
                            """,
                            (at, at, source, account_id, chat_id, message_id),
                        )
                        if cur.rowcount > 0:
                            self._cancel_pending_media_jobs_for_message_unlocked(
                                source=source,
                                account_id=account_id,
                                chat_id=chat_id,
                                message_id=message_id,
                                reason="message_deleted",
                                now=at,
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
                if seqs:
                    self._conn.commit()
                else:
                    self._conn.rollback()
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise
        return seqs, resolved

    def update_reactions(
        self,
        source: str,
        chat_id: int,
        message_id: int,
        reactions: Any,
        event_at: str | None = None,
        raw_payload: dict[str, Any] | None = None,
        account_id: str = "default",
    ) -> int | None:
        at = event_at or utc_now_iso()
        reactions_json = json.dumps(reactions, ensure_ascii=False, default=str)
        with self._lock:
            try:
                cur = self._conn.execute(
                    """
                    UPDATE messages
                    SET reactions_json = ?, ingested_at = ?, version = version + 1
                    WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
                    """,
                    (reactions_json, at, source, account_id, chat_id, message_id),
                )
                if cur.rowcount == 0:
                    if self._conn.in_transaction:
                        self._conn.rollback()
                    return None
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
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise


    def _upsert_media_file_row_unlocked(self, media_file: MediaFileRecord) -> None:
        downloaded_at = media_file.downloaded_at or utc_now_iso()
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

    def upsert_media_file(self, media_file: MediaFileRecord) -> None:
        with self._lock:
            self._upsert_media_file_row_unlocked(media_file)
            self._conn.commit()

    def upsert_media_file_if_policy_allows(
        self,
        *,
        source: str,
        account_id: str,
        chat_id: int,
        topic_id: int = 0,
        is_sticker_like: bool = False,
        prepare_media: Callable[[], MediaFileRecord],
        rollback_media: Callable[[MediaFileRecord], None] | None = None,
    ) -> MediaFileRecord | None:
        """Atomically verify effective media policy, publish staged media, and insert media_files in one transaction."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            published: MediaFileRecord | None = None
            try:
                effective_policy, _policy_source = self._resolve_effective_backup_policy_unlocked(
                    source,
                    account_id,
                    chat_id,
                    topic_id=int(topic_id or 0),
                )
                allowed = bool(effective_policy.get("enabled")) and bool(
                    effective_policy.get("download_stickers" if is_sticker_like else "download_media")
                )
                if not allowed:
                    self._conn.rollback()
                    return None
                published = prepare_media()
                self._upsert_media_file_row_unlocked(published)
                self._conn.commit()
                return published
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                if published is not None and rollback_media is not None:
                    try:
                        rollback_media(published)
                    except OSError:
                        pass
                raise

    def _cancel_pending_media_jobs_for_message_unlocked(
        self,
        *,
        source: str,
        account_id: str,
        chat_id: int,
        message_id: int,
        reason: str,
        now: str,
    ) -> None:
        self._conn.execute(
            """
            UPDATE media_download_jobs
            SET status = 'canceled',
                next_attempt_at = NULL,
                last_error = ?,
                error_code = ?,
                updated_at = ?,
                completed_at = ?
            WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
              AND status IN ('pending', 'retry')
            """,
            (reason, reason, now, now, source, account_id, chat_id, message_id),
        )

    def _invalidate_media_jobs_on_edit_unlocked(
        self,
        *,
        source: str,
        account_id: str,
        chat_id: int,
        message_id: int,
        reason: str,
        now: str,
    ) -> None:
        self._conn.execute(
            """
            UPDATE media_download_jobs
            SET status = 'canceled',
                claim_token = CASE WHEN status = 'downloading' THEN claim_token + 1 ELSE claim_token END,
                replace_existing = 0,
                next_attempt_at = NULL,
                last_error = ?,
                error_code = ?,
                updated_at = ?,
                completed_at = ?
            WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
              AND status IN ('pending', 'retry', 'downloading')
            """,
            (reason, reason, now, now, source, account_id, chat_id, message_id),
        )

    def _media_file_path_referenced_unlocked(self, file_path: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM media_files WHERE file_path = ? LIMIT 1",
            (file_path,),
        ).fetchone()
        return row is not None

    def _enqueue_media_download_job_unlocked(
        self,
        *,
        source: str,
        account_id: str,
        chat_id: int,
        message_id: int,
        topic_id: int = 0,
        media_kind: str | None = None,
        is_sticker_like: bool = False,
        max_attempts: int = 8,
        requeue_failed: bool = False,
        replace_existing: bool = False,
        target_edited_at: str | None = None,
        now: str | None = None,
    ) -> dict[str, Any] | None:
        if not replace_existing:
            existing_file = self._conn.execute(
                """
                SELECT 1 FROM media_files
                WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
                LIMIT 1
                """,
                (source, account_id, chat_id, message_id),
            ).fetchone()
            if existing_file is not None:
                return None
        deleted_msg = self._conn.execute(
            """
            SELECT 1 FROM messages
            WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
              AND deleted_at IS NOT NULL
            LIMIT 1
            """,
            (source, account_id, chat_id, message_id),
        ).fetchone()
        if deleted_msg is not None:
            return None
        ts = now or utc_now_iso()
        attempts_limit = max(1, int(max_attempts))
        sticker_flag = 1 if is_sticker_like else 0
        replace_flag = 1 if replace_existing else 0
        existing = self._conn.execute(
            """
            SELECT * FROM media_download_jobs
            WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
            """,
            (source, account_id, chat_id, message_id),
        ).fetchone()
        if existing is not None:
            status = str(existing["status"])
            if replace_existing:
                # An edited attachment invalidates any completed/failed/in-flight attempt for an older version.
                self._conn.execute(
                    """
                    UPDATE media_download_jobs
                    SET topic_id = ?,
                        media_kind = ?,
                        is_sticker_like = ?,
                        replace_existing = 1,
                        target_edited_at = ?,
                        status = 'pending',
                        attempts = 0,
                        max_attempts = ?,
                        claim_token = CASE WHEN status = 'downloading' THEN claim_token + 1 ELSE claim_token END,
                        next_attempt_at = NULL,
                        last_error = NULL,
                        error_code = NULL,
                        updated_at = ?,
                        completed_at = NULL
                    WHERE job_id = ?
                    """,
                    (
                        int(topic_id or 0),
                        media_kind,
                        sticker_flag,
                        target_edited_at,
                        attempts_limit,
                        ts,
                        existing["job_id"],
                    ),
                )
                refreshed = self._conn.execute(
                    "SELECT * FROM media_download_jobs WHERE job_id = ?",
                    (existing["job_id"],),
                ).fetchone()
                return _row_to_dict(refreshed, bool_fields={"is_sticker_like", "replace_existing"}) if refreshed else None
            if status in ("pending", "downloading", "retry", "completed"):
                return _row_to_dict(existing, bool_fields={"is_sticker_like", "replace_existing"})
            if status == "failed" and not requeue_failed:
                return _row_to_dict(existing, bool_fields={"is_sticker_like", "replace_existing"})
            self._conn.execute(
                """
                UPDATE media_download_jobs
                SET topic_id = ?,
                    media_kind = ?,
                    is_sticker_like = ?,
                    replace_existing = ?,
                    target_edited_at = ?,
                    status = 'pending',
                    attempts = 0,
                    max_attempts = ?,
                    next_attempt_at = NULL,
                    last_error = NULL,
                    error_code = NULL,
                    updated_at = ?,
                    completed_at = NULL
                WHERE job_id = ?
                """,
                (
                    int(topic_id or 0),
                    media_kind,
                    sticker_flag,
                    replace_flag,
                    target_edited_at,
                    attempts_limit,
                    ts,
                    existing["job_id"],
                ),
            )
            refreshed = self._conn.execute(
                "SELECT * FROM media_download_jobs WHERE job_id = ?",
                (existing["job_id"],),
            ).fetchone()
            return _row_to_dict(refreshed, bool_fields={"is_sticker_like", "replace_existing"}) if refreshed else None

        job_id = f"mdj_{uuid.uuid4().hex[:12]}"
        self._conn.execute(
            """
            INSERT INTO media_download_jobs(
              job_id, source, account_id, chat_id, message_id, topic_id, media_kind,
              is_sticker_like, replace_existing, target_edited_at, pending_file_path,
              status, attempts, max_attempts, claim_token,
              next_attempt_at, last_error, error_code, created_at, updated_at, completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'pending', 0, ?, 0, NULL, NULL, NULL, ?, ?, NULL)
            """,
            (
                job_id,
                source,
                account_id,
                int(chat_id),
                int(message_id),
                int(topic_id or 0),
                media_kind,
                sticker_flag,
                replace_flag,
                target_edited_at,
                attempts_limit,
                ts,
                ts,
            ),
        )
        inserted = self._conn.execute(
            "SELECT * FROM media_download_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        return _row_to_dict(inserted, bool_fields={"is_sticker_like", "replace_existing"}) if inserted else None

    def enqueue_media_download_job(
        self,
        *,
        source: str = SOURCE_TELEGRAM,
        account_id: str,
        chat_id: int,
        message_id: int,
        topic_id: int = 0,
        media_kind: str | None = None,
        is_sticker_like: bool = False,
        max_attempts: int = 8,
        requeue_failed: bool = False,
        replace_existing: bool = False,
        target_edited_at: str | None = None,
    ) -> dict[str, Any] | None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now = utc_now_iso()
                before_changes = self._conn.total_changes
                job = self._enqueue_media_download_job_unlocked(
                    source=source,
                    account_id=account_id,
                    chat_id=chat_id,
                    message_id=message_id,
                    topic_id=topic_id,
                    media_kind=media_kind,
                    is_sticker_like=is_sticker_like,
                    max_attempts=max_attempts,
                    requeue_failed=requeue_failed,
                    replace_existing=replace_existing,
                    target_edited_at=target_edited_at,
                    now=now,
                )
                if self._conn.total_changes > before_changes:
                    self._conn.commit()
                else:
                    self._conn.rollback()
                return job
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def claim_media_download_job(
        self,
        account_id: str,
        *,
        source: str = SOURCE_TELEGRAM,
        stale_seconds: float = 300.0,
    ) -> dict[str, Any] | None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now_dt = datetime.now(dt_timezone.utc)
                now = now_dt.isoformat()
                stale_before = (now_dt - timedelta(seconds=max(0.0, float(stale_seconds)))).isoformat()
                expired = self._conn.execute(
                    """
                    UPDATE media_download_jobs
                    SET status = 'failed',
                        error_code = COALESCE(error_code, 'media_download_stale_exhausted'),
                        last_error = COALESCE(last_error, 'stale download lease exhausted max attempts'),
                        next_attempt_at = NULL,
                        updated_at = ?,
                        completed_at = ?
                    WHERE source = ?
                      AND account_id = ?
                      AND status = 'downloading'
                      AND updated_at <= ?
                      AND attempts >= max_attempts
                    """,
                    (now, now, source, account_id, stale_before),
                )
                row = self._conn.execute(
                    """
                    SELECT *
                    FROM media_download_jobs
                    WHERE source = ?
                      AND account_id = ?
                      AND (
                        (status IN ('pending', 'retry') AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
                        OR (status = 'downloading' AND updated_at <= ? AND attempts < max_attempts)
                      )
                    ORDER BY COALESCE(next_attempt_at, created_at) ASC, created_at ASC, job_id ASC
                    LIMIT 1
                    """,
                    (source, account_id, now, stale_before),
                ).fetchone()
                if row is None:
                    if expired.rowcount > 0:
                        self._conn.commit()
                    else:
                        self._conn.rollback()
                    return None
                cur = self._conn.execute(
                    """
                    UPDATE media_download_jobs
                    SET status = 'downloading',
                        claim_token = claim_token + 1,
                        attempts = attempts + 1,
                        next_attempt_at = NULL,
                        updated_at = ?
                    WHERE job_id = ? AND claim_token = ?
                    """,
                    (now, row["job_id"], int(row["claim_token"])),
                )
                if cur.rowcount == 0:
                    if expired.rowcount > 0:
                        self._conn.commit()
                    else:
                        self._conn.rollback()
                    return None
                claimed = self._conn.execute(
                    "SELECT * FROM media_download_jobs WHERE job_id = ?",
                    (row["job_id"],),
                ).fetchone()
                self._conn.commit()
                return _row_to_dict(claimed, bool_fields={"is_sticker_like", "replace_existing"}) if claimed else None
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def next_media_download_due_at(
        self,
        account_id: str,
        *,
        source: str = SOURCE_TELEGRAM,
        stale_seconds: float = 300.0,
    ) -> str | None:
        with self._lock:
            immediate = self._conn.execute(
                """
                SELECT 1
                FROM media_download_jobs
                WHERE source = ? AND account_id = ?
                  AND status IN ('pending', 'retry')
                  AND next_attempt_at IS NULL
                LIMIT 1
                """,
                (source, account_id),
            ).fetchone()
            if immediate is not None:
                return utc_now_iso()
            next_row = self._conn.execute(
                """
                SELECT MIN(next_attempt_at) AS due_at
                FROM media_download_jobs
                WHERE source = ? AND account_id = ?
                  AND status IN ('pending', 'retry')
                  AND next_attempt_at IS NOT NULL
                """,
                (source, account_id),
            ).fetchone()
            downloading_row = self._conn.execute(
                """
                SELECT MIN(updated_at) AS oldest_updated_at
                FROM media_download_jobs
                WHERE source = ? AND account_id = ?
                  AND status = 'downloading'
                """,
                (source, account_id),
            ).fetchone()
        candidates: list[str] = []
        if next_row is not None and next_row["due_at"]:
            candidates.append(str(next_row["due_at"]))
        if downloading_row is not None and downloading_row["oldest_updated_at"]:
            try:
                started = datetime.fromisoformat(str(downloading_row["oldest_updated_at"]))
                if started.tzinfo is None:
                    started = started.replace(tzinfo=dt_timezone.utc)
                stale_due = (started + timedelta(seconds=max(0.0, float(stale_seconds)))).isoformat()
                candidates.append(stale_due)
            except ValueError:
                pass
        return min(candidates) if candidates else None

    def heartbeat_media_download_job(self, job_id: str, *, expected_claim_token: int) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now = utc_now_iso()
                cur = self._conn.execute(
                    """
                    UPDATE media_download_jobs
                    SET updated_at = ?
                    WHERE job_id = ? AND status = 'downloading' AND claim_token = ?
                    """,
                    (now, job_id, int(expected_claim_token)),
                )
                if cur.rowcount > 0:
                    self._conn.commit()
                    return True
                self._conn.rollback()
                return False
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def record_media_download_pending_path(
        self,
        job_id: str,
        *,
        expected_claim_token: int,
        pending_file_path: str,
    ) -> str:
        """Persist an exclusive target file path before os.replace; returns ok, stale_claim, pending_uncleaned, or path_conflict."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now = utc_now_iso()
                row = self._conn.execute(
                    """
                    SELECT pending_file_path
                    FROM media_download_jobs
                    WHERE job_id = ? AND status = 'downloading' AND claim_token = ?
                    """,
                    (job_id, int(expected_claim_token)),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return "stale_claim"
                existing_pending = row["pending_file_path"]
                if existing_pending and str(existing_pending) != pending_file_path:
                    self._conn.rollback()
                    return "pending_uncleaned"
                conflict_file = self._conn.execute(
                    "SELECT 1 FROM media_files WHERE file_path = ? LIMIT 1",
                    (pending_file_path,),
                ).fetchone()
                conflict_job = self._conn.execute(
                    "SELECT 1 FROM media_download_jobs WHERE pending_file_path = ? AND job_id != ? LIMIT 1",
                    (pending_file_path, job_id),
                ).fetchone()
                if conflict_file is not None or conflict_job is not None:
                    self._conn.rollback()
                    return "path_conflict"
                cur = self._conn.execute(
                    """
                    UPDATE media_download_jobs
                    SET pending_file_path = ?,
                        updated_at = ?
                    WHERE job_id = ? AND status = 'downloading' AND claim_token = ?
                      AND (pending_file_path IS NULL OR pending_file_path = ?)
                    """,
                    (pending_file_path, now, job_id, int(expected_claim_token), pending_file_path),
                )
                if cur.rowcount > 0:
                    self._conn.commit()
                    return "ok"
                self._conn.rollback()
                return "stale_claim"
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def cleanup_media_download_pending_path(
        self,
        job_id: str,
        *,
        expected_claim_token: int,
        require_downloading: bool = True,
        stale_seconds: float | None = None,
    ) -> tuple[bool, str | None]:
        """Unlink an uncommitted pending_file_path and clear SQLite only once removal is confirmed.

        Returns `(cleared_or_absent, unlinked_path)`. When `cleared_or_absent` is False,
        the pending path could not be unlinked and remains recorded for a future retry.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now_dt = datetime.now(dt_timezone.utc)
                row = self._conn.execute(
                    """
                    SELECT status, claim_token, updated_at, pending_file_path, error_code
                    FROM media_download_jobs
                    WHERE job_id = ? AND claim_token = ?
                    """,
                    (job_id, int(expected_claim_token)),
                ).fetchone()
                if row is None or not row["pending_file_path"]:
                    self._conn.rollback()
                    return True, None
                status = str(row["status"])
                error_code = str(row["error_code"] or "")
                if require_downloading:
                    if status != "downloading":
                        self._conn.rollback()
                        return True, None
                elif status == "downloading":
                    if stale_seconds is None:
                        self._conn.rollback()
                        return True, None
                    stale_before = (now_dt - timedelta(seconds=max(0.0, float(stale_seconds)))).isoformat()
                    if str(row["updated_at"]) > stale_before:
                        self._conn.rollback()
                        return True, None
                candidate = str(row["pending_file_path"])
                referenced = self._media_file_path_referenced_unlocked(candidate)
                if not referenced:
                    if not _unlink_file_confirmed(candidate):
                        self._conn.rollback()
                        return False, None
                if status == "canceled" and error_code == "account_deleted":
                    self._conn.execute(
                        """
                        DELETE FROM media_download_jobs
                        WHERE job_id = ? AND claim_token = ?
                        """,
                        (job_id, int(expected_claim_token)),
                    )
                else:
                    self._conn.execute(
                        """
                        UPDATE media_download_jobs
                        SET pending_file_path = NULL
                        WHERE job_id = ? AND claim_token = ?
                        """,
                        (job_id, int(expected_claim_token)),
                    )
                self._conn.commit()
                return True, (None if referenced else candidate)
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def fence_stale_media_download_claim(
        self,
        job_id: str,
        *,
        expected_claim_token: int,
        stale_seconds: float = 300.0,
        source: str | None = None,
        account_id: str | None = None,
        allow_fencing_downloading: bool = True,
    ) -> bool:
        """Atomically verify or invalidate a claim before deleting its staging directory.

        Returns True only when the job no longer exists, is not downloading (for the
        matching source/account), has advanced past `expected_claim_token`, or was stale
        and has now been fenced in SQLite.
        """
        with self._lock:
            now_dt = datetime.now(dt_timezone.utc)
            stale_before = (
                now_dt - timedelta(seconds=max(0.0, float(stale_seconds)))
            ).isoformat()
            # Lock-free read fast-path avoids write-lock contention for live or already-finished claims.
            quick = self._conn.execute(
                """
                SELECT source, account_id, status, claim_token, updated_at
                FROM media_download_jobs
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
            if quick is None:
                return True
            if source is not None and str(quick["source"]) != source:
                return False
            if account_id is not None and str(quick["account_id"]) != account_id:
                return False
            quick_status = str(quick["status"])
            quick_token = int(quick["claim_token"])
            if quick_status != "downloading" or quick_token > int(expected_claim_token):
                return True
            if (
                not allow_fencing_downloading
                or quick_token < int(expected_claim_token)
                or str(quick["updated_at"]) > stale_before
            ):
                return False

            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now_dt = datetime.now(dt_timezone.utc)
                now = now_dt.isoformat()
                stale_before = (
                    now_dt - timedelta(seconds=max(0.0, float(stale_seconds)))
                ).isoformat()
                row = self._conn.execute(
                    """
                    SELECT source, account_id, status, claim_token, attempts, max_attempts, updated_at
                    FROM media_download_jobs
                    WHERE job_id = ?
                    """,
                    (job_id,),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return True
                if source is not None and str(row["source"]) != source:
                    self._conn.rollback()
                    return False
                if account_id is not None and str(row["account_id"]) != account_id:
                    self._conn.rollback()
                    return False
                status = str(row["status"])
                current_token = int(row["claim_token"])
                if status != "downloading" or current_token > int(expected_claim_token):
                    self._conn.rollback()
                    return True
                if (
                    not allow_fencing_downloading
                    or current_token < int(expected_claim_token)
                    or str(row["updated_at"]) > stale_before
                ):
                    self._conn.rollback()
                    return False
                attempts = int(row["attempts"])
                max_attempts = int(row["max_attempts"])
                if attempts >= max_attempts:
                    cur = self._conn.execute(
                        """
                        UPDATE media_download_jobs
                        SET status = 'failed',
                            claim_token = claim_token + 1,
                            error_code = COALESCE(error_code, 'media_download_stale_exhausted'),
                            last_error = COALESCE(last_error, 'stale download lease exhausted max attempts'),
                            next_attempt_at = NULL,
                            updated_at = ?,
                            completed_at = ?
                        WHERE job_id = ? AND status = 'downloading' AND claim_token = ? AND updated_at <= ?
                        """,
                        (now, now, job_id, int(expected_claim_token), stale_before),
                    )
                else:
                    cur = self._conn.execute(
                        """
                        UPDATE media_download_jobs
                        SET status = 'retry',
                            claim_token = claim_token + 1,
                            next_attempt_at = ?,
                            error_code = COALESCE(error_code, 'media_download_stale'),
                            last_error = COALESCE(last_error, 'stale download lease fenced during cleanup'),
                            updated_at = ?
                        WHERE job_id = ? AND status = 'downloading' AND claim_token = ? AND updated_at <= ?
                        """,
                        (now, now, job_id, int(expected_claim_token), stale_before),
                    )
                if cur.rowcount > 0:
                    self._conn.commit()
                    return True
                self._conn.rollback()
                return False
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def list_media_jobs_with_pending_paths(
        self,
        *,
        account_id: str | None = None,
        source: str = SOURCE_TELEGRAM,
        stale_seconds: float = 300.0,
        include_deleted_accounts: bool = True,
        deleted_accounts_only: bool = False,
    ) -> list[dict[str, Any]]:
        stale_before = (
            datetime.now(dt_timezone.utc) - timedelta(seconds=max(0.0, float(stale_seconds)))
        ).isoformat()
        with self._lock:
            if deleted_accounts_only:
                rows = self._conn.execute(
                    """
                    SELECT job_id, account_id, claim_token, status, updated_at, pending_file_path, error_code
                    FROM media_download_jobs
                    WHERE source = ?
                      AND pending_file_path IS NOT NULL
                      AND status = 'canceled'
                      AND error_code = 'account_deleted'
                      AND NOT EXISTS (
                        SELECT 1 FROM accounts a
                        WHERE a.source = media_download_jobs.source
                          AND a.account_id = media_download_jobs.account_id
                      )
                    """,
                    (source,),
                ).fetchall()
            elif account_id is None:
                rows = self._conn.execute(
                    """
                    SELECT job_id, account_id, claim_token, status, updated_at, pending_file_path, error_code
                    FROM media_download_jobs
                    WHERE source = ?
                      AND pending_file_path IS NOT NULL
                      AND (status != 'downloading' OR updated_at <= ?)
                    """,
                    (source, stale_before),
                ).fetchall()
            elif include_deleted_accounts:
                rows = self._conn.execute(
                    """
                    SELECT job_id, account_id, claim_token, status, updated_at, pending_file_path, error_code
                    FROM media_download_jobs
                    WHERE source = ?
                      AND pending_file_path IS NOT NULL
                      AND (
                        (account_id = ? AND (status != 'downloading' OR updated_at <= ?))
                        OR (
                          status = 'canceled'
                          AND error_code = 'account_deleted'
                          AND NOT EXISTS (
                            SELECT 1 FROM accounts a
                            WHERE a.source = media_download_jobs.source
                              AND a.account_id = media_download_jobs.account_id
                          )
                        )
                      )
                    """,
                    (source, account_id, stale_before),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT job_id, account_id, claim_token, status, updated_at, pending_file_path, error_code
                    FROM media_download_jobs
                    WHERE source = ? AND account_id = ?
                      AND pending_file_path IS NOT NULL
                      AND (status != 'downloading' OR updated_at <= ?)
                    """,
                    (source, account_id, stale_before),
                ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def cleanup_orphan_media_pending_paths(
        self,
        *,
        account_id: str | None = None,
        source: str = SOURCE_TELEGRAM,
        stale_seconds: float = 300.0,
        include_deleted_accounts: bool = True,
        deleted_accounts_only: bool = False,
        stop_event: threading.Event | None = None,
    ) -> bool:
        """Clean up DB-tracked pending/superseded media paths; returns True when retryable items remain."""
        if stop_event is not None and stop_event.is_set():
            return False
        has_remaining = False
        for pending in self.list_media_jobs_with_pending_paths(
            account_id=account_id,
            source=source,
            stale_seconds=stale_seconds,
            include_deleted_accounts=include_deleted_accounts,
            deleted_accounts_only=deleted_accounts_only,
        ):
            if stop_event is not None and stop_event.is_set():
                return True
            cleared, _unlinked = self.cleanup_media_download_pending_path(
                str(pending["job_id"]),
                expected_claim_token=int(pending["claim_token"]),
                require_downloading=False,
                stale_seconds=stale_seconds,
            )
            if not cleared:
                has_remaining = True
        return has_remaining

    def cleanup_orphan_media_staging_dirs(
        self,
        *,
        account_id: str | None = None,
        source: str = SOURCE_TELEGRAM,
        stale_seconds: float = 300.0,
        include_deleted_accounts: bool = True,
        deleted_accounts_only: bool = False,
        active_account_ids: Iterable[str] | None = None,
        allow_fencing_downloading: bool = True,
        only_job_ids: set[str] | None = None,
        stop_event: threading.Event | None = None,
    ) -> bool:
        """Remove fenced/stale `.part-<job_id>-t<token>` staging directories; returns True when retryable items remain."""
        if stop_event is not None and stop_event.is_set():
            return False
        has_remaining = False
        media_root = self.database_path.parent / "media"
        if not media_root.exists():
            return False
        active_accounts: set[str] = set(active_account_ids or ())
        with self._lock:
            rows = self._conn.execute(
                "SELECT account_id FROM accounts WHERE source = ?",
                (source,),
            ).fetchall()
            active_accounts.update(str(row["account_id"]) for row in rows)

        account_dirs: list[Path] = []
        seen_names: set[str] = set()
        if not deleted_accounts_only and account_id is not None:
            own_dir = media_root / account_id
            if own_dir.is_dir():
                account_dirs.append(own_dir)
                seen_names.add(account_id)
        if deleted_accounts_only or account_id is None or include_deleted_accounts:
            try:
                children = list(media_root.iterdir())
            except OSError:
                return True
            for child in children:
                if stop_event is not None and stop_event.is_set():
                    return True
                if not child.is_dir() or child.name in seen_names:
                    continue
                if deleted_accounts_only or (account_id is not None and include_deleted_accounts):
                    if child.name in active_accounts:
                        continue
                account_dirs.append(child)
                seen_names.add(child.name)

        for acc_dir in account_dirs:
            if stop_event is not None and stop_event.is_set():
                return True
            try:
                chat_dirs = list(acc_dir.iterdir())
            except FileNotFoundError:
                continue
            except OSError:
                has_remaining = True
                continue
            for chat_dir in chat_dirs:
                if stop_event is not None and stop_event.is_set():
                    return True
                if not chat_dir.is_dir():
                    continue
                try:
                    entries = list(chat_dir.iterdir())
                except FileNotFoundError:
                    continue
                except OSError:
                    has_remaining = True
                    continue
                for entry in entries:
                    if stop_event is not None and stop_event.is_set():
                        return True
                    if not entry.name.startswith(".part-") or not entry.is_dir():
                        continue
                    parsed = _parse_media_staging_dir_name(entry.name)
                    if parsed is None:
                        continue
                    job_id, token = parsed
                    if only_job_ids is not None and job_id not in only_job_ids:
                        continue
                    if not self.fence_stale_media_download_claim(
                        job_id,
                        expected_claim_token=token,
                        stale_seconds=stale_seconds,
                        source=source,
                        account_id=acc_dir.name,
                        allow_fencing_downloading=allow_fencing_downloading,
                    ):
                        continue
                    try:
                        shutil.rmtree(entry)
                    except FileNotFoundError:
                        pass
                    except OSError:
                        has_remaining = True
        return has_remaining

    def cleanup_deleted_account_media_artifacts(
        self,
        *,
        source: str = SOURCE_TELEGRAM,
        stale_seconds: float = 300.0,
        active_account_ids: Iterable[str] | None = None,
        stop_event: threading.Event | None = None,
    ) -> bool:
        """Clean up both staging dirs and pending paths left behind by deleted accounts."""
        if stop_event is not None and stop_event.is_set():
            return False
        has_remaining_fs = self.cleanup_orphan_media_staging_dirs(
            source=source,
            stale_seconds=stale_seconds,
            deleted_accounts_only=True,
            active_account_ids=active_account_ids,
            stop_event=stop_event,
        )
        if stop_event is not None and stop_event.is_set():
            return has_remaining_fs
        has_remaining_pending = self.cleanup_orphan_media_pending_paths(
            source=source,
            stale_seconds=stale_seconds,
            deleted_accounts_only=True,
            stop_event=stop_event,
        )
        return has_remaining_fs or has_remaining_pending

    def _clear_pending_path_value_unlocked(self, pending_path: str | None) -> str | None:
        """Return NULL if pending_path is absent, referenced, or confirmed unlinked; else preserve it for retry."""
        if not pending_path:
            return None
        if self._media_file_path_referenced_unlocked(pending_path):
            return None
        if _unlink_file_confirmed(pending_path):
            return None
        return pending_path

    def complete_media_download_job(self, job_id: str, *, expected_claim_token: int) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now = utc_now_iso()
                row = self._conn.execute(
                    """
                    SELECT pending_file_path
                    FROM media_download_jobs
                    WHERE job_id = ? AND status = 'downloading' AND claim_token = ?
                    """,
                    (job_id, int(expected_claim_token)),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return False
                remaining_pending = self._clear_pending_path_value_unlocked(row["pending_file_path"])
                cur = self._conn.execute(
                    """
                    UPDATE media_download_jobs
                    SET status = 'completed',
                        replace_existing = 0,
                        pending_file_path = ?,
                        next_attempt_at = NULL,
                        last_error = NULL,
                        error_code = NULL,
                        updated_at = ?,
                        completed_at = ?
                    WHERE job_id = ? AND status = 'downloading' AND claim_token = ?
                    """,
                    (remaining_pending, now, now, job_id, int(expected_claim_token)),
                )
                if cur.rowcount > 0:
                    self._conn.commit()
                    return True
                self._conn.rollback()
                return False
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def complete_media_download_with_file(
        self,
        *,
        job_id: str,
        expected_claim_token: int,
        source: str,
        account_id: str,
        chat_id: int,
        topic_id: int = 0,
        is_sticker_like: bool = False,
        prepare_media: Callable[[], MediaFileRecord],
        rollback_media: Callable[[MediaFileRecord], None] | None = None,
    ) -> tuple[str, MediaFileRecord | None]:
        """Atomically fence claim_token, verify message & effective policy, publish media, emit sync event, and complete job."""
        superseded_file_path: str | None = None
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            published: MediaFileRecord | None = None
            try:
                now = utc_now_iso()
                job_row = self._conn.execute(
                    """
                    SELECT chat_id, message_id, topic_id, replace_existing, target_edited_at, pending_file_path
                    FROM media_download_jobs
                    WHERE job_id = ? AND source = ? AND account_id = ? AND chat_id = ?
                      AND status = 'downloading' AND claim_token = ?
                    """,
                    (job_id, source, account_id, int(chat_id), int(expected_claim_token)),
                ).fetchone()
                if job_row is None:
                    self._conn.rollback()
                    return "stale_claim", None
                message_id = int(job_row["message_id"])
                replace_existing = bool(job_row["replace_existing"])
                target_edited_at = job_row["target_edited_at"]
                job_pending_path = job_row["pending_file_path"]
                msg_row = self._conn.execute(
                    """
                    SELECT topic_id, deleted_at, edited_at
                    FROM messages
                    WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
                    """,
                    (source, account_id, chat_id, message_id),
                ).fetchone()
                if msg_row is None or msg_row["deleted_at"] is not None:
                    remaining_pending = self._clear_pending_path_value_unlocked(job_pending_path)
                    self._conn.execute(
                        """
                        UPDATE media_download_jobs
                        SET status = 'canceled',
                            replace_existing = 0,
                            pending_file_path = ?,
                            next_attempt_at = NULL,
                            last_error = 'message_deleted',
                            error_code = 'message_deleted',
                            updated_at = ?,
                            completed_at = ?
                        WHERE job_id = ? AND claim_token = ?
                        """,
                        (remaining_pending, now, now, job_id, int(expected_claim_token)),
                    )
                    self._conn.commit()
                    return "canceled", None

                if (
                    target_edited_at is not None
                    and msg_row["edited_at"] is not None
                    and str(msg_row["edited_at"]) > str(target_edited_at)
                ):
                    remaining_pending = self._clear_pending_path_value_unlocked(job_pending_path)
                    self._conn.execute(
                        """
                        UPDATE media_download_jobs
                        SET status = 'canceled',
                            replace_existing = 0,
                            pending_file_path = ?,
                            next_attempt_at = NULL,
                            last_error = 'superseded_by_newer_edit',
                            error_code = 'superseded_by_newer_edit',
                            updated_at = ?,
                            completed_at = ?
                        WHERE job_id = ? AND claim_token = ?
                        """,
                        (remaining_pending, now, now, job_id, int(expected_claim_token)),
                    )
                    self._conn.commit()
                    return "stale_claim", None

                effective_topic_id = int(msg_row["topic_id"] or topic_id or 0)
                effective_policy, _policy_source = self._resolve_effective_backup_policy_unlocked(
                    source,
                    account_id,
                    chat_id,
                    topic_id=effective_topic_id,
                )
                allowed = bool(effective_policy.get("enabled")) and bool(
                    effective_policy.get("download_stickers" if is_sticker_like else "download_media")
                )
                if not allowed:
                    remaining_pending = self._clear_pending_path_value_unlocked(job_pending_path)
                    self._conn.execute(
                        """
                        UPDATE media_download_jobs
                        SET status = 'canceled',
                            replace_existing = 0,
                            pending_file_path = ?,
                            next_attempt_at = NULL,
                            last_error = 'policy_disabled',
                            error_code = 'policy_disabled',
                            updated_at = ?,
                            completed_at = ?
                        WHERE job_id = ? AND claim_token = ?
                        """,
                        (remaining_pending, now, now, job_id, int(expected_claim_token)),
                    )
                    self._conn.commit()
                    return "canceled", None

                existing_file = self._conn.execute(
                    """
                    SELECT file_path FROM media_files
                    WHERE source = ? AND account_id = ? AND chat_id = ? AND message_id = ?
                    LIMIT 1
                    """,
                    (source, account_id, chat_id, message_id),
                ).fetchone()
                if existing_file is not None and not replace_existing:
                    remaining_pending = self._clear_pending_path_value_unlocked(job_pending_path)
                    self._conn.execute(
                        """
                        UPDATE media_download_jobs
                        SET status = 'completed',
                            replace_existing = 0,
                            pending_file_path = ?,
                            next_attempt_at = NULL,
                            last_error = NULL,
                            error_code = NULL,
                            updated_at = ?,
                            completed_at = ?
                        WHERE job_id = ? AND claim_token = ?
                        """,
                        (remaining_pending, now, now, job_id, int(expected_claim_token)),
                    )
                    self._conn.commit()
                    return "already_exists", None
                old_path = str(existing_file["file_path"]) if existing_file is not None else None

                published = prepare_media()
                self._upsert_media_file_row_unlocked(published)
                self._insert_event(
                    source=published.source,
                    account_id=published.account_id,
                    event_type="media_ready",
                    chat_id=published.chat_id,
                    message_id=published.message_id,
                    event_at=now,
                    payload={
                        "job_id": job_id,
                        "file_path": published.file_path,
                        "media_kind": published.media_kind,
                        "file_size": published.file_size,
                        "downloaded_at": published.downloaded_at or now,
                    },
                )
                if (
                    old_path is not None
                    and old_path != published.file_path
                    and not self._media_file_path_referenced_unlocked(old_path)
                ):
                    superseded_file_path = old_path
                self._conn.execute(
                    """
                    UPDATE media_download_jobs
                    SET status = 'completed',
                        replace_existing = 0,
                        pending_file_path = ?,
                        next_attempt_at = NULL,
                        last_error = NULL,
                        error_code = NULL,
                        updated_at = ?,
                        completed_at = ?
                    WHERE job_id = ? AND claim_token = ?
                    """,
                    (superseded_file_path, now, now, job_id, int(expected_claim_token)),
                )
                self._conn.commit()
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                if published is not None and rollback_media is not None:
                    try:
                        rollback_media(published)
                    except OSError:
                        pass
                raise
        if superseded_file_path is not None:
            self.cleanup_media_download_pending_path(
                job_id,
                expected_claim_token=expected_claim_token,
                require_downloading=False,
            )
        return "completed", published

    def retry_media_download_job(
        self,
        job_id: str,
        *,
        error: str,
        error_code: str,
        next_attempt_at: str,
        expected_claim_token: int,
        preserve_attempt_budget: bool = False,
    ) -> str | None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now = utc_now_iso()
                row = self._conn.execute(
                    """
                    SELECT attempts, max_attempts, pending_file_path
                    FROM media_download_jobs
                    WHERE job_id = ? AND status = 'downloading' AND claim_token = ?
                    """,
                    (job_id, int(expected_claim_token)),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return None
                remaining_pending = self._clear_pending_path_value_unlocked(row["pending_file_path"])
                attempts = int(row["attempts"])
                max_attempts = int(row["max_attempts"])
                new_attempts = max(0, attempts - 1) if preserve_attempt_budget else attempts
                if not preserve_attempt_budget and new_attempts >= max_attempts:
                    self._conn.execute(
                        """
                        UPDATE media_download_jobs
                        SET status = 'failed',
                            attempts = ?,
                            pending_file_path = ?,
                            next_attempt_at = NULL,
                            last_error = ?,
                            error_code = ?,
                            updated_at = ?,
                            completed_at = ?
                        WHERE job_id = ? AND claim_token = ?
                        """,
                        (new_attempts, remaining_pending, error, error_code, now, now, job_id, int(expected_claim_token)),
                    )
                    self._conn.commit()
                    return "failed"
                self._conn.execute(
                    """
                    UPDATE media_download_jobs
                    SET status = 'retry',
                        attempts = ?,
                        pending_file_path = ?,
                        next_attempt_at = ?,
                        last_error = ?,
                        error_code = ?,
                        updated_at = ?
                    WHERE job_id = ? AND claim_token = ?
                    """,
                    (new_attempts, remaining_pending, next_attempt_at, error, error_code, now, job_id, int(expected_claim_token)),
                )
                self._conn.commit()
                return "retry"
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def release_media_download_job(
        self,
        job_id: str,
        *,
        error: str = "worker_stopped",
        expected_claim_token: int,
    ) -> bool:
        """Release a claimed media download on graceful shutdown without spending attempt budget or reusing claim_token."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now = utc_now_iso()
                row = self._conn.execute(
                    """
                    SELECT pending_file_path
                    FROM media_download_jobs
                    WHERE job_id = ? AND status = 'downloading' AND claim_token = ?
                    """,
                    (job_id, int(expected_claim_token)),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return False
                remaining_pending = self._clear_pending_path_value_unlocked(row["pending_file_path"])
                cur = self._conn.execute(
                    """
                    UPDATE media_download_jobs
                    SET status = 'retry',
                        attempts = MAX(0, attempts - 1),
                        pending_file_path = ?,
                        next_attempt_at = ?,
                        last_error = ?,
                        error_code = 'worker_stopped',
                        updated_at = ?
                    WHERE job_id = ? AND status = 'downloading' AND claim_token = ?
                    """,
                    (remaining_pending, now, error, now, job_id, int(expected_claim_token)),
                )
                if cur.rowcount > 0:
                    self._conn.commit()
                    return True
                self._conn.rollback()
                return False
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def fail_media_download_job(
        self,
        job_id: str,
        *,
        error: str,
        error_code: str,
        expected_claim_token: int,
    ) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now = utc_now_iso()
                row = self._conn.execute(
                    """
                    SELECT pending_file_path
                    FROM media_download_jobs
                    WHERE job_id = ? AND status = 'downloading' AND claim_token = ?
                    """,
                    (job_id, int(expected_claim_token)),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return False
                remaining_pending = self._clear_pending_path_value_unlocked(row["pending_file_path"])
                cur = self._conn.execute(
                    """
                    UPDATE media_download_jobs
                    SET status = 'failed',
                        pending_file_path = ?,
                        next_attempt_at = NULL,
                        last_error = ?,
                        error_code = ?,
                        updated_at = ?,
                        completed_at = ?
                    WHERE job_id = ? AND status = 'downloading' AND claim_token = ?
                    """,
                    (remaining_pending, error, error_code, now, now, job_id, int(expected_claim_token)),
                )
                if cur.rowcount > 0:
                    self._conn.commit()
                    return True
                self._conn.rollback()
                return False
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def cancel_media_download_job(
        self,
        job_id: str,
        *,
        reason: str,
        expected_claim_token: int,
    ) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now = utc_now_iso()
                row = self._conn.execute(
                    """
                    SELECT pending_file_path
                    FROM media_download_jobs
                    WHERE job_id = ? AND status = 'downloading' AND claim_token = ?
                    """,
                    (job_id, int(expected_claim_token)),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return False
                remaining_pending = self._clear_pending_path_value_unlocked(row["pending_file_path"])
                cur = self._conn.execute(
                    """
                    UPDATE media_download_jobs
                    SET status = 'canceled',
                        replace_existing = 0,
                        pending_file_path = ?,
                        next_attempt_at = NULL,
                        last_error = ?,
                        error_code = ?,
                        updated_at = ?,
                        completed_at = ?
                    WHERE job_id = ? AND status = 'downloading' AND claim_token = ?
                    """,
                    (remaining_pending, reason, reason, now, now, job_id, int(expected_claim_token)),
                )
                if cur.rowcount > 0:
                    self._conn.commit()
                    return True
                self._conn.rollback()
                return False
            except Exception:
                if self._conn.in_transaction:
                    self._conn.rollback()
                raise

    def get_media_download_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM media_download_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return _row_to_dict(row, bool_fields={"is_sticker_like", "replace_existing"}) if row else None

    def list_media_download_jobs(
        self,
        *,
        account_id: str | None = None,
        status: str | None = None,
        source: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if source is not None:
            clauses.append("source = ?")
            params.append(source)
        if account_id is not None:
            clauses.append("account_id = ?")
            params.append(account_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, int(limit)))
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM media_download_jobs
                {where}
                ORDER BY created_at ASC, job_id ASC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        return [_row_to_dict(row, bool_fields={"is_sticker_like", "replace_existing"}) for row in rows]

    def list_media_files(
        self,
        account_id: str | None = None,
        chat_id: int | None = None,
        message_id: int | None = None,
        limit: int = 500,
        filename_query: str | None = None,
        media_type: str | None = None,
        file_extension: str | None = None,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT m.source, m.account_id, m.chat_id, m.message_id, m.file_index, m.file_path,
                   m.media_kind, m.mime_type, m.file_size, m.downloaded_at, m.raw_json,
                   tmc_media_filename(m.file_path) AS filename,
                   tmc_media_file_extension(m.file_path) AS file_extension,
                   tmc_media_type(m.media_kind, m.mime_type, m.file_path) AS media_type,
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
        if filename_query is not None:
            clauses.append("tmc_media_filename_contains(m.file_path, ?)")
            params.append(filename_query)
        if media_type is not None:
            clauses.append("tmc_media_type(m.media_kind, m.mime_type, m.file_path) = ?")
            params.append(normalize_media_type_filter(media_type))
        if file_extension is not None:
            clauses.append("tmc_media_file_extension(m.file_path) = ?")
            params.append(normalize_file_extension_filter(file_extension))
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
                       tmc_media_filename(m.file_path) AS filename,
                       tmc_media_file_extension(m.file_path) AS file_extension,
                       tmc_media_type(m.media_kind, m.mime_type, m.file_path) AS media_type,
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
                   tmc_media_filename(m.file_path) AS filename,
                   tmc_media_file_extension(m.file_path) AS file_extension,
                   tmc_media_type(m.media_kind, m.mime_type, m.file_path) AS media_type,
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
        offset: int = 0,
        exclude_topic_ids: Iterable[int] | None = None,
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
        elif exclude_topic_ids:
            excluded = sorted({int(tid) for tid in exclude_topic_ids if int(tid) != 0})
            if excluded:
                placeholders = ", ".join("?" for _ in excluded)
                clauses.append(f"COALESCE(m.topic_id, 0) NOT IN ({placeholders})")
                params.extend(excluded)
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
            LIMIT ? OFFSET ?
        """
        params.append(_bounded_limit(limit, max_limit=50000))
        params.append(max(0, int(offset)))
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
                "timezone": DEFAULT_TIMEZONE,
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

    def _execute_upsert_daily_package_run(self, run: DailyPackageRunRecord) -> None:
        started_at = run.started_at or utc_now_iso()
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

    def upsert_daily_package_run(self, run: DailyPackageRunRecord) -> dict[str, Any]:
        with self._lock:
            self._execute_upsert_daily_package_run(run)
            self._conn.commit()
        item = self.get_daily_package_run(run.run_id)
        if item is None:
            raise ValueError("daily package run was not persisted")
        return item

    def upsert_daily_package_run_for_job(
        self,
        run: DailyPackageRunRecord,
        *,
        job_id: str,
        worker_id: str,
        attempt: int,
        now: str | None = None,
    ) -> dict[str, Any] | None:
        required_statuses = ("running",) if run.status == "completed" else ("running", "cancel_requested")
        placeholders = ", ".join("?" for _ in required_statuses)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                effective_now = now or utc_now_iso()
                owner = self._conn.execute(
                    f"""
                    SELECT 1 FROM daily_summary_jobs
                    WHERE job_id = ? AND worker_id = ? AND attempt = ?
                      AND status IN ({placeholders})
                      AND lease_until IS NOT NULL AND lease_until >= ?
                    """,
                    (job_id, worker_id, int(attempt), *required_statuses, effective_now),
                ).fetchone()
                if owner is None:
                    self._conn.rollback()
                    return None
                self._execute_upsert_daily_package_run(run)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return self.get_daily_package_run(run.run_id)

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

    def _execute_upsert_daily_summary_run(self, run: DailySummaryRunRecord) -> None:
        started_at = run.started_at or utc_now_iso()
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

    def upsert_daily_summary_run(self, run: DailySummaryRunRecord) -> dict[str, Any]:
        with self._lock:
            self._execute_upsert_daily_summary_run(run)
            self._conn.commit()
        item = self.get_daily_summary_run(run.run_id)
        if item is None:
            raise ValueError("daily summary run was not persisted")
        return item

    def upsert_daily_summary_run_for_job(
        self,
        run: DailySummaryRunRecord,
        *,
        job_id: str,
        worker_id: str,
        attempt: int,
        now: str | None = None,
    ) -> dict[str, Any] | None:
        required_statuses = ("running",) if run.status == "completed" else ("running", "cancel_requested")
        placeholders = ", ".join("?" for _ in required_statuses)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                effective_now = now or utc_now_iso()
                owner = self._conn.execute(
                    f"""
                    SELECT 1 FROM daily_summary_jobs
                    WHERE job_id = ? AND worker_id = ? AND attempt = ?
                      AND status IN ({placeholders})
                      AND lease_until IS NOT NULL AND lease_until >= ?
                    """,
                    (job_id, worker_id, int(attempt), *required_statuses, effective_now),
                ).fetchone()
                if owner is None:
                    self._conn.rollback()
                    return None
                self._execute_upsert_daily_summary_run(run)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return self.get_daily_summary_run(run.run_id)

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

    def insert_daily_summary_job(self, job: DailySummaryJobRecord) -> dict[str, Any]:
        now = job.updated_at or utc_now_iso()
        started_at = job.started_at or now
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO daily_summary_jobs(
                  job_id, status, date, timezone, scope_json, package_run_id,
                  summary_run_id, provider, progress_total, progress_current,
                  progress_label, progress_json, request_json, dedupe_key, worker_id,
                  lease_until, heartbeat_at, attempt, retry_at, retry_count,
                  cancel_requested_at, error, started_at, finished_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    job.retry_at,
                    int(job.retry_count),
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
                       lease_until, heartbeat_at, attempt, retry_at, retry_count,
                       cancel_requested_at, error,
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
                   lease_until, heartbeat_at, attempt, retry_at, retry_count,
                   cancel_requested_at, error,
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
                       lease_until, heartbeat_at, attempt, retry_at, retry_count,
                       cancel_requested_at, error,
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
                       lease_until, heartbeat_at, attempt, retry_at, retry_count,
                       cancel_requested_at, error,
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
        job_id: str | None = None,
    ) -> dict[str, Any] | None:
        claimed_job_id: str | None = None
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    UPDATE daily_summary_jobs
                    SET status = 'canceled', finished_at = COALESCE(finished_at, ?),
                        updated_at = ?, worker_id = NULL, lease_until = NULL,
                        heartbeat_at = NULL, retry_at = NULL
                    WHERE status = 'cancel_requested'
                      AND (worker_id IS NULL OR lease_until IS NULL OR lease_until < ?)
                    """,
                    (now, now, now),
                )
                self._close_canceled_job_runs(now)
                job_clause = " AND job_id = ?" if job_id else ""
                params: list[Any] = [now, now]
                if job_id:
                    params.append(job_id)
                row = self._conn.execute(
                    f"""
                    SELECT job_id
                    FROM daily_summary_jobs
                    WHERE ((status = 'queued' AND (retry_at IS NULL OR retry_at <= ?))
                       OR (status = 'running' AND (lease_until IS NULL OR lease_until < ?)))
                      {job_clause}
                    ORDER BY started_at ASC
                    LIMIT 1
                    """,
                    tuple(params),
                ).fetchone()
                if row is not None:
                    claimed_job_id = str(row["job_id"])
                    self._conn.execute(
                        """
                        UPDATE daily_summary_jobs
                        SET status = 'running', worker_id = ?, lease_until = ?, heartbeat_at = ?,
                            attempt = attempt + 1, retry_at = NULL, updated_at = ?
                        WHERE job_id = ?
                        """,
                        (worker_id, lease_until, now, now, claimed_job_id),
                    )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return self.get_daily_summary_job(claimed_job_id) if claimed_job_id else None

    def update_daily_summary_job_progress(
        self,
        job_id: str,
        worker_id: str,
        attempt: int,
        *,
        progress: dict[str, Any],
        lease_until: str | None = None,
        now: str | None = None,
        lease_seconds: float | None = None,
        package_run_id: str | None = None,
        summary_run_id: str | None = None,
        date: str | None = None,
        timezone: str | None = None,
        provider: str | None = None,
    ) -> bool:
        progress_total = int(progress.get("total") or 0)
        progress_current = int(progress.get("current") or 0)
        progress_label = str(progress.get("label") or "running")
        progress_json = json.dumps(progress, ensure_ascii=False, sort_keys=True)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now_dt = datetime.now(dt_timezone.utc)
                effective_now = now or now_dt.isoformat()
                if lease_until is not None:
                    effective_lease_until = lease_until
                elif lease_seconds is not None:
                    effective_lease_until = (now_dt + timedelta(seconds=float(lease_seconds))).isoformat()
                else:
                    raise ValueError("lease_until or lease_seconds is required")
                cur = self._conn.execute(
                    """
                    UPDATE daily_summary_jobs
                    SET date = COALESCE(?, date),
                        timezone = COALESCE(?, timezone),
                        package_run_id = COALESCE(?, package_run_id),
                        summary_run_id = COALESCE(?, summary_run_id),
                        provider = COALESCE(?, provider),
                        progress_total = ?,
                        progress_current = ?,
                        progress_label = ?,
                        progress_json = ?,
                        lease_until = ?,
                        heartbeat_at = ?,
                        updated_at = ?
                    WHERE job_id = ? AND worker_id = ? AND attempt = ?
                      AND status IN ('running', 'cancel_requested')
                      AND lease_until IS NOT NULL AND lease_until >= ?
                    """,
                    (
                        date,
                        timezone,
                        package_run_id,
                        summary_run_id,
                        provider,
                        progress_total,
                        progress_current,
                        progress_label,
                        progress_json,
                        effective_lease_until,
                        effective_now,
                        effective_now,
                        job_id,
                        worker_id,
                        int(attempt),
                        effective_now,
                    ),
                )
                self._conn.commit()
                return cur.rowcount > 0
            except Exception:
                self._conn.rollback()
                raise

    def renew_daily_summary_job_lease(
        self,
        job_id: str,
        worker_id: str,
        attempt: int,
        *,
        now: str | None = None,
        lease_until: str | None = None,
        lease_seconds: float | None = None,
    ) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now_dt = datetime.now(dt_timezone.utc)
                effective_now = now or now_dt.isoformat()
                if lease_until is not None:
                    effective_lease_until = lease_until
                elif lease_seconds is not None:
                    effective_lease_until = (now_dt + timedelta(seconds=float(lease_seconds))).isoformat()
                else:
                    raise ValueError("lease_until or lease_seconds is required")
                cur = self._conn.execute(
                    """
                    UPDATE daily_summary_jobs
                    SET heartbeat_at = ?, lease_until = ?, updated_at = ?
                    WHERE job_id = ? AND worker_id = ? AND attempt = ?
                      AND status IN ('running', 'cancel_requested')
                      AND lease_until IS NOT NULL AND lease_until >= ?
                    """,
                    (effective_now, effective_lease_until, effective_now, job_id, worker_id, int(attempt), effective_now),
                )
                self._conn.commit()
                return cur.rowcount > 0
            except Exception:
                self._conn.rollback()
                raise

    def finish_daily_summary_job(
        self,
        job_id: str,
        worker_id: str,
        attempt: int,
        *,
        status: str,
        progress: dict[str, Any],
        now: str | None = None,
        package_run_id: str | None = None,
        summary_run_id: str | None = None,
        date: str | None = None,
        timezone: str | None = None,
        provider: str | None = None,
        error: str | None = None,
        discard_unsent_outbox: bool = False,
    ) -> bool:
        if status not in {"completed", "failed", "canceled"}:
            raise ValueError(f"Unsupported terminal daily summary job status: {status}")
        allowed = ("running",) if status == "completed" else ("running", "cancel_requested")
        placeholders = ", ".join("?" for _ in allowed)
        progress_total = int(progress.get("total") or 0)
        progress_current = int(progress.get("current") or 0)
        progress_label = str(progress.get("label") or status)
        progress_json = json.dumps(progress, ensure_ascii=False, sort_keys=True)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                effective_now = now or utc_now_iso()
                cur = self._conn.execute(
                    f"""
                    UPDATE daily_summary_jobs
                    SET status = ?,
                        date = COALESCE(?, date),
                        timezone = COALESCE(?, timezone),
                        package_run_id = COALESCE(?, package_run_id),
                        summary_run_id = COALESCE(?, summary_run_id),
                        provider = COALESCE(?, provider),
                        progress_total = ?,
                        progress_current = ?,
                        progress_label = ?,
                        progress_json = ?,
                        error = ?,
                        retry_at = NULL,
                        lease_until = NULL,
                        heartbeat_at = ?,
                        finished_at = ?,
                        updated_at = ?
                    WHERE job_id = ? AND worker_id = ? AND attempt = ?
                      AND status IN ({placeholders})
                      AND lease_until IS NOT NULL AND lease_until >= ?
                    """,
                    (
                        status,
                        date,
                        timezone,
                        package_run_id,
                        summary_run_id,
                        provider,
                        progress_total,
                        progress_current,
                        progress_label,
                        progress_json,
                        error,
                        effective_now,
                        effective_now,
                        effective_now,
                        job_id,
                        worker_id,
                        int(attempt),
                        *allowed,
                        effective_now,
                    ),
                )
                updated = cur.rowcount > 0
                if updated and discard_unsent_outbox:
                    self._conn.execute(
                        "DELETE FROM delivery_outbox WHERE job_id = ? AND status NOT IN ('sent', 'failed')",
                        (job_id,),
                    )
                if updated and status == "canceled":
                    self._close_canceled_job_runs(effective_now)
                self._conn.commit()
                return updated
            except Exception:
                self._conn.rollback()
                raise

    def daily_summary_job_cancel_requested(self, job_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT status, cancel_requested_at FROM daily_summary_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return bool(row and (row["status"] == "cancel_requested" or row["cancel_requested_at"]))

    def requeue_daily_summary_job(
        self,
        job_id: str,
        worker_id: str,
        attempt: int,
        *,
        now: str | None = None,
    ) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                effective_now = now or utc_now_iso()
                cur = self._conn.execute(
                    """
                    UPDATE daily_summary_jobs
                    SET status = CASE
                          WHEN status = 'cancel_requested' OR cancel_requested_at IS NOT NULL
                          THEN 'canceled'
                          ELSE 'queued'
                        END,
                        worker_id = NULL, lease_until = NULL,
                        heartbeat_at = NULL, retry_at = NULL,
                        progress_label = CASE
                          WHEN status = 'cancel_requested' OR cancel_requested_at IS NOT NULL
                          THEN 'canceled'
                          ELSE 'queued after worker stop'
                        END,
                        finished_at = CASE
                          WHEN status = 'cancel_requested' OR cancel_requested_at IS NOT NULL
                          THEN COALESCE(finished_at, ?)
                          ELSE NULL
                        END,
                        updated_at = ?
                    WHERE job_id = ? AND worker_id = ? AND attempt = ?
                      AND status IN ('running', 'cancel_requested')
                      AND lease_until IS NOT NULL AND lease_until >= ?
                    """,
                    (effective_now, effective_now, job_id, worker_id, int(attempt), effective_now),
                )
                requeued = cur.rowcount > 0
                if requeued:
                    self._close_canceled_job_runs(effective_now)
                self._conn.commit()
                return requeued
            except Exception:
                self._conn.rollback()
                raise

    def _close_canceled_job_runs(self, now: str) -> None:
        """Drop unsent chunks and close running summaries of jobs that ended canceled."""
        self._conn.execute(
            """
            DELETE FROM delivery_outbox
            WHERE status NOT IN ('sent', 'failed')
              AND job_id IN (SELECT job_id FROM daily_summary_jobs WHERE status = 'canceled')
            """
        )
        self._conn.execute(
            """
            UPDATE daily_summary_runs
            SET status = 'canceled', progress_label = 'canceled', finished_at = COALESCE(finished_at, ?)
            WHERE status = 'running'
              AND run_id IN (
                SELECT summary_run_id FROM daily_summary_jobs
                WHERE status = 'canceled' AND summary_run_id IS NOT NULL
              )
            """,
            (now,),
        )

    def schedule_daily_summary_job_retry(
        self,
        job_id: str,
        worker_id: str,
        attempt: int,
        *,
        retry_at: str | None = None,
        retry_delay_seconds: int | None = None,
        max_retries: int,
        progress_label: str,
        progress_json: str,
        error: str | None,
        now: str | None = None,
    ) -> bool:
        if max_retries <= 0:
            return False
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now_dt = datetime.now(dt_timezone.utc)
                effective_now = now or now_dt.isoformat()
                if retry_at is not None:
                    effective_retry_at = retry_at
                elif retry_delay_seconds is not None:
                    effective_retry_at = (now_dt + timedelta(seconds=max(0, int(retry_delay_seconds)))).isoformat()
                else:
                    raise ValueError("retry_at or retry_delay_seconds is required")
                cur = self._conn.execute(
                    """
                    UPDATE daily_summary_jobs
                    SET status = 'queued', retry_at = ?, retry_count = retry_count + 1,
                        worker_id = NULL, lease_until = NULL, heartbeat_at = NULL,
                        progress_label = ?, progress_json = ?, error = ?,
                        finished_at = NULL, updated_at = ?
                    WHERE job_id = ? AND worker_id = ? AND attempt = ? AND status = 'running'
                      AND lease_until IS NOT NULL AND lease_until >= ?
                      AND cancel_requested_at IS NULL AND retry_count < ?
                    """,
                    (
                        effective_retry_at,
                        progress_label,
                        progress_json,
                        error,
                        effective_now,
                        job_id,
                        worker_id,
                        int(attempt),
                        effective_now,
                        int(max_retries),
                    ),
                )
                self._conn.commit()
                return cur.rowcount > 0
            except Exception:
                self._conn.rollback()
                raise

    def request_daily_summary_job_cancel(self, job_id: str) -> dict[str, Any] | None:
        now = utc_now_iso()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE daily_summary_jobs
                    SET status = 'cancel_requested',
                        cancel_requested_at = COALESCE(cancel_requested_at, ?),
                        retry_at = NULL,
                        updated_at = ?
                    WHERE job_id = ?
                      AND status IN ('queued', 'running', 'cancel_requested')
                    """,
                    (now, now, job_id),
                )
                if cur.rowcount > 0:
                    self._conn.execute(
                        "DELETE FROM delivery_outbox WHERE job_id = ? AND status NOT IN ('sent', 'failed')",
                        (job_id,),
                    )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return self.get_daily_summary_job(job_id)

    def upsert_daily_message_point(
        self,
        point: DailyMessagePointRecord,
        *,
        commit: bool = True,
    ) -> dict[str, Any]:
        importance_score = int(point.importance_score)
        if not 1 <= importance_score <= 5:
            raise ValueError("daily message point importance_score must be between 1 and 5")
        now = point.updated_at or utc_now_iso()
        created_at = point.created_at or now
        tags_json = _json_array_text(point.tags_json, "tags_json")
        source_refs_json = _json_array_text(point.source_refs_json, "source_refs_json")
        tags_csv = point.tags_csv if point.tags_csv is not None else _tags_csv_from_json(tags_json)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO daily_message_points(
                  point_id, run_id, package_run_id, date, timezone, source, account_id,
                  origin_id, topic_id, origin_title, message_id, occurred_at, tags_json,
                  tags_csv, content, telegram_deeplink, permalink, importance_score,
                  importance_reason, origin_important, source_refs_json, provider,
                  created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(point_id) DO UPDATE SET
                  run_id = excluded.run_id,
                  package_run_id = excluded.package_run_id,
                  date = excluded.date,
                  timezone = excluded.timezone,
                  source = excluded.source,
                  account_id = excluded.account_id,
                  origin_id = excluded.origin_id,
                  topic_id = excluded.topic_id,
                  origin_title = excluded.origin_title,
                  message_id = excluded.message_id,
                  occurred_at = excluded.occurred_at,
                  tags_json = excluded.tags_json,
                  tags_csv = excluded.tags_csv,
                  content = excluded.content,
                  telegram_deeplink = excluded.telegram_deeplink,
                  permalink = excluded.permalink,
                  importance_score = excluded.importance_score,
                  importance_reason = excluded.importance_reason,
                  origin_important = excluded.origin_important,
                  source_refs_json = excluded.source_refs_json,
                  provider = excluded.provider,
                  updated_at = excluded.updated_at
                """,
                (
                    point.point_id,
                    point.run_id,
                    point.package_run_id,
                    point.date,
                    point.timezone,
                    point.source,
                    point.account_id,
                    int(point.origin_id),
                    int(point.topic_id),
                    point.origin_title,
                    point.message_id,
                    point.occurred_at,
                    tags_json,
                    tags_csv,
                    point.content,
                    point.telegram_deeplink,
                    point.permalink,
                    importance_score,
                    point.importance_reason,
                    int(point.origin_important),
                    source_refs_json,
                    point.provider,
                    created_at,
                    now,
                ),
            )
            if commit:
                self._conn.commit()
        item = self.get_daily_message_point(point.point_id)
        if item is None:
            raise ValueError("daily message point was not persisted")
        return item

    def persist_daily_message_points(
        self,
        run_id: str,
        points: list[DailyMessagePointRecord],
    ) -> list[dict[str, Any]]:
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            raise ValueError("daily message point run_id is required")
        if any(point.run_id != clean_run_id for point in points):
            raise ValueError("all daily message points must belong to the replacement run_id")
        persisted: list[dict[str, Any]] = []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute("DELETE FROM daily_message_points WHERE run_id = ?", (clean_run_id,))
                for point in points:
                    persisted.append(self.upsert_daily_message_point(point, commit=False))
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return persisted

    def get_daily_message_point(self, point_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT p.point_id, p.run_id, p.package_run_id, p.date, p.timezone, p.source, p.account_id,
                       p.origin_id, p.topic_id, p.origin_title, p.message_id, p.occurred_at, p.tags_json,
                       p.tags_csv, p.content, p.telegram_deeplink, p.permalink, p.importance_score,
                       p.importance_reason, p.origin_important, p.source_refs_json, p.provider,
                       p.created_at, p.updated_at, r.status AS run_status,
                       (
                         SELECT j.status FROM daily_summary_jobs j
                         WHERE j.summary_run_id = p.run_id
                         ORDER BY j.updated_at DESC LIMIT 1
                       ) AS job_status
                FROM daily_message_points p
                LEFT JOIN daily_summary_runs r ON r.run_id = p.run_id
                WHERE p.point_id = ?
                """,
                (point_id,),
            ).fetchone()
        return _daily_message_point_from_row(row) if row is not None else None

    def list_daily_message_points(
        self,
        *,
        point_id: str | None = None,
        run_id: str | None = None,
        package_run_id: str | None = None,
        date: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        source: str | None = None,
        account_id: str | None = None,
        origin_id: int | None = None,
        topic_id: int | None = None,
        message_id: int | None = None,
        tags: list[str] | None = None,
        importance_min: int | None = None,
        importance_max: int | None = None,
        origin_important: bool | None = None,
        q: str | None = None,
        include_incomplete: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if importance_min is not None and not 1 <= int(importance_min) <= 5:
            raise ValueError("importance_min must be between 1 and 5")
        if importance_max is not None and not 1 <= int(importance_max) <= 5:
            raise ValueError("importance_max must be between 1 and 5")
        if importance_min is not None and importance_max is not None and int(importance_min) > int(importance_max):
            raise ValueError("importance_min must not exceed importance_max")
        sql = """
            SELECT p.point_id, p.run_id, p.package_run_id, p.date, p.timezone, p.source,
                   p.account_id, p.origin_id, p.topic_id, p.origin_title, p.message_id,
                   p.occurred_at, p.tags_json, p.tags_csv, p.content, p.telegram_deeplink,
                   p.permalink, p.importance_score, p.importance_reason, p.origin_important,
                   p.source_refs_json, p.provider, p.created_at, p.updated_at,
                   r.status AS run_status,
                   (
                     SELECT j.status FROM daily_summary_jobs j
                     WHERE j.summary_run_id = p.run_id
                     ORDER BY j.updated_at DESC LIMIT 1
                   ) AS job_status
            FROM daily_message_points p
            LEFT JOIN daily_summary_runs r ON r.run_id = p.run_id
        """
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("p.point_id", point_id),
            ("p.run_id", run_id),
            ("p.package_run_id", package_run_id),
            ("p.date", date),
            ("p.source", source),
            ("p.account_id", account_id),
            ("p.origin_id", origin_id),
            ("p.topic_id", topic_id),
            ("p.message_id", message_id),
        ):
            if value is not None and value != "":
                clauses.append(f"{column} = ?")
                params.append(value)
        if date_from:
            clauses.append("p.date >= ?")
            params.append(date_from)
        if date_to:
            clauses.append("p.date <= ?")
            params.append(date_to)
        if importance_min is not None:
            clauses.append("p.importance_score >= ?")
            params.append(int(importance_min))
        if importance_max is not None:
            clauses.append("p.importance_score <= ?")
            params.append(int(importance_max))
        if origin_important is not None:
            clauses.append("p.origin_important = ?")
            params.append(int(origin_important))
        if q:
            clauses.append(
                "(p.content LIKE ? ESCAPE '\\' OR p.origin_title LIKE ? ESCAPE '\\' "
                "OR p.importance_reason LIKE ? ESCAPE '\\')"
            )
            pattern = _like_pattern(q)
            params.extend((pattern, pattern, pattern))
        if not include_incomplete:
            clauses.append("r.status = 'completed'")
            clauses.append(
                "NOT EXISTS (SELECT 1 FROM daily_summary_jobs hidden_job "
                "WHERE hidden_job.summary_run_id = p.run_id AND hidden_job.status != 'completed')"
            )
        for tag in {_normalize_tag(item) for item in (tags or []) if _normalize_tag(item)}:
            clauses.append(
                "EXISTS (SELECT 1 FROM json_each(p.tags_json) point_tag "
                "WHERE lower(trim(CAST(point_tag.value AS TEXT))) = ?)"
            )
            params.append(tag)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY p.date DESC, p.occurred_at DESC, p.point_id DESC LIMIT ? OFFSET ?"
        params.append(_bounded_limit(limit, max_limit=1000))
        params.append(max(0, int(offset)))
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [_daily_message_point_from_row(row) for row in rows]

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
        record_type = _summary_record_type(record.record_type, record.content_json)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO daily_summary_records(
                  summary_id, run_id, package_run_id, date, timezone, scope_json,
                  tags_json, tags_csv, important, record_type, provider, title, content_md, content_json,
                  summary_path, origin_count, group_count, image_count, content_length,
                  deleted_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(summary_id) DO UPDATE SET
                  run_id = excluded.run_id,
                  package_run_id = excluded.package_run_id,
                  date = excluded.date,
                  timezone = excluded.timezone,
                  scope_json = excluded.scope_json,
                  tags_json = excluded.tags_json,
                  tags_csv = excluded.tags_csv,
                  important = excluded.important,
                  record_type = excluded.record_type,
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
                    record_type,
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

    def _insert_outbox_items(self, outbox_items: list[dict[str, Any]] | None) -> None:
        if not outbox_items:
            return
        run_ids = sorted({str(item["summary_run_id"]) for item in outbox_items})
        job_ids = sorted({str(item["job_id"]) for item in outbox_items if item.get("job_id")})
        legacy_runs: set[str] = set()
        legacy_jobs: set[str] = set()
        if run_ids:
            rows = self._conn.execute(
                f"""
                SELECT DISTINCT summary_run_id FROM delivery_outbox
                WHERE content_kind = 'summary'
                  AND summary_run_id IN ({', '.join('?' for _ in run_ids)})
                """,
                tuple(run_ids),
            ).fetchall()
            legacy_runs = {str(row["summary_run_id"]) for row in rows}
        if job_ids:
            rows = self._conn.execute(
                f"""
                SELECT DISTINCT job_id FROM delivery_outbox
                WHERE content_kind = 'summary'
                  AND job_id IN ({', '.join('?' for _ in job_ids)})
                """,
                tuple(job_ids),
            ).fetchall()
            legacy_jobs = {str(row["job_id"]) for row in rows if row["job_id"] is not None}
        for item in outbox_items:
            run_id = str(item["summary_run_id"])
            job_id = str(item["job_id"]) if item.get("job_id") else None
            content_kind = str(item.get("content_kind") or "summary")
            if content_kind != "summary" and (run_id in legacy_runs or (job_id is not None and job_id in legacy_jobs)):
                continue
            outbox_id = str(item.get("outbox_id") or f"out_{uuid.uuid4().hex[:16]}")
            send_random_id = (
                int(item["send_random_id"])
                if item.get("send_random_id") is not None
                else outbox_send_random_id(outbox_id)
            )
            now = str(item.get("created_at") or utc_now_iso())
            self._conn.execute(
                """
                INSERT INTO delivery_outbox(
                  outbox_id, summary_run_id, job_id, account_id, origin_id, topic_id,
                  content_kind, chunk_index, chunk_count, content, status, attempts,
                  claim_token, send_random_id, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, ?, ?, ?)
                ON CONFLICT(summary_run_id, account_id, origin_id, topic_id, content_kind, chunk_index)
                DO NOTHING
                """,
                (
                    outbox_id,
                    run_id,
                    job_id,
                    str(item["account_id"]),
                    int(item["origin_id"]),
                    int(item.get("topic_id") or 0),
                    content_kind,
                    int(item["chunk_index"]),
                    int(item["chunk_count"]),
                    str(item["content"]),
                    send_random_id,
                    now,
                    now,
                ),
            )

    def list_legacy_delivery_outbox(
        self,
        *,
        summary_run_id: str | None = None,
        job_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return pre-v20 ('summary') outbox rows associated with a summary run or job."""
        match_clauses: list[str] = []
        params: list[Any] = []
        if summary_run_id:
            match_clauses.append("summary_run_id = ?")
            params.append(summary_run_id)
        if job_id:
            match_clauses.append("job_id = ?")
            params.append(job_id)
        if not match_clauses:
            return []
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM delivery_outbox
                WHERE content_kind = 'summary'
                  AND ({' OR '.join(match_clauses)})
                ORDER BY created_at, chunk_index
                """,
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]

    def publish_daily_summary(
        self,
        summary_run: DailySummaryRunRecord,
        records: list[DailySummaryRecord],
        outbox_items: list[dict[str, Any]] | None = None,
        *,
        message_points: list[DailyMessagePointRecord] | None = None,
        job_owner: tuple[str, str, int] | None = None,
        now: str | None = None,
    ) -> dict[str, Any] | None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                effective_now = now or utc_now_iso()
                if job_owner is not None:
                    job_id, worker_id, attempt = job_owner
                    owner = self._conn.execute(
                        """
                        SELECT 1 FROM daily_summary_jobs
                        WHERE job_id = ? AND worker_id = ? AND attempt = ?
                          AND status = 'running'
                          AND lease_until IS NOT NULL AND lease_until >= ?
                        """,
                        (job_id, worker_id, int(attempt), effective_now),
                    ).fetchone()
                    if owner is None:
                        self._conn.rollback()
                        return None
                self._execute_upsert_daily_summary_run(summary_run)
                for record in records:
                    self.upsert_daily_summary_record(record, commit=False)
                if message_points is not None:
                    if any(point.run_id != summary_run.run_id for point in message_points):
                        raise ValueError("all daily message points must belong to the published summary run")
                    self._conn.execute("DELETE FROM daily_message_points WHERE run_id = ?", (summary_run.run_id,))
                    for point in message_points:
                        self.upsert_daily_message_point(point, commit=False)
                self._insert_outbox_items(outbox_items)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return self.get_daily_summary_run(summary_run.run_id)

    def persist_daily_summary_batch(
        self,
        records: list[DailySummaryRecord],
        outbox_items: list[dict[str, Any]] | None = None,
        message_points: list[DailyMessagePointRecord] | None = None,
    ) -> list[dict[str, Any]]:
        persisted: list[dict[str, Any]] = []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for record in records:
                    persisted.append(self.upsert_daily_summary_record(record, commit=False))
                if message_points is not None:
                    point_run_ids = {point.run_id for point in message_points}
                    if not point_run_ids:
                        point_run_ids = {record.run_id for record in records}
                    if len(point_run_ids) != 1:
                        raise ValueError("daily message point batch must belong to exactly one run_id")
                    point_run_id = next(iter(point_run_ids))
                    if any(point.run_id != point_run_id for point in message_points):
                        raise ValueError("all daily message points must belong to the replacement run_id")
                    self._conn.execute("DELETE FROM daily_message_points WHERE run_id = ?", (point_run_id,))
                    for point in message_points:
                        self.upsert_daily_message_point(point, commit=False)
                self._insert_outbox_items(outbox_items)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return persisted

    def discard_unsent_delivery_outbox(
        self,
        *,
        job_id: str | None = None,
        summary_run_id: str | None = None,
    ) -> int:
        clauses = ["status NOT IN ('sent', 'failed')"]
        params: list[Any] = []
        if job_id:
            clauses.append("job_id = ?")
            params.append(job_id)
        if summary_run_id:
            clauses.append("summary_run_id = ?")
            params.append(summary_run_id)
        if len(clauses) == 1:
            raise ValueError("job_id or summary_run_id is required to discard delivery outbox rows")
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM delivery_outbox WHERE " + " AND ".join(clauses),
                tuple(params),
            )
            self._conn.commit()
            return max(cur.rowcount, 0)

    def list_delivery_outbox(
        self,
        *,
        summary_run_id: str | None = None,
        job_id: str | None = None,
        status: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM delivery_outbox"
        clauses: list[str] = []
        params: list[Any] = []
        if summary_run_id and job_id:
            clauses.append("(summary_run_id = ? OR job_id = ?)")
            params.extend([summary_run_id, job_id])
        elif summary_run_id:
            clauses.append("summary_run_id = ?")
            params.append(summary_run_id)
        elif job_id:
            clauses.append("job_id = ?")
            params.append(job_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at, content_kind, chunk_index LIMIT ?"
        params.append(_bounded_limit(limit, max_limit=2000))
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def reset_retry_backoff_for_summary(
        self,
        summary_run_id: str | None = None,
        *,
        job_id: str | None = None,
    ) -> int:
        """Clear retry backoff timestamps for one summary run or job so standalone recovery retries immediately."""
        match_clauses: list[str] = []
        params: list[Any] = []
        if summary_run_id:
            match_clauses.append("summary_run_id = ?")
            params.append(summary_run_id)
        if job_id:
            match_clauses.append("job_id = ?")
            params.append(job_id)
        if not match_clauses:
            return 0
        with self._lock:
            cur = self._conn.execute(
                f"""
                UPDATE delivery_outbox
                SET next_attempt_at = NULL
                WHERE status = 'retry' AND ({' OR '.join(match_clauses)})
                """,
                tuple(params),
            )
            self._conn.commit()
            return max(cur.rowcount, 0)

    def claim_delivery_outbox(
        self,
        *,
        now: str | None = None,
        stale_before: str | None = None,
        stale_seconds: float = 300.0,
        summary_run_id: str | None = None,
        job_id: str | None = None,
        job_owner: tuple[str, str, int] | None = None,
    ) -> dict[str, Any] | None:
        claimed: dict[str, Any] | None = None
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                now_dt = datetime.now(dt_timezone.utc)
                effective_now = now or now_dt.isoformat()
                effective_stale_before = (
                    stale_before
                    if stale_before is not None
                    else (now_dt - timedelta(seconds=float(stale_seconds))).isoformat()
                )
                if job_owner is not None:
                    owner_job_id, worker_id, attempt = job_owner
                    owner = self._conn.execute(
                        """
                        SELECT 1 FROM daily_summary_jobs
                        WHERE job_id = ? AND worker_id = ? AND attempt = ?
                          AND status = 'running'
                          AND lease_until IS NOT NULL AND lease_until >= ?
                        """,
                        (owner_job_id, worker_id, int(attempt), effective_now),
                    ).fetchone()
                    if owner is None:
                        self._conn.rollback()
                        return None
                clauses = [
                    "(o.status IN ('pending', 'retry') AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= ?))",
                    "(o.status = 'sending' AND o.updated_at < ?)",
                ]
                params: list[Any] = [effective_now, effective_stale_before]
                scope_clause = ""
                if summary_run_id and job_id:
                    scope_clause = " AND (o.summary_run_id = ? OR o.job_id = ?)"
                    params.extend([summary_run_id, job_id])
                elif summary_run_id:
                    scope_clause = " AND o.summary_run_id = ?"
                    params.append(summary_run_id)
                elif job_id:
                    scope_clause = " AND o.job_id = ?"
                    params.append(job_id)
                row = self._conn.execute(
                    f"""
                    SELECT o.outbox_id
                    FROM delivery_outbox o
                    WHERE ({' OR '.join(clauses)}){scope_clause}
                      AND NOT EXISTS (
                        SELECT 1 FROM delivery_outbox previous
                        WHERE previous.summary_run_id = o.summary_run_id
                          AND previous.account_id = o.account_id
                          AND previous.origin_id = o.origin_id
                          AND previous.topic_id = o.topic_id
                          AND previous.content_kind = o.content_kind
                          AND previous.chunk_index < o.chunk_index
                          AND previous.status != 'sent'
                      )
                    ORDER BY o.created_at, o.content_kind, o.chunk_index
                    LIMIT 1
                    """,
                    tuple(params),
                ).fetchone()
                if row is not None:
                    outbox_id = str(row["outbox_id"])
                    self._conn.execute(
                        """
                        UPDATE delivery_outbox
                        SET status = 'sending',
                            attempts = attempts + 1,
                            claim_token = claim_token + 1,
                            updated_at = ?
                        WHERE outbox_id = ?
                        """,
                        (effective_now, outbox_id),
                    )
                    claimed_row = self._conn.execute(
                        "SELECT * FROM delivery_outbox WHERE outbox_id = ?",
                        (outbox_id,),
                    ).fetchone()
                    if claimed_row is not None:
                        claimed = dict(claimed_row)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return claimed

    def complete_delivery_outbox(
        self,
        outbox_id: str,
        *,
        message_id: int | None,
        now: str,
        expected_claim_token: int,
    ) -> bool:
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE delivery_outbox
                SET status = 'sent', message_id = ?, last_error = NULL,
                    next_attempt_at = NULL, sent_at = ?, updated_at = ?
                WHERE outbox_id = ? AND status = 'sending' AND claim_token = ?
                """,
                (message_id, now, now, outbox_id, int(expected_claim_token)),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def retry_delivery_outbox(
        self,
        outbox_id: str,
        *,
        error: str,
        next_attempt_at: str,
        now: str,
        expected_claim_token: int,
        preserve_attempt_budget: bool = False,
    ) -> bool:
        attempt_expr = "MAX(0, attempts - 1)" if preserve_attempt_budget else "attempts"
        with self._lock:
            cur = self._conn.execute(
                f"""
                UPDATE delivery_outbox
                SET status = 'retry', attempts = {attempt_expr}, last_error = ?,
                    next_attempt_at = ?, updated_at = ?
                WHERE outbox_id = ? AND status = 'sending' AND claim_token = ?
                """,
                (error, next_attempt_at, now, outbox_id, int(expected_claim_token)),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def release_delivery_outbox(
        self,
        outbox_id: str,
        *,
        error: str,
        now: str,
        expected_claim_token: int,
    ) -> bool:
        """Return a chunk interrupted by shutdown to retry without spending an attempt or reusing its claim token."""
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE delivery_outbox
                SET status = 'retry', attempts = MAX(0, attempts - 1), last_error = ?,
                    next_attempt_at = NULL, updated_at = ?
                WHERE outbox_id = ? AND status = 'sending' AND claim_token = ?
                """,
                (error, now, outbox_id, int(expected_claim_token)),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def fail_delivery_outbox(
        self,
        outbox_id: str,
        *,
        error: str,
        now: str,
        expected_claim_token: int,
    ) -> bool:
        """Mark a chunk and all remaining unsent chunks of the same report as failed."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    """
                    SELECT summary_run_id, account_id, origin_id, topic_id, content_kind, chunk_index
                    FROM delivery_outbox
                    WHERE outbox_id = ? AND status = 'sending' AND claim_token = ?
                    """,
                    (outbox_id, int(expected_claim_token)),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return False
                self._conn.execute(
                    """
                    UPDATE delivery_outbox
                    SET status = 'failed', last_error = ?, next_attempt_at = NULL, updated_at = ?
                    WHERE summary_run_id = ?
                      AND account_id = ?
                      AND origin_id = ?
                      AND topic_id = ?
                      AND content_kind = ?
                      AND chunk_index >= ?
                      AND status != 'sent'
                    """,
                    (
                        error,
                        now,
                        row["summary_run_id"],
                        row["account_id"],
                        row["origin_id"],
                        row["topic_id"],
                        row["content_kind"],
                        row["chunk_index"],
                    ),
                )
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def get_daily_summary_record(
        self,
        *,
        summary_id: str | None = None,
        run_id: str | None = None,
        record_type: str | None = None,
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
        if record_type:
            clauses.append("record_type = ?")
            params.append(record_type)
        if not clauses:
            return None
        if not include_deleted:
            clauses.append("deleted_at IS NULL")
        with self._lock:
            row = self._conn.execute(
                """
                SELECT summary_id, run_id, package_run_id, date, timezone, scope_json,
                       tags_json, tags_csv, important, record_type, provider, title, content_md, content_json,
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
        record_type: str | None = None,
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
                   tags_json, tags_csv, important, record_type, provider, title, content_md, content_json,
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
        if record_type:
            clauses.append("record_type = ?")
            params.append(record_type)
        if important is not None:
            clauses.append("important = ?")
            params.append(int(important))
        if q:
            clauses.append("(title LIKE ? ESCAPE '\\' OR content_md LIKE ? ESCAPE '\\')")
            params.extend((_like_pattern(q), _like_pattern(q)))
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
            media_counts = self._conn.execute(
                """
                SELECT
                  SUM(CASE WHEN status IN ('pending', 'downloading', 'retry') THEN 1 ELSE 0 END) AS pending_count,
                  SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count
                FROM media_download_jobs
                """
            ).fetchone()
            return {
                "database_id": self.get_meta("database_id"),
                "schema_version": int(self.get_meta("schema_version") or 0),
                "last_event_seq": row["seq"],
                "message_count": message_count["count"],
                "operation_error_count": operation_error_count["count"],
                "media_download_pending": int((media_counts["pending_count"] or 0) if media_counts else 0),
                "media_download_failed": int((media_counts["failed_count"] or 0) if media_counts else 0),
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

    def clear_message_raw_json_before(
        self,
        cutoff_sent_at: str,
        *,
        batch_size: int = 10_000,
    ) -> dict[str, int]:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        before = self.message_raw_json_stats(cutoff_sent_at=cutoff_sent_at)
        removed_count = 0
        last_rowid = 0
        while True:
            with self._lock:
                rows = self._conn.execute(
                    """
                    SELECT rowid
                    FROM messages
                    WHERE rowid > ?
                      AND raw_json IS NOT NULL
                      AND sent_at < ?
                    ORDER BY rowid
                    LIMIT ?
                    """,
                    (last_rowid, cutoff_sent_at, batch_size),
                ).fetchall()
                if not rows:
                    break
                first_rowid = int(rows[0]["rowid"])
                last_rowid = int(rows[-1]["rowid"])
                cur = self._conn.execute(
                    """
                    UPDATE messages
                    SET raw_json = NULL
                    WHERE rowid BETWEEN ? AND ?
                      AND raw_json IS NOT NULL
                      AND sent_at < ?
                    """,
                    (first_rowid, last_rowid, cutoff_sent_at),
                )
                self._conn.commit()
                removed_count += max(cur.rowcount, 0)
        return {
            "message_count": int(removed_count),
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
        sql = """
                SELECT m.*
                FROM message_fts f
                JOIN messages m ON m.rowid = f.rowid
                WHERE message_fts MATCH ?
                ORDER BY m.sent_at DESC
                LIMIT ?
                """
        with self._lock:
            try:
                rows = self._conn.execute(sql, (query, limit)).fetchall()
            except sqlite3.OperationalError as exc:
                if not _is_fts_query_syntax_error(exc):
                    raise
                rows = self._conn.execute(sql, (_fts_literal_query(query), limit)).fetchall()
        return [_row_to_dict(row, json_fields={"reactions_json", "raw_json"}) for row in rows]

    def query_messages(
        self,
        *,
        fts_query: str | None = None,
        text_terms: list[str] | None = None,
        account_id: str | None = None,
        origin_id: int | None = None,
        topic_id: int | None = None,
        sender: str | None = None,
        sent_from: str | None = None,
        sent_before: str | None = None,
        include_deleted: bool = False,
        descending: bool = True,
        after: tuple[str, int] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return compact message rows ordered by (sent_at, rowid).

        `after` is the (sent_at, row_id) of the last row of the previous page.
        """
        source_sql = "messages m"
        clauses: list[str] = []
        params: list[Any] = []
        if fts_query:
            # CROSS JOIN pins the full-text index as the outer loop.
            source_sql = "message_fts f CROSS JOIN messages m ON m.rowid = f.rowid"
            clauses.append("message_fts MATCH ?")
            params.append(fts_query)
        if text_terms and account_id is not None and origin_id is not None:
            # A substring scan of one origin reads only that origin through the
            # primary key.
            clauses.extend(("m.source = ?", "m.account_id = ?", "m.chat_id = ?"))
            params.extend((SOURCE_TELEGRAM, account_id, origin_id))
        else:
            # Every other query walks idx_messages_sent_at in order (or the
            # full-text index) and stops at the limit; unary plus keeps the
            # planner from skip-scanning the primary key and sorting instead.
            for column, value in (("+m.account_id", account_id), ("+m.chat_id", origin_id)):
                if value is not None:
                    clauses.append(f"{column} = ?")
                    params.append(value)
        if topic_id is not None:
            clauses.append("COALESCE(m.topic_id, 0) = ?")
            params.append(topic_id)
        if sent_from:
            clauses.append("m.sent_at >= ?")
            params.append(sent_from)
        if sent_before:
            clauses.append("m.sent_at < ?")
            params.append(sent_before)
        if not include_deleted:
            # Unary plus keeps the planner off idx_messages_deleted.
            clauses.append("+m.deleted_at IS NULL")
        for term in text_terms or []:
            clauses.append("m.text LIKE ? ESCAPE '\\'")
            params.append(_like_pattern(term))
        if sender:
            clauses.append("(m.sender_name LIKE ? ESCAPE '\\' OR m.sender_username LIKE ? ESCAPE '\\')")
            params.extend((_like_pattern(sender), _like_pattern(sender)))
        if after is not None:
            clauses.append(f"(m.sent_at, m.rowid) {'<' if descending else '>'} (?, ?)")
            params.extend(after)
        direction = "DESC" if descending else "ASC"
        sql = f"""
            SELECT {_MESSAGE_VIEW_COLUMNS}
            FROM {source_sql}
            {_MESSAGE_VIEW_JOINS}
            {"WHERE " + " AND ".join(clauses) if clauses else ""}
            ORDER BY m.sent_at {direction}, m.rowid {direction}
            LIMIT ?
        """
        params.append(_bounded_limit(limit))
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def list_message_context(
        self,
        account_id: str,
        origin_id: int,
        message_id: int,
        *,
        before: int = 10,
        after: int = 10,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        """Return a message with its neighbours in the same origin and topic, oldest first.

        `include_deleted` filters the neighbours only; the message itself is
        returned even when deleted so callers can tell deleted from missing.
        """
        base_sql = f"""
            SELECT {_MESSAGE_VIEW_COLUMNS}
            FROM messages m
            {_MESSAGE_VIEW_JOINS}
            WHERE m.source = ? AND m.account_id = ? AND m.chat_id = ?
        """
        key = (SOURCE_TELEGRAM, account_id, origin_id)
        deleted_sql = "" if include_deleted else " AND +m.deleted_at IS NULL"
        with self._lock:
            target = self._conn.execute(base_sql + " AND m.message_id = ?", (*key, message_id)).fetchone()
            if target is None:
                return []
            topic = target["topic_id"] or 0
            older = self._conn.execute(
                base_sql
                + " AND m.message_id < ? AND COALESCE(m.topic_id, 0) = ?"
                + deleted_sql
                + " ORDER BY m.message_id DESC LIMIT ?",
                (*key, message_id, topic, max(0, before)),
            ).fetchall()
            newer = self._conn.execute(
                base_sql
                + " AND m.message_id > ? AND COALESCE(m.topic_id, 0) = ?"
                + deleted_sql
                + " ORDER BY m.message_id ASC LIMIT ?",
                (*key, message_id, topic, max(0, after)),
            ).fetchall()
        return [dict(row) for row in (*reversed(older), target, *newer)]

    def content_overview(self) -> dict[str, Any]:
        """Return message, daily report, and message point ranges for read-only clients."""
        with self._lock:
            messages = self._conn.execute(
                """
                SELECT (SELECT MIN(sent_at) FROM messages) AS first_sent_at,
                       (SELECT MAX(sent_at) FROM messages) AS last_sent_at
                """
            ).fetchone()
            reports = self._conn.execute(
                """
                SELECT record_type, COUNT(*) AS count, MIN(date) AS date_from, MAX(date) AS date_to
                FROM daily_summary_records
                WHERE deleted_at IS NULL
                GROUP BY record_type
                ORDER BY record_type
                """
            ).fetchall()
            points = self._conn.execute(
                """
                SELECT COUNT(*) AS count, MIN(p.date) AS date_from, MAX(p.date) AS date_to
                FROM daily_message_points p
                JOIN daily_summary_runs r ON r.run_id = p.run_id
                WHERE r.status = 'completed'
                  AND NOT EXISTS (
                    SELECT 1 FROM daily_summary_jobs hidden_job
                    WHERE hidden_job.summary_run_id = p.run_id AND hidden_job.status != 'completed'
                  )
                """
            ).fetchone()
        return {
            "messages": dict(messages),
            "daily_reports": [dict(row) for row in reports],
            "message_points": dict(points),
        }

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
        self._ensure_column(
            "backup_policies",
            "download_stickers",
            "INTEGER NOT NULL DEFAULT 0",
        )
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
        if self._has_table("delivery_outbox") and self._has_column("delivery_outbox", "content_kind"):
            needs_outbox_repair = (
                not self._has_column("delivery_outbox", "claim_token")
                or not self._has_column("delivery_outbox", "send_random_id")
                or self._conn.execute(
                    "SELECT 1 FROM delivery_outbox WHERE send_random_id IS NULL LIMIT 1"
                ).fetchone()
                is not None
            )
            if needs_outbox_repair:
                if self._conn.in_transaction:
                    self._conn.commit()
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    _migration_20(self._conn)
                    self._conn.commit()
                except Exception:
                    self._conn.rollback()
                    raise
        if self._has_table("media_download_jobs"):
            needs_media_jobs_repair = (
                not self._has_column("media_download_jobs", "replace_existing")
                or not self._has_column("media_download_jobs", "target_edited_at")
                or not self._has_column("media_download_jobs", "pending_file_path")
            )
            if needs_media_jobs_repair:
                if self._conn.in_transaction:
                    self._conn.commit()
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    _migration_21(self._conn)
                    self._conn.commit()
                except Exception:
                    self._conn.rollback()
                    raise

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
              record_type TEXT NOT NULL DEFAULT 'summary',
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
              tags_json, tags_csv, important, record_type, provider, title, content_md, content_json,
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
              important,
              CASE
                WHEN content_json IS NOT NULL AND content_json != '' AND json_valid(content_json)
                THEN COALESCE(CAST(json_extract(content_json, '$.record_type') AS TEXT), 'summary')
                ELSE 'summary'
              END,
              provider, title, content_md, content_json,
              summary_path, origin_count, group_count, image_count, content_length,
              NULL, created_at, updated_at
            FROM daily_summary_records_old_unique;
            DROP TABLE daily_summary_records_old_unique;
            CREATE INDEX IF NOT EXISTS idx_daily_summary_records_date ON daily_summary_records(date, created_at);
            CREATE INDEX IF NOT EXISTS idx_daily_summary_records_package ON daily_summary_records(package_run_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_daily_summary_records_important ON daily_summary_records(important, created_at);
            CREATE INDEX IF NOT EXISTS idx_daily_summary_records_type_date ON daily_summary_records(record_type, date, created_at);
            COMMIT;
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


@functools.cache
def _schema_object_names() -> frozenset[str]:
    return frozenset(
        re.findall(
            r"CREATE\s+(?:UNIQUE\s+)?(?:VIRTUAL\s+)?(?:TABLE|INDEX|TRIGGER)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
            _schema_sql(),
            flags=re.IGNORECASE,
        )
    )


def _schema_sql_parts() -> tuple[str, list[str]]:
    """Split the schema snapshot into table statements and the indexes and triggers built on them."""
    tables: list[str] = []
    objects: list[str] = []
    statement = ""
    for line in _schema_sql().splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            target = tables if re.match(r"\s*CREATE\s+(VIRTUAL\s+)?TABLE\b", statement) else objects
            target.append(statement.strip())
            statement = ""
    return "\n".join(tables), objects


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


def _daily_message_point_from_row(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(
        row,
        json_fields={"tags_json", "source_refs_json"},
        bool_fields={"origin_important"},
    )
    data["tags"] = data.pop("tags_json") or []
    data["source_refs"] = data.pop("source_refs_json") or []
    if not data.get("tags_csv"):
        data["tags_csv"] = ",".join(str(tag) for tag in data["tags"])
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


def _json_array_text(value: str | None, field: str) -> str:
    if not value:
        return "[]"
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"daily message point {field} must be valid JSON") from exc
    if not isinstance(payload, list):
        raise ValueError(f"daily message point {field} must be a JSON array")
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _summary_record_type(record_type: str | None, content_json: str | None) -> str:
    normalized = str(record_type or "").strip()
    if normalized and normalized != "summary":
        return normalized
    if content_json:
        try:
            payload = json.loads(content_json)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            derived = str(payload.get("record_type") or "").strip()
            if derived:
                return derived
    return normalized or "summary"


def _normalize_tag(tag: Any) -> str:
    return str(tag or "").strip().lower()


def _like_pattern(term: str) -> str:
    escaped = term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def _is_fts_query_syntax_error(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "fts5: syntax error",
            "malformed match expression",
            "unterminated string",
            "no such column",
            "unknown special query",
        )
    )


def _fts_literal_query(query: str) -> str:
    quoted: list[str] = []
    safe_query = "".join(character if character >= " " else " " for character in query)
    for token in safe_query.split():
        escaped = token.replace('"', '""')
        quoted.append(f'"{escaped}"')
    return " ".join(quoted) or '""'


def default_disabled_backup_policy() -> dict[str, Any]:
    return {
        "enabled": False,
        "capture_text": True,
        "capture_media_metadata": True,
        "download_media": False,
        "download_stickers": False,
        "tags": None,
        "updated_at": None,
    }


def resolve_effective_policy(
    self_policy: dict[str, Any] | None,
    parent_policy: dict[str, Any] | None,
    *,
    topic_id: int,
    self_archived: bool,
    parent_archived: bool,
) -> tuple[dict[str, Any], str]:
    """Resolve one canonical effective backup policy across ingest, daily, MCP, and management API."""
    if self_archived or (topic_id != 0 and parent_archived):
        return (default_disabled_backup_policy(), "archived")
    if self_policy is not None:
        return (dict(self_policy), "self")
    if topic_id != 0 and parent_policy is not None:
        return (dict(parent_policy), "parent")
    return (default_disabled_backup_policy(), "none")


def _parse_auth_state_meta(raw: str | None) -> tuple[int, str, int]:
    if raw is None:
        return (0, "", 0)
    text = str(raw).strip()
    if not text:
        return (0, "", 0)
    if text.isdigit():
        return (int(text), "", 0)
    try:
        parsed = json.loads(text)
    except Exception:
        return (0, "", 0)
    if isinstance(parsed, dict):
        seq = int(parsed.get("seq") or 0)
        owner = str(parsed.get("owner") or "")
        epoch = int(parsed.get("epoch") or 0)
        return (seq, owner, epoch)
    return (0, "", 0)


def _parse_media_staging_dir_name(name: str) -> tuple[str, int] | None:
    if not name.startswith(".part-"):
        return None
    body = name[len(".part-") :]
    job_id, sep, token_str = body.rpartition("-t")
    if not sep or not job_id or not token_str.isdigit():
        return None
    return job_id, int(token_str)


def _unlink_file_confirmed(file_path: str) -> bool:
    """Remove file_path (and its empty job-claim publish directory, if any).

    Returns True only when unlink succeeds or raises FileNotFoundError; any other
    OSError (such as EACCES/EBUSY) returns False so callers keep the path for retry.
    """
    path = Path(file_path)
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        return False
    parent = path.parent
    parent_name = parent.name
    if parent_name.startswith("mdj_") and "-t" in parent_name:
        try:
            parent.rmdir()
        except OSError:
            pass
    return True


def _bounded_limit(limit: int, max_limit: int = 1000) -> int:
    if limit <= 0:
        return 1
    return min(limit, max_limit)
