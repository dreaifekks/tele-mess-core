from __future__ import annotations

import sqlite3
from typing import Callable


Migration = Callable[[sqlite3.Connection], None]


def apply_migrations(connection: sqlite3.Connection, current_version: int, target_version: int) -> None:
    if current_version > target_version:
        raise RuntimeError(
            f"Database schema version {current_version} is newer than supported version {target_version}"
        )
    for version in range(current_version + 1, target_version + 1):
        migration = MIGRATIONS.get(version)
        if migration is None:
            raise RuntimeError(f"Missing database migration for version {version}")
        connection.execute("BEGIN IMMEDIATE")
        try:
            migration(connection)
            connection.execute(f"PRAGMA user_version = {version}")
            connection.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(version),),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise


def _migration_13(connection: sqlite3.Connection) -> None:
    _ensure_column(connection, "daily_summary_jobs", "request_json", "TEXT")
    _ensure_column(connection, "daily_summary_jobs", "dedupe_key", "TEXT")
    _ensure_column(connection, "daily_summary_jobs", "worker_id", "TEXT")
    _ensure_column(connection, "daily_summary_jobs", "lease_until", "TEXT")
    _ensure_column(connection, "daily_summary_jobs", "heartbeat_at", "TEXT")
    _ensure_column(connection, "daily_summary_jobs", "attempt", "INTEGER NOT NULL DEFAULT 0")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS delivery_outbox (
          outbox_id TEXT PRIMARY KEY,
          summary_run_id TEXT NOT NULL,
          job_id TEXT,
          account_id TEXT NOT NULL,
          origin_id INTEGER NOT NULL,
          topic_id INTEGER NOT NULL DEFAULT 0,
          chunk_index INTEGER NOT NULL,
          chunk_count INTEGER NOT NULL,
          content TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          attempts INTEGER NOT NULL DEFAULT 0,
          message_id INTEGER,
          last_error TEXT,
          next_attempt_at TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          sent_at TEXT,
          UNIQUE(summary_run_id, account_id, origin_id, topic_id, chunk_index)
        )
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_summary_jobs_active_dedupe
        ON daily_summary_jobs(dedupe_key)
        WHERE dedupe_key IS NOT NULL
          AND status IN ('queued', 'running', 'cancel_requested')
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_daily_summary_jobs_claim ON daily_summary_jobs(status, lease_until, started_at)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_delivery_outbox_pending ON delivery_outbox(status, next_attempt_at, created_at)"
    )


def _migration_14(connection: sqlite3.Connection) -> None:
    statements = (
        """
        CREATE TRIGGER IF NOT EXISTS daily_summary_jobs_validate_insert
        BEFORE INSERT ON daily_summary_jobs
        WHEN NEW.status NOT IN ('queued', 'running', 'cancel_requested', 'completed', 'failed', 'canceled')
          OR NEW.progress_total < 0
          OR NEW.progress_current < 0
          OR (NEW.progress_total > 0 AND NEW.progress_current > NEW.progress_total)
          OR NEW.attempt < 0
        BEGIN
          SELECT RAISE(ABORT, 'invalid daily summary job state');
        END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS daily_summary_jobs_validate_update
        BEFORE UPDATE ON daily_summary_jobs
        WHEN NEW.status NOT IN ('queued', 'running', 'cancel_requested', 'completed', 'failed', 'canceled')
          OR NEW.progress_total < 0
          OR NEW.progress_current < 0
          OR (NEW.progress_total > 0 AND NEW.progress_current > NEW.progress_total)
          OR NEW.attempt < 0
        BEGIN
          SELECT RAISE(ABORT, 'invalid daily summary job state');
        END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS delivery_outbox_validate_insert
        BEFORE INSERT ON delivery_outbox
        WHEN NEW.status NOT IN ('pending', 'sending', 'retry', 'sent')
          OR NEW.chunk_index <= 0
          OR NEW.chunk_count <= 0
          OR NEW.chunk_index > NEW.chunk_count
          OR NEW.attempts < 0
        BEGIN
          SELECT RAISE(ABORT, 'invalid delivery outbox state');
        END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS delivery_outbox_validate_update
        BEFORE UPDATE ON delivery_outbox
        WHEN NEW.status NOT IN ('pending', 'sending', 'retry', 'sent')
          OR NEW.chunk_index <= 0
          OR NEW.chunk_count <= 0
          OR NEW.chunk_index > NEW.chunk_count
          OR NEW.attempts < 0
        BEGIN
          SELECT RAISE(ABORT, 'invalid delivery outbox state');
        END
        """,
    )
    for statement in statements:
        connection.execute(statement)


def _ensure_column(connection: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    rows = connection.execute(f"PRAGMA table_info({table})").fetchall()
    names = {str(row[1]) for row in rows}
    if column not in names:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


MIGRATIONS: dict[int, Migration] = {
    13: _migration_13,
    14: _migration_14,
}
