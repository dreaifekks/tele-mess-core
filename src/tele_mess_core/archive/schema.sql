CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  display_name TEXT,
  kind TEXT,
  updated_at TEXT NOT NULL,
  raw_json TEXT,
  PRIMARY KEY (source, account_id)
);


CREATE TABLE IF NOT EXISTS account_auth (
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  auth_state TEXT NOT NULL DEFAULT 'unknown',
  phone TEXT,
  session_name TEXT,
  session_dir TEXT,
  last_error TEXT,
  updated_at TEXT NOT NULL,
  raw_json TEXT,
  PRIMARY KEY (source, account_id)
);

CREATE TABLE IF NOT EXISTS chats (
  source TEXT NOT NULL,
  account_id TEXT NOT NULL DEFAULT 'default',
  chat_id INTEGER NOT NULL,
  title TEXT,
  username TEXT,
  kind TEXT,
  updated_at TEXT NOT NULL,
  raw_json TEXT,
  PRIMARY KEY (source, account_id, chat_id)
);

CREATE TABLE IF NOT EXISTS users (
  source TEXT NOT NULL,
  account_id TEXT NOT NULL DEFAULT 'default',
  user_id INTEGER NOT NULL,
  username TEXT,
  display_name TEXT,
  is_bot INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  raw_json TEXT,
  PRIMARY KEY (source, account_id, user_id)
);


CREATE TABLE IF NOT EXISTS origins (
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  origin_id INTEGER NOT NULL,
  topic_id INTEGER NOT NULL DEFAULT 0,
  origin_type TEXT NOT NULL,
  parent_origin_id INTEGER,
  title TEXT,
  username TEXT,
  is_forum INTEGER NOT NULL DEFAULT 0,
  important INTEGER NOT NULL DEFAULT 0,
  archived_at TEXT,
  last_message_at TEXT,
  discovered_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  raw_json TEXT,
  PRIMARY KEY (source, account_id, origin_id, topic_id)
);

CREATE TABLE IF NOT EXISTS backup_policies (
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  origin_id INTEGER NOT NULL,
  topic_id INTEGER NOT NULL DEFAULT 0,
  enabled INTEGER NOT NULL DEFAULT 0,
  capture_text INTEGER NOT NULL DEFAULT 1,
  capture_media_metadata INTEGER NOT NULL DEFAULT 1,
  download_media INTEGER NOT NULL DEFAULT 0,
  download_stickers INTEGER NOT NULL DEFAULT 0,
  tags TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (source, account_id, origin_id, topic_id)
);

CREATE TABLE IF NOT EXISTS participants (
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  origin_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  username TEXT,
  display_name TEXT,
  is_bot INTEGER NOT NULL DEFAULT 0,
  role TEXT,
  last_seen_at TEXT,
  updated_at TEXT NOT NULL,
  raw_json TEXT,
  PRIMARY KEY (source, account_id, origin_id, user_id)
);


CREATE TABLE IF NOT EXISTS capture_cursors (
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  origin_id INTEGER NOT NULL,
  topic_id INTEGER NOT NULL DEFAULT 0,
  last_message_id INTEGER NOT NULL DEFAULT 0,
  history_scanned_through_id INTEGER NOT NULL DEFAULT 0,
  observed_max_message_id INTEGER NOT NULL DEFAULT 0,
  last_message_at TEXT,
  last_backfill_at TEXT,
  backfill_head_message_id INTEGER,
  backfill_status TEXT,
  backfill_error TEXT,
  backfill_count INTEGER,
  updated_at TEXT NOT NULL,
  raw_json TEXT,
  PRIMARY KEY (source, account_id, origin_id, topic_id)
);

CREATE TABLE IF NOT EXISTS messages (
  source TEXT NOT NULL,
  account_id TEXT NOT NULL DEFAULT 'default',
  chat_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  topic_id INTEGER,
  sender_id INTEGER,
  sender_name TEXT,
  sender_username TEXT,
  sent_at TEXT NOT NULL,
  edited_at TEXT,
  ingested_at TEXT NOT NULL,
  deleted_at TEXT,
  text TEXT,
  has_media INTEGER NOT NULL DEFAULT 0,
  media_kind TEXT,
  grouped_id TEXT,
  reply_to_message_id INTEGER,
  forward_from_id TEXT,
  forward_from_name TEXT,
  permalink TEXT,
  reactions_json TEXT,
  raw_json TEXT,
  version INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (source, account_id, chat_id, message_id)
);


CREATE TABLE IF NOT EXISTS media_files (
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  chat_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  file_index INTEGER NOT NULL DEFAULT 0,
  file_path TEXT NOT NULL,
  media_kind TEXT,
  mime_type TEXT,
  file_size INTEGER,
  downloaded_at TEXT NOT NULL,
  raw_json TEXT,
  PRIMARY KEY (source, account_id, chat_id, message_id, file_index)
);

CREATE TABLE IF NOT EXISTS operation_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  operation TEXT NOT NULL,
  status TEXT NOT NULL,
  subject_type TEXT,
  subject_id TEXT,
  error_code TEXT,
  message TEXT,
  retry_after INTEGER,
  occurred_at TEXT NOT NULL,
  raw_json TEXT
);

CREATE TABLE IF NOT EXISTS daily_package_schedule (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  enabled INTEGER NOT NULL DEFAULT 0,
  time_of_day TEXT NOT NULL DEFAULT '08:00',
  timezone TEXT NOT NULL DEFAULT 'Asia/Tokyo',
  scope_json TEXT,
  system_manager TEXT NOT NULL DEFAULT 'systemd-user',
  installed INTEGER NOT NULL DEFAULT 0,
  last_installed_at TEXT,
  last_error TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_summary_delivery (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  enabled INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0, 1)),
  account_id TEXT,
  origin_id INTEGER,
  topic_id INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  CHECK (enabled = 0 OR (account_id IS NOT NULL AND account_id != '' AND origin_id IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS daily_package_runs (
  run_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  date TEXT NOT NULL,
  timezone TEXT NOT NULL,
  scope_json TEXT,
  output_dir TEXT,
  package_json_path TEXT,
  package_md_path TEXT,
  origin_count INTEGER NOT NULL DEFAULT 0,
  message_count INTEGER NOT NULL DEFAULT 0,
  media_count INTEGER NOT NULL DEFAULT 0,
  important_origin_count INTEGER NOT NULL DEFAULT 0,
  progress_total INTEGER NOT NULL DEFAULT 0,
  progress_current INTEGER NOT NULL DEFAULT 0,
  progress_label TEXT,
  progress_json TEXT,
  error TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT
);

CREATE TABLE IF NOT EXISTS daily_summary_runs (
  run_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  package_run_id TEXT,
  date TEXT,
  timezone TEXT,
  scope_json TEXT,
  output_dir TEXT,
  summary_path TEXT,
  provider TEXT,
  origin_count INTEGER NOT NULL DEFAULT 0,
  group_count INTEGER NOT NULL DEFAULT 0,
  image_count INTEGER NOT NULL DEFAULT 0,
  progress_total INTEGER NOT NULL DEFAULT 0,
  progress_current INTEGER NOT NULL DEFAULT 0,
  progress_label TEXT,
  progress_json TEXT,
  error TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT
);

CREATE TABLE IF NOT EXISTS daily_summary_jobs (
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
  request_json TEXT,
  dedupe_key TEXT,
  worker_id TEXT,
  lease_until TEXT,
  heartbeat_at TEXT,
  attempt INTEGER NOT NULL DEFAULT 0,
  retry_at TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  cancel_requested_at TEXT,
  error TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_outbox (
  outbox_id TEXT PRIMARY KEY,
  summary_run_id TEXT NOT NULL,
  job_id TEXT,
  account_id TEXT NOT NULL,
  origin_id INTEGER NOT NULL,
  topic_id INTEGER NOT NULL DEFAULT 0,
  content_kind TEXT NOT NULL DEFAULT 'summary',
  chunk_index INTEGER NOT NULL,
  chunk_count INTEGER NOT NULL,
  content TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  claim_token INTEGER NOT NULL DEFAULT 0,
  send_random_id INTEGER,
  message_id INTEGER,
  last_error TEXT,
  next_attempt_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  sent_at TEXT,
  UNIQUE(summary_run_id, account_id, origin_id, topic_id, content_kind, chunk_index)
);

CREATE TABLE IF NOT EXISTS daily_summary_records (
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

CREATE TABLE IF NOT EXISTS daily_message_points (
  point_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  package_run_id TEXT NOT NULL,
  date TEXT NOT NULL,
  timezone TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'telegram',
  account_id TEXT NOT NULL,
  origin_id INTEGER NOT NULL,
  topic_id INTEGER NOT NULL DEFAULT 0,
  origin_title TEXT,
  message_id INTEGER,
  occurred_at TEXT NOT NULL,
  tags_json TEXT NOT NULL DEFAULT '[]',
  tags_csv TEXT,
  content TEXT NOT NULL CHECK (length(trim(content)) > 0),
  telegram_deeplink TEXT,
  permalink TEXT,
  importance_score INTEGER NOT NULL DEFAULT 3 CHECK (importance_score BETWEEN 1 AND 5),
  importance_reason TEXT,
  origin_important INTEGER NOT NULL DEFAULT 0 CHECK (origin_important IN (0, 1)),
  source_refs_json TEXT NOT NULL DEFAULT '[]',
  provider TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  account_id TEXT NOT NULL DEFAULT 'default',
  event_type TEXT NOT NULL,
  chat_id INTEGER NOT NULL,
  message_id INTEGER,
  event_at TEXT NOT NULL,
  payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS media_download_jobs (
  job_id TEXT PRIMARY KEY,
  source TEXT NOT NULL DEFAULT 'telegram',
  account_id TEXT NOT NULL,
  chat_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  topic_id INTEGER NOT NULL DEFAULT 0,
  media_kind TEXT,
  is_sticker_like INTEGER NOT NULL DEFAULT 0,
  replace_existing INTEGER NOT NULL DEFAULT 0,
  target_edited_at TEXT,
  pending_file_path TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 8,
  claim_token INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT,
  last_error TEXT,
  error_code TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT,
  UNIQUE(source, account_id, chat_id, message_id)
);


CREATE INDEX IF NOT EXISTS idx_origins_account_type ON origins(source, account_id, origin_type);
CREATE INDEX IF NOT EXISTS idx_capture_cursors_account ON capture_cursors(source, account_id, origin_id);
CREATE INDEX IF NOT EXISTS idx_media_files_message ON media_files(source, account_id, chat_id, message_id);
CREATE INDEX IF NOT EXISTS idx_media_files_downloaded ON media_files(downloaded_at);
CREATE INDEX IF NOT EXISTS idx_media_files_file_path ON media_files(file_path);
CREATE INDEX IF NOT EXISTS idx_media_download_jobs_claim ON media_download_jobs(source, account_id, status, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_media_download_jobs_status ON media_download_jobs(status);
CREATE INDEX IF NOT EXISTS idx_media_download_jobs_downloading
ON media_download_jobs(source, account_id, updated_at)
WHERE status = 'downloading';
CREATE INDEX IF NOT EXISTS idx_media_download_jobs_pending_path
ON media_download_jobs(pending_file_path)
WHERE pending_file_path IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_operation_events_account ON operation_events(source, account_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_operation_events_status ON operation_events(status, error_code);
CREATE INDEX IF NOT EXISTS idx_backup_policies_enabled ON backup_policies(enabled);
CREATE INDEX IF NOT EXISTS idx_participants_account_origin ON participants(source, account_id, origin_id);
CREATE INDEX IF NOT EXISTS idx_daily_package_runs_date ON daily_package_runs(date, started_at);
CREATE INDEX IF NOT EXISTS idx_daily_summary_runs_package ON daily_summary_runs(package_run_id, started_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_summary_jobs_active_dedupe
ON daily_summary_jobs(dedupe_key)
WHERE dedupe_key IS NOT NULL
  AND status IN ('queued', 'running', 'cancel_requested');
CREATE INDEX IF NOT EXISTS idx_daily_summary_jobs_claim ON daily_summary_jobs(status, retry_at, lease_until, started_at);
CREATE INDEX IF NOT EXISTS idx_delivery_outbox_pending ON delivery_outbox(status, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_daily_summary_records_date ON daily_summary_records(date, created_at);
CREATE INDEX IF NOT EXISTS idx_daily_summary_records_package ON daily_summary_records(package_run_id, created_at);
CREATE INDEX IF NOT EXISTS idx_daily_summary_records_important ON daily_summary_records(important, created_at);
CREATE INDEX IF NOT EXISTS idx_daily_summary_records_type_date ON daily_summary_records(record_type, date, created_at);
CREATE INDEX IF NOT EXISTS idx_daily_message_points_run ON daily_message_points(run_id, occurred_at, point_id);
CREATE INDEX IF NOT EXISTS idx_daily_message_points_lookup ON daily_message_points(date, source, account_id, origin_id, topic_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_daily_message_points_importance ON daily_message_points(date, importance_score, occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_seq ON events(seq);
CREATE INDEX IF NOT EXISTS idx_events_chat_msg ON events(source, account_id, chat_id, message_id);
CREATE INDEX IF NOT EXISTS idx_messages_sent_at ON messages(sent_at);
CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(source, account_id, sender_id);
CREATE INDEX IF NOT EXISTS idx_messages_deleted ON messages(deleted_at);

CREATE VIRTUAL TABLE IF NOT EXISTS message_fts USING fts5(
  text,
  sender_name,
  content='messages',
  content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
  INSERT INTO message_fts(rowid, text, sender_name)
  VALUES (new.rowid, COALESCE(new.text, ''), COALESCE(new.sender_name, ''));
END;

CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
  INSERT INTO message_fts(message_fts, rowid, text, sender_name)
  VALUES ('delete', old.rowid, COALESCE(old.text, ''), COALESCE(old.sender_name, ''));
END;

CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages
WHEN old.text IS NOT new.text OR old.sender_name IS NOT new.sender_name
BEGIN
  INSERT INTO message_fts(message_fts, rowid, text, sender_name)
  VALUES ('delete', old.rowid, COALESCE(old.text, ''), COALESCE(old.sender_name, ''));
  INSERT INTO message_fts(rowid, text, sender_name)
  VALUES (new.rowid, COALESCE(new.text, ''), COALESCE(new.sender_name, ''));
END;

CREATE TRIGGER IF NOT EXISTS daily_summary_jobs_validate_insert
BEFORE INSERT ON daily_summary_jobs
WHEN NEW.status NOT IN ('queued', 'running', 'cancel_requested', 'completed', 'failed', 'canceled')
  OR NEW.progress_total < 0
  OR NEW.progress_current < 0
  OR (NEW.progress_total > 0 AND NEW.progress_current > NEW.progress_total)
  OR NEW.attempt < 0
BEGIN
  SELECT RAISE(ABORT, 'invalid daily summary job state');
END;

CREATE TRIGGER IF NOT EXISTS daily_summary_jobs_validate_update
BEFORE UPDATE ON daily_summary_jobs
WHEN NEW.status NOT IN ('queued', 'running', 'cancel_requested', 'completed', 'failed', 'canceled')
  OR NEW.progress_total < 0
  OR NEW.progress_current < 0
  OR (NEW.progress_total > 0 AND NEW.progress_current > NEW.progress_total)
  OR NEW.attempt < 0
BEGIN
  SELECT RAISE(ABORT, 'invalid daily summary job state');
END;

CREATE TRIGGER IF NOT EXISTS delivery_outbox_validate_insert
BEFORE INSERT ON delivery_outbox
WHEN NEW.status NOT IN ('pending', 'sending', 'retry', 'sent', 'failed')
  OR length(trim(COALESCE(NEW.content_kind, ''))) = 0
  OR NEW.chunk_index <= 0
  OR NEW.chunk_count <= 0
  OR NEW.chunk_index > NEW.chunk_count
  OR NEW.attempts < 0
  OR NEW.claim_token < 0
BEGIN
  SELECT RAISE(ABORT, 'invalid delivery outbox state');
END;

CREATE TRIGGER IF NOT EXISTS delivery_outbox_validate_update
BEFORE UPDATE ON delivery_outbox
WHEN NEW.status NOT IN ('pending', 'sending', 'retry', 'sent', 'failed')
  OR length(trim(COALESCE(NEW.content_kind, ''))) = 0
  OR NEW.chunk_index <= 0
  OR NEW.chunk_count <= 0
  OR NEW.chunk_index > NEW.chunk_count
  OR NEW.attempts < 0
  OR NEW.claim_token < 0
BEGIN
  SELECT RAISE(ABORT, 'invalid delivery outbox state');
END;

CREATE TRIGGER IF NOT EXISTS media_download_jobs_validate_insert
BEFORE INSERT ON media_download_jobs
WHEN NEW.status NOT IN ('pending', 'downloading', 'retry', 'completed', 'failed', 'canceled')
  OR NEW.attempts < 0
  OR NEW.max_attempts <= 0
  OR NEW.claim_token < 0
  OR NEW.is_sticker_like NOT IN (0, 1)
  OR NEW.replace_existing NOT IN (0, 1)
BEGIN
  SELECT RAISE(ABORT, 'invalid media download job state');
END;

CREATE TRIGGER IF NOT EXISTS media_download_jobs_validate_update
BEFORE UPDATE ON media_download_jobs
WHEN NEW.status NOT IN ('pending', 'downloading', 'retry', 'completed', 'failed', 'canceled')
  OR NEW.attempts < 0
  OR NEW.max_attempts <= 0
  OR NEW.claim_token < 0
  OR NEW.is_sticker_like NOT IN (0, 1)
  OR NEW.replace_existing NOT IN (0, 1)
BEGIN
  SELECT RAISE(ABORT, 'invalid media download job state');
END;
