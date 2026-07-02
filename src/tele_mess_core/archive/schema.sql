PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

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
  last_message_at TEXT,
  last_backfill_at TEXT,
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


CREATE INDEX IF NOT EXISTS idx_origins_account_type ON origins(source, account_id, origin_type);
CREATE INDEX IF NOT EXISTS idx_capture_cursors_account ON capture_cursors(source, account_id, origin_id);
CREATE INDEX IF NOT EXISTS idx_media_files_message ON media_files(source, account_id, chat_id, message_id);
CREATE INDEX IF NOT EXISTS idx_media_files_downloaded ON media_files(downloaded_at);
CREATE INDEX IF NOT EXISTS idx_operation_events_account ON operation_events(source, account_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_operation_events_status ON operation_events(status, error_code);
CREATE INDEX IF NOT EXISTS idx_backup_policies_enabled ON backup_policies(enabled);
CREATE INDEX IF NOT EXISTS idx_participants_account_origin ON participants(source, account_id, origin_id);

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

CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
  INSERT INTO message_fts(message_fts, rowid, text, sender_name)
  VALUES ('delete', old.rowid, COALESCE(old.text, ''), COALESCE(old.sender_name, ''));
  INSERT INTO message_fts(rowid, text, sender_name)
  VALUES (new.rowid, COALESCE(new.text, ''), COALESCE(new.sender_name, ''));
END;
