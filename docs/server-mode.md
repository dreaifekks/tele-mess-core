# Server Mode

Server mode runs `tele-mess-core` on an always-on host such as devNuc.

The server owns Telegram collection, durable storage, and capture-management
state. A Mac client connects over LAN or Tailscale and uses the HTTP API for
sync plus management.

## Responsibilities

The server does:

- Keep one Telethon session online per configured account.
- Subscribe to configured Telegram chats across one or more accounts.
- Store new messages, edits, deletes, and reactions in SQLite.
- Expose cursor-based sync endpoints.
- Expose token-protected management endpoints for account state, origins,
  backup policies, participant metadata, and capture cursors.
- Serve a built-in web console for account login, origin selection, backup
  policy editing, participant refresh, cursor inspection, and media-file
  inspection.
- Run bounded history backfill and reconnect catch-up using per-origin cursors.
- Discover origins/topics and refresh participants from authenticated Telegram
  sessions on management request.
- Record structured operation events for Telegram auth, discovery, participant
  refresh, and media-download failures.

The server does not:

- Forward messages to backup Telegram groups.
- Generate summaries.
- Manage Mac UI state, labels, or client-specific processing.

## Data Flow

```text
Telegram account(s) -> Telethon adapter(s) -> SQLite archive -> Sync API -> Mac client
Mac/web client -> Management API -> account/origin/policy/participant tables
```

## Config

Copy `config.example.yml` to `config.yml` and edit:

```yaml
storage:
  data_dir: "/home/dreaife/.local/share/tele-mess-core"
  database: "/home/dreaife/.local/share/tele-mess-core/archive.db"

telegram:
  backfill:
    enabled: true
    initial_limit: 1000
    catch_up_limit: 1000
  media_download:
    retries: 2
    retry_delay_seconds: 1.0
  accounts:
    - account_id: "main"
      api_id: 123456
      api_hash: "your_api_hash_here"
      session_name: "main"
      session_dir: "/home/dreaife/.local/share/tele-mess-core/sessions"
      chats:
        - id: -1001234567890
          name: "Source Group"
    - account_id: "alt"
      api_id: 654321
      api_hash: "another_api_hash_here"
      session_name: "alt"
      session_dir: "/home/dreaife/.local/share/tele-mess-core/sessions"
      chats: []

server:
  host: "127.0.0.1"
  port: 8765
  token: "change-this"

logging:
  file: "/home/dreaife/.local/state/tele-mess-core/tele-mess-core.log"
```

For Mac access from another machine, bind `server.host` to a LAN/Tailscale
address or put a reverse proxy in front of the local server.

## Commands

```bash
tele-mess-core init-db --config config.yml
tele-mess-core smoke-telegram --config config.yml
tele-mess-core run-server --config config.yml
```

For debugging:

```bash
tele-mess-core serve-api --config config.yml
tele-mess-core run-telegram --config config.yml
tele-mess-core smoke-telegram --config config.yml --discover-origins --topic-limit 20
```

## Sync Contract

Clients should store the last consumed event cursor. Rows are keyed by
`source`, `account_id`, `chat_id`, and `message_id`.

1. Call `GET /sync/state`.
2. Call `GET /sync/accounts` and `GET /sync/chats` for account/source metadata.
3. Call `GET /sync/messages?after=<last_event_seq>&limit=500`.
4. Upsert returned message rows locally.
5. Save `next_cursor`.
6. Repeat while `has_more` is true.

Clients can also call `GET /sync/events` if they need raw event history.

Deleted messages are represented as message rows with `deleted_at` set.

## Management Contract

Management endpoints use the same token as sync endpoints. They provide the
basic control-plane model for future Mac and web clients:

1. Call `GET /manage/capabilities` to inspect supported management objects.
2. Call `GET /manage/accounts` to list configured or remotely registered
   Telegram accounts and auth/session state.
3. Use `POST /manage/accounts`, `POST /manage/accounts/auth/request-code`,
   and `POST /manage/accounts/auth/submit-code` to register account metadata,
   request Telegram login codes, submit login codes, and provide 2FA passwords
   when Telegram requires one.
4. Use `GET` and `POST /manage/origins` to maintain group/channel/chat/topic
   metadata.
5. Use `GET` and `POST` or `PATCH /manage/backup-policies` to control whether
   an origin is backed up and whether text, media metadata, or media files are
   desired. Downloaded media files are stored under `media/` next to the SQLite
   database and exposed through `GET /sync/media-files`.
6. Use `GET /manage/capture-cursors` to inspect backfill/catch-up progress.
7. Use `GET /manage/operation-events` to inspect recent structured runtime
   failures and partial operations.
8. Use `POST /manage/discover-origins` to discover dialogs and best-effort forum
   topics from an already authenticated Telegram session.
9. Use `POST /manage/participants/refresh` to refresh participant profiles for a
   specific origin.
10. Use `GET /console` for the built-in web console. The console can be opened
   in a browser without a token header, then the operator enters `server.token`;
   all API calls still use the token-protected sync and management endpoints.

History backfill runs when Telegram ingestion starts. It uses `telegram.backfill`
limits and per-origin cursors so restarts only ask Telegram for messages newer
than the last stored message ID.
