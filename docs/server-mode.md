# Server Mode

Server mode runs `tele-mess-core` on an always-on host such as devNuc.

The server owns Telegram collection, durable storage, and capture-management
state. A Mac client connects over LAN or Tailscale and uses the HTTP API for
sync plus management.

Daily packaging and staged AI analysis details live in
[daily-packaging.md](daily-packaging.md).

## Responsibilities

The server does:

- Keep one Telethon session online per configured account.
- Subscribe to configured Telegram chats across one or more accounts.
- Store new messages, edits, deletes, and reactions in SQLite.
- Expose cursor-based sync endpoints.
- Expose token-protected management endpoints for account state, origins,
  backup policies, participant metadata, and capture cursors.
- Serve a built-in web console for account login, origin selection, backup
  policy editing, participant refresh, cursor/media inspection, daily run
  status, typed summary records, and message-point lookup.
- Run bounded history backfill and reconnect catch-up using per-origin cursors.
- Discover origins/topics and refresh participants from authenticated Telegram
  sessions on management request.
- Record structured operation events for Telegram auth, discovery, participant
  refresh, and media-download failures.
- Generate daily packages from archived messages by local date, timezone,
  origin, and tag group.
- Run local Codex-backed daily analysis on demand: image OCR/visual analysis,
  all-origin structured message-point extraction, full-context important-origin
  analysis, an important-only daily report, and a separate point-based digest.
- Manage a daily package system timer through user-level systemd timer files.

The server does not:

- Forward messages to backup Telegram groups.
- Manage Mac UI state, labels, or client-specific processing.

## Data Flow

```text
Telegram account(s) -> Telethon adapter(s) -> SQLite archive -> Sync API -> Mac client
Mac/web client -> Management API -> account/origin/policy/participant tables
Systemd user timer -> CLI daily-package -> SQLite archive -> package files
Manual/API trigger -> daily-summary -> staged local Codex CLI tasks -> important report + message points + point digest
API/systemd trigger -> durable daily job queue -> package + analysis -> typed summary records + SQLite points -> delivery outbox -> Telegram target
```

## Runtime Ownership

`run-server` is the lifecycle owner for three explicit components:

- `TelegramRuntimeManager`, which keeps one long-lived client per account and
  shares it across ingestion, auth, discovery, participant refresh, and summary
  delivery. Account reconnect failures are isolated and retried independently.
- `DailyJobWorker`, which claims durable SQLite jobs with leases, recovers work
  after restart, observes cancellation, and drains retryable delivery-outbox
  chunks.
- `SyncApiServer`, whose route authorization and request shape checks are read
  from the same contract registry that generates the OpenAPI and Markdown docs.

`ArchiveStore` gives each request/worker thread its own SQLite connection. WAL,
busy timeouts, and short `BEGIN IMMEDIATE` claim/commit sections keep long AI or
Telegram operations outside database transactions.

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
    - account_id: "alt"
      api_id: 654321
      api_hash: "another_api_hash_here"
      session_name: "alt"
      session_dir: "/home/dreaife/.local/share/tele-mess-core/sessions"

server:
  host: "127.0.0.1"
  port: 8765
  token: "replace-with-a-long-random-token"
  allow_unauthenticated_localhost: false

logging:
  file: "/home/dreaife/.local/state/tele-mess-core/tele-mess-core.log"

daily:
  output_dir: "/home/dreaife/.local/share/tele-mess-core/daily-packages"
  systemd_user_dir: "/home/dreaife/.config/systemd/user"
  cli_path: "/home/dreaife/dev/tele-mess-core/.venv/bin/tele-mess-core"
  ai:
    provider: "codex-cli"
    model: "gpt-5.6-sol"
    # command can use {model}, {output_schema}, {output}, {images}, and {task}
    timeout_seconds: 900
```

For Mac access from another machine, bind `server.host` to a LAN/Tailscale
address or put a reverse proxy in front of the local server.

`server.token` is required by default. For isolated local development only,
you can set `allow_unauthenticated_localhost: true` while keeping `host` on a
loopback address. The server refuses to start unauthenticated on LAN, Tailscale,
or wildcard bind addresses.

Telegram chat/channel/private/topic sources are not configured in YAML. Use
`/console` or the management API to discover origins and enable backup policies;
the running ingestion process reads enabled policies from SQLite.

## Commands

```bash
tele-mess-core init-db --config config.yml
tele-mess-core smoke-telegram --config config.yml
tele-mess-core run-server --config config.yml
tele-mess-core daily-package --config config.yml --date 2026-07-03 --timezone Asia/Tokyo
tele-mess-core daily-summary --config config.yml --package-run-id <run-id>
tele-mess-core daily-schedule --config config.yml install --activate-systemd
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
   Clients that need to detect interface changes should also call
   `GET /manage/api-manifest` and compare `contract_hash` before issuing
   write requests.
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
10. Use `GET`/`PATCH /manage/daily-package-schedule` to inspect or update the
   daily package system timer settings.
11. Use `GET`/`PATCH /manage/daily-summary-delivery` to inspect or persist the
   account, group/channel, and optional topic that receives the important report
   and the separate `#point` digest. SQLite delivery settings override the YAML
   fallback.
12. Use `POST /manage/daily-packages` to generate one package immediately, and
   `GET /manage/daily-package-runs` to inspect package run state.
13. Use `POST /manage/daily-summaries` to enqueue or wait for one summary,
   `GET /manage/daily-summary-runs` to inspect run state, and
   `GET /manage/daily-summary-records` to list/filter typed important and point
   summary content. Use `GET /manage/daily-message-points` and
   `GET /manage/daily-message-points/item` to search or inspect the structured
   points extracted from all eligible origins.
14. Use `POST`/`GET /manage/daily-summary-jobs` to enqueue and inspect the
    durable package-plus-summary workflow, and
    `PATCH /manage/daily-summary-jobs/cancel` to request cancellation.
15. Use `GET /console` for the built-in web console. The console can be opened
   in a browser without a token header, then the operator enters `server.token`;
   all API calls still use the token-protected sync and management endpoints.

History backfill runs when Telegram ingestion starts. It uses `telegram.backfill`
limits and per-origin cursors so restarts only ask Telegram for messages newer
than the last stored message ID.

## API Documentation

The API reference is generated from `src/tele_mess_core/server/contracts.py`.

```bash
tele-mess-core generate-api-docs
tele-mess-core generate-api-docs --check
```

Generated files live at `docs/api.md`, `docs/openapi.json`, and
`docs/api-agent.md`. The running server also exposes `GET /openapi.json`,
`GET /docs/api.md`, and token-protected `GET /manage/api-manifest`.
