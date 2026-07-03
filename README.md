# tele-mess-core

`tele-mess-core` is a single-user, multi-Telegram-account archive core for
future Mac and web clients.

It stores Telegram messages in SQLite and exposes sync plus management endpoints
for remote clients. The core is meant to run as a long-running local database and
control plane: clients manage account authentication, origin discovery, backup
selection, capture policy, topics, and participant metadata through the core API.

It intentionally does not forward messages to backup Telegram groups and does not
run summary generation on the server. See
[docs/product-direction.md](docs/product-direction.md) for the management
interface direction.

## Current Scope

- Telegram ingestion with Telethon.
- Multiple Telegram accounts feeding one archive.
- SQLite archive for chats, users, messages, reactions, and event cursors.
- Cursor-based HTTP sync API for LAN or Tailscale use.
- Token-protected management API for account state, origins, backup policies,
  topics, participant metadata, and capture cursors.
- Built-in web console for the same management surface at `GET /console`.
- Policy-aware ingestion with bounded history backfill and reconnect catch-up.
- Live origin discovery and participant refresh endpoints for authenticated
  Telegram sessions.
- Runtime operation events for Telegram auth/discovery/media-download failures.
- Server daemon mode for devNuc-style always-on deployment.

## Quick Start

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
cp config.example.yml config.yml
tele-mess-core init-db --config config.yml
tele-mess-core smoke-telegram --config config.yml
tele-mess-core run-server --config config.yml
```

The first Telethon run may ask for Telegram login if no session file exists.
See [docs/server-mode.md](docs/server-mode.md) for the devNuc-style deployment
shape and client sync contract.

Use `telegram.accounts[]` for multi-account auth/runtime configuration. Message
capture sources are managed in SQLite through origin discovery plus backup
policies; `telegram.chats` in config is no longer used.

## Sync API

- `GET /healthz`
- `GET /sync/state`
- `GET /sync/events?after=0&limit=500`
- `GET /sync/messages?after=0&limit=500`
- `GET /sync/accounts`
- `GET /sync/chats`
- `GET /sync/search?q=term`
- `GET /sync/media-files?account_id=main`

## Management API

- `GET /manage/capabilities`
- `GET /manage/accounts`
- `POST /manage/accounts`
- `POST` or `PATCH /manage/accounts/auth`
- `POST /manage/accounts/auth/status`
- `POST /manage/accounts/auth/request-code`
- `POST /manage/accounts/auth/submit-code`
- `GET /manage/origins?account_id=main`
- `POST /manage/origins`
- `GET /manage/backup-policies?account_id=main`
- `POST` or `PATCH /manage/backup-policies`
- `GET /manage/participants?account_id=main&origin_id=-100123`
- `POST /manage/participants`
- `GET /manage/capture-cursors?account_id=main`
- `GET /manage/operation-events?account_id=main&status=failed`
- `POST /manage/discover-origins`
- `POST /manage/participants/refresh`
- `GET /console`

The authoritative API reference is generated from
`src/tele_mess_core/server/contracts.py`:

- `docs/api.md` for human-readable endpoint docs.
- `docs/openapi.json` for tools.
- `docs/api-agent.md` for short agent lookup.
- `GET /manage/api-manifest` for the runtime contract version/hash and route
  registry.
- `GET /openapi.json` and `GET /docs/api.md` for runtime docs served by the
  core process.

Regenerate and verify these files with:

```bash
tele-mess-core generate-api-docs
tele-mess-core generate-api-docs --check
```

`GET /console` serves the built-in management console. The page can be opened in
a browser without a token header, then the operator enters `server.token` in the
page. API calls from the console still use the same token-protected management
and sync endpoints as external clients.

If `server.token` is configured, pass it as:

```text
Authorization: Bearer <token>
```

or:

```text
X-Api-Token: <token>
```

## Media Backup Semantics

Backup policy separates three media modes:

- `capture_text`: store message text.
- `capture_media_metadata`: store Telegram media metadata in the message row.
- `download_media`: download media files and expose them through `/sync/media-files`.

Media files requested by `download_media: true` are stored under a `media/`
directory next to the SQLite database. Download failures are retried according
to `telegram.media_download` and then recorded in `/manage/operation-events`
if they still fail.

## Design Boundary

The server is responsible for durable collection, sync, and capture-management
state. Client-side features such as search UI, summaries, labels, AI workflows,
and higher-level processing should live in the Mac app.

## License

Apache-2.0. See [LICENSE](LICENSE).
