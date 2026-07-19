# tele-mess-core

`tele-mess-core` is a single-user, multi-Telegram-account archive core for
future Mac and web clients.

It stores Telegram messages in SQLite and exposes sync plus management endpoints
for remote clients. The core is meant to run as a long-running local database and
control plane: clients manage account authentication, origin discovery, backup
selection, capture policy, topics, and participant metadata through the core API.

It intentionally does not forward messages to backup Telegram groups. Daily
package generation and local Codex-backed daily summaries run as local jobs
against the archive. See
[product direction](https://github.com/dreaifekks/tele-mess-core/blob/master/docs/product-direction.md)
for the management interface direction and
[daily packaging](https://github.com/dreaifekks/tele-mess-core/blob/master/docs/daily-packaging.md)
for daily package and AI analysis details.

## Current Scope

- Telegram ingestion with Telethon.
- Multiple Telegram accounts feeding one archive.
- One supervised, long-lived Telethon client per account, shared by ingestion,
  auth, discovery, participant refresh, and summary delivery.
- SQLite archive for chats, users, messages, reactions, and event cursors.
- Cursor-based HTTP sync API for LAN or Tailscale use.
- Token-protected management API for account state, origins, backup policies,
  topics, participant metadata, and capture cursors.
- Built-in web console for the same management surface at `GET /console`.
- Policy-aware ingestion with bounded history backfill and reconnect catch-up.
- Live origin discovery and participant refresh endpoints for authenticated
  Telegram sessions.
- Runtime operation events for Telegram auth/discovery/media-download failures.
- Server daemon mode for an always-on Linux deployment.
- macOS-oriented local CLI mode with durable jobs and no HTTP listener by
  default.
- Daily package generation by origin, tag group, timezone, and local date.
- Local Codex-backed daily analysis with important-origin full-context reports,
  all-origin structured message points, and point-based daily digests.
- Durable daily package-and-summary jobs with deduplication, cancellation,
  restart recovery, leases, and a retryable Telegram delivery outbox.
- System-managed daily package and summary scheduling through user-level systemd
  timer files.
- Optional raw Telegram JSON retention cleanup for keeping the SQLite archive
  compact while preserving structured message rows.

## Run from PyPI with uvx

[`uvx`](https://docs.astral.sh/uv/guides/tools/) installs the published package
and its Python dependencies into an isolated cache, then runs the
`tele-mess-core` command. It does not clone the repository into the current
directory, and runtime state remains in the selected workspace rather than the
tool environment.

Once a release is available on PyPI and the Mac workspace contains a valid
`config.yml`, the local core is one command:

```bash
uvx tele-mess-core run-local
```

Use the same stable workspace across invocations, or select one explicitly:

```bash
uvx tele-mess-core run-local \
  --workspace "$HOME/Library/Application Support/tele-mess-core-personal"
```

For a reproducible launcher, pin the distribution while keeping the executable
name explicit:

```bash
uvx --from "tele-mess-core==X.Y.Z" tele-mess-core run-local
```

Maintainers can find the tag and Trusted Publishing process in the
[release guide](https://github.com/dreaifekks/tele-mess-core/blob/master/docs/releasing.md).

A fresh machine still needs Telegram API credentials and a local configuration
before `run-local` can ingest messages. In the macOS product, `mess-end` owns
that first-run UI and drives the core management/auth API for code/2FA login,
origin discovery, and capture-policy management; these are intentionally not
duplicated as interactive core CLI setup. A standalone operator may enable
`run-local --web` temporarily as a fallback. The normal local runtime keeps HTTP
disabled.

## Source Checkout

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
cp config.example.yml config.yml
tele-mess-core init-db --config config.yml
tele-mess-core smoke-telegram --config config.yml
tele-mess-core run-server --config config.yml
```

If no authorized Telegram session exists, use the built-in console while
`run-server` is active to request a login code and submit code/2FA credentials.
See the
[server-mode guide](https://github.com/dreaifekks/tele-mess-core/blob/master/docs/server-mode.md)
for the always-on deployment shape and client sync contract. See the
[daily-packaging guide](https://github.com/dreaifekks/tele-mess-core/blob/master/docs/daily-packaging.md)
for the daily packaging, scheduling, and staged AI analysis workflow.

## macOS Local Mode

`run-local` starts Telegram ingestion plus the durable daily worker without
starting the HTTP API or web console. On macOS its default workspace is
`~/Library/Application Support/tele-mess-core`, so it does not depend on the
Terminal or launcher current directory.

```bash
mkdir -p "$HOME/Library/Application Support/tele-mess-core"
cp config.example.yml "$HOME/Library/Application Support/tele-mess-core/config.yml"
tele-mess-core paths
tele-mess-core run-local
```

Use `--workspace PATH` (alias `--work-dir`),
`TELE_MESS_CORE_WORKSPACE`, or `TELE_MESS_CORE_HOME` to select another stable
instance root. `TELE_MESS_CORE_CONFIG` or an explicit `--config` selects a
specific config. `tele-mess-core paths` prints the resolved non-secret paths
without opening the database.

HTTP remains opt-in in local mode:

```bash
tele-mess-core run-local --web
```

This enables the existing API and `/console`; it never opens a browser
automatically. See the
[local-mode guide](https://github.com/dreaifekks/tele-mess-core/blob/master/docs/local-mode.md)
for precedence, path semantics, first-login limitations, and configuration
examples.

Use `telegram.accounts[]` for multi-account auth/runtime configuration. Message
capture sources are managed in SQLite through origin discovery plus backup
policies; `telegram.chats` in config is no longer used.

## Raw JSON Cleanup

Message rows keep structured fields plus a raw Telethon JSON payload for recent
forensics. The raw payload can be cleared after a retention window without
removing message text, timestamps, senders, search data, or sync cursors.

```bash
tele-mess-core cleanup-raw-json --config config.yml --retention-days 7
tele-mess-core cleanup-raw-json --config config.yml --retention-days 7 --dry-run
tele-mess-core raw-json-cleanup-schedule --config config.yml install --activate-systemd
```

The cleanup timer defaults to `OnCalendar=weekly` and reads
`storage.raw_json_retention_days`, which defaults to `7`. Add `--vacuum` only
when you want the SQLite file to shrink immediately; without it, SQLite reuses
the freed pages for later messages.

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
- `GET` or `PATCH /manage/daily-package-schedule`
- `GET` or `PATCH /manage/daily-summary-delivery`
- `POST /manage/daily-packages`
- `GET /manage/daily-package-runs`
- `POST /manage/daily-summaries`
- `POST` or `GET /manage/daily-summary-jobs`
- `PATCH /manage/daily-summary-jobs/cancel`
- `GET /manage/daily-summary-runs`
- `GET /manage/daily-summary-records`
- `GET /manage/daily-summary-records/item`
- `GET /manage/daily-message-points`
- `GET /manage/daily-message-points/item`
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
and sync endpoints as external clients. The console keeps the token in tab
session storage rather than persistent browser storage.

If `server.token` is configured, pass it as:

```text
Authorization: Bearer <token>
```

or:

```text
X-Api-Token: <token>
```

The server requires a token by default. An empty token is accepted only when
`server.allow_unauthenticated_localhost: true` is explicitly configured and the
server is bound to a loopback address.

## Media Backup Semantics

Backup policy separates three media modes:

- `capture_text`: store message text.
- `capture_media_metadata`: store Telegram media metadata in the message row.
- `download_media`: download media files and expose them through `/sync/media-files`.

Media files requested by `download_media: true` are stored under a `media/`
directory next to the SQLite database. Download failures are retried according
to `telegram.media_download` and then recorded in `/manage/operation-events`
if they still fail.

## Daily Packaging

Daily packages are generated from already archived messages. The run selects
enabled, non-removed backup origins by account, origin, topic, tag intersection,
or tag groups, then skips origins with no messages in the selected daily
window. Parent origins and forum topics are grouped together by the parent's
tags unless a topic has explicit different tags or is marked important.
When no ad hoc tag group scope is supplied, origins are grouped by their
effective CSV tag set for package navigation and point metadata. Explicit tag
groups are assigned from most-specific to least-specific, but unmatched origins
still enter the all-origin point flow. Normal tag groups no longer create their
own summary records.

Origin rows can be marked `important`; important origins are packaged separately
and analyzed in full context. Every eligible origin, including important ones,
also participates in a separate structured message-point pipeline. Daily runs
therefore produce two independent products:

- image media analysis with OCR/visual extraction through Codex image inputs;
- non-image long media such as PDF/video preserved as file references;
- message-point extraction from important and non-important origins, with time,
  tags, content, Telegram links, importance, and source references;
- full-context analysis and a daily report sourced only from important origins;
- a separate daily digest sourced only from the persisted message points.

Package and summary artifacts are written under the configured daily output
directory, while SQLite stores run status, paths, counts, errors, typed summary
records, and individually queryable message points for API lookup/filtering.
Normal point queries expose completed runs; diagnostic callers can opt into
failed, canceled, or still-running run points explicitly.

When Telegram delivery is enabled, the important report and point digest are
sent as separate logical messages to the configured target. The point digest
uses the fixed searchable tag `#point`; the important report keeps its source
tags.

The default Codex CLI template selects `gpt-5.6-sol` and expands task-specific
`{model}` and `{output_schema}` placeholders before invoking the provider.

API and scheduled package-plus-summary requests use the same durable SQLite job
queue. Equivalent active or completed requests are deduplicated unless
`force: true` or CLI `--force` is supplied. Summary records and delivery outbox
chunks are committed atomically; delivery failures remain retryable without
turning an already completed summary into a failed run.

## Runtime Architecture

- `TelegramRuntimeManager` supervises each account independently and reuses one
  connected client for all Telegram operations.
- `DailyJobWorker` owns package-plus-summary execution, lease recovery,
  cancellation, and delivery-outbox draining. No background job depends on an
  untracked daemon thread.
- `ArchiveStore` uses WAL, busy timeouts, explicit short transactions, and one
  SQLite connection per worker/request thread.
- Numbered, transactional migrations upgrade the archive schema. Job and
  outbox state transitions have database-level validation triggers.
- The HTTP route/auth registry and request validation are driven by
  `server/contracts.py`; generated Markdown/OpenAPI files and runtime docs share
  the same contract hash.

The default AI provider is a configurable local `codex exec` command template
using `--output-last-message`. Templates can use `{output}`, `{images}`, and
`{task}`. An optional `daily.ai.fallback` can switch the remainder of a run to
an OpenAI-compatible Responses endpoint only when Codex reports a usage limit.
The API key is read from an ignored local file; transient fallback failures can
be durably retried once after a configured delay. Set `daily.ai.provider:
disabled` only for local testing or dry runs.

## Design Boundary

The server is responsible for durable collection, sync, capture-management
state, daily packaging, and local daily analysis jobs. Client-side features such
as labels and app-specific UI state should live in the Mac app.

`mess-end` is the host-facing manager, while `tele-mess-core` remains an
independently runnable engine:

| Owner | Responsibilities |
| --- | --- |
| `tele-mess-core` | Telegram sessions, SQLite schema and migrations, ingestion, capture policy, daily jobs, delivery, and optional HTTP/API service. |
| `mess-end` | Version selection and installation, workspace selection, first-run UI, process start/stop/status, updates, and macOS LaunchAgent integration. |

`mess-end` should use the public CLI and management API rather than importing
internal Python modules or modifying the archive database directly. It should
also enforce one core process per workspace: `run-local` and `run-server` must
not own the same Telegram sessions and SQLite archive simultaneously. For a
persistent managed installation, `mess-end` should point LaunchAgent at a
stable, pinned executable rather than an incidental `uvx` cache path.

## License

Apache-2.0. See the
[license](https://github.com/dreaifekks/tele-mess-core/blob/master/LICENSE).
