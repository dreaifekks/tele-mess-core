# tele-mess-core Server Mode Plan

## Goal

Build `tele-mess-core` as a single-user, multi-Telegram-account archive core.
It should run as a durable local database and remotely managed control plane for
future Mac and web clients.

The primary product surface is the client/core management interface:

- Telegram account authentication and session management.
- Telegram origin discovery for groups, channels, private chats, and group topics.
- Backup selection and capture policy per origin.
- Text-only versus media-inclusive backup configuration.
- Participant/member metadata capture for future important-source recognition.
- Cursor-based sync for client-side mirrors and UI state.

The core must also:
- Keep the original devNuc `group-backup-bot` repo untouched.
- Drop backup-group forwarding from the product center.
- Drop server-side summary generation from the first version.
- Store Telegram messages/events in a local SQLite archive.
- Expose sync and management endpoints for future Tauri/Svelte Mac and web
  clients.

## Phases

| Phase | Status | Notes |
| --- | --- | --- |
| Planning files and repo scaffold | complete | Empty workspace initialized as new repo content. |
| SQLite archive and models | complete | Message/event/chat/user tables plus cursor sync. |
| Telegram ingestion daemon | complete | Telethon adapter for new/edit/delete/reaction events, policy-aware storage, and bounded backfill/catch-up. |
| Sync HTTP API and CLI | complete | Stdlib HTTP server to avoid unnecessary runtime dependencies. |
| Management API foundation | complete | Account auth state, origin registry, backup policies, participant metadata, capture cursors, live discovery, and participant refresh are exposed over token-protected HTTP endpoints. |
| Operational hardening | complete | Expected Telegram auth/discovery errors, bounded topic/participant scans, media-download retry/failure tracking, operation-event API, and Telegram smoke CLI are implemented. |
| Docs/config/deploy template | complete | DevNuc server-mode config and systemd template added. |
| Local verification | complete | Unit tests, compileall, CLI help, and Telegram smoke checks are used to verify the core; `/console` smoke test returns 200 while API endpoints keep token protection. |

## Decisions

- SQLite is the source of truth for server mode.
- Telegram backup-group forwarding is not implemented in this repo.
- Summary/AI processing is client-side future work.
- Sync API is read-only and designed for LAN/Tailscale access; management API supports token-protected writes for control-plane state.
- Use stdlib HTTP server for sync API to keep deployment small; Telethon and PyYAML remain runtime dependencies for ingestion/config.
- Telegram ingestion is account-aware; `telegram.accounts[]` is the formal config shape, while the old single-account shape maps to `account_id: default`.
- Product scope is single owner/user with multiple Telegram accounts, not a multi-tenant service.
- Future clients should manage Telegram account authentication, origin discovery, backup selection, text/media capture policy, topics, and participant metadata through explicit core APIs.

## Management Interface Direction

| Area | Status | Notes |
| --- | --- | --- |
| Product direction doc | complete | See `docs/product-direction.md`. |
| Telegram account management API | complete | Remote account metadata, auth/session state, Telegram code request, code submission, and 2FA password handoff are implemented. |
| Origin discovery API | complete | Origin/topic metadata can be registered, listed, and refreshed from authenticated Telegram sessions with paged forum-topic discovery. |
| Backup selection API | basic | Per-origin enable/disable and capture policy can be read and updated. |
| Media capture policy | complete | Policy fields control text, media metadata, and media file downloading; downloaded files are stored and exposed via sync. |
| Participant metadata API | basic | Participant profiles can be stored, queried, and refreshed from authenticated Telegram sessions. |
| Runtime operation events | complete | Auth/discovery/media-download failures and partial operations are stored and exposed via `/manage/operation-events`. |
| Core web console | complete | Built-in `/console` page provides tabs for overview, account login, origin/policy management, participant refresh, cursor/media inspection, and raw snapshots; it reuses the same token-protected management APIs. |

## Review Backlog

| Area | Status | Notes |
| --- | --- | --- |
| Historical backfill and reconnect catch-up | complete | Ingestion runs bounded per-origin backfill using capture cursors and updates cursors on live messages. |
| Media backup semantics | complete | Media backup semantics are explicit: text, media metadata, and file download are separate policy flags. |
| Single-user multi-account boundary | recorded | Product direction is one owner/user managing multiple Telegram accounts, not multi-tenant hosting. |
| API startup health | complete | Background API startup waits for bind success and raises startup failures. |
| Search API status | complete | `/sync/search` is now documented in README. |
| Telegram runtime hardening | complete | Auth failures are classified, topic/participant scans are bounded, media downloads retry before recording failure, and `smoke-telegram` validates live sessions. |

## Errors Encountered

| Error | Attempt | Resolution |
| --- | --- | --- |
| Skill path read failed | Tried `.codex/skills/...` path | Re-read from `.agents/skills/...`. |
| `unittest discover` found 0 tests | Ran discovery without explicit test dir | Used `python3 -m unittest discover -s tests -v`. |
| Localhost bind denied in sandbox | HTTP API test tried to bind `127.0.0.1:0` | Re-ran tests with approved elevated localhost binding. |
| DevNuc `tele-mess-core init-db --config config.yml` failed | CLI only accepted global `--config` before subcommands | Added hidden per-subcommand `--config` support so both command orders work. |

## DevNuc Deployment Status

| Item | Status | Notes |
| --- | --- | --- |
| New directory | complete | `/home/dreaife/dev/tele-mess-core` |
| Venv/install | complete | `.venv` with editable install and runtime deps |
| Config | complete | `config.yml` generated from old Telegram API/group config; token not printed |
| Database | complete | `/home/dreaife/.local/share/tele-mess-core/archive.db` initialized |
| Unit tests | complete | 22 tests OK on devNuc |
| API smoke test | complete | `/console` returns 200 without a token header, `/sync/state` returns 401 without token and 200 with token. |
| Telegram ingestion | ready | Code supports remote Telegram login and existing-session ingestion; actual start still requires runtime credentials/login code from the operator. |

## Multi-Account Adaptation

| Item | Status | Notes |
| --- | --- | --- |
| Config model | complete | Added `telegram.accounts[]`; kept old single-account config compatible. |
| Archive schema | complete | Added `account_id` to chats, users, messages, and events. |
| Migration | complete | v1 databases migrate into v2 with `account_id='default'`. |
| Ingestion | complete | One Telethon adapter can run per configured account. |
| Sync payload | complete | Events/messages/chats include `account_id`. |
