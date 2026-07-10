# tele-mess-core Agent Guide

## Project Shape

`tele-mess-core` is a single-user, multi-Telegram-account archive core. Treat it
as a long-running local database plus a token-protected management control
plane for Mac/web clients.

The core owns Telegram ingestion, SQLite storage, sync cursors, account auth
state, origin discovery, backup policy, participants, capture cursors, media
file records, structured operation events, daily package runs, system-managed
daily package schedules, local Codex-backed important-summary and all-origin
message-point runs, queryable daily message points, and stored daily summary
content records.
Client-specific UI state and labels belong outside the core.

## API Contract Workflow

`src/tele_mess_core/server/contracts.py` is the source of truth for HTTP
endpoint shape.

When changing any API handler, do this in the same change:

1. Update `contracts.py`.
2. Update the handler in `src/tele_mess_core/server/api.py`.
3. Update the built-in console if it calls or displays the changed data.
4. Run `tele-mess-core generate-api-docs`.
5. Run `tele-mess-core generate-api-docs --check`.

Generated API artifacts:

- `docs/api.md` is the human-readable API reference.
- `docs/openapi.json` is the OpenAPI snapshot for tools.
- `docs/api-agent.md` is the quick API lookup for agents.
- `GET /manage/api-manifest` returns the runtime contract version/hash and
  endpoint registry.
- `GET /openapi.json` returns the runtime OpenAPI document.
- `GET /docs/api.md` returns the runtime Markdown API reference.

The built-in console reads `/manage/api-manifest` and stores the last seen
contract hash in `localStorage` under `teleMessApiContractHash`.

## Auth And Secrets

Most API endpoints require either:

```text
Authorization: Bearer <server.token>
```

or:

```text
X-Api-Token: <server.token>
```

Public local endpoints are limited to `/`, `/console`, `/openapi.json`, and
`/docs/api.md`.

Do not read, print, commit, or include local secrets unless the user explicitly
asks and the data is required for the task:

- `config.yml`
- Telegram `.session` files
- SQLite archives
- downloaded media files
- API tokens
- phone numbers
- Telegram login codes
- 2FA passwords

## Validation

Use the project virtualenv when available:

```bash
./.venv/bin/tele-mess-core generate-api-docs --check
./.venv/bin/python -m unittest discover -s tests -v
```

Fallback without the installed console script:

```bash
python3 -m unittest discover -s tests -v
```
