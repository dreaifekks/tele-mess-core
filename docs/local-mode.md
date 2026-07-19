# macOS Local CLI Mode

Local mode runs the archive core directly on a Mac without requiring the
built-in HTTP API or browser console.

```text
TelegramRuntimeManager + DailyJobWorker
HTTP API / web console: disabled by default
```

The existing server commands are unchanged. `run-server` still starts
Telegram ingestion, durable jobs, and HTTP; `run-telegram` remains an
ingestion-only debugging command.

## Workspace

`run-local` uses a stable workspace rather than the process current directory.
On macOS the default is:

```text
~/Library/Application Support/tele-mess-core
```

The default configuration path is `config.yml` inside that workspace. Create
the directory and copy a configuration into it before the first run:

```bash
mkdir -p "$HOME/Library/Application Support/tele-mess-core"
cp config.example.yml "$HOME/Library/Application Support/tele-mess-core/config.yml"
tele-mess-core paths
```

Configuration discovery uses this order:

1. `--config PATH`
2. `TELE_MESS_CORE_CONFIG`
3. `--workspace PATH`, `TELE_MESS_CORE_WORKSPACE`, or
   `TELE_MESS_CORE_HOME`, followed by `config.yml`
4. The platform local-workspace default

When a workspace is selected explicitly or through its environment variable, a
relative `--config` and managed file paths inside that config are resolved from
the workspace. Without a workspace override, an explicit config keeps the
existing behavior: relative managed paths are resolved from the config file's
directory. Command names such as `daily.cli_path` and `daily.ai.command[0]`
still use normal executable/PATH lookup unless an absolute path is configured.

The process never calls `chdir()`. Database, session, media, logs, daily output,
and external AI working paths are resolved to stable absolute paths instead.
This makes the same command safe to launch from different Terminal directories
and prepares it for a future LaunchAgent.

Use `paths` to inspect the effective paths without opening or creating the
SQLite database:

```bash
tele-mess-core paths
tele-mess-core paths --workspace "$HOME/Library/Application Support/tele-mess-core"
tele-mess-core paths --config /absolute/path/to/config.yml
```

The JSON output contains paths only; it does not print Telegram credentials,
server tokens, phone numbers, or fallback API keys.

## Running

Start the full local runtime without opening a TCP listener:

```bash
tele-mess-core run-local
```

Use an explicit workspace when keeping multiple local instances:

```bash
tele-mess-core run-local --workspace "$HOME/Library/Application Support/tele-mess-core-personal"
```

HTTP is an explicit opt-in. This starts the existing JSON API and built-in web
console; it does not open a browser automatically:

```bash
tele-mess-core run-local --web
```

`run-local --web` uses the configured `server.host`, `server.port`, token, and
localhost-auth policy exactly like `run-server`.

At every reconnect, Telethon first recovers its persisted update state and the
archive then reconciles each enabled capture target through a fixed remote
message head. `telegram.backfill.catch_up_limit` is the per-page size, not a
maximum number of messages per restart (`<= 0` falls back to a safe page size of
1,000), so a Mac that was offline for several days continues paging until that
startup range is complete. Failed pages retain the last completed history
cursor and retry in the background.

The first v0.3 run upgrades capture cursors to schema v18. Because the previous
single cursor could not prove that history below its live high-water mark was
complete, existing targets perform one conservative full-history reconciliation
in bounded pages. This may make the first upgraded startup longer, but it closes
legacy gaps; later startups resume only from the independent completed-history
cursor.

## Configuration Notes

For backward compatibility, omitted `storage.database` and account
`session_dir` values still default to `./data/archive.db` and
`./data/sessions` under the workspace. Set them explicitly when using a custom
`storage.data_dir`.

`daily.ai.work_dir` controls only the current directory of the Codex or fallback
subprocess. It defaults to `storage.data_dir` for backward compatibility and
does not change config or data path resolution.

```yaml
storage:
  data_dir: "./data"

telegram:
  accounts:
    - account_id: "main"
      api_id: 123456
      api_hash: "your_api_hash_here"
      session_name: "main"

daily:
  ai:
    work_dir: "./data/ai-work"
```

Relative `session_dir` values written through the management API are normalized
against the workspace before persistence, so restored accounts do not depend on
the directory from which the process happened to start.

## Current Boundary

No-Web local mode expects Telegram sessions and SQLite-backed capture policies
to have already been configured. `mess-end` owns first-run configuration and
uses the core management API for code/2FA login, origin discovery, and policy
editing; the core does not duplicate those flows as interactive CLI prompts.
Standalone operators can enable `--web` temporarily for the same management
surface. `mess-end` also owns macOS LaunchAgent installation and should restart
the pinned core command at login; the core then performs update recovery and
fixed-head history catch-up before continuing live ingestion.

Do not run `run-local` and `run-server` against the same workspace at the same
time; both would try to own the same Telegram session and SQLite archive.
