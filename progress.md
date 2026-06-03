# Progress

## 2026-06-01

- Started new `tele-mess-core` server-mode implementation in the empty workspace.
- Read existing devNuc bot structure over SSH without modifying the original repo.
- Created planning files and recorded the target architecture.
- Implemented SQLite archive with event cursor sync.
- Implemented read-only HTTP sync API.
- Implemented Telethon ingestion adapter for new/edit/delete/reaction events.
- Implemented CLI commands: `init-db`, `serve-api`, `run-telegram`, `run-server`, `import-ndjson`.
- Added server-mode deployment notes and a systemd template.
- Verified with `PYTHONPATH=src python3 -m unittest discover -s tests -v` after approving localhost bind for API tests.
- Verified syntax with `PYTHONPATH=src python3 -m compileall -q src tests`.
- Verified CLI help with `PYTHONPATH=src python3 -m tele_mess_core --help`.
- Began devNuc deployment to a separate `~/dev/tele-mess-core` directory.
- Created devNuc venv and installed package dependencies.
- Generated `config.yml` from old Telegram API/group config without copying forwarding targets, summaries, sessions, logs, or data.
- Ran devNuc unit tests successfully.
- Initialized `/home/dreaife/.local/share/tele-mess-core/archive.db`.
- Smoke-tested the sync API with the real config; `/sync/state` returned `last_event_seq=0`, `message_count=0`, `schema_version=1`.
- Confirmed no `tele-mess-core` process was left running after the temporary API test.
- Confirmed the old repo still has the same pre-existing dirty status; no old repo files were modified by this deployment.
- Reworked the core for multi-account Telegram management with account-aware config, schema, ingestion, and sync payloads.
- Added v1-to-v2 migration so the already-initialized empty devNuc DB can move to the account-aware schema without manual deletion.
