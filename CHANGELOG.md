# Changelog

## 0.2.1 - 2026-07-03

- Added raw Telegram JSON retention cleanup for message rows.
- Added a weekly systemd user timer installer for raw JSON cleanup.
- Added WAL checkpoint support after cleanup and documented optional VACUUM usage.

## 0.2.0 - 2026-07-03

- Added daily origin packaging by group, tag, tag intersections, timezone, and daily window.
- Added important-origin handling with staged AI analysis tasks, including media OCR/image analysis placeholders and Codex CLI provider support.
- Added persistent daily summary/package storage plus list, detail, filter, create, and rerun APIs.
- Added systemd user timer installation and schedule update commands for daily package jobs.
- Documented the durable daily packaging and AI analysis behavior in `docs/daily-packaging.md`.

## 0.1.1 - 2026-07-03

- Added a generated API contract registry for sync, management, docs, and console endpoints.
- Added runtime API documentation endpoints: `/manage/api-manifest`, `/openapi.json`, and `/docs/api.md`.
- Added `tele-mess-core generate-api-docs` and `--check` for static API documentation.
- Added generated `docs/api.md`, `docs/openapi.json`, and `docs/api-agent.md`.
- Added `AGENT.md` and `AGENTS.md` for agent-facing repository guidance.
- Updated the built-in console to read and display the API contract hash.
