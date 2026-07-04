# Changelog

## 0.2.4 - 2026-07-04

- Fixed daily package media enrichment for large message batches by splitting media lookups before SQLite expression-depth limits are reached.
- Updated daily summary prompts to produce topic/event based reports instead of flat message lists.
- Added Telegram `tg://` deeplinks to daily package message records and instructed summaries to prefer them for topic start links.

## 0.2.3 - 2026-07-04

- Added daily package/summary progress counters and soft-deletable summary records with batch management APIs.
- Added background daily summary jobs that run package plus summary end to end and support cancellation of active provider processes.

## 0.2.2 - 2026-07-04

- Changed the daily systemd job to run package generation and Codex-backed summary generation together.
- Added tag-set based daily summary grouping with stored `tags_csv` metadata and multiple summary records per run.
- Updated parent/topic tag handling so topics inherit parent grouping unless they have distinct tags or are important.
- Skipped origins with no messages in the daily window before grouping and summary generation.
- Expanded daily summary prompts to preserve full normal-origin content up to 200 messages, handle important origins at full length, and add an `info` tag instruction focused on information collection.

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
