# Changelog

## Unreleased

- Render daily Telegram summary deliveries with Markdown instead of sending the
  generated Markdown source as plain text; standard headings are adapted to
  Telegram-compatible bold headings while hashtags such as `#point` remain searchable.
- Added a Codex usage-limit circuit breaker that switches the remainder of a
  summary run to a configurable OpenAI-compatible Responses fallback without
  storing its API key in tracked configuration.
- Added durable delayed retry state for transient fallback failures, including
  one configurable 20-minute retry that survives worker restarts and remains
  cancelable.
- Sanitized provider errors before storing them, and downgrade image stages to
  explicit no-vision artifacts when the fallback model cannot accept images.
- Split daily AI output into an important-origin full-context report and a
  separate `#point` digest built from structured message points extracted from
  all eligible origins.
- Added queryable message-point fields for time, tags, content, Telegram source,
  importance, and evidence, plus a filtered Message Points view in the built-in
  console.
- Added typed daily summary records so important daily reports, point digests,
  and legacy per-origin/group artifacts can be distinguished.
- Hid failed/canceled-run points from normal queries while keeping an explicit
  diagnostic view, and discard unsent Telegram chunks when a job is canceled.
- Paginated origin-window reads so high-volume days are not silently truncated
  at 10,000 messages.
- Changed the default Codex CLI model template to `gpt-5.6-sol` with `{model}`
  and task-specific `{output_schema}` placeholders.
- Added SQLite-backed `GET`/`PATCH /manage/daily-summary-delivery` configuration for clients.
- Allowed schedule updates to persist a nested `delivery` target and reject unknown fields instead of returning a false success.
- Changed scheduled and background summary jobs to resolve the persisted delivery target before creating the Telegram outbox.

## 0.2.5 - 2026-07-09

- Added configurable Telegram delivery for final daily summaries, targeting a chosen account, group/channel, and optional forum topic.
- Added Telegram-searchable hashtag/date/provider headers before delivered summary Markdown.
- Documented discovery-based group/channel/topic selection for summary delivery.

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
