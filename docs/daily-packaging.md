# Daily Packaging And AI Analysis

Daily packaging builds a reviewable local record from already archived Telegram
origins, then optionally runs two staged local AI products through a configurable
Codex CLI command: a full-context report sourced only from important origins,
and structured message points extracted from every eligible origin followed by
a separate point-based daily digest.

## Scope

This feature uses existing SQLite archive data: origins, backup policies, tags,
messages, and media file records. It does not collect Telegram data by itself
and does not forward messages to backup groups.

Scheduling is limited to one host-managed user-level systemd timer. The timer
generates the daily package and immediately runs the staged summary for that
package.

## Origin And Tag Selection

Origins are eligible when their backup policy is enabled and they are not
archived. They only enter packaging and summary grouping when they have at
least one message in the selected local-day window. Package runs can filter by:

- account ID;
- origin ID and topic ID;
- date and timezone;
- comma-separated tag intersection;
- configured tag groups.

Tags are parsed from `backup_policies.tags`, trimmed, deduplicated, and compared
case-insensitively. Tag groups are all-of matches. Parent origins and forum
topics are treated as one local source for grouping: a topic uses the parent's
tags when its own tags are empty or equivalent to the parent. A topic only uses
its own local tags for grouping when those tags are non-empty and different from
the parent, or when the topic is marked important.

When a run does not provide explicit `tag_groups`, package generation creates
groups automatically from each enabled origin's effective CSV tag set. For
example, origins tagged `web3,info` are analyzed together, while `ai,info` is a
separate group. Explicit `tag_groups` remain supported for ad hoc/manual runs;
when provided, groups are assigned from the most specific to least specific:
the group with more tags runs first, and once an origin is assigned it is
removed from broader groups.

`origins.important` marks an origin for dedicated full-context analysis.
Important origins remain separate from normal tag-group packaging for that
report. Message-point extraction is deliberately independent of this split: it
runs for both important and non-important origins so the point store represents
the whole selected day.

## Package Output

Each package run writes files under:

```text
<daily.output_dir>/<YYYY-MM-DD>/<package-run-id>/
  package.json
  package.md
  normal-groups/
  important-origins/
  point-origins/
  analysis/
```

`package.json` is the machine-readable source of truth. It includes:

- package metadata: local date, timezone, UTC window, scope, counts;
- origin metadata: source, account, origin/topic IDs, title, tags, important;
- message lists: speaker, sent time, local time, text, web permalink, Telegram
  `tg://` deeplink;
- media metadata: kind, MIME type, file size, downloaded file path.

`package.md` and the per-group/per-origin Markdown files are human review
entrances.
Origin history is read in pages, so the previous implicit 10,000-message daily
ceiling does not truncate important full-context analysis or point extraction.

## AI Analysis Pipeline

`daily-summary` runs a staged AI pipeline and stores every prompt and output
inside the package run directory:

```text
analysis/<summary-run-id>/
  summary.md
  summary.json
  analysis.json
  important-summary.md
  important-summary.prompt.md
  point-summary.md
  point-summary.prompt.md
  stages/
    media/
    message-points/
    important-origins/
```

The stages are:

- `media_image_analysis`: image files are passed to Codex with `--image`; the
  prompt asks for text-dominant vs visual-information classification, OCR text,
  visual facts, and reusable archive content.
- `media_file_reference`: non-image long media such as PDF and video are not
  parsed in the MVP. Their path, filename, MIME type, size, origin, and message
  reference are preserved.
- `message_point_extraction`: every eligible origin, including important
  origins, is scanned for discrete reusable facts, events, decisions, resources,
  risks, opportunities, and actions. Each structured point records its time,
  tags, concise content, Telegram deeplink or web fallback, importance score and
  reason, source-origin importance, and source message references. Task output
  is validated as JSON before points are committed to SQLite.
- `important_origin_analysis`: each important origin gets priority analysis
  over the full daily window. Important origins still scan the full
  chronological message set, but the output is organized into readable topics,
  events, decisions, actions, and risks. The prompt first asks Codex to scan
  segment importance, then decide from the surrounding context whether each
  media item deserves OCR/visual extraction or should only be listed by path.
- `important_daily_summary`: a readable full-context daily report built only
  from important-origin analysis and its relevant media evidence. It does not
  absorb non-important origin content. `important-summary.md` is its explicit
  artifact.
- `daily_point_summary`: a separate daily digest built only from the validated
  structured message points persisted for the run before this stage. It groups related points without
  discarding their time, tags, importance, or source links and writes
  `point-summary.md`. `summary.md` remains a compatibility entrance containing
  both report sections, without mixing their evidence pipelines.

For groups or origins tagged `info`, point extraction adds an explicit
information-collection instruction: collect facts, announcements, events,
resources, and links instead of producing a chat-atmosphere summary.

The default AI provider is a local Codex command template:

```yaml
daily:
  ai:
    provider: "codex-cli"
    model: "gpt-5.6-sol"
    command:
      - "codex"
      - "-a"
      - "never"
      - "exec"
      - "{model}"
      - "--skip-git-repo-check"
      - "--output-last-message"
      - "{output}"
      - "{output_schema}"
      - "{images}"
      - "-"
```

Supported command placeholders:

- `{output}`: stage output path for `--output-last-message`;
- `{model}`: expands to `--model <id>` using the configured AI model, defaulting
  to `gpt-5.6-sol`;
- `{output_schema}`: expands to `--output-schema <path>` for structured stages
  and is omitted for Markdown stages;
- `{images}`: repeated `--image <path>` arguments for image tasks;
- `{task}`: task name, useful for wrappers and logs.

Recognized direct `codex exec` commands from older configs receive missing
model and output-schema flags automatically. Custom wrapper commands should
include the placeholders explicitly.

An optional fallback is activated only after the Codex CLI returns a confirmed
usage-limit error. Once activated, the rest of that summary run goes directly
to the fallback instead of repeatedly starting Codex:

```yaml
daily:
  ai:
    fallback:
      enabled: true
      provider: "openai-compatible"
      trigger: "usage-limit"
      base_url: "https://api.example.invalid/v1"
      model: "deepseek-v4-flash"
      api_key_file: "./.secrets/openai-compatible-api-key"
      retry_delay_seconds: 1200
      max_retries: 1
      supports_images: false
      supports_json_schema: false
```

`api_key_file` is resolved relative to the main config file and must remain
untracked with restrictive local permissions. The key and Authorization header
are never included in job requests, dedupe keys, provider errors, or API
responses. The fallback uses the OpenAI Responses shape. When server-enforced
JSON Schema is unavailable, the schema is appended to the prompt and message
points are still checked by the local validator before persistence.

When `supports_images` is false, `media_image_analysis` creates an explicit
`fallback_has_no_vision` artifact without making OCR or visual claims.
Important-origin analysis continues from message text and metadata without
attaching images. A transient fallback network, rate-limit, or 5xx failure can
return the durable job to `queued` until `retry_at`; the worker releases its
lease, the retry survives restart, and cancellation remains available.

Set `daily.ai.provider: disabled` for local dry runs. In that mode the pipeline
still creates stage files and summary records. Structured extraction emits an
empty valid point set, while Markdown AI stages use a disabled-provider marker.

The two daily outputs can also be delivered back to Telegram after they are
generated:

```yaml
daily:
  delivery:
    enabled: true
    account_id: "main"
    origin_id: -1001234567890
    topic_id: 0
```

`account_id` selects the configured Telegram session that sends the messages.
`origin_id` is the target group or channel ID. `topic_id` is optional; set it
to a forum topic ID when the summary should be posted into that topic. To pick
a target from a client, run live discovery with
`POST /manage/discover-origins`, then list saved choices with
`GET /manage/origins?account_id=<account_id>`. Returned rows include
`origin_type` (`group`, `channel`, or `topic`), `origin_id`, `topic_id`, title,
username, and forum metadata. The important report keeps its generated date,
timezone, source-tag, and provider header. The point digest is sent as a
separate logical delivery whose searchable Telegram tag is always `#point`.
Telegram sends use Telethon Markdown parsing; standard Markdown headings are
adapted to bold Telegram headings at send time while hashtags remain searchable.

YAML remains the fallback for existing deployments. Management clients should
use `GET` and `PATCH /manage/daily-summary-delivery`; the API stores the target
in SQLite and that database value takes precedence for scheduled and background
runs. `PATCH /manage/daily-package-schedule` also accepts the same object under
the nested `delivery` field for clients that save schedule and destination in
one request. Unknown schedule fields return an error instead of being silently
ignored.

## Persistence And API

SQLite stores run state and queryable summary content:

- `origins.important`;
- `daily_package_schedule`;
- `daily_summary_delivery`;
- `daily_package_runs`;
- `daily_summary_runs`;
- `daily_summary_records`;
- `daily_message_points`;
- `daily_summary_jobs`;
- `delivery_outbox`.

Package and summary run rows include progress counters (`progress_current`,
`progress_total`), the current label, and a structured `progress` object so
clients can monitor how many package units or AI analysis tasks are queued and
how far the current run has advanced.

For UI clients that need one button to run the full workflow, daily summary
jobs wrap package generation plus summary analysis in one durable job. A job
stores its canonical request, deduplication key, package/summary run IDs,
current stage, counters, worker lease, heartbeat, and attempt count.
Cancellation marks the job as cancel-requested, stops the active provider
process when possible, and leaves the package/summary run rows with their last
recorded state. Expired running leases are reclaimed after service restart.

When Telegram delivery is enabled, summary records and deterministic delivery
chunks are committed in one transaction. Important and point reports are built
as independent payloads and chunk sequences before they are queued. The worker
sends chunks in order and stores returned Telegram message IDs. A send failure
moves only the outbox row to retry state with backoff; it does not roll back or
fail the completed analysis. Canceling a job discards its unsent outbox rows;
an already in-flight Telegram request is best-effort and cannot be retracted.
Repeating an equivalent request returns its active
or completed job by default; use API `force: true` or CLI `--force` for an
intentional rerun.

Summary records store Markdown plus metadata and stage output references. Their
queryable `record_type` distinguishes compatibility records such as `tag_group`,
`important_origin`, and `final` from the new run-level `important_daily` and
`point_daily` records. Tags are stored in both `tags_json` and `tags_csv`, and
returned by the API as `tags` plus `tags_csv`. Summary records are soft-deleted
through `deleted_at`; normal list and item reads hide deleted records unless
requested. List responses return previews by default; direct record lookup
returns full Markdown.

`daily_message_points` stores each point independently from summary Markdown.
Rows retain the summary/package run IDs, local date and timezone, origin and
primary message identity, occurrence time, tags, concise content, Telegram and
web links, 1-5 importance score and reason, whether the source origin was marked
important, and all supporting source references. This makes points directly
searchable and reviewable without rerunning the provider.

Primary management endpoints:

- `GET` / `PATCH` `/manage/daily-package-schedule`;
- `GET` / `PATCH` `/manage/daily-summary-delivery`;
- `POST` `/manage/daily-packages`;
- `GET` `/manage/daily-package-runs`;
- `GET` `/manage/daily-package-runs/content`;
- `POST` `/manage/daily-summaries`;
- `POST` / `GET` `/manage/daily-summary-jobs`;
- `PATCH` `/manage/daily-summary-jobs/cancel`;
- `GET` `/manage/daily-summary-runs`;
- `GET` `/manage/daily-summary-runs/content`;
- `GET` `/manage/daily-summary-records`;
- `GET` `/manage/daily-summary-records/item`;
- `PATCH` / `DELETE` `/manage/daily-summary-records`;
- `GET` `/manage/daily-message-points`;
- `GET` `/manage/daily-message-points/item`;
- `PATCH /manage/origins/important`.

`GET /manage/daily-summary-records` supports filtering by summary ID, run ID,
package run ID, date range, provider, important flag, all-of tags, substring
query, record type, deleted state, and result limit.

`GET /manage/daily-message-points` supports filtering by point, summary run,
package run, local date range, account/origin/topic, tag, minimum importance,
important-origin state, content substring, and result limit. By default it only
returns points from completed summary runs whose owning background job also
completed, so a failed or canceled run followed by a successful retry does not
duplicate the normal view. Set
`include_incomplete=true` for diagnostics. Direct lookup by `point_id` returns
the complete point including source references and its run status.

## CLI

```bash
tele-mess-core daily-package --config config.yml --date 2026-07-03 --timezone Asia/Tokyo
tele-mess-core daily-summary --config config.yml --package-run-id <package-run-id>
tele-mess-core daily-run --config config.yml --date 2026-07-03 --timezone Asia/Tokyo
tele-mess-core daily-schedule --config config.yml install --activate-systemd
tele-mess-core daily-schedule --config config.yml remove
```

The systemd timer runs `daily-run --scheduled`, so it writes one
`daily_package_runs` row, one `daily_summary_runs` row, independently queryable
message-point rows, and the important/point daily summary records when the run
completes.
