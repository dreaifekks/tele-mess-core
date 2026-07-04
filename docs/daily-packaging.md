# Daily Packaging And AI Analysis

Daily packaging builds a reviewable local record from already archived Telegram
origins, then optionally runs staged local AI analysis through a configurable
Codex CLI command.

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

`origins.important` marks an origin for dedicated analysis. Important origins
are excluded from normal tag groups and handled one-by-one before the final
rollup.

## Package Output

Each package run writes files under:

```text
<daily.output_dir>/<YYYY-MM-DD>/<package-run-id>/
  package.json
  package.md
  normal-groups/
  important-origins/
  analysis/
```

`package.json` is the machine-readable source of truth. It includes:

- package metadata: local date, timezone, UTC window, scope, counts;
- origin metadata: source, account, origin/topic IDs, title, tags, important;
- message lists: speaker, sent time, local time, text, permalink;
- media metadata: kind, MIME type, file size, downloaded file path.

`package.md` and the per-group/per-origin Markdown files are human review
entrances.

## AI Analysis Pipeline

`daily-summary` runs a staged AI pipeline and stores every prompt and output
inside the package run directory:

```text
analysis/<summary-run-id>/
  prompt.md
  summary.md
  summary.json
  analysis.json
  stages/
    media/
    normal-origins/
    normal-groups/
    important-origins/
```

The stages are:

- `media_image_analysis`: image files are passed to Codex with `--image`; the
  prompt asks for text-dominant vs visual-information classification, OCR text,
  visual facts, and reusable archive content.
- `media_file_reference`: non-image long media such as PDF and video are not
  parsed in the MVP. Their path, filename, MIME type, size, origin, and message
  reference are preserved.
- `normal_origin_key_extraction`: each normal origin in a tag group is reduced
  to a reusable origin record, key information strings, suggested tags, and
  ignored noise. When the origin has 200 or fewer messages in the daily window,
  the prompt asks Codex to preserve the full chronological message content
  instead of only extracting highlights.
- `normal_group_analysis`: extracted normal-origin facts are summarized by tag
  group with a full content digest, key threads, derived tags, risks,
  opportunities, actions, and source references.
- `important_origin_analysis`: each important origin gets a complete record and
  priority analysis. Important origins keep the full chronological message
  content for the daily window. The prompt first asks Codex to scan segment
  importance, then decide from the surrounding context whether each media item
  deserves OCR/visual extraction or should only be listed by path.
- `final_daily_summary`: final Markdown rollup from important-origin analysis,
  normal group analysis, and media analysis outputs.

For groups or origins tagged `info`, the prompts add an explicit information
collection instruction: collect facts, announcements, events, resources, links,
numbers, times, action items, controversies, and source references instead of
only producing a chat atmosphere summary.

The default AI provider is a local Codex command template:

```yaml
daily:
  ai:
    provider: "codex-cli"
    command:
      - "codex"
      - "-a"
      - "never"
      - "exec"
      - "--skip-git-repo-check"
      - "--output-last-message"
      - "{output}"
      - "{images}"
      - "-"
```

Supported command placeholders:

- `{output}`: stage output path for `--output-last-message`;
- `{images}`: repeated `--image <path>` arguments for image tasks;
- `{task}`: task name, useful for wrappers and logs.

Set `daily.ai.provider: disabled` for local dry runs. In that mode the pipeline
still creates stage files and summary records, but stage content is a disabled
provider marker.

## Persistence And API

SQLite stores run state and queryable summary content:

- `origins.important`;
- `daily_package_schedule`;
- `daily_package_runs`;
- `daily_summary_runs`;
- `daily_summary_records`.

Summary records store group-level Markdown plus metadata and stage output
references. One daily summary run can create multiple `daily_summary_records`
rows: one row per normal tag group, and one row per important origin. Tags are
stored in both `tags_json` and `tags_csv`, and returned by the API as `tags`
plus `tags_csv`. The final
daily rollup remains available through the summary run content path. List
responses return previews by default; direct record lookup returns full
Markdown.

Primary management endpoints:

- `GET` / `PATCH /manage/daily-package-schedule`;
- `POST /manage/daily-packages`;
- `GET /manage/daily-package-runs`;
- `GET /manage/daily-package-runs/content`;
- `POST /manage/daily-summaries`;
- `GET /manage/daily-summary-runs`;
- `GET /manage/daily-summary-runs/content`;
- `GET /manage/daily-summary-records`;
- `GET /manage/daily-summary-records/item`;
- `PATCH /manage/origins/important`.

`GET /manage/daily-summary-records` supports filtering by summary ID, run ID,
package run ID, date range, provider, important flag, all-of tags, substring
query, and result limit.

## CLI

```bash
tele-mess-core daily-package --config config.yml --date 2026-07-03 --timezone Asia/Tokyo
tele-mess-core daily-summary --config config.yml --package-run-id <package-run-id>
tele-mess-core daily-run --config config.yml --date 2026-07-03 --timezone Asia/Tokyo
tele-mess-core daily-schedule --config config.yml install --activate-systemd
tele-mess-core daily-schedule --config config.yml remove
```

The systemd timer runs `daily-run --scheduled`, so it writes one
`daily_package_runs` row, one `daily_summary_runs` row, and one or more
group-level `daily_summary_records` rows when the package completes.
