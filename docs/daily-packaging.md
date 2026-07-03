# Daily Packaging And AI Analysis

Daily packaging builds a reviewable local record from already archived Telegram
origins, then optionally runs staged local AI analysis through a configurable
Codex CLI command.

## Scope

This feature uses existing SQLite archive data: origins, backup policies, tags,
messages, and media file records. It does not collect Telegram data by itself
and does not forward messages to backup groups.

Scheduling is limited to one daily package timer managed by the host system
through a user-level systemd timer. Summary generation is a run-now CLI/API job;
it is not independently scheduled.

## Origin And Tag Selection

Origins are eligible when their backup policy is enabled and they are not
archived. Package runs can filter by:

- account ID;
- origin ID and topic ID;
- date and timezone;
- comma-separated tag intersection;
- configured tag groups.

Tags are parsed from `backup_policies.tags`, trimmed, deduplicated, and compared
case-insensitively. Tag groups are all-of matches. Groups are assigned from the
most specific to least specific: the group with more tags runs first, and once
an origin is assigned it is removed from broader groups.

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
  to key information strings, suggested tags, and ignored noise.
- `normal_group_analysis`: extracted normal-origin facts are summarized by tag
  group with key threads, derived tags, risks, opportunities, actions, and
  source references.
- `important_origin_analysis`: each important origin gets a complete record and
  priority analysis. Image OCR/visual findings are inserted into the record;
  long media remain as file references.
- `final_daily_summary`: final Markdown rollup from important-origin analysis,
  normal group analysis, and media analysis outputs.

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

Summary records store final Markdown plus metadata and stage output references.
List responses return previews by default; direct record lookup returns full
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
tele-mess-core daily-schedule --config config.yml install --activate-systemd
tele-mess-core daily-schedule --config config.yml remove
```

The systemd timer runs the package CLI only. Summary runs remain explicit API or
CLI jobs.
