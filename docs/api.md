# tele-mess-core API

This file is generated from `tele_mess_core.server.contracts`.

- Contract version: `2026-07-19.1`
- Contract hash: `4ef7a958cada8ce2`
- Runtime manifest: `/manage/api-manifest`
- OpenAPI: `/openapi.json`

## Authentication

Token-protected endpoints accept either `Authorization: Bearer <token>` or `X-Api-Token: <token>`.
The built-in console and generated documentation endpoints are public on the local server.

## Endpoint Index

- `GET /` (console, public) - Serve the built-in management console.
- `GET /console` (console, public) - Serve the built-in management console.
- `GET /openapi.json` (docs, public) - Return the current OpenAPI document.
- `GET /docs/api.md` (docs, public) - Return the generated Markdown API reference.
- `GET /healthz` (sync, token) - Return health and archive state.
- `GET /sync/state` (sync, token) - Return archive sync state.
- `GET /sync/events` (sync, token) - Return raw event rows after a cursor.
- `GET /sync/messages` (sync, token) - Return message rows after a cursor or the latest messages.
- `GET /sync/accounts` (sync, token) - Return archive account metadata.
- `GET /sync/chats` (sync, token) - Return archived chat metadata.
- `GET /sync/search` (sync, token) - Search archived message text.
- `GET /sync/media-files` (sync, token) - Return downloaded media file records.
- `GET /sync/media-files/content` (sync, token) - Return the binary contents of a registered media file.
- `GET /manage/capabilities` (management, token) - Return supported management capabilities.
- `GET /manage/api-manifest` (management, token) - Return the machine-readable API contract manifest.
- `GET /manage/accounts` (management, token) - List management account state.
- `POST /manage/accounts` (management, token) - Create or update account metadata.
- `DELETE /manage/accounts` (management, token) - Delete management account metadata.
- `POST /manage/accounts/auth` (management, token) - Create or update account auth state.
- `PATCH /manage/accounts/auth` (management, token) - Patch account auth state.
- `POST /manage/accounts/auth/status` (management, token) - Check live Telegram auth status.
- `POST /manage/accounts/auth/request-code` (management, token) - Request a Telegram login code.
- `POST /manage/accounts/auth/submit-code` (management, token) - Submit a Telegram login code and optional 2FA password.
- `GET /manage/origins` (management, token) - List known origins and topics.
- `POST /manage/origins` (management, token) - Create or update origin metadata.
- `DELETE /manage/origins` (management, token) - Delete an origin and related management metadata.
- `PATCH /manage/origins/archive` (management, token) - Archive or restore an origin.
- `PATCH /manage/origins/important` (management, token) - Mark or unmark an origin as important.
- `GET /manage/backup-policies` (management, token) - List origin backup policies.
- `POST /manage/backup-policies` (management, token) - Create or update an origin backup policy.
- `PATCH /manage/backup-policies` (management, token) - Patch an origin backup policy.
- `DELETE /manage/backup-policies` (management, token) - Delete an origin backup policy.
- `GET /manage/participants` (management, token) - List participant profiles.
- `POST /manage/participants` (management, token) - Create or update a participant profile.
- `DELETE /manage/participants` (management, token) - Delete a participant profile.
- `GET /manage/capture-cursors` (management, token) - List capture cursors.
- `GET /manage/operation-events` (management, token) - List structured operation events.
- `DELETE /manage/operation-events` (management, token) - Delete one or more operation events.
- `GET /manage/daily-package-schedule` (management, token) - Return the daily package system schedule.
- `PATCH /manage/daily-package-schedule` (management, token) - Update the daily package system schedule.
- `GET /manage/daily-summary-delivery` (management, token) - Return the effective daily summary Telegram delivery target.
- `PATCH /manage/daily-summary-delivery` (management, token) - Persist the daily summary Telegram delivery target.
- `POST /manage/daily-packages` (management, token) - Generate a daily package immediately.
- `GET /manage/daily-package-runs` (management, token) - List daily package runs.
- `GET /manage/daily-package-runs/content` (management, token) - Return daily package run content.
- `POST /manage/daily-summaries` (management, token) - Run or enqueue a daily summary.
- `POST /manage/daily-summary-jobs` (management, token) - Start a background daily package and summary job.
- `GET /manage/daily-summary-jobs` (management, token) - List background daily package and summary jobs.
- `PATCH /manage/daily-summary-jobs/cancel` (management, token) - Request cancellation of a running daily summary job.
- `GET /manage/daily-summary-runs` (management, token) - List daily summary runs.
- `GET /manage/daily-summary-runs/content` (management, token) - Return daily summary run content.
- `GET /manage/daily-summary-records` (management, token) - List stored daily summary contents.
- `GET /manage/daily-summary-records/item` (management, token) - Return one stored daily summary content record.
- `PATCH /manage/daily-summary-records` (management, token) - Soft-delete or restore one or more stored daily summary records.
- `DELETE /manage/daily-summary-records` (management, token) - Soft-delete one or more stored daily summary records.
- `GET /manage/daily-message-points` (management, token) - List stored daily message points.
- `GET /manage/daily-message-points/item` (management, token) - Return one stored daily message point.
- `POST /manage/discover-origins` (management, token) - Discover Telegram dialogs and topics for an authenticated account.
- `POST /manage/participants/refresh` (management, token) - Refresh participants for a Telegram origin.

## Endpoints

### GET /

Serve the built-in management console.

- Tag: `console`
- Auth: `public`
- Success: `200`

Request body: none

Response: `text/html`

### GET /console

Serve the built-in management console.

- Tag: `console`
- Auth: `public`
- Success: `200`

Request body: none

Response: `text/html`

### GET /openapi.json

Return the current OpenAPI document.

- Tag: `docs`
- Auth: `public`
- Success: `200`

Request body: none

Response: `application/json`

### GET /docs/api.md

Return the generated Markdown API reference.

- Tag: `docs`
- Auth: `public`
- Success: `200`

Request body: none

Response: `text/markdown`

### GET /healthz

Return health and archive state.

- Tag: `sync`
- Auth: `required`
- Success: `200`

Request body: none

Response: `HealthResponse`

### GET /sync/state

Return archive sync state.

- Tag: `sync`
- Auth: `required`
- Success: `200`

Request body: none

Response: `StateResponse`

### GET /sync/events

Return raw event rows after a cursor.

- Tag: `sync`
- Auth: `required`
- Success: `200`

Query parameters:

- `after` (`integer`, optional, default `0`) - Last consumed event sequence.
- `limit` (`integer`, optional, default `500`) - Maximum rows to return.

Request body: none

Response: `EventPage`

### GET /sync/messages

Return message rows after a cursor or the latest messages.

- Tag: `sync`
- Auth: `required`
- Success: `200`

Query parameters:

- `after` (`integer`, optional, default `0`) - Last consumed event sequence.
- `limit` (`integer`, optional, default `500`) - Maximum rows to return.
- `latest` (`boolean`, optional, default `False`) - Return latest messages instead of cursor page.
- `include_media` (`boolean`, optional, default `False`) - Attach media_files to each message.

Request body: none

Response: `MessagePage`

### GET /sync/accounts

Return archive account metadata.

- Tag: `sync`
- Auth: `required`
- Success: `200`

Request body: none

Response: `AccountListResponse`

### GET /sync/chats

Return archived chat metadata.

- Tag: `sync`
- Auth: `required`
- Success: `200`

Request body: none

Response: `ChatListResponse`

### GET /sync/search

Search archived message text.

- Tag: `sync`
- Auth: `required`
- Success: `200`

Query parameters:

- `q` (`string`, optional) - Search text.
- `limit` (`integer`, optional, default `50`) - Maximum messages to return.
- `include_media` (`boolean`, optional, default `False`) - Attach media_files to each message.

Request body: none

Response: `MessageListResponse`

### GET /sync/media-files

Return downloaded media file records.

- Tag: `sync`
- Auth: `required`
- Success: `200`

Query parameters:

- `account_id` (`string`, optional) - Local Telegram account ID.
- `chat_id` (`integer`, optional) - Filter to a Telegram chat or origin ID.
- `message_id` (`integer`, optional) - Filter to a message ID.
- `limit` (`integer`, optional, default `500`) - Maximum rows to return.

Request body: none

Response: `MediaFileListResponse`

### GET /sync/media-files/content

Return the binary contents of a registered media file.

- Tag: `sync`
- Auth: `required`
- Success: `200`

Query parameters:

- `source` (`string`, optional, default `telegram`) - Source system.
- `account_id` (`string`, required) - Local Telegram account ID.
- `chat_id` (`integer`, required) - Telegram chat or origin ID.
- `message_id` (`integer`, required) - Telegram message ID.
- `file_index` (`integer`, optional, default `0`) - Media index on the message.

Request body: none

Response: `application/octet-stream`

### GET /manage/capabilities

Return supported management capabilities.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: none

Response: `CapabilitiesResponse`

### GET /manage/api-manifest

Return the machine-readable API contract manifest.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: none

Response: `ApiManifest`

### GET /manage/accounts

List management account state.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: none

Response: `AccountListResponse`

### POST /manage/accounts

Create or update account metadata.

- Tag: `management`
- Auth: `required`
- Success: `201`

Request body: `AccountInput`

Response: `AccountItemResponse`

### DELETE /manage/accounts

Delete management account metadata.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `AuthStatusInput`

Response: `AccountDeleteResponse`

### POST /manage/accounts/auth

Create or update account auth state.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `AccountAuthInput`

Response: `AccountItemResponse`

### PATCH /manage/accounts/auth

Patch account auth state.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `AccountAuthInput`

Response: `AccountItemResponse`

### POST /manage/accounts/auth/status

Check live Telegram auth status.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `AuthStatusInput`

Response: `AuthResultResponse`

### POST /manage/accounts/auth/request-code

Request a Telegram login code.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `RequestCodeInput`

Response: `AuthResultResponse`

### POST /manage/accounts/auth/submit-code

Submit a Telegram login code and optional 2FA password.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `SubmitCodeInput`

Response: `AuthResultResponse`

### GET /manage/origins

List known origins and topics.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `account_id` (`string`, optional) - Local Telegram account ID.
- `include_archived` (`boolean`, optional, default `False`) - Include removed origins.

Request body: none

Response: `OriginListResponse`

### POST /manage/origins

Create or update origin metadata.

- Tag: `management`
- Auth: `required`
- Success: `201`

Request body: `OriginInput`

Response: `OriginItemResponse`

### DELETE /manage/origins

Delete an origin and related management metadata.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `OriginArchiveInput`

Response: `OriginDeleteResponse`

### PATCH /manage/origins/archive

Archive or restore an origin.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `OriginArchiveInput`

Response: `OriginArchiveResponse`

### PATCH /manage/origins/important

Mark or unmark an origin as important.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `OriginImportantInput`

Response: `OriginItemResponse`

### GET /manage/backup-policies

List origin backup policies.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `account_id` (`string`, optional) - Local Telegram account ID.

Request body: none

Response: `BackupPolicyListResponse`

### POST /manage/backup-policies

Create or update an origin backup policy.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `BackupPolicyInput`

Response: `BackupPolicyItemResponse`

### PATCH /manage/backup-policies

Patch an origin backup policy.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `BackupPolicyInput`

Response: `BackupPolicyItemResponse`

### DELETE /manage/backup-policies

Delete an origin backup policy.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `BackupPolicyInput`

Response: `BackupPolicyDeleteResponse`

### GET /manage/participants

List participant profiles.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `account_id` (`string`, optional) - Local Telegram account ID.
- `origin_id` (`integer`, optional) - Filter to an origin.

Request body: none

Response: `ParticipantListResponse`

### POST /manage/participants

Create or update a participant profile.

- Tag: `management`
- Auth: `required`
- Success: `201`

Request body: `ParticipantInput`

Response: `ParticipantItemResponse`

### DELETE /manage/participants

Delete a participant profile.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `ParticipantInput`

Response: `ParticipantDeleteResponse`

### GET /manage/capture-cursors

List capture cursors.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `account_id` (`string`, optional) - Local Telegram account ID.

Request body: none

Response: `CaptureCursorListResponse`

### GET /manage/operation-events

List structured operation events.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `account_id` (`string`, optional) - Local Telegram account ID.
- `status` (`string`, optional) - Filter by status, for example failed.
- `limit` (`integer`, optional, default `100`) - Maximum events to return.

Request body: none

Response: `OperationEventListResponse`

### DELETE /manage/operation-events

Delete one or more operation events.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `OperationEventDeleteInput`

Response: `OperationEventDeleteResponse`

### GET /manage/daily-package-schedule

Return the daily package system schedule.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: none

Response: `DailyPackageScheduleResponse`

### PATCH /manage/daily-package-schedule

Update the daily package system schedule.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `DailyPackageScheduleInput`

Response: `DailyPackageScheduleResponse`

### GET /manage/daily-summary-delivery

Return the effective daily summary Telegram delivery target.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: none

Response: `DailySummaryDeliveryResponse`

### PATCH /manage/daily-summary-delivery

Persist the daily summary Telegram delivery target.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `DailySummaryDeliveryInput`

Response: `DailySummaryDeliveryResponse`

### POST /manage/daily-packages

Generate a daily package immediately.

- Tag: `management`
- Auth: `required`
- Success: `201`

Request body: `DailyPackageRunInput`

Response: `DailyPackageRunResponse`

### GET /manage/daily-package-runs

List daily package runs.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `status` (`string`, optional) - Filter by run status.
- `limit` (`integer`, optional, default `500`) - Maximum rows to return.

Request body: none

Response: `DailyPackageRunListResponse`

### GET /manage/daily-package-runs/content

Return daily package run content.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `run_id` (`string`, required) - Daily package run ID.
- `format` (`string`, optional, default `md`) - Content format: md or json.

Request body: none

Response: `text/markdown`

### POST /manage/daily-summaries

Run or enqueue a daily summary.

- Tag: `management`
- Auth: `required`
- Success: `201`

Request body: `DailySummaryRunInput`

Response: `DailySummarySubmitResponse`

### POST /manage/daily-summary-jobs

Start a background daily package and summary job.

- Tag: `management`
- Auth: `required`
- Success: `201`

Request body: `DailySummaryRunInput`

Response: `DailySummaryJobResponse`

### GET /manage/daily-summary-jobs

List background daily package and summary jobs.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `job_id` (`string`, optional) - Filter by job ID.
- `status` (`string`, optional) - Filter by job status.
- `limit` (`integer`, optional, default `500`) - Maximum rows to return.

Request body: none

Response: `DailySummaryJobListResponse`

### PATCH /manage/daily-summary-jobs/cancel

Request cancellation of a running daily summary job.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `DailySummaryJobCancelInput`

Response: `DailySummaryJobResponse`

### GET /manage/daily-summary-runs

List daily summary runs.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `package_run_id` (`string`, optional) - Filter by package run ID.
- `status` (`string`, optional) - Filter by run status.
- `limit` (`integer`, optional, default `500`) - Maximum rows to return.

Request body: none

Response: `DailySummaryRunListResponse`

### GET /manage/daily-summary-runs/content

Return daily summary run content.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `run_id` (`string`, required) - Daily summary run ID.

Request body: none

Response: `text/markdown`

### GET /manage/daily-summary-records

List stored daily summary contents.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `summary_id` (`string`, optional) - Filter by summary content ID.
- `run_id` (`string`, optional) - Filter by summary run ID.
- `package_run_id` (`string`, optional) - Filter by package run ID.
- `date` (`string`, optional) - Filter by local summary date.
- `date_from` (`string`, optional) - Filter summaries on or after this local date.
- `date_to` (`string`, optional) - Filter summaries on or before this local date.
- `provider` (`string`, optional) - Filter by AI provider.
- `record_type` (`string`, optional) - Filter by stored artifact type, such as important_daily or point_daily.
- `important` (`boolean`, optional) - Filter summaries that include important origins.
- `tag` (`string`, optional) - Required tag. Repeatable; all tags must match.
- `tags` (`string`, optional) - Comma-separated required tags; all tags must match.
- `q` (`string`, optional) - Filter by title or Markdown content substring.
- `include_deleted` (`boolean`, optional, default `False`) - Include soft-deleted summaries.
- `deleted` (`boolean`, optional) - Filter by soft-deleted state.
- `include_content` (`boolean`, optional, default `False`) - Include full Markdown content in list items.
- `limit` (`integer`, optional, default `500`) - Maximum rows to return.

Request body: none

Response: `DailySummaryRecordListResponse`

### GET /manage/daily-summary-records/item

Return one stored daily summary content record.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `summary_id` (`string`, optional) - Summary content ID.
- `run_id` (`string`, optional) - Summary run ID.
- `record_type` (`string`, optional) - Stored artifact type when a run has multiple records.
- `include_deleted` (`boolean`, optional, default `False`) - Allow returning a soft-deleted record.

Request body: none

Response: `DailySummaryRecordResponse`

### PATCH /manage/daily-summary-records

Soft-delete or restore one or more stored daily summary records.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `DailySummaryRecordDeleteInput`

Response: `DailySummaryRecordDeleteResponse`

### DELETE /manage/daily-summary-records

Soft-delete one or more stored daily summary records.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `DailySummaryRecordDeleteInput`

Response: `DailySummaryRecordDeleteResponse`

### GET /manage/daily-message-points

List stored daily message points.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `point_id` (`string`, optional) - Filter by message point ID.
- `run_id` (`string`, optional) - Filter by summary run ID.
- `package_run_id` (`string`, optional) - Filter by package run ID.
- `date` (`string`, optional) - Filter by local package date.
- `date_from` (`string`, optional) - Filter points on or after this local date.
- `date_to` (`string`, optional) - Filter points on or before this local date.
- `source` (`string`, optional) - Filter by source system.
- `account_id` (`string`, optional) - Local Telegram account ID.
- `origin_id` (`integer`, optional) - Filter by Telegram origin ID.
- `topic_id` (`integer`, optional) - Filter by Telegram topic ID.
- `message_id` (`integer`, optional) - Filter by primary source message ID.
- `tag` (`string`, optional) - Required tag. Repeatable; all tags must match.
- `tags` (`string`, optional) - Comma-separated required tags; all tags must match.
- `importance_min` (`integer`, optional) - Minimum importance score from 1 to 5.
- `importance_max` (`integer`, optional) - Maximum importance score from 1 to 5.
- `origin_important` (`boolean`, optional) - Filter by the origin's important flag.
- `q` (`string`, optional) - Filter by point content, origin title, or importance reason substring.
- `include_incomplete` (`boolean`, optional, default `False`) - Include points from running, failed, or canceled summary runs.
- `limit` (`integer`, optional, default `100`) - Maximum message points to return.

Request body: none

Response: `DailyMessagePointListResponse`

### GET /manage/daily-message-points/item

Return one stored daily message point.

- Tag: `management`
- Auth: `required`
- Success: `200`

Query parameters:

- `point_id` (`string`, required) - Message point ID.

Request body: none

Response: `DailyMessagePointResponse`

### POST /manage/discover-origins

Discover Telegram dialogs and topics for an authenticated account.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `DiscoveryInput`

Response: `DiscoveryResultResponse`

### POST /manage/participants/refresh

Refresh participants for a Telegram origin.

- Tag: `management`
- Auth: `required`
- Success: `200`

Request body: `ParticipantRefreshInput`

Response: `ParticipantRefreshResultResponse`

## Schemas

### Account

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | yes |
| `account_id` | `string` | yes |
| `display_name` | `string` | no |
| `kind` | `string` | no |
| `auth_state` | `string` | no |
| `phone` | `string` | no |
| `session_name` | `string` | no |
| `session_dir` | `string` | no |
| `last_error` | `string` | no |
| `updated_at` | `string` | no |
| `auth_updated_at` | `string` | no |
| `raw_json` | `object` | no |

### AccountAuthInput

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | no |
| `account_id` | `string` | yes |
| `auth_state` | `string` | no |
| `status` | `string` | no |
| `phone` | `string` | no |
| `session_name` | `string` | no |
| `session_dir` | `string` | no |
| `last_error` | `string` | no |

### AccountDeleteResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `object` | yes |

### AccountInput

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | no |
| `account_id` | `string` | yes |
| `display_name` | `string` | no |
| `kind` | `string` | no |
| `auth_state` | `string` | no |
| `phone` | `string` | no |
| `session_name` | `string` | no |
| `session_dir` | `string` | no |

### AccountItemResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `Account` | yes |

### AccountListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<Account>` | yes |

### ApiManifest

| Field | Type | Required |
| --- | --- | --- |
| `name` | `string` | no |
| `contract_version` | `string` | yes |
| `contract_hash` | `string` | yes |
| `openapi_url` | `string` | no |
| `markdown_url` | `string` | no |
| `agent_doc` | `string` | no |
| `endpoints` | `array` | yes |
| `schemas` | `object` | no |

### AuthResult

| Field | Type | Required |
| --- | --- | --- |
| `account_id` | `string` | no |
| `authorized` | `boolean` | no |
| `auth_state` | `string` | no |
| `phone` | `string` | no |
| `message` | `string` | no |
| `requires_password` | `boolean` | no |
| `phone_code_hash` | `string` | no |

### AuthResultResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `AuthResult` | yes |

### AuthStatusInput

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | no |
| `account_id` | `string` | yes |

### BackupPolicy

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | yes |
| `account_id` | `string` | yes |
| `origin_id` | `integer` | yes |
| `topic_id` | `integer` | yes |
| `enabled` | `boolean` | no |
| `capture_text` | `boolean` | no |
| `capture_media_metadata` | `boolean` | no |
| `download_media` | `boolean` | no |
| `tags` | `string` | no |
| `updated_at` | `string` | no |

### BackupPolicyDeleteResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `object` | yes |

### BackupPolicyInput

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | no |
| `account_id` | `string` | yes |
| `origin_id` | `integer` | yes |
| `topic_id` | `integer` | no |
| `enabled` | `boolean` | no |
| `capture_text` | `boolean` | no |
| `capture_media_metadata` | `boolean` | no |
| `download_media` | `boolean` | no |
| `tags` | `string` | no |

### BackupPolicyItemResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `BackupPolicy` | yes |

### BackupPolicyListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<BackupPolicy>` | yes |

### CapabilitiesResponse

| Field | Type | Required |
| --- | --- | --- |
| `mode` | `string` | yes |
| `sync` | `array` | yes |
| `management` | `array` | yes |
| `auth_flow` | `object` | no |
| `api_contract` | `object` | no |

### CaptureCursor

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | yes |
| `account_id` | `string` | yes |
| `origin_id` | `integer` | yes |
| `topic_id` | `integer` | yes |
| `last_message_id` | `integer` | no |
| `history_scanned_through_id` | `integer` | no |
| `observed_max_message_id` | `integer` | no |
| `last_message_at` | `string` | no |
| `last_backfill_at` | `string` | no |
| `backfill_head_message_id` | `integer` | no |
| `backfill_status` | `string` | no |
| `backfill_error` | `string` | no |
| `backfill_count` | `integer` | no |
| `updated_at` | `string` | no |
| `origin_title` | `string` | no |
| `raw_json` | `object` | no |

### CaptureCursorListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<CaptureCursor>` | yes |

### Chat

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | yes |
| `account_id` | `string` | yes |
| `chat_id` | `integer` | yes |
| `title` | `string` | no |
| `username` | `string` | no |
| `kind` | `string` | no |
| `updated_at` | `string` | no |
| `raw_json` | `object` | no |

### ChatListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<Chat>` | yes |

### DailyMessagePoint

| Field | Type | Required |
| --- | --- | --- |
| `point_id` | `string` | yes |
| `run_id` | `string` | yes |
| `package_run_id` | `string` | yes |
| `date` | `string` | yes |
| `timezone` | `string` | yes |
| `source` | `string` | yes |
| `account_id` | `string` | yes |
| `origin_id` | `integer` | yes |
| `topic_id` | `integer` | yes |
| `origin_title` | `string` | no |
| `message_id` | `integer` | no |
| `occurred_at` | `string` | yes |
| `tags` | `array` | yes |
| `tags_csv` | `string` | no |
| `content` | `string` | yes |
| `telegram_deeplink` | `string` | no |
| `permalink` | `string` | no |
| `importance_score` | `integer` | yes |
| `importance_reason` | `string` | no |
| `origin_important` | `boolean` | yes |
| `source_refs` | `array` | yes |
| `provider` | `string` | no |
| `run_status` | `string` | no |
| `job_status` | `string` | no |
| `created_at` | `string` | no |
| `updated_at` | `string` | no |

### DailyMessagePointListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<DailyMessagePoint>` | yes |

### DailyMessagePointResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `DailyMessagePoint` | yes |

### DailyPackageRun

| Field | Type | Required |
| --- | --- | --- |
| `run_id` | `string` | yes |
| `status` | `string` | yes |
| `date` | `string` | yes |
| `timezone` | `string` | yes |
| `scope` | `object` | no |
| `output_dir` | `string` | no |
| `package_json_path` | `string` | no |
| `package_md_path` | `string` | no |
| `origin_count` | `integer` | no |
| `message_count` | `integer` | no |
| `media_count` | `integer` | no |
| `important_origin_count` | `integer` | no |
| `progress_total` | `integer` | no |
| `progress_current` | `integer` | no |
| `progress_label` | `string` | no |
| `progress` | `object` | no |
| `error` | `string` | no |
| `started_at` | `string` | no |
| `finished_at` | `string` | no |

### DailyPackageRunInput

| Field | Type | Required |
| --- | --- | --- |
| `date` | `string` | no |
| `timezone` | `string` | no |
| `scope` | `object` | no |
| `account_id` | `string` | no |
| `origin_id` | `integer` | no |
| `topic_id` | `integer` | no |
| `tags` | `string` | no |
| `tag_groups` | `array` | no |

### DailyPackageRunListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<DailyPackageRun>` | yes |

### DailyPackageRunResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `DailyPackageRun` | yes |

### DailyPackageSchedule

| Field | Type | Required |
| --- | --- | --- |
| `enabled` | `boolean` | yes |
| `time_of_day` | `string` | yes |
| `timezone` | `string` | yes |
| `scope` | `object` | yes |
| `delivery` | `DailySummaryDelivery` | no |
| `system_manager` | `string` | yes |
| `installed` | `boolean` | yes |
| `last_installed_at` | `string` | no |
| `last_error` | `string` | no |
| `updated_at` | `string` | no |

### DailyPackageScheduleInput

| Field | Type | Required |
| --- | --- | --- |
| `enabled` | `boolean` | no |
| `time_of_day` | `string` | no |
| `timezone` | `string` | no |
| `scope` | `object` | no |
| `delivery` | `DailySummaryDeliveryInput` | no |
| `system_manager` | `string` | no |
| `activate_systemd` | `boolean` | no |

### DailyPackageScheduleResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `DailyPackageSchedule` | yes |

### DailySummaryDelivery

| Field | Type | Required |
| --- | --- | --- |
| `enabled` | `boolean` | yes |
| `account_id` | `string` | yes |
| `origin_id` | `integer` | yes |
| `topic_id` | `integer` | yes |
| `source` | `string` | yes |
| `updated_at` | `string` | no |

### DailySummaryDeliveryInput

| Field | Type | Required |
| --- | --- | --- |
| `enabled` | `boolean` | no |
| `account_id` | `string` | no |
| `origin_id` | `integer` | no |
| `topic_id` | `integer` | no |

### DailySummaryDeliveryResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `DailySummaryDelivery` | yes |

### DailySummaryJob

| Field | Type | Required |
| --- | --- | --- |
| `job_id` | `string` | yes |
| `status` | `string` | yes |
| `date` | `string` | no |
| `timezone` | `string` | no |
| `scope` | `object` | no |
| `package_run_id` | `string` | no |
| `summary_run_id` | `string` | no |
| `provider` | `string` | no |
| `progress_total` | `integer` | no |
| `progress_current` | `integer` | no |
| `progress_label` | `string` | no |
| `progress` | `object` | no |
| `request` | `object` | no |
| `dedupe_key` | `string` | no |
| `worker_id` | `string` | no |
| `lease_until` | `string` | no |
| `heartbeat_at` | `string` | no |
| `attempt` | `integer` | no |
| `retry_at` | `string` | no |
| `retry_count` | `integer` | no |
| `cancel_requested_at` | `string` | no |
| `error` | `string` | no |
| `started_at` | `string` | no |
| `finished_at` | `string` | no |
| `updated_at` | `string` | no |

### DailySummaryJobCancelInput

| Field | Type | Required |
| --- | --- | --- |
| `job_id` | `string` | yes |

### DailySummaryJobListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<DailySummaryJob>` | yes |

### DailySummaryJobResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `DailySummaryJob` | yes |

### DailySummaryRecord

| Field | Type | Required |
| --- | --- | --- |
| `summary_id` | `string` | yes |
| `run_id` | `string` | yes |
| `package_run_id` | `string` | no |
| `date` | `string` | no |
| `timezone` | `string` | no |
| `scope` | `object` | no |
| `tags` | `array` | no |
| `tags_csv` | `string` | no |
| `important` | `boolean` | no |
| `record_type` | `string` | yes |
| `provider` | `string` | no |
| `title` | `string` | no |
| `content_preview` | `string` | yes |
| `content_md` | `string` | no |
| `content_json` | `object` | no |
| `summary_path` | `string` | no |
| `origin_count` | `integer` | no |
| `group_count` | `integer` | no |
| `image_count` | `integer` | no |
| `content_length` | `integer` | no |
| `deleted` | `boolean` | no |
| `deleted_at` | `string` | no |
| `created_at` | `string` | no |
| `updated_at` | `string` | no |

### DailySummaryRecordDeleteInput

| Field | Type | Required |
| --- | --- | --- |
| `summary_id` | `string` | no |
| `summary_ids` | `array` | no |
| `ids` | `array` | no |
| `deleted` | `boolean` | no |

### DailySummaryRecordDeleteResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `DailySummaryRecordDeleteResult` | yes |

### DailySummaryRecordDeleteResult

| Field | Type | Required |
| --- | --- | --- |
| `summary_ids` | `array` | yes |
| `deleted` | `boolean` | yes |
| `changed_rows` | `integer` | yes |

### DailySummaryRecordListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<DailySummaryRecord>` | yes |

### DailySummaryRecordResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `DailySummaryRecord` | yes |

### DailySummaryRun

| Field | Type | Required |
| --- | --- | --- |
| `run_id` | `string` | yes |
| `status` | `string` | yes |
| `package_run_id` | `string` | no |
| `date` | `string` | no |
| `timezone` | `string` | no |
| `scope` | `object` | no |
| `output_dir` | `string` | no |
| `summary_path` | `string` | no |
| `provider` | `string` | no |
| `origin_count` | `integer` | no |
| `group_count` | `integer` | no |
| `image_count` | `integer` | no |
| `progress_total` | `integer` | no |
| `progress_current` | `integer` | no |
| `progress_label` | `string` | no |
| `progress` | `object` | no |
| `error` | `string` | no |
| `started_at` | `string` | no |
| `finished_at` | `string` | no |

### DailySummaryRunInput

| Field | Type | Required |
| --- | --- | --- |
| `package_run_id` | `string` | no |
| `date` | `string` | no |
| `timezone` | `string` | no |
| `scope` | `object` | no |
| `account_id` | `string` | no |
| `origin_id` | `integer` | no |
| `topic_id` | `integer` | no |
| `tags` | `string` | no |
| `tag_groups` | `array` | no |
| `background` | `boolean` | no |
| `force` | `boolean` | no |

### DailySummaryRunListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<DailySummaryRun>` | yes |

### DailySummaryRunResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `DailySummaryRun` | yes |

### DailySummarySubmitResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `object` | yes |

### DiscoveryInput

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | no |
| `account_id` | `string` | yes |
| `include_topics` | `boolean` | no |
| `include_private` | `boolean` | no |
| `topic_limit` | `integer` | no |

### DiscoveryResult

| Field | Type | Required |
| --- | --- | --- |
| `account_id` | `string` | no |
| `authorized` | `boolean` | no |
| `status` | `string` | no |
| `origins` | `integer` | no |
| `topics` | `integer` | no |
| `private_skipped` | `integer` | no |
| `errors` | `array` | no |
| `topics_truncated` | `boolean` | no |
| `topic_limit` | `integer` | no |
| `include_private` | `boolean` | no |

### DiscoveryResultResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `DiscoveryResult` | yes |

### ErrorResponse

| Field | Type | Required |
| --- | --- | --- |
| `error` | `string` | yes |
| `detail` | `string` | no |

### Event

| Field | Type | Required |
| --- | --- | --- |
| `seq` | `integer` | yes |
| `source` | `string` | yes |
| `account_id` | `string` | yes |
| `event_type` | `string` | yes |
| `chat_id` | `integer` | no |
| `message_id` | `integer` | no |
| `event_at` | `string` | no |
| `payload_json` | `object` | no |

### EventPage

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<Event>` | yes |
| `next_cursor` | `integer` | yes |
| `has_more` | `boolean` | yes |

### HealthResponse

| Field | Type | Required |
| --- | --- | --- |
| `ok` | `boolean` | yes |
| `database_id` | `string` | no |
| `schema_version` | `integer` | yes |
| `message_count` | `integer` | yes |
| `last_event_seq` | `integer` | yes |
| `operation_error_count` | `integer` | no |
| `server_time` | `string` | no |

### MediaFile

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | yes |
| `account_id` | `string` | yes |
| `chat_id` | `integer` | yes |
| `message_id` | `integer` | yes |
| `file_index` | `integer` | yes |
| `file_path` | `string` | no |
| `media_kind` | `string` | no |
| `mime_type` | `string` | no |
| `file_size` | `integer` | no |
| `downloaded_at` | `string` | no |
| `chat_title` | `string` | no |
| `origin_title` | `string` | no |
| `content_type` | `string` | no |
| `preview_kind` | `string` | no |
| `access_url` | `string` | no |
| `download_url` | `string` | no |
| `raw_json` | `object` | no |

### MediaFileListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<MediaFile>` | yes |

### Message

| Field | Type | Required |
| --- | --- | --- |
| `event_seq` | `integer` | no |
| `source` | `string` | yes |
| `account_id` | `string` | yes |
| `chat_id` | `integer` | yes |
| `message_id` | `integer` | yes |
| `topic_id` | `integer` | no |
| `sender_id` | `integer` | no |
| `sender_name` | `string` | no |
| `sender_username` | `string` | no |
| `sent_at` | `string` | no |
| `edited_at` | `string` | no |
| `ingested_at` | `string` | no |
| `deleted_at` | `string` | no |
| `text` | `string` | no |
| `has_media` | `boolean` | no |
| `media_kind` | `string` | no |
| `media_count` | `integer` | no |
| `media_files` | `array<MediaFile>` | no |
| `chat_title` | `string` | no |
| `origin_title` | `string` | no |
| `raw_json` | `object` | no |

### MessageListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<Message>` | yes |

### MessagePage

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<Message>` | yes |
| `next_cursor` | `integer` | yes |
| `has_more` | `boolean` | yes |

### OperationEvent

| Field | Type | Required |
| --- | --- | --- |
| `id` | `integer` | yes |
| `source` | `string` | yes |
| `account_id` | `string` | yes |
| `operation` | `string` | yes |
| `status` | `string` | yes |
| `subject_type` | `string` | no |
| `subject_id` | `string` | no |
| `error_code` | `string` | no |
| `message` | `string` | no |
| `retry_after` | `integer` | no |
| `occurred_at` | `string` | no |
| `error` | `object` | no |
| `error_type` | `string` | no |
| `auth_state` | `string` | no |
| `subject` | `object` | no |
| `subject_label` | `string` | no |
| `raw_json` | `object` | no |

### OperationEventDeleteInput

| Field | Type | Required |
| --- | --- | --- |
| `id` | `integer` | no |
| `ids` | `array` | no |

### OperationEventDeleteResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `OperationEventDeleteResult` | yes |

### OperationEventDeleteResult

| Field | Type | Required |
| --- | --- | --- |
| `ids` | `array` | yes |
| `deleted` | `integer` | yes |

### OperationEventListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<OperationEvent>` | yes |

### Origin

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | yes |
| `account_id` | `string` | yes |
| `origin_id` | `integer` | yes |
| `topic_id` | `integer` | yes |
| `origin_type` | `string` | yes |
| `parent_origin_id` | `integer` | no |
| `title` | `string` | no |
| `username` | `string` | no |
| `is_forum` | `boolean` | no |
| `important` | `boolean` | no |
| `archived_at` | `string` | no |
| `last_message_at` | `string` | no |
| `discovered_at` | `string` | no |
| `updated_at` | `string` | no |
| `parent_title` | `string` | no |
| `backup_policy` | `BackupPolicy` | no |
| `raw_json` | `object` | no |

### OriginArchiveInput

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | no |
| `account_id` | `string` | yes |
| `origin_id` | `integer` | yes |
| `topic_id` | `integer` | no |
| `archived` | `boolean` | no |

### OriginArchiveResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `object` | yes |

### OriginDeleteResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `object` | yes |

### OriginImportantInput

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | no |
| `account_id` | `string` | yes |
| `origin_id` | `integer` | yes |
| `topic_id` | `integer` | no |
| `important` | `boolean` | no |

### OriginInput

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | no |
| `account_id` | `string` | yes |
| `origin_id` | `integer` | yes |
| `topic_id` | `integer` | no |
| `origin_type` | `string` | yes |
| `parent_origin_id` | `integer` | no |
| `title` | `string` | no |
| `username` | `string` | no |
| `is_forum` | `boolean` | no |
| `important` | `boolean` | no |
| `last_message_at` | `string` | no |

### OriginItemResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `Origin` | yes |

### OriginListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<Origin>` | yes |

### Participant

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | yes |
| `account_id` | `string` | yes |
| `origin_id` | `integer` | yes |
| `user_id` | `integer` | yes |
| `username` | `string` | no |
| `display_name` | `string` | no |
| `is_bot` | `boolean` | no |
| `role` | `string` | no |
| `last_seen_at` | `string` | no |
| `updated_at` | `string` | no |
| `raw_json` | `object` | no |

### ParticipantDeleteResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `object` | yes |

### ParticipantInput

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | no |
| `account_id` | `string` | yes |
| `origin_id` | `integer` | yes |
| `user_id` | `integer` | yes |
| `username` | `string` | no |
| `display_name` | `string` | no |
| `is_bot` | `boolean` | no |
| `role` | `string` | no |
| `last_seen_at` | `string` | no |

### ParticipantItemResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `Participant` | yes |

### ParticipantListResponse

| Field | Type | Required |
| --- | --- | --- |
| `items` | `array<Participant>` | yes |

### ParticipantRefreshInput

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | no |
| `account_id` | `string` | yes |
| `origin_id` | `integer` | yes |
| `limit` | `integer` | no |

### ParticipantRefreshResult

| Field | Type | Required |
| --- | --- | --- |
| `account_id` | `string` | no |
| `origin_id` | `integer` | no |
| `authorized` | `boolean` | no |
| `status` | `string` | no |
| `participants` | `integer` | no |
| `errors` | `array` | no |
| `participants_truncated` | `boolean` | no |
| `limit` | `integer` | no |

### ParticipantRefreshResultResponse

| Field | Type | Required |
| --- | --- | --- |
| `item` | `ParticipantRefreshResult` | yes |

### RequestCodeInput

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | no |
| `account_id` | `string` | yes |
| `phone` | `string` | yes |

### StateResponse

| Field | Type | Required |
| --- | --- | --- |
| `database_id` | `string` | no |
| `schema_version` | `integer` | yes |
| `message_count` | `integer` | yes |
| `last_event_seq` | `integer` | yes |
| `operation_error_count` | `integer` | no |
| `server_time` | `string` | no |

### SubmitCodeInput

| Field | Type | Required |
| --- | --- | --- |
| `source` | `string` | no |
| `account_id` | `string` | yes |
| `phone` | `string` | yes |
| `code` | `string` | yes |
| `password` | `string` | no |
