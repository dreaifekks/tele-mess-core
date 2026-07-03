# tele-mess-core API

This file is generated from `tele_mess_core.server.contracts`.

- Contract version: `2026-07-03.1`
- Contract hash: `56d36c6eac0a0f95`
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
| `last_message_at` | `string` | no |
| `last_backfill_at` | `string` | no |
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
