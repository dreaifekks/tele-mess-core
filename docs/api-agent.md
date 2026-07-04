# tele-mess-core Agent API Notes

This file is generated from `tele_mess_core.server.contracts` for quick agent lookup.

- Contract version: `2026-07-03.3`
- Contract hash: `17089891060961e6`
- Full reference: `docs/api.md`
- OpenAPI snapshot: `docs/openapi.json`
- Runtime manifest: `/manage/api-manifest`

## Agent Rules

- Treat `contracts.py` as the source of truth for endpoint shape.
- When changing an API handler, update the contract and regenerate docs in the same change.
- Do not read or commit local secrets such as `config.yml`, Telegram `.session` files, SQLite archives, media files, tokens, phone numbers, login codes, or 2FA passwords unless the user explicitly asks and the data is needed.
- Token-protected endpoints accept `Authorization: Bearer <token>` or `X-Api-Token: <token>`.

## Write Endpoints

- `POST /manage/accounts` body `AccountInput` - Create or update account metadata.
- `DELETE /manage/accounts` body `AuthStatusInput` - Delete management account metadata.
- `POST /manage/accounts/auth` body `AccountAuthInput` - Create or update account auth state.
- `PATCH /manage/accounts/auth` body `AccountAuthInput` - Patch account auth state.
- `POST /manage/accounts/auth/status` body `AuthStatusInput` - Check live Telegram auth status.
- `POST /manage/accounts/auth/request-code` body `RequestCodeInput` - Request a Telegram login code.
- `POST /manage/accounts/auth/submit-code` body `SubmitCodeInput` - Submit a Telegram login code and optional 2FA password.
- `POST /manage/origins` body `OriginInput` - Create or update origin metadata.
- `DELETE /manage/origins` body `OriginArchiveInput` - Delete an origin and related management metadata.
- `PATCH /manage/origins/archive` body `OriginArchiveInput` - Archive or restore an origin.
- `PATCH /manage/origins/important` body `OriginImportantInput` - Mark or unmark an origin as important.
- `POST /manage/backup-policies` body `BackupPolicyInput` - Create or update an origin backup policy.
- `PATCH /manage/backup-policies` body `BackupPolicyInput` - Patch an origin backup policy.
- `DELETE /manage/backup-policies` body `BackupPolicyInput` - Delete an origin backup policy.
- `POST /manage/participants` body `ParticipantInput` - Create or update a participant profile.
- `DELETE /manage/participants` body `ParticipantInput` - Delete a participant profile.
- `DELETE /manage/operation-events` body `OperationEventDeleteInput` - Delete one or more operation events.
- `PATCH /manage/daily-package-schedule` body `DailyPackageScheduleInput` - Update the daily package system schedule.
- `POST /manage/daily-packages` body `DailyPackageRunInput` - Generate a daily package immediately.
- `POST /manage/daily-summaries` body `DailySummaryRunInput` - Run a daily summary immediately.
- `POST /manage/discover-origins` body `DiscoveryInput` - Discover Telegram dialogs and topics for an authenticated account.
- `POST /manage/participants/refresh` body `ParticipantRefreshInput` - Refresh participants for a Telegram origin.

## Required Checks

```bash
tele-mess-core generate-api-docs --check
python -m unittest discover -s tests -v
```
