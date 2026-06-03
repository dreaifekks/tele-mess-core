# Product Direction

## Current Goal

`tele-mess-core` is a single-user core for managing multiple Telegram accounts.
It should behave like a long-running archive database plus a remotely managed
control plane for Mac clients and a future core web console.

The main product focus is the client/core interface. Telegram ingestion is the
foundation, but remote management APIs decide how the Mac client can configure,
inspect, and operate the core.


## Current Implementation Status

The basic management model is implemented in SQLite and exposed through HTTP:

- Account metadata, auth/session state, remote code request, code submission, and 2FA password handoff.
- Origin and topic registry.
- Per-origin backup policy with text, media metadata, and actual media file downloading.
- Participant profile registry.
- Capture cursors for bounded history backfill and reconnect catch-up.
- Live origin discovery and participant refresh using existing authenticated
  Telegram sessions.
- Token-protected management endpoints shared by future Mac and web clients.
- Built-in web console using the same management endpoints.

The remaining work is operational hardening against real Telegram edge cases,
such as unusual login errors, very large forum-topic sets, and long-running media
download failures.

## Product Boundary

- Single owner/user in the first version.
- Multiple Telegram accounts under that owner.
- One local SQLite archive as the server-side source of truth.
- Remote clients can manage capture configuration and sync archived data.
- A future core web console should use the same management concepts as the Mac
  client instead of requiring a separate product model.

This means there is no multi-tenant account system in the initial scope. Remote
authentication still matters because adding Telegram accounts and changing backup
configuration are privileged actions.

## Managed Objects

The core should expose enough structured metadata for clients to understand and
configure Telegram origins:

- Telegram account: local `account_id`, session state, display metadata, and
  authentication/add-account state.
- Origin: a backup-selectable source such as group, channel, private chat, or
  other Telethon dialog type.
- Topic: group or forum topic metadata under an origin when Telegram exposes it.
- Origin identity: stable source, `account_id`, origin type, Telegram origin ID,
  username/title, and raw origin metadata for later migration.
- Backup policy: whether an origin is selected for backup, whether only text is
  archived, and whether media metadata or media files should be included.
- Member/person profile: group participants and sender profiles used later to
  identify important information sources.

## Interface Priorities

The next API surface should be management-first, not just sync-first:

- List Telegram accounts and their login/session status.
- Add or authenticate a Telegram account remotely with a secure challenge flow.
- List discoverable origins per account, including group/channel/topic shape and
  original Telegram IDs.
- Read and update origin backup selection.
- Read and update capture mode per origin, including text-only versus
  media-inclusive backup.
- Fetch and refresh group participants where available.
- Keep sync endpoints cursor-based so clients can efficiently mirror archive
  changes.

## Open Design Questions

- Whether media-inclusive backup first means media metadata only, downloaded file
  blobs, or both.
- How to represent Telegram account login prompts over a remote API without
  leaking codes or long-lived session material.
- How much participant data should be stored by default, given privacy and local
  database size concerns.
- Whether the core web console should be served by this Python process or by a
  separate frontend that calls the same HTTP API.
