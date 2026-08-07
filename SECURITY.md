# Security Policy

`tele-mess-core` handles Telegram sessions, message archives, media, and API
tokens. Treat vulnerability reports and reproduction material accordingly.

## Supported Code

The project does not maintain long-term support branches. Security fixes target
the default branch and, when appropriate, the latest release. Older releases
are not guaranteed to receive backports.

## Runtime Security Model

- The initial product assumes one trusted operator and trusted processes running
  as the same OS user. It does not provide multi-tenant isolation.
- The built-in HTTP server has no TLS termination. It binds to loopback by
  default; do not expose it directly to an untrusted LAN or the public internet.
  Use a trusted private overlay or a correctly configured TLS reverse proxy when
  remote access is required.
- Management and sync requests require the configured bearer token by default.
  The console and static API documentation are public to the bound interface,
  but privileged calls made by the console still require that token.
- The core does not encrypt SQLite archives, Telegram session files, downloaded
  media, logs, or generated packages at rest. Restrict workspace permissions,
  protect backups, and use OS or full-disk encryption where appropriate.
- Anyone who can read the workspace or act as its OS user may be able to access
  archived messages or authenticated Telegram sessions. Do not share a
  workspace between mutually untrusted users.

## AI Provider Data Boundary

Local-first storage does not mean local AI inference. With
`daily.ai.provider: codex-cli`, the core runs the Codex CLI as a local process,
but selected message text, prompts, and configured image inputs may be sent to
the service used by that Codex account. If the optional OpenAI-compatible
fallback is enabled and triggered, the remaining eligible stage inputs are sent
to its configured endpoint.

Default direct Codex batch commands use `--disable hooks` and `--ephemeral`, so
configured Codex lifecycle hooks do not receive those batches and Codex does not
persist local session rollout files for them. This local isolation does not
change what the selected AI provider receives or the summary artifacts retained
by `tele-mess-core`.

Review the selected origins and provider terms before enabling analysis on
private archives. Set `daily.ai.provider: disabled` to prevent AI-provider
calls; archival, sync, and management features remain available, while analysis
stages produce explicit disabled-provider artifacts.

## Report a Vulnerability Privately

Use GitHub's
[private vulnerability reporting](https://github.com/dreaifekks/tele-mess-core/security/advisories/new)
whenever it is available. Include:

- the affected version or commit;
- the security impact;
- minimal reproduction steps using synthetic data; and
- any suggested mitigation.

Do not include real Telegram sessions, tokens, phone numbers, login codes, 2FA
passwords, message contents, SQLite archives, or downloaded media.

If private vulnerability reporting is unavailable, open a public issue with
only:

- a title such as `Private security contact requested`;
- the affected public version; and
- a request for a private reporting path.

Do **not** disclose exploit details, logs, account identifiers, screenshots,
private messages, or reproduction code in that public issue.

Use the dedicated
[private security contact request](https://github.com/dreaifekks/tele-mess-core/issues/new?template=security_contact.yml)
template so this fallback remains available even when blank issues are disabled.

The maintainer will respond as availability permits; the project does not
currently promise a response-time SLA. Please allow time for investigation and
a fix before public disclosure.

## If a Secret Has Already Leaked

Revoke or rotate the credential immediately. Delete exposed Telegram session
files and terminate the corresponding Telegram sessions where appropriate.
Removing a post or filing a report does not invalidate a leaked credential.

## Scope

Useful reports include authentication or authorization bypasses, token or
session disclosure, unintended archive or media access, unsafe file handling,
and injection or remote-code-execution paths. Reports should test only data and
accounts you are authorized to use.
