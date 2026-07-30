# Contributing to tele-mess-core

Thank you for helping improve `tele-mess-core`.

## Before You Start

- Use an issue to discuss a substantial change before investing in a large
  implementation.
- Use [SUPPORT.md](SUPPORT.md) for usage questions.
- Report vulnerabilities through the private process in
  [SECURITY.md](SECURITY.md), not through a public issue.

Never attach real Telegram data or local credentials to an issue, pull request,
test fixture, or log excerpt. This includes `config.yml`, `.session` files,
SQLite archives, downloaded media, API tokens, phone numbers, login codes, and
2FA passwords. Use synthetic data and redact identifiers.

## Development Setup

The project requires Python 3.11 or newer.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
```

Run the test suite:

```bash
python -m unittest discover -s tests -v
```

## API Contract Changes

`src/tele_mess_core/server/contracts.py` is the source of truth for HTTP
endpoint shapes. When changing an API handler:

1. Update `contracts.py`.
2. Update the handler in `src/tele_mess_core/server/api.py`.
3. Update the built-in console if it uses the changed endpoint.
4. Regenerate and check the API documentation.

```bash
tele-mess-core generate-api-docs
tele-mess-core generate-api-docs --check
```

The generated artifacts are `docs/api.md`, `docs/openapi.json`, and
`docs/api-agent.md`.

## Pull Requests

Keep each pull request focused. Include:

- the problem and the intended behavior;
- tests for changed behavior;
- the commands you ran to validate the change;
- documentation or changelog updates when user-visible behavior changes; and
- any security, privacy, migration, or compatibility impact.

Avoid tests that depend on a contributor's live Telegram account or local
archive. Prefer temporary directories, synthetic records, and mocked Telegram
boundaries.

By submitting a contribution, you agree that it is licensed under the
repository's [Apache License 2.0](LICENSE).
