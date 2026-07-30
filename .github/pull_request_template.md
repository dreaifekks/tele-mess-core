## Summary

Describe the problem and the change.

## Validation

List the commands or checks you ran.

## Checklist

- [ ] I kept this pull request focused and added or updated tests for changed behavior.
- [ ] `python -m unittest discover -s tests -v` passes, or I explained why it was not run.
- [ ] `tele-mess-core generate-api-docs --check` passes, or the change does not affect generated API documentation.
- [ ] If an API handler changed, I updated `contracts.py`, the handler, affected console code, and generated API artifacts together.
- [ ] I documented user-visible behavior and noted compatibility or migration impact.
- [ ] I used synthetic test data and included no Telegram sessions, tokens, phone numbers, login codes, 2FA passwords, private messages, archives, media, or unredacted logs.

## Security and Privacy

Describe any authentication, authorization, secret-handling, archive, media, or
personal-data impact. Write `None` if there is no impact.
