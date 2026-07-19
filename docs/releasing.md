# Publishing a Release

Releases are built from Git tags and published to PyPI by
`.github/workflows/release.yml`. The workflow builds both an sdist and a
platform-independent wheel, validates them, stores them as a GitHub Actions
artifact, and publishes the same files through PyPI Trusted Publishing.

## One-Time PyPI Setup

1. Create or use a personal PyPI account with a verified email address and
   two-factor authentication.
2. Create a GitHub environment named `pypi`. Requiring manual approval for the
   environment is recommended.
3. In the PyPI account's **Publishing** settings, add a pending GitHub
   publisher with these values:

   ```text
   PyPI project: tele-mess-core
   GitHub owner: dreaifekks
   GitHub repository: tele-mess-core
   Workflow: release.yml
   Environment: pypi
   ```

The pending publisher creates the PyPI project on its first successful upload.
It does not reserve the project name before that upload. No long-lived PyPI API
token belongs in GitHub secrets or this repository.

## Release Checklist

1. Start from a clean worktree and run the full validation suite:

   ```bash
   ./.venv/bin/python -m unittest discover -s tests -v
   ./.venv/bin/tele-mess-core generate-api-docs --check
   ./.venv/bin/python -m compileall -q src tests
   ```

2. Choose a new PEP 440 version. PyPI release files are immutable; never reuse
   a version that has already been uploaded.
3. Keep the release number aligned in:

   ```text
   pyproject.toml
   src/tele_mess_core/__init__.py
   src/tele_mess_core/server/api.py
   CHANGELOG.md
   ```

4. Commit the release, then create and push an annotated tag matching the
   project version exactly:

   ```bash
   git tag -a vX.Y.Z -m "Release vX.Y.Z"
   git push origin master
   git push origin vX.Y.Z
   ```

The workflow rejects a tag when it differs from `v<project.version>`, either
runtime version differs from the project version, or the changelog has no dated
section for the release.

## Verify the Published Package

After the publish job succeeds, force a fresh PyPI resolution and run the wheel
through its public command:

```bash
uvx --refresh --from "tele-mess-core==X.Y.Z" \
  tele-mess-core run-local --help
```

The GitHub Actions `python-distributions-X.Y.Z` artifact contains the exact
sdist and wheel submitted to PyPI.
