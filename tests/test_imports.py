from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BLOCK_TELETHON = """
import sys

class BlockTelethon:
    def find_spec(self, name, path=None, target=None):
        if name == "telethon" or name.startswith("telethon."):
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)
        return None

sys.meta_path.insert(0, BlockTelethon())
"""


def _run_without_telethon(code: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "PYTHONDONTWRITEBYTECODE": "1", "PYTHON_COLORS": "0"}
    return subprocess.run(
        [sys.executable, "-c", BLOCK_TELETHON + code, *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )


class OptionalTelethonImportTest(unittest.TestCase):
    def test_read_only_entry_points_import_without_telethon(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = _run_without_telethon(
                "import tele_mess_core.server.mcp\n"
                "import tele_mess_core.daily_jobs\n"
                "from tele_mess_core.cli import main\n"
                "sys.exit(main(['generate-api-docs', '--output-dir', sys.argv[1]]))\n",
                tmp,
            )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_discovery_reports_missing_telethon_as_import_error(self) -> None:
        result = _run_without_telethon("import tele_mess_core.telegram.discovery\n")

        self.assertNotEqual(result.returncode, 0)
        self.assertRegex(result.stderr, r"discovery\.py\", line \d+, in <module>\n\s+from telethon import functions")

class TelegramPackageExportTest(unittest.TestCase):
    def test_package_lists_its_exports_before_they_are_loaded(self) -> None:
        result = _run_without_telethon(
            "import tele_mess_core.telegram as telegram\n"
            "sys.exit(sorted(set(telegram.__all__) - set(dir(telegram))) != [])\n"
        )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_package_exports_stay_importable(self) -> None:
        from tele_mess_core.telegram import (
            TelegramArchiveService,
            TelegramAuthService,
            TelegramDiscoveryService,
            TelegramRuntimeManager,
        )
        from tele_mess_core.telegram.auth import TelegramAuthService as auth_service

        self.assertIs(TelegramAuthService, auth_service)
        self.assertEqual(
            {TelegramArchiveService.__name__, TelegramDiscoveryService.__name__, TelegramRuntimeManager.__name__},
            {"TelegramArchiveService", "TelegramDiscoveryService", "TelegramRuntimeManager"},
        )
