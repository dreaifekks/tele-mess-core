from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tele_mess_core.config import load_config
from tele_mess_core.maintenance import install_raw_json_cleanup_timer


class MaintenanceTest(unittest.TestCase):
    def test_raw_json_cleanup_schedule_writes_systemd_user_timer_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.yml"
            config_path.write_text(
                f"""
storage:
  database: "{root / "archive.db"}"
  raw_json_retention_days: 7
telegram:
  api_id: 1
  api_hash: hash
daily:
  systemd_user_dir: "{root / "systemd-user"}"
  cli_path: ./bin/tele-mess-core
""",
                encoding="utf-8",
            )
            config = load_config(config_path)

            result = install_raw_json_cleanup_timer(
                config,
                retention_days=7,
                on_calendar="weekly",
                vacuum=True,
                activate=False,
            )

            self.assertTrue(result["installed"])
            service = root / "systemd-user" / "tele-mess-core-raw-json-cleanup.service"
            timer = root / "systemd-user" / "tele-mess-core-raw-json-cleanup.timer"
            self.assertIn("cleanup-raw-json --retention-days 7 --vacuum", service.read_text(encoding="utf-8"))
            self.assertIn("OnCalendar=weekly", timer.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
