from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tele_mess_core.runtime_paths import default_local_workspace, resolve_runtime_paths


class RuntimePathsTest(unittest.TestCase):
    def test_macos_local_default_uses_application_support(self) -> None:
        home = Path("/Users/example")

        selection = resolve_runtime_paths(
            None,
            None,
            local_mode=True,
            environ={},
            cwd=Path("/tmp/elsewhere"),
            home=home,
            platform="darwin",
        )

        expected = home / "Library" / "Application Support" / "tele-mess-core"
        self.assertEqual(selection.workspace_dir, expected)
        self.assertEqual(selection.config_path, expected / "config.yml")
        self.assertEqual(selection.workspace_source, "platform-default")
        self.assertEqual(selection.config_source, "workspace-default")

    def test_explicit_workspace_anchors_relative_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace with spaces"
            selection = resolve_runtime_paths(
                "settings/config.yml",
                root,
                local_mode=True,
                environ={},
                cwd=Path(tmp) / "unrelated",
            )

        self.assertEqual(selection.workspace_dir, root.resolve())
        self.assertEqual(selection.config_path, (root / "settings" / "config.yml").resolve())
        self.assertEqual(selection.workspace_source, "argument")
        self.assertEqual(selection.config_source, "argument")

    def test_environment_config_sets_workspace_to_its_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "instance" / "config.yml"
            selection = resolve_runtime_paths(
                None,
                None,
                local_mode=True,
                environ={"TELE_MESS_CORE_CONFIG": str(config_path)},
                cwd=Path(tmp) / "other",
            )

        self.assertEqual(selection.config_path, config_path.resolve())
        self.assertEqual(selection.workspace_dir, config_path.parent.resolve())
        self.assertEqual(selection.workspace_source, "config")
        self.assertEqual(selection.config_source, "environment")

    def test_xdg_default_is_supported_outside_macos(self) -> None:
        expected = Path("/tmp/xdg-data") / "tele-mess-core"
        self.assertEqual(
            default_local_workspace(
                environ={"XDG_DATA_HOME": "/tmp/xdg-data"},
                home=Path("/home/example"),
                platform="linux",
            ),
            expected,
        )


if __name__ == "__main__":
    unittest.main()
