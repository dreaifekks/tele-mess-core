from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import sys
from typing import Mapping


CONFIG_ENV = "TELE_MESS_CORE_CONFIG"
HOME_ENV = "TELE_MESS_CORE_HOME"
WORKSPACE_ENV = "TELE_MESS_CORE_WORKSPACE"


@dataclass(frozen=True, slots=True)
class RuntimePathSelection:
    workspace_dir: Path
    config_path: Path
    workspace_source: str
    config_source: str


def resolve_runtime_paths(
    config: str | Path | None,
    workspace: str | Path | None,
    *,
    local_mode: bool,
    environ: Mapping[str, str] | None = None,
    cwd: Path | None = None,
    home: Path | None = None,
    platform: str | None = None,
) -> RuntimePathSelection:
    """Resolve one stable instance root without changing the process cwd."""

    env = os.environ if environ is None else environ
    current_dir = _absolute_path(cwd or Path.cwd(), Path.cwd())
    home_dir = _absolute_path(home or Path.home(), current_dir)
    platform_name = sys.platform if platform is None else platform

    workspace_raw = workspace
    workspace_source = "argument"
    if workspace_raw in (None, ""):
        workspace_raw = env.get(WORKSPACE_ENV) or env.get(HOME_ENV)
        workspace_source = "environment"

    explicit_workspace: Path | None = None
    if workspace_raw not in (None, ""):
        explicit_workspace = _absolute_path(Path(str(workspace_raw)).expanduser(), current_dir)

    config_raw = config
    config_source = "argument"
    if config_raw in (None, ""):
        config_raw = env.get(CONFIG_ENV)
        config_source = "environment"

    if config_raw not in (None, ""):
        config_base = explicit_workspace or current_dir
        config_path = _absolute_path(Path(str(config_raw)).expanduser(), config_base)
        if explicit_workspace is not None:
            workspace_dir = explicit_workspace
        else:
            workspace_dir = config_path.parent
            workspace_source = "config"
        return RuntimePathSelection(
            workspace_dir=workspace_dir,
            config_path=config_path,
            workspace_source=workspace_source,
            config_source=config_source,
        )

    if explicit_workspace is not None:
        workspace_dir = explicit_workspace
    elif local_mode:
        workspace_dir = default_local_workspace(environ=env, home=home_dir, platform=platform_name)
        workspace_source = "platform-default"
    else:
        workspace_dir = current_dir
        workspace_source = "current-directory"
    return RuntimePathSelection(
        workspace_dir=workspace_dir,
        config_path=workspace_dir / "config.yml",
        workspace_source=workspace_source,
        config_source="workspace-default",
    )


def default_local_workspace(
    *,
    environ: Mapping[str, str] | None = None,
    home: Path | None = None,
    platform: str | None = None,
) -> Path:
    env = os.environ if environ is None else environ
    home_dir = (home or Path.home()).expanduser()
    platform_name = sys.platform if platform is None else platform
    if platform_name == "darwin":
        return (home_dir / "Library" / "Application Support" / "tele-mess-core").resolve()
    if platform_name.startswith("win"):
        local_app_data = env.get("LOCALAPPDATA")
        if local_app_data:
            return (Path(local_app_data).expanduser() / "tele-mess-core").resolve()
        return (home_dir / "AppData" / "Local" / "tele-mess-core").resolve()
    xdg_data_home = env.get("XDG_DATA_HOME")
    if xdg_data_home:
        return (Path(xdg_data_home).expanduser() / "tele-mess-core").resolve()
    return (home_dir / ".local" / "share" / "tele-mess-core").resolve()


def _absolute_path(path: Path, base: Path) -> Path:
    expanded = path.expanduser()
    if expanded.is_absolute():
        return expanded.resolve()
    return (base / expanded).resolve()
