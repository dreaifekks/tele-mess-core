from __future__ import annotations

from importlib import resources


def console_html() -> str:
    return resources.files("tele_mess_core.server").joinpath("console.html").read_text(encoding="utf-8")
