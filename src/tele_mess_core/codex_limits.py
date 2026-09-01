from __future__ import annotations

import json
import os
import queue
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Sequence, TextIO


@dataclass(frozen=True, slots=True)
class CodexRateLimitStatus:
    reached: bool
    reason: str | None = None


def codex_app_server_command(ai_command: Sequence[str]) -> list[str] | None:
    if not ai_command:
        return None
    executable = str(ai_command[0])
    name = Path(executable).name.lower()
    if name not in {"codex", "codex.exe", "codex.cmd"}:
        return None
    return [executable, "app-server", "--stdio"]


def read_codex_rate_limit_status(
    command: Sequence[str],
    *,
    timeout_seconds: float = 10.0,
    cancel_check: Callable[[], None] | None = None,
    now_epoch: int | None = None,
) -> CodexRateLimitStatus | None:
    if not command:
        return None
    try:
        process = subprocess.Popen(
            list(command),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            start_new_session=(os.name != "nt"),
        )
    except OSError:
        return None
    if process.stdin is None or process.stdout is None:
        _terminate_process(process)
        _close_process_pipes(process)
        return None

    messages = (
        {
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "tele-mess-core",
                    "version": "rate-limit-probe/1",
                }
            },
        },
        {"method": "initialized"},
        {"id": 2, "method": "account/rateLimits/read", "params": None},
    )
    try:
        for message in messages:
            process.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
        process.stdin.flush()
    except OSError:
        _terminate_process(process)
        _close_process_pipes(process)
        return None

    lines: queue.Queue[str | None] = queue.Queue()
    reader = threading.Thread(
        target=_read_lines,
        args=(process.stdout, lines),
        name="codex-rate-limit-reader",
        daemon=True,
    )
    reader.start()
    deadline = time.monotonic() + max(0.1, float(timeout_seconds))
    result: dict[str, Any] | None = None
    try:
        while time.monotonic() < deadline:
            if cancel_check:
                cancel_check()
            try:
                line = lines.get(timeout=min(0.25, max(0.01, deadline - time.monotonic())))
            except queue.Empty:
                if process.poll() is not None and not reader.is_alive():
                    break
                continue
            if line is None:
                break
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(message, dict) or message.get("id") != 2:
                continue
            candidate = message.get("result")
            if isinstance(candidate, dict):
                result = candidate
            break
    except BaseException:
        _terminate_process(process)
        raise
    finally:
        _close_stream(process.stdin)
        if process.poll() is None:
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                _terminate_process(process)
        reader.join(timeout=1)
        _close_stream(process.stdout)

    if result is None:
        return None
    return classify_codex_rate_limits(result, now_epoch=now_epoch)


def classify_codex_rate_limits(
    result: dict[str, Any],
    *,
    now_epoch: int | None = None,
) -> CodexRateLimitStatus | None:
    snapshots = _codex_snapshots(result)
    if not snapshots:
        return None
    now = int(time.time()) if now_epoch is None else int(now_epoch)
    for snapshot in snapshots:
        limit_id = str(snapshot.get("limitId") or "codex")
        reached_type = str(snapshot.get("rateLimitReachedType") or "").strip()
        if reached_type:
            return CodexRateLimitStatus(True, f"{limit_id}:{reached_type}")
        if snapshot.get("spendControlReached") is True:
            return CodexRateLimitStatus(True, f"{limit_id}:spend_control_reached")
        individual_limit = snapshot.get("individualLimit")
        if isinstance(individual_limit, dict):
            remaining = individual_limit.get("remainingPercent")
            resets_at = individual_limit.get("resetsAt")
            if _number_at_most(remaining, 0) and not _reset_has_passed(resets_at, now):
                return CodexRateLimitStatus(True, f"{limit_id}:individual_limit_reached")
        credits_available = _credits_available(snapshot.get("credits"))
        if credits_available is not False:
            continue
        for window_name in ("primary", "secondary"):
            window = snapshot.get(window_name)
            if not isinstance(window, dict):
                continue
            if _number_at_least(window.get("usedPercent"), 100) and not _reset_has_passed(
                window.get("resetsAt"), now
            ):
                return CodexRateLimitStatus(True, f"{limit_id}:{window_name}_window_reached")
    return CodexRateLimitStatus(False)


def _codex_snapshots(result: dict[str, Any]) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    by_limit_id = result.get("rateLimitsByLimitId")
    if isinstance(by_limit_id, dict) and isinstance(by_limit_id.get("codex"), dict):
        snapshots.append(by_limit_id["codex"])
    historical = result.get("rateLimits")
    if isinstance(historical, dict) and historical not in snapshots:
        snapshots.append(historical)
    return snapshots


def _credits_available(raw: Any) -> bool | None:
    if not isinstance(raw, dict):
        return None
    if raw.get("unlimited") is True:
        return True
    if raw.get("hasCredits") is not True:
        return False
    balance = raw.get("balance")
    if balance in (None, ""):
        return True
    try:
        return Decimal(str(balance)) > 0
    except InvalidOperation:
        return True


def _number_at_least(raw: Any, threshold: int) -> bool:
    try:
        return int(raw) >= threshold
    except (TypeError, ValueError):
        return False


def _number_at_most(raw: Any, threshold: int) -> bool:
    try:
        return int(raw) <= threshold
    except (TypeError, ValueError):
        return False


def _reset_has_passed(raw: Any, now_epoch: int) -> bool:
    try:
        return int(raw) <= now_epoch
    except (TypeError, ValueError):
        return False


def _read_lines(stream: TextIO, output: queue.Queue[str | None]) -> None:
    try:
        for line in stream:
            output.put(line)
    finally:
        output.put(None)


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "nt":
            process.terminate()
        else:
            os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=2)
    except Exception:
        try:
            if os.name == "nt":
                process.kill()
            else:
                os.killpg(process.pid, signal.SIGKILL)
        except Exception:
            pass


def _close_process_pipes(process: subprocess.Popen[str]) -> None:
    _close_stream(process.stdin)
    _close_stream(process.stdout)


def _close_stream(stream: TextIO | None) -> None:
    if stream is None:
        return
    try:
        stream.close()
    except (OSError, ValueError):
        pass
