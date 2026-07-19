from __future__ import annotations

import asyncio
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tele_mess_core.cli import _run_local, main


class LocalCliTest(unittest.TestCase):
    def test_paths_reports_effective_workspace_without_opening_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "Mac Workspace"
            root.mkdir()
            (root / "config.yml").write_text(
                """
storage:
  data_dir: ./state
  database: ./state/archive.db
telegram:
  api_id: 1
  api_hash: hash
  session_dir: ./state/sessions
logging:
  file: ./logs/core.log
""",
                encoding="utf-8",
            )
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                code = main(["paths", "--workspace", str(root)])

            payload = json.loads(stdout.getvalue())
            self.assertEqual(code, 0)
            self.assertEqual(payload["workspace_dir"], str(root.resolve()))
            self.assertEqual(payload["database"], str(root / "state" / "archive.db"))
            self.assertEqual(payload["session_dirs"]["default"], str(root / "state" / "sessions"))
            self.assertFalse(payload["local_web_default"])
            self.assertFalse((root / "state" / "archive.db").exists())

    def test_missing_local_config_does_not_create_cwd_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "empty-workspace"
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = main(["--workspace", str(root), "paths"])

            payload = json.loads(stdout.getvalue())
            self.assertEqual(code, 2)
            self.assertFalse(payload["config_exists"])
            self.assertIn(str(root / "config.yml"), stderr.getvalue())
            self.assertFalse(root.exists())


class LocalLifecycleTest(unittest.IsolatedAsyncioTestCase):
    async def test_local_mode_does_not_construct_http_server_by_default(self) -> None:
        events: list[str] = []
        loop = asyncio.get_running_loop()
        event_loop_thread = threading.get_ident()
        worker_stop_threads: list[int] = []

        async def acknowledge_worker_stop() -> None:
            events.append("worker-stop-acknowledged")

        class FakeRuntime:
            def __init__(self, config: object, store: object) -> None:
                events.append("runtime-created")

            async def start(self) -> None:
                events.append("runtime-started")

            async def stop(self) -> None:
                events.append("runtime-stopped")

        class FakeWorker:
            def __init__(self, store: object, config: object, *, telegram_runtime: object) -> None:
                events.append("worker-created")

            def start(self) -> None:
                events.append("worker-started")

            def stop(self) -> None:
                worker_stop_threads.append(threading.get_ident())
                asyncio.run_coroutine_threadsafe(acknowledge_worker_stop(), loop).result(timeout=1)
                events.append("worker-stopped")

        def stop_immediately(event: asyncio.Event) -> None:
            event.set()

        with (
            patch("tele_mess_core.telegram.manager.TelegramRuntimeManager", FakeRuntime),
            patch("tele_mess_core.daily_jobs.DailyJobWorker", FakeWorker),
            patch("tele_mess_core.cli.SyncApiServer") as api,
            patch("tele_mess_core.cli._install_stop_handlers", stop_immediately),
        ):
            await _run_local(object(), object())  # type: ignore[arg-type]

        api.assert_not_called()
        self.assertEqual(len(worker_stop_threads), 1)
        self.assertNotEqual(worker_stop_threads[0], event_loop_thread)
        self.assertEqual(
            events,
            [
                "runtime-created",
                "runtime-started",
                "worker-created",
                "worker-started",
                "worker-stop-acknowledged",
                "worker-stopped",
                "runtime-stopped",
            ],
        )

    async def test_web_is_explicit_opt_in_for_local_mode(self) -> None:
        events: list[str] = []
        never_stopped = asyncio.Event()

        class FakeRuntime:
            def __init__(self, config: object, store: object) -> None:
                pass

            async def start(self) -> None:
                events.append("runtime-started")

            async def stop(self) -> None:
                events.append("runtime-stopped")

        class FakeWorker:
            def __init__(self, store: object, config: object, *, telegram_runtime: object) -> None:
                pass

            def start(self) -> None:
                events.append("worker-started")

            def stop(self) -> None:
                events.append("worker-stopped")

        class FakeApi:
            def __init__(self, *args: object, **kwargs: object) -> None:
                events.append("api-created")

            def start_background(self) -> None:
                events.append("api-started")

            async def wait_stopped(self) -> None:
                await never_stopped.wait()

            def stop(self) -> None:
                events.append("api-stopped")

        def stop_immediately(event: asyncio.Event) -> None:
            event.set()

        with (
            patch("tele_mess_core.telegram.manager.TelegramRuntimeManager", FakeRuntime),
            patch("tele_mess_core.daily_jobs.DailyJobWorker", FakeWorker),
            patch("tele_mess_core.cli.SyncApiServer", FakeApi),
            patch("tele_mess_core.cli._install_stop_handlers", stop_immediately),
        ):
            config = SimpleNamespace(
                server=SimpleNamespace(
                    host="127.0.0.1",
                    port=8765,
                    token="",
                    allow_unauthenticated_localhost=True,
                )
            )
            await _run_local(config, object(), web=True)  # type: ignore[arg-type]

        self.assertIn("api-created", events)
        self.assertIn("api-started", events)
        self.assertEqual(events[-3:], ["api-stopped", "worker-stopped", "runtime-stopped"])


if __name__ == "__main__":
    unittest.main()
