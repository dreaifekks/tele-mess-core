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
            def __init__(self, config: object, store: object, *, ingest: bool = True) -> None:
                events.append(f"runtime-created:ingest={ingest}")

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
                "runtime-created:ingest=True",
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
            def __init__(self, config: object, store: object, *, ingest: bool = True) -> None:
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
                    mcp_token="",
                    allow_unauthenticated_localhost=True,
                )
            )
            await _run_local(config, object(), web=True)  # type: ignore[arg-type]

        self.assertIn("api-created", events)
        self.assertIn("api-started", events)
        self.assertEqual(events[-3:], ["api-stopped", "worker-stopped", "runtime-stopped"])

    async def test_serve_api_runs_core_without_ingestion(self) -> None:
        from tele_mess_core.cli import _run_core

        observed_ingest: list[bool] = []
        never_stopped = asyncio.Event()

        class FakeRuntime:
            def __init__(self, config: object, store: object, *, ingest: bool = True) -> None:
                observed_ingest.append(ingest)

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                return None

        class FakeWorker:
            def __init__(self, store: object, config: object, *, telegram_runtime: object) -> None:
                pass

            def start(self) -> None:
                return None

            def stop(self) -> None:
                return None

        class FakeApi:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def start_background(self) -> None:
                return None

            async def wait_stopped(self) -> None:
                await never_stopped.wait()

            def stop(self) -> None:
                return None

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
                    mcp_token="",
                    allow_unauthenticated_localhost=True,
                )
            )
            await _run_core(config, object(), web=True, ingest=False)  # type: ignore[arg-type]

        self.assertEqual(observed_ingest, [False])

    async def test_smoke_telegram_fails_fast_when_offline(self) -> None:
        from tele_mess_core.archive import ArchiveStore
        from tele_mess_core.cli import _smoke_telegram
        from tele_mess_core.config import AppConfig, LoggingConfig, ServerConfig, StorageConfig, TelegramAccountConfig, TelegramConfig
        from tele_mess_core.telegram.manager import TelegramRuntimeManager

        class OfflineClient:
            async def connect(self) -> None:
                raise ConnectionError("offline")

            async def disconnect(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root / "archive.db")
            store.initialize()
            account = TelegramAccountConfig(
                account_id="main",
                api_id=1,
                api_hash="hash",
                session_name="main",
                session_dir=root / "sessions",
            )
            config = AppConfig(
                storage=StorageConfig(data_dir=root, database=root / "archive.db"),
                telegram=TelegramConfig(accounts=[account]),
                server=ServerConfig(token="secret"),
                logging=LoggingConfig(file=None),
            )
            stdout = io.StringIO()
            original_init = TelegramRuntimeManager.__init__

            def fast_init(self_rt, *args, **kwargs):
                kwargs["client_factory"] = lambda _cfg: OfflineClient()
                kwargs["connect_wait_seconds"] = 0.5
                original_init(self_rt, *args, **kwargs)

            try:
                with patch.object(TelegramRuntimeManager, "__init__", fast_init), redirect_stdout(stdout):
                    code = await _smoke_telegram(config, store, "main", False, 20)
            finally:
                store.close()

            self.assertEqual(code, 1)
            payload = json.loads(stdout.getvalue())
            self.assertFalse(payload["status"]["authorized"])
            self.assertEqual(payload["status"]["error"]["code"], "auth_status_failed")


class DailyCliExecutionTest(unittest.TestCase):
    def _config_file(self, root: Path) -> Path:
        config_path = root / "config.yml"
        config_path.write_text(
            f"""
storage:
  data_dir: {root / 'data'}
  database: {root / 'data' / 'archive.db'}
telegram:
  accounts:
    - account_id: main
      api_id: 1
      api_hash: hash
      session_name: main
      session_dir: {root / 'data' / 'sessions'}
server:
  token: secret
logging:
  file: null
daily:
  output_dir: {root / 'data' / 'daily-packages'}
  ai:
    provider: disabled
""",
            encoding="utf-8",
        )
        return config_path

    def test_daily_run_runs_only_its_own_job_when_no_service_is_running(self) -> None:
        from tele_mess_core.archive import ArchiveStore
        from tele_mess_core.daily_jobs import enqueue_daily_summary_job
        from tele_mess_core.config import load_config

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._config_file(root)
            config = load_config(config_path)
            store = ArchiveStore(config.storage.database)
            store.initialize()
            try:
                earlier = enqueue_daily_summary_job(store, config, run_date="2026-07-02", timezone_name="UTC")
            finally:
                store.close()

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                code = main(["--config", str(config_path), "daily-run", "--date", "2026-07-03", "--timezone", "UTC"])

            self.assertEqual(code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "completed")
            self.assertNotEqual(payload["job_id"], earlier["job_id"])

            store = ArchiveStore(config.storage.database)
            try:
                earlier_after = store.get_daily_summary_job(earlier["job_id"])
                assert earlier_after is not None
                self.assertEqual(earlier_after["status"], "queued")
            finally:
                store.close()

    def test_daily_run_waits_for_running_service_when_worker_lock_is_held(self) -> None:
        from tele_mess_core.archive import ArchiveStore
        from tele_mess_core.config import load_config
        from tele_mess_core.daily_jobs import DailyJobWorker

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._config_file(root)
            config = load_config(config_path)
            store = ArchiveStore(config.storage.database)
            store.initialize()
            worker = DailyJobWorker(store, config, poll_interval=0.05, worker_id="service-worker")
            worker.start()
            try:
                # Wait until the service worker acquires the process lock.
                lock_path = config.storage.data_dir / ".daily-worker.lock"
                for _ in range(50):
                    if lock_path.exists() and "service-worker" in lock_path.read_text(encoding="utf-8"):
                        break
                    import time
                    time.sleep(0.02)

                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    code = main(["--config", str(config_path), "daily-run", "--date", "2026-07-03", "--timezone", "UTC"])

                self.assertEqual(code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["status"], "completed")
                job = store.get_daily_summary_job(payload["job_id"])
                assert job is not None
                self.assertEqual(job["worker_id"], "service-worker")
            finally:
                worker.stop()
                store.close()

    def test_daily_summary_exits_nonzero_and_prints_job_on_canceled_or_failed(self) -> None:
        from tele_mess_core.archive import ArchiveStore
        from tele_mess_core.config import load_config
        from tele_mess_core.models import DailySummaryRunRecord

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._config_file(root)
            config = load_config(config_path)
            store = ArchiveStore(config.storage.database)
            store.initialize()
            try:
                store.upsert_daily_summary_run(
                    DailySummaryRunRecord(
                        run_id="sum_stale_done",
                        status="completed",
                        package_run_id="pkg_1",
                        date="2026-07-03",
                        timezone="UTC",
                        scope_json="{}",
                        output_dir=str(root),
                    )
                )
            finally:
                store.close()

            canceled_job = {
                "job_id": "job_canceled_1",
                "status": "canceled",
                "summary_run_id": "sum_stale_done",
                "error": "canceled during delivery",
            }
            stdout = io.StringIO()
            with patch("tele_mess_core.daily_jobs.run_daily_summary_job_from_cli", return_value=canceled_job):
                with redirect_stdout(stdout):
                    code = main(["--config", str(config_path), "daily-summary", "--date", "2026-07-03", "--timezone", "UTC"])

            self.assertEqual(code, 1)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "canceled")
            self.assertEqual(payload["job_id"], "job_canceled_1")
            self.assertEqual(payload["error"], "canceled during delivery")

    def test_run_core_waits_for_worker_lock_before_starting_telegram_runtime(self) -> None:
        import asyncio
        import threading
        import time
        from types import SimpleNamespace
        from tele_mess_core.cli import _run_core
        from tele_mess_core.daily_jobs import DAILY_WORKER_LOCK_FILENAME, DailyWorkerLock

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            holder = DailyWorkerLock(root / DAILY_WORKER_LOCK_FILENAME, owner="cli:active")
            self.assertTrue(holder.try_acquire())
            events: list[str] = []

            class FakeRuntime:
                def __init__(self, *args: object, **kwargs: object) -> None:
                    pass

                async def start(self) -> None:
                    events.append("runtime_start")

                async def stop(self) -> None:
                    events.append("runtime_stop")

            class FakeWorker:
                def __init__(self, *args: object, **kwargs: object) -> None:
                    self.use_process_lock = kwargs.get("use_process_lock")

                def start(self) -> None:
                    events.append("worker_start")

                def stop(self) -> None:
                    events.append("worker_stop")

            def release_lock() -> None:
                time.sleep(0.15)
                events.append("cli_lock_release")
                holder.release()

            async def run() -> None:
                stopper = threading.Thread(target=release_lock, daemon=True)
                stopper.start()
                config = SimpleNamespace(storage=SimpleNamespace(data_dir=root))

                def install_stop(stop_event: asyncio.Event) -> None:
                    asyncio.get_running_loop().call_later(0.35, stop_event.set)

                try:
                    with (
                        patch("tele_mess_core.telegram.manager.TelegramRuntimeManager", FakeRuntime),
                        patch("tele_mess_core.daily_jobs.DailyJobWorker", FakeWorker),
                        patch("tele_mess_core.cli._install_stop_handlers", side_effect=install_stop),
                    ):
                        await _run_core(config, object(), web=False)  # type: ignore[arg-type]
                finally:
                    holder.release()
                    stopper.join(timeout=2)

            asyncio.run(run())
            self.assertIn("cli_lock_release", events)
            self.assertIn("runtime_start", events)
            self.assertLess(events.index("cli_lock_release"), events.index("runtime_start"))

    def test_real_sigterm_interrupts_waiting_standalone_cli_without_deadlock(self) -> None:
        import os
        import signal
        import subprocess
        import sys
        import time
        from tele_mess_core.config import load_config
        from tele_mess_core.daily_jobs import DAILY_WORKER_LOCK_FILENAME, DailyWorkerLock

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._config_file(root)
            config = load_config(config_path)
            holder = DailyWorkerLock(config.storage.data_dir / DAILY_WORKER_LOCK_FILENAME, owner="svc", mode="service")
            self.assertTrue(holder.try_acquire())
            env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
            proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "tele_mess_core",
                    "--config",
                    str(config_path),
                    "daily-run",
                    "--date",
                    "2026-07-03",
                    "--timezone",
                    "UTC",
                ],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                # Wait until the child has initialized the DB, enqueued its job, and entered the wait loop.
                import sqlite3

                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline:
                    if config.storage.database.exists():
                        conn = None
                        try:
                            conn = sqlite3.connect(str(config.storage.database), timeout=0.2)
                            row = conn.execute("SELECT COUNT(*) FROM daily_summary_jobs").fetchone()
                            if row and row[0] > 0:
                                break
                        except sqlite3.Error:
                            pass
                        finally:
                            if conn is not None:
                                conn.close()
                    time.sleep(0.05)
                time.sleep(0.1)
                proc.send_signal(signal.SIGTERM)
                exit_code = proc.wait(timeout=5.0)
                self.assertEqual(exit_code, 128 + int(signal.SIGTERM))
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=2.0)
                if proc.stdout is not None:
                    proc.stdout.close()
                if proc.stderr is not None:
                    proc.stderr.close()
                holder.release()


if __name__ == "__main__":
    unittest.main()
