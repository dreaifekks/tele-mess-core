from __future__ import annotations

import asyncio
from concurrent.futures import Future
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.cli import _sync_configured_accounts
from tele_mess_core.config import (
    AppConfig,
    LoggingConfig,
    ServerConfig,
    StorageConfig,
    TelegramAccountConfig,
    TelegramConfig,
    stored_account_config,
)
from tele_mess_core.models import AccountAuthRecord, AccountRecord, SOURCE_TELEGRAM
from tele_mess_core.telegram.manager import TelegramRuntimeManager, _default_client_factory


class FakeClient:
    def __init__(self, *, authorized: bool = False, connect_error: Exception | None = None):
        self.authorized = authorized
        self.connect_error = connect_error
        self.connect_count = 0
        self.disconnect_count = 0
        self.disconnected = asyncio.get_running_loop().create_future()

    async def connect(self) -> None:
        self.connect_count += 1
        if self.connect_error is not None:
            raise self.connect_error

    async def disconnect(self) -> None:
        self.disconnect_count += 1
        if not self.disconnected.done():
            self.disconnected.set_result(None)

    async def is_user_authorized(self) -> bool:
        return self.authorized

    async def send_code_request(self, phone: str) -> SimpleNamespace:
        return SimpleNamespace(phone_code_hash=f"hash-{phone}")


class HandlerAwareClient(FakeClient):
    def __init__(self, *, fail_catch_up_count: int = 0) -> None:
        super().__init__(authorized=True)
        self.handlers: list[object] = []
        self.handlers_registered_before_connect = False
        self.catch_up_count = 0
        self.fail_catch_up_count = fail_catch_up_count

    def on(self, event: object):
        def register(handler: object) -> object:
            self.handlers.append(handler)
            return handler

        return register

    async def connect(self) -> None:
        self.handlers_registered_before_connect = len(self.handlers) == 4
        await super().connect()

    async def catch_up(self) -> None:
        self.catch_up_count += 1
        if self.fail_catch_up_count > 0:
            self.fail_catch_up_count -= 1
            raise RuntimeError("temporary catch-up failure")


class TelegramRuntimeManagerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = ArchiveStore(self.root / "archive.db")
        self.store.initialize()
        self.account = TelegramAccountConfig(
            account_id="main",
            api_id=1,
            api_hash="hash",
            session_name="main",
            session_dir=self.root / "sessions",
        )
        self.config = AppConfig(
            storage=StorageConfig(data_dir=self.root, database=self.root / "archive.db"),
            telegram=TelegramConfig(accounts=[self.account]),
            server=ServerConfig(token="secret"),
            logging=LoggingConfig(file=None),
            workspace_dir=self.root,
        )

    async def asyncTearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    async def test_one_client_is_reused_for_account_commands(self) -> None:
        clients: list[FakeClient] = []

        def factory(config: TelegramAccountConfig) -> FakeClient:
            client = FakeClient(authorized=False)
            clients.append(client)
            return client

        manager = TelegramRuntimeManager(self.config, self.store, client_factory=factory)
        await manager.start()
        try:
            first = await manager.execute("main", "auth_status")
            second = await manager.execute("main", "request_code", phone="+10000000000")

            self.assertFalse(first["authorized"])
            self.assertEqual(second["auth_state"], "code_sent")
            self.assertEqual(len(clients), 1)
            self.assertEqual(clients[0].disconnect_count, 0)
        finally:
            await manager.stop()

        self.assertGreaterEqual(clients[0].disconnect_count, 1)

    async def test_completed_future_wins_race_with_cancel_event(self) -> None:
        manager = TelegramRuntimeManager(self.config, self.store)
        completed: Future[dict[str, object]] = Future()
        completed.set_result({"status": "sent"})
        cancel_event = threading.Event()
        cancel_event.set()

        result = manager._wait_for_future(completed, cancel_event=cancel_event)

        self.assertEqual(result, {"status": "sent"})
        self.assertFalse(completed.cancelled())

    async def test_one_account_connection_failure_does_not_stop_other_account(self) -> None:
        alt = TelegramAccountConfig(
            account_id="alt",
            api_id=1,
            api_hash="hash",
            session_name="alt",
            session_dir=self.root / "sessions",
        )
        config = AppConfig(
            storage=self.config.storage,
            telegram=TelegramConfig(accounts=[self.account, alt]),
            server=self.config.server,
            logging=self.config.logging,
        )

        def factory(account: TelegramAccountConfig) -> FakeClient:
            if account.account_id == "alt":
                return FakeClient(connect_error=RuntimeError("alt offline"))
            return FakeClient(authorized=False)

        manager = TelegramRuntimeManager(config, self.store, client_factory=factory)
        await manager.start()
        try:
            result = await asyncio.wait_for(manager.execute("main", "auth_status"), timeout=1)
            await asyncio.sleep(0)
            self.assertFalse(result["authorized"])
            statuses = {item["account_id"]: item for item in manager.statuses()}
            self.assertTrue(statuses["main"]["supervisor_running"])
            self.assertTrue(statuses["alt"]["supervisor_running"])
        finally:
            await manager.stop()

    async def test_runtime_reconnects_when_failure_persistence_also_fails(self) -> None:
        clients: list[FakeClient] = []

        def factory(config: TelegramAccountConfig) -> FakeClient:
            client = FakeClient(authorized=False)
            if not clients:
                client.disconnected.set_result(None)
            clients.append(client)
            return client

        original_upsert = self.store.upsert_account_auth

        def fail_error_auth(record: AccountAuthRecord, **kwargs: object) -> bool:
            if record.auth_state == "error":
                raise sqlite3.OperationalError("database or disk is full")
            return original_upsert(record, **kwargs)  # type: ignore[arg-type]

        manager = TelegramRuntimeManager(self.config, self.store, client_factory=factory)
        with (
            patch("tele_mess_core.telegram.manager.CONNECTION_RETRY_INITIAL_SECONDS", 0.001),
            patch.object(self.store, "upsert_account_auth", side_effect=fail_error_auth),
            patch.object(
                self.store,
                "add_operation_event",
                side_effect=sqlite3.OperationalError("database or disk is full"),
            ),
        ):
            await manager.start()
            try:
                async def wait_until_reconnected() -> None:
                    while len(clients) < 2 or not manager.statuses()[0]["supervisor_running"]:
                        await asyncio.sleep(0.001)

                await asyncio.wait_for(wait_until_reconnected(), timeout=1)
                self.assertGreaterEqual(len(clients), 2)
                self.assertTrue(manager.statuses()[0]["supervisor_running"])
            finally:
                await manager.stop()

    def test_thread_bridge_timeout_and_cancel_errors_are_nonempty(self) -> None:
        manager = TelegramRuntimeManager(self.config, self.store, call_timeout=0.001)
        timed_out: Future[object] = Future()

        with self.assertRaisesRegex(TimeoutError, "deliver_chunk timed out after"):
            manager._wait_for_future(timed_out, operation="deliver_chunk")
        self.assertTrue(timed_out.cancelled())

        canceled: Future[object] = Future()
        cancel_event = threading.Event()
        cancel_event.set()
        with self.assertRaisesRegex(RuntimeError, "deliver_chunk canceled while waiting"):
            manager._wait_for_future(
                canceled,
                operation="deliver_chunk",
                cancel_event=cancel_event,
            )
        self.assertTrue(canceled.cancelled())

    async def test_threaded_register_and_unregister_updates_runtime(self) -> None:
        manager = TelegramRuntimeManager(
            self.config,
            self.store,
            client_factory=lambda config: FakeClient(authorized=False),
        )
        await manager.start()
        second = TelegramAccountConfig(
            account_id="second",
            api_id=1,
            api_hash="hash",
            session_name="second",
            session_dir=self.root / "sessions",
        )
        try:
            registered = await asyncio.to_thread(manager.register_account, second)
            self.assertEqual(registered["account_id"], "second")
            stopped = await asyncio.to_thread(manager.unregister_account, "second")
            self.assertTrue(stopped["stopped"])
            self.assertNotIn("second", {item["account_id"] for item in manager.statuses()})
        finally:
            await manager.stop()

    async def test_completed_attach_task_does_not_register_ingestion_twice(self) -> None:
        instances: list[object] = []

        class FakeIngestion:
            def __init__(self, *args: object, **kwargs: object) -> None:
                self.attach_count = 0
                self.refresh_count = 0
                instances.append(self)

            async def attach(self, client: FakeClient) -> None:
                self.attach_count += 1

            async def refresh_capture_targets(self) -> None:
                self.refresh_count += 1

        manager = TelegramRuntimeManager(
            self.config,
            self.store,
            client_factory=lambda config: FakeClient(authorized=True),
        )
        with patch("tele_mess_core.telegram.manager.TelegramArchiveService", FakeIngestion):
            await manager.start()
            try:
                await manager.execute("main", "auth_status")
                await asyncio.sleep(0)
                await manager.execute("main", "auth_status")
                await asyncio.sleep(0)

                self.assertEqual(len(instances), 1)
                self.assertEqual(instances[0].attach_count, 1)
                self.assertTrue(manager.statuses()[0]["ingest_running"])
            finally:
                await manager.stop()

    async def test_update_handlers_are_registered_before_connect_and_catch_up(self) -> None:
        client = HandlerAwareClient()
        manager = TelegramRuntimeManager(
            self.config,
            self.store,
            client_factory=lambda config: client,
        )

        await manager.start()
        try:
            await asyncio.wait_for(manager.execute("main", "auth_status"), timeout=1)

            async def wait_until_active() -> None:
                while not manager.statuses()[0]["ingest_running"]:
                    await asyncio.sleep(0)

            await asyncio.wait_for(wait_until_active(), timeout=1)

            self.assertTrue(client.handlers_registered_before_connect)
            self.assertEqual(len(client.handlers), 4)
            self.assertEqual(client.catch_up_count, 1)
            self.assertTrue(manager.statuses()[0]["ingest_running"])
        finally:
            await manager.stop()

    async def test_failed_activation_reuses_preconnected_handlers_on_retry(self) -> None:
        client = HandlerAwareClient(fail_catch_up_count=1)
        with patch("tele_mess_core.telegram.manager.INGEST_RETRY_INITIAL_SECONDS", 0.001):
            manager = TelegramRuntimeManager(
                self.config,
                self.store,
                client_factory=lambda config: client,
            )

            await manager.start()
            try:
                async def wait_until_recovered() -> None:
                    while client.catch_up_count < 2 or not manager.statuses()[0]["ingest_running"]:
                        await asyncio.sleep(0.001)

                await asyncio.wait_for(wait_until_recovered(), timeout=1)

                self.assertEqual(client.catch_up_count, 2)
                self.assertEqual(len(client.handlers), 4)
                self.assertTrue(manager.statuses()[0]["ingest_running"])
            finally:
                await manager.stop()

    def test_default_client_enables_telethon_update_recovery(self) -> None:
        sentinel = object()
        with patch("telethon.TelegramClient", return_value=sentinel) as constructor:
            client = _default_client_factory(self.account)

        self.assertIs(client, sentinel)
        constructor.assert_called_once_with(
            str(self.account.session_dir / self.account.session_name),
            self.account.api_id,
            self.account.api_hash,
            catch_up=True,
            sequential_updates=True,
        )

    async def test_stored_relative_session_dir_is_anchored_to_workspace(self) -> None:
        manager_account = {
            "account_id": "saved",
            "session_name": "saved-main",
            "session_dir": "managed-sessions",
        }

        resolved = stored_account_config(self.config, manager_account)

        self.assertEqual(resolved.account_id, "saved")
        self.assertEqual(resolved.session_dir, self.root / "managed-sessions")

    async def test_start_only_registers_telegram_management_accounts(self) -> None:
        self.store.upsert_account(
            AccountRecord(source="other", account_id="elsewhere", display_name="Other", kind="other")
        )
        self.store.upsert_account(
            AccountRecord(source=SOURCE_TELEGRAM, account_id="saved", display_name="Saved", kind="telegram")
        )
        manager = TelegramRuntimeManager(self.config, self.store, client_factory=lambda item: FakeClient())

        await manager.start()
        try:
            self.assertEqual(sorted(manager._runtimes), ["main", "saved"])
        finally:
            await manager.stop()

    async def test_ingest_false_never_attaches_handlers_or_ingestion(self) -> None:
        clients: list[HandlerAwareClient] = []

        def factory(config: TelegramAccountConfig) -> HandlerAwareClient:
            client = HandlerAwareClient()
            clients.append(client)
            return client

        manager = TelegramRuntimeManager(self.config, self.store, client_factory=factory, ingest=False)
        await manager.start()
        try:
            status = await manager.execute("main", "auth_status")
            self.assertTrue(status["authorized"])
            refresh = await manager.execute("main", "refresh_capture")
            self.assertEqual(refresh["status"], "ingest_disabled")
            self.assertEqual(clients[0].handlers, [])
            self.assertEqual(clients[0].catch_up_count, 0)
        finally:
            await manager.stop()

    async def test_offline_execute_returns_structured_errors_and_recovers_when_online(self) -> None:
        from tele_mess_core.config import DailyDeliveryConfig
        from tele_mess_core.telegram.runtime import TelegramOperationError

        online = False

        def factory(config: TelegramAccountConfig) -> FakeClient:
            if not online:
                return FakeClient(connect_error=ConnectionError("offline"))
            return FakeClient(authorized=True)

        manager = TelegramRuntimeManager(
            self.config,
            self.store,
            client_factory=factory,
            ingest=False,
            connect_wait_seconds=1.0,
        )
        await manager.start()
        try:
            auth = await manager.execute("main", "auth_status")
            self.assertFalse(auth["authorized"])
            self.assertEqual(auth["error"]["code"], "auth_status_failed")

            discover = await manager.execute("main", "discover_origins")
            self.assertFalse(discover["authorized"])
            self.assertEqual(discover["status"], "failed")
            self.assertEqual(discover["errors"][0]["code"], "discover_origins_failed")

            with self.assertRaises(TelegramOperationError):
                await manager.execute(
                    "main",
                    "deliver_chunk",
                    delivery=DailyDeliveryConfig(enabled=True, account_id="main", origin_id=-9001),
                    content="hello",
                )

            # Once connectivity returns, the next call wakes the supervisor and succeeds immediately.
            online = True
            recovered = await manager.execute("main", "auth_status")
            self.assertTrue(recovered["authorized"])
            self.assertEqual(recovered["auth_state"], "authorized")
        finally:
            await manager.stop()

    async def test_queued_management_timeout_does_not_disconnect_active_delivery(self) -> None:
        from tele_mess_core.config import DailyDeliveryConfig

        send_started = asyncio.Event()
        release_send = asyncio.Event()
        disconnects = 0

        class SlowSendClient(FakeClient):
            async def get_entity(self, origin_id: int) -> str:
                return f"entity:{origin_id}"

            async def send_message(self, entity: object, body: str, **kwargs: object) -> object:
                send_started.set()
                await release_send.wait()

                class Sent:
                    id = 88

                return Sent()

            async def disconnect(self) -> None:
                nonlocal disconnects
                disconnects += 1
                await super().disconnect()

        client = SlowSendClient(authorized=True)
        manager = TelegramRuntimeManager(
            self.config,
            self.store,
            client_factory=lambda _cfg: client,
            ingest=False,
            connect_wait_seconds=1.0,
            management_timeout_seconds=0.1,
            delivery_timeout_seconds=2.0,
        )
        await manager.start()
        try:
            delivery = DailyDeliveryConfig(enabled=True, account_id="main", origin_id=-9001)
            delivery_task = asyncio.create_task(
                manager.execute("main", "deliver_chunk", delivery=delivery, content="hello")
            )
            await asyncio.wait_for(send_started.wait(), timeout=2.0)

            # Management op times out waiting on _command_lock, NOT on the client itself.
            auth_result = await manager.execute("main", "auth_status")
            self.assertFalse(auth_result["authorized"])
            self.assertEqual(auth_result["error"]["code"], "auth_status_failed")
            self.assertEqual(disconnects, 0)
            self.assertTrue(manager.statuses()[0]["connected"])

            # In-flight delivery completes cleanly on the uninterrupted client.
            release_send.set()
            delivered = await asyncio.wait_for(delivery_task, timeout=2.0)
            self.assertEqual(delivered["status"], "sent")
            self.assertEqual(delivered["message_ids"], [88])
        finally:
            await manager.stop()

    async def test_late_offline_diagnostic_does_not_overwrite_recovered_auth_state(self) -> None:
        import threading

        online = {"value": False}
        diag_entered = threading.Event()
        release_diag = threading.Event()
        original_add_event = self.store.add_operation_event

        def gated_add_event(record: object) -> None:
            if getattr(record, "operation", None) == "auth_status":
                diag_entered.set()
                release_diag.wait(timeout=3.0)
            original_add_event(record)  # type: ignore[arg-type]

        class ToggleClient(FakeClient):
            async def connect(self) -> None:
                if not online["value"]:
                    raise ConnectionError("offline")
                await super().connect()

        manager = TelegramRuntimeManager(
            self.config,
            self.store,
            client_factory=lambda _cfg: ToggleClient(authorized=False),
            ingest=False,
            connect_wait_seconds=0.05,
        )
        with patch.object(self.store, "add_operation_event", side_effect=gated_add_event):
            await manager.start()
            try:
                offline_result = await manager.execute("main", "auth_status")
                self.assertEqual(offline_result["auth_state"], "auth_failed")
                self.assertTrue(diag_entered.wait(2.0))

                # Network returns and request_code writes code_sent while old offline diagnostic is still paused.
                online["value"] = True
                sent = await manager.execute("main", "request_code", phone="+1000")
                self.assertEqual(sent["auth_state"], "code_sent")

                # Release the paused offline diagnostic and verify code_sent is NOT overwritten.
                release_diag.set()
            finally:
                release_diag.set()
                await manager.stop()

        accounts = {item["account_id"]: item for item in self.store.list_management_accounts()}
        self.assertEqual(accounts["main"]["auth_state"], "code_sent")

    async def test_stale_supervisor_probe_and_delayed_write_cannot_overwrite_code_sent(self) -> None:
        probe_entered = asyncio.Event()
        release_probe = asyncio.Event()

        class SlowProbeClient(FakeClient):
            async def is_user_authorized(self) -> bool:
                probe_entered.set()
                await release_probe.wait()
                return False

        manager = TelegramRuntimeManager(
            self.config,
            self.store,
            client_factory=lambda _cfg: SlowProbeClient(authorized=False),
            ingest=False,
            connect_wait_seconds=1.0,
            management_timeout_seconds=2.0,
        )
        await manager.start()
        try:
            await asyncio.wait_for(probe_entered.wait(), timeout=2.0)
            runtime = manager._runtimes["main"]
            stale_epoch = runtime._auth_state_epoch

            # Interactive request_code completes and writes code_sent while supervisor probe is still running.
            sent = await manager.execute("main", "request_code", phone="+1000")
            self.assertEqual(sent["auth_state"], "code_sent")

            # Now let the earlier supervisor probe finish AND also directly attempt a delayed DB write with stale_epoch.
            release_probe.set()
            await asyncio.sleep(0.05)
            wrote = self.store.upsert_account_auth(
                AccountAuthRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    auth_state="error",
                    last_error="stale error",
                ),
                monotonic_seq=stale_epoch,
            )
            self.assertFalse(wrote)
        finally:
            release_probe.set()
            await manager.stop()

        accounts = {item["account_id"]: item for item in self.store.list_management_accounts()}
        self.assertEqual(accounts["main"]["auth_state"], "code_sent")

    async def test_stale_supervisor_probe_failure_cannot_overwrite_code_sent(self) -> None:
        probe_entered = asyncio.Event()
        release_probe = asyncio.Event()

        class FailingSlowProbeClient(FakeClient):
            async def is_user_authorized(self) -> bool:
                probe_entered.set()
                await release_probe.wait()
                raise ConnectionError("stale probe disconnect")

        manager = TelegramRuntimeManager(
            self.config,
            self.store,
            client_factory=lambda _cfg: FailingSlowProbeClient(authorized=False),
            ingest=False,
            connect_wait_seconds=1.0,
            management_timeout_seconds=2.0,
        )
        await manager.start()
        try:
            await asyncio.wait_for(probe_entered.wait(), timeout=2.0)
            sent = await manager.execute("main", "request_code", phone="+1000")
            self.assertEqual(sent["auth_state"], "code_sent")
            release_probe.set()
            await asyncio.sleep(0.08)
        finally:
            release_probe.set()
            await manager.stop()

        accounts = {item["account_id"]: item for item in self.store.list_management_accounts()}
        self.assertEqual(accounts["main"]["auth_state"], "code_sent")

    async def test_auth_seq_survives_restart_and_clock_rollback(self) -> None:
        # Simulate a prior process that persisted a very high auth_state_seq (e.g. before clock rollback).
        self.store.upsert_account_auth(
            AccountAuthRecord(
                source=SOURCE_TELEGRAM,
                account_id="main",
                auth_state="needs_login",
            ),
            monotonic_seq=10000000000000000000,
        )

        manager = TelegramRuntimeManager(
            self.config,
            self.store,
            client_factory=lambda _cfg: FakeClient(authorized=False),
            ingest=False,
            connect_wait_seconds=1.0,
            management_timeout_seconds=2.0,
        )
        await manager.start()
        try:
            sent = await manager.execute("main", "request_code", phone="+1000")
            self.assertEqual(sent["auth_state"], "code_sent")
        finally:
            await manager.stop()

        accounts = {item["account_id"]: item for item in self.store.list_management_accounts()}
        self.assertEqual(accounts["main"]["auth_state"], "code_sent")
        self.assertGreater(self.store.get_auth_state_seq(SOURCE_TELEGRAM, "main"), 10000000000000000000)

    async def test_auth_rpc_error_persister_callbacks_run_without_name_error(self) -> None:
        from tele_mess_core.telegram.auth import TelegramAuthService

        queued: list[object] = []

        class SessionPasswordNeededError(Exception):
            pass

        class FailingAuthClient:
            async def is_user_authorized(self) -> bool:
                raise ConnectionError("status offline")

            async def send_code_request(self, phone: str) -> object:
                raise ConnectionError("request offline")

            async def sign_in(self, **kwargs: object) -> None:
                if "password" in kwargs:
                    raise ConnectionError("password offline")
                if kwargs.get("code") == "need-pw":
                    raise SessionPasswordNeededError("2fa required")
                raise ConnectionError("submit offline")

        service = TelegramAuthService(
            self.account,
            self.store,
            error_persister=lambda fn: queued.append(fn),
        )
        client = FailingAuthClient()

        r1 = await service.status(client=client)
        r2 = await service.request_code("+1000", client=client)
        r3 = await service.submit_code("+1000", "12345", client=client)
        r4 = await service.submit_code("+1000", "need-pw", password="pw", client=client)

        self.assertEqual(r1["error"]["code"], "auth_status_failed")
        self.assertEqual(r2["error"]["code"], "request_code_failed")
        self.assertEqual(r3["error"]["code"], "submit_code_failed")
        self.assertEqual(r4["error"]["code"], "submit_code_failed")
        self.assertEqual(len(queued), 4)

        # Executing the deferred callbacks after all except blocks have exited must not raise NameError.
        for callback in queued:
            assert callable(callback)
            callback()

        events = self.store.list_operation_events(account_id="main", limit=10)
        ops = [event["operation"] for event in events]
        self.assertEqual(ops.count("auth_status"), 1)
        self.assertEqual(ops.count("request_code"), 1)
        self.assertEqual(ops.count("submit_code"), 2)


class ConfiguredAccountSyncTest(unittest.TestCase):
    def test_bootstrap_preserves_observed_auth_state(self) -> None:
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
            store.upsert_account_auth(
                AccountAuthRecord(
                    source=SOURCE_TELEGRAM,
                    account_id="main",
                    auth_state="authorized",
                    last_error="keep-me",
                )
            )

            _sync_configured_accounts(store, config)

            item = store.list_management_accounts()[0]
            self.assertEqual(item["auth_state"], "authorized")
            self.assertEqual(item["last_error"], "keep-me")
            store.close()


if __name__ == "__main__":
    unittest.main()
