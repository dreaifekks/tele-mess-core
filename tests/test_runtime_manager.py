from __future__ import annotations

import asyncio
import tempfile
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
)
from tele_mess_core.models import AccountAuthRecord, SOURCE_TELEGRAM
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
        manager = TelegramRuntimeManager(self.config, self.store, client_factory=lambda item: FakeClient())

        resolved = manager._stored_account_config(manager_account)

        self.assertEqual(resolved.account_id, "saved")
        self.assertEqual(resolved.session_dir, self.root / "managed-sessions")


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
