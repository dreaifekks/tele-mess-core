from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import TelegramAccountConfig
from tele_mess_core.telegram.auth import TelegramAuthService


class PasswordNeeded(Exception):
    pass


class FakeClient:
    def __init__(self):
        self.authorized = False
        self.disconnected = False
        self.password_needed = False
        self.sign_in_calls = []

    async def connect(self):
        return None

    async def disconnect(self):
        self.disconnected = True

    async def is_user_authorized(self):
        return self.authorized

    async def send_code_request(self, phone):
        return SimpleNamespace(phone_code_hash="hash123")

    async def sign_in(self, **kwargs):
        self.sign_in_calls.append(kwargs)
        if "password" in kwargs:
            self.authorized = True
            return None
        if self.password_needed:
            raise PasswordNeeded("password needed")
        self.authorized = True
        return None


class FakeAuthService(TelegramAuthService):
    def __init__(self, config, store, client):
        super().__init__(config, store)
        self.client = client

    async def _connected_client(self):
        return self.client


class TelegramAuthTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = ArchiveStore(Path(self.tmp.name) / "archive.db")
        self.store.initialize()
        self.config = TelegramAccountConfig(
            account_id="main",
            api_id=1,
            api_hash="hash",
            session_name="main",
            chats=[],
        )
        self.client = FakeClient()
        self.service = FakeAuthService(self.config, self.store, self.client)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    async def test_request_and_submit_code_updates_auth_state(self) -> None:
        requested = await self.service.request_code("+10000000000")
        self.assertEqual(requested["auth_state"], "code_sent")
        accounts = self.store.list_management_accounts()
        self.assertEqual(accounts[0]["auth_state"], "code_sent")
        self.assertEqual(self.store.get_meta("telegram_auth:main:phone_code_hash"), "hash123")

        submitted = await self.service.submit_code("+10000000000", "12345")
        self.assertTrue(submitted["authorized"])
        accounts = self.store.list_management_accounts()
        self.assertEqual(accounts[0]["auth_state"], "authorized")
        self.assertEqual(self.store.get_meta("telegram_auth:main:phone_code_hash"), "")

    async def test_submit_code_reports_password_needed_and_accepts_password(self) -> None:
        await self.service.request_code("+10000000000")
        self.client.password_needed = True
        result = await self.service.submit_code("+10000000000", "12345")
        self.assertEqual(result["auth_state"], "password_needed")

        self.client.password_needed = True
        result = await self.service.submit_code("+10000000000", "12345", password="secret")
        self.assertTrue(result["authorized"])
        self.assertTrue(any("password" in call for call in self.client.sign_in_calls))


if __name__ == "__main__":
    unittest.main()
