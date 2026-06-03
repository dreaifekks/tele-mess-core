from __future__ import annotations

import json
import logging
from typing import Any

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import TelegramAccountConfig
from tele_mess_core.models import AccountAuthRecord, AccountRecord, SOURCE_TELEGRAM, utc_now_iso


class TelegramAuthService:
    def __init__(self, config: TelegramAccountConfig, store: ArchiveStore):
        self.config = config
        self.account_id = config.account_id
        self.store = store
        self.logger = logging.getLogger(__name__)

    async def status(self) -> dict[str, Any]:
        client = await self._connected_client()
        try:
            authorized = await client.is_user_authorized()
            state = "authorized" if authorized else "needs_login"
            self._record_state(state)
            return {"account_id": self.account_id, "auth_state": state, "authorized": authorized}
        finally:
            await client.disconnect()

    async def request_code(self, phone: str) -> dict[str, Any]:
        client = await self._connected_client()
        try:
            result = await client.send_code_request(phone)
            phone_code_hash = getattr(result, "phone_code_hash", None)
            if phone_code_hash:
                self.store.set_meta(_phone_code_hash_key(self.account_id), str(phone_code_hash))
            self._record_state(
                "code_sent",
                phone=phone,
                raw={"phone": phone, "phone_code_hash_set": bool(phone_code_hash)},
            )
            return {"account_id": self.account_id, "auth_state": "code_sent", "phone": phone}
        except Exception as exc:
            self._record_state("auth_failed", phone=phone, last_error=str(exc))
            raise
        finally:
            await client.disconnect()

    async def submit_code(self, phone: str, code: str, password: str | None = None) -> dict[str, Any]:
        client = await self._connected_client()
        try:
            phone_code_hash = self.store.get_meta(_phone_code_hash_key(self.account_id))
            try:
                if phone_code_hash:
                    await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
                else:
                    await client.sign_in(phone=phone, code=code)
            except Exception as exc:
                if _is_password_needed(exc):
                    if not password:
                        self._record_state("password_needed", phone=phone)
                        return {"account_id": self.account_id, "auth_state": "password_needed", "authorized": False}
                    await client.sign_in(password=password)
                else:
                    self._record_state("auth_failed", phone=phone, last_error=str(exc))
                    raise

            self.store.set_meta(_phone_code_hash_key(self.account_id), "")
            self._record_state("authorized", phone=phone)
            return {"account_id": self.account_id, "auth_state": "authorized", "authorized": True}
        finally:
            await client.disconnect()

    async def _connected_client(self) -> Any:
        from telethon import TelegramClient

        self.config.session_dir.mkdir(parents=True, exist_ok=True)
        session_file = self.config.session_dir / self.config.session_name
        client = TelegramClient(str(session_file), self.config.api_id, self.config.api_hash)
        await client.connect()
        return client

    def _record_state(
        self,
        state: str,
        phone: str | None = None,
        last_error: str | None = None,
        raw: dict[str, Any] | None = None,
    ) -> None:
        now = utc_now_iso()
        self.store.upsert_account(
            AccountRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                display_name=self.account_id,
                kind="telegram",
                updated_at=now,
            )
        )
        self.store.upsert_account_auth(
            AccountAuthRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                auth_state=state,
                phone=phone,
                session_name=self.config.session_name,
                session_dir=str(self.config.session_dir),
                last_error=last_error,
                updated_at=now,
                raw_json=json.dumps(raw or {"auth_state": state}, ensure_ascii=False),
            )
        )


def _phone_code_hash_key(account_id: str) -> str:
    return f"telegram_auth:{account_id}:phone_code_hash"


def _is_password_needed(exc: Exception) -> bool:
    if exc.__class__.__name__ == "SessionPasswordNeededError":
        return True
    return "password" in str(exc).lower() and "needed" in str(exc).lower()
