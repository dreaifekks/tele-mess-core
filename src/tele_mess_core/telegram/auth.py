from __future__ import annotations

import json
import logging
from typing import Any

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import TelegramAccountConfig
from tele_mess_core.models import AccountAuthRecord, AccountRecord, OperationEventRecord, SOURCE_TELEGRAM, utc_now_iso
from tele_mess_core.telegram.runtime import classify_telegram_exception


class TelegramAuthService:
    def __init__(self, config: TelegramAccountConfig, store: ArchiveStore):
        self.config = config
        self.account_id = config.account_id
        self.store = store
        self.logger = logging.getLogger(__name__)

    async def status(self, client: Any | None = None) -> dict[str, Any]:
        owns_client = client is None
        try:
            if client is None:
                client = await self._connected_client()
            authorized = await client.is_user_authorized()
            state = "authorized" if authorized else "needs_login"
            self._record_state(state)
            return {"account_id": self.account_id, "auth_state": state, "authorized": authorized}
        except Exception as exc:
            error = classify_telegram_exception(exc, default_code="auth_status_failed")
            self._record_state(error.auth_state, last_error=error.message)
            self._record_operation("auth_status", "failed", error=error)
            return {
                "account_id": self.account_id,
                "auth_state": error.auth_state,
                "authorized": False,
                "error": error.to_public_dict(),
            }
        finally:
            if owns_client and client is not None:
                await client.disconnect()

    async def request_code(self, phone: str, client: Any | None = None) -> dict[str, Any]:
        owns_client = client is None
        try:
            if client is None:
                client = await self._connected_client()
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
            error = classify_telegram_exception(exc, default_code="request_code_failed")
            self._record_state(error.auth_state, phone=phone, last_error=error.message)
            self._record_operation("request_code", "rate_limited" if error.code == "flood_wait" else "failed", error=error)
            return {
                "account_id": self.account_id,
                "auth_state": error.auth_state,
                "phone": phone,
                "sent": False,
                "error": error.to_public_dict(),
            }
        finally:
            if owns_client and client is not None:
                await client.disconnect()

    async def submit_code(
        self,
        phone: str,
        code: str,
        password: str | None = None,
        client: Any | None = None,
    ) -> dict[str, Any]:
        owns_client = client is None
        try:
            if client is None:
                client = await self._connected_client()
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
                        self._record_operation("submit_code", "password_needed")
                        return {"account_id": self.account_id, "auth_state": "password_needed", "authorized": False}
                    try:
                        await client.sign_in(password=password)
                    except Exception as password_exc:
                        error = classify_telegram_exception(password_exc, default_code="submit_code_failed")
                        self._record_state(error.auth_state, phone=phone, last_error=error.message)
                        self._record_operation(
                            "submit_code",
                            "rate_limited" if error.code == "flood_wait" else "failed",
                            error=error,
                        )
                        return {
                            "account_id": self.account_id,
                            "auth_state": error.auth_state,
                            "authorized": False,
                            "error": error.to_public_dict(),
                        }
                else:
                    error = classify_telegram_exception(exc, default_code="submit_code_failed")
                    self._record_state(error.auth_state, phone=phone, last_error=error.message)
                    self._record_operation(
                        "submit_code",
                        "rate_limited" if error.code == "flood_wait" else "failed",
                        error=error,
                    )
                    return {
                        "account_id": self.account_id,
                        "auth_state": error.auth_state,
                        "authorized": False,
                        "error": error.to_public_dict(),
                    }

            self.store.set_meta(_phone_code_hash_key(self.account_id), "")
            self._record_state("authorized", phone=phone)
            return {"account_id": self.account_id, "auth_state": "authorized", "authorized": True}
        finally:
            if owns_client and client is not None:
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

    def _record_operation(self, operation: str, status: str, error: Any | None = None) -> None:
        self.store.add_operation_event(
            OperationEventRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                operation=operation,
                status=status,
                error_code=getattr(error, "code", None),
                message=getattr(error, "message", None),
                retry_after=getattr(error, "retry_after", None),
                occurred_at=utc_now_iso(),
                raw_json=json.dumps(error.to_public_dict(), ensure_ascii=False) if error else None,
            )
        )


def _phone_code_hash_key(account_id: str) -> str:
    return f"telegram_auth:{account_id}:phone_code_hash"


def _is_password_needed(exc: Exception) -> bool:
    if exc.__class__.__name__ == "SessionPasswordNeededError":
        return True
    return "password" in str(exc).lower() and "needed" in str(exc).lower()
