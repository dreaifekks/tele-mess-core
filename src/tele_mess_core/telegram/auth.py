from __future__ import annotations

import json
import logging
from typing import Any, Awaitable, Callable

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import TelegramAccountConfig
from tele_mess_core.models import AccountAuthRecord, AccountRecord, OperationEventRecord, SOURCE_TELEGRAM, utc_now_iso
from tele_mess_core.telegram.runtime import classify_telegram_exception


class TelegramAuthService:
    def __init__(
        self,
        config: TelegramAccountConfig,
        store: ArchiveStore,
        *,
        error_persister: Callable[[Callable[[], Any]], None] | None = None,
        auth_probe_provider: Callable[[], tuple[int, str, int]] | None = None,
        auth_owner_provider: Callable[[], tuple[str, int]] | None = None,
        store_write_runner: Callable[[Callable[[], Any]], Awaitable[Any]] | None = None,
    ):
        self.config = config
        self.account_id = config.account_id
        self.store = store
        self._error_persister = error_persister
        self._auth_probe_provider = auth_probe_provider
        self._auth_owner_provider = auth_owner_provider
        self._store_write_runner = store_write_runner
        self.logger = logging.getLogger(__name__)

    async def _run_write(self, writer: Callable[[], Any]) -> Any:
        if self._store_write_runner is not None:
            return await self._store_write_runner(writer)
        return writer()

    async def status(self, client: Any) -> dict[str, Any]:
        probe = self._auth_probe_provider() if self._auth_probe_provider is not None else None
        try:
            authorized = await client.is_user_authorized()
            state = "authorized" if authorized else "needs_login"
            await self._run_write(lambda st=state, pr=probe: self._record_state(st, probe=pr))
            return {"account_id": self.account_id, "auth_state": state, "authorized": authorized}
        except Exception as exc:
            return self._handle_rpc_error(
                lambda persist, persist_auth, err=exc: self.status_error_result(
                    err,
                    persist=persist,
                    persist_auth_state=persist_auth,
                )
            )

    def status_error_result(
        self,
        exc: Exception,
        *,
        persist: bool = True,
        persist_auth_state: bool = True,
    ) -> dict[str, Any]:
        error = classify_telegram_exception(exc, default_code="auth_status_failed")
        result = {
            "account_id": self.account_id,
            "auth_state": error.auth_state,
            "authorized": False,
            "error": error.to_public_dict(),
        }
        if persist:
            self._persist_error_diagnostic(
                "auth_status",
                "failed",
                error,
                persist_auth_state=persist_auth_state,
            )
        return result

    async def request_code(self, phone: str, client: Any) -> dict[str, Any]:
        try:
            result = await client.send_code_request(phone)
            phone_code_hash = getattr(result, "phone_code_hash", None)
            # Capture ordering token BEFORE scheduling the off-loop write so a timed-out
            # earlier write cannot overwrite a newer request_code's hash or auth state.
            probe = self._auth_probe_provider() if self._auth_probe_provider is not None else None
            extra_meta = (
                {_phone_code_hash_key(self.account_id): str(phone_code_hash)}
                if phone_code_hash
                else None
            )

            def persist_sent() -> bool:
                return self._record_state(
                    "code_sent",
                    phone=phone,
                    raw={"phone": phone, "phone_code_hash_set": bool(phone_code_hash)},
                    probe=probe,
                    extra_meta=extra_meta,
                )

            wrote = await self._run_write(persist_sent)
            if not wrote:
                return self._conflict_result("request_code", phone=phone)
            return {"account_id": self.account_id, "auth_state": "code_sent", "phone": phone}
        except Exception as exc:
            return self._handle_rpc_error(
                lambda persist, persist_auth, p=phone, err=exc: self.request_code_error_result(
                    p,
                    err,
                    persist=persist,
                    persist_auth_state=persist_auth,
                )
            )

    def request_code_error_result(
        self,
        phone: str,
        exc: Exception,
        *,
        persist: bool = True,
        persist_auth_state: bool = True,
    ) -> dict[str, Any]:
        error = classify_telegram_exception(exc, default_code="request_code_failed")
        result = {
            "account_id": self.account_id,
            "auth_state": error.auth_state,
            "phone": phone,
            "sent": False,
            "error": error.to_public_dict(),
        }
        if persist:
            self._persist_error_diagnostic(
                "request_code",
                "rate_limited" if error.code == "flood_wait" else "failed",
                error,
                phone=phone,
                persist_auth_state=persist_auth_state,
            )
        return result

    async def submit_code(
        self,
        phone: str,
        code: str,
        password: str | None = None,
        *,
        client: Any,
    ) -> dict[str, Any]:
        phone_code_hash = self.store.get_meta(_phone_code_hash_key(self.account_id))
        try:
            if phone_code_hash:
                await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
            else:
                await client.sign_in(phone=phone, code=code)
        except Exception as exc:
            if _is_password_needed(exc):
                if not password:
                    probe = self._auth_probe_provider() if self._auth_probe_provider is not None else None

                    def persist_password_needed() -> bool:
                        wrote = self._record_state("password_needed", phone=phone, probe=probe)
                        if wrote:
                            self._record_operation("submit_code", "password_needed")
                        return wrote

                    wrote = await self._run_write(persist_password_needed)
                    if not wrote:
                        return self._conflict_result("submit_code", phone=phone)
                    return {"account_id": self.account_id, "auth_state": "password_needed", "authorized": False}
                try:
                    await client.sign_in(password=password)
                except Exception as password_exc:
                    return self._handle_rpc_error(
                        lambda persist, persist_auth, p=phone, err=password_exc: self.submit_code_error_result(
                            p,
                            err,
                            persist=persist,
                            persist_auth_state=persist_auth,
                        )
                    )
            else:
                return self._handle_rpc_error(
                    lambda persist, persist_auth, p=phone, err=exc: self.submit_code_error_result(
                        p,
                        err,
                        persist=persist,
                        persist_auth_state=persist_auth,
                    )
                )

        probe = self._auth_probe_provider() if self._auth_probe_provider is not None else None

        def persist_authorized() -> bool:
            return self._record_state(
                "authorized",
                phone=phone,
                probe=probe,
                extra_meta={_phone_code_hash_key(self.account_id): ""},
            )

        wrote = await self._run_write(persist_authorized)
        if not wrote:
            return self._conflict_result("submit_code", phone=phone)
        return {"account_id": self.account_id, "auth_state": "authorized", "authorized": True}

    def submit_code_error_result(
        self,
        phone: str,
        exc: Exception,
        *,
        persist: bool = True,
        persist_auth_state: bool = True,
    ) -> dict[str, Any]:
        error = classify_telegram_exception(exc, default_code="submit_code_failed")
        result = {
            "account_id": self.account_id,
            "auth_state": error.auth_state,
            "authorized": False,
            "error": error.to_public_dict(),
        }
        if persist:
            self._persist_error_diagnostic(
                "submit_code",
                "rate_limited" if error.code == "flood_wait" else "failed",
                error,
                phone=phone,
                persist_auth_state=persist_auth_state,
            )
        return result

    def _persist_error_diagnostic(
        self,
        operation: str,
        status: str,
        error: Any,
        *,
        phone: str | None = None,
        persist_auth_state: bool = True,
    ) -> None:
        if persist_auth_state:
            try:
                self._record_state(error.auth_state, phone=phone, last_error=error.message)
            except Exception:
                self.logger.warning(
                    "Failed to persist auth error state for account %s (%s)",
                    self.account_id,
                    operation,
                    exc_info=True,
                )
        try:
            self._record_operation(operation, status, error=error)
        except Exception:
            self.logger.warning(
                "Failed to persist auth operation event for account %s (%s)",
                self.account_id,
                operation,
                exc_info=True,
            )

    def _conflict_result(self, operation: str, *, phone: str) -> dict[str, Any]:
        error_payload = {
            "code": "auth_state_conflict",
            "message": f"Account auth state changed concurrently while saving {operation}; retry the operation.",
            "retryable": True,
        }
        if operation == "request_code":
            return {
                "account_id": self.account_id,
                "auth_state": "conflict",
                "phone": phone,
                "sent": False,
                "error": error_payload,
            }
        return {
            "account_id": self.account_id,
            "auth_state": "conflict",
            "authorized": False,
            "error": error_payload,
        }

    def _handle_rpc_error(self, builder: Callable[[bool, bool], dict[str, Any]]) -> dict[str, Any]:
        if self._error_persister is None:
            return builder(True, True)
        result = builder(False, False)
        self._error_persister(lambda fn=builder: fn(True, False))
        return result

    def _record_state(
        self,
        state: str,
        phone: str | None = None,
        last_error: str | None = None,
        raw: dict[str, Any] | None = None,
        *,
        probe: tuple[int, str, int] | None = None,
        extra_meta: dict[str, str] | None = None,
    ) -> bool:
        now = utc_now_iso()
        auth_record = AccountAuthRecord(
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
        if probe is not None:
            expected_seq, owner_id, owner_epoch = probe
            wrote = self.store.upsert_account_auth(
                auth_record,
                expected_seq=expected_seq,
                owner_id=owner_id,
                owner_epoch=owner_epoch,
                extra_meta=extra_meta,
            )
        elif self._auth_owner_provider is not None:
            owner_id, owner_epoch = self._auth_owner_provider()
            wrote = self.store.upsert_account_auth(
                auth_record,
                owner_id=owner_id,
                owner_epoch=owner_epoch,
                extra_meta=extra_meta,
            )
        else:
            wrote = self.store.upsert_account_auth(auth_record, extra_meta=extra_meta)
        if not wrote:
            return False
        self.store.upsert_account(
            AccountRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                display_name=self.account_id,
                kind="telegram",
                updated_at=now,
            )
        )
        return True

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
