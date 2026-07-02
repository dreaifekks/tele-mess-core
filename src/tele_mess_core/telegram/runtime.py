from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus
from typing import Any


@dataclass(slots=True)
class TelegramOperationError(Exception):
    code: str
    message: str
    auth_state: str = "auth_failed"
    http_status: HTTPStatus = HTTPStatus.BAD_GATEWAY
    retry_after: int | None = None
    error_type: str | None = None

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_public_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "auth_state": self.auth_state,
        }
        if self.retry_after is not None:
            payload["retry_after"] = self.retry_after
        if self.error_type:
            payload["type"] = self.error_type
        return payload


def classify_telegram_exception(
    exc: Exception,
    *,
    default_code: str = "telegram_error",
    default_auth_state: str = "auth_failed",
    default_status: HTTPStatus = HTTPStatus.BAD_GATEWAY,
) -> TelegramOperationError:
    error_type = exc.__class__.__name__
    message = _safe_message(exc)
    retry_after = _retry_after(exc)
    lower_name = error_type.lower()
    lower_message = message.lower()

    if "floodwait" in lower_name or "flood wait" in lower_message:
        return TelegramOperationError(
            code="flood_wait",
            message=message,
            auth_state=default_auth_state,
            http_status=HTTPStatus.TOO_MANY_REQUESTS,
            retry_after=retry_after,
            error_type=error_type,
        )
    if "phonecodeinvalid" in lower_name:
        return TelegramOperationError("invalid_code", message, default_auth_state, HTTPStatus.BAD_REQUEST, error_type=error_type)
    if "phonecodeexpired" in lower_name:
        return TelegramOperationError("expired_code", message, "needs_login", HTTPStatus.BAD_REQUEST, error_type=error_type)
    if "phonenumberinvalid" in lower_name:
        return TelegramOperationError("invalid_phone", message, default_auth_state, HTTPStatus.BAD_REQUEST, error_type=error_type)
    if "passwordhashinvalid" in lower_name:
        return TelegramOperationError("invalid_password", message, "password_needed", HTTPStatus.BAD_REQUEST, error_type=error_type)
    if "sessionpasswordneeded" in lower_name:
        return TelegramOperationError("password_needed", message, "password_needed", HTTPStatus.OK, error_type=error_type)
    if any(key in lower_name for key in ("authkey", "sessionrevoked", "unauthorized")):
        return TelegramOperationError("needs_login", message, "needs_login", HTTPStatus.UNAUTHORIZED, error_type=error_type)
    if any(key in lower_name for key in ("chatadminrequired", "userprivacyrestricted", "channelprivate")):
        return TelegramOperationError("access_denied", message, default_auth_state, HTTPStatus.FORBIDDEN, error_type=error_type)
    return TelegramOperationError(default_code, message, default_auth_state, default_status, error_type=error_type)


def _safe_message(exc: Exception) -> str:
    text = str(exc) or exc.__class__.__name__
    return text[:500]


def _retry_after(exc: Exception) -> int | None:
    for attr in ("seconds", "value"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
    return None
