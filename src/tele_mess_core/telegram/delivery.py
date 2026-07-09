from __future__ import annotations

import json
import logging
from typing import Any

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import DailyDeliveryConfig, TelegramAccountConfig
from tele_mess_core.models import AccountAuthRecord, OperationEventRecord, SOURCE_TELEGRAM, utc_now_iso
from tele_mess_core.telegram.runtime import TelegramOperationError, classify_telegram_exception


MAX_TELEGRAM_MESSAGE_CHARS = 3800


class TelegramSummaryDeliveryService:
    def __init__(self, config: TelegramAccountConfig, store: ArchiveStore):
        self.config = config
        self.account_id = config.account_id
        self.store = store
        self.logger = logging.getLogger(__name__)

    async def send_summary(self, delivery: DailyDeliveryConfig, content: str) -> dict[str, Any]:
        if delivery.origin_id is None:
            raise ValueError("daily.delivery.origin_id is required")
        client = None
        try:
            client = await self._connected_client()
            if not await client.is_user_authorized():
                self._set_auth_state("needs_login")
                raise TelegramOperationError(
                    code="needs_login",
                    message=f"Telegram account {self.account_id} is not authorized",
                    auth_state="needs_login",
                )

            self._set_auth_state("authorized")
            entity = await client.get_entity(delivery.origin_id)
            chunks = split_telegram_message(content)
            message_ids: list[int] = []
            for index, chunk in enumerate(chunks, start=1):
                body = chunk
                if len(chunks) > 1:
                    body = f"[{index}/{len(chunks)}]\n\n{chunk}"
                sent = await client.send_message(
                    entity,
                    body,
                    reply_to=delivery.topic_id or None,
                    parse_mode=None,
                )
                message_id = getattr(sent, "id", None)
                if message_id is not None:
                    message_ids.append(int(message_id))

            result = {
                "account_id": self.account_id,
                "origin_id": delivery.origin_id,
                "topic_id": delivery.topic_id,
                "status": "sent",
                "message_count": len(chunks),
                "message_ids": message_ids,
            }
            self._record_operation("deliver_daily_summary", "ok", result=result)
            return result
        except TelegramOperationError as exc:
            self._record_operation(
                "deliver_daily_summary",
                "failed",
                subject_id=_delivery_subject_id(delivery),
                error=exc.to_public_dict(),
            )
            raise
        except Exception as exc:
            error = classify_telegram_exception(
                exc,
                default_code="daily_summary_delivery_failed",
                default_auth_state="authorized",
            )
            payload = error.to_public_dict()
            self._record_operation(
                "deliver_daily_summary",
                "failed",
                subject_id=_delivery_subject_id(delivery),
                error=payload,
            )
            raise TelegramOperationError(error.code, error.message, error.auth_state, error.http_status, error.retry_after, error.error_type) from exc
        finally:
            if client is not None:
                await client.disconnect()

    async def _connected_client(self) -> Any:
        from telethon import TelegramClient

        self.config.session_dir.mkdir(parents=True, exist_ok=True)
        session_file = self.config.session_dir / self.config.session_name
        client = TelegramClient(str(session_file), self.config.api_id, self.config.api_hash)
        await client.connect()
        return client

    def _set_auth_state(self, state: str) -> None:
        self.store.upsert_account_auth(
            AccountAuthRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                auth_state=state,
                session_name=self.config.session_name,
                session_dir=str(self.config.session_dir),
                updated_at=utc_now_iso(),
            )
        )

    def _record_operation(
        self,
        operation: str,
        status: str,
        *,
        subject_id: str | None = None,
        error: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
    ) -> None:
        self.store.add_operation_event(
            OperationEventRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                operation=operation,
                status=status,
                subject_type="origin" if subject_id else None,
                subject_id=subject_id,
                error_code=error.get("code") if error else None,
                message=error.get("message") if error else None,
                retry_after=error.get("retry_after") if error else None,
                occurred_at=utc_now_iso(),
                raw_json=json.dumps(error or result or {}, ensure_ascii=False),
            )
        )


def split_telegram_message(content: str, limit: int = MAX_TELEGRAM_MESSAGE_CHARS) -> list[str]:
    text = str(content or "").strip()
    if not text:
        return ["Daily summary is empty."]
    if limit < 100:
        raise ValueError("limit must be at least 100")
    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        split_at = _best_split_index(remaining, limit)
        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def _best_split_index(text: str, limit: int) -> int:
    for separator in ("\n\n", "\n", ". ", " "):
        index = text.rfind(separator, 0, limit + 1)
        if index >= max(1, limit // 2):
            return index + len(separator)
    return limit


def _delivery_subject_id(delivery: DailyDeliveryConfig) -> str:
    if delivery.topic_id:
        return f"{delivery.origin_id}/{delivery.topic_id}"
    return str(delivery.origin_id)
