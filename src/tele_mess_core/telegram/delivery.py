from __future__ import annotations

import copy
import inspect
import json
import logging
import re
from typing import Any

from telethon.extensions import markdown as telethon_markdown

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import DailyDeliveryConfig, TelegramAccountConfig
from tele_mess_core.models import AccountAuthRecord, OperationEventRecord, SOURCE_TELEGRAM, utc_now_iso
from tele_mess_core.telegram.runtime import TelegramOperationError, classify_telegram_exception


MAX_TELEGRAM_MESSAGE_CHARS = 3800
TELEGRAM_SUMMARY_PARSE_MODE = "md"


class TelegramSummaryDeliveryService:
    def __init__(self, config: TelegramAccountConfig, store: ArchiveStore):
        self.config = config
        self.account_id = config.account_id
        self.store = store
        self.logger = logging.getLogger(__name__)

    async def send_chunk(
        self,
        delivery: DailyDeliveryConfig,
        content: str,
        *,
        client: Any,
        random_id: int | None = None,
    ) -> dict[str, Any]:
        if delivery.origin_id is None:
            raise ValueError("daily.delivery.origin_id is required")
        try:
            if not await client.is_user_authorized():
                self._set_auth_state("needs_login")
                raise TelegramOperationError(
                    code="needs_login",
                    message=f"Telegram account {self.account_id} is not authorized",
                    auth_state="needs_login",
                )

            self._set_auth_state("authorized")
            entity = await client.get_entity(delivery.origin_id)
            body = telegram_markdown_for_send(content)
            message_id = await self._send_with_random_id(
                client,
                entity,
                body,
                topic_id=delivery.topic_id,
                random_id=random_id,
            )
            message_ids = [int(message_id)] if message_id is not None else []
            result = {
                "account_id": self.account_id,
                "origin_id": delivery.origin_id,
                "topic_id": delivery.topic_id,
                "status": "sent",
                "message_count": 1,
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
            raise self.record_delivery_error(delivery, exc) from exc

    async def _send_with_random_id(
        self,
        client: Any,
        entity: Any,
        body: str,
        *,
        topic_id: int,
        random_id: int | None,
    ) -> int | None:
        if (
            random_id is not None
            and callable(client)
            and callable(getattr(client, "get_input_entity", None))
            and callable(getattr(client, "_get_response_message", None))
        ):
            from telethon.tl import functions, types

            plain_text, entities = telethon_markdown.parse(body)
            input_peer = await client.get_input_entity(entity)
            reply_to = types.InputReplyToMessage(topic_id) if topic_id else None
            request = functions.messages.SendMessageRequest(
                peer=input_peer,
                message=plain_text or body,
                entities=entities or None,
                reply_to=reply_to,
                random_id=int(random_id),
            )
            response = await client(request)
            if isinstance(response, types.UpdateShortSentMessage):
                return getattr(response, "id", None)
            sent = client._get_response_message(request, response, input_peer)
            return getattr(sent, "id", None)

        send_kwargs: dict[str, Any] = {
            "reply_to": topic_id or None,
            "parse_mode": TELEGRAM_SUMMARY_PARSE_MODE,
        }
        if random_id is not None:
            try:
                sig = inspect.signature(client.send_message)
                if "random_id" in sig.parameters or any(
                    p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
                ):
                    send_kwargs["random_id"] = int(random_id)
            except (TypeError, ValueError):
                pass
        sent = await client.send_message(entity, body, **send_kwargs)
        return getattr(sent, "id", None)

    def record_delivery_error(
        self,
        delivery: DailyDeliveryConfig,
        exc: Exception,
    ) -> TelegramOperationError:
        error = classify_telegram_exception(
            exc,
            default_code="daily_summary_delivery_failed",
            default_auth_state="authorized",
        )
        self._record_operation(
            "deliver_daily_summary",
            "failed",
            subject_id=_delivery_subject_id(delivery),
            error=error.to_public_dict(),
        )
        return TelegramOperationError(
            error.code,
            error.message,
            error.auth_state,
            error.http_status,
            error.retry_after,
            error.error_type,
        )

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


def split_telegram_markdown(content: str, limit: int = MAX_TELEGRAM_MESSAGE_CHARS) -> list[str]:
    """Split Markdown into independently parseable Telegram messages."""
    markdown_content = telegram_markdown_for_send(content)
    plain_text, entities = telethon_markdown.parse(markdown_content)
    ranges = _split_telegram_text_ranges(plain_text, limit)
    if not ranges:
        return ["Daily summary is empty."]

    utf16_offsets = _utf16_prefix_offsets(plain_text)
    chunks: list[str] = []
    for start, end in ranges:
        start_offset = utf16_offsets[start]
        end_offset = utf16_offsets[end]
        chunk_entities = _slice_telegram_entities(entities, start_offset, end_offset)
        chunks.append(telethon_markdown.unparse(plain_text[start:end], chunk_entities))
    return chunks


def telegram_markdown_for_send(content: str) -> str:
    """Adapt common Markdown headings to the subset rendered by Telethon."""
    lines: list[str] = []
    in_fenced_code = False
    for line in str(content or "").splitlines():
        stripped = line.lstrip()
        if stripped.startswith("```"):
            in_fenced_code = not in_fenced_code
            lines.append(line)
            continue
        if not in_fenced_code:
            heading = re.match(r"^\s{0,3}#{1,6}[ \t]+(.+?)\s*$", line)
            if heading:
                line = f"**{heading.group(1)}**"
        lines.append(line)
    return "\n".join(lines).strip()


def _split_telegram_text_ranges(content: str, limit: int) -> list[tuple[int, int]]:
    if limit < 100:
        raise ValueError("limit must be at least 100")
    if not content:
        return []

    start = len(content) - len(content.lstrip())
    end = len(content.rstrip())
    if start >= end:
        return []

    ranges: list[tuple[int, int]] = []
    while _utf16_length(content[start:end]) > limit:
        split_at = _best_utf16_split_index(content[start:end], limit)
        raw_end = start + split_at
        chunk_end = raw_end
        while chunk_end > start and content[chunk_end - 1].isspace():
            chunk_end -= 1
        if chunk_end == start:
            chunk_end = raw_end
        ranges.append((start, chunk_end))

        start = raw_end
        while start < end and content[start].isspace():
            start += 1

    if start < end:
        ranges.append((start, end))
    return ranges


def _best_utf16_split_index(text: str, limit: int) -> int:
    units = 0
    hard_limit = 0
    for index, char in enumerate(text):
        next_units = units + (2 if ord(char) > 0xFFFF else 1)
        if next_units > limit:
            break
        units = next_units
        hard_limit = index + 1
    if hard_limit <= 0:
        return 1

    for separator in ("\n\n", "\n", ". ", " "):
        index = text.rfind(separator, 0, hard_limit)
        if index >= max(1, hard_limit // 2):
            return index + len(separator)
    return hard_limit


def _utf16_prefix_offsets(text: str) -> list[int]:
    offsets = [0]
    for char in text:
        offsets.append(offsets[-1] + (2 if ord(char) > 0xFFFF else 1))
    return offsets


def _utf16_length(text: str) -> int:
    return sum(2 if ord(char) > 0xFFFF else 1 for char in text)


def _slice_telegram_entities(
    entities: list[Any],
    start_offset: int,
    end_offset: int,
) -> list[Any]:
    chunk_entities: list[Any] = []
    for entity in entities:
        entity_start = int(entity.offset)
        entity_end = entity_start + int(entity.length)
        overlap_start = max(entity_start, start_offset)
        overlap_end = min(entity_end, end_offset)
        if overlap_start >= overlap_end:
            continue
        chunk_entity = copy.copy(entity)
        chunk_entity.offset = overlap_start - start_offset
        chunk_entity.length = overlap_end - overlap_start
        chunk_entities.append(chunk_entity)
    return chunk_entities


def _delivery_subject_id(delivery: DailyDeliveryConfig) -> str:
    if delivery.topic_id:
        return f"{delivery.origin_id}/{delivery.topic_id}"
    return str(delivery.origin_id)
