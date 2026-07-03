from __future__ import annotations

import json
import logging
import asyncio
from pathlib import Path
from typing import Any

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import BackfillConfig, MediaDownloadConfig, TelegramAccountConfig
from tele_mess_core.models import (
    AccountAuthRecord,
    AccountRecord,
    BackupPolicyRecord,
    CaptureCursorRecord,
    ChatRecord,
    MediaFileRecord,
    MessageRecord,
    OperationEventRecord,
    OriginRecord,
    SOURCE_TELEGRAM,
    UserRecord,
    to_iso,
    utc_now_iso,
)
from tele_mess_core.telegram.runtime import classify_telegram_exception


class TelegramArchiveService:
    def __init__(
        self,
        config: TelegramAccountConfig,
        store: ArchiveStore,
        backfill: BackfillConfig | None = None,
        media_download: MediaDownloadConfig | None = None,
    ):
        self.config = config
        self.backfill = backfill or BackfillConfig()
        self.media_download = media_download or MediaDownloadConfig()
        self.account_id = config.account_id
        self.store = store
        self.logger = logging.getLogger(__name__)
        self.client = None

    async def run(self) -> None:
        from telethon import TelegramClient, events, utils
        from telethon.tl.types import MessageService, UpdateMessageReactions

        self.config.session_dir.mkdir(parents=True, exist_ok=True)
        session_file = self.config.session_dir / self.config.session_name
        self.client = TelegramClient(str(session_file), self.config.api_id, self.config.api_hash)
        await self.client.start()
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
                auth_state="authorized",
                session_name=self.config.session_name,
                session_dir=str(self.config.session_dir),
                updated_at=now,
            )
        )

        chat_ids = [chat.id for chat in self.config.chats]
        for chat in self.config.chats:
            self.store.upsert_chat(
                ChatRecord(
                    source=SOURCE_TELEGRAM,
                    account_id=self.account_id,
                    chat_id=chat.id,
                    title=chat.name,
                    updated_at=utc_now_iso(),
                )
            )
            self.store.upsert_origin(
                OriginRecord(
                    source=SOURCE_TELEGRAM,
                    account_id=self.account_id,
                    origin_id=chat.id,
                    origin_type="configured_chat",
                    title=chat.name,
                    updated_at=utc_now_iso(),
                )
            )
            self.store.set_backup_policy(
                BackupPolicyRecord(
                    source=SOURCE_TELEGRAM,
                    account_id=self.account_id,
                    origin_id=chat.id,
                    enabled=True,
                    capture_text=True,
                    capture_media_metadata=True,
                    download_media=False,
                    updated_at=utc_now_iso(),
                )
            )

        self.logger.info("Monitoring %s Telegram chats for account %s", len(chat_ids), self.account_id)

        @self.client.on(events.NewMessage(chats=chat_ids))
        async def on_new(event: Any) -> None:
            if isinstance(event.message, MessageService):
                return
            await self._store_message(event.message, event_type="new")

        @self.client.on(events.MessageEdited(chats=chat_ids))
        async def on_edit(event: Any) -> None:
            original_update = getattr(event, "original_update", None)
            if isinstance(original_update, UpdateMessageReactions):
                return
            if not getattr(event.message, "edit_date", None):
                return
            await self._store_message(event.message, event_type="edit")

        @self.client.on(events.MessageDeleted(chats=chat_ids))
        async def on_delete(event: Any) -> None:
            chat_id = getattr(event, "chat_id", None)
            if chat_id is None or not self._policy_for(int(chat_id)).get("enabled", False):
                return
            self.store.mark_deleted(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                chat_id=int(chat_id),
                message_ids=[int(mid) for mid in event.deleted_ids],
                event_at=utc_now_iso(),
                raw_payload={"deleted_ids": list(event.deleted_ids)},
            )

        @self.client.on(events.Raw)
        async def on_raw(event: Any) -> None:
            if not isinstance(event, UpdateMessageReactions):
                return
            peer_id = utils.get_peer_id(event.peer)
            chat_id = self._match_chat_id(peer_id, chat_ids)
            if chat_id is None or not self._policy_for(chat_id).get("enabled", False):
                return
            self.store.update_reactions(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                chat_id=chat_id,
                message_id=int(event.msg_id),
                reactions=_reaction_payload(event),
                event_at=utc_now_iso(),
                raw_payload=_safe_dict(event),
            )

        await self._backfill_configured_chats(chat_ids)
        await self.client.run_until_disconnected()

    def _match_chat_id(self, peer_id: int, configured_chat_ids: list[int]) -> int | None:
        if peer_id in configured_chat_ids:
            return peer_id
        if peer_id > 0:
            channel_id = int(f"-100{peer_id}")
            if channel_id in configured_chat_ids:
                return channel_id
        return None

    async def _store_message(self, message: Any, event_type: str) -> bool:
        chat_id = int(message.chat_id)
        topic_id = _topic_id(message)
        policy = self._policy_for(chat_id, topic_id)
        if not policy.get("enabled", False):
            return False

        sender = None
        chat = None
        try:
            sender = await message.get_sender()
        except Exception as exc:
            self.logger.debug("Failed to fetch sender for %s: %s", getattr(message, "id", "?"), exc)
        try:
            chat = await message.get_chat()
        except Exception as exc:
            self.logger.debug("Failed to fetch chat for %s: %s", getattr(message, "id", "?"), exc)

        if chat:
            self.store.upsert_chat(_chat_record(self.account_id, chat, chat_id=chat_id))
        if sender:
            self.store.upsert_user(_user_record(self.account_id, sender))

        record = MessageRecord(
            source=SOURCE_TELEGRAM,
            account_id=self.account_id,
            chat_id=chat_id,
            message_id=int(message.id),
            topic_id=topic_id,
            sender_id=getattr(sender, "id", None) if sender else getattr(message, "sender_id", None),
            sender_name=_display_name(sender),
            sender_username=getattr(sender, "username", None) if sender else None,
            sent_at=to_iso(getattr(message, "date", None)) or utc_now_iso(),
            edited_at=to_iso(getattr(message, "edit_date", None)),
            ingested_at=utc_now_iso(),
            text=getattr(message, "text", None) if policy.get("capture_text", True) else None,
            has_media=bool(getattr(message, "media", None))
            and (policy.get("capture_media_metadata", True) or policy.get("download_media", False)),
            media_kind=type(message.media).__name__
            if getattr(message, "media", None)
            and (policy.get("capture_media_metadata", True) or policy.get("download_media", False))
            else None,
            grouped_id=str(message.grouped_id)
            if getattr(message, "grouped_id", None)
            and (policy.get("capture_media_metadata", True) or policy.get("download_media", False))
            else None,
            reply_to_message_id=getattr(message, "reply_to_msg_id", None),
            forward_from_id=_forward_from_id(message),
            forward_from_name=_forward_from_name(message),
            permalink=_permalink(chat, message, chat_id=int(message.chat_id)),
            reactions_json=json.dumps(_message_reactions(message), ensure_ascii=False, default=str)
            if getattr(message, "reactions", None)
            else None,
            raw_json=json.dumps(_safe_dict(message), ensure_ascii=False, default=str)
            if policy.get("capture_media_metadata", True)
            else None,
        )
        self.store.upsert_message(record, event_type=event_type)
        if policy.get("download_media", False) and _message_media_downloadable(message):
            await self._download_message_media(message, record)
        self._update_capture_cursor(chat_id, int(message.id), record.sent_at)
        return True


    async def _download_message_media(self, message: Any, record: MessageRecord) -> None:
        target_dir = self.store.database_path.parent / "media" / self.account_id / str(record.chat_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        downloaded = None
        attempts = max(1, self.media_download.retries + 1)
        success_attempt = 0
        for attempt in range(1, attempts + 1):
            try:
                downloaded = await message.download_media(file=str(target_dir))
                success_attempt = attempt
                break
            except Exception as exc:
                error = classify_telegram_exception(exc, default_code="media_download_failed")
                self.logger.warning(
                    "Failed to download media for account=%s chat=%s message=%s attempt=%s/%s: %s",
                    self.account_id,
                    record.chat_id,
                    record.message_id,
                    attempt,
                    attempts,
                    error.message,
                )
                if attempt >= attempts:
                    self._record_operation(
                        "media_download",
                        "failed",
                        subject_type="message",
                        subject_id=f"{record.chat_id}/{record.message_id}",
                        error=error.to_public_dict() | {"attempts": attempts},
                    )
                    return
                if self.media_download.retry_delay_seconds > 0:
                    await asyncio.sleep(self.media_download.retry_delay_seconds)
        if not downloaded:
            self._record_operation(
                "media_download",
                "failed",
                subject_type="message",
                subject_id=f"{record.chat_id}/{record.message_id}",
                error={"code": "media_download_empty", "message": "download_media returned no file path", "attempts": attempts},
            )
            return
        path = Path(downloaded)
        file_size = path.stat().st_size if path.exists() else None
        self.store.upsert_media_file(
            MediaFileRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                chat_id=record.chat_id,
                message_id=record.message_id,
                file_path=str(path),
                media_kind=record.media_kind,
                file_size=file_size,
                downloaded_at=utc_now_iso(),
                raw_json=json.dumps({"downloaded": str(path)}, ensure_ascii=False),
            )
        )
        if success_attempt > 1:
            self._record_operation(
                "media_download",
                "ok",
                subject_type="message",
                subject_id=f"{record.chat_id}/{record.message_id}",
                error={"attempts": success_attempt},
            )


    async def _backfill_configured_chats(self, chat_ids: list[int]) -> None:
        if not self.backfill.enabled or not chat_ids:
            return
        from telethon.tl.types import MessageService

        for chat_id in chat_ids:
            policy = self._policy_for(chat_id)
            if not policy.get("enabled", False):
                self.logger.info("Skipping backfill for disabled origin %s/%s", self.account_id, chat_id)
                continue
            try:
                cursor = self.store.get_capture_cursor(SOURCE_TELEGRAM, self.account_id, chat_id)
                min_id = int(cursor["last_message_id"]) if cursor else 0
                limit = self.backfill.catch_up_limit if min_id else self.backfill.initial_limit
                limit_arg = None if limit <= 0 else limit
                count = 0
                last_message_id = min_id
                last_message_at = cursor.get("last_message_at") if cursor else None
                self.logger.info(
                    "Backfilling account=%s chat=%s min_id=%s limit=%s",
                    self.account_id,
                    chat_id,
                    min_id,
                    limit_arg if limit_arg is not None else "unlimited",
                )
                async for message in self.client.iter_messages(chat_id, min_id=min_id, reverse=True, limit=limit_arg):
                    if isinstance(message, MessageService):
                        continue
                    stored = await self._store_message(message, event_type="backfill")
                    if stored:
                        last_message_id = max(last_message_id, int(message.id))
                        last_message_at = to_iso(getattr(message, "date", None)) or last_message_at
                        count += 1
                self.store.upsert_capture_cursor(
                    CaptureCursorRecord(
                        source=SOURCE_TELEGRAM,
                        account_id=self.account_id,
                        origin_id=chat_id,
                        last_message_id=last_message_id,
                        last_message_at=last_message_at,
                        last_backfill_at=utc_now_iso(),
                        updated_at=utc_now_iso(),
                        raw_json=json.dumps({"count": count, "min_id": min_id, "limit": limit_arg}, ensure_ascii=False),
                    )
                )
                self.logger.info("Backfilled %s messages for account=%s chat=%s", count, self.account_id, chat_id)
            except Exception as exc:
                error = classify_telegram_exception(exc, default_code="backfill_failed", default_auth_state="authorized")
                self.logger.warning(
                    "Backfill failed for account=%s chat=%s: %s",
                    self.account_id,
                    chat_id,
                    error.message,
                )
                self._record_operation(
                    "backfill",
                    "failed",
                    subject_type="origin",
                    subject_id=str(chat_id),
                    error=error.to_public_dict(),
                )

    def _policy_for(self, chat_id: int, topic_id: int | None = None) -> dict[str, Any]:
        if topic_id:
            topic_policy = self.store.get_backup_policy(SOURCE_TELEGRAM, self.account_id, chat_id, topic_id)
            if topic_policy is not None:
                return topic_policy
        origin_policy = self.store.get_backup_policy(SOURCE_TELEGRAM, self.account_id, chat_id, 0)
        if origin_policy is not None:
            return origin_policy
        return {
            "enabled": False,
            "capture_text": True,
            "capture_media_metadata": True,
            "download_media": False,
        }

    def _update_capture_cursor(self, chat_id: int, message_id: int, message_at: str | None) -> None:
        self.store.upsert_capture_cursor(
            CaptureCursorRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                origin_id=chat_id,
                last_message_id=message_id,
                last_message_at=message_at,
                updated_at=utc_now_iso(),
            )
        )

    def _record_operation(
        self,
        operation: str,
        status: str,
        subject_type: str | None = None,
        subject_id: str | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        self.store.add_operation_event(
            OperationEventRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                operation=operation,
                status=status,
                subject_type=subject_type,
                subject_id=subject_id,
                error_code=error.get("code") if error else None,
                message=error.get("message") if error else None,
                retry_after=error.get("retry_after") if error else None,
                occurred_at=utc_now_iso(),
                raw_json=json.dumps(error, ensure_ascii=False, default=str) if error else None,
            )
        )


def _chat_record(account_id: str, chat: Any, chat_id: int | None = None) -> ChatRecord:
    return ChatRecord(
        source=SOURCE_TELEGRAM,
        account_id=account_id,
        chat_id=int(chat_id if chat_id is not None else getattr(chat, "id")),
        title=getattr(chat, "title", None) or _display_name(chat),
        username=getattr(chat, "username", None),
        kind=type(chat).__name__,
        updated_at=utc_now_iso(),
        raw_json=json.dumps(_safe_dict(chat), ensure_ascii=False, default=str),
    )


def _user_record(account_id: str, user: Any) -> UserRecord:
    return UserRecord(
        source=SOURCE_TELEGRAM,
        account_id=account_id,
        user_id=int(getattr(user, "id")),
        username=getattr(user, "username", None),
        display_name=_display_name(user),
        is_bot=bool(getattr(user, "bot", False)),
        updated_at=utc_now_iso(),
        raw_json=json.dumps(_safe_dict(user), ensure_ascii=False, default=str),
    )


def _display_name(entity: Any) -> str | None:
    if entity is None:
        return None
    first = getattr(entity, "first_name", None) or ""
    last = getattr(entity, "last_name", None) or ""
    name = f"{first} {last}".strip()
    return name or getattr(entity, "title", None) or getattr(entity, "username", None)


def _topic_id(message: Any) -> int | None:
    reply_to = getattr(message, "reply_to", None)
    if not reply_to:
        return None
    if getattr(reply_to, "forum_topic", False):
        return getattr(reply_to, "reply_to_top_id", None) or getattr(reply_to, "reply_to_msg_id", None)
    return None


def _message_media_downloadable(message: Any) -> bool:
    media = getattr(message, "media", None)
    if media is None:
        return False
    media_type = type(media).__name__
    downloadable_types = {
        "MessageMediaPhoto",
        "Photo",
        "MessageMediaDocument",
        "Document",
        "MessageMediaContact",
        "WebDocument",
        "WebDocumentNoProxy",
    }
    if media_type in downloadable_types:
        return True
    if media_type != "MessageMediaWebPage":
        return False
    webpage = getattr(media, "webpage", None)
    if type(webpage).__name__ != "WebPage":
        return False
    return bool(getattr(webpage, "document", None) or getattr(webpage, "photo", None))


def _forward_from_id(message: Any) -> str | None:
    fwd = getattr(message, "fwd_from", None)
    if not fwd:
        return None
    from_id = getattr(fwd, "from_id", None)
    return str(from_id) if from_id else None


def _forward_from_name(message: Any) -> str | None:
    fwd = getattr(message, "fwd_from", None)
    if not fwd:
        return None
    return getattr(fwd, "from_name", None)


def _permalink(chat: Any, message: Any, chat_id: int | None = None) -> str | None:
    if not chat:
        return None
    username = getattr(chat, "username", None)
    if username:
        return f"https://t.me/{username}/{message.id}"
    chat_id_str = str(chat_id if chat_id is not None else getattr(chat, "id", ""))
    if chat_id_str.startswith("-100"):
        return f"https://t.me/c/{chat_id_str[4:]}/{message.id}"
    if chat_id_str.startswith("-"):
        return f"https://t.me/c/{chat_id_str[1:]}/{message.id}"
    return None


def _message_reactions(message: Any) -> Any:
    return _safe_dict(getattr(message, "reactions", None))


def _reaction_payload(event: Any) -> dict[str, Any]:
    reactions = getattr(event, "reactions", None)
    return _safe_dict(reactions) if reactions else {}


def _safe_dict(obj: Any) -> Any:
    if obj is None:
        return None
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if hasattr(obj, "to_json"):
        return json.loads(obj.to_json())
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, Path):
        return str(obj)
    return str(obj)
