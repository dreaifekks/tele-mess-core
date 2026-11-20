from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import BackfillConfig, MediaDownloadConfig, TelegramAccountConfig
from tele_mess_core.models import (
    AccountAuthRecord,
    AccountRecord,
    CaptureCursorRecord,
    ChatRecord,
    MediaFileRecord,
    MessageRecord,
    OperationEventRecord,
    SOURCE_TELEGRAM,
    UserRecord,
    to_iso,
    utc_now_iso,
)
from tele_mess_core.telegram.runtime import classify_telegram_exception


@dataclass(frozen=True, slots=True)
class CaptureTarget:
    chat_id: int
    topic_id: int = 0


@dataclass(eq=False, slots=True)
class _InflightFetchState:
    deleted: bool = False


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
        self._handlers_registered = False
        self._backfill_lock = asyncio.Lock()
        self._retry_targets: set[CaptureTarget] = set()
        self._retry_task: asyncio.Task[None] | None = None
        self._backfill_retry_wait_seconds = 5.0
        self._closed = False
        self._inflight_fetches: dict[tuple[int, int], dict[int, _InflightFetchState]] = {}

    async def run(self) -> None:
        from telethon import TelegramClient

        self.config.session_dir.mkdir(parents=True, exist_ok=True)
        session_file = self.config.session_dir / self.config.session_name
        client = TelegramClient(
            str(session_file),
            self.config.api_id,
            self.config.api_hash,
            catch_up=True,
            sequential_updates=True,
        )
        self.client = client
        self.register_handlers(client)
        try:
            await client.start()
            await self.activate(client)
            await client.run_until_disconnected()
        finally:
            await self.stop()
            if client.is_connected():
                await client.disconnect()

    async def attach(self, client: Any) -> None:
        self.register_handlers(client)
        await self.activate(client)

    def register_handlers(self, client: Any) -> None:
        from telethon import TelegramClient, events, utils
        from telethon.tl.types import MessageService, UpdateMessageReactions

        if not isinstance(client, TelegramClient):
            # Tests and compatible adapters may provide a duck-typed client.
            self.logger.debug("Attaching non-TelegramClient adapter for account %s", self.account_id)
        self.client = client
        if self._handlers_registered:
            return

        @client.on(events.NewMessage())
        async def on_new(event: Any) -> None:
            if isinstance(event.message, MessageService):
                return
            await self._store_message(event.message, event_type="new")

        @client.on(events.MessageEdited())
        async def on_edit(event: Any) -> None:
            original_update = getattr(event, "original_update", None)
            if isinstance(original_update, UpdateMessageReactions):
                return
            if not getattr(event.message, "edit_date", None):
                return
            await self._store_message(event.message, event_type="edit")

        @client.on(events.MessageDeleted())
        async def on_delete(event: Any) -> None:
            deleted_ids = [int(mid) for mid in getattr(event, "deleted_ids", None) or []]
            if not deleted_ids:
                return
            chat_id = getattr(event, "chat_id", None)
            at = utc_now_iso()
            raw = {"deleted_ids": list(deleted_ids)}
            if chat_id is None:
                inflight_by_msg: dict[int, set[int]] = {}
                for (cid, mid), states in self._inflight_fetches.items():
                    if states and cid > -1000000000000 and mid in deleted_ids:
                        inflight_by_msg.setdefault(mid, set()).add(cid)
                _seqs, resolved_chat_ids = self.store.resolve_and_mark_deleted_without_chat(
                    source=SOURCE_TELEGRAM,
                    account_id=self.account_id,
                    message_ids=deleted_ids,
                    inflight_chat_ids_by_message=inflight_by_msg,
                    event_at=at,
                    raw_payload=raw,
                )
                for deleted_id, resolved_chat_id in resolved_chat_ids.items():
                    for state in self._inflight_fetches.get((resolved_chat_id, deleted_id), {}).values():
                        state.deleted = True
                return
            target_chat_id = int(chat_id)
            for deleted_id in deleted_ids:
                for state in self._inflight_fetches.get((target_chat_id, deleted_id), {}).values():
                    state.deleted = True
            self.store.mark_deleted(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                chat_id=target_chat_id,
                message_ids=deleted_ids,
                event_at=at,
                raw_payload=raw,
            )

        @client.on(events.Raw)
        async def on_raw(event: Any) -> None:
            if not isinstance(event, UpdateMessageReactions):
                return
            chat_id = int(utils.get_peer_id(event.peer))
            self.store.update_reactions(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                chat_id=chat_id,
                message_id=int(event.msg_id),
                reactions=_reaction_payload(event),
                event_at=utc_now_iso(),
                raw_payload=_safe_dict(event),
            )

        self._handlers_registered = True

    async def activate(self, client: Any) -> None:
        self.register_handlers(client)
        if not await client.is_user_authorized():
            raise RuntimeError(f"Telegram account {self.account_id} is not authorized")
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
        catch_up = getattr(client, "catch_up", None)
        if callable(catch_up):
            await catch_up()
        await self.refresh_capture_targets()

    async def refresh_capture_targets(self) -> None:
        capture_targets = self._capture_targets()
        self.logger.info(
            "Monitoring Telegram messages for account %s with %s enabled capture targets",
            self.account_id,
            len(capture_targets),
        )
        failed_targets = await self._backfill_capture_targets(capture_targets)
        self._schedule_backfill_retry(failed_targets)

    async def stop(self) -> None:
        self._closed = True
        self._retry_targets.clear()
        task = self._retry_task
        self._retry_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def _schedule_backfill_retry(self, targets: list[CaptureTarget]) -> None:
        if self._closed or not targets:
            return
        self._retry_targets.update(targets)
        if self._retry_task is not None and not self._retry_task.done():
            return
        self._retry_task = asyncio.create_task(
            self._retry_failed_backfills(),
            name=f"telegram-backfill-retry-{self.account_id}",
        )

    async def _retry_failed_backfills(self) -> None:
        delay = max(5.0, self._backfill_retry_wait_seconds)
        try:
            while self._retry_targets and not self._closed:
                await asyncio.sleep(delay)
                self._backfill_retry_wait_seconds = 5.0
                targets = list(self._retry_targets)
                self._retry_targets.clear()
                failed_targets = await self._backfill_capture_targets(targets)
                self._retry_targets.update(failed_targets)
                delay = max(min(delay * 2, 300.0), self._backfill_retry_wait_seconds)
        except asyncio.CancelledError:
            raise
        finally:
            if self._retry_task is asyncio.current_task():
                self._retry_task = None
            if self._retry_targets and not self._closed:
                self._schedule_backfill_retry(list(self._retry_targets))

    def _capture_targets(self) -> list[CaptureTarget]:
        targets: list[CaptureTarget] = []
        for policy in self.store.list_backup_policies(account_id=self.account_id):
            if policy.get("source") != SOURCE_TELEGRAM or not policy.get("enabled"):
                continue
            origin_id = int(policy["origin_id"])
            topic_id = int(policy.get("topic_id") or 0)
            effective = self.store.resolve_effective_backup_policy(
                SOURCE_TELEGRAM,
                self.account_id,
                origin_id,
                topic_id,
            )
            if not effective.get("enabled"):
                continue
            targets.append(CaptureTarget(origin_id, topic_id))
        return _dedupe_capture_targets(targets)

    def _register_inflight_fetch(self, chat_id: int, message_id: int) -> _InflightFetchState:
        state = _InflightFetchState()
        self._inflight_fetches.setdefault((chat_id, message_id), {})[id(state)] = state
        return state

    def _unregister_inflight_fetch(
        self,
        chat_id: int,
        message_id: int,
        state: _InflightFetchState,
    ) -> None:
        fetch_key = (chat_id, message_id)
        bucket = self._inflight_fetches.get(fetch_key)
        if bucket is not None:
            bucket.pop(id(state), None)
            if not bucket:
                self._inflight_fetches.pop(fetch_key, None)

    def _unregister_backfill_entries(
        self,
        entries: list[tuple[Any, _InflightFetchState | None]],
    ) -> None:
        for message, state in entries:
            if state is not None:
                self._unregister_inflight_fetch(int(message.chat_id), int(message.id), state)

    def _should_download_media_for(
        self,
        chat_id: int,
        topic_id: int,
        *,
        is_sticker_like: bool,
    ) -> bool:
        policy = self._policy_for(chat_id, topic_id)
        if not policy.get("enabled", False):
            return False
        if is_sticker_like:
            return bool(policy.get("download_stickers", False))
        return bool(policy.get("download_media", False))

    async def _store_message(
        self,
        message: Any,
        event_type: str,
        *,
        fetch_state: _InflightFetchState | None = None,
    ) -> bool:
        chat_id = int(message.chat_id)
        message_id = int(message.id)
        topic_id = _topic_id(message)
        owned_state = fetch_state is None
        active_state = self._register_inflight_fetch(chat_id, message_id) if owned_state else fetch_state
        assert active_state is not None
        built_record: MessageRecord | None = None
        is_sticker_like = _message_media_is_sticker_like(message)
        try:
            if active_state.deleted:
                return False
            policy = self._policy_for(chat_id, topic_id)
            if not policy.get("enabled", False):
                return False

            sent_at = to_iso(getattr(message, "date", None)) or utc_now_iso()
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

            if active_state.deleted:
                return False

            def _build_record(effective: dict[str, Any]) -> MessageRecord:
                nonlocal built_record
                capture_text = bool(effective.get("capture_text", True))
                capture_media_metadata = bool(effective.get("capture_media_metadata", True))
                download_media = bool(effective.get("download_media", False))
                download_stickers = bool(effective.get("download_stickers", False))
                message_text = getattr(message, "text", None) if capture_text else None
                if is_sticker_like:
                    message_text = _sticker_archive_text(message, include_message_text=capture_text)
                captures_media = (
                    bool(getattr(message, "media", None))
                    and (
                        download_stickers
                        if is_sticker_like
                        else capture_media_metadata or download_media
                    )
                )
                built_record = MessageRecord(
                    source=SOURCE_TELEGRAM,
                    account_id=self.account_id,
                    chat_id=chat_id,
                    message_id=message_id,
                    topic_id=topic_id,
                    sender_id=getattr(sender, "id", None) if sender else getattr(message, "sender_id", None),
                    sender_name=_display_name(sender),
                    sender_username=getattr(sender, "username", None) if sender else None,
                    sent_at=sent_at,
                    edited_at=to_iso(getattr(message, "edit_date", None)),
                    ingested_at=utc_now_iso(),
                    text=message_text,
                    has_media=captures_media,
                    media_kind=type(message.media).__name__ if captures_media else None,
                    grouped_id=str(message.grouped_id)
                    if captures_media and getattr(message, "grouped_id", None)
                    else None,
                    reply_to_message_id=getattr(message, "reply_to_msg_id", None),
                    forward_from_id=_forward_from_id(message),
                    forward_from_name=_forward_from_name(message),
                    permalink=_permalink(chat, message, chat_id=int(message.chat_id)),
                    reactions_json=json.dumps(_message_reactions(message), ensure_ascii=False, default=str)
                    if getattr(message, "reactions", None)
                    else None,
                    raw_json=json.dumps(_safe_dict(message), ensure_ascii=False, default=str)
                    if capture_media_metadata and (not is_sticker_like or download_stickers)
                    else None,
                )
                return built_record

            result = self.store.upsert_message_if_policy_enabled(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                chat_id=chat_id,
                topic_id=topic_id,
                event_type=event_type,
                build_record=_build_record,
            )
            if result is None or built_record is None:
                return False
            if chat:
                self.store.upsert_chat(_chat_record(self.account_id, chat, chat_id=chat_id))
            if sender:
                self.store.upsert_user(_user_record(self.account_id, sender))
        finally:
            self._unregister_inflight_fetch(chat_id, message_id, active_state)

        if self._should_download_media_for(
            chat_id,
            topic_id,
            is_sticker_like=is_sticker_like,
        ) and _message_media_downloadable(
            message,
            include_stickers=is_sticker_like,
        ):
            existing_media = (
                self.store.list_media_files(
                    account_id=self.account_id,
                    chat_id=built_record.chat_id,
                    message_id=built_record.message_id,
                )
                if event_type == "backfill"
                else []
            )
            if not existing_media:
                await self._download_message_media(
                    message,
                    built_record,
                    is_sticker_like=is_sticker_like,
                )
        self._update_capture_cursor(chat_id, topic_id, message_id, built_record.sent_at)
        return True


    async def _download_message_media(
        self,
        message: Any,
        record: MessageRecord,
        *,
        is_sticker_like: bool = False,
    ) -> None:
        target_dir = self.store.database_path.parent / "media" / self.account_id / str(record.chat_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        downloaded: str | None = None
        staging_dir: Path | None = None
        attempts = max(1, self.media_download.retries + 1)
        success_attempt = 0
        try:
            for attempt in range(1, attempts + 1):
                if not self._should_download_media_for(
                    record.chat_id,
                    record.topic_id,
                    is_sticker_like=is_sticker_like,
                ):
                    return
                attempt_dir = target_dir / f".part-{uuid.uuid4().hex}"
                attempt_dir.mkdir(parents=True, exist_ok=True)
                try:
                    downloaded = await message.download_media(file=str(attempt_dir))
                    staging_dir = attempt_dir
                    success_attempt = attempt
                    break
                except Exception as exc:
                    shutil.rmtree(attempt_dir, ignore_errors=True)
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
                except BaseException:
                    shutil.rmtree(attempt_dir, ignore_errors=True)
                    raise
            if not downloaded:
                self._record_operation(
                    "media_download",
                    "failed",
                    subject_type="message",
                    subject_id=f"{record.chat_id}/{record.message_id}",
                    error={"code": "media_download_empty", "message": "download_media returned no file path", "attempts": attempts},
                )
                return
            staged_path = Path(downloaded)
            if not staged_path.exists():
                self._record_operation(
                    "media_download",
                    "failed",
                    subject_type="message",
                    subject_id=f"{record.chat_id}/{record.message_id}",
                    error={"code": "media_download_missing", "message": "download_media file path does not exist", "attempts": attempts},
                )
                return

            def _prepare_media() -> MediaFileRecord:
                file_size = staged_path.stat().st_size
                final_path = _unique_media_target_path(target_dir, staged_path.name, record.message_id)
                media_record = MediaFileRecord(
                    source=SOURCE_TELEGRAM,
                    account_id=self.account_id,
                    chat_id=record.chat_id,
                    message_id=record.message_id,
                    file_path=str(final_path),
                    media_kind=record.media_kind,
                    file_size=file_size,
                    downloaded_at=utc_now_iso(),
                    raw_json=json.dumps({"downloaded": str(final_path)}, ensure_ascii=False),
                )
                os.replace(staged_path, final_path)
                return media_record

            def _rollback_media(media_record: MediaFileRecord) -> None:
                Path(media_record.file_path).unlink(missing_ok=True)

            published = self.store.upsert_media_file_if_policy_allows(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                chat_id=record.chat_id,
                topic_id=record.topic_id,
                is_sticker_like=is_sticker_like,
                prepare_media=_prepare_media,
                rollback_media=_rollback_media,
            )
            if published is None:
                return
            if success_attempt > 1:
                self._record_operation(
                    "media_download",
                    "ok",
                    subject_type="message",
                    subject_id=f"{record.chat_id}/{record.message_id}",
                    error={"attempts": success_attempt},
                )
        finally:
            if staging_dir is not None:
                shutil.rmtree(staging_dir, ignore_errors=True)


    async def _backfill_capture_targets(self, targets: list[CaptureTarget]) -> list[CaptureTarget]:
        async with self._backfill_lock:
            return await self._backfill_capture_targets_locked(targets)

    async def _backfill_capture_targets_locked(self, targets: list[CaptureTarget]) -> list[CaptureTarget]:
        if not self.backfill.enabled or not targets:
            return []
        from telethon.tl.types import MessageService

        targets = _dedupe_capture_targets(targets)
        root_chat_ids = {target.chat_id for target in targets if target.topic_id == 0}
        failed_targets: list[CaptureTarget] = []
        for target in targets:
            if target.topic_id and target.chat_id in root_chat_ids:
                continue
            policy = self._policy_for(target.chat_id, target.topic_id or None)
            if not policy.get("enabled", False):
                self.logger.info(
                    "Skipping backfill for disabled origin %s/%s/%s",
                    self.account_id,
                    target.chat_id,
                    target.topic_id,
                )
                continue
            history_scanned_through_id = 0
            backfill_head_message_id: int | None = None
            count = 0
            last_message_at: str | None = None
            cursor_status = ""
            try:
                cursor = self.store.get_capture_cursor(
                    SOURCE_TELEGRAM,
                    self.account_id,
                    target.chat_id,
                    target.topic_id,
                )
                history_scanned_through_id = (
                    int(cursor.get("history_scanned_through_id") or 0) if cursor else 0
                )
                last_message_at = cursor.get("last_message_at") if cursor else None
                cursor_status = str(cursor.get("backfill_status") or "") if cursor else ""
                backfill_head_message_id = await self._remote_history_head(target)
                migration_rescan_pending = cursor_status.startswith("migration_rescan")
                is_initial_backfill = not migration_rescan_pending and (
                    history_scanned_through_id == 0
                    and not (cursor and cursor.get("last_backfill_at"))
                )
                mode = (
                    "migration_rescan"
                    if migration_rescan_pending
                    else "initial" if is_initial_backfill else "catch_up"
                )
                self.logger.info(
                    "Backfilling account=%s chat=%s topic=%s scanned_through=%s head=%s mode=%s",
                    self.account_id,
                    target.chat_id,
                    target.topic_id,
                    history_scanned_through_id,
                    backfill_head_message_id,
                    mode,
                )
                self._save_backfill_cursor(
                    target,
                    history_scanned_through_id=history_scanned_through_id,
                    backfill_head_message_id=backfill_head_message_id,
                    status="migration_rescan_running" if migration_rescan_pending else "running",
                    error="",
                    count=0,
                    last_message_at=last_message_at,
                    raw_payload={
                        "mode": mode,
                        "head_message_id": backfill_head_message_id,
                    },
                )

                if is_initial_backfill and self.backfill.initial_limit > 0:
                    entries = await self._initial_history_messages(
                        target,
                        backfill_head_message_id,
                        MessageService,
                    )
                    page_high, page_count, page_last_message_at = await self._store_backfill_page(
                        entries,
                        MessageService,
                    )
                    count += page_count
                    if page_high:
                        last_message_at = page_last_message_at or last_message_at
                    # The initial_limit is an intentional retention boundary. Once
                    # that newest window succeeds, future runs continue after this
                    # fixed head instead of walking older history unexpectedly.
                    history_scanned_through_id = backfill_head_message_id
                elif backfill_head_message_id <= history_scanned_through_id:
                    pass
                else:
                    page_size = self.backfill.catch_up_limit
                    limit_arg = page_size if page_size > 0 else 1000
                    while history_scanned_through_id < backfill_head_message_id:
                        entries = await self._history_page(
                            target,
                            min_id=history_scanned_through_id,
                            max_id=backfill_head_message_id + 1,
                            limit=limit_arg,
                            message_service_type=MessageService,
                        )
                        if not entries:
                            history_scanned_through_id = backfill_head_message_id
                            break
                        page_high, page_count, page_last_message_at = await self._store_backfill_page(
                            entries,
                            MessageService,
                        )
                        if page_high <= history_scanned_through_id:
                            raise RuntimeError("Telegram history page did not advance its cursor")
                        history_scanned_through_id = min(page_high, backfill_head_message_id)
                        count += page_count
                        last_message_at = page_last_message_at or last_message_at
                        self._save_backfill_cursor(
                            target,
                            history_scanned_through_id=history_scanned_through_id,
                            backfill_head_message_id=backfill_head_message_id,
                            status=(
                                "migration_rescan_running"
                                if migration_rescan_pending
                                else "running"
                            ),
                            error="",
                            count=count,
                            last_message_at=last_message_at,
                            raw_payload={
                                "mode": mode,
                                "head_message_id": backfill_head_message_id,
                                "page_size": limit_arg,
                            },
                        )
                        if len(entries) < limit_arg:
                            history_scanned_through_id = backfill_head_message_id
                            break

                self._save_backfill_cursor(
                    target,
                    history_scanned_through_id=history_scanned_through_id,
                    backfill_head_message_id=backfill_head_message_id,
                    status="completed",
                    error="",
                    count=count,
                    last_message_at=last_message_at,
                    completed=True,
                    raw_payload={
                        "count": count,
                        "head_message_id": backfill_head_message_id,
                        "history_scanned_through_id": history_scanned_through_id,
                    },
                )
                self.logger.info(
                    "Backfilled %s messages for account=%s chat=%s topic=%s",
                    count,
                    self.account_id,
                    target.chat_id,
                    target.topic_id,
                )
            except Exception as exc:
                error = classify_telegram_exception(
                    exc,
                    default_code="backfill_failed",
                    default_auth_state="authorized",
                )
                self.logger.warning(
                    "Backfill failed for account=%s chat=%s topic=%s: %s",
                    self.account_id,
                    target.chat_id,
                    target.topic_id,
                    error.message,
                )
                self._save_backfill_cursor(
                    target,
                    history_scanned_through_id=history_scanned_through_id,
                    backfill_head_message_id=backfill_head_message_id,
                    status=(
                        "migration_rescan_failed"
                        if cursor_status.startswith("migration_rescan")
                        else "failed"
                    ),
                    error=error.message,
                    count=count,
                    last_message_at=last_message_at,
                    raw_payload=error.to_public_dict(),
                )
                self._record_operation(
                    "backfill",
                    "failed",
                    subject_type="origin",
                    subject_id=_origin_subject_id(target),
                    error=error.to_public_dict(),
                )
                if error.code not in {"access_denied", "needs_login"}:
                    failed_targets.append(target)
                    if error.retry_after is not None:
                        self._backfill_retry_wait_seconds = max(
                            self._backfill_retry_wait_seconds,
                            float(error.retry_after),
                        )
        return failed_targets

    async def _remote_history_head(self, target: CaptureTarget) -> int:
        kwargs: dict[str, Any] = {"limit": 1}
        if target.topic_id:
            kwargs["reply_to"] = target.topic_id
        get_messages = getattr(self.client, "get_messages", None)
        if callable(get_messages):
            result = await get_messages(target.chat_id, **kwargs)
            messages = _coerce_messages(result)
        else:
            messages = []
            async for message in self.client.iter_messages(target.chat_id, **kwargs):
                messages.append(message)
        return max((int(message.id) for message in messages), default=0)

    async def _initial_history_messages(
        self,
        target: CaptureTarget,
        head_message_id: int,
        message_service_type: type[Any],
    ) -> list[tuple[Any, _InflightFetchState | None]]:
        limit = self.backfill.initial_limit
        limit_arg = None if limit <= 0 else limit
        kwargs: dict[str, Any] = {
            "max_id": head_message_id + 1,
            "limit": limit_arg,
        }
        if target.topic_id:
            kwargs["reply_to"] = target.topic_id
        collected: list[tuple[Any, _InflightFetchState | None]] = []
        try:
            iter_messages = getattr(self.client, "iter_messages", None)
            if callable(iter_messages):
                async for message in iter_messages(target.chat_id, **kwargs):
                    if int(message.id) <= head_message_id:
                        state = (
                            None
                            if isinstance(message, message_service_type)
                            else self._register_inflight_fetch(int(message.chat_id), int(message.id))
                        )
                        collected.append((message, state))
            else:
                result = await self.client.get_messages(target.chat_id, **kwargs)
                for message in _coerce_messages(result):
                    if int(message.id) <= head_message_id:
                        state = (
                            None
                            if isinstance(message, message_service_type)
                            else self._register_inflight_fetch(int(message.chat_id), int(message.id))
                        )
                        collected.append((message, state))
        except BaseException:
            self._unregister_backfill_entries(collected)
            raise
        collected.sort(key=lambda item: int(item[0].id))
        return collected

    async def _history_page(
        self,
        target: CaptureTarget,
        *,
        min_id: int,
        max_id: int,
        limit: int | None,
        message_service_type: type[Any],
    ) -> list[tuple[Any, _InflightFetchState | None]]:
        kwargs: dict[str, Any] = {
            "min_id": min_id,
            "max_id": max_id,
            "reverse": True,
            "limit": limit,
        }
        if target.topic_id:
            kwargs["reply_to"] = target.topic_id
        collected: list[tuple[Any, _InflightFetchState | None]] = []
        try:
            async for message in self.client.iter_messages(target.chat_id, **kwargs):
                if min_id < int(message.id) < max_id:
                    state = (
                        None
                        if isinstance(message, message_service_type)
                        else self._register_inflight_fetch(int(message.chat_id), int(message.id))
                    )
                    collected.append((message, state))
        except BaseException:
            self._unregister_backfill_entries(collected)
            raise
        return collected

    async def _store_backfill_page(
        self,
        entries: list[tuple[Any, _InflightFetchState | None]],
        message_service_type: type[Any],
    ) -> tuple[int, int, str | None]:
        page_high = 0
        stored_count = 0
        last_message_at: str | None = None
        try:
            for message, fetch_state in entries:
                page_high = max(page_high, int(message.id))
                if isinstance(message, message_service_type):
                    continue
                stored = await self._store_message(
                    message,
                    event_type="backfill",
                    fetch_state=fetch_state,
                )
                if stored:
                    stored_count += 1
                    last_message_at = to_iso(getattr(message, "date", None)) or last_message_at
        finally:
            self._unregister_backfill_entries(entries)
        return page_high, stored_count, last_message_at

    def _save_backfill_cursor(
        self,
        target: CaptureTarget,
        *,
        history_scanned_through_id: int,
        backfill_head_message_id: int | None,
        status: str,
        error: str,
        count: int,
        last_message_at: str | None,
        raw_payload: dict[str, Any],
        completed: bool = False,
    ) -> None:
        now = utc_now_iso()
        self.store.upsert_capture_cursor(
            CaptureCursorRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                origin_id=target.chat_id,
                topic_id=target.topic_id,
                last_message_id=0,
                history_scanned_through_id=history_scanned_through_id,
                last_message_at=last_message_at,
                last_backfill_at=now if completed else None,
                backfill_head_message_id=backfill_head_message_id,
                backfill_status=status,
                backfill_error=error,
                backfill_count=count,
                updated_at=now,
                raw_json=json.dumps(raw_payload, ensure_ascii=False),
            )
        )

    def _policy_for(self, chat_id: int, topic_id: int | None = None) -> dict[str, Any]:
        return self.store.resolve_effective_backup_policy(
            SOURCE_TELEGRAM,
            self.account_id,
            int(chat_id),
            int(topic_id or 0),
        )

    def _update_capture_cursor(
        self,
        chat_id: int,
        topic_id: int | None,
        message_id: int,
        message_at: str | None,
    ) -> None:
        self.store.upsert_capture_cursor(
            CaptureCursorRecord(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                origin_id=chat_id,
                topic_id=topic_id or 0,
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
    top_id = getattr(reply_to, "reply_to_top_id", None)
    if getattr(reply_to, "forum_topic", False):
        return top_id or getattr(reply_to, "reply_to_msg_id", None)
    if top_id:
        return int(top_id)
    return None


def _dedupe_capture_targets(targets: list[CaptureTarget]) -> list[CaptureTarget]:
    unique = {(int(target.chat_id), int(target.topic_id or 0)) for target in targets}
    return [CaptureTarget(chat_id, topic_id) for chat_id, topic_id in sorted(unique)]


def _origin_subject_id(target: CaptureTarget) -> str:
    return f"{target.chat_id}/{target.topic_id}" if target.topic_id else str(target.chat_id)


def _coerce_messages(value: Any) -> list[Any]:
    if value is None:
        return []
    if hasattr(value, "id"):
        return [value]
    try:
        return list(value)
    except TypeError:
        return [value]


def _message_media_downloadable(message: Any, *, include_stickers: bool = False) -> bool:
    media = getattr(message, "media", None)
    if media is None:
        return False
    if _message_media_is_sticker_like(message) and not include_stickers:
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


def _message_media_is_sticker_like(message: Any) -> bool:
    sticker = getattr(message, "sticker", None)
    if sticker:
        return True
    media = getattr(message, "media", None)
    if media is None:
        return False
    if type(media).__name__ == "Document" and _document_has_sticker_attribute(media):
        return True
    document = getattr(media, "document", None)
    if _document_has_sticker_attribute(document):
        return True
    webpage = getattr(media, "webpage", None)
    if type(webpage).__name__ == "WebPage":
        return _document_has_sticker_attribute(getattr(webpage, "document", None))
    return False


def _sticker_archive_text(message: Any, *, include_message_text: bool) -> str:
    original_text = getattr(message, "text", None) if include_message_text else None
    original_text = str(original_text).strip() if original_text else ""
    emoji = _message_sticker_emoji(message)
    if original_text and emoji and original_text != emoji:
        return f"{original_text}\n{emoji}"
    return original_text or emoji or "[sticker]"


def _message_sticker_emoji(message: Any) -> str | None:
    file_info = getattr(message, "file", None)
    file_emoji = getattr(file_info, "emoji", None) if file_info is not None else None
    if file_emoji and str(file_emoji).strip():
        return str(file_emoji).strip()

    media = getattr(message, "media", None)
    documents = [getattr(message, "sticker", None)]
    if type(media).__name__ == "Document":
        documents.append(media)
    documents.append(getattr(media, "document", None))
    webpage = getattr(media, "webpage", None)
    if type(webpage).__name__ == "WebPage":
        documents.append(getattr(webpage, "document", None))

    sticker_attribute_types = {"DocumentAttributeSticker", "DocumentAttributeCustomEmoji"}
    for document in documents:
        for attribute in getattr(document, "attributes", []) or []:
            if type(attribute).__name__ not in sticker_attribute_types:
                continue
            alt = getattr(attribute, "alt", None)
            if alt and str(alt).strip():
                return str(alt).strip()
    return None


def _document_has_sticker_attribute(document: Any) -> bool:
    if document is None:
        return False
    sticker_attribute_types = {"DocumentAttributeSticker", "DocumentAttributeCustomEmoji"}
    return any(type(attribute).__name__ in sticker_attribute_types for attribute in getattr(document, "attributes", []) or [])


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


def _unique_media_target_path(target_dir: Path, filename: str, message_id: int) -> Path:
    clean_name = Path(filename).name if filename else ""
    if not clean_name or clean_name in {".", ".."}:
        clean_name = f"msg-{message_id}.bin"
    candidate = target_dir / clean_name
    if not candidate.exists():
        return candidate
    stem = candidate.stem or f"msg-{message_id}"
    suffix = candidate.suffix
    numbered = target_dir / f"{stem}_m{message_id}{suffix}"
    if not numbered.exists():
        return numbered
    index = 1
    while True:
        alt = target_dir / f"{stem}_m{message_id}_{index}{suffix}"
        if not alt.exists():
            return alt
        index += 1


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
