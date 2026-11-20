from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone as dt_timezone
import json
import logging
import os
import shutil
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


class _MediaDownloadResultError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


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
        self._media_wakeup = asyncio.Event()
        self._media_worker_task: asyncio.Task[None] | None = None
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
        self._closed = False
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
        self._start_media_worker()
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
        self._media_wakeup.set()

    async def stop(self) -> None:
        self._closed = True
        self._retry_targets.clear()
        self._media_wakeup.set()
        tasks: list[asyncio.Task[None]] = []
        task = self._retry_task
        self._retry_task = None
        if task is not None and not task.done():
            task.cancel()
            tasks.append(task)
        media_task = self._media_worker_task
        self._media_worker_task = None
        if media_task is not None and not media_task.done():
            media_task.cancel()
            tasks.append(media_task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _start_media_worker(self) -> None:
        if self._closed:
            return
        if self._media_worker_task is not None and not self._media_worker_task.done():
            self._media_wakeup.set()
            return
        self._media_worker_task = asyncio.create_task(
            self._media_worker_loop(),
            name=f"telegram-media-worker-{self.account_id}",
        )
        self._media_wakeup.set()

    async def _media_worker_loop(self) -> None:
        cleanup_retry_count = 0
        cleanup_retry_cap = 8
        try:
            try:
                self._cleanup_orphan_media_artifacts()
            except Exception as exc:
                self.logger.warning(
                    "Failed to clean up orphan media artifacts for account=%s: %s",
                    self.account_id,
                    exc,
                )
            while not self._closed:
                self._media_wakeup.clear()
                if self.client is None:
                    await self._wait_media_wakeup(1.0)
                    continue
                is_connected = getattr(self.client, "is_connected", None)
                if callable(is_connected) and not is_connected():
                    await self._wait_media_wakeup(1.0)
                    continue
                try:
                    job = self.store.claim_media_download_job(
                        self.account_id,
                        source=SOURCE_TELEGRAM,
                        stale_seconds=self.media_download.stale_seconds,
                    )
                except Exception as exc:
                    self.logger.warning(
                        "Failed to claim media download job for account=%s: %s",
                        self.account_id,
                        exc,
                    )
                    await self._wait_media_wakeup(1.0)
                    continue
                if job is not None:
                    cleanup_retry_count = 0
                    try:
                        await self._execute_media_download_job(job)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self.logger.warning(
                            "Media download job %s failed during state persistence for account=%s: %s",
                            job.get("job_id"),
                            self.account_id,
                            exc,
                        )
                        await self._wait_media_wakeup(1.0)
                    continue
                has_remaining_cleanup = False
                try:
                    has_remaining_cleanup = self._cleanup_orphan_media_artifacts()
                except Exception as exc:
                    has_remaining_cleanup = True
                    self.logger.warning(
                        "Failed to clean up orphan media artifacts for account=%s: %s",
                        self.account_id,
                        exc,
                    )
                if not has_remaining_cleanup:
                    cleanup_retry_count = 0
                try:
                    next_due = self.store.next_media_download_due_at(
                        self.account_id,
                        source=SOURCE_TELEGRAM,
                        stale_seconds=self.media_download.stale_seconds,
                    )
                except Exception as exc:
                    self.logger.warning(
                        "Failed to inspect media download queue for account=%s: %s",
                        self.account_id,
                        exc,
                    )
                    await self._wait_media_wakeup(1.0)
                    continue
                if next_due is None:
                    if has_remaining_cleanup and cleanup_retry_count < cleanup_retry_cap:
                        base_delay = max(0.05, float(self.media_download.retry_delay_seconds or 0.05))
                        delay = min(base_delay * (2 ** cleanup_retry_count), 60.0)
                        cleanup_retry_count += 1
                        await self._wait_media_wakeup(delay)
                        continue
                    await self._media_wakeup.wait()
                    cleanup_retry_count = 0
                    continue
                sleep_seconds = _seconds_until_iso(next_due)
                if has_remaining_cleanup and cleanup_retry_count < cleanup_retry_cap:
                    base_delay = max(0.05, float(self.media_download.retry_delay_seconds or 0.05))
                    delay = min(base_delay * (2 ** cleanup_retry_count), 60.0)
                    cleanup_retry_count += 1
                    sleep_seconds = min(sleep_seconds, delay) if sleep_seconds > 0 else delay
                if sleep_seconds > 0:
                    await self._wait_media_wakeup(min(sleep_seconds, 60.0))
        except asyncio.CancelledError:
            raise
        finally:
            if self._media_worker_task is asyncio.current_task():
                self._media_worker_task = None

    def _cleanup_orphan_media_artifacts(self) -> bool:
        """Remove stale job staging directories and uncommitted pending publish files after crashes.

        Returns True when any orphan artifact could not be removed and should be retried later.
        """
        has_remaining = False
        account_media_dir = self.store.database_path.parent / "media" / self.account_id
        stale_seconds = max(0.0, float(self.media_download.stale_seconds))
        stale_before = (
            datetime.now(dt_timezone.utc) - timedelta(seconds=stale_seconds)
        ).isoformat()
        if account_media_dir.exists():
            for chat_dir in account_media_dir.iterdir():
                if not chat_dir.is_dir():
                    continue
                for entry in chat_dir.iterdir():
                    if not entry.is_dir():
                        continue
                    if entry.name.startswith(".part-"):
                        parsed = _parse_staging_dir_name(entry.name)
                        if parsed is None:
                            continue
                        job_id, token = parsed
                        job = self.store.get_media_download_job(job_id)
                        should_remove = job is None
                        if job is not None:
                            status = str(job.get("status") or "")
                            current_token = int(job.get("claim_token") or 0)
                            updated_at = str(job.get("updated_at") or "")
                            should_remove = (
                                status != "downloading"
                                or current_token > token
                                or updated_at <= stale_before
                            )
                        if should_remove:
                            try:
                                shutil.rmtree(entry)
                            except OSError:
                                if entry.exists():
                                    has_remaining = True
                    elif entry.name.startswith("mdj_") and "-t" in entry.name:
                        try:
                            next(entry.iterdir())
                        except StopIteration:
                            try:
                                entry.rmdir()
                            except OSError:
                                pass
                        except OSError:
                            pass
        for pending in self.store.list_media_jobs_with_pending_paths(
            account_id=self.account_id,
            source=SOURCE_TELEGRAM,
            stale_seconds=stale_seconds,
        ):
            cleared, _unlinked = self.store.cleanup_media_download_pending_path(
                str(pending["job_id"]),
                expected_claim_token=int(pending["claim_token"]),
                require_downloading=False,
                stale_seconds=stale_seconds,
            )
            if not cleared:
                has_remaining = True
        return has_remaining

    def _cleanup_prior_job_staging_dirs(self, target_dir: Path, job_id: str, current_token: int) -> None:
        if not target_dir.exists():
            return
        prefix = f".part-{job_id}-t"
        for entry in target_dir.iterdir():
            if not entry.is_dir() or not entry.name.startswith(prefix):
                continue
            parsed = _parse_staging_dir_name(entry.name)
            if parsed is not None and parsed[0] == job_id and parsed[1] < current_token:
                shutil.rmtree(entry, ignore_errors=True)

    async def _wait_media_wakeup(self, timeout: float) -> None:
        if timeout <= 0:
            return
        try:
            await asyncio.wait_for(self._media_wakeup.wait(), timeout=timeout)
        except TimeoutError:
            return

    async def process_pending_media_downloads(self, *, max_jobs: int | None = None) -> int:
        """Deterministically drain currently due media download jobs for this account."""
        self._cleanup_orphan_media_artifacts()
        processed = 0
        while max_jobs is None or processed < max_jobs:
            job = self.store.claim_media_download_job(
                self.account_id,
                source=SOURCE_TELEGRAM,
                stale_seconds=self.media_download.stale_seconds,
            )
            if job is None:
                self._cleanup_orphan_media_artifacts()
                break
            await self._execute_media_download_job(job)
            processed += 1
        return processed

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

            enqueued_media = False

            def _build_media_job(
                effective: dict[str, Any],
                record: MessageRecord,
            ) -> dict[str, Any] | None:
                nonlocal enqueued_media
                download_media = bool(effective.get("download_media", False))
                download_stickers = bool(effective.get("download_stickers", False))
                should_download = download_stickers if is_sticker_like else download_media
                if not should_download or not _message_media_downloadable(
                    message,
                    include_stickers=is_sticker_like,
                ):
                    return None
                enqueued_media = True
                return {
                    "media_kind": record.media_kind,
                    "is_sticker_like": is_sticker_like,
                    "max_attempts": self.media_download.effective_max_attempts(),
                    "replace_existing": event_type == "edit",
                    "target_edited_at": record.edited_at,
                }

            result = self.store.upsert_message_if_policy_enabled(
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                chat_id=chat_id,
                topic_id=topic_id,
                event_type=event_type,
                build_record=_build_record,
                build_media_job=_build_media_job,
            )
            if result is None or built_record is None:
                return False
            if chat:
                self.store.upsert_chat(_chat_record(self.account_id, chat, chat_id=chat_id))
            if sender:
                self.store.upsert_user(_user_record(self.account_id, sender))
        finally:
            self._unregister_inflight_fetch(chat_id, message_id, active_state)

        if enqueued_media:
            self._media_wakeup.set()
        self._update_capture_cursor(chat_id, topic_id, message_id, built_record.sent_at)
        return True

    async def _execute_media_download_job(self, job: dict[str, Any]) -> None:
        from telethon.tl.types import MessageService

        job_id = str(job["job_id"])
        claim_token = int(job["claim_token"])
        attempts = int(job["attempts"])
        max_attempts = int(job["max_attempts"])
        chat_id = int(job["chat_id"])
        message_id = int(job["message_id"])
        topic_id = int(job.get("topic_id") or 0)
        is_sticker_like = bool(job.get("is_sticker_like", False))
        replace_existing = bool(job.get("replace_existing", False))
        effective_is_sticker = is_sticker_like
        media_kind = job.get("media_kind")
        target_dir = self.store.database_path.parent / "media" / self.account_id / str(chat_id)
        attempt_dir: Path | None = None
        heartbeat_task: asyncio.Task[None] | None = None

        async def _heartbeat_loop() -> None:
            interval = max(0.05, min(float(self.media_download.stale_seconds) / 3.0, 30.0))
            while True:
                await asyncio.sleep(interval)
                try:
                    alive = self.store.heartbeat_media_download_job(
                        job_id,
                        expected_claim_token=claim_token,
                    )
                    if not alive:
                        return
                except Exception:
                    return

        try:
            # Start lease heartbeat immediately upon claim so slow get_messages() RPCs never expire the lease.
            heartbeat_task = asyncio.create_task(
                _heartbeat_loop(),
                name=f"media-download-heartbeat-{job_id}",
            )
            self._cleanup_prior_job_staging_dirs(target_dir, job_id, claim_token)
            if job.get("pending_file_path"):
                cleared, _unlinked = self.store.cleanup_media_download_pending_path(
                    job_id,
                    expected_claim_token=claim_token,
                    require_downloading=True,
                )
                if not cleared:
                    raise _MediaDownloadResultError(
                        "media_cleanup_failed",
                        "previous uncommitted media file could not be cleaned up",
                    )

            if not replace_existing:
                existing_media = self.store.list_media_files(
                    account_id=self.account_id,
                    chat_id=chat_id,
                    message_id=message_id,
                )
                if existing_media:
                    self.store.complete_media_download_job(job_id, expected_claim_token=claim_token)
                    return

            stored_message = self.store.get_message(SOURCE_TELEGRAM, self.account_id, chat_id, message_id)
            if stored_message is None or stored_message.get("deleted_at") is not None:
                self.store.cancel_media_download_job(
                    job_id,
                    reason="message_deleted",
                    expected_claim_token=claim_token,
                )
                return
            topic_id = int(stored_message.get("topic_id") or topic_id or 0)

            if not self._should_download_media_for(
                chat_id,
                topic_id,
                is_sticker_like=is_sticker_like,
            ):
                self.store.cancel_media_download_job(
                    job_id,
                    reason="policy_disabled",
                    expected_claim_token=claim_token,
                )
                return

            if self.client is None:
                self.store.release_media_download_job(
                    job_id,
                    error="client_unavailable",
                    expected_claim_token=claim_token,
                )
                return

            fetched = await self.client.get_messages(chat_id, ids=message_id)
            refreshed_messages = _coerce_messages(fetched)
            refreshed_msg = next(
                (
                    item
                    for item in refreshed_messages
                    if int(getattr(item, "id", 0)) == message_id
                    and (
                        getattr(item, "chat_id", None) is None
                        or int(getattr(item, "chat_id")) == chat_id
                    )
                ),
                None,
            )
            if refreshed_msg is None or isinstance(refreshed_msg, MessageService):
                self.store.cancel_media_download_job(
                    job_id,
                    reason="media_unavailable",
                    expected_claim_token=claim_token,
                )
                return

            refreshed_is_sticker = _message_media_is_sticker_like(refreshed_msg)
            effective_is_sticker = refreshed_is_sticker
            if not self._should_download_media_for(
                chat_id,
                topic_id,
                is_sticker_like=refreshed_is_sticker,
            ):
                self.store.cancel_media_download_job(
                    job_id,
                    reason="policy_disabled",
                    expected_claim_token=claim_token,
                )
                return

            if not _message_media_downloadable(refreshed_msg, include_stickers=refreshed_is_sticker):
                self.store.cancel_media_download_job(
                    job_id,
                    reason="media_unavailable",
                    expected_claim_token=claim_token,
                )
                return

            refreshed_media_kind = (
                type(refreshed_msg.media).__name__
                if getattr(refreshed_msg, "media", None) is not None
                else media_kind
            )
            target_dir.mkdir(parents=True, exist_ok=True)
            attempt_dir = target_dir / f".part-{job_id}-t{claim_token}"
            if attempt_dir.exists():
                shutil.rmtree(attempt_dir, ignore_errors=True)
            attempt_dir.mkdir(parents=True, exist_ok=True)

            downloaded = await refreshed_msg.download_media(file=str(attempt_dir))
            if not downloaded:
                raise _MediaDownloadResultError(
                    "media_download_empty",
                    "download_media returned no file path",
                )
            staged_path = Path(downloaded)
            if not staged_path.exists():
                raise _MediaDownloadResultError(
                    "media_download_missing",
                    "download_media file path does not exist",
                )

            planned_final_path: Path | None = None
            for suffix_index in range(8):
                candidate_path = _exclusive_media_target_path(
                    target_dir,
                    staged_path.name,
                    job_id=job_id,
                    claim_token=claim_token,
                    suffix_index=suffix_index,
                )
                if candidate_path.exists():
                    continue
                reserve_status = self.store.record_media_download_pending_path(
                    job_id,
                    expected_claim_token=claim_token,
                    pending_file_path=str(candidate_path),
                )
                if reserve_status == "ok":
                    planned_final_path = candidate_path
                    break
                if reserve_status == "stale_claim":
                    return
                if reserve_status == "pending_uncleaned":
                    raise _MediaDownloadResultError(
                        "media_cleanup_failed",
                        "previous uncommitted media file could not be cleaned up",
                    )
            if planned_final_path is None:
                raise _MediaDownloadResultError(
                    "media_path_conflict",
                    "failed to reserve an exclusive media publish path",
                )

            def _prepare_media() -> MediaFileRecord:
                file_size = staged_path.stat().st_size
                media_record = MediaFileRecord(
                    source=SOURCE_TELEGRAM,
                    account_id=self.account_id,
                    chat_id=chat_id,
                    message_id=message_id,
                    file_path=str(planned_final_path),
                    media_kind=refreshed_media_kind,
                    file_size=file_size,
                    downloaded_at=utc_now_iso(),
                    raw_json=json.dumps({"downloaded": str(planned_final_path)}, ensure_ascii=False),
                )
                planned_final_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(staged_path, planned_final_path)
                return media_record

            def _rollback_media(media_record: MediaFileRecord) -> None:
                path = Path(media_record.file_path)
                path.unlink(missing_ok=True)
                try:
                    path.parent.rmdir()
                except OSError:
                    pass

            outcome, _published = self.store.complete_media_download_with_file(
                job_id=job_id,
                expected_claim_token=claim_token,
                source=SOURCE_TELEGRAM,
                account_id=self.account_id,
                chat_id=chat_id,
                topic_id=topic_id,
                is_sticker_like=refreshed_is_sticker,
                prepare_media=_prepare_media,
                rollback_media=_rollback_media,
            )
            if outcome == "completed" and attempts > 1:
                self._record_operation(
                    "media_download",
                    "ok",
                    subject_type="message",
                    subject_id=f"{chat_id}/{message_id}",
                    error={"attempts": attempts, "job_id": job_id},
                )
        except asyncio.CancelledError:
            if attempt_dir is not None:
                shutil.rmtree(attempt_dir, ignore_errors=True)
                attempt_dir = None
            try:
                self.store.release_media_download_job(
                    job_id,
                    error="worker_stopped",
                    expected_claim_token=claim_token,
                )
            except Exception as exc:
                self.logger.debug("Failed to release media download job %s on cancel: %s", job_id, exc)
            raise
        except Exception as exc:
            if attempt_dir is not None:
                shutil.rmtree(attempt_dir, ignore_errors=True)
                attempt_dir = None
            if not self._should_download_media_for(
                chat_id,
                topic_id,
                is_sticker_like=effective_is_sticker,
            ):
                self.store.cancel_media_download_job(
                    job_id,
                    reason="policy_disabled",
                    expected_claim_token=claim_token,
                )
                return
            if isinstance(exc, _MediaDownloadResultError):
                error = classify_telegram_exception(exc, default_code=exc.code)
            else:
                error = classify_telegram_exception(exc, default_code="media_download_failed")
            self.logger.warning(
                "Failed to download media for account=%s chat=%s message=%s attempt=%s/%s: %s",
                self.account_id,
                chat_id,
                message_id,
                attempts,
                max_attempts,
                error.message,
            )
            if error.code == "flood_wait":
                wait_seconds = max(1.0, float(error.retry_after or 30))
                next_at = (datetime.now(dt_timezone.utc) + timedelta(seconds=wait_seconds)).isoformat()
                self.store.retry_media_download_job(
                    job_id,
                    error=error.message,
                    error_code=error.code,
                    next_attempt_at=next_at,
                    expected_claim_token=claim_token,
                    preserve_attempt_budget=True,
                )
                return
            if error.code in {"access_denied", "needs_login"}:
                failed = self.store.fail_media_download_job(
                    job_id,
                    error=error.message,
                    error_code=error.code,
                    expected_claim_token=claim_token,
                )
                if failed:
                    self._record_operation(
                        "media_download",
                        "failed",
                        subject_type="message",
                        subject_id=f"{chat_id}/{message_id}",
                        error=error.to_public_dict() | {"attempts": attempts, "job_id": job_id},
                    )
                return
            base_delay = max(0.0, float(self.media_download.retry_delay_seconds))
            backoff = min(
                base_delay * (2 ** max(0, attempts - 1)),
                max(0.0, float(self.media_download.max_retry_delay_seconds)),
            )
            next_at = (datetime.now(dt_timezone.utc) + timedelta(seconds=backoff)).isoformat()
            result_status = self.store.retry_media_download_job(
                job_id,
                error=error.message,
                error_code=error.code,
                next_attempt_at=next_at,
                expected_claim_token=claim_token,
                preserve_attempt_budget=False,
            )
            if result_status == "failed":
                self._record_operation(
                    "media_download",
                    "failed",
                    subject_type="message",
                    subject_id=f"{chat_id}/{message_id}",
                    error=error.to_public_dict() | {"attempts": attempts, "job_id": job_id},
                )
        finally:
            if heartbeat_task is not None and not heartbeat_task.done():
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
            if attempt_dir is not None:
                shutil.rmtree(attempt_dir, ignore_errors=True)


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
        return [item for item in value if item is not None]
    except TypeError:
        return [value]


def _parse_staging_dir_name(name: str) -> tuple[str, int] | None:
    if not name.startswith(".part-"):
        return None
    body = name[len(".part-") :]
    job_id, sep, token_str = body.rpartition("-t")
    if not sep or not job_id or not token_str.isdigit():
        return None
    return job_id, int(token_str)


def _seconds_until_iso(iso_timestamp: str) -> float:
    try:
        target = datetime.fromisoformat(iso_timestamp)
        if target.tzinfo is None:
            target = target.replace(tzinfo=dt_timezone.utc)
        delta = (target - datetime.now(dt_timezone.utc)).total_seconds()
        return max(0.0, delta)
    except ValueError:
        return 0.0


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


def _exclusive_media_target_path(
    target_dir: Path,
    filename: str,
    *,
    job_id: str,
    claim_token: int,
    suffix_index: int = 0,
) -> Path:
    """Return a publish path inside a per-claim exclusive subdirectory while preserving the original filename."""
    clean_name = Path(filename).name if filename else ""
    if not clean_name or clean_name in {".", ".."}:
        clean_name = "media.bin"
    dir_name = f"{job_id}-t{claim_token}" if suffix_index == 0 else f"{job_id}-t{claim_token}-{suffix_index}"
    return target_dir / dir_name / clean_name


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
