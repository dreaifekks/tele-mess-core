from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable

from telethon import functions

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import TelegramAccountConfig
from tele_mess_core.models import (
    AccountAuthRecord,
    OperationEventRecord,
    OriginRecord,
    ParticipantRecord,
    SOURCE_TELEGRAM,
    to_iso,
    utc_now_iso,
)
from tele_mess_core.telegram.runtime import classify_telegram_exception


MAX_TOPIC_DISCOVERY_LIMIT = 5000
MAX_PARTICIPANT_REFRESH_LIMIT = 10000


class TelegramDiscoveryService:
    def __init__(
        self,
        config: TelegramAccountConfig,
        store: ArchiveStore,
        *,
        error_persister: Callable[[Callable[[], Any]], None] | None = None,
        auth_probe_provider: Callable[[], tuple[int, str, int]] | None = None,
        account_generation: int | None = None,
        store_write_runner: Callable[[Callable[[], Any]], Awaitable[Any]] | None = None,
    ):
        self.config = config
        self.account_id = config.account_id
        self.store = store
        self._error_persister = error_persister
        self._auth_probe_provider = auth_probe_provider
        self._account_generation = account_generation
        self._store_write_runner = store_write_runner
        self.logger = logging.getLogger(__name__)

    async def _run_write(self, writer: Callable[[], Any]) -> Any:
        if self._store_write_runner is not None:
            return await self._store_write_runner(writer)
        return writer()

    async def discover_origins(
        self,
        include_topics: bool = True,
        topic_limit: int = 100,
        include_private: bool = False,
        *,
        client: Any,
        progress: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        probe = self._auth_probe_provider() if self._auth_probe_provider is not None else None
        effective_topic_limit, requested_topic_limit = _bounded_discovery_limit(topic_limit, MAX_TOPIC_DISCOVERY_LIMIT)
        state = progress if progress is not None else {}
        state.setdefault("authorized", False)
        state.setdefault("origins", 0)
        state.setdefault("topics", 0)
        state.setdefault("private_skipped", 0)
        state.setdefault("errors", [])
        state.setdefault("topics_truncated", False)
        state["topic_limit"] = effective_topic_limit
        state["include_private"] = include_private
        try:
            if not await client.is_user_authorized():
                self._set_auth_state("needs_login", probe=probe)
                return {"account_id": self.account_id, "authorized": False, "status": "needs_login", "origins": 0, "topics": 0}

            self._set_auth_state("authorized", probe=probe)
            state["authorized"] = True
            try:
                async for dialog in client.iter_dialogs():
                    try:
                        entity = getattr(dialog, "entity", None)
                        origin_id = int(getattr(dialog, "id"))
                        origin_type = _dialog_origin_type(dialog, entity)
                        if origin_type == "private" and not include_private:
                            state["private_skipped"] = int(state["private_skipped"]) + 1
                            continue
                        is_forum = bool(getattr(entity, "forum", False))
                        origin_record = OriginRecord(
                            source=SOURCE_TELEGRAM,
                            account_id=self.account_id,
                            origin_id=origin_id,
                            origin_type=origin_type,
                            title=getattr(dialog, "title", None) or _display_name(entity),
                            username=getattr(entity, "username", None),
                            is_forum=is_forum,
                            last_message_at=_dialog_last_message_at(dialog),
                            updated_at=utc_now_iso(),
                            raw_json=json.dumps(_safe_dict(entity), ensure_ascii=False, default=str),
                        )
                        await self._run_write(lambda rec=origin_record: self.store.upsert_origin(rec))
                        state["origins"] = int(state["origins"]) + 1
                        if include_topics and is_forum:
                            topic_result = await self._discover_topics(
                                client,
                                entity,
                                origin_id,
                                effective_topic_limit,
                                progress=state,
                            )
                            state["topics_truncated"] = bool(state["topics_truncated"]) or bool(topic_result["truncated"])
                    except Exception as exc:
                        error = classify_telegram_exception(exc, default_code="origin_discovery_failed")
                        payload = error.to_public_dict()
                        state["errors"].append(payload)
                        self._record_operation_diagnostic(
                            "discover_origins",
                            "failed",
                            subject_type="origin",
                            subject_id=str(getattr(dialog, "id", "unknown")),
                            error=payload,
                        )
            except Exception as exc:
                error = classify_telegram_exception(exc, default_code="dialog_discovery_failed")
                payload = error.to_public_dict()
                state["errors"].append(payload)
                self._record_operation_diagnostic("discover_origins", "failed", error=payload)
            if requested_topic_limit > effective_topic_limit:
                state["topics_truncated"] = True
            origin_count = int(state["origins"])
            topic_count = int(state["topics"])
            private_skipped = int(state["private_skipped"])
            errors = list(state["errors"])
            topics_truncated = bool(state["topics_truncated"])
            status = "ok" if not errors else "partial" if (origin_count or topic_count) else "failed"
            return {
                "account_id": self.account_id,
                "authorized": True,
                "status": status,
                "origins": origin_count,
                "topics": topic_count,
                "private_skipped": private_skipped,
                "errors": errors,
                "topics_truncated": topics_truncated,
                "topic_limit": effective_topic_limit,
                "include_private": include_private,
            }
        except Exception as exc:
            if self._error_persister is not None:
                result = self.discover_origins_error_result(exc, persist=False, progress=state)
                self._error_persister(lambda err=exc: self.discover_origins_error_result(err, persist=True))
                return result
            return self.discover_origins_error_result(exc, progress=state)

    def discover_origins_error_result(
        self,
        exc: Exception,
        *,
        persist: bool = True,
        progress: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        error = classify_telegram_exception(exc, default_code="discover_origins_failed")
        payload = error.to_public_dict()
        origins = int((progress or {}).get("origins", 0))
        topics = int((progress or {}).get("topics", 0))
        prior_errors = list((progress or {}).get("errors") or [])
        result: dict[str, Any] = {
            "account_id": self.account_id,
            "authorized": bool((progress or {}).get("authorized", False)),
            "status": "partial" if (origins > 0 or topics > 0) else "failed",
            "origins": origins,
            "topics": topics,
            "errors": [*prior_errors, payload],
            "topics_truncated": bool((progress or {}).get("topics_truncated", False)),
        }
        if progress and "private_skipped" in progress:
            result["private_skipped"] = int(progress["private_skipped"])
        if progress and "topic_limit" in progress:
            result["topic_limit"] = int(progress["topic_limit"])
        if progress and "include_private" in progress:
            result["include_private"] = bool(progress["include_private"])
        if persist:
            try:
                self._record_operation("discover_origins", "failed", error=payload)
            except Exception:
                self.logger.warning(
                    "Failed to persist discover_origins error for account %s",
                    self.account_id,
                    exc_info=True,
                )
        return result

    async def refresh_participants(
        self,
        origin_id: int,
        limit: int = 500,
        *,
        client: Any,
        progress: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        probe = self._auth_probe_provider() if self._auth_probe_provider is not None else None
        effective_limit, requested_limit = _bounded_discovery_limit(limit, MAX_PARTICIPANT_REFRESH_LIMIT)
        if progress is not None:
            progress.setdefault("authorized", False)
            progress.setdefault("participants", 0)
            progress.setdefault("errors", [])
            progress["participants_truncated"] = requested_limit > effective_limit
            progress["limit"] = effective_limit
        try:
            if not await client.is_user_authorized():
                self._set_auth_state("needs_login", probe=probe)
                return {
                    "account_id": self.account_id,
                    "origin_id": origin_id,
                    "authorized": False,
                    "status": "needs_login",
                    "participants": 0,
                }

            self._set_auth_state("authorized", probe=probe)
            if progress is not None:
                progress["authorized"] = True
            count = 0
            errors: list[dict[str, Any]] = []
            try:
                async for user in client.iter_participants(origin_id, limit=effective_limit):
                    try:
                        participant_record = ParticipantRecord(
                            source=SOURCE_TELEGRAM,
                            account_id=self.account_id,
                            origin_id=origin_id,
                            user_id=int(getattr(user, "id")),
                            username=getattr(user, "username", None),
                            display_name=_display_name(user),
                            is_bot=bool(getattr(user, "bot", False)),
                            updated_at=utc_now_iso(),
                            raw_json=json.dumps(_safe_dict(user), ensure_ascii=False, default=str),
                        )
                        await self._run_write(lambda rec=participant_record: self.store.upsert_participant(rec))
                        count += 1
                        if progress is not None:
                            progress["participants"] = count
                    except Exception as exc:
                        error = classify_telegram_exception(exc, default_code="participant_persist_failed")
                        payload = error.to_public_dict()
                        errors.append(payload)
                        if progress is not None:
                            progress["errors"].append(payload)
                        self._record_operation_diagnostic(
                            "refresh_participants",
                            "failed",
                            subject_type="participant",
                            subject_id=str(getattr(user, "id", "unknown")),
                            error=payload,
                        )
            except Exception as exc:
                error = classify_telegram_exception(exc, default_code="participant_refresh_failed")
                payload = error.to_public_dict()
                errors.append(payload)
                if progress is not None:
                    progress["errors"].append(payload)
                self._record_operation_diagnostic(
                    "refresh_participants",
                    "failed",
                    subject_type="origin",
                    subject_id=str(origin_id),
                    error=payload,
                )
            status = "ok" if not errors else "partial" if count else "failed"
            return {
                "account_id": self.account_id,
                "origin_id": origin_id,
                "authorized": True,
                "status": status,
                "participants": count,
                "errors": errors,
                "participants_truncated": requested_limit > effective_limit,
                "limit": effective_limit,
            }
        except Exception as exc:
            if self._error_persister is not None:
                result = self.refresh_participants_error_result(origin_id, exc, persist=False, progress=progress)
                self._error_persister(
                    lambda oid=origin_id, err=exc: self.refresh_participants_error_result(oid, err, persist=True)
                )
                return result
            return self.refresh_participants_error_result(origin_id, exc, progress=progress)

    def refresh_participants_error_result(
        self,
        origin_id: int,
        exc: Exception,
        *,
        persist: bool = True,
        progress: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        error = classify_telegram_exception(exc, default_code="refresh_participants_failed")
        payload = error.to_public_dict()
        participants = int((progress or {}).get("participants", 0))
        prior_errors = list((progress or {}).get("errors") or [])
        result: dict[str, Any] = {
            "account_id": self.account_id,
            "origin_id": origin_id,
            "authorized": bool((progress or {}).get("authorized", False)),
            "status": "partial" if participants > 0 else "failed",
            "participants": participants,
            "errors": [*prior_errors, payload],
        }
        if progress and "participants_truncated" in progress:
            result["participants_truncated"] = bool(progress["participants_truncated"])
        if progress and "limit" in progress:
            result["limit"] = int(progress["limit"])
        if persist:
            try:
                self._record_operation(
                    "refresh_participants",
                    "failed",
                    subject_type="origin",
                    subject_id=str(origin_id),
                    error=payload,
                )
            except Exception:
                self.logger.warning(
                    "Failed to persist refresh_participants error for account %s",
                    self.account_id,
                    exc_info=True,
                )
        return result

    async def _discover_topics(
        self,
        client: Any,
        entity: Any,
        origin_id: int,
        topic_limit: int,
        *,
        progress: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        errors: list[dict[str, Any]] = []

        count = 0
        base_topics = int((progress or {}).get("topics", 0))
        offset_id = 0
        offset_topic = 0
        max_topics = max(1, min(topic_limit, MAX_TOPIC_DISCOVERY_LIMIT))
        while count < max_topics:
            batch_limit = min(100, max_topics - count)
            if batch_limit <= 0:
                break
            try:
                result = await client(
                    self._forum_topics_request(
                        functions,
                        entity,
                        offset_id=offset_id,
                        offset_topic=offset_topic,
                        limit=batch_limit,
                    )
                )
            except Exception as exc:
                error = classify_telegram_exception(
                    exc,
                    default_code="topic_discovery_failed",
                    default_auth_state="authorized",
                )
                payload = error.to_public_dict()
                errors.append(payload)
                if progress is not None:
                    progress["errors"].append(payload)
                self._record_operation_diagnostic(
                    "discover_topics",
                    "failed",
                    subject_type="origin",
                    subject_id=str(origin_id),
                    error=payload,
                )
                break

            topics = list(getattr(result, "topics", []) or [])[: max_topics - count]
            if not topics:
                break
            for topic in topics:
                topic_id = int(getattr(topic, "id"))
                topic_record = OriginRecord(
                    source=SOURCE_TELEGRAM,
                    account_id=self.account_id,
                    origin_id=origin_id,
                    topic_id=topic_id,
                    origin_type="topic",
                    parent_origin_id=origin_id,
                    title=getattr(topic, "title", None),
                    last_message_at=_topic_last_message_at(topic),
                    updated_at=utc_now_iso(),
                    raw_json=json.dumps(_safe_dict(topic), ensure_ascii=False, default=str),
                )
                await self._run_write(lambda rec=topic_record: self.store.upsert_origin(rec))
                count += 1
                if progress is not None:
                    progress["topics"] = base_topics + count
            last_topic = topics[-1]
            next_offset_topic = int(getattr(last_topic, "id", 0) or 0)
            next_offset_id = int(getattr(last_topic, "top_message", 0) or 0)
            if next_offset_topic == offset_topic and next_offset_id == offset_id:
                break
            offset_topic = next_offset_topic
            offset_id = next_offset_id
            total = getattr(result, "count", None)
            if isinstance(total, int) and count >= total:
                break
        total = locals().get("total")
        truncated = count >= max_topics and (not isinstance(total, int) or count < total)
        return {"topics": count, "errors": errors, "truncated": truncated}

    def _forum_topics_request(
        self,
        functions: Any,
        entity: Any,
        offset_id: int,
        offset_topic: int,
        limit: int,
    ) -> Any:
        if hasattr(functions, "messages") and hasattr(functions.messages, "GetForumTopicsRequest"):
            return functions.messages.GetForumTopicsRequest(
                peer=entity,
                q="",
                offset_date=None,
                offset_id=offset_id,
                offset_topic=offset_topic,
                limit=limit,
            )
        if hasattr(functions, "channels") and hasattr(functions.channels, "GetForumTopicsRequest"):
            return functions.channels.GetForumTopicsRequest(
                channel=entity,
                q="",
                offset_date=None,
                offset_id=offset_id,
                offset_topic=offset_topic,
                limit=limit,
            )
        raise RuntimeError("Telethon does not expose GetForumTopicsRequest")

    def _set_auth_state(self, state: str, *, probe: tuple[int, str, int] | None = None) -> None:
        if self._error_persister is not None:
            self._error_persister(lambda st=state, pr=probe: self._persist_auth_state(st, probe=pr))
            return
        self._persist_auth_state(state, probe=probe)

    def _persist_auth_state(self, state: str, *, probe: tuple[int, str, int] | None = None) -> None:
        record = AccountAuthRecord(
            source=SOURCE_TELEGRAM,
            account_id=self.account_id,
            auth_state=state,
            session_name=self.config.session_name,
            session_dir=str(self.config.session_dir),
            updated_at=utc_now_iso(),
        )
        if probe is not None:
            expected_seq, owner_id, owner_epoch = probe
            self.store.upsert_account_auth(
                record,
                expected_seq=expected_seq,
                owner_id=owner_id,
                owner_epoch=owner_epoch,
                expected_generation=self._account_generation,
            )
        else:
            self.store.upsert_account_auth(
                record,
                expected_generation=self._account_generation,
            )

    def _record_operation_diagnostic(
        self,
        operation: str,
        status: str,
        subject_type: str | None = None,
        subject_id: str | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        error_copy = dict(error) if error is not None else None
        if self._error_persister is not None:
            self._error_persister(
                lambda op=operation, st=status, stype=subject_type, sid=subject_id, err=error_copy: self._record_operation(
                    op,
                    st,
                    subject_type=stype,
                    subject_id=sid,
                    error=err,
                )
            )
            return
        self._record_operation(
            operation,
            status,
            subject_type=subject_type,
            subject_id=subject_id,
            error=error_copy,
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
                raw_json=json.dumps(error, ensure_ascii=False) if error else None,
            )
        )


def _dialog_origin_type(dialog: Any, entity: Any) -> str:
    if bool(getattr(dialog, "is_user", False)):
        return "private"
    if bool(getattr(entity, "megagroup", False)) or bool(getattr(dialog, "is_group", False)):
        return "group"
    if bool(getattr(entity, "broadcast", False)) or bool(getattr(dialog, "is_channel", False)):
        return "channel"
    return type(entity).__name__ if entity is not None else "unknown"


def _dialog_last_message_at(dialog: Any) -> str | None:
    message = getattr(dialog, "message", None)
    return to_iso(
        getattr(dialog, "date", None)
        or getattr(message, "date", None)
        or getattr(message, "created_at", None)
    )


def _topic_last_message_at(topic: Any) -> str | None:
    return to_iso(
        getattr(topic, "date", None)
        or getattr(topic, "last_message_at", None)
        or getattr(topic, "top_message_date", None)
    )


def _bounded_discovery_limit(value: int, max_limit: int) -> tuple[int, int]:
    requested = int(value)
    if requested <= 0:
        requested = max_limit
    return min(requested, max_limit), requested


def _display_name(entity: Any) -> str | None:
    if entity is None:
        return None
    first = getattr(entity, "first_name", None) or ""
    last = getattr(entity, "last_name", None) or ""
    name = f"{first} {last}".strip()
    return name or getattr(entity, "title", None) or getattr(entity, "username", None)


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
