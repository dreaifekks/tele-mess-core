from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import TelegramAccountConfig
from tele_mess_core.models import (
    AccountAuthRecord,
    OriginRecord,
    ParticipantRecord,
    SOURCE_TELEGRAM,
    utc_now_iso,
)


class TelegramDiscoveryService:
    def __init__(self, config: TelegramAccountConfig, store: ArchiveStore):
        self.config = config
        self.account_id = config.account_id
        self.store = store
        self.logger = logging.getLogger(__name__)

    async def discover_origins(self, include_topics: bool = True, topic_limit: int = 100) -> dict[str, Any]:
        client = await self._connected_client()
        try:
            if not await client.is_user_authorized():
                self._set_auth_state("needs_login")
                return {"account_id": self.account_id, "authorized": False, "origins": 0, "topics": 0}

            self._set_auth_state("authorized")
            origin_count = 0
            topic_count = 0
            async for dialog in client.iter_dialogs():
                entity = getattr(dialog, "entity", None)
                origin_id = int(getattr(dialog, "id"))
                origin_type = _dialog_origin_type(dialog, entity)
                is_forum = bool(getattr(entity, "forum", False))
                self.store.upsert_origin(
                    OriginRecord(
                        source=SOURCE_TELEGRAM,
                        account_id=self.account_id,
                        origin_id=origin_id,
                        origin_type=origin_type,
                        title=getattr(dialog, "title", None) or _display_name(entity),
                        username=getattr(entity, "username", None),
                        is_forum=is_forum,
                        updated_at=utc_now_iso(),
                        raw_json=json.dumps(_safe_dict(entity), ensure_ascii=False, default=str),
                    )
                )
                origin_count += 1
                if include_topics and is_forum:
                    topic_count += await self._discover_topics(client, entity, origin_id, topic_limit)
            return {"account_id": self.account_id, "authorized": True, "origins": origin_count, "topics": topic_count}
        finally:
            await client.disconnect()

    async def refresh_participants(self, origin_id: int, limit: int = 500) -> dict[str, Any]:
        client = await self._connected_client()
        try:
            if not await client.is_user_authorized():
                self._set_auth_state("needs_login")
                return {"account_id": self.account_id, "origin_id": origin_id, "authorized": False, "participants": 0}

            self._set_auth_state("authorized")
            count = 0
            async for user in client.iter_participants(origin_id, limit=None if limit <= 0 else limit):
                self.store.upsert_participant(
                    ParticipantRecord(
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
                )
                count += 1
            return {"account_id": self.account_id, "origin_id": origin_id, "authorized": True, "participants": count}
        finally:
            await client.disconnect()

    async def _connected_client(self) -> Any:
        from telethon import TelegramClient

        self.config.session_dir.mkdir(parents=True, exist_ok=True)
        session_file = self.config.session_dir / self.config.session_name
        client = TelegramClient(str(session_file), self.config.api_id, self.config.api_hash)
        await client.connect()
        return client

    async def _discover_topics(self, client: Any, entity: Any, origin_id: int, topic_limit: int) -> int:
        try:
            from telethon import functions
        except Exception as exc:
            self.logger.info("Failed to import Telethon topic request for %s/%s: %s", self.account_id, origin_id, exc)
            return 0

        count = 0
        offset_id = 0
        offset_topic = 0
        max_topics = None if topic_limit <= 0 else topic_limit
        while max_topics is None or count < max_topics:
            batch_limit = 100 if max_topics is None else min(100, max_topics - count)
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
                self.logger.info("Failed to discover topics for %s/%s: %s", self.account_id, origin_id, exc)
                break

            topics = list(getattr(result, "topics", []) or [])
            if not topics:
                break
            for topic in topics:
                topic_id = int(getattr(topic, "id"))
                self.store.upsert_origin(
                    OriginRecord(
                        source=SOURCE_TELEGRAM,
                        account_id=self.account_id,
                        origin_id=origin_id,
                        topic_id=topic_id,
                        origin_type="topic",
                        parent_origin_id=origin_id,
                        title=getattr(topic, "title", None),
                        updated_at=utc_now_iso(),
                        raw_json=json.dumps(_safe_dict(topic), ensure_ascii=False, default=str),
                    )
                )
                count += 1
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
        return count

    def _forum_topics_request(
        self,
        functions: Any,
        entity: Any,
        offset_id: int,
        offset_topic: int,
        limit: int,
    ) -> Any:
        return functions.channels.GetForumTopicsRequest(
            channel=entity,
            q="",
            offset_date=None,
            offset_id=offset_id,
            offset_topic=offset_topic,
            limit=limit,
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


def _dialog_origin_type(dialog: Any, entity: Any) -> str:
    if bool(getattr(dialog, "is_user", False)):
        return "private"
    if bool(getattr(entity, "megagroup", False)) or bool(getattr(dialog, "is_group", False)):
        return "group"
    if bool(getattr(entity, "broadcast", False)) or bool(getattr(dialog, "is_channel", False)):
        return "channel"
    return type(entity).__name__ if entity is not None else "unknown"


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
