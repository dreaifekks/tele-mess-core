from __future__ import annotations

import asyncio
import hashlib
import json
import mimetypes
import os
import re
import shlex
import signal
import subprocess
import time as time_module
import uuid
from dataclasses import dataclass
from datetime import date as date_type
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import AppConfig, DailyDeliveryConfig
from tele_mess_core.models import (
    DailyPackageRunRecord,
    DailyPackageScheduleRecord,
    DailyMessagePointRecord,
    DailySummaryDeliveryRecord,
    DailySummaryRecord,
    DailySummaryRunRecord,
    SOURCE_TELEGRAM,
    utc_now_iso,
)


DAILY_SYSTEMD_BASENAME = "tele-mess-core-daily-package"
MESSAGE_POINT_CHUNK_SIZE = 200
PACKAGE_MESSAGE_PAGE_SIZE = 5000


class DailyJobCancelled(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class TagGroup:
    name: str
    tags: tuple[str, ...]
    order: int = 0

    @property
    def normalized_tags(self) -> frozenset[str]:
        return frozenset(normalize_tag(tag) for tag in self.tags if normalize_tag(tag))


def parse_tags(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        parts = raw.split(",")
    elif isinstance(raw, list):
        parts = raw
    else:
        parts = [raw]
    tags: list[str] = []
    seen: set[str] = set()
    for item in parts:
        tag = str(item).strip()
        key = normalize_tag(tag)
        if not key or key in seen:
            continue
        seen.add(key)
        tags.append(tag)
    return tags


def normalize_tag(tag: str) -> str:
    return str(tag or "").strip().lower()


def parse_tag_groups(raw: Any) -> list[TagGroup]:
    if raw is None:
        return []
    if isinstance(raw, str):
        raw_items: list[Any] = [raw]
    elif isinstance(raw, list):
        raw_items = raw
    else:
        raise ValueError("tag_groups must be a string or list")

    groups: list[TagGroup] = []
    for index, item in enumerate(raw_items):
        if isinstance(item, str):
            tags = tuple(tag for tag in item.split() if tag.strip())
            name = " ".join(tags)
        elif isinstance(item, dict):
            tags = tuple(parse_tags(item.get("tags") or item.get("name") or ""))
            name = str(item.get("name") or " ".join(tags)).strip()
        else:
            raise ValueError("tag group entries must be strings or objects")
        if not tags:
            continue
        groups.append(TagGroup(name=name or " ".join(tags), tags=tags, order=index))
    return groups


def assign_origins_to_tag_groups(
    origins: list[dict[str, Any]],
    tag_groups: list[TagGroup],
) -> dict[str, Any]:
    if not tag_groups:
        return {
            "groups": [
                {
                    "name": "all",
                    "tags": [],
                    "origins": origins,
                }
            ]
            if origins
            else [],
            "unmatched": [],
        }

    sorted_groups = sorted(tag_groups, key=lambda group: (-len(group.normalized_tags), group.order))
    remaining = list(origins)
    groups: list[dict[str, Any]] = []
    for group in sorted_groups:
        group_tags = group.normalized_tags
        if not group_tags:
            continue
        matched: list[dict[str, Any]] = []
        next_remaining: list[dict[str, Any]] = []
        for origin in remaining:
            origin_tags = set(parse_tags(origin.get("tags")))
            normalized = {normalize_tag(tag) for tag in origin_tags}
            if group_tags.issubset(normalized):
                matched.append(origin)
            else:
                next_remaining.append(origin)
        groups.append({"name": group.name, "tags": list(group.tags), "origins": matched})
        remaining = next_remaining
    return {"groups": groups, "unmatched": remaining}


def assign_origins_to_effective_tag_groups(origins: list[dict[str, Any]]) -> dict[str, Any]:
    buckets: dict[tuple[str, ...], dict[str, Any]] = {}
    order: list[tuple[str, ...]] = []
    for origin in origins:
        tags = parse_tags(origin.get("tags"))
        normalized_tags = tuple(normalize_tag(tag) for tag in tags if normalize_tag(tag))
        key = normalized_tags or ("untagged",)
        if key not in buckets:
            buckets[key] = {
                "name": ",".join(tags) if tags else "untagged",
                "tags": tags,
                "origins": [],
            }
            order.append(key)
        buckets[key]["origins"].append(origin)
    return {"groups": [buckets[key] for key in order], "unmatched": []}


def build_daily_package(
    store: ArchiveStore,
    config: AppConfig,
    *,
    run_date: str | None = None,
    timezone_name: str | None = None,
    scope: dict[str, Any] | None = None,
    run_id: str | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    cancel_check: Callable[[], None] | None = None,
) -> dict[str, Any]:
    scope = dict(scope or {})
    timezone_name = timezone_name or str(scope.get("timezone") or "Asia/Tokyo")
    tz = _zoneinfo(timezone_name)
    package_date = _resolve_package_date(run_date or scope.get("date"), tz)
    window_start_local = datetime.combine(package_date, time.min, tzinfo=tz)
    window_end_local = window_start_local + timedelta(days=1)
    window_start_utc = window_start_local.astimezone(timezone.utc)
    window_end_utc = window_end_local.astimezone(timezone.utc)
    run_id = run_id or _new_run_id("pkg")
    output_root = _daily_output_dir(config) / package_date.isoformat() / run_id
    package_json_path = output_root / "package.json"
    package_md_path = output_root / "package.md"
    scope_json = json.dumps(scope, ensure_ascii=False, sort_keys=True)
    progress_state: dict[str, Any] = {"current": 0, "total": 0, "label": "starting"}

    def update_progress(
        current: int,
        total: int,
        label: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        if cancel_check:
            cancel_check()
        progress_state.clear()
        progress_state.update({"stage": "package", "current": current, "total": total, "label": label, **(extra or {})})
        store.upsert_daily_package_run(
            DailyPackageRunRecord(
                run_id=run_id,
                status="running",
                date=package_date.isoformat(),
                timezone=timezone_name,
                scope_json=scope_json,
                output_dir=str(output_root),
                package_json_path=str(package_json_path),
                package_md_path=str(package_md_path),
                progress_total=total,
                progress_current=current,
                progress_label=label,
                progress_json=json.dumps(progress_state, ensure_ascii=False, sort_keys=True),
                started_at=utc_now_iso(),
            )
        )
        if progress_callback:
            progress_callback(dict(progress_state))

    update_progress(0, 0, "selecting origins")
    try:
        output_root.mkdir(parents=True, exist_ok=True)
        (output_root / "normal-groups").mkdir(exist_ok=True)
        (output_root / "important-origins").mkdir(exist_ok=True)
        (output_root / "point-origins").mkdir(exist_ok=True)
        (output_root / "analysis").mkdir(exist_ok=True)

        selected_origins = [
            origin
            for origin in _select_package_origins(store, scope)
            if _origin_has_messages(store, origin, window_start_utc, window_end_utc)
        ]
        if cancel_check:
            cancel_check()
        important_origins = [origin for origin in selected_origins if origin.get("important")]
        normal_origins = [origin for origin in selected_origins if not origin.get("important")]
        packaged_origins: dict[tuple[str, str, int, int], dict[str, Any]] = {}

        def package_selected_origin(origin: dict[str, Any]) -> dict[str, Any]:
            key = _origin_identity(origin)
            payload = packaged_origins.get(key)
            if payload is None:
                payload = _package_origin(store, origin, window_start_utc, window_end_utc, tz)
                packaged_origins[key] = payload
            return payload

        tag_groups = parse_tag_groups(scope.get("tag_groups"))
        grouped = (
            assign_origins_to_tag_groups(normal_origins, tag_groups)
            if tag_groups
            else assign_origins_to_effective_tag_groups(normal_origins)
        )
        package_units = len(grouped["groups"]) + len(important_origins) + len(grouped["unmatched"])
        package_current = 0
        update_progress(
            package_current,
            package_units,
            "packaging",
            {
                "normal_group_count": len(grouped["groups"]),
                "important_origin_count": len(important_origins),
                "selected_origin_count": len(selected_origins),
                "unmatched_origin_count": len(grouped["unmatched"]),
            },
        )

        group_packages = []
        totals = {"origin_count": 0, "message_count": 0, "media_count": 0, "important_origin_count": len(important_origins)}
        for group in grouped["groups"]:
            update_progress(
                package_current,
                package_units,
                f"packaging group {group['name']}",
                {"current_group": group["name"], "unit_type": "normal_group"},
            )
            if cancel_check:
                cancel_check()
            origin_packages = [
                package_selected_origin(origin)
                for origin in group["origins"]
            ]
            group_payload = {
                "name": group["name"],
                "tags": group["tags"],
                "origin_count": len(origin_packages),
                "message_count": sum(item["package_meta"]["message_count"] for item in origin_packages),
                "media_count": sum(item["package_meta"]["media_count"] for item in origin_packages),
                "origins": origin_packages,
            }
            _write_json(output_root / "normal-groups" / f"{_slug(group['name'])}.json", group_payload)
            (output_root / "normal-groups" / f"{_slug(group['name'])}.md").write_text(
                _group_markdown(group_payload),
                encoding="utf-8",
            )
            group_packages.append(group_payload)
            totals["origin_count"] += len(origin_packages)
            totals["message_count"] += group_payload["message_count"]
            totals["media_count"] += group_payload["media_count"]
            package_current += 1
            update_progress(
                package_current,
                package_units,
                f"packaged group {group['name']}",
                {"current_group": group["name"], "unit_type": "normal_group"},
            )

        important_packages = []
        for origin in important_origins:
            origin_label = _origin_ref({"origin": origin})
            update_progress(
                package_current,
                package_units,
                f"packaging important {origin_label}",
                {"current_origin": origin_label, "unit_type": "important_origin"},
            )
            if cancel_check:
                cancel_check()
            payload = package_selected_origin(origin)
            important_packages.append(payload)
            name = _origin_file_stem(origin)
            _write_json(output_root / "important-origins" / f"{name}.json", payload)
            (output_root / "important-origins" / f"{name}.md").write_text(_origin_markdown(payload), encoding="utf-8")
            totals["origin_count"] += 1
            totals["message_count"] += payload["package_meta"]["message_count"]
            totals["media_count"] += payload["package_meta"]["media_count"]
            package_current += 1
            update_progress(
                package_current,
                package_units,
                f"packaged important {origin_label}",
                {"current_origin": origin_label, "unit_type": "important_origin"},
            )

        for origin in grouped["unmatched"]:
            origin_label = _origin_ref({"origin": origin})
            update_progress(
                package_current,
                package_units,
                f"packaging point-only {origin_label}",
                {"current_origin": origin_label, "unit_type": "point_origin"},
            )
            package_selected_origin(origin)
            package_current += 1
            update_progress(
                package_current,
                package_units,
                f"packaged point-only {origin_label}",
                {"current_origin": origin_label, "unit_type": "point_origin"},
            )

        point_packages = _canonicalize_point_origin_packages(
            [package_selected_origin(origin) for origin in selected_origins]
        )
        for point_payload in point_packages:
            point_origin = point_payload.get("origin") or {}
            name = _origin_file_stem(point_origin)
            _write_json(output_root / "point-origins" / f"{name}.json", point_payload)
            (output_root / "point-origins" / f"{name}.md").write_text(
                _origin_markdown(point_payload),
                encoding="utf-8",
            )
        point_message_count = sum(len(item.get("messages") or []) for item in point_packages)
        point_media_count = sum(
            int((item.get("package_meta") or {}).get("media_count") or 0)
            for item in point_packages
        )
        totals = {
            **totals,
            "analysis_origin_count": totals["origin_count"],
            "analysis_message_count": totals["message_count"],
            "analysis_media_count": totals["media_count"],
            "origin_count": len(point_packages),
            "message_count": point_message_count,
            "media_count": point_media_count,
        }

        package_payload = {
            "run_id": run_id,
            "generated_at": utc_now_iso(),
            "date": package_date.isoformat(),
            "timezone": timezone_name,
            "window_start": window_start_utc.isoformat(),
            "window_end": window_end_utc.isoformat(),
            "window_start_local": window_start_local.isoformat(),
            "window_end_local": window_end_local.isoformat(),
            "scope": scope,
            "tag_groups": [
                {"name": group.name, "tags": list(group.tags), "normalized_tags": sorted(group.normalized_tags)}
                for group in tag_groups
            ],
            "auto_tag_groups": not bool(tag_groups),
            "normal_groups": group_packages,
            "important_origins": important_packages,
            "point_origins": point_packages,
            "unmatched_origins": [_origin_summary(origin) for origin in grouped["unmatched"]],
            "stats": {
                **totals,
                "point_origin_count": len(point_packages),
                "point_message_count": point_message_count,
            },
        }
        _write_json(package_json_path, package_payload)
        package_md_path.write_text(_package_markdown(package_payload), encoding="utf-8")

        finished = utc_now_iso()
        return store.upsert_daily_package_run(
            DailyPackageRunRecord(
                run_id=run_id,
                status="completed",
                date=package_date.isoformat(),
                timezone=timezone_name,
                scope_json=scope_json,
                output_dir=str(output_root),
                package_json_path=str(package_json_path),
                package_md_path=str(package_md_path),
                origin_count=totals["origin_count"],
                message_count=totals["message_count"],
                media_count=totals["media_count"],
                important_origin_count=totals["important_origin_count"],
                progress_total=package_units,
                progress_current=package_units,
                progress_label="completed",
                progress_json=json.dumps(
                    {
                        "stage": "package",
                        "current": package_units,
                        "total": package_units,
                        "label": "completed",
                        "normal_group_count": len(grouped["groups"]),
                        "important_origin_count": len(important_origins),
                        "selected_origin_count": len(selected_origins),
                        "unmatched_origin_count": len(grouped["unmatched"]),
                        "point_origin_count": len(point_packages),
                        "point_message_count": point_message_count,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                finished_at=finished,
            )
        )
    except DailyJobCancelled as exc:
        return store.upsert_daily_package_run(
            DailyPackageRunRecord(
                run_id=run_id,
                status="canceled",
                date=package_date.isoformat(),
                timezone=timezone_name,
                scope_json=scope_json,
                output_dir=str(output_root),
                package_json_path=str(package_json_path),
                package_md_path=str(package_md_path),
                progress_total=int(progress_state.get("total") or 0),
                progress_current=int(progress_state.get("current") or 0),
                progress_label="canceled",
                progress_json=json.dumps(
                    {**progress_state, "label": "canceled", "error": str(exc) or "canceled"},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                error=str(exc) or "canceled",
                finished_at=utc_now_iso(),
            )
        )
    except Exception as exc:
        return store.upsert_daily_package_run(
            DailyPackageRunRecord(
                run_id=run_id,
                status="failed",
                date=package_date.isoformat(),
                timezone=timezone_name,
                scope_json=scope_json,
                output_dir=str(output_root),
                package_json_path=str(package_json_path),
                package_md_path=str(package_md_path),
                progress_total=int(progress_state.get("total") or 0),
                progress_current=int(progress_state.get("current") or 0),
                progress_label="failed",
                progress_json=json.dumps(
                    {**progress_state, "label": "failed", "error": str(exc)},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                error=str(exc),
                finished_at=utc_now_iso(),
            )
        )


def _summary_progress_counts(package_payload: dict[str, Any], *, include_delivery: bool = False) -> dict[str, int]:
    normal_group_count = len(package_payload.get("normal_groups") or [])
    important_origin_count = len(package_payload.get("important_origins") or [])
    point_origin_count = len(_iter_point_origin_payloads(package_payload))
    point_extraction_count = sum(
        (len(origin_payload.get("messages") or []) + MESSAGE_POINT_CHUNK_SIZE - 1)
        // MESSAGE_POINT_CHUNK_SIZE
        for origin_payload in _iter_point_origin_payloads(package_payload)
    )
    media_count = len(_collect_media_analysis_targets(package_payload))
    important_summary_count = 1 if important_origin_count else 0
    point_summary_count = 1
    delivery_count = (1 + int(bool(important_origin_count))) if include_delivery else 0
    return {
        "media_count": media_count,
        "normal_origin_count": 0,
        "normal_group_count": normal_group_count,
        "important_origin_count": important_origin_count,
        "point_origin_count": point_origin_count,
        "point_extraction_count": point_extraction_count,
        "important_summary_count": important_summary_count,
        "point_summary_count": point_summary_count,
        "final_count": important_summary_count + point_summary_count,
        "delivery_count": delivery_count,
        "total": (
            media_count
            + point_extraction_count
            + important_origin_count
            + important_summary_count
            + point_summary_count
            + delivery_count
        ),
    }


def _summary_progress_payload(
    counts: dict[str, int],
    *,
    current: int,
    label: str,
    phase: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "stage": "summary",
        "current": current,
        "total": int(counts.get("total") or 0),
        "label": label,
        "phase": phase,
        "media_count": int(counts.get("media_count") or 0),
        "normal_origin_count": int(counts.get("normal_origin_count") or 0),
        "normal_group_count": int(counts.get("normal_group_count") or 0),
        "important_origin_count": int(counts.get("important_origin_count") or 0),
        "point_origin_count": int(counts.get("point_origin_count") or 0),
        "point_extraction_count": int(counts.get("point_extraction_count") or 0),
        "important_summary_count": int(counts.get("important_summary_count") or 0),
        "point_summary_count": int(counts.get("point_summary_count") or 0),
        "final_count": int(counts.get("final_count") or 0),
        "delivery_count": int(counts.get("delivery_count") or 0),
        **(extra or {}),
    }


def run_daily_summary(
    store: ArchiveStore,
    config: AppConfig,
    *,
    package_run_id: str | None = None,
    run_date: str | None = None,
    timezone_name: str | None = None,
    scope: dict[str, Any] | None = None,
    run_id: str | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    cancel_check: Callable[[], None] | None = None,
    process_callback: Callable[[subprocess.Popen[str] | None, str], None] | None = None,
    telegram_runtime: Any | None = None,
    defer_delivery: bool = False,
    job_id: str | None = None,
) -> dict[str, Any]:
    package_run = store.get_daily_package_run(package_run_id) if package_run_id else None
    if package_run is None:
        package_run = build_daily_package(store, config, run_date=run_date, timezone_name=timezone_name, scope=scope)
        package_run_id = package_run["run_id"]
    if package_run.get("status") != "completed":
        raise ValueError(f"Package run is not completed: {package_run_id}")
    package_json_path = Path(str(package_run.get("package_json_path") or ""))
    if not package_json_path.is_file():
        raise ValueError(f"Package JSON is missing for run: {package_run_id}")

    package_payload = json.loads(package_json_path.read_text(encoding="utf-8"))
    delivery = resolve_daily_summary_delivery(store, config)
    progress_counts = _summary_progress_counts(package_payload, include_delivery=delivery.enabled)
    image_count_estimate = len(_collect_image_paths(package_payload))
    run_id = run_id or _new_run_id("sum")
    output_root = Path(str(package_run["output_dir"])) / "analysis" / run_id
    summary_path = output_root / "summary.md"
    important_summary_path = output_root / "important-summary.md"
    important_prompt_path = output_root / "important-summary.prompt.md"
    point_summary_path = output_root / "point-summary.md"
    point_prompt_path = output_root / "point-summary.prompt.md"
    output_root.mkdir(parents=True, exist_ok=True)
    scope_json = json.dumps(scope or package_run.get("scope") or {}, ensure_ascii=False, sort_keys=True)
    progress_state = _summary_progress_payload(progress_counts, current=0, label="queued", phase="queued")

    def update_progress(
        current: int,
        label: str,
        phase: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        nonlocal progress_state
        if cancel_check:
            cancel_check()
        progress_state = _summary_progress_payload(
            progress_counts,
            current=current,
            label=label,
            phase=phase,
            extra=extra,
        )
        store.upsert_daily_summary_run(
            DailySummaryRunRecord(
                run_id=run_id,
                status="running",
                package_run_id=str(package_run_id),
                date=package_run.get("date"),
                timezone=package_run.get("timezone"),
                scope_json=scope_json,
                output_dir=str(output_root),
                summary_path=str(summary_path),
                provider=config.daily.ai.provider,
                origin_count=int(
                    package_payload.get("stats", {}).get("point_origin_count")
                    or package_payload.get("stats", {}).get("origin_count")
                    or 0
                ),
                group_count=len(package_payload.get("normal_groups") or []),
                image_count=image_count_estimate,
                progress_total=int(progress_counts.get("total") or 0),
                progress_current=current,
                progress_label=label,
                progress_json=json.dumps(progress_state, ensure_ascii=False, sort_keys=True),
                started_at=utc_now_iso(),
            )
        )
        if progress_callback:
            progress_callback(dict(progress_state))

    message_point_records: list[DailyMessagePointRecord] = []

    def persist_message_points(points: list[dict[str, Any]]) -> None:
        nonlocal message_point_records
        message_point_records = _build_message_point_records(
            run_id=run_id,
            package_run_id=str(package_run_id),
            package_run=package_run,
            provider=config.daily.ai.provider,
            points=points,
        )
        store.persist_daily_message_points(run_id, message_point_records)

    update_progress(0, "starting summary", "starting")
    try:
        analysis = _run_ai_analysis_pipeline(
            config,
            package_payload,
            output_root,
            combined_summary_path=summary_path,
            important_summary_path=important_summary_path,
            important_prompt_path=important_prompt_path,
            point_summary_path=point_summary_path,
            point_prompt_path=point_prompt_path,
            points_ready_callback=persist_message_points,
            progress_callback=update_progress,
            cancel_check=cancel_check,
            process_callback=process_callback,
        )
        image_paths = _collect_image_paths(package_payload)
        important_summary_text = str((analysis.get("important_summary") or {}).get("content") or "")
        point_summary_text = str((analysis.get("point_summary") or {}).get("content") or "")
        _write_summary_analysis_json(
            output_root / "summary.json",
            run_id=run_id,
            package_run_id=str(package_run_id),
            provider=config.daily.ai.provider,
            image_paths=image_paths,
            summary_path=summary_path,
            analysis=analysis,
            delivery=None,
        )
        summary_records = _build_summary_records(
            run_id=run_id,
            package_run_id=str(package_run_id),
            package_run=package_run,
            package_payload=package_payload,
            provider=config.daily.ai.provider,
            summary_path=summary_path,
            image_paths=image_paths,
            analysis=analysis,
        )
        delivery_result: dict[str, Any] | None = None
        delivery_contents: list[tuple[str, str]] = []
        outbox_items: list[dict[str, Any]] = []
        if delivery.enabled:
            if package_payload.get("important_origins"):
                delivery_contents.append(
                    (
                        "important_summary",
                        _summary_delivery_markdown(
                            package_run=package_run,
                            package_payload=package_payload,
                            provider=config.daily.ai.provider,
                            summary_text=important_summary_text,
                            summary_kind="important",
                        ),
                    )
                )
            delivery_contents.append(
                (
                    "point_summary",
                    _summary_delivery_markdown(
                        package_run=package_run,
                        package_payload=package_payload,
                        provider=config.daily.ai.provider,
                        summary_text=point_summary_text,
                        summary_kind="point",
                    ),
                )
            )
            if defer_delivery:
                from tele_mess_core.telegram.delivery import split_telegram_message

                pending_chunks = [
                    (kind, chunk)
                    for kind, content in delivery_contents
                    for chunk in split_telegram_message(content)
                ]
                for index, (kind, chunk) in enumerate(pending_chunks, start=1):
                    body = f"[{index}/{len(pending_chunks)}]\n\n{chunk}" if len(pending_chunks) > 1 else chunk
                    outbox_items.append(
                        {
                            "outbox_id": f"out_{run_id}_{index}",
                            "summary_run_id": run_id,
                            "job_id": job_id,
                            "account_id": delivery.account_id,
                            "origin_id": delivery.origin_id,
                            "topic_id": delivery.topic_id,
                            "chunk_index": index,
                            "chunk_count": len(pending_chunks),
                            "content_kind": kind,
                            "content": body,
                        }
                    )
        store.persist_daily_summary_batch(
            summary_records,
            outbox_items,
            message_points=message_point_records,
        )
        if delivery.enabled:
            delivery_start = max(
                0,
                int(progress_counts.get("total") or 0) - int(progress_counts.get("delivery_count") or 0),
            )
            update_progress(
                delivery_start,
                "queueing daily deliveries" if defer_delivery else "delivering daily summaries",
                "delivery",
                {
                    "delivery": _delivery_target_payload(delivery),
                    "delivery_kinds": [kind for kind, _ in delivery_contents],
                },
            )
            if defer_delivery:
                delivery_result = {
                    **_delivery_target_payload(delivery),
                    "status": "queued",
                    "message_count": len(outbox_items),
                    "outbox_ids": [item["outbox_id"] for item in outbox_items],
                    "delivery_kinds": [kind for kind, _ in delivery_contents],
                }
            else:
                delivery_results: list[dict[str, Any]] = []
                for delivery_index, (kind, content) in enumerate(delivery_contents, start=1):
                    result = deliver_daily_summary(
                        store,
                        config,
                        content,
                        telegram_runtime=telegram_runtime,
                        delivery=delivery,
                    )
                    delivery_results.append({"kind": kind, "result": result})
                    update_progress(
                        delivery_start + delivery_index,
                        f"delivered {kind}",
                        "delivery",
                        {"delivery": result, "delivery_kind": kind},
                    )
                delivery_result = {
                    **_delivery_target_payload(delivery),
                    "status": "sent",
                    "deliveries": delivery_results,
                }
            _write_summary_analysis_json(
                output_root / "summary.json",
                run_id=run_id,
                package_run_id=str(package_run_id),
                provider=config.daily.ai.provider,
                image_paths=image_paths,
                summary_path=summary_path,
                analysis=analysis,
                delivery=delivery_result,
            )
            update_progress(
                int(progress_counts.get("total") or 0),
                "queued daily deliveries" if defer_delivery else "delivered daily summaries",
                "delivery",
                {"delivery": delivery_result},
            )
        return store.upsert_daily_summary_run(
            DailySummaryRunRecord(
                run_id=run_id,
                status="completed",
                package_run_id=str(package_run_id),
                date=package_run.get("date"),
                timezone=package_run.get("timezone"),
                scope_json=scope_json,
                output_dir=str(output_root),
                summary_path=str(summary_path),
                provider=config.daily.ai.provider,
                origin_count=int(
                    package_payload.get("stats", {}).get("point_origin_count")
                    or package_payload.get("stats", {}).get("origin_count")
                    or 0
                ),
                group_count=len(package_payload.get("normal_groups") or []),
                image_count=len(image_paths),
                progress_total=int(progress_counts.get("total") or 0),
                progress_current=int(progress_counts.get("total") or 0),
                progress_label="completed",
                progress_json=json.dumps(
                    _summary_progress_payload(
                        progress_counts,
                        current=int(progress_counts.get("total") or 0),
                        label="completed",
                        phase="completed",
                    ),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                finished_at=utc_now_iso(),
            )
        )
    except DailyJobCancelled as exc:
        return store.upsert_daily_summary_run(
            DailySummaryRunRecord(
                run_id=run_id,
                status="canceled",
                package_run_id=str(package_run_id),
                date=package_run.get("date"),
                timezone=package_run.get("timezone"),
                scope_json=scope_json,
                output_dir=str(output_root),
                summary_path=str(summary_path),
                provider=config.daily.ai.provider,
                origin_count=int(
                    package_payload.get("stats", {}).get("point_origin_count")
                    or package_payload.get("stats", {}).get("origin_count")
                    or 0
                ),
                group_count=len(package_payload.get("normal_groups") or []),
                image_count=image_count_estimate,
                progress_total=int(progress_counts.get("total") or 0),
                progress_current=int(progress_state.get("current") or 0),
                progress_label="canceled",
                progress_json=json.dumps(
                    {**progress_state, "label": "canceled", "phase": "canceled", "error": str(exc) or "canceled"},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                error=str(exc) or "canceled",
                finished_at=utc_now_iso(),
            )
        )
    except Exception as exc:
        return store.upsert_daily_summary_run(
            DailySummaryRunRecord(
                run_id=run_id,
                status="failed",
                package_run_id=str(package_run_id),
                date=package_run.get("date"),
                timezone=package_run.get("timezone"),
                scope_json=scope_json,
                output_dir=str(output_root),
                summary_path=str(summary_path),
                provider=config.daily.ai.provider,
                origin_count=int(
                    package_payload.get("stats", {}).get("point_origin_count")
                    or package_payload.get("stats", {}).get("origin_count")
                    or 0
                ),
                group_count=len(package_payload.get("normal_groups") or []),
                image_count=image_count_estimate,
                progress_total=int(progress_counts.get("total") or 0),
                progress_current=int(progress_state.get("current") or 0),
                progress_label="failed",
                progress_json=json.dumps(
                    {**progress_state, "label": "failed", "phase": "failed", "error": str(exc)},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                error=str(exc),
                finished_at=utc_now_iso(),
            )
        )


def run_daily_package_and_summary(
    store: ArchiveStore,
    config: AppConfig,
    *,
    run_date: str | None = None,
    timezone_name: str | None = None,
    scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    package = build_daily_package(store, config, run_date=run_date, timezone_name=timezone_name, scope=scope)
    result: dict[str, Any] = {
        "status": package.get("status"),
        "package_run_id": package.get("run_id"),
        "summary_run_id": None,
        "package": package,
        "summary": None,
        "error": package.get("error"),
    }
    if package.get("status") != "completed":
        result["status"] = "failed"
        result["error"] = package.get("error") or "Daily package did not complete"
        return result

    summary = run_daily_summary(store, config, package_run_id=str(package["run_id"]), scope=scope)
    result.update(
        {
            "status": "completed" if summary.get("status") == "completed" else "failed",
            "summary_run_id": summary.get("run_id"),
            "summary": summary,
            "error": summary.get("error"),
        }
    )
    return result


def deliver_daily_summary(
    store: ArchiveStore,
    config: AppConfig,
    content: str,
    *,
    telegram_runtime: Any | None = None,
    delivery: DailyDeliveryConfig | None = None,
) -> dict[str, Any] | None:
    delivery = delivery or resolve_daily_summary_delivery(store, config)
    if not delivery.enabled:
        return None
    if delivery.origin_id is None:
        raise ValueError("daily.delivery.origin_id is required when daily.delivery.enabled is true")
    if telegram_runtime is not None:
        return telegram_runtime.call(
            delivery.account_id,
            "deliver_summary",
            delivery=delivery,
            content=content,
        )
    account = _delivery_account_config(config, delivery)
    from tele_mess_core.telegram.delivery import TelegramSummaryDeliveryService

    return asyncio.run(TelegramSummaryDeliveryService(account, store).send_summary(delivery, content))


def daily_summary_delivery_state(store: ArchiveStore, config: AppConfig) -> dict[str, Any]:
    stored = store.get_daily_summary_delivery()
    if stored is not None:
        return {**stored, "source": "database"}
    delivery = config.daily.delivery
    return {
        "enabled": bool(delivery.enabled),
        "account_id": delivery.account_id or None,
        "origin_id": delivery.origin_id,
        "topic_id": int(delivery.topic_id),
        "updated_at": None,
        "source": "config",
    }


def resolve_daily_summary_delivery(store: ArchiveStore, config: AppConfig) -> DailyDeliveryConfig:
    state = daily_summary_delivery_state(store, config)
    return DailyDeliveryConfig(
        enabled=bool(state.get("enabled")),
        account_id=str(state.get("account_id") or ""),
        origin_id=int(state["origin_id"]) if state.get("origin_id") is not None else None,
        topic_id=int(state.get("topic_id") or 0),
    )


def update_daily_summary_delivery(
    store: ArchiveStore,
    config: AppConfig,
    payload: dict[str, Any],
) -> dict[str, Any]:
    current = daily_summary_delivery_state(store, config)
    enabled = _payload_bool(payload, "enabled", bool(current.get("enabled", False)))
    account_id_raw = payload["account_id"] if "account_id" in payload else current.get("account_id")
    origin_id_raw = payload["origin_id"] if "origin_id" in payload else current.get("origin_id")
    topic_id_raw = payload["topic_id"] if "topic_id" in payload else current.get("topic_id", 0)
    account_id = str(account_id_raw or "").strip() or None
    origin_id = None if origin_id_raw in (None, "") else int(origin_id_raw)
    topic_id = 0 if topic_id_raw in (None, "") else int(topic_id_raw)
    if topic_id < 0:
        raise ValueError("daily summary delivery topic_id must be zero or positive")
    if enabled:
        if not account_id:
            raise ValueError("daily summary delivery account_id is required when enabled is true")
        if origin_id is None:
            raise ValueError("daily summary delivery origin_id is required when enabled is true")
        if not any(account.account_id == account_id for account in config.telegram.accounts):
            raise ValueError(f"Unknown daily summary delivery account_id: {account_id}")
    stored = store.set_daily_summary_delivery(
        DailySummaryDeliveryRecord(
            enabled=enabled,
            account_id=account_id,
            origin_id=origin_id,
            topic_id=topic_id,
            updated_at=utc_now_iso(),
        )
    )
    return {**stored, "source": "database"}


def _delivery_account_config(config: AppConfig, delivery: DailyDeliveryConfig):
    account_id = delivery.account_id
    for account in config.telegram.accounts:
        if account.account_id == account_id:
            return account
    raise ValueError(f"Unknown daily.delivery.account_id: {account_id}")


def _delivery_target_payload(delivery: DailyDeliveryConfig) -> dict[str, Any]:
    return {
        "account_id": delivery.account_id,
        "origin_id": delivery.origin_id,
        "topic_id": delivery.topic_id,
    }


def _write_summary_analysis_json(
    path: Path,
    *,
    run_id: str,
    package_run_id: str,
    provider: str,
    image_paths: list[str],
    summary_path: Path,
    analysis: dict[str, Any],
    delivery: dict[str, Any] | None,
) -> None:
    payload = {
        "run_id": run_id,
        "package_run_id": package_run_id,
        "provider": provider,
        "image_paths": image_paths,
        "summary_path": str(summary_path),
        "analysis": _analysis_record_payload(analysis),
        "delivery": delivery,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _summary_delivery_markdown(
    *,
    package_run: dict[str, Any],
    package_payload: dict[str, Any],
    provider: str,
    summary_text: str,
    summary_kind: str = "important",
) -> str:
    date = str(package_payload.get("date") or package_run.get("date") or "unknown")
    timezone_name = str(package_payload.get("timezone") or package_run.get("timezone") or "")
    if summary_kind == "point":
        title = "# Daily Message Point Summary"
        tag_text = "#point"
    else:
        title = "# Important Daily Summary"
        tags = _collect_important_summary_tags(package_payload)
        hashtags = [_telegram_hashtag(tag) for tag in tags]
        tag_text = " ".join(hashtag for hashtag in hashtags if hashtag) or "#important"
    header = [
        title,
        "",
        f"- Date: `{date}`",
    ]
    if timezone_name:
        header.append(f"- Timezone: `{timezone_name}`")
    header.extend(
        [
            f"- Tags: {tag_text}",
            f"- Summary provider: `{provider}`",
            "",
        ]
    )
    body = str(summary_text or "").strip()
    return "\n".join(header) + "\n" + (body or "_No summary content._") + "\n"


def _telegram_hashtag(tag: str) -> str:
    cleaned = re.sub(r"[^\w]+", "_", str(tag or "").strip(), flags=re.UNICODE).strip("_")
    return f"#{cleaned}" if cleaned else ""


def _build_summary_records(
    *,
    run_id: str,
    package_run_id: str,
    package_run: dict[str, Any],
    package_payload: dict[str, Any],
    provider: str,
    summary_path: Path,
    image_paths: list[str],
    analysis: dict[str, Any] | None = None,
) -> list[DailySummaryRecord]:
    stats = package_payload.get("stats") or {}
    now = utc_now_iso()
    analysis = analysis or {}
    date = str(package_run.get("date") or package_payload.get("date") or "")
    timezone_name = str(package_run.get("timezone") or package_payload.get("timezone") or "")
    scope_json = json.dumps(package_payload.get("scope") or package_run.get("scope") or {}, ensure_ascii=False, sort_keys=True)
    records: list[DailySummaryRecord] = []

    for artifact in analysis.get("important_origins") or []:
        metadata = artifact.get("metadata") or {}
        origin = metadata.get("origin") or {}
        tags = parse_tags(origin.get("tags"))
        origin_ref = _origin_ref_from_summary(origin)
        title = str(origin.get("title") or origin_ref or "important")
        content = str(artifact.get("content") or "")
        artifact_images = list(artifact.get("image_paths") or [])
        content_json = _summary_record_content_json(
            package_run_id=package_run_id,
            package_run=package_run,
            record_type="important_origin",
            tags=tags,
            image_paths=artifact_images,
            artifact=artifact,
            final_summary_path=summary_path,
        )
        records.append(
            DailySummaryRecord(
                summary_id=f"{run_id}--important--{_slug(origin_ref or title)}",
                run_id=run_id,
                record_type="important_origin",
                package_run_id=package_run_id,
                date=date or None,
                timezone=timezone_name or None,
                scope_json=scope_json,
                tags_json=json.dumps(tags, ensure_ascii=False),
                tags_csv=",".join(tags),
                important=True,
                provider=provider,
                title=f"Important Summary {date} - {title}" if date else f"Important Summary - {title}",
                content_md=content,
                content_json=json.dumps(content_json, ensure_ascii=False, sort_keys=True),
                summary_path=str(artifact.get("output_path") or summary_path),
                origin_count=1,
                group_count=0,
                image_count=len(artifact_images),
                content_length=len(content),
                created_at=now,
                updated_at=now,
            )
        )

    important_summary = analysis.get("important_summary") or {}
    if package_payload.get("important_origins") and important_summary:
        important_tags = _collect_important_summary_tags(package_payload)
        important_content = str(important_summary.get("content") or "")
        content_json = _summary_record_content_json(
            package_run_id=package_run_id,
            package_run=package_run,
            record_type="important_daily",
            tags=important_tags,
            image_paths=image_paths,
            artifact=important_summary,
            final_summary_path=summary_path,
        )
        records.append(
            DailySummaryRecord(
                summary_id=f"{run_id}--important-daily",
                run_id=run_id,
                record_type="important_daily",
                package_run_id=package_run_id,
                date=date or None,
                timezone=timezone_name or None,
                scope_json=scope_json,
                tags_json=json.dumps(important_tags, ensure_ascii=False),
                tags_csv=",".join(important_tags),
                important=True,
                provider=provider,
                title=f"Important Daily Summary {date}" if date else "Important Daily Summary",
                content_md=important_content,
                content_json=json.dumps(content_json, ensure_ascii=False, sort_keys=True),
                summary_path=str(important_summary.get("output_path") or summary_path),
                origin_count=len(package_payload.get("important_origins") or []),
                group_count=0,
                image_count=len(image_paths),
                content_length=len(important_content),
                created_at=now,
                updated_at=now,
            )
        )

    point_summary = analysis.get("point_summary") or {}
    point_content = str(point_summary.get("content") or "")
    point_tags = ["point"]
    point_content_json = _summary_record_content_json(
        package_run_id=package_run_id,
        package_run=package_run,
        record_type="point_daily",
        tags=point_tags,
        image_paths=[],
        artifact=point_summary,
        final_summary_path=summary_path,
    )
    records.append(
        DailySummaryRecord(
            summary_id=f"{run_id}--point-daily",
            run_id=run_id,
            record_type="point_daily",
            package_run_id=package_run_id,
            date=date or None,
            timezone=timezone_name or None,
            scope_json=scope_json,
            tags_json=json.dumps(point_tags, ensure_ascii=False),
            tags_csv="point",
            important=False,
            provider=provider,
            title=f"Daily Message Point Summary {date}" if date else "Daily Message Point Summary",
            content_md=point_content,
            content_json=json.dumps(point_content_json, ensure_ascii=False, sort_keys=True),
            summary_path=str(point_summary.get("output_path") or summary_path),
            origin_count=int(stats.get("point_origin_count") or stats.get("origin_count") or 0),
            group_count=0,
            image_count=0,
            content_length=len(point_content),
            created_at=now,
            updated_at=now,
        )
    )
    return records


def _build_message_point_records(
    *,
    run_id: str,
    package_run_id: str,
    package_run: dict[str, Any],
    provider: str,
    points: list[dict[str, Any]],
) -> list[DailyMessagePointRecord]:
    date = str(package_run.get("date") or "")
    timezone_name = str(package_run.get("timezone") or "")
    now = utc_now_iso()
    records: list[DailyMessagePointRecord] = []
    for index, point in enumerate(points, start=1):
        source_refs = list(point.get("source_refs") or [])
        identity = json.dumps(
            {
                "run_id": run_id,
                "source": point.get("source"),
                "account_id": point.get("account_id"),
                "origin_id": point.get("origin_id"),
                "topic_id": point.get("topic_id"),
                "source_refs": source_refs,
                "content": point.get("content"),
                "index": index,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        point_id = f"point_{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:24]}"
        tags = parse_tags(point.get("tags"))
        records.append(
            DailyMessagePointRecord(
                point_id=point_id,
                run_id=run_id,
                package_run_id=package_run_id,
                date=date,
                timezone=timezone_name,
                source=str(point.get("source") or SOURCE_TELEGRAM),
                account_id=str(point.get("account_id") or ""),
                origin_id=int(point.get("origin_id") or 0),
                topic_id=int(point.get("topic_id") or 0),
                origin_title=str(point.get("origin_title") or "") or None,
                message_id=int(point["message_id"]) if point.get("message_id") is not None else None,
                occurred_at=str(point.get("local_occurred_at") or point.get("occurred_at") or date),
                tags_json=json.dumps(tags, ensure_ascii=False),
                tags_csv=",".join(tags),
                content=str(point.get("content") or ""),
                telegram_deeplink=str(point.get("telegram_deeplink") or "") or None,
                permalink=str(point.get("permalink") or "") or None,
                importance_score=int(point.get("importance_score") or 3),
                importance_reason=str(point.get("importance_reason") or "") or None,
                origin_important=bool(point.get("origin_important")),
                source_refs_json=json.dumps(source_refs, ensure_ascii=False),
                provider=provider,
                created_at=now,
                updated_at=now,
            )
        )
    return records


def _summary_record_content_json(
    *,
    package_run_id: str,
    package_run: dict[str, Any],
    record_type: str,
    tags: list[str],
    image_paths: list[str],
    artifact: dict[str, Any],
    final_summary_path: Path,
    full_analysis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "record_type": record_type,
        "package_run_id": package_run_id,
        "package_json_path": package_run.get("package_json_path"),
        "package_md_path": package_run.get("package_md_path"),
        "final_summary_path": str(final_summary_path),
        "tags": tags,
        "tags_csv": ",".join(tags),
        "image_paths": image_paths,
        "artifact": _artifact_payload(artifact),
    }
    if full_analysis is not None:
        payload["analysis"] = _analysis_record_payload(full_analysis)
    return payload


def _origin_ref_from_summary(origin: dict[str, Any]) -> str:
    return f"{origin.get('account_id')}/{origin.get('origin_id')}/{origin.get('topic_id') or 0}"


def _collect_important_summary_tags(package_payload: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()
    for origin_payload in package_payload.get("important_origins") or []:
        for tag in parse_tags((origin_payload.get("origin") or {}).get("tags")):
            normalized = normalize_tag(tag)
            if normalized and normalized not in seen:
                seen.add(normalized)
                tags.append(tag)
    return tags


def _run_ai_analysis_pipeline(
    config: AppConfig,
    package_payload: dict[str, Any],
    output_root: Path,
    *,
    combined_summary_path: Path,
    important_summary_path: Path,
    important_prompt_path: Path,
    point_summary_path: Path,
    point_prompt_path: Path,
    points_ready_callback: Callable[[list[dict[str, Any]]], None] | None = None,
    progress_callback: Callable[[int, str, str, dict[str, Any] | None], None] | None = None,
    cancel_check: Callable[[], None] | None = None,
    process_callback: Callable[[subprocess.Popen[str] | None, str], None] | None = None,
) -> dict[str, Any]:
    completed = 0

    def mark_started(label: str, phase: str, extra: dict[str, Any] | None = None) -> None:
        if progress_callback:
            progress_callback(completed, label, phase, extra)

    def mark_completed(label: str, phase: str, extra: dict[str, Any] | None = None) -> None:
        nonlocal completed
        completed += 1
        if progress_callback:
            progress_callback(completed, label, phase, extra)

    stages_dir = output_root / "stages"
    if cancel_check:
        cancel_check()
    mark_started("analyzing media", "media")
    media_artifacts = _analyze_package_media(
        config,
        package_payload,
        stages_dir / "media",
        progress_done=mark_completed,
        cancel_check=cancel_check,
        process_callback=process_callback,
    )

    point_artifacts: list[dict[str, Any]] = []
    message_points: list[dict[str, Any]] = []
    point_dir = stages_dir / "message-points"
    for origin_payload in _iter_point_origin_payloads(package_payload):
        origin_ref = _origin_ref(origin_payload)
        messages = list(origin_payload.get("messages") or [])
        chunk_count = (len(messages) + MESSAGE_POINT_CHUNK_SIZE - 1) // MESSAGE_POINT_CHUNK_SIZE
        for chunk_index, start in enumerate(range(0, len(messages), MESSAGE_POINT_CHUNK_SIZE), start=1):
            chunk = messages[start : start + MESSAGE_POINT_CHUNK_SIZE]
            label = f"extracting points {origin_ref} ({chunk_index}/{chunk_count})"
            progress_extra = {
                "origin_ref": origin_ref,
                "chunk_index": chunk_index,
                "chunk_count": chunk_count,
                "message_count": len(chunk),
            }
            mark_started(label, "message_points", progress_extra)
            if cancel_check:
                cancel_check()
            artifact = _run_ai_task(
                config,
                "message_point_extraction",
                _message_point_extraction_prompt(origin_payload, chunk, media_artifacts),
                point_dir / f"{_slug(origin_ref)}--{chunk_index:03d}.json",
                image_paths=[],
                metadata={
                    "origin": origin_payload.get("origin") or {},
                    **progress_extra,
                },
                output_schema=_message_point_output_schema(),
                cancel_check=cancel_check,
                process_callback=process_callback,
            )
            extracted = _validated_message_points(
                str(artifact.get("content") or ""),
                origin_payload=origin_payload,
                messages=chunk,
            )
            artifact["metadata"]["point_count"] = len(extracted)
            point_artifacts.append(artifact)
            message_points.extend(extracted)
            mark_completed(
                f"extracted {len(extracted)} points from {origin_ref} ({chunk_index}/{chunk_count})",
                "message_points",
                {**progress_extra, "point_count": len(extracted)},
            )

    mark_started(
        f"persisting {len(message_points)} message points",
        "message_points",
        {"point_count": len(message_points)},
    )
    if points_ready_callback:
        points_ready_callback(list(message_points))

    important_artifacts: list[dict[str, Any]] = []
    important_dir = stages_dir / "important-origins"
    for origin_payload in package_payload.get("important_origins") or []:
        origin_ref = _origin_ref(origin_payload)
        image_paths = _origin_image_paths(origin_payload, limit=None)
        mark_started(
            f"analyzing important {origin_ref}",
            "important_origin",
            {"origin_ref": origin_ref, "image_count": len(image_paths)},
        )
        if cancel_check:
            cancel_check()
        prompt = _important_origin_analysis_prompt(origin_payload, media_artifacts)
        artifact = _run_ai_task(
            config,
            "important_origin_analysis",
            prompt,
            important_dir / f"{_slug(origin_ref)}.md",
            image_paths=image_paths,
            metadata={
                "origin": origin_payload.get("origin") or {},
                "image_paths": image_paths,
            },
            cancel_check=cancel_check,
            process_callback=process_callback,
        )
        important_artifacts.append(artifact)
        mark_completed(
            f"analyzed important {origin_ref}",
            "important_origin",
            {"origin_ref": origin_ref, "image_count": len(image_paths)},
        )

    if important_artifacts:
        mark_started("writing important daily summary", "important_summary")
        if cancel_check:
            cancel_check()
        important_artifact = _run_ai_task(
            config,
            "important_daily_summary",
            _important_daily_summary_prompt(
                package_payload,
                important_artifacts=important_artifacts,
                media_artifacts=media_artifacts,
            ),
            important_summary_path,
            image_paths=[],
            prompt_path=important_prompt_path,
            metadata={
                "package_run_id": package_payload.get("run_id"),
                "date": package_payload.get("date"),
                "timezone": package_payload.get("timezone"),
            },
            cancel_check=cancel_check,
            process_callback=process_callback,
        )
        mark_completed("wrote important daily summary", "important_summary")
    else:
        important_artifact = _static_artifact(
            task="important_daily_summary",
            output_path=important_summary_path,
            content="# Important Daily Summary\n\n_No important-origin messages were archived for this date._\n",
            metadata={"package_run_id": package_payload.get("run_id")},
        )

    mark_started("writing daily point summary", "point_summary", {"point_count": len(message_points)})
    if cancel_check:
        cancel_check()
    if message_points:
        point_summary_artifact = _run_ai_task(
            config,
            "daily_point_summary",
            _daily_point_summary_prompt(package_payload, message_points),
            point_summary_path,
            image_paths=[],
            prompt_path=point_prompt_path,
            metadata={
                "package_run_id": package_payload.get("run_id"),
                "date": package_payload.get("date"),
                "timezone": package_payload.get("timezone"),
                "point_count": len(message_points),
            },
            cancel_check=cancel_check,
            process_callback=process_callback,
        )
    else:
        point_summary_artifact = _static_artifact(
            task="daily_point_summary",
            output_path=point_summary_path,
            content="# Daily Message Point Summary\n\n_No message points were extracted for this date._\n",
            metadata={
                "package_run_id": package_payload.get("run_id"),
                "point_count": 0,
            },
        )
    mark_completed("wrote daily point summary", "point_summary", {"point_count": len(message_points)})

    _write_combined_daily_summary(
        combined_summary_path,
        important_content=str(important_artifact.get("content") or ""),
        point_content=str(point_summary_artifact.get("content") or ""),
    )
    analysis = {
        "media": list(media_artifacts.values()),
        "message_point_extractions": point_artifacts,
        "message_points": message_points,
        "important_origins": important_artifacts,
        "important_summary": important_artifact,
        "point_summary": point_summary_artifact,
        "final": important_artifact,
    }
    _write_json(output_root / "analysis.json", _analysis_record_payload(analysis))
    return analysis


def _analyze_package_media(
    config: AppConfig,
    package_payload: dict[str, Any],
    output_dir: Path,
    *,
    progress_done: Callable[[str, str, dict[str, Any] | None], None] | None = None,
    cancel_check: Callable[[], None] | None = None,
    process_callback: Callable[[subprocess.Popen[str] | None, str], None] | None = None,
) -> dict[str, dict[str, Any]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, dict[str, Any]] = {}
    for target in _collect_media_analysis_targets(package_payload):
        if cancel_check:
            cancel_check()
        origin_payload = target["origin_payload"]
        message = target["message"]
        media = target["media"]
        file_path = str(media.get("file_path") or "")
        descriptor = _media_descriptor(origin_payload, message, media)
        stem = _slug(f"{descriptor['account_id']}-{descriptor['origin_id']}-{descriptor['message_id']}-{descriptor['file_index']}")
        output_path = output_dir / f"{stem}.md"
        if _is_image_media(media) and Path(file_path).is_file():
            prompt = _media_image_analysis_prompt(descriptor)
            artifacts[file_path] = _run_ai_task(
                config,
                "media_image_analysis",
                prompt,
                output_path,
                image_paths=[file_path],
                metadata=descriptor,
                cancel_check=cancel_check,
                process_callback=process_callback,
            )
        else:
            content = _media_reference_markdown(descriptor)
            output_path.write_text(content, encoding="utf-8")
            artifacts[file_path] = {
                "task": "media_file_reference",
                "id": stem,
                "output_path": str(output_path),
                "prompt_path": None,
                "content": content,
                "image_paths": [],
                "generated_by_ai": False,
                "metadata": descriptor,
            }
        if progress_done:
            progress_done(
                f"analyzed media {Path(file_path).name}",
                "media",
                {"file_path": file_path, "media_kind": media.get("media_kind")},
            )
    return artifacts


def _run_ai_task(
    config: AppConfig,
    task_name: str,
    prompt: str,
    output_path: Path,
    *,
    image_paths: list[str],
    metadata: dict[str, Any],
    prompt_path: Path | None = None,
    output_schema: dict[str, Any] | None = None,
    cancel_check: Callable[[], None] | None = None,
    process_callback: Callable[[subprocess.Popen[str] | None, str], None] | None = None,
) -> dict[str, Any]:
    if cancel_check:
        cancel_check()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path = prompt_path or output_path.with_suffix(".prompt.md")
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(prompt, encoding="utf-8")
    output_schema_path = output_path.with_suffix(".schema.json") if output_schema is not None else None
    if output_schema_path is not None:
        output_schema_path.write_text(
            json.dumps(output_schema, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    content = _run_summary_provider(
        config,
        prompt,
        output_path,
        image_paths,
        task_name=task_name,
        output_schema_path=output_schema_path,
        cancel_check=cancel_check,
        process_callback=process_callback,
    )
    if output_path.exists():
        content = output_path.read_text(encoding="utf-8")
    else:
        output_path.write_text(content, encoding="utf-8")
    return {
        "task": task_name,
        "id": _slug(output_path.stem),
        "output_path": str(output_path),
        "prompt_path": str(prompt_path),
        "output_schema_path": str(output_schema_path) if output_schema_path else None,
        "content": content,
        "image_paths": image_paths,
        "generated_by_ai": config.daily.ai.provider != "disabled",
        "metadata": metadata,
    }


def _static_artifact(
    *,
    task: str,
    output_path: Path,
    content: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    return {
        "task": task,
        "id": _slug(output_path.stem),
        "output_path": str(output_path),
        "prompt_path": None,
        "output_schema_path": None,
        "content": content,
        "image_paths": [],
        "generated_by_ai": False,
        "metadata": metadata,
    }


def _message_point_output_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "points": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "source_message_ids": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "minItems": 1,
                            "maxItems": 20,
                        },
                        "tags": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 12,
                        },
                        "content": {"type": "string"},
                        "importance_score": {"type": "integer", "minimum": 1, "maximum": 5},
                        "importance_reason": {"type": "string"},
                    },
                    "required": [
                        "source_message_ids",
                        "tags",
                        "content",
                        "importance_score",
                        "importance_reason",
                    ],
                },
            }
        },
        "required": ["points"],
    }


def _message_point_extraction_prompt(
    origin_payload: dict[str, Any],
    messages: list[dict[str, Any]],
    media_artifacts: dict[str, dict[str, Any]],
) -> str:
    origin = origin_payload.get("origin") or {}
    tag_instruction = _tag_specific_instruction(origin.get("tags"))
    evidence: list[dict[str, Any]] = []
    for message in messages:
        media_evidence = []
        for media in message.get("media_files") or []:
            file_path = str(media.get("file_path") or "")
            artifact = media_artifacts.get(file_path) or {}
            media_evidence.append(
                {
                    "file_path": file_path,
                    "file_name": Path(file_path).name if file_path else "",
                    "media_kind": media.get("media_kind"),
                    "analysis": _truncate_text(str(artifact.get("content") or ""), 3000),
                }
            )
        evidence.append(
            {
                "message_id": message.get("message_id"),
                "speaker": message.get("speaker"),
                "sent_at": message.get("sent_at"),
                "local_sent_at": message.get("local_sent_at"),
                "text": message.get("text"),
                "media": media_evidence,
            }
        )
    return (
        "TASK: message_point_extraction\n"
        "你是 Telegram 每日消息点提取器。请从当前批次中提取独立、可复用、值得后续查看的信息点，"
        "覆盖事实、事件、公告、观点变化、资源、链接、数值、时间、决定、行动项、风险与机会。"
        "合并表达同一件事的相邻消息，跳过纯表情、问候、重复转发和没有上下文的低价值闲聊。\n"
        "每个 point 只返回输入中真实存在的 `source_message_ids`；不要返回时间或 URL，"
        "系统会从这些消息 ID 回填可信的时间和 Telegram deeplink。"
        "`content` 必须是一句可脱离聊天上下文理解的中文描述，不得编造输入中没有的信息。\n"
        "`importance_score` 使用 1-5：1=低价值背景，2=可参考，3=有明确价值，4=重要，5=紧急或高影响；"
        "来源是否 important 与单个 point 的 score 是两个不同概念。"
        "`tags` 使用简短内容标签，不带 #；`importance_reason` 用一句话说明评分依据。\n"
        "严格按提供的 JSON Schema 输出，不要输出 Markdown、代码围栏或额外说明。\n\n"
        f"{tag_instruction}\n"
        "Origin metadata:\n"
        f"{json.dumps(origin, ensure_ascii=False, indent=2)}\n\n"
        "Message evidence:\n"
        f"{json.dumps(evidence, ensure_ascii=False, indent=2)}\n"
    )


def _validated_message_points(
    content: str,
    *,
    origin_payload: dict[str, Any],
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    payload = _parse_json_object(content)
    raw_points = payload.get("points") if isinstance(payload, dict) else None
    if not isinstance(raw_points, list):
        return []
    message_by_id = {
        int(message["message_id"]): message
        for message in messages
        if message.get("message_id") is not None
    }
    origin = origin_payload.get("origin") or {}
    origin_tags = parse_tags(origin.get("tags"))
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[tuple[int, ...], str]] = set()
    for raw in raw_points:
        if not isinstance(raw, dict):
            continue
        source_ids: list[int] = []
        for value in raw.get("source_message_ids") or []:
            try:
                message_id = int(value)
            except (TypeError, ValueError):
                continue
            if message_id in message_by_id and message_id not in source_ids:
                source_ids.append(message_id)
        content_text = str(raw.get("content") or "").strip()
        try:
            importance_score = int(raw.get("importance_score"))
        except (TypeError, ValueError):
            continue
        if not source_ids or not content_text or not 1 <= importance_score <= 5:
            continue
        dedupe_key = (tuple(source_ids), content_text.casefold())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        source_messages = [message_by_id[message_id] for message_id in source_ids]
        primary = source_messages[0]
        tags = parse_tags([*origin_tags, *parse_tags(raw.get("tags"))])
        deeplink = next(
            (str(message.get("telegram_deeplink")) for message in source_messages if message.get("telegram_deeplink")),
            None,
        )
        permalink = next(
            (str(message.get("permalink")) for message in source_messages if message.get("permalink")),
            None,
        )
        normalized.append(
            {
                "source": str(origin.get("source") or SOURCE_TELEGRAM),
                "account_id": str(origin.get("account_id") or ""),
                "origin_id": int(origin.get("origin_id") or 0),
                "topic_id": int(origin.get("topic_id") or 0),
                "origin_title": str(origin.get("title") or "") or None,
                "origin_important": bool(origin.get("important")),
                "message_id": int(primary["message_id"]),
                "occurred_at": str(primary.get("sent_at") or "") or None,
                "local_occurred_at": str(primary.get("local_sent_at") or "") or None,
                "tags": tags,
                "content": content_text,
                "telegram_deeplink": deeplink,
                "permalink": permalink,
                "importance_score": importance_score,
                "importance_reason": str(raw.get("importance_reason") or "").strip() or None,
                "source_message_ids": source_ids,
                "source_refs": [
                    f"{origin.get('account_id')}/{origin.get('origin_id')}/{origin.get('topic_id') or 0}/{message_id}"
                    for message_id in source_ids
                ],
            }
        )
    return normalized


def _parse_json_object(content: str) -> dict[str, Any]:
    text = str(content or "").strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        text = fenced.group(1).strip()
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            return {}
        try:
            value = json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            return {}
    return value if isinstance(value, dict) else {}


def _daily_point_summary_prompt(
    package_payload: dict[str, Any],
    message_points: list[dict[str, Any]],
) -> str:
    compact_points = [
        {
            "time": point.get("local_occurred_at") or point.get("occurred_at"),
            "tags": point.get("tags") or [],
            "content": point.get("content"),
            "telegram_deeplink": point.get("telegram_deeplink"),
            "importance_score": point.get("importance_score"),
            "origin_title": point.get("origin_title"),
            "origin_ref": f"{point.get('account_id')}/{point.get('origin_id')}/{point.get('topic_id') or 0}",
            "source_message_ids": point.get("source_message_ids") or [],
        }
        for point in message_points
    ]
    return (
        "TASK: daily_point_summary\n"
        "你是 Telegram 每日消息点日报编辑器。输入是已经验证并持久化前标准化的 message points；"
        "不要回看或重新概括原始消息，也不要添加输入之外的事实。\n"
        "输出可直接发送到 Telegram 的 Markdown：先给出 3-8 条 Highest Priority Points，"
        "再按时间段和 tag 聚合当日信息点。每个主题保留关键内容、重要程度和可用的 Telegram deeplink；"
        "合并重复 point，但不要丢失不同来源的冲突、数值、日期、资源或行动项。"
        "最后给出 Watch Next。不要输出逐条原始消息流水账，不要输出 JSON。\n\n"
        "Daily metadata:\n"
        f"{json.dumps({'date': package_payload.get('date'), 'timezone': package_payload.get('timezone'), 'point_count': len(compact_points)}, ensure_ascii=False, indent=2)}\n\n"
        "Validated message points:\n"
        f"{json.dumps(compact_points, ensure_ascii=False, indent=2)}\n"
    )


def _write_combined_daily_summary(
    path: Path,
    *,
    important_content: str,
    point_content: str,
) -> None:
    path.write_text(
        "# Daily Analysis\n\n"
        "## Important Full Summary\n\n"
        + str(important_content or "").strip()
        + "\n\n## Daily Message Point Summary\n\n"
        + str(point_content or "").strip()
        + "\n",
        encoding="utf-8",
    )


def _analysis_record_payload(analysis: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_count": (
            len(analysis.get("media") or [])
            + len(analysis.get("message_point_extractions") or [])
            + len(analysis.get("important_origins") or [])
            + (1 if analysis.get("important_summary") else 0)
            + (1 if analysis.get("point_summary") else 0)
        ),
        "media": [_artifact_payload(item) for item in analysis.get("media") or []],
        "message_point_extractions": [
            _artifact_payload(item) for item in analysis.get("message_point_extractions") or []
        ],
        "message_points": list(analysis.get("message_points") or []),
        "normal_origins": [],
        "normal_groups": [],
        "important_origins": [_artifact_payload(item) for item in analysis.get("important_origins") or []],
        "important_summary": _artifact_payload(analysis.get("important_summary") or {}),
        "point_summary": _artifact_payload(analysis.get("point_summary") or {}),
        "final": _artifact_payload(analysis.get("final") or {}),
    }


def _artifact_payload(artifact: dict[str, Any]) -> dict[str, Any]:
    if not artifact:
        return {}
    return {
        "task": artifact.get("task"),
        "id": artifact.get("id"),
        "output_path": artifact.get("output_path"),
        "prompt_path": artifact.get("prompt_path"),
        "output_schema_path": artifact.get("output_schema_path"),
        "image_paths": artifact.get("image_paths") or [],
        "generated_by_ai": bool(artifact.get("generated_by_ai")),
        "metadata": artifact.get("metadata") or {},
        "content": artifact.get("content") or "",
    }


def _media_descriptor(origin_payload: dict[str, Any], message: dict[str, Any], media: dict[str, Any]) -> dict[str, Any]:
    origin = origin_payload.get("origin") or {}
    file_path = str(media.get("file_path") or "")
    return {
        "source": origin.get("source"),
        "account_id": origin.get("account_id"),
        "origin_id": origin.get("origin_id"),
        "topic_id": origin.get("topic_id") or 0,
        "origin_title": origin.get("title"),
        "origin_tags": origin.get("tags") or [],
        "message_id": message.get("message_id"),
        "local_sent_at": message.get("local_sent_at"),
        "speaker": message.get("speaker"),
        "message_text": message.get("text"),
        "file_index": media.get("file_index"),
        "file_path": file_path,
        "file_name": Path(file_path).name if file_path else "",
        "media_kind": media.get("media_kind"),
        "mime_type": media.get("mime_type"),
        "content_type": media.get("content_type"),
        "file_size": media.get("file_size"),
    }


def _media_image_analysis_prompt(descriptor: dict[str, Any]) -> str:
    return (
        "TASK: media_image_analysis\n"
        "你是 Telegram 每日归档中的图片分析器。请只基于提供的图片和元数据输出 Markdown。\n"
        "目标：判断图片是文字为主、图像信息为主、混合，或无法判断；文字为主时做 OCR；"
        "图像信息为主时提取可见事实、图表/截图含义和与消息文本的关系。\n"
        "输出固定包含：\n"
        "- `classification`: text_dominant | image_info | mixed | unclear\n"
        "- `ocr_text`: 若有可读文字，尽量完整转写；没有则写 none\n"
        "- `visual_facts`: 图像中的客观信息\n"
        "- `archive_content`: 可插入后续 important/group 分析的内容片段\n"
        "- `source_refs`: file_path、origin、message_id\n\n"
        "Media metadata:\n"
        f"{json.dumps(descriptor, ensure_ascii=False, indent=2)}\n"
    )


def _media_reference_markdown(descriptor: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Media File Reference",
            "",
            f"- file_path: `{descriptor.get('file_path')}`",
            f"- file_name: `{descriptor.get('file_name')}`",
            f"- media_kind: `{descriptor.get('media_kind')}`",
            f"- mime_type: `{descriptor.get('mime_type') or descriptor.get('content_type')}`",
            f"- origin: `{descriptor.get('account_id')}/{descriptor.get('origin_id')}/{descriptor.get('topic_id')}`",
            f"- message_id: `{descriptor.get('message_id')}`",
            "",
            "This long or non-image media item is preserved by path and file name only.",
            "",
        ]
    )


def _tag_specific_instruction(*tag_values: Any) -> str:
    tags: list[str] = []
    for value in tag_values:
        tags.extend(parse_tags(value))
    if "info" not in {normalize_tag(tag) for tag in tags}:
        return ""
    return (
        "Tag-specific instruction for `info`:\n"
        "这个 tag 表示当前分组/来源的核心目标是获取信息。请重点收集群组中的信息点并返回："
        "事实、公告、事件、资源、链接、数值、时间、行动项、争议点和 source_refs。"
        "不要只做闲聊氛围、情绪或泛泛摘要；低价值闲聊可以标为 noise，但有信息量的短句也要保留。\n"
    )


def _topic_summary_instruction() -> str:
    return (
        "Readable summary format:\n"
        "- 不要把输入消息机械地逐条重排成消息列表；先按话题、事件、线索或决策聚合。\n"
        "- 每个 topic 使用 `### 主题标题 ([起始消息](telegram_deeplink 或 source_ref))`，链接取该 topic 第一条关键消息；"
        "优先使用 `telegram_deeplink` 这种 `tg://` 链接，不要把网页版 `https://t.me/...` 当作首选链接。"
        "如果没有 telegram_deeplink，就在标题或 bullet 中保留 origin title/message_id。\n"
        "- 每个 topic 下用 2-5 条 bullet 写清楚发生了什么、谁在讨论、关键结论/资源/数值/时间、后续行动。\n"
        "- 低价值闲聊、重复表情和无上下文短句合并进 `低价值/噪声`，不要逐条复述。\n"
        "- 保留必要 source_refs，但不要为了引用而把每条消息都展开成独立 bullet。\n"
    )


def _important_origin_analysis_prompt(
    origin_payload: dict[str, Any],
    media_artifacts: dict[str, dict[str, Any]],
) -> str:
    payload = _compact_origin_for_analysis(origin_payload, media_artifacts, message_limit=None)
    tag_instruction = _tag_specific_instruction((origin_payload.get("origin") or {}).get("tags"))
    return (
        "TASK: important_origin_analysis\n"
        "你是 Telegram 每日归档的 important origin 分析器。这个 origin 需要单独分析，不能只并入普通 tag group。\n"
        "important origin 永远按全量消息处理：输入中的 messages 已包含查询时间窗内全部消息，"
        "必须扫描全部消息文本、发言人、时间和 source_refs，不要做 200 条截断摘要；"
        "但最终输出要按话题/事件/决策聚合，不要把全部消息按时间顺序机械列出来。\n"
        "请先做 `Segment Importance Scan`：按时间段/讨论段落判断重要度，再根据 media 所在消息或前后上下文的重要度决定是否处理 media。"
        "只有重要上下文中的图片才需要 OCR/视觉事实提取并插入记录；低重要度 media 只列出路径和跳过原因。"
        "PDF/视频等长内容只保留路径和文件名，除非上下文显示它是重要信息源。\n"
        f"{_topic_summary_instruction()}"
        "输出 Markdown，包含：\n"
        "## Important Topic / Event Summary\n"
        "## Segment Importance Scan\n"
        "## Important Decisions / Action Items / Risks\n"
        "## Tags\n"
        "## Media Handling\n"
        "## Source Refs\n\n"
        f"{tag_instruction}\n"
        "Important origin package:\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n"
    )


def _important_daily_summary_prompt(
    package_payload: dict[str, Any],
    *,
    important_artifacts: list[dict[str, Any]],
    media_artifacts: dict[str, dict[str, Any]],
) -> str:
    payload = {
        "run_id": package_payload.get("run_id"),
        "date": package_payload.get("date"),
        "timezone": package_payload.get("timezone"),
        "stats": package_payload.get("stats"),
        "important_origin_analyses": [
            {
                "origin": artifact.get("metadata", {}).get("origin"),
                "analysis": _truncate_text(str(artifact.get("content") or ""), 12000),
            }
            for artifact in important_artifacts
        ],
        "media_references": [
            {
                "file_path": artifact.get("metadata", {}).get("file_path"),
                "task": artifact.get("task"),
                "content": _truncate_text(str(artifact.get("content") or ""), 3000),
            }
            for artifact in media_artifacts.values()
        ],
    }
    return (
        "TASK: important_daily_summary\n"
        "你是每日 Telegram 归档的 important 全量总结器。请只基于已经完成的 important origin 全量分析"
        "以及相关 media 证据输出 Markdown；不得混入 normal origin 或 message point summary。"
        "最终读者需要一份可浏览的 important 日报，不需要看到逐条消息流水账。\n"
        "要求：\n"
        "1. 先给出 Important Highlights，每个重点按主题/事件写标题、起始消息链接和 2-5 条 bullet。\n"
        "2. 汇总跨 important origin 的决定、行动项、风险、机会、资源、数值和时间。\n"
        "3. 把图片 OCR/图像分析作为内容依据引用；PDF/视频只引用路径和文件名，不编造内容。\n"
        "4. 每个结论尽量保留 source_refs，引用 origin title/message_id/file_path。\n"
        "5. 输出应是可直接阅读的 Markdown，不要返回 JSON。\n"
        f"{_topic_summary_instruction()}\n"
        "Analysis inputs:\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n"
    )


def _compact_origin_for_analysis(
    origin_payload: dict[str, Any],
    media_artifacts: dict[str, dict[str, Any]],
    *,
    message_limit: int | None = 200,
) -> dict[str, Any]:
    messages = []
    raw_messages = origin_payload.get("messages") or []
    included_messages = raw_messages if message_limit is None else raw_messages[:message_limit]
    truncated_message_count = 0 if message_limit is None else max(0, len(raw_messages) - message_limit)
    for message in included_messages:
        media_files = []
        for media in message.get("media_files") or []:
            file_path = str(media.get("file_path") or "")
            artifact = media_artifacts.get(file_path)
            media_files.append(
                {
                    **media,
                    "file_name": Path(file_path).name if file_path else "",
                    "image_markdown": f"![media]({file_path})" if _is_image_media(media) and file_path else None,
                    "analysis_task": artifact.get("task") if artifact else None,
                    "analysis": _truncate_text(str((artifact or {}).get("content") or ""), 5000),
                }
            )
        messages.append(
            {
                "message_id": message.get("message_id"),
                "speaker": message.get("speaker"),
                "local_sent_at": message.get("local_sent_at"),
                "text": message.get("text"),
                "permalink": message.get("permalink"),
                "telegram_deeplink": message.get("telegram_deeplink") or _telegram_deeplink(message),
                "media_files": media_files,
            }
        )
    return {
        "origin": origin_payload.get("origin"),
        "package_meta": origin_payload.get("package_meta"),
        "message_count": len(raw_messages),
        "full_content_policy": (
            "all_messages_included"
            if truncated_message_count == 0
            else f"first_{message_limit}_messages_included"
        ),
        "messages": messages,
        "truncated_message_count": truncated_message_count,
    }


def _origin_ref(origin_payload: dict[str, Any]) -> str:
    origin = origin_payload.get("origin") or {}
    return f"{origin.get('account_id')}/{origin.get('origin_id')}/{origin.get('topic_id') or 0}"


def _origin_image_paths(origin_payload: dict[str, Any], limit: int | None = 20) -> list[str]:
    paths: list[str] = []
    for message in origin_payload.get("messages") or []:
        for media in message.get("media_files") or []:
            file_path = str(media.get("file_path") or "")
            if _is_image_media(media) and file_path and Path(file_path).is_file():
                paths.append(file_path)
                if limit is not None and len(paths) >= limit:
                    return paths
    return paths


def _is_image_media(media: dict[str, Any]) -> bool:
    content_type = str(media.get("content_type") or media.get("mime_type") or "")
    return content_type.startswith("image/")


def _truncate_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "\n...[truncated]"


def update_daily_package_schedule(
    store: ArchiveStore,
    config: AppConfig,
    payload: dict[str, Any],
) -> dict[str, Any]:
    current = store.get_daily_package_schedule()
    scope = payload.get("scope", current.get("scope") or {})
    if not isinstance(scope, dict):
        raise ValueError("scope must be an object")
    time_of_day = str(payload.get("time_of_day", current.get("time_of_day") or "08:00"))
    _validate_time_of_day(time_of_day)
    timezone_name = str(payload.get("timezone", current.get("timezone") or "Asia/Tokyo"))
    _zoneinfo(timezone_name)
    enabled = _payload_bool(payload, "enabled", bool(current.get("enabled", False)))
    system_manager = str(payload.get("system_manager", current.get("system_manager") or "systemd-user"))
    installed = bool(current.get("installed", False))
    last_installed_at = current.get("last_installed_at")
    last_error = None
    if system_manager != "systemd-user":
        last_error = f"Unsupported system_manager: {system_manager}"
    else:
        install_result = install_daily_package_timer(
            config,
            {
                "enabled": enabled,
                "time_of_day": time_of_day,
                "timezone": timezone_name,
                "scope": scope,
                "system_manager": system_manager,
            },
            activate=_payload_bool(payload, "activate_systemd", False),
        )
        installed = install_result["installed"]
        last_installed_at = install_result["last_installed_at"]
        last_error = install_result.get("last_error")

    return store.set_daily_package_schedule(
        DailyPackageScheduleRecord(
            enabled=enabled,
            time_of_day=time_of_day,
            timezone=timezone_name,
            scope_json=json.dumps(scope, ensure_ascii=False, sort_keys=True),
            system_manager=system_manager,
            installed=installed,
            last_installed_at=last_installed_at,
            last_error=last_error,
            updated_at=utc_now_iso(),
        )
    )


def install_daily_package_timer(
    config: AppConfig,
    schedule: dict[str, Any],
    *,
    activate: bool = False,
) -> dict[str, Any]:
    if config.config_path is None:
        return {"installed": False, "last_installed_at": None, "last_error": "config_path is required"}
    systemd_dir = config.daily.systemd_user_dir or (Path.home() / ".config" / "systemd" / "user")
    try:
        systemd_dir.mkdir(parents=True, exist_ok=True)
        service_path = systemd_dir / f"{DAILY_SYSTEMD_BASENAME}.service"
        timer_path = systemd_dir / f"{DAILY_SYSTEMD_BASENAME}.timer"
        service_path.write_text(_systemd_service(config), encoding="utf-8")
        timer_path.write_text(
            _systemd_timer(
                time_of_day=str(schedule.get("time_of_day") or "08:00"),
                timezone_name=str(schedule.get("timezone") or "Asia/Tokyo"),
            ),
            encoding="utf-8",
        )
        if activate:
            subprocess.run(["systemctl", "--user", "daemon-reload"], check=True, capture_output=True, text=True)
            timer_unit = f"{DAILY_SYSTEMD_BASENAME}.timer"
            if schedule.get("enabled"):
                subprocess.run(["systemctl", "--user", "enable", "--now", timer_unit], check=True, capture_output=True, text=True)
            else:
                subprocess.run(["systemctl", "--user", "disable", "--now", timer_unit], check=True, capture_output=True, text=True)
        return {"installed": True, "last_installed_at": utc_now_iso(), "last_error": None}
    except Exception as exc:
        return {"installed": False, "last_installed_at": None, "last_error": str(exc)}


def read_run_content(store: ArchiveStore, run_type: str, run_id: str, content_format: str) -> tuple[str, str]:
    if run_type == "package":
        item = store.get_daily_package_run(run_id)
        if item is None:
            raise ValueError("Unknown daily package run")
        path = item.get("package_json_path") if content_format == "json" else item.get("package_md_path")
    elif run_type == "summary":
        item = store.get_daily_summary_run(run_id)
        if item is None:
            raise ValueError("Unknown daily summary run")
        path = item.get("summary_path")
        content_format = "md"
        if path and Path(str(path)).is_file():
            return Path(str(path)).read_text(encoding="utf-8"), "text/markdown; charset=utf-8"
        record = store.get_daily_summary_record(summary_id=run_id)
        if record is not None and record.get("content_md") is not None:
            return str(record["content_md"]), "text/markdown; charset=utf-8"
    else:
        raise ValueError("Unknown run type")
    if not path:
        raise ValueError("Run content is not available")
    file_path = Path(str(path))
    if not file_path.is_file():
        raise ValueError("Run content file is missing")
    content_type = "application/json; charset=utf-8" if content_format == "json" else "text/markdown; charset=utf-8"
    return file_path.read_text(encoding="utf-8"), content_type


def _select_package_origins(store: ArchiveStore, scope: dict[str, Any]) -> list[dict[str, Any]]:
    account_id = scope.get("account_id")
    origins = store.list_origins(account_id=str(account_id) if account_id else None, include_archived=False)
    parent_lookup = {
        _parent_lookup_key(origin): origin
        for origin in origins
        if int(origin.get("topic_id") or 0) == 0
    }
    selected: list[dict[str, Any]] = []
    required_tags = {normalize_tag(tag) for tag in parse_tags(scope.get("tags"))}
    origin_id = scope.get("origin_id")
    topic_id = scope.get("topic_id")
    for origin in origins:
        policy = origin.get("backup_policy")
        if not policy or not policy.get("enabled"):
            continue
        if origin.get("source") != SOURCE_TELEGRAM:
            continue
        if origin_id not in (None, "") and int(origin["origin_id"]) != int(origin_id):
            continue
        if topic_id not in (None, "") and int(origin.get("topic_id") or 0) != int(topic_id):
            continue
        local_tags = parse_tags(policy.get("tags"))
        tags = local_tags
        topic_grouping = "origin"
        parent_tags: list[str] = []
        parent_important = False
        if int(origin.get("topic_id") or 0):
            parent = parent_lookup.get(_parent_lookup_key(origin))
            parent_policy = (parent or {}).get("backup_policy") or {}
            parent_tags = parse_tags(parent_policy.get("tags"))
            parent_important = bool((parent or {}).get("important"))
            local_normalized = _normalized_tag_set(local_tags)
            parent_normalized = _normalized_tag_set(parent_tags)
            topic_has_own_grouping = bool(origin.get("important")) or (
                bool(local_normalized) and local_normalized != parent_normalized
            )
            if topic_has_own_grouping:
                topic_grouping = "topic"
            else:
                tags = parent_tags
                topic_grouping = "parent"
        normalized = _normalized_tag_set(tags)
        if required_tags and not required_tags.issubset(normalized):
            continue
        item = dict(origin)
        item["tags"] = tags
        item["local_tags"] = local_tags
        if parent_tags:
            item["parent_tags"] = parent_tags
        if topic_grouping != "origin":
            item["tag_grouping"] = topic_grouping
        if parent_important and topic_grouping == "parent":
            item["important"] = True
            item["parent_important"] = True
        selected.append(item)
    return selected


def _parent_lookup_key(origin: dict[str, Any]) -> tuple[str, str, int]:
    return (
        str(origin.get("source") or SOURCE_TELEGRAM),
        str(origin.get("account_id") or ""),
        int(origin.get("origin_id") or 0),
    )


def _origin_identity(origin: dict[str, Any]) -> tuple[str, str, int, int]:
    return (
        str(origin.get("source") or SOURCE_TELEGRAM),
        str(origin.get("account_id") or ""),
        int(origin.get("origin_id") or 0),
        int(origin.get("topic_id") or 0),
    )


def _packaged_message_identity(message: dict[str, Any]) -> tuple[str, str, int, int]:
    return (
        str(message.get("source") or SOURCE_TELEGRAM),
        str(message.get("account_id") or ""),
        int(message.get("chat_id") or 0),
        int(message.get("message_id") or 0),
    )


def _canonicalize_point_origin_packages(
    origin_packages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Assign each archived message to one point origin, preferring a topic over its parent."""
    topic_message_keys = {
        _packaged_message_identity(message)
        for origin_payload in origin_packages
        if int((origin_payload.get("origin") or {}).get("topic_id") or 0)
        for message in origin_payload.get("messages") or []
    }
    seen: set[tuple[str, str, int, int]] = set()
    canonical: list[dict[str, Any]] = []
    for origin_payload in origin_packages:
        origin = origin_payload.get("origin") or {}
        is_parent = int(origin.get("topic_id") or 0) == 0
        messages: list[dict[str, Any]] = []
        for message in origin_payload.get("messages") or []:
            key = _packaged_message_identity(message)
            if is_parent and key in topic_message_keys:
                continue
            if key in seen:
                continue
            seen.add(key)
            messages.append(message)
        if not messages:
            continue
        media_count = sum(len(message.get("media_files") or []) for message in messages)
        canonical.append(
            {
                **origin_payload,
                "package_meta": {
                    **(origin_payload.get("package_meta") or {}),
                    "message_count": len(messages),
                    "media_count": media_count,
                    "point_canonicalized": True,
                },
                "messages": messages,
            }
        )
    return canonical


def _normalized_tag_set(tags: Any) -> frozenset[str]:
    return frozenset(normalize_tag(tag) for tag in parse_tags(tags) if normalize_tag(tag))


def _origin_has_messages(
    store: ArchiveStore,
    origin: dict[str, Any],
    window_start_utc: datetime,
    window_end_utc: datetime,
) -> bool:
    return bool(
        store.list_messages_for_origin_window(
            str(origin.get("source") or SOURCE_TELEGRAM),
            str(origin["account_id"]),
            int(origin["origin_id"]),
            topic_id=int(origin.get("topic_id") or 0),
            window_start=window_start_utc.isoformat(),
            window_end=window_end_utc.isoformat(),
            limit=1,
        )
    )


def _package_origin(
    store: ArchiveStore,
    origin: dict[str, Any],
    window_start_utc: datetime,
    window_end_utc: datetime,
    local_tz: ZoneInfo,
) -> dict[str, Any]:
    messages: list[dict[str, Any]] = []
    offset = 0
    while True:
        page = store.list_messages_for_origin_window(
            str(origin.get("source") or SOURCE_TELEGRAM),
            str(origin["account_id"]),
            int(origin["origin_id"]),
            topic_id=int(origin.get("topic_id") or 0),
            window_start=window_start_utc.isoformat(),
            window_end=window_end_utc.isoformat(),
            limit=PACKAGE_MESSAGE_PAGE_SIZE,
            offset=offset,
        )
        messages.extend(page)
        if len(page) < PACKAGE_MESSAGE_PAGE_SIZE:
            break
        offset += len(page)
    media_by_message = store.list_media_files_for_messages(messages)
    packaged_messages = []
    media_count = 0
    for message in messages:
        key = (
            message.get("source"),
            message.get("account_id"),
            message.get("chat_id"),
            message.get("message_id"),
        )
        media_files = [
            _package_media_file(media)
            for media in media_by_message.get(key, [])
        ]
        media_count += len(media_files)
        packaged_messages.append(_package_message(message, media_files, local_tz))
    return {
        "origin": _origin_summary(origin),
        "package_meta": {
            "message_count": len(packaged_messages),
            "media_count": media_count,
            "window_start": window_start_utc.isoformat(),
            "window_end": window_end_utc.isoformat(),
            "generated_at": utc_now_iso(),
        },
        "messages": packaged_messages,
    }


def _origin_summary(origin: dict[str, Any]) -> dict[str, Any]:
    summary = {
        "source": origin.get("source"),
        "account_id": origin.get("account_id"),
        "origin_id": origin.get("origin_id"),
        "topic_id": origin.get("topic_id") or 0,
        "origin_type": origin.get("origin_type"),
        "title": origin.get("title"),
        "username": origin.get("username"),
        "tags": parse_tags(origin.get("tags") or (origin.get("backup_policy") or {}).get("tags")),
        "important": bool(origin.get("important")),
        "last_message_at": origin.get("last_message_at"),
    }
    if "local_tags" in origin:
        summary["local_tags"] = parse_tags(origin.get("local_tags"))
    if "parent_tags" in origin:
        summary["parent_tags"] = parse_tags(origin.get("parent_tags"))
    if origin.get("tag_grouping"):
        summary["tag_grouping"] = origin.get("tag_grouping")
    if origin.get("parent_important"):
        summary["parent_important"] = True
    return summary


def _package_message(message: dict[str, Any], media_files: list[dict[str, Any]], local_tz: ZoneInfo) -> dict[str, Any]:
    sent_at = str(message.get("sent_at") or "")
    return {
        "source": message.get("source"),
        "account_id": message.get("account_id"),
        "chat_id": message.get("chat_id"),
        "message_id": message.get("message_id"),
        "topic_id": message.get("topic_id"),
        "speaker": message.get("sender_name") or message.get("sender_username") or message.get("sender_id"),
        "sender_id": message.get("sender_id"),
        "sender_username": message.get("sender_username"),
        "sent_at": sent_at,
        "local_sent_at": _local_iso(sent_at, local_tz),
        "text": message.get("text"),
        "has_media": bool(message.get("has_media")),
        "media_kind": message.get("media_kind"),
        "permalink": message.get("permalink"),
        "telegram_deeplink": _telegram_deeplink(message),
        "media_files": media_files,
    }


def _telegram_deeplink(message: dict[str, Any]) -> str | None:
    permalink = str(message.get("permalink") or "")
    message_id = message.get("message_id")
    if not message_id:
        return None

    match = re.match(r"^https://t\.me/c/(?P<channel>\d+)/(?P<post>\d+)(?:\?.*)?$", permalink)
    if match:
        return f"tg://privatepost?channel={match.group('channel')}&post={match.group('post')}"

    match = re.match(r"^https://t\.me/(?P<domain>[A-Za-z0-9_]+)/(?P<post>\d+)(?:\?.*)?$", permalink)
    if match:
        return f"tg://resolve?domain={match.group('domain')}&post={match.group('post')}"

    chat_id = str(message.get("chat_id") or "")
    if chat_id.startswith("-100"):
        return f"tg://privatepost?channel={chat_id[4:]}&post={message_id}"
    if chat_id.startswith("-"):
        return f"tg://privatepost?channel={chat_id[1:]}&post={message_id}"
    return None


def _package_media_file(media: dict[str, Any]) -> dict[str, Any]:
    file_path = str(media.get("file_path") or "")
    mime_type = media.get("mime_type") or mimetypes.guess_type(file_path)[0]
    return {
        "file_index": media.get("file_index"),
        "file_path": file_path,
        "media_kind": media.get("media_kind"),
        "mime_type": mime_type,
        "file_size": media.get("file_size"),
        "downloaded_at": media.get("downloaded_at"),
        "content_type": mime_type or "application/octet-stream",
    }


def _run_summary_provider(
    config: AppConfig,
    prompt: str,
    output_path: Path,
    image_paths: list[str],
    *,
    task_name: str = "summary",
    output_schema_path: Path | None = None,
    cancel_check: Callable[[], None] | None = None,
    process_callback: Callable[[subprocess.Popen[str] | None, str], None] | None = None,
) -> str:
    if cancel_check:
        cancel_check()
    if config.daily.ai.provider == "disabled":
        text = (
            '{"points": []}\n'
            if output_schema_path is not None
            else f"# AI Task Disabled\n\nTask: `{task_name}`\n\nAI provider is disabled for this run.\n"
        )
        output_path.write_text(text, encoding="utf-8")
        return text
    command = _expand_command(
        config.daily.ai.command,
        output_path,
        image_paths,
        task_name=task_name,
        model=config.daily.ai.model,
        output_schema_path=output_schema_path,
    )
    config.storage.data_dir.mkdir(parents=True, exist_ok=True)
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(config.storage.data_dir),
        start_new_session=(os.name != "nt"),
    )
    if process_callback:
        process_callback(process, task_name)
    deadline = time_module.monotonic() + max(1, int(config.daily.ai.timeout_seconds))
    input_text: str | None = prompt
    try:
        while True:
            if cancel_check:
                cancel_check()
            timeout = max(0.1, min(0.5, deadline - time_module.monotonic()))
            try:
                stdout, stderr = process.communicate(input=input_text, timeout=timeout)
                break
            except subprocess.TimeoutExpired:
                input_text = None
                if time_module.monotonic() >= deadline:
                    _terminate_process(process)
                    raise RuntimeError(f"AI provider timed out after {config.daily.ai.timeout_seconds} seconds")
    except DailyJobCancelled:
        _terminate_process(process)
        raise
    finally:
        if process_callback:
            process_callback(None, task_name)
    if cancel_check:
        cancel_check()
    if process.returncode != 0:
        detail = stderr.strip() or stdout.strip() or f"exit code {process.returncode}"
        raise RuntimeError(f"AI provider failed: {detail}")
    if output_path.exists():
        return output_path.read_text(encoding="utf-8")
    if stdout.strip():
        output_path.write_text(stdout, encoding="utf-8")
        return stdout
    raise RuntimeError("AI provider completed without writing a summary")


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "nt":
            process.terminate()
        else:
            os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=3)
    except Exception:
        try:
            if os.name == "nt":
                process.kill()
            else:
                os.killpg(process.pid, signal.SIGKILL)
        except Exception:
            pass


def _expand_command(
    command: list[str],
    output_path: Path,
    image_paths: list[str],
    *,
    task_name: str = "summary",
    model: str = "",
    output_schema_path: Path | None = None,
) -> list[str]:
    expanded: list[str] = []
    image_args: list[str] = []
    for path in image_paths:
        image_args.extend(["--image", path])
    for token in command:
        if token == "{images}":
            expanded.extend(image_args)
        elif token == "{model}":
            if model:
                expanded.extend(["--model", model])
        elif token == "{output_schema}":
            if output_schema_path is not None:
                expanded.extend(["--output-schema", str(output_schema_path)])
        else:
            expanded.append(
                token.replace("{output}", str(output_path))
                .replace("{task}", task_name)
                .replace("{model}", model)
                .replace("{output_schema}", str(output_schema_path or ""))
            )
    executable = Path(expanded[0]).name.lower() if expanded else ""
    if executable in {"codex", "codex.exe"} and "exec" in expanded:
        insert_at = expanded.index("exec") + 1
        has_model = any(
            token in {"-m", "--model"} or token.startswith("--model=")
            for token in expanded
        )
        if model and not has_model:
            expanded[insert_at:insert_at] = ["--model", model]
            insert_at += 2
        has_output_schema = any(
            token == "--output-schema" or token.startswith("--output-schema=")
            for token in expanded
        )
        if output_schema_path is not None and not has_output_schema:
            expanded[insert_at:insert_at] = ["--output-schema", str(output_schema_path)]
    return expanded


def _collect_image_paths(package_payload: dict[str, Any], limit: int = 20) -> list[str]:
    paths: list[str] = []
    for origin_payload in _iter_point_origin_payloads(package_payload):
        for message in origin_payload.get("messages") or []:
            for media in message.get("media_files") or []:
                content_type = str(media.get("content_type") or media.get("mime_type") or "")
                file_path = str(media.get("file_path") or "")
                if content_type.startswith("image/") and file_path and Path(file_path).is_file():
                    paths.append(file_path)
                    if len(paths) >= limit:
                        return paths
    return paths


def _collect_media_analysis_targets(package_payload: dict[str, Any]) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    for origin_payload in _iter_point_origin_payloads(package_payload):
        for message in origin_payload.get("messages") or []:
            for media in message.get("media_files") or []:
                file_path = str(media.get("file_path") or "")
                if not file_path or file_path in seen_paths:
                    continue
                seen_paths.add(file_path)
                targets.append({"origin_payload": origin_payload, "message": message, "media": media})
    return targets


def _iter_origin_payloads(package_payload: dict[str, Any]) -> list[dict[str, Any]]:
    origins: list[dict[str, Any]] = []
    for group in package_payload.get("normal_groups") or []:
        origins.extend(group.get("origins") or [])
    origins.extend(package_payload.get("important_origins") or [])
    return origins


def _iter_point_origin_payloads(package_payload: dict[str, Any]) -> list[dict[str, Any]]:
    point_origins = package_payload.get("point_origins")
    if isinstance(point_origins, list):
        return point_origins
    return _canonicalize_point_origin_packages(_iter_origin_payloads(package_payload))


def _package_markdown(package_payload: dict[str, Any]) -> str:
    lines = [
        f"# Daily Package {package_payload['date']}",
        "",
        f"- Run: `{package_payload['run_id']}`",
        f"- Timezone: `{package_payload['timezone']}`",
        f"- Window: `{package_payload['window_start_local']}` to `{package_payload['window_end_local']}`",
        f"- Origins: {package_payload['stats']['origin_count']}",
        f"- Messages: {package_payload['stats']['message_count']}",
        f"- Media files: {package_payload['stats']['media_count']}",
        f"- Point origins: {package_payload['stats'].get('point_origin_count', 0)}",
        f"- Canonical point messages: {package_payload['stats'].get('point_message_count', 0)}",
        "",
        "## Normal Groups",
        "",
    ]
    for group in package_payload.get("normal_groups") or []:
        lines.append(f"### {group['name']}")
        lines.append(f"- Tags: {', '.join(group.get('tags') or []) or '-'}")
        lines.append(f"- Origins: {group['origin_count']}; messages: {group['message_count']}; media: {group['media_count']}")
        lines.append("")
    lines.extend(["## Important Origins", ""])
    for origin in package_payload.get("important_origins") or []:
        meta = origin["origin"]
        lines.append(f"### {meta.get('title') or meta.get('origin_id')}")
        lines.append(f"- Messages: {origin['package_meta']['message_count']}; media: {origin['package_meta']['media_count']}")
        lines.append("")
    lines.extend(["## Point Origins", ""])
    for origin in _iter_point_origin_payloads(package_payload):
        meta = origin.get("origin") or {}
        lines.append(f"### {meta.get('title') or meta.get('origin_id')}")
        lines.append(f"- Tags: {', '.join(meta.get('tags') or []) or '-'}")
        lines.append(
            f"- Messages: {origin.get('package_meta', {}).get('message_count', 0)}; "
            f"important origin: {bool(meta.get('important'))}"
        )
        lines.append("")
    return "\n".join(lines)


def _group_markdown(group_payload: dict[str, Any]) -> str:
    lines = [
        f"# Group {group_payload['name']}",
        "",
        f"- Tags: {', '.join(group_payload.get('tags') or []) or '-'}",
        f"- Origins: {group_payload['origin_count']}",
        f"- Messages: {group_payload['message_count']}",
        f"- Media: {group_payload['media_count']}",
        "",
    ]
    for origin in group_payload.get("origins") or []:
        lines.append(_origin_markdown(origin))
    return "\n".join(lines)


def _origin_markdown(origin_payload: dict[str, Any]) -> str:
    meta = origin_payload["origin"]
    lines = [
        f"## {meta.get('title') or meta.get('origin_id')}",
        "",
        f"- Origin: `{meta.get('account_id')}/{meta.get('origin_id')}/{meta.get('topic_id')}`",
        f"- Tags: {', '.join(meta.get('tags') or []) or '-'}",
        f"- Important: {meta.get('important')}",
        f"- Messages: {origin_payload['package_meta']['message_count']}",
        f"- Media: {origin_payload['package_meta']['media_count']}",
        "",
    ]
    for message in origin_payload.get("messages") or []:
        speaker = message.get("speaker") or "-"
        text = str(message.get("text") or "").replace("\n", " ")
        lines.append(f"- `{message.get('local_sent_at')}` {speaker}: {text}")
        for media in message.get("media_files") or []:
            lines.append(f"  - media `{media.get('media_kind') or media.get('mime_type')}`: `{media.get('file_path')}`")
    lines.append("")
    return "\n".join(lines)


def _systemd_service(config: AppConfig) -> str:
    config_path = str(config.config_path)
    exec_start = " ".join(
        [
            "/usr/bin/env",
            shlex.quote(config.daily.cli_path),
            "--config",
            shlex.quote(config_path),
            "daily-run",
            "--scheduled",
        ]
    )
    return "\n".join(
        [
            "[Unit]",
            "Description=tele-mess-core daily package and summary run",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=oneshot",
            f"WorkingDirectory={shlex.quote(str(Path(config_path).parent))}",
            "Environment=PYTHONUNBUFFERED=1",
            "TimeoutStartSec=0",
            f"ExecStart={exec_start}",
            "",
        ]
    )


def _systemd_timer(time_of_day: str, timezone_name: str) -> str:
    _validate_time_of_day(time_of_day)
    _zoneinfo(timezone_name)
    return "\n".join(
        [
            "[Unit]",
            "Description=Run tele-mess-core daily package and summary",
            "",
            "[Timer]",
            f"OnCalendar=*-*-* {time_of_day}:00 {timezone_name}",
            "Persistent=true",
            f"Unit={DAILY_SYSTEMD_BASENAME}.service",
            "",
            "[Install]",
            "WantedBy=timers.target",
            "",
        ]
    )


def _daily_output_dir(config: AppConfig) -> Path:
    path = config.daily.output_dir or (config.storage.data_dir / "daily-packages")
    path.mkdir(parents=True, exist_ok=True)
    return path


def _resolve_package_date(raw_date: Any, tz: ZoneInfo) -> date_type:
    if raw_date:
        return date_type.fromisoformat(str(raw_date))
    return datetime.now(tz).date() - timedelta(days=1)


def _zoneinfo(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except Exception as exc:
        raise ValueError(f"Invalid timezone: {name}") from exc


def _validate_time_of_day(value: str) -> None:
    if not re.fullmatch(r"\d{2}:\d{2}", value):
        raise ValueError("time_of_day must use HH:MM")
    hour, minute = (int(part) for part in value.split(":", 1))
    if hour > 23 or minute > 59:
        raise ValueError("time_of_day must use a valid 24-hour time")


def _local_iso(value: str, tz: ZoneInfo) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(tz).isoformat()
    except ValueError:
        return value


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _new_run_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _slug(value: Any) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "item").strip()).strip("-")
    return slug[:80] or "item"


def _origin_file_stem(origin: dict[str, Any]) -> str:
    return _slug(f"{origin.get('account_id')}-{origin.get('origin_id')}-{origin.get('topic_id') or 0}")


def _payload_bool(payload: dict[str, Any], key: str, default: bool = False) -> bool:
    value = payload.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)
