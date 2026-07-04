from __future__ import annotations

import json
import mimetypes
import re
import shlex
import subprocess
import threading
import uuid
from dataclasses import dataclass
from datetime import date as date_type
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from tele_mess_core.archive import ArchiveStore
from tele_mess_core.config import AppConfig
from tele_mess_core.models import (
    DailyPackageRunRecord,
    DailyPackageScheduleRecord,
    DailySummaryRecord,
    DailySummaryRunRecord,
    SOURCE_TELEGRAM,
    utc_now_iso,
)


DAILY_SYSTEMD_BASENAME = "tele-mess-core-daily-package"


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

    store.upsert_daily_package_run(
        DailyPackageRunRecord(
            run_id=run_id,
            status="running",
            date=package_date.isoformat(),
            timezone=timezone_name,
            scope_json=scope_json,
            output_dir=str(output_root),
            started_at=utc_now_iso(),
        )
    )
    try:
        output_root.mkdir(parents=True, exist_ok=True)
        (output_root / "normal-groups").mkdir(exist_ok=True)
        (output_root / "important-origins").mkdir(exist_ok=True)
        (output_root / "analysis").mkdir(exist_ok=True)

        selected_origins = [
            origin
            for origin in _select_package_origins(store, scope)
            if _origin_has_messages(store, origin, window_start_utc, window_end_utc)
        ]
        important_origins = [origin for origin in selected_origins if origin.get("important")]
        normal_origins = [origin for origin in selected_origins if not origin.get("important")]
        tag_groups = parse_tag_groups(scope.get("tag_groups"))
        grouped = (
            assign_origins_to_tag_groups(normal_origins, tag_groups)
            if tag_groups
            else assign_origins_to_effective_tag_groups(normal_origins)
        )

        group_packages = []
        totals = {"origin_count": 0, "message_count": 0, "media_count": 0, "important_origin_count": len(important_origins)}
        for group in grouped["groups"]:
            origin_packages = [
                _package_origin(store, origin, window_start_utc, window_end_utc, tz)
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

        important_packages = []
        for origin in important_origins:
            payload = _package_origin(store, origin, window_start_utc, window_end_utc, tz)
            important_packages.append(payload)
            name = _origin_file_stem(origin)
            _write_json(output_root / "important-origins" / f"{name}.json", payload)
            (output_root / "important-origins" / f"{name}.md").write_text(_origin_markdown(payload), encoding="utf-8")
            totals["origin_count"] += 1
            totals["message_count"] += payload["package_meta"]["message_count"]
            totals["media_count"] += payload["package_meta"]["media_count"]

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
            "unmatched_origins": [_origin_summary(origin) for origin in grouped["unmatched"]],
            "stats": totals,
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
                finished_at=finished,
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
                error=str(exc),
                finished_at=utc_now_iso(),
            )
        )


def run_daily_summary(
    store: ArchiveStore,
    config: AppConfig,
    *,
    package_run_id: str | None = None,
    run_date: str | None = None,
    timezone_name: str | None = None,
    scope: dict[str, Any] | None = None,
    run_id: str | None = None,
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
    run_id = run_id or _new_run_id("sum")
    output_root = Path(str(package_run["output_dir"])) / "analysis" / run_id
    summary_path = output_root / "summary.md"
    prompt_path = output_root / "prompt.md"
    output_root.mkdir(parents=True, exist_ok=True)
    scope_json = json.dumps(scope or package_run.get("scope") or {}, ensure_ascii=False, sort_keys=True)
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
            started_at=utc_now_iso(),
        )
    )
    try:
        analysis = _run_ai_analysis_pipeline(
            config,
            package_payload,
            output_root,
            final_summary_path=summary_path,
            final_prompt_path=prompt_path,
        )
        image_paths = _collect_image_paths(package_payload)
        summary_text = str((analysis.get("final") or {}).get("content") or "")
        (output_root / "summary.json").write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "package_run_id": package_run_id,
                    "provider": config.daily.ai.provider,
                    "image_paths": image_paths,
                    "summary_path": str(summary_path),
                    "analysis": _analysis_record_payload(analysis),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        summary_records = _build_summary_records(
            run_id=run_id,
            package_run_id=str(package_run_id),
            package_run=package_run,
            package_payload=package_payload,
            provider=config.daily.ai.provider,
            summary_path=summary_path,
            summary_text=summary_text,
            image_paths=image_paths,
            analysis=analysis,
        )
        for summary_record in summary_records:
            store.upsert_daily_summary_record(summary_record)
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
                origin_count=int(package_payload.get("stats", {}).get("origin_count") or 0),
                group_count=len(package_payload.get("normal_groups") or []),
                image_count=len(image_paths),
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


def _build_summary_records(
    *,
    run_id: str,
    package_run_id: str,
    package_run: dict[str, Any],
    package_payload: dict[str, Any],
    provider: str,
    summary_path: Path,
    summary_text: str,
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

    for artifact in analysis.get("normal_groups") or []:
        metadata = artifact.get("metadata") or {}
        group_name = str(metadata.get("group_name") or "untagged")
        tags = parse_tags(metadata.get("group_tags"))
        origin_refs = list(metadata.get("origin_refs") or [])
        content = str(artifact.get("content") or "")
        content_json = _summary_record_content_json(
            package_run_id=package_run_id,
            package_run=package_run,
            record_type="tag_group",
            tags=tags,
            image_paths=image_paths,
            artifact=artifact,
            final_summary_path=summary_path,
        )
        records.append(
            DailySummaryRecord(
                summary_id=f"{run_id}--group--{_slug(group_name)}",
                run_id=run_id,
                package_run_id=package_run_id,
                date=date or None,
                timezone=timezone_name or None,
                scope_json=scope_json,
                tags_json=json.dumps(tags, ensure_ascii=False),
                tags_csv=",".join(tags),
                important=False,
                provider=provider,
                title=f"Daily Summary {date} - {group_name}" if date else f"Daily Summary - {group_name}",
                content_md=content,
                content_json=json.dumps(content_json, ensure_ascii=False, sort_keys=True),
                summary_path=str(artifact.get("output_path") or summary_path),
                origin_count=len(origin_refs),
                group_count=1,
                image_count=0,
                content_length=len(content),
                created_at=now,
                updated_at=now,
            )
        )

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

    if records:
        return records

    content_json = _summary_record_content_json(
        package_run_id=package_run_id,
        package_run=package_run,
        record_type="final",
        tags=_collect_summary_tags(package_payload),
        image_paths=image_paths,
        artifact=analysis.get("final") or {},
        final_summary_path=summary_path,
        full_analysis=analysis,
    )
    return [
        DailySummaryRecord(
            summary_id=run_id,
            run_id=run_id,
            package_run_id=package_run_id,
            date=date or None,
            timezone=timezone_name or None,
            scope_json=scope_json,
            tags_json=json.dumps(_collect_summary_tags(package_payload), ensure_ascii=False),
            tags_csv=",".join(_collect_summary_tags(package_payload)),
            important=bool(package_payload.get("important_origins") or stats.get("important_origin_count")),
            provider=provider,
            title=f"Daily Summary {date}" if date else "Daily Summary",
            content_md=summary_text,
            content_json=json.dumps(content_json, ensure_ascii=False, sort_keys=True),
            summary_path=str(summary_path),
            origin_count=int(stats.get("origin_count") or 0),
            group_count=len(package_payload.get("normal_groups") or []),
            image_count=len(image_paths),
            content_length=len(summary_text),
            created_at=now,
            updated_at=now,
        )
    ]


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


def _collect_summary_tags(package_payload: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()

    def add_many(values: Any) -> None:
        for tag in parse_tags(values):
            normalized = normalize_tag(tag)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            tags.append(tag)

    scope = package_payload.get("scope") or {}
    if isinstance(scope, dict):
        add_many(scope.get("tags"))
    for tag_group in package_payload.get("tag_groups") or []:
        if isinstance(tag_group, dict):
            add_many(tag_group.get("tags"))
    for group in package_payload.get("normal_groups") or []:
        add_many(group.get("tags"))
        for origin_payload in group.get("origins") or []:
            add_many((origin_payload.get("origin") or {}).get("tags"))
    for origin_payload in package_payload.get("important_origins") or []:
        add_many((origin_payload.get("origin") or {}).get("tags"))
    return tags


def _run_ai_analysis_pipeline(
    config: AppConfig,
    package_payload: dict[str, Any],
    output_root: Path,
    *,
    final_summary_path: Path,
    final_prompt_path: Path,
) -> dict[str, Any]:
    stages_dir = output_root / "stages"
    media_artifacts = _analyze_package_media(config, package_payload, stages_dir / "media")

    normal_origin_artifacts: list[dict[str, Any]] = []
    normal_origin_by_ref: dict[str, dict[str, Any]] = {}
    normal_origin_dir = stages_dir / "normal-origins"
    for group in package_payload.get("normal_groups") or []:
        group_name = str(group.get("name") or "group")
        for origin_payload in group.get("origins") or []:
            origin_ref = _origin_ref(origin_payload)
            prompt = _normal_origin_key_prompt(group, origin_payload, media_artifacts)
            artifact = _run_ai_task(
                config,
                "normal_origin_key_extraction",
                prompt,
                normal_origin_dir / f"{_slug(group_name)}--{_slug(origin_ref)}.md",
                image_paths=[],
                metadata={
                    "group_name": group_name,
                    "group_tags": group.get("tags") or [],
                    "origin": origin_payload.get("origin") or {},
                },
            )
            normal_origin_artifacts.append(artifact)
            normal_origin_by_ref[origin_ref] = artifact

    normal_group_artifacts: list[dict[str, Any]] = []
    group_dir = stages_dir / "normal-groups"
    for group in package_payload.get("normal_groups") or []:
        group_name = str(group.get("name") or "group")
        origin_refs = [_origin_ref(origin_payload) for origin_payload in group.get("origins") or []]
        origin_artifacts = [normal_origin_by_ref[ref] for ref in origin_refs if ref in normal_origin_by_ref]
        prompt = _normal_group_analysis_prompt(group, origin_artifacts)
        artifact = _run_ai_task(
            config,
            "normal_group_analysis",
            prompt,
            group_dir / f"{_slug(group_name)}.md",
            image_paths=[],
            metadata={
                "group_name": group_name,
                "group_tags": group.get("tags") or [],
                "origin_refs": origin_refs,
            },
        )
        normal_group_artifacts.append(artifact)

    important_artifacts: list[dict[str, Any]] = []
    important_dir = stages_dir / "important-origins"
    for origin_payload in package_payload.get("important_origins") or []:
        origin_ref = _origin_ref(origin_payload)
        image_paths = _origin_image_paths(origin_payload, limit=None)
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
        )
        important_artifacts.append(artifact)

    final_prompt = _final_daily_summary_prompt(
        package_payload,
        normal_group_artifacts=normal_group_artifacts,
        important_artifacts=important_artifacts,
        media_artifacts=media_artifacts,
    )
    final_artifact = _run_ai_task(
        config,
        "final_daily_summary",
        final_prompt,
        final_summary_path,
        image_paths=[],
        prompt_path=final_prompt_path,
        metadata={
            "package_run_id": package_payload.get("run_id"),
            "date": package_payload.get("date"),
            "timezone": package_payload.get("timezone"),
        },
    )
    analysis = {
        "media": list(media_artifacts.values()),
        "normal_origins": normal_origin_artifacts,
        "normal_groups": normal_group_artifacts,
        "important_origins": important_artifacts,
        "final": final_artifact,
    }
    _write_json(output_root / "analysis.json", _analysis_record_payload(analysis))
    return analysis


def _analyze_package_media(
    config: AppConfig,
    package_payload: dict[str, Any],
    output_dir: Path,
) -> dict[str, dict[str, Any]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, dict[str, Any]] = {}
    for origin_payload in _iter_origin_payloads(package_payload):
        if (origin_payload.get("origin") or {}).get("important"):
            continue
        for message in origin_payload.get("messages") or []:
            for media in message.get("media_files") or []:
                file_path = str(media.get("file_path") or "")
                if not file_path or file_path in artifacts:
                    continue
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
) -> dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path = prompt_path or output_path.with_suffix(".prompt.md")
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(prompt, encoding="utf-8")
    content = _run_summary_provider(config, prompt, output_path, image_paths, task_name=task_name)
    if output_path.exists():
        content = output_path.read_text(encoding="utf-8")
    else:
        output_path.write_text(content, encoding="utf-8")
    return {
        "task": task_name,
        "id": _slug(output_path.stem),
        "output_path": str(output_path),
        "prompt_path": str(prompt_path),
        "content": content,
        "image_paths": image_paths,
        "generated_by_ai": config.daily.ai.provider != "disabled",
        "metadata": metadata,
    }


def _analysis_record_payload(analysis: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_count": sum(
            len(analysis.get(key) or [])
            for key in ("media", "normal_origins", "normal_groups", "important_origins")
        )
        + (1 if analysis.get("final") else 0),
        "media": [_artifact_payload(item) for item in analysis.get("media") or []],
        "normal_origins": [_artifact_payload(item) for item in analysis.get("normal_origins") or []],
        "normal_groups": [_artifact_payload(item) for item in analysis.get("normal_groups") or []],
        "important_origins": [_artifact_payload(item) for item in analysis.get("important_origins") or []],
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


def _normal_origin_key_prompt(
    group: dict[str, Any],
    origin_payload: dict[str, Any],
    media_artifacts: dict[str, dict[str, Any]],
) -> str:
    payload = _compact_origin_for_analysis(origin_payload, media_artifacts)
    tag_instruction = _tag_specific_instruction(
        group.get("tags"),
        (origin_payload.get("origin") or {}).get("tags"),
    )
    return (
        "TASK: normal_origin_key_extraction\n"
        "你是 Telegram 每日归档的关键信息提取器。当前 origin 不是 important，属于 normal tag group。\n"
        "请从这个 origin 的消息中整理可用于组内汇总的原始内容和关键内容，不要做最终总结。"
        "若 origin 是按 parent tags 进入本组的 topic，请只处理本 topic 的消息，后续再与 parent/group 整合。\n"
        "内容保留规则：当 `truncated_message_count` 为 0 时，输入消息不超过 200 条，必须按时间顺序保留全部消息文本、发言人、时间和 source_refs；"
        "不要只抽几条重点。只有当消息超过 200 条并被截断时，才做重点段落/主题摘要。\n"
        "输出 Markdown，包含：\n"
        "## Full Content Record\n"
        "- 若未截断，逐条保留全部消息；若已截断，说明截断数量并保留代表性段落。\n"
        "## Key Information Strings\n"
        "- 每条是一句独立、可引用的事实/观点/事件/资源，保留 source_refs。\n"
        "## Suggested Tags\n"
        "- 从原始 tags 和内容中提炼 3-8 个 tags。\n"
        "## Noise Or Low Value\n"
        "- 简述被忽略的闲聊/重复内容类型。\n\n"
        "Group metadata:\n"
        f"{json.dumps({'name': group.get('name'), 'tags': group.get('tags')}, ensure_ascii=False, indent=2)}\n\n"
        f"{tag_instruction}\n"
        "Origin package:\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n"
    )


def _normal_group_analysis_prompt(group: dict[str, Any], origin_artifacts: list[dict[str, Any]]) -> str:
    tag_instruction = _tag_specific_instruction(group.get("tags"))
    extracted = [
        {
            "origin": artifact.get("metadata", {}).get("origin"),
            "content": _truncate_text(str(artifact.get("content") or ""), 8000),
        }
        for artifact in origin_artifacts
    ]
    return (
        "TASK: normal_group_analysis\n"
        "你是 Telegram 每日归档的 tag group 分析器。请基于组内多个 origin 的关键内容串，"
        "总结这一组的分析结果和 tags。同一 parent 下按 parent tags 分入本组的 topics 要与 parent 作为一体整合。"
        "如果 origin 提取结果包含 `Full Content Record`，说明该 origin 当天消息量不大，请把完整记录作为主要依据，"
        "输出的信息密度要接近原始内容，不要压缩成一两句话。\n"
        "输出 Markdown，包含：\n"
        "## Group Analysis\n"
        "## Full Content Digest\n"
        "- 按 origin/topic 分块，保留主要原文事实、链接、资源名、数值、时间和 source_refs。\n"
        "## Key Threads\n"
        "## Derived Tags\n"
        "## Risks / Opportunities / Actions\n"
        "## Source Refs\n\n"
        "Group metadata:\n"
        f"{json.dumps({'name': group.get('name'), 'tags': group.get('tags'), 'origin_count': group.get('origin_count')}, ensure_ascii=False, indent=2)}\n\n"
        f"{tag_instruction}\n"
        "Origin key extractions:\n"
        f"{json.dumps(extracted, ensure_ascii=False, indent=2)}\n"
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
        "必须按时间顺序把全部消息文本、发言人、时间和 source_refs 写入 `Complete Important Record`，不要做 200 条截断摘要。\n"
        "请先做 `Segment Importance Scan`：按时间段/讨论段落判断重要度，再根据 media 所在消息或前后上下文的重要度决定是否处理 media。"
        "只有重要上下文中的图片才需要 OCR/视觉事实提取并插入记录；低重要度 media 只列出路径和跳过原因。"
        "PDF/视频等长内容只保留路径和文件名，除非上下文显示它是重要信息源。\n"
        "输出 Markdown，包含：\n"
        "## Complete Important Record\n"
        "## Segment Importance Scan\n"
        "## Important Analysis\n"
        "## Tags\n"
        "## Media Handling\n"
        "## Source Refs\n\n"
        f"{tag_instruction}\n"
        "Important origin package:\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n"
    )


def _final_daily_summary_prompt(
    package_payload: dict[str, Any],
    *,
    normal_group_artifacts: list[dict[str, Any]],
    important_artifacts: list[dict[str, Any]],
    media_artifacts: dict[str, dict[str, Any]],
) -> str:
    payload = {
        "run_id": package_payload.get("run_id"),
        "date": package_payload.get("date"),
        "timezone": package_payload.get("timezone"),
        "stats": package_payload.get("stats"),
        "normal_group_analyses": [
            {
                "group_name": artifact.get("metadata", {}).get("group_name"),
                "group_tags": artifact.get("metadata", {}).get("group_tags"),
                "analysis": _truncate_text(str(artifact.get("content") or ""), 10000),
            }
            for artifact in normal_group_artifacts
        ],
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
        "TASK: final_daily_summary\n"
        "你是每日 Telegram 归档最终分析器。请基于已经完成的 media、important origin、normal tag group 分析，"
        "输出最终 Markdown 总结。\n"
        "要求：\n"
        "1. 先给出 Important Highlights，并保留 important 和 tag 分析结果。\n"
        "2. 再按 tag group 输出 normal content 的分析、机会、风险和行动项；当组内分析包含 Full Content Digest 时，"
        "最终总结也要保留足够细节，不要压缩成极短摘要。\n"
        "3. 把图片 OCR/图像分析作为内容依据引用；PDF/视频只引用路径和文件名，不编造内容。\n"
        "4. 每个结论尽量保留 source_refs，引用 origin title/message_id/file_path。\n"
        "5. 输出应是可直接阅读的 Markdown，不要返回 JSON。\n\n"
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


def start_daily_summary_thread(
    store: ArchiveStore,
    config: AppConfig,
    *,
    package_run_id: str | None = None,
    run_date: str | None = None,
    timezone_name: str | None = None,
    scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    package_run = store.get_daily_package_run(package_run_id) if package_run_id else None
    if package_run is None:
        package_run = build_daily_package(store, config, run_date=run_date, timezone_name=timezone_name, scope=scope)
    run_id = _new_run_id("sum")
    output_root = Path(str(package_run["output_dir"])) / "analysis" / run_id
    summary_path = output_root / "summary.md"
    output_root.mkdir(parents=True, exist_ok=True)
    scope_json = json.dumps(scope or package_run.get("scope") or {}, ensure_ascii=False, sort_keys=True)
    item = store.upsert_daily_summary_run(
        DailySummaryRunRecord(
            run_id=run_id,
            status="running",
            package_run_id=str(package_run["run_id"]),
            date=package_run.get("date"),
            timezone=package_run.get("timezone"),
            scope_json=scope_json,
            output_dir=str(output_root),
            summary_path=str(summary_path),
            provider=config.daily.ai.provider,
            started_at=utc_now_iso(),
        )
    )

    def worker() -> None:
        run_daily_summary(
            store,
            config,
            package_run_id=str(package_run["run_id"]),
            scope=scope,
            run_id=run_id,
        )

    thread = threading.Thread(target=worker, name=f"daily-summary-{run_id}", daemon=True)
    thread.start()
    return item


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
    messages = store.list_messages_for_origin_window(
        str(origin.get("source") or SOURCE_TELEGRAM),
        str(origin["account_id"]),
        int(origin["origin_id"]),
        topic_id=int(origin.get("topic_id") or 0),
        window_start=window_start_utc.isoformat(),
        window_end=window_end_utc.isoformat(),
    )
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
        "media_files": media_files,
    }


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
) -> str:
    if config.daily.ai.provider == "disabled":
        text = f"# AI Task Disabled\n\nTask: `{task_name}`\n\nAI provider is disabled for this run.\n"
        output_path.write_text(text, encoding="utf-8")
        return text
    command = _expand_command(config.daily.ai.command, output_path, image_paths, task_name=task_name)
    config.storage.data_dir.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        command,
        input=prompt,
        capture_output=True,
        text=True,
        timeout=config.daily.ai.timeout_seconds,
        cwd=str(config.storage.data_dir),
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or f"exit code {completed.returncode}"
        raise RuntimeError(f"AI provider failed: {detail}")
    if output_path.exists():
        return output_path.read_text(encoding="utf-8")
    if completed.stdout.strip():
        output_path.write_text(completed.stdout, encoding="utf-8")
        return completed.stdout
    raise RuntimeError("AI provider completed without writing a summary")


def _expand_command(command: list[str], output_path: Path, image_paths: list[str], *, task_name: str = "summary") -> list[str]:
    expanded: list[str] = []
    image_args: list[str] = []
    for path in image_paths:
        image_args.extend(["--image", path])
    for token in command:
        if token == "{images}":
            expanded.extend(image_args)
        else:
            expanded.append(token.replace("{output}", str(output_path)).replace("{task}", task_name))
    return expanded


def _summary_prompt(package_payload: dict[str, Any]) -> str:
    compact_payload = {
        "run_id": package_payload.get("run_id"),
        "date": package_payload.get("date"),
        "timezone": package_payload.get("timezone"),
        "stats": package_payload.get("stats"),
        "normal_groups": _compact_groups(package_payload.get("normal_groups") or []),
        "important_origins": _compact_origins(package_payload.get("important_origins") or []),
        "unmatched_origins": package_payload.get("unmatched_origins") or [],
    }
    return (
        "你是每日 Telegram 归档分析器。请基于输入的 package JSON 输出 Markdown。\n"
        "要求：\n"
        "1. 先给出 important origin 的重点分析。\n"
        "2. 再按 tag group 总结关键线索、风险、机会和行动项。\n"
        "3. 图片若已作为附件提供，请结合图片内容；若只看到路径，则引用路径。\n"
        "4. PDF、视频等长内容只保留文件名和路径，不编造内容。\n"
        "5. 保留 source_refs，引用 origin title/message_id/file_path。\n\n"
        "Package JSON:\n"
        f"{json.dumps(compact_payload, ensure_ascii=False, indent=2)}\n"
    )


def _compact_groups(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "name": group.get("name"),
            "tags": group.get("tags"),
            "origin_count": group.get("origin_count"),
            "message_count": group.get("message_count"),
            "media_count": group.get("media_count"),
            "origins": _compact_origins(group.get("origins") or []),
        }
        for group in groups
    ]


def _compact_origins(origins: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compact = []
    for origin in origins:
        messages = origin.get("messages") or []
        compact.append(
            {
                "origin": origin.get("origin"),
                "package_meta": origin.get("package_meta"),
                "messages": [
                    {
                        "message_id": message.get("message_id"),
                        "speaker": message.get("speaker"),
                        "local_sent_at": message.get("local_sent_at"),
                        "text": message.get("text"),
                        "permalink": message.get("permalink"),
                        "media_files": message.get("media_files"),
                    }
                    for message in messages[:200]
                ],
                "truncated_message_count": max(0, len(messages) - 200),
            }
        )
    return compact


def _collect_image_paths(package_payload: dict[str, Any], limit: int = 20) -> list[str]:
    paths: list[str] = []
    for origin_payload in _iter_origin_payloads(package_payload):
        for message in origin_payload.get("messages") or []:
            for media in message.get("media_files") or []:
                content_type = str(media.get("content_type") or media.get("mime_type") or "")
                file_path = str(media.get("file_path") or "")
                if content_type.startswith("image/") and file_path and Path(file_path).is_file():
                    paths.append(file_path)
                    if len(paths) >= limit:
                        return paths
    return paths


def _iter_origin_payloads(package_payload: dict[str, Any]) -> list[dict[str, Any]]:
    origins: list[dict[str, Any]] = []
    for group in package_payload.get("normal_groups") or []:
        origins.extend(group.get("origins") or [])
    origins.extend(package_payload.get("important_origins") or [])
    return origins


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
