from __future__ import annotations

import argparse
import base64
import json
import mimetypes
import re
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener


MAX_RESPONSE_BYTES = 16 * 1024 * 1024
_JSON_PATH_PATTERN = re.compile(r'("(?:file|output|summary|prompt)_path"\s*:\s*")([^"]+)(")')
_POSIX_ABSOLUTE_PATH_PATTERN = re.compile(
    r'(?<![A-Za-z0-9_:/.])/(?:[^/\s"\'`<>\\]+/)+[^/\s"\'`<>\\)\],}]*'
)
_WINDOWS_ABSOLUTE_PATH_PATTERN = re.compile(
    r'(?<![A-Za-z0-9_])[A-Za-z]:\\(?:[^\\\s"\'`<>]+\\)+[^\\\s"\'`<>\)\],}]*'
)


class FallbackRequestError(RuntimeError):
    def __init__(self, message: str, *, kind: str, retryable: bool = False) -> None:
        super().__init__(message)
        self.kind = kind
        self.retryable = retryable


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Request | None:
        raise FallbackRequestError(
            "OpenAI-compatible fallback refused an HTTP redirect",
            kind="fallback_redirect",
        )


_NO_REDIRECT_OPENER = build_opener(_NoRedirectHandler())


def _open_no_redirect(request: Request, *, timeout: int):
    return _NO_REDIRECT_OPENER.open(request, timeout=timeout)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--api-key-file", required=True)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--supports-images", action="store_true")
    parser.add_argument("--supports-json-schema", action="store_true")
    return parser


def _safe_prompt(
    prompt: str,
    image_paths: list[str],
    redact_roots: list[str] | None = None,
) -> str:
    def replace_json_path(match: re.Match[str]) -> str:
        return f"{match.group(1)}{Path(match.group(2)).name}{match.group(3)}"

    cleaned = _JSON_PATH_PATTERN.sub(replace_json_path, prompt)
    for raw_path in [*(redact_roots or []), *image_paths]:
        if raw_path:
            path = Path(raw_path)
            replacement = path.name if raw_path in image_paths else "<local-root>"
            cleaned = cleaned.replace(raw_path, replacement)
            escaped_path = json.dumps(raw_path, ensure_ascii=False)[1:-1]
            if escaped_path != raw_path:
                cleaned = cleaned.replace(escaped_path, replacement)
            try:
                resolved = str(path.expanduser().resolve())
            except (OSError, RuntimeError):
                resolved = ""
            if resolved and resolved != raw_path:
                cleaned = cleaned.replace(resolved, replacement)
                escaped_resolved = json.dumps(resolved, ensure_ascii=False)[1:-1]
                if escaped_resolved != resolved:
                    cleaned = cleaned.replace(escaped_resolved, replacement)
    cleaned = _POSIX_ABSOLUTE_PATH_PATTERN.sub("<local-path>", cleaned)
    cleaned = _WINDOWS_ABSOLUTE_PATH_PATTERN.sub("<local-path>", cleaned)
    return cleaned


def _schema_prompt(prompt: str, output_schema: dict[str, Any] | None) -> str:
    if not output_schema:
        return prompt
    return (
        f"{prompt.rstrip()}\n\n"
        "The remote fallback does not support server-enforced structured output. "
        "Return one JSON value that validates against this schema, without Markdown fences or commentary:\n"
        f"{json.dumps(output_schema, ensure_ascii=False, separators=(',', ':'))}\n"
    )


def _data_url(path: Path) -> str:
    mime_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _response_text(payload: dict[str, Any]) -> str:
    status = str(payload.get("status") or "completed")
    if status != "completed":
        raise FallbackRequestError(
            "OpenAI-compatible fallback response was incomplete",
            kind="fallback_incomplete",
            retryable=status in {"queued", "in_progress", "incomplete"},
        )
    texts: list[str] = []
    for item in payload.get("output") or []:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            if content.get("type") == "refusal":
                raise FallbackRequestError(
                    "OpenAI-compatible fallback refused the request",
                    kind="fallback_refusal",
                )
            if content.get("type") == "output_text" and isinstance(content.get("text"), str):
                texts.append(str(content["text"]))
    if not texts and isinstance(payload.get("output_text"), str):
        texts.append(str(payload["output_text"]))
    text = "\n".join(part for part in texts if part.strip()).strip()
    if not text:
        raise FallbackRequestError(
            "OpenAI-compatible fallback returned no output text",
            kind="fallback_empty_output",
            retryable=True,
        )
    return text


def _request_error(status: int) -> FallbackRequestError:
    if status in {401, 403}:
        return FallbackRequestError(
            f"OpenAI-compatible fallback authentication failed with HTTP {status}",
            kind="fallback_auth",
        )
    if status == 429:
        return FallbackRequestError(
            "OpenAI-compatible fallback was rate limited with HTTP 429",
            kind="fallback_rate_limit",
            retryable=True,
        )
    if status >= 500:
        return FallbackRequestError(
            f"OpenAI-compatible fallback upstream failed with HTTP {status}",
            kind="fallback_upstream",
            retryable=True,
        )
    return FallbackRequestError(
        f"OpenAI-compatible fallback rejected the request with HTTP {status}",
        kind="fallback_request_rejected",
    )


def run_request(args: argparse.Namespace, request_payload: dict[str, Any]) -> dict[str, Any]:
    key_path = Path(args.api_key_file)
    try:
        api_key = key_path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise FallbackRequestError(
            "OpenAI-compatible fallback API key file is not readable",
            kind="fallback_key_file",
        ) from exc
    if not api_key:
        raise FallbackRequestError(
            "OpenAI-compatible fallback API key file is empty",
            kind="fallback_key_file",
        )

    prompt = str(request_payload.get("prompt") or "")
    task_name = str(request_payload.get("task_name") or "summary")
    image_paths = [str(item) for item in request_payload.get("image_paths") or [] if str(item)]
    redact_roots = [str(item) for item in request_payload.get("redact_roots") or [] if str(item)]
    output_schema = request_payload.get("output_schema")
    if output_schema is not None and not isinstance(output_schema, dict):
        raise FallbackRequestError(
            "Fallback output schema must be an object",
            kind="fallback_protocol",
        )

    if task_name == "media_image_analysis" and image_paths and not args.supports_images:
        names = ", ".join(Path(item).name for item in image_paths)
        return {
            "content": (
                "- `classification`: unclear\n"
                "- `analysis_status`: unavailable\n"
                "- `reason`: fallback_has_no_vision\n"
                "- `ocr_text`: none\n"
                "- `visual_facts`: none; the fallback model was not given image access\n"
                "- `archive_content`: image retained without visual claims\n"
                f"- `source_refs`: {names or 'image'}\n"
            ),
            "provider": f"openai-compatible:{args.model}",
            "generated_by_ai": False,
        }

    prompt = _safe_prompt(prompt, image_paths, redact_roots)
    if image_paths and not args.supports_images:
        prompt = (
            f"{prompt.rstrip()}\n\n"
            "Fallback capability notice: image inputs are unavailable. Do not infer OCR, visual facts, "
            "or image meaning; use only message text and explicit metadata.\n"
        )
    if output_schema is not None and not args.supports_json_schema:
        prompt = _schema_prompt(prompt, output_schema)

    if image_paths and args.supports_images:
        content: list[dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        for raw_path in image_paths:
            path = Path(raw_path)
            if path.is_file():
                content.append({"type": "input_image", "image_url": _data_url(path)})
        response_input: Any = [{"role": "user", "content": content}]
    else:
        response_input = prompt

    body: dict[str, Any] = {"model": args.model, "input": response_input}
    if output_schema is not None and args.supports_json_schema:
        body["text"] = {
            "format": {
                "type": "json_schema",
                "name": re.sub(r"[^A-Za-z0-9_-]+", "_", task_name)[:64] or "structured_output",
                "schema": output_schema,
                "strict": True,
            }
        }
    endpoint = f"{str(args.base_url).rstrip('/')}/responses"
    request = Request(
        endpoint,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with _open_no_redirect(request, timeout=max(1, int(args.timeout))) as response:
            raw = response.read(MAX_RESPONSE_BYTES + 1)
    except HTTPError as exc:
        raise _request_error(int(exc.code)) from None
    except (TimeoutError, URLError, OSError) as exc:
        raise FallbackRequestError(
            "OpenAI-compatible fallback network request failed",
            kind="fallback_network",
            retryable=True,
        ) from exc
    if len(raw) > MAX_RESPONSE_BYTES:
        raise FallbackRequestError(
            "OpenAI-compatible fallback response exceeded the size limit",
            kind="fallback_response_too_large",
        )
    try:
        response_payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FallbackRequestError(
            "OpenAI-compatible fallback returned malformed JSON",
            kind="fallback_protocol",
            retryable=True,
        ) from exc
    if not isinstance(response_payload, dict):
        raise FallbackRequestError(
            "OpenAI-compatible fallback returned an invalid response object",
            kind="fallback_protocol",
            retryable=True,
        )
    return {
        "content": _response_text(response_payload),
        "provider": f"openai-compatible:{args.model}",
        "generated_by_ai": True,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        request_payload = json.load(sys.stdin)
        if not isinstance(request_payload, dict):
            raise FallbackRequestError(
                "Fallback request payload must be an object",
                kind="fallback_protocol",
            )
        result = run_request(args, request_payload)
    except FallbackRequestError as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": str(exc),
                    "kind": exc.kind,
                    "retryable": exc.retryable,
                },
                ensure_ascii=False,
            )
        )
        return 2
    except Exception:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "OpenAI-compatible fallback failed unexpectedly",
                    "kind": "fallback_internal",
                    "retryable": True,
                }
            )
        )
        return 2
    print(json.dumps({"ok": True, **result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
