from __future__ import annotations

import argparse
import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest.mock import patch

from tele_mess_core.openai_fallback import (
    FallbackRequestError,
    _safe_prompt,
    _request_error,
    _response_text,
    run_request,
)


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, _limit: int) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


class OpenAiFallbackTest(unittest.TestCase):
    def _args(self, key_file: Path, **overrides: object) -> argparse.Namespace:
        values: dict[str, object] = {
            "base_url": "https://fallback.example/v1",
            "model": "deepseek-v4-flash",
            "api_key_file": str(key_file),
            "timeout": 5,
            "supports_images": False,
            "supports_json_schema": False,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_text_request_appends_schema_and_removes_local_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            key_file = root / "key"
            key_file.write_text("secret-value", encoding="utf-8")
            captured: dict[str, object] = {}

            def fake_urlopen(request: object, *, timeout: int) -> _FakeResponse:
                captured["request"] = request
                captured["timeout"] = timeout
                return _FakeResponse(
                    {
                        "status": "completed",
                        "output": [
                            {
                                "type": "message",
                                "content": [{"type": "output_text", "text": '{"points":[]}'}],
                            }
                        ],
                    }
                )

            with patch("tele_mess_core.openai_fallback._open_no_redirect", side_effect=fake_urlopen):
                result = run_request(
                    self._args(key_file),
                    {
                        "task_name": "message_point_extraction",
                        "prompt": 'Evidence: {"file_path":"/home/user/private/image.png"}',
                        "image_paths": ["/home/user/private/image.png"],
                        "output_schema": {
                            "type": "object",
                            "properties": {"points": {"type": "array"}},
                            "required": ["points"],
                        },
                    },
                )

            self.assertEqual(result["content"], '{"points":[]}')
            self.assertEqual(result["provider"], "openai-compatible:deepseek-v4-flash")
            request = captured["request"]
            body = json.loads(request.data.decode("utf-8"))  # type: ignore[attr-defined]
            self.assertNotIn("text", body)
            self.assertIsInstance(body["input"], str)
            self.assertIn("Return one JSON value", body["input"])
            self.assertIn('"required":["points"]', body["input"])
            self.assertNotIn("/home/user/private", body["input"])
            self.assertNotIn("secret-value", request.full_url)  # type: ignore[attr-defined]

    def test_media_task_without_vision_returns_static_artifact_without_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            key_file = Path(tmp) / "key"
            key_file.write_text("secret-value", encoding="utf-8")
            with patch("tele_mess_core.openai_fallback._open_no_redirect") as provider:
                result = run_request(
                    self._args(key_file),
                    {
                        "task_name": "media_image_analysis",
                        "prompt": "analyze image",
                        "image_paths": ["/home/user/private/image.png"],
                    },
                )

            provider.assert_not_called()
            self.assertFalse(result["generated_by_ai"])
            self.assertIn("fallback_has_no_vision", str(result["content"]))
            self.assertNotIn("/home/user/private", str(result["content"]))

    def test_safe_prompt_redacts_nested_analysis_paths_without_damaging_urls(self) -> None:
        prompt = (
            '{"analysis":"source_refs: /home/user/private/image.png",'
            '"other":"/srv/archive/daily/output.md",'
            '"url":"https://example.com/a/b"}'
        )

        cleaned = _safe_prompt(prompt, [], ["/srv/archive"])

        self.assertNotIn("/home/user/private", cleaned)
        self.assertNotIn("/srv/archive", cleaned)
        self.assertIn("https://example.com/a/b", cleaned)

        windows_root = r"C:\Users\private\archive"
        windows_prompt = json.dumps(
            {"analysis": rf"source_refs: {windows_root}\images\one.png"}
        )
        windows_cleaned = _safe_prompt(windows_prompt, [], [windows_root])
        self.assertNotIn(r"C:\\Users\\private", windows_cleaned)
        self.assertNotIn(r"C:\Users\private", windows_cleaned)

    def test_redirect_is_rejected_without_forwarding_authorization(self) -> None:
        state = {"target_called": False, "target_authorization": None}

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                self.send_response(302)
                self.send_header("Location", "/redirect-target")
                self.end_headers()

            def do_GET(self) -> None:
                state["target_called"] = True
                state["target_authorization"] = self.headers.get("Authorization")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status":"completed","output_text":"unexpected"}')

            def log_message(self, _format: str, *_args: object) -> None:
                return

        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                key_file = Path(tmp) / "key"
                key_file.write_text("secret-value", encoding="utf-8")
                args = self._args(
                    key_file,
                    base_url=f"http://127.0.0.1:{server.server_port}/v1",
                )

                with self.assertRaises(FallbackRequestError) as caught:
                    run_request(args, {"prompt": "hello"})

            self.assertEqual(caught.exception.kind, "fallback_redirect")
            self.assertFalse(state["target_called"])
            self.assertIsNone(state["target_authorization"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_response_parser_rejects_refusal_and_incomplete_status(self) -> None:
        with self.assertRaisesRegex(FallbackRequestError, "refused"):
            _response_text(
                {
                    "status": "completed",
                    "output": [
                        {"type": "message", "content": [{"type": "refusal", "refusal": "no"}]}
                    ],
                }
            )
        with self.assertRaisesRegex(FallbackRequestError, "incomplete") as caught:
            _response_text({"status": "incomplete", "output": []})
        self.assertTrue(caught.exception.retryable)

        unsafe_status = "failed /home/user/private sk-sensitive"
        with self.assertRaises(FallbackRequestError) as unsafe:
            _response_text({"status": unsafe_status, "output": []})
        self.assertNotIn(unsafe_status, str(unsafe.exception))
        self.assertNotIn("/home", str(unsafe.exception))
        self.assertNotIn("sk-sensitive", str(unsafe.exception))

    def test_http_error_classification_is_safe_and_retryable_only_when_transient(self) -> None:
        self.assertFalse(_request_error(401).retryable)
        self.assertTrue(_request_error(429).retryable)
        self.assertTrue(_request_error(502).retryable)
        self.assertNotIn("secret", str(_request_error(502)))


if __name__ == "__main__":
    unittest.main()
