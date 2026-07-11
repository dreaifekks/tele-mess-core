from __future__ import annotations

import json
import unittest
from pathlib import Path

from tele_mess_core.cli import main
from tele_mess_core.server.api import ENDPOINTS_BY_ROUTE, METHODS_BY_PATH
from tele_mess_core.server.contracts import (
    API_CONTRACT_HASH,
    API_ENDPOINTS,
    agent_markdown_document,
    api_manifest,
    markdown_document,
    openapi_document,
    openapi_json,
    validate_query_params,
    validate_request_payload,
)


class ApiContractTest(unittest.TestCase):
    def test_manifest_and_openapi_cover_registered_endpoints(self) -> None:
        manifest = api_manifest()
        openapi = openapi_document()
        expected = {(endpoint.method.lower(), endpoint.path) for endpoint in API_ENDPOINTS}
        manifest_routes = {(item["method"].lower(), item["path"]) for item in manifest["endpoints"]}
        openapi_routes = {
            (method, path)
            for path, methods in openapi["paths"].items()
            for method in methods
        }

        self.assertEqual(manifest["contract_hash"], API_CONTRACT_HASH)
        self.assertEqual(manifest_routes, expected)
        self.assertEqual(openapi_routes, expected)
        self.assertIn("/manage/api-manifest", openapi["paths"])
        self.assertIn("/manage/daily-message-points", openapi["paths"])
        self.assertIn("DailyMessagePoint", openapi["components"]["schemas"])
        daily_job_properties = openapi["components"]["schemas"]["DailySummaryJob"]["properties"]
        self.assertIn("retry_at", daily_job_properties)
        self.assertIn("retry_count", daily_job_properties)
        self.assertIn("ApiManifest", openapi["components"]["schemas"])
        self.assertEqual(set(ENDPOINTS_BY_ROUTE), {(method.upper(), path) for method, path in expected})
        self.assertEqual(
            METHODS_BY_PATH,
            {
                path: {method.upper() for method, route_path in expected if route_path == path}
                for _, path in expected
            },
        )

    def test_generated_docs_are_current(self) -> None:
        root = Path(__file__).resolve().parents[1]
        expected = {
            root / "docs" / "api.md": markdown_document(),
            root / "docs" / "api-agent.md": agent_markdown_document(),
            root / "docs" / "openapi.json": openapi_json(),
        }

        for path, content in expected.items():
            self.assertTrue(path.exists(), f"missing generated doc: {path}")
            self.assertEqual(path.read_text(encoding="utf-8"), content)

    def test_cli_generate_api_docs_check(self) -> None:
        self.assertEqual(main(["generate-api-docs", "--check"]), 0)

    def test_openapi_json_is_parseable(self) -> None:
        parsed = json.loads(openapi_json())

        self.assertEqual(parsed["info"]["x-contract-hash"], API_CONTRACT_HASH)
        self.assertIn("/sync/messages", parsed["paths"])

    def test_contract_validates_runtime_request_inputs(self) -> None:
        account_endpoint = next(
            endpoint for endpoint in API_ENDPOINTS if endpoint.method == "POST" and endpoint.path == "/manage/accounts"
        )
        validate_request_payload(account_endpoint, {"account_id": "main"})
        with self.assertRaisesRegex(ValueError, "body.account_id must be a string"):
            validate_request_payload(account_endpoint, {"account_id": 123})

        media_endpoint = next(
            endpoint
            for endpoint in API_ENDPOINTS
            if endpoint.method == "GET" and endpoint.path == "/sync/media-files/content"
        )
        with self.assertRaisesRegex(ValueError, "account_id"):
            validate_query_params(media_endpoint, {"chat_id": ["-1001"], "message_id": ["1"]})

        points_endpoint = next(
            endpoint
            for endpoint in API_ENDPOINTS
            if endpoint.method == "GET" and endpoint.path == "/manage/daily-message-points"
        )
        with self.assertRaisesRegex(ValueError, "importance_min must be an integer"):
            validate_query_params(points_endpoint, {"importance_min": ["high"]})


if __name__ == "__main__":
    unittest.main()
