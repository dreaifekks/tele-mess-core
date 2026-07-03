from __future__ import annotations

import json
import unittest
from pathlib import Path

from tele_mess_core.cli import main
from tele_mess_core.server.contracts import (
    API_CONTRACT_HASH,
    API_ENDPOINTS,
    agent_markdown_document,
    api_manifest,
    markdown_document,
    openapi_document,
    openapi_json,
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
        self.assertIn("ApiManifest", openapi["components"]["schemas"])

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


if __name__ == "__main__":
    unittest.main()
