from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from tele_mess_core.codex_limits import (
    classify_codex_rate_limits,
    codex_app_server_command,
    read_codex_rate_limit_status,
)


class CodexLimitsTest(unittest.TestCase):
    def test_app_server_command_only_accepts_direct_codex_commands(self) -> None:
        self.assertEqual(
            codex_app_server_command(["/opt/codex", "exec", "-"]),
            ["/opt/codex", "app-server", "--stdio"],
        )
        self.assertEqual(
            codex_app_server_command(["C:/Tools/codex.exe", "exec", "-"]),
            ["C:/Tools/codex.exe", "app-server", "--stdio"],
        )
        self.assertIsNone(codex_app_server_command([sys.executable, "wrapper.py"]))
        self.assertIsNone(codex_app_server_command([]))

    def test_classification_uses_definitive_reached_signals(self) -> None:
        reached_type = classify_codex_rate_limits(
            {
                "rateLimits": {
                    "limitId": "codex",
                    "rateLimitReachedType": "workspace_member_usage_limit_reached",
                }
            },
            now_epoch=100,
        )
        assert reached_type is not None
        self.assertTrue(reached_type.reached)
        self.assertIn("workspace_member_usage_limit_reached", str(reached_type.reason))

        spend_control = classify_codex_rate_limits(
            {"rateLimits": {"limitId": "codex", "spendControlReached": True}},
            now_epoch=100,
        )
        assert spend_control is not None
        self.assertTrue(spend_control.reached)
        self.assertIn("spend_control_reached", str(spend_control.reason))

        individual = classify_codex_rate_limits(
            {
                "rateLimits": {
                    "limitId": "codex",
                    "individualLimit": {"remainingPercent": 0, "resetsAt": 200},
                }
            },
            now_epoch=100,
        )
        assert individual is not None
        self.assertTrue(individual.reached)
        self.assertIn("individual_limit_reached", str(individual.reason))

    def test_full_window_requires_no_credits_and_an_active_reset_window(self) -> None:
        base = {
            "limitId": "codex",
            "primary": {"usedPercent": 100, "resetsAt": 200},
        }
        exhausted = classify_codex_rate_limits(
            {
                "rateLimits": {
                    **base,
                    "credits": {"hasCredits": False, "unlimited": False, "balance": "0"},
                }
            },
            now_epoch=100,
        )
        assert exhausted is not None
        self.assertTrue(exhausted.reached)
        self.assertIn("primary_window_reached", str(exhausted.reason))

        for name, snapshot in (
            (
                "credits",
                {
                    **base,
                    "credits": {"hasCredits": True, "unlimited": False, "balance": "1"},
                },
            ),
            (
                "expired",
                {
                    **base,
                    "primary": {"usedPercent": 100, "resetsAt": 50},
                    "credits": {"hasCredits": False, "unlimited": False, "balance": "0"},
                },
            ),
            (
                "unknown credits",
                base,
            ),
        ):
            with self.subTest(name=name):
                status = classify_codex_rate_limits(
                    {"rateLimits": snapshot},
                    now_epoch=100,
                )
                assert status is not None
                self.assertFalse(status.reached)

    def test_unrelated_limit_bucket_does_not_block_codex(self) -> None:
        status = classify_codex_rate_limits(
            {
                "rateLimits": {"limitId": "codex", "primary": {"usedPercent": 20}},
                "rateLimitsByLimitId": {
                    "codex": {"limitId": "codex", "primary": {"usedPercent": 20}},
                    "codex_bengalfox": {
                        "limitId": "codex_bengalfox",
                        "rateLimitReachedType": "rate_limit_reached",
                    },
                },
            },
            now_epoch=100,
        )
        assert status is not None
        self.assertFalse(status.reached)

    def test_stdio_probe_reads_response_without_starting_a_turn(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / "fake_app_server.py"
            script.write_text(
                "import json\n"
                "import sys\n"
                "for line in sys.stdin:\n"
                "    message = json.loads(line)\n"
                "    if message.get('id') == 1:\n"
                "        print(json.dumps({'id': 1, 'result': {'ready': True}}), flush=True)\n"
                "    if message.get('id') == 2:\n"
                "        result = {'rateLimits': {'limitId': 'codex', "
                "'rateLimitReachedType': 'rate_limit_reached'}}\n"
                "        print(json.dumps({'id': 2, 'result': result}), flush=True)\n"
                "        break\n",
                encoding="utf-8",
            )

            status = read_codex_rate_limit_status(
                [sys.executable, str(script)],
                timeout_seconds=2,
                now_epoch=100,
            )

        assert status is not None
        self.assertTrue(status.reached)
        self.assertEqual(status.reason, "codex:rate_limit_reached")

    def test_probe_timeout_is_fail_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / "slow_app_server.py"
            script.write_text(
                "import sys\n"
                "import time\n"
                "sys.stdin.readline()\n"
                "time.sleep(10)\n",
                encoding="utf-8",
            )

            status = read_codex_rate_limit_status(
                [sys.executable, str(script)],
                timeout_seconds=0.1,
                now_epoch=100,
            )

        self.assertIsNone(status)


if __name__ == "__main__":
    unittest.main()
