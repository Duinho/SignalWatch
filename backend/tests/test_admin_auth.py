import os
import sys
import unittest
from pathlib import Path

from fastapi import HTTPException


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class AdminAuthScopeTests(unittest.TestCase):
    ENV_KEYS = [
        "SIGNALWATCH_ADMIN_KEY",
        "SIGNALWATCH_ADMIN_READ_KEY",
        "SIGNALWATCH_ADMIN_WRITE_KEY",
        "ADMIN_WRITE_RATE_LIMIT_COUNT",
        "ADMIN_WRITE_RATE_LIMIT_WINDOW_SEC",
        "ADMIN_WRITE_RATE_LIMIT_APPLY_KEYWORD_RULE_COUNT",
        "ADMIN_WRITE_RATE_LIMIT_APPLY_KEYWORD_RULE_WINDOW_SEC",
    ]

    def setUp(self):
        self._env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}
        for key in self.ENV_KEYS:
            os.environ.pop(key, None)
        with main.ADMIN_WRITE_RATE_LOCK:
            main.ADMIN_WRITE_RATE_BUCKETS.clear()

    def tearDown(self):
        for key in self.ENV_KEYS:
            old = self._env_backup.get(key)
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old
        with main.ADMIN_WRITE_RATE_LOCK:
            main.ADMIN_WRITE_RATE_BUCKETS.clear()

    def test_disabled_mode_allows_without_key(self):
        read_auth = main.require_admin_read(None)
        write_auth = main.require_admin_write(None)

        self.assertEqual(read_auth.get("auth_mode"), "disabled")
        self.assertEqual(write_auth.get("auth_mode"), "disabled")

    def test_scoped_keys_enforce_read_write_boundaries(self):
        os.environ["SIGNALWATCH_ADMIN_READ_KEY"] = "read-key"
        os.environ["SIGNALWATCH_ADMIN_WRITE_KEY"] = "write-key"

        read_auth = main.require_admin_read("read-key")
        self.assertEqual(read_auth.get("auth_scope"), "read")

        write_auth = main.require_admin_write("write-key")
        self.assertEqual(write_auth.get("auth_scope"), "write")

        with self.assertRaises(HTTPException) as exc:
            main.require_admin_write("read-key")
        self.assertEqual(exc.exception.status_code, 401)

    def test_write_rate_limit_blocks_excess_requests(self):
        os.environ["SIGNALWATCH_ADMIN_WRITE_KEY"] = "write-key"
        os.environ["ADMIN_WRITE_RATE_LIMIT_COUNT"] = "2"
        os.environ["ADMIN_WRITE_RATE_LIMIT_WINDOW_SEC"] = "60"

        auth = main.require_admin_write("write-key")
        first = main.enforce_admin_write_rate_limit(auth, "upsert_user_tier")
        second = main.enforce_admin_write_rate_limit(auth, "upsert_user_tier")

        self.assertEqual(first.get("remaining"), 1)
        self.assertEqual(second.get("remaining"), 0)

        with self.assertRaises(HTTPException) as exc:
            main.enforce_admin_write_rate_limit(auth, "upsert_user_tier")
        self.assertEqual(exc.exception.status_code, 429)

    def test_action_specific_rate_limit_override(self):
        os.environ["SIGNALWATCH_ADMIN_WRITE_KEY"] = "write-key"
        os.environ["ADMIN_WRITE_RATE_LIMIT_COUNT"] = "5"
        os.environ["ADMIN_WRITE_RATE_LIMIT_WINDOW_SEC"] = "60"
        os.environ["ADMIN_WRITE_RATE_LIMIT_APPLY_KEYWORD_RULE_COUNT"] = "1"
        os.environ["ADMIN_WRITE_RATE_LIMIT_APPLY_KEYWORD_RULE_WINDOW_SEC"] = "60"

        auth = main.require_admin_write("write-key")
        first_apply = main.enforce_admin_write_rate_limit(auth, "apply_keyword_rule")
        self.assertEqual(first_apply.get("remaining"), 0)

        with self.assertRaises(HTTPException) as exc:
            main.enforce_admin_write_rate_limit(auth, "apply_keyword_rule")
        self.assertEqual(exc.exception.status_code, 429)

        # Different action should still use its own bucket/config.
        other_action = main.enforce_admin_write_rate_limit(auth, "upsert_user_tier")
        self.assertEqual(other_action.get("max_requests"), 5)
        self.assertEqual(other_action.get("remaining"), 4)

    def test_health_reports_scope_config(self):
        os.environ["SIGNALWATCH_ADMIN_READ_KEY"] = "read-key"
        os.environ["SIGNALWATCH_ADMIN_WRITE_KEY"] = "write-key"
        os.environ["ADMIN_WRITE_RATE_LIMIT_COUNT"] = "10"
        os.environ["ADMIN_WRITE_RATE_LIMIT_WINDOW_SEC"] = "30"

        health = main.health_check()

        self.assertTrue(health.get("admin_key_configured"))
        admin_auth = health.get("admin_auth", {})
        self.assertTrue(admin_auth.get("read_scope_configured"))
        self.assertTrue(admin_auth.get("write_scope_configured"))
        self.assertEqual(admin_auth.get("write_rate_limit_default", {}).get("max_requests"), 10)
        self.assertEqual(admin_auth.get("write_rate_limit_default", {}).get("window_sec"), 30)


if __name__ == "__main__":
    unittest.main()
