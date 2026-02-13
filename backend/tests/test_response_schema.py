import sys
import tempfile
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402
from alert_store import AlertStore  # noqa: E402


class ResponseSchemaTests(unittest.TestCase):
    def setUp(self):
        self._original_get_news = main.search_crawler.get_news_by_keyword
        self._original_get_meta = main.search_crawler.get_last_result_meta
        self._original_alert_store = main.alert_store
        main.stop_monitoring_scheduler()
        with main.SCHEDULER_LOCK:
            main.SCHEDULER_RUN_HISTORY.clear()

        sample_news = [
            {
                "title": "삼성전자 대형 수주 계약 체결",
                "link": "https://example.com/news-1",
                "source": "테스트언론",
                "date": "2026-02-13 10:00:00",
            },
            {
                "title": "삼성전자 실적 개선 기대",
                "link": "https://example.com/news-2",
                "source": "테스트언론2",
                "date": "2026-02-13 10:03:00",
            },
        ]

        def _fake_get_news(_keyword, _limit=10):
            return sample_news[:_limit]

        def _fake_meta(_keyword):
            return {
                "source": "network",
                "age_sec": 0,
                "fetched_at": "2026-02-13 10:05:00",
                "cache_ttl_sec": 180,
            }

        main.search_crawler.get_news_by_keyword = _fake_get_news
        main.search_crawler.get_last_result_meta = _fake_meta

        self._tmp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        temp_alert_db = str(Path(self._tmp_dir.name) / "alert_schema_test.db")
        main.alert_store = AlertStore(db_path=temp_alert_db)

    def tearDown(self):
        main.search_crawler.get_news_by_keyword = self._original_get_news
        main.search_crawler.get_last_result_meta = self._original_get_meta
        main.alert_store = self._original_alert_store
        self._tmp_dir.cleanup()

    def test_news_response_schema(self):
        res = main.get_stock_news(stock_code="005930", limit=2)
        self.assertTrue(res.get("success"))
        self.assertIn("sentiment_summary", res)
        self.assertIn("alert_decision", res)
        self.assertIn("fetch_meta", res)
        self.assertIn("data", res)
        self.assertGreaterEqual(res.get("count", 0), 1)

    def test_alerts_response_schema_and_history(self):
        res = main.get_alerts(
            priority=None,
            delivery_level_filter=None,
            min_score=0,
            limit=5,
        )
        self.assertTrue(res.get("success"))
        self.assertIn("alerts", res)
        self.assertIn("summary", res)
        self.assertIn("persisted_count", res)
        self.assertIn("history_prune", res)

        history = main.get_alert_history(
            stock_code=None,
            delivery_level=None,
            min_score=0,
            limit=10,
        )
        self.assertTrue(history.get("success"))
        self.assertIn("history", history)
        self.assertIn("summary", history)

    def test_alert_history_export_csv(self):
        main.get_alerts(
            priority=None,
            delivery_level_filter=None,
            min_score=0,
            limit=5,
        )
        resp = main.export_alert_history_csv(
            stock_code=None,
            delivery_level=None,
            min_score=0,
            limit=10,
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )

        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/csv", resp.media_type)
        self.assertIn("Content-Disposition", resp.headers)
        body_text = resp.body.decode("utf-8")
        self.assertIn("stock_code", body_text)

    def test_admin_alert_prune_preview_schema(self):
        main.get_alerts(
            priority=None,
            delivery_level_filter=None,
            min_score=0,
            limit=5,
        )
        response = main.preview_alert_history_prune(
            retention_days=30,
            max_rows=20000,
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )

        self.assertTrue(response.get("success"))
        self.assertIn("preview", response)
        preview = response.get("preview", {})
        self.assertIn("total_rows", preview)
        self.assertIn("would_delete_total", preview)

    def test_monitoring_scheduler_runs_schema(self):
        main.run_monitoring_cycle_once(trigger="schema_test")
        response = main.get_monitoring_scheduler_runs(
            limit=10,
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(response.get("success"))
        self.assertIn("source", response)
        self.assertIn("runs", response)
        self.assertGreaterEqual(response.get("count", 0), 1)

    def test_monitoring_scheduler_adaptive_schema(self):
        response = main.get_monitoring_scheduler_adaptive(
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(response.get("success"))
        self.assertIn("adaptive", response)
        self.assertIn("config", response["adaptive"])
        self.assertIn("state", response["adaptive"])

    def test_monitoring_scheduler_adaptive_update_schema(self):
        response = main.update_monitoring_scheduler_adaptive(
            payload=main.MonitoringAdaptiveUpdateRequest(enabled=True, target_alert_count=2),
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(response.get("success"))
        self.assertIn("result", response)
        self.assertIn("config", response["result"])
        self.assertIn("state", response["result"])

    def test_monitoring_scheduler_adaptive_profiles_schema(self):
        response = main.get_monitoring_scheduler_adaptive_profiles(
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(response.get("success"))
        self.assertIn("profiles", response)
        self.assertIn("current_policy_config", response)

    def test_monitoring_scheduler_adaptive_profiles_update_schema(self):
        response = main.update_monitoring_scheduler_adaptive_profiles(
            payload=main.MonitoringAdaptiveProfileUpdateRequest(
                policy_name="market_open",
                target_alert_count=5,
                alert_band=2,
            ),
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(response.get("success"))
        self.assertIn("result", response)
        self.assertIn("policy_profile", response["result"])
        self.assertEqual(response["result"].get("policy_name"), "market_open")


if __name__ == "__main__":
    unittest.main()
