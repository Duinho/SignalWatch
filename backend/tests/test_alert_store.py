import tempfile
import unittest
from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from alert_store import AlertStore  # noqa: E402


class AlertStoreTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = str(Path(self._tmp_dir.name) / "alerts_test.db")
        self.store = AlertStore(db_path=self.db_path)

    def tearDown(self):
        del self.store
        self._tmp_dir.cleanup()

    def test_save_and_list_with_filters(self):
        alerts = [
            {
                "stock_code": "005930",
                "stock_name": "삼성전자",
                "importance_score": 75,
                "delivery_level": "push_immediate",
                "priority": "high",
                "article_count": 9,
                "sentiment": "positive",
                "summary": "호재 6건 / 악재 1건 / 중립 2건",
            },
            {
                "stock_code": "000660",
                "stock_name": "SK하이닉스",
                "importance_score": 45,
                "delivery_level": "in_app",
                "priority": "medium",
                "article_count": 4,
                "sentiment": "neutral",
                "summary": "호재 2건 / 악재 1건 / 중립 1건",
            },
        ]

        inserted = self.store.save_alerts(alerts, created_at="2026-02-13 14:00:00")
        self.assertEqual(inserted, 2)

        all_rows = self.store.list_alert_history(limit=10)
        self.assertEqual(len(all_rows), 2)

        samsung_only = self.store.list_alert_history(limit=10, stock_code="005930")
        self.assertEqual(len(samsung_only), 1)
        self.assertEqual(samsung_only[0]["delivery_level"], "push_immediate")

        high_only = self.store.list_alert_history(limit=10, delivery_level="push_immediate", min_score=70)
        self.assertEqual(len(high_only), 1)
        self.assertEqual(high_only[0]["stock_code"], "005930")

    def test_prune_history_removes_old_rows(self):
        old_alert = {
            "stock_code": "005930",
            "stock_name": "삼성전자",
            "importance_score": 60,
            "delivery_level": "in_app",
            "priority": "medium",
            "article_count": 5,
            "sentiment": "neutral",
            "summary": "old",
        }
        new_alert = {
            "stock_code": "000660",
            "stock_name": "SK하이닉스",
            "importance_score": 80,
            "delivery_level": "push_immediate",
            "priority": "high",
            "article_count": 8,
            "sentiment": "positive",
            "summary": "new",
        }

        self.store.save_alerts([old_alert], created_at="2020-01-01 00:00:00")
        self.store.save_alerts([new_alert], created_at="2099-01-01 00:00:00")

        result = self.store.prune_history(retention_days=30, max_rows=20000)
        self.assertGreaterEqual(result["deleted_total"], 1)

        rows = self.store.list_alert_history(limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["stock_code"], "000660")

    def test_preview_prune_reports_expected_counts(self):
        old_alert = {
            "stock_code": "005930",
            "stock_name": "Samsung Electronics",
            "importance_score": 55,
            "delivery_level": "daily_digest",
            "priority": "low",
            "article_count": 2,
            "sentiment": "neutral",
            "summary": "old",
        }
        new_alert = {
            "stock_code": "000660",
            "stock_name": "SK hynix",
            "importance_score": 80,
            "delivery_level": "push_immediate",
            "priority": "high",
            "article_count": 9,
            "sentiment": "positive",
            "summary": "new",
        }

        self.store.save_alerts([old_alert], created_at="2020-01-01 00:00:00")
        self.store.save_alerts([new_alert], created_at="2099-01-01 00:00:00")

        preview = self.store.preview_prune(retention_days=30, max_rows=20000)
        self.assertEqual(preview["total_rows"], 2)
        self.assertGreaterEqual(preview["would_delete_old"], 1)
        self.assertEqual(preview["would_delete_overflow"], 0)
        self.assertGreaterEqual(preview["would_delete_total"], 1)

    def test_monitoring_run_history_persistence_and_metrics(self):
        run1 = {
            "status": "success",
            "trigger": "scheduler",
            "policy_name": "market_open",
            "started_at": "2026-02-13 10:00:00",
            "finished_at": "2026-02-13 10:00:01",
            "duration_ms": 1000.0,
            "result_count": 5,
            "average_score": 61.5,
        }
        run2 = {
            "status": "error",
            "trigger": "manual_api",
            "policy_name": "market_open",
            "started_at": "2026-02-13 10:10:00",
            "finished_at": "2026-02-13 10:10:02",
            "duration_ms": 2000.0,
            "result_count": 0,
            "average_score": 0.0,
            "error": "sample error",
        }

        self.store.save_monitoring_run(run1, created_at="2026-02-13 10:00:01")
        self.store.save_monitoring_run(run2, created_at="2026-02-13 10:10:02")

        rows = self.store.list_monitoring_runs(limit=10)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["status"], "error")
        self.assertEqual(rows[1]["status"], "success")

        metrics = self.store.get_monitoring_run_metrics(since_hours=24)
        self.assertEqual(metrics["total_runs"], 2)
        self.assertEqual(metrics["total_success_runs"], 1)
        self.assertEqual(metrics["total_error_runs"], 1)
        self.assertGreaterEqual(len(metrics["by_policy"]), 1)


if __name__ == "__main__":
    unittest.main()
