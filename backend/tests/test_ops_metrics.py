import sys
import tempfile
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402
from alert_store import AlertStore  # noqa: E402
from feedback_store import FeedbackStore  # noqa: E402


class OpsMetricsEndpointTests(unittest.TestCase):
    def setUp(self):
        self._original_alert_store = main.alert_store
        self._original_feedback_store = main.feedback_store

        self._tmp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        temp_alert_db = str(Path(self._tmp_dir.name) / "ops_alerts.db")
        temp_feedback_db = str(Path(self._tmp_dir.name) / "ops_feedback.db")

        main.alert_store = AlertStore(db_path=temp_alert_db)
        main.feedback_store = FeedbackStore(db_path=temp_feedback_db)

        main.alert_store.save_alerts(
            [
                {
                    "stock_code": "005930",
                    "stock_name": "Samsung Electronics",
                    "importance_score": 72,
                    "delivery_level": "push_immediate",
                    "priority": "high",
                    "article_count": 8,
                    "sentiment": "positive",
                    "summary": "positive 5 / negative 1 / neutral 2",
                }
            ],
            created_at="2026-02-13 15:00:00",
        )

        main.feedback_store.submit_feedback(
            user_id="tester-a",
            stock_code="005930",
            article_link="https://example.com/article-1",
            article_title="Samsung wins new chip contract",
            article_source="example.com",
            ai_label="positive",
            user_label="positive",
            user_confidence=4,
            note="looks accurate",
        )

    def tearDown(self):
        main.alert_store = self._original_alert_store
        main.feedback_store = self._original_feedback_store
        self._tmp_dir.cleanup()

    def test_ops_metrics_response_schema(self):
        response = main.get_ops_metrics(
            hours=24,
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )

        self.assertTrue(response.get("success"))
        self.assertEqual(response.get("period_hours"), 24)
        self.assertIn("crawler", response)
        self.assertIn("feedback", response)
        self.assertIn("alerts", response)
        self.assertIn("news_count_history", response)
        self.assertIn("monitoring_scheduler", response)
        self.assertGreaterEqual(response["feedback"].get("total_feedback_events", 0), 1)
        self.assertGreaterEqual(response["alerts"].get("total_alert_history_rows", 0), 1)
        self.assertIn("label_distribution", response["feedback"])
        self.assertIn("delivery_level_distribution", response["alerts"])
        self.assertIn("status", response["monitoring_scheduler"])
        self.assertIn("recent_runs_memory", response["monitoring_scheduler"])
        self.assertIn("recent_runs_persistent", response["monitoring_scheduler"])
        self.assertIn("metrics", response["monitoring_scheduler"])
        self.assertIn("adaptive", response["monitoring_scheduler"])


if __name__ == "__main__":
    unittest.main()
