import sys
import tempfile
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402
from alert_store import AlertStore  # noqa: E402


class AlertFetchLimitTests(unittest.TestCase):
    def setUp(self):
        self._original_get_news = main.search_crawler.get_news_by_keyword
        self._original_get_meta = main.search_crawler.get_last_result_meta
        self._original_alert_store = main.alert_store

        def _fake_get_news(keyword: str, max_count: int = 10):
            return [
                {
                    "title": f"{keyword} sample news {idx}",
                    "link": f"https://example.com/{keyword}/{idx}",
                    "source": "example.com",
                    "published_date": "2026-02-13 15:00:00",
                }
                for idx in range(max_count)
            ]

        def _fake_meta(_keyword: str):
            return {
                "source": "network",
                "age_sec": 0,
                "fetched_at": "2026-02-13 15:05:00",
                "cache_ttl_sec": 180,
            }

        main.search_crawler.get_news_by_keyword = _fake_get_news
        main.search_crawler.get_last_result_meta = _fake_meta

        self._tmp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        temp_alert_db = str(Path(self._tmp_dir.name) / "alerts_fetch_limit.db")
        main.alert_store = AlertStore(db_path=temp_alert_db)

    def tearDown(self):
        main.search_crawler.get_news_by_keyword = self._original_get_news
        main.search_crawler.get_last_result_meta = self._original_get_meta
        main.alert_store = self._original_alert_store
        self._tmp_dir.cleanup()

    def test_alerts_use_scoring_fetch_limit_separate_from_preview_limit(self):
        response = main.get_alerts(
            priority=None,
            delivery_level_filter=None,
            min_score=0,
            limit=5,
            news_fetch_limit=30,
            news_preview_limit=2,
        )

        self.assertTrue(response.get("success"))
        self.assertGreaterEqual(response.get("count", 0), 1)
        self.assertEqual(response.get("summary", {}).get("scoring_fetch_limit"), 30)
        self.assertEqual(response.get("summary", {}).get("news_preview_limit"), 2)

        first_alert = response["alerts"][0]
        self.assertEqual(first_alert.get("article_count"), 30)
        self.assertEqual(len(first_alert.get("latest_news", [])), 2)
        self.assertEqual(first_alert.get("news_fetch_limit"), 30)
        self.assertEqual(first_alert.get("news_preview_limit"), 2)


if __name__ == "__main__":
    unittest.main()
