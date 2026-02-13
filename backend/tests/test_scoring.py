import sys
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class ScoringEngineTests(unittest.TestCase):
    def test_high_impact_batch_triggers_push_immediate(self):
        news = []
        for idx in range(10):
            news.append(
                {
                    "title": f"삼성전자 수주 계약 양산 확대 {idx}",
                    "source": f"source-{idx}",
                }
            )

        enriched, sentiment_summary = main.enrich_news_with_sentiment(news)
        result = main.calculate_importance_score(
            stock_code="005930",
            news_list=enriched,
            sentiment_summary=sentiment_summary,
            update_history=False,
        )

        self.assertGreaterEqual(result["score"], 70)
        self.assertEqual(result["delivery_level"], "push_immediate")
        self.assertEqual(result["priority"], "high")

    def test_small_neutral_batch_stays_daily_digest(self):
        news = [
            {"title": "General update on company operations", "source": "source-a"},
            {"title": "Routine market commentary", "source": "source-b"},
        ]
        enriched, sentiment_summary = main.enrich_news_with_sentiment(news)
        result = main.calculate_importance_score(
            stock_code="005930",
            news_list=enriched,
            sentiment_summary=sentiment_summary,
            update_history=False,
        )

        self.assertLess(result["score"], 40)
        self.assertEqual(result["delivery_level"], "daily_digest")
        self.assertEqual(result["priority"], "low")

    def test_duplicate_topic_penalty_reduces_inflated_score(self):
        news = []
        for idx in range(20):
            news.append(
                {
                    "title": "Samsung earnings update",
                    "source": f"source-{idx}",
                }
            )

        enriched, sentiment_summary = main.enrich_news_with_sentiment(news)
        result = main.calculate_importance_score(
            stock_code="005930",
            news_list=enriched,
            sentiment_summary=sentiment_summary,
            update_history=False,
        )

        self.assertLessEqual(result["score"], 20)
        self.assertEqual(result["delivery_level"], "daily_digest")
        self.assertLess(result["metrics"].get("topic_ratio", 1.0), 0.35)


if __name__ == "__main__":
    unittest.main()
