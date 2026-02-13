import sys
import tempfile
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402
from feedback_store import FeedbackStore  # noqa: E402


class FeedbackAdjustmentTests(unittest.TestCase):
    def setUp(self):
        self._original_feedback_store = main.feedback_store
        self._tmp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        temp_feedback_db = str(Path(self._tmp_dir.name) / "feedback_adjustment.db")
        main.feedback_store = FeedbackStore(db_path=temp_feedback_db)

    def tearDown(self):
        main.feedback_store = self._original_feedback_store
        self._tmp_dir.cleanup()

    def test_adjustment_is_zero_without_feedback(self):
        adjustment = main.calculate_feedback_score_adjustment("005930")
        self.assertEqual(adjustment.get("score_delta"), 0)
        self.assertEqual(adjustment.get("reasons"), [])
        self.assertFalse(adjustment.get("signal", {}).get("ready"))

    def test_adjustment_applies_for_strong_consensus_and_ai_mismatch(self):
        for idx in range(6):
            main.feedback_store.submit_feedback(
                user_id=f"tester-{idx}",
                stock_code="005930",
                article_link=f"https://example.com/mismatch-{idx}",
                article_title=f"Samsung article {idx}",
                article_source="example.com",
                ai_label="positive",
                user_label="negative",
                user_confidence=4,
                note="mismatch",
            )

        adjustment = main.calculate_feedback_score_adjustment("005930")
        expected_min_delta = main.FEEDBACK_SCORE_DELTA_CONSENSUS + main.FEEDBACK_SCORE_DELTA_AI_MISMATCH

        self.assertGreaterEqual(adjustment.get("score_delta", 0), expected_min_delta)
        reasons = adjustment.get("reasons", [])
        self.assertIn("테스터 악재 합의", reasons)
        self.assertIn("AI-사용자 판단 불일치", reasons)
        signal = adjustment.get("signal", {})
        self.assertTrue(signal.get("ready"))
        self.assertEqual(signal.get("consensus_label"), "negative")


if __name__ == "__main__":
    unittest.main()
