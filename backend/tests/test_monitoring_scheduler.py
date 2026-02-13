import sys
import tempfile
import time
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402
from alert_store import AlertStore  # noqa: E402


class MonitoringSchedulerTests(unittest.TestCase):
    def setUp(self):
        self._original_get_alerts = main.get_alerts
        self._original_alert_store = main.alert_store
        main.stop_monitoring_scheduler()

        self._tmp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        temp_alert_db = str(Path(self._tmp_dir.name) / "scheduler_alerts.db")
        main.alert_store = AlertStore(db_path=temp_alert_db)

        with main.SCHEDULER_LOCK:
            main.SCHEDULER_STATE.update(
                {
                    "running": False,
                    "run_total": 0,
                    "success_total": 0,
                    "error_total": 0,
                    "last_trigger": "",
                    "last_run_started_at": "",
                    "last_run_finished_at": "",
                    "last_error": "",
                    "last_result_count": 0,
                    "last_alert_average_score": 0.0,
                    "last_run_duration_ms": 0.0,
                    "effective_min_score": main.MONITORING_SCHEDULER_MIN_SCORE,
                    "adaptive_enabled": main.MONITORING_ADAPTIVE_MIN_SCORE_ENABLED,
                    "adaptive_target_alert_count": main.MONITORING_ADAPTIVE_TARGET_ALERT_COUNT,
                    "adaptive_alert_band": main.MONITORING_ADAPTIVE_ALERT_BAND,
                    "adaptive_score_step": main.MONITORING_ADAPTIVE_SCORE_STEP,
                    "adaptive_min_bound": main.MONITORING_ADAPTIVE_MIN_BOUND,
                    "adaptive_max_bound": main.MONITORING_ADAPTIVE_MAX_BOUND,
                    "adaptive_profiles": {
                        k: dict(v) for k, v in main.DEFAULT_ADAPTIVE_POLICY_OVERRIDES.items()
                    },
                    "adaptive_current_min_score": main.MONITORING_SCHEDULER_MIN_SCORE,
                    "adaptive_last_adjustment": "",
                    "adaptive_last_reason": "",
                    "adaptive_last_direction": "hold",
                    "next_interval_sec": 0,
                    "active_policy_name": "",
                }
            )
            main.SCHEDULER_RUN_HISTORY.clear()

        def _fake_get_alerts(
            priority=None,
            delivery_level_filter=None,
            min_score=0,
            limit=20,
            news_fetch_limit=30,
            news_preview_limit=3,
        ):
            return {
                "success": True,
                "count": 2,
                "alerts": [],
                "summary": {
                    "average_score": 55.5,
                },
                "generated_at": "2026-02-13 17:00:00",
            }

        main.get_alerts = _fake_get_alerts

    def tearDown(self):
        main.get_alerts = self._original_get_alerts
        main.alert_store = self._original_alert_store
        main.stop_monitoring_scheduler()
        self._tmp_dir.cleanup()

    def test_run_monitoring_cycle_once_updates_state(self):
        result = main.run_monitoring_cycle_once(trigger="test")

        self.assertTrue(result.get("success"))
        self.assertEqual(result.get("count"), 2)
        snapshot = main.scheduler_status_snapshot()
        self.assertEqual(snapshot.get("run_total"), 1)
        self.assertEqual(snapshot.get("success_total"), 1)
        self.assertEqual(snapshot.get("error_total"), 0)
        self.assertEqual(snapshot.get("last_trigger"), "test")
        self.assertGreaterEqual(float(snapshot.get("last_run_duration_ms", 0.0)), 0.0)

        runs = main.scheduler_run_history_snapshot(limit=10)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].get("status"), "success")
        self.assertEqual(runs[0].get("trigger"), "test")
        self.assertEqual(runs[0].get("result_count"), 2)

    def test_scheduler_start_and_stop_endpoints(self):
        start = main.start_monitoring_scheduler_endpoint(
            force_restart=False,
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(start.get("success"))
        self.assertTrue(start.get("scheduler", {}).get("running"))

        time.sleep(0.05)

        status = main.get_monitoring_scheduler_status(
            runs_limit=10,
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(status.get("success"))
        self.assertIn("scheduler", status)
        self.assertIn("recent_runs", status)

        stop = main.stop_monitoring_scheduler_endpoint(
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(stop.get("success"))
        self.assertFalse(stop.get("scheduler", {}).get("running"))

    def test_scheduler_runs_endpoint(self):
        main.run_monitoring_cycle_once(trigger="manual_test")
        response = main.get_monitoring_scheduler_runs(
            limit=10,
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(response.get("success"))
        self.assertGreaterEqual(response.get("count", 0), 1)
        self.assertEqual(response.get("runs", [])[0].get("trigger"), "manual_test")
        self.assertIn(response.get("source"), ("memory", "persistent"))

        persistent_response = main.get_monitoring_scheduler_runs(
            limit=10,
            source="persistent",
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(persistent_response.get("success"))
        self.assertGreaterEqual(persistent_response.get("count", 0), 1)

    def test_adaptive_status_and_reset_endpoints(self):
        adaptive = main.get_monitoring_scheduler_adaptive(
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(adaptive.get("success"))
        self.assertIn("adaptive", adaptive)
        self.assertIn("config", adaptive["adaptive"])
        self.assertIn("state", adaptive["adaptive"])

        reset = main.reset_monitoring_scheduler_adaptive(
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(reset.get("success"))
        self.assertIn("result", reset)

    def test_adaptive_update_endpoint(self):
        update = main.update_monitoring_scheduler_adaptive(
            payload=main.MonitoringAdaptiveUpdateRequest(
                enabled=True,
                target_alert_count=1,
                alert_band=0,
                score_step=5,
                min_bound=0,
                max_bound=20,
                reset_current_min_score=True,
            ),
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(update.get("success"))
        result = update.get("result", {})
        self.assertTrue(result.get("config", {}).get("enabled"))
        self.assertEqual(result.get("config", {}).get("target_alert_count"), 1)

        run = main.run_monitoring_cycle_once(trigger="adaptive_test")
        self.assertTrue(run.get("success"))
        self.assertIn("adaptive", run)
        self.assertTrue(run["adaptive"].get("enabled"))

    def test_adaptive_profile_endpoints(self):
        get_profiles = main.get_monitoring_scheduler_adaptive_profiles(
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(get_profiles.get("success"))
        self.assertIn("profiles", get_profiles)
        self.assertIn("market_open", get_profiles.get("profiles", {}))

        update_profiles = main.update_monitoring_scheduler_adaptive_profiles(
            payload=main.MonitoringAdaptiveProfileUpdateRequest(
                policy_name="market_open",
                target_alert_count=6,
                alert_band=2,
                score_step=4,
                min_bound=5,
                max_bound=85,
            ),
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        self.assertTrue(update_profiles.get("success"))
        result = update_profiles.get("result", {})
        self.assertEqual(result.get("policy_name"), "market_open")
        self.assertEqual(result.get("policy_profile", {}).get("target_alert_count"), 6)
        self.assertEqual(result.get("policy_profile", {}).get("min_bound"), 5)

    def test_run_uses_policy_adaptive_profile(self):
        main.update_monitoring_scheduler_adaptive(
            payload=main.MonitoringAdaptiveUpdateRequest(
                enabled=True,
                target_alert_count=1,
                alert_band=0,
                score_step=5,
                min_bound=0,
                max_bound=20,
                reset_current_min_score=True,
            ),
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )
        main.update_monitoring_scheduler_adaptive_profiles(
            payload=main.MonitoringAdaptiveProfileUpdateRequest(
                policy_name="market_open",
                target_alert_count=1,
                alert_band=0,
                score_step=7,
                min_bound=0,
                max_bound=90,
            ),
            admin={"auth_mode": "disabled", "auth_scope": "disabled"},
        )

        run = main.run_monitoring_cycle_once(trigger="adaptive_profile_test")
        self.assertTrue(run.get("success"))
        self.assertEqual(run.get("policy_name"), "market_open")
        adaptive = run.get("adaptive", {})
        self.assertTrue(adaptive.get("enabled"))
        self.assertEqual(adaptive.get("policy_name"), "market_open")
        self.assertEqual(adaptive.get("new_min_score"), 7)


if __name__ == "__main__":
    unittest.main()
