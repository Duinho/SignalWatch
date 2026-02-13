# SignalWatch handoff

## Last Updated
2026-02-13 19:25

## Done
- Feedback store with SQLite (article feedback, keyword votes, keyword rules)
- 1-user-1-vote upsert for same article (vote_action: created/updated)
- Consensus evaluation on article feedback (consensus_ready, reasons, thresholds)
- Trust weighting for testers (manual trust override + weighted voting)
- Tester tier defaults (core/general/observer) with automatic trust fallback
- Tier management APIs (/api/feedback/user-tier*) and trust APIs (/api/feedback/user-trust*)
- Tester quality analyzer and tier auto-apply workflow (dry_run + apply)
- .gitignore now excludes local DB and venv files
- Minimal admin key auth for management endpoints (`X-Admin-Key`, env: `SIGNALWATCH_ADMIN_KEY`)
- Admin audit logs for management actions (`/api/admin/audit-logs`)
- Admin scope split: read vs write (`SIGNALWATCH_ADMIN_READ_KEY`, `SIGNALWATCH_ADMIN_WRITE_KEY`)
- Admin write API rate limit (`ADMIN_WRITE_RATE_LIMIT_COUNT`, `ADMIN_WRITE_RATE_LIMIT_WINDOW_SEC`)
- Admin write rate limit supports action-specific overrides (e.g. `ADMIN_WRITE_RATE_LIMIT_APPLY_KEYWORD_RULE_COUNT`)
- Added regression tests:
- `backend/tests/test_admin_auth.py`
- `backend/tests/test_alert_store.py`
- `backend/tests/test_scoring.py`
- `backend/tests/test_response_schema.py`
- `backend/tests/test_ops_metrics.py`
- `backend/tests/test_alert_fetch_limits.py`
- `backend/tests/test_feedback_adjustment.py`
- `backend/tests/test_monitoring_scheduler.py`
- Total: 32 tests passing
- Tester-quality default thresholds are now environment-configurable
- Tester-quality supports `recent_days` window filter and summary breakdown
- Added alert history persistence: `/api/alerts` stores snapshots, `/api/alerts/history` reads them
- Added alert history retention policy:
- Automatic prune on `/api/alerts` response path
- Manual prune endpoint: `POST /api/admin/alerts/prune`
- Added CSV export endpoint: `GET /api/alerts/history/export`
- Added operational metrics endpoint: `GET /api/metrics/ops`
- Metrics include crawler runtime/cache stats, feedback quality stats, alert throughput stats, and in-memory news-history stats
- Added prune preview endpoint: `GET /api/admin/alerts/prune-preview`
- `/api/alerts` now separates scoring fetch size and response preview size:
- scoring fetch default: 30 (`ALERT_SCORING_FETCH_LIMIT_DEFAULT`)
- response preview default: 3 (`ALERT_NEWS_PREVIEW_LIMIT_DEFAULT`)
- Added feedback-based score adjustment in alert scoring:
- strong tester consensus adjustment
- AI/user mismatch adjustment
- Added duplicate-topic penalty in scoring to reduce syndicated-copy inflation
- Added time-based monitoring scheduler:
- status endpoint: `GET /api/monitoring/scheduler`
- runs endpoint: `GET /api/monitoring/scheduler/runs`
- adaptive status endpoint: `GET /api/monitoring/scheduler/adaptive`
- adaptive update endpoint: `POST /api/monitoring/scheduler/adaptive`
- adaptive profiles endpoint: `GET /api/monitoring/scheduler/adaptive/profiles`
- adaptive profiles update endpoint: `POST /api/monitoring/scheduler/adaptive/profiles`
- adaptive reset endpoint: `POST /api/monitoring/scheduler/adaptive/reset`
- start/stop endpoints: `POST /api/monitoring/scheduler/start`, `POST /api/monitoring/scheduler/stop`
- manual run endpoint: `POST /api/monitoring/run-once`
- optional auto-start via env `MONITORING_SCHEDULER_AUTOSTART`
- scheduler recent run history is exposed in `/api/metrics/ops`
- scheduler run history is now also persisted in SQLite (survives process restart)
- added adaptive min_score logic (default OFF) for automatic alert sensitivity control
- adaptive min_score now supports time-window policy-specific profile overrides
- fixed scheduler run cycle to apply policy-specific adaptive min_score correctly per run

## In Progress
- Tuning policy-specific adaptive profiles with live traffic distribution

## Next Step (Top Priority)
- Tune policy-specific adaptive profile defaults using actual alert volume data

## How to Run
- cd backend
- python main.py

## Quick Check URLs
- http://localhost:8080/docs
- http://localhost:8080/api/news/005930?limit=10
- http://localhost:8080/api/alerts?limit=5
- http://localhost:8080/api/monitoring-policy
- http://localhost:8080/api/feedback/tester-quality?min_votes=5&promote_threshold=0.8&demote_threshold=0.4
- http://localhost:8080/api/admin/audit-logs?limit=20
- http://localhost:8080/api/alerts/history?limit=20
- http://localhost:8080/api/admin/alerts/prune-preview?retention_days=30&max_rows=20000
- POST http://localhost:8080/api/admin/alerts/prune
- http://localhost:8080/api/alerts/history/export?limit=100
- http://localhost:8080/api/metrics/ops?hours=24
- http://localhost:8080/api/monitoring/scheduler
- http://localhost:8080/api/monitoring/scheduler/runs?limit=50
- http://localhost:8080/api/monitoring/scheduler/runs?limit=50&source=persistent
- http://localhost:8080/api/monitoring/scheduler/adaptive
- POST http://localhost:8080/api/monitoring/scheduler/adaptive
- http://localhost:8080/api/monitoring/scheduler/adaptive/profiles
- POST http://localhost:8080/api/monitoring/scheduler/adaptive/profiles
- POST http://localhost:8080/api/monitoring/scheduler/adaptive/reset
- POST http://localhost:8080/api/monitoring/scheduler/start
- POST http://localhost:8080/api/monitoring/scheduler/stop
- POST http://localhost:8080/api/monitoring/run-once

## Notes
- Local feedback DB file: backend/signalwatch_feedback.db (ignored by git)
- Under burst traffic, Naver may still return intermittent 403
- Auth header: `X-Admin-Key`
- If both read/write admin keys are empty, admin auth is disabled for local development
- `SIGNALWATCH_ADMIN_KEY` remains as legacy fallback key
- Write scope is rate-limited (default 60 requests per 60 seconds)
- App version: `1.24.0`
