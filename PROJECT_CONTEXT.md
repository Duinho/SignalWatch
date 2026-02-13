# SignalWatch Project Context

## Goal
- Monitor stock news and send only meaningful alerts.
- No auto-trading. User makes final investment decisions.

## Stack
- Backend: Python 3.11, FastAPI, requests, BeautifulSoup4
- Infra target: Cloud Run
- Mobile target: Flutter + FCM

## Core Endpoints
- GET /
- GET /api/health
- GET /api/monitoring-policy
- GET /api/monitoring/scheduler
- GET /api/monitoring/scheduler/runs
- GET /api/monitoring/scheduler/adaptive
- POST /api/monitoring/scheduler/adaptive
- GET /api/monitoring/scheduler/adaptive/profiles
- POST /api/monitoring/scheduler/adaptive/profiles
- GET /api/news/{stock_code}
- GET /api/alerts
- GET /api/alerts/history
- GET /api/alerts/history/export
- GET /api/metrics/ops
- GET /api/watchlist
- GET /api/multiple-news
- POST /api/monitoring/scheduler/start
- POST /api/monitoring/scheduler/stop
- POST /api/monitoring/run-once
- POST /api/monitoring/scheduler/adaptive/reset
- GET /api/admin/alerts/prune-preview
- POST /api/admin/alerts/prune

## Monitoring Scheduler
- Time-window scheduler follows monitoring policy intervals.
- Scheduler exposes status and recent run history (in-memory + persisted SQLite).
- `/api/monitoring/scheduler/runs` supports memory/persistent source inspection.
- `/api/metrics/ops` includes scheduler status, recent runs, and policy-level run metrics.
- Adaptive scheduler sensitivity can auto-adjust run min_score based on alert volume (default disabled).
- Adaptive scheduler config can be updated at runtime via API (without restart).
- Adaptive scheduler now supports policy-specific profiles (`pre_market`, `market_open`, `after_close`, `night_watch`).

## Feedback Learning Endpoints
- POST /api/feedback/article
- GET /api/feedback/article-summary
- GET /api/feedback/keyword-candidates
- GET /api/feedback/keyword-rules
- POST /api/feedback/keyword-rules/apply
- POST /api/feedback/keyword-rules/disable
- POST /api/feedback/keyword-rules/auto-apply
- Note: management endpoints require `X-Admin-Key` when admin keys are configured

## Tester Trust and Tier Endpoints
- POST /api/feedback/user-trust
- POST /api/feedback/user-trust/reset
- GET /api/feedback/user-trust
- GET /api/feedback/user-trust/list
- POST /api/feedback/user-tier
- GET /api/feedback/user-tier
- GET /api/feedback/user-tier/list
- GET /api/feedback/tester-quality
- POST /api/feedback/user-tier/auto-apply
- GET /api/admin/audit-logs
- `tester-quality` endpoints support `recent_days` for time-window analysis

## Current Alert Engine
- Sentiment: title keyword based (positive/negative/neutral)
- Alert scoring uses per-stock fetch size (default 30) for better surge detection
- Alert payload returns only a small preview list (default 3) for lightweight responses
- Feedback-based score adjustment is applied (recent tester consensus + AI mismatch signal)
- Duplicate-topic penalty is applied to reduce score inflation from syndicated copies
- Importance score (0-100):
  - News volume / surge
  - Multi-source coverage
  - High-impact keywords
  - Positive/negative concentration
  - Tester feedback consensus signal
  - Duplicate-topic ratio penalty
- Delivery levels:
  - score >= 70: push_immediate
  - score >= 40: in_app
  - else: daily_digest

## Crawler Stability
- Retry/backoff for transient network and HTTP errors (403/429/5xx)
- Request throttle interval
- In-memory TTL cache (default 180s)
- Fallback to stale cache when live request fails

## Alert Persistence
- `/api/alerts` now persists generated alert snapshots into local SQLite
- `/api/alerts/history` provides filtered history (`stock_code`, `delivery_level`, `min_score`, `limit`)
- Alert history is pruned automatically by retention policy and can be pruned manually by admin API
- `GET /api/admin/alerts/prune-preview` estimates deletion impact before manual prune
- Alert history can be exported as CSV (`/api/alerts/history/export`)

## Feedback Model Notes
- 1-user-1-vote per article (upsert by user_id_hash + article_link)
- Weighted vote score = user_confidence(1-5) * effective_trust_weight
- Effective trust priority:
  - manual override
  - tester tier default (core/general/observer)
  - system default (1.0)
- Tier auto-apply supports dry_run mode first
- Admin management actions are persisted to audit logs

## Environment Knobs
- NAVER_MIN_REQUEST_INTERVAL_SEC (default: 0.9)
- NEWS_CACHE_TTL_SEC (default: 180)
- FEEDBACK_CONSENSUS_MIN_VOTES (default: 20)
- FEEDBACK_CONSENSUS_THRESHOLD (default: 0.8)
- TESTER_QUALITY_MIN_VOTES_DEFAULT (default: 20)
- TESTER_QUALITY_PROMOTE_THRESHOLD_DEFAULT (default: 0.8)
- TESTER_QUALITY_DEMOTE_THRESHOLD_DEFAULT (default: 0.4)
- SIGNALWATCH_ADMIN_READ_KEY (read scope key)
- SIGNALWATCH_ADMIN_WRITE_KEY (write scope key)
- SIGNALWATCH_ADMIN_KEY (legacy fallback key for both scopes)
- ADMIN_WRITE_RATE_LIMIT_COUNT (write scope max requests per window, default: 60)
- ADMIN_WRITE_RATE_LIMIT_WINDOW_SEC (write scope window seconds, default: 60)
- ADMIN_WRITE_RATE_LIMIT_<ACTION>_COUNT (action-specific write limit override)
- ADMIN_WRITE_RATE_LIMIT_<ACTION>_WINDOW_SEC (action-specific write window override)
- ALERT_HISTORY_RETENTION_DAYS (alert history retention period, default: 30)
- ALERT_HISTORY_MAX_ROWS (max rows kept in alert history, default: 20000)
- ALERT_SCORING_FETCH_LIMIT_DEFAULT (per-stock news fetch size for scoring, default: 30)
- ALERT_NEWS_PREVIEW_LIMIT_DEFAULT (latest_news size in alert payload, default: 3)
- FEEDBACK_SCORE_RECENT_HOURS (feedback signal lookback window, default: 72)
- FEEDBACK_SCORE_MIN_VOTES (minimum votes to activate feedback adjustment, default: 5)
- FEEDBACK_SCORE_CONSENSUS_THRESHOLD (consensus threshold for adjustment, default: 0.75)
- FEEDBACK_SCORE_DELTA_CONSENSUS (score delta when strong consensus is present, default: 5)
- FEEDBACK_SCORE_DELTA_AI_MISMATCH (score delta when AI/user mismatch is high, default: 4)
- MONITORING_SCHEDULER_AUTOSTART (auto start scheduler on app startup, default: false)
- MONITORING_SCHEDULER_ALERT_LIMIT (scheduler run alert limit, default: 20)
- MONITORING_SCHEDULER_MIN_SCORE (scheduler run min score filter, default: 0)
- MONITORING_SCHEDULER_HISTORY_LIMIT (in-memory scheduler run history size, default: 200)
- MONITORING_ADAPTIVE_MIN_SCORE_ENABLED (adaptive min_score mode, default: false)
- MONITORING_ADAPTIVE_TARGET_ALERT_COUNT (adaptive target alert count per run, default: 3)
- MONITORING_ADAPTIVE_ALERT_BAND (allowed alert count band, default: 1)
- MONITORING_ADAPTIVE_SCORE_STEP (adaptive score step, default: 5)
- MONITORING_ADAPTIVE_MIN_BOUND (adaptive min_score lower bound, default: 0)
- MONITORING_ADAPTIVE_MAX_BOUND (adaptive min_score upper bound, default: 80)

## Next Priorities
1. Tune tester-quality thresholds with real data.
2. Continue broadening regression coverage for live crawl edge cases.
3. Add optional retention policy dashboard endpoint.
4. Add UI/admin panel flow for viewing ops metrics with filters.
