from collections import Counter, defaultdict, deque
import csv
from datetime import datetime
import io
from pathlib import Path
from typing import Deque, Dict, List, Literal, Optional, Tuple
import hashlib
import os
import re
import threading
import time

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

from crawler import NaverNewsSearchCrawler
from feedback_store import FeedbackStore
from alert_store import AlertStore


app = FastAPI(
    title="SignalWatch Stock Alert API",
    description="Stock news monitoring and alert API",
    version="1.24.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

search_crawler = NaverNewsSearchCrawler()
feedback_store = FeedbackStore(
    db_path=os.getenv(
        "FEEDBACK_DB_PATH",
        str(Path(__file__).resolve().parent / "signalwatch_feedback.db"),
    )
)
alert_store = AlertStore(
    db_path=os.getenv(
        "ALERT_DB_PATH",
        str(Path(__file__).resolve().parent / "signalwatch_alerts.db"),
    )
)

STOCK_CODE_TO_NAME: Dict[str, str] = {
    "005930": "\uc0bc\uc131\uc804\uc790",
    "000660": "SK\ud558\uc774\ub2c9\uc2a4",
    "035720": "\uce74\uce74\uc624",
    "051910": "LG\ud654\ud559",
    "006400": "\uc0bc\uc131SDI",
}

DEFAULT_WATCHLIST = [{"code": code, "name": name} for code, name in STOCK_CODE_TO_NAME.items()]

POSITIVE_KEYWORDS = {
    "\uc0c1\uc2b9", "\uae09\ub4f1", "\ud751\uc790", "\uc0ac\uc0c1\ucd5c\ub300", "\uc131\uc7a5", "\uc218\uc8fc", "\uacc4\uc57d", "\uccb4\uacb0",
    "\uc2b9\uc778", "\ud1b5\uacfc", "\ud2b9\ud5c8", "\ud655\ub300", "\uc99d\uac00", "\ud638\uc2e4\uc801", "\uc2e0\uace0\uac00", "\ubc30\ub2f9",
    "\ub9e4\uc218", "\ud22c\uc790\uc720\uce58", "\ucd9c\uc2dc", "\uc591\uc0b0", "\ud30c\ud2b8\ub108\uc2ed", "\uacf5\uae09", "\uc218\ud61c",
    "\uac15\uc138", "\uac1c\uc120",
}

NEGATIVE_KEYWORDS = {
    "\ud558\ub77d", "\uae09\ub77d", "\uc801\uc790", "\uac10\uc18c", "\ucd95\uc18c", "\uc911\ub2e8", "\uc9c0\uc5f0", "\ub9ac\uc2a4\ud06c", "\uc545\ud654",
    "\uc6b0\ub824", "\uc18c\uc1a1", "\uc81c\uc7ac", "\uc870\uc0ac", "\uc2e4\ud328", "\ucca0\ud68c", "\ucde8\uc18c", "\ud30c\uc5c5", "\uc190\uc2e4",
    "\uacbd\uace0", "\ub9e4\ub3c4", "\uc57d\uc138", "\ubd80\uc9c4", "\uac10\uc6d0", "\uc720\uc99d", "\ud574\uc9c0", "\uace0\ubc1c",
}

IMPACT_POSITIVE_KEYWORDS: Dict[str, int] = {
    "\uc2e4\uc801": 8,
    "\uc218\uc8fc": 12,
    "\uacc4\uc57d": 10,
    "\uc591\uc0b0": 10,
    "\uc2b9\uc778": 10,
    "\ud751\uc790": 9,
    "\uc0ac\uc0c1\ucd5c\ub300": 12,
    "\uc2e0\uace0\uac00": 7,
}

IMPACT_NEGATIVE_KEYWORDS: Dict[str, int] = {
    "\uc801\uc790": 12,
    "\uc2e4\ud328": 10,
    "\uc18c\uc1a1": 10,
    "\uc81c\uc7ac": 12,
    "\ub9ac\uc2a4\ud06c": 8,
    "\uac10\uc6d0": 8,
    "\uc911\ub2e8": 8,
    "\ud558\ud5a5": 8,
}

FEEDBACK_RULE_SCORE_BOOST = 2
FEEDBACK_CONSENSUS_MIN_VOTES = int(os.getenv("FEEDBACK_CONSENSUS_MIN_VOTES", "20"))
FEEDBACK_CONSENSUS_THRESHOLD = float(os.getenv("FEEDBACK_CONSENSUS_THRESHOLD", "0.8"))
USER_TIER_DEFAULT_WEIGHTS = {
    "core": 1.8,
    "general": 1.0,
    "observer": 0.7,
}
ADMIN_KEY_ENV = "SIGNALWATCH_ADMIN_KEY"
ADMIN_READ_KEY_ENV = "SIGNALWATCH_ADMIN_READ_KEY"
ADMIN_WRITE_KEY_ENV = "SIGNALWATCH_ADMIN_WRITE_KEY"
ADMIN_WRITE_RATE_LIMIT_COUNT_ENV = "ADMIN_WRITE_RATE_LIMIT_COUNT"
ADMIN_WRITE_RATE_LIMIT_WINDOW_SEC_ENV = "ADMIN_WRITE_RATE_LIMIT_WINDOW_SEC"
ADMIN_WRITE_RATE_LIMIT_ACTION_PREFIX = "ADMIN_WRITE_RATE_LIMIT"
ADMIN_WRITE_ACTIONS = [
    "upsert_user_trust",
    "reset_user_trust",
    "upsert_user_tier",
    "auto_apply_user_tier",
    "apply_keyword_rule",
    "disable_keyword_rule",
    "auto_apply_keyword_rules",
    "prune_alert_history",
    "monitoring_scheduler_start",
    "monitoring_scheduler_stop",
    "monitoring_run_once",
    "monitoring_adaptive_reset",
    "monitoring_adaptive_update",
    "monitoring_adaptive_profile_update",
]
ADMIN_WRITE_RATE_LOCK = threading.Lock()
ADMIN_WRITE_RATE_BUCKETS: Dict[str, Deque[float]] = defaultdict(deque)


def _bounded_int_env(name: str, default: int, min_value: int, max_value: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError:
        value = default
    return max(min_value, min(max_value, value))


def _bounded_float_env(name: str, default: float, min_value: float, max_value: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = float(raw)
    except ValueError:
        value = default
    return max(min_value, min(max_value, value))


def _bounded_int(value: object, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(min_value, min(max_value, parsed))


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


TESTER_QUALITY_MIN_VOTES_DEFAULT = _bounded_int_env("TESTER_QUALITY_MIN_VOTES_DEFAULT", 20, 5, 10000)
TESTER_QUALITY_PROMOTE_THRESHOLD_DEFAULT = _bounded_float_env("TESTER_QUALITY_PROMOTE_THRESHOLD_DEFAULT", 0.8, 0.5, 1.0)
TESTER_QUALITY_DEMOTE_THRESHOLD_DEFAULT = _bounded_float_env("TESTER_QUALITY_DEMOTE_THRESHOLD_DEFAULT", 0.4, 0.0, 0.9)
ALERT_HISTORY_RETENTION_DAYS = _bounded_int_env("ALERT_HISTORY_RETENTION_DAYS", 30, 1, 3650)
ALERT_HISTORY_MAX_ROWS = _bounded_int_env("ALERT_HISTORY_MAX_ROWS", 20000, 100, 2000000)
ALERT_SCORING_FETCH_LIMIT_DEFAULT = _bounded_int_env("ALERT_SCORING_FETCH_LIMIT_DEFAULT", 30, 10, 100)
ALERT_NEWS_PREVIEW_LIMIT_DEFAULT = _bounded_int_env("ALERT_NEWS_PREVIEW_LIMIT_DEFAULT", 3, 1, 20)
FEEDBACK_SCORE_RECENT_HOURS = _bounded_int_env("FEEDBACK_SCORE_RECENT_HOURS", 72, 1, 720)
FEEDBACK_SCORE_MIN_VOTES = _bounded_int_env("FEEDBACK_SCORE_MIN_VOTES", 5, 1, 10000)
FEEDBACK_SCORE_CONSENSUS_THRESHOLD = _bounded_float_env("FEEDBACK_SCORE_CONSENSUS_THRESHOLD", 0.75, 0.5, 1.0)
FEEDBACK_SCORE_DELTA_CONSENSUS = _bounded_int_env("FEEDBACK_SCORE_DELTA_CONSENSUS", 5, 0, 30)
FEEDBACK_SCORE_DELTA_AI_MISMATCH = _bounded_int_env("FEEDBACK_SCORE_DELTA_AI_MISMATCH", 4, 0, 30)
MONITORING_SCHEDULER_ALERT_LIMIT = _bounded_int_env("MONITORING_SCHEDULER_ALERT_LIMIT", 20, 1, 100)
MONITORING_SCHEDULER_MIN_SCORE = _bounded_int_env("MONITORING_SCHEDULER_MIN_SCORE", 0, 0, 100)
MONITORING_SCHEDULER_HISTORY_LIMIT = _bounded_int_env("MONITORING_SCHEDULER_HISTORY_LIMIT", 200, 10, 1000)
MONITORING_SCHEDULER_AUTOSTART = _bool_env("MONITORING_SCHEDULER_AUTOSTART", default=False)
MONITORING_ADAPTIVE_MIN_SCORE_ENABLED = _bool_env("MONITORING_ADAPTIVE_MIN_SCORE_ENABLED", default=False)
MONITORING_ADAPTIVE_TARGET_ALERT_COUNT = _bounded_int_env("MONITORING_ADAPTIVE_TARGET_ALERT_COUNT", 3, 0, 100)
MONITORING_ADAPTIVE_ALERT_BAND = _bounded_int_env("MONITORING_ADAPTIVE_ALERT_BAND", 1, 0, 20)
MONITORING_ADAPTIVE_SCORE_STEP = _bounded_int_env("MONITORING_ADAPTIVE_SCORE_STEP", 5, 1, 20)
MONITORING_ADAPTIVE_MIN_BOUND = _bounded_int_env("MONITORING_ADAPTIVE_MIN_BOUND", 0, 0, 100)
MONITORING_ADAPTIVE_MAX_BOUND = _bounded_int_env("MONITORING_ADAPTIVE_MAX_BOUND", 80, 0, 100)
if MONITORING_ADAPTIVE_MIN_BOUND > MONITORING_ADAPTIVE_MAX_BOUND:
    MONITORING_ADAPTIVE_MIN_BOUND, MONITORING_ADAPTIVE_MAX_BOUND = (
        MONITORING_ADAPTIVE_MAX_BOUND,
        MONITORING_ADAPTIVE_MIN_BOUND,
    )

# In-memory history to detect "news surge" patterns.
NEWS_COUNT_HISTORY: Dict[str, Deque[int]] = defaultdict(lambda: deque(maxlen=40))

MONITORING_POLICY = [
    {
        "name": "pre_market",
        "start": "08:00",
        "end": "09:00",
        "interval_sec": 180,
        "purpose": "\uc7a5 \uc2dc\uc791 \uc804 \uc900\ube44",
    },
    {
        "name": "market_open",
        "start": "09:00",
        "end": "15:30",
        "interval_sec": 60,
        "purpose": "\uc7a5\uc911 \uc2e4\uc2dc\uac04 \ub300\uc751",
    },
    {
        "name": "after_close",
        "start": "15:30",
        "end": "18:00",
        "interval_sec": 300,
        "purpose": "\uc7a5 \ub9c8\uac10 \ud6c4 \uc2e4\uc801/\uacf5\uc2dc \ubaa8\ub2c8\ud130\ub9c1",
    },
    {
        "name": "night_watch",
        "start": "18:00",
        "end": "08:00",
        "interval_sec": 1800,
        "purpose": "\uc57c\uac04 \uc911\ub300 \uc774\uc288 \ubaa8\ub2c8\ud130\ub9c1",
    },
]
MONITORING_POLICY_NAMES = {str(profile.get("name", "")) for profile in MONITORING_POLICY if profile.get("name")}

DEFAULT_ADAPTIVE_POLICY_OVERRIDES: Dict[str, Dict[str, int]] = {
    "pre_market": {
        "target_alert_count": 2,
        "alert_band": 1,
        "score_step": 5,
        "min_bound": 0,
        "max_bound": 70,
    },
    "market_open": {
        "target_alert_count": 4,
        "alert_band": 1,
        "score_step": 5,
        "min_bound": 0,
        "max_bound": 80,
    },
    "after_close": {
        "target_alert_count": 2,
        "alert_band": 1,
        "score_step": 5,
        "min_bound": 5,
        "max_bound": 80,
    },
    "night_watch": {
        "target_alert_count": 1,
        "alert_band": 0,
        "score_step": 5,
        "min_bound": 10,
        "max_bound": 90,
    },
}

SCHEDULER_LOCK = threading.Lock()
SCHEDULER_STOP_EVENT = threading.Event()
SCHEDULER_THREAD: Optional[threading.Thread] = None
SCHEDULER_RUN_HISTORY: Deque[Dict[str, object]] = deque(maxlen=MONITORING_SCHEDULER_HISTORY_LIMIT)
SCHEDULER_STATE: Dict[str, object] = {
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
    "effective_min_score": MONITORING_SCHEDULER_MIN_SCORE,
    "adaptive_enabled": MONITORING_ADAPTIVE_MIN_SCORE_ENABLED,
    "adaptive_target_alert_count": MONITORING_ADAPTIVE_TARGET_ALERT_COUNT,
    "adaptive_alert_band": MONITORING_ADAPTIVE_ALERT_BAND,
    "adaptive_score_step": MONITORING_ADAPTIVE_SCORE_STEP,
    "adaptive_min_bound": MONITORING_ADAPTIVE_MIN_BOUND,
    "adaptive_max_bound": MONITORING_ADAPTIVE_MAX_BOUND,
    "adaptive_profiles": {k: dict(v) for k, v in DEFAULT_ADAPTIVE_POLICY_OVERRIDES.items()},
    "adaptive_current_min_score": MONITORING_SCHEDULER_MIN_SCORE,
    "adaptive_last_adjustment": "",
    "adaptive_last_reason": "",
    "adaptive_last_direction": "hold",
    "next_interval_sec": 0,
    "active_policy_name": "",
}


SentimentLabel = Literal["positive", "negative", "neutral"]


class ArticleFeedbackRequest(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=128, description="Tester/device unique id")
    stock_code: str = Field(..., min_length=1, max_length=20)
    article_link: str = Field(..., min_length=5, max_length=2000)
    article_title: str = Field(..., min_length=2, max_length=500)
    article_source: str = Field(default="", max_length=100)
    ai_label: SentimentLabel
    user_label: SentimentLabel
    user_confidence: int = Field(default=3, ge=1, le=5)
    note: str = Field(default="", max_length=500)


class KeywordRuleApplyRequest(BaseModel):
    keyword: str = Field(..., min_length=2, max_length=100)
    label: SentimentLabel
    support_votes: int = Field(default=0, ge=0)
    consensus_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    source: str = Field(default="manual", max_length=64)


class KeywordRuleDisableRequest(BaseModel):
    keyword: str = Field(..., min_length=2, max_length=100)


class UserTrustUpdateRequest(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=128, description="Tester/device unique id")
    trust_weight: float = Field(..., ge=0.2, le=3.0)
    note: str = Field(default="", max_length=200)


class UserTrustResetRequest(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=128, description="Tester/device unique id")


class UserTierUpdateRequest(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=128, description="Tester/device unique id")
    tester_tier: Literal["core", "general", "observer"]
    note: str = Field(default="", max_length=200)


class UserTierAutoApplyRequest(BaseModel):
    min_votes: int = Field(default=TESTER_QUALITY_MIN_VOTES_DEFAULT, ge=5, le=10000)
    promote_threshold: float = Field(default=TESTER_QUALITY_PROMOTE_THRESHOLD_DEFAULT, ge=0.5, le=1.0)
    demote_threshold: float = Field(default=TESTER_QUALITY_DEMOTE_THRESHOLD_DEFAULT, ge=0.0, le=0.9)
    recent_days: Optional[int] = Field(default=None, ge=1, le=3650)
    limit: int = Field(default=200, ge=1, le=5000)
    max_apply: int = Field(default=50, ge=1, le=500)
    dry_run: bool = Field(default=True)


class AlertHistoryPruneRequest(BaseModel):
    retention_days: int = Field(default=ALERT_HISTORY_RETENTION_DAYS, ge=1, le=3650)
    max_rows: int = Field(default=ALERT_HISTORY_MAX_ROWS, ge=100, le=2000000)


class MonitoringAdaptiveUpdateRequest(BaseModel):
    enabled: Optional[bool] = None
    target_alert_count: Optional[int] = Field(default=None, ge=0, le=100)
    alert_band: Optional[int] = Field(default=None, ge=0, le=20)
    score_step: Optional[int] = Field(default=None, ge=1, le=20)
    min_bound: Optional[int] = Field(default=None, ge=0, le=100)
    max_bound: Optional[int] = Field(default=None, ge=0, le=100)
    reset_current_min_score: bool = Field(default=False)


class MonitoringAdaptiveProfileUpdateRequest(BaseModel):
    policy_name: Literal["pre_market", "market_open", "after_close", "night_watch"]
    enabled: Optional[bool] = None
    target_alert_count: Optional[int] = Field(default=None, ge=0, le=100)
    alert_band: Optional[int] = Field(default=None, ge=0, le=20)
    score_step: Optional[int] = Field(default=None, ge=1, le=20)
    min_bound: Optional[int] = Field(default=None, ge=0, le=100)
    max_bound: Optional[int] = Field(default=None, ge=0, le=100)
    clear_profile: bool = Field(default=False)


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _admin_key_config() -> Dict[str, object]:
    legacy_key = os.getenv(ADMIN_KEY_ENV, "").strip()
    read_key = os.getenv(ADMIN_READ_KEY_ENV, "").strip() or legacy_key
    write_key = os.getenv(ADMIN_WRITE_KEY_ENV, "").strip() or legacy_key
    configured = bool(read_key or write_key)
    return {
        "configured": configured,
        "read_key": read_key,
        "write_key": write_key,
        "legacy_key_enabled": bool(legacy_key),
    }


def _rate_action_token(action: str) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "_", action).strip("_").upper()
    return token or "DEFAULT"


def _write_rate_limit_config(action: Optional[str] = None) -> Dict[str, object]:
    default_count = _bounded_int_env(ADMIN_WRITE_RATE_LIMIT_COUNT_ENV, 60, 1, 100000)
    default_window = _bounded_int_env(ADMIN_WRITE_RATE_LIMIT_WINDOW_SEC_ENV, 60, 1, 86400)

    if not action:
        return {
            "action": "default",
            "action_token": "DEFAULT",
            "max_requests": default_count,
            "window_sec": default_window,
            "count_env": ADMIN_WRITE_RATE_LIMIT_COUNT_ENV,
            "window_env": ADMIN_WRITE_RATE_LIMIT_WINDOW_SEC_ENV,
        }

    action_token = _rate_action_token(action)
    count_env = f"{ADMIN_WRITE_RATE_LIMIT_ACTION_PREFIX}_{action_token}_COUNT"
    window_env = f"{ADMIN_WRITE_RATE_LIMIT_ACTION_PREFIX}_{action_token}_WINDOW_SEC"
    max_requests = _bounded_int_env(count_env, default_count, 1, 100000)
    window_sec = _bounded_int_env(window_env, default_window, 1, 86400)
    return {
        "action": action,
        "action_token": action_token,
        "max_requests": max_requests,
        "window_sec": window_sec,
        "count_env": count_env,
        "window_env": window_env,
    }


def _admin_key_fingerprint(key: str) -> str:
    if not key:
        return "unknown"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def _enforce_admin_write_rate_limit(identity: str, action: str) -> Dict[str, object]:
    cfg = _write_rate_limit_config(action=action)
    now_ts = time.time()
    max_requests = int(cfg["max_requests"])
    window_sec = int(cfg["window_sec"])
    action_token = str(cfg["action_token"])
    bucket_key = f"{identity}:{action_token}"

    with ADMIN_WRITE_RATE_LOCK:
        bucket = ADMIN_WRITE_RATE_BUCKETS[bucket_key]
        while bucket and now_ts - bucket[0] >= window_sec:
            bucket.popleft()

        if len(bucket) >= max_requests:
            retry_after = max(1, int(window_sec - (now_ts - bucket[0])))
            raise HTTPException(
                status_code=429,
                detail=f"Admin write rate limit exceeded for action={action}. Retry after {retry_after}s",
            )

        bucket.append(now_ts)
        remaining = max(0, max_requests - len(bucket))

    return {
        "action": action,
        "action_token": action_token,
        "max_requests": max_requests,
        "window_sec": window_sec,
        "remaining": remaining,
        "count_env": str(cfg["count_env"]),
        "window_env": str(cfg["window_env"]),
    }


def _authorize_admin(required_scope: str, x_admin_key: Optional[str]) -> Dict[str, str]:
    cfg = _admin_key_config()
    if not bool(cfg["configured"]):
        return {"auth_mode": "disabled", "auth_scope": "disabled"}

    read_key = str(cfg["read_key"])
    write_key = str(cfg["write_key"])
    allowed_read = bool(read_key and x_admin_key == read_key)
    allowed_write = bool(write_key and x_admin_key == write_key)

    if required_scope == "read" and (allowed_read or allowed_write):
        scope = "write" if allowed_write else "read"
        return {"auth_mode": "enabled", "auth_scope": scope}

    if required_scope == "write" and allowed_write:
        return {"auth_mode": "enabled", "auth_scope": "write"}

    raise HTTPException(status_code=401, detail=f"Invalid admin key for {required_scope} scope")


def require_admin_read(x_admin_key: Optional[str] = Header(default=None, alias="X-Admin-Key")) -> Dict[str, str]:
    return _authorize_admin(required_scope="read", x_admin_key=x_admin_key)


def require_admin_write(x_admin_key: Optional[str] = Header(default=None, alias="X-Admin-Key")) -> Dict[str, str]:
    auth = _authorize_admin(required_scope="write", x_admin_key=x_admin_key)
    if auth.get("auth_mode") == "enabled":
        auth["admin_identity"] = _admin_key_fingerprint(x_admin_key or "")
    return auth


def log_admin_action_safe(action: str, target_type: str, target_id: str, meta: Optional[Dict[str, object]] = None) -> None:
    try:
        feedback_store.log_admin_action(
            action=action,
            target_type=target_type,
            target_id=target_id,
            meta=meta or {},
        )
    except Exception:
        # Do not break primary API behavior if audit log insert fails.
        pass


def enforce_admin_write_rate_limit(admin: Dict[str, str], action: str) -> Dict[str, object]:
    if str(admin.get("auth_mode")) != "enabled":
        return {
            "action": action,
            "action_token": _rate_action_token(action),
            "max_requests": 0,
            "window_sec": 0,
            "remaining": 0,
            "count_env": ADMIN_WRITE_RATE_LIMIT_COUNT_ENV,
            "window_env": ADMIN_WRITE_RATE_LIMIT_WINDOW_SEC_ENV,
        }
    identity = str(admin.get("admin_identity", "unknown"))
    return _enforce_admin_write_rate_limit(identity=identity, action=action)


def stock_code_to_keyword(stock_code: str) -> str:
    return STOCK_CODE_TO_NAME.get(stock_code, stock_code)


def normalize_fetch_meta(keyword: str) -> Dict[str, object]:
    meta = search_crawler.get_last_result_meta(keyword)
    return {
        "source": meta.get("source", "unknown"),
        "age_sec": meta.get("age_sec", None),
        "fetched_at": meta.get("fetched_at", None),
        "cache_ttl_sec": meta.get("cache_ttl_sec", None),
    }


def collect_news_history_metrics() -> Dict[str, object]:
    by_stock: List[Dict[str, object]] = []
    for stock_code, history in NEWS_COUNT_HISTORY.items():
        if not history:
            continue
        history_list = list(history)
        by_stock.append(
            {
                "stock_code": stock_code,
                "stock_name": STOCK_CODE_TO_NAME.get(stock_code, stock_code),
                "samples": len(history_list),
                "latest_count": int(history_list[-1]),
                "avg_count": round(sum(history_list) / max(1, len(history_list)), 2),
                "max_count": int(max(history_list)),
                "min_count": int(min(history_list)),
            }
        )

    by_stock.sort(key=lambda item: (item["latest_count"], item["avg_count"]), reverse=True)
    return {
        "tracked_stocks": len(by_stock),
        "stocks": by_stock,
    }


def calculate_feedback_score_adjustment(stock_code: str) -> Dict[str, object]:
    signal = feedback_store.get_stock_feedback_signal(
        stock_code=stock_code,
        since_hours=FEEDBACK_SCORE_RECENT_HOURS,
        min_votes=FEEDBACK_SCORE_MIN_VOTES,
    )

    score_delta = 0
    reasons: List[str] = []

    if bool(signal.get("ready")):
        consensus_ratio = float(signal.get("consensus_ratio", 0.0))
        consensus_label = str(signal.get("consensus_label", "neutral"))
        ai_match_ratio = float(signal.get("ai_match_ratio", 1.0))

        if consensus_ratio >= FEEDBACK_SCORE_CONSENSUS_THRESHOLD:
            score_delta += FEEDBACK_SCORE_DELTA_CONSENSUS
            if consensus_label == "positive":
                reasons.append("테스터 호재 합의")
            elif consensus_label == "negative":
                reasons.append("테스터 악재 합의")
            else:
                reasons.append("테스터 중립 합의")

        if ai_match_ratio < 0.5:
            score_delta += FEEDBACK_SCORE_DELTA_AI_MISMATCH
            reasons.append("AI-사용자 판단 불일치")

    return {
        "score_delta": score_delta,
        "reasons": reasons,
        "signal": signal,
    }


def _hhmm_to_minutes(value: str) -> int:
    hour, minute = value.split(":")
    return int(hour) * 60 + int(minute)


def current_monitoring_profile(now: Optional[datetime] = None) -> Dict[str, object]:
    current = now or datetime.now()
    current_minutes = current.hour * 60 + current.minute

    for profile in MONITORING_POLICY:
        start = _hhmm_to_minutes(profile["start"])
        end = _hhmm_to_minutes(profile["end"])
        if start <= end:
            in_range = start <= current_minutes < end
        else:
            in_range = current_minutes >= start or current_minutes < end
        if in_range:
            return dict(profile)

    return dict(MONITORING_POLICY[-1])


def scheduler_status_snapshot() -> Dict[str, object]:
    with SCHEDULER_LOCK:
        thread_alive = bool(SCHEDULER_THREAD and SCHEDULER_THREAD.is_alive())
        snapshot = dict(SCHEDULER_STATE)
        snapshot["history_size"] = len(SCHEDULER_RUN_HISTORY)
    snapshot["thread_alive"] = thread_alive
    return snapshot


def scheduler_run_history_snapshot(limit: int = 20) -> List[Dict[str, object]]:
    resolved_limit = _bounded_int(limit, default=20, min_value=1, max_value=200)
    with SCHEDULER_LOCK:
        rows = list(SCHEDULER_RUN_HISTORY)
    rows.reverse()
    return rows[:resolved_limit]


def _record_scheduler_run(entry: Dict[str, object]):
    row = dict(entry)
    with SCHEDULER_LOCK:
        SCHEDULER_RUN_HISTORY.append(dict(row))
    try:
        alert_store.save_monitoring_run(
            run=row,
            created_at=str(row.get("finished_at") or now_str()),
        )
    except Exception:
        # Monitoring history persistence should not break alert pipeline.
        pass


def _set_scheduler_state(**kwargs):
    with SCHEDULER_LOCK:
        for key, value in kwargs.items():
            SCHEDULER_STATE[key] = value


def _sanitize_adaptive_profiles_unlocked() -> Dict[str, Dict[str, object]]:
    raw_profiles = SCHEDULER_STATE.get("adaptive_profiles", {})
    sanitized: Dict[str, Dict[str, object]] = {}

    for policy_name in MONITORING_POLICY_NAMES:
        base = dict(DEFAULT_ADAPTIVE_POLICY_OVERRIDES.get(policy_name, {}))
        override: Dict[str, object] = {}
        if isinstance(raw_profiles, dict):
            candidate = raw_profiles.get(policy_name)
            if isinstance(candidate, dict):
                if "enabled" in candidate:
                    override["enabled"] = bool(candidate.get("enabled"))
                if candidate.get("target_alert_count") is not None:
                    override["target_alert_count"] = _bounded_int(candidate.get("target_alert_count"), 0, 0, 100)
                if candidate.get("alert_band") is not None:
                    override["alert_band"] = _bounded_int(candidate.get("alert_band"), 0, 0, 20)
                if candidate.get("score_step") is not None:
                    override["score_step"] = _bounded_int(candidate.get("score_step"), 5, 1, 20)
                if candidate.get("min_bound") is not None:
                    override["min_bound"] = _bounded_int(candidate.get("min_bound"), 0, 0, 100)
                if candidate.get("max_bound") is not None:
                    override["max_bound"] = _bounded_int(candidate.get("max_bound"), 80, 0, 100)

        merged = dict(base)
        merged.update(override)
        min_bound = merged.get("min_bound")
        max_bound = merged.get("max_bound")
        if isinstance(min_bound, int) and isinstance(max_bound, int) and min_bound > max_bound:
            merged["min_bound"], merged["max_bound"] = max_bound, min_bound
        sanitized[policy_name] = merged

    SCHEDULER_STATE["adaptive_profiles"] = {k: dict(v) for k, v in sanitized.items()}
    return sanitized


def adaptive_scheduler_profiles() -> Dict[str, Dict[str, object]]:
    with SCHEDULER_LOCK:
        profiles = _sanitize_adaptive_profiles_unlocked()
    return {k: dict(v) for k, v in profiles.items()}


def _adaptive_runtime_config_unlocked(policy_name: Optional[str] = None) -> Dict[str, object]:
    enabled = bool(SCHEDULER_STATE.get("adaptive_enabled", MONITORING_ADAPTIVE_MIN_SCORE_ENABLED))
    target_alert_count = _bounded_int(
        SCHEDULER_STATE.get("adaptive_target_alert_count", MONITORING_ADAPTIVE_TARGET_ALERT_COUNT),
        default=MONITORING_ADAPTIVE_TARGET_ALERT_COUNT,
        min_value=0,
        max_value=100,
    )
    alert_band = _bounded_int(
        SCHEDULER_STATE.get("adaptive_alert_band", MONITORING_ADAPTIVE_ALERT_BAND),
        default=MONITORING_ADAPTIVE_ALERT_BAND,
        min_value=0,
        max_value=20,
    )
    score_step = _bounded_int(
        SCHEDULER_STATE.get("adaptive_score_step", MONITORING_ADAPTIVE_SCORE_STEP),
        default=MONITORING_ADAPTIVE_SCORE_STEP,
        min_value=1,
        max_value=20,
    )
    min_bound = _bounded_int(
        SCHEDULER_STATE.get("adaptive_min_bound", MONITORING_ADAPTIVE_MIN_BOUND),
        default=MONITORING_ADAPTIVE_MIN_BOUND,
        min_value=0,
        max_value=100,
    )
    max_bound = _bounded_int(
        SCHEDULER_STATE.get("adaptive_max_bound", MONITORING_ADAPTIVE_MAX_BOUND),
        default=MONITORING_ADAPTIVE_MAX_BOUND,
        min_value=0,
        max_value=100,
    )
    normalized_policy_name = str(policy_name or "").strip()
    policy_profile: Dict[str, object] = {}
    if normalized_policy_name in MONITORING_POLICY_NAMES:
        profiles = _sanitize_adaptive_profiles_unlocked()
        policy_profile = dict(profiles.get(normalized_policy_name, {}))
        if "enabled" in policy_profile:
            enabled = bool(policy_profile.get("enabled"))
        if policy_profile.get("target_alert_count") is not None:
            target_alert_count = _bounded_int(policy_profile.get("target_alert_count"), target_alert_count, 0, 100)
        if policy_profile.get("alert_band") is not None:
            alert_band = _bounded_int(policy_profile.get("alert_band"), alert_band, 0, 20)
        if policy_profile.get("score_step") is not None:
            score_step = _bounded_int(policy_profile.get("score_step"), score_step, 1, 20)
        if policy_profile.get("min_bound") is not None:
            min_bound = _bounded_int(policy_profile.get("min_bound"), min_bound, 0, 100)
        if policy_profile.get("max_bound") is not None:
            max_bound = _bounded_int(policy_profile.get("max_bound"), max_bound, 0, 100)
    if min_bound > max_bound:
        min_bound, max_bound = max_bound, min_bound
    current_min_score = _bounded_int(
        SCHEDULER_STATE.get("adaptive_current_min_score", MONITORING_SCHEDULER_MIN_SCORE),
        default=MONITORING_SCHEDULER_MIN_SCORE,
        min_value=min_bound,
        max_value=max_bound,
    )
    return {
        "policy_name": normalized_policy_name,
        "policy_profile": policy_profile,
        "enabled": enabled,
        "target_alert_count": target_alert_count,
        "alert_band": alert_band,
        "score_step": score_step,
        "min_bound": min_bound,
        "max_bound": max_bound,
        "current_min_score": current_min_score,
    }


def adaptive_scheduler_config(policy_name: Optional[str] = None) -> Dict[str, object]:
    with SCHEDULER_LOCK:
        cfg = _adaptive_runtime_config_unlocked(policy_name=policy_name)
    return {
        "policy_name": str(cfg.get("policy_name", "")),
        "policy_profile": dict(cfg.get("policy_profile", {})),
        "enabled": bool(cfg["enabled"]),
        "target_alert_count": int(cfg["target_alert_count"]),
        "alert_band": int(cfg["alert_band"]),
        "score_step": int(cfg["score_step"]),
        "min_bound": int(cfg["min_bound"]),
        "max_bound": int(cfg["max_bound"]),
        "current_min_score": int(cfg["current_min_score"]),
    }


def effective_scheduler_min_score(policy_name: Optional[str] = None) -> int:
    with SCHEDULER_LOCK:
        cfg = _adaptive_runtime_config_unlocked(policy_name=policy_name)
    if not bool(cfg["enabled"]):
        return MONITORING_SCHEDULER_MIN_SCORE
    return int(cfg["current_min_score"])


def apply_adaptive_min_score(result_count: int, policy_name: str) -> Dict[str, object]:
    with SCHEDULER_LOCK:
        cfg = _adaptive_runtime_config_unlocked(policy_name=policy_name)

    current = int(cfg["current_min_score"])
    normalized_policy_name = str(cfg.get("policy_name", "") or policy_name)
    if not bool(cfg["enabled"]):
        _set_scheduler_state(
            adaptive_enabled=False,
            adaptive_current_min_score=current,
            effective_min_score=MONITORING_SCHEDULER_MIN_SCORE,
            adaptive_last_direction="hold",
            adaptive_last_reason=f"adaptive_disabled;policy={normalized_policy_name}",
            adaptive_last_adjustment=now_str(),
        )
        return {
            "enabled": False,
            "policy_name": normalized_policy_name,
            "direction": "hold",
            "old_min_score": current,
            "new_min_score": current,
            "reason": "adaptive_disabled",
        }

    target = int(cfg["target_alert_count"])
    band = int(cfg["alert_band"])
    step = int(cfg["score_step"])
    min_bound = int(cfg["min_bound"])
    max_bound = int(cfg["max_bound"])
    high_threshold = target + band
    low_threshold = max(0, target - band)
    direction = "hold"
    reason = "within_band"
    new_score = current

    if result_count > high_threshold:
        new_score = min(max_bound, current + step)
        direction = "up"
        reason = f"alerts_above_target:{result_count}>{high_threshold}"
    elif result_count < low_threshold:
        new_score = max(min_bound, current - step)
        direction = "down"
        reason = f"alerts_below_target:{result_count}<{low_threshold}"

    adjusted_at = now_str()
    _set_scheduler_state(
        adaptive_enabled=True,
        adaptive_current_min_score=new_score,
        effective_min_score=new_score,
        adaptive_last_direction=direction,
        adaptive_last_reason=f"{reason};policy={normalized_policy_name}",
        adaptive_last_adjustment=adjusted_at,
    )
    return {
        "enabled": True,
        "policy_name": normalized_policy_name,
        "direction": direction,
        "old_min_score": current,
        "new_min_score": new_score,
        "target_alert_count": target,
        "alert_band": band,
        "reason": reason,
        "adjusted_at": adjusted_at,
    }


def update_adaptive_scheduler_config(payload: MonitoringAdaptiveUpdateRequest) -> Dict[str, object]:
    with SCHEDULER_LOCK:
        cfg = _adaptive_runtime_config_unlocked()
        enabled = bool(cfg["enabled"]) if payload.enabled is None else bool(payload.enabled)
        target_alert_count = int(cfg["target_alert_count"])
        alert_band = int(cfg["alert_band"])
        score_step = int(cfg["score_step"])
        min_bound = int(cfg["min_bound"])
        max_bound = int(cfg["max_bound"])

        if payload.target_alert_count is not None:
            target_alert_count = _bounded_int(payload.target_alert_count, target_alert_count, 0, 100)
        if payload.alert_band is not None:
            alert_band = _bounded_int(payload.alert_band, alert_band, 0, 20)
        if payload.score_step is not None:
            score_step = _bounded_int(payload.score_step, score_step, 1, 20)
        if payload.min_bound is not None:
            min_bound = _bounded_int(payload.min_bound, min_bound, 0, 100)
        if payload.max_bound is not None:
            max_bound = _bounded_int(payload.max_bound, max_bound, 0, 100)
        if min_bound > max_bound:
            min_bound, max_bound = max_bound, min_bound

        if payload.reset_current_min_score:
            current_min_score = _bounded_int(
                MONITORING_SCHEDULER_MIN_SCORE,
                default=MONITORING_SCHEDULER_MIN_SCORE,
                min_value=min_bound,
                max_value=max_bound,
            )
        else:
            current_min_score = _bounded_int(
                cfg["current_min_score"],
                default=MONITORING_SCHEDULER_MIN_SCORE,
                min_value=min_bound,
                max_value=max_bound,
            )

        active_policy_name = str(SCHEDULER_STATE.get("active_policy_name", "")).strip()
        if active_policy_name not in MONITORING_POLICY_NAMES:
            active_policy_name = str(current_monitoring_profile().get("name", "")).strip()

        SCHEDULER_STATE["adaptive_enabled"] = enabled
        SCHEDULER_STATE["adaptive_target_alert_count"] = target_alert_count
        SCHEDULER_STATE["adaptive_alert_band"] = alert_band
        SCHEDULER_STATE["adaptive_score_step"] = score_step
        SCHEDULER_STATE["adaptive_min_bound"] = min_bound
        SCHEDULER_STATE["adaptive_max_bound"] = max_bound
        SCHEDULER_STATE["adaptive_current_min_score"] = current_min_score
        effective_cfg = _adaptive_runtime_config_unlocked(policy_name=active_policy_name)
        SCHEDULER_STATE["effective_min_score"] = (
            int(effective_cfg["current_min_score"])
            if bool(effective_cfg["enabled"])
            else MONITORING_SCHEDULER_MIN_SCORE
        )
        SCHEDULER_STATE["adaptive_last_direction"] = "config_update"
        SCHEDULER_STATE["adaptive_last_reason"] = f"manual_config_update;policy={active_policy_name}"
        SCHEDULER_STATE["adaptive_last_adjustment"] = now_str()

    snapshot = scheduler_status_snapshot()
    policy_name = str(current_monitoring_profile().get("name", ""))
    return {
        "config": adaptive_scheduler_config(),
        "current_policy_config": adaptive_scheduler_config(policy_name=policy_name),
        "policy_profiles": adaptive_scheduler_profiles(),
        "state": {
            "enabled": bool(snapshot.get("adaptive_enabled", False)),
            "effective_min_score": int(snapshot.get("effective_min_score", MONITORING_SCHEDULER_MIN_SCORE)),
            "adaptive_current_min_score": int(
                snapshot.get("adaptive_current_min_score", MONITORING_SCHEDULER_MIN_SCORE)
            ),
            "last_direction": str(snapshot.get("adaptive_last_direction", "hold")),
            "last_reason": str(snapshot.get("adaptive_last_reason", "")),
            "last_adjustment": str(snapshot.get("adaptive_last_adjustment", "")),
        },
    }


def update_adaptive_profile_config(payload: MonitoringAdaptiveProfileUpdateRequest) -> Dict[str, object]:
    policy_name = str(payload.policy_name).strip()
    if policy_name not in MONITORING_POLICY_NAMES:
        raise HTTPException(status_code=400, detail=f"Unknown policy_name: {policy_name}")

    with SCHEDULER_LOCK:
        profiles = _sanitize_adaptive_profiles_unlocked()
        profile = dict(profiles.get(policy_name, {}))
        default_profile = dict(DEFAULT_ADAPTIVE_POLICY_OVERRIDES.get(policy_name, {}))

        if payload.clear_profile:
            profile = default_profile
        else:
            if payload.enabled is not None:
                profile["enabled"] = bool(payload.enabled)
            if payload.target_alert_count is not None:
                profile["target_alert_count"] = _bounded_int(payload.target_alert_count, 0, 0, 100)
            if payload.alert_band is not None:
                profile["alert_band"] = _bounded_int(payload.alert_band, 0, 0, 20)
            if payload.score_step is not None:
                profile["score_step"] = _bounded_int(payload.score_step, 5, 1, 20)
            if payload.min_bound is not None:
                profile["min_bound"] = _bounded_int(payload.min_bound, 0, 0, 100)
            if payload.max_bound is not None:
                profile["max_bound"] = _bounded_int(payload.max_bound, 80, 0, 100)

            min_bound = profile.get("min_bound")
            max_bound = profile.get("max_bound")
            if isinstance(min_bound, int) and isinstance(max_bound, int) and min_bound > max_bound:
                profile["min_bound"], profile["max_bound"] = max_bound, min_bound

        profiles[policy_name] = profile
        SCHEDULER_STATE["adaptive_profiles"] = {k: dict(v) for k, v in profiles.items()}

        active_policy_name = str(SCHEDULER_STATE.get("active_policy_name", "")).strip()
        if active_policy_name not in MONITORING_POLICY_NAMES:
            active_policy_name = str(current_monitoring_profile().get("name", "")).strip()

        if policy_name == active_policy_name:
            effective_cfg = _adaptive_runtime_config_unlocked(policy_name=active_policy_name)
            SCHEDULER_STATE["adaptive_current_min_score"] = int(effective_cfg["current_min_score"])
            SCHEDULER_STATE["effective_min_score"] = (
                int(effective_cfg["current_min_score"])
                if bool(effective_cfg["enabled"])
                else MONITORING_SCHEDULER_MIN_SCORE
            )

        SCHEDULER_STATE["adaptive_last_direction"] = "profile_update"
        SCHEDULER_STATE["adaptive_last_reason"] = f"manual_profile_update;policy={policy_name}"
        SCHEDULER_STATE["adaptive_last_adjustment"] = now_str()

    snapshot = scheduler_status_snapshot()
    return {
        "policy_name": policy_name,
        "policy_profile": dict(adaptive_scheduler_profiles().get(policy_name, {})),
        "policy_config": adaptive_scheduler_config(policy_name=policy_name),
        "config": adaptive_scheduler_config(),
        "policy_profiles": adaptive_scheduler_profiles(),
        "state": {
            "enabled": bool(snapshot.get("adaptive_enabled", False)),
            "effective_min_score": int(snapshot.get("effective_min_score", MONITORING_SCHEDULER_MIN_SCORE)),
            "adaptive_current_min_score": int(
                snapshot.get("adaptive_current_min_score", MONITORING_SCHEDULER_MIN_SCORE)
            ),
            "last_direction": str(snapshot.get("adaptive_last_direction", "hold")),
            "last_reason": str(snapshot.get("adaptive_last_reason", "")),
            "last_adjustment": str(snapshot.get("adaptive_last_adjustment", "")),
        },
    }


def reset_adaptive_min_score() -> Dict[str, object]:
    with SCHEDULER_LOCK:
        policy_name = str(SCHEDULER_STATE.get("active_policy_name", "")).strip()
        if policy_name not in MONITORING_POLICY_NAMES:
            policy_name = str(current_monitoring_profile().get("name", "")).strip()
        cfg = _adaptive_runtime_config_unlocked(policy_name=policy_name)
        min_bound = int(cfg["min_bound"])
        max_bound = int(cfg["max_bound"])
        enabled = bool(cfg["enabled"])

    reset_to = _bounded_int(
        MONITORING_SCHEDULER_MIN_SCORE,
        default=0,
        min_value=min_bound,
        max_value=max_bound,
    )
    _set_scheduler_state(
        adaptive_enabled=enabled,
        adaptive_current_min_score=reset_to,
        effective_min_score=reset_to if enabled else MONITORING_SCHEDULER_MIN_SCORE,
        adaptive_last_direction="reset",
        adaptive_last_reason=f"manual_reset;policy={policy_name}",
        adaptive_last_adjustment=now_str(),
    )
    return {
        "enabled": enabled,
        "policy_name": policy_name,
        "reset_to_min_score": reset_to,
        "adjusted_at": now_str(),
    }


def run_monitoring_cycle_once(trigger: str = "manual") -> Dict[str, object]:
    started_ts = time.time()
    started_at = now_str()
    profile = current_monitoring_profile()
    policy_name = str(profile.get("name", ""))
    effective_min_score = effective_scheduler_min_score(policy_name=policy_name)
    _set_scheduler_state(last_trigger=trigger, last_run_started_at=started_at)

    try:
        result = get_alerts(
            priority=None,
            delivery_level_filter=None,
            min_score=effective_min_score,
            limit=MONITORING_SCHEDULER_ALERT_LIMIT,
            news_fetch_limit=ALERT_SCORING_FETCH_LIMIT_DEFAULT,
            news_preview_limit=ALERT_NEWS_PREVIEW_LIMIT_DEFAULT,
        )
        count = int(result.get("count", 0))
        avg_score = float(result.get("summary", {}).get("average_score", 0.0))
        adaptive = apply_adaptive_min_score(result_count=count, policy_name=policy_name)
        finished_at = now_str()
        duration_ms = round((time.time() - started_ts) * 1000.0, 2)
        with SCHEDULER_LOCK:
            SCHEDULER_STATE["run_total"] = int(SCHEDULER_STATE.get("run_total", 0)) + 1
            SCHEDULER_STATE["success_total"] = int(SCHEDULER_STATE.get("success_total", 0)) + 1
            SCHEDULER_STATE["last_error"] = ""
            SCHEDULER_STATE["last_result_count"] = count
            SCHEDULER_STATE["last_alert_average_score"] = avg_score
            SCHEDULER_STATE["last_run_duration_ms"] = duration_ms
            SCHEDULER_STATE["effective_min_score"] = (
                int(adaptive.get("new_min_score", effective_min_score))
                if bool(adaptive.get("enabled", False))
                else MONITORING_SCHEDULER_MIN_SCORE
            )
            SCHEDULER_STATE["last_run_finished_at"] = finished_at
        _record_scheduler_run(
            {
                "status": "success",
                "trigger": trigger,
                "policy_name": policy_name,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_ms": duration_ms,
                "result_count": count,
                "average_score": avg_score,
                "effective_min_score": effective_min_score,
                "adaptive": adaptive,
            }
        )
        return {
            "success": True,
            "trigger": trigger,
            "policy_name": policy_name,
            "count": count,
            "average_score": avg_score,
            "duration_ms": duration_ms,
            "effective_min_score": effective_min_score,
            "adaptive": adaptive,
            "generated_at": result.get("generated_at", finished_at),
        }
    except Exception as e:
        finished_at = now_str()
        duration_ms = round((time.time() - started_ts) * 1000.0, 2)
        with SCHEDULER_LOCK:
            SCHEDULER_STATE["run_total"] = int(SCHEDULER_STATE.get("run_total", 0)) + 1
            SCHEDULER_STATE["error_total"] = int(SCHEDULER_STATE.get("error_total", 0)) + 1
            SCHEDULER_STATE["last_error"] = str(e)
            SCHEDULER_STATE["last_run_duration_ms"] = duration_ms
            SCHEDULER_STATE["effective_min_score"] = effective_min_score
            SCHEDULER_STATE["last_run_finished_at"] = finished_at
        _record_scheduler_run(
            {
                "status": "error",
                "trigger": trigger,
                "policy_name": policy_name,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_ms": duration_ms,
                "result_count": 0,
                "average_score": 0.0,
                "effective_min_score": effective_min_score,
                "error": str(e),
            }
        )
        raise


def _monitoring_scheduler_worker():
    while not SCHEDULER_STOP_EVENT.is_set():
        profile = current_monitoring_profile()
        interval_sec = int(profile.get("interval_sec", 60))
        _set_scheduler_state(
            active_policy_name=str(profile.get("name", "")),
            next_interval_sec=interval_sec,
        )

        try:
            run_monitoring_cycle_once(trigger="scheduler")
        except Exception:
            # Keep scheduler loop alive across transient crawler/API failures.
            pass

        if SCHEDULER_STOP_EVENT.wait(interval_sec):
            break

    _set_scheduler_state(
        running=False,
        next_interval_sec=0,
        active_policy_name="",
    )


def start_monitoring_scheduler(force_restart: bool = False) -> Dict[str, object]:
    global SCHEDULER_THREAD
    with SCHEDULER_LOCK:
        already_running = bool(SCHEDULER_THREAD and SCHEDULER_THREAD.is_alive())
    if already_running and not force_restart:
        snapshot = scheduler_status_snapshot()
        snapshot["message"] = "Scheduler already running"
        return snapshot

    if force_restart:
        stop_monitoring_scheduler()

    SCHEDULER_STOP_EVENT.clear()
    thread = threading.Thread(
        target=_monitoring_scheduler_worker,
        name="signalwatch-monitoring-scheduler",
        daemon=True,
    )
    thread.start()
    SCHEDULER_THREAD = thread
    _set_scheduler_state(running=True)
    snapshot = scheduler_status_snapshot()
    snapshot["message"] = "Scheduler started"
    return snapshot


def stop_monitoring_scheduler() -> Dict[str, object]:
    global SCHEDULER_THREAD
    SCHEDULER_STOP_EVENT.set()
    thread = SCHEDULER_THREAD
    if thread and thread.is_alive():
        thread.join(timeout=2.0)
    SCHEDULER_THREAD = None
    _set_scheduler_state(running=False, next_interval_sec=0, active_policy_name="")
    snapshot = scheduler_status_snapshot()
    snapshot["message"] = "Scheduler stopped"
    return snapshot


def analyze_title_sentiment(title: str) -> Dict[str, object]:
    text = title.strip()
    positive_hits = sorted({kw for kw in POSITIVE_KEYWORDS if kw in text})
    negative_hits = sorted({kw for kw in NEGATIVE_KEYWORDS if kw in text})
    score = len(positive_hits) - len(negative_hits)

    feedback_positive_hits: List[str] = []
    feedback_negative_hits: List[str] = []
    feedback_neutral_hits: List[str] = []
    for keyword, rule in feedback_store.match_applied_rules(text):
        label = str(rule.get("label", "neutral"))
        if label == "positive":
            feedback_positive_hits.append(keyword)
            score += FEEDBACK_RULE_SCORE_BOOST
        elif label == "negative":
            feedback_negative_hits.append(keyword)
            score -= FEEDBACK_RULE_SCORE_BOOST
        else:
            feedback_neutral_hits.append(keyword)

    if score > 0:
        label = "positive"
    elif score < 0:
        label = "negative"
    else:
        label = "neutral"

    return {
        "label": label,
        "score": score,
        "keywords": {
            "positive": positive_hits,
            "negative": negative_hits,
            "feedback_rules": {
                "positive": sorted(set(feedback_positive_hits)),
                "negative": sorted(set(feedback_negative_hits)),
                "neutral": sorted(set(feedback_neutral_hits)),
            },
        },
    }


def enrich_news_with_sentiment(news_list: List[Dict[str, object]]) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    enriched: List[Dict[str, object]] = []
    label_counter: Counter = Counter()

    for item in news_list:
        sentiment = analyze_title_sentiment(str(item.get("title", "")))
        new_item = dict(item)
        new_item["sentiment_label"] = sentiment["label"]
        new_item["sentiment_score"] = sentiment["score"]
        new_item["sentiment_keywords"] = sentiment["keywords"]
        enriched.append(new_item)
        label_counter[sentiment["label"]] += 1

    positive = label_counter["positive"]
    negative = label_counter["negative"]
    neutral = label_counter["neutral"]

    if positive > negative:
        dominant = "positive"
    elif negative > positive:
        dominant = "negative"
    else:
        dominant = "neutral"

    summary = {
        "positive": positive,
        "negative": negative,
        "neutral": neutral,
        "dominant": dominant,
        "total": len(enriched),
    }
    return enriched, summary


def dominant_label_ko(dominant: str) -> str:
    return {
        "positive": "\ud638\uc7ac \uc6b0\uc138",
        "negative": "\uc545\uc7ac \uc6b0\uc138",
        "neutral": "\uc911\ub9bd",
    }.get(dominant, "\uc911\ub9bd")


def delivery_level(score: int) -> str:
    if score >= 70:
        return "push_immediate"
    if score >= 40:
        return "in_app"
    return "daily_digest"


def delivery_level_ko(level: str) -> str:
    return {
        "push_immediate": "\uc989\uc2dc \ud478\uc2dc",
        "in_app": "\uc571 \ub0b4 \uc54c\ub9bc",
        "daily_digest": "\uc77c\uc77c \uc694\uc57d",
    }.get(level, "\uc77c\uc77c \uc694\uc57d")


def priority_from_level(level: str) -> str:
    return {
        "push_immediate": "high",
        "in_app": "medium",
        "daily_digest": "low",
    }.get(level, "low")


def topic_key_from_title(title: str) -> str:
    text = str(title or "").lower()
    text = re.sub(r"\[[^\]]*\]", " ", text)
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[\"'“”‘’`·…,:;!?/\\|]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def unique_news_by_topic(news_list: List[Dict[str, object]]) -> List[Dict[str, object]]:
    unique: List[Dict[str, object]] = []
    seen = set()
    for item in news_list:
        key = topic_key_from_title(str(item.get("title", "")))
        if not key:
            key = str(item.get("title", "")).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def calculate_importance_score(
    stock_code: str,
    news_list: List[Dict[str, object]],
    sentiment_summary: Dict[str, object],
    feedback_adjustment: Optional[Dict[str, object]] = None,
    update_history: bool = True,
) -> Dict[str, object]:
    article_count = len(news_list)
    unique_news = unique_news_by_topic(news_list)
    unique_topic_count = len(unique_news)
    topic_ratio = (unique_topic_count / article_count) if article_count > 0 else 0.0
    effective_count = unique_topic_count
    history = NEWS_COUNT_HISTORY[stock_code]

    average_count = sum(history) / len(history) if history else max(1, effective_count)
    surge_ratio = (effective_count / average_count) if average_count > 0 else 1.0

    score = 0
    reasons: List[str] = []

    # 1) News surge score
    if effective_count >= 6 and surge_ratio >= 3.0:
        score += 30
        reasons.append(f"\ub274\uc2a4 \uae09\uc99d({surge_ratio:.1f}x)")
    elif effective_count >= 10:
        score += 20
        reasons.append("\ub274\uc2a4 \ub7c9 \ub9ce\uc74c(\ud1a0\ud53d 10\uac74 \uc774\uc0c1)")

    # 2) Multi-source reporting score
    source_count = len(
        {
            str(item.get("source", "")).strip()
            for item in news_list
            if str(item.get("source", "")).strip()
        }
    )
    if source_count >= 5:
        score += 20
        reasons.append(f"\ub2e4\uc911 \uc5b8\ub860\uc0ac({source_count}\uac1c)")
    elif source_count >= 3:
        score += 10
        reasons.append(f"\ub2e4\uc911 \uc5b8\ub860\uc0ac({source_count}\uac1c)")

    # 3) High-impact keyword score
    impact_score = 0
    impact_positive_hits = set()
    impact_negative_hits = set()
    for item in unique_news:
        title = str(item.get("title", ""))
        for kw, weight in IMPACT_POSITIVE_KEYWORDS.items():
            if kw in title:
                impact_score += weight
                impact_positive_hits.add(kw)
        for kw, weight in IMPACT_NEGATIVE_KEYWORDS.items():
            if kw in title:
                impact_score += weight
                impact_negative_hits.add(kw)

    impact_score = min(30, impact_score)
    if impact_score > 0:
        score += impact_score
        reasons.append(f"\ud575\uc2ec \ud0a4\uc6cc\ub4dc({impact_score}\uc810)")

    # 4) Polarity concentration bonus
    unique_negative_count = len([item for item in unique_news if str(item.get("sentiment_label", "neutral")) == "negative"])
    unique_positive_count = len([item for item in unique_news if str(item.get("sentiment_label", "neutral")) == "positive"])

    if unique_negative_count >= 4:
        score += 10
        reasons.append("\uc545\uc7ac \uae30\uc0ac \uc9d1\uc911")
    elif unique_positive_count >= 5:
        score += 5
        reasons.append("\ud638\uc7ac \uae30\uc0ac \uc9d1\uc911")

    # 5) Duplicate-topic penalty (syndicated copies should not overinflate score)
    if article_count >= 10:
        if topic_ratio < 0.35:
            score -= 20
            reasons.append("\uc911\ubcf5 \uae30\uc0ac \ube44\uc911 \ub192\uc74c")
        elif topic_ratio < 0.5:
            score -= 10
            reasons.append("\uc911\ubcf5 \uae30\uc0ac \ub2e4\uc218")

    feedback_meta = {
        "score_delta": 0,
        "ready": False,
        "total_votes": 0,
        "consensus_label": "neutral",
        "consensus_ratio": 0.0,
        "ai_match_ratio": 0.0,
    }
    if feedback_adjustment:
        delta = int(feedback_adjustment.get("score_delta", 0))
        if delta != 0:
            score += delta
        feedback_reasons = [str(item) for item in feedback_adjustment.get("reasons", []) if str(item)]
        reasons.extend(feedback_reasons)

        signal = dict(feedback_adjustment.get("signal", {}))
        feedback_meta = {
            "score_delta": delta,
            "ready": bool(signal.get("ready", False)),
            "total_votes": int(signal.get("total_votes", 0)),
            "consensus_label": str(signal.get("consensus_label", "neutral")),
            "consensus_ratio": float(signal.get("consensus_ratio", 0.0)),
            "ai_match_ratio": float(signal.get("ai_match_ratio", 0.0)),
        }

    score = max(0, min(100, score))
    level = delivery_level(score)

    if update_history:
        history.append(effective_count)

    return {
        "score": score,
        "delivery_level": level,
        "delivery_level_ko": delivery_level_ko(level),
        "priority": priority_from_level(level),
        "reasons": reasons,
        "metrics": {
            "article_count": article_count,
            "effective_count": effective_count,
            "unique_topic_count": unique_topic_count,
            "topic_ratio": round(topic_ratio, 4),
            "average_count": round(average_count, 2),
            "surge_ratio": round(surge_ratio, 2),
            "source_count": source_count,
            "unique_positive_count": unique_positive_count,
            "unique_negative_count": unique_negative_count,
            "impact_positive_keywords": sorted(impact_positive_hits),
            "impact_negative_keywords": sorted(impact_negative_hits),
            "feedback": feedback_meta,
        },
    }


def build_alert_payload(
    stock_code: str,
    stock_name: str,
    enriched_news: List[Dict[str, object]],
    sentiment_summary: Dict[str, object],
    update_history: bool,
    feedback_adjustment: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    scoring = calculate_importance_score(
        stock_code=stock_code,
        news_list=enriched_news,
        sentiment_summary=sentiment_summary,
        feedback_adjustment=feedback_adjustment,
        update_history=update_history,
    )
    dominant = sentiment_summary["dominant"]
    dominant_ko = dominant_label_ko(dominant)
    article_count = len(enriched_news)

    return {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "article_count": article_count,
        "importance_score": scoring["score"],
        "delivery_level": scoring["delivery_level"],
        "delivery_level_ko": scoring["delivery_level_ko"],
        "priority": scoring["priority"],
        "score_reasons": scoring["reasons"],
        "score_metrics": scoring["metrics"],
        "sentiment": dominant,
        "sentiment_ko": dominant_ko,
        "sentiment_summary": sentiment_summary,
        "title": f"{stock_name} \ub274\uc2a4 {article_count}\uac74 ({dominant_ko})",
        "summary": (
            f"\ud638\uc7ac {sentiment_summary['positive']}\uac74 / "
            f"\uc545\uc7ac {sentiment_summary['negative']}\uac74 / "
            f"\uc911\ub9bd {sentiment_summary['neutral']}\uac74"
        ),
    }


@app.get("/")
def root():
    current_policy = current_monitoring_profile()
    return {
        "service": "SignalWatch Stock Alert Server",
        "status": "running",
        "version": "1.24.0",
        "description": "Real-time stock news monitoring and alerts",
        "timestamp": now_str(),
        "current_monitoring_policy": current_policy,
        "endpoints": {
            "health": "/api/health",
            "monitoring_policy": "/api/monitoring-policy",
            "news": "/api/news/{stock_code}",
            "alerts": "/api/alerts",
            "alerts_history": "/api/alerts/history",
            "alerts_history_export": "/api/alerts/history/export",
            "monitoring_scheduler_status": "/api/monitoring/scheduler",
            "monitoring_scheduler_runs": "/api/monitoring/scheduler/runs",
            "monitoring_scheduler_adaptive": "/api/monitoring/scheduler/adaptive",
            "monitoring_scheduler_adaptive_update": "/api/monitoring/scheduler/adaptive",
            "monitoring_scheduler_adaptive_profiles": "/api/monitoring/scheduler/adaptive/profiles",
            "monitoring_scheduler_adaptive_profiles_update": "/api/monitoring/scheduler/adaptive/profiles",
            "monitoring_scheduler_adaptive_reset": "/api/monitoring/scheduler/adaptive/reset",
            "monitoring_scheduler_start": "/api/monitoring/scheduler/start",
            "monitoring_scheduler_stop": "/api/monitoring/scheduler/stop",
            "monitoring_run_once": "/api/monitoring/run-once",
            "watchlist": "/api/watchlist",
            "multiple_news": "/api/multiple-news",
            "feedback_submit": "/api/feedback/article",
            "feedback_article_summary": "/api/feedback/article-summary",
            "feedback_keyword_candidates": "/api/feedback/keyword-candidates",
            "feedback_keyword_rules": "/api/feedback/keyword-rules",
            "feedback_user_trust_get": "/api/feedback/user-trust?user_id={id}",
            "feedback_user_trust_set": "/api/feedback/user-trust",
            "feedback_user_trust_reset": "/api/feedback/user-trust/reset",
            "feedback_user_trust_list": "/api/feedback/user-trust/list",
            "feedback_user_tier_get": "/api/feedback/user-tier?user_id={id}",
            "feedback_user_tier_set": "/api/feedback/user-tier",
            "feedback_user_tier_list": "/api/feedback/user-tier/list",
            "feedback_tester_quality": "/api/feedback/tester-quality",
            "feedback_user_tier_auto_apply": "/api/feedback/user-tier/auto-apply",
            "admin_audit_logs": "/api/admin/audit-logs",
            "admin_alerts_prune_preview": "/api/admin/alerts/prune-preview",
            "admin_alerts_prune": "/api/admin/alerts/prune",
            "ops_metrics": "/api/metrics/ops",
            "docs": "/docs",
        },
    }


@app.get("/api/health")
def health_check():
    cfg = _admin_key_config()
    write_rl = _write_rate_limit_config()
    action_overrides = {action: _write_rate_limit_config(action=action) for action in ADMIN_WRITE_ACTIONS}
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "uptime": "server is running",
        "feedback_db_path": str(Path(__file__).resolve().parent / "signalwatch_feedback.db"),
        "alert_db_path": str(Path(__file__).resolve().parent / "signalwatch_alerts.db"),
        "admin_key_configured": bool(cfg["configured"]),
        "admin_auth": {
            "read_scope_configured": bool(cfg["read_key"]),
            "write_scope_configured": bool(cfg["write_key"]),
            "legacy_fallback_enabled": bool(cfg["legacy_key_enabled"]),
            "header": "X-Admin-Key",
            "env_keys": {
                "read": ADMIN_READ_KEY_ENV,
                "write": ADMIN_WRITE_KEY_ENV,
                "legacy": ADMIN_KEY_ENV,
            },
            "write_rate_limit_default": write_rl,
            "write_rate_limit_actions": action_overrides,
        },
        "tester_quality_defaults": {
            "min_votes": TESTER_QUALITY_MIN_VOTES_DEFAULT,
            "promote_threshold": TESTER_QUALITY_PROMOTE_THRESHOLD_DEFAULT,
            "demote_threshold": TESTER_QUALITY_DEMOTE_THRESHOLD_DEFAULT,
        },
        "alert_history_policy": {
            "retention_days": ALERT_HISTORY_RETENTION_DAYS,
            "max_rows": ALERT_HISTORY_MAX_ROWS,
        },
        "alert_scoring_defaults": {
            "scoring_fetch_limit": ALERT_SCORING_FETCH_LIMIT_DEFAULT,
            "news_preview_limit": ALERT_NEWS_PREVIEW_LIMIT_DEFAULT,
        },
        "monitoring_scheduler_defaults": {
            "autostart": MONITORING_SCHEDULER_AUTOSTART,
            "alert_limit": MONITORING_SCHEDULER_ALERT_LIMIT,
            "min_score": MONITORING_SCHEDULER_MIN_SCORE,
            "history_limit": MONITORING_SCHEDULER_HISTORY_LIMIT,
            "adaptive": adaptive_scheduler_config(),
            "adaptive_profiles": adaptive_scheduler_profiles(),
        },
        "feedback_score_adjustment": {
            "recent_hours": FEEDBACK_SCORE_RECENT_HOURS,
            "min_votes": FEEDBACK_SCORE_MIN_VOTES,
            "consensus_threshold": FEEDBACK_SCORE_CONSENSUS_THRESHOLD,
            "delta_consensus": FEEDBACK_SCORE_DELTA_CONSENSUS,
            "delta_ai_mismatch": FEEDBACK_SCORE_DELTA_AI_MISMATCH,
        },
        "monitoring_scheduler_status": scheduler_status_snapshot(),
    }


@app.get("/api/metrics/ops")
def get_ops_metrics(
    hours: int = Query(default=24, ge=1, le=720),
    scheduler_runs_limit: int = Query(default=20, ge=1, le=200),
    admin: Dict[str, str] = Depends(require_admin_read),
):
    try:
        resolved_scheduler_runs_limit = _bounded_int(
            value=scheduler_runs_limit,
            default=20,
            min_value=1,
            max_value=200,
        )
        crawler_metrics = search_crawler.get_runtime_metrics()
        feedback_metrics = feedback_store.get_metrics(since_hours=hours)
        alert_metrics = alert_store.get_metrics(since_hours=hours)
        history_metrics = collect_news_history_metrics()
        scheduler_status = scheduler_status_snapshot()
        scheduler_runs_memory = scheduler_run_history_snapshot(limit=resolved_scheduler_runs_limit)
        scheduler_runs_persistent = alert_store.list_monitoring_runs(
            limit=resolved_scheduler_runs_limit,
            since_hours=hours,
        )
        scheduler_metrics = alert_store.get_monitoring_run_metrics(since_hours=hours)
        current_policy_name = str(current_monitoring_profile().get("name", ""))

        return {
            "success": True,
            "period_hours": hours,
            "crawler": crawler_metrics,
            "feedback": feedback_metrics,
            "alerts": alert_metrics,
            "news_count_history": history_metrics,
            "monitoring_scheduler": {
                "status": scheduler_status,
                "recent_runs_limit": resolved_scheduler_runs_limit,
                "recent_runs_memory": scheduler_runs_memory,
                "recent_runs_persistent": scheduler_runs_persistent,
                "metrics": scheduler_metrics,
                "adaptive": {
                    "config": adaptive_scheduler_config(),
                    "current_policy_name": current_policy_name,
                    "current_policy_config": adaptive_scheduler_config(policy_name=current_policy_name),
                    "policy_profiles": adaptive_scheduler_profiles(),
                    "state": {
                        "enabled": bool(scheduler_status.get("adaptive_enabled", False)),
                        "effective_min_score": int(
                            scheduler_status.get("effective_min_score", MONITORING_SCHEDULER_MIN_SCORE)
                        ),
                        "adaptive_current_min_score": int(
                            scheduler_status.get("adaptive_current_min_score", MONITORING_SCHEDULER_MIN_SCORE)
                        ),
                        "last_direction": str(scheduler_status.get("adaptive_last_direction", "hold")),
                        "last_reason": str(scheduler_status.get("adaptive_last_reason", "")),
                    },
                },
            },
            "auth_mode": admin.get("auth_mode", "disabled"),
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to collect operational metrics: {e}")


@app.get("/api/monitoring-policy")
def get_monitoring_policy():
    current_policy = current_monitoring_profile()
    return {
        "success": True,
        "current": current_policy,
        "policy": MONITORING_POLICY,
        "generated_at": now_str(),
    }


@app.get("/api/monitoring/scheduler")
def get_monitoring_scheduler_status(
    runs_limit: int = Query(default=20, ge=1, le=200),
    admin: Dict[str, str] = Depends(require_admin_read),
):
    resolved_runs_limit = _bounded_int(
        value=runs_limit,
        default=20,
        min_value=1,
        max_value=200,
    )
    snapshot = scheduler_status_snapshot()
    memory_runs = scheduler_run_history_snapshot(limit=resolved_runs_limit)
    persistent_runs = alert_store.list_monitoring_runs(limit=resolved_runs_limit)
    return {
        "success": True,
        "scheduler": snapshot,
        "recent_runs_limit": resolved_runs_limit,
        "recent_runs": memory_runs,
        "recent_runs_memory": memory_runs,
        "recent_runs_persistent": persistent_runs,
        "auth_mode": admin.get("auth_mode", "disabled"),
        "generated_at": now_str(),
    }


@app.get("/api/monitoring/scheduler/adaptive")
def get_monitoring_scheduler_adaptive(
    admin: Dict[str, str] = Depends(require_admin_read),
):
    snapshot = scheduler_status_snapshot()
    current_policy_name = str(current_monitoring_profile().get("name", ""))
    return {
        "success": True,
        "adaptive": {
            "config": adaptive_scheduler_config(),
            "current_policy_name": current_policy_name,
            "current_policy_config": adaptive_scheduler_config(policy_name=current_policy_name),
            "policy_profiles": adaptive_scheduler_profiles(),
            "state": {
                "enabled": bool(snapshot.get("adaptive_enabled", False)),
                "effective_min_score": int(snapshot.get("effective_min_score", MONITORING_SCHEDULER_MIN_SCORE)),
                "adaptive_current_min_score": int(
                    snapshot.get("adaptive_current_min_score", MONITORING_SCHEDULER_MIN_SCORE)
                ),
                "last_direction": str(snapshot.get("adaptive_last_direction", "hold")),
                "last_reason": str(snapshot.get("adaptive_last_reason", "")),
                "last_adjustment": str(snapshot.get("adaptive_last_adjustment", "")),
            },
        },
        "auth_mode": admin.get("auth_mode", "disabled"),
        "generated_at": now_str(),
    }


@app.post("/api/monitoring/scheduler/adaptive")
def update_monitoring_scheduler_adaptive(
    payload: MonitoringAdaptiveUpdateRequest,
    admin: Dict[str, str] = Depends(require_admin_write),
):
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "monitoring_adaptive_update")
        result = update_adaptive_scheduler_config(payload)
        log_admin_action_safe(
            action="monitoring_adaptive_update",
            target_type="system",
            target_id="monitoring_scheduler",
            meta={
                "payload": payload.model_dump() if hasattr(payload, "model_dump") else payload.dict(),
                "result": result,
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "result": result,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update adaptive scheduler config: {e}")


@app.get("/api/monitoring/scheduler/adaptive/profiles")
def get_monitoring_scheduler_adaptive_profiles(
    admin: Dict[str, str] = Depends(require_admin_read),
):
    current_policy_name = str(current_monitoring_profile().get("name", ""))
    return {
        "success": True,
        "current_policy_name": current_policy_name,
        "profiles": adaptive_scheduler_profiles(),
        "current_policy_config": adaptive_scheduler_config(policy_name=current_policy_name),
        "auth_mode": admin.get("auth_mode", "disabled"),
        "generated_at": now_str(),
    }


@app.post("/api/monitoring/scheduler/adaptive/profiles")
def update_monitoring_scheduler_adaptive_profiles(
    payload: MonitoringAdaptiveProfileUpdateRequest,
    admin: Dict[str, str] = Depends(require_admin_write),
):
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "monitoring_adaptive_profile_update")
        result = update_adaptive_profile_config(payload)
        log_admin_action_safe(
            action="monitoring_adaptive_profile_update",
            target_type="system",
            target_id=f"monitoring_scheduler:{payload.policy_name}",
            meta={
                "payload": payload.model_dump() if hasattr(payload, "model_dump") else payload.dict(),
                "result": result,
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "result": result,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update adaptive scheduler profile: {e}")


@app.post("/api/monitoring/scheduler/adaptive/reset")
def reset_monitoring_scheduler_adaptive(
    admin: Dict[str, str] = Depends(require_admin_write),
):
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "monitoring_adaptive_reset")
        result = reset_adaptive_min_score()
        log_admin_action_safe(
            action="monitoring_adaptive_reset",
            target_type="system",
            target_id="monitoring_scheduler",
            meta={
                "result": result,
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "result": result,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reset adaptive scheduler min score: {e}")


@app.get("/api/monitoring/scheduler/runs")
def get_monitoring_scheduler_runs(
    limit: int = Query(default=50, ge=1, le=200),
    source: str = Query(default="auto", pattern="^(auto|memory|persistent)$"),
    admin: Dict[str, str] = Depends(require_admin_read),
):
    resolved_limit = _bounded_int(
        value=limit,
        default=50,
        min_value=1,
        max_value=200,
    )
    source_value = str(source).strip().lower()
    if source_value not in {"auto", "memory", "persistent"}:
        source_value = "auto"

    memory_runs = scheduler_run_history_snapshot(limit=resolved_limit)
    persistent_runs = alert_store.list_monitoring_runs(limit=resolved_limit)

    resolved_source = source_value
    if source_value == "auto":
        if memory_runs:
            runs = memory_runs
            resolved_source = "memory"
        else:
            runs = persistent_runs
            resolved_source = "persistent"
    elif source_value == "memory":
        runs = memory_runs
    else:
        runs = persistent_runs

    return {
        "success": True,
        "count": len(runs),
        "limit": resolved_limit,
        "source": resolved_source,
        "runs": runs,
        "memory_count": len(memory_runs),
        "persistent_count": len(persistent_runs),
        "auth_mode": admin.get("auth_mode", "disabled"),
        "generated_at": now_str(),
    }


@app.post("/api/monitoring/scheduler/start")
def start_monitoring_scheduler_endpoint(
    force_restart: bool = Query(default=False),
    admin: Dict[str, str] = Depends(require_admin_write),
):
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "monitoring_scheduler_start")
        result = start_monitoring_scheduler(force_restart=force_restart)
        log_admin_action_safe(
            action="monitoring_scheduler_start",
            target_type="system",
            target_id="monitoring_scheduler",
            meta={
                "force_restart": force_restart,
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "scheduler": result,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start monitoring scheduler: {e}")


@app.post("/api/monitoring/scheduler/stop")
def stop_monitoring_scheduler_endpoint(
    admin: Dict[str, str] = Depends(require_admin_write),
):
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "monitoring_scheduler_stop")
        result = stop_monitoring_scheduler()
        log_admin_action_safe(
            action="monitoring_scheduler_stop",
            target_type="system",
            target_id="monitoring_scheduler",
            meta={
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "scheduler": result,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop monitoring scheduler: {e}")


@app.post("/api/monitoring/run-once")
def run_monitoring_once_endpoint(
    admin: Dict[str, str] = Depends(require_admin_write),
):
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "monitoring_run_once")
        result = run_monitoring_cycle_once(trigger="manual_api")
        log_admin_action_safe(
            action="monitoring_run_once",
            target_type="system",
            target_id="monitoring_scheduler",
            meta={
                "result_count": result.get("count", 0),
                "average_score": result.get("average_score", 0.0),
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "result": result,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run monitoring cycle: {e}")


@app.get("/api/news/{stock_code}")
def get_stock_news(
    stock_code: str,
    limit: int = Query(default=10, ge=1, le=50, description="Number of news items"),
):
    keyword = stock_code_to_keyword(stock_code)
    try:
        print(f"[API 요청] {stock_code} -> '{keyword}' 뉴스 {limit}개 검색")
        news_list = search_crawler.get_news_by_keyword(keyword, limit)
        fetch_meta = normalize_fetch_meta(keyword)

        if not news_list:
            return {
                "success": False,
                "message": f"'{keyword}' 관련 뉴스를 찾을 수 없습니다.",
                "stock_code": stock_code,
                "keyword": keyword,
                "count": 0,
                "data": [],
                "sentiment_summary": {
                    "positive": 0,
                    "negative": 0,
                    "neutral": 0,
                    "dominant": "neutral",
                    "total": 0,
                },
                "alert_decision": None,
                "fetch_meta": fetch_meta,
            }

        enriched, sentiment_summary = enrich_news_with_sentiment(news_list)
        feedback_adjustment = calculate_feedback_score_adjustment(stock_code)
        alert_decision = build_alert_payload(
            stock_code=stock_code,
            stock_name=keyword,
            enriched_news=enriched,
            sentiment_summary=sentiment_summary,
            feedback_adjustment=feedback_adjustment,
            update_history=False,
        )

        return {
            "success": True,
            "stock_code": stock_code,
            "keyword": keyword,
            "count": len(enriched),
            "data": enriched,
            "sentiment_summary": sentiment_summary,
            "alert_decision": {
                "importance_score": alert_decision["importance_score"],
                "delivery_level": alert_decision["delivery_level"],
                "delivery_level_ko": alert_decision["delivery_level_ko"],
                "priority": alert_decision["priority"],
                "score_reasons": alert_decision["score_reasons"],
                "score_metrics": alert_decision["score_metrics"],
                "sentiment": alert_decision["sentiment"],
                "sentiment_ko": alert_decision["sentiment_ko"],
            },
            "fetch_meta": fetch_meta,
            "crawled_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"뉴스 조회 중 오류: {e}")


@app.get("/api/alerts")
def get_alerts(
    priority: Optional[str] = Query(default=None, description="high/medium/low"),
    delivery_level_filter: Optional[str] = Query(
        default=None,
        alias="delivery_level",
        description="push_immediate/in_app/daily_digest",
    ),
    min_score: int = Query(default=0, ge=0, le=100, description="Minimum importance score"),
    limit: int = Query(default=20, ge=1, le=100),
    news_fetch_limit: int = Query(
        default=ALERT_SCORING_FETCH_LIMIT_DEFAULT,
        ge=10,
        le=100,
        description="Per-stock fetch size used for scoring accuracy",
    ),
    news_preview_limit: int = Query(
        default=ALERT_NEWS_PREVIEW_LIMIT_DEFAULT,
        ge=1,
        le=20,
        description="Number of latest articles embedded in each alert payload",
    ),
):
    try:
        resolved_news_fetch_limit = _bounded_int(
            value=news_fetch_limit,
            default=ALERT_SCORING_FETCH_LIMIT_DEFAULT,
            min_value=10,
            max_value=100,
        )
        resolved_news_preview_limit = _bounded_int(
            value=news_preview_limit,
            default=ALERT_NEWS_PREVIEW_LIMIT_DEFAULT,
            min_value=1,
            max_value=20,
        )

        alerts: List[Dict[str, object]] = []
        for stock in DEFAULT_WATCHLIST:
            stock_code = stock["code"]
            stock_name = stock["name"]
            news_list = search_crawler.get_news_by_keyword(stock_name, resolved_news_fetch_limit)
            fetch_meta = normalize_fetch_meta(stock_name)
            if not news_list:
                continue

            enriched, sentiment_summary = enrich_news_with_sentiment(news_list)
            feedback_adjustment = calculate_feedback_score_adjustment(stock_code)
            alert = build_alert_payload(
                stock_code=stock_code,
                stock_name=stock_name,
                enriched_news=enriched,
                sentiment_summary=sentiment_summary,
                feedback_adjustment=feedback_adjustment,
                update_history=True,
            )
            alert["id"] = f"alert_{stock_code}_{int(datetime.now().timestamp())}"
            alert["created_at"] = now_str()
            alert["latest_news"] = enriched[:resolved_news_preview_limit]
            alert["fetch_meta"] = fetch_meta
            alert["news_fetch_limit"] = resolved_news_fetch_limit
            alert["news_preview_limit"] = resolved_news_preview_limit
            alerts.append(alert)

        if priority:
            alerts = [item for item in alerts if item["priority"] == priority]
        if delivery_level_filter:
            alerts = [item for item in alerts if item["delivery_level"] == delivery_level_filter]
        if min_score > 0:
            alerts = [item for item in alerts if item["importance_score"] >= min_score]

        alerts.sort(
            key=lambda item: (item.get("importance_score", 0), item.get("article_count", 0)),
            reverse=True,
        )

        summary_counter = Counter([item["delivery_level"] for item in alerts])
        source_counter = Counter([item.get("fetch_meta", {}).get("source", "unknown") for item in alerts])
        score_avg = round(sum([item["importance_score"] for item in alerts]) / len(alerts), 2) if alerts else 0.0
        generated_at = now_str()
        persisted_count = alert_store.save_alerts(alerts=alerts, created_at=generated_at)
        prune_result = alert_store.prune_history(
            retention_days=ALERT_HISTORY_RETENTION_DAYS,
            max_rows=ALERT_HISTORY_MAX_ROWS,
        )

        return {
            "success": True,
            "count": len(alerts),
            "alerts": alerts[:limit],
            "summary": {
                "push_immediate": summary_counter.get("push_immediate", 0),
                "in_app": summary_counter.get("in_app", 0),
                "daily_digest": summary_counter.get("daily_digest", 0),
                "average_score": score_avg,
                "scoring_fetch_limit": resolved_news_fetch_limit,
                "news_preview_limit": resolved_news_preview_limit,
                "data_source": {
                    "network": source_counter.get("network", 0),
                    "cache": source_counter.get("cache", 0),
                    "stale_cache": source_counter.get("stale_cache", 0),
                    "unknown": source_counter.get("unknown", 0),
                },
            },
            "persisted_count": persisted_count,
            "history_prune": prune_result,
            "generated_at": generated_at,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"알림 생성 중 오류: {e}")


@app.get("/api/alerts/history")
def get_alert_history(
    stock_code: Optional[str] = Query(default=None, min_length=1, max_length=20),
    delivery_level: Optional[str] = Query(default=None, pattern="^(push_immediate|in_app|daily_digest)$"),
    min_score: int = Query(default=0, ge=0, le=100),
    limit: int = Query(default=100, ge=1, le=1000),
):
    try:
        rows = alert_store.list_alert_history(
            limit=limit,
            stock_code=stock_code,
            delivery_level=delivery_level,
            min_score=min_score,
        )
        level_counter = Counter([str(row.get("delivery_level", "daily_digest")) for row in rows])
        score_avg = round(sum([int(row.get("importance_score", 0)) for row in rows]) / len(rows), 2) if rows else 0.0
        return {
            "success": True,
            "count": len(rows),
            "history": rows,
            "summary": {
                "push_immediate": level_counter.get("push_immediate", 0),
                "in_app": level_counter.get("in_app", 0),
                "daily_digest": level_counter.get("daily_digest", 0),
                "average_score": score_avg,
            },
            "filters": {
                "stock_code": stock_code,
                "delivery_level": delivery_level,
                "min_score": min_score,
                "limit": limit,
            },
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"알림 히스토리 조회 중 오류: {e}")


@app.get("/api/alerts/history/export")
def export_alert_history_csv(
    stock_code: Optional[str] = Query(default=None, min_length=1, max_length=20),
    delivery_level: Optional[str] = Query(default=None, pattern="^(push_immediate|in_app|daily_digest)$"),
    min_score: int = Query(default=0, ge=0, le=100),
    limit: int = Query(default=1000, ge=1, le=5000),
    admin: Dict[str, str] = Depends(require_admin_read),
):
    try:
        rows = alert_store.list_alert_history(
            limit=limit,
            stock_code=stock_code,
            delivery_level=delivery_level,
            min_score=min_score,
        )

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "id",
                "created_at",
                "stock_code",
                "stock_name",
                "importance_score",
                "delivery_level",
                "priority",
                "article_count",
                "sentiment",
                "summary",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.get("id"),
                    row.get("created_at"),
                    row.get("stock_code"),
                    row.get("stock_name"),
                    row.get("importance_score"),
                    row.get("delivery_level"),
                    row.get("priority"),
                    row.get("article_count"),
                    row.get("sentiment"),
                    row.get("summary"),
                ]
            )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"signalwatch_alert_history_{timestamp}.csv"
        return Response(
            content=output.getvalue(),
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-Auth-Mode": str(admin.get("auth_mode", "disabled")),
                "X-Auth-Scope": str(admin.get("auth_scope", "disabled")),
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"알림 히스토리 CSV 내보내기 중 오류: {e}")


@app.get("/api/watchlist")
def get_watchlist():
    return {
        "success": True,
        "watchlist": DEFAULT_WATCHLIST,
        "count": len(DEFAULT_WATCHLIST),
    }


@app.get("/api/multiple-news")
def get_multiple_news(
    codes: str = Query(..., description="Comma-separated stock codes"),
    limit_each: int = Query(default=5, ge=1, le=20),
):
    try:
        stock_codes = [code.strip() for code in codes.split(",") if code.strip()]
        if len(stock_codes) > 10:
            raise HTTPException(status_code=400, detail="한 번에 최대 10개 종목까지 조회 가능합니다.")

        results: Dict[str, List[Dict[str, object]]] = {}
        summaries: Dict[str, Dict[str, object]] = {}
        alert_decisions: Dict[str, Dict[str, object]] = {}

        for stock_code in stock_codes:
            keyword = stock_code_to_keyword(stock_code)
            news_list = search_crawler.get_news_by_keyword(keyword, limit_each)
            fetch_meta = normalize_fetch_meta(keyword)
            enriched, sentiment_summary = enrich_news_with_sentiment(news_list)
            results[stock_code] = enriched
            summaries[stock_code] = {"keyword": keyword, "fetch_meta": fetch_meta, **sentiment_summary}
            feedback_adjustment = calculate_feedback_score_adjustment(stock_code)

            decision = build_alert_payload(
                stock_code=stock_code,
                stock_name=keyword,
                enriched_news=enriched,
                sentiment_summary=sentiment_summary,
                feedback_adjustment=feedback_adjustment,
                update_history=False,
            )
            alert_decisions[stock_code] = {
                "importance_score": decision["importance_score"],
                "delivery_level": decision["delivery_level"],
                "delivery_level_ko": decision["delivery_level_ko"],
                "priority": decision["priority"],
                "score_reasons": decision["score_reasons"],
                "fetch_meta": fetch_meta,
            }

        return {
            "success": True,
            "requested_codes": stock_codes,
            "results": results,
            "summaries": summaries,
            "alert_decisions": alert_decisions,
            "crawled_at": now_str(),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"다중 뉴스 조회 중 오류: {e}")


@app.post("/api/feedback/article")
def submit_article_feedback(payload: ArticleFeedbackRequest):
    try:
        result = feedback_store.submit_feedback(
            user_id=payload.user_id,
            stock_code=payload.stock_code,
            article_link=str(payload.article_link),
            article_title=payload.article_title,
            article_source=payload.article_source,
            ai_label=payload.ai_label,
            user_label=payload.user_label,
            user_confidence=payload.user_confidence,
            note=payload.note,
        )
        consensus_evaluation = feedback_store.evaluate_article_consensus(
            article_link=str(payload.article_link),
            min_votes=FEEDBACK_CONSENSUS_MIN_VOTES,
            min_consensus_ratio=FEEDBACK_CONSENSUS_THRESHOLD,
        )
        return {
            "success": True,
            "feedback_id": result["feedback_id"],
            "vote_action": result.get("vote_action", "created"),
            "keyword_count": result["keyword_count"],
            "trust_weight": result.get("trust_weight", 1.0),
            "weighted_score": result.get("weighted_score", payload.user_confidence),
            "trust_source": result.get("trust_source", "system_default"),
            "tester_tier": result.get("tester_tier"),
            "article_summary": result["article_summary"],
            "consensus_evaluation": consensus_evaluation,
            "message": "Feedback upserted",
            "saved_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"피드백 저장 중 오류: {e}")


@app.get("/api/feedback/article-summary")
def get_feedback_article_summary(
    article_link: str = Query(..., min_length=5, max_length=2000),
    min_votes: int = Query(default=FEEDBACK_CONSENSUS_MIN_VOTES, ge=1, le=10000),
    min_consensus_ratio: float = Query(default=FEEDBACK_CONSENSUS_THRESHOLD, ge=0.5, le=1.0),
):
    try:
        summary = feedback_store.get_article_summary(article_link=article_link)
        consensus_evaluation = feedback_store.evaluate_article_consensus(
            article_link=article_link,
            min_votes=min_votes,
            min_consensus_ratio=min_consensus_ratio,
        )
        return {
            "success": True,
            "article_summary": summary,
            "consensus_evaluation": consensus_evaluation,
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"기사 피드백 요약 조회 중 오류: {e}")


@app.post("/api/feedback/user-trust")
def upsert_feedback_user_trust(
    payload: UserTrustUpdateRequest,
    admin: Dict[str, str] = Depends(require_admin_write),
):
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "upsert_user_trust")
        profile = feedback_store.upsert_user_trust_profile(
            user_id=payload.user_id,
            trust_weight=payload.trust_weight,
            note=payload.note,
        )
        log_admin_action_safe(
            action="upsert_user_trust",
            target_type="user",
            target_id=str(profile.get("user_id_hash", "")),
            meta={
                "trust_weight": profile.get("trust_weight"),
                "updated_feedback_count": profile.get("updated_feedback_count"),
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "profile": profile,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "message": "User trust weight updated",
            "saved_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"유저 신뢰도 저장 중 오류: {e}")


@app.get("/api/feedback/user-trust")
def get_feedback_user_trust(
    user_id: str = Query(..., min_length=3, max_length=128, description="Tester/device unique id"),
    admin: Dict[str, str] = Depends(require_admin_read),
):
    try:
        profile = feedback_store.get_user_trust_profile(user_id=user_id)
        return {
            "success": True,
            "profile": profile,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"유저 신뢰도 조회 중 오류: {e}")


@app.get("/api/feedback/user-trust/list")
def list_feedback_user_trust_profiles(
    limit: int = Query(default=200, ge=1, le=5000),
    admin: Dict[str, str] = Depends(require_admin_read),
):
    try:
        profiles = feedback_store.list_user_trust_profiles(limit=limit)
        return {
            "success": True,
            "count": len(profiles),
            "profiles": profiles,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"유저 신뢰도 목록 조회 중 오류: {e}")


@app.post("/api/feedback/user-trust/reset")
def reset_feedback_user_trust(
    payload: UserTrustResetRequest,
    admin: Dict[str, str] = Depends(require_admin_write),
):
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "reset_user_trust")
        profile = feedback_store.clear_user_trust_profile(user_id=payload.user_id)
        log_admin_action_safe(
            action="reset_user_trust",
            target_type="user",
            target_id=str(profile.get("user_id_hash", "")),
            meta={
                "source_after_reset": profile.get("source"),
                "trust_weight_after_reset": profile.get("trust_weight"),
                "updated_feedback_count": profile.get("updated_feedback_count"),
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "profile": profile,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "message": "Manual trust override cleared",
            "saved_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"유저 신뢰도 초기화 중 오류: {e}")


@app.post("/api/feedback/user-tier")
def upsert_feedback_user_tier(
    payload: UserTierUpdateRequest,
    admin: Dict[str, str] = Depends(require_admin_write),
):
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "upsert_user_tier")
        profile = feedback_store.upsert_user_tester_tier(
            user_id=payload.user_id,
            tester_tier=payload.tester_tier,
            note=payload.note,
        )
        log_admin_action_safe(
            action="upsert_user_tier",
            target_type="user",
            target_id=str(profile.get("user_id_hash", "")),
            meta={
                "tester_tier": profile.get("tester_tier"),
                "effective_source": profile.get("effective_source"),
                "updated_feedback_count": profile.get("updated_feedback_count"),
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "profile": profile,
            "available_tiers": USER_TIER_DEFAULT_WEIGHTS,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "message": "User tier updated",
            "saved_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"유저 등급 저장 중 오류: {e}")


@app.get("/api/feedback/user-tier")
def get_feedback_user_tier(
    user_id: str = Query(..., min_length=3, max_length=128, description="Tester/device unique id"),
    admin: Dict[str, str] = Depends(require_admin_read),
):
    try:
        profile = feedback_store.get_user_tester_tier(user_id=user_id)
        return {
            "success": True,
            "profile": profile,
            "available_tiers": USER_TIER_DEFAULT_WEIGHTS,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"유저 등급 조회 중 오류: {e}")


@app.get("/api/feedback/user-tier/list")
def list_feedback_user_tier_profiles(
    limit: int = Query(default=200, ge=1, le=5000),
    admin: Dict[str, str] = Depends(require_admin_read),
):
    try:
        profiles = feedback_store.list_user_tester_tiers(limit=limit)
        return {
            "success": True,
            "count": len(profiles),
            "profiles": profiles,
            "available_tiers": USER_TIER_DEFAULT_WEIGHTS,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"유저 등급 목록 조회 중 오류: {e}")


@app.get("/api/feedback/tester-quality")
def get_feedback_tester_quality(
    min_votes: int = Query(default=TESTER_QUALITY_MIN_VOTES_DEFAULT, ge=5, le=10000),
    promote_threshold: float = Query(default=TESTER_QUALITY_PROMOTE_THRESHOLD_DEFAULT, ge=0.5, le=1.0),
    demote_threshold: float = Query(default=TESTER_QUALITY_DEMOTE_THRESHOLD_DEFAULT, ge=0.0, le=0.9),
    recent_days: Optional[int] = Query(default=None, ge=1, le=3650),
    limit: int = Query(default=200, ge=1, le=5000),
    admin: Dict[str, str] = Depends(require_admin_read),
):
    if promote_threshold <= demote_threshold:
        raise HTTPException(status_code=400, detail="promote_threshold must be greater than demote_threshold")

    try:
        candidates = feedback_store.get_tester_quality_candidates(
            min_votes=min_votes,
            promote_threshold=promote_threshold,
            demote_threshold=demote_threshold,
            recent_days=recent_days,
            limit=limit,
        )
        actionable_count = len([item for item in candidates if item.get("recommended_tier")])
        recommendation_counter = Counter([str(item.get("recommendation", "keep")) for item in candidates])
        tier_counter = Counter([str(item.get("current_tier", "general")) for item in candidates])
        return {
            "success": True,
            "count": len(candidates),
            "actionable_count": actionable_count,
            "candidates": candidates,
            "summary": {
                "by_recommendation": dict(recommendation_counter),
                "by_current_tier": dict(tier_counter),
            },
            "thresholds": {
                "min_votes": min_votes,
                "promote_threshold": promote_threshold,
                "demote_threshold": demote_threshold,
                "recent_days": recent_days,
            },
            "auth_mode": admin.get("auth_mode", "disabled"),
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"테스터 품질 분석 중 오류: {e}")


@app.post("/api/feedback/user-tier/auto-apply")
def auto_apply_feedback_user_tier(
    payload: UserTierAutoApplyRequest,
    admin: Dict[str, str] = Depends(require_admin_write),
):
    if payload.promote_threshold <= payload.demote_threshold:
        raise HTTPException(status_code=400, detail="promote_threshold must be greater than demote_threshold")

    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "auto_apply_user_tier")
        result = feedback_store.auto_apply_tester_tiers(
            min_votes=payload.min_votes,
            promote_threshold=payload.promote_threshold,
            demote_threshold=payload.demote_threshold,
            recent_days=payload.recent_days,
            limit=payload.limit,
            max_apply=payload.max_apply,
            dry_run=payload.dry_run,
        )
        log_admin_action_safe(
            action="auto_apply_user_tier",
            target_type="system",
            target_id="tester_tiers",
            meta={
                "dry_run": payload.dry_run,
                "applied_count": result.get("applied_count", 0),
                "actionable_count": result.get("actionable_count", 0),
                "thresholds": {
                    "min_votes": payload.min_votes,
                    "promote_threshold": payload.promote_threshold,
                    "demote_threshold": payload.demote_threshold,
                },
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "result": result,
            "thresholds": {
                "min_votes": payload.min_votes,
                "promote_threshold": payload.promote_threshold,
                "demote_threshold": payload.demote_threshold,
                "recent_days": payload.recent_days,
                "limit": payload.limit,
                "max_apply": payload.max_apply,
                "dry_run": payload.dry_run,
            },
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "message": "User tier auto-apply executed",
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"유저 등급 자동 반영 중 오류: {e}")


@app.get("/api/feedback/keyword-candidates")
def get_feedback_keyword_candidates(
    min_votes: int = Query(default=30, ge=5, le=10000),
    consensus_threshold: float = Query(default=0.8, ge=0.5, le=1.0),
    min_disagreement_ratio: float = Query(default=0.3, ge=0.0, le=1.0),
    limit: int = Query(default=100, ge=1, le=1000),
):
    try:
        candidates = feedback_store.get_keyword_candidates(
            min_votes=min_votes,
            consensus_threshold=consensus_threshold,
            min_disagreement_ratio=min_disagreement_ratio,
            limit=limit,
        )
        return {
            "success": True,
            "count": len(candidates),
            "candidates": candidates,
            "rule_hint": {
                "auto_apply_suggestion": (
                    "Use candidates where vote_count >= 30 and consensus_ratio >= 0.8 "
                    "to reduce noisy updates."
                )
            },
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"키워드 후보 조회 중 오류: {e}")


@app.get("/api/feedback/keyword-rules")
def list_feedback_keyword_rules(
    status: str = Query(default="applied", pattern="^(applied|disabled)$"),
    limit: int = Query(default=200, ge=1, le=5000),
    admin: Dict[str, str] = Depends(require_admin_read),
):
    try:
        rules = feedback_store.list_keyword_rules(status=status, limit=limit)
        return {
            "success": True,
            "count": len(rules),
            "rules": rules,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"키워드 룰 조회 중 오류: {e}")


@app.post("/api/feedback/keyword-rules/apply")
def apply_feedback_keyword_rule(
    payload: KeywordRuleApplyRequest,
    admin: Dict[str, str] = Depends(require_admin_write),
):
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "apply_keyword_rule")
        rule = feedback_store.apply_keyword_rule(
            keyword=payload.keyword,
            label=payload.label,
            support_votes=payload.support_votes,
            consensus_ratio=payload.consensus_ratio,
            source=payload.source,
        )
        log_admin_action_safe(
            action="apply_keyword_rule",
            target_type="keyword",
            target_id=str(rule.get("keyword", "")),
            meta={
                "label": rule.get("label"),
                "source": rule.get("source"),
                "support_votes": rule.get("support_votes"),
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "rule": rule,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "message": "Keyword rule applied",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"키워드 룰 적용 중 오류: {e}")


@app.post("/api/feedback/keyword-rules/disable")
def disable_feedback_keyword_rule(
    payload: KeywordRuleDisableRequest,
    admin: Dict[str, str] = Depends(require_admin_write),
):
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "disable_keyword_rule")
        result = feedback_store.disable_keyword_rule(keyword=payload.keyword)
        log_admin_action_safe(
            action="disable_keyword_rule",
            target_type="keyword",
            target_id=str(result.get("keyword", payload.keyword)),
            meta={
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "result": result,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "message": "Keyword rule disabled",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"키워드 룰 비활성화 중 오류: {e}")


@app.post("/api/feedback/keyword-rules/auto-apply")
def auto_apply_feedback_keyword_rules(
    min_votes: int = Query(default=30, ge=5, le=10000),
    consensus_threshold: float = Query(default=0.8, ge=0.5, le=1.0),
    min_disagreement_ratio: float = Query(default=0.3, ge=0.0, le=1.0),
    limit: int = Query(default=20, ge=1, le=500),
    admin: Dict[str, str] = Depends(require_admin_write),
):
    """
    Safe auto-apply: only promotes candidates that satisfy strict thresholds.
    """
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "auto_apply_keyword_rules")
        candidates = feedback_store.get_keyword_candidates(
            min_votes=min_votes,
            consensus_threshold=consensus_threshold,
            min_disagreement_ratio=min_disagreement_ratio,
            limit=limit,
        )

        applied: List[Dict[str, object]] = []
        for candidate in candidates:
            rule = feedback_store.apply_keyword_rule(
                keyword=str(candidate["keyword"]),
                label=str(candidate["recommended_label"]),
                support_votes=int(candidate["vote_count"]),
                consensus_ratio=float(candidate["consensus_ratio"]),
                source="auto_feedback",
            )
            applied.append(rule)

        log_admin_action_safe(
            action="auto_apply_keyword_rules",
            target_type="system",
            target_id="keyword_rules",
            meta={
                "candidates_count": len(candidates),
                "applied_count": len(applied),
                "thresholds": {
                    "min_votes": min_votes,
                    "consensus_threshold": consensus_threshold,
                    "min_disagreement_ratio": min_disagreement_ratio,
                },
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )

        return {
            "success": True,
            "candidates_count": len(candidates),
            "applied_count": len(applied),
            "applied_rules": applied,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"자동 룰 반영 중 오류: {e}")


@app.get("/api/admin/audit-logs")
def get_admin_audit_logs(
    limit: int = Query(default=200, ge=1, le=5000),
    action: Optional[str] = Query(default=None, max_length=100),
    admin: Dict[str, str] = Depends(require_admin_read),
):
    try:
        logs = feedback_store.list_admin_audit_logs(limit=limit, action=action)
        return {
            "success": True,
            "count": len(logs),
            "logs": logs,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"감사 로그 조회 중 오류: {e}")


@app.get("/api/admin/alerts/prune-preview")
def preview_alert_history_prune(
    retention_days: int = Query(default=ALERT_HISTORY_RETENTION_DAYS, ge=1, le=3650),
    max_rows: int = Query(default=ALERT_HISTORY_MAX_ROWS, ge=100, le=2000000),
    admin: Dict[str, str] = Depends(require_admin_read),
):
    try:
        preview = alert_store.preview_prune(
            retention_days=retention_days,
            max_rows=max_rows,
        )
        return {
            "success": True,
            "preview": preview,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to preview alert history prune: {e}")


@app.post("/api/admin/alerts/prune")
def prune_alert_history(
    payload: AlertHistoryPruneRequest,
    admin: Dict[str, str] = Depends(require_admin_write),
):
    try:
        rate_limit = enforce_admin_write_rate_limit(admin, "prune_alert_history")
        result = alert_store.prune_history(
            retention_days=payload.retention_days,
            max_rows=payload.max_rows,
        )
        log_admin_action_safe(
            action="prune_alert_history",
            target_type="system",
            target_id="alert_history",
            meta={
                "retention_days": payload.retention_days,
                "max_rows": payload.max_rows,
                "deleted_total": result.get("deleted_total", 0),
                "remaining": result.get("remaining", 0),
                "auth_mode": admin.get("auth_mode", "disabled"),
                "rate_limit": rate_limit,
            },
        )
        return {
            "success": True,
            "result": result,
            "auth_mode": admin.get("auth_mode", "disabled"),
            "write_rate_limit": rate_limit,
            "message": "Alert history pruned",
            "generated_at": now_str(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"알림 히스토리 정리 중 오류: {e}")


@app.on_event("startup")
def _startup_monitoring_scheduler():
    if MONITORING_SCHEDULER_AUTOSTART:
        try:
            start_monitoring_scheduler(force_restart=False)
        except Exception:
            pass


@app.on_event("shutdown")
def _shutdown_monitoring_scheduler():
    try:
        stop_monitoring_scheduler()
    except Exception:
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(
        f"\nSignalWatch 서버 시작\n"
        f"- URL: http://localhost:{port}\n"
        f"- Docs: http://localhost:{port}/docs\n"
    )
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level="info",
    )
