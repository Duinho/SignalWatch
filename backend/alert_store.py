import json
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class AlertStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS alert_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    importance_score INTEGER NOT NULL,
                    delivery_level TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    article_count INTEGER NOT NULL,
                    sentiment TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_alert_history_created
                ON alert_history(created_at DESC);

                CREATE INDEX IF NOT EXISTS idx_alert_history_stock
                ON alert_history(stock_code, created_at DESC);

                CREATE INDEX IF NOT EXISTS idx_alert_history_level
                ON alert_history(delivery_level, created_at DESC);

                CREATE TABLE IF NOT EXISTS monitoring_run_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    trigger TEXT NOT NULL,
                    policy_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result_count INTEGER NOT NULL,
                    average_score REAL NOT NULL,
                    duration_ms REAL NOT NULL,
                    error_message TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_monitoring_run_created
                ON monitoring_run_history(created_at DESC);

                CREATE INDEX IF NOT EXISTS idx_monitoring_run_status
                ON monitoring_run_history(status, created_at DESC);
                """
            )

    def save_alerts(self, alerts: List[Dict[str, object]], created_at: Optional[str] = None) -> int:
        if not alerts:
            return 0

        timestamp = created_at or now_str()
        rows = []
        for alert in alerts:
            rows.append(
                (
                    timestamp,
                    str(alert.get("stock_code", "")),
                    str(alert.get("stock_name", "")),
                    int(alert.get("importance_score", 0)),
                    str(alert.get("delivery_level", "daily_digest")),
                    str(alert.get("priority", "low")),
                    int(alert.get("article_count", 0)),
                    str(alert.get("sentiment", "neutral")),
                    str(alert.get("summary", "")),
                    json.dumps(alert, ensure_ascii=False, separators=(",", ":")),
                )
            )

        with self._lock, self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO alert_history (
                    created_at, stock_code, stock_name, importance_score, delivery_level, priority,
                    article_count, sentiment, summary, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def list_alert_history(
        self,
        limit: int = 100,
        stock_code: Optional[str] = None,
        delivery_level: Optional[str] = None,
        min_score: int = 0,
    ) -> List[Dict[str, object]]:
        conditions = ["importance_score >= ?"]
        params: List[object] = [int(min_score)]

        if stock_code:
            conditions.append("stock_code = ?")
            params.append(stock_code)
        if delivery_level:
            conditions.append("delivery_level = ?")
            params.append(delivery_level)

        where_clause = " AND ".join(conditions)
        params.append(int(limit))

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    id, created_at, stock_code, stock_name, importance_score, delivery_level,
                    priority, article_count, sentiment, summary, payload_json
                FROM alert_history
                WHERE {where_clause}
                ORDER BY id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()

        result: List[Dict[str, object]] = []
        for row in rows:
            payload_raw = str(row["payload_json"] or "{}")
            try:
                payload = json.loads(payload_raw)
            except json.JSONDecodeError:
                payload = {"raw": payload_raw}

            result.append(
                {
                    "id": int(row["id"]),
                    "created_at": str(row["created_at"]),
                    "stock_code": str(row["stock_code"]),
                    "stock_name": str(row["stock_name"]),
                    "importance_score": int(row["importance_score"]),
                    "delivery_level": str(row["delivery_level"]),
                    "priority": str(row["priority"]),
                    "article_count": int(row["article_count"]),
                    "sentiment": str(row["sentiment"]),
                    "summary": str(row["summary"]),
                    "payload": payload,
                }
            )
        return result

    def get_metrics(self, since_hours: int = 24) -> Dict[str, object]:
        bounded_hours = max(1, int(since_hours))
        cutoff = (datetime.now() - timedelta(hours=bounded_hours)).strftime("%Y-%m-%d %H:%M:%S")

        with self._connect() as conn:
            total_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_alert_history_rows,
                    MIN(created_at) AS first_alert_at,
                    MAX(created_at) AS last_alert_at
                FROM alert_history
                """
            ).fetchone()

            recent_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS recent_alert_history_rows,
                    AVG(importance_score) AS avg_importance_score
                FROM alert_history
                WHERE created_at >= ?
                """,
                (cutoff,),
            ).fetchone()

            total_level_rows = conn.execute(
                """
                SELECT delivery_level, COUNT(*) AS cnt
                FROM alert_history
                GROUP BY delivery_level
                """
            ).fetchall()

            recent_level_rows = conn.execute(
                """
                SELECT delivery_level, COUNT(*) AS cnt
                FROM alert_history
                WHERE created_at >= ?
                GROUP BY delivery_level
                """,
                (cutoff,),
            ).fetchall()

            total_priority_rows = conn.execute(
                """
                SELECT priority, COUNT(*) AS cnt
                FROM alert_history
                GROUP BY priority
                """
            ).fetchall()

            recent_priority_rows = conn.execute(
                """
                SELECT priority, COUNT(*) AS cnt
                FROM alert_history
                WHERE created_at >= ?
                GROUP BY priority
                """,
                (cutoff,),
            ).fetchall()

        total_levels: Dict[str, int] = {}
        for row in total_level_rows:
            total_levels[str(row["delivery_level"])] = int(row["cnt"] or 0)

        recent_levels: Dict[str, int] = {}
        for row in recent_level_rows:
            recent_levels[str(row["delivery_level"])] = int(row["cnt"] or 0)

        total_priorities: Dict[str, int] = {}
        for row in total_priority_rows:
            total_priorities[str(row["priority"])] = int(row["cnt"] or 0)

        recent_priorities: Dict[str, int] = {}
        for row in recent_priority_rows:
            recent_priorities[str(row["priority"])] = int(row["cnt"] or 0)

        return {
            "window_hours": bounded_hours,
            "window_start": cutoff,
            "total_alert_history_rows": int(total_row["total_alert_history_rows"] or 0),
            "recent_alert_history_rows": int(recent_row["recent_alert_history_rows"] or 0),
            "recent_avg_importance_score": round(float(recent_row["avg_importance_score"] or 0.0), 2),
            "delivery_level_distribution": total_levels,
            "recent_delivery_level_distribution": recent_levels,
            "priority_distribution": total_priorities,
            "recent_priority_distribution": recent_priorities,
            "first_alert_at": str(total_row["first_alert_at"] or ""),
            "last_alert_at": str(total_row["last_alert_at"] or ""),
        }

    def save_monitoring_run(self, run: Dict[str, object], created_at: Optional[str] = None) -> int:
        timestamp = created_at or str(run.get("finished_at") or now_str())
        trigger = str(run.get("trigger", ""))
        policy_name = str(run.get("policy_name", ""))
        status = str(run.get("status", "success"))
        result_count = int(run.get("result_count", 0))
        average_score = float(run.get("average_score", 0.0))
        duration_ms = float(run.get("duration_ms", 0.0))
        error_message = str(run.get("error", ""))
        payload_json = json.dumps(run, ensure_ascii=False, separators=(",", ":"))

        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO monitoring_run_history (
                    created_at, trigger, policy_name, status, result_count, average_score,
                    duration_ms, error_message, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp,
                    trigger,
                    policy_name,
                    status,
                    result_count,
                    average_score,
                    duration_ms,
                    error_message,
                    payload_json,
                ),
            )
            return int(cursor.lastrowid)

    def list_monitoring_runs(
        self,
        limit: int = 50,
        status: Optional[str] = None,
        trigger: Optional[str] = None,
        since_hours: Optional[int] = None,
    ) -> List[Dict[str, object]]:
        conditions: List[str] = []
        params: List[object] = []

        if status:
            conditions.append("status = ?")
            params.append(status)
        if trigger:
            conditions.append("trigger = ?")
            params.append(trigger)
        if since_hours is not None:
            bounded_hours = max(1, int(since_hours))
            cutoff = (datetime.now() - timedelta(hours=bounded_hours)).strftime("%Y-%m-%d %H:%M:%S")
            conditions.append("created_at >= ?")
            params.append(cutoff)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(int(limit))

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    id, created_at, trigger, policy_name, status, result_count,
                    average_score, duration_ms, error_message, payload_json
                FROM monitoring_run_history
                {where_clause}
                ORDER BY id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()

        result: List[Dict[str, object]] = []
        for row in rows:
            payload_raw = str(row["payload_json"] or "{}")
            try:
                payload = json.loads(payload_raw)
            except json.JSONDecodeError:
                payload = {"raw": payload_raw}
            result.append(
                {
                    "id": int(row["id"]),
                    "created_at": str(row["created_at"]),
                    "trigger": str(row["trigger"]),
                    "policy_name": str(row["policy_name"]),
                    "status": str(row["status"]),
                    "result_count": int(row["result_count"] or 0),
                    "average_score": float(row["average_score"] or 0.0),
                    "duration_ms": float(row["duration_ms"] or 0.0),
                    "error": str(row["error_message"] or ""),
                    "payload": payload,
                }
            )
        return result

    def get_monitoring_run_metrics(self, since_hours: int = 24) -> Dict[str, object]:
        bounded_hours = max(1, int(since_hours))
        cutoff = (datetime.now() - timedelta(hours=bounded_hours)).strftime("%Y-%m-%d %H:%M:%S")

        with self._connect() as conn:
            total_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_runs,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS total_success_runs,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS total_error_runs,
                    AVG(duration_ms) AS total_avg_duration_ms,
                    AVG(average_score) AS total_avg_score,
                    MIN(created_at) AS first_run_at,
                    MAX(created_at) AS last_run_at
                FROM monitoring_run_history
                """
            ).fetchone()

            recent_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS recent_runs,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS recent_success_runs,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS recent_error_runs,
                    AVG(duration_ms) AS recent_avg_duration_ms,
                    AVG(average_score) AS recent_avg_score
                FROM monitoring_run_history
                WHERE created_at >= ?
                """,
                (cutoff,),
            ).fetchone()

            policy_rows = conn.execute(
                """
                SELECT
                    policy_name,
                    COUNT(*) AS run_count,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
                    AVG(average_score) AS avg_score,
                    AVG(duration_ms) AS avg_duration_ms
                FROM monitoring_run_history
                WHERE created_at >= ?
                GROUP BY policy_name
                ORDER BY run_count DESC, policy_name ASC
                """,
                (cutoff,),
            ).fetchall()

        by_policy: List[Dict[str, object]] = []
        for row in policy_rows:
            run_count = int(row["run_count"] or 0)
            success_count = int(row["success_count"] or 0)
            by_policy.append(
                {
                    "policy_name": str(row["policy_name"] or ""),
                    "run_count": run_count,
                    "success_count": success_count,
                    "success_ratio": round(success_count / max(1, run_count), 4),
                    "avg_score": round(float(row["avg_score"] or 0.0), 2),
                    "avg_duration_ms": round(float(row["avg_duration_ms"] or 0.0), 2),
                }
            )

        total_runs = int(total_row["total_runs"] or 0)
        recent_runs = int(recent_row["recent_runs"] or 0)
        total_success = int(total_row["total_success_runs"] or 0)
        recent_success = int(recent_row["recent_success_runs"] or 0)

        return {
            "window_hours": bounded_hours,
            "window_start": cutoff,
            "total_runs": total_runs,
            "recent_runs": recent_runs,
            "total_success_runs": total_success,
            "recent_success_runs": recent_success,
            "total_error_runs": int(total_row["total_error_runs"] or 0),
            "recent_error_runs": int(recent_row["recent_error_runs"] or 0),
            "total_success_ratio": round(total_success / max(1, total_runs), 4),
            "recent_success_ratio": round(recent_success / max(1, recent_runs), 4),
            "total_avg_duration_ms": round(float(total_row["total_avg_duration_ms"] or 0.0), 2),
            "recent_avg_duration_ms": round(float(recent_row["recent_avg_duration_ms"] or 0.0), 2),
            "total_avg_score": round(float(total_row["total_avg_score"] or 0.0), 2),
            "recent_avg_score": round(float(recent_row["recent_avg_score"] or 0.0), 2),
            "first_run_at": str(total_row["first_run_at"] or ""),
            "last_run_at": str(total_row["last_run_at"] or ""),
            "by_policy": by_policy,
        }

    def preview_prune(self, retention_days: int = 30, max_rows: int = 20000) -> Dict[str, int]:
        bounded_days = max(1, int(retention_days))
        bounded_max_rows = max(100, int(max_rows))
        cutoff = (datetime.now() - timedelta(days=bounded_days)).strftime("%Y-%m-%d %H:%M:%S")

        with self._connect() as conn:
            total_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM alert_history
                """
            ).fetchone()
            total_count = int(total_row["cnt"] or 0)

            old_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM alert_history
                WHERE created_at < ?
                """,
                (cutoff,),
            ).fetchone()
            old_count = int(old_row["cnt"] or 0)

        remaining_after_old = max(0, total_count - old_count)
        overflow_count = max(0, remaining_after_old - bounded_max_rows)
        would_delete_total = old_count + overflow_count
        would_remain = max(0, total_count - would_delete_total)

        return {
            "retention_days": bounded_days,
            "max_rows": bounded_max_rows,
            "total_rows": total_count,
            "would_delete_old": old_count,
            "would_delete_overflow": overflow_count,
            "would_delete_total": would_delete_total,
            "would_remain": would_remain,
        }

    def prune_history(self, retention_days: int = 30, max_rows: int = 20000) -> Dict[str, int]:
        bounded_days = max(1, int(retention_days))
        bounded_max_rows = max(100, int(max_rows))
        cutoff = (datetime.now() - timedelta(days=bounded_days)).strftime("%Y-%m-%d %H:%M:%S")

        with self._lock, self._connect() as conn:
            old_deleted = conn.execute(
                """
                DELETE FROM alert_history
                WHERE created_at < ?
                """,
                (cutoff,),
            ).rowcount

            row = conn.execute("SELECT COUNT(*) AS cnt FROM alert_history").fetchone()
            current_count = int(row["cnt"] or 0)
            overflow_deleted = 0
            if current_count > bounded_max_rows:
                overflow = current_count - bounded_max_rows
                overflow_deleted = conn.execute(
                    """
                    DELETE FROM alert_history
                    WHERE id IN (
                        SELECT id
                        FROM alert_history
                        ORDER BY id ASC
                        LIMIT ?
                    )
                    """,
                    (overflow,),
                ).rowcount

            row_after = conn.execute("SELECT COUNT(*) AS cnt FROM alert_history").fetchone()
            remaining = int(row_after["cnt"] or 0)

        return {
            "retention_days": bounded_days,
            "max_rows": bounded_max_rows,
            "old_deleted": int(old_deleted or 0),
            "overflow_deleted": int(overflow_deleted or 0),
            "deleted_total": int((old_deleted or 0) + (overflow_deleted or 0)),
            "remaining": remaining,
        }
