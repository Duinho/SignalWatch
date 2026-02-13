import hashlib
import json
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple


LABELS = ("positive", "negative", "neutral")
TESTER_TIERS = ("core", "general", "observer")
STOPWORDS = {
    "기자",
    "뉴스",
    "관련",
    "오늘",
    "단독",
    "속보",
    "종합",
    "시장",
    "기업",
    "주식",
}
DEFAULT_CONSENSUS_MIN_VOTES = 20
DEFAULT_CONSENSUS_THRESHOLD = 0.8
DEFAULT_TRUST_WEIGHT = 1.0
MIN_TRUST_WEIGHT = 0.2
MAX_TRUST_WEIGHT = 3.0
TESTER_TIER_WEIGHTS = {
    "core": 1.8,
    "general": 1.0,
    "observer": 0.7,
}
TIER_PROMOTE_TARGET = {
    "observer": "general",
    "general": "core",
    "core": "core",
}
TIER_DEMOTE_TARGET = {
    "core": "general",
    "general": "observer",
    "observer": "observer",
}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def hash_user_id(user_id: str) -> str:
    return hashlib.sha256(user_id.encode("utf-8")).hexdigest()


def extract_keywords(text: str) -> List[str]:
    tokens = re.findall(r"[0-9A-Za-z가-힣]{2,}", text.lower())
    unique: List[str] = []
    seen = set()
    for token in tokens:
        if token in STOPWORDS:
            continue
        if token in seen:
            continue
        seen.add(token)
        unique.append(token)
    return unique


class FeedbackStore:
    """
    SQLite-backed human feedback store.
    - Article feedback events
    - Keyword vote aggregates
    - Manually or semi-automatically applied keyword rules
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._rules_cache: Dict[str, Dict[str, object]] = {}
        self._rules_cache_ts = 0.0
        self._rules_cache_ttl_sec = 30.0
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS feedback_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    user_id_hash TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    article_link TEXT NOT NULL,
                    article_title TEXT NOT NULL,
                    article_source TEXT DEFAULT '',
                    ai_label TEXT NOT NULL CHECK(ai_label IN ('positive','negative','neutral')),
                    user_label TEXT NOT NULL CHECK(user_label IN ('positive','negative','neutral')),
                    user_confidence INTEGER NOT NULL DEFAULT 3 CHECK(user_confidence >= 1 AND user_confidence <= 5),
                    note TEXT DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_feedback_article ON feedback_events(article_link);
                CREATE INDEX IF NOT EXISTS idx_feedback_stock ON feedback_events(stock_code);
                CREATE INDEX IF NOT EXISTS idx_feedback_created ON feedback_events(created_at);

                CREATE TABLE IF NOT EXISTS keyword_votes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    feedback_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    keyword TEXT NOT NULL,
                    ai_label TEXT NOT NULL CHECK(ai_label IN ('positive','negative','neutral')),
                    user_label TEXT NOT NULL CHECK(user_label IN ('positive','negative','neutral')),
                    weight REAL NOT NULL DEFAULT 3.0,
                    FOREIGN KEY(feedback_id) REFERENCES feedback_events(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_keyword_votes_keyword ON keyword_votes(keyword);
                CREATE INDEX IF NOT EXISTS idx_keyword_votes_created ON keyword_votes(created_at);

                CREATE TABLE IF NOT EXISTS keyword_rules (
                    keyword TEXT PRIMARY KEY,
                    label TEXT NOT NULL CHECK(label IN ('positive','negative','neutral')),
                    status TEXT NOT NULL CHECK(status IN ('applied','disabled')),
                    source TEXT NOT NULL DEFAULT 'manual',
                    support_votes INTEGER NOT NULL DEFAULT 0,
                    consensus_ratio REAL NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS user_trust_profiles (
                    user_id_hash TEXT PRIMARY KEY,
                    trust_weight REAL NOT NULL
                        CHECK(trust_weight >= 0.2 AND trust_weight <= 3.0),
                    note TEXT DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_user_trust_updated
                ON user_trust_profiles(updated_at);

                CREATE TABLE IF NOT EXISTS user_tester_tiers (
                    user_id_hash TEXT PRIMARY KEY,
                    tester_tier TEXT NOT NULL
                        CHECK(tester_tier IN ('core', 'general', 'observer')),
                    note TEXT DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_user_tier_updated
                ON user_tester_tiers(updated_at);

                CREATE TABLE IF NOT EXISTS admin_audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    meta_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_admin_audit_created
                ON admin_audit_logs(created_at);

                CREATE INDEX IF NOT EXISTS idx_admin_audit_action
                ON admin_audit_logs(action);
                """
            )
            self._ensure_feedback_columns(conn)
            self._dedupe_feedback_events(conn)
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_feedback_user_article
                ON feedback_events(user_id_hash, article_link);
                """
            )

    def _column_exists(self, conn: sqlite3.Connection, table: str, column: str) -> bool:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(str(row["name"]) == column for row in rows)

    def _ensure_feedback_columns(self, conn: sqlite3.Connection):
        if not self._column_exists(conn, "feedback_events", "trust_weight"):
            conn.execute(
                """
                ALTER TABLE feedback_events
                ADD COLUMN trust_weight REAL NOT NULL DEFAULT 1.0
                """
            )

        if not self._column_exists(conn, "feedback_events", "weighted_score"):
            conn.execute(
                """
                ALTER TABLE feedback_events
                ADD COLUMN weighted_score REAL NOT NULL DEFAULT 0.0
                """
            )

        conn.execute(
            """
            UPDATE feedback_events
            SET trust_weight = 1.0
            WHERE trust_weight IS NULL OR trust_weight <= 0
            """
        )
        conn.execute(
            """
            UPDATE feedback_events
            SET weighted_score = ROUND(user_confidence * trust_weight, 4)
            WHERE weighted_score IS NULL OR weighted_score <= 0
            """
        )

    def _resolve_effective_trust(self, conn: sqlite3.Connection, user_id_hash: str) -> Dict[str, object]:
        manual_row = conn.execute(
            """
            SELECT trust_weight, note, updated_at
            FROM user_trust_profiles
            WHERE user_id_hash = ?
            LIMIT 1
            """,
            (user_id_hash,),
        ).fetchone()
        if manual_row:
            weight = float(manual_row["trust_weight"] or DEFAULT_TRUST_WEIGHT)
            return {
                "trust_weight": weight,
                "source": "manual",
                "tester_tier": None,
                "note": str(manual_row["note"] or ""),
                "updated_at": str(manual_row["updated_at"] or ""),
            }

        tier_row = conn.execute(
            """
            SELECT tester_tier, note, updated_at
            FROM user_tester_tiers
            WHERE user_id_hash = ?
            LIMIT 1
            """,
            (user_id_hash,),
        ).fetchone()
        if tier_row:
            tier = str(tier_row["tester_tier"] or "general")
            weight = float(TESTER_TIER_WEIGHTS.get(tier, DEFAULT_TRUST_WEIGHT))
            return {
                "trust_weight": weight,
                "source": "tier_default",
                "tester_tier": tier,
                "note": str(tier_row["note"] or ""),
                "updated_at": str(tier_row["updated_at"] or ""),
            }

        return {
            "trust_weight": DEFAULT_TRUST_WEIGHT,
            "source": "system_default",
            "tester_tier": None,
            "note": "",
            "updated_at": "",
        }

    def _reweight_user_feedback(self, conn: sqlite3.Connection, user_id_hash: str, trust_weight: float) -> int:
        feedback_rows = conn.execute(
            """
            SELECT id, user_confidence
            FROM feedback_events
            WHERE user_id_hash = ?
            """,
            (user_id_hash,),
        ).fetchall()

        for row in feedback_rows:
            feedback_id = int(row["id"])
            confidence = int(row["user_confidence"] or 3)
            weighted_score = round(confidence * trust_weight, 4)
            conn.execute(
                """
                UPDATE feedback_events
                SET trust_weight = ?, weighted_score = ?
                WHERE id = ?
                """,
                (trust_weight, weighted_score, feedback_id),
            )
            conn.execute(
                """
                UPDATE keyword_votes
                SET weight = ?
                WHERE feedback_id = ?
                """,
                (weighted_score, feedback_id),
            )

        return len(feedback_rows)

    def _dedupe_feedback_events(self, conn: sqlite3.Connection):
        duplicate_groups = conn.execute(
            """
            SELECT user_id_hash, article_link, COUNT(*) AS cnt
            FROM feedback_events
            GROUP BY user_id_hash, article_link
            HAVING COUNT(*) > 1
            """
        ).fetchall()

        for group in duplicate_groups:
            user_id_hash = str(group["user_id_hash"])
            article_link = str(group["article_link"])
            rows = conn.execute(
                """
                SELECT id
                FROM feedback_events
                WHERE user_id_hash = ? AND article_link = ?
                ORDER BY id DESC
                """,
                (user_id_hash, article_link),
            ).fetchall()
            if not rows:
                continue

            keep_id = int(rows[0]["id"])
            remove_ids = [int(row["id"]) for row in rows[1:]]
            if not remove_ids:
                continue

            conn.executemany(
                "DELETE FROM keyword_votes WHERE feedback_id = ?",
                [(feedback_id,) for feedback_id in remove_ids],
            )
            conn.executemany(
                "DELETE FROM feedback_events WHERE id = ?",
                [(feedback_id,) for feedback_id in remove_ids],
            )

    def submit_feedback(
        self,
        user_id: str,
        stock_code: str,
        article_link: str,
        article_title: str,
        article_source: str,
        ai_label: str,
        user_label: str,
        user_confidence: int,
        note: str = "",
    ) -> Dict[str, object]:
        if ai_label not in LABELS:
            raise ValueError("ai_label must be one of positive/negative/neutral")
        if user_label not in LABELS:
            raise ValueError("user_label must be one of positive/negative/neutral")

        user_hash = hash_user_id(user_id)
        created_at = now_str()
        confidence = max(1, min(5, int(user_confidence)))
        keywords = extract_keywords(article_title)

        with self._lock, self._connect() as conn:
            trust_meta = self._resolve_effective_trust(conn=conn, user_id_hash=user_hash)
            trust_weight = float(trust_meta["trust_weight"])
            weighted_score = round(confidence * trust_weight, 4)
            existing = conn.execute(
                """
                SELECT id
                FROM feedback_events
                WHERE user_id_hash = ? AND article_link = ?
                LIMIT 1
                """,
                (user_hash, article_link),
            ).fetchone()
            vote_action = "updated" if existing else "created"

            conn.execute(
                """
                INSERT INTO feedback_events (
                    created_at, user_id_hash, stock_code, article_link, article_title, article_source,
                    ai_label, user_label, user_confidence, trust_weight, weighted_score, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id_hash, article_link) DO UPDATE SET
                    created_at = excluded.created_at,
                    stock_code = excluded.stock_code,
                    article_title = excluded.article_title,
                    article_source = excluded.article_source,
                    ai_label = excluded.ai_label,
                    user_label = excluded.user_label,
                    user_confidence = excluded.user_confidence,
                    trust_weight = excluded.trust_weight,
                    weighted_score = excluded.weighted_score,
                    note = excluded.note
                """,
                (
                    created_at,
                    user_hash,
                    stock_code,
                    article_link,
                    article_title,
                    article_source,
                    ai_label,
                    user_label,
                    confidence,
                    trust_weight,
                    weighted_score,
                    note,
                ),
            )
            feedback_row = conn.execute(
                """
                SELECT id
                FROM feedback_events
                WHERE user_id_hash = ? AND article_link = ?
                LIMIT 1
                """,
                (user_hash, article_link),
            ).fetchone()
            if not feedback_row:
                raise RuntimeError("feedback upsert failed")
            feedback_id = int(feedback_row["id"])

            conn.execute("DELETE FROM keyword_votes WHERE feedback_id = ?", (feedback_id,))

            for keyword in keywords:
                conn.execute(
                    """
                    INSERT INTO keyword_votes (feedback_id, created_at, keyword, ai_label, user_label, weight)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (feedback_id, created_at, keyword, ai_label, user_label, weighted_score),
                )

        summary = self.get_article_summary(article_link)
        return {
            "feedback_id": feedback_id,
            "vote_action": vote_action,
            "keyword_count": len(keywords),
            "trust_weight": trust_weight,
            "weighted_score": weighted_score,
            "trust_source": str(trust_meta.get("source", "system_default")),
            "tester_tier": trust_meta.get("tester_tier"),
            "article_summary": summary,
        }

    def get_article_summary(self, article_link: str) -> Dict[str, object]:
        with self._connect() as conn:
            label_rows = conn.execute(
                """
                SELECT
                    user_label,
                    COUNT(*) AS votes,
                    SUM(weighted_score) AS weighted_votes
                FROM feedback_events
                WHERE article_link = ?
                GROUP BY user_label
                """,
                (article_link,),
            ).fetchall()

            total_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_votes,
                    COUNT(DISTINCT user_id_hash) AS unique_users,
                    SUM(CASE WHEN ai_label = user_label THEN 1 ELSE 0 END) AS ai_match_votes,
                    SUM(weighted_score) AS total_weighted_votes
                FROM feedback_events
                WHERE article_link = ?
                """,
                (article_link,),
            ).fetchone()

        if not total_row or int(total_row["total_votes"] or 0) == 0:
            return {
                "article_link": article_link,
                "total_votes": 0,
                "unique_users": 0,
                "consensus_label": "unknown",
                "consensus_ratio": 0.0,
                "ai_match_ratio": 0.0,
                "total_weighted_votes": 0.0,
                "breakdown": {"positive": 0, "negative": 0, "neutral": 0},
                "weighted_breakdown": {"positive": 0.0, "negative": 0.0, "neutral": 0.0},
            }

        weighted = {"positive": 0.0, "negative": 0.0, "neutral": 0.0}
        raw = {"positive": 0, "negative": 0, "neutral": 0}
        for row in label_rows:
            label = str(row["user_label"])
            raw[label] = int(row["votes"] or 0)
            weighted[label] = float(row["weighted_votes"] or 0.0)

        best_label = max(weighted.items(), key=lambda x: x[1])[0]
        total_weight = sum(weighted.values()) or 1.0
        consensus_ratio = weighted[best_label] / total_weight

        total_votes = int(total_row["total_votes"] or 0)
        ai_match_votes = int(total_row["ai_match_votes"] or 0)

        return {
            "article_link": article_link,
            "total_votes": total_votes,
            "unique_users": int(total_row["unique_users"] or 0),
            "consensus_label": best_label,
            "consensus_ratio": round(consensus_ratio, 4),
            "ai_match_ratio": round(ai_match_votes / max(1, total_votes), 4),
            "total_weighted_votes": round(float(total_row["total_weighted_votes"] or 0.0), 4),
            "breakdown": raw,
            "weighted_breakdown": {
                "positive": round(weighted["positive"], 4),
                "negative": round(weighted["negative"], 4),
                "neutral": round(weighted["neutral"], 4),
            },
        }

    def evaluate_article_consensus(
        self,
        article_link: str,
        min_votes: int = DEFAULT_CONSENSUS_MIN_VOTES,
        min_consensus_ratio: float = DEFAULT_CONSENSUS_THRESHOLD,
    ) -> Dict[str, object]:
        summary = self.get_article_summary(article_link)

        total_votes = int(summary.get("total_votes", 0))
        consensus_ratio = float(summary.get("consensus_ratio", 0.0))
        consensus_label = str(summary.get("consensus_label", "unknown"))

        reasons: List[str] = []
        if total_votes < min_votes:
            reasons.append("not_enough_votes")
        if consensus_ratio < min_consensus_ratio:
            reasons.append("low_consensus_ratio")
        if consensus_label not in LABELS:
            reasons.append("invalid_consensus_label")

        return {
            **summary,
            "consensus_ready": len(reasons) == 0,
            "consensus_requirements": {
                "min_votes": int(min_votes),
                "min_consensus_ratio": float(min_consensus_ratio),
            },
            "consensus_reasons": reasons if reasons else ["ready"],
        }

    def get_user_trust_weight(
        self,
        user_id: Optional[str] = None,
        user_id_hash: Optional[str] = None,
    ) -> float:
        if not user_id_hash:
            if not user_id:
                raise ValueError("user_id or user_id_hash is required")
            user_id_hash = hash_user_id(user_id)

        with self._connect() as conn:
            trust_meta = self._resolve_effective_trust(conn=conn, user_id_hash=user_id_hash)
        return float(trust_meta["trust_weight"])

    def upsert_user_trust_profile(self, user_id: str, trust_weight: float, note: str = "") -> Dict[str, object]:
        if not user_id:
            raise ValueError("user_id is required")

        bounded_weight = max(MIN_TRUST_WEIGHT, min(MAX_TRUST_WEIGHT, float(trust_weight)))
        user_id_hash = hash_user_id(user_id)
        updated_at = now_str()

        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO user_trust_profiles (user_id_hash, trust_weight, note, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id_hash) DO UPDATE SET
                    trust_weight = excluded.trust_weight,
                    note = excluded.note,
                    updated_at = excluded.updated_at
                """,
                (user_id_hash, bounded_weight, note, updated_at),
            )
            updated_feedback_count = self._reweight_user_feedback(
                conn=conn,
                user_id_hash=user_id_hash,
                trust_weight=bounded_weight,
            )

        return {
            "user_id_hash": user_id_hash,
            "trust_weight": round(bounded_weight, 4),
            "note": note,
            "updated_at": updated_at,
            "updated_feedback_count": updated_feedback_count,
            "source": "manual",
        }

    def clear_user_trust_profile(self, user_id: str) -> Dict[str, object]:
        if not user_id:
            raise ValueError("user_id is required")

        user_id_hash = hash_user_id(user_id)
        updated_at = now_str()

        with self._lock, self._connect() as conn:
            conn.execute(
                """
                DELETE FROM user_trust_profiles
                WHERE user_id_hash = ?
                """,
                (user_id_hash,),
            )
            trust_meta = self._resolve_effective_trust(conn=conn, user_id_hash=user_id_hash)
            effective_weight = float(trust_meta["trust_weight"])
            updated_feedback_count = self._reweight_user_feedback(
                conn=conn,
                user_id_hash=user_id_hash,
                trust_weight=effective_weight,
            )

        return {
            "user_id_hash": user_id_hash,
            "trust_weight": round(effective_weight, 4),
            "source": str(trust_meta.get("source", "system_default")),
            "tester_tier": trust_meta.get("tester_tier"),
            "updated_at": updated_at,
            "updated_feedback_count": updated_feedback_count,
        }

    def get_user_trust_profile(self, user_id: str) -> Dict[str, object]:
        if not user_id:
            raise ValueError("user_id is required")
        user_id_hash = hash_user_id(user_id)

        with self._connect() as conn:
            manual_row = conn.execute(
                """
                SELECT user_id_hash, trust_weight, note, updated_at
                FROM user_trust_profiles
                WHERE user_id_hash = ?
                LIMIT 1
                """,
                (user_id_hash,),
            ).fetchone()
            tier_row = conn.execute(
                """
                SELECT tester_tier, note, updated_at
                FROM user_tester_tiers
                WHERE user_id_hash = ?
                LIMIT 1
                """,
                (user_id_hash,),
            ).fetchone()
            effective = self._resolve_effective_trust(conn=conn, user_id_hash=user_id_hash)

        return {
            "user_id_hash": user_id_hash,
            "effective_trust_weight": float(effective["trust_weight"]),
            "effective_source": str(effective["source"]),
            "tester_tier": effective.get("tester_tier"),
            "manual_override": {
                "enabled": bool(manual_row),
                "trust_weight": float(manual_row["trust_weight"]) if manual_row else None,
                "note": str(manual_row["note"] or "") if manual_row else "",
                "updated_at": str(manual_row["updated_at"]) if manual_row else None,
            },
            "tier_profile": {
                "enabled": bool(tier_row),
                "tester_tier": str(tier_row["tester_tier"]) if tier_row else None,
                "default_weight": (
                    float(TESTER_TIER_WEIGHTS.get(str(tier_row["tester_tier"]), DEFAULT_TRUST_WEIGHT))
                    if tier_row
                    else DEFAULT_TRUST_WEIGHT
                ),
                "note": str(tier_row["note"] or "") if tier_row else "",
                "updated_at": str(tier_row["updated_at"]) if tier_row else None,
            },
        }

    def list_user_trust_profiles(self, limit: int = 200) -> List[Dict[str, object]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT user_id_hash, trust_weight, note, updated_at
                FROM user_trust_profiles
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()

        return [
            {
                "user_id_hash": str(row["user_id_hash"]),
                "trust_weight": float(row["trust_weight"]),
                "note": str(row["note"] or ""),
                "updated_at": str(row["updated_at"]),
            }
            for row in rows
        ]

    def _upsert_user_tier_by_hash(
        self,
        conn: sqlite3.Connection,
        user_id_hash: str,
        tester_tier: str,
        note: str,
        updated_at: str,
    ) -> Dict[str, object]:
        if tester_tier not in TESTER_TIERS:
            raise ValueError(f"tester_tier must be one of {TESTER_TIERS}")

        default_weight = float(TESTER_TIER_WEIGHTS[tester_tier])
        conn.execute(
            """
            INSERT INTO user_tester_tiers (user_id_hash, tester_tier, note, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id_hash) DO UPDATE SET
                tester_tier = excluded.tester_tier,
                note = excluded.note,
                updated_at = excluded.updated_at
            """,
            (user_id_hash, tester_tier, note, updated_at),
        )

        effective_meta = self._resolve_effective_trust(conn=conn, user_id_hash=user_id_hash)
        effective_weight = float(effective_meta["trust_weight"])
        updated_feedback_count = 0
        if str(effective_meta.get("source")) == "tier_default":
            updated_feedback_count = self._reweight_user_feedback(
                conn=conn,
                user_id_hash=user_id_hash,
                trust_weight=effective_weight,
            )

        return {
            "user_id_hash": user_id_hash,
            "tester_tier": tester_tier,
            "default_weight": default_weight,
            "effective_trust_weight": round(effective_weight, 4),
            "effective_source": str(effective_meta.get("source", "system_default")),
            "note": note,
            "updated_at": updated_at,
            "updated_feedback_count": updated_feedback_count,
        }

    def upsert_user_tester_tier(self, user_id: str, tester_tier: str, note: str = "") -> Dict[str, object]:
        if not user_id:
            raise ValueError("user_id is required")

        user_id_hash = hash_user_id(user_id)
        updated_at = now_str()

        with self._lock, self._connect() as conn:
            return self._upsert_user_tier_by_hash(
                conn=conn,
                user_id_hash=user_id_hash,
                tester_tier=tester_tier,
                note=note,
                updated_at=updated_at,
            )

    def get_user_tester_tier(self, user_id: str) -> Dict[str, object]:
        if not user_id:
            raise ValueError("user_id is required")

        user_id_hash = hash_user_id(user_id)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT tester_tier, note, updated_at
                FROM user_tester_tiers
                WHERE user_id_hash = ?
                LIMIT 1
                """,
                (user_id_hash,),
            ).fetchone()
            effective_meta = self._resolve_effective_trust(conn=conn, user_id_hash=user_id_hash)

        if not row:
            return {
                "user_id_hash": user_id_hash,
                "tester_tier": None,
                "default_weight": DEFAULT_TRUST_WEIGHT,
                "note": "",
                "updated_at": None,
                "effective_trust_weight": float(effective_meta["trust_weight"]),
                "effective_source": str(effective_meta["source"]),
            }

        tier = str(row["tester_tier"])
        return {
            "user_id_hash": user_id_hash,
            "tester_tier": tier,
            "default_weight": float(TESTER_TIER_WEIGHTS.get(tier, DEFAULT_TRUST_WEIGHT)),
            "note": str(row["note"] or ""),
            "updated_at": str(row["updated_at"]),
            "effective_trust_weight": float(effective_meta["trust_weight"]),
            "effective_source": str(effective_meta["source"]),
        }

    def list_user_tester_tiers(self, limit: int = 200) -> List[Dict[str, object]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT user_id_hash, tester_tier, note, updated_at
                FROM user_tester_tiers
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()

        return [
            {
                "user_id_hash": str(row["user_id_hash"]),
                "tester_tier": str(row["tester_tier"]),
                "default_weight": float(TESTER_TIER_WEIGHTS.get(str(row["tester_tier"]), DEFAULT_TRUST_WEIGHT)),
                "note": str(row["note"] or ""),
                "updated_at": str(row["updated_at"]),
            }
            for row in rows
        ]

    def get_tester_quality_candidates(
        self,
        min_votes: int = 20,
        promote_threshold: float = 0.8,
        demote_threshold: float = 0.4,
        recent_days: Optional[int] = None,
        limit: int = 200,
    ) -> List[Dict[str, object]]:
        if promote_threshold <= demote_threshold:
            raise ValueError("promote_threshold must be greater than demote_threshold")

        rows_query = """
            SELECT user_id_hash, article_link, ai_label, user_label, weighted_score
            FROM feedback_events
        """
        rows_params: Tuple[object, ...] = ()
        if recent_days is not None:
            bounded_days = max(1, int(recent_days))
            cutoff = (datetime.now() - timedelta(days=bounded_days)).strftime("%Y-%m-%d %H:%M:%S")
            rows_query += " WHERE created_at >= ?"
            rows_params = (cutoff,)

        with self._connect() as conn:
            rows = conn.execute(rows_query, rows_params).fetchall()
            manual_rows = conn.execute(
                """
                SELECT user_id_hash, trust_weight
                FROM user_trust_profiles
                """
            ).fetchall()
            tier_rows = conn.execute(
                """
                SELECT user_id_hash, tester_tier
                FROM user_tester_tiers
                """
            ).fetchall()

        if not rows:
            return []

        article_consensus_weights: Dict[str, Dict[str, float]] = {}
        for row in rows:
            article_link = str(row["article_link"])
            label = str(row["user_label"])
            weighted_score = float(row["weighted_score"] or 0.0)
            if article_link not in article_consensus_weights:
                article_consensus_weights[article_link] = {
                    "positive": 0.0,
                    "negative": 0.0,
                    "neutral": 0.0,
                }
            article_consensus_weights[article_link][label] += weighted_score

        article_consensus_label: Dict[str, str] = {}
        for article_link, weights in article_consensus_weights.items():
            consensus_label = max(weights.items(), key=lambda x: x[1])[0]
            article_consensus_label[article_link] = consensus_label

        manual_map = {
            str(row["user_id_hash"]): float(row["trust_weight"] or DEFAULT_TRUST_WEIGHT)
            for row in manual_rows
        }
        tier_map = {
            str(row["user_id_hash"]): str(row["tester_tier"] or "general")
            for row in tier_rows
        }

        user_stats: Dict[str, Dict[str, float]] = {}
        for row in rows:
            user_id_hash = str(row["user_id_hash"])
            article_link = str(row["article_link"])
            ai_label = str(row["ai_label"])
            user_label = str(row["user_label"])
            weighted_score = float(row["weighted_score"] or 0.0)

            stat = user_stats.setdefault(
                user_id_hash,
                {
                    "vote_count": 0.0,
                    "weighted_votes": 0.0,
                    "ai_match_votes": 0.0,
                    "consensus_match_votes": 0.0,
                },
            )

            stat["vote_count"] += 1
            stat["weighted_votes"] += weighted_score
            if ai_label == user_label:
                stat["ai_match_votes"] += 1
            if user_label == article_consensus_label.get(article_link, "neutral"):
                stat["consensus_match_votes"] += 1

        candidates: List[Dict[str, object]] = []
        for user_id_hash, stat in user_stats.items():
            vote_count = int(stat["vote_count"])
            if vote_count < min_votes:
                continue

            ai_match_ratio = float(stat["ai_match_votes"]) / max(1, vote_count)
            consensus_match_ratio = float(stat["consensus_match_votes"]) / max(1, vote_count)

            current_tier = tier_map.get(user_id_hash, "general")
            manual_override = user_id_hash in manual_map
            if manual_override:
                effective_source = "manual"
                effective_weight = float(manual_map[user_id_hash])
            elif user_id_hash in tier_map:
                effective_source = "tier_default"
                effective_weight = float(TESTER_TIER_WEIGHTS.get(current_tier, DEFAULT_TRUST_WEIGHT))
            else:
                effective_source = "system_default"
                effective_weight = DEFAULT_TRUST_WEIGHT

            recommended_tier: Optional[str] = None
            recommendation = "keep"

            if manual_override:
                recommendation = "manual_override_keep"
            elif consensus_match_ratio >= promote_threshold:
                promote_target = TIER_PROMOTE_TARGET.get(current_tier, current_tier)
                if promote_target != current_tier:
                    recommended_tier = promote_target
                    recommendation = f"promote_{current_tier}_to_{promote_target}"
            elif consensus_match_ratio <= demote_threshold:
                demote_target = TIER_DEMOTE_TARGET.get(current_tier, current_tier)
                if demote_target != current_tier:
                    recommended_tier = demote_target
                    recommendation = f"demote_{current_tier}_to_{demote_target}"

            candidates.append(
                {
                    "user_id_hash": user_id_hash,
                    "vote_count": vote_count,
                    "weighted_votes": round(float(stat["weighted_votes"]), 4),
                    "consensus_match_ratio": round(consensus_match_ratio, 4),
                    "ai_match_ratio": round(ai_match_ratio, 4),
                    "current_tier": current_tier,
                    "effective_source": effective_source,
                    "effective_trust_weight": round(effective_weight, 4),
                    "manual_override": manual_override,
                    "recommended_tier": recommended_tier,
                    "recommendation": recommendation,
                    "recent_days": recent_days,
                }
            )

        candidates.sort(
            key=lambda item: (
                1 if item.get("recommended_tier") else 0,
                int(item.get("vote_count", 0)),
                float(item.get("consensus_match_ratio", 0.0)),
            ),
            reverse=True,
        )
        return candidates[:limit]

    def auto_apply_tester_tiers(
        self,
        min_votes: int = 20,
        promote_threshold: float = 0.8,
        demote_threshold: float = 0.4,
        recent_days: Optional[int] = None,
        limit: int = 200,
        max_apply: int = 50,
        dry_run: bool = True,
    ) -> Dict[str, object]:
        candidates = self.get_tester_quality_candidates(
            min_votes=min_votes,
            promote_threshold=promote_threshold,
            demote_threshold=demote_threshold,
            recent_days=recent_days,
            limit=limit,
        )
        actionable = [
            candidate
            for candidate in candidates
            if candidate.get("recommended_tier") and not bool(candidate.get("manual_override"))
        ]

        if dry_run:
            return {
                "dry_run": True,
                "candidates_count": len(candidates),
                "actionable_count": len(actionable),
                "applied_count": 0,
                "applied": [],
                "preview": actionable[:max_apply],
            }

        applied: List[Dict[str, object]] = []
        with self._lock, self._connect() as conn:
            for candidate in actionable[:max_apply]:
                user_id_hash = str(candidate["user_id_hash"])
                recommended_tier = str(candidate["recommended_tier"])
                note = (
                    "auto_quality:"
                    f"{candidate['recommendation']};"
                    f"votes={candidate['vote_count']};"
                    f"consensus={candidate['consensus_match_ratio']}"
                )
                result = self._upsert_user_tier_by_hash(
                    conn=conn,
                    user_id_hash=user_id_hash,
                    tester_tier=recommended_tier,
                    note=note,
                    updated_at=now_str(),
                )
                applied.append(
                    {
                        "user_id_hash": user_id_hash,
                        "from_tier": candidate["current_tier"],
                        "to_tier": recommended_tier,
                        "recommendation": candidate["recommendation"],
                        "consensus_match_ratio": candidate["consensus_match_ratio"],
                        "vote_count": candidate["vote_count"],
                        "result": result,
                    }
                )

        return {
            "dry_run": False,
            "candidates_count": len(candidates),
            "actionable_count": len(actionable),
            "applied_count": len(applied),
            "applied": applied,
        }

    def get_keyword_candidates(
        self,
        min_votes: int = 30,
        consensus_threshold: float = 0.8,
        min_disagreement_ratio: float = 0.3,
        limit: int = 100,
    ) -> List[Dict[str, object]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                WITH agg AS (
                    SELECT
                        keyword,
                        COUNT(*) AS vote_count,
                        SUM(weight) AS total_weight,
                        SUM(CASE WHEN user_label = 'positive' THEN weight ELSE 0 END) AS w_pos,
                        SUM(CASE WHEN user_label = 'negative' THEN weight ELSE 0 END) AS w_neg,
                        SUM(CASE WHEN user_label = 'neutral' THEN weight ELSE 0 END) AS w_neu,
                        SUM(CASE WHEN ai_label != user_label THEN weight ELSE 0 END) AS w_disagree
                    FROM keyword_votes
                    GROUP BY keyword
                )
                SELECT * FROM agg
                WHERE vote_count >= ?
                ORDER BY vote_count DESC, total_weight DESC
                LIMIT ?
                """,
                (min_votes, limit * 3),
            ).fetchall()

        candidates: List[Dict[str, object]] = []
        for row in rows:
            total_weight = float(row["total_weight"] or 0.0)
            if total_weight <= 0:
                continue

            weights = {
                "positive": float(row["w_pos"] or 0.0),
                "negative": float(row["w_neg"] or 0.0),
                "neutral": float(row["w_neu"] or 0.0),
            }
            best_label, best_weight = max(weights.items(), key=lambda x: x[1])
            consensus_ratio = best_weight / total_weight
            disagreement_ratio = float(row["w_disagree"] or 0.0) / total_weight

            if consensus_ratio < consensus_threshold:
                continue
            if disagreement_ratio < min_disagreement_ratio:
                continue

            candidates.append(
                {
                    "keyword": str(row["keyword"]),
                    "recommended_label": best_label,
                    "vote_count": int(row["vote_count"] or 0),
                    "consensus_ratio": round(consensus_ratio, 4),
                    "disagreement_ratio": round(disagreement_ratio, 4),
                    "weights": {
                        "positive": weights["positive"],
                        "negative": weights["negative"],
                        "neutral": weights["neutral"],
                    },
                }
            )

            if len(candidates) >= limit:
                break

        return candidates

    def apply_keyword_rule(
        self,
        keyword: str,
        label: str,
        support_votes: int = 0,
        consensus_ratio: float = 0.0,
        source: str = "manual",
    ) -> Dict[str, object]:
        if label not in LABELS:
            raise ValueError("label must be one of positive/negative/neutral")

        keyword = keyword.strip().lower()
        if not keyword:
            raise ValueError("keyword is required")

        updated_at = now_str()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO keyword_rules (keyword, label, status, source, support_votes, consensus_ratio, updated_at)
                VALUES (?, ?, 'applied', ?, ?, ?, ?)
                ON CONFLICT(keyword) DO UPDATE SET
                    label = excluded.label,
                    status = 'applied',
                    source = excluded.source,
                    support_votes = excluded.support_votes,
                    consensus_ratio = excluded.consensus_ratio,
                    updated_at = excluded.updated_at
                """,
                (keyword, label, source, int(support_votes), float(consensus_ratio), updated_at),
            )

        self._rules_cache_ts = 0.0
        return {
            "keyword": keyword,
            "label": label,
            "status": "applied",
            "source": source,
            "support_votes": int(support_votes),
            "consensus_ratio": float(consensus_ratio),
            "updated_at": updated_at,
        }

    def disable_keyword_rule(self, keyword: str) -> Dict[str, object]:
        keyword = keyword.strip().lower()
        if not keyword:
            raise ValueError("keyword is required")

        updated_at = now_str()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE keyword_rules
                SET status = 'disabled', updated_at = ?
                WHERE keyword = ?
                """,
                (updated_at, keyword),
            )

        self._rules_cache_ts = 0.0
        return {"keyword": keyword, "status": "disabled", "updated_at": updated_at}

    def list_keyword_rules(self, status: str = "applied", limit: int = 200) -> List[Dict[str, object]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT keyword, label, status, source, support_votes, consensus_ratio, updated_at
                FROM keyword_rules
                WHERE status = ?
                ORDER BY support_votes DESC, consensus_ratio DESC, updated_at DESC
                LIMIT ?
                """,
                (status, limit),
            ).fetchall()

        return [
            {
                "keyword": str(row["keyword"]),
                "label": str(row["label"]),
                "status": str(row["status"]),
                "source": str(row["source"]),
                "support_votes": int(row["support_votes"] or 0),
                "consensus_ratio": float(row["consensus_ratio"] or 0.0),
                "updated_at": str(row["updated_at"]),
            }
            for row in rows
        ]

    def log_admin_action(
        self,
        action: str,
        target_type: str,
        target_id: str,
        meta: Optional[Dict[str, object]] = None,
    ) -> int:
        created_at = now_str()
        meta_json = json.dumps(meta or {}, ensure_ascii=False, separators=(",", ":"))

        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO admin_audit_logs (created_at, action, target_type, target_id, meta_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (created_at, action, target_type, target_id, meta_json),
            )
            return int(cursor.lastrowid)

    def list_admin_audit_logs(self, limit: int = 200, action: Optional[str] = None) -> List[Dict[str, object]]:
        with self._connect() as conn:
            if action:
                rows = conn.execute(
                    """
                    SELECT id, created_at, action, target_type, target_id, meta_json
                    FROM admin_audit_logs
                    WHERE action = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (action, int(limit)),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, created_at, action, target_type, target_id, meta_json
                    FROM admin_audit_logs
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall()

        logs: List[Dict[str, object]] = []
        for row in rows:
            raw_meta = str(row["meta_json"] or "{}")
            try:
                meta = json.loads(raw_meta)
            except json.JSONDecodeError:
                meta = {"raw": raw_meta}
            logs.append(
                {
                    "id": int(row["id"]),
                    "created_at": str(row["created_at"]),
                    "action": str(row["action"]),
                    "target_type": str(row["target_type"]),
                    "target_id": str(row["target_id"]),
                    "meta": meta,
                }
            )
        return logs

    def get_metrics(self, since_hours: int = 24) -> Dict[str, object]:
        bounded_hours = max(1, int(since_hours))
        cutoff = (datetime.now() - timedelta(hours=bounded_hours)).strftime("%Y-%m-%d %H:%M:%S")

        with self._connect() as conn:
            total_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_feedback_events,
                    COUNT(DISTINCT user_id_hash) AS total_unique_users,
                    SUM(weighted_score) AS total_weighted_votes,
                    SUM(CASE WHEN ai_label = user_label THEN 1 ELSE 0 END) AS total_ai_match_votes,
                    MIN(created_at) AS first_feedback_at,
                    MAX(created_at) AS last_feedback_at
                FROM feedback_events
                """
            ).fetchone()

            recent_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS recent_feedback_events,
                    COUNT(DISTINCT user_id_hash) AS recent_unique_users,
                    SUM(weighted_score) AS recent_weighted_votes,
                    SUM(CASE WHEN ai_label = user_label THEN 1 ELSE 0 END) AS recent_ai_match_votes
                FROM feedback_events
                WHERE created_at >= ?
                """,
                (cutoff,),
            ).fetchone()

            total_labels = conn.execute(
                """
                SELECT user_label, COUNT(*) AS cnt
                FROM feedback_events
                GROUP BY user_label
                """
            ).fetchall()

            recent_labels = conn.execute(
                """
                SELECT user_label, COUNT(*) AS cnt
                FROM feedback_events
                WHERE created_at >= ?
                GROUP BY user_label
                """,
                (cutoff,),
            ).fetchall()

            keyword_votes_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM keyword_votes
                """
            ).fetchone()

            keyword_rules_row = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END) AS applied_count,
                    SUM(CASE WHEN status = 'disabled' THEN 1 ELSE 0 END) AS disabled_count
                FROM keyword_rules
                """
            ).fetchone()

            trust_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM user_trust_profiles
                """
            ).fetchone()

            tier_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM user_tester_tiers
                """
            ).fetchone()

            audit_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM admin_audit_logs
                """
            ).fetchone()

        total_feedback_events = int(total_row["total_feedback_events"] or 0)
        recent_feedback_events = int(recent_row["recent_feedback_events"] or 0)
        total_ai_match_votes = int(total_row["total_ai_match_votes"] or 0)
        recent_ai_match_votes = int(recent_row["recent_ai_match_votes"] or 0)

        total_label_distribution = {"positive": 0, "negative": 0, "neutral": 0}
        for row in total_labels:
            total_label_distribution[str(row["user_label"])] = int(row["cnt"] or 0)

        recent_label_distribution = {"positive": 0, "negative": 0, "neutral": 0}
        for row in recent_labels:
            recent_label_distribution[str(row["user_label"])] = int(row["cnt"] or 0)

        return {
            "window_hours": bounded_hours,
            "window_start": cutoff,
            "total_feedback_events": total_feedback_events,
            "recent_feedback_events": recent_feedback_events,
            "total_unique_users": int(total_row["total_unique_users"] or 0),
            "recent_unique_users": int(recent_row["recent_unique_users"] or 0),
            "total_weighted_votes": round(float(total_row["total_weighted_votes"] or 0.0), 4),
            "recent_weighted_votes": round(float(recent_row["recent_weighted_votes"] or 0.0), 4),
            "total_ai_match_ratio": round(total_ai_match_votes / max(1, total_feedback_events), 4),
            "recent_ai_match_ratio": round(recent_ai_match_votes / max(1, recent_feedback_events), 4),
            "label_distribution": total_label_distribution,
            "recent_label_distribution": recent_label_distribution,
            "keyword_votes_count": int(keyword_votes_row["cnt"] or 0),
            "keyword_rules_applied_count": int(keyword_rules_row["applied_count"] or 0),
            "keyword_rules_disabled_count": int(keyword_rules_row["disabled_count"] or 0),
            "user_trust_profile_count": int(trust_row["cnt"] or 0),
            "user_tier_count": int(tier_row["cnt"] or 0),
            "admin_audit_log_count": int(audit_row["cnt"] or 0),
            "first_feedback_at": str(total_row["first_feedback_at"] or ""),
            "last_feedback_at": str(total_row["last_feedback_at"] or ""),
        }

    def get_stock_feedback_signal(
        self,
        stock_code: str,
        since_hours: int = 72,
        min_votes: int = 5,
    ) -> Dict[str, object]:
        bounded_hours = max(1, int(since_hours))
        bounded_min_votes = max(1, int(min_votes))
        cutoff = (datetime.now() - timedelta(hours=bounded_hours)).strftime("%Y-%m-%d %H:%M:%S")

        with self._connect() as conn:
            total_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_votes,
                    COUNT(DISTINCT user_id_hash) AS unique_users,
                    SUM(weighted_score) AS total_weighted_votes,
                    SUM(CASE WHEN ai_label = user_label THEN 1 ELSE 0 END) AS ai_match_votes
                FROM feedback_events
                WHERE stock_code = ? AND created_at >= ?
                """,
                (stock_code, cutoff),
            ).fetchone()

            label_rows = conn.execute(
                """
                SELECT
                    user_label,
                    COUNT(*) AS vote_count,
                    SUM(weighted_score) AS weighted_votes
                FROM feedback_events
                WHERE stock_code = ? AND created_at >= ?
                GROUP BY user_label
                """,
                (stock_code, cutoff),
            ).fetchall()

        total_votes = int(total_row["total_votes"] or 0)
        unique_users = int(total_row["unique_users"] or 0)
        total_weighted_votes = float(total_row["total_weighted_votes"] or 0.0)
        ai_match_votes = int(total_row["ai_match_votes"] or 0)

        label_votes = {"positive": 0, "negative": 0, "neutral": 0}
        label_weighted_votes = {"positive": 0.0, "negative": 0.0, "neutral": 0.0}
        for row in label_rows:
            label = str(row["user_label"])
            label_votes[label] = int(row["vote_count"] or 0)
            label_weighted_votes[label] = float(row["weighted_votes"] or 0.0)

        consensus_label = "neutral"
        consensus_ratio = 0.0
        if total_weighted_votes > 0:
            consensus_label = max(label_weighted_votes.items(), key=lambda x: x[1])[0]
            consensus_ratio = label_weighted_votes[consensus_label] / total_weighted_votes

        return {
            "stock_code": stock_code,
            "window_hours": bounded_hours,
            "window_start": cutoff,
            "min_votes": bounded_min_votes,
            "total_votes": total_votes,
            "unique_users": unique_users,
            "total_weighted_votes": round(total_weighted_votes, 4),
            "ai_match_ratio": round(ai_match_votes / max(1, total_votes), 4),
            "consensus_label": consensus_label,
            "consensus_ratio": round(consensus_ratio, 4),
            "label_votes": label_votes,
            "label_weighted_votes": {
                "positive": round(label_weighted_votes["positive"], 4),
                "negative": round(label_weighted_votes["negative"], 4),
                "neutral": round(label_weighted_votes["neutral"], 4),
            },
            "ready": total_votes >= bounded_min_votes,
        }

    def get_applied_rules_map(self) -> Dict[str, Dict[str, object]]:
        now = time.time()
        if now - self._rules_cache_ts <= self._rules_cache_ttl_sec and self._rules_cache:
            return dict(self._rules_cache)

        rules = self.list_keyword_rules(status="applied", limit=5000)
        mapped = {
            rule["keyword"]: {
                "label": rule["label"],
                "support_votes": rule["support_votes"],
                "consensus_ratio": rule["consensus_ratio"],
                "source": rule["source"],
            }
            for rule in rules
        }
        self._rules_cache = mapped
        self._rules_cache_ts = now
        return dict(mapped)

    def match_applied_rules(self, text: str) -> List[Tuple[str, Dict[str, object]]]:
        rules = self.get_applied_rules_map()
        lowered = text.lower()
        matched: List[Tuple[str, Dict[str, object]]] = []
        for keyword, rule in rules.items():
            if keyword and keyword in lowered:
                matched.append((keyword, rule))
        return matched
