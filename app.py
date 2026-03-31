

from __future__ import annotations

import json
import math
import os
import random
import re
import sqlite3
import time
import hashlib
import hmac
import secrets
import uuid
import logging
import urllib.parse
import urllib.request
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.pool
import psycopg2.extras
import psycopg2.extensions

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

BASE = Path(__file__).parent
DB_PATH = BASE / "app.db"
QUESTIONS_PATH = BASE / "questions.json"
INDEX_PATH = BASE / "index.html"
PRICING_PATH = BASE / "pricing.html"
STORE_PATH = BASE / "store.html"
CHILD_PATH = BASE / "child.html"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_MAX_CONN = int(os.getenv("DB_MAX_CONN", "10"))
USE_POSTGRES = bool(DATABASE_URL)
POSTGRES_POOL: Optional[psycopg2.pool.ThreadedConnectionPool] = None

@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()

    import threading
    threading.Thread(target=rebuild_question_calibration, daemon=True).start()

    yield


app = FastAPI(title="Tiny Tutor (3 files, better)", lifespan=lifespan)
LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("tiny_tutor")
def configure_postgres_pool() -> None:
    global POSTGRES_POOL, USE_POSTGRES
    if not USE_POSTGRES:
        return
    try:
        POSTGRES_POOL = psycopg2.pool.ThreadedConnectionPool(1, DB_MAX_CONN, dsn=DATABASE_URL)
    except Exception as exc:
        LOGGER.exception("postgres_pool_init_failed")
        USE_POSTGRES = False

configure_postgres_pool()

PBKDF2_ITERATIONS = int(os.getenv("PBKDF2_ITERATIONS", "210000"))
TOKEN_TTL_SECONDS = int(os.getenv("TOKEN_TTL_SECONDS", str(60 * 60 * 24 * 14)))  # 14 days
TOKEN_BYTES = 32
LOGIN_WINDOW_SECONDS = int(os.getenv("LOGIN_WINDOW_SECONDS", str(10 * 60)))
LOGIN_MAX_FAILS = int(os.getenv("LOGIN_MAX_FAILS", "8"))
LOGIN_LOCK_SECONDS = int(os.getenv("LOGIN_LOCK_SECONDS", str(10 * 60)))
LOGIN_GUARD: Dict[str, Dict[str, float]] = {}
PASSWORD_MIN_LENGTH = 6
PASSWORD_MAX_LENGTH = 200
ANSWER_MAX_LENGTH = 1000
MAX_TIME_MS = 10 * 60 * 1000
ALLOWED_PRACTICE_MODES = {"warmup", "core", "stretch"}
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))
RATE_LIMIT_BUCKETS: Dict[str, Dict[str, float]] = {}
RATE_LIMIT_AUTH_MAX = int(os.getenv("RATE_LIMIT_AUTH_MAX", "25"))
RATE_LIMIT_ANSWER_MAX = int(os.getenv("RATE_LIMIT_ANSWER_MAX", "120"))
RATE_LIMIT_MAX_BUCKETS = int(os.getenv("RATE_LIMIT_MAX_BUCKETS", "50000"))
RATE_LIMIT_STALE_FACTOR = float(os.getenv("RATE_LIMIT_STALE_FACTOR", "4.0"))
RATE_LIMIT_CLEANUP_EVERY = int(os.getenv("RATE_LIMIT_CLEANUP_EVERY", "500"))
RATE_LIMIT_CALL_COUNT = 0
PLAN_AUTO_APPROVE_SECONDS = int(os.getenv("PLAN_AUTO_APPROVE_SECONDS", "10"))
STORE_PRICE_MULTIPLIER = float(os.getenv("STORE_PRICE_MULTIPLIER", "0.08"))
STORE_PRICE_MIN = int(os.getenv("STORE_PRICE_MIN", "10"))
QUESTION_VALIDATION_STRICT = os.getenv("QUESTION_VALIDATION_STRICT", "1").strip().lower() not in {"0", "false", "no"}
QUESTION_CALIBRATION_MIN_ATTEMPTS = int(os.getenv("QUESTION_CALIBRATION_MIN_ATTEMPTS", "5"))
QUESTION_QUALITY_REPORT: Dict[str, Any] = {}
QUESTION_CALIBRATION: Dict[str, Dict[str, Any]] = {}
OPS_LOCK = threading.Lock()
OPS_METRICS: Dict[str, Any] = {
    "started_at": int(time.time()),
    "requests_total": 0,
    "requests_2xx": 0,
    "requests_4xx": 0,
    "requests_5xx": 0,
    "request_duration_ms_sum": 0,
    "request_duration_ms_max": 0,
    "last_error_at": 0,
}
APP_ENV = os.getenv("APP_ENV", "development").strip().lower()
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:5000").strip()
ALLOWED_HOSTS = [h.strip().lower() for h in os.getenv("ALLOWED_HOSTS", "127.0.0.1,localhost").split(",") if h.strip()]
ENFORCE_HTTPS = os.getenv("ENFORCE_HTTPS", "0").strip().lower() in {"1", "true", "yes"}
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()
ALERT_MIN_INTERVAL_SECONDS = int(os.getenv("ALERT_MIN_INTERVAL_SECONDS", "300"))
LAST_ALERT_AT = 0
PAYMENT_PROVIDER = os.getenv("PAYMENT_PROVIDER", "none").strip().lower()
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "").strip()
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
STRIPE_API_BASE = os.getenv("STRIPE_API_BASE", "https://api.stripe.com/v1").strip().rstrip("/")
STRIPE_SUCCESS_PATH = os.getenv("STRIPE_SUCCESS_PATH", "/pricing.html?payment=success").strip()
STRIPE_CANCEL_PATH = os.getenv("STRIPE_CANCEL_PATH", "/pricing.html?payment=cancel").strip()
STRIPE_PRICE_LOOKUP_KEYS = {
    "starter": os.getenv("STRIPE_PRICE_STARTER", "starter_monthly").strip(),
    "plus": os.getenv("STRIPE_PRICE_PLUS", "plus_monthly").strip(),
    "pro": os.getenv("STRIPE_PRICE_PRO", "pro_monthly").strip(),
}
PLAN_ORDER = ["starter", "plus", "pro", "enterprise"]
PLAN_CATALOG: Dict[str, Dict[str, Any]] = {
    "starter": {
        "code": "starter",
        "name": "Starter",
        "price_gbp_month": 12,
        "price_text": "GBP 12 / month",
        "features": [
            "Core 11+ practice access",
            "Daily goals and streak tracking",
            "Practice assignment up to 20 questions",
            "Core mode assignments only",
            "Up to 1 child account",
        ],
        "capabilities": {
            "max_practice_size": 20,
            "allowed_modes": ["core"],
            "parent_report": False,
            "max_children": 1,
        },
    },
    "plus": {
        "code": "plus",
        "name": "Plus",
        "price_gbp_month": 20,
        "price_text": "GBP 20 / month",
        "features": [
            "Everything in Starter",
            "Practice assignment up to 60 questions",
            "Core and Warm-up assignment modes",
            "Parent report and cohort progress view",
            "Up to 3 child accounts",
        ],
        "capabilities": {
            "max_practice_size": 60,
            "allowed_modes": ["core", "warmup"],
            "parent_report": True,
            "max_children": 3,
        },
    },
    "pro": {
        "code": "pro",
        "name": "Pro",
        "price_gbp_month": 35,
        "price_text": "GBP 35 / month",
        "features": [
            "Everything in Plus",
            "Practice assignment up to 100 questions",
            "Core, Warm-up, and Stretch assignment modes",
            "Enhanced family insights",
            "Up to 8 child accounts",
        ],
        "capabilities": {
            "max_practice_size": 100,
            "allowed_modes": ["core", "warmup", "stretch"],
            "parent_report": True,
            "max_children": 8,
        },
    },
    "enterprise": {
        "code": "enterprise",
        "name": "Enterprise",
        "price_gbp_month": 100,
        "price_text": "GBP 100 / month",
        "features": [
            "Everything in Pro plus enterprise visibility",
            "Up to 20 child accounts",
            "Built-in tracking dashboard for assignments",
            "Dedicated audit logging surfaced in reports",
        ],
        "capabilities": {
            "max_practice_size": 180,
            "allowed_modes": ["core", "warmup", "stretch"],
            "parent_report": True,
            "max_children": 20,
            "enterprise_tracking": True,
        },
    },
}


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
    start = time.time()
    host = request.headers.get("host", "")
    if not is_allowed_host(host):
        raise HTTPException(status_code=400, detail="Invalid host header")
    if ENFORCE_HTTPS and APP_ENV == "production":
        proto = str(request.headers.get("x-forwarded-proto") or request.url.scheme or "http").lower()
        if proto != "https":
            raise HTTPException(status_code=400, detail="HTTPS is required")
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = int((time.time() - start) * 1000)
        with OPS_LOCK:
            OPS_METRICS["requests_total"] = int(OPS_METRICS.get("requests_total", 0)) + 1
            OPS_METRICS["requests_5xx"] = int(OPS_METRICS.get("requests_5xx", 0)) + 1
            OPS_METRICS["request_duration_ms_sum"] = int(OPS_METRICS.get("request_duration_ms_sum", 0)) + elapsed_ms
            OPS_METRICS["request_duration_ms_max"] = max(int(OPS_METRICS.get("request_duration_ms_max", 0)), elapsed_ms)
            OPS_METRICS["last_error_at"] = int(time.time())
        maybe_send_alert(f"[tiny_tutor] request_failed path={request.url.path} request_id={request_id}")
        LOGGER.exception("request_failed request_id=%s method=%s path=%s duration_ms=%d", request_id, request.method, request.url.path, elapsed_ms)
        raise
    elapsed_ms = int((time.time() - start) * 1000)
    response.headers["x-request-id"] = request_id
    response.headers["x-content-type-options"] = "nosniff"
    response.headers["x-frame-options"] = "DENY"
    response.headers["referrer-policy"] = "strict-origin-when-cross-origin"
    response.headers["permissions-policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["content-security-policy"] = "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'"
    if APP_ENV == "production":
        response.headers["strict-transport-security"] = "max-age=31536000; includeSubDomains; preload"
    with OPS_LOCK:
        OPS_METRICS["requests_total"] = int(OPS_METRICS.get("requests_total", 0)) + 1
        OPS_METRICS["request_duration_ms_sum"] = int(OPS_METRICS.get("request_duration_ms_sum", 0)) + elapsed_ms
        OPS_METRICS["request_duration_ms_max"] = max(int(OPS_METRICS.get("request_duration_ms_max", 0)), elapsed_ms)
        status = int(response.status_code)
        if 200 <= status <= 299:
            OPS_METRICS["requests_2xx"] = int(OPS_METRICS.get("requests_2xx", 0)) + 1
        elif 400 <= status <= 499:
            OPS_METRICS["requests_4xx"] = int(OPS_METRICS.get("requests_4xx", 0)) + 1
        elif status >= 500:
            OPS_METRICS["requests_5xx"] = int(OPS_METRICS.get("requests_5xx", 0)) + 1
            OPS_METRICS["last_error_at"] = int(time.time())
    LOGGER.info(
        "request_complete request_id=%s method=%s path=%s status=%d duration_ms=%d",
        request_id,
        request.method,
        request.url.path,
        int(response.status_code),
        elapsed_ms,
    )
    return response


def client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return str(fwd.split(",")[0].strip() or "unknown")
    if request.client and request.client.host:
        return str(request.client.host)
    return "unknown"


def normalize_host(host: str) -> str:
    raw = str(host or "").strip().lower()
    if ":" in raw:
        return raw.split(":", 1)[0]
    return raw


def is_allowed_host(host: str) -> bool:
    nh = normalize_host(host)
    if not ALLOWED_HOSTS:
        return True
    return nh in set(ALLOWED_HOSTS)


def maybe_send_alert(message: str) -> None:
    global LAST_ALERT_AT
    if not ALERT_WEBHOOK_URL:
        return
    now = int(time.time())
    if (now - int(LAST_ALERT_AT)) < max(10, ALERT_MIN_INTERVAL_SECONDS):
        return
    LAST_ALERT_AT = now
    payload = json.dumps({"text": message, "ts": now}).encode("utf-8")
    req = urllib.request.Request(
        ALERT_WEBHOOK_URL,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=3):
            pass
    except Exception:
        LOGGER.exception("alert_webhook_failed")


def apply_rate_limit(key: str, limit: int, window_seconds: int) -> None:
    global RATE_LIMIT_CALL_COUNT
    now = time.time()
    RATE_LIMIT_CALL_COUNT += 1
    if RATE_LIMIT_CALL_COUNT % max(1, RATE_LIMIT_CLEANUP_EVERY) == 0:
        cleanup_rate_limit_buckets(now, window_seconds)
    state = RATE_LIMIT_BUCKETS.get(key)
    if not state:
        RATE_LIMIT_BUCKETS[key] = {"count": 1.0, "window_start": now}
        return
    start = float(state.get("window_start", now) or now)
    if (now - start) >= window_seconds:
        RATE_LIMIT_BUCKETS[key] = {"count": 1.0, "window_start": now}
        return
    count = float(state.get("count", 0.0) or 0.0) + 1.0
    if count > float(limit):
        retry_after = int(max(1, window_seconds - (now - start)))
        raise HTTPException(status_code=429, detail=f"Rate limit exceeded. Try again in {retry_after}s.")
    state["count"] = count
    RATE_LIMIT_BUCKETS[key] = state
    if len(RATE_LIMIT_BUCKETS) > RATE_LIMIT_MAX_BUCKETS:
        cleanup_rate_limit_buckets(now, window_seconds, aggressive=True)


def cleanup_rate_limit_buckets(now: float, window_seconds: int, aggressive: bool = False) -> None:
    stale_after = float(window_seconds) * RATE_LIMIT_STALE_FACTOR
    stale_keys = []
    for bucket_key, bucket_state in RATE_LIMIT_BUCKETS.items():
        start = float(bucket_state.get("window_start", now) or now)
        if (now - start) > stale_after:
            stale_keys.append(bucket_key)
    for bucket_key in stale_keys:
        RATE_LIMIT_BUCKETS.pop(bucket_key, None)
    if aggressive and len(RATE_LIMIT_BUCKETS) > RATE_LIMIT_MAX_BUCKETS:
        for bucket_key in list(RATE_LIMIT_BUCKETS.keys())[: len(RATE_LIMIT_BUCKETS) // 10]:
            RATE_LIMIT_BUCKETS.pop(bucket_key, None)

# -------------------- Models --------------------
class LoginIn(BaseModel):
    email: Optional[str] = Field(default=None, min_length=3, max_length=120)
    username: Optional[str] = Field(default=None, min_length=2, max_length=120)
    password: str = Field(min_length=4, max_length=200)


class SignupIn(BaseModel):
    email: str = Field(min_length=4, max_length=120)
    password: str = Field(min_length=PASSWORD_MIN_LENGTH, max_length=PASSWORD_MAX_LENGTH)
    role: str = Field(default="child")

class LoginOut(BaseModel):
    user_id: int
    token: str
    email: Optional[str] = None
    username: Optional[str] = None
    role: str
    needs_plan_selection: bool = False
    current_plan: Dict[str, Any] = Field(default_factory=dict)

class NextOut(BaseModel):
    qid: str
    qtype: str
    domain: str
    skill: str
    difficulty: int
    stem: str
    choices: Optional[List[str]] = None
    time_limit_s: int = 45
    due: bool = False  # due for review vs new
    level_band: str = "Level 1"
    curriculum_tag: str = "11+ Core"
    curriculum_stage: str = "11+ Core"
    curriculum_strand: str = "General"
    curriculum_objective: str = "Core practice"
    curriculum_year: str = "Year 5/6"
    curriculum_topic: str = "Mixed practice"
    curriculum_subtopic: str = "Core skills"
    selection_note: str = ""

class AnswerIn(BaseModel):
    token: str
    qid: str
    answer: str
    time_ms: int = 0

class AnswerOut(BaseModel):
    correct: bool
    correct_answer: str
    explanation: str
    coach_tip: str = ""
    mistake_check: str = ""
    next_action: str = ""
    speed_band: str = "steady"
    next_due_in_days: Optional[float] = None

class StatsIn(BaseModel):
    token: str

class StatsOut(BaseModel):
    total_attempts: int
    accuracy: float
    today_attempts: int
    daily_goal: int
    streak_days: int
    due_now: int
    coins: int
    weakest_skills: List[Dict[str, Any]]
    skills: List[Dict[str, Any]]
    progress_stage: str = "building"
    recommended_difficulty: int = 3
    recent_accuracy: float = 0.0
    recent_avg_time_ms: int = 0
    learning_focus: str = ""
    domain_progress: List[Dict[str, Any]] = Field(default_factory=list)
    mastery_score: int = 0
    readiness_score: int = 0
    mastery_band: str = "starting"
    next_recommended_domains: List[str] = Field(default_factory=list)
    plan_tracking_child_count: int = 0
    plan_tracking_assigned_count: int = 0
    plan_tracking_due_count: int = 0

class AnalyticsChildUsage(BaseModel):
    child_user_id: int
    username: str
    attempts_7d: int
    correct_7d: int
    accuracy_7d: float
    coins: int
    streak_days: int
    daily_goal: int
    due_now: int
    last_answer_at: int
    assigned_7d: int
    completed_7d: int
    quit_7d: int
    progress_stage: str = "building"
    recommended_difficulty: int = 3
    learning_focus: str = ""

class AnalyticsPlanUsageRecord(BaseModel):
    plan_code: str
    child_count: int
    assigned_count: int
    due_count: int
    recorded_at: int

class AnalyticsStoreActivity(BaseModel):
    purchases_last_30d: int
    spent_last_30d: int
    last_purchase_at: int

class AnalyticsOut(BaseModel):
    generated_at: int
    window_seconds: int
    current_plan: Dict[str, Any]
    child_usage: List[AnalyticsChildUsage]
    practice_event_totals: Dict[str, int]
    plan_event_totals: Dict[str, int]
    plan_usage_trend: List[AnalyticsPlanUsageRecord]
    store_activity: AnalyticsStoreActivity

class GoalIn(BaseModel):
    token: str
    daily_goal: int = Field(ge=5, le=300)

class RecentOut(BaseModel):
    items: List[Dict[str, Any]]


class MeOut(BaseModel):
    user_id: int
    email: Optional[str] = None
    username: Optional[str] = None
    role: str
    parent_user_id: Optional[int] = None
    plan_code: str = "starter"
    plan_status: str = "none"
    plan_pending_target: str = ""
    plan_read_only: bool = False


class ParentCreateChildIn(BaseModel):
    token: str
    username: str = Field(min_length=2, max_length=120)
    password: str = Field(min_length=PASSWORD_MIN_LENGTH, max_length=PASSWORD_MAX_LENGTH)


class ParentDeleteChildIn(BaseModel):
    token: str
    child_user_id: int = Field(gt=0)


class ParentAssignPracticeIn(BaseModel):
    token: str
    child_user_id: int
    name: str = Field(min_length=1, max_length=120)
    size: int = Field(ge=5, le=200)
    domain: str = ""
    skills: List[str] = Field(default_factory=list)
    mode: str = Field(default="core")


class AssignedPracticesOut(BaseModel):
    items: List[Dict[str, Any]]


class AssignedPracticeCompleteIn(BaseModel):
    token: str
    assignment_id: int


class AssignedPracticeQuitIn(BaseModel):
    token: str
    assignment_id: int


class SwitchAccountIn(BaseModel):
    token: str
    target_role: str = Field(pattern="^(parent|child)$")
    child_user_id: Optional[int] = None


class PlanSelectIn(BaseModel):
    token: str
    plan_code: str = Field(min_length=2, max_length=40)


class PlanUpgradeIn(BaseModel):
    token: str
    target_plan_code: str = Field(min_length=2, max_length=40)


class PlanCancelIn(BaseModel):
    token: str


class PlanCheckoutIn(BaseModel):
    token: str
    target_plan_code: str = Field(min_length=2, max_length=40)
    action: str = Field(default="upgrade", pattern="^(select|upgrade)$")

# -------------------- DB --------------------
def db() -> sqlite3.Connection:
    if USE_POSTGRES and POSTGRES_POOL:
        conn = POSTGRES_POOL.getconn()
        conn.autocommit = False
        return conn
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def db_cursor(conn):
    if USE_POSTGRES and isinstance(conn, psycopg2.extensions.connection):
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn.cursor()

def utc_day_id(ts: Optional[int] = None) -> int:
    # days since epoch UTC (stable for streaks)
    if ts is None:
        ts = int(time.time())
    return int(ts // 86400)

SCHEMA_SQLITE = [
    """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL DEFAULT '',
            role TEXT NOT NULL DEFAULT 'child',
            parent_user_id INTEGER,
            token TEXT UNIQUE NOT NULL,
            token_issued_at INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            daily_goal INTEGER NOT NULL DEFAULT 30,
            streak_days INTEGER NOT NULL DEFAULT 0,
            last_active_day INTEGER NOT NULL DEFAULT 0,
            coins INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(parent_user_id) REFERENCES users(id)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            qid TEXT NOT NULL,
            domain TEXT NOT NULL,
            skill TEXT NOT NULL,
            correct INTEGER NOT NULL,
            time_ms INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            answer TEXT NOT NULL,
            correct_answer TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS qprog (
            user_id INTEGER NOT NULL,
            qid TEXT NOT NULL,
            ef REAL NOT NULL DEFAULT 2.5,
            reps INTEGER NOT NULL DEFAULT 0,
            interval_days REAL NOT NULL DEFAULT 0,
            due_at INTEGER NOT NULL DEFAULT 0,
            last_result INTEGER NOT NULL DEFAULT 0,
            lapses INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(user_id, qid),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            item_id TEXT NOT NULL,
            item_name TEXT NOT NULL,
            cost INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS equipped_items (
            user_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            item_id TEXT NOT NULL,
            item_name TEXT NOT NULL DEFAULT '',
            equipped_at INTEGER NOT NULL,
            PRIMARY KEY(user_id, kind),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS assigned_practices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_user_id INTEGER NOT NULL,
            child_user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            size INTEGER NOT NULL,
            domain TEXT NOT NULL DEFAULT '',
            skills_json TEXT NOT NULL DEFAULT '[]',
            mode TEXT NOT NULL DEFAULT 'core',
            created_at INTEGER NOT NULL,
            FOREIGN KEY(parent_user_id) REFERENCES users(id),
            FOREIGN KEY(child_user_id) REFERENCES users(id)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS practice_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assignment_id INTEGER,
            parent_user_id INTEGER NOT NULL,
            child_user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            name TEXT NOT NULL,
            domain TEXT NOT NULL DEFAULT '',
            size INTEGER NOT NULL DEFAULT 0,
            skills_count INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(parent_user_id) REFERENCES users(id),
            FOREIGN KEY(child_user_id) REFERENCES users(id)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS plan_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            from_plan TEXT NOT NULL DEFAULT '',
            to_plan TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS payment_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            provider TEXT NOT NULL,
            event_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT '',
            plan_code TEXT NOT NULL DEFAULT '',
            checkout_session_id TEXT NOT NULL DEFAULT '',
            payment_ref TEXT NOT NULL DEFAULT '',
            raw_json TEXT NOT NULL DEFAULT '{}',
            created_at INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS webhook_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            event_id TEXT NOT NULL,
            event_type TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            UNIQUE(provider, event_id)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS plan_usage_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_user_id INTEGER NOT NULL,
            plan_code TEXT NOT NULL,
            child_count INTEGER NOT NULL,
            assigned_count INTEGER NOT NULL,
            due_count INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(parent_user_id) REFERENCES users(id)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version TEXT NOT NULL UNIQUE,
            applied_at INTEGER NOT NULL
        );
    """,
]

SCHEMA_POSTGRES = [
    """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL DEFAULT '',
            role TEXT NOT NULL DEFAULT 'child',
            parent_user_id INTEGER REFERENCES users(id),
            token TEXT UNIQUE NOT NULL,
            token_issued_at INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            daily_goal INTEGER NOT NULL DEFAULT 30,
            streak_days INTEGER NOT NULL DEFAULT 0,
            last_active_day INTEGER NOT NULL DEFAULT 0,
            coins INTEGER NOT NULL DEFAULT 0,
            plan_code TEXT NOT NULL DEFAULT 'starter',
            plan_status TEXT NOT NULL DEFAULT 'active',
            plan_started_at INTEGER NOT NULL DEFAULT 0,
            plan_pending_since INTEGER NOT NULL DEFAULT 0,
            plan_pending_target TEXT NOT NULL DEFAULT '',
            plan_checkout_session_id TEXT NOT NULL DEFAULT '',
            plan_payment_ref TEXT NOT NULL DEFAULT ''
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS attempts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            qid TEXT NOT NULL,
            domain TEXT NOT NULL,
            skill TEXT NOT NULL,
            correct INTEGER NOT NULL,
            time_ms INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            answer TEXT NOT NULL,
            correct_answer TEXT NOT NULL
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS qprog (
            user_id INTEGER NOT NULL REFERENCES users(id),
            qid TEXT NOT NULL,
            ef DOUBLE PRECISION NOT NULL DEFAULT 2.5,
            reps INTEGER NOT NULL DEFAULT 0,
            interval_days DOUBLE PRECISION NOT NULL DEFAULT 0,
            due_at INTEGER NOT NULL DEFAULT 0,
            last_result INTEGER NOT NULL DEFAULT 0,
            lapses INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(user_id, qid)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS purchases (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            item_id TEXT NOT NULL,
            item_name TEXT NOT NULL,
            cost INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS equipped_items (
            user_id INTEGER NOT NULL REFERENCES users(id),
            kind TEXT NOT NULL,
            item_id TEXT NOT NULL,
            item_name TEXT NOT NULL DEFAULT '',
            equipped_at INTEGER NOT NULL,
            PRIMARY KEY(user_id, kind)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS assigned_practices (
            id SERIAL PRIMARY KEY,
            parent_user_id INTEGER NOT NULL REFERENCES users(id),
            child_user_id INTEGER NOT NULL REFERENCES users(id),
            name TEXT NOT NULL,
            size INTEGER NOT NULL,
            domain TEXT NOT NULL DEFAULT '',
            skills_json TEXT NOT NULL DEFAULT '[]',
            mode TEXT NOT NULL DEFAULT 'core',
            created_at INTEGER NOT NULL
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS practice_events (
            id SERIAL PRIMARY KEY,
            assignment_id INTEGER,
            parent_user_id INTEGER NOT NULL REFERENCES users(id),
            child_user_id INTEGER NOT NULL REFERENCES users(id),
            event_type TEXT NOT NULL,
            name TEXT NOT NULL,
            domain TEXT NOT NULL DEFAULT '',
            size INTEGER NOT NULL DEFAULT 0,
            skills_count INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS plan_events (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            event_type TEXT NOT NULL,
            from_plan TEXT NOT NULL DEFAULT '',
            to_plan TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS payment_events (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            provider TEXT NOT NULL,
            event_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT '',
            plan_code TEXT NOT NULL DEFAULT '',
            checkout_session_id TEXT NOT NULL DEFAULT '',
            payment_ref TEXT NOT NULL DEFAULT '',
            raw_json TEXT NOT NULL DEFAULT '{}',
            created_at INTEGER NOT NULL
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS webhook_events (
            id SERIAL PRIMARY KEY,
            provider TEXT NOT NULL,
            event_id TEXT NOT NULL,
            event_type TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            UNIQUE(provider, event_id)
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS plan_usage_records (
            id SERIAL PRIMARY KEY,
            parent_user_id INTEGER NOT NULL REFERENCES users(id),
            plan_code TEXT NOT NULL,
            child_count INTEGER NOT NULL,
            assigned_count INTEGER NOT NULL,
            due_count INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );
    """,
    """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at INTEGER NOT NULL
        );
    """,
]


def init_db() -> None:
    conn = db()
    cur = db_cursor(conn)
    schema = SCHEMA_POSTGRES if USE_POSTGRES else SCHEMA_SQLITE
    for stmt in schema:
        cur.execute(stmt)

    if not USE_POSTGRES:
        cur.execute("PRAGMA table_info(users)")
        user_cols = {str(r[1]) for r in cur.fetchall()}
        if "password_hash" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN password_hash TEXT NOT NULL DEFAULT ''")
        if "role" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'child'")
        if "parent_user_id" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN parent_user_id INTEGER")
        if "daily_goal" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN daily_goal INTEGER NOT NULL DEFAULT 30")
        if "token_issued_at" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN token_issued_at INTEGER NOT NULL DEFAULT 0")
        if "streak_days" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN streak_days INTEGER NOT NULL DEFAULT 0")
        if "last_active_day" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN last_active_day INTEGER NOT NULL DEFAULT 0")
        if "coins" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN coins INTEGER NOT NULL DEFAULT 0")
        if "plan_code" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN plan_code TEXT NOT NULL DEFAULT 'starter'")
        if "plan_status" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN plan_status TEXT NOT NULL DEFAULT 'active'")
        if "plan_started_at" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN plan_started_at INTEGER NOT NULL DEFAULT 0")
        if "plan_pending_since" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN plan_pending_since INTEGER NOT NULL DEFAULT 0")
        if "plan_pending_target" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN plan_pending_target TEXT NOT NULL DEFAULT ''")
        if "plan_checkout_session_id" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN plan_checkout_session_id TEXT NOT NULL DEFAULT ''")
        if "plan_payment_ref" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN plan_payment_ref TEXT NOT NULL DEFAULT ''")

        cur.execute("PRAGMA table_info(attempts)")
        attempt_cols = {str(r[1]) for r in cur.fetchall()}
        if "domain" not in attempt_cols:
            cur.execute("ALTER TABLE attempts ADD COLUMN domain TEXT NOT NULL DEFAULT 'general'")
        if "answer" not in attempt_cols:
            cur.execute("ALTER TABLE attempts ADD COLUMN answer TEXT NOT NULL DEFAULT ''")
        if "correct_answer" not in attempt_cols:
            cur.execute("ALTER TABLE attempts ADD COLUMN correct_answer TEXT NOT NULL DEFAULT ''")

        cur.execute("UPDATE users SET role='child' WHERE role IS NULL OR TRIM(role)=''" )
        cur.execute("PRAGMA table_info(assigned_practices)")
        ap_cols = {str(r[1]) for r in cur.fetchall()}
        if "mode" not in ap_cols:
            cur.execute("ALTER TABLE assigned_practices ADD COLUMN mode TEXT NOT NULL DEFAULT 'core'")
    else:
        cur.execute("UPDATE users SET role='child' WHERE role IS NULL OR TRIM(role)='' ")
        cur.execute(
            """
            UPDATE users
            SET plan_status='active'
            WHERE role='child' AND COALESCE(TRIM(plan_status),'')='' AND plan_status IS NOT NULL
            """
        )
        cur.execute(
            """
            UPDATE users
            SET plan_status='active'
            WHERE role='child' AND plan_status='none'
            """
        )

    migration_sql = (
        "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?,?)"
        if not USE_POSTGRES
        else "INSERT INTO schema_migrations(version, applied_at) VALUES (%s,%s) ON CONFLICT(version) DO NOTHING"
    )
    now_ts = int(time.time())
    cur.execute(migration_sql, ("2026_03_baseline", now_ts))
    cur.execute(migration_sql, ("2026_03_payment_webhook_idempotency", now_ts))

    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            qid TEXT NOT NULL,
            domain TEXT NOT NULL,
            skill TEXT NOT NULL,
            correct INTEGER NOT NULL,
            time_ms INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            answer TEXT NOT NULL,
            correct_answer TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
    """)

    # Per-user per-question spaced repetition (SM-2)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS qprog (
            user_id INTEGER NOT NULL,
            qid TEXT NOT NULL,
            ef REAL NOT NULL DEFAULT 2.5,
            reps INTEGER NOT NULL DEFAULT 0,
            interval_days REAL NOT NULL DEFAULT 0,
            due_at INTEGER NOT NULL DEFAULT 0,
            last_result INTEGER NOT NULL DEFAULT 0,
            lapses INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(user_id, qid),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            item_id TEXT NOT NULL,
            item_name TEXT NOT NULL,
            cost INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS equipped_items (
            user_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            item_id TEXT NOT NULL,
            item_name TEXT NOT NULL DEFAULT '',
            equipped_at INTEGER NOT NULL,
            PRIMARY KEY(user_id, kind),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS assigned_practices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_user_id INTEGER NOT NULL,
            child_user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            size INTEGER NOT NULL,
            domain TEXT NOT NULL DEFAULT '',
            skills_json TEXT NOT NULL DEFAULT '[]',
            mode TEXT NOT NULL DEFAULT 'core',
            created_at INTEGER NOT NULL,
            FOREIGN KEY(parent_user_id) REFERENCES users(id),
            FOREIGN KEY(child_user_id) REFERENCES users(id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS practice_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assignment_id INTEGER,
            parent_user_id INTEGER NOT NULL,
            child_user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            name TEXT NOT NULL,
            domain TEXT NOT NULL DEFAULT '',
            size INTEGER NOT NULL DEFAULT 0,
            skills_count INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(parent_user_id) REFERENCES users(id),
            FOREIGN KEY(child_user_id) REFERENCES users(id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS plan_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            from_plan TEXT NOT NULL DEFAULT '',
            to_plan TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
    """)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS payment_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            provider TEXT NOT NULL,
            event_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT '',
            plan_code TEXT NOT NULL DEFAULT '',
            checkout_session_id TEXT NOT NULL DEFAULT '',
            payment_ref TEXT NOT NULL DEFAULT '',
            raw_json TEXT NOT NULL DEFAULT '{}',
            created_at INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS webhook_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            event_id TEXT NOT NULL,
            event_type TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            UNIQUE(provider, event_id)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS plan_usage_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_user_id INTEGER NOT NULL,
            plan_code TEXT NOT NULL,
            child_count INTEGER NOT NULL,
            assigned_count INTEGER NOT NULL,
            due_count INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(parent_user_id) REFERENCES users(id)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version TEXT NOT NULL UNIQUE,
            applied_at INTEGER NOT NULL
        );
        """
    )

    # Lightweight migrations for older local DBs
    cur.execute("PRAGMA table_info(users)")
    user_cols = {str(r[1]) for r in cur.fetchall()}
    if "password_hash" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN password_hash TEXT NOT NULL DEFAULT ''")
    if "role" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'child'")
    if "parent_user_id" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN parent_user_id INTEGER")
    if "daily_goal" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN daily_goal INTEGER NOT NULL DEFAULT 30")
    if "token_issued_at" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN token_issued_at INTEGER NOT NULL DEFAULT 0")
    if "streak_days" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN streak_days INTEGER NOT NULL DEFAULT 0")
    if "last_active_day" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN last_active_day INTEGER NOT NULL DEFAULT 0")
    if "coins" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN coins INTEGER NOT NULL DEFAULT 0")
    if "plan_code" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN plan_code TEXT NOT NULL DEFAULT 'starter'")
    if "plan_status" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN plan_status TEXT NOT NULL DEFAULT 'active'")
    if "plan_started_at" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN plan_started_at INTEGER NOT NULL DEFAULT 0")
    if "plan_pending_since" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN plan_pending_since INTEGER NOT NULL DEFAULT 0")
    if "plan_pending_target" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN plan_pending_target TEXT NOT NULL DEFAULT ''")
    if "plan_checkout_session_id" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN plan_checkout_session_id TEXT NOT NULL DEFAULT ''")
    if "plan_payment_ref" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN plan_payment_ref TEXT NOT NULL DEFAULT ''")

    # Migrate older attempts tables from earlier app versions.
    cur.execute("PRAGMA table_info(attempts)")
    attempt_cols = {str(r[1]) for r in cur.fetchall()}
    if "domain" not in attempt_cols:
        cur.execute("ALTER TABLE attempts ADD COLUMN domain TEXT NOT NULL DEFAULT 'general'")
    if "answer" not in attempt_cols:
        cur.execute("ALTER TABLE attempts ADD COLUMN answer TEXT NOT NULL DEFAULT ''")
    if "correct_answer" not in attempt_cols:
        cur.execute("ALTER TABLE attempts ADD COLUMN correct_answer TEXT NOT NULL DEFAULT ''")

    # Ensure legacy rows have a valid role value.
    cur.execute("UPDATE users SET role='child' WHERE role IS NULL OR TRIM(role)=''")
    cur.execute("PRAGMA table_info(assigned_practices)")
    ap_cols = {str(r[1]) for r in cur.fetchall()}
    if "mode" not in ap_cols:
        cur.execute("ALTER TABLE assigned_practices ADD COLUMN mode TEXT NOT NULL DEFAULT 'core'")
    cur.execute("UPDATE users SET token_issued_at=created_at WHERE COALESCE(token_issued_at,0)=0")
    cur.execute("UPDATE users SET plan_code='starter' WHERE COALESCE(TRIM(plan_code),'')=''")
    cur.execute("UPDATE users SET plan_status='active' WHERE COALESCE(TRIM(plan_status),'')=''")
    cur.execute("UPDATE users SET plan_status='active' WHERE plan_status NOT IN ('active','pending','none')")
    cur.execute("UPDATE users SET plan_pending_since=0 WHERE plan_pending_since IS NULL OR plan_pending_since<0")
    cur.execute("UPDATE users SET plan_started_at=created_at WHERE COALESCE(plan_started_at,0)=0 AND plan_status='active'")
    cur.execute("UPDATE users SET plan_pending_target='' WHERE plan_status!='pending'")
    cur.execute("UPDATE users SET plan_status='active' WHERE role='child' AND plan_status='none'")
    # Migration tracking: keeps schema evolution explicit for safer rollbacks.
    cur.execute("INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?,?)", ("2026_03_baseline", int(time.time())))
    cur.execute("INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?,?)", ("2026_03_payment_webhook_idempotency", int(time.time())))

    conn.commit()
    conn.close()

def make_token() -> str:
    return "t_" + secrets.token_urlsafe(TOKEN_BYTES)


def normalize_username(username: str) -> str:
    return username.strip().lower()


def normalize_email(email: str) -> str:
    return email.strip().lower()


def validate_email(email: str) -> None:
    cleaned = email.strip().lower()
    if "@" not in cleaned or "." not in cleaned.split("@")[-1]:
        raise HTTPException(status_code=400, detail="Please enter a valid email address")


def validate_username(username: str) -> None:
    cleaned = username.replace("_", "").replace("-", "")
    if not cleaned.isalnum():
        raise HTTPException(status_code=400, detail="Username must be letters/numbers/_/- only")


def validate_password_strength(password: str) -> None:
    if not password:
        raise HTTPException(status_code=400, detail="Password is required")
    if len(password) < PASSWORD_MIN_LENGTH:
        raise HTTPException(status_code=400, detail=f"Password must be at least {PASSWORD_MIN_LENGTH} characters")
    has_upper = any(ch.isupper() for ch in password)
    has_digit = any(ch.isdigit() for ch in password)
    if not (has_upper and has_digit):
        raise HTTPException(
            status_code=400,
            detail="Password must include at least one uppercase letter and one number",
        )


def normalize_practice_mode(mode: Optional[str]) -> str:
    m = str(mode or "core").strip().lower()
    if m not in ALLOWED_PRACTICE_MODES:
        return "core"
    return m


def sanitize_answer_payload(answer: str, time_ms: int) -> Tuple[str, int]:
    cleaned_answer = str(answer or "").strip()
    if len(cleaned_answer) > ANSWER_MAX_LENGTH:
        raise HTTPException(status_code=400, detail="Answer is too long")
    safe_time_ms = clamp_int(int(time_ms or 0), 0, MAX_TIME_MS)
    return cleaned_answer, safe_time_ms


def normalize_role(role: str) -> str:
    role_norm = (role or "child").strip().lower()
    if role_norm not in {"parent", "child"}:
        raise HTTPException(status_code=400, detail="Role must be parent or child")
    return role_norm


def normalize_plan_code(plan_code: str) -> str:
    code = str(plan_code or "").strip().lower()
    if code not in PLAN_CATALOG:
        raise HTTPException(status_code=400, detail="Invalid plan_code")
    return code


def plan_rank(plan_code: str) -> int:
    code = normalize_plan_code(plan_code)
    return PLAN_ORDER.index(code)


def plan_capabilities_for_code(plan_code: str) -> Dict[str, Any]:
    code = str(plan_code or "starter").strip().lower()
    if code not in PLAN_CATALOG:
        code = "starter"
    raw = dict(PLAN_CATALOG[code].get("capabilities") or {})
    modes_raw = raw.get("allowed_modes", ["core"])
    modes = [str(m).strip().lower() for m in (modes_raw if isinstance(modes_raw, list) else ["core"])]
    modes = [m for m in modes if m in ALLOWED_PRACTICE_MODES]
    if not modes:
        modes = ["core"]
    return {
        "max_practice_size": clamp_int(int(raw.get("max_practice_size", 20)), 5, 200),
        "allowed_modes": modes,
        "parent_report": bool(raw.get("parent_report", False)),
        "max_children": clamp_int(int(raw.get("max_children", 1)), 1, 20),
    }


def payment_enabled() -> bool:
    return PAYMENT_PROVIDER == "stripe" and bool(STRIPE_SECRET_KEY)


def pricing_mode() -> str:
    return "live" if payment_enabled() else "simulated"


def base_url_join(path: str) -> str:
    p = str(path or "").strip()
    if not p:
        p = "/"
    if p.startswith("http://") or p.startswith("https://"):
        return p
    return APP_BASE_URL.rstrip("/") + (p if p.startswith("/") else f"/{p}")


def stripe_signing_secret_available() -> bool:
    return bool(STRIPE_WEBHOOK_SECRET)


def resolve_token_from_request(token: Optional[str] = Query(None), authorization: Optional[str] = Header(None)) -> str:
    if token:
        return token
    if authorization:
        parts = str(authorization).split()
        if len(parts) >= 2 and parts[0].lower() == "bearer":
            return parts[1]
    raise HTTPException(status_code=401, detail="Authentication token is required")


def stripe_create_checkout_session(
    *,
    user: sqlite3.Row,
    target_plan: str,
    current_plan: str,
    action: str,
) -> Dict[str, Any]:
    if not payment_enabled():
        raise HTTPException(status_code=400, detail="Live payment is not enabled")
    lookup_key = STRIPE_PRICE_LOOKUP_KEYS.get(target_plan, "")
    if not lookup_key:
        raise HTTPException(status_code=400, detail=f"Stripe price lookup key is missing for plan '{target_plan}'")

    metadata = {
        "user_id": str(int(user["id"])),
        "target_plan": str(target_plan),
        "current_plan": str(current_plan),
        "action": str(action),
    }
    body_fields = [
        ("mode", "subscription"),
        ("success_url", base_url_join(STRIPE_SUCCESS_PATH)),
        ("cancel_url", base_url_join(STRIPE_CANCEL_PATH)),
        ("client_reference_id", str(int(user["id"]))),
        ("line_items[0][price]", lookup_key),
        ("line_items[0][quantity]", "1"),
    ]
    email_val = str(user["username"] or "")
    if "@" in email_val:
        body_fields.append(("customer_email", email_val))
    for k, v in metadata.items():
        body_fields.append((f"metadata[{k}]", v))

    encoded = urllib.parse.urlencode(body_fields).encode("utf-8")
    req = urllib.request.Request(
        f"{STRIPE_API_BASE}/checkout/sessions",
        data=encoded,
        method="POST",
        headers={
            "Authorization": f"Bearer {STRIPE_SECRET_KEY}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        LOGGER.exception("stripe_checkout_create_failed user_id=%s target_plan=%s", int(user["id"]), target_plan)
        raise HTTPException(status_code=502, detail="Unable to create Stripe checkout session") from exc
    if not isinstance(payload, dict) or not payload.get("id") or not payload.get("url"):
        raise HTTPException(status_code=502, detail="Invalid Stripe checkout response")
    return payload


def log_payment_event(
    *,
    user_id: int,
    provider: str,
    event_type: str,
    status: str,
    plan_code: str,
    checkout_session_id: str = "",
    payment_ref: str = "",
    raw_payload: Optional[Dict[str, Any]] = None,
) -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO payment_events(
            user_id, provider, event_type, status, plan_code, checkout_session_id, payment_ref, raw_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            int(user_id),
            str(provider or ""),
            str(event_type or ""),
            str(status or ""),
            str(plan_code or ""),
            str(checkout_session_id or ""),
            str(payment_ref or ""),
            json.dumps(raw_payload or {}, ensure_ascii=False),
            int(time.time()),
        ),
    )
    conn.commit()
    conn.close()


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        PBKDF2_ITERATIONS,
    )
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt}${dk.hex()}"


def password_hash_needs_upgrade(stored_hash: str) -> bool:
    if not stored_hash:
        return True
    if not stored_hash.startswith("pbkdf2_sha256$"):
        return True
    parts = stored_hash.split("$", 3)
    if len(parts) != 4:
        return True
    try:
        return int(parts[1]) < PBKDF2_ITERATIONS
    except Exception:
        return True


def verify_password(stored_hash: str, password: str) -> bool:
    if not stored_hash:
        return False
    if stored_hash.startswith("pbkdf2_sha256$"):
        parts = stored_hash.split("$", 3)
        if len(parts) != 4:
            return False
        _, iters_str, salt, digest = parts
        try:
            iters = int(iters_str)
        except Exception:
            return False
        check = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            iters,
        ).hex()
        return hmac.compare_digest(digest, check)

    # Legacy local format: salt$sha256(salt+password)
    if "$" not in stored_hash:
        return False
    salt, digest = stored_hash.split("$", 1)
    check = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
    return hmac.compare_digest(digest, check)


def login_guard_key(username: str) -> str:
    return normalize_username(username or "")


def check_login_guard(username: str) -> None:
    now = time.time()
    key = login_guard_key(username)
    if not key:
        return
    state = LOGIN_GUARD.get(key)
    if not state:
        return
    lock_until = float(state.get("lock_until", 0.0) or 0.0)
    if lock_until > now:
        wait_s = int(max(1, lock_until - now))
        raise HTTPException(status_code=429, detail=f"Too many login attempts. Try again in {wait_s}s.")
    # Drop stale state outside the rolling window.
    first_fail = float(state.get("first_fail", 0.0) or 0.0)
    if first_fail and (now - first_fail) > LOGIN_WINDOW_SECONDS:
        LOGIN_GUARD.pop(key, None)


def record_login_result(username: str, success: bool) -> None:
    now = time.time()
    key = login_guard_key(username)
    if not key:
        return
    if success:
        LOGIN_GUARD.pop(key, None)
        return
    state = LOGIN_GUARD.get(key, {"fails": 0.0, "first_fail": now, "lock_until": 0.0})
    first_fail = float(state.get("first_fail", now) or now)
    if (now - first_fail) > LOGIN_WINDOW_SECONDS:
        state = {"fails": 0.0, "first_fail": now, "lock_until": 0.0}
    state["fails"] = float(state.get("fails", 0.0) or 0.0) + 1.0
    state["first_fail"] = float(state.get("first_fail", now) or now)
    if state["fails"] >= LOGIN_MAX_FAILS:
        state["lock_until"] = now + LOGIN_LOCK_SECONDS
        state["fails"] = 0.0
        state["first_fail"] = now
    LOGIN_GUARD[key] = state


def level_band_for_difficulty(difficulty: int) -> str:
    d = clamp_int(int(difficulty or 1), 1, 7)
    if d <= 2:
        return "Level 1"
    if d <= 3:
        return "Level 2"
    if d <= 4:
        return "Level 3"
    if d <= 5:
        return "Level 4"
    return "Level 5"


CURRICULUM_SKILL_META: Dict[Tuple[str, str], Dict[str, str]] = {
    ("maths", "mental_subtraction"): {
        "tag": "11+ Maths: Arithmetic Fluency",
        "stage": "KS2 + 11+ Preparation",
        "strand": "Number and Arithmetic",
        "objective": "Subtract confidently with multi-digit numbers",
        "year": "Year 4-6",
        "topic": "Maths",
        "subtopic": "Mental subtraction",
    },
    ("maths", "percentages"): {
        "tag": "11+ Maths: Percentages",
        "stage": "KS2 + 11+ Preparation",
        "strand": "Fractions, Decimals and Percentages",
        "objective": "Find percentages of quantities quickly and accurately",
        "year": "Year 4-6",
        "topic": "Maths",
        "subtopic": "Percentages",
    },
    ("maths", "fractions_of_amounts"): {
        "tag": "11+ Maths: Fractions",
        "stage": "KS2 + 11+ Preparation",
        "strand": "Fractions",
        "objective": "Calculate fractions of amounts using structured steps",
        "year": "Year 4-6",
        "topic": "Maths",
        "subtopic": "Fractions of amounts",
    },
    ("maths", "number_sequence"): {
        "tag": "11+ Maths: Sequences",
        "stage": "KS2 + 11+ Preparation",
        "strand": "Patterns and Algebraic Thinking",
        "objective": "Identify and extend number patterns",
        "year": "Year 4-6",
        "topic": "Maths",
        "subtopic": "Number sequences",
    },
    ("maths", "multi_step_word_problem"): {
        "tag": "11+ Maths: Problem Solving",
        "stage": "KS2 + 11+ Preparation",
        "strand": "Problem Solving",
        "objective": "Solve multi-step worded maths scenarios",
        "year": "Year 4-6",
        "topic": "Maths",
        "subtopic": "Multi-step word problems",
    },
    ("verbal_reasoning", "analogy"): {
        "tag": "11+ Verbal Reasoning: Analogies",
        "stage": "KS2 + 11+ Preparation",
        "strand": "Word Relationships",
        "objective": "Recognize relation patterns between word pairs",
        "year": "Year 4-6",
        "topic": "Verbal reasoning",
        "subtopic": "Analogies",
    },
    ("verbal_reasoning", "letter_code"): {
        "tag": "11+ Verbal Reasoning: Codes",
        "stage": "KS2 + 11+ Preparation",
        "strand": "Code and Sequence Logic",
        "objective": "Decode and encode letter-shift rules",
        "year": "Year 4-6",
        "topic": "Verbal reasoning",
        "subtopic": "Letter codes",
    },
    ("verbal_reasoning", "odd_one_out"): {
        "tag": "11+ Verbal Reasoning: Classification",
        "stage": "KS2 + 11+ Preparation",
        "strand": "Classification",
        "objective": "Identify category membership and mismatches",
        "year": "Year 4-6",
        "topic": "Verbal reasoning",
        "subtopic": "Odd one out",
    },
    ("non_verbal_reasoning", "pattern_sequence"): {
        "tag": "11+ NVR: Pattern Sequences",
        "stage": "KS2 + 11+ Preparation",
        "strand": "Pattern Recognition",
        "objective": "Continue visual or symbolic sequences",
        "year": "Year 4-6",
        "topic": "Non-verbal reasoning",
        "subtopic": "Pattern sequences",
    },
    ("non_verbal_reasoning", "rotation"): {
        "tag": "11+ NVR: Rotations",
        "stage": "KS2 + 11+ Preparation",
        "strand": "Spatial Reasoning",
        "objective": "Track orientation after clockwise and anticlockwise turns",
        "year": "Year 4-6",
        "topic": "Non-verbal reasoning",
        "subtopic": "Rotations",
    },
    ("non_verbal_reasoning", "matrix_rule"): {
        "tag": "11+ NVR: Matrix Rules",
        "stage": "KS2 + 11+ Preparation",
        "strand": "Matrix Logic",
        "objective": "Infer row-column rules in grid puzzles",
        "year": "Year 4-6",
        "topic": "Non-verbal reasoning",
        "subtopic": "Matrix rules",
    },
    ("comprehension", "inference_and_retrieval"): {
        "tag": "11+ English: Comprehension",
        "stage": "KS2 + 11+ Preparation",
        "strand": "Reading Comprehension",
        "objective": "Retrieve explicit evidence and infer meaning from text",
        "year": "Year 4-6",
        "topic": "English",
        "subtopic": "Inference and retrieval",
    },
}


def curriculum_metadata_for_question(q: Dict[str, Any]) -> Dict[str, str]:
    domain = str(q.get("domain", "general") or "general").strip().lower()
    skill = str(q.get("skill", "unknown") or "unknown").strip().lower()
    fallback = {
        "tag": f"KS2/11+ {domain.replace('_', ' ').title()}",
        "stage": "KS2 + 11+ Preparation",
        "strand": domain.replace("_", " ").title(),
        "objective": "Core practice",
        "year": "Year 4-6",
        "topic": domain.replace("_", " ").title(),
        "subtopic": skill.replace("_", " ").title(),
    }
    preset = CURRICULUM_SKILL_META.get((domain, skill), fallback)
    return {
        "curriculum_tag": str(q.get("curriculum_tag") or preset["tag"]),
        "curriculum_stage": str(q.get("curriculum_stage") or preset["stage"]),
        "curriculum_strand": str(q.get("curriculum_strand") or preset["strand"]),
        "curriculum_objective": str(q.get("curriculum_objective") or preset["objective"]),
        "curriculum_year": str(q.get("curriculum_year") or preset["year"]),
        "curriculum_topic": str(q.get("curriculum_topic") or preset["topic"]),
        "curriculum_subtopic": str(q.get("curriculum_subtopic") or preset["subtopic"]),
    }


def curriculum_tag_for_question(q: Dict[str, Any]) -> str:
    return curriculum_metadata_for_question(q)["curriculum_tag"]


def user_by_token(token: str) -> sqlite3.Row:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE token=?", (token,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=401, detail="Invalid token (please login again).")
    issued = int((row["token_issued_at"] if "token_issued_at" in row.keys() else 0) or 0)
    if issued and (int(time.time()) - issued) > TOKEN_TTL_SECONDS:
        raise HTTPException(status_code=401, detail="Session expired. Please login again.")
    return row


def user_by_id(user_id: int) -> Optional[sqlite3.Row]:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id=?", (int(user_id),))
    row = cur.fetchone()
    conn.close()
    return row


def log_plan_event(
    user_id: int,
    event_type: str,
    from_plan: str = "",
    to_plan: str = "",
    status: str = "",
) -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO plan_events(user_id, event_type, from_plan, to_plan, status, created_at) VALUES (?,?,?,?,?,?)",
        (
            int(user_id),
            str(event_type),
            str(from_plan or ""),
            str(to_plan or ""),
            str(status or ""),
            int(time.time()),
        ),
    )
    conn.commit()
    conn.close()


def maybe_auto_approve_parent_plan(parent_user_id: int) -> None:
    if payment_enabled():
        return
    now = int(time.time())
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, role, plan_code, plan_status, plan_pending_target, plan_pending_since FROM users WHERE id=?",
        (int(parent_user_id),),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return
    if str(row["role"] or "child") != "parent":
        conn.close()
        return
    status = str(row["plan_status"] or "active")
    if status != "pending":
        conn.close()
        return
    pending_since = int(row["plan_pending_since"] or 0)
    target = str(row["plan_pending_target"] or "").strip().lower()
    current = str(row["plan_code"] or "starter").strip().lower()
    if target not in PLAN_CATALOG:
        cur.execute(
            "UPDATE users SET plan_status='active', plan_pending_since=0, plan_pending_target='' WHERE id=?",
            (int(parent_user_id),),
        )
        conn.commit()
        conn.close()
        return
    if pending_since <= 0:
        pending_since = now
    if (now - pending_since) < PLAN_AUTO_APPROVE_SECONDS:
        conn.close()
        return
    cur.execute(
        "UPDATE users SET plan_code=?, plan_status='active', plan_started_at=?, plan_pending_since=0, plan_pending_target='' WHERE id=?",
        (target, now, int(parent_user_id)),
    )
    conn.commit()
    conn.close()
    log_plan_event(int(parent_user_id), "upgrade_auto_approved", current, target, "active")


def _pending_seconds_remaining(status: str, pending_since: int) -> int:
    if status != "pending":
        return 0
    if pending_since <= 0:
        return PLAN_AUTO_APPROVE_SECONDS
    return max(0, PLAN_AUTO_APPROVE_SECONDS - (int(time.time()) - int(pending_since)))


def plan_snapshot_for_user(user: sqlite3.Row) -> Dict[str, Any]:
    role = str(user["role"] or "child")
    uid = int(user["id"])
    if role == "parent":
        maybe_auto_approve_parent_plan(uid)
        refreshed = user_by_id(uid) or user
        plan_code = str(refreshed["plan_code"] or "starter").strip().lower()
        if plan_code not in PLAN_CATALOG:
            plan_code = "starter"
        plan_status = str(refreshed["plan_status"] or "active").strip().lower()
        if plan_status not in {"active", "pending", "none"}:
            plan_status = "active"
        pending_target = str(refreshed["plan_pending_target"] or "")
        pending_since = int(refreshed["plan_pending_since"] or 0)
        return {
            "code": plan_code,
            "status": plan_status,
            "pending_target": pending_target if plan_status == "pending" else "",
            "pending_seconds_remaining": _pending_seconds_remaining(plan_status, pending_since),
            "read_only": False,
            "owner_user_id": uid,
            "needs_selection": plan_status == "none",
        }

    parent_id = int(user["parent_user_id"]) if user["parent_user_id"] is not None else 0
    if parent_id <= 0:
        return {
            "code": "starter",
            "status": "none",
            "pending_target": "",
            "pending_seconds_remaining": 0,
            "read_only": True,
            "owner_user_id": None,
            "needs_selection": False,
        }
    maybe_auto_approve_parent_plan(parent_id)
    parent_row = user_by_id(parent_id)
    if not parent_row:
        return {
            "code": "starter",
            "status": "none",
            "pending_target": "",
            "pending_seconds_remaining": 0,
            "read_only": True,
            "owner_user_id": parent_id,
            "needs_selection": False,
        }
    parent_code = str(parent_row["plan_code"] or "starter").strip().lower()
    if parent_code not in PLAN_CATALOG:
        parent_code = "starter"
    parent_status = str(parent_row["plan_status"] or "active").strip().lower()
    if parent_status not in {"active", "pending", "none"}:
        parent_status = "active"
    parent_pending_target = str(parent_row["plan_pending_target"] or "")
    parent_pending_since = int(parent_row["plan_pending_since"] or 0)
    return {
        "code": parent_code,
        "status": parent_status,
        "pending_target": parent_pending_target if parent_status == "pending" else "",
        "pending_seconds_remaining": _pending_seconds_remaining(parent_status, parent_pending_since),
        "read_only": True,
        "owner_user_id": parent_id,
        "needs_selection": False,
    }


def parent_plan_capabilities(user: sqlite3.Row) -> Dict[str, Any]:
    require_parent(user)
    snap = plan_snapshot_for_user(user)
    return plan_capabilities_for_code(str(snap.get("code", "starter")))


def plan_catalog_payload() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for idx, code in enumerate(PLAN_ORDER):
        item = dict(PLAN_CATALOG[code])
        item["order"] = idx
        items.append(item)
    return items


def require_parent(user: sqlite3.Row) -> None:
    if str(user["role"] or "child") != "parent":
        raise HTTPException(status_code=403, detail="Parent account required")


def require_parent_plan_selected(user: sqlite3.Row) -> None:
    require_parent(user)
    snap = plan_snapshot_for_user(user)
    if str(snap.get("status", "none")) == "none":
        raise HTTPException(status_code=403, detail="Please choose a plan to continue")


STORE_ITEMS: List[Dict[str, Any]] = [
    {"id": "sticker_star", "name": "Star Sticker", "cost": 250, "kind": "sticker"},
    {"id": "sticker_rocket", "name": "Rocket Sticker", "cost": 450, "kind": "sticker"},
    {"id": "sticker_moon", "name": "Moon Sticker", "cost": 550, "kind": "sticker"},
    {"id": "sticker_trophy", "name": "Trophy Sticker", "cost": 700, "kind": "sticker"},
    {"id": "sticker_lightning", "name": "Lightning Sticker", "cost": 850, "kind": "sticker"},
    {"id": "badge_bronze", "name": "Bronze Badge", "cost": 600, "kind": "badge"},
    {"id": "badge_silver", "name": "Silver Badge", "cost": 1200, "kind": "badge"},
    {"id": "badge_gold", "name": "Gold Badge", "cost": 2400, "kind": "badge"},
    {"id": "pet_egg", "name": "Mini Pet Egg", "cost": 900, "kind": "pet"},
    {"id": "pet_cat", "name": "Pixel Cat", "cost": 1600, "kind": "pet"},
    {"id": "pet_dog", "name": "Pocket Pup", "cost": 1700, "kind": "pet"},
    {"id": "pet_owl", "name": "Study Owl", "cost": 2200, "kind": "pet"},
    {"id": "pet_dragon", "name": "Tiny Dragon", "cost": 4800, "kind": "pet"},
    {"id": "theme_ocean", "name": "Ocean Theme", "cost": 1200, "kind": "theme"},
    {"id": "theme_space", "name": "Space Theme", "cost": 1800, "kind": "theme"},
    {"id": "theme_forest", "name": "Forest Theme", "cost": 1900, "kind": "theme"},
    {"id": "theme_sunrise", "name": "Sunrise Theme", "cost": 2100, "kind": "theme"},
    {"id": "theme_arcade", "name": "Arcade Theme", "cost": 2600, "kind": "theme"},
    {"id": "theme_royal", "name": "Royal Theme", "cost": 4200, "kind": "theme"},
    {"id": "trail_spark", "name": "Spark Trail", "cost": 1500, "kind": "effect"},
    {"id": "trail_comet", "name": "Comet Trail", "cost": 2600, "kind": "effect"},
    {"id": "trail_rainbow", "name": "Rainbow Trail", "cost": 3900, "kind": "effect"},
    {"id": "desk_pencil", "name": "Pencil Desk Set", "cost": 1100, "kind": "desk"},
    {"id": "desk_planet", "name": "Planet Desk Set", "cost": 2400, "kind": "desk"},
    {"id": "desk_castle", "name": "Castle Desk Set", "cost": 5200, "kind": "desk"},
    {"id": "crown_gold", "name": "Gold Crown", "cost": 3000, "kind": "cosmetic"},
    {"id": "cape_hero", "name": "Hero Cape", "cost": 3500, "kind": "cosmetic"},
    {"id": "glasses_genius", "name": "Genius Glasses", "cost": 2800, "kind": "cosmetic"},
    {"id": "crown_crystal", "name": "Crystal Crown", "cost": 6500, "kind": "cosmetic"},
    {"id": "throne_quibly", "name": "Quibbly Throne", "cost": 9500, "kind": "legendary"},
]

# Expanded store catalog for a wider selection.
STORE_ITEMS.extend([
    {"id": "sticker_comet", "name": "Comet Sticker", "cost": 950, "kind": "sticker"},
    {"id": "sticker_puzzle", "name": "Puzzle Sticker", "cost": 980, "kind": "sticker"},
    {"id": "sticker_bookworm", "name": "Bookworm Sticker", "cost": 1050, "kind": "sticker"},
    {"id": "sticker_quill", "name": "Quill Sticker", "cost": 1150, "kind": "sticker"},
    {"id": "sticker_crown", "name": "Crown Sticker", "cost": 1300, "kind": "sticker"},
    {"id": "sticker_globe", "name": "Globe Sticker", "cost": 1450, "kind": "sticker"},
    {"id": "sticker_magnet", "name": "Magnet Sticker", "cost": 1550, "kind": "sticker"},
    {"id": "sticker_volcano", "name": "Volcano Sticker", "cost": 1750, "kind": "sticker"},
    {"id": "sticker_chess", "name": "Chess Sticker", "cost": 1900, "kind": "sticker"},
    {"id": "sticker_aurora", "name": "Aurora Sticker", "cost": 2200, "kind": "sticker"},
    {"id": "badge_focus", "name": "Focus Badge", "cost": 1800, "kind": "badge"},
    {"id": "badge_velocity", "name": "Velocity Badge", "cost": 2100, "kind": "badge"},
    {"id": "badge_precision", "name": "Precision Badge", "cost": 2600, "kind": "badge"},
    {"id": "badge_streak", "name": "Streak Badge", "cost": 3000, "kind": "badge"},
    {"id": "badge_platinum", "name": "Platinum Badge", "cost": 3200, "kind": "badge"},
    {"id": "badge_diamond", "name": "Diamond Badge", "cost": 4200, "kind": "badge"},
    {"id": "badge_mastery", "name": "Mastery Badge", "cost": 5600, "kind": "badge"},
    {"id": "badge_quibbly_elite", "name": "Quibbly Elite Badge", "cost": 7000, "kind": "badge"},
    {"id": "pet_fox", "name": "Clever Fox", "cost": 2100, "kind": "pet"},
    {"id": "pet_rabbit", "name": "Rapid Rabbit", "cost": 2300, "kind": "pet"},
    {"id": "pet_penguin", "name": "Polar Penguin", "cost": 2600, "kind": "pet"},
    {"id": "pet_panda", "name": "Puzzle Panda", "cost": 2900, "kind": "pet"},
    {"id": "pet_koala", "name": "Calm Koala", "cost": 3100, "kind": "pet"},
    {"id": "pet_unicorn", "name": "Study Unicorn", "cost": 5200, "kind": "pet"},
    {"id": "pet_phoenix", "name": "Phoenix Buddy", "cost": 6800, "kind": "pet"},
    {"id": "pet_robot", "name": "Robot Rover", "cost": 7400, "kind": "pet"},
    {"id": "pet_whale", "name": "Wisdom Whale", "cost": 8800, "kind": "pet"},
    {"id": "theme_library", "name": "Library Theme", "cost": 2300, "kind": "theme"},
    {"id": "theme_laboratory", "name": "Laboratory Theme", "cost": 2500, "kind": "theme"},
    {"id": "theme_neon_city", "name": "Neon City Theme", "cost": 2800, "kind": "theme"},
    {"id": "theme_coral_reef", "name": "Coral Reef Theme", "cost": 3000, "kind": "theme"},
    {"id": "theme_autumn", "name": "Autumn Theme", "cost": 3200, "kind": "theme"},
    {"id": "theme_mountain", "name": "Mountain Theme", "cost": 3500, "kind": "theme"},
    {"id": "theme_midnight", "name": "Midnight Theme", "cost": 3800, "kind": "theme"},
    {"id": "theme_garden", "name": "Garden Theme", "cost": 4000, "kind": "theme"},
    {"id": "theme_honeycomb", "name": "Honeycomb Theme", "cost": 4400, "kind": "theme"},
    {"id": "theme_galaxy", "name": "Galaxy Theme", "cost": 5200, "kind": "theme"},
    {"id": "trail_stardust", "name": "Stardust Trail", "cost": 1800, "kind": "effect"},
    {"id": "trail_ember", "name": "Ember Trail", "cost": 2100, "kind": "effect"},
    {"id": "trail_leaf", "name": "Leaf Trail", "cost": 2400, "kind": "effect"},
    {"id": "trail_wave", "name": "Wave Trail", "cost": 2700, "kind": "effect"},
    {"id": "trail_binary", "name": "Binary Trail", "cost": 3200, "kind": "effect"},
    {"id": "trail_firefly", "name": "Firefly Trail", "cost": 3600, "kind": "effect"},
    {"id": "trail_prism", "name": "Prism Trail", "cost": 4300, "kind": "effect"},
    {"id": "trail_meteor", "name": "Meteor Trail", "cost": 5200, "kind": "effect"},
    {"id": "desk_science", "name": "Science Desk Set", "cost": 1800, "kind": "desk"},
    {"id": "desk_music", "name": "Music Desk Set", "cost": 2000, "kind": "desk"},
    {"id": "desk_ocean", "name": "Ocean Desk Set", "cost": 2300, "kind": "desk"},
    {"id": "desk_vintage", "name": "Vintage Desk Set", "cost": 2700, "kind": "desk"},
    {"id": "desk_orbit", "name": "Orbit Desk Set", "cost": 3300, "kind": "desk"},
    {"id": "desk_jungle", "name": "Jungle Desk Set", "cost": 3900, "kind": "desk"},
    {"id": "desk_crystal", "name": "Crystal Desk Set", "cost": 4700, "kind": "desk"},
    {"id": "desk_aurora", "name": "Aurora Desk Set", "cost": 6200, "kind": "desk"},
    {"id": "hat_wizard", "name": "Wizard Hat", "cost": 2600, "kind": "cosmetic"},
    {"id": "hat_astronaut", "name": "Astronaut Helmet", "cost": 3400, "kind": "cosmetic"},
    {"id": "hat_graduate", "name": "Graduate Cap", "cost": 4200, "kind": "cosmetic"},
    {"id": "cape_comet", "name": "Comet Cape", "cost": 4100, "kind": "cosmetic"},
    {"id": "mask_fox", "name": "Fox Mask", "cost": 2900, "kind": "cosmetic"},
    {"id": "headphones_gamer", "name": "Gamer Headphones", "cost": 3600, "kind": "cosmetic"},
    {"id": "pin_lightbulb", "name": "Lightbulb Pin", "cost": 1900, "kind": "cosmetic"},
    {"id": "boots_jet", "name": "Jet Boots", "cost": 5000, "kind": "cosmetic"},
    {"id": "gloves_arcade", "name": "Arcade Gloves", "cost": 3100, "kind": "cosmetic"},
    {"id": "crown_solar", "name": "Solar Crown", "cost": 7800, "kind": "cosmetic"},
    {"id": "frame_oak", "name": "Oak Frame", "cost": 1500, "kind": "frame"},
    {"id": "frame_cloud", "name": "Cloud Frame", "cost": 2600, "kind": "frame"},
    {"id": "frame_silver", "name": "Silver Frame", "cost": 2400, "kind": "frame"},
    {"id": "frame_gold", "name": "Gold Frame", "cost": 3400, "kind": "frame"},
    {"id": "frame_neon", "name": "Neon Frame", "cost": 4200, "kind": "frame"},
    {"id": "frame_royal", "name": "Royal Frame", "cost": 5200, "kind": "frame"},
    {"id": "frame_hologram", "name": "Hologram Frame", "cost": 6800, "kind": "frame"},
    {"id": "frame_lava", "name": "Lava Frame", "cost": 7600, "kind": "frame"},
    {"id": "banner_daily_grind", "name": "Daily Grind Banner", "cost": 1400, "kind": "banner"},
    {"id": "banner_top_scorer", "name": "Top Scorer Banner", "cost": 2600, "kind": "banner"},
    {"id": "banner_streak_star", "name": "Streak Star Banner", "cost": 3200, "kind": "banner"},
    {"id": "banner_problem_crusher", "name": "Problem Crusher Banner", "cost": 3800, "kind": "banner"},
    {"id": "banner_speed_runner", "name": "Speed Runner Banner", "cost": 4300, "kind": "banner"},
    {"id": "banner_precision_pro", "name": "Precision Pro Banner", "cost": 5000, "kind": "banner"},
    {"id": "banner_master_scholar", "name": "Master Scholar Banner", "cost": 6200, "kind": "banner"},
    {"id": "banner_quibbly_legend", "name": "Quibbly Legend Banner", "cost": 8200, "kind": "banner"},
    {"id": "emote_thumbsup", "name": "Thumbs Up Emote", "cost": 600, "kind": "emote"},
    {"id": "emote_brainwave", "name": "Brainwave Emote", "cost": 900, "kind": "emote"},
    {"id": "emote_confetti", "name": "Confetti Emote", "cost": 1200, "kind": "emote"},
    {"id": "emote_fire", "name": "Fire Emote", "cost": 1600, "kind": "emote"},
    {"id": "emote_lightning", "name": "Lightning Emote", "cost": 2100, "kind": "emote"},
    {"id": "emote_rocket", "name": "Rocket Emote", "cost": 2700, "kind": "emote"},
    {"id": "emote_sparkle", "name": "Sparkle Emote", "cost": 3300, "kind": "emote"},
    {"id": "emote_trophy", "name": "Trophy Emote", "cost": 3900, "kind": "emote"},
    {"id": "buddy_notebook", "name": "Notebook Buddy", "cost": 2500, "kind": "buddy"},
    {"id": "buddy_calculator", "name": "Calculator Buddy", "cost": 3000, "kind": "buddy"},
    {"id": "buddy_globe", "name": "Globe Buddy", "cost": 3600, "kind": "buddy"},
    {"id": "buddy_compass", "name": "Compass Buddy", "cost": 4200, "kind": "buddy"},
    {"id": "buddy_satellite", "name": "Satellite Buddy", "cost": 5200, "kind": "buddy"},
    {"id": "buddy_crystal_orb", "name": "Crystal Orb Buddy", "cost": 6400, "kind": "buddy"},
    {"id": "buddy_timekeeper", "name": "Timekeeper Buddy", "cost": 7800, "kind": "buddy"},
    {"id": "buddy_guardian_bot", "name": "Guardian Bot Buddy", "cost": 9200, "kind": "buddy"},
    {"id": "legend_arcane_library", "name": "Arcane Library", "cost": 12500, "kind": "legendary"},
    {"id": "legend_starlight_throne", "name": "Starlight Throne", "cost": 16000, "kind": "legendary"},
    {"id": "legend_quantum_cape", "name": "Quantum Cape", "cost": 18500, "kind": "legendary"},
    {"id": "legend_dragon_companion", "name": "Dragon Companion", "cost": 21000, "kind": "legendary"},
    {"id": "legend_celestial_halo", "name": "Celestial Halo", "cost": 24000, "kind": "legendary"},
    {"id": "legend_time_crown", "name": "Time Crown", "cost": 27500, "kind": "legendary"},
    {"id": "legend_founders_banner", "name": "Founders Banner", "cost": 30000, "kind": "legendary"},
    {"id": "legend_infinity_frame", "name": "Infinity Frame", "cost": 34000, "kind": "legendary"},
    {"id": "legend_aurora_palace", "name": "Aurora Palace", "cost": 38000, "kind": "legendary"},
    {"id": "legend_mythic_vault", "name": "Mythic Vault", "cost": 42000, "kind": "legendary"},
])

# Additional, less-childish catalog options with stronger clothing/theme/workspace coverage.
STORE_ITEMS.extend([
    {"id": "clothing_charcoal_blazer", "name": "Charcoal Blazer", "cost": 2600, "kind": "clothing"},
    {"id": "clothing_navy_blazer", "name": "Navy Blazer", "cost": 2800, "kind": "clothing"},
    {"id": "clothing_minimal_hoodie", "name": "Minimal Hoodie", "cost": 1800, "kind": "clothing"},
    {"id": "clothing_merino_sweater", "name": "Merino Sweater", "cost": 2200, "kind": "clothing"},
    {"id": "clothing_oxford_white", "name": "Oxford Shirt (White)", "cost": 2000, "kind": "clothing"},
    {"id": "clothing_oxford_blue", "name": "Oxford Shirt (Blue)", "cost": 2000, "kind": "clothing"},
    {"id": "clothing_track_jacket", "name": "Graphite Track Jacket", "cost": 2300, "kind": "clothing"},
    {"id": "clothing_wool_coat", "name": "Wool Coat", "cost": 3600, "kind": "clothing"},
    {"id": "clothing_trench_stone", "name": "Stone Trench Coat", "cost": 3400, "kind": "clothing"},
    {"id": "clothing_chinos_sand", "name": "Sand Chinos", "cost": 1700, "kind": "clothing"},
    {"id": "clothing_tailored_trousers", "name": "Tailored Trousers", "cost": 2100, "kind": "clothing"},
    {"id": "clothing_clean_sneakers", "name": "Clean Sneakers", "cost": 1900, "kind": "clothing"},
    {"id": "clothing_leather_boots", "name": "Leather Boots", "cost": 3200, "kind": "clothing"},
    {"id": "clothing_runner_trainers", "name": "Runner Trainers", "cost": 2400, "kind": "clothing"},
    {"id": "clothing_silk_scarf", "name": "Silk Scarf", "cost": 1400, "kind": "clothing"},
    {"id": "clothing_steel_watch", "name": "Steel Watch", "cost": 4200, "kind": "clothing"},
    {"id": "clothing_leather_belt", "name": "Leather Belt", "cost": 1250, "kind": "clothing"},
    {"id": "clothing_canvas_tote", "name": "Canvas Tote", "cost": 1300, "kind": "clothing"},
    {"id": "clothing_crossbody_bag", "name": "Crossbody Bag", "cost": 2300, "kind": "clothing"},
    {"id": "clothing_round_frames", "name": "Round Frames", "cost": 2100, "kind": "clothing"},
    {"id": "clothing_rect_frames", "name": "Rectangular Frames", "cost": 2100, "kind": "clothing"},
    {"id": "clothing_silver_ring", "name": "Silver Ring", "cost": 1500, "kind": "clothing"},
    {"id": "clothing_minimal_bracelet", "name": "Minimal Bracelet", "cost": 1450, "kind": "clothing"},
    {"id": "clothing_cufflinks", "name": "Cufflinks", "cost": 1950, "kind": "clothing"},
    {"id": "theme_monochrome_studio", "name": "Monochrome Studio Theme", "cost": 2800, "kind": "theme"},
    {"id": "theme_sandstone", "name": "Sandstone Theme", "cost": 2600, "kind": "theme"},
    {"id": "theme_nordic_mist", "name": "Nordic Mist Theme", "cost": 2900, "kind": "theme"},
    {"id": "theme_ink_and_paper", "name": "Ink and Paper Theme", "cost": 2700, "kind": "theme"},
    {"id": "theme_terracotta", "name": "Terracotta Theme", "cost": 3100, "kind": "theme"},
    {"id": "theme_emerald_office", "name": "Emerald Office Theme", "cost": 3200, "kind": "theme"},
    {"id": "theme_midnight_blueprint", "name": "Midnight Blueprint Theme", "cost": 3400, "kind": "theme"},
    {"id": "theme_concrete_lounge", "name": "Concrete Lounge Theme", "cost": 3600, "kind": "theme"},
    {"id": "theme_sage_notebook", "name": "Sage Notebook Theme", "cost": 3800, "kind": "theme"},
    {"id": "theme_brass_wood", "name": "Brass and Wood Theme", "cost": 4000, "kind": "theme"},
    {"id": "theme_rain_window", "name": "Rain Window Theme", "cost": 4300, "kind": "theme"},
    {"id": "theme_black_gold", "name": "Black and Gold Theme", "cost": 4600, "kind": "theme"},
    {"id": "theme_aurora_glass", "name": "Aurora Glass Theme", "cost": 5000, "kind": "theme"},
    {"id": "theme_coffee_bar", "name": "Coffee Bar Theme", "cost": 3400, "kind": "theme"},
    {"id": "theme_slate_chrome", "name": "Slate Chrome Theme", "cost": 4700, "kind": "theme"},
    {"id": "theme_zen_garden", "name": "Zen Garden Theme", "cost": 3900, "kind": "theme"},
    {"id": "theme_city_dusk", "name": "City Dusk Theme", "cost": 4100, "kind": "theme"},
    {"id": "workspace_brass_lamp", "name": "Brass Desk Lamp", "cost": 1700, "kind": "workspace"},
    {"id": "workspace_monitor_arm", "name": "Monitor Arm", "cost": 2300, "kind": "workspace"},
    {"id": "workspace_keyboard_oak", "name": "Oak Keyboard Tray", "cost": 2400, "kind": "workspace"},
    {"id": "workspace_notebook_stack", "name": "Notebook Stack", "cost": 950, "kind": "workspace"},
    {"id": "workspace_pen_roll", "name": "Leather Pen Roll", "cost": 1100, "kind": "workspace"},
    {"id": "workspace_wireless_pad", "name": "Wireless Charger Pad", "cost": 1900, "kind": "workspace"},
    {"id": "workspace_ambient_strip", "name": "Ambient Light Strip", "cost": 1650, "kind": "workspace"},
    {"id": "workspace_cork_board", "name": "Cork Board", "cost": 1300, "kind": "workspace"},
    {"id": "workspace_fountain_pen", "name": "Fountain Pen", "cost": 1750, "kind": "workspace"},
    {"id": "workspace_bookend_metal", "name": "Metal Bookend Pair", "cost": 2100, "kind": "workspace"},
    {"id": "workspace_tablet_stand", "name": "Tablet Stand", "cost": 1500, "kind": "workspace"},
    {"id": "workspace_wool_desk_mat", "name": "Wool Desk Mat", "cost": 1800, "kind": "workspace"},
    {"id": "clothing_linen_shirt", "name": "Linen Shirt", "cost": 1900, "kind": "clothing"},
    {"id": "clothing_cashmere_crew", "name": "Cashmere Crewneck", "cost": 3300, "kind": "clothing"},
    {"id": "clothing_denim_jacket", "name": "Denim Jacket", "cost": 2400, "kind": "clothing"},
    {"id": "clothing_bomber_jacket", "name": "Bomber Jacket", "cost": 2800, "kind": "clothing"},
    {"id": "clothing_polo_slate", "name": "Slate Polo", "cost": 1500, "kind": "clothing"},
    {"id": "clothing_polo_cream", "name": "Cream Polo", "cost": 1500, "kind": "clothing"},
    {"id": "clothing_loafers", "name": "Leather Loafers", "cost": 2700, "kind": "clothing"},
    {"id": "clothing_wool_trousers", "name": "Wool Trousers", "cost": 2400, "kind": "clothing"},
    {"id": "clothing_travel_backpack", "name": "Travel Backpack", "cost": 2600, "kind": "clothing"},
    {"id": "clothing_minimal_cap", "name": "Minimal Cap", "cost": 1200, "kind": "clothing"},
    {"id": "clothing_tie_set", "name": "Tie Set", "cost": 1450, "kind": "clothing"},
    {"id": "clothing_tailored_vest", "name": "Tailored Vest", "cost": 2100, "kind": "clothing"},
    {"id": "clothing_weekend_shorts", "name": "Weekend Shorts", "cost": 1300, "kind": "clothing"},
    {"id": "clothing_canvas_jacket", "name": "Canvas Jacket", "cost": 2300, "kind": "clothing"},
    {"id": "theme_studio_gray", "name": "Studio Gray Theme", "cost": 2500, "kind": "theme"},
    {"id": "theme_oak_noir", "name": "Oak Noir Theme", "cost": 3200, "kind": "theme"},
    {"id": "theme_silver_mist", "name": "Silver Mist Theme", "cost": 3000, "kind": "theme"},
    {"id": "workspace_desk_clock", "name": "Desk Clock", "cost": 1200, "kind": "workspace"},
    {"id": "workspace_felt_organizer", "name": "Felt Organizer", "cost": 1400, "kind": "workspace"},
    {"id": "workspace_book_light", "name": "Book Light", "cost": 900, "kind": "workspace"},
])

# Re-label older categories to a more neutral tone and make all store prices cheaper.
for _item in STORE_ITEMS:
    _kind = str(_item.get("kind") or "").strip().lower()
    if _kind == "sticker":
        _item["kind"] = "decal"
        _name = str(_item.get("name") or "").replace("Sticker", "Decal")
        _item["name"] = _name or "Decal"
    elif _kind == "pet":
        _item["kind"] = "companion"
        _name = str(_item.get("name") or "")
        _name = _name.replace("Pet", "Companion").replace("Pup", "Companion")
        if "Companion" not in _name:
            _name = f"{_name} Companion".strip()
        _item["name"] = _name
    elif _kind == "desk":
        _item["kind"] = "workspace"

    try:
        _raw_cost = int(_item.get("cost") or 0)
    except Exception:
        _raw_cost = 0
    _item["cost"] = max(STORE_PRICE_MIN, int(round(_raw_cost * STORE_PRICE_MULTIPLIER)))

# Keep the visible catalog focused on non-childish options.
STORE_VISIBLE_KINDS = {"clothing", "theme", "workspace", "frame"}
STORE_ITEMS = [item for item in STORE_ITEMS if str(item.get("kind") or "").strip().lower() in STORE_VISIBLE_KINDS]

def touch_activity(user_id: int) -> None:
    today = utc_day_id()
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT streak_days, last_active_day FROM users WHERE id=?", (user_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return

    streak = int(row["streak_days"])
    last_day = int(row["last_active_day"])

    if last_day == today:
        # already counted today
        conn.close()
        return

    if last_day == today - 1:
        streak += 1
    else:
        streak = 1  # reset, but starting a new streak

    cur.execute(
        "UPDATE users SET streak_days=?, last_active_day=? WHERE id=?",
        (streak, today, user_id)
    )
    conn.commit()
    conn.close()

# -------------------- Questions --------------------
def load_questions() -> Dict[str, Dict[str, Any]]:
    global QUESTION_QUALITY_REPORT
    if not QUESTIONS_PATH.exists():
        raise RuntimeError("questions.json not found next to app.py")
    raw = json.loads(QUESTIONS_PATH.read_text(encoding="utf-8"))

    # Support both legacy schema:
    #   { "questions": [...] }
    # and new grouped schema:
    #   { "7_plus": [...], "5_plus": [...] }
    source_items: List[Dict[str, Any]] = []
    if isinstance(raw, dict) and isinstance(raw.get("questions"), list):
        source_items = list(raw.get("questions", []))
    elif isinstance(raw, dict):
        for bucket in ("7_plus", "5_plus"):
            items = raw.get(bucket, [])
            if isinstance(items, list):
                source_items.extend(items)

    qmap: Dict[str, Dict[str, Any]] = {}
    quality: Dict[str, Any] = {
        "source_items": len(source_items),
        "loaded_items": 0,
        "invalid_items": 0,
        "duplicate_ids": 0,
        "duplicate_stems": 0,
        "fixed_answer_indexes": 0,
        "fixed_explanations": 0,
        "warnings": [],
    }
    seen_stems: set[str] = set()

    for original in source_items:
        if not isinstance(original, dict):
            quality["invalid_items"] += 1
            continue
        q = dict(original)

        # Normalize new schema field names for the existing app logic.
        if "question" in q and "stem" not in q:
            q["stem"] = q["question"]
        if "options" in q and "choices" not in q:
            q["choices"] = q["options"]

        qid = str(q.get("id", "")).strip()
        if not qid:
            quality["invalid_items"] += 1
            continue
        if qid in qmap:
            quality["duplicate_ids"] += 1
            quality["invalid_items"] += 1
            continue

        qtype = str(q.get("type", "mcq") or "mcq").strip().lower()
        if qtype not in {"mcq", "short"}:
            quality["invalid_items"] += 1
            continue

        domain = str(q.get("domain", "general") or "general").strip().lower()
        skill = str(q.get("skill", "unknown") or "unknown").strip().lower()
        stem = str(q.get("stem", "") or "").strip()
        if not stem:
            quality["invalid_items"] += 1
            continue
        stem_key = re.sub(r"\\s+", " ", stem.lower())
        if stem_key in seen_stems:
            quality["duplicate_stems"] += 1
        else:
            seen_stems.add(stem_key)

        try:
            difficulty = int(q.get("difficulty", 3))
        except Exception:
            difficulty = 3
        difficulty = max(1, min(7, difficulty))

        try:
            time_limit_s = int(q.get("time_limit_s", 45))
        except Exception:
            time_limit_s = 45
        time_limit_s = max(20, min(180, time_limit_s))

        normalized: Dict[str, Any] = {
            "id": qid,
            "type": qtype,
            "domain": domain,
            "skill": skill,
            "difficulty": difficulty,
            "time_limit_s": time_limit_s,
            "stem": stem,
        }

        if qtype == "mcq":
            raw_choices = q.get("choices", [])
            if not isinstance(raw_choices, list):
                quality["invalid_items"] += 1
                continue
            choices: List[str] = []
            seen_choices: set[str] = set()
            for c in raw_choices:
                t = str(c).strip()
                if not t:
                    continue
                key = t.lower()
                if key in seen_choices:
                    continue
                choices.append(t)
                seen_choices.add(key)
            if len(choices) < 2:
                quality["invalid_items"] += 1
                continue

            raw_answer = str(q.get("answer", "")).strip()
            answer = raw_answer
            if raw_answer.isdigit():
                idx = int(raw_answer) - 1
                if 0 <= idx < len(choices):
                    answer = choices[idx]
                    quality["fixed_answer_indexes"] += 1

            if answer not in choices:
                choice_lookup = {c.lower(): c for c in choices}
                answer = choice_lookup.get(answer.lower(), "")
            if answer not in choices:
                quality["invalid_items"] += 1
                continue

            normalized["choices"] = choices
            normalized["answer"] = answer
        else:
            raw_acc = q.get("accepted_answers", [])
            accepted: List[str] = []
            if isinstance(raw_acc, list):
                for item in raw_acc:
                    t = str(item).strip()
                    if t:
                        accepted.append(t)
            if not accepted:
                fallback_answer = str(q.get("answer", "")).strip()
                if fallback_answer:
                    accepted = [fallback_answer]
            if not accepted:
                quality["invalid_items"] += 1
                continue
            normalized["accepted_answers"] = accepted

        explanation = str(q.get("explanation", "") or "").strip()
        if not explanation:
            explanation = f"Apply the {skill.replace('_', ' ')} rule and verify the result against the options."
            quality["fixed_explanations"] += 1
        normalized["explanation"] = explanation
        normalized.update(curriculum_metadata_for_question(normalized))

        qmap[qid] = normalized
        quality["loaded_items"] += 1

    if quality["duplicate_stems"] > 0:
        quality["warnings"].append(f"{quality['duplicate_stems']} stems have exact text duplicates")
    if quality["invalid_items"] > 0:
        quality["warnings"].append(f"{quality['invalid_items']} items were dropped during validation")
    if quality["duplicate_ids"] > 0:
        quality["warnings"].append(f"{quality['duplicate_ids']} duplicate ids were rejected")
    quality["question_count"] = len(qmap)
    QUESTION_QUALITY_REPORT = quality
    if QUESTION_VALIDATION_STRICT and quality["invalid_items"] > 0:
        raise RuntimeError(
            f"questions.json validation failed: invalid_items={quality['invalid_items']} duplicate_ids={quality['duplicate_ids']}"
        )
    LOGGER.info(
        "questions_loaded count=%d source=%d invalid=%d duplicate_ids=%d duplicate_stems=%d fixed_answer_indexes=%d fixed_explanations=%d",
        int(quality["question_count"]),
        int(quality["source_items"]),
        int(quality["invalid_items"]),
        int(quality["duplicate_ids"]),
        int(quality["duplicate_stems"]),
        int(quality["fixed_answer_indexes"]),
        int(quality["fixed_explanations"]),
    )
    if not qmap:
        raise RuntimeError("No questions in questions.json")
    return qmap

Q = load_questions()
ALL_QIDS = list(Q.keys())

def mark(q: Dict[str, Any], answer: str) -> Tuple[bool, str]:
    qtype = q.get("type")
    if qtype == "mcq":
        raw_correct = str(q.get("answer", "")).strip()
        choices = [str(c) for c in (q.get("choices") or [])]
        submitted = answer.strip()

        # Support both MCQ answer formats:
        # 1) exact choice text ("70p")
        # 2) 1-based option index stored as a string ("1", "2", ...)
        if raw_correct.isdigit():
            idx = int(raw_correct) - 1
            if 0 <= idx < len(choices):
                correct_text = choices[idx]
                return (submitted == correct_text or submitted == raw_correct, correct_text)

        return (submitted == raw_correct, raw_correct)

    if qtype == "short":
        ans = answer.strip().lower()
        accepted = [str(a).strip().lower() for a in q.get("accepted_answers", [])]
        if not accepted:
            return (False, "")
        return (ans in accepted, accepted[0])

    raise HTTPException(status_code=400, detail="Unsupported question type: %s" % qtype)


SKILL_TIPS: Dict[str, str] = {
    "mental_subtraction": "Use place value columns mentally: hundreds, tens, then ones.",
    "percentages": "Convert to 10% or 1%, then scale up to the required percentage.",
    "fractions_of_amounts": "Find one part first, then multiply to the numerator.",
    "number_sequence": "Check first differences, then second differences for hidden rules.",
    "multi_step_word_problem": "Underline each number and solve in ordered mini-steps.",
    "analogy": "Look for the relationship type before checking answer choices.",
    "letter_code": "Map each letter move consistently across the full word.",
    "odd_one_out": "Group three items by one shared property, then remove the mismatch.",
    "pattern_sequence": "Track one feature at a time: shape, count, position, rotation.",
    "rotation": "Visualize quarter-turn steps and test clockwise vs anticlockwise.",
    "matrix_rule": "Compare rows and columns to find the repeated transformation.",
    "inference_and_retrieval": "Separate direct evidence from inference before selecting.",
}

EXPLANATION_FRAMES_BY_SKILL: Dict[str, List[str]] = {
    "mental_subtraction": ["Method:", "Quick method:", "Check:"],
    "percentages": ["Percent method:", "Key step:", "Check:"],
    "fractions_of_amounts": ["Fraction method:", "Key step:", "Check:"],
    "number_sequence": ["Pattern rule:", "Sequence method:", "Check:"],
    "multi_step_word_problem": ["Step plan:", "Method:", "Check:"],
    "analogy": ["Relationship rule:", "Verbal rule:", "Check:"],
    "letter_code": ["Code rule:", "Method:", "Check:"],
    "odd_one_out": ["Category rule:", "Reasoning:", "Check:"],
    "pattern_sequence": ["Visual rule:", "Pattern method:", "Check:"],
    "rotation": ["Rotation rule:", "Direction method:", "Check:"],
    "matrix_rule": ["Matrix rule:", "Grid method:", "Check:"],
    "inference_and_retrieval": ["Evidence rule:", "Reading method:", "Check:"],
}


def diversify_explanation_text(explanation: str, skill: str, correct_answer: str) -> str:
    text = str(explanation or "").strip()
    if not text:
        return text
    frames = EXPLANATION_FRAMES_BY_SKILL.get(str(skill or ""), ["Method:", "Check:", "Reasoning:"])
    frame = random.choice(frames)
    varied = text
    if random.random() < 0.55:
        varied = f"{frame} {text}"
    if correct_answer:
        answer_text = str(correct_answer).strip()
        if answer_text and answer_text.lower() not in varied.lower() and random.random() < 0.40:
            varied = f"{varied} Final answer: {answer_text}."
    return varied


def build_answer_feedback(
    q: Dict[str, Any],
    submitted_answer: str,
    correct: bool,
    correct_answer: str,
    time_ms: int,
) -> Dict[str, str]:
    domain = str(q.get("domain", "general"))
    skill = str(q.get("skill", "unknown"))
    tip = SKILL_TIPS.get(skill, "Break the question into smaller checks before choosing.")

    if time_ms <= 20000:
        speed_band = "fast"
    elif time_ms <= 45000:
        speed_band = "steady"
    else:
        speed_band = "careful"

    if correct:
        if speed_band == "fast":
            next_action = "Strong pace. Try stretch mode for harder items."
        elif speed_band == "careful":
            next_action = "Correct with careful thinking. Aim to keep accuracy and trim time slightly."
        else:
            next_action = "Good control. Keep this pace for consistency."
        mistake_check = "Check complete: your reasoning matched the target rule."
    else:
        if domain == "maths":
            mistake_check = "Likely slip: arithmetic or order-of-operations check missed."
        elif domain == "verbal_reasoning":
            mistake_check = "Likely slip: relationship type changed between stem and choice."
        elif domain == "non_verbal_reasoning":
            mistake_check = "Likely slip: one visual rule (rotation/shape/count) was skipped."
        elif domain == "comprehension":
            mistake_check = "Likely slip: answer not fully supported by passage evidence."
        else:
            mistake_check = "Likely slip: one key condition was not applied."
        next_action = f"Re-read the stem, then eliminate two options before selecting '{correct_answer}'."

    submitted_clean = str(submitted_answer).strip()
    if not correct and submitted_clean:
        tip = f"{tip} You picked '{submitted_clean}', so compare it directly against '{correct_answer}'."

    return {
        "coach_tip": tip,
        "mistake_check": mistake_check,
        "next_action": next_action,
        "speed_band": speed_band,
    }


# -------------------- Spaced repetition (SM-2-ish) --------------------
def sm2_update(ef: float, reps: int, interval_days: float, correct: bool) -> Tuple[float, int, float, int]:
    """
    Simplified SM-2 update.
    quality:
      correct -> 4
      wrong   -> 1
    """
    q = 4 if correct else 1

    # update ease factor
    ef_new = ef + (0.1 - (5 - q) * (0.08 + (5 - q) * 0.02))
    if ef_new < 1.3:
        ef_new = 1.3
    if ef_new > 2.8:
        ef_new = 2.8

    lapses_add = 0

    if correct:
        reps_new = reps + 1
        if reps_new == 1:
            interval_new = 1.0
        elif reps_new == 2:
            interval_new = 3.0
        else:
            # main growth
            interval_new = max(1.0, interval_days * ef_new)
    else:
        # reset repetition, schedule sooner
        lapses_add = 1
        reps_new = 0
        interval_new = 0.5  # 12 hours-ish
    return ef_new, reps_new, interval_new, lapses_add

def ensure_qprog_row(user_id: int, qid: str) -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM qprog WHERE user_id=? AND qid=?", (user_id, qid))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO qprog(user_id, qid, ef, reps, interval_days, due_at, last_result, lapses) VALUES (?,?,?,?,?,?,?,?)",
            (user_id, qid, 2.5, 0, 0.0, 0, 0, 0)
        )
        conn.commit()
    conn.close()

def update_qprog(user_id: int, qid: str, correct: bool) -> Optional[float]:
    ensure_qprog_row(user_id, qid)
    now = int(time.time())

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT ef, reps, interval_days, due_at, lapses FROM qprog WHERE user_id=? AND qid=?", (user_id, qid))
    r = cur.fetchone()
    ef = float(r["ef"])
    reps = int(r["reps"])
    interval_days = float(r["interval_days"])
    lapses = int(r["lapses"])

    ef2, reps2, interval2, lapses_add = sm2_update(ef, reps, interval_days, correct)
    due_at = now + int(interval2 * 86400)

    cur.execute("""
        UPDATE qprog
        SET ef=?, reps=?, interval_days=?, due_at=?, last_result=?, lapses=?
        WHERE user_id=? AND qid=?
    """, (ef2, reps2, interval2, due_at, 1 if correct else 0, lapses + lapses_add, user_id, qid))

    conn.commit()
    conn.close()

    return interval2

def due_count(user_id: int) -> int:
    now = int(time.time())
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS n FROM qprog WHERE user_id=? AND due_at>0 AND due_at<=?", (user_id, now))
    n = int(cur.fetchone()["n"] or 0)
    conn.close()
    return n


def clamp_int(value: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(value)))


def derive_calibrated_difficulty(base_difficulty: int, attempts: int, accuracy: float, avg_time_ms: int) -> int:
    d = clamp_int(int(base_difficulty or 3), 1, 7)
    if attempts < QUESTION_CALIBRATION_MIN_ATTEMPTS:
        return d

    if accuracy <= 0.42:
        d += 2
    elif accuracy <= 0.58:
        d += 1
    elif accuracy >= 0.92 and avg_time_ms > 0 and avg_time_ms <= 22000:
        d -= 2
    elif accuracy >= 0.86 and avg_time_ms > 0 and avg_time_ms <= 32000:
        d -= 1

    if avg_time_ms > 0:
        if avg_time_ms >= 75000:
            d += 1
        elif avg_time_ms <= 15000 and accuracy >= 0.80:
            d -= 1
    return clamp_int(d, 1, 7)


def effective_difficulty_for_qid(qid: str, q: Optional[Dict[str, Any]] = None) -> int:
    item = q if q is not None else Q.get(str(qid))
    base = clamp_int(int((item or {}).get("difficulty", 3)), 1, 7)
    meta = QUESTION_CALIBRATION.get(str(qid), {})
    attempts = int(meta.get("attempts", 0) or 0)
    if attempts < QUESTION_CALIBRATION_MIN_ATTEMPTS:
        return base
    return clamp_int(int(meta.get("difficulty", base) or base), 1, 7)


def rebuild_question_calibration() -> None:
    global QUESTION_CALIBRATION
    if not DB_PATH.exists():
        QUESTION_CALIBRATION = {}
        return

    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT qid, COUNT(*) AS n, AVG(correct) AS acc, AVG(CASE WHEN time_ms>0 THEN time_ms END) AS avg_ms
            FROM attempts
            GROUP BY qid
            """
        )
        rows = cur.fetchall()
    except sqlite3.Error:
        conn.close()
        QUESTION_CALIBRATION = {}
        return
    conn.close()

    calibrated: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        qid = str(r["qid"] or "")
        q = Q.get(qid)
        if not q:
            continue
        attempts = int(r["n"] or 0)
        accuracy = float(r["acc"] or 0.0)
        avg_ms = int(float(r["avg_ms"] or 0.0)) if r["avg_ms"] is not None else 0
        base = clamp_int(int(q.get("difficulty", 3)), 1, 7)
        calibrated[qid] = {
            "attempts": attempts,
            "accuracy": accuracy,
            "avg_time_ms": avg_ms,
            "difficulty": derive_calibrated_difficulty(base, attempts, accuracy, avg_ms),
        }
    QUESTION_CALIBRATION = calibrated
    LOGGER.info("question_calibration_rebuilt rows=%d calibrated=%d", len(rows), len(calibrated))


def refresh_question_calibration_for_qid(qid: str) -> None:
    qid_norm = str(qid or "").strip()
    if not qid_norm:
        return
    q = Q.get(qid_norm)
    if not q:
        return

    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) AS n, AVG(correct) AS acc, AVG(CASE WHEN time_ms>0 THEN time_ms END) AS avg_ms
            FROM attempts
            WHERE qid=?
            """,
            (qid_norm,),
        )
        row = cur.fetchone()
    except sqlite3.Error:
        conn.close()
        return
    conn.close()
    if not row:
        return

    attempts = int(row["n"] or 0)
    if attempts <= 0:
        QUESTION_CALIBRATION.pop(qid_norm, None)
        return
    accuracy = float(row["acc"] or 0.0)
    avg_ms = int(float(row["avg_ms"] or 0.0)) if row["avg_ms"] is not None else 0
    base = clamp_int(int(q.get("difficulty", 3)), 1, 7)
    QUESTION_CALIBRATION[qid_norm] = {
        "attempts": attempts,
        "accuracy": accuracy,
        "avg_time_ms": avg_ms,
        "difficulty": derive_calibrated_difficulty(base, attempts, accuracy, avg_ms),
    }


def calibrated_recommended_difficulty(
    recent_rows: List[sqlite3.Row],
    recent_acc: float,
    recent_avg_time_ms: int,
    base_difficulty: int,
) -> int:
    target = clamp_int(base_difficulty, 1, 7)
    if not recent_rows:
        return target

    buckets: Dict[int, Dict[str, Any]] = {}
    for r in recent_rows:
        qid = str(r["qid"] or "")
        q = Q.get(qid)
        if not q:
            continue
        d = effective_difficulty_for_qid(qid, q)
        b = buckets.setdefault(d, {"n": 0, "correct": 0, "times": []})
        b["n"] += 1
        b["correct"] += int(r["correct"] or 0)
        t = int(r["time_ms"] or 0)
        if t > 0:
            b["times"].append(t)

    passing_levels: List[int] = []
    for d in range(1, 8):
        b = buckets.get(d)
        if not b or int(b["n"]) < 3:
            continue
        acc = float(b["correct"]) / float(max(1, b["n"]))
        avg_ms = int(sum(b["times"]) / len(b["times"])) if b["times"] else 0
        if acc >= 0.78 and (avg_ms == 0 or avg_ms <= 55000):
            passing_levels.append(d)
    if passing_levels:
        target = max(passing_levels)

    current_bucket = buckets.get(target)
    if current_bucket and int(current_bucket["n"]) >= 4:
        curr_acc = float(current_bucket["correct"]) / float(max(1, current_bucket["n"]))
        if curr_acc < 0.55:
            target -= 1

    if len(recent_rows) >= 8:
        if recent_acc >= 0.90 and (recent_avg_time_ms == 0 or recent_avg_time_ms <= 28000):
            target += 1
        elif recent_acc < 0.55:
            target -= 1

    return clamp_int(target, 1, 7)


def build_progress_snapshot(user_id: int) -> Dict[str, Any]:
    """
    Lightweight progression profile used for adaptive selection and parent reports.
    It is computed from attempts + question metadata and calibrated by live outcomes.
    Backward compatibility is preserved for older schemas because difficulty still
    has a stable base value in questions.json.
    """
    now = int(time.time())
    day_start = utc_day_id(now) * 86400
    week_start = now - (7 * 86400)

    conn = db()
    cur = conn.cursor()

    cur.execute("""
        SELECT qid, domain, skill, correct, time_ms, created_at
        FROM attempts
        WHERE user_id=?
        ORDER BY created_at DESC
        LIMIT 50
    """, (user_id,))
    recent_rows = cur.fetchall()

    cur.execute("""
        SELECT domain, COUNT(*) AS n, AVG(correct) AS acc, AVG(time_ms) AS avg_ms
        FROM attempts
        WHERE user_id=?
        GROUP BY domain
        ORDER BY n DESC
        LIMIT 8
    """, (user_id,))
    domain_rows = cur.fetchall()

    cur.execute("""
        SELECT COUNT(*) AS attempts_7d, AVG(correct) AS acc_7d,
               AVG(CASE WHEN correct=1 AND time_ms>0 THEN time_ms END) AS avg_time_correct_7d
        FROM attempts
        WHERE user_id=? AND created_at>=?
    """, (user_id, week_start))
    week_row = cur.fetchone()

    cur.execute("""
        SELECT COUNT(*) AS correct_today
        FROM attempts
        WHERE user_id=? AND created_at>=? AND correct=1
    """, (user_id, day_start))
    correct_today = int((cur.fetchone() or {"correct_today": 0})["correct_today"] or 0)

    conn.close()

    recent_count = len(recent_rows)
    recent_correct = sum(int(r["correct"] or 0) for r in recent_rows)
    recent_acc = (recent_correct / recent_count) if recent_count else 0.0
    recent_times = [int(r["time_ms"] or 0) for r in recent_rows if int(r["time_ms"] or 0) > 0]
    recent_avg_time = int(sum(recent_times) / len(recent_times)) if recent_times else 0

    # Derive difficulty trend using calibrated per-question difficulty.
    seen_difficulties: List[int] = []
    for r in recent_rows:
        qid = str(r["qid"] or "")
        q = Q.get(qid)
        if not q:
            continue
        d = effective_difficulty_for_qid(qid, q)
        seen_difficulties.append(d)
    base_diff = 3
    if seen_difficulties:
        base_diff = sorted(seen_difficulties)[len(seen_difficulties) // 2]
    recommended_difficulty = calibrated_recommended_difficulty(
        recent_rows=recent_rows,
        recent_acc=recent_acc,
        recent_avg_time_ms=recent_avg_time,
        base_difficulty=base_diff,
    )

    progress_stage = "building"
    if recent_count >= 12 and recent_acc >= 0.80:
        progress_stage = "secure"
    if recent_count >= 18 and recent_acc >= 0.90 and (recent_avg_time == 0 or recent_avg_time <= 25000):
        progress_stage = "stretch"

    domain_progress: List[Dict[str, Any]] = []
    weak_domain = None
    strong_domain = None
    for r in domain_rows:
        acc = float(r["acc"] or 0.0)
        avg_ms = int(float(r["avg_ms"] or 0.0)) if r["avg_ms"] is not None else 0
        recency_bonus = 0.0
        if recent_count:
            recent_domain_rows = [rr for rr in recent_rows if str(rr["domain"] or "general") == str(r["domain"] or "general")]
            if recent_domain_rows:
                recency_bonus = min(0.08, len(recent_domain_rows) / max(1, recent_count) * 0.18)
        domain_mastery = clamp_int(round((acc * 100) + recency_bonus * 100), 0, 100)
        item = {
            "domain": str(r["domain"] or "general"),
            "attempts": int(r["n"] or 0),
            "accuracy": acc,
            "avg_time_ms": avg_ms,
            "mastery_score": domain_mastery,
        }
        domain_progress.append(item)
    if domain_progress:
        weak_domain = min(domain_progress, key=lambda d: (d["accuracy"], -d["attempts"]))["domain"]
        strong_domain = max(domain_progress, key=lambda d: (d["accuracy"], d["attempts"]))["domain"]

    learning_focus = ""
    if weak_domain and recent_count >= 8:
        learning_focus = f"Reinforce {weak_domain}"
    elif progress_stage == "stretch":
        learning_focus = "Increase challenge"
    elif progress_stage == "secure":
        learning_focus = "Keep accuracy stable"
    else:
        learning_focus = "Build steady habits"

    # Composite scores: simple but stable enough for UI guidance.
    total_attempts_weight = min(1.0, recent_count / 20.0)
    speed_component = 0.5
    if recent_avg_time > 0:
        # 15s => strong, 90s => weaker
        speed_component = max(0.0, min(1.0, (90000 - recent_avg_time) / 75000))
    mastery_score = clamp_int(round(
        (recent_acc * 65 + (float((week_row["acc_7d"] if week_row else 0.0) or 0.0) * 15) + (speed_component * 10) + (total_attempts_weight * 10))
    ), 0, 100)
    if progress_stage == "stretch":
        recommended_difficulty = clamp_int(recommended_difficulty + 1, 1, 7)
    elif progress_stage == "building" and recent_count >= 8 and recent_acc < 0.55:
        recommended_difficulty = clamp_int(recommended_difficulty - 1, 1, 7)
    readiness_score = clamp_int(round(
        mastery_score * 0.55
        + min(25, correct_today)
        + min(10, due_count(user_id) * 0.5)
        + (8 if progress_stage == "stretch" else 4 if progress_stage == "secure" else 0)
    ), 0, 100)

    mastery_band = "starting"
    if mastery_score >= 75:
        mastery_band = "confident"
    if mastery_score >= 88:
        mastery_band = "advanced"

    next_recommended_domains: List[str] = []
    if domain_progress:
        sorted_domains = sorted(domain_progress, key=lambda d: (float(d.get("accuracy", 0.0)), -int(d.get("attempts", 0))))
        next_recommended_domains = [str(d["domain"]) for d in sorted_domains[:2]]
        if strong_domain and strong_domain not in next_recommended_domains and progress_stage == "stretch":
            next_recommended_domains.append(str(strong_domain))
            next_recommended_domains = next_recommended_domains[:3]

    return {
        "progress_stage": progress_stage,
        "recommended_difficulty": recommended_difficulty,
        "recent_accuracy": recent_acc,
        "recent_avg_time_ms": recent_avg_time,
        "learning_focus": learning_focus,
        "domain_progress": domain_progress,
        "week_attempts": int((week_row["attempts_7d"] if week_row else 0) or 0),
        "week_accuracy": float((week_row["acc_7d"] if week_row else 0.0) or 0.0),
        "week_avg_time_ms": int(float((week_row["avg_time_correct_7d"] if week_row else 0.0) or 0.0)),
        "correct_today": correct_today,
        "weak_domain": weak_domain or "",
        "strong_domain": strong_domain or "",
        "mastery_score": mastery_score,
        "readiness_score": readiness_score,
        "mastery_band": mastery_band,
        "next_recommended_domains": next_recommended_domains,
    }


def insert_practice_event(
    cur: sqlite3.Cursor,
    *,
    assignment_id: Optional[int],
    parent_user_id: int,
    child_user_id: int,
    event_type: str,
    name: str,
    domain: str,
    size: int,
    skills_count: int,
    created_at: Optional[int] = None,
) -> None:
    cur.execute(
        """
        INSERT INTO practice_events(
            assignment_id, parent_user_id, child_user_id, event_type, name, domain, size, skills_count, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            int(assignment_id) if assignment_id is not None else None,
            int(parent_user_id),
            int(child_user_id),
            str(event_type),
            str(name or "Practice"),
            str(domain or ""),
            int(size or 0),
            int(skills_count or 0),
            int(created_at or time.time()),
        ),
    )

# -------------------- Adaptive selection --------------------
STEM_SIGNATURE_STOPWORDS = {
    "the", "a", "an", "of", "to", "in", "on", "for", "is", "are", "and", "or",
    "what", "which", "how", "find", "calculate", "work", "out", "does", "do",
}

STEM_PREFIX_BY_SKILL: Dict[str, List[str]] = {
    "mental_subtraction": ["Quick subtract:", "Number drill:", "Mental maths:"],
    "percentages": ["Percentage check:", "Percent drill:", "Quick percent:"],
    "fractions_of_amounts": ["Fraction focus:", "Fraction drill:", "Quick fraction:"],
    "number_sequence": ["Sequence challenge:", "Pattern check:", "Number pattern:"],
    "multi_step_word_problem": ["Word problem:", "Step-by-step problem:", "Maths scenario:"],
    "analogy": ["Analogy challenge:", "Word link task:", "Verbal reasoning:"],
    "letter_code": ["Code puzzle:", "Letter code task:", "Code challenge:"],
    "odd_one_out": ["Odd-one-out:", "Category check:", "Spot the mismatch:"],
    "pattern_sequence": ["Pattern puzzle:", "NVR pattern:", "Visual sequence:"],
    "rotation": ["Rotation puzzle:", "Turn the shape:", "Direction challenge:"],
    "matrix_rule": ["Matrix rule:", "Grid logic:", "Rule-box challenge:"],
    "inference_and_retrieval": ["Reading clue:", "Comprehension check:", "Evidence hunt:"],
}

STEM_PREFIX_GENERIC = ["Quick challenge:", "Practice task:", "Focus question:"]

STEM_OPENING_SWAPS: List[Tuple[str, List[str]]] = [
    ("What is ", ["Find ", "Calculate ", "Work out "]),
    ("Find ", ["Calculate ", "Work out "]),
    ("Which ", ["Select which ", "Choose which "]),
]


def normalize_stem_signature(stem: str) -> str:
    text = re.sub(r"\d+", " # ", str(stem or "").lower())
    text = re.sub(r"[^a-z# ]+", " ", text)
    words = [w for w in text.split() if w and w not in STEM_SIGNATURE_STOPWORDS]
    return " ".join(words[:12])


def diversify_stem_text(stem: str, skill: str) -> str:
    text = str(stem or "").strip()
    if not text:
        return text

    varied = text
    for source, alts in STEM_OPENING_SWAPS:
        if varied.startswith(source) and random.random() < 0.55:
            varied = random.choice(alts) + varied[len(source):]
            break

    if random.random() < 0.45:
        prefixes = STEM_PREFIX_BY_SKILL.get(str(skill or ""), STEM_PREFIX_GENERIC)
        prefix = random.choice(prefixes)
        lowered = varied.lower()
        if not lowered.startswith(("quick challenge:", "practice task:", "focus question:", "sequence challenge:", "word problem:", "analogy challenge:", "code puzzle:", "pattern puzzle:", "reading clue:")):
            varied = f"{prefix} {varied}"

    return varied


def rotation_bonus(recent_count: int, kind: str = "skill") -> float:
    if recent_count <= 0:
        return 0.18 if kind == "skill" else 0.16
    if recent_count == 1:
        return 0.08 if kind == "skill" else 0.06
    if recent_count >= 4:
        return -0.16 if kind == "skill" else -0.14
    if recent_count >= 3:
        return -0.10 if kind == "skill" else -0.08
    return 0.0


def weighted_pick_qid(candidates: List[Tuple[float, str]], top_k: int = 12) -> str:
    ranked = sorted(candidates, key=lambda x: x[0], reverse=True)[: max(1, min(top_k, len(candidates)))]
    min_score = min(score for score, _ in ranked)
    weights = [max(0.01, (score - min_score) + 0.05) for score, _ in ranked]
    return random.choices([qid for _, qid in ranked], weights=weights, k=1)[0]


def pick_next(
    user_id: int,
    allowed_domains: Optional[set[str]] = None,
    allowed_skills: Optional[set[str]] = None,
    target_difficulty: Optional[int] = None,
    practice_mode: str = "core",
) -> Tuple[Dict[str, Any], bool, str]:
    """
    Selector goals:
      - Prefer due reviews (spaced repetition)
      - Keep variety high across skills/domains/difficulty
      - Avoid very recent repeats and near-duplicate stem patterns
      - Bias toward weaker skills while still rotating coverage
    """
    now = int(time.time())

    conn = db()
    cur = conn.cursor()

    # Recent attempts with metadata (newest first).
    cur.execute(
        """
        SELECT qid, domain, skill
        FROM attempts
        WHERE user_id=?
        ORDER BY created_at DESC
        LIMIT 45
        """,
        (user_id,),
    )
    recent_rows = cur.fetchall()
    recent_order = [str(r["qid"]) for r in recent_rows]
    recent = set(recent_order[:30])

    # Local recency pressure: discourage over-repeating same domain/skill.
    recent_domain_counts: Dict[str, int] = {}
    recent_skill_counts: Dict[str, int] = {}
    for r in recent_rows[:18]:
        d = str(r["domain"] or "general")
        s = str(r["skill"] or "unknown")
        recent_domain_counts[d] = recent_domain_counts.get(d, 0) + 1
        recent_skill_counts[s] = recent_skill_counts.get(s, 0) + 1

    # Penalize pattern clones by stem signature in very recent history.
    recent_signatures: set[str] = set()
    recent_difficulty_counts: Dict[int, int] = {}
    for qid in recent_order[:16]:
        q_recent = Q.get(qid)
        if not q_recent:
            continue
        sig = normalize_stem_signature(str(q_recent.get("stem", "")))
        if sig:
            recent_signatures.add(sig)
        d = effective_difficulty_for_qid(qid, q_recent)
        recent_difficulty_counts[d] = recent_difficulty_counts.get(d, 0) + 1

    # Per-skill accuracy.
    cur.execute(
        """
        SELECT skill, COUNT(*) AS n, AVG(correct) AS acc
        FROM attempts
        WHERE user_id=?
        GROUP BY skill
        """,
        (user_id,),
    )
    skill_stats: Dict[str, Tuple[int, float]] = {}
    for r in cur.fetchall():
        skill_stats[str(r["skill"])] = (int(r["n"]), float(r["acc"]))

    # Historical per-domain counts for long-run balancing.
    cur.execute(
        """
        SELECT domain, COUNT(*) AS n
        FROM attempts
        WHERE user_id=?
        GROUP BY domain
        """,
        (user_id,),
    )
    domain_attempt_counts = {str(r["domain"] or "general"): int(r["n"] or 0) for r in cur.fetchall()}
    max_domain_attempts = max(domain_attempt_counts.values()) if domain_attempt_counts else 0

    # Due items.
    cur.execute(
        """
        SELECT qid, due_at
        FROM qprog
        WHERE user_id=? AND due_at>0 AND due_at<=?
        ORDER BY due_at ASC
        LIMIT 60
        """,
        (user_id, now),
    )
    due_rows = cur.fetchall()

    # Seen items.
    cur.execute("SELECT qid FROM qprog WHERE user_id=?", (user_id,))
    seen = {str(r["qid"]) for r in cur.fetchall()}

    conn.close()

    mode = (practice_mode or "core").strip().lower()
    if mode not in {"warmup", "core", "stretch"}:
        mode = "core"
    base_target = clamp_int(int(target_difficulty) if target_difficulty is not None else 3, 1, 7)
    if mode == "warmup":
        diff_target = clamp_int(base_target - 1, 1, 7)
        min_diff = 1
    elif mode == "stretch":
        diff_target = clamp_int(base_target + 1, 1, 7)
        min_diff = max(1, diff_target - 1)
    else:
        diff_target = base_target
        min_diff = max(1, diff_target - 2)

    def domain_history_bonus(domain: str) -> float:
        if max_domain_attempts <= 0:
            return 0.0
        n = domain_attempt_counts.get(domain, 0)
        ratio = n / max_domain_attempts if max_domain_attempts > 0 else 0.0
        return max(-0.03, 0.12 * (1.0 - ratio))

    # If there are due items, keep a strong preference, but still allow new content.
    serve_due = False
    if due_rows:
        due_bias = 0.88 if mode == "warmup" else 0.72 if mode == "core" else 0.40
        serve_due = random.random() < due_bias

    if serve_due:
        candidates_due: List[Tuple[float, str]] = []
        for r in due_rows:
            qid = str(r["qid"])
            if qid in recent:
                continue
            q = Q.get(qid)
            if not q:
                continue
            q_domain = str(q.get("domain", "general"))
            q_skill = str(q.get("skill", "unknown"))
            q_diff = effective_difficulty_for_qid(qid, q)
            if q_diff < min_diff:
                continue
            if allowed_domains and q_domain not in allowed_domains:
                continue
            if allowed_skills and q_skill not in allowed_skills:
                continue

            n, acc = skill_stats.get(q_skill, (0, 0.0))
            weakness = 0.90 if n == 0 else (1.0 - acc)
            due_at = int(r["due_at"] or 0)
            urgency = min(0.32, max(0.0, (now - due_at) / 86400.0 * 0.05))
            diff_fit = max(0.0, 0.16 - abs(q_diff - diff_target) * 0.05)
            domain_rot = rotation_bonus(recent_domain_counts.get(q_domain, 0), kind="domain")
            skill_rot = rotation_bonus(recent_skill_counts.get(q_skill, 0), kind="skill")
            sig = normalize_stem_signature(str(q.get("stem", "")))
            sig_penalty = -0.12 if (sig and sig in recent_signatures) else 0.0

            score = (
                (weakness * 0.72)
                + urgency
                + diff_fit
                + (domain_rot * 0.50)
                + (skill_rot * 0.60)
                + (sig_penalty * 0.60)
                + random.random() * 0.08
            )
            if not allowed_skills and recent_skill_counts.get(q_skill, 0) >= 2:
                score -= 0.24
            if not allowed_domains and recent_domain_counts.get(q_domain, 0) >= 3:
                score -= 0.18
            if mode == "warmup" and q_diff <= diff_target:
                score += 0.06
            if mode == "stretch" and q_diff >= diff_target:
                score += 0.06
            candidates_due.append((score, qid))

        if candidates_due:
            chosen_due = weighted_pick_qid(candidates_due, top_k=10)
            return Q[chosen_due], True, "due"

    # New/unseen question path with stronger interleaving.
    unseen_qids = [qid for qid in ALL_QIDS if qid not in seen]
    pool = unseen_qids if unseen_qids else ALL_QIDS

    candidates_new: List[Tuple[float, str]] = []
    for qid in pool:
        if qid in recent:
            continue
        q = Q[qid]
        q_domain = str(q.get("domain", "general"))
        q_skill = str(q.get("skill", "unknown"))
        q_diff = effective_difficulty_for_qid(qid, q)
        if q_diff < min_diff:
            continue
        if allowed_domains and q_domain not in allowed_domains:
            continue
        if allowed_skills and q_skill not in allowed_skills:
            continue

        n, acc = skill_stats.get(q_skill, (0, 0.0))
        weakness = 0.85 if n == 0 else (1.0 - acc)
        diff_shape = -abs(q_diff - diff_target) * 0.04
        domain_rot = rotation_bonus(recent_domain_counts.get(q_domain, 0), kind="domain")
        skill_rot = rotation_bonus(recent_skill_counts.get(q_skill, 0), kind="skill")
        hist_bonus = domain_history_bonus(q_domain)
        diff_variety = 0.07 if recent_difficulty_counts.get(q_diff, 0) == 0 else -(0.02 * min(2, recent_difficulty_counts.get(q_diff, 0)))
        sig = normalize_stem_signature(str(q.get("stem", "")))
        sig_penalty = -0.12 if (sig and sig in recent_signatures) else 0.0

        score = (
            weakness
            + diff_shape
            + domain_rot
            + skill_rot
            + hist_bonus
            + diff_variety
            + sig_penalty
            + random.random() * 0.11
        )
        if not allowed_skills and recent_skill_counts.get(q_skill, 0) >= 2:
            score -= 0.26
        if not allowed_domains and recent_domain_counts.get(q_domain, 0) >= 3:
            score -= 0.20
        if mode == "stretch" and q_diff >= diff_target:
            score += 0.06
        if mode == "warmup" and q_diff <= diff_target:
            score += 0.06
        candidates_new.append((score, qid))

    if candidates_new:
        chosen_new = weighted_pick_qid(candidates_new, top_k=14)
        return Q[chosen_new], False, "new"

    # Filter fallback: keep practice moving even on tight filters.
    filtered_pool: List[str] = []
    for qid2 in pool:
        q2 = Q[qid2]
        q2_domain = str(q2.get("domain", "general"))
        q2_skill = str(q2.get("skill", "unknown"))
        q2_diff = effective_difficulty_for_qid(qid2, q2)
        if q2_diff < min_diff:
            continue
        if allowed_domains and q2_domain not in allowed_domains:
            continue
        if allowed_skills and q2_skill not in allowed_skills:
            continue
        filtered_pool.append(qid2)
    qid = random.choice(filtered_pool or pool)
    return Q[qid], False, "fallback"

# -------------------- Routes --------------------
@app.get("/", response_class=HTMLResponse)
def home() -> HTMLResponse:
    if not INDEX_PATH.exists():
        return HTMLResponse("<h1>index.html not found</h1><p>Put index.html next to app.py</p>", status_code=500)
    return HTMLResponse(INDEX_PATH.read_text(encoding="utf-8"))


@app.get("/pricing.html", response_class=HTMLResponse)
def pricing_page() -> HTMLResponse:
    if not PRICING_PATH.exists():
        return HTMLResponse("<h1>pricing.html not found</h1><p>Put pricing.html next to app.py</p>", status_code=500)
    return HTMLResponse(PRICING_PATH.read_text(encoding="utf-8"))


@app.get("/store.html", response_class=HTMLResponse)
def store_page() -> HTMLResponse:
    if not STORE_PATH.exists():
        return HTMLResponse("<h1>store.html not found</h1><p>Put store.html next to app.py</p>", status_code=500)
    return HTMLResponse(STORE_PATH.read_text(encoding="utf-8"))


@app.get("/child.html", response_class=HTMLResponse)
def child_page() -> HTMLResponse:
    if not CHILD_PATH.exists():
        return HTMLResponse("<h1>child.html not found</h1><p>Put child.html next to app.py</p>", status_code=500)
    return HTMLResponse(CHILD_PATH.read_text(encoding="utf-8"))


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {"ok": True, "ts": int(time.time())}


@app.get("/readyz")
def readyz() -> Dict[str, Any]:
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        _ = cur.fetchone()
        conn.close()
    except Exception:
        raise HTTPException(status_code=503, detail="Database not ready")
    if not QUESTIONS_PATH.exists():
        raise HTTPException(status_code=503, detail="Question bank not ready")
    return {"ok": True, "ts": int(time.time())}


@app.get("/metricsz")
def metricsz() -> Dict[str, Any]:
    with OPS_LOCK:
        snap = dict(OPS_METRICS)
    total = int(snap.get("requests_total", 0) or 0)
    avg_ms = int((int(snap.get("request_duration_ms_sum", 0) or 0) / total)) if total > 0 else 0
    return {
        "ok": True,
        "env": APP_ENV,
        "uptime_s": max(0, int(time.time()) - int(snap.get("started_at", int(time.time())))),
        "requests_total": total,
        "requests_2xx": int(snap.get("requests_2xx", 0) or 0),
        "requests_4xx": int(snap.get("requests_4xx", 0) or 0),
        "requests_5xx": int(snap.get("requests_5xx", 0) or 0),
        "request_duration_ms_avg": avg_ms,
        "request_duration_ms_max": int(snap.get("request_duration_ms_max", 0) or 0),
        "last_error_at": int(snap.get("last_error_at", 0) or 0),
        "question_count": len(Q),
        "question_quality": {
            "invalid_items": int((QUESTION_QUALITY_REPORT.get("invalid_items", 0) if isinstance(QUESTION_QUALITY_REPORT, dict) else 0) or 0),
            "duplicate_ids": int((QUESTION_QUALITY_REPORT.get("duplicate_ids", 0) if isinstance(QUESTION_QUALITY_REPORT, dict) else 0) or 0),
        },
    }


@app.post("/api/login", response_model=LoginOut)
def login(payload: LoginIn, request: Request) -> LoginOut:
    apply_rate_limit(f"login:{client_ip(request)}", RATE_LIMIT_AUTH_MAX, RATE_LIMIT_WINDOW_SECONDS)
    if not payload.email and not payload.username:
        raise HTTPException(status_code=400, detail="Email (parent) or username (child) is required")
    identifier_is_email = bool(payload.email)
    if identifier_is_email:
        identifier = normalize_email(payload.email or "")
        validate_email(identifier)
    else:
        identifier = normalize_username(payload.username or "")
        validate_username(identifier)
    check_login_guard(identifier)

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE username=?", (identifier,))
    row = cur.fetchone()
    if not row:
        conn.close()
        record_login_result(identifier, False)
        raise HTTPException(status_code=400, detail="Account not found. Please sign up or check the username/email.")

    stored_hash = str(row["password_hash"] or "")
    if stored_hash:
        if not verify_password(stored_hash, payload.password):
            conn.close()
            record_login_result(identifier, False)
            raise HTTPException(status_code=401, detail="Incorrect password")
        if password_hash_needs_upgrade(stored_hash):
            cur.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(payload.password), int(row["id"])))
    else:
        # Upgrade legacy local accounts on first password login.
        cur.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(payload.password), int(row["id"])))

    token = make_token()
    cur.execute("UPDATE users SET token=?, token_issued_at=? WHERE id=?", (token, int(time.time()), int(row["id"])))
    conn.commit()
    record_login_result(identifier, True)
    plan_snapshot = plan_snapshot_for_user(row)
    out = LoginOut(
        user_id=int(row["id"]),
        token=token,
        email=str(row["username"]) if "@" in str(row["username"]) else None,
        username=str(row["username"]) if "@" not in str(row["username"]) else None,
        role=str(row["role"] or "child"),
        needs_plan_selection=bool(plan_snapshot.get("needs_selection", False)),
        current_plan=plan_snapshot,
    )
    conn.close()
    return out


@app.post("/api/signup", response_model=LoginOut)
def signup(payload: SignupIn, request: Request) -> LoginOut:
    apply_rate_limit(f"signup:{client_ip(request)}", RATE_LIMIT_AUTH_MAX, RATE_LIMIT_WINDOW_SECONDS)
    email = normalize_email(payload.email)
    validate_email(email)
    validate_password_strength(payload.password)
    role = normalize_role(payload.role)

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE username=?", (email,))
    if cur.fetchone():
        conn.close()
        raise HTTPException(status_code=400, detail="Email already exists. Please login.")

    now = int(time.time())
    token = make_token()
    plan_code = "starter"
    plan_status = "none" if role == "parent" else "active"
    plan_started_at = 0 if role == "parent" else now
    cur.execute(
        "INSERT INTO users("
        "username, password_hash, role, parent_user_id, token, token_issued_at, created_at, "
        "daily_goal, streak_days, last_active_day, coins, "
        "plan_code, plan_status, plan_started_at, plan_pending_since, plan_pending_target"
        ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            email,
            hash_password(payload.password),
            role,
            None,
            token,
            now,
            now,
            30,
            0,
            0,
            0,
            plan_code,
            plan_status,
            plan_started_at,
            0,
            "",
        ),
    )
    conn.commit()
    user_id = int(cur.lastrowid)
    conn.close()
    current_plan = {
        "code": plan_code,
        "status": plan_status,
        "pending_target": "",
        "pending_seconds_remaining": 0,
        "read_only": False,
        "owner_user_id": user_id,
        "needs_selection": role == "parent",
    }
    return LoginOut(
        user_id=user_id,
        token=token,
        email=email,
        username=None,
        role=role,
        needs_plan_selection=(role == "parent"),
        current_plan=current_plan,
    )


@app.get("/api/me", response_model=MeOut)
def me(token: str = Depends(resolve_token_from_request)) -> MeOut:
    user = user_by_token(token)
    plan_snapshot = plan_snapshot_for_user(user)
    return MeOut(
        user_id=int(user["id"]),
        email=str(user["username"]) if "@" in str(user["username"]) else None,
        username=str(user["username"]) if "@" not in str(user["username"]) else None,
        role=str(user["role"] or "child"),
        parent_user_id=int(user["parent_user_id"]) if user["parent_user_id"] is not None else None,
        plan_code=str(plan_snapshot.get("code", "starter")),
        plan_status=str(plan_snapshot.get("status", "none")),
        plan_pending_target=str(plan_snapshot.get("pending_target", "")),
        plan_read_only=bool(plan_snapshot.get("read_only", False)),
    )


@app.get("/api/plans")
def plans(token: str = Depends(resolve_token_from_request)) -> Dict[str, Any]:
    user = user_by_token(token)
    snapshot = plan_snapshot_for_user(user)
    current_capabilities = plan_capabilities_for_code(str(snapshot.get("code", "starter")))
    current_code = str(snapshot.get("code", "starter"))
    current_rank = plan_rank(current_code)
    upgrade_targets: List[str] = []
    downgrade_targets: List[str] = []
    if str(user["role"] or "child") == "parent" and not bool(snapshot.get("read_only", False)):
        upgrade_targets = [code for code in PLAN_ORDER if plan_rank(code) > current_rank]
        downgrade_targets = [code for code in PLAN_ORDER if plan_rank(code) < current_rank]
    return {
        "plans": plan_catalog_payload(),
        "current_plan": snapshot,
        "current_capabilities": current_capabilities,
        "upgrade_targets": upgrade_targets,
        "downgrade_targets": downgrade_targets,
        "can_cancel_pending": str(snapshot.get("status", "active")) == "pending",
        "can_quit_plan": str(user["role"] or "child") == "parent" and not bool(snapshot.get("read_only", False)),
        "auto_approve_seconds": PLAN_AUTO_APPROVE_SECONDS,
        "payment_mode": pricing_mode(),
        "payment_provider": PAYMENT_PROVIDER if payment_enabled() else "none",
        "payment_enabled": payment_enabled(),
        "checkout_enabled": payment_enabled(),
        "webhook_configured": stripe_signing_secret_available(),
    }


@app.post("/api/plan/select")
def plan_select(payload: PlanSelectIn) -> Dict[str, Any]:
    if payment_enabled():
        raise HTTPException(status_code=400, detail="Live payment mode enabled. Use /api/plan/checkout for selection.")
    user = user_by_token(payload.token)
    require_parent(user)
    parent_id = int(user["id"])
    code = normalize_plan_code(payload.plan_code)

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT plan_code, plan_status FROM users WHERE id=?", (parent_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Parent account not found")
    status = str(row["plan_status"] or "none")
    current = str(row["plan_code"] or "starter")
    if status != "none":
        conn.close()
        raise HTTPException(status_code=400, detail="Plan already selected. Use upgrade.")

    now = int(time.time())
    cur.execute(
        "UPDATE users SET plan_code=?, plan_status='active', plan_started_at=?, plan_pending_since=0, plan_pending_target='' WHERE id=?",
        (code, now, parent_id),
    )
    conn.commit()
    conn.close()

    log_plan_event(parent_id, "signup_plan_selected", current, code, "active")
    refreshed = user_by_id(parent_id)
    if not refreshed:
        raise HTTPException(status_code=404, detail="Parent account not found after update")
    snapshot = plan_snapshot_for_user(refreshed)
    return {"ok": True, "current_plan": snapshot}


@app.post("/api/plan/upgrade")
def plan_upgrade(payload: PlanUpgradeIn) -> Dict[str, Any]:
    if payment_enabled():
        raise HTTPException(status_code=400, detail="Live payment mode enabled. Use /api/plan/checkout for upgrades.")
    user = user_by_token(payload.token)
    require_parent(user)
    parent_id = int(user["id"])
    target = normalize_plan_code(payload.target_plan_code)

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT plan_code, plan_status, plan_pending_target FROM users WHERE id=?", (parent_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Parent account not found")
    current = normalize_plan_code(str(row["plan_code"] or "starter"))
    status = str(row["plan_status"] or "active")
    pending_target = str(row["plan_pending_target"] or "")

    if status == "none":
        conn.close()
        raise HTTPException(status_code=400, detail="Choose your first plan before requesting an upgrade")
    if status == "pending":
        conn.close()
        if pending_target == target:
            raise HTTPException(status_code=409, detail="Upgrade request already pending for this plan")
        raise HTTPException(status_code=409, detail="An upgrade request is already pending")
    if plan_rank(target) <= plan_rank(current):
        conn.close()
        raise HTTPException(status_code=400, detail="Upgrades must move to a higher plan")

    now = int(time.time())
    cur.execute(
        "UPDATE users SET plan_status='pending', plan_pending_target=?, plan_pending_since=? WHERE id=?",
        (target, now, parent_id),
    )
    conn.commit()
    conn.close()

    log_plan_event(parent_id, "upgrade_requested", current, target, "pending")
    refreshed = user_by_id(parent_id)
    if not refreshed:
        raise HTTPException(status_code=404, detail="Parent account not found after upgrade request")
    snapshot = plan_snapshot_for_user(refreshed)
    return {"ok": True, "current_plan": snapshot, "eta_seconds": int(snapshot.get("pending_seconds_remaining", PLAN_AUTO_APPROVE_SECONDS))}


@app.post("/api/plan/checkout")
def plan_checkout(payload: PlanCheckoutIn) -> Dict[str, Any]:
    user = user_by_token(payload.token)
    require_parent(user)
    if not payment_enabled():
        raise HTTPException(status_code=400, detail="Live payment is not enabled")

    parent_id = int(user["id"])
    target = normalize_plan_code(payload.target_plan_code)
    action = str(payload.action or "upgrade").strip().lower()
    if action not in {"select", "upgrade"}:
        raise HTTPException(status_code=400, detail="Invalid action")

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT plan_code, plan_status, plan_pending_target FROM users WHERE id=?", (parent_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Parent account not found")
    current = normalize_plan_code(str(row["plan_code"] or "starter"))
    status = str(row["plan_status"] or "active")

    if status == "pending":
        raise HTTPException(status_code=409, detail="An upgrade request is already pending")
    if action == "select":
        if status != "none":
            raise HTTPException(status_code=400, detail="Initial plan already selected")
    else:
        if status == "none":
            raise HTTPException(status_code=400, detail="Choose your first plan before requesting an upgrade")
        if plan_rank(target) <= plan_rank(current):
            raise HTTPException(status_code=400, detail="Upgrades must move to a higher plan")

    session_payload = stripe_create_checkout_session(
        user=user,
        target_plan=target,
        current_plan=current,
        action=action,
    )
    checkout_session_id = str(session_payload.get("id", ""))
    checkout_url = str(session_payload.get("url", ""))

    conn = db()
    cur = conn.cursor()
    now = int(time.time())
    cur.execute(
        """
        UPDATE users
        SET plan_status='pending', plan_pending_target=?, plan_pending_since=?, plan_checkout_session_id=?
        WHERE id=?
        """,
        (target, now, checkout_session_id, parent_id),
    )
    conn.commit()
    conn.close()

    log_payment_event(
        user_id=parent_id,
        provider="stripe",
        event_type="checkout_session_created",
        status="pending",
        plan_code=target,
        checkout_session_id=checkout_session_id,
        raw_payload={"action": action},
    )
    log_plan_event(parent_id, "checkout_started", current, target, "pending")
    refreshed = user_by_id(parent_id)
    snapshot = plan_snapshot_for_user(refreshed or user)
    return {
        "ok": True,
        "payment_mode": "live",
        "checkout_url": checkout_url,
        "checkout_session_id": checkout_session_id,
        "current_plan": snapshot,
    }


@app.post("/api/plan/downgrade")
def plan_downgrade(payload: PlanUpgradeIn) -> Dict[str, Any]:
    user = user_by_token(payload.token)
    require_parent(user)
    parent_id = int(user["id"])
    target = normalize_plan_code(payload.target_plan_code)

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT plan_code, plan_status, plan_pending_target FROM users WHERE id=?", (parent_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Parent account not found")
    current = normalize_plan_code(str(row["plan_code"] or "starter"))
    status = str(row["plan_status"] or "active")

    if status == "none":
        conn.close()
        raise HTTPException(status_code=400, detail="Choose your first plan before requesting a downgrade")
    if plan_rank(target) >= plan_rank(current):
        conn.close()
        raise HTTPException(status_code=400, detail="Downgrades must move to a lower plan")

    now = int(time.time())
    cur.execute(
        "UPDATE users SET plan_code=?, plan_status='active', plan_started_at=?, plan_pending_since=0, plan_pending_target='' WHERE id=?",
        (target, now, parent_id),
    )
    conn.commit()
    conn.close()

    log_plan_event(parent_id, "downgrade_applied", current, target, "active")
    refreshed = user_by_id(parent_id)
    if not refreshed:
        raise HTTPException(status_code=404, detail="Parent account not found after downgrade")
    snapshot = plan_snapshot_for_user(refreshed)
    return {"ok": True, "current_plan": snapshot}


@app.post("/api/plan/cancel-pending")
def plan_cancel_pending(payload: PlanCancelIn) -> Dict[str, Any]:
    user = user_by_token(payload.token)
    require_parent(user)
    parent_id = int(user["id"])

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT plan_code, plan_status, plan_pending_target FROM users WHERE id=?", (parent_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Parent account not found")
    current = normalize_plan_code(str(row["plan_code"] or "starter"))
    status = str(row["plan_status"] or "active")
    pending_target = str(row["plan_pending_target"] or "")
    if status != "pending":
        conn.close()
        raise HTTPException(status_code=400, detail="No pending plan request to cancel")

    cur.execute(
        "UPDATE users SET plan_status='active', plan_pending_since=0, plan_pending_target='' WHERE id=?",
        (parent_id,),
    )
    conn.commit()
    conn.close()

    log_plan_event(parent_id, "upgrade_canceled", current, pending_target, "active")
    refreshed = user_by_id(parent_id)
    if not refreshed:
        raise HTTPException(status_code=404, detail="Parent account not found after cancel")
    snapshot = plan_snapshot_for_user(refreshed)
    return {"ok": True, "current_plan": snapshot}


@app.post("/api/plan/quit")
def plan_quit(payload: PlanCancelIn) -> Dict[str, Any]:
    user = user_by_token(payload.token)
    require_parent(user)
    parent_id = int(user["id"])

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT plan_code, plan_status, plan_pending_target FROM users WHERE id=?", (parent_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Parent account not found")
    current = normalize_plan_code(str(row["plan_code"] or "starter"))
    status = str(row["plan_status"] or "active")
    pending_target = str(row["plan_pending_target"] or "")
    if status == "none":
        conn.close()
        refreshed_none = user_by_id(parent_id)
        if not refreshed_none:
            raise HTTPException(status_code=404, detail="Parent account not found")
        return {"ok": True, "current_plan": plan_snapshot_for_user(refreshed_none)}

    cur.execute(
        "UPDATE users SET plan_status='none', plan_pending_since=0, plan_pending_target='' WHERE id=?",
        (parent_id,),
    )
    conn.commit()
    conn.close()

    log_plan_event(parent_id, "plan_quit", current, pending_target, "none")
    refreshed = user_by_id(parent_id)
    if not refreshed:
        raise HTTPException(status_code=404, detail="Parent account not found after quit")
    snapshot = plan_snapshot_for_user(refreshed)
    return {"ok": True, "current_plan": snapshot}


def verify_stripe_webhook_signature(payload: bytes, stripe_signature: str) -> bool:
    if not STRIPE_WEBHOOK_SECRET:
        return False
    parts = {}
    for part in str(stripe_signature or "").split(","):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        parts[k.strip()] = v.strip()
    timestamp = parts.get("t", "")
    signature = parts.get("v1", "")
    if not timestamp or not signature:
        return False
    signed_payload = f"{timestamp}.{payload.decode('utf-8')}".encode("utf-8")
    expected = hmac.new(STRIPE_WEBHOOK_SECRET.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def register_webhook_event(provider: str, event_id: str, event_type: str) -> bool:
    event_id_norm = str(event_id or "").strip()
    if not event_id_norm:
        return False
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO webhook_events(provider, event_id, event_type, created_at) VALUES (?,?,?,?)",
            (str(provider or ""), event_id_norm, str(event_type or ""), int(time.time())),
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.rollback()
        conn.close()
        return False


def log_plan_usage(parent_id: int, plan_code: str, child_cnt: int, assigned_cnt: int, due_cnt: int) -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO plan_usage_records(parent_user_id, plan_code, child_count, assigned_count, due_count, created_at)
        VALUES (?,?,?,?,?,?)
        """,
        (int(parent_id), str(plan_code or ""), int(child_cnt), int(assigned_cnt), int(due_cnt), int(time.time())),
    )
    conn.commit()
    conn.close()


@app.post("/api/payment/stripe/webhook")
async def stripe_webhook(request: Request) -> Dict[str, Any]:
    if not payment_enabled():
        raise HTTPException(status_code=404, detail="Stripe webhook disabled")
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="Stripe webhook secret is not configured")

    body = await request.body()
    sig_header = request.headers.get("stripe-signature", "")
    if not verify_stripe_webhook_signature(body, sig_header):
        raise HTTPException(status_code=400, detail="Invalid Stripe signature")

    try:
        event = json.loads(body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event_id = str(event.get("id", "")).strip()
    event_type = str(event.get("type", ""))
    if not register_webhook_event("stripe", event_id, event_type):
        return {"ok": True, "ignored": True, "reason": "duplicate_event", "event_id": event_id}

    data_obj = ((event.get("data") or {}).get("object") or {})
    if event_type != "checkout.session.completed":
        return {"ok": True, "ignored": True, "event_type": event_type}

    metadata = data_obj.get("metadata") or {}
    try:
        user_id = int(metadata.get("user_id") or 0)
    except Exception:
        user_id = 0
    target_plan = str(metadata.get("target_plan") or "").strip().lower()
    action = str(metadata.get("action") or "upgrade").strip().lower()
    checkout_session_id = str(data_obj.get("id") or "")
    payment_ref = str(data_obj.get("payment_intent") or data_obj.get("subscription") or "")

    if user_id <= 0 or target_plan not in PLAN_CATALOG:
        raise HTTPException(status_code=400, detail="Invalid session metadata")

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT plan_code, plan_status FROM users WHERE id=?", (user_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="User not found")
    old_plan = normalize_plan_code(str(row["plan_code"] or "starter"))
    now = int(time.time())
    cur.execute(
        """
        UPDATE users
        SET plan_code=?, plan_status='active', plan_started_at=?, plan_pending_since=0, plan_pending_target='',
            plan_checkout_session_id='', plan_payment_ref=?
        WHERE id=?
        """,
        (target_plan, now, payment_ref, user_id),
    )
    conn.commit()
    conn.close()

    log_payment_event(
        user_id=user_id,
        provider="stripe",
        event_type="checkout_session_completed",
        status="paid",
        plan_code=target_plan,
        checkout_session_id=checkout_session_id,
        payment_ref=payment_ref,
        raw_payload={"event_id": event_id, "action": action},
    )
    log_plan_event(user_id, "stripe_payment_completed", old_plan, target_plan, "active")
    return {"ok": True}


@app.post("/api/switch-account", response_model=LoginOut)
def switch_account(payload: SwitchAccountIn) -> LoginOut:
    user = user_by_token(payload.token)
    uid = int(user["id"])
    role = str(user["role"] or "child")
    target = str(payload.target_role or "").strip().lower()
    if target not in {"parent", "child"}:
        raise HTTPException(status_code=400, detail="target_role must be parent or child")
    if role == target:
        raise HTTPException(status_code=400, detail=f"Already in {target} account")

    conn = db()
    cur = conn.cursor()

    target_row: Optional[sqlite3.Row] = None
    if role == "child" and target == "parent":
        conn.close()
        raise HTTPException(status_code=403, detail="Child to parent switch is disabled")
    elif role == "parent" and target == "child":
        require_parent_plan_selected(user)
        child_id = int(payload.child_user_id or 0)
        if child_id:
            cur.execute(
                "SELECT * FROM users WHERE id=? AND role='child' AND parent_user_id=?",
                (child_id, uid),
            )
            target_row = cur.fetchone()
        else:
            cur.execute(
                "SELECT * FROM users WHERE role='child' AND parent_user_id=? ORDER BY created_at DESC LIMIT 1",
                (uid,),
            )
            target_row = cur.fetchone()
        if not target_row:
            conn.close()
            raise HTTPException(status_code=404, detail="No linked child account found to switch into")
    else:
        conn.close()
        raise HTTPException(status_code=403, detail="This switch is not allowed")

    token = make_token()
    cur.execute(
        "UPDATE users SET token=?, token_issued_at=? WHERE id=?",
        (token, int(time.time()), int(target_row["id"])),
    )
    conn.commit()
    conn.close()

    uname = str(target_row["username"] or "")
    plan_snapshot = plan_snapshot_for_user(target_row)
    return LoginOut(
        user_id=int(target_row["id"]),
        token=token,
        email=uname if "@" in uname else None,
        username=uname if "@" not in uname else None,
        role=str(target_row["role"] or target),
        needs_plan_selection=bool(plan_snapshot.get("needs_selection", False)),
        current_plan=plan_snapshot,
    )

@app.post("/api/goal")
def set_goal(payload: GoalIn) -> Dict[str, Any]:
    user = user_by_token(payload.token)
    uid = int(user["id"])
    conn = db()
    cur = conn.cursor()
    cur.execute("UPDATE users SET daily_goal=? WHERE id=?", (int(payload.daily_goal), uid))
    conn.commit()
    conn.close()
    return {"ok": True, "daily_goal": int(payload.daily_goal)}


@app.get("/api/parent/children")
def parent_children(token: str = Depends(resolve_token_from_request)) -> Dict[str, Any]:
    user = user_by_token(token)
    require_parent_plan_selected(user)
    parent_id = int(user["id"])
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, created_at FROM users WHERE role='child' AND parent_user_id=? ORDER BY username ASC",
        (parent_id,),
    )
    items = [
        {
            "user_id": int(r["id"]),
            "username": str(r["username"]),
            "created_at": int(r["created_at"]),
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return {"items": items}


@app.post("/api/parent/children")
def parent_create_child(payload: ParentCreateChildIn) -> Dict[str, Any]:
    parent = user_by_token(payload.token)
    require_parent_plan_selected(parent)
    caps = parent_plan_capabilities(parent)
    parent_id = int(parent["id"])
    username = normalize_username(payload.username)
    validate_username(username)
    validate_password_strength(payload.password)
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS n FROM users WHERE role='child' AND parent_user_id=?", (parent_id,))
    child_count = int((cur.fetchone()["n"] or 0))
    max_children = int(caps.get("max_children", 1))
    if child_count >= max_children:
        conn.close()
        raise HTTPException(status_code=403, detail=f"Your current plan allows up to {max_children} child account(s)")
    cur.execute("SELECT id FROM users WHERE username=?", (username,))
    if cur.fetchone():
        conn.close()
        raise HTTPException(status_code=400, detail="Username already exists")
    token = make_token()
    now = int(time.time())
    cur.execute(
        "INSERT INTO users("
        "username, password_hash, role, parent_user_id, token, token_issued_at, created_at, "
        "daily_goal, streak_days, last_active_day, coins, "
        "plan_code, plan_status, plan_started_at, plan_pending_since, plan_pending_target"
        ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            username,
            hash_password(payload.password),
            "child",
            parent_id,
            token,
            now,
            now,
            30,
            0,
            0,
            0,
            "starter",
            "active",
            now,
            0,
            "",
        ),
    )
    conn.commit()
    child_id = int(cur.lastrowid)
    conn.close()
    return {"ok": True, "child": {"user_id": child_id, "username": username}}


@app.delete("/api/parent/children")
def parent_delete_child(payload: ParentDeleteChildIn) -> Dict[str, Any]:
    parent = user_by_token(payload.token)
    require_parent_plan_selected(parent)
    parent_id = int(parent["id"])
    child_id = int(payload.child_user_id)
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, role, parent_user_id FROM users WHERE id=?",
        (child_id,),
    )
    child = cur.fetchone()
    if not child or str(child["role"] or "child") != "child":
        conn.close()
        raise HTTPException(status_code=404, detail="Child account not found")
    if child["parent_user_id"] is None or int(child["parent_user_id"]) != parent_id:
        conn.close()
        raise HTTPException(status_code=403, detail="That child belongs to a different parent account")

    # Remove child-linked rows first to keep older SQLite setups consistent.
    cur.execute("DELETE FROM practice_events WHERE child_user_id=?", (child_id,))
    cur.execute("DELETE FROM practice_events WHERE parent_user_id=?", (child_id,))
    cur.execute("DELETE FROM assigned_practices WHERE child_user_id=?", (child_id,))
    cur.execute("DELETE FROM assigned_practices WHERE parent_user_id=?", (child_id,))
    cur.execute("DELETE FROM attempts WHERE user_id=?", (child_id,))
    cur.execute("DELETE FROM qprog WHERE user_id=?", (child_id,))
    cur.execute("DELETE FROM purchases WHERE user_id=?", (child_id,))
    cur.execute("DELETE FROM equipped_items WHERE user_id=?", (child_id,))
    cur.execute("DELETE FROM users WHERE id=? AND role='child' AND parent_user_id=?", (child_id, parent_id))
    if int(cur.rowcount or 0) <= 0:
        conn.close()
        raise HTTPException(status_code=409, detail="Child account could not be deleted")
    conn.commit()
    deleted = {"user_id": child_id, "username": str(child["username"] or "")}
    conn.close()
    return {"ok": True, "deleted_child": deleted}


@app.post("/api/parent/assign-practice")
def parent_assign_practice(payload: ParentAssignPracticeIn) -> Dict[str, Any]:
    parent = user_by_token(payload.token)
    require_parent_plan_selected(parent)
    caps = parent_plan_capabilities(parent)
    parent_id = int(parent["id"])
    requested_size = int(payload.size)
    max_size = int(caps.get("max_practice_size", 20))
    if requested_size > max_size:
        raise HTTPException(status_code=400, detail=f"Your current plan allows assignment size up to {max_size}")
    mode_norm = normalize_practice_mode(payload.mode)
    allowed_modes = {str(m) for m in (caps.get("allowed_modes") or ["core"])}
    if mode_norm not in allowed_modes:
        allowed_modes_text = ", ".join(sorted(list(allowed_modes)))
        raise HTTPException(status_code=400, detail=f"Your current plan allows these modes: {allowed_modes_text}")

    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, role, parent_user_id FROM users WHERE id=?",
        (int(payload.child_user_id),),
    )
    child = cur.fetchone()
    if not child or str(child["role"] or "child") != "child":
        conn.close()
        raise HTTPException(status_code=404, detail="Child account not found")
    if child["parent_user_id"] is not None and int(child["parent_user_id"]) != parent_id:
        conn.close()
        raise HTTPException(status_code=403, detail="That child belongs to a different parent account")

    # If the child was self-created, link it to the current parent on first assignment.
    if child["parent_user_id"] is None:
        cur.execute("UPDATE users SET parent_user_id=? WHERE id=?", (parent_id, int(payload.child_user_id)))

    domain = str(payload.domain or "").strip()
    skills = [str(s).strip() for s in payload.skills if str(s).strip()]
    cur.execute(
        "INSERT INTO assigned_practices(parent_user_id, child_user_id, name, size, domain, skills_json, mode, created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            parent_id,
            int(payload.child_user_id),
            payload.name.strip(),
            requested_size,
            domain,
            json.dumps(skills),
            mode_norm,
            int(time.time()),
        ),
    )
    conn.commit()
    assignment_id = int(cur.lastrowid)
    insert_practice_event(
        cur,
        assignment_id=assignment_id,
        parent_user_id=parent_id,
        child_user_id=int(payload.child_user_id),
        event_type="assigned",
        name=payload.name.strip(),
        domain=domain,
        size=requested_size,
        skills_count=len(skills),
    )
    conn.commit()
    conn.close()
    return {"ok": True, "assignment_id": assignment_id}


@app.get("/api/assigned-practices", response_model=AssignedPracticesOut)
def assigned_practices(
    token: str = Depends(resolve_token_from_request),
    child_user_id: Optional[int] = None,
) -> AssignedPracticesOut:
    user = user_by_token(token)
    uid = int(user["id"])
    role = str(user["role"] or "child")
    conn = db()
    cur = conn.cursor()
    if role == "parent":
        require_parent_plan_selected(user)
        parent_id = uid
        if child_user_id is None:
            cur.execute(
                "SELECT id, parent_user_id, child_user_id, name, size, domain, skills_json, mode, created_at "
                "FROM assigned_practices WHERE parent_user_id=? ORDER BY created_at DESC",
                (parent_id,),
            )
        else:
            cur.execute(
                "SELECT id, parent_user_id, child_user_id, name, size, domain, skills_json, mode, created_at "
                "FROM assigned_practices WHERE parent_user_id=? AND child_user_id=? ORDER BY created_at DESC",
                (parent_id, int(child_user_id)),
            )
    else:
        cur.execute(
            "SELECT id, parent_user_id, child_user_id, name, size, domain, skills_json, mode, created_at "
            "FROM assigned_practices WHERE child_user_id=? ORDER BY created_at DESC",
            (uid,),
        )

    items: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        try:
            skills = json.loads(str(r["skills_json"] or "[]"))
            if not isinstance(skills, list):
                skills = []
        except Exception:
            skills = []
        items.append({
            "assignment_id": int(r["id"]),
            "parent_user_id": int(r["parent_user_id"]),
            "child_user_id": int(r["child_user_id"]),
            "name": str(r["name"]),
            "size": int(r["size"]),
            "domain": str(r["domain"] or ""),
            "skills": [str(s) for s in skills],
            "mode": str(r["mode"] or "core"),
            "created_at": int(r["created_at"]),
        })
    conn.close()
    return AssignedPracticesOut(items=items)


@app.post("/api/assigned-practices/complete")
def complete_assigned_practice(payload: AssignedPracticeCompleteIn) -> Dict[str, Any]:
    user = user_by_token(payload.token)
    uid = int(user["id"])
    role = str(user["role"] or "child")

    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, parent_user_id, child_user_id, name, size, domain, skills_json, mode FROM assigned_practices WHERE id=?",
        (int(payload.assignment_id),),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        # Idempotent success so repeated completion calls don't fail the child flow.
        return {"ok": True, "removed": False}

    parent_id = int(row["parent_user_id"])
    child_id = int(row["child_user_id"])
    if role == "parent":
        if uid != parent_id:
            conn.close()
            raise HTTPException(status_code=403, detail="Not allowed to modify this assigned practice")
    else:
        if uid != child_id:
            conn.close()
            raise HTTPException(status_code=403, detail="Not allowed to complete this assigned practice")

    skills_count = 0
    try:
        skills_val = json.loads(str(row["skills_json"] or "[]"))
        if isinstance(skills_val, list):
            skills_count = len(skills_val)
    except Exception:
        skills_count = 0
    insert_practice_event(
        cur,
        assignment_id=int(row["id"]),
        parent_user_id=parent_id,
        child_user_id=child_id,
        event_type="completed",
        name=str(row["name"] or "Practice"),
        domain=str(row["domain"] or ""),
        size=int(row["size"] or 0),
        skills_count=skills_count,
    )
    cur.execute("DELETE FROM assigned_practices WHERE id=?", (int(payload.assignment_id),))
    conn.commit()
    conn.close()
    return {"ok": True, "removed": True}


@app.post("/api/assigned-practices/quit")
def quit_assigned_practice(payload: AssignedPracticeQuitIn) -> Dict[str, Any]:
    user = user_by_token(payload.token)
    uid = int(user["id"])
    role = str(user["role"] or "child")

    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, parent_user_id, child_user_id, name, size, domain, skills_json, mode FROM assigned_practices WHERE id=?",
        (int(payload.assignment_id),),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return {"ok": True, "removed": False}

    parent_id = int(row["parent_user_id"])
    child_id = int(row["child_user_id"])
    if role == "parent":
        if uid != parent_id:
            conn.close()
            raise HTTPException(status_code=403, detail="Not allowed to modify this assigned practice")
    else:
        if uid != child_id:
            conn.close()
            raise HTTPException(status_code=403, detail="Not allowed to quit this assigned practice")

    skills_count = 0
    try:
        skills_val = json.loads(str(row["skills_json"] or "[]"))
        if isinstance(skills_val, list):
            skills_count = len(skills_val)
    except Exception:
        skills_count = 0

    insert_practice_event(
        cur,
        assignment_id=int(row["id"]),
        parent_user_id=parent_id,
        child_user_id=child_id,
        event_type="quit",
        name=str(row["name"] or "Practice"),
        domain=str(row["domain"] or ""),
        size=int(row["size"] or 0),
        skills_count=skills_count,
    )
    cur.execute("DELETE FROM assigned_practices WHERE id=?", (int(payload.assignment_id),))
    conn.commit()
    conn.close()
    return {"ok": True, "removed": True}


@app.get("/api/parent/report")
def parent_report(token: str = Depends(resolve_token_from_request)) -> Dict[str, Any]:
    parent = user_by_token(token)
    require_parent_plan_selected(parent)
    caps = parent_plan_capabilities(parent)
    if not bool(caps.get("parent_report", False)):
        raise HTTPException(status_code=403, detail="Parent report is available on Plus and Pro plans")
    parent_id = int(parent["id"])
    now = int(time.time())
    day_start = utc_day_id(now) * 86400
    week_start = now - (7 * 86400)

    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT u.id, u.username, COALESCE(u.daily_goal,30) AS daily_goal,
               COALESCE(u.streak_days,0) AS streak_days, COALESCE(u.coins,0) AS coins,
               COUNT(a.id) AS total_attempts,
               AVG(a.correct) AS accuracy,
               SUM(CASE WHEN a.created_at>=? AND a.correct=1 THEN 1 ELSE 0 END) AS correct_today,
               SUM(CASE WHEN a.created_at>=? THEN 1 ELSE 0 END) AS attempts_7d,
               AVG(CASE WHEN a.created_at>=? THEN a.correct END) AS accuracy_7d,
               MAX(a.created_at) AS last_answer_at
        FROM users u
        LEFT JOIN attempts a ON a.user_id = u.id
        WHERE u.role='child' AND u.parent_user_id=?
        GROUP BY u.id, u.username, u.daily_goal, u.streak_days, u.coins
        ORDER BY LOWER(u.username) ASC
        """,
        (day_start, week_start, week_start, parent_id),
    )
    child_rows = cur.fetchall()

    cur.execute(
        "SELECT child_user_id, COUNT(*) AS n FROM assigned_practices WHERE parent_user_id=? GROUP BY child_user_id",
        (parent_id,),
    )
    active_assignments = {int(r["child_user_id"]): int(r["n"] or 0) for r in cur.fetchall()}

    cur.execute(
        """
        SELECT child_user_id, event_type, COUNT(*) AS n
        FROM practice_events
        WHERE parent_user_id=? AND created_at>=?
        GROUP BY child_user_id, event_type
        """,
        (parent_id, week_start),
    )
    event_counts: Dict[int, Dict[str, int]] = {}
    for r in cur.fetchall():
        child_id = int(r["child_user_id"])
        event_counts.setdefault(child_id, {})[str(r["event_type"])] = int(r["n"] or 0)

    cur.execute(
        """
        SELECT id, assignment_id, child_user_id, event_type, name, domain, size, skills_count, created_at
        FROM practice_events
        WHERE parent_user_id=?
        ORDER BY created_at DESC
        LIMIT 30
        """,
        (parent_id,),
    )
    event_rows = cur.fetchall()

    conn.close()

    child_items: List[Dict[str, Any]] = []
    child_name_map: Dict[int, str] = {}
    for r in child_rows:
        child_id = int(r["id"])
        child_name = str(r["username"])
        child_name_map[child_id] = child_name
        progress = build_progress_snapshot(child_id)
        counts = event_counts.get(child_id, {})
        child_items.append({
            "child_user_id": child_id,
            "username": child_name,
            "daily_goal": int(r["daily_goal"] or 30),
            "streak_days": int(r["streak_days"] or 0),
            "coins": int(r["coins"] or 0),
            "total_attempts": int(r["total_attempts"] or 0),
            "accuracy": float(r["accuracy"] or 0.0),
            "correct_today": int(r["correct_today"] or 0),
            "attempts_7d": int(r["attempts_7d"] or 0),
            "accuracy_7d": float(r["accuracy_7d"] or 0.0),
            "last_answer_at": int(r["last_answer_at"] or 0),
            "active_assignments": int(active_assignments.get(child_id, 0)),
            "assigned_7d": int(counts.get("assigned", 0)),
            "completed_7d": int(counts.get("completed", 0)),
            "quit_7d": int(counts.get("quit", 0)),
            "due_now": due_count(child_id),
            "progress_stage": str(progress.get("progress_stage", "building")),
            "recommended_difficulty": int(progress.get("recommended_difficulty", 3)),
            "recent_accuracy": float(progress.get("recent_accuracy", 0.0)),
            "recent_avg_time_ms": int(progress.get("recent_avg_time_ms", 0)),
            "learning_focus": str(progress.get("learning_focus", "")),
            "weak_domain": str(progress.get("weak_domain", "")),
            "strong_domain": str(progress.get("strong_domain", "")),
            "mastery_score": int(progress.get("mastery_score", 0)),
            "readiness_score": int(progress.get("readiness_score", 0)),
            "mastery_band": str(progress.get("mastery_band", "starting")),
            "next_recommended_domains": list(progress.get("next_recommended_domains", [])),
            "goal_progress_pct": clamp_int(round((int(r["correct_today"] or 0) / max(1, int(r["daily_goal"] or 30))) * 100), 0, 100),
            "completion_rate_7d": float(int(counts.get("completed", 0)) / max(1, int(counts.get("assigned", 0)))) if int(counts.get("assigned", 0)) else 0.0,
        })

    recent_events: List[Dict[str, Any]] = []
    for r in event_rows:
        child_id = int(r["child_user_id"])
        recent_events.append({
            "event_id": int(r["id"]),
            "assignment_id": int(r["assignment_id"]) if r["assignment_id"] is not None else None,
            "child_user_id": child_id,
            "child_username": child_name_map.get(child_id, f"child-{child_id}"),
            "event_type": str(r["event_type"]),
            "name": str(r["name"]),
            "domain": str(r["domain"] or ""),
            "size": int(r["size"] or 0),
            "skills_count": int(r["skills_count"] or 0),
            "created_at": int(r["created_at"] or 0),
        })

    return {
        "generated_at": now,
        "children": child_items,
        "recent_events": recent_events,
    }


@app.get("/api/parent/analytics", response_model=AnalyticsOut)
def parent_analytics(token: str = Depends(resolve_token_from_request)) -> AnalyticsOut:
    parent = user_by_token(token)
    require_parent_plan_selected(parent)
    parent_id = int(parent["id"])
    now = int(time.time())
    week_seconds = 7 * 86400
    month_seconds = 30 * 86400
    week_ago = now - week_seconds
    month_ago = now - month_seconds

    current_plan = plan_snapshot_for_user(parent)

    conn = db()
    cur = conn.cursor()

    cur.execute(
        "SELECT id, username, daily_goal, streak_days, coins FROM users WHERE role='child' AND parent_user_id=?",
        (parent_id,),
    )
    child_rows = cur.fetchall()
    child_ids = [int(r["id"]) for r in child_rows]
    child_placeholders = ",".join("?" for _ in child_ids) if child_ids else ""

    attempt_map: Dict[int, Dict[str, int]] = {}
    if child_ids:
        cur.execute(
            f"""
            SELECT user_id, COUNT(*) AS attempts, SUM(correct) AS corrects, MAX(created_at) AS last_answer_at
            FROM attempts
            WHERE user_id IN ({child_placeholders}) AND created_at>=?
            GROUP BY user_id
            """,
            (*child_ids, week_ago),
        )
        for row in cur.fetchall():
            cid = int(row["user_id"])
            attempt_map[cid] = {
                "attempts_7d": int(row["attempts"] or 0),
                "correct_7d": int(row["corrects"] or 0),
                "last_answer_at": int(row["last_answer_at"] or 0),
            }

    practice_event_totals: Dict[str, int] = {}
    child_event_map: Dict[int, Dict[str, int]] = {}
    cur.execute(
        "SELECT child_user_id, event_type, COUNT(*) AS n FROM practice_events WHERE parent_user_id=? AND created_at>=? GROUP BY child_user_id, event_type",
        (parent_id, week_ago),
    )
    for row in cur.fetchall():
        cid = int(row["child_user_id"])
        event_type = str(row["event_type"] or "")
        count = int(row["n"] or 0)
        if not event_type:
            event_type = "unknown"
        child_event_map.setdefault(cid, {})[event_type] = count
        practice_event_totals[event_type] = practice_event_totals.get(event_type, 0) + count

    plan_event_totals: Dict[str, int] = {}
    cur.execute(
        "SELECT event_type, COUNT(*) AS n FROM plan_events WHERE user_id=? AND created_at>=? GROUP BY event_type",
        (parent_id, month_ago),
    )
    for row in cur.fetchall():
        key = str(row["event_type"] or "")
        if not key:
            key = "unknown"
        plan_event_totals[key] = int(row["n"] or 0)

    plan_usage_records: List[AnalyticsPlanUsageRecord] = []
    cur.execute(
        "SELECT plan_code, child_count, assigned_count, due_count, created_at FROM plan_usage_records WHERE parent_user_id=? ORDER BY created_at DESC LIMIT 10",
        (parent_id,),
    )
    for row in cur.fetchall():
        plan_usage_records.append(
            AnalyticsPlanUsageRecord(
                plan_code=str(row["plan_code"] or "starter"),
                child_count=int(row["child_count"] or 0),
                assigned_count=int(row["assigned_count"] or 0),
                due_count=int(row["due_count"] or 0),
                recorded_at=int(row["created_at"] or 0),
            )
        )

    purchases_last_30d = 0
    spent_last_30d = 0
    last_purchase_at = 0
    if child_ids:
        cur.execute(
            f"""
            SELECT COUNT(*) AS purchases, COALESCE(SUM(cost),0) AS spent, COALESCE(MAX(created_at),0) AS last_purchase_at
            FROM purchases
            WHERE user_id IN ({child_placeholders}) AND created_at>=?
            """,
            (*child_ids, month_ago),
        )
        row = cur.fetchone()
        if row:
            purchases_last_30d = int(row["purchases"] or 0)
            spent_last_30d = int(row["spent"] or 0)
            last_purchase_at = int(row["last_purchase_at"] or 0)

    conn.close()

    child_usage: List[AnalyticsChildUsage] = []
    for child in child_rows:
        cid = int(child["id"])
        attempts_data = attempt_map.get(cid, {})
        attempts = attempts_data.get("attempts_7d", 0)
        correct = attempts_data.get("correct_7d", 0)
        accuracy = float(correct) / attempts if attempts else 0.0
        event_counts = child_event_map.get(cid, {})
        assigned = int(event_counts.get("assigned", 0))
        completed = int(event_counts.get("completed", 0))
        quit_events = int(event_counts.get("quit", 0))
        progress = build_progress_snapshot(cid)
        child_usage.append(
            AnalyticsChildUsage(
                child_user_id=cid,
                username=str(child["username"]),
                attempts_7d=attempts,
                correct_7d=correct,
                accuracy_7d=accuracy,
                coins=int(child.get("coins") or 0),
                streak_days=int(child.get("streak_days") or 0),
                daily_goal=int(child.get("daily_goal") or 30),
                due_now=due_count(cid),
                last_answer_at=attempts_data.get("last_answer_at", 0),
                assigned_7d=assigned,
                completed_7d=completed,
                quit_7d=quit_events,
                progress_stage=str(progress.get("progress_stage", "building")),
                recommended_difficulty=int(progress.get("recommended_difficulty", 3)),
                learning_focus=str(progress.get("learning_focus", "")),
            )
        )

    plan_usage_records = list(reversed(plan_usage_records))
    return AnalyticsOut(
        generated_at=now,
        window_seconds=week_seconds,
        current_plan=current_plan,
        child_usage=child_usage,
        practice_event_totals=practice_event_totals,
        plan_event_totals=plan_event_totals,
        plan_usage_trend=plan_usage_records,
        store_activity=AnalyticsStoreActivity(
            purchases_last_30d=purchases_last_30d,
            spent_last_30d=spent_last_30d,
            last_purchase_at=last_purchase_at,
        ),
    )

@app.get("/api/catalog")
def catalog() -> Dict[str, Any]:
    domains: Dict[str, Dict[str, Any]] = {}
    for q in Q.values():
        domain = str(q.get("domain", "general"))
        skill = str(q.get("skill", "unknown"))
        entry = domains.setdefault(domain, {"domain": domain, "skills": set(), "count": 0})
        entry["skills"].add(skill)
        entry["count"] += 1
    domain_list = []
    all_skills = set()
    for d in sorted(domains.keys()):
        info = domains[d]
        skills_sorted = sorted(list(info["skills"]))
        all_skills.update(skills_sorted)
        domain_list.append({
            "domain": d,
            "count": int(info["count"]),
            "skills": skills_sorted,
        })
    return {
        "domains": domain_list,
        "skills": sorted(list(all_skills)),
        "question_count": len(Q),
    }


@app.get("/api/questions/quality")
def questions_quality() -> Dict[str, Any]:
    calibrated_questions = sum(
        1
        for v in QUESTION_CALIBRATION.values()
        if int(v.get("attempts", 0) or 0) >= QUESTION_CALIBRATION_MIN_ATTEMPTS
    )
    return {
        "question_count": len(Q),
        "validation": QUESTION_QUALITY_REPORT,
        "calibration": {
            "min_attempts": QUESTION_CALIBRATION_MIN_ATTEMPTS,
            "questions_with_attempts": len(QUESTION_CALIBRATION),
            "calibrated_questions": calibrated_questions,
        },
    }

@app.get("/api/next", response_model=NextOut)
def next_question(
    token: str,
    domains: Optional[str] = None,
    skills: Optional[str] = None,
    mode: Optional[str] = None,
) -> NextOut:
    user = user_by_token(token)
    if str(user["role"] or "child") != "child":
        raise HTTPException(status_code=403, detail="Questions are available on child accounts only")
    _ = plan_snapshot_for_user(user)
    uid = int(user["id"])
    allowed_domains = None
    allowed_skills = None
    if domains:
        parsed = {d.strip() for d in domains.split(",") if d.strip()}
        if parsed:
            allowed_domains = parsed
    if skills:
        parsed = {s.strip() for s in skills.split(",") if s.strip()}
        if parsed:
            allowed_skills = parsed

    # If filters exclude everything, fail with a clear message.
    if allowed_domains or allowed_skills:
        any_match = False
        for qx in Q.values():
            q_dom = str(qx.get("domain", "general"))
            q_skill = str(qx.get("skill", "unknown"))
            if allowed_domains and q_dom not in allowed_domains:
                continue
            if allowed_skills and q_skill not in allowed_skills:
                continue
            any_match = True
            break
        if not any_match:
            raise HTTPException(status_code=400, detail="No questions match the selected practice filters.")

    progress = build_progress_snapshot(uid)
    practice_mode = normalize_practice_mode(mode)

    q, is_due, selection_mode = pick_next(
        uid,
        allowed_domains=allowed_domains,
        allowed_skills=allowed_skills,
        target_difficulty=int(progress.get("recommended_difficulty", 3)),
        practice_mode=practice_mode,
    )

    q = dict(q)
    q["stem"] = diversify_stem_text(str(q.get("stem", "")), str(q.get("skill", "unknown")))

    # Slight shuffle to reduce memorisation while keeping answer text intact.
    if q.get("type") == "mcq":
        orig_choices = list(q.get("choices") or [])
        random.shuffle(orig_choices)
        q["choices"] = orig_choices

    if is_due:
        selection_note = "Due review prioritized; rotation safeguards keep it varied."
    elif selection_mode == "new":
        selection_note = "New item selected with skill/domain rotation for variety."
    else:
        selection_note = "Fallback mix used to keep practice flowing under your filters."
    qid = str(q["id"])
    effective_difficulty = effective_difficulty_for_qid(qid, q)
    curriculum = curriculum_metadata_for_question(q)

    return NextOut(
        qid=qid,
        qtype=str(q["type"]),
        domain=str(q.get("domain", "general")),
        skill=str(q.get("skill", "unknown")),
        difficulty=effective_difficulty,
        stem=str(q.get("stem", "")),
        choices=q.get("choices", None),
        time_limit_s=int(q.get("time_limit_s", 45)),
        due=is_due,
        level_band=level_band_for_difficulty(effective_difficulty),
        curriculum_tag=str(curriculum.get("curriculum_tag", "11+ Core")),
        curriculum_stage=str(curriculum.get("curriculum_stage", "11+ Core")),
        curriculum_strand=str(curriculum.get("curriculum_strand", "General")),
        curriculum_objective=str(curriculum.get("curriculum_objective", "Core practice")),
        curriculum_year=str(curriculum.get("curriculum_year", "Year 5/6")),
        curriculum_topic=str(curriculum.get("curriculum_topic", "Mixed practice")),
        curriculum_subtopic=str(curriculum.get("curriculum_subtopic", "Core skills")),
        selection_note=selection_note,
    )

@app.post("/api/answer", response_model=AnswerOut)
def answer(payload: AnswerIn, request: Request) -> AnswerOut:
    apply_rate_limit(f"answer:{client_ip(request)}", RATE_LIMIT_ANSWER_MAX, RATE_LIMIT_WINDOW_SECONDS)
    user = user_by_token(payload.token)
    if str(user["role"] or "child") != "child":
        raise HTTPException(status_code=403, detail="Answering questions is available on child accounts only")
    uid = int(user["id"])

    safe_answer, safe_time_ms = sanitize_answer_payload(payload.answer, payload.time_ms)

    q = Q.get(payload.qid)
    if not q:
        raise HTTPException(status_code=404, detail="Question not found")

    correct, correct_answer = mark(q, safe_answer)
    explanation = diversify_explanation_text(
        str(q.get("explanation", "")),
        str(q.get("skill", "unknown")),
        str(correct_answer),
    )
    feedback = build_answer_feedback(
        q,
        submitted_answer=safe_answer,
        correct=correct,
        correct_answer=str(correct_answer),
        time_ms=safe_time_ms,
    )

    # streak + goal tracking
    touch_activity(uid)

    # spaced repetition update
    interval_days = update_qprog(uid, payload.qid, correct)

    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO attempts(user_id, qid, domain, skill, correct, time_ms, created_at, answer, correct_answer) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (
            uid,
            str(q["id"]),
            str(q.get("domain", "general")),
            str(q.get("skill", "unknown")),
            1 if correct else 0,
            safe_time_ms,
            int(time.time()),
            safe_answer,
            str(correct_answer),
        ),
    )
    if correct:
        cur.execute("UPDATE users SET coins = COALESCE(coins, 0) + 1 WHERE id=?", (uid,))
    conn.commit()
    conn.close()
    refresh_question_calibration_for_qid(str(q["id"]))

    return AnswerOut(
        correct=correct,
        correct_answer=str(correct_answer),
        explanation=explanation,
        coach_tip=str(feedback.get("coach_tip", "")),
        mistake_check=str(feedback.get("mistake_check", "")),
        next_action=str(feedback.get("next_action", "")),
        speed_band=str(feedback.get("speed_band", "steady")),
        next_due_in_days=float(interval_days) if interval_days is not None else None,
    )

@app.post("/api/stats", response_model=StatsOut)
def stats(payload: StatsIn) -> StatsOut:
    user = user_by_token(payload.token)
    snapshot = plan_snapshot_for_user(user)
    role = str(user["role"] or "child")
    uid = int(user["id"])

    now = int(time.time())
    today = utc_day_id(now)
    day_start = today * 86400

    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT daily_goal, streak_days, coins FROM users WHERE id=?", (uid,))
    urow = cur.fetchone()
    daily_goal = int(urow["daily_goal"])
    streak_days = int(urow["streak_days"])
    coins = int((urow["coins"] if "coins" in urow.keys() else 0) or 0)

    cur.execute("SELECT COUNT(*) AS n, AVG(correct) AS acc FROM attempts WHERE user_id=?", (uid,))
    row = cur.fetchone()
    total = int(row["n"] or 0)
    acc = float(row["acc"] or 0.0)

    # Daily goal counts correct answers only (child-friendly progress).
    cur.execute(
        "SELECT COUNT(*) AS n FROM attempts WHERE user_id=? AND created_at>=? AND correct=1",
        (uid, day_start),
    )
    today_attempts = int(cur.fetchone()["n"] or 0)

    # weakest skills (need a few attempts)
    cur.execute("""
        SELECT skill, COUNT(*) AS n, AVG(correct) AS acc
        FROM attempts
        WHERE user_id=?
        GROUP BY skill
        HAVING n >= 4
        ORDER BY acc ASC, n DESC
        LIMIT 5
    """, (uid,))
    weakest = []
    for r in cur.fetchall():
        weakest.append({
            "skill": str(r["skill"]),
            "attempts": int(r["n"]),
            "accuracy": float(r["acc"]),
        })

    # full skill breakdown (top 12 by attempts)
    cur.execute("""
        SELECT skill, COUNT(*) AS n, AVG(correct) AS acc
        FROM attempts
        WHERE user_id=?
        GROUP BY skill
        ORDER BY n DESC
        LIMIT 12
    """, (uid,))
    skills = []
    for r in cur.fetchall():
        skills.append({
            "skill": str(r["skill"]),
            "attempts": int(r["n"]),
            "accuracy": float(r["acc"]),
        })

    child_count = 0
    assigned_count = 0
    if str(user["role"] or "child") == "parent":
        cur.execute("SELECT COUNT(*) AS n FROM users WHERE role='child' AND parent_user_id=?", (uid,))
        child_count = int((cur.fetchone()["n"] or 0))
        cur.execute("SELECT COUNT(*) AS n FROM assigned_practices WHERE parent_user_id=?", (uid,))
        assigned_count = int((cur.fetchone()["n"] or 0))

    conn.close()

    due_now = due_count(uid)
    progress = build_progress_snapshot(uid)

    if role == "parent":
        log_plan_usage(uid, str(snapshot.get("code", "starter")), child_count, assigned_count, due_now)
    return StatsOut(
        total_attempts=total,
        accuracy=acc,
        today_attempts=today_attempts,
        daily_goal=daily_goal,
        streak_days=streak_days,
        due_now=due_now,
        coins=coins,
        weakest_skills=weakest,
        skills=skills,
        progress_stage=str(progress.get("progress_stage", "building")),
        recommended_difficulty=int(progress.get("recommended_difficulty", 3)),
        recent_accuracy=float(progress.get("recent_accuracy", 0.0)),
        recent_avg_time_ms=int(progress.get("recent_avg_time_ms", 0)),
        learning_focus=str(progress.get("learning_focus", "")),
        domain_progress=list(progress.get("domain_progress", [])),
        mastery_score=int(progress.get("mastery_score", 0)),
        readiness_score=int(progress.get("readiness_score", 0)),
        mastery_band=str(progress.get("mastery_band", "starting")),
        next_recommended_domains=list(progress.get("next_recommended_domains", [])),
        plan_tracking_child_count=child_count,
        plan_tracking_assigned_count=assigned_count,
        plan_tracking_due_count=due_now,
    )


def store_item_by_id(item_id: str) -> Optional[Dict[str, Any]]:
    needle = str(item_id or "").strip()
    if not needle:
        return None
    return next((item for item in STORE_ITEMS if str(item.get("id") or "") == needle), None)


def store_equipped_for_user(cur: sqlite3.Cursor, user_id: int) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT kind, item_id, item_name, equipped_at
        FROM equipped_items
        WHERE user_id=?
        ORDER BY kind ASC
        """,
        (int(user_id),),
    )
    return [
        {
            "kind": str(r["kind"] or ""),
            "item_id": str(r["item_id"] or ""),
            "item_name": str(r["item_name"] or ""),
            "equipped_at": int(r["equipped_at"] or 0),
        }
        for r in cur.fetchall()
    ]


@app.post("/api/store")
def store(payload: StatsIn) -> Dict[str, Any]:
    user = user_by_token(payload.token)
    uid = int(user["id"])
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(coins,0) AS coins FROM users WHERE id=?", (uid,))
    coins = int(cur.fetchone()["coins"] or 0)
    cur.execute("""
        SELECT item_id, item_name, cost, COUNT(*) AS qty
        FROM purchases
        WHERE user_id=?
        GROUP BY item_id, item_name, cost
        ORDER BY item_name ASC
    """, (uid,))
    inventory = [
        {
            "item_id": str(r["item_id"]),
            "item_name": str(r["item_name"]),
            "cost": int(r["cost"]),
            "qty": int(r["qty"]),
        }
        for r in cur.fetchall()
    ]
    equipped = store_equipped_for_user(cur, uid)
    conn.close()
    return {"coins": coins, "items": STORE_ITEMS, "inventory": inventory, "equipped": equipped}


class StoreBuyIn(BaseModel):
    token: str
    item_id: str


class StoreEquipIn(BaseModel):
    token: str
    item_id: str


class StoreUnequipIn(BaseModel):
    token: str
    kind: str = ""
    item_id: str = ""


@app.post("/api/store/buy")
def store_buy(payload: StoreBuyIn) -> Dict[str, Any]:
    user = user_by_token(payload.token)
    uid = int(user["id"])
    item = store_item_by_id(payload.item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Store item not found")

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(coins,0) AS coins FROM users WHERE id=?", (uid,))
    row = cur.fetchone()
    coins = int(row["coins"] or 0)
    cost = int(item["cost"])
    if coins < cost:
        conn.close()
        raise HTTPException(status_code=400, detail="Not enough coins")

    cur.execute("UPDATE users SET coins = coins - ? WHERE id=?", (cost, uid))
    cur.execute(
        "INSERT INTO purchases(user_id, item_id, item_name, cost, created_at) VALUES (?,?,?,?,?)",
        (uid, str(item["id"]), str(item["name"]), cost, int(time.time())),
    )
    # Auto-equip first owned item in a kind so wearables feel immediate.
    item_kind = str(item.get("kind") or "").strip().lower()
    if item_kind:
        cur.execute("SELECT item_id FROM equipped_items WHERE user_id=? AND kind=?", (uid, item_kind))
        has_equipped_kind = cur.fetchone()
        if not has_equipped_kind:
            cur.execute(
                "INSERT OR REPLACE INTO equipped_items(user_id, kind, item_id, item_name, equipped_at) VALUES (?,?,?,?,?)",
                (uid, item_kind, str(item["id"]), str(item["name"]), int(time.time())),
            )
    conn.commit()
    cur.execute("SELECT COALESCE(coins,0) AS coins FROM users WHERE id=?", (uid,))
    new_coins = int(cur.fetchone()["coins"] or 0)
    equipped = store_equipped_for_user(cur, uid)
    conn.close()
    return {"ok": True, "coins": new_coins, "bought": item, "equipped": equipped}


@app.post("/api/store/equip")
def store_equip(payload: StoreEquipIn) -> Dict[str, Any]:
    user = user_by_token(payload.token)
    uid = int(user["id"])
    item = store_item_by_id(payload.item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Store item not found")

    item_id = str(item.get("id") or "")
    kind = str(item.get("kind") or "").strip().lower()
    if not kind:
        raise HTTPException(status_code=400, detail="Item cannot be equipped")

    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 AS ok FROM purchases WHERE user_id=? AND item_id=? LIMIT 1",
        (uid, item_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=400, detail="Buy this item before equipping it")

    cur.execute(
        "INSERT OR REPLACE INTO equipped_items(user_id, kind, item_id, item_name, equipped_at) VALUES (?,?,?,?,?)",
        (uid, kind, item_id, str(item.get("name") or ""), int(time.time())),
    )
    conn.commit()
    equipped = store_equipped_for_user(cur, uid)
    conn.close()
    return {"ok": True, "equipped": equipped, "equipped_item": item}


@app.post("/api/store/unequip")
def store_unequip(payload: StoreUnequipIn) -> Dict[str, Any]:
    user = user_by_token(payload.token)
    uid = int(user["id"])

    kind = str(payload.kind or "").strip().lower()
    if not kind and payload.item_id:
        item = store_item_by_id(payload.item_id)
        if item:
            kind = str(item.get("kind") or "").strip().lower()
    if not kind:
        raise HTTPException(status_code=400, detail="Provide kind or item_id to unequip")

    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM equipped_items WHERE user_id=? AND kind=?", (uid, kind))
    conn.commit()
    equipped = store_equipped_for_user(cur, uid)
    conn.close()
    return {"ok": True, "equipped": equipped, "kind": kind}

@app.get("/api/recent", response_model=RecentOut)
def recent(token: str = Depends(resolve_token_from_request)) -> RecentOut:
    user = user_by_token(token)
    uid = int(user["id"])
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT qid, skill, correct, created_at, answer, correct_answer
        FROM attempts
        WHERE user_id=?
        ORDER BY created_at DESC
        LIMIT 30
    """, (uid,))
    items = []
    for r in cur.fetchall():
        qid = str(r["qid"])
        q = Q.get(qid, {})
        items.append({
            "qid": qid,
            "stem": str(q.get("stem", ""))[:160],
            "skill": str(r["skill"]),
            "correct": int(r["correct"]),
            "answer": str(r["answer"]),
            "correct_answer": str(r["correct_answer"]),
            "ts": int(r["created_at"]),
        })
    conn.close()
    return RecentOut(items=items)


if __name__ == "__main__":
    # Convenience entrypoint so `python3 app.py` works during local development.
    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
