import asyncio
import json
import logging
import os
import random
import signal
import secrets
import sqlite3
import threading
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request as UrlRequest, urlopen

import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

from curl_cffi import requests as cffi_requests
from rate_limits import classify_upstream_429
from rotator import flow_lock, active_flows_count, get_public_ip, get_ip_location, rotation_count

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
IP_HISTORY_LIMIT = 20
PAGE_SIZE = 5
BACKOFF_CAP = 30
POLL_ATTEMPTS = 6
WARP_ROTATION_ATTEMPTS = 4
WARP_POST_ROTATION_SLEEP = 3
DEFAULT_PROMPT_TOKENS = 50
DEFAULT_COMPLETION_TOKENS = 100
STREAM_CHUNK_SIZE = 4096
MODEL_DISCOVERY_INTERVAL = 300
DASHBOARD_REFRESH_INTERVAL = 3
STARTUP_TIME = time.time()
ENABLE_HTTP2 = os.environ.get("ENABLE_HTTP2", "false").lower() in ("true", "1", "yes")
STREAM_TIMEOUT = 600
FALLBACK_RELAY_URL = os.environ.get("FALLBACK_RELAY_URL", "").strip()  # Set to your own relay URL to enable Tier 3 (e.g. https://your-relay.workers.dev)
FLOW_LEASE_TTL_SECONDS = int(os.environ.get("FLOW_LEASE_TTL_SECONDS", "90"))
FLOW_LEASE_HEARTBEAT_SECONDS = int(os.environ.get("FLOW_LEASE_HEARTBEAT_SECONDS", "15"))

# -----------------------------------------------------------------------------
# JSON Structured Logging
# -----------------------------------------------------------------------------
class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": self.formatTime(record, self.datefmt or "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)

# -----------------------------------------------------------------------------
# Proxy Pool / Custom Proxy List Support
# -----------------------------------------------------------------------------
DEFAULT_DATA_DIR = Path("/app/data") if Path("/.dockerenv").exists() else Path(__file__).resolve().parent / "data"
PROXY_FILE = Path(os.environ.get("PROXY_LIST_FILE", str(DEFAULT_DATA_DIR / "proxies.txt")))
_proxy_pool: List[str] = []
_proxy_index = 0
_proxy_lock = threading.Lock()
_last_proxy_mtime: float = 0.0

def load_proxy_list(force: bool = False):
    global _proxy_pool, _last_proxy_mtime
    with _proxy_lock:
        if PROXY_FILE.exists():
            try:
                mtime = PROXY_FILE.stat().st_mtime
                if not force and mtime == _last_proxy_mtime and _proxy_pool:
                    return
                _last_proxy_mtime = mtime
                lines = []
                with open(PROXY_FILE, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            if "://" not in line:
                                line = f"http://{line}"
                            lines.append(line)
                env_proxies = os.environ.get("PROXY_LIST", "").strip()
                if env_proxies:
                    for p in env_proxies.split(","):
                        p = p.strip()
                        if p:
                            if "://" not in p:
                                p = f"http://{p}"
                            lines.append(p)
                _proxy_pool = list(dict.fromkeys(lines))
                if _proxy_pool:
                    log.info(f"Loaded {len(_proxy_pool)} custom proxies into pool.")
            except Exception as e:
                log.error(f"Error reading proxies.txt: {e}")
        elif os.environ.get("PROXY_LIST", "").strip():
            try:
                lines = []
                for p in os.environ["PROXY_LIST"].split(","):
                    p = p.strip()
                    if p:
                        if "://" not in p:
                            p = f"http://{p}"
                        lines.append(p)
                _proxy_pool = list(dict.fromkeys(lines))
                if _proxy_pool:
                    log.info(f"Loaded {len(_proxy_pool)} custom proxies from PROXY_LIST env.")
            except Exception as e:
                log.error(f"Error reading PROXY_LIST env: {e}")
_direct_cooldown_until: float = 0.0
DIRECT_COOLDOWN_SECONDS: int = int(os.environ.get("DIRECT_COOLDOWN_SECONDS", "300"))
ACTIVE_TIER_STATE_FILE = Path("/tmp/opencode-active-tier.json")
_current_active_tier: str = "direct"

def record_active_tier(tier: str, label: str) -> None:
    global _current_active_tier
    _current_active_tier = tier
    try:
        data = {
            "tier": tier,
            "label": label,
            "direct_available": is_direct_available(),
            "timestamp": time.time(),
        }
        fd, tmp_path = tempfile.mkstemp(prefix="opencode-active-tier.", dir="/tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, str(ACTIVE_TIER_STATE_FILE))
        except Exception:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            raise
    except Exception as e:
        log.warning("Failed to record active tier '%s': %s", tier, e)


def is_direct_available() -> bool:
    global _direct_cooldown_until
    return time.time() >= _direct_cooldown_until

def mark_direct_rate_limited():
    global _direct_cooldown_until
    _direct_cooldown_until = time.time() + DIRECT_COOLDOWN_SECONDS
    log.warning(
        "Tier 1 (Direct Home IP) hit 429. Cooldown active for %ds until %s.",
        DIRECT_COOLDOWN_SECONDS,
        time.strftime("%H:%M:%S", time.localtime(_direct_cooldown_until))
    )
    record_active_tier("warp", "warp")

def get_warp_proxy() -> Optional[Dict[str, str]]:
    if not Path("/.dockerenv").exists():
        import socket
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.3)
                if s.connect_ex(("127.0.0.1", 40000)) == 0:
                    return {"http": "socks5h://127.0.0.1:40000", "https": "socks5h://127.0.0.1:40000"}
        except Exception:
            pass
    return None

def get_tiered_proxy(attempt: int = 1) -> tuple[str, Optional[Dict[str, str]]]:
    """
    Tier 1 (attempt == 1 and not cooling down): Direct Home IP (None)
    Tier 2 (attempt > 1 or direct in cooldown): Cloudflare WARP or Custom Proxy Pool
    """
    # Tier 1: Direct Home IP if available and attempt == 1
    if is_direct_available() and attempt == 1:
        record_active_tier("direct", "direct (home)")
        return "direct", None

    # Custom proxy pool if loaded
    load_proxy_list()
    global _proxy_index
    with _proxy_lock:
        if _proxy_pool:
            p_url = _proxy_pool[_proxy_index % len(_proxy_pool)]
            _proxy_index += 1
            return "proxy_pool", {"http": p_url, "https": p_url}
        custom_proxy = os.environ.get("CUSTOM_OUTBOUND_PROXY", "").strip()
        if custom_proxy:
            return "custom_proxy", {"http": custom_proxy, "https": custom_proxy}

    # Tier 2: Cloudflare WARP
    warp_proxy = get_warp_proxy()
    if warp_proxy:
        return "warp", warp_proxy

    return "direct", None

def get_next_outbound_proxy() -> Optional[Dict[str, str]]:
    _, proxy = get_tiered_proxy(attempt=1)
    return proxy
# -----------------------------------------------------------------------------
# SQLite — WAL mode + retry for concurrent safety
# -----------------------------------------------------------------------------
DB_FILE = Path(os.environ.get("METRICS_DB_PATH", str(DEFAULT_DATA_DIR / "metrics.db")))
_db_lock = threading.Lock()

def _get_conn():
    conn = sqlite3.connect(str(DB_FILE), timeout=5)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def _db_execute(statement: str, params=()):
    for attempt in range(3):
        try:
            with _db_lock:
                conn = _get_conn()
                try:
                    cursor = conn.cursor()
                    cursor.execute(statement, params)
                    conn.commit()
                    return cursor
                finally:
                    conn.close()
        except sqlite3.OperationalError as e:
            if "busy" in str(e).lower() and attempt < 2:
                time.sleep(0.1 * (attempt + 1))
                continue
            raise

def _db_fetchall(statement: str, params=()) -> list:
    for attempt in range(3):
        try:
            with _db_lock:
                conn = _get_conn()
                try:
                    cursor = conn.cursor()
                    cursor.execute(statement, params)
                    return cursor.fetchall()
                finally:
                    conn.close()
        except sqlite3.OperationalError as e:
            if "busy" in str(e).lower() and attempt < 2:
                time.sleep(0.1 * (attempt + 1))
                continue
            raise

def init_db():
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS model_usage (
            model_name TEXT PRIMARY KEY,
            requests INTEGER DEFAULT 0,
            prompt_tokens INTEGER DEFAULT 0,
            completion_tokens INTEGER DEFAULT 0,
            total_tokens INTEGER DEFAULT 0,
            estimated_cost_usd REAL DEFAULT 0.0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS ip_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ip TEXT,
            country TEXT,
            flag TEXT,
            timestamp TEXT,
            reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS warp_quality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            success INTEGER,
            latency_ms REAL,
            old_ip TEXT,
            new_ip TEXT
        )
    """)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS active_flow_leases (
            lease_id TEXT PRIMARY KEY,
            expires_at REAL NOT NULL
        )
    """)


def acquire_flow_lease() -> str:
    lease_id = uuid.uuid4().hex
    touch_flow_lease(lease_id)
    return lease_id


def touch_flow_lease(lease_id: str) -> None:
    _db_execute(
        "INSERT OR REPLACE INTO active_flow_leases (lease_id, expires_at) VALUES (?, ?)",
        (lease_id, time.time() + FLOW_LEASE_TTL_SECONDS),
    )


def release_flow_lease(lease_id: str) -> None:
    _db_execute("DELETE FROM active_flow_leases WHERE lease_id = ?", (lease_id,))

def log_ip_rotation_to_db(ip: str, country: str, flag: str, timestamp: str, reason: str):
    try:
        _db_execute(
            "INSERT INTO ip_history (ip, country, flag, timestamp, reason) VALUES (?, ?, ?, ?, ?)",
            (ip, country, flag, timestamp, reason)
        )
    except Exception as e:
        log.error(f"Failed to log IP rotation to DB: {e}")

def load_ip_history_from_db() -> List[Dict[str, any]]:
    if not DB_FILE.exists():
        return []
    try:
        rows = _db_fetchall(
            "SELECT ip, country, flag, timestamp, reason FROM ip_history ORDER BY id DESC LIMIT ?",
            (IP_HISTORY_LIMIT,)
        )
        history = []
        for r in reversed(rows):
            history.append({
                "ip": r[0], "country": r[1], "flag": r[2],
                "timestamp": r[3], "reason": r[4]
            })
        return history
    except Exception as e:
        log.error(f"Error loading IP history from DB: {e}")
        return []

def load_metrics_from_db() -> Dict[str, Dict[str, any]]:
    if not DB_FILE.exists():
        return {}
    try:
        rows = _db_fetchall(
            "SELECT model_name, requests, prompt_tokens, completion_tokens, total_tokens, estimated_cost_usd FROM model_usage"
        )
        stats = {}
        for r in rows:
            stats[r[0]] = {
                "requests": r[1], "prompt_tokens": r[2],
                "completion_tokens": r[3], "total_tokens": r[4],
                "estimated_cost_usd": r[5]
            }
        return stats
    except Exception as e:
        log.error(f"Error loading metrics from DB: {e}")
        return {}

# -----------------------------------------------------------------------------
# Prometheus Metrics
# -----------------------------------------------------------------------------
prom_requests_total = Counter("proxy_requests_total", "Total proxied requests", ["model", "endpoint"])
prom_requests_success = Counter("proxy_requests_success", "Successful proxied requests", ["model"])
prom_requests_rate_limited = Counter("proxy_requests_rate_limited", "Rate-limited requests", ["model"])
prom_rotation_count = Counter("proxy_rotations_total", "Total WARP rotations")
prom_active_flows = Gauge("proxy_active_flows", "Currently active streaming flows")
prom_request_duration = Histogram("proxy_request_duration_seconds", "Request duration", ["model", "endpoint"],
                                   buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0))
prom_warp_health = Gauge("proxy_warp_health", "WARP health (1=healthy, 0=unhealthy)")

# -----------------------------------------------------------------------------
# curl_cffi Session Pool
# -----------------------------------------------------------------------------
_session_pool: Dict[str, "SessionType"] = {}
_session_pool_lock = threading.Lock()
SessionType = None  # resolved at first use

def _get_session(endpoint: str):
    global SessionType
    if SessionType is None:
        from curl_cffi.requests import Session as SessionType
    with _session_pool_lock:
        if endpoint not in _session_pool:
            kwargs = {}
            if not ENABLE_HTTP2:
                from curl_cffi import CurlHttpVersion
                kwargs["http_version"] = CurlHttpVersion.V1_1
            _session_pool[endpoint] = SessionType(**kwargs)
        return _session_pool[endpoint]

def create_fresh_session(is_stream: bool):
    global SessionType
    if SessionType is None:
        from curl_cffi.requests import Session as SessionType
    kwargs = {}
    if not ENABLE_HTTP2:
        from curl_cffi import CurlHttpVersion
        kwargs["http_version"] = CurlHttpVersion.V1_1
    return SessionType(**kwargs)

def _close_all_sessions():
    with _session_pool_lock:
        for ep, sess in _session_pool.items():
            try:
                sess.close()
            except Exception:
                pass
        _session_pool.clear()

_discovery_stop = threading.Event()

# -----------------------------------------------------------------------------
# Request Queue (drains during rotation)
# -----------------------------------------------------------------------------
_rotation_in_progress = threading.Event()
_request_drain_event = asyncio.Event()
_request_drain_event.set()

async def wait_for_rotation_drain():
    if _rotation_in_progress.is_set():
        try:
            await asyncio.wait_for(_request_drain_event.wait(), timeout=15)
        except asyncio.TimeoutError:
            log.warning("Rotation drain wait timed out after 15s; proceeding with request.")
            metrics["rotation_drain_timeouts"] = metrics.get("rotation_drain_timeouts", 0) + 1

def signal_rotation_start():
    _rotation_in_progress.set()
    _request_drain_event.clear()

def signal_rotation_done():
    _rotation_in_progress.clear()
    _request_drain_event.set()

# -----------------------------------------------------------------------------
# Dual-WARP (active/passive tracking)
# -----------------------------------------------------------------------------
_dual_warp = {
    "active_ip": None,
    "standby_ip": None,
    "active_registration": "primary",
}
_dual_warp_lock = threading.Lock()

def swap_warp_registration():
    with _dual_warp_lock:
        _dual_warp["active_registration"] = (
            "standby" if _dual_warp["active_registration"] == "primary" else "primary"
        )
        return _dual_warp["active_registration"]

# -----------------------------------------------------------------------------
# Model pricing reference (USD per 1M tokens)
# -----------------------------------------------------------------------------
MODEL_PRICING = {
    "deepseek-v4-flash-free": {"input_per_1m": 0.15, "output_per_1m": 0.60},
    "mimo-v2.5-free": {"input_per_1m": 0.20, "output_per_1m": 0.80},
    "qwen3.6-plus-free": {"input_per_1m": 0.40, "output_per_1m": 1.20},
    "minimax-m3-free": {"input_per_1m": 0.30, "output_per_1m": 1.00},
    "nemotron-3-ultra-free": {"input_per_1m": 0.25, "output_per_1m": 0.90},
    "ling-3.0-flash-free": {"input_per_1m": 0.15, "output_per_1m": 0.50},
    "laguna-s-2.1-free": {"input_per_1m": 0.20, "output_per_1m": 0.70},
}

_model_usage_lock = threading.Lock()

def track_token_usage(model_name: str, prompt_tokens: int = 0, completion_tokens: int = 0):
    global model_usage_stats
    pricing = MODEL_PRICING.get(model_name, {"input_per_1m": 0.20, "output_per_1m": 0.80})
    prompt_cost = (prompt_tokens / 1_000_000) * pricing["input_per_1m"]
    completion_cost = (completion_tokens / 1_000_000) * pricing["output_per_1m"]
    cost = prompt_cost + completion_cost

    with _model_usage_lock:
        if model_name not in model_usage_stats:
            model_usage_stats[model_name] = {
                "requests": 0, "prompt_tokens": 0, "completion_tokens": 0,
                "total_tokens": 0, "estimated_cost_usd": 0.0
            }
        model_usage_stats[model_name]["requests"] += 1
        model_usage_stats[model_name]["prompt_tokens"] += prompt_tokens
        model_usage_stats[model_name]["completion_tokens"] += completion_tokens
        model_usage_stats[model_name]["total_tokens"] += (prompt_tokens + completion_tokens)
        model_usage_stats[model_name]["estimated_cost_usd"] += cost

    try:
        _db_execute("""
            INSERT INTO model_usage (model_name, requests, prompt_tokens, completion_tokens, total_tokens, estimated_cost_usd)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(model_name) DO UPDATE SET
                requests = requests + excluded.requests,
                prompt_tokens = prompt_tokens + excluded.prompt_tokens,
                completion_tokens = completion_tokens + excluded.completion_tokens,
                total_tokens = total_tokens + excluded.total_tokens,
                estimated_cost_usd = estimated_cost_usd + excluded.estimated_cost_usd,
                updated_at = CURRENT_TIMESTAMP
        """, (model_name, 1, prompt_tokens, completion_tokens, prompt_tokens + completion_tokens, cost))
    except Exception as e:
        log.error(f"Failed to persist metrics to SQLite: {e}")

# -----------------------------------------------------------------------------
# WARP Quality Metrics
# -----------------------------------------------------------------------------
warp_quality_stats = {
    "total_attempts": 0, "successful_rotations": 0, "failed_rotations": 0,
    "last_latency_ms": 0.0, "avg_latency_ms": 0.0
}
_warp_quality_lock = threading.Lock()

def record_warp_rotation(success: bool, latency_ms: float = 0, old_ip: str = "", new_ip: str = ""):
    with _warp_quality_lock:
        warp_quality_stats["total_attempts"] += 1
        if success:
            warp_quality_stats["successful_rotations"] += 1
            warp_quality_stats["last_latency_ms"] = latency_ms
            n = warp_quality_stats["successful_rotations"]
            warp_quality_stats["avg_latency_ms"] = (
                (warp_quality_stats["avg_latency_ms"] * (n - 1) + latency_ms) / n
            )
        else:
            warp_quality_stats["failed_rotations"] += 1
    try:
        _db_execute(
            "INSERT INTO warp_quality (timestamp, success, latency_ms, old_ip, new_ip) VALUES (?, ?, ?, ?, ?)",
            (time.strftime("%Y-%m-%d %H:%M:%S"), 1 if success else 0, latency_ms, old_ip, new_ip)
        )
    except Exception:
        pass

# -----------------------------------------------------------------------------
# Backoff helper
# -----------------------------------------------------------------------------
def compute_backoff_delay(attempt: int, base: float = 1.0, cap: int = BACKOFF_CAP) -> float:
    return min(base * (2 ** (attempt - 1)), cap) + random.uniform(0.5, 1.5)

# -----------------------------------------------------------------------------
# Jinja2 Templates
# -----------------------------------------------------------------------------
_templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

# -----------------------------------------------------------------------------
model_usage_stats: Dict[str, Dict[str, float]] = {}

# Configuration & Dynamic Discovery
# -----------------------------------------------------------------------------
PORT = int(os.environ.get("OPENCODE_ZEN_PORT", "8765"))
HOST = os.environ.get("OPENCODE_ZEN_HOST", "127.0.0.1")
TARGET_ZEN_BASE = os.environ.get("OPENCODE_ZEN_TARGET_BASE", "https://opencode.ai/zen/v1")
TARGET_ZEN_URL = f"{TARGET_ZEN_BASE}/chat/completions"
TARGET_ZEN_ANTHROPIC_URL = f"{TARGET_ZEN_BASE}/messages"
TARGET_ZEN_RESPONSES_URL = f"{TARGET_ZEN_BASE}/responses"

MAX_RETRIES_ON_429 = int(os.environ.get("MAX_RETRIES_ON_429", "8"))
INITIAL_BACKOFF = float(os.environ.get("INITIAL_BACKOFF", "1"))
WARP_ROTATOR_URL = os.environ.get("WARP_ROTATOR_URL", "http://127.0.0.1:8001").rstrip("/")
CORS_ALLOW_ORIGINS = [
    origin.strip()
    for origin in os.environ.get("CORS_ALLOW_ORIGINS", "http://127.0.0.1:8765,http://localhost:8765").split(",")
    if origin.strip()
]

metrics = {
    "total_requests": 0,
    "successful_requests": 0,
    "rate_limited_requests": 0,
    "fallback_triggered": 0,
    "discovered_models_count": 0,
    "start_time": time.time()
}

DEFAULT_FREE_MODELS = [
    {"id": "space-bunny-free", "name": "Space Bunny Free"},
    {"id": "mimo-v2.6-flash-free", "name": "MiMo V2.6 Flash Free"},
    {"id": "deepseek-v4-flash-free", "name": "DeepSeek V4 Flash Free"},
    {"id": "mimo-v2.5-free", "name": "MiMo V2.5 Free"},
    {"id": "nemotron-3.5-lightning-free", "name": "Nemotron 3.5 Lightning Free"},
    {"id": "ling-3.1-flash-free", "name": "Ling 3.1 Flash Free"},
]

discovered_models: List[Dict[str, str]] = DEFAULT_FREE_MODELS.copy()
_discovery_lock = threading.Lock()

LOG_FORMAT = os.environ.get("LOG_FORMAT", "text").lower()
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_LEVEL_VALUE = getattr(logging, LOG_LEVEL, logging.INFO)

if LOG_FORMAT == "json":
    _handler = logging.StreamHandler()
    _handler.setFormatter(JSONFormatter())
    logging.basicConfig(level=LOG_LEVEL_VALUE, handlers=[_handler], force=True)
else:
    logging.basicConfig(
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        level=LOG_LEVEL_VALUE,
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
log = logging.getLogger("zen_server")

@asynccontextmanager
async def lifespan(application: FastAPI):
    global model_usage_stats
    init_db()
    model_usage_stats = load_metrics_from_db()
    record_active_tier("direct", "direct (home)")
    _discovery_stop.clear()
    threading.Thread(target=discover_models_task, daemon=True).start()
    yield
    _close_all_sessions()
    _discovery_stop.set()

app = FastAPI(title="OpenCode Zen v3.0 Ultra Resilient Proxy", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def discover_models_task():
    global discovered_models
    while not _discovery_stop.is_set():
        try:
            req = UrlRequest(
                f"{TARGET_ZEN_BASE}/models",
                headers={"Authorization": "Bearer public", "User-Agent": "Mozilla/5.0"}
            )
            with urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                models_data = data.get("data", [])
                if models_data:
                    new_models = []
                    for m in models_data:
                        m_id = m.get("id", "")
                        if "free" in m_id.lower() or "zen" in m_id.lower():
                            new_models.append({"id": m_id, "name": m_id.replace("-", " ").title()})
                    
                    if new_models:
                        with _discovery_lock:
                            discovered_models = new_models
                            metrics["discovered_models_count"] = len(discovered_models)
                        log.info(f"Auto-Discovery refreshed: {len(discovered_models)} active model(s) fetched.")
        except Exception as e:
            log.debug(f"Auto-Discovery fallback active: {e}")
        _discovery_stop.wait(300)

class FlowContext:
    def __enter__(self):
        global active_flows_count
        with flow_lock:
            active_flows_count += 1
            prom_active_flows.set(active_flows_count)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global active_flows_count
        with flow_lock:
            active_flows_count = max(0, active_flows_count - 1)
            prom_active_flows.set(active_flows_count)

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    model: str = Field(default="space-bunny-free")
    messages: List[ChatMessage]
    stream: Optional[bool] = False
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = None

DEFAULT_USER_AGENT = "opencode/1.18.31 ai-sdk/provider-utils/4.0.40 runtime/bun/1.3.14"
BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
OPENCODE_FINGERPRINT_TOOLS = ["bash", "glob", "grep", "read", "edit", "write"]
OPENCODE_PROJECT_ID = secrets.token_hex(20)

def generate_session_id() -> str:
    now_ms = int(time.time() * 1000)
    current = (now_ms * 0x1000) + random.randint(1, 4095)
    val = (~current) & 0xFFFFFFFFFFFF
    time_hex = f"{val:012x}"
    rand_chars = "".join(random.choice(BASE62) for _ in range(14))
    return f"ses_{time_hex}{rand_chars}"

def generate_request_id() -> str:
    now_ms = int(time.time() * 1000)
    current = (now_ms * 0x1000) + 1
    val = current & 0xFFFFFFFFFFFF
    time_hex = f"{val:012x}"
    rand_chars = "".join(random.choice(BASE62) for _ in range(14))
    return f"msg_{time_hex}{rand_chars}"

def ensure_opencode_fingerprint(payload: dict, is_responses: bool = False) -> None:
    payload["stream"] = True
    if is_responses:
        payload["store"] = False
        tools = payload.setdefault("tools", [])
        existing = {t.get("name") for t in tools if isinstance(t, dict)}
        for name in OPENCODE_FINGERPRINT_TOOLS:
            if name not in existing:
                tools.append({
                    "type": "function",
                    "name": name,
                    "description": f"OpenCode built-in {name} tool",
                    "parameters": {"type": "object", "properties": {}},
                })
    else:
        tools = payload.setdefault("tools", [])
        existing = set()
        for t in tools:
            if isinstance(t, dict):
                fn = t.get("function")
                if isinstance(fn, dict) and "name" in fn:
                    existing.add(fn["name"])
                elif "name" in t:
                    existing.add(t["name"])
        for name in OPENCODE_FINGERPRINT_TOOLS:
            if name not in existing:
                tools.append({
                    "type": "function",
                    "function": {
                        "name": name,
                        "description": f"OpenCode built-in {name} tool",
                        "parameters": {"type": "object", "properties": {}},
                    },
                })
        if payload.get("tool_choice") is None:
            payload["tool_choice"] = "none"

def aggregate_stream_to_chat_completion(stream_lines, model_name: str) -> dict:
    content_parts = []
    finish_reason = "stop"
    completion_id = f"chatcmpl-{int(time.time())}"
    for line in stream_lines:
        if isinstance(line, bytes):
            line = line.decode("utf-8", errors="ignore")
        line = line.strip()
        if not line or not line.startswith("data:"):
            continue
        data_str = line[5:].strip()
        if data_str == "[DONE]":
            break
        try:
            chunk = json.loads(data_str)
            if "id" in chunk:
                completion_id = chunk["id"]
            choices = chunk.get("choices", [])
            if choices:
                delta = choices[0].get("delta", {})
                if "content" in delta and delta["content"]:
                    content_parts.append(delta["content"])
                if choices[0].get("finish_reason"):
                    finish_reason = choices[0]["finish_reason"]
            elif chunk.get("type") in ("response.output_text.delta", "response.output_item.delta"):
                delta_raw = chunk.get("delta", "")
                delta_text = delta_raw if isinstance(delta_raw, str) else (delta_raw.get("text", "") if isinstance(delta_raw, dict) else "")
                if delta_text:
                    content_parts.append(delta_text)
        except Exception:
            continue
    full_content = "".join(content_parts)
    return {
        "id": completion_id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model_name,
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": full_content,
                },
                "finish_reason": finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": DEFAULT_PROMPT_TOKENS,
            "completion_tokens": max(1, len(full_content) // 4),
            "total_tokens": DEFAULT_PROMPT_TOKENS + max(1, len(full_content) // 4),
        },
    }
def aggregate_stream_to_response(stream_lines, model_name: str) -> dict:
    content_parts = []
    response_id = f"resp_{int(time.time())}"
    for line in stream_lines:
        if isinstance(line, bytes):
            line = line.decode("utf-8", errors="ignore")
        line = line.strip()
        if not line or not line.startswith("data:"):
            continue
        data_str = line[5:].strip()
        if data_str == "[DONE]":
            break
        try:
            chunk = json.loads(data_str)
            if chunk.get("type") == "response.created":
                r_obj = chunk.get("response", {})
                if "id" in r_obj:
                    response_id = r_obj["id"]
            elif chunk.get("type") == "response.output_item.delta":
                delta = chunk.get("delta", {})
                if "text" in delta:
                    content_parts.append(delta["text"])
            elif chunk.get("type") == "response.completed":
                r_obj = chunk.get("response", {})
                if "id" in r_obj:
                    response_id = r_obj["id"]
        except Exception:
            continue
    full_content = "".join(content_parts)
    return {
        "id": response_id,
        "object": "response",
        "created": int(time.time()),
        "model": model_name,
        "status": "completed",
        "output": [
            {
                "id": f"item_{int(time.time())}",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text": full_content}],
            }
        ],
    }

def get_realistic_headers(raw_request: Optional[Request] = None) -> Dict[str, str]:
    session = generate_session_id()
    trace_id = secrets.token_hex(16)
    span_id = secrets.token_hex(8)
    headers = {
        "content-type": "application/json",
        "authorization": "Bearer public",
        "accept": "text/event-stream, application/json, */*",
        "user-agent": DEFAULT_USER_AGENT,
        "x-opencode-client": "desktop",
        "x-opencode-project": OPENCODE_PROJECT_ID,
        "x-opencode-session": session,
        "x-session-affinity": session,
        "x-opencode-request": generate_request_id(),
        "b3": f"{trace_id}-{span_id}-1-{span_id}",
        "traceparent": f"00-{trace_id}-{span_id}-01",
    }
    if raw_request:
        for k, v in raw_request.headers.items():
            kl = k.lower()
            if kl == "user-agent":
                if v.startswith("opencode/"):
                    headers["user-agent"] = v
            elif kl == "authorization" and v.strip() and v != "Bearer placeholder":
                headers["authorization"] = v
            elif kl.startswith("x-opencode-") or kl.startswith("anthropic-") or kl in ("b3", "traceparent", "x-session-affinity"):
                if kl in ("x-opencode-session", "x-session-affinity") and not v.startswith("ses_"):
                    continue
                headers[kl] = v
    headers["host"] = "opencode.ai"
    return headers


SAFE_UPSTREAM_HEADERS = {
    "content-type",
    "retry-after",
    "x-request-id",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "cf-ray",
}
SENSITIVE_LOG_KEYS = {"authorization", "api_key", "apikey", "token", "password", "secret"}


def redact_for_log(value):
    if isinstance(value, dict):
        return {
            key: "[redacted]"
            if any(marker in key.lower().replace("-", "_") for marker in SENSITIVE_LOG_KEYS)
            else redact_for_log(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_for_log(item) for item in value]
    if isinstance(value, str) and len(value) > 1000:
        return value[:1000] + "...[truncated]"
    return value


def log_upstream_response(response, model_name: str, endpoint: str, attempt: int, uses_proxy: bool) -> None:
    headers = {
        key: value
        for key, value in response.headers.items()
        if key.lower() in SAFE_UPSTREAM_HEADERS
    }
    log.debug(
        "Upstream response model=%s endpoint=%s attempt=%s status=%s uses_proxy=%s headers=%s",
        model_name,
        endpoint,
        attempt,
        response.status_code,
        uses_proxy,
        headers,
    )


def upstream_rate_limit_response(response, model_name: str) -> JSONResponse:
    category, retry_seconds, payload = classify_upstream_429(response)
    headers = {"X-Rate-Limit-Reason": category}
    if retry_seconds is not None:
        headers["Retry-After"] = str(retry_seconds)
    log.warning(
        "Upstream 429 for model '%s' classified as %s (retry_after=%s); headers=%s payload=%s",
        model_name,
        category,
        retry_seconds,
        {key: value for key, value in response.headers.items() if key.lower() in SAFE_UPSTREAM_HEADERS},
        redact_for_log(payload),
    )
    return JSONResponse(status_code=429, content=payload, headers=headers)


def format_upstream_error_response(response, model_name: str) -> JSONResponse:
    try:
        content = response.json()
    except Exception:
        content = {"error": {"message": response.text or "Upstream error", "code": response.status_code, "model": model_name}}
    return JSONResponse(status_code=response.status_code, content=content)

def rotate_egress(reason: str) -> tuple[bool, Optional[str]]:
    """Request rotation from the service that owns the shared WARP namespace."""
    try:
        response = cffi_requests.post(f"{WARP_ROTATOR_URL}/rotate", timeout=35)
        data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        if response.status_code == 200 and data.get("status") == "success":
            return True, data.get("verified_ip")
        return False, None
    except Exception as exc:
        log.warning("Rotator request failed: %s", exc)
        return False, None
def attempt_cloud_relay_fallback(
    headers: Dict[str, str],
    payload: dict,
    relay_path: str,
    model_name: str,
) -> tuple[Optional[object], Optional[object]]:
    """POST payload to Tier 3 cloud relay. Returns (response, session) on 200, else (None, None)."""
    if not FALLBACK_RELAY_URL:
        return None, None
    fallback_session = None
    fallback_resp = None
    try:
        log.warning(
            "Local tiers exhausted for '%s'. Escalating to Tier 3 (Cloud Relay): %s",
            model_name,
            FALLBACK_RELAY_URL,
        )
        relay_headers = {k: v for k, v in headers.items() if k.lower() not in ("host", "x-relay-target", "x-relay-path")}
        relay_headers["x-relay-target"] = "https://opencode.ai"
        relay_headers["x-relay-path"] = relay_path
        fallback_session = create_fresh_session(True)
        fallback_resp = fallback_session.post(
            FALLBACK_RELAY_URL,
            json=payload,
            headers=relay_headers,
            impersonate="chrome124",
            stream=True,
            timeout=(6, STREAM_TIMEOUT),
        )
        if fallback_resp.status_code == 200:
            log.info("Tier 3 (Cloud Relay) succeeded 200 for model '%s'", model_name)
            record_active_tier("render", "render")
            return fallback_resp, fallback_session
        log.warning(
            "Cloud relay fallback returned HTTP %s for model '%s'",
            fallback_resp.status_code,
            model_name,
        )
    except Exception as fb_err:
        log.warning("Cloud relay fallback failed: %s", fb_err)
    for obj in (fallback_resp, fallback_session):
        try:
            if obj is not None:
                obj.close()
        except Exception:
            pass
    return None, None
def close_upstream(response: Optional[object] = None, session: Optional[object] = None) -> None:
    """Best-effort close of a curl_cffi response/session superseded by retry or fallback."""
    for obj in (response, session):
        try:
            if obj is not None:
                obj.close()
        except Exception:
            pass



class EmptyStreamError(Exception):
    """Raised when upstream returns an empty or truncated stream without valid content/tool calls."""
    pass

async def stream_response(response, model_name: str, session=None) -> AsyncGenerator[bytes, None]:
    loop = asyncio.get_event_loop()
    global active_flows_count

    with flow_lock:
        active_flows_count += 1
        prom_active_flows.set(active_flows_count)
    lease_id = await asyncio.to_thread(acquire_flow_lease)

    async def keep_flow_lease_alive():
        try:
            while True:
                await asyncio.sleep(FLOW_LEASE_HEARTBEAT_SECONDS)
                await asyncio.to_thread(touch_flow_lease, lease_id)
        except asyncio.CancelledError:
            return

    lease_heartbeat = asyncio.create_task(keep_flow_lease_alive())

    chunk_count = 0
    last_raw_line = ""
    buffered_lines = []
    has_meaningful_content = False
    truncated = False

    try:
        def get_next_line(iter_lines):
            try:
                return next(iter_lines)
            except StopIteration:
                return "STOP_ITERATION"
            except Exception as exc:
                log.error(f"[STREAM DEBUG] Upstream socket/connection error for '{model_name}': {type(exc).__name__}: {exc}")
                return "SOCKET_ERROR"

        line_iter = response.iter_lines()

        # Step 1: Buffer up to 10 initial lines or until we confirm non-empty content
        while len(buffered_lines) < 10:
            item = await loop.run_in_executor(None, get_next_line, line_iter)
            if item in ("STOP_ITERATION", "SOCKET_ERROR"):
                break

            line = item
            if line:
                raw_text = line.decode("utf-8", errors="ignore").strip()
                if raw_text and not raw_text.startswith(":"):
                    buffered_lines.append(line)
                    if "content" in raw_text or "tool_calls" in raw_text or "reasoning_content" in raw_text:
                        # Check if it's not just an empty choices array
                        if '"choices":[]' not in raw_text.replace(" ", ""):
                            has_meaningful_content = True
                            break

        # If stream ended prematurely without producing any meaningful content or tool calls
        if not has_meaningful_content and len(buffered_lines) < 4:
            all_buffered = "".join([l.decode("utf-8", errors="ignore") for l in buffered_lines])
            if '"choices":[]' in all_buffered.replace(" ", "") or len(buffered_lines) == 0:
                log.warning(f"[STREAM RECOVERY] Upstream returned empty/truncated stream for '{model_name}'. Triggering IP rotation & raising EmptyStreamError.")
                raise EmptyStreamError("Upstream returned empty response stream")

        # Yield buffered initial lines
        # Yield buffered initial lines with proper SSE event delimiters
        for b_line in buffered_lines:
            chunk_count += 1
            if b_line:
                yield b_line + b"\n\n"
        # Step 2: Continue streaming remaining lines
        while True:
            item = await loop.run_in_executor(None, get_next_line, line_iter)
            if item == "STOP_ITERATION":
                log.info(f"[STREAM DEBUG] Upstream reached natural StopIteration for '{model_name}'. Total lines: {chunk_count}")
                break
            if item == "SOCKET_ERROR":
                log.warning(f"[STREAM DEBUG] Upstream connection aborted via socket error for '{model_name}'. Lines sent: {chunk_count}")
                truncated = True
                break

            line = item
            if line:
                chunk_count += 1
                try:
                    last_raw_line = line.decode("utf-8", errors="ignore")
                except Exception:
                    pass
                yield line + b"\n"
            else:
                yield b"\n"

        if truncated:
            log.warning(f"[STREAM DEBUG] Upstream stream truncated for '{model_name}' after {chunk_count} lines; closing without [DONE].")
            yield b'\ndata: {"error": {"message": "Upstream stream truncated", "type": "upstream_error"}}\n\n'
        else:
            log.info(f"Streaming completed successfully for model '{model_name}' ({chunk_count} lines sent).")
            yield b"\ndata: [DONE]\n\n"
    except EmptyStreamError:
        raise
    except GeneratorExit:
        log.warning(f"[STREAM DEBUG] Client (OpenCode) explicitly closed/aborted SSE connection prematurely for '{model_name}' after {chunk_count} lines.")
    except Exception as e:
        log.error(f"Stream exception caught for model '{model_name}': {type(e).__name__}: {e}", exc_info=True)
        yield b'\ndata: {"error": {"message": "Upstream stream error", "type": "upstream_error"}}\n\n'
    finally:
        lease_heartbeat.cancel()
        await asyncio.gather(lease_heartbeat, return_exceptions=True)
        await asyncio.to_thread(release_flow_lease, lease_id)
        with flow_lock:
            active_flows_count = max(0, active_flows_count - 1)
            prom_active_flows.set(active_flows_count)
        if session:
            try:
                session.close()
            except Exception:
                pass
async def stream_responses_as_chat_completions(response, model_name: str, session=None) -> AsyncGenerator[bytes, None]:
    loop = asyncio.get_event_loop()
    line_iter = response.iter_lines()
    completion_id = f"chatcmpl-muse-{int(time.time())}"
    def get_next_line(iter_lines):
        try:
            return next(iter_lines)
        except StopIteration:
            return "STOP_ITERATION"
        except Exception:
            return "SOCKET_ERROR"

    try:
        while True:
            item = await loop.run_in_executor(None, get_next_line, line_iter)
            if item in ("STOP_ITERATION", "SOCKET_ERROR"):
                break
            if not item:
                continue
            line = item.decode("utf-8", errors="ignore") if isinstance(item, bytes) else item
            line = line.strip()
            if not line.startswith("data:"):
                continue
            data_str = line[5:].strip()
            if data_str == "[DONE]":
                break
            try:
                evt = json.loads(data_str)
                evt_type = evt.get("type")
                if evt_type in ("response.output_text.delta", "response.output_item.delta"):
                    delta_raw = evt.get("delta", "")
                    delta_text = delta_raw if isinstance(delta_raw, str) else (delta_raw.get("text", "") if isinstance(delta_raw, dict) else "")
                    if delta_text:
                        chunk = {
                            "id": completion_id,
                            "object": "chat.completion.chunk",
                            "created": int(time.time()),
                            "model": model_name,
                            "choices": [{"index": 0, "delta": {"content": delta_text}, "finish_reason": None}],
                        }
                        yield f"data: {json.dumps(chunk)}\n\n".encode("utf-8")
                elif evt_type == "response.completed":
                    chunk = {
                        "id": completion_id,
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": model_name,
                        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                    }
                    yield f"data: {json.dumps(chunk)}\n\n".encode("utf-8")
            except Exception:
                continue
        yield b"data: [DONE]\n\n"
    except Exception as e:
        log.error(f"Stream exception caught in muse translator: {e}")
        yield b"data: [DONE]\n\n"
    finally:
        if session:
            try:
                session.close()
            except Exception:
                pass


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    return _templates.TemplateResponse(request=request, name="dashboard.html")

@app.get("/metrics-prometheus")
async def metrics_prometheus():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/api/rotate")
async def manual_rotate():
    signal_rotation_start()
    try:
        started = time.monotonic()
        result, verified_ip = await asyncio.to_thread(rotate_egress, "Manual API trigger")
        record_warp_rotation(result, (time.monotonic() - started) * 1000, new_ip=verified_ip or "")
        if result:
            swap_warp_registration()
            return {"status": "success", "verified_ip": verified_ip}
        raise HTTPException(status_code=503, detail="WARP rotator did not complete the requested rotation")
    finally:
        signal_rotation_done()

@app.get("/metrics")
async def get_metrics():
    uptime = int(time.time() - metrics["start_time"])

    # Always ensure full historical stats from SQLite DB are included
    db_usage = load_metrics_from_db()
    for m_name, m_data in db_usage.items():
        if m_name not in model_usage_stats:
            model_usage_stats[m_name] = m_data
        else:
            # Sync highest values or keep memory in sync with DB
            model_usage_stats[m_name]["requests"] = max(model_usage_stats[m_name]["requests"], m_data["requests"])
            model_usage_stats[m_name]["prompt_tokens"] = max(model_usage_stats[m_name]["prompt_tokens"], m_data["prompt_tokens"])
            model_usage_stats[m_name]["completion_tokens"] = max(model_usage_stats[m_name]["completion_tokens"], m_data["completion_tokens"])
            model_usage_stats[m_name]["total_tokens"] = max(model_usage_stats[m_name]["total_tokens"], m_data["total_tokens"])
            model_usage_stats[m_name]["estimated_cost_usd"] = max(model_usage_stats[m_name]["estimated_cost_usd"], m_data["estimated_cost_usd"])

    # Fetch live data from warp-rotator microservice (offloaded to executor — blocking call)
    rotator_ip = None
    rotator_location = None
    rotator_rotations = rotation_count
    rotator_history = []

    def fetch_rotator_status():
        try:
            r = cffi_requests.get(f"{WARP_ROTATOR_URL}/status", impersonate="chrome124", timeout=4)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            log.debug(f"warp-rotator status fetch error: {e}")
        return None

    loop = asyncio.get_event_loop()
    rdata = await loop.run_in_executor(None, fetch_rotator_status)

    if rdata:
        rotator_ip = rdata.get("current_ip")
        rotator_rotations = rdata.get("rotations", rotation_count)
        rotator_history = rdata.get("history", [])
        if rotator_ip:
            def fetch_location():
                return get_ip_location(rotator_ip)
            rotator_location = await loop.run_in_executor(None, fetch_location)

    # Fallback: local IP lookup
    if not rotator_ip:
        def fetch_local_ip():
            ip = get_public_ip()
            loc = get_ip_location(ip) if ip else {"country": "Unknown", "flag": "🌐"}
            return ip, loc
        rotator_ip, rotator_location = await loop.run_in_executor(None, fetch_local_ip)

    # Fallback: SQLite history
    if not rotator_history:
        rotator_history = load_ip_history_from_db()

    if not rotator_location:
        rotator_location = {"country": "Unknown", "flag": "🌐"}

    return {
        "uptime_seconds": uptime,
        "verified_public_ip": rotator_ip,
        "egress_verification_scope": "shared proxy and WARP network namespace",
        "location": rotator_location,
        "total_rotations": rotator_rotations,
        "metrics": metrics,
        "active_flows": active_flows_count,
        "discovered_models": discovered_models,
        "model_usage": model_usage_stats,
        "ip_history": rotator_history,
        "warp_quality": dict(warp_quality_stats),
        "dual_warp": dict(_dual_warp),
        "rotation_in_progress": _rotation_in_progress.is_set()
    }

@app.get("/health")
async def health():
    ip = get_public_ip()
    prom_warp_health.set(1 if ip and ip != "Disconnected" else 0)
    db_ok = False
    try:
        _db_execute("SELECT 1")
        db_ok = True
    except Exception:
        pass
    return {
        "status": "healthy" if db_ok else "degraded",
        "database": "connected" if db_ok else "unreachable",
        "uptime_seconds": int(time.time() - metrics["start_time"]),
        "active_flows": active_flows_count,
        "total_rotations": rotation_count,
        "warp_quality": dict(warp_quality_stats),
    }

@app.get("/v1/models")
async def list_models():
    with _discovery_lock:
        return {
            "object": "list",
            "data": [
                {
                    "id": m["id"],
                    "object": "model",
                    "created": 1700000000,
                    "owned_by": "opencode"
                }
                for m in discovered_models
            ]
        }

@app.post("/v1/chat/completions")
async def chat_completions(raw_request: Request):
    metrics["total_requests"] += 1
    prom_requests_total.labels(model="chat", endpoint="chat_completions").inc()
    await wait_for_rotation_drain()

    start_time = time.time()
    try:
        payload = await raw_request.json()
    except Exception:
        payload = {}

    current_model = payload.get("model", "deepseek-v4-flash-free")
    client_wants_stream = payload.get("stream", False)
    is_stream = True
    is_muse = current_model.startswith("muse-spark")
    if is_muse:
        responses_input = []
        for m in payload.get("messages", []):
            c = m.get("content", "")
            if isinstance(c, list):
                parts = []
                for p in c:
                    if isinstance(p, dict) and "text" in p:
                        parts.append(p["text"])
                    elif isinstance(p, str):
                        parts.append(p)
                c = "\n".join(parts)
            responses_input.append({
                "type": "message",
                "role": m.get("role", "user"),
                "content": c,
            })
        converted_tools = []
        names = set()
        for t in payload.get("tools", []):
            if isinstance(t, dict):
                fn = t.get("function")
                if isinstance(fn, dict) and "name" in fn:
                    name = fn["name"]
                    names.add(name)
                    converted_tools.append({
                        "type": "function",
                        "name": name,
                        "description": fn.get("description", f"tool {name}"),
                        "parameters": fn.get("parameters", {"type": "object", "properties": {}}),
                    })
                elif "name" in t:
                    name = t["name"]
                    names.add(name)
                    converted_tools.append({
                        "type": "function",
                        "name": name,
                        "description": t.get("description", f"tool {name}"),
                        "parameters": t.get("parameters", {"type": "object", "properties": {}}),
                    })
        for name in OPENCODE_FINGERPRINT_TOOLS:
            if name not in names:
                converted_tools.append({
                    "type": "function",
                    "name": name,
                    "description": f"OpenCode built-in {name} tool",
                    "parameters": {"type": "object", "properties": {}},
                })
        target_payload = {
            "model": current_model,
            "input": responses_input,
            "stream": True,
            "store": False,
            "tools": converted_tools,
        }
        upstream_target_url = TARGET_ZEN_RESPONSES_URL
    else:
        ensure_opencode_fingerprint(payload, is_responses=False)
        target_payload = payload
        upstream_target_url = TARGET_ZEN_URL

    log.info(f"Received request for model '{current_model}' (Client Stream: {client_wants_stream} | Muse: {is_muse})")
    headers = get_realistic_headers(raw_request)

    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        try:
            await asyncio.sleep(random.uniform(0.1, 0.3))
            tier_name, proxies = get_tiered_proxy(attempt)
            log.info(f"Dispatching '{current_model}' via [{tier_name.upper()}] (Attempt {attempt}/{MAX_RETRIES_ON_429})")
            session = create_fresh_session(True)
            response = session.post(
                upstream_target_url,
                json=target_payload,
                headers=headers,
                impersonate="chrome124",
                stream=True,
                proxies=proxies,
                timeout=(6, STREAM_TIMEOUT)
            )
            log_upstream_response(response, current_model, "chat_completions", attempt, proxies is not None)

            if response.status_code == 429:
                category, retry_seconds, err_payload = classify_upstream_429(response)
                metrics["rate_limited_requests"] += 1
                prom_requests_rate_limited.labels(model=current_model).inc()

                # Quota errors are account-level: rotation cannot help. Fail fast.
                if category == "quota":
                    close_upstream(response, session)
                    return upstream_rate_limit_response(response, current_model)

                # Tier 1 (Direct Home IP): mark cooldown and immediately advance to Tier 2 (WARP)
                if tier_name == "direct":
                    close_upstream(response, session)
                    mark_direct_rate_limited()
                    log.warning(f"Tier 1 (Direct Home IP) rate limited on '{current_model}'. Escalating to Tier 2 (WARP)...")
                    continue

                # Tier 2 (WARP / Proxy Pool): rotate WARP IP
                if tier_name in ("warp", "proxy_pool") and attempt < (MAX_RETRIES_ON_429 - 1):
                    close_upstream(response, session)
                    log.warning(f"Tier 2 (WARP) rate limited on '{current_model}' (Attempt {attempt}/{MAX_RETRIES_ON_429}). Rotating WARP IP...")
                    rotated, verified_ip = await asyncio.to_thread(rotate_egress, f"Tier 2 WARP 429 on {current_model}")
                    if rotated:
                        swap_warp_registration()
                        await asyncio.sleep(1)
                        continue

                # Tier 3 (Cloud Relay): fallback before giving up.
                # On success, fall through to normal success handling below (same iteration).
                relay_path = "/zen/v1/responses" if is_muse else "/zen/v1/chat/completions"
                relay_resp, relay_session = attempt_cloud_relay_fallback(headers, target_payload, relay_path, current_model)
                close_upstream(response, session)
                if relay_resp is not None:
                    response = relay_resp
                    session = relay_session
                    tier_name = "render"
                else:
                    return upstream_rate_limit_response(response, current_model)

            if response.status_code >= 500 or response.status_code == 408:
                if attempt < 2:
                    close_upstream(response, session)
                    delay = min(INITIAL_BACKOFF, 1.0)
                    log.warning("Upstream HTTP %s for '%s'; retrying in %.2fs.", response.status_code, current_model, delay)
                    await asyncio.sleep(delay)
                    continue
                return format_upstream_error_response(response, current_model)

            if response.status_code != 200:
                return format_upstream_error_response(response, current_model)
            record_active_tier(tier_name, "direct (home)" if tier_name == "direct" else ("warp" if tier_name in ("warp", "proxy_pool") else tier_name))
            metrics["successful_requests"] += 1
            prom_requests_success.labels(model=current_model).inc()
            prom_request_duration.labels(model=current_model, endpoint="chat_completions").observe(time.time() - start_time)

            if client_wants_stream:
                try:
                    if is_muse:
                        stream_gen = stream_responses_as_chat_completions(response, current_model, session=session)
                    else:
                        stream_gen = stream_response(response, current_model, session=session)
                    metrics["successful_requests"] += 1
                    prom_requests_success.labels(model=current_model).inc()
                    prom_request_duration.labels(model=current_model, endpoint="chat_completions").observe(time.time() - start_time)
                    track_token_usage(current_model, prompt_tokens=100, completion_tokens=150)
                    return StreamingResponse(
                        stream_gen,
                        media_type="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"}
                    )
                except EmptyStreamError:
                    log.warning("Empty stream for '%s'; retrying without egress rotation (%s/%s).", current_model, attempt, MAX_RETRIES_ON_429)
                    delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                    await asyncio.sleep(delay)
                    continue
            else:
                with FlowContext():
                    try:
                        stream_lines = list(response.iter_lines())
                        res_json = aggregate_stream_to_chat_completion(stream_lines, current_model)
                        usage = res_json.get("usage", {})
                        track_token_usage(
                            current_model,
                            prompt_tokens=usage.get("prompt_tokens", DEFAULT_PROMPT_TOKENS),
                            completion_tokens=usage.get("completion_tokens", DEFAULT_COMPLETION_TOKENS)
                        )
                        metrics["successful_requests"] += 1
                        prom_requests_success.labels(model=current_model).inc()
                        prom_request_duration.labels(model=current_model, endpoint="chat_completions").observe(time.time() - start_time)
                        return JSONResponse(content=res_json)
                    except Exception as agg_err:
                        log.error("Failed to aggregate stream for %s: %s", current_model, agg_err)
                        return JSONResponse(content={"id": f"chatcmpl-{int(time.time())}", "object": "chat.completion", "choices": [{"index": 0, "message": {"role": "assistant", "content": response.text}}]})

        except Exception as e:
            log.error(f"[Attempt {attempt}/{MAX_RETRIES_ON_429}] Connection error for model '{current_model}': {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES_ON_429:
                await asyncio.sleep(min(2 ** attempt, BACKOFF_CAP))
            continue

    log.error(f"All {MAX_RETRIES_ON_429} attempts exhausted for model '{current_model}'. Returning 503.")
    return JSONResponse(
        status_code=503,
        content={"error": {"message": f"Upstream unavailable after {MAX_RETRIES_ON_429} attempts. Please retry.", "type": "upstream_error", "code": 503}},
        headers={"Retry-After": "10"}
    )

# -----------------------------------------------------------------------------
# Anthropic API Compatibility Endpoint (/v1/messages)
# -----------------------------------------------------------------------------
@app.post("/v1/messages")
async def anthropic_messages(raw_request: Request):
    metrics["total_requests"] += 1
    prom_requests_total.labels(model="messages", endpoint="anthropic_messages").inc()
    await wait_for_rotation_drain()

    start_time = time.time()
    try:
        body = await raw_request.json()
    except Exception:
        body = {}

    model_name = body.get("model", "deepseek-v4-flash-free")
    is_stream = body.get("stream", False)
    log.info(f"Received Anthropic-format request for model '{model_name}' (Stream: {is_stream})")

    client_api_key = raw_request.headers.get("x-api-key") or ""
    if not client_api_key:
        auth = raw_request.headers.get("authorization", "")
        if auth.startswith("Bearer "):
            client_api_key = auth[7:]

    headers = get_realistic_headers(raw_request)
    headers["x-api-key"] = client_api_key or "public"

    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        try:
            await asyncio.sleep(random.uniform(0.1, 0.3))
            tier_name, proxies = get_tiered_proxy(attempt)
            session = create_fresh_session(is_stream) if is_stream else _get_session("anthropic")
            response = session.post(
                TARGET_ZEN_ANTHROPIC_URL,
                json=body,
                headers=headers,
                impersonate="chrome124",
                stream=is_stream,
                proxies=proxies,
                timeout=(6, STREAM_TIMEOUT if is_stream else 60),
            )
            log_upstream_response(response, model_name, "messages", attempt, proxies is not None)

            if response.status_code == 429:
                category, retry_seconds, err_payload = classify_upstream_429(response)
                metrics["rate_limited_requests"] += 1
                prom_requests_rate_limited.labels(model=model_name).inc()

                # Quota errors are account-level: rotation cannot help. Fail fast.
                if category == "quota":
                    if is_stream:
                        close_upstream(response, session)
                    return upstream_rate_limit_response(response, model_name)

                # Tier 1 (Direct Home IP): mark cooldown and immediately retry on Tier 2
                if tier_name == "direct":
                    if is_stream:
                        close_upstream(response, session)
                    mark_direct_rate_limited()
                    log.warning(f"Tier 1 (Direct Home IP) rate limited on '{model_name}'. Escalating to Tier 2 (WARP)...")
                    continue

                if tier_name in ("warp", "proxy_pool") and attempt < (MAX_RETRIES_ON_429 - 1):
                    log.warning("Upstream 429 rate limit hit for '%s' (Attempt %s/%s). Rotating WARP IP...", model_name, attempt, MAX_RETRIES_ON_429)
                    rotated, verified_ip = await asyncio.to_thread(rotate_egress, f"429 rate limit on {model_name}")
                    if rotated:
                        swap_warp_registration()
                        await asyncio.sleep(1)
                        if is_stream:
                            close_upstream(response, session)
                        continue
                # Tier 3 (Cloud Relay): fallback before giving up (streaming only;
                # non-streaming uses a pooled session that cannot adopt the relay stream).
                # On success, fall through to normal success handling below (same iteration).
                if not is_stream:
                    return upstream_rate_limit_response(response, model_name)
                relay_resp, relay_session = attempt_cloud_relay_fallback(headers, body, "/zen/v1/messages", model_name)
                close_upstream(response, session)
                if relay_resp is not None:
                    response = relay_resp
                    session = relay_session
                    tier_name = "render"
                else:
                    return upstream_rate_limit_response(response, model_name)
            if response.status_code >= 500 or response.status_code == 408:
                if attempt < 2:
                    if is_stream:
                        close_upstream(response, session)
                    delay = min(INITIAL_BACKOFF, 1.0)
                    log.warning("Upstream HTTP %s for '%s'; retrying in %.2fs.", response.status_code, model_name, delay)
                    await asyncio.sleep(delay)
                    continue
                return format_upstream_error_response(response, model_name)

            if response.status_code != 200:
                return format_upstream_error_response(response, model_name)
            record_active_tier(tier_name, "direct (home)" if tier_name == "direct" else ("warp" if tier_name in ("warp", "proxy_pool") else tier_name))
            metrics["successful_requests"] += 1
            prom_requests_success.labels(model=model_name).inc()
            prom_request_duration.labels(model=model_name, endpoint="anthropic_messages").observe(time.time() - start_time)

            if is_stream:
                return StreamingResponse(
                    stream_response(response, model_name, session=session),
                    media_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
                )
            else:
                with FlowContext():
                    try:
                        res_json = await asyncio.to_thread(response.json)
                        usage = res_json.get("usage", {})
                        track_token_usage(
                            model_name,
                            prompt_tokens=usage.get("input_tokens", DEFAULT_PROMPT_TOKENS),
                            completion_tokens=usage.get("output_tokens", DEFAULT_COMPLETION_TOKENS),
                        )
                        return JSONResponse(content=res_json)
                    except Exception:
                        track_token_usage(model_name, prompt_tokens=DEFAULT_PROMPT_TOKENS, completion_tokens=DEFAULT_COMPLETION_TOKENS)
                        return JSONResponse(content=response.text)

        except Exception as e:
            log.error(f"[Attempt {attempt}/{MAX_RETRIES_ON_429}] Anthropic endpoint error for model '{model_name}': {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES_ON_429:
                await asyncio.sleep(min(2 ** attempt, BACKOFF_CAP))
            continue

    log.error(f"All {MAX_RETRIES_ON_429} attempts exhausted for Anthropic model '{model_name}'. Returning 503.")
    return JSONResponse(
        status_code=503,
        content={"error": {"message": f"Upstream unavailable after {MAX_RETRIES_ON_429} attempts. Please retry.", "type": "upstream_error", "code": 503}},
        headers={"Retry-After": "10"}
    )

@app.post("/v1/responses")
@app.post("/responses")
@app.post("/v1/response")
@app.post("/response")
async def responses_endpoint(raw_request: Request):
    metrics["total_requests"] += 1
    prom_requests_total.labels(model="responses", endpoint="responses").inc()
    await wait_for_rotation_drain()

    start_time = time.time()
    try:
        body = await raw_request.json()
    except Exception:
        body = {}

    model_name = body.get("model", "muse-spark-1.3-contributor-free")
    client_wants_stream = body.get("stream", False)
    is_stream = True

    # Convert standard chat messages format to responses input format if needed
    if "messages" in body and "input" not in body:
        responses_input = []
        for m in body.get("messages", []):
            c = m.get("content", "")
            if isinstance(c, list):
                parts = []
                for p in c:
                    if isinstance(p, dict) and "text" in p:
                        parts.append(p["text"])
                    elif isinstance(p, str):
                        parts.append(p)
                c = "\n".join(parts)
            responses_input.append({
                "type": "message",
                "role": m.get("role", "user"),
                "content": c,
            })
        body["input"] = responses_input
        del body["messages"]

    # Flatten array content inside input items so OpenCode never returns 400
    if isinstance(body.get("input"), list):
        for item in body["input"]:
            if isinstance(item, dict) and isinstance(item.get("content"), list):
                parts = []
                for p in item["content"]:
                    if isinstance(p, dict) and "text" in p:
                        parts.append(p["text"])
                    elif isinstance(p, str):
                        parts.append(p)
                item["content"] = "\n".join(parts)

    ensure_opencode_fingerprint(body, is_responses=True)
    log.info(f"Received Responses API request for model '{model_name}' (Client Stream: {client_wants_stream})")

    headers = get_realistic_headers(raw_request)

    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        try:
            await asyncio.sleep(random.uniform(0.1, 0.3))
            tier_name, proxies = get_tiered_proxy(attempt)
            log.info(f"Dispatching Responses API '{model_name}' via [{tier_name.upper()}] (Attempt {attempt}/{MAX_RETRIES_ON_429})")
            session = create_fresh_session(True)
            response = session.post(
                TARGET_ZEN_RESPONSES_URL,
                json=body,
                headers=headers,
                impersonate="chrome124",
                stream=True,
                proxies=proxies,
                timeout=(6, STREAM_TIMEOUT),
            )
            log_upstream_response(response, model_name, "responses", attempt, proxies is not None)

            if response.status_code == 429:
                category, retry_seconds, err_payload = classify_upstream_429(response)
                metrics["rate_limited_requests"] += 1
                prom_requests_rate_limited.labels(model=model_name).inc()

                # Quota errors are account-level: rotation cannot help. Fail fast.
                if category == "quota":
                    close_upstream(response, session)
                    return upstream_rate_limit_response(response, model_name)

                # Tier 1 (Direct Home IP): mark cooldown and advance to Tier 2 (WARP)
                if tier_name == "direct":
                    close_upstream(response, session)
                    mark_direct_rate_limited()
                    log.warning(f"Tier 1 (Direct Home IP) rate limited on '{model_name}'. Advancing to Tier 2 (WARP)...")
                    continue

                # Tier 2 (WARP / Proxy Pool): rotate WARP IP
                if tier_name in ("warp", "proxy_pool") and attempt < (MAX_RETRIES_ON_429 - 1):
                    close_upstream(response, session)
                    log.warning(f"Tier 2 (WARP) rate limited on '{model_name}' (Attempt {attempt}/{MAX_RETRIES_ON_429}). Rotating WARP IP...")
                    rotated, verified_ip = await asyncio.to_thread(rotate_egress, f"Tier 2 WARP 429 on {model_name}")
                    if rotated:
                        swap_warp_registration()
                        await asyncio.sleep(1)
                        continue

                # Tier 3 (Cloud Relay): fallback before giving up.
                # On success, fall through to normal success handling below (same iteration).
                relay_resp, relay_session = attempt_cloud_relay_fallback(headers, body, "/zen/v1/responses", model_name)
                close_upstream(response, session)
                if relay_resp is not None:
                    response = relay_resp
                    session = relay_session
                    tier_name = "render"
                else:
                    return upstream_rate_limit_response(response, model_name)
            if response.status_code >= 500 or response.status_code == 408:
                if attempt < 2:
                    close_upstream(response, session)
                    delay = min(INITIAL_BACKOFF, 1.0)
                    log.warning("Upstream HTTP %s for '%s'; retrying in %.2fs.", response.status_code, model_name, delay)
                    await asyncio.sleep(delay)
                    continue
                return format_upstream_error_response(response, model_name)

            if response.status_code != 200:
                log.warning("Responses endpoint upstream error [%s]: %s", response.status_code, response.text)
                return format_upstream_error_response(response, model_name)
            record_active_tier(tier_name, "direct (home)" if tier_name == "direct" else ("warp" if tier_name in ("warp", "proxy_pool") else tier_name))
            metrics["successful_requests"] += 1
            prom_requests_success.labels(model=model_name).inc()
            prom_request_duration.labels(model=model_name, endpoint="responses").observe(time.time() - start_time)

            if client_wants_stream:
                return StreamingResponse(
                    stream_response(response, model_name, session=session),
                    media_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
                )
            else:
                with FlowContext():
                    try:
                        stream_lines = list(response.iter_lines())
                        res_json = aggregate_stream_to_response(stream_lines, model_name)
                        return JSONResponse(content=res_json)
                    except Exception:
                        return JSONResponse(content=response.text)
        except Exception as e:
            log.error(f"[Attempt {attempt}/{MAX_RETRIES_ON_429}] Responses endpoint error for model '{model_name}': {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES_ON_429:
                await asyncio.sleep(min(2 ** attempt, BACKOFF_CAP))
            continue

    log.error(f"All {MAX_RETRIES_ON_429} attempts exhausted for Responses model '{model_name}'. Returning 503.")
    return JSONResponse(
        status_code=503,
        content={"error": {"message": f"Upstream unavailable after {MAX_RETRIES_ON_429} attempts. Please retry.", "type": "upstream_error", "code": 503}},
        headers={"Retry-After": "10"}
    )

# -----------------------------------------------------------------------------
# Pi-Bansos Relay Endpoint
# -----------------------------------------------------------------------------
@app.api_route("/", methods=["GET", "POST", "OPTIONS"])
@app.api_route("/relay", methods=["GET", "POST", "OPTIONS"])
async def relay_handler(raw_request: Request):
    await wait_for_rotation_drain()
    target = raw_request.headers.get("x-relay-target")
    if not target:
        return JSONResponse({"status": "healthy", "service": "opencode-ip-rotator"})

    relay_path = raw_request.headers.get("x-relay-path") or "/"
    target_url = f"{target.rstrip('/')}{relay_path}"

    body_bytes = await raw_request.body()

    headers = get_realistic_headers(raw_request) if "opencode.ai" in target_url else {}
    for k, v in raw_request.headers.items():
        kl = k.lower()
        if kl in ("x-relay-target", "x-relay-path", "host", "content-length"):
            continue
        if kl == "user-agent" and not v.startswith("opencode/"):
            continue
        if kl in ("x-opencode-session", "x-session-affinity") and not v.startswith("ses_"):
            continue
        headers[kl] = v
    headers["host"] = target.replace("https://", "").replace("http://", "").split("/")[0]

    is_stream = "text/event-stream" in headers.get("accept", "") or b'"stream":true' in body_bytes or b'"stream": true' in body_bytes
    model_name = "relay"
    try:
        body_json = json.loads(body_bytes.decode())
        model_name = body_json.get("model", "relay")
        if "opencode.ai" in target_url:
            ensure_opencode_fingerprint(body_json, is_responses=("/responses" in target_url))
            body_bytes = json.dumps(body_json).encode()
            headers["content-length"] = str(len(body_bytes))
    except Exception:
        pass
    metrics["total_requests"] += 1
    log.info(f"Relaying {raw_request.method} to {target_url} (model={model_name}, stream={is_stream})")

    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        try:
            tier_name, proxies = get_tiered_proxy(attempt)
            session = create_fresh_session(is_stream) if is_stream else _get_session("relay")
            response = session.request(
                method=raw_request.method,
                url=target_url,
                data=body_bytes if body_bytes else None,
                headers=headers,
                proxies=proxies,
                impersonate="chrome124",
                stream=is_stream,
                timeout=(6, STREAM_TIMEOUT if is_stream else 60),
            )
            log_upstream_response(response, model_name, "relay", attempt, proxies is not None)

            if response.status_code == 429:
                category, retry_seconds, err_payload = classify_upstream_429(response)
                metrics["rate_limited_requests"] += 1
                prom_requests_rate_limited.labels(model=model_name).inc()
                # Quota errors are account-level: rotation cannot help. Fail fast.
                if category == "quota":
                    if is_stream:
                        close_upstream(response, session)
                    return upstream_rate_limit_response(response, model_name)
                if tier_name == "direct":
                    if is_stream:
                        close_upstream(response, session)
                    mark_direct_rate_limited()
                    log.warning("Relay Tier 1 (Direct) rate limited for '%s'. Escalating to Tier 2 (WARP)...", model_name)
                    continue
                log.warning("Relay 429 rate limit hit for '%s'. Rotating WARP IP...", model_name)
                rotated, verified_ip = await asyncio.to_thread(rotate_egress, f"Relay 429 rate limit on {model_name}")
                if rotated:
                    swap_warp_registration()
                    await asyncio.sleep(1)
                    if is_stream:
                        close_upstream(response, session)
                    continue
                return upstream_rate_limit_response(response, model_name)

            if response.status_code >= 500 or response.status_code == 408:
                if attempt < 2:
                    if is_stream:
                        close_upstream(response, session)
                    delay = min(INITIAL_BACKOFF, 1.0)
                    log.warning("Relay upstream HTTP %s; retrying in %.2fs.", response.status_code, delay)
                    await asyncio.sleep(delay)
                    continue
                return format_upstream_error_response(response, model_name)

            if response.status_code != 200:
                return format_upstream_error_response(response, model_name)

            metrics["successful_requests"] += 1
            if is_stream:
                return StreamingResponse(
                    stream_response(response, model_name, session=session),
                    media_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
                )
            else:
                ct = response.headers.get("content-type", "application/json")
                return Response(content=response.content, status_code=response.status_code, media_type=ct)

        except Exception as e:
            log.error(f"[Relay Attempt {attempt}/{MAX_RETRIES_ON_429}] Error: {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES_ON_429:
                await asyncio.sleep(min(2 ** attempt, BACKOFF_CAP))
            continue

    return JSONResponse(status_code=503, content={"error": {"message": "Relay upstream unavailable", "code": 503}})
# Global exception handler for standard error format
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    log.error(f"Unhandled error on {request.method} {request.url.path}: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": {"message": "Internal server error", "type": "internal_error", "code": 500}},
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"message": exc.detail, "type": "http_error", "code": exc.status_code}},
    )

if __name__ == "__main__":
    load_proxy_list()
    log.info(f"Starting OpenCode IP Proxy Server on {HOST}:{PORT}...")
    uvicorn.run(app, host=HOST, port=PORT)
