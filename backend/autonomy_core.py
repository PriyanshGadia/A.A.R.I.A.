# autonomy_core.py - Production A.A.R.I.A Autonomous Action Engine (Async-Compatible)
from __future__ import annotations

import time
import logging
import heapq
import hashlib
import json
import base64
import traceback
import asyncio
from typing import List, Dict, Any, Optional, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
from collections import deque
from contextlib import asynccontextmanager
from functools import wraps

# optional crypto and prometheus
try:
    from Crypto.Cipher import AES
    from Crypto.Random import get_random_bytes
    CRYPTO_AVAILABLE = True
except Exception:
    CRYPTO_AVAILABLE = False

try:
    from prometheus_client import Counter as PromCounter, Histogram, Gauge
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False

# hologram_state is optional; guard uses below
try:
    import hologram_state
except Exception:
    hologram_state = None

# local imports (may raise if module missing)
try:
    from persona_core import PersonaCore
except Exception:
    PersonaCore = None

try:
    from assistant_core import AssistantCore
except Exception:
    AssistantCore = None

logger = logging.getLogger("AARIA.Autonomy")

# Prometheus metrics (optional)
if PROMETHEUS_AVAILABLE:
    autonomy_metrics = {
        'actions_executed': PromCounter('autonomy_actions_total', 'Total actions executed', ['type', 'status']),
        'action_duration': Histogram('autonomy_action_duration_seconds', 'Action execution time', ['type']),
        'queue_size': Gauge('autonomy_queue_size', 'Current action queue size'),
        'concurrent_actions': Gauge('autonomy_concurrent_actions', 'Currently executing actions'),
        'circuit_breaker_state': Gauge('autonomy_circuit_breaker_state', 'Circuit breaker state', ['service']),
        'validation_failures': PromCounter('autonomy_validation_failures_total', 'Action validation failures', ['reason']),
        'rate_limit_hits': PromCounter('autonomy_rate_limit_hits_total', 'Rate limit hits', ['type'])
    }
else:
    autonomy_metrics = {}

# Async retry decorator
def async_retry_operation(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0,
                         exceptions: Tuple = (Exception,)):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries, current_delay = 0, delay
            while True:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error("Operation %s failed after %s retries: %s", func.__name__, max_retries, e)
                        raise
                    logger.warning("Operation %s failed (attempt %s/%s): %s", func.__name__, retries, max_retries, e)
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator

# Enums
class ActionPriority(Enum):
    CRITICAL = 5
    HIGH = 4
    NORMAL = 3
    LOW = 2
    BACKGROUND = 1

class ActionCategory(Enum):
    NOTIFICATION = "notification"
    CALENDAR = "calendar"
    CONTACT = "contact"
    SYSTEM = "system"
    LLM = "llm"

# Circuit breaker
class AsyncCircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60, max_recovery_time: int = 3600):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.base_recovery_timeout = recovery_timeout
        self.max_recovery_time = max_recovery_time
        self.last_failure_time: Optional[float] = None
        self.state = "CLOSED"
        self._lock = asyncio.Lock()
        self._last_state_check = 0.0

    async def can_execute(self) -> bool:
        async with self._lock:
            self._last_state_check = time.time()
            if self.state == "OPEN":
                current_timeout = min(
                    self.base_recovery_timeout * (2 ** max(0, (self.failure_count - self.failure_threshold))),
                    self.max_recovery_time
                )
                if self.last_failure_time is None:
                    return False
                if time.time() - self.last_failure_time > current_timeout:
                    self.state = "HALF_OPEN"
                    return True
                return False
            return True

    async def get_state_info(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "state": self.state,
                "failure_count": self.failure_count,
                "last_failure_time": self.last_failure_time,
                "last_state_check": self._last_state_check
            }

    async def record_success(self):
        async with self._lock:
            self.failure_count = 0
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"

# AES-GCM encryptor with fallback
class AESGCMEncryptor:
    def __init__(self, key: Optional[bytes] = None):
        self.key = key or self._generate_key()
        self._key_warning_logged = False

    def _generate_key(self) -> bytes:
        if CRYPTO_AVAILABLE:
            return get_random_bytes(32)
        else:
            if not self._key_warning_logged:
                logger.warning("PyCryptodome missing — using deterministic fallback key (not secure for prod).")
                self._key_warning_logged = True
            return hashlib.sha256(b"autonomy_core_fallback_key").digest()

    def encrypt(self, data: str) -> Dict[str, Any]:
        try:
            if not CRYPTO_AVAILABLE:
                encoded = base64.b64encode(data.encode("utf-8")).decode("utf-8")
                return {"encryption": "base64_fallback", "data": encoded}
            nonce = get_random_bytes(12)
            cipher = AES.new(self.key, AES.MODE_GCM, nonce=nonce)
            ciphertext, tag = cipher.encrypt_and_digest(data.encode("utf-8"))
            return {
                "encryption": "aes-gcm",
                "nonce": base64.b64encode(nonce).decode("utf-8"),
                "ciphertext": base64.b64encode(ciphertext).decode("utf-8"),
                "tag": base64.b64encode(tag).decode("utf-8")
            }
        except Exception as e:
            logger.error("Encryption failed: %s", e)
            return {"encryption": "plaintext_fallback", "data": data}

    def decrypt(self, encrypted_data: Dict[str, Any]) -> str:
        try:
            et = encrypted_data.get("encryption", "unknown")
            if et == "plaintext_fallback":
                return encrypted_data["data"]
            if et == "base64_fallback":
                return base64.b64decode(encrypted_data["data"]).decode("utf-8")
            if et == "aes-gcm" and CRYPTO_AVAILABLE:
                nonce = base64.b64decode(encrypted_data["nonce"])
                ciphertext = base64.b64decode(encrypted_data["ciphertext"])
                tag = base64.b64decode(encrypted_data["tag"])
                cipher = AES.new(self.key, AES.MODE_GCM, nonce=nonce)
                return cipher.decrypt_and_verify(ciphertext, tag).decode("utf-8")
            raise ValueError(f"Unsupported encryption type: {et}")
        except Exception as e:
            logger.error("Decryption failed: %s", e)
            raise

# Async multi-level priority heap-backed queue
class AsyncMultiLevelQueue:
    def __init__(self):
        # priority -> list used as heap
        self.queues: Dict[int, List] = {p: [] for p in range(1, 6)}
        self._lock = asyncio.Lock()

    async def push(self, action: "Action"):
        async with self._lock:
            heapq.heappush(self.queues[action.priority], action)

    async def pop(self) -> Optional["Action"]:
        async with self._lock:
            # highest numeric priority wins (5->1)
            for priority in range(5, 0, -1):
                q = self.queues[priority]
                if q:
                    return heapq.heappop(q)
            return None

    async def size(self) -> int:
        async with self._lock:
            return sum(len(q) for q in self.queues.values())

    async def clear_priority(self, priority: int) -> int:
        async with self._lock:
            count = len(self.queues.get(priority, []))
            self.queues[priority] = []
            return count

    async def get_summary(self) -> List[Dict[str, Any]]:
        async with self._lock:
            out = []
            for priority in range(5, 0, -1):
                out.extend([a.to_dict() for a in list(self.queues[priority])])
            return out

# Action dataclass
@dataclass
class Action:
    action_type: str
    details: Dict[str, Any]
    priority: int = ActionPriority.NORMAL.value
    source: str = "autonomy"
    timestamp: Optional[float] = None
    executed: bool = False
    result: Optional[Dict[str, Any]] = None
    id: Optional[str] = None
    category: str = field(default_factory=lambda: ActionCategory.SYSTEM.value)
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()
        self.priority = max(1, min(int(self.priority), 5))
        if self.id is None:
            key = f"{self.action_type}:{json.dumps(self.details, sort_keys=True)}:{self.timestamp}"
            self.id = f"act_{int(self.timestamp)}_{hashlib.md5(key.encode()).hexdigest()[:8]}"
        # auto-detect category
        if self.category == ActionCategory.SYSTEM.value:
            if self.action_type.startswith("notify"):
                self.category = ActionCategory.NOTIFICATION.value
            elif self.action_type.startswith("calendar"):
                self.category = ActionCategory.CALENDAR.value
            elif self.action_type.startswith("contact"):
                self.category = ActionCategory.CONTACT.value
            elif "llm" in self.action_type or self.source == "llm_autonomy":
                self.category = ActionCategory.LLM.value

    def __lt__(self, other: "Action"):
        # heapq uses __lt__ for ordering
        if self.priority == other.priority:
            return self.timestamp < other.timestamp
        # Higher priority value should come first in our pop order; but heapq pops smallest,
        # so we invert: store actions with negative priority? Simpler: compare priority descending.
        return self.priority > other.priority

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "action_type": self.action_type,
            "details": self.details,
            "priority": self.priority,
            "source": self.source,
            "timestamp": self.timestamp,
            "executed": self.executed,
            "result": self.result,
            "category": self.category,
            "tags": self.tags,
            "metadata": self.metadata
        }

# Autonomy core
class AutonomyCore:
    def __init__(self, persona: Any, core: Any, max_concurrent: int = 3):
        self.persona = persona
        self.core = core
        self.max_concurrent = max_concurrent

        self._queue_lock = asyncio.Lock()
        self._exec_lock = asyncio.Lock()
        self._metrics_lock = asyncio.Lock()
        self._config_lock = asyncio.Lock()

        self.action_queue = AsyncMultiLevelQueue()
        self.executed_actions: deque = deque(maxlen=2000)
        self.failed_actions: deque = deque(maxlen=1000)
        self.rate_limits: Dict[str, List[float]] = {}
        self.autonomy_enabled: bool = True
        self.currently_executing: int = 0

        self.circuit_breakers: Dict[str, AsyncCircuitBreaker] = {
            "calendar": AsyncCircuitBreaker(failure_threshold=3, recovery_timeout=60, max_recovery_time=600),
            "notifications": AsyncCircuitBreaker(failure_threshold=5, recovery_timeout=30, max_recovery_time=300),
            "contacts": AsyncCircuitBreaker(failure_threshold=3, recovery_timeout=45, max_recovery_time=450)
        }

        self.encryptor = AESGCMEncryptor()
        self._config_handles: List[Any] = []
        self._initialized = False

        # config & requirements
        self.action_requirements: Dict[str, Dict[str, Any]] = {}
        self.rate_limits_config: Dict[str, Dict[str, int]] = {}
        self.llm_config: Dict[str, Any] = {}
        self._recent_actions_cache: List[Action] = []
        self._cache_valid = False
        self._last_health_check = 0.0

        # background task handle
        self._autonomy_task: Optional[asyncio.Task] = None

    async def initialize(self):
        # load defaults
        await self._load_dynamic_config()
        self._setup_config_watcher()
        self._initialized = True

        # set action requirements & rate limits
        self.action_requirements = {
            "notify": {"required": ["channel", "message"], "optional": ["urgency"], "category": ActionCategory.NOTIFICATION.value},
            "calendar.add": {"required": ["title", "datetime"], "optional": ["priority", "location", "description"], "category": ActionCategory.CALENDAR.value},
            "calendar.list": {"required": [], "optional": ["days"], "category": ActionCategory.CALENDAR.value},
            "contact.get": {"required": ["id"], "optional": [], "category": ActionCategory.CONTACT.value},
            "contact.search": {"required": ["q"], "optional": ["limit"], "category": ActionCategory.CONTACT.value},
        }
        self.rate_limits_config = {
            "notify": {"window": 60, "max_actions": 5},
            "calendar.add": {"window": 300, "max_actions": 2},
            "contact.get": {"window": 30, "max_actions": 10},
            "contact.search": {"window": 30, "max_actions": 10},
            "default": {"window": 60, "max_actions": 3}
        }
        self.llm_config = {
            "summarization_model": "claude-3-haiku",
            "autonomy_model": "claude-3-haiku",
            "fallback_models": ["gpt-3.5-turbo", "local-llm"],
            "max_retries": 2
        }

        if PROMETHEUS_AVAILABLE:
            autonomy_metrics['queue_size'].set(0)
            autonomy_metrics['concurrent_actions'].set(0)

        logger.info("Async AutonomyCore initialized with multi-level queue and enhanced async support")

    async def _load_dynamic_config(self):
        try:
            if hasattr(self.core, "config_manager"):
                config = self.core.config_manager.get("autonomy", {}) or {}
                async with self._config_lock:
                    self.max_concurrent = config.get("max_concurrent", self.max_concurrent)
                    llm_cfg = config.get("llm", {})
                    if llm_cfg:
                        self.llm_config.update(llm_cfg)
                    rate_limits = config.get("rate_limits", {})
                    if rate_limits:
                        self.rate_limits_config.update(rate_limits)
                logger.debug("Loaded dynamic config for AutonomyCore")
        except Exception as e:
            logger.warning("Failed to load dynamic config: %s", e)

    def _setup_config_watcher(self):
        try:
            if hasattr(self.core, "config_manager") and hasattr(self.core.config_manager, "watch"):
                async def _handler(new_config):
                    logger.info("Autonomy config change detected -> reloading")
                    await self.reload_config()
                handle = self.core.config_manager.watch("autonomy", _handler)
                self._config_handles.append(handle)
                logger.debug("Config watcher established for AutonomyCore")
        except Exception:
            logger.debug("Config watching not available for AutonomyCore (non-fatal)")

    # Queue management
    async def enqueue_action(self, action: Action) -> bool:
        ok, reason = await self._validate_action(action)
        if not ok:
            logger.debug("Enqueue validation failed: %s", reason)
            if PROMETHEUS_AVAILABLE:
                autonomy_metrics['validation_failures'].labels(reason=reason.split(':')[0] if ':' in reason else 'unknown').inc()
            return False
        await self.action_queue.push(action)
        if PROMETHEUS_AVAILABLE:
            autonomy_metrics['queue_size'].inc()
        logger.debug("Enqueued action %s (id=%s)", action.action_type, action.id)
        return True

    async def dequeue_action(self) -> Optional[Action]:
        act = await self.action_queue.pop()
        if act and PROMETHEUS_AVAILABLE:
            autonomy_metrics['queue_size'].dec()
        return act

    async def pending_actions(self) -> int:
        return await self.action_queue.size()

    async def get_queue_summary(self) -> List[Dict[str, Any]]:
        return await self.action_queue.get_summary()

    async def clear_queue(self, priority_filter: Optional[int] = None) -> int:
        if priority_filter is None:
            count = await self.pending_actions()
            for p in range(1, 6):
                await self.action_queue.clear_priority(p)
            if PROMETHEUS_AVAILABLE:
                autonomy_metrics['queue_size'].set(0)
            return count
        return await self.action_queue.clear_priority(priority_filter)

    # Validation
    async def _validate_action(self, action: Action) -> Tuple[bool, str]:
        if action.action_type not in self.action_requirements:
            return False, f"unknown_action_type:{action.action_type}"
        req = self.action_requirements[action.action_type]
        for f in req.get("required", []):
            if f not in action.details:
                return False, f"missing_field:{f}"
        if action.action_type == "notify":
            valid_channels = ["desktop", "sms", "call"]
            if action.details.get("channel") not in valid_channels:
                return False, f"invalid_channel:{action.details.get('channel')}"
        if action.action_type == "calendar.add":
            if not await self._validate_datetime(action.details.get("datetime", "")):
                return False, "invalid_datetime_format"
        return True, "valid"

    async def _validate_datetime(self, dt_str: str) -> bool:
        try:
            if not dt_str:
                return False
            # Accept common ISO forms; fromisoformat supports offsets
            if 'Z' in dt_str:
                dt_str = dt_str.replace('Z', '+00:00')
            datetime.fromisoformat(dt_str)
            return True
        except Exception:
            return False

    def _get_service_from_action(self, action_type: str) -> str:
        if action_type.startswith("calendar"):
            return "calendar"
        if action_type.startswith("notify"):
            return "notifications"
        if action_type.startswith("contact"):
            return "contacts"
        return "default"

    async def _can_execute(self, action: Action) -> Tuple[bool, str]:
        # user consent
        try:
            if hasattr(self.persona, "core") and hasattr(self.persona.core, "load_user_profile"):
                profile = await self.persona.core.load_user_profile() or {}
            else:
                profile = {}
            if not profile.get("allow_autonomy", True):
                return False, "user_consent_denied"
        except Exception as e:
            logger.debug("Consent check failed: %s", e)
            return False, "consent_check_failed"

        service = self._get_service_from_action(action.action_type)
        cb = self.circuit_breakers.get(service)
        if cb and not await cb.can_execute():
            return False, f"circuit_breaker_open:{service}"

        limit = self.rate_limits_config.get(action.action_type, self.rate_limits_config["default"])
        window_start = time.time() - limit["window"]

        # update cache if stale
        if not self._cache_valid or time.time() - self._last_health_check > 30:
            await self._update_recent_actions_cache()

        recent = [a for a in self._recent_actions_cache if a.action_type == action.action_type and a.timestamp > window_start]
        if len(recent) >= limit["max_actions"]:
            if PROMETHEUS_AVAILABLE:
                autonomy_metrics['rate_limit_hits'].labels(type=action.action_type).inc()
            oldest = min(a.timestamp for a in recent) if recent else window_start
            wait = limit["window"] - (time.time() - oldest)
            return False, f"rate_limit_exceeded:{wait:.1f}s"
        return True, "allowed"

    async def _update_recent_actions_cache(self):
        async with self._exec_lock:
            combined = list(self.executed_actions) + list(self.failed_actions)
            # keep only last 1000 for cache
            self._recent_actions_cache = combined[-1000:]
            self._cache_valid = True
            self._last_health_check = time.time()

    async def _update_rate_limits(self, action: Action):
        now = time.time()
        limit = self.rate_limits_config.get(action.action_type, self.rate_limits_config["default"])
        async with self._exec_lock:
            self.rate_limits.setdefault(action.action_type, []).append(now)
            cutoff = now - limit["window"]
            self.rate_limits[action.action_type] = [t for t in self.rate_limits[action.action_type] if t > cutoff]
            self._cache_valid = False

    # Execution context
    @asynccontextmanager
    async def _execution_context(self, action: Action):
        start = time.monotonic()
        async with self._metrics_lock:
            self.currently_executing += 1
            if PROMETHEUS_AVAILABLE:
                autonomy_metrics['concurrent_actions'].inc()
        try:
            yield
        finally:
            duration = time.monotonic() - start
            async with self._metrics_lock:
                self.currently_executing = max(0, self.currently_executing - 1)
                if PROMETHEUS_AVAILABLE:
                    try:
                        autonomy_metrics['concurrent_actions'].dec()
                        autonomy_metrics['action_duration'].labels(type=action.action_type).observe(duration)
                    except Exception:
                        pass

    # Execution entry
    async def execute_action(self, action: Action) -> Tuple[bool, Any]:
        can_exec, reason = await self._can_execute(action)
        if not can_exec:
            logger.debug("Action %s blocked: %s", action.id, reason)
            if PROMETHEUS_AVAILABLE and reason.startswith("rate_limit"):
                autonomy_metrics['actions_executed'].labels(type=action.action_type, status='rate_limited').inc()
            return False, reason

        service = self._get_service_from_action(action.action_type)
        node_id = f"auto_task_{action.id}"
        link_id = f"link_{node_id}"

        async with self._execution_context(action):
            start = time.monotonic()
            try:
                # hologram spawn (best-effort)
                if hologram_state is not None:
                    try:
                        maybe = getattr(hologram_state, "spawn_and_link", None)
                        if callable(maybe):
                            r = maybe(node_id=node_id, node_type="task", label=action.action_type, size=6, source_id="AutonomyCore", link_id=link_id)
                            if asyncio.iscoroutine(r):
                                await r
                        s = getattr(hologram_state, "set_node_active", None)
                        if callable(s):
                            r2 = s("AutonomyCore")
                            if asyncio.iscoroutine(r2):
                                await r2
                    except Exception:
                        logger.debug("Hologram spawn failed (non-fatal)")

                # actual execution
                result = await self._execute_action_internal(action)
                exec_time = time.monotonic() - start
                action.result = await self._process_action_result(action, result, exec_time, True)
                await self._mark_executed(action)
                if service in self.circuit_breakers:
                    await self.circuit_breakers[service].record_success()
                if PROMETHEUS_AVAILABLE:
                    autonomy_metrics['actions_executed'].labels(type=action.action_type, status='success').inc()
                logger.debug("Action %s executed in %.3fs", action.id, exec_time)
                return True, result
            except Exception as e:
                exec_time = time.monotonic() - start
                err = {"message": str(e), "trace": traceback.format_exc()}
                action.result = await self._process_action_result(action, err, exec_time, False)
                await self._mark_failed(action)
                if service in self.circuit_breakers:
                    await self.circuit_breakers[service].record_failure()
                if PROMETHEUS_AVAILABLE:
                    autonomy_metrics['actions_executed'].labels(type=action.action_type, status='failure').inc()
                logger.error("Action %s failed: %s", action.id, e)
                # hologram mark error (best-effort)
                if hologram_state is not None and hasattr(hologram_state, "set_node_error"):
                    try:
                        r = hologram_state.set_node_error(node_id)
                        if asyncio.iscoroutine(r):
                            await r
                    except Exception:
                        pass
                return False, err
            finally:
                # hologram cleanup (best-effort)
                if hologram_state is not None:
                    try:
                        s = getattr(hologram_state, "set_node_idle", None)
                        if callable(s):
                            r = s("AutonomyCore")
                            if asyncio.iscoroutine(r):
                                await r
                        d = getattr(hologram_state, "despawn_and_unlink", None)
                        if callable(d):
                            r2 = d(node_id, link_id)
                            if asyncio.iscoroutine(r2):
                                await r2
                    except Exception:
                        logger.debug("Hologram cleanup failed (non-fatal)")

    @async_retry_operation(max_retries=2, delay=0.5, exceptions=(Exception,))
    async def _execute_action_internal(self, action: Action) -> Any:
        if action.action_type == "notify":
            return await self._execute_notify(action.details)
        if action.action_type == "calendar.add":
            return await self._execute_calendar_add(action.details)
        if action.action_type == "calendar.list":
            return await self._execute_calendar_list(action.details)
        if action.action_type == "contact.get":
            return await self._execute_contact_get(action.details)
        if action.action_type == "contact.search":
            return await self._execute_contact_search(action.details)
        raise ValueError(f"Unsupported action type: {action.action_type}")

    async def _process_action_result(self, action: Action, result: Any, exec_time: float, success: bool) -> Dict[str, Any]:
        return {
            "success": bool(success),
            "execution_time": exec_time,
            "timestamp": time.time(),
            "action_type": action.action_type,
            "action_id": action.id,
            "category": action.category,
            "details": result if success else {"error": result},
            "priority": action.priority
        }

    async def _mark_executed(self, action: Action):
        async with self._exec_lock:
            action.executed = True
            self.executed_actions.append(action)
            await self._update_rate_limits(action)
            self._cache_valid = False

        # store memory about action execution (await properly)
        try:
            memory_summary_user = f"Autonomy executed action {action.action_type}"
            memory_summary_assistant = json.dumps({"execution": action.result}, default=str)
            # persona.store_memory expects (user_input, assistant_response, subject_identity_id=..., importance=..., ...)
            if hasattr(self.persona, "store_memory"):
                maybe = self.persona.store_memory(
                    user_input=memory_summary_user,
                    assistant_response=memory_summary_assistant,
                    subject_identity_id="owner_primary",
                    importance=4 if not action.result.get("success") else 3,
                    metadata={"action_id": action.id, "category": action.category}
                )
                if asyncio.iscoroutine(maybe):
                    await maybe
        except Exception:
            logger.debug("Failed to persist action execution to persona memory (non-fatal)")

        await self._audit_action(action, action.result)

    async def _mark_failed(self, action: Action):
        async with self._exec_lock:
            self.failed_actions.append(action)
            await self._update_rate_limits(action)
            self._cache_valid = False

    async def _audit_action(self, action: Action, result: Dict[str, Any]):
        try:
            audit_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action_id": action.id,
                "user_context": await self._get_user_context(),
                "action_details": action.to_dict(),
                "result": result,
                "compliance_tags": await self._generate_compliance_tags(action)
            }
            encrypted = self.encryptor.encrypt(json.dumps(audit_entry, default=str))
            if hasattr(self.core, "audit_logger"):
                try:
                    # prefer async/info if available
                    if hasattr(self.core.audit_logger, "ainfo"):
                        maybe = self.core.audit_logger.ainfo("autonomy_action", extra={"audit_data": encrypted})
                        if asyncio.iscoroutine(maybe):
                            await maybe
                    else:
                        self.core.audit_logger.info("autonomy_action", extra={"audit_data": encrypted})
                except Exception:
                    logger.debug("core.audit_logger failed, falling back to store")
            elif hasattr(self.core, "store") and hasattr(self.core.store, "put"):
                key = f"audit_action_{action.id}_{int(time.time())}"
                maybe = self.core.store.put(key, "autonomy_audit", encrypted)
                if asyncio.iscoroutine(maybe):
                    await maybe
            else:
                logger.info("AUTONOMY_AUDIT: %s", json.dumps(encrypted))
        except Exception:
            logger.debug("Failed to create audit entry", exc_info=True)

    async def _get_user_context(self) -> Dict[str, Any]:
        try:
            if hasattr(self.persona, "get_user_context"):
                ctx = self.persona.get_user_context()
                if asyncio.iscoroutine(ctx):
                    return await ctx
                return ctx
        except Exception:
            logger.debug("Failed to get user context from persona")
        return {"user_available": False}

    async def _generate_compliance_tags(self, action: Action) -> Dict[str, str]:
        return {
            "data_category": "operational" if action.category == ActionCategory.CALENDAR.value else "communication",
            "retention_period": "7y",
            "encryption_required": "true",
            "pii_handling": "minimal" if action.category == ActionCategory.CONTACT.value else "none",
            "audit_required": "true"
        }

    # Implementations (simple & extensible)
    async def _execute_notify(self, details: Dict[str, Any]) -> Dict[str, Any]:
        message = details.get("message", "")
        # Summarize long messages via LLM if available
        if len(message) > 200 and hasattr(self.core, "llm_orchestrator"):
            summary = await self._summarize_message_with_fallback(message)
            if summary:
                message = summary
        return {
            "status": "sent",
            "channel": details.get("channel"),
            "message_preview": message[:200],
            "original_length": len(details.get("message", "")),
            "final_length": len(message)
        }

    async def _summarize_message_with_fallback(self, message: str) -> Optional[str]:
        # Attempt to use LLM adapters; be defensive if adapters are missing
        try:
            from llm_adapter import LLMAdapterFactory, LLMRequest  # type: ignore
        except Exception:
            return None

        models_to_try = [self.llm_config.get("summarization_model")] + list(self.llm_config.get("fallback_models", []))
        for model in models_to_try:
            if not model:
                continue
            try:
                adapter_ctx = LLMAdapterFactory.get_adapter(model)
                async with adapter_ctx as adapter:
                    req = LLMRequest(messages=[{"role": "user", "content": f"Summarize in <=100 chars: {message}"}],
                                     max_tokens=60, temperature=0.1)
                    resp = await adapter.chat(req)
                    if resp and getattr(resp, "content", None):
                        return resp.content.strip()
            except Exception:
                logger.debug("Summarization attempt failed for model %s", model, exc_info=True)
                continue
        return None

    async def _execute_calendar_add(self, details: Dict[str, Any]) -> Dict[str, Any]:
        try:
            event_id = f"event_{int(time.time())}"
            if hasattr(self.core, "add_event"):
                res = self.core.add_event(details)
                if asyncio.iscoroutine(res):
                    res = await res
                return {"status": "added", "event_id": event_id, "core_result": res}
            return {"status": "simulated", "event_id": event_id, "note": "Calendar integration not available"}
        except Exception as e:
            return {"status": "failed", "error": str(e), "trace": traceback.format_exc()}

    async def _execute_calendar_list(self, details: Dict[str, Any]) -> Dict[str, Any]:
        days = details.get("days", 7)
        try:
            if hasattr(self.core, "list_upcoming_events"):
                res = self.core.list_upcoming_events(days)
                if asyncio.iscoroutine(res):
                    res = await res
                return {"status": "listed", "count": len(res) if isinstance(res, list) else 0, "days": days}
            return {"status": "simulated", "count": 0, "days": days}
        except Exception as e:
            return {"status": "failed", "error": str(e)}

    async def _execute_contact_get(self, details: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if hasattr(self.core, "list_contacts"):
                res = self.core.list_contacts()
                if asyncio.iscoroutine(res):
                    res = await res
                contact = next((c for c in res if c.get("id") == details.get("id")), None)
                return {"status": "retrieved" if contact else "not_found", "contact": contact}
            return {"status": "simulated", "contact": None}
        except Exception as e:
            return {"status": "failed", "error": str(e)}

    async def _execute_contact_search(self, details: Dict[str, Any]) -> Dict[str, Any]:
        try:
            limit = details.get("limit", 10)
            query = details.get("q", "").lower()
            if hasattr(self.core, "list_contacts"):
                res = self.core.list_contacts()
                if asyncio.iscoroutine(res):
                    res = await res
                filtered = [c for c in res if query in c.get("name", "").lower() or query in c.get("email", "").lower()]
                return {"status": "searched", "count": len(filtered[:limit]), "results": filtered[:limit]}
            return {"status": "simulated", "count": 0, "results": []}
        except Exception as e:
            return {"status": "failed", "error": str(e)}

    # Autonomy cycle & continuous loop
    async def run_autonomy_cycle(self) -> Dict[str, Any]:
        if not self.autonomy_enabled:
            return {"status": "disabled", "executed": 0}
        executed = 0
        successful = 0
        start = time.monotonic()
        while await self.pending_actions() > 0 and executed < self.max_concurrent:
            action = await self.dequeue_action()
            if not action:
                break
            ok, _ = await self.execute_action(action)
            executed += 1
            if ok:
                successful += 1
        cycle_time = time.monotonic() - start
        summary = {
            "status": "completed",
            "executed": executed,
            "successful": successful,
            "cycle_time": cycle_time,
            "queue_remaining": await self.pending_actions(),
            "currently_executing": self.currently_executing,
            "timestamp": time.time()
        }
        logger.debug("Autonomy cycle summary: %s", summary)
        return summary

    async def start_continuous_autonomy(self, interval: float = 5.0):
        if self._autonomy_task and not self._autonomy_task.done():
            logger.debug("Continuous autonomy already running")
            return
        async def _loop():
            while self.autonomy_enabled:
                try:
                    await self.run_autonomy_cycle()
                except Exception:
                    logger.exception("Autonomy continuous loop error")
                await asyncio.sleep(interval)
        self._autonomy_task = asyncio.create_task(_loop())
        logger.info("Started continuous autonomy (interval=%.1fs)", interval)

    async def stop_continuous_autonomy(self):
        self.autonomy_enabled = False
        if self._autonomy_task:
            self._autonomy_task.cancel()
            try:
                await self._autonomy_task
            except asyncio.CancelledError:
                pass
            self._autonomy_task = None
        logger.info("Stopped continuous autonomy")

    # LLM-driven autonomy decision
    async def evaluate_autonomous_decision(self, context: Dict[str, Any]) -> Optional[Action]:
        try:
            from llm_adapter import LLMAdapterFactory, LLMRequest  # type: ignore
        except Exception:
            return None
        prompt = await self._build_autonomy_prompt(context)
        models = [self.llm_config.get("autonomy_model")] + list(self.llm_config.get("fallback_models", []))
        for model in models:
            if not model:
                continue
            try:
                async with LLMAdapterFactory.get_adapter(model) as adapter:
                    req = LLMRequest(messages=[{"role": "system", "content": prompt}], temperature=0.1, max_tokens=500)
                    resp = await adapter.chat(req)
                    content = getattr(resp, "content", "") if resp is not None else ""
                    action = await self._parse_llm_action_robust(content, context)
                    if action:
                        return action
            except Exception:
                logger.debug("LLM autonomy model %s failed (trying fallback)", model, exc_info=True)
                continue
        return None

    async def _build_autonomy_prompt(self, context: Dict[str, Any]) -> str:
        return (
            "Analyze the context and determine if an autonomous action should be taken.\n\n"
            "Available actions: notify, calendar.add, calendar.list, contact.get, contact.search\n\n"
            f"Context: {json.dumps(context, default=str)}\n\n"
            "Respond with JSON only: {\"action\":\"action_type\",\"priority\":1-5,\"details\":{...}} or {\"action\":null}.\n"
            "Priority guide: 5=critical ... 1=background."
        )

    async def _parse_llm_action_robust(self, response: str, context: Dict[str, Any]) -> Optional[Action]:
        try:
            # Try direct JSON parse, otherwise extract braces
            data = None
            try:
                data = json.loads(response.strip())
            except Exception:
                cleaned = response.strip()
                if "```json" in cleaned:
                    cleaned = cleaned.split("```json", 1)[1].split("```", 1)[0]
                elif "```" in cleaned:
                    cleaned = cleaned.split("```", 1)[1].split("```", 1)[0]
                start = cleaned.find("{")
                end = cleaned.rfind("}")
                if start != -1 and end != -1:
                    json_blob = cleaned[start:end+1]
                    data = json.loads(json_blob)
            if not data:
                return None
            action = data.get("action")
            details = data.get("details", {})
            priority = data.get("priority", ActionPriority.NORMAL.value)
            if not action or action not in self.action_requirements:
                logger.debug("LLM produced invalid or unknown action: %s", action)
                return None
            return Action(action_type=action, details=details, priority=priority, source="llm_autonomy",
                          tags=["llm_generated"], metadata={"llm_context": context})
        except Exception:
            logger.debug("Failed to parse LLM action", exc_info=True)
            return None

    # Monitoring & health
    async def get_system_health(self) -> Dict[str, Any]:
        now = time.time()
        if not self._cache_valid or now - self._last_health_check > 30:
            await self._update_recent_actions_cache()
        async with self._exec_lock:
            recent = [a for a in self._recent_actions_cache if now - a.timestamp < 300]
            total = len(recent)
            success = sum(1 for a in recent if a.result and a.result.get("success"))
            failed_count = len([a for a in self.failed_actions if now - a.timestamp < 300])
        cb_states = {}
        for service, cb in self.circuit_breakers.items():
            cb_states[service] = await cb.get_state_info()
            if PROMETHEUS_AVAILABLE:
                try:
                    val = {"CLOSED": 0, "HALF_OPEN": 1, "OPEN": 2}.get(cb.state, -1)
                    autonomy_metrics['circuit_breaker_state'].labels(service=service).set(val)
                except Exception:
                    pass
        health_status = "healthy" if total > 0 and success == total and all(cb.state == "CLOSED" for cb in self.circuit_breakers.values()) else "degraded"
        return {
            "status": health_status,
            "queue_size": await self.pending_actions(),
            "recent_executions": total,
            "success_rate": (success / max(1, total)),
            "rate_limits_active": len(self.rate_limits),
            "autonomy_enabled": self.autonomy_enabled,
            "total_executed": len(self.executed_actions),
            "failed_actions": len(self.failed_actions),
            "failure_rate": len(self.failed_actions) / max(1, len(self.executed_actions)),
            "circuit_breakers": cb_states,
            "currently_executing": self.currently_executing,
            "timestamp": now
        }

    # Lifecycle methods
    async def reload_config(self):
        logger.info("Reloading AutonomyCore config")
        try:
            old = self.max_concurrent
            await self._load_dynamic_config()
            if self.max_concurrent <= 0:
                logger.warning("Invalid max_concurrent loaded, resetting to 3")
                self.max_concurrent = 3
            if old != self.max_concurrent:
                logger.info("max_concurrent updated %s -> %s", old, self.max_concurrent)
            # validate rate limits shape
            for atype, lim in list(self.rate_limits_config.items()):
                if lim.get("window", 0) <= 0 or lim.get("max_actions", 0) <= 0:
                    logger.warning("Invalid rate limit for %s: %s", atype, lim)
            logger.info("AutonomyCore config reloaded")
        except Exception:
            logger.exception("Failed to reload AutonomyCore config (non-fatal)")

    async def shutdown(self):
        self.autonomy_enabled = False
        logger.info("AutonomyCore shutdown initiated")
        # Stop continuous loop if running
        await self.stop_continuous_autonomy()
        # Wait for executing actions to finish with progressive backoff
        max_wait = 30
        start = time.time()
        interval = 0.1
        while self.currently_executing > 0 and (time.time() - start) < max_wait:
            logger.info("Waiting for %s executing actions to finish...", self.currently_executing)
            await asyncio.sleep(interval)
            interval = min(interval * 1.5, 2.0)
        if self.currently_executing > 0:
            logger.warning("Shutdown with %s actions still executing", self.currently_executing)
        audit_count = await self._perform_shutdown_audit()
        await self.cleanup()
        logger.info("AutonomyCore shutdown complete. Audited %s items.", audit_count)

    async def _perform_shutdown_audit(self) -> int:
        audit_count = 0
        ts = time.time()
        async with self._exec_lock:
            recent_actions = list(self.executed_actions)[-200:]
            queue_snapshot = await self.get_queue_summary()
            snapshot = {
                "shutdown_ts": ts,
                "final_queue_size": len(queue_snapshot),
                "currently_executing": self.currently_executing,
                "total_executed": len(self.executed_actions),
                "total_failed": len(self.failed_actions),
                "sample_count": len(recent_actions),
                "circuit_breakers": {s: await cb.get_state_info() for s, cb in self.circuit_breakers.items()}
            }
            try:
                if hasattr(self.core, "store") and hasattr(self.core.store, "put"):
                    key = f"shutdown_snapshot_{int(ts)}"
                    encrypted = self.encryptor.encrypt(json.dumps(snapshot, default=str))
                    maybe = self.core.store.put(key, "shutdown_snapshot", encrypted)
                    if asyncio.iscoroutine(maybe):
                        await maybe
                    audit_count += 1
            except Exception:
                logger.debug("Failed to persist shutdown snapshot (non-fatal)")

            for action in recent_actions:
                try:
                    audit_data = {"action": action.to_dict(), "snapshot": snapshot, "audit_type": "shutdown"}
                    encrypted = self.encryptor.encrypt(json.dumps(audit_data, default=str))
                    if hasattr(self.core, "audit_logger"):
                        try:
                            if hasattr(self.core.audit_logger, "ainfo"):
                                maybe = self.core.audit_logger.ainfo("autonomy_shutdown_action", extra={"audit_data": encrypted})
                                if asyncio.iscoroutine(maybe):
                                    await maybe
                            else:
                                self.core.audit_logger.info("autonomy_shutdown_action", extra={"audit_data": encrypted})
                            audit_count += 1
                        except Exception:
                            pass
                    elif hasattr(self.core, "store") and hasattr(self.core.store, "put"):
                        key = f"shutdown_audit_{action.id}"
                        maybe = self.core.store.put(key, "autonomy_audit", encrypted)
                        if asyncio.iscoroutine(maybe):
                            await maybe
                        audit_count += 1
                    else:
                        logger.info("SHUTDOWN_AUDIT %s", json.dumps(encrypted))
                        audit_count += 1
                except Exception:
                    logger.debug("Failed to audit action %s during shutdown", action.id, exc_info=True)
        return audit_count

    async def reset_circuit_breakers(self):
        changed = 0
        for service, cb in self.circuit_breakers.items():
            old = cb.state
            cb.failure_count = 0
            cb.state = "CLOSED"
            cb.last_failure_time = None
            if old != "CLOSED":
                changed += 1
                logger.info("Reset circuit breaker %s from %s to CLOSED", service, old)
        if changed:
            logger.info("Reset %s circuit breakers", changed)
        else:
            logger.debug("No circuit breakers required reset")

    async def cleanup(self):
        self.autonomy_enabled = False
        for h in list(self._config_handles):
            try:
                if hasattr(h, "cancel"):
                    h.cancel()
            except Exception:
                logger.debug("Config handle cleanup error", exc_info=True)
        logger.info("AutonomyCore cleanup complete")

# Factory
async def create_autonomy_core(persona: Any, core: Any, max_concurrent: int = 3) -> AutonomyCore:
    ac = AutonomyCore(persona=persona, core=core, max_concurrent=max_concurrent)
    await ac.initialize()
    logger.info("AutonomyCore created successfully")
    return ac