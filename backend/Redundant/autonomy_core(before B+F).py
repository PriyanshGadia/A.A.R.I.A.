"""
autonomy_core.py - Production A.A.R.I.A Autonomous Action Engine (Async-Compatible)

Key Updates for Compatibility:
- Full async/await support aligned with other core modules
- Integration with updated LLM adapter patterns
- Compatibility with async SecureStorage and AssistantCore
- Fixed context manager usage for LLM adapters
- Enhanced async health checks and monitoring
- Proper async action execution with fallbacks
"""

import time
import logging
import heapq
import hashlib
import json
import base64
import traceback
import asyncio
import hologram_state
from typing import List, Dict, Any, Optional, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
from collections import Counter, deque, defaultdict
from contextlib import asynccontextmanager
from functools import wraps

try:
    from Crypto.Cipher import AES
    from Crypto.Random import get_random_bytes
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    from prometheus_client import Counter as PromCounter, Histogram, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

from persona_core import PersonaCore
from assistant_core import AssistantCore

logger = logging.getLogger(__name__)

# Log crypto availability once
if not CRYPTO_AVAILABLE:
    logger.warning("PyCryptodome not available, using base64 fallback encryption")

# -------------------------
# Prometheus Metrics
# -------------------------
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

# -------------------------
# Async Retry Decorator
# -------------------------
def async_retry_operation(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0,
                         exceptions: Tuple[Exception, ...] = (Exception,)):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries, current_delay = 0, delay
            while True:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Operation {func.__name__} failed after {max_retries} retries: {e}")
                        raise
                    logger.warning(f"Operation {func.__name__} failed (attempt {retries}/{max_retries}): {e}")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator

# -------------------------
# Enums
# -------------------------
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

# -------------------------
# Async Circuit Breaker
# -------------------------
class AsyncCircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60, max_recovery_time: int = 3600):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.base_recovery_timeout = recovery_timeout
        self.max_recovery_time = max_recovery_time
        self.last_failure_time = None
        self.state = "CLOSED"
        self._lock = asyncio.Lock()
        self._last_state_check = 0

    async def can_execute(self) -> bool:
        async with self._lock:
            self._last_state_check = time.time()
            if self.state == "OPEN":
                current_timeout = min(
                    self.base_recovery_timeout * (2 ** (self.failure_count - self.failure_threshold)),
                    self.max_recovery_time
                )
                if time.time() - self.last_failure_time > current_timeout:
                    self.state = "HALF_OPEN"
                    return True
                return False
            return True

    async def get_state_info(self) -> Dict[str, Any]:
        """Get state info without modifying circuit breaker state"""
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

# -------------------------
# AES-GCM Encryption with Secure Fallback
# -------------------------
class AESGCMEncryptor:
    def __init__(self, key: Optional[bytes] = None):
        self.key = key or self._generate_key()
        self._key_warning_logged = False
    
    def _generate_key(self) -> bytes:
        if CRYPTO_AVAILABLE:
            return get_random_bytes(32)
        else:
            if not self._key_warning_logged:
                logger.warning("Using deterministic fallback key - not for production use")
                self._key_warning_logged = True
            return hashlib.sha256(b"autonomy_core_fallback_key").digest()
    
    def encrypt(self, data: str) -> Dict[str, Any]:
        try:
            if not CRYPTO_AVAILABLE:
                encoded = base64.b64encode(data.encode('utf-8')).decode('utf-8')
                return {'encryption': 'base64_fallback', 'data': encoded}
            
            nonce = get_random_bytes(12)
            cipher = AES.new(self.key, AES.MODE_GCM, nonce=nonce)
            ciphertext, tag = cipher.encrypt_and_digest(data.encode('utf-8'))
            return {
                'encryption': 'aes-gcm',
                'nonce': base64.b64encode(nonce).decode('utf-8'),
                'ciphertext': base64.b64encode(ciphertext).decode('utf-8'),
                'tag': base64.b64encode(tag).decode('utf-8')
            }
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            return {'encryption': 'plaintext_fallback', 'data': data}
    
    def decrypt(self, encrypted_data: Dict[str, Any]) -> str:
        try:
            encryption_type = encrypted_data.get('encryption', 'unknown')
            
            if encryption_type == 'plaintext_fallback':
                return encrypted_data['data']
            elif encryption_type == 'base64_fallback':
                return base64.b64decode(encrypted_data['data']).decode('utf-8')
            elif encryption_type == 'aes-gcm' and CRYPTO_AVAILABLE:
                nonce = base64.b64decode(encrypted_data['nonce'])
                ciphertext = base64.b64decode(encrypted_data['ciphertext'])
                tag = base64.b64decode(encrypted_data['tag'])
                
                cipher = AES.new(self.key, AES.MODE_GCM, nonce=nonce)
                return cipher.decrypt_and_verify(ciphertext, tag).decode('utf-8')
            else:
                raise ValueError(f"Unsupported encryption type: {encryption_type}")
                
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise

# -------------------------
# Async Multi-Level Queue
# -------------------------
class AsyncMultiLevelQueue:
    def __init__(self):
        self.queues = {priority: [] for priority in range(1, 6)}
        self.timestamps = {priority: [] for priority in range(1, 6)}
        self._lock = asyncio.Lock()
    
    async def push(self, action: 'Action'):
        async with self._lock:
            heapq.heappush(self.queues[action.priority], action)
            self.timestamps[action.priority].append(action.timestamp)
    
    async def pop(self) -> Optional['Action']:
        async with self._lock:
            for priority in range(5, 0, -1):
                if self.queues[priority]:
                    action = heapq.heappop(self.queues[priority])
                    if self.timestamps[priority]:
                        self.timestamps[priority].pop(0)
                    return action
            return None
    
    async def size(self) -> int:
        async with self._lock:
            return sum(len(q) for q in self.queues.values())
    
    async def clear_priority(self, priority: int) -> int:
        async with self._lock:
            count = len(self.queues[priority])
            self.queues[priority].clear()
            self.timestamps[priority].clear()
            return count
    
    async def get_summary(self) -> List[Dict[str, Any]]:
        async with self._lock:
            all_actions = []
            for priority in range(5, 0, -1):
                all_actions.extend([action.to_dict() for action in self.queues[priority]])
            return all_actions

# -------------------------
# Action Dataclass
# -------------------------
@dataclass
class Action:
    action_type: str
    details: Dict[str, Any]
    priority: int = ActionPriority.NORMAL.value
    source: str = "autonomy"
    timestamp: float = None
    executed: bool = False
    result: Optional[Dict[str, Any]] = None
    id: str = None
    category: str = field(default_factory=lambda: ActionCategory.SYSTEM.value)
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()
        self.priority = max(1, min(self.priority, 5))
        if self.id is None:
            self.id = f"act_{int(self.timestamp)}_{hashlib.md5(f'{self.action_type}{json.dumps(self.details, sort_keys=True)}'.encode()).hexdigest()[:8]}"
        
        # Auto-detect category if not specified
        if self.category == ActionCategory.SYSTEM.value:
            if self.action_type.startswith('notify'):
                self.category = ActionCategory.NOTIFICATION.value
            elif self.action_type.startswith('calendar'):
                self.category = ActionCategory.CALENDAR.value
            elif self.action_type.startswith('contact'):
                self.category = ActionCategory.CONTACT.value
            elif 'llm' in self.action_type or self.source == 'llm_autonomy':
                self.category = ActionCategory.LLM.value

    def __lt__(self, other: 'Action'):
        if self.priority == other.priority:
            return self.timestamp < other.timestamp
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

# -------------------------
# Async Autonomy Core
# -------------------------
class AutonomyCore:
    def __init__(self, persona: PersonaCore, core: AssistantCore, max_concurrent: int = 3):
        self.persona = persona
        self.core = core
        self.max_concurrent = max_concurrent

        # Async locks
        self._queue_lock = asyncio.Lock()
        self._exec_lock = asyncio.Lock()
        self._metrics_lock = asyncio.Lock()
        self._config_lock = asyncio.Lock()

        # Async multi-level queue for better performance
        self.action_queue = AsyncMultiLevelQueue()
        self.executed_actions: deque = deque(maxlen=1000)
        self.failed_actions: deque = deque(maxlen=500)
        self.rate_limits: Dict[str, List[float]] = {}
        self.autonomy_enabled: bool = True
        self.currently_executing: int = 0

        # Async circuit breakers
        self.circuit_breakers = {
            "calendar": AsyncCircuitBreaker(failure_threshold=3, recovery_timeout=60, max_recovery_time=600),
            "notifications": AsyncCircuitBreaker(failure_threshold=5, recovery_timeout=30, max_recovery_time=300),
            "contacts": AsyncCircuitBreaker(failure_threshold=3, recovery_timeout=45, max_recovery_time=450)
        }

        # Encryption
        self.encryptor = AESGCMEncryptor()

        # Configuration
        self._config_handles = []
        self._initialized = False

    async def initialize(self):
        """Initialize async components"""
        await self._load_dynamic_config()
        self._setup_config_watcher()
        self._initialized = True
        
        # ✅ FIXED: Schedule async config load instead of calling directly
        asyncio.create_task(self._load_dynamic_config())
        
        self._setup_config_watcher()

        # Action requirements and rate limits
        self.action_requirements = {
            "notify": {"required": ["channel", "message"], "optional": ["urgency"], "category": ActionCategory.NOTIFICATION.value},
            "calendar.add": {"required": ["title", "datetime"], "optional": ["priority", "location", "description"], "category": ActionCategory.CALENDAR.value},
            "calendar.list": {"required": [], "optional": ["days"], "category": ActionCategory.CALENDAR.value},
            "contact.get": {"required": ["id"], "optional": [], "category": ActionCategory.CONTACT.value},
            "contact.search": {"required": ["q"], "optional": ["limit"], "category": ActionCategory.CONTACT.value}
        }
        self.rate_limits_config = {
            "notify": {"window": 60, "max_actions": 5},
            "calendar.add": {"window": 300, "max_actions": 2},
            "contact.get": {"window": 30, "max_actions": 10},
            "contact.search": {"window": 30, "max_actions": 10},
            "default": {"window": 60, "max_actions": 3}
        }

        # LLM configuration - updated for new adapter pattern
        self.llm_config = {
            "summarization_model": "claude-3-haiku",
            "autonomy_model": "claude-3-haiku",
            "fallback_models": ["gpt-3.5-turbo", "local-llm"],
            "max_retries": 2
        }

        # Performance optimizations
        self._recent_actions_cache = None
        self._cache_valid = False
        self._last_health_check = 0

        if PROMETHEUS_AVAILABLE:
            autonomy_metrics['queue_size'].set(0)
            autonomy_metrics['concurrent_actions'].set(0)

        logger.info("Async AutonomyCore initialized with multi-level queue and enhanced async support")
    

    # -------------------------
    # Configuration Management - FIXED: Now async methods
    # -------------------------
    async def _load_dynamic_config(self):
        """Load dynamic configuration with async lock support"""
        try:
            if hasattr(self.core, 'config_manager'):
                config = self.core.config_manager.get("autonomy", {})
                async with self._config_lock:
                    self.max_concurrent = config.get("max_concurrent", self.max_concurrent)
                    
                    llm_config = config.get("llm", {})
                    if llm_config:
                        self.llm_config.update(llm_config)
                    
                    # Update rate limits if provided
                    rate_limits = config.get("rate_limits", {})
                    if rate_limits:
                        self.rate_limits_config.update(rate_limits)
                
                logger.debug(f"Loaded dynamic configuration: max_concurrent={self.max_concurrent}")
        except Exception as e:
            logger.warning(f"Failed to load dynamic configuration: {e}")

    def _setup_config_watcher(self):
        """Setup configuration change watcher if available"""
        try:
            if hasattr(self.core, 'config_manager') and hasattr(self.core.config_manager, 'watch'):
                async def config_change_handler(new_config):
                    logger.info("Configuration change detected, reloading...")
                    await self.reload_config()
                
                handle = self.core.config_manager.watch("autonomy", config_change_handler)
                self._config_handles.append(handle)
                logger.debug("Configuration watcher established")
        except Exception as e:
            logger.debug(f"Configuration watching not available: {e}")

    # -------------------------
    # Queue Management
    # -------------------------
    async def enqueue_action(self, action: Action) -> bool:
        is_valid, msg = await self._validate_action(action)
        if not is_valid:
            logger.debug(f"Action validation failed: {msg}")
            if PROMETHEUS_AVAILABLE:
                autonomy_metrics['validation_failures'].labels(reason=msg.split(':')[0] if ':' in msg else 'unknown').inc()
            return False
        
        await self.action_queue.push(action)
        if PROMETHEUS_AVAILABLE:
            autonomy_metrics['queue_size'].inc()
        
        logger.debug(f"Enqueued action '{action.action_type}' with priority {action.priority} (ID: {action.id})")
        return True

    async def dequeue_action(self) -> Optional[Action]:
        action = await self.action_queue.pop()
        if action and PROMETHEUS_AVAILABLE:
            autonomy_metrics['queue_size'].dec()
        return action

    async def pending_actions(self) -> int:
        return await self.action_queue.size()

    async def get_queue_summary(self) -> List[Dict[str, Any]]:
        return await self.action_queue.get_summary()

    async def clear_queue(self, priority_filter: Optional[int] = None) -> int:
        if priority_filter is None:
            count = await self.pending_actions()
            # Clear all queues
            for priority in range(1, 6):
                await self.action_queue.clear_priority(priority)
            if PROMETHEUS_AVAILABLE:
                autonomy_metrics['queue_size'].set(0)
            return count
        else:
            return await self.action_queue.clear_priority(priority_filter)

    # -------------------------
    # Validation
    # -------------------------
    async def _validate_action(self, action: Action) -> Tuple[bool, str]:
        if action.action_type not in self.action_requirements:
            return False, f"unknown_action_type:{action.action_type}"
        
        requirements = self.action_requirements[action.action_type]
        for field in requirements["required"]:
            if field not in action.details:
                return False, f"missing_field:{field}"
        
        if action.action_type == "notify":
            valid_channels = ["desktop", "sms", "call"]
            if action.details.get("channel") not in valid_channels:
                return False, f"invalid_channel:{action.details.get('channel')}"
        
        elif action.action_type == "calendar.add":
            if not await self._validate_datetime(action.details["datetime"]):
                return False, "invalid_datetime_format"
        
        return True, "valid"

    async def _validate_datetime(self, dt_str: str) -> bool:
        """Validate and normalize datetime string"""
        try:
            # Handle various ISO 8601 formats
            if 'Z' in dt_str:
                dt_str = dt_str.replace('Z', '+00:00')
            elif dt_str.count('-') == 2 and 'T' in dt_str and '+' not in dt_str and 'Z' not in dt_str:
                # Assume local time, convert to UTC
                dt = datetime.fromisoformat(dt_str)
                dt_str = dt.astimezone(timezone.utc).isoformat()
            
            datetime.fromisoformat(dt_str)
            return True
        except (ValueError, AttributeError):
            return False

    def _get_service_from_action(self, action_type: str) -> str:
        if action_type.startswith("calendar"):
            return "calendar"
        elif action_type.startswith("notify"):
            return "notifications"
        elif action_type.startswith("contact"):
            return "contacts"
        return "default"

    async def _can_execute(self, action: Action) -> Tuple[bool, str]:
        # Check user consent
        try:
            profile = await self.persona.core.load_user_profile() or {}
            if not profile.get("allow_autonomy", True):
                return False, "user_consent_denied"
        except Exception as e:
            logger.debug(f"Failed to check user profile: {e}")
            return False, "consent_check_failed"
        
        # Check circuit breaker
        service = self._get_service_from_action(action.action_type)
        circuit_breaker = self.circuit_breakers.get(service)
        if circuit_breaker and not await circuit_breaker.can_execute():
            return False, f"circuit_breaker_open:{service}"
        
        # Check rate limits
        limit = self.rate_limits_config.get(action.action_type, self.rate_limits_config["default"])
        window_start = time.time() - limit["window"]
        
        async with self._exec_lock:
            # Use cached recent actions for better performance
            if not self._cache_valid or time.time() - self._last_health_check > 30:
                await self._update_recent_actions_cache()
            
            recent = [
                a for a in self._recent_actions_cache 
                if a.action_type == action.action_type and a.timestamp > window_start
            ]
        
        if len(recent) >= limit["max_actions"]:
            if PROMETHEUS_AVAILABLE:
                autonomy_metrics['rate_limit_hits'].labels(type=action.action_type).inc()
            oldest_timestamp = min(a.timestamp for a in recent) if recent else window_start
            wait = limit["window"] - (time.time() - oldest_timestamp)
            return False, f"rate_limit_exceeded:{wait:.1f}s"
        
        return True, "allowed"

    async def _update_recent_actions_cache(self):
        """Update cache of recent actions for performance"""
        async with self._exec_lock:
            self._recent_actions_cache = list(self.executed_actions) + list(self.failed_actions)
            self._cache_valid = True
            self._last_health_check = time.time()

    async def _update_rate_limits(self, action: Action):
        now = time.time()
        limit = self.rate_limits_config.get(action.action_type, self.rate_limits_config["default"])
        async with self._exec_lock:
            self.rate_limits.setdefault(action.action_type, []).append(now)
            cutoff = now - limit["window"]
            self.rate_limits[action.action_type] = [t for t in self.rate_limits[action.action_type] if t > cutoff]
            self._cache_valid = False  # Invalidate cache

    # -------------------------
    # Execution Context
    # -------------------------
    @asynccontextmanager
    async def _execution_context(self, action: Action):
        start_time = time.monotonic()
        async with self._metrics_lock:
            self.currently_executing += 1
            if PROMETHEUS_AVAILABLE:
                autonomy_metrics['concurrent_actions'].inc()
        try:
            yield
        finally:
            exec_time = time.monotonic() - start_time
            async with self._metrics_lock:
                self.currently_executing -= 1
                if PROMETHEUS_AVAILABLE:
                    autonomy_metrics['concurrent_actions'].dec()
                    autonomy_metrics['action_duration'].labels(type=action.action_type).observe(exec_time)

    # -------------------------
    # Action Execution
    # -------------------------
    async def execute_action(self, action: Action) -> Tuple[bool, Any]:
        can_exec, reason = await self._can_execute(action)
        if not can_exec:
            logger.debug(f"Action blocked: {reason}")
            if PROMETHEUS_AVAILABLE and 'rate_limit' in reason:
                autonomy_metrics['actions_executed'].labels(type=action.action_type, status='rate_limited').inc()
            return False, reason
        
        service = self._get_service_from_action(action.action_type)
        
        # --- NEW: Hologram Task Tracking ---
        node_id = f"auto_task_{action.id}"
        link_id = f"link_auto_{node_id}"
        
        async with self._execution_context(action):
            start = time.monotonic()
            
            try:
                # --- NEW: Spawn "Action" node ---
                await hologram_state.spawn_and_link(
                    node_id=node_id, node_type="task", label=action.action_type, size=6,
                    source_id="AutonomyCore", link_id=link_id
                )
                await hologram_state.set_node_active("AutonomyCore")
                # --- End New Block ---
                
                try:
                    result = await self._execute_action_internal(action)
                    exec_time = time.monotonic() - start
                    action.result = await self._process_action_result(action, result, exec_time, True)
                    await self._mark_executed(action)
                    
                    if service in self.circuit_breakers:
                        await self.circuit_breakers[service].record_success()
                    
                    if PROMETHEUS_AVAILABLE:
                        autonomy_metrics['actions_executed'].labels(type=action.action_type, status='success').inc()
                    
                    logger.debug(f"Executed '{action.action_type}' in {exec_time:.3f}s")
                    return True, result
                        
                except Exception as e:
                    exec_time = time.monotonic() - start
                    error = {"message": str(e), "trace": traceback.format_exc()}
                    action.result = await self._process_action_result(action, error, exec_time, False)
                    await self._mark_failed(action)
                    
                    if service in self.circuit_breakers:
                        await self.circuit_breakers[service].record_failure()
                    
                    if PROMETHEUS_AVAILABLE:
                        autonomy_metrics['actions_executed'].labels(type=action.action_type, status='failure').inc()
                    
                    logger.error(f"Execution failed for {action.action_type}: {e}")
                    await hologram_state.set_node_error(node_id) # <-- NEW: Show error
                    return False, error
            
            finally:
                # --- NEW: Despawn "Action" node ---
                await hologram_state.set_node_idle("AutonomyCore")
                await hologram_state.despawn_and_unlink(node_id, link_id)
                # --- End New Block ---

    @async_retry_operation(max_retries=2, delay=0.5, exceptions=(Exception,))
    async def _execute_action_internal(self, action: Action) -> Any:
        """Internal execution with retry logic"""
        if action.action_type == "notify":
            return await self._execute_notify(action.details)
        elif action.action_type == "calendar.add":
            return await self._execute_calendar_add(action.details)
        elif action.action_type == "calendar.list":
            return await self._execute_calendar_list(action.details)
        elif action.action_type == "contact.get":
            return await self._execute_contact_get(action.details)
        elif action.action_type == "contact.search":
            return await self._execute_contact_search(action.details)
        else:
            raise ValueError(f"Unsupported action type: {action.action_type}")

    async def _process_action_result(self, action: Action, result: Any, exec_time: float, success: bool) -> Dict[str, Any]:
        return {
            "success": success,
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
        
        try:
            memory_summary = f"Executed {action.action_type}: {action.result['success']}"
            self.persona.store_memory(
                "action_execution",
                memory_summary,
                {
                    "execution_time": action.result['execution_time'],
                    "success": action.result['success'],
                    "action_type": action.action_type,
                    "action_id": action.id,
                    "category": action.category
                },
                importance=4 if not action.result['success'] else 3
            )
        except Exception as e:
            logger.debug(f"Failed to store action in memory: {e}")
        
        await self._audit_action(action, action.result)

    async def _mark_failed(self, action: Action):
        async with self._exec_lock:
            self.failed_actions.append(action)
            # Don't add to executed_actions to keep metrics clean
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
            
            encrypted_audit = self.encryptor.encrypt(json.dumps(audit_entry, default=str))
            
            # Standardized audit logging path
            if hasattr(self.core, 'audit_logger'):
                if hasattr(self.core.audit_logger, 'ainfo'):
                    await self.core.audit_logger.ainfo("autonomy_action", extra={"audit_data": encrypted_audit})
                else:
                    self.core.audit_logger.info("autonomy_action", extra={"audit_data": encrypted_audit})
            elif hasattr(self.core, 'store'):
                audit_key = f"audit_action_{action.id}"
                await self.core.store.put(audit_key, encrypted_audit)
            else:
                logger.info(f"AUTONOMY_AUDIT {json.dumps(encrypted_audit)}")
                
        except Exception as e:
            logger.debug(f"Failed to create audit entry: {e}")

    async def _get_user_context(self) -> Dict[str, Any]:
        try:
            if hasattr(self.persona, 'get_user_context'):
                user_context = self.persona.get_user_context()
                if asyncio.iscoroutine(user_context):
                    return await user_context
                return user_context
        except Exception as e:
            logger.debug(f"Failed to get user context: {e}")
        return {"user_available": False}

    async def _generate_compliance_tags(self, action: Action) -> Dict[str, str]:
        return {
            "data_category": "operational" if action.category == ActionCategory.CALENDAR.value else "communication",
            "retention_period": "7y",
            "encryption_required": "true",
            "pii_handling": "minimal" if action.category == ActionCategory.CONTACT.value else "none",
            "audit_required": "true"
        }

    # -------------------------
    # Action Implementations
    # -------------------------
    async def _execute_notify(self, details: Dict[str, Any]) -> Dict[str, Any]:
        message = details["message"]
        
        if len(message) > 200 and hasattr(self.core, 'llm_orchestrator'):
            summarized_message = await self._summarize_message_with_fallback(message)
            if summarized_message:
                message = summarized_message
        
        return {
            "status": "sent", 
            "channel": details["channel"], 
            "message_preview": message[:50],
            "original_length": len(details["message"]),
            "final_length": len(message)
        }

    async def _summarize_message_with_fallback(self, message: str) -> Optional[str]:
        from llm_adapter import LLMAdapterFactory, LLMProvider, LLMRequest
        
        models_to_try = [
            self.llm_config["summarization_model"],
            *self.llm_config["fallback_models"]
        ]
        
        for model in models_to_try:
            try:
                # Use the updated LLM adapter pattern
                async with LLMAdapterFactory.get_adapter(LLMProvider.GROQ) as adapter:
                    request = LLMRequest(
                        messages=[{"role": "user", "content": f"Summarize this in under 100 chars: {message}"}],
                        max_tokens=50,
                        temperature=0.1
                    )
                    response = await adapter.chat(request)
                    if response and response.content.strip():
                        return response.content.strip()
            except Exception as e:
                logger.debug(f"Summarization failed with model {model}: {e}")
                continue
        
        return None

    async def _execute_calendar_add(self, details: Dict[str, Any]) -> Dict[str, Any]:
        try:
            event_id = f"event_{int(time.time())}"
            if hasattr(self.core, "add_event"):
                result = await self.core.add_event(details)
                return {"status": "added", "event_id": event_id, "core_result": result}
            else:
                return {"status": "simulated", "event_id": event_id, "note": "Calendar integration not available"}
        except Exception as e:
            return {"status": "failed", "error": str(e), "trace": traceback.format_exc()}

    async def _execute_calendar_list(self, details: Dict[str, Any]) -> Dict[str, Any]:
        days = details.get("days", 7)
        try:
            events = await self.core.list_upcoming_events(days) if hasattr(self.core, "list_upcoming_events") else []
            return {"status": "listed", "count": len(events), "days": days}
        except Exception as e:
            return {"status": "failed", "error": str(e), "count": 0, "days": days}

    async def _execute_contact_get(self, details: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # Updated to use async contact methods
            contacts = await self.core.list_contacts() if hasattr(self.core, "list_contacts") else []
            contact = next((c for c in contacts if c.get('id') == details["id"]), None)
            return {"status": "retrieved" if contact else "not_found", "contact": contact}
        except Exception as e:
            return {"status": "failed", "error": str(e), "contact_id": details["id"]}

    async def _execute_contact_search(self, details: Dict[str, Any]) -> Dict[str, Any]:
        try:
            limit = details.get("limit", 10)
            contacts = await self.core.list_contacts() if hasattr(self.core, "list_contacts") else []
            query = details["q"].lower()
            results = [c for c in contacts if query in c.get('name', '').lower() or 
                      query in c.get('email', '').lower()][:limit]
            return {"status": "searched", "count": len(results), "query": details["q"], "limit": limit}
        except Exception as e:
            return {"status": "failed", "error": str(e), "query": details["q"]}

    # -------------------------
    # Autonomous Loop
    # -------------------------
    async def run_autonomy_cycle(self) -> Dict[str, Any]:
        if not self.autonomy_enabled:
            return {"status": "disabled", "executed": 0}
        
        executed_count = 0
        successful_count = 0
        start_time = time.monotonic()
        
        while await self.pending_actions() > 0 and executed_count < self.max_concurrent:
            action = await self.dequeue_action()
            if action:
                success, _ = await self.execute_action(action)
                executed_count += 1
                if success:
                    successful_count += 1
        
        cycle_time = time.monotonic() - start_time
        summary = {
            "status": "completed",
            "executed": executed_count,
            "successful": successful_count,
            "cycle_time": cycle_time,
            "queue_remaining": await self.pending_actions(),
            "currently_executing": self.currently_executing,
            "timestamp": time.time()
        }
        
        logger.debug(f"Autonomy cycle: {executed_count} executed, {successful_count} successful")
        return summary

    async def start_continuous_autonomy(self, interval: float = 5.0):
        """Start continuous autonomy loop"""
        async def autonomy_loop():
            while self.autonomy_enabled:
                try:
                    await self.run_autonomy_cycle()
                    await asyncio.sleep(interval)
                except Exception as e:
                    logger.error(f"Autonomy loop error: {e}")
                    await asyncio.sleep(interval * 2)
        
        # Start the loop in the background
        asyncio.create_task(autonomy_loop())
        logger.info(f"Started continuous autonomy with {interval}s interval")

    # -------------------------
    # LLM-Driven Autonomy
    # -------------------------
    async def evaluate_autonomous_decision(self, context: Dict[str, Any]) -> Optional[Action]:
        from llm_adapter import LLMAdapterFactory, LLMProvider, LLMRequest
        
        prompt = await self._build_autonomy_prompt(context)
        
        for model in [self.llm_config["autonomy_model"], *self.llm_config["fallback_models"]]:
            try:
                async with LLMAdapterFactory.get_adapter(LLMProvider.GROQ) as adapter:
                    request = LLMRequest(
                        messages=[{"role": "system", "content": prompt}],
                        temperature=0.1,
                        max_tokens=500
                    )
                    response = await adapter.chat(request)
                    
                    action = await self._parse_llm_action_robust(response.content, context)
                    if action:
                        return action
                        
            except Exception as e:
                logger.debug(f"Autonomy decision failed with model {model}: {e}")
                continue
        
        return None

    async def _build_autonomy_prompt(self, context: Dict[str, Any]) -> str:
        return f"""
Analyze the context and determine if an autonomous action should be taken.
Available actions: notify, calendar.add, calendar.list, contact.get, contact.search

Context: {json.dumps(context, default=str)}

Respond with valid JSON only. Format:
{{
    "action": "action_type", 
    "priority": 1-5, 
    "details": {{...}}
}}
or {{"action": null}} if no action needed.

Priority guide: 5=critical, 4=high, 3=normal, 2=low, 1=background
"""

    async def _parse_llm_action_robust(self, response: str, context: Dict[str, Any]) -> Optional[Action]:
        """Robust LLM response parsing with multiple format support"""
        try:
            # Try direct JSON parsing first
            try:
                data = json.loads(response.strip())
            except json.JSONDecodeError:
                # Extract JSON from markdown or other formatting
                cleaned_response = response.strip()
                
                # Handle ```json ... ``` format
                if '```json' in cleaned_response:
                    cleaned_response = cleaned_response.split('```json')[1].split('```')[0].strip()
                elif '```' in cleaned_response:
                    cleaned_response = cleaned_response.split('```')[1].split('```')[0].strip()
                
                # Handle { ... } format
                start_idx = cleaned_response.find('{')
                end_idx = cleaned_response.rfind('}')
                if start_idx != -1 and end_idx != -1:
                    cleaned_response = cleaned_response[start_idx:end_idx+1]
                
                data = json.loads(cleaned_response)
            
            if not data.get("action"):
                return None
                
            # Validate action type
            if data["action"] not in self.action_requirements:
                logger.debug(f"LLM suggested invalid action type: {data['action']}")
                return None
                
            return Action(
                action_type=data["action"],
                details=data["details"],
                priority=data.get("priority", ActionPriority.NORMAL.value),
                source="llm_autonomy",
                tags=["llm_generated"],
                metadata={"llm_context": context}
            )
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.debug(f"Failed to parse LLM action: {e}")
            return None

    # -------------------------
    # Enhanced Monitoring & Analytics
    # -------------------------
    async def get_system_health(self) -> Dict[str, Any]:
        now = time.time()
        
        # Use cached data if recent
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
                state_value = {"CLOSED": 0, "HALF_OPEN": 1, "OPEN": 2}.get(cb.state, -1)
                autonomy_metrics['circuit_breaker_state'].labels(service=service).set(state_value)
        
        health_status = "healthy" if success == total and all(cb.state == "CLOSED" for cb in self.circuit_breakers.values()) else "degraded"
        
        return {
            "status": health_status,
            "queue_size": await self.pending_actions(),
            "recent_executions": total,
            "success_rate": success / max(1, total),
            "rate_limits_active": len(self.rate_limits),
            "autonomy_enabled": self.autonomy_enabled,
            "total_executed": len(self.executed_actions),
            "failed_actions": len(self.failed_actions),
            "failure_rate": len(self.failed_actions) / max(1, len(self.executed_actions)),
            "circuit_breakers": cb_states,
            "currently_executing": self.currently_executing,
            "timestamp": now
        }

    # -------------------------
    # Lifecycle Management - FIXED: Now async methods
    # -------------------------
    async def reload_config(self):
        """Reload configuration with validation"""
        logger.info("Reloading AutonomyCore configuration...")
        
        try:
            old_concurrent = self.max_concurrent
            await self._load_dynamic_config()  # Now properly awaited
            
            # Validate new configuration
            if self.max_concurrent <= 0:
                logger.warning(f"Invalid max_concurrent value: {self.max_concurrent}, using default 3")
                self.max_concurrent = 3
            
            # Log configuration changes
            if old_concurrent != self.max_concurrent:
                logger.info(f"Updated max_concurrent: {old_concurrent} -> {self.max_concurrent}")
            
            # Validate rate limits
            for action_type, limits in self.rate_limits_config.items():
                if limits["window"] <= 0 or limits["max_actions"] <= 0:
                    logger.warning(f"Invalid rate limits for {action_type}: {limits}")
            
            logger.info("AutonomyCore configuration reloaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")
            # Restore safe defaults if possible
            self.max_concurrent = 3

    async def shutdown(self):
        """Graceful shutdown with comprehensive state preservation"""
        self.autonomy_enabled = False
        logger.info("AutonomyCore shutdown initiated")
        
        # Wait for current executions with progressive backoff
        max_wait = 30
        start_wait = time.time()
        check_interval = 0.1
        
        while self.currently_executing > 0 and (time.time() - start_wait) < max_wait:
            remaining = self.currently_executing
            logger.info(f"Waiting for {remaining} actions to complete...")
            await asyncio.sleep(check_interval)
            check_interval = min(check_interval * 1.5, 2.0)  # Progressive backoff
        
        if self.currently_executing > 0:
            logger.warning(f"Shutdown completed with {self.currently_executing} actions still executing")
        
        # Comprehensive state audit
        audit_count = await self._perform_shutdown_audit()
        
        # Cleanup resources
        await self.cleanup()
        
        logger.info(f"AutonomyCore shutdown complete. Audited {audit_count} actions.")

    async def _perform_shutdown_audit(self) -> int:
        """Perform comprehensive shutdown audit"""
        audit_count = 0
        shutdown_timestamp = time.time()
        
        async with self._exec_lock:
            # Audit recent executed actions
            recent_actions = list(self.executed_actions)[-200:]
            queue_actions = await self.get_queue_summary()
            
            # Create shutdown snapshot
            shutdown_snapshot = {
                "shutdown_timestamp": shutdown_timestamp,
                "final_queue_size": len(queue_actions),
                "final_executing_count": self.currently_executing,
                "total_executed_actions": len(self.executed_actions),
                "total_failed_actions": len(self.failed_actions),
                "recent_actions_sample": len(recent_actions),
                "circuit_breaker_states": {s: await cb.get_state_info() for s, cb in self.circuit_breakers.items()}
            }
            
            # Store shutdown snapshot
            try:
                if hasattr(self.core, 'store'):
                    snapshot_key = f"shutdown_snapshot_{int(shutdown_timestamp)}"
                    encrypted_snapshot = self.encryptor.encrypt(json.dumps(shutdown_snapshot, default=str))
                    await self.core.store.put(snapshot_key, "shutdown_snapshot", encrypted_snapshot)
                    audit_count += 1
            except Exception as e:
                logger.warning(f"Failed to store shutdown snapshot: {e}")
            
            # Audit individual actions
            for action in recent_actions:
                try:
                    audit_data = {
                        "action": action.to_dict(),
                        "shutdown_context": shutdown_snapshot,
                        "audit_type": "shutdown"
                    }
                    
                    encrypted_data = self.encryptor.encrypt(json.dumps(audit_data, default=str))
                    
                    # Use primary audit path
                    if hasattr(self.core, 'audit_logger'):
                        if hasattr(self.core.audit_logger, 'ainfo'):
                            await self.core.audit_logger.ainfo("autonomy_shutdown_action", extra={"audit_data": encrypted_data})
                        else:
                            self.core.audit_logger.info("autonomy_shutdown_action", extra={"audit_data": encrypted_data})
                        audit_count += 1
                    elif hasattr(self.core, 'store'):
                        audit_key = f"shutdown_audit_{action.id}"
                        await self.core.store.put(audit_key, "autonomy_audit", encrypted_data)
                        audit_count += 1
                    else:
                        logger.info(f"SHUTDOWN_AUDIT {json.dumps(encrypted_data)}")
                        audit_count += 1
                        
                except Exception as e:
                    logger.debug(f"Failed to audit action {action.id} during shutdown: {e}")
        
        return audit_count

    async def reset_circuit_breakers(self):
        """Reset all circuit breakers with logging"""
        reset_count = 0
        for service, cb in self.circuit_breakers.items():
            old_state = cb.state
            cb.failure_count = 0
            cb.state = "CLOSED"
            cb.last_failure_time = None
            
            if old_state != "CLOSED":
                reset_count += 1
                logger.info(f"Reset circuit breaker for {service} from {old_state} to CLOSED")
        
        if reset_count > 0:
            logger.info(f"Reset {reset_count} circuit breakers")
        else:
            logger.debug("No circuit breakers needed reset")

    async def cleanup(self):
        """Cleanup resources"""
        self.autonomy_enabled = False
        
        # Remove config watchers
        for handle in self._config_handles:
            try:
                if hasattr(handle, 'cancel'):
                    handle.cancel()
            except Exception as e:
                logger.debug(f"Error cleaning up config watcher: {e}")
        
        logger.info("AutonomyCore cleanup completed")

# -------------------------
# Helper Functions
# -------------------------
async def create_autonomy_core(persona: PersonaCore, core: AssistantCore, max_concurrent: int = 3) -> AutonomyCore:
    """Factory function to create and initialize AutonomyCore"""
    autonomy_core = AutonomyCore(persona, core, max_concurrent)
    logger.info("AutonomyCore created successfully")
    return autonomy_core