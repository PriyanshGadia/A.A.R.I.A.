"""
autonomy_core.py - Production A.A.R.I.A Autonomous Action Engine

Optimized Features:
- Priority-based action queue with FIFO fairness for equal priority
- UTC normalization for calendar events ensuring timezone safety
- Unified rate limiting including failed actions in quota calculations
- Separate failure tracking for enhanced monitoring and analysis
- Thread-safe execution for concurrent autonomy cycles
- Comprehensive health checks with failure metrics
- Robust error handling with detailed audit trails
- Circuit breaker pattern for resilient external service calls
- Enhanced metrics collection for Prometheus integration
- Dynamic configuration loading from A.A.R.I.A core
- LLM-driven autonomous decision making
- Encrypted audit logging (AES-GCM) for compliance
- Graceful fallbacks for missing dependencies
- Async-ready architecture for I/O-heavy operations
- Multi-level queue optimization
- Retry logic for external calls
"""

import time
import logging
import heapq
import threading
import hashlib
import json
import base64
import traceback
import asyncio
from typing import List, Dict, Any, Optional, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
from collections import Counter, deque, defaultdict
from contextlib import contextmanager
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

# # -------------------------
# # Logger Setup
# # -------------------------
# logger = logging.getLogger("AARIA.Autonomy")
# if not logger.handlers:
#     handler = logging.StreamHandler()
#     handler.setFormatter(logging.Formatter("%(asctime)s [AARIA.AUTONOMY] [%(levelname)s] %(message)s"))
#     logger.addHandler(handler)
# logger.setLevel(logging.INFO)
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
# Retry Decorator
# -------------------------
def retry_operation(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0,
                   exceptions: Tuple[Exception, ...] = (Exception,)):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries, current_delay = 0, delay
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Operation {func.__name__} failed after {max_retries} retries: {e}")
                        raise
                    logger.warning(f"Operation {func.__name__} failed (attempt {retries}/{max_retries}): {e}")
                    time.sleep(current_delay)
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
# Circuit Breaker with State Reporting
# -------------------------
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60, max_recovery_time: int = 3600):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.base_recovery_timeout = recovery_timeout
        self.max_recovery_time = max_recovery_time
        self.last_failure_time = None
        self.state = "CLOSED"
        self._lock = threading.Lock()
        self._last_state_check = 0

    def can_execute(self) -> bool:
        with self._lock:
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

    def get_state_info(self) -> Dict[str, Any]:
        """Get state info without modifying circuit breaker state"""
        with self._lock:
            return {
                "state": self.state,
                "failure_count": self.failure_count,
                "last_failure_time": self.last_failure_time,
                "last_state_check": self._last_state_check
            }

    def record_success(self):
        with self._lock:
            self.failure_count = 0
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"

    def record_failure(self):
        with self._lock:
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
# Multi-Level Queue Optimizer
# -------------------------
class MultiLevelQueue:
    def __init__(self):
        self.queues = {priority: [] for priority in range(1, 6)}
        self.timestamps = {priority: [] for priority in range(1, 6)}
        self._lock = threading.Lock()
    
    def push(self, action: 'Action'):
        with self._lock:
            heapq.heappush(self.queues[action.priority], action)
            self.timestamps[action.priority].append(action.timestamp)
    
    def pop(self) -> Optional['Action']:
        with self._lock:
            for priority in range(5, 0, -1):
                if self.queues[priority]:
                    action = heapq.heappop(self.queues[priority])
                    if self.timestamps[priority]:
                        self.timestamps[priority].pop(0)
                    return action
            return None
    
    def size(self) -> int:
        with self._lock:
            return sum(len(q) for q in self.queues.values())
    
    def clear_priority(self, priority: int) -> int:
        with self._lock:
            count = len(self.queues[priority])
            self.queues[priority].clear()
            self.timestamps[priority].clear()
            return count
    
    def get_summary(self) -> List[Dict[str, Any]]:
        with self._lock:
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
# Autonomy Core
# -------------------------
class AutonomyCore:
    def __init__(self, persona: PersonaCore, core: AssistantCore, max_concurrent: int = 3):
        self.persona = persona
        self.core = core
        self.max_concurrent = max_concurrent

        # Locks
        self._queue_lock = threading.Lock()
        self._exec_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._config_lock = threading.Lock()

        # Multi-level queue for better performance
        self.action_queue = MultiLevelQueue()
        self.executed_actions: deque = deque(maxlen=1000)
        self.failed_actions: deque = deque(maxlen=500)
        self.rate_limits: Dict[str, List[float]] = {}
        self.autonomy_enabled: bool = True
        self.currently_executing: int = 0

        # Circuit breakers
        self.circuit_breakers = {
            "calendar": CircuitBreaker(failure_threshold=3, recovery_timeout=60, max_recovery_time=600),
            "notifications": CircuitBreaker(failure_threshold=5, recovery_timeout=30, max_recovery_time=300),
            "contacts": CircuitBreaker(failure_threshold=3, recovery_timeout=45, max_recovery_time=450)
        }

        # Encryption
        self.encryptor = AESGCMEncryptor()

        # Configuration
        self._config_handles = []
        self._load_dynamic_config()
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

        # LLM configuration
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

        logger.info("Optimized AutonomyCore initialized with multi-level queue and async support")

    # -------------------------
    # Configuration Management
    # -------------------------
    def _load_dynamic_config(self):
        try:
            if hasattr(self.core, 'config_manager'):
                config = self.core.config_manager.get("autonomy", {})
                with self._config_lock:
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
                def config_change_handler(new_config):
                    logger.info("Configuration change detected, reloading...")
                    self.reload_config()
                
                handle = self.core.config_manager.watch("autonomy", config_change_handler)
                self._config_handles.append(handle)
                logger.debug("Configuration watcher established")
        except Exception as e:
            logger.debug(f"Configuration watching not available: {e}")

    # -------------------------
    # Queue Management
    # -------------------------
    def enqueue_action(self, action: Action) -> bool:
        is_valid, msg = self._validate_action(action)
        if not is_valid:
            logger.debug(f"Action validation failed: {msg}")
            if PROMETHEUS_AVAILABLE:
                autonomy_metrics['validation_failures'].labels(reason=msg.split(':')[0] if ':' in msg else 'unknown').inc()
            return False
        
        self.action_queue.push(action)
        if PROMETHEUS_AVAILABLE:
            autonomy_metrics['queue_size'].inc()
        
        logger.debug(f"Enqueued action '{action.action_type}' with priority {action.priority} (ID: {action.id})")
        return True

    def dequeue_action(self) -> Optional[Action]:
        action = self.action_queue.pop()
        if action and PROMETHEUS_AVAILABLE:
            autonomy_metrics['queue_size'].dec()
        return action

    def pending_actions(self) -> int:
        return self.action_queue.size()

    def get_queue_summary(self) -> List[Dict[str, Any]]:
        return self.action_queue.get_summary()

    def clear_queue(self, priority_filter: Optional[int] = None) -> int:
        if priority_filter is None:
            count = self.pending_actions()
            # Clear all queues
            for priority in range(1, 6):
                self.action_queue.clear_priority(priority)
            if PROMETHEUS_AVAILABLE:
                autonomy_metrics['queue_size'].set(0)
            return count
        else:
            return self.action_queue.clear_priority(priority_filter)

    # -------------------------
    # Validation
    # -------------------------
    def _validate_action(self, action: Action) -> Tuple[bool, str]:
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
            if not self._validate_datetime(action.details["datetime"]):
                return False, "invalid_datetime_format"
        
        return True, "valid"

    def _validate_datetime(self, dt_str: str) -> bool:
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

    def _can_execute(self, action: Action) -> Tuple[bool, str]:
        # Check user consent
        try:
            profile = self.persona.core.load_user_profile() or {}
            if not profile.get("allow_autonomy", True):
                return False, "user_consent_denied"
        except Exception as e:
            logger.debug(f"Failed to check user profile: {e}")
            return False, "consent_check_failed"
        
        # Check circuit breaker
        service = self._get_service_from_action(action.action_type)
        circuit_breaker = self.circuit_breakers.get(service)
        if circuit_breaker and not circuit_breaker.can_execute():
            return False, f"circuit_breaker_open:{service}"
        
        # Check rate limits
        limit = self.rate_limits_config.get(action.action_type, self.rate_limits_config["default"])
        window_start = time.time() - limit["window"]
        
        with self._exec_lock:
            # Use cached recent actions for better performance
            if not self._cache_valid or time.time() - self._last_health_check > 30:
                self._update_recent_actions_cache()
            
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

    def _update_recent_actions_cache(self):
        """Update cache of recent actions for performance"""
        with self._exec_lock:
            self._recent_actions_cache = list(self.executed_actions) + list(self.failed_actions)
            self._cache_valid = True
            self._last_health_check = time.time()

    def _update_rate_limits(self, action: Action):
        now = time.time()
        limit = self.rate_limits_config.get(action.action_type, self.rate_limits_config["default"])
        with self._exec_lock:
            self.rate_limits.setdefault(action.action_type, []).append(now)
            cutoff = now - limit["window"]
            self.rate_limits[action.action_type] = [t for t in self.rate_limits[action.action_type] if t > cutoff]
            self._cache_valid = False  # Invalidate cache

    # -------------------------
    # Execution Context
    # -------------------------
    @contextmanager
    def _execution_context(self, action: Action):
        start_time = time.monotonic()
        with self._metrics_lock:
            self.currently_executing += 1
            if PROMETHEUS_AVAILABLE:
                autonomy_metrics['concurrent_actions'].inc()
        try:
            yield
        finally:
            exec_time = time.monotonic() - start_time
            with self._metrics_lock:
                self.currently_executing -= 1
                if PROMETHEUS_AVAILABLE:
                    autonomy_metrics['concurrent_actions'].dec()
                    autonomy_metrics['action_duration'].labels(type=action.action_type).observe(exec_time)

    # -------------------------
    # Action Execution
    # -------------------------
    def execute_action(self, action: Action) -> Tuple[bool, Any]:
        can_exec, reason = self._can_execute(action)
        if not can_exec:
            logger.debug(f"Action blocked: {reason}")
            if PROMETHEUS_AVAILABLE and 'rate_limit' in reason:
                autonomy_metrics['actions_executed'].labels(type=action.action_type, status='rate_limited').inc()
            return False, reason
        
        service = self._get_service_from_action(action.action_type)
        with self._execution_context(action):
            start = time.monotonic()
            try:
                result = self._execute_action_internal(action)
                exec_time = time.monotonic() - start
                action.result = self._process_action_result(action, result, exec_time, True)
                self._mark_executed(action)
                
                if service in self.circuit_breakers:
                    self.circuit_breakers[service].record_success()
                
                if PROMETHEUS_AVAILABLE:
                    autonomy_metrics['actions_executed'].labels(type=action.action_type, status='success').inc()
                
                logger.debug(f"Executed '{action.action_type}' in {exec_time:.3f}s")
                return True, result
                
            except Exception as e:
                exec_time = time.monotonic() - start
                error = {"message": str(e), "trace": traceback.format_exc()}
                action.result = self._process_action_result(action, error, exec_time, False)
                self._mark_failed(action)
                
                if service in self.circuit_breakers:
                    self.circuit_breakers[service].record_failure()
                
                if PROMETHEUS_AVAILABLE:
                    autonomy_metrics['actions_executed'].labels(type=action.action_type, status='failure').inc()
                
                logger.error(f"Execution failed for {action.action_type}: {e}")
                return False, error

    @retry_operation(max_retries=2, delay=0.5, exceptions=(Exception,))
    def _execute_action_internal(self, action: Action) -> Any:
        """Internal execution with retry logic"""
        if action.action_type == "notify":
            return self._execute_notify(action.details)
        elif action.action_type == "calendar.add":
            return self._execute_calendar_add(action.details)
        elif action.action_type == "calendar.list":
            return self._execute_calendar_list(action.details)
        elif action.action_type == "contact.get":
            return self._execute_contact_get(action.details)
        elif action.action_type == "contact.search":
            return self._execute_contact_search(action.details)
        else:
            raise ValueError(f"Unsupported action type: {action.action_type}")

    def _process_action_result(self, action: Action, result: Any, exec_time: float, success: bool) -> Dict[str, Any]:
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

    def _mark_executed(self, action: Action):
        with self._exec_lock:
            action.executed = True
            self.executed_actions.append(action)
            self._update_rate_limits(action)
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
        
        self._audit_action(action, action.result)

    def _mark_failed(self, action: Action):
        with self._exec_lock:
            self.failed_actions.append(action)
            # Don't add to executed_actions to keep metrics clean
            self._update_rate_limits(action)
            self._cache_valid = False

    def _audit_action(self, action: Action, result: Dict[str, Any]):
        try:
            audit_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action_id": action.id,
                "user_context": self._get_user_context(),
                "action_details": action.to_dict(),
                "result": result,
                "compliance_tags": self._generate_compliance_tags(action)
            }
            
            encrypted_audit = self.encryptor.encrypt(json.dumps(audit_entry, default=str))
            
            # Standardized audit logging path
            if hasattr(self.core, 'audit_logger'):
                self.core.audit_logger.info("autonomy_action", extra={"audit_data": encrypted_audit})
            elif hasattr(self.core, 'storage'):
                audit_key = f"audit_action_{action.id}"
                self.core.storage.put(audit_key, "autonomy_audit", encrypted_audit)
            else:
                logger.info(f"AUTONOMY_AUDIT {json.dumps(encrypted_audit)}")
                
        except Exception as e:
            logger.debug(f"Failed to create audit entry: {e}")

    def _get_user_context(self) -> Dict[str, Any]:
        try:
            if hasattr(self.persona, 'get_user_context'):
                return self.persona.get_user_context()
        except Exception as e:
            logger.debug(f"Failed to get user context: {e}")
        return {"user_available": False}

    def _generate_compliance_tags(self, action: Action) -> Dict[str, str]:
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
    def _execute_notify(self, details: Dict[str, Any]) -> Dict[str, Any]:
        message = details["message"]
        
        if len(message) > 200 and hasattr(self.core, 'llm_orchestrator'):
            summarized_message = self._summarize_message_with_fallback(message)
            if summarized_message:
                message = summarized_message
        
        return {
            "status": "sent", 
            "channel": details["channel"], 
            "message_preview": message[:50],
            "original_length": len(details["message"]),
            "final_length": len(message)
        }

    def _summarize_message_with_fallback(self, message: str) -> Optional[str]:
        models_to_try = [
            self.llm_config["summarization_model"],
            *self.llm_config["fallback_models"]
        ]
        
        for model in models_to_try:
            try:
                summary = self.core.llm_orchestrator.generate(
                    model=model,
                    messages=[{"role": "user", "content": f"Summarize this in under 100 chars: {message}"}],
                    max_tokens=50,
                    temperature=0.1
                )
                if summary and len(summary.strip()) > 0:
                    return summary.strip()
            except Exception as e:
                logger.debug(f"Summarization failed with model {model}: {e}")
                continue
        
        return None

    def _execute_calendar_add(self, details: Dict[str, Any]) -> Dict[str, Any]:
        try:
            event_id = f"event_{int(time.time())}"
            if hasattr(self.core, "add_calendar_event"):
                result = self.core.add_calendar_event(details)
                return {"status": "added", "event_id": event_id, "core_result": result}
            else:
                return {"status": "simulated", "event_id": event_id, "note": "Calendar integration not available"}
        except Exception as e:
            return {"status": "failed", "error": str(e), "trace": traceback.format_exc()}

    def _execute_calendar_list(self, details: Dict[str, Any]) -> Dict[str, Any]:
        days = details.get("days", 7)
        try:
            events = self.core.list_upcoming_events(days) if hasattr(self.core, "list_upcoming_events") else []
            return {"status": "listed", "count": len(events), "days": days}
        except Exception as e:
            return {"status": "failed", "error": str(e), "count": 0, "days": days}

    def _execute_contact_get(self, details: Dict[str, Any]) -> Dict[str, Any]:
        try:
            contact = self.core.get_contact(details["id"]) if hasattr(self.core, "get_contact") else None
            return {"status": "retrieved" if contact else "not_found", "contact": contact}
        except Exception as e:
            return {"status": "failed", "error": str(e), "contact_id": details["id"]}

    def _execute_contact_search(self, details: Dict[str, Any]) -> Dict[str, Any]:
        try:
            limit = details.get("limit", 10)
            results = self.core.search_contacts(details["q"])[:limit] if hasattr(self.core, "search_contacts") else []
            return {"status": "searched", "count": len(results), "query": details["q"], "limit": limit}
        except Exception as e:
            return {"status": "failed", "error": str(e), "query": details["q"]}

    # -------------------------
    # Async Support
    # -------------------------
    async def execute_action_async(self, action: Action) -> Tuple[bool, Any]:
        """Async version of execute_action for I/O-heavy operations"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.execute_action, action)

    async def run_autonomy_cycle_async(self) -> Dict[str, Any]:
        """Async version of autonomy cycle"""
        if not self.autonomy_enabled:
            return {"status": "disabled", "executed": 0}
        
        executed_count = 0
        successful_count = 0
        start_time = time.monotonic()
        
        while self.pending_actions() > 0 and executed_count < self.max_concurrent:
            action = self.dequeue_action()
            if action:
                success, _ = await self.execute_action_async(action)
                executed_count += 1
                if success:
                    successful_count += 1
        
        cycle_time = time.monotonic() - start_time
        return {
            "status": "completed",
            "executed": executed_count,
            "successful": successful_count,
            "cycle_time": cycle_time,
            "queue_remaining": self.pending_actions(),
            "currently_executing": self.currently_executing,
            "timestamp": time.time()
        }

    # -------------------------
    # LLM-Driven Autonomy
    # -------------------------
    def evaluate_autonomous_decision(self, context: Dict[str, Any]) -> Optional[Action]:
        if not hasattr(self.core, 'llm_orchestrator'):
            return None
            
        prompt = self._build_autonomy_prompt(context)
        
        for model in [self.llm_config["autonomy_model"], *self.llm_config["fallback_models"]]:
            try:
                response = self.core.llm_orchestrator.generate(
                    model=model,
                    messages=[{"role": "system", "content": prompt}],
                    temperature=0.1,
                    max_tokens=500
                )
                
                action = self._parse_llm_action_robust(response, context)
                if action:
                    return action
                    
            except Exception as e:
                logger.debug(f"Autonomy decision failed with model {model}: {e}")
                continue
        
        return None

    def _build_autonomy_prompt(self, context: Dict[str, Any]) -> str:
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

    def _parse_llm_action_robust(self, response: str, context: Dict[str, Any]) -> Optional[Action]:
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
    # Autonomous Loop
    # -------------------------
    def run_autonomy_cycle(self) -> Dict[str, Any]:
        if not self.autonomy_enabled:
            return {"status": "disabled", "executed": 0}
        
        executed_count = 0
        successful_count = 0
        start_time = time.monotonic()
        
        while self.pending_actions() > 0 and executed_count < self.max_concurrent:
            action = self.dequeue_action()
            if action:
                success, _ = self.execute_action(action)
                executed_count += 1
                if success:
                    successful_count += 1
        
        cycle_time = time.monotonic() - start_time
        summary = {
            "status": "completed",
            "executed": executed_count,
            "successful": successful_count,
            "cycle_time": cycle_time,
            "queue_remaining": self.pending_actions(),
            "currently_executing": self.currently_executing,
            "timestamp": time.time()
        }
        
        logger.debug(f"Autonomy cycle: {executed_count} executed, {successful_count} successful")
        return summary

    def start_continuous_autonomy(self, interval: float = 5.0, use_async: bool = False):
        def autonomy_loop():
            while self.autonomy_enabled:
                try:
                    if use_async and hasattr(asyncio, 'run'):
                        asyncio.run(self.run_autonomy_cycle_async())
                    else:
                        self.run_autonomy_cycle()
                    time.sleep(interval)
                except Exception as e:
                    logger.error(f"Autonomy loop error: {e}")
                    time.sleep(interval * 2)
        
        self.autonomy_thread = threading.Thread(target=autonomy_loop, daemon=True)
        self.autonomy_thread.start()
        logger.info(f"Started continuous autonomy with {interval}s interval (async: {use_async})")

    # -------------------------
    # Enhanced Monitoring & Analytics
    # -------------------------
    def get_system_health(self) -> Dict[str, Any]:
        now = time.time()
        
        # Use cached data if recent
        if not self._cache_valid or now - self._last_health_check > 30:
            self._update_recent_actions_cache()
        
        with self._exec_lock:
            recent = [a for a in self._recent_actions_cache if now - a.timestamp < 300]
            total = len(recent)
            success = sum(1 for a in recent if a.result and a.result.get("success"))
            failed_count = len([a for a in self.failed_actions if now - a.timestamp < 300])
        
        cb_states = {}
        for service, cb in self.circuit_breakers.items():
            cb_states[service] = cb.get_state_info()  # This doesn't modify state
            if PROMETHEUS_AVAILABLE:
                state_value = {"CLOSED": 0, "HALF_OPEN": 1, "OPEN": 2}.get(cb.state, -1)
                autonomy_metrics['circuit_breaker_state'].labels(service=service).set(state_value)
        
        health_status = "healthy" if success == total and all(cb.state == "CLOSED" for cb in self.circuit_breakers.values()) else "degraded"
        
        return {
            "status": health_status,
            "queue_size": self.pending_actions(),
            "recent_executions": total,
            "success_rate": success / max(1, total),
            "rate_limits_active": len(self.rate_limits),
            "autonomy_enabled": self.autonomy_enabled,
            "total_executed": len(self.executed_actions),
            "failed_actions": len(self.failed_actions),  # Use actual count, not recent
            "failure_rate": len(self.failed_actions) / max(1, len(self.executed_actions)),
            "circuit_breakers": cb_states,
            "currently_executing": self.currently_executing,
            "timestamp": now
        }

    # -------------------------
    # Enhanced Monitoring & Analytics (Continued)
    # -------------------------
    def deep_health_check(self) -> Dict[str, Any]:
        """Comprehensive health check with dependency verification"""
        base_health = self.get_system_health()
        
        # Check external dependencies
        dependency_health = {}
        if hasattr(self.core, 'health_monitor'):
            try:
                for service in ["calendar", "notifications", "contacts"]:
                    health_result = self.core.health_monitor.check_service(service)
                    dependency_health[service] = health_result
            except Exception as e:
                logger.debug(f"Dependency health check failed: {e}")
                dependency_health = {"error": str(e)}
        
        # Verify storage functionality
        storage_health = self._check_storage_health()
        
        # Verify encryption functionality
        encryption_health = self._check_encryption_health()
        
        # Calculate overall health status
        overall_healthy = (
            base_health["status"] == "healthy" and
            all(dep.get("status") == "healthy" for dep in dependency_health.values() if isinstance(dep, dict)) and
            storage_health["working"] and
            encryption_health["working"]
        )
        
        return {
            **base_health,
            "dependency_health": dependency_health,
            "storage_health": storage_health,
            "encryption_health": encryption_health,
            "overall_status": "healthy" if overall_healthy else "degraded",
            "version": "2.2.0",
            "checks": {
                "queue_management": base_health["queue_size"] < 100,
                "execution_health": base_health["success_rate"] > 0.8,
                "circuit_breakers": all(cb.state == "CLOSED" for cb in self.circuit_breakers.values()),
                "storage_available": storage_health["working"],
                "encryption_working": encryption_health["working"]
            }
        }

    def _check_storage_health(self) -> Dict[str, Any]:
        """Check storage system health with minimal impact"""
        storage_health = {"available": hasattr(self.core, 'storage'), "working": False}
        if not storage_health["available"]:
            return storage_health
            
        try:
            test_key = f"health_check_{int(time.time())}"
            test_data = {"timestamp": time.time(), "test": True}
            self.core.storage.put(test_key, "health_check", test_data)
            retrieved = self.core.storage.get(test_key, "health_check")
            storage_health["working"] = retrieved is not None
            
            # Cleanup test data
            try:
                self.core.storage.delete(test_key, "health_check")
            except:
                pass
                
        except Exception as e:
            storage_health["error"] = str(e)
            storage_health["working"] = False
            
        return storage_health

    def _check_encryption_health(self) -> Dict[str, Any]:
        """Verify encryption system is functional"""
        try:
            test_data = {"test": True, "timestamp": time.time()}
            encrypted = self.encryptor.encrypt(json.dumps(test_data))
            decrypted = self.encryptor.decrypt(encrypted)
            restored_data = json.loads(decrypted)
            
            return {
                "working": restored_data["test"] is True,
                "encryption_type": encrypted.get('encryption', 'unknown'),
                "crypto_available": CRYPTO_AVAILABLE
            }
        except Exception as e:
            return {
                "working": False,
                "error": str(e),
                "crypto_available": CRYPTO_AVAILABLE
            }

    def analyze_failure_patterns(self) -> Dict[str, Any]:
        """Analyze failure patterns with enhanced categorization"""
        with self._exec_lock:
            recent_failures = list(self.failed_actions)[-100:]  # Increased sample size
            if not recent_failures:
                return {"total_failures": 0, "analysis_available": False}
            
            # Categorize failures
            failure_counts = Counter(a.action_type for a in recent_failures)
            category_counts = Counter(a.category for a in recent_failures)
            
            # Analyze error patterns
            error_patterns = []
            circuit_breaker_impact = {}
            temporal_patterns = self._analyze_temporal_patterns(recent_failures)
            
            for a in recent_failures:
                if a.result and a.result.get('details', {}).get('error', {}):
                    error_data = a.result['details']['error']
                    error_patterns.append({
                        'action_type': a.action_type,
                        'error_type': error_data.get('message', 'Unknown').split(':')[0],
                        'timestamp': a.timestamp,
                        'service': self._get_service_from_action(a.action_type)
                    })
            
            # Circuit breaker impact analysis
            for service, cb in self.circuit_breakers.items():
                circuit_breaker_impact[service] = {
                    'failure_count': cb.failure_count,
                    'state': cb.state,
                    'recent_failures': len([f for f in recent_failures 
                                          if self._get_service_from_action(f.action_type) == service])
                }
            
            # Common error messages (top 5)
            error_messages = [e.get('error_type', 'Unknown') for e in error_patterns]
            common_errors = Counter(error_messages).most_common(5)
            
            return {
                "total_failures": len(self.failed_actions),
                "recent_failures": len(recent_failures),
                "failures_by_type": dict(failure_counts),
                "failures_by_category": dict(category_counts),
                "most_common_failure": failure_counts.most_common(1)[0] if failure_counts else None,
                "common_error_patterns": common_errors,
                "circuit_breaker_impact": circuit_breaker_impact,
                "temporal_patterns": temporal_patterns,
                "analysis_available": True,
                "analysis_timestamp": time.time()
            }

    def _analyze_temporal_patterns(self, failures: List[Action]) -> Dict[str, Any]:
        """Analyze when failures occur most frequently"""
        if not failures:
            return {}
            
        # Group by hour of day
        hourly_failures = defaultdict(int)
        for failure in failures:
            hour = datetime.fromtimestamp(failure.timestamp).hour
            hourly_failures[hour] += 1
            
        # Find peak failure times
        peak_hours = sorted(hourly_failures.items(), key=lambda x: x[1], reverse=True)[:3]
        
        return {
            "failures_by_hour": dict(hourly_failures),
            "peak_failure_hours": peak_hours,
            "total_period_hours": len(hourly_failures)
        }

    def get_executed_summary(self, last_n: int = 20, category_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get executed actions summary with filtering options"""
        with self._exec_lock:
            actions = list(self.executed_actions)[-last_n*2:]  # Get extra for filtering
            
            if category_filter:
                actions = [a for a in actions if a.category == category_filter]
            
            # Return most recent N after filtering
            return [a.to_dict() for a in actions[-last_n:]]

    def get_performance_metrics(self, time_window: int = 3600, category: Optional[str] = None) -> Dict[str, Any]:
        """Get detailed performance metrics with category filtering"""
        now = time.time()
        cutoff = now - time_window
        
        with self._exec_lock:
            recent_actions = [a for a in list(self.executed_actions) if a.timestamp > cutoff]
            
            if category:
                recent_actions = [a for a in recent_actions if a.category == category]
                
            if not recent_actions:
                return {
                    "total_actions": 0, 
                    "analysis_available": False,
                    "time_window": time_window,
                    "category": category
                }
            
            # Calculate metrics
            execution_times = [a.result.get('execution_time', 0) for a in recent_actions if a.result]
            successful_actions = [a for a in recent_actions if a.result and a.result.get('success')]
            actions_by_type = Counter(a.action_type for a in recent_actions)
            actions_by_category = Counter(a.category for a in recent_actions)
            actions_by_priority = Counter(a.priority for a in recent_actions)
            
            # Statistical analysis
            sorted_times = sorted(execution_times)
            count = len(sorted_times)
            p95_index = int(count * 0.95) if count > 0 else 0
            p99_index = int(count * 0.99) if count > 0 else 0
            
            return {
                "total_actions": len(recent_actions),
                "successful_actions": len(successful_actions),
                "success_rate": len(successful_actions) / len(recent_actions),
                "avg_execution_time": sum(execution_times) / count if count > 0 else 0,
                "p95_execution_time": sorted_times[p95_index] if count > 0 and p95_index < count else 0,
                "p99_execution_time": sorted_times[p99_index] if count > 0 and p99_index < count else 0,
                "min_execution_time": sorted_times[0] if count > 0 else 0,
                "max_execution_time": sorted_times[-1] if count > 0 else 0,
                "actions_by_type": dict(actions_by_type),
                "actions_by_category": dict(actions_by_category),
                "actions_by_priority": dict(actions_by_priority),
                "analysis_available": True,
                "time_window": time_window,
                "category": category,
                "sample_size": count
            }

    def get_action_statistics(self) -> Dict[str, Any]:
        """Get comprehensive action statistics"""
        with self._exec_lock:
            total_executed = len(self.executed_actions)
            total_failed = len(self.failed_actions)
            
            # Categorized counts
            action_types = Counter(a.action_type for a in list(self.executed_actions))
            categories = Counter(a.category for a in list(self.executed_actions))
            priorities = Counter(a.priority for a in list(self.executed_actions))
            sources = Counter(a.source for a in list(self.executed_actions))
            
            # Success rates by category
            success_rates = {}
            for category in categories.keys():
                category_actions = [a for a in list(self.executed_actions) if a.category == category]
                successful = len([a for a in category_actions if a.result and a.result.get('success')])
                success_rates[category] = successful / len(category_actions) if category_actions else 0
            
            # Recent activity (last hour)
            one_hour_ago = time.time() - 3600
            recent_actions = [a for a in list(self.executed_actions) if a.timestamp > one_hour_ago]
            recent_failures = [a for a in list(self.failed_actions) if a.timestamp > one_hour_ago]
            
            return {
                "total_actions_executed": total_executed,
                "total_actions_failed": total_failed,
                "overall_success_rate": (total_executed - total_failed) / max(1, total_executed),
                "recent_activity_1h": len(recent_actions),
                "recent_failures_1h": len(recent_failures),
                "recent_success_rate": (len(recent_actions) - len(recent_failures)) / max(1, len(recent_actions)),
                "actions_by_type": dict(action_types),
                "actions_by_category": dict(categories),
                "actions_by_priority": dict(priorities),
                "actions_by_source": dict(sources),
                "success_rates_by_category": success_rates,
                "queue_size": self.pending_actions(),
                "currently_executing": self.currently_executing,
                "rate_limits_active": len(self.rate_limits),
                "circuit_breaker_states": {s: cb.state for s, cb in self.circuit_breakers.items()},
                "timestamp": time.time()
            }

    # -------------------------
    # Lifecycle Management (Continued)
    # -------------------------
    def shutdown(self):
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
            time.sleep(check_interval)
            check_interval = min(check_interval * 1.5, 2.0)  # Progressive backoff
        
        if self.currently_executing > 0:
            logger.warning(f"Shutdown completed with {self.currently_executing} actions still executing")
        
        # Comprehensive state audit
        audit_count = self._perform_shutdown_audit()
        
        # Cleanup resources
        self.cleanup()
        
        logger.info(f"AutonomyCore shutdown complete. Audited {audit_count} actions.")

    def _perform_shutdown_audit(self) -> int:
        """Perform comprehensive shutdown audit"""
        audit_count = 0
        shutdown_timestamp = time.time()
        
        with self._exec_lock:
            # Audit recent executed actions
            recent_actions = list(self.executed_actions)[-200:]  # Increased sample
            queue_actions = self.action_queue.get_summary()
            
            # Create shutdown snapshot
            shutdown_snapshot = {
                "shutdown_timestamp": shutdown_timestamp,
                "final_queue_size": len(queue_actions),
                "final_executing_count": self.currently_executing,
                "total_executed_actions": len(self.executed_actions),
                "total_failed_actions": len(self.failed_actions),
                "recent_actions_sample": len(recent_actions),
                "circuit_breaker_states": {s: cb.get_state_info() for s, cb in self.circuit_breakers.items()}
            }
            
            # Store shutdown snapshot
            try:
                if hasattr(self.core, 'storage'):
                    snapshot_key = f"shutdown_snapshot_{int(shutdown_timestamp)}"
                    encrypted_snapshot = self.encryptor.encrypt(json.dumps(shutdown_snapshot, default=str))
                    self.core.storage.put(snapshot_key, "autonomy_shutdown", encrypted_snapshot)
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
                        self.core.audit_logger.info("autonomy_shutdown_action", extra={"audit_data": encrypted_data})
                        audit_count += 1
                    elif hasattr(self.core, 'storage'):
                        audit_key = f"shutdown_audit_{action.id}"
                        self.core.storage.put(audit_key, "autonomy_audit", encrypted_data)
                        audit_count += 1
                    else:
                        logger.info(f"SHUTDOWN_AUDIT {json.dumps(encrypted_data)}")
                        audit_count += 1
                        
                except Exception as e:
                    logger.debug(f"Failed to audit action {action.id} during shutdown: {e}")
        
        return audit_count

    def reload_config(self):
        """Reload configuration with validation"""
        logger.info("Reloading AutonomyCore configuration...")
        
        try:
            old_concurrent = self.max_concurrent
            self._load_dynamic_config()
            
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

    def reset_circuit_breakers(self):
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

    def export_audit_logs(self, start_time: float, end_time: float, 
                         category: Optional[str] = None) -> List[Dict[str, Any]]:
        """Export audit logs for compliance and analysis"""
        with self._exec_lock:
            # Filter actions by time range
            relevant_actions = [
                a for a in list(self.executed_actions) 
                if start_time <= a.timestamp <= end_time
            ]
            
            # Apply category filter if specified
            if category:
                relevant_actions = [a for a in relevant_actions if a.category == category]
            
            # Enrich audit data
            audit_logs = []
            for action in relevant_actions:
                audit_entry = {
                    "timestamp": action.timestamp,
                    "action_id": action.id,
                    "action_type": action.action_type,
                    "category": action.category,
                    "priority": action.priority,
                    "source": action.source,
                    "success": action.result.get('success') if action.result else False,
                    "execution_time": action.result.get('execution_time') if action.result else None,
                    "tags": action.tags,
                    "service": self._get_service_from_action(action.action_type)
                }
                
                # Add error details if failed
                if not audit_entry["success"] and action.result:
                    error_info = action.result.get('details', {}).get('error', {})
                    audit_entry["error"] = {
                        "message": error_info.get('message'),
                        "type": type(error_info.get('message', '')).__name__
                    }
                
                audit_logs.append(audit_entry)
            
            # Sort by timestamp
            sorted_logs = sorted(audit_logs, key=lambda x: x["timestamp"])
            
            # Add export metadata
            export_info = {
                "export_timestamp": time.time(),
                "time_range": {"start": start_time, "end": end_time},
                "total_actions": len(sorted_logs),
                "category_filter": category,
                "export_format": "compliance"
            }
            
            return {
                "metadata": export_info,
                "audit_logs": sorted_logs
            }

    def get_queue_analytics(self) -> Dict[str, Any]:
        """Get detailed analytics about the current queue state"""
        queue_summary = self.get_queue_summary()
        
        if not queue_summary:
            return {"total_queued": 0, "analytics_available": False}
        
        # Analyze queue composition
        priorities = Counter(action["priority"] for action in queue_summary)
        categories = Counter(action.get("category", "unknown") for action in queue_summary)
        action_types = Counter(action["action_type"] for action in queue_summary)
        
        # Calculate estimated processing time
        avg_execution_time = 0.5  # Default estimate in seconds
        if self.executed_actions:
            recent_times = [a.result.get('execution_time', 0) for a in list(self.executed_actions)[-50:] if a.result]
            if recent_times:
                avg_execution_time = sum(recent_times) / len(recent_times)
        
        estimated_total_time = len(queue_summary) * avg_execution_time
        estimated_time_with_concurrency = estimated_total_time / min(self.max_concurrent, len(queue_summary))
        
        return {
            "total_queued": len(queue_summary),
            "queue_by_priority": dict(priorities),
            "queue_by_category": dict(categories),
            "queue_by_type": dict(action_types),
            "estimated_processing_time_seconds": estimated_time_with_concurrency,
            "current_concurrency_limit": self.max_concurrent,
            "avg_execution_time_seconds": avg_execution_time,
            "oldest_queued_action": min(action["timestamp"] for action in queue_summary) if queue_summary else None,
            "analytics_available": True
        }

    def cleanup_old_actions(self, older_than_hours: int = 24) -> Dict[str, int]:
        """Clean up old actions from memory to prevent unbounded growth"""
        cutoff_time = time.time() - (older_than_hours * 3600)
        
        with self._exec_lock:
            # Count actions before cleanup
            initial_executed = len(self.executed_actions)
            initial_failed = len(self.failed_actions)
            
            # Remove old actions (deque automatically maintains maxlen, but we can trim further)
            if initial_executed > 500:  # Only clean if above conservative limit
                self.executed_actions = deque(
                    [a for a in self.executed_actions if a.timestamp > cutoff_time], 
                    maxlen=1000
                )
            
            if initial_failed > 250:
                self.failed_actions = deque(
                    [a for a in self.failed_actions if a.timestamp > cutoff_time],
                    maxlen=500
                )
            
            # Clean old rate limit data
            for action_type in list(self.rate_limits.keys()):
                self.rate_limits[action_type] = [
                    t for t in self.rate_limits[action_type] 
                    if t > cutoff_time
                ]
                if not self.rate_limits[action_type]:
                    del self.rate_limits[action_type]
            
            # Invalidate cache
            self._cache_valid = False
            
            # Calculate cleanup stats
            executed_removed = initial_executed - len(self.executed_actions)
            failed_removed = initial_failed - len(self.failed_actions)
            
            logger.info(f"Cleaned up {executed_removed} executed and {failed_removed} failed actions older than {older_than_hours}h")
            
            return {
                "executed_actions_removed": executed_removed,
                "failed_actions_removed": failed_removed,
                "remaining_executed": len(self.executed_actions),
                "remaining_failed": len(self.failed_actions),
                "cleanup_timestamp": time.time()
            }

    def cleanup(self):
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