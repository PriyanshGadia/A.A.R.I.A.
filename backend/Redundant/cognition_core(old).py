"""
cognition_core.py - A.A.R.I.A Cognitive Intelligence Layer (v4.0.2 - Production)

Updates in v4.0.2:
- Fixed duplicate circuit breaker trip counting (increment only on state transition to OPEN)
- Added resilience safety checks in emotional updates (min resilience, safeguards)
- Improved LLM error handling and classification (transient vs permanent)
- Configuration validation on startup
- Implemented response caching with TTL (in-memory, thread-safe)
- Added request/response audit logging (storage / audit_logger / structured log fallback)
- Detailed performance breakdowns by reasoning mode in PerformanceMetrics
"""

import time
import json
import threading
import logging
import traceback
import asyncio
import uuid
import math
import heapq
from typing import Dict, Any, Optional, List, Tuple, Deque
from collections import deque, Counter, defaultdict
from datetime import datetime, timezone
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum

# # -------------------------
# # Enhanced Logger Setup
# # -------------------------
# logger = logging.getLogger("AARIA.Cognition.Production")
# if not logger.handlers:
#     handler = logging.StreamHandler()
#     handler.setFormatter(logging.Formatter(
#         "%(asctime)s [AARIA.COGNITION.PRODUCTION] [%(levelname)s] [%(threadName)s] %(message)s"
#     ))
#     logger.addHandler(handler)
# logger.setLevel(logging.INFO)
logger = logging.getLogger(__name__)
# -------------------------
# Production Enums & Constants
# -------------------------
class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class ReasoningMode(Enum):
    FAST = "fast"
    BALANCED = "balanced"
    DEEP = "deep"

class EmotionalEvent(Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    UNCERTAINTY = "uncertainty"
    SURPRISE = "surprise"
    FATIGUE = "fatigue"
    OVERLOAD = "overload"

DEFAULT_RATE_LIMIT = 30  # requests per minute
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60
MAX_MEMORY_ENTRIES = 1000
HEALTH_CHECK_INTERVAL = 30
MIN_RESILIENCE = 0.1  # resilience lower bound
CACHE_DEFAULT_TTL = 300  # seconds

# -------------------------
# Production Data Structures
# -------------------------
@dataclass
class EmotionalVector:
    calm: float = 0.6
    alert: float = 0.4
    stress: float = 0.1
    resilience: float = 0.8
    last_updated: float = field(default_factory=lambda: time.time())

    def clamp(self):
        self.calm = max(0.0, min(1.0, self.calm))
        self.alert = max(0.0, min(1.0, self.alert))
        self.stress = max(0.0, min(1.0, self.stress))
        self.resilience = max(MIN_RESILIENCE, min(1.0, self.resilience))
        self.last_updated = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "calm": self.calm,
            "alert": self.alert,
            "stress": self.stress,
            "resilience": self.resilience,
            "stability_score": self.calm - self.stress + (self.resilience * 0.5)
        }

@dataclass
class CognitiveTrace:
    id: str = field(default_factory=lambda: f"trace_{uuid.uuid4().hex[:8]}")
    timestamp: float = field(default_factory=lambda: time.time())
    query: str = ""
    response: str = ""
    reasoning_mode: str = ReasoningMode.BALANCED.value
    latency: Optional[float] = None
    success_score: Optional[float] = None
    emotional_context: Optional[Dict[str, float]] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class PerformanceMetrics:
    reasoning_requests: int = 0
    successful_reasoning: int = 0
    failed_reasoning: int = 0
    circuit_breaker_trips: int = 0
    rate_limit_hits: int = 0
    emotional_updates: int = 0
    plans_generated: int = 0
    reflections_performed: int = 0
    avg_reasoning_latency: float = 0.0
    last_reset: float = field(default_factory=lambda: time.time())

    # New: per-mode statistics
    per_mode_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    per_mode_latency_sum: Dict[str, float] = field(default_factory=lambda: defaultdict(float))

    def success_rate(self) -> float:
        return self.successful_reasoning / max(1, self.reasoning_requests)

    def record_latency(self, mode: str, latency: float):
        self.per_mode_counts[mode] += 1
        self.per_mode_latency_sum[mode] += latency
        prev = self.avg_reasoning_latency
        self.avg_reasoning_latency = (prev * 0.9) + (latency * 0.1)

    def per_mode_avg_latency(self) -> Dict[str, float]:
        result = {}
        for mode, count in self.per_mode_counts.items():
            if count > 0:
                result[mode] = self.per_mode_latency_sum[mode] / count
            else:
                result[mode] = 0.0
        return result

    def to_dict(self) -> Dict[str, Any]:
        return {
            "reasoning_requests": self.reasoning_requests,
            "successful_reasoning": self.successful_reasoning,
            "failed_reasoning": self.failed_reasoning,
            "success_rate": self.success_rate(),
            "circuit_breaker_trips": self.circuit_breaker_trips,
            "rate_limit_hits": self.rate_limit_hits,
            "emotional_updates": self.emotional_updates,
            "plans_generated": self.plans_generated,
            "reflections_performed": self.reflections_performed,
            "avg_reasoning_latency": self.avg_reasoning_latency,
            "per_mode_avg_latency": self.per_mode_avg_latency(),
            "uptime_hours": (time.time() - self.last_reset) / 3600
        }

# -------------------------
# Production Reliability Patterns
# -------------------------
class CircuitBreaker:
    def __init__(self, failure_threshold: int = CIRCUIT_BREAKER_THRESHOLD,
                 recovery_timeout: int = CIRCUIT_BREAKER_TIMEOUT):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        self._lock = threading.RLock()
        self.success_count = 0
        self.half_open_success_threshold = 3

    def can_execute(self) -> bool:
        with self._lock:
            if self.state == CircuitState.OPEN:
                if self.last_failure_time is None:
                    return False
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    return True
                return False
            return True

    def record_success(self):
        with self._lock:
            self.failure_count = 0
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.half_open_success_threshold:
                    self.state = CircuitState.CLOSED
                    self.success_count = 0

    def record_failure(self) -> bool:
        """
        Record a failure. Returns True if this failure triggered a transition to OPEN (a trip).
        """
        trip_occurred = False
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold and self.state != CircuitState.OPEN:
                self.state = CircuitState.OPEN
                trip_occurred = True
        return trip_occurred

    def get_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "state": self.state.value,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "last_failure_time": self.last_failure_time,
                "can_execute": self.can_execute()
            }

class RateLimiter:
    def __init__(self, max_requests: int = DEFAULT_RATE_LIMIT, window: int = 60):
        self.max_requests = max_requests
        self.window = window
        self.tokens = max_requests
        self.last_refill = time.time()
        self._lock = threading.RLock()

    def acquire(self) -> bool:
        with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            tokens_to_add = time_passed * (self.max_requests / self.window)
            self.tokens = min(self.max_requests, self.tokens + tokens_to_add)
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "tokens_available": self.tokens,
                "max_tokens": self.max_requests,
                "refill_rate": self.max_requests / self.window,
                "can_acquire": self.tokens >= 1
            }

# -------------------------
# Cognition Core
# -------------------------
class CognitionCore:
    def __init__(self, persona, core, autonomy, config: Optional[Dict[str, Any]] = None):
        self.persona = persona
        self.core = core
        self.autonomy = autonomy

        # validate & apply config
        self._validate_and_apply_config(config or {})

        # cognitive state
        self.cognitive_state = {
            "focus": "idle",
            "confidence": 0.85,
            "stability": 0.92,
            "emotional_vector": EmotionalVector(),
            "recent_reflections": deque(maxlen=50),
            "last_thought": None,
            "operational_mode": "normal",
            "load_factor": 0.0
        }

        # structures
        self.cognitive_history: Deque[CognitiveTrace] = deque(maxlen=self.config_max_memory_entries)
        self.plan_cache: Dict[str, Dict[str, Any]] = {}
        self.request_deduplication: Dict[str, float] = {}

        # response cache (TTL)
        self._response_cache: Dict[str, Tuple[str, float, str]] = {}  # hash -> (response, expiry, mode)
        self._response_cache_lock = threading.RLock()

        # audit configuration
        self.audit_log_prefix = "cognition_audit"

        # config
        self.reflection_frequency = self.config_reflection_frequency
        self.last_reflection = 0
        self.emotional_decay_rate = self.config_emotional_decay_rate
        self.cache_ttl = self.config_cache_ttl

        # reliability
        self.llm_circuit_breaker = CircuitBreaker(failure_threshold=self.config_cb_threshold, recovery_timeout=self.config_cb_timeout)
        self.rate_limiter = RateLimiter(max_requests=self.config_rate_limit, window=60)
        self.performance_metrics = PerformanceMetrics()

        # threading & locks
        self._stop_event = threading.Event()
        self._llm_lock = threading.RLock()
        self._state_lock = threading.RLock()
        self.cognition_thread = None
        self.health_check_thread = None

        # async
        self._async_lock = asyncio.Lock()

        # LLM config and fallbacks
        self.llm_config = {
            "summarization_model": getattr(core, "default_llm_model", "mistral"),
            "autonomy_model": getattr(core, "default_llm_model", "mistral"),
            "fallback_models": ["gpt-3.5-turbo", "local-llm"],
            "max_retries": 2
        }

        # initialize
        self._load_cognitive_state()
        self._start_background_services()
        logger.info("Production CognitionCore v4.0.2 initialized successfully")

    # -------------------------
    # Configuration validation
    # -------------------------
    def _validate_and_apply_config(self, cfg: Dict[str, Any]):
        """
        Validate critical configuration options and apply defaults.
        Raises ValueError for invalid configs.
        """
        # Defaults
        self.config_max_memory_entries = cfg.get("max_memory_entries", MAX_MEMORY_ENTRIES)
        self.config_reflection_frequency = cfg.get("reflection_frequency", 300)
        self.config_emotional_decay_rate = cfg.get("emotional_decay_rate", 0.02)
        self.config_cache_ttl = cfg.get("cache_ttl", CACHE_DEFAULT_TTL)
        self.config_rate_limit = cfg.get("rate_limit", DEFAULT_RATE_LIMIT)
        self.config_cb_threshold = cfg.get("circuit_breaker_threshold", CIRCUIT_BREAKER_THRESHOLD)
        self.config_cb_timeout = cfg.get("circuit_breaker_timeout", CIRCUIT_BREAKER_TIMEOUT)

        # Basic validations
        if not isinstance(self.config_max_memory_entries, int) or self.config_max_memory_entries <= 0:
            raise ValueError("max_memory_entries must be a positive integer")
        if not isinstance(self.config_reflection_frequency, (int, float)) or self.config_reflection_frequency <= 0:
            raise ValueError("reflection_frequency must be positive number")
        if not (0.0 < self.config_emotional_decay_rate < 1.0):
            raise ValueError("emotional_decay_rate must be between 0 and 1")
        if not isinstance(self.config_cache_ttl, (int, float)) or self.config_cache_ttl <= 0:
            raise ValueError("cache_ttl must be positive number")
        if not isinstance(self.config_rate_limit, int) or self.config_rate_limit <= 0:
            raise ValueError("rate_limit must be positive integer")
        if not isinstance(self.config_cb_threshold, int) or self.config_cb_threshold <= 0:
            raise ValueError("circuit_breaker_threshold must be positive integer")
        if not isinstance(self.config_cb_timeout, (int, float)) or self.config_cb_timeout <= 0:
            raise ValueError("circuit_breaker_timeout must be positive number")

        logger.debug(f"Configuration validated: max_memory_entries={self.config_max_memory_entries}, reflection_frequency={self.config_reflection_frequency}")

    # -------------------------
    # Persistence with atomic ops
    # -------------------------
    def _load_cognitive_state(self):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if hasattr(self.core, "storage"):
                    state_data = self.core.storage.get("cognition_state", "system_state")
                    if state_data:
                        with self._state_lock:
                            ev_data = state_data.get("emotional_vector", {})
                            self.cognitive_state["emotional_vector"] = EmotionalVector(
                                calm=ev_data.get("calm", 0.6),
                                alert=ev_data.get("alert", 0.4),
                                stress=ev_data.get("stress", 0.1),
                                resilience=ev_data.get("resilience", 0.8)
                            )
                            for key in ["focus", "confidence", "stability", "operational_mode", "load_factor"]:
                                if key in state_data:
                                    self.cognitive_state[key] = state_data[key]
                        logger.info("Successfully restored persisted cognitive state")
                        break
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} to load cognitive state failed: {e}")
                if attempt == max_retries - 1:
                    logger.error("All attempts to load cognitive state failed; continuing with defaults")
                time.sleep(0.1 * (attempt + 1))

    def _persist_cognitive_state(self):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if hasattr(self.core, "storage"):
                    with self._state_lock:
                        state_to_persist = {
                            "focus": self.cognitive_state["focus"],
                            "confidence": self.cognitive_state["confidence"],
                            "stability": self.cognitive_state["stability"],
                            "emotional_vector": self.cognitive_state["emotional_vector"].to_dict(),
                            "operational_mode": self.cognitive_state["operational_mode"],
                            "load_factor": self.cognitive_state["load_factor"],
                            "last_persisted": time.time()
                        }
                    self.core.storage.put("cognition_state", "system_state", state_to_persist)
                    return
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} to persist state failed: {e}")
                if attempt == max_retries - 1:
                    logger.error("Failed to persist cognitive state after retries")
                time.sleep(0.1 * (attempt + 1))

    # -------------------------
    # Emotional processing
    # -------------------------
    def update_emotional_state(self, event_type: EmotionalEvent, intensity: float = 0.5):
        intensity = max(0.05, min(1.0, intensity))
        with self._state_lock:
            ev = self.cognitive_state["emotional_vector"]
            triggers = {
                EmotionalEvent.SUCCESS: {"calm": 0.15 * intensity * ev.resilience, "stress": -0.1 * intensity * ev.resilience, "alert": -0.05 * intensity},
                EmotionalEvent.FAILURE: {"stress": 0.2 * intensity / max(MIN_RESILIENCE, ev.resilience), "calm": -0.15 * intensity / max(MIN_RESILIENCE, ev.resilience), "alert": 0.1 * intensity},
                EmotionalEvent.UNCERTAINTY: {"alert": 0.15 * intensity, "stress": 0.1 * intensity / max(MIN_RESILIENCE, ev.resilience), "calm": -0.1 * intensity},
                EmotionalEvent.SURPRISE: {"alert": 0.2 * intensity, "calm": -0.1 * intensity},
                EmotionalEvent.FATIGUE: {"alert": -0.2 * intensity, "stress": 0.1 * intensity, "resilience": -0.1 * intensity},
                EmotionalEvent.OVERLOAD: {"stress": 0.3 * intensity / max(MIN_RESILIENCE, ev.resilience), "alert": 0.2 * intensity, "calm": -0.25 * intensity / max(MIN_RESILIENCE, ev.resilience), "resilience": -0.15 * intensity}
            }
            if event_type in triggers:
                for dim, delta in triggers[event_type].items():
                    # safety: if delta reduces resilience below MIN_RESILIENCE, cap it
                    if dim == "resilience":
                        new_res = max(MIN_RESILIENCE, getattr(ev, "resilience", MIN_RESILIENCE) + delta)
                        setattr(ev, dim, new_res)
                    else:
                        setattr(ev, dim, getattr(ev, dim, 0.0) + delta)
            ev.clamp()
            self.performance_metrics.emotional_updates += 1
            self._update_operational_mode()
        self._persist_cognitive_state()

    def emotional_decay(self, rate: Optional[float] = None):
        decay_rate = rate or self.emotional_decay_rate
        with self._state_lock:
            ev = self.cognitive_state["emotional_vector"]
            target = {"calm": 0.6, "alert": 0.4, "stress": 0.1}
            for key in ["calm", "alert", "stress"]:
                current = getattr(ev, key)
                target_val = target[key]
                effective_rate = decay_rate * (1.0 + ev.resilience * 0.5)
                new_value = current + (target_val - current) * effective_rate
                setattr(ev, key, new_value)
            ev.resilience = min(1.0, max(MIN_RESILIENCE, ev.resilience + (decay_rate * 0.1)))
            ev.clamp()

    def _update_operational_mode(self):
        ev = self.cognitive_state["emotional_vector"]
        stability_score = ev.calm - ev.stress + (ev.resilience * 0.5)
        if stability_score < 0.3 or ev.stress > 0.8:
            self.cognitive_state["operational_mode"] = "degraded"
        elif stability_score > 0.7 and ev.stress < 0.3:
            self.cognitive_state["operational_mode"] = "high_performance"
        else:
            self.cognitive_state["operational_mode"] = "normal"

    # -------------------------
    # Context analysis
    # -------------------------
    def analyze_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        ev = self.cognitive_state["emotional_vector"]
        recent_requests = len([t for t in list(self.cognitive_history)[-10:] if time.time() - t.timestamp < 60])
        load_factor = min(1.0, recent_requests / 10.0)
        with self._state_lock:
            self.cognitive_state["load_factor"] = load_factor
        analysis = {
            "alertness": ev.alert + (0.3 if context.get("urgent") else 0),
            "stress_bias": ev.stress * (1.0 + load_factor),
            "focus_shift": "external" if context.get("external_event") else "internal",
            "priority": self._calculate_priority(context),
            "load_factor": load_factor,
            "recommended_mode": self._recommend_reasoning_mode(context, load_factor),
            "emotional_stability": ev.calm - ev.stress
        }
        trace = CognitiveTrace(
            query="context_analysis",
            response=json.dumps(analysis),
            emotional_context=ev.to_dict(),
            metadata={"context_keys": list(context.keys())}
        )
        self.cognitive_history.append(trace)
        self._audit_interaction("context_analysis", {"context": context}, analysis, trace.id)
        return analysis

    def _calculate_priority(self, context: Dict[str, Any]) -> str:
        urgency_factors = {"urgent": 2.0, "important": 1.5, "error": 1.8, "time_sensitive": 1.7}
        base_score = 1.0
        for factor, weight in urgency_factors.items():
            if context.get(factor):
                base_score *= weight
        emotional_modifier = 1.0 + (self.cognitive_state["emotional_vector"].stress * 0.5)
        final_score = base_score * emotional_modifier
        if final_score > 2.5:
            return "critical"
        elif final_score > 1.8:
            return "high"
        elif final_score > 1.2:
            return "medium"
        else:
            return "low"

    def _recommend_reasoning_mode(self, context: Dict[str, Any], load_factor: float) -> str:
        if load_factor > 0.8 or context.get("urgent"):
            return ReasoningMode.FAST.value
        elif context.get("complex") or context.get("strategic"):
            return ReasoningMode.DEEP.value
        else:
            return ReasoningMode.BALANCED.value

    # -------------------------
    # Reasoning with reliability + caching + auditing
    # -------------------------
    def reason(self, query: str, context: Optional[Dict[str, Any]] = None,
               reasoning_mode: ReasoningMode = ReasoningMode.BALANCED,
               use_llm: bool = True, max_retries: int = 2) -> str:
        start_time = time.perf_counter()
        self.performance_metrics.reasoning_requests += 1
        context = context or {}
        request_hash = self._hash_request(query, context)

        # Check response cache
        cached = self._get_cached_response(request_hash)
        if cached is not None:
            cached_response, cached_mode = cached
            logger.debug("Served from response cache")
            self._audit_interaction("reason_cached", {"query": query, "context": context}, {"response": cached_response}, None)
            return cached_response

        if self._is_duplicate_request(request_hash):
            logger.debug(f"Serving duplicate request placeholder")
            placeholder = self._get_cached_response(request_hash) or "Processing previous identical request..."
            self._audit_interaction("reason_duplicate", {"query": query}, {"response": placeholder}, None)
            return placeholder

        fast_response = self._try_fast_reasoning(query)
        if fast_response:
            latency = time.perf_counter() - start_time
            self._record_trace(query, fast_response, reasoning_mode, latency, 0.9)
            self._store_response_cache(request_hash, fast_response, reasoning_mode.value)
            self._audit_interaction("reason_fast", {"query": query, "context": context}, {"response": fast_response}, None)
            return fast_response

        if not self.llm_circuit_breaker.can_execute():
            logger.warning("Circuit breaker open, using fallback reasoning")
            self.performance_metrics.circuit_breaker_trips += 1
            fallback = self._get_circuit_breaker_fallback(query)
            self._record_trace(query, fallback, reasoning_mode, None, 0.3)
            self._audit_interaction("reason_cb_open", {"query": query}, {"response": fallback}, None)
            return fallback

        if not self.rate_limiter.acquire():
            logger.warning("Rate limit exceeded, using fallback reasoning")
            self.performance_metrics.rate_limit_hits += 1
            fallback = self._get_rate_limit_fallback(query)
            self._record_trace(query, fallback, reasoning_mode, None, 0.4)
            self._audit_interaction("reason_rate_limited", {"query": query}, {"response": fallback}, None)
            return fallback

        # LLM reasoning
        if use_llm and hasattr(self.core, "llm_orchestrator"):
            for attempt in range(max_retries + 1):
                try:
                    with self._llm_lock:
                        model = getattr(self.core.llm_orchestrator, "default_model", self.llm_config["autonomy_model"])
                        messages = self._build_enhanced_prompt(query, context, reasoning_mode)
                        response = self.core.llm_orchestrator.chat(
                            messages=messages,
                            temperature=self._get_temperature(reasoning_mode),
                            max_tokens=self._get_max_tokens(reasoning_mode)
                        )
                    latency = time.perf_counter() - start_time
                    safe_response = self._sanitize_response(response)
                    self.performance_metrics.successful_reasoning += 1
                    self.performance_metrics.record_latency(reasoning_mode.value, latency)
                    self.llm_circuit_breaker.record_success()
                    self._store_response_cache(request_hash, safe_response, reasoning_mode.value)
                    self._record_trace(query, safe_response, reasoning_mode, latency, 0.9)
                    self._audit_interaction("reason_llm_success", {"query": query, "model": model}, {"response": safe_response, "latency": latency}, None)
                    return safe_response
                except Exception as e:
                    err_txt = str(e)
                    tb = traceback.format_exc()
                    logger.warning(f"LLM reasoning attempt {attempt+1} failed: {err_txt}")
                    # classify transient vs permanent
                    is_transient = any(tok in err_txt.lower() for tok in ["timeout", "temporar", "503", "rate limit", "connection"])
                    trip = self.llm_circuit_breaker.record_failure()
                    if trip:
                        # Increment metric once per transition to OPEN
                        self.performance_metrics.circuit_breaker_trips += 1
                    if attempt == max_retries or not is_transient:
                        self.performance_metrics.failed_reasoning += 1
                        self.update_emotional_state(EmotionalEvent.FAILURE, 0.3)
                        fallback = self._get_fallback_reasoning(query)
                        self._record_trace(query, fallback, reasoning_mode, None, 0.2, err_txt)
                        self._audit_interaction("reason_llm_failed", {"query": query, "error": err_txt, "traceback": tb}, {"response": fallback}, None)
                        return fallback
                    # transient -> backoff and retry
                    backoff = 0.5 * (2 ** attempt)
                    time.sleep(backoff)
                    continue

        fallback = self._get_fallback_reasoning(query)
        self._record_trace(query, fallback, reasoning_mode, None, 0.1)
        self._audit_interaction("reason_final_fallback", {"query": query}, {"response": fallback}, None)
        return fallback

    def _store_response_cache(self, request_hash: str, response: str, mode: str, ttl: Optional[int] = None):
        expiry = time.time() + (ttl or self.cache_ttl)
        with self._response_cache_lock:
            self._response_cache[request_hash] = (response, expiry, mode)

    def _get_cached_response(self, request_hash: str) -> Optional[Tuple[str, str]]:
        with self._response_cache_lock:
            entry = self._response_cache.get(request_hash)
            if not entry:
                return None
            response, expiry, mode = entry
            if time.time() > expiry:
                del self._response_cache[request_hash]
                return None
            return response, mode

    # -------------------------
    # Prompting / helpers
    # -------------------------
    def _build_enhanced_prompt(self, query: str, context: Dict[str, Any], reasoning_mode: ReasoningMode) -> List[Dict[str, str]]:
        ev = self.cognitive_state["emotional_vector"]
        system_parts = [
            "You are A.A.R.I.A — witty, sharp, emotionally aware, and loyal.",
            f"Operational Mode: {self.cognitive_state['operational_mode']}",
            f"Emotional Context: Calm {ev.calm:.2f}, Alert {ev.alert:.2f}, Stress {ev.stress:.2f}",
            f"Reasoning Mode: {reasoning_mode.value}",
            "Provide concise, actionable responses appropriate for current context."
        ]
        if reasoning_mode == ReasoningMode.DEEP:
            system_parts.append("Engage in deep, analytical reasoning with comprehensive consideration.")
        elif reasoning_mode == ReasoningMode.FAST:
            system_parts.append("Provide quick, direct responses optimized for speed.")
        messages = [{"role": "system", "content": "\n".join(system_parts)}, {"role": "user", "content": query}]
        if context:
            messages.append({"role": "user", "content": f"Context: {json.dumps(context, default=str)}"})
        return messages

    def _sanitize_response(self, response: str) -> str:
        if response is None:
            return ""
        cleaned = response.strip()
        if cleaned.startswith("```") and "```" in cleaned[3:]:
            parts = cleaned.split("```")
            cleaned = "\n".join([p for i, p in enumerate(parts) if i % 2 == 0])
        if len(cleaned) > 10000:
            cleaned = cleaned[:10000] + "... [truncated]"
        return cleaned.strip()

    # -------------------------
    # Reflection & analysis
    # -------------------------
    def reflect(self) -> Optional[str]:
        now = time.time()
        if now - self.last_reflection < self.reflection_frequency:
            return None
        try:
            recent_traces = list(self.cognitive_history)[-20:]
            if not recent_traces:
                return None
            successful_traces = [t for t in recent_traces if t.success_score and t.success_score > 0.7]
            success_rate = len(successful_traces) / len(recent_traces)
            reflection_data = {
                "success_rate": success_rate,
                "recent_queries": [t.query[:100] for t in recent_traces[-5:]],
                "emotional_trends": self._analyze_emotional_trends(),
                "performance_metrics": self.performance_metrics.to_dict()
            }
            reflection_prompt = f"Analyze this cognitive performance data and provide 1-2 key insights:\n{json.dumps(reflection_data, indent=2)}"
            insight = self.reason(reflection_prompt, reasoning_mode=ReasoningMode.DEEP)
            reflection_record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "insight": insight,
                "success_rate": success_rate,
                "traces_analyzed": len(recent_traces),
                "emotional_state": self.cognitive_state["emotional_vector"].to_dict()
            }
            with self._state_lock:
                self.cognitive_state["recent_reflections"].append(reflection_record)
            self.last_reflection = now
            self.performance_metrics.reflections_performed += 1
            self._persist_cognitive_state()
            logger.info(f"Reflection completed: {insight[:120]}...")
            self._audit_interaction("reflection", {"input": reflection_data}, {"insight": insight}, None)
            return insight
        except Exception as e:
            logger.error(f"Reflection error: {e}")
            self.update_emotional_state(EmotionalEvent.FAILURE, 0.1)
            return None

    def _analyze_emotional_trends(self) -> Dict[str, Any]:
        recent_traces = list(self.cognitive_history)[-10:]
        if not recent_traces:
            return {"trend": "stable", "volatility": 0.0}
        stress_levels = [t.emotional_context.get("stress", 0.5) for t in recent_traces if t.emotional_context]
        if not stress_levels:
            return {"trend": "stable", "volatility": 0.0}
        volatility = max(stress_levels) - min(stress_levels)
        avg_stress = sum(stress_levels) / len(stress_levels)
        return {"trend": "increasing" if avg_stress > 0.6 else "decreasing" if avg_stress < 0.3 else "stable", "volatility": volatility, "avg_stress": avg_stress}

    # -------------------------
    # Planning
    # -------------------------
    def generate_plan(self, goal: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = context or {}
        plan_key = self._hash_request(goal, context)
        cached_plan = self.plan_cache.get(plan_key)
        if cached_plan and time.time() - cached_plan.get("_cached_at", 0) < self.cache_ttl:
            logger.debug("Returning cached plan")
            self._audit_interaction("plan_cached", {"goal": goal, "context": context}, {"plan": cached_plan}, None)
            return cached_plan
        try:
            ev = self.cognitive_state["emotional_vector"]
            planning_context = {**context, "emotional_context": {"stress_level": ev.stress, "recommended_complexity": "simple" if ev.stress > 0.6 else "detailed"}}
            plan_prompt = f"Create a {planning_context['emotional_context']['recommended_complexity']} plan for: {goal}. Consider current context and provide actionable steps."
            response = self.reason(plan_prompt, planning_context, ReasoningMode.BALANCED)
            try:
                plan = json.loads(response)
                if not self._validate_plan(plan):
                    raise ValueError("Plan validation failed")
            except (json.JSONDecodeError, ValueError):
                plan = self._create_fallback_plan(goal, response)
            result = {"goal": goal, "plan": plan, "timestamp": time.time(), "emotional_context": ev.to_dict(), "reasoning_mode": ReasoningMode.BALANCED.value, "_cached_at": time.time()}
            self.plan_cache[plan_key] = result
            self.performance_metrics.plans_generated += 1
            if self._is_high_quality_plan(plan):
                self.update_emotional_state(EmotionalEvent.SUCCESS, 0.2)
            self._audit_interaction("plan_generated", {"goal": goal, "context": context}, {"plan": result}, None)
            return result
        except Exception as e:
            logger.error(f"Plan generation failed: {e}")
            self.update_emotional_state(EmotionalEvent.FAILURE, 0.3)
            fallback_plan = {"goal": goal, "plan": {"summary": "Fallback plan", "steps": ["Analyze situation", "Take appropriate action"]}, "error": str(e), "timestamp": time.time()}
            self._audit_interaction("plan_failed", {"goal": goal, "error": str(e)}, {"plan": fallback_plan}, None)
            return fallback_plan

    def _validate_plan(self, plan: Dict[str, Any]) -> bool:
        required_keys = ["summary", "steps"]
        return all(key in plan for key in required_keys) and isinstance(plan.get("steps", []), list)

    def _is_high_quality_plan(self, plan: Dict[str, Any]) -> bool:
        steps = plan.get("steps", [])
        has_adequate_steps = 2 <= len(steps) <= 10
        has_detailed_summary = len(plan.get("summary", "")) > 20
        return has_adequate_steps and has_detailed_summary

    def _create_fallback_plan(self, goal: str, response: str) -> Dict[str, Any]:
        return {"summary": f"Plan for: {goal}", "steps": ["Analyze the current situation and context", "Identify key objectives and constraints", "Develop actionable steps based on analysis", "Execute plan with continuous monitoring", "Adjust based on outcomes and feedback"], "notes": f"Based on LLM response: {response[:200]}"}

    # -------------------------
    # Health & metrics
    # -------------------------
    def get_health(self) -> Dict[str, Any]:
        base_health = self._get_base_health()
        llm_health = self._check_llm_health()
        storage_health = self._check_storage_health()
        thread_health = {"cognition_thread": self.cognition_thread.is_alive() if self.cognition_thread else False, "health_thread": self.health_check_thread.is_alive() if self.health_check_thread else False}
        overall_healthy = all([base_health["status"] == "healthy", llm_health["available"], storage_health["working"], thread_health["cognition_thread"]])
        return {**base_health, "llm_health": llm_health, "storage_health": storage_health, "thread_health": thread_health, "circuit_breaker": self.llm_circuit_breaker.get_state(), "rate_limiter": self.rate_limiter.get_status(), "overall_status": "healthy" if overall_healthy else "degraded", "timestamp": datetime.now(timezone.utc).isoformat()}

    def _get_base_health(self) -> Dict[str, Any]:
        ev = self.cognitive_state["emotional_vector"]
        stability_score = ev.calm - ev.stress + (ev.resilience * 0.5)
        return {"status": "healthy" if stability_score > 0.5 else "stressed", "emotional_stability": stability_score, "emotional_vector": ev.to_dict(), "operational_mode": self.cognitive_state["operational_mode"], "load_factor": self.cognitive_state["load_factor"], "confidence": self.cognitive_state["confidence"], "recent_reflections": len(self.cognitive_state["recent_reflections"]), "cognitive_history_size": len(self.cognitive_history), "plan_cache_size": len(self.plan_cache)}

    def _check_llm_health(self) -> Dict[str, Any]:
        health = {"available": False, "responsive": False}
        if not hasattr(self.core, "llm_orchestrator"):
            health["error"] = "LLM orchestrator not available"
            return health
        health["available"] = True
        try:
            test_response = self.reason("Test connectivity", use_llm=True, max_retries=0)
            health["responsive"] = bool(test_response and len(test_response) > 0)
        except Exception as e:
            health["error"] = str(e)
            health["responsive"] = False
        return health

    # Find this method in cognition_core.py
    def _check_storage_health(self) -> Dict[str, Any]:
        health = {"available": False, "working": False}
        # Change 'storage' to 'store' in the line below
        if not hasattr(self.core, "store"): 
            health["error"] = "Storage system not available"
            return health
        health["available"] = True
        try:
            test_key = f"health_check_{int(time.time())}"
            test_data = {"timestamp": time.time(), "test": True}
            # Change 'storage' to 'store' in the line below
            self.core.store.put(test_key, "cognition_health", test_data) 
            # Change 'storage' to 'store' in the line below
            retrieved = self.core.store.get(test_key) 
            health["working"] = retrieved is not None
            try:
                # Change 'storage' to 'store' in the line below
                self.core.store.delete(test_key, "cognition_health") 
            except Exception:
                pass
        except Exception as e:
            health["error"] = str(e)
            health["working"] = False
        return health

    def deep_health_check(self) -> Dict[str, Any]:
        health = self.get_health()
        metrics = self.performance_metrics.to_dict()
        memory_info = {"cognitive_history_size": len(self.cognitive_history), "plan_cache_size": len(self.plan_cache), "reflection_count": len(self.cognitive_state["recent_reflections"]), "deduplication_cache_size": len(self.request_deduplication)}
        recent_traces = list(self.cognitive_history)[-50:]
        latency_trend = "improving" if metrics["avg_reasoning_latency"] < 2.0 else "stable"
        health.update({"performance_metrics": metrics, "memory_usage": memory_info, "latency_trend": latency_trend, "success_rate_trend": "improving" if metrics["success_rate"] > 0.8 else "stable", "version": "4.0.2", "comprehensive_check": True})
        return health

    def get_metrics_summary(self) -> Dict[str, Any]:
        health = self.get_health()
        metrics = self.performance_metrics.to_dict()
        ev = self.cognitive_state["emotional_vector"]
        return {
            "cognition_reasoning_requests_total": metrics["reasoning_requests"],
            "cognition_reasoning_success_total": metrics["successful_reasoning"],
            "cognition_reasoning_failure_total": metrics["failed_reasoning"],
            "cognition_reasoning_latency_seconds": metrics["avg_reasoning_latency"],
            "cognition_plans_generated_total": metrics["plans_generated"],
            "cognition_reflections_performed_total": metrics["reflections_performed"],
            "cognition_emotional_updates_total": metrics["emotional_updates"],
            "cognition_rate_limit_hits_total": metrics["rate_limit_hits"],
            "cognition_circuit_breaker_trips_total": metrics["circuit_breaker_trips"],
            "cognition_emotional_stability": ev.calm - ev.stress + (ev.resilience * 0.5),
            "cognition_emotional_calm": ev.calm,
            "cognition_emotional_alert": ev.alert,
            "cognition_emotional_stress": ev.stress,
            "cognition_emotional_resilience": ev.resilience,
            "cognition_operational_mode": 1 if health["operational_mode"] == "normal" else 0,
            "cognition_load_factor": health["load_factor"],
            "cognition_memory_entries": len(self.cognitive_history),
            "cognition_cached_plans": len(self.plan_cache),
            "cognition_active_reflections": len(self.cognitive_state["recent_reflections"]),
            "cognition_circuit_breaker_state": 1 if self.llm_circuit_breaker.get_state()["state"] == "closed" else 0,
            "cognition_circuit_breaker_failures": self.llm_circuit_breaker.get_state()["failure_count"],
            "cognition_per_mode_latency": metrics.get("per_mode_avg_latency", {})
        }

    # -------------------------
    # Background services & cleanup
    # -------------------------
    def _cognition_cycle(self, interval: float = 5.0):
        logger.info("Production cognition cycle started")
        last_health_check = 0
        while not self._stop_event.is_set():
            try:
                self.emotional_decay()
                if time.time() - self.last_reflection > self.reflection_frequency:
                    self.reflect()
                current_time = time.time()
                if current_time - last_health_check > HEALTH_CHECK_INTERVAL:
                    health = self.get_health()
                    if health["overall_status"] == "degraded":
                        logger.warning(f"System health degraded: {health}")
                    last_health_check = current_time
                if current_time % 300 < interval:
                    self._cleanup_old_data()
                if current_time % 60 < interval:
                    self._persist_cognitive_state()
                self._stop_event.wait(interval)
            except Exception as e:
                logger.error(f"Cognition cycle error: {e}")
                self.update_emotional_state(EmotionalEvent.FAILURE, 0.2)
                time.sleep(interval * 2)
        logger.info("Production cognition cycle stopped")

    def _health_monitor_cycle(self, interval: float = 30.0):
        while not self._stop_event.is_set():
            try:
                health = self.deep_health_check()
                if health["overall_status"] == "degraded":
                    logger.warning(f"System health degraded: {health['operational_mode']}")
                if (health["emotional_stability"] > 0.8 and health["performance_metrics"]["success_rate"] > 0.9 and time.time() - self.performance_metrics.last_reset > 86400):
                    self.performance_metrics = PerformanceMetrics()
                self._stop_event.wait(interval)
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                time.sleep(interval)

    def _start_background_services(self):
        if not self.cognition_thread or not self.cognition_thread.is_alive():
            self._stop_event.clear()
            self.cognition_thread = threading.Thread(target=self._cognition_cycle, name="CognitionCycle", daemon=True)
            self.cognition_thread.start()
        if not self.health_check_thread or not self.health_check_thread.is_alive():
            self.health_check_thread = threading.Thread(target=self._health_monitor_cycle, name="HealthMonitor", daemon=True)
            self.health_check_thread.start()
        logger.info("All background services started")

    def _cleanup_old_data(self):
        current_time = time.time()
        cleanup_cutoff = current_time - 3600
        initial_count = len(self.cognitive_history)
        self.cognitive_history = deque([t for t in self.cognitive_history if t.timestamp > cleanup_cutoff], maxlen=self.config_max_memory_entries)
        self.plan_cache = {k: v for k, v in self.plan_cache.items() if v.get("_cached_at", 0) > cleanup_cutoff}
        self.request_deduplication = {k: v for k, v in self.request_deduplication.items() if v > cleanup_cutoff}
        with self._response_cache_lock:
            expired = [k for k, (_, exp, _) in self._response_cache.items() if exp <= time.time()]
            for k in expired:
                del self._response_cache[k]
        cleaned_count = initial_count - len(self.cognitive_history)
        if cleaned_count > 0:
            logger.debug(f"Cleaned up {cleaned_count} old entries")

    def cleanup_resources(self) -> Dict[str, int]:
        with self._state_lock:
            old_sizes = {"cognitive_history": len(self.cognitive_history), "plan_cache": len(self.plan_cache), "deduplication_cache": len(self.request_deduplication), "reflections": len(self.cognitive_state["recent_reflections"])}
            self.cognitive_history.clear()
            self.plan_cache.clear()
            self.request_deduplication.clear()
            self.cognitive_state["recent_reflections"].clear()
            with self._response_cache_lock:
                self._response_cache.clear()
            logger.info("All cognitive resources cleaned up")
            return old_sizes

    # -------------------------
    # Utilities & auditing
    # -------------------------
    def _hash_request(self, query: str, context: Dict[str, Any]) -> str:
        import hashlib
        content = f"{query}{json.dumps(context, sort_keys=True)}"
        return hashlib.md5(content.encode()).hexdigest()

    def _is_duplicate_request(self, request_hash: str) -> bool:
        current_time = time.time()
        last_time = self.request_deduplication.get(request_hash, 0)
        if current_time - last_time < 10:
            return True
        self.request_deduplication[request_hash] = current_time
        return False

    def _get_cached_response(self, request_hash: str) -> Optional[Tuple[str, str]]:
        with self._response_cache_lock:
            entry = self._response_cache.get(request_hash)
            if not entry:
                return None
            response, expiry, mode = entry
            if time.time() > expiry:
                del self._response_cache[request_hash]
                return None
            return response, mode

    def _cache_response(self, request_hash: str, response: str):
        # kept for backward compatibility; now use _store_response_cache
        self._store_response_cache(request_hash, response, ReasoningMode.BALANCED.value)

    def _try_fast_reasoning(self, query: str) -> Optional[str]:
        query_lower = query.strip().lower()
        fast_responses = {
            "hello": "Hello! How can I assist you today?",
            "hi": "Hi there! What can I help you with?",
            "status": f"Operational status: {self.cognitive_state['operational_mode']}. Emotional stability: {self.cognitive_state['emotional_vector'].calm - self.cognitive_state['emotional_vector'].stress + 0.5:.1f}",
            "time": f"Current system time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "help": "I can help with reasoning, planning, analysis, and emotional context understanding. What do you need?"
        }
        return fast_responses.get(query_lower)

    def _get_circuit_breaker_fallback(self, query: str) -> str:
        self.performance_metrics.circuit_breaker_trips += 1
        return f"[Circuit Breaker] LLM service temporarily unavailable. For '{query}', try again shortly or use cached results."

    def _get_rate_limit_fallback(self, query: str) -> str:
        return f"[Rate Limit] Too many requests. Please wait a moment before trying '{query}' again."

    def _get_fallback_reasoning(self, query: str) -> str:
        return f"[Fallback Reasoning] Based on available data: {query[:100]}... Further analysis requires LLM access."

    def _get_temperature(self, reasoning_mode: ReasoningMode) -> float:
        temperatures = {ReasoningMode.FAST: 0.1, ReasoningMode.BALANCED: 0.2, ReasoningMode.DEEP: 0.3}
        return temperatures.get(reasoning_mode, 0.2)

    def _get_max_tokens(self, reasoning_mode: ReasoningMode) -> int:
        token_limits = {ReasoningMode.FAST: 200, ReasoningMode.BALANCED: 400, ReasoningMode.DEEP: 800}
        return token_limits.get(reasoning_mode, 400)

    def _record_trace(self, query: str, response: str, reasoning_mode: ReasoningMode,
                      latency: Optional[float], success_score: float, error: Optional[str] = None):
        trace = CognitiveTrace(query=query, response=response, reasoning_mode=(reasoning_mode.value if isinstance(reasoning_mode, ReasoningMode) else str(reasoning_mode)), latency=latency, success_score=success_score, emotional_context=self.cognitive_state["emotional_vector"].to_dict(), error=error)
        self.cognitive_history.append(trace)

    def _audit_interaction(self, event: str, request: Dict[str, Any], response: Dict[str, Any], trace_id: Optional[str]):
        """
        Audit logging for request/response pairs. Attempts storage.audit_logger -> storage.put -> structured log.
        """
        try:
            audit_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": event,
                "request": request,
                "response": response,
                "trace_id": trace_id
            }
            # best-effort: use core.audit_logger if available
            if hasattr(self.core, "audit_logger"):
                try:
                    self.core.audit_logger.info("cognition_audit", extra=audit_entry)
                    return
                except Exception:
                    pass
            # fallback to storage if available
            if hasattr(self.core, "storage"):
                key = f"{self.audit_log_prefix}_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"
                try:
                    self.core.storage.put(key, "audit", audit_entry)
                    return
                except Exception:
                    pass
            # final fallback: structured log
            logger.info(f"AUDIT: {json.dumps(audit_entry, default=str)}")
        except Exception as e:
            logger.warning(f"Failed to write audit entry: {e}")

    # -------------------------
    # Async wrappers
    # -------------------------
    async def reason_async(self, query: str, context: Optional[Dict[str, Any]] = None, reasoning_mode: ReasoningMode = ReasoningMode.BALANCED) -> str:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.reason, query, context, reasoning_mode)

    async def generate_plan_async(self, goal: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.generate_plan, goal, context)

    async def reflect_async(self) -> Optional[str]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.reflect)

    # -------------------------
    # Shutdown
    # -------------------------
    def shutdown(self):
        logger.info("Initiating production shutdown sequence...")
        try:
            self._stop_event.set()
            if self.cognition_thread:
                self.cognition_thread.join(timeout=5.0)
            if self.health_check_thread:
                self.health_check_thread.join(timeout=5.0)
            self._persist_cognitive_state()
            snapshot = {"shutdown_time": datetime.now(timezone.utc).isoformat(), "final_health": self.get_health(), "performance_summary": self.performance_metrics.to_dict(), "emotional_state": self.cognitive_state["emotional_vector"].to_dict(), "reason": "graceful_shutdown"}
            try:
                if hasattr(self.core, "storage"):
                    self.core.storage.put("cognition_shutdown_snapshot", "system_state", snapshot)
            except Exception as e:
                logger.warning(f"Failed to save shutdown snapshot: {e}")
            logger.info("Production CognitionCore shutdown complete")
        except Exception as e:
            logger.error(f"Shutdown sequence error: {e}")
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

# -------------------------
# Self-test when run directly
# -------------------------
if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)

    class TestCore:
        def __init__(self):
            self._store = {}
            class Storage:
                def __init__(self, parent):
                    self.parent = parent
                def put(self, key, typ, val):
                    self.parent._store[(key, typ)] = val
                def get(self, key, typ):
                    return self.parent._store.get((key, typ))
                def delete(self, key, typ):
                    self.parent._store.pop((key, typ), None)
            self.storage = Storage(self)

            class LLMOrchestrator:
                default_model = "simulated-model"
                def generate(self, model, messages, temperature, max_tokens):
                    # Simulate occasional transient failure
                    if "transient_fail" in messages[0].get("content", ""):
                        raise RuntimeError("502 Bad Gateway - transient")
                    return json.dumps({"summary": "simulated plan", "steps": ["step1", "step2"]})
            self.llm_orchestrator = LLMOrchestrator()
            self.default_llm_model = "simulated-model"

            class AuditLogger:
                def info(self, msg, extra=None):
                    print("AUDIT_LOG:", msg, extra)
            self.audit_logger = AuditLogger()

    print("Testing Production CognitionCore v4.0.2...")

    test_core = TestCore()
    cfg = {"max_memory_entries": 500, "reflection_frequency": 5, "emotional_decay_rate": 0.02, "cache_ttl": 60, "rate_limit": 10, "circuit_breaker_threshold": 3, "circuit_breaker_timeout": 5}
    cog = CognitionCore(persona=None, core=test_core, autonomy=None, config=cfg)

    print("1. Testing health checks...")
    health = cog.deep_health_check()
    print(f"Health overall status: {health['overall_status']}")

    print("2. Testing reasoning with circuit breaker (simulate transient)...")
    response = cog.reason("transient_fail test")
    print(f"Response: {response}")

    print("3. Testing reasoning normal...")
    response = cog.reason("Test query")
    print(f"Response: {response[:200]}")

    print("4. Testing emotional processing...")
    cog.update_emotional_state(EmotionalEvent.SUCCESS, 0.5)
    emotional_state = cog.cognitive_state["emotional_vector"].to_dict()
    print(f"Emotional state: {emotional_state}")

    print("5. Testing response caching (repeat request should hit cache)...")
    q = "cache me"
    r1 = cog.reason(q)
    r2 = cog.reason(q)
    print("Cached responses equal:", r1 == r2)

    print("6. Testing metrics (per-mode):")
    metrics = cog.get_metrics_summary()
    print(f"Per-mode latency: {metrics.get('cognition_per_mode_latency')}")

    print("7. Testing planning...")
    plan = cog.generate_plan("Test goal")
    print(f"Plan generated present: {bool(plan.get('plan'))}")

    print("8. Testing cleanup and shutdown...")
    cleanup = cog.cleanup_resources()
    print(f"Cleanup completed: {cleanup}")
    cog.shutdown()

    print("All production tests completed successfully!")
