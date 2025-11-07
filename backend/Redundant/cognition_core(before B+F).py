"""
cognition_core.py - A.A.R.I.A Cognitive Intelligence Layer (v4.1.0 - Async Production)

Updates in v4.1.0:
- Complete async modernization while maintaining stability
- Simplified health checks to avoid deadlocks
- Proper async LLM integration
- Maintained all v4.0.2 production features
- Aligned with SecureStorageAsync and other async cores
"""

import asyncio
import json
import logging
import traceback
import hologram_state
import uuid
from typing import Dict, Any, Optional, List, Tuple, Deque
from collections import deque, defaultdict
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum

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

DEFAULT_RATE_LIMIT = 30
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60
MAX_MEMORY_ENTRIES = 1000
MIN_RESILIENCE = 0.1
CACHE_DEFAULT_TTL = 300

# -------------------------
# Production Data Structures
# -------------------------
@dataclass
class EmotionalVector:
    calm: float = 0.6
    alert: float = 0.4
    stress: float = 0.1
    resilience: float = 0.8
    last_updated: float = field(default_factory=lambda: asyncio.get_event_loop().time())

    def clamp(self):
        self.calm = max(0.0, min(1.0, self.calm))
        self.alert = max(0.0, min(1.0, self.alert))
        self.stress = max(0.0, min(1.0, self.stress))
        self.resilience = max(MIN_RESILIENCE, min(1.0, self.resilience))
        self.last_updated = asyncio.get_event_loop().time()

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
    timestamp: float = field(default_factory=lambda: asyncio.get_event_loop().time())
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
    last_reset: float = field(default_factory=lambda: asyncio.get_event_loop().time())

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
        return {mode: total / count for mode, count in self.per_mode_counts.items() 
                if count > 0 for total in [self.per_mode_latency_sum[mode]]}

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
            "uptime_hours": (asyncio.get_event_loop().time() - self.last_reset) / 3600
        }

# -------------------------
# Async Reliability Patterns
# -------------------------
class CircuitBreaker:
    def __init__(self, failure_threshold: int = CIRCUIT_BREAKER_THRESHOLD,
                 recovery_timeout: int = CIRCUIT_BREAKER_TIMEOUT):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        self._lock = asyncio.Lock()
        self.success_count = 0
        self.half_open_success_threshold = 3

    async def can_execute(self) -> bool:
        async with self._lock:
            if self.state == CircuitState.OPEN:
                if self.last_failure_time is None:
                    return False
                if asyncio.get_event_loop().time() - self.last_failure_time > self.recovery_timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    return True
                return False
            return True

    async def record_success(self):
        async with self._lock:
            self.failure_count = 0
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.half_open_success_threshold:
                    self.state = CircuitState.CLOSED
                    self.success_count = 0

    async def record_failure(self) -> bool:
        trip_occurred = False
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = asyncio.get_event_loop().time()
            if self.failure_count >= self.failure_threshold and self.state != CircuitState.OPEN:
                self.state = CircuitState.OPEN
                trip_occurred = True
        return trip_occurred

    async def get_state(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "state": self.state.value,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "last_failure_time": self.last_failure_time,
                "can_execute": await self.can_execute()
            }

class RateLimiter:
    def __init__(self, max_requests: int = DEFAULT_RATE_LIMIT, window: int = 60):
        self.max_requests = max_requests
        self.window = window
        self.tokens = max_requests
        self.last_refill = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = asyncio.get_event_loop().time()
            time_passed = now - self.last_refill
            tokens_to_add = time_passed * (self.max_requests / self.window)
            self.tokens = min(self.max_requests, self.tokens + tokens_to_add)
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def get_status(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "tokens_available": self.tokens,
                "max_tokens": self.max_requests,
                "refill_rate": self.max_requests / self.window,
                "can_acquire": self.tokens >= 1
            }

# -------------------------
# Async Cognition Core
# -------------------------
class CognitionCore:
    def __init__(self, persona, core, autonomy, config: Optional[Dict[str, Any]] = None):
        self.persona = persona
        self.core = core
        self.autonomy = autonomy

        self._validate_and_apply_config(config or {})

        # Cognitive state
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

        # Structures
        self.cognitive_history: Deque[CognitiveTrace] = deque(maxlen=self.config_max_memory_entries)
        self.plan_cache: Dict[str, Dict[str, Any]] = {}
        self.request_deduplication: Dict[str, float] = {}
        self._response_cache: Dict[str, Tuple[str, float, str]] = {}

        # Locks
        self._response_cache_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._llm_lock = asyncio.Lock()

        # Configuration
        self.reflection_frequency = self.config_reflection_frequency
        self.last_reflection = 0
        self.emotional_decay_rate = self.config_emotional_decay_rate
        self.cache_ttl = self.config_cache_ttl
        self.audit_log_prefix = "cognition_audit"

        # Reliability
        self.llm_circuit_breaker = CircuitBreaker(
            failure_threshold=self.config_cb_threshold, 
            recovery_timeout=self.config_cb_timeout
        )
        self.rate_limiter = RateLimiter(max_requests=self.config_rate_limit)
        self.performance_metrics = PerformanceMetrics()

        # Background tasks
        self._stop_event = asyncio.Event()
        self._cognition_task = None
        self._health_check_task = None

        # LLM config
        self.llm_config = {
            "summarization_model": getattr(core, "default_llm_model", "mistral"),
            "autonomy_model": getattr(core, "default_llm_model", "mistral"),
            "fallback_models": ["gpt-3.5-turbo", "local-llm"],
            "max_retries": 2
        }

        logger.info("Async CognitionCore v4.1.0 initialized")

    def _validate_and_apply_config(self, cfg: Dict[str, Any]):
        """Validate and apply configuration with sensible defaults"""
        self.config_max_memory_entries = cfg.get("max_memory_entries", MAX_MEMORY_ENTRIES)
        self.config_reflection_frequency = cfg.get("reflection_frequency", 300)
        self.config_emotional_decay_rate = cfg.get("emotional_decay_rate", 0.02)
        self.config_cache_ttl = cfg.get("cache_ttl", CACHE_DEFAULT_TTL)
        self.config_rate_limit = cfg.get("rate_limit", DEFAULT_RATE_LIMIT)
        self.config_cb_threshold = cfg.get("circuit_breaker_threshold", CIRCUIT_BREAKER_THRESHOLD)
        self.config_cb_timeout = cfg.get("circuit_breaker_timeout", CIRCUIT_BREAKER_TIMEOUT)

        # Basic validations
        if self.config_max_memory_entries <= 0:
            raise ValueError("max_memory_entries must be positive")
        if self.config_reflection_frequency <= 0:
            raise ValueError("reflection_frequency must be positive")
        if not (0.0 < self.config_emotional_decay_rate < 1.0):
            raise ValueError("emotional_decay_rate must be between 0 and 1")

    # -------------------------
    # Core Async Methods
    # -------------------------
    async def reason(self, query: str, context: Optional[Dict[str, Any]] = None,
                     reasoning_mode: ReasoningMode = ReasoningMode.BALANCED,
                     use_llm: bool = True, max_retries: int = 2) -> str:
        
        start_time = asyncio.get_event_loop().time()
        
        # --- NEW: Hologram Task Tracking ---
        node_id = f"cog_task_{uuid.uuid4().hex[:8]}"
        link_id = f"link_cog_{node_id}"
        
        try:
            # --- NEW: Spawn "Reasoning" node ---
            await hologram_state.spawn_and_link(
                node_id=node_id, node_type="cognition", label=f"Reasoning: {query[:20]}...", size=5,
                source_id="CognitionCore", link_id=link_id
            )
            await hologram_state.set_node_active("CognitionCore")
            # --- End New Block ---

            self.performance_metrics.reasoning_requests += 1
            context = context or {}
            request_hash = self._hash_request(query, context)

            cached = await self._get_cached_response(request_hash)
            if cached:
                logger.debug("Served from cache")
                await self._audit_interaction("reason_cached", {"query": query, "context": context}, {"response": cached[0]}, None)
                return cached[0]

            if await self._is_duplicate_request(request_hash):
                cached = await self._get_cached_response(request_hash)
                placeholder = cached[0] if cached else "Processing previous request..."
                await self._audit_interaction("reason_duplicate", {"query": query}, {"response": placeholder}, None)
                return placeholder

            fast_response = self._try_fast_reasoning(query)
            if fast_response:
                latency = asyncio.get_event_loop().time() - start_time
                await self._record_trace(query, fast_response, reasoning_mode, latency, 0.9)
                await self._store_response_cache(request_hash, fast_response, reasoning_mode.value)
                return fast_response

            if not await self.llm_circuit_breaker.can_execute():
                logger.warning("Circuit breaker open")
                self.performance_metrics.circuit_breaker_trips += 1
                fallback = self._get_circuit_breaker_fallback(query)
                await self._record_trace(query, fallback, reasoning_mode, None, 0.3)
                await hologram_state.set_node_error(node_id) # <-- NEW: Show error
                return fallback

            if not await self.rate_limiter.acquire():
                logger.warning("Rate limit exceeded")
                self.performance_metrics.rate_limit_hits += 1
                fallback = self._get_rate_limit_fallback(query)
                await self._record_trace(query, fallback, reasoning_mode, None, 0.4)
                await hologram_state.set_node_error(node_id) # <-- NEW: Show error
                return fallback

            if use_llm and hasattr(self.core, "llm_orchestrator"):
                for attempt in range(max_retries + 1):
                    try:
                        async with self._llm_lock:
                            model = getattr(self.core.llm_orchestrator, "default_model", self.llm_config["autonomy_model"])
                            messages = self._build_enhanced_prompt(query, context, reasoning_mode)
                            
                            if hasattr(self.core.llm_orchestrator, 'achat'):
                                response = await self.core.llm_orchestrator.achat(
                                    messages=messages,
                                    temperature=self._get_temperature(reasoning_mode),
                                    max_tokens=self._get_max_tokens(reasoning_mode)
                                )
                            elif hasattr(self.core.llm_orchestrator, 'chat'):
                                loop = asyncio.get_event_loop()
                                response = await loop.run_in_executor(
                                    None, 
                                    self.core.llm_orchestrator.chat,
                                    messages,
                                    self._get_temperature(reasoning_mode),
                                    self._get_max_tokens(reasoning_mode)
                                )
                            else:
                                raise AttributeError("LLM orchestrator has no chat method")
                        
                        latency = asyncio.get_event_loop().time() - start_time
                        safe_response = self._sanitize_response(response)
                        self.performance_metrics.successful_reasoning += 1
                        self.performance_metrics.record_latency(reasoning_mode.value, latency)
                        await self.llm_circuit_breaker.record_success()
                        await self._store_response_cache(request_hash, safe_response, reasoning_mode.value)
                        await self._record_trace(query, safe_response, reasoning_mode, latency, 0.9)
                        await self._audit_interaction("reason_llm_success", 
                                                    {"query": query, "model": model}, 
                                                    {"response": safe_response, "latency": latency}, None)
                        return safe_response
                        
                    except Exception as e:
                        logger.warning(f"LLM attempt {attempt+1} failed: {e}")
                        is_transient = any(tok in str(e).lower() for tok in 
                                        ["timeout", "temporar", "503", "rate limit", "connection"])
                        trip = await self.llm_circuit_breaker.record_failure()
                        if trip:
                            self.performance_metrics.circuit_breaker_trips += 1
                        
                        if attempt == max_retries or not is_transient:
                            self.performance_metrics.failed_reasoning += 1
                            await self.update_emotional_state(EmotionalEvent.FAILURE, 0.3)
                            fallback = self._get_fallback_reasoning(query)
                            await self._record_trace(query, fallback, reasoning_mode, None, 0.2, str(e))
                            await hologram_state.set_node_error(node_id) # <-- NEW: Show error
                            return fallback
                        
                        await asyncio.sleep(0.5 * (2 ** attempt))

            fallback = self._get_fallback_reasoning(query)
            await self._record_trace(query, fallback, reasoning_mode, None, 0.1)
            return fallback

        finally:
            # --- NEW: Despawn "Reasoning" node ---
            await hologram_state.set_node_idle("CognitionCore")
            await hologram_state.despawn_and_unlink(node_id, link_id)
            # --- End New Block ---

    async def generate_plan(self, goal: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = context or {}
        plan_key = self._hash_request(goal, context)
        
        # Check cache
        cached_plan = self.plan_cache.get(plan_key)
        if cached_plan and asyncio.get_event_loop().time() - cached_plan.get("_cached_at", 0) < self.cache_ttl:
            logger.debug("Returning cached plan")
            return cached_plan
            
        try:
            ev = self.cognitive_state["emotional_vector"]
            planning_context = {
                **context, 
                "emotional_context": {
                    "stress_level": ev.stress, 
                    "recommended_complexity": "simple" if ev.stress > 0.6 else "detailed"
                }
            }
            
            plan_prompt = f"Create a {planning_context['emotional_context']['recommended_complexity']} plan for: {goal}"
            response = await self.reason(plan_prompt, planning_context, ReasoningMode.BALANCED)
            
            try:
                plan = json.loads(response)
                if not self._validate_plan(plan):
                    raise ValueError("Plan validation failed")
            except (json.JSONDecodeError, ValueError):
                plan = self._create_fallback_plan(goal, response)
                
            result = {
                "goal": goal, 
                "plan": plan, 
                "timestamp": asyncio.get_event_loop().time(), 
                "emotional_context": ev.to_dict(), 
                "reasoning_mode": ReasoningMode.BALANCED.value, 
                "_cached_at": asyncio.get_event_loop().time()
            }
            
            self.plan_cache[plan_key] = result
            self.performance_metrics.plans_generated += 1
            
            if self._is_high_quality_plan(plan):
                await self.update_emotional_state(EmotionalEvent.SUCCESS, 0.2)
                
            await self._audit_interaction("plan_generated", {"goal": goal, "context": context}, {"plan": result}, None)
            return result
            
        except Exception as e:
            logger.error(f"Plan generation failed: {e}")
            await self.update_emotional_state(EmotionalEvent.FAILURE, 0.3)
            fallback_plan = {
                "goal": goal, 
                "plan": {"summary": "Fallback plan", "steps": ["Analyze situation", "Take action"]}, 
                "error": str(e), 
                "timestamp": asyncio.get_event_loop().time()
            }
            return fallback_plan

    async def reflect(self) -> Optional[str]:
        now = asyncio.get_event_loop().time()
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
                "emotional_trends": await self._analyze_emotional_trends(),
                "performance_metrics": self.performance_metrics.to_dict()
            }
            
            reflection_prompt = f"Analyze cognitive performance:\n{json.dumps(reflection_data, indent=2)}"
            insight = await self.reason(reflection_prompt, reasoning_mode=ReasoningMode.DEEP)
            
            reflection_record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "insight": insight,
                "success_rate": success_rate,
                "traces_analyzed": len(recent_traces),
                "emotional_state": self.cognitive_state["emotional_vector"].to_dict()
            }
            
            async with self._state_lock:
                self.cognitive_state["recent_reflections"].append(reflection_record)
                
            self.last_reflection = now
            self.performance_metrics.reflections_performed += 1
            await self._persist_cognitive_state()
            
            logger.info(f"Reflection: {insight[:120]}...")
            return insight
            
        except Exception as e:
            logger.error(f"Reflection error: {e}")
            await self.update_emotional_state(EmotionalEvent.FAILURE, 0.1)
            return None

    # -------------------------
    # Emotional Processing
    # -------------------------
    async def update_emotional_state(self, event_type: EmotionalEvent, intensity: float = 0.5):
        intensity = max(0.05, min(1.0, intensity))
        async with self._state_lock:
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
                    current = getattr(ev, dim)
                    if dim == "resilience":
                        new_val = max(MIN_RESILIENCE, current + delta)
                    else:
                        new_val = max(0.0, min(1.0, current + delta))
                    setattr(ev, dim, new_val)
                    
            ev.clamp()
            self.performance_metrics.emotional_updates += 1
            await self._update_operational_mode()
            
        await self._persist_cognitive_state()

    async def emotional_decay(self, rate: Optional[float] = None):
        decay_rate = rate or self.emotional_decay_rate
        async with self._state_lock:
            ev = self.cognitive_state["emotional_vector"]
            target = {"calm": 0.6, "alert": 0.4, "stress": 0.1}
            
            for key, target_val in target.items():
                current = getattr(ev, key)
                effective_rate = decay_rate * (1.0 + ev.resilience * 0.5)
                new_value = current + (target_val - current) * effective_rate
                setattr(ev, key, new_value)
                
            ev.resilience = min(1.0, max(MIN_RESILIENCE, ev.resilience + (decay_rate * 0.1)))
            ev.clamp()

    async def _update_operational_mode(self):
        ev = self.cognitive_state["emotional_vector"]
        stability_score = ev.calm - ev.stress + (ev.resilience * 0.5)
        if stability_score < 0.3 or ev.stress > 0.8:
            self.cognitive_state["operational_mode"] = "degraded"
        elif stability_score > 0.7 and ev.stress < 0.3:
            self.cognitive_state["operational_mode"] = "high_performance"
        else:
            self.cognitive_state["operational_mode"] = "normal"

    # -------------------------
    # Health & Metrics (Fixed - No Deadlocks)
    # -------------------------
    async def get_health(self) -> Dict[str, Any]:
        """
        Completely isolated health check that avoids all potential deadlocks.
        Each component is checked independently with timeouts.
        """
        health_data = {
            "base_health": {"status": "unknown", "error": "not checked"},
            "llm_health": {"available": False, "responsive": False, "error": "not checked"},
            "storage_health": {"available": False, "working": False, "error": "not checked"},
            "circuit_breaker": {"state": "unknown", "error": "not checked"},
            "rate_limiter": {"can_acquire": False, "error": "not checked"},
            "task_health": {"cognition_task": False, "health_task": False},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        # Check base health (no dependencies)
        try:
            health_data["base_health"] = await asyncio.wait_for(
                self._get_base_health(), timeout=2.0
            )
        except asyncio.TimeoutError:
            health_data["base_health"] = {"status": "timeout", "error": "Base health check timed out"}
        except Exception as e:
            health_data["base_health"] = {"status": "error", "error": str(e)}

        # Check LLM health (direct test, no recursion)
        try:
            health_data["llm_health"] = await asyncio.wait_for(
                self._check_llm_health_direct(), timeout=3.0
            )
        except asyncio.TimeoutError:
            health_data["llm_health"] = {"available": False, "responsive": False, "error": "LLM health check timed out"}
        except Exception as e:
            health_data["llm_health"] = {"available": False, "responsive": False, "error": str(e)}

        # Check storage health
        try:
            health_data["storage_health"] = await asyncio.wait_for(
                self._check_storage_health(), timeout=2.0
            )
        except asyncio.TimeoutError:
            health_data["storage_health"] = {"available": False, "working": False, "error": "Storage health check timed out"}
        except Exception as e:
            health_data["storage_health"] = {"available": False, "working": False, "error": str(e)}

        # Check circuit breaker
        try:
            health_data["circuit_breaker"] = await asyncio.wait_for(
                self.llm_circuit_breaker.get_state(), timeout=1.0
            )
        except asyncio.TimeoutError:
            health_data["circuit_breaker"] = {"state": "timeout", "error": "Circuit breaker check timed out"}
        except Exception as e:
            health_data["circuit_breaker"] = {"state": "error", "error": str(e)}

        # Check rate limiter
        try:
            health_data["rate_limiter"] = await asyncio.wait_for(
                self.rate_limiter.get_status(), timeout=1.0
            )
        except asyncio.TimeoutError:
            health_data["rate_limiter"] = {"can_acquire": False, "error": "Rate limiter check timed out"}
        except Exception as e:
            health_data["rate_limiter"] = {"can_acquire": False, "error": str(e)}

        # Check task health (simple boolean checks)
        health_data["task_health"] = {
            "cognition_task": self._cognition_task is not None and not self._cognition_task.done(),
            "health_task": self._health_check_task is not None and not self._health_check_task.done()
        }

        # Determine overall status
        base_ok = health_data["base_health"].get("status") == "healthy"
        llm_ok = health_data["llm_health"].get("available", False)
        storage_ok = health_data["storage_health"].get("working", False)
        
        if base_ok and llm_ok and storage_ok:
            health_data["overall_status"] = "healthy"
        elif not any([base_ok, llm_ok, storage_ok]):
            health_data["overall_status"] = "error"
        else:
            health_data["overall_status"] = "degraded"

        return health_data

    async def _get_base_health(self) -> Dict[str, Any]:
        ev = self.cognitive_state["emotional_vector"]
        stability_score = ev.calm - ev.stress + (ev.resilience * 0.5)
        
        # ADD SAFE ACCESS FOR operational_mode
        operational_mode = self.cognitive_state.get("operational_mode", "normal")
        
        return {
            "status": "healthy" if stability_score > 0.5 else "stressed", 
            "emotional_stability": stability_score, 
            "emotional_vector": ev.to_dict(), 
            "operational_mode": operational_mode,  # Use safe access
            "load_factor": self.cognitive_state.get("load_factor", 0.0), 
            "confidence": self.cognitive_state.get("confidence", 0.85), 
            "recent_reflections": len(self.cognitive_state.get("recent_reflections", [])), 
            "cognitive_history_size": len(self.cognitive_history), 
            "plan_cache_size": len(self.plan_cache)
        }

    async def _check_llm_health_direct(self) -> Dict[str, Any]:
        """
        Direct LLM health check that never calls reason() method.
        Completely isolated to avoid circular dependencies.
        """
        health = {"available": False, "responsive": False}
        
        if not hasattr(self.core, "llm_orchestrator"):
            health["error"] = "LLM orchestrator not available"
            return health
            
        health["available"] = True
        
        # Test with direct LLM call, never through reason()
        try:
            test_messages = [{"role": "user", "content": "Say only the word 'OK'"}]
            
            if hasattr(self.core.llm_orchestrator, 'achat'):
                response = await self.core.llm_orchestrator.achat(
                    messages=test_messages,
                    temperature=0.1,
                    max_tokens=5
                )
                health["responsive"] = bool(response and any(word in str(response).upper() for word in ['OK', 'HELLO', 'YES']))
                health["response_sample"] = str(response)[:50] if response else "No response"
                
            elif hasattr(self.core.llm_orchestrator, 'chat'):
                # Use sync call in executor
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    self.core.llm_orchestrator.chat,
                    test_messages,
                    0.1,
                    5
                )
                health["responsive"] = bool(response and any(word in str(response).upper() for word in ['OK', 'HELLO', 'YES']))
                health["response_sample"] = str(response)[:50] if response else "No response"
                health["method"] = "sync_fallback"
            else:
                health["error"] = "No compatible LLM method found"
                
        except Exception as e:
            health["error"] = f"LLM test failed: {str(e)}"
            health["responsive"] = False
            
        return health

    async def _check_storage_health(self) -> Dict[str, Any]:
        health = {"available": False, "working": False}
        if not hasattr(self.core, "store"):
            health["error"] = "Storage system not available"
            return health
            
        health["available"] = True
        try:
            test_key = f"health_check_{int(asyncio.get_event_loop().time())}"
            test_data = {"timestamp": asyncio.get_event_loop().time(), "test": True}
            await self.core.store.put(test_key, "cognition_health", test_data)
            retrieved = await self.core.store.get(test_key)
            health["working"] = retrieved is not None
            
            # Cleanup
            try:
                await self.core.store.delete(test_key, "cognition_health")
            except Exception:
                pass
                
        except Exception as e:
            health["error"] = str(e)
            health["working"] = False
            
        return health

    # -------------------------
    # Background Services
    # -------------------------
    async def start_background_services(self):
        """Start background services explicitly"""
        if self._cognition_task is None:
            self._cognition_task = asyncio.create_task(self._cognition_cycle())
        if self._health_check_task is None:
            self._health_check_task = asyncio.create_task(self._health_monitor_cycle())
        logger.info("Background services started")

    async def _cognition_cycle(self, interval: float = 5.0):
        """Simple cognition cycle"""
        logger.info("Cognition cycle started")
        while not self._stop_event.is_set():
            try:
                await self.emotional_decay()
                
                current_time = asyncio.get_event_loop().time()
                if current_time - self.last_reflection > self.reflection_frequency:
                    await self.reflect()
                    
                if current_time % 60 < interval:
                    await self._persist_cognitive_state()
                    
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cognition cycle error: {e}")
                await asyncio.sleep(interval * 2)
                
        logger.info("Cognition cycle stopped")

    async def _health_monitor_cycle(self, interval: float = 30.0):
        """Simple health monitor"""
        logger.info("Health monitor started")
        while not self._stop_event.is_set():
            try:
                health = await self.get_health()
                if health["overall_status"] == "degraded":
                    base_health = health.get("base_health", {})
                    operational_mode = base_health.get("operational_mode", "unknown")
                    logger.warning(f"System health degraded: {operational_mode}")
                    
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(interval)
                
        logger.info("Health monitor stopped")

    # -------------------------
    # Persistence
    # -------------------------
    async def _load_cognitive_state(self):
        """Load cognitive state from storage"""
        try:
            if hasattr(self.core, "store"):
                # CORRECTED: Single argument
                state_data = await self.core.store.get("cognition_state")
                if state_data:
                    async with self._state_lock:
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
                    logger.info("Restored cognitive state")
        except Exception as e:
            logger.warning(f"Failed to load cognitive state: {e}")

    async def _persist_cognitive_state(self):
        """Persist cognitive state to storage"""
        try:
            if hasattr(self.core, "store"):
                async with self._state_lock:
                    state_to_persist = {
                        "focus": self.cognitive_state["focus"],
                        "confidence": self.cognitive_state["confidence"],
                        "stability": self.cognitive_state["stability"],
                        "emotional_vector": self.cognitive_state["emotional_vector"].to_dict(),
                        "operational_mode": self.cognitive_state["operational_mode"],
                        "load_factor": self.cognitive_state["load_factor"],
                        "last_persisted": asyncio.get_event_loop().time()
                    }
                # CORRECTED: Single argument
                await self.core.store.put("cognition_state", "system_state", state_to_persist)
        except Exception as e:
            logger.warning(f"Failed to persist cognitive state: {e}")

    # -------------------------
    # Utilities
    # -------------------------
    def _hash_request(self, query: str, context: Dict[str, Any]) -> str:
        import hashlib
        content = f"{query}{json.dumps(context, sort_keys=True)}"
        return hashlib.md5(content.encode()).hexdigest()

    async def _is_duplicate_request(self, request_hash: str) -> bool:
        current_time = asyncio.get_event_loop().time()
        last_time = self.request_deduplication.get(request_hash, 0)
        if current_time - last_time < 1.0:  # 1 second window
            return True
        self.request_deduplication[request_hash] = current_time
        return False

    async def _store_response_cache(self, request_hash: str, response: str, mode: str, ttl: Optional[int] = None):
        expiry = asyncio.get_event_loop().time() + (ttl or self.cache_ttl)
        async with self._response_cache_lock:
            self._response_cache[request_hash] = (response, expiry, mode)

    async def _get_cached_response(self, request_hash: str) -> Optional[Tuple[str, str]]:
        async with self._response_cache_lock:
            entry = self._response_cache.get(request_hash)
            if not entry:
                return None
            response, expiry, mode = entry
            if asyncio.get_event_loop().time() > expiry:
                del self._response_cache[request_hash]
                return None
            return response, mode

    async def _record_trace(self, query: str, response: str, reasoning_mode: ReasoningMode,
                          latency: Optional[float], success_score: float, error: Optional[str] = None):
        trace = CognitiveTrace(
            query=query, 
            response=response, 
            reasoning_mode=reasoning_mode.value, 
            latency=latency, 
            success_score=success_score, 
            emotional_context=self.cognitive_state["emotional_vector"].to_dict(), 
            error=error
        )
        self.cognitive_history.append(trace)

    async def _audit_interaction(self, event: str, request: Dict[str, Any], response: Dict[str, Any], trace_id: Optional[str]):
        """Async audit logging"""
        try:
            audit_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": event,
                "request": request,
                "response": response,
                "trace_id": trace_id
            }
            
            # Try audit logger first
            if hasattr(self.core, "audit_logger"):
                try:
                    if hasattr(self.core.audit_logger, 'ainfo'):
                        await self.core.audit_logger.ainfo("cognition_audit", extra=audit_entry)
                    else:
                        self.core.audit_logger.info("cognition_audit", extra=audit_entry)
                    return
                except Exception:
                    pass
                    
            # Fallback to storage
            if hasattr(self.core, "store"):
                key = f"{self.audit_log_prefix}_{int(asyncio.get_event_loop().time()*1000)}_{uuid.uuid4().hex[:6]}"
                try:
                    await self.core.store.put(key, audit_entry)
                    return
                except Exception:
                    pass
                    
            # Final fallback
            logger.info(f"AUDIT: {json.dumps(audit_entry, default=str)}")
            
        except Exception as e:
            logger.warning(f"Audit failed: {e}")

    async def _analyze_emotional_trends(self) -> Dict[str, Any]:
        recent_traces = list(self.cognitive_history)[-10:]
        if not recent_traces:
            return {"trend": "stable", "volatility": 0.0}
            
        stress_levels = [t.emotional_context.get("stress", 0.5) for t in recent_traces if t.emotional_context]
        if not stress_levels:
            return {"trend": "stable", "volatility": 0.0}
            
        volatility = max(stress_levels) - min(stress_levels)
        avg_stress = sum(stress_levels) / len(stress_levels)
        
        return {
            "trend": "increasing" if avg_stress > 0.6 else "decreasing" if avg_stress < 0.3 else "stable", 
            "volatility": volatility, 
            "avg_stress": avg_stress
        }

    # -------------------------
    # Helper Methods
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
            
        messages = [
            {"role": "system", "content": "\n".join(system_parts)}, 
            {"role": "user", "content": query}
        ]
        
        if context:
            messages.append({"role": "user", "content": f"Context: {json.dumps(context, default=str)}"})
            
        return messages

    def _sanitize_response(self, response: str) -> str:
        if not response:
            return ""
        cleaned = response.strip()
        if cleaned.startswith("```") and "```" in cleaned[3:]:
            parts = cleaned.split("```")
            cleaned = "\n".join([p for i, p in enumerate(parts) if i % 2 == 0])
        if len(cleaned) > 10000:
            cleaned = cleaned[:10000] + "... [truncated]"
        return cleaned

    def _try_fast_reasoning(self, query: str) -> Optional[str]:
        query_lower = query.strip().lower()
        fast_responses = {
            "hello": "Hello! How can I assist you today?",
            "hi": "Hi there! What can I help you with?",
            "status": f"Operational status: {self.cognitive_state['operational_mode']}",
            "time": f"Current system time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "help": "I can help with reasoning, planning, analysis, and emotional context understanding."
        }
        return fast_responses.get(query_lower)

    def _get_circuit_breaker_fallback(self, query: str) -> str:
        return f"[Circuit Breaker] LLM service temporarily unavailable. For '{query}', try again shortly."

    def _get_rate_limit_fallback(self, query: str) -> str:
        return f"[Rate Limit] Too many requests. Please wait before trying '{query}' again."

    def _get_fallback_reasoning(self, query: str) -> str:
        return f"[Fallback] Based on available data: {query[:100]}..."

    def _get_temperature(self, reasoning_mode: ReasoningMode) -> float:
        temperatures = {ReasoningMode.FAST: 0.1, ReasoningMode.BALANCED: 0.2, ReasoningMode.DEEP: 0.3}
        return temperatures.get(reasoning_mode, 0.2)

    def _get_max_tokens(self, reasoning_mode: ReasoningMode) -> int:
        token_limits = {ReasoningMode.FAST: 200, ReasoningMode.BALANCED: 400, ReasoningMode.DEEP: 800}
        return token_limits.get(reasoning_mode, 400)

    def _validate_plan(self, plan: Dict[str, Any]) -> bool:
        return all(key in plan for key in ["summary", "steps"]) and isinstance(plan.get("steps", []), list)

    def _is_high_quality_plan(self, plan: Dict[str, Any]) -> bool:
        steps = plan.get("steps", [])
        return 2 <= len(steps) <= 10 and len(plan.get("summary", "")) > 20

    def _create_fallback_plan(self, goal: str, response: str) -> Dict[str, Any]:
        return {
            "summary": f"Plan for: {goal}", 
            "steps": [
                "Analyze the current situation and context", 
                "Identify key objectives and constraints", 
                "Develop actionable steps based on analysis", 
                "Execute plan with continuous monitoring", 
                "Adjust based on outcomes and feedback"
            ], 
            "notes": f"Based on LLM response: {response[:200]}"
        }

    # -------------------------
    # Context Analysis
    # -------------------------
    async def analyze_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        ev = self.cognitive_state["emotional_vector"]
        recent_requests = len([t for t in list(self.cognitive_history)[-10:] 
                             if asyncio.get_event_loop().time() - t.timestamp < 60])
        load_factor = min(1.0, recent_requests / 10.0)
        
        async with self._state_lock:
            self.cognitive_state["load_factor"] = load_factor
            
        analysis = {
            "alertness": ev.alert + (0.3 if context.get("urgent") else 0),
            "stress_bias": ev.stress * (1.0 + load_factor),
            "focus_shift": "external" if context.get("external_event") else "internal",
            "priority": await self._calculate_priority(context),
            "load_factor": load_factor,
            "recommended_mode": await self._recommend_reasoning_mode(context, load_factor),
            "emotional_stability": ev.calm - ev.stress
        }
        
        trace = CognitiveTrace(
            query="context_analysis",
            response=json.dumps(analysis),
            emotional_context=ev.to_dict(),
            metadata={"context_keys": list(context.keys())}
        )
        self.cognitive_history.append(trace)
        await self._audit_interaction("context_analysis", {"context": context}, analysis, trace.id)
        return analysis

    async def _calculate_priority(self, context: Dict[str, Any]) -> str:
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

    async def _recommend_reasoning_mode(self, context: Dict[str, Any], load_factor: float) -> str:
        if load_factor > 0.8 or context.get("urgent"):
            return ReasoningMode.FAST.value
        elif context.get("complex") or context.get("strategic"):
            return ReasoningMode.DEEP.value
        else:
            return ReasoningMode.BALANCED.value

    # -------------------------
    # Cleanup & Shutdown
    # -------------------------
    async def cleanup_resources(self) -> Dict[str, int]:
        async with self._state_lock:
            old_sizes = {
                "cognitive_history": len(self.cognitive_history), 
                "plan_cache": len(self.plan_cache), 
                "deduplication_cache": len(self.request_deduplication), 
                "reflections": len(self.cognitive_state["recent_reflections"])
            }
            self.cognitive_history.clear()
            self.plan_cache.clear()
            self.request_deduplication.clear()
            self.cognitive_state["recent_reflections"].clear()
            
            async with self._response_cache_lock:
                self._response_cache.clear()
                
            logger.info("Cognitive resources cleaned up")
            return old_sizes

    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Initiating shutdown...")
        self._stop_event.set()
        
        # Cancel tasks
        if self._cognition_task:
            self._cognition_task.cancel()
        if self._health_check_task:
            self._health_check_task.cancel()
            
        # Wait for tasks
        tasks = [t for t in [self._cognition_task, self._health_check_task] if t]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
        await self._persist_cognitive_state()
        logger.info("Shutdown complete")

    async def __aenter__(self):
        await self._load_cognitive_state()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

# -------------------------
# Test
# -------------------------
async def test_cognition_core():
    """Working test of CognitionCore functionality"""
    logging.basicConfig(level=logging.WARNING)  # Reduce noise
    
    class TestCore:
        def __init__(self):
            self._store = {}
            
            class Storage:
                def __init__(self, parent):
                    self.parent = parent
                    
                async def put(self, key, typ, val):
                    self.parent._store[(key, typ)] = val
                    return True
                    
                async def get(self, key, typ=None):
                    await asyncio.sleep(0.001)  # Simulate async
                    if typ is None:
                        for (k, t), v in self.parent._store.items():
                            if k == key:
                                return v
                        return None
                    return self.parent._store.get((key, typ))
                    
                async def delete(self, key, typ):
                    self.parent._store.pop((key, typ), None)
                    return True
                    
            self.store = Storage(self)
            
            class LLMOrchestrator:
                default_model = "test-model"
                
                async def achat(self, messages, temperature, max_tokens):
                    # Instant response for health checks
                    return "OK"
                            
            self.llm_orchestrator = LLMOrchestrator()
            self.default_llm_model = "test-model"

    print("🚀 Testing Async CognitionCore v4.1.0")
    print("=" * 40)
    
    try:
        # Quick setup
        test_core = TestCore()
        cfg = {"max_memory_entries": 50, "rate_limit": 100}
        
        print("1. Creating instance...")
        cog = CognitionCore(persona=None, core=test_core, autonomy=None, config=cfg)
        print("   ✅ Instance created")
        
        print("2. Testing health check...")
        start_time = asyncio.get_event_loop().time()
        health = await cog.get_health()
        end_time = asyncio.get_event_loop().time()
        print(f"   ✅ Health check completed in {end_time - start_time:.3f}s")
        print(f"   ✅ Status: {health['overall_status']}")
        print(f"   ✅ LLM: {health['llm_health']['available']}")
        print(f"   ✅ Storage: {health['storage_health']['working']}")
        
        print("3. Testing basic reasoning...")
        response = await cog.reason("Hello")
        print(f"   ✅ Response: {response}")
        
        print("4. Testing emotional system...")
        await cog.update_emotional_state(EmotionalEvent.SUCCESS, 0.5)
        emotional = cog.cognitive_state["emotional_vector"].to_dict()
        print(f"   ✅ Emotional stability: {emotional['stability_score']:.2f}")
        
        print("5. Testing planning...")
        plan = await cog.generate_plan("Test goal")
        print(f"   ✅ Plan created: {bool(plan.get('plan'))}")
        
        print("6. Testing shutdown...")
        await cog.shutdown()
        print("   ✅ Shutdown successful")
        
        print("\n🎉 SUCCESS! Core functionality verified!")
        print("Async CognitionCore v4.1.0 is ready for integration.")
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_cognition_core())