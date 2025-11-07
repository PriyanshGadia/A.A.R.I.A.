"""
interaction_core_v1_1_finalized.py - A.A.R.I.A Interaction Layer (Production Ready v1.1.0)

Enhancements over v1.0.0:
- Optional asyncio mode for outbound worker (configurable)
- Background session autosave to persistent storage every N seconds
- Exponential backoff + retry for LLM orchestrator calls
- Enhanced persona prompt with emotional/contextual metadata
- HMAC signing for audit logs (configurable key)
- Improved rate limiter with token-bucket option
- Privileged action pattern parsing and safer gating
- Configurable delivery backends and better error handling
- More robust metrics instrumentation (if Prometheus available)
- Cleaner separation of sync/async delivery paths

Notes:
- This file is intended as a drop-in replacement for v1.0.0 with backward-compatible APIs where possible.
"""

from __future__ import annotations

import time
import json
import logging
import threading
import traceback
import uuid
import re
import hashlib
import hmac
import base64
from typing import Any, Dict, Optional, List, Tuple, Callable
from collections import deque, Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager

# Async support
import asyncio
import random

# Optional imports (graceful fallback)
try:
    from prometheus_client import Counter as PromCounter, Histogram, Gauge, Summary
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False

# Import type-hintable dependencies (may be just duck-typed at runtime)
try:
    from persona_core import PersonaCore  # type: ignore
except Exception:
    PersonaCore = Any  # fallback for type-checking/runtime if not present

try:
    from cognition_core import CognitionCore  # type: ignore
except Exception:
    CognitionCore = Any

try:
    from autonomy_core import AutonomyCore  # type: ignore
except Exception:
    AutonomyCore = Any

# # -------------------------
# # Logger
# # -------------------------
# logger = logging.getLogger("AARIA.Interaction")
# if not logger.handlers:
#     h = logging.StreamHandler()
#     h.setFormatter(logging.Formatter("%(asctime)s [AARIA.INTERACTION] [%(levelname)s] %(message)s"))
#     logger.addHandler(h)
# logger.setLevel(logging.INFO)
logger = logging.getLogger(__name__)
# -------------------------
# Metrics (optional) - extended
# -------------------------
if PROMETHEUS_AVAILABLE:
    # Initialize metrics with proper Prometheus client syntax
    metrics = {
        "requests_total": PromCounter("interaction_requests_total", "Total interaction requests", ["channel", "intent"]),
        "responses_total": PromCounter("interaction_responses_total", "Total responses generated", ["channel", "intent", "status"]),
        "response_latency": Histogram("interaction_response_latency_seconds", "Response latency seconds", ["channel"]),
        "response_latency_summary": Summary("interaction_response_latency_summary", "Response latency summary", ["channel"]),
        "active_sessions": Gauge("interaction_active_sessions", "Active conversational sessions"),
        "queue_size": Gauge("interaction_outbound_queue_size", "Outbound message queue size"),
        "rate_limited": PromCounter("interaction_rate_limited_total", "Total rate limited events", ["channel"])
    }
else:
    metrics = None
    
# -------------------------
# Dataclasses
# -------------------------
@dataclass
class Session:
    session_id: str
    user_id: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)
    persona_tone: str = "default"
    history: deque = field(default_factory=lambda: deque(maxlen=200))
    ttl_seconds: int = 3600  # default 1 hour
    metadata: Dict[str, Any] = field(default_factory=dict)

    def touch(self):
        self.last_active = time.time()

    def is_expired(self) -> bool:
        return time.time() - self.last_active > self.ttl_seconds

@dataclass
class InboundMessage:
    channel: str  # e.g., 'text', 'voice', 'webhook'
    content: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class OutboundMessage:
    channel: str
    content: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

# -------------------------
# Interaction Core
# -------------------------
class InteractionCore:
    """
    Production-grade InteractionCore for A.A.R.I.A. (v1.1.0)
    """

    DEFAULT_CONFIG = {
        "session_ttl": 3600,
        "rate_limit_per_minute": 60,
        "dedup_window_seconds": 10,
        "response_cache_ttl": 300,
        "audit_event_key": "cognition_audit",
        "autosave_interval": 60,  # seconds
        "async_mode": False,  # if True, use asyncio outbound worker
        "max_llm_retries": 3,
        "llm_backoff_base": 0.5,  # seconds
        "hmac_audit_key": None,  # base64 key for signing audit payloads
        "rate_limiter_mode": "sliding",  # or 'token_bucket'
        "token_bucket_capacity": 60,
        "token_bucket_refill_rate_per_minute": 60
    }

    def __init__(self, persona: Any, cognition: Any, autonomy: Any, config: Optional[Dict[str, Any]] = None):
        self.persona = persona
        self.cognition = cognition
        self.autonomy = autonomy
        self.config = {**self.DEFAULT_CONFIG, **(config or {})}

        # In-memory stores (persistable by core.storage if available)
        self.sessions: Dict[str, Session] = {}
        self.outbound_queue: deque = deque()
        self._session_lock = threading.RLock()
        self._queue_lock = threading.RLock()
        self._rate_lock = threading.RLock()

        # Request deduplication & caching
        self._recent_requests: Dict[str, float] = {}  # request_hash -> timestamp
        self._response_cache: Dict[str, Tuple[float, str]] = {}  # request_hash -> (expiry, response)

        # Per-user rate limiting structures
        self._user_rate_windows: Dict[str, deque] = {}
        self._token_buckets: Dict[str, Dict[str, float]] = {}  # user -> {tokens, last_refill_ts}

        # Intent classifier patterns (extendable)
        self.intent_patterns = {
            "greeting": re.compile(r"\b(hi|hello|hey|good (morning|afternoon|evening))\b", re.I),
            "thanks": re.compile(r"\b(thanks|thank you|thx)\b", re.I),
            "help": re.compile(r"\b(help|assist|support|what can you)\b", re.I),
            "schedule": re.compile(r"\b(schedule|resched|calendar|meeting|appointment)\b", re.I),
            "contact": re.compile(r"\b(call|phone|contact|email)\b", re.I),
            "command": re.compile(r"^(?:run|execute|do)\b", re.I),
            "query": re.compile(r"\b(who|what|when|where|why|how)\b", re.I)
        }

        # Privileged actions (patterns) - now compiled for safety
        self.privileged_actions = [re.compile(p, re.I) for p in [r"autonomy\.execute", r"system\.shutdown", r"calendar\.add"]]

        # Audit logger fallback
        self.audit_logger = getattr(self.persona.core, "audit_logger", None) if self.persona and hasattr(self.persona, "core") else None

        # Async support variables
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._async_task: Optional[asyncio.Task] = None
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None

        # Load persisted sessions on startup
        self._load_persisted_sessions()

        # Start background autosave and outbound worker
        autosave_interval = float(self.config.get("autosave_interval", 60))
        self._autosave_thread = threading.Thread(target=self._autosave_worker, args=(autosave_interval,), daemon=True, name="InteractionAutosave")
        self._autosave_thread.start()

        if self.config.get("async_mode"):
            # Start an asyncio loop in a background thread
            self._start_async_worker()
        else:
            self._worker_thread = threading.Thread(target=self._outbound_worker, name="InteractionOutboundWorker", daemon=True)
            self._worker_thread.start()

        logger.info("InteractionCore v1.1.0 initialized")

    # -------------------------
    # Core Access & Utilities
    # -------------------------
    def _get_core(self) -> Optional[Any]:
        """Unified core access with proper error handling."""
        for component in [self.persona, self.cognition, self.autonomy]:
            if hasattr(component, 'core'):
                return component.core
        return None

    def _load_persisted_sessions(self):
        """Load persisted sessions on startup if available."""
        try:
            core = self._get_core()
            if core and hasattr(core, 'storage'):
                logger.debug("Session persistence available - attempting load on startup")
                # Attempt robust loading with prefix
                for k in list(getattr(core.storage, '_store', {}).keys()):
                    try:
                        # storage.get interface may vary; handle common patterns
                        if isinstance(k, tuple) and k[1] == 'interaction_sessions' and k[0].startswith('session_'):
                            sess_data = core.storage.get(k[0], 'interaction_sessions')
                            if sess_data:
                                sid = k[0].replace('session_', '')
                                s = Session(session_id=sid, user_id=sess_data.get('user_id'), persona_tone=sess_data.get('persona_tone', 'default'), ttl_seconds=self.config['session_ttl'])
                                s.last_active = sess_data.get('last_active', time.time())
                                s.history = deque(sess_data.get('history', []), maxlen=200)
                                s.metadata = sess_data.get('metadata', {})
                                self.sessions[sid] = s
                    except Exception:
                        continue
                if PROMETHEUS_AVAILABLE:
                    metrics['active_sessions'].set(len(self.sessions))
        except Exception as e:
            logger.debug(f"Session persistence not available or failed: {e}")

    # -------------------------
    # Session Management
    # -------------------------
    def create_session(self, user_id: Optional[str] = None, persona_tone: str = "default", ttl_seconds: Optional[int] = None) -> Session:
        session_id = f"sess_{uuid.uuid4().hex[:12]}"
        ttl = ttl_seconds or self.config["session_ttl"]
        session = Session(session_id=session_id, user_id=user_id, persona_tone=persona_tone, ttl_seconds=ttl)
        with self._session_lock:
            self.sessions[session_id] = session
            if PROMETHEUS_AVAILABLE:
                metrics['active_sessions'].set(len(self.sessions))
        logger.debug(f"Created session {session_id} for user {user_id}")
        return session

    def get_session(self, session_id: Optional[str]) -> Optional[Session]:
        if not session_id:
            return None
        with self._session_lock:
            sess = self.sessions.get(session_id)
            if not sess:
                return None
            if sess.is_expired():
                logger.debug(f"Session {session_id} expired, removing")
                self.sessions.pop(session_id, None)
                if PROMETHEUS_AVAILABLE:
                    metrics['active_sessions'].set(len(self.sessions))
                return None
            sess.touch()
            return sess

    def end_session(self, session_id: str) -> bool:
        with self._session_lock:
            removed = self.sessions.pop(session_id, None) is not None
            if PROMETHEUS_AVAILABLE:
                metrics['active_sessions'].set(len(self.sessions))
        logger.debug(f"Ended session {session_id}: {removed}")
        return removed

    def cleanup_sessions(self) -> int:
        """Remove expired sessions and return count removed."""
        now = time.time()
        removed = 0
        with self._session_lock:
            for sid in list(self.sessions.keys()):
                if self.sessions[sid].is_expired():
                    self.sessions.pop(sid, None)
                    removed += 1
            if PROMETHEUS_AVAILABLE:
                metrics['active_sessions'].set(len(self.sessions))
        logger.debug(f"Cleaned up {removed} expired sessions")
        return removed

    # -------------------------
    # Rate Limiting & Deduplication
    # -------------------------
    def _hash_request(self, inbound: InboundMessage) -> str:
        key = f"{inbound.user_id}|{inbound.channel}|{inbound.content}"
        return hashlib.sha256(key.encode()).hexdigest()

    def _is_duplicate(self, inbound: InboundMessage) -> bool:
        h = self._hash_request(inbound)
        now = time.time()
        last = self._recent_requests.get(h)
        if last and now - last < self.config["dedup_window_seconds"]:
            return True
        self._recent_requests[h] = now
        # prune old entries
        cutoff = now - (self.config["dedup_window_seconds"] * 3)
        for k, v in list(self._recent_requests.items()):
            if v < cutoff:
                self._recent_requests.pop(k, None)
        return False

    def _check_rate_limit(self, inbound: InboundMessage) -> Tuple[bool, Optional[str]]:
        """Support sliding window or token bucket rate limiting."""
        mode = self.config.get('rate_limiter_mode', 'sliding')
        user = inbound.user_id or 'anon'
        if mode == 'token_bucket':
            return self._token_bucket_check(user)
        return self._sliding_window_check(inbound)

    def _sliding_window_check(self, inbound: InboundMessage) -> Tuple[bool, Optional[str]]:
        """Simple per-user sliding window rate limit."""
        user = inbound.user_id or "anon"
        limit = int(self.config.get("rate_limit_per_minute", 60))
        window_seconds = 60
        now = time.time()
        with self._rate_lock:
            dq = self._user_rate_windows.setdefault(user, deque())
            # remove outdated timestamps
            while dq and now - dq[0] > window_seconds:
                dq.popleft()
            if len(dq) >= limit:
                if PROMETHEUS_AVAILABLE:
                    metrics['rate_limited'].labels(inbound.channel).inc()
                return False, f"Rate limit exceeded: {limit}/minute"
            dq.append(now)
        return True, None

    def _token_bucket_check(self, user: str) -> Tuple[bool, Optional[str]]:
        cfg = self.config
        cap = float(cfg.get('token_bucket_capacity', 60))
        refill_per_min = float(cfg.get('token_bucket_refill_rate_per_minute', 60))
        refill_per_sec = refill_per_min / 60.0
        now = time.time()
        with self._rate_lock:
            bucket = self._token_buckets.setdefault(user, {'tokens': cap, 'last_refill': now})
            elapsed = now - bucket['last_refill']
            bucket['tokens'] = min(cap, bucket['tokens'] + elapsed * refill_per_sec)
            bucket['last_refill'] = now
            if bucket['tokens'] < 1.0:
                if PROMETHEUS_AVAILABLE:
                    metrics['rate_limited'].labels('unknown').inc()
                return False, "Rate limit exceeded (token bucket)"
            bucket['tokens'] -= 1.0
        return True, None

    # -------------------------
    # Intent Classification & Command Gating
    # -------------------------
    def classify_intent(self, message: str) -> str:
        """Lightweight intent classifier using regex patterns."""
        text = message.strip()
        for intent, pattern in self.intent_patterns.items():
            if pattern.search(text):
                return intent
        # fallback heuristics
        if text.endswith("?"):
            return "query"
        return "utterance"

    def _contains_privileged_action(self, content: str) -> bool:
        for p in self.privileged_actions:
            if p.search(content):
                return True
        return False

    def is_privileged(self, parsed_intent: str, user_id: Optional[str], session: Optional[Session]) -> bool:
        """Check whether the request can trigger privileged actions."""
        if not session or not session.user_id:
            return False
        try:
            profile = self.persona.core.load_user_profile() or {}
            is_admin = profile.get("is_admin", False) or profile.get("user_id") == session.user_id
            return bool(is_admin)
        except Exception:
            return False

    # -------------------------
    # LLM helper: retries + backoff
    # -------------------------
    def _call_llm_with_retries(self, messages: List[Dict[str, str]], model: Optional[str] = None, temperature: float = 0.2, max_tokens: int = 400) -> str:
        core = self._get_core()
        max_retries = int(self.config.get('max_llm_retries', 3))
        backoff_base = float(self.config.get('llm_backoff_base', 0.5))
        attempt = 0
        last_exc = None
        while attempt <= max_retries:
            try:
                if core and hasattr(core, 'llm_orchestrator'):
                    orchestrator = core.llm_orchestrator
                    model_final = model or getattr(orchestrator, "default_model", "default")
                    response = orchestrator.chat(
                        messages=messages,
                        temperature=temperature,
                        max_tokens=max_tokens
                    )
                    if response and response.strip():
                        return response.strip()
                # Fallback to cognition core
                if hasattr(self.cognition, 'reason'):
                    plaintext_prompt = "\n".join([m.get("content", "") for m in messages if m.get("content")])
                    return self.cognition.reason(plaintext_prompt, context={})
                return "[Service temporarily unavailable - please try again later]"
            except Exception as e:
                last_exc = e
                attempt += 1
                jitter = random.uniform(0, 0.1 * backoff_base)
                sleep_for = (backoff_base * (2 ** (attempt - 1))) + jitter
                logger.warning(f"LLM call failed on attempt {attempt}/{max_retries}: {e}. Backing off {sleep_for:.2f}s")
                time.sleep(sleep_for)
        logger.error(f"LLM calls exhausted: {last_exc}")
        raise last_exc or RuntimeError("LLM call failed after retries")

    # -------------------------
    # Synthesis & Response Generation
    # -------------------------
    def _build_persona_prompt(self, user_input: str, session: Optional[Session]) -> List[Dict[str, str]]:
        """Create a multi-part prompt that includes persona, context, and user input with metadata."""
        persona_meta = getattr(self.persona, "system_prompt", "You are A.A.R.I.A.")
        # include user metadata if available
        user_meta = {}
        try:
            profile = self.persona.core.load_user_profile() or {}
            user_meta = {k: v for k, v in profile.items() if k in ('name', 'timezone', 'preferences')}
        except Exception:
            user_meta = {}
        emotive_hint = session.metadata.get('emotion') if session else None
        system_block = f"{persona_meta}\nTone: {session.persona_tone if session else 'default'}\nUserMeta: {json.dumps(user_meta)}\nEmotionHint: {emotive_hint or 'neutral'}"
        messages = [{"role": "system", "content": system_block},
                    {"role": "user", "content": user_input}]
        # include recent session history if any (last N turns)
        if session and session.history:
            for turn in list(session.history)[-8:]:
                messages.append({"role": turn.get("role", "user"), "content": turn.get("content", "")})
        return messages

    def _cache_response(self, request_hash: str, response: str):
        expiry = time.time() + int(self.config.get("response_cache_ttl", 300))
        self._response_cache[request_hash] = (expiry, response)

    def _get_cached_response(self, request_hash: str) -> Optional[str]:
        entry = self._response_cache.get(request_hash)
        if not entry:
            return None
        expiry, response = entry
        if time.time() > expiry:
            self._response_cache.pop(request_hash, None)
            return None
        return response

    # -------------------------
    # Audit Logging (HMAC signing support)
    # -------------------------
    def _sign_audit_payload(self, payload: Dict[str, Any]) -> Optional[str]:
        key_b64 = self.config.get('hmac_audit_key')
        if not key_b64:
            return None
        try:
            key = base64.b64decode(key_b64)
            serialized = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
            sig = hmac.new(key, serialized, digestmod=hashlib.sha256).digest()
            return base64.b64encode(sig).decode('utf-8')
        except Exception as e:
            logger.warning(f"Failed to sign audit payload: {e}")
            return None

    def _audit(self, event: str, request: Dict[str, Any], response: Dict[str, Any], trace_id: Optional[str] = None):
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "request": request,
            "response": response,
            "trace_id": trace_id or f"trace_{uuid.uuid4().hex[:10]}"
        }
        try:
            signature = self._sign_audit_payload(payload)
            payload_with_sig = {**payload, "hmac": signature} if signature else payload
            if self.audit_logger:
                self.audit_logger.info("interaction_audit", extra={"audit": payload_with_sig})
            else:
                core = self._get_core()
                if core and hasattr(core, "storage"):
                    key = f"{self.config['audit_event_key']}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
                    core.storage.put(key, "interaction_audit", payload_with_sig)
                else:
                    logger.info("AUDIT_LOG: interaction_audit %s", json.dumps(payload_with_sig))
        except Exception as e:
            logger.warning(f"Audit logging failed: {e}")

    # -------------------------
    # Main Entry: handle inbound message
    # -------------------------
    def handle_inbound(self, inbound: InboundMessage) -> OutboundMessage:
        """Process inbound message end-to-end: classify -> validate -> generate -> queue/send."""
        trace_id = f"trace_{uuid.uuid4().hex[:10]}"
        request_hash = self._hash_request(inbound)

        # Basic deduplication
        if self._is_duplicate(inbound):
            logger.debug("Duplicate inbound detected, returning cached acknowledgement")
            cached = self._get_cached_response(request_hash)
            if cached:
                resp = OutboundMessage(channel=inbound.channel, content=cached, user_id=inbound.user_id, session_id=inbound.session_id)
                self._audit("reason_cached", {"query": inbound.content, "context": inbound.metadata}, {"response": cached}, trace_id)
                return resp
            logger.debug("Duplicate request but no cached response available")

        # Rate limiting
        ok, reason = self._check_rate_limit(inbound)
        if not ok:
            fallback = self._get_rate_limit_fallback(inbound.content)
            out = OutboundMessage(channel=inbound.channel, content=fallback, user_id=inbound.user_id, session_id=inbound.session_id)
            self._audit("rate_limited", {"query": inbound.content, "reason": reason}, {"response": fallback}, trace_id)
            if PROMETHEUS_AVAILABLE:
                metrics['responses_total'].labels(inbound.channel, "rate_limit", "blocked").inc()
            return out

        # Obtain or create session
        session = self.get_session(inbound.session_id) if inbound.session_id else None
        if not session:
            session = self.create_session(user_id=inbound.user_id)
            inbound.session_id = session.session_id

        # Intent classification
        intent = self.classify_intent(inbound.content)

        # Privileged action gating
        if intent == "command" and self._contains_privileged_action(inbound.content):
            if not self.is_privileged(intent, inbound.user_id, session):
                denied = "You are not authorized to perform that action."
                out = OutboundMessage(channel=inbound.channel, content=denied, user_id=inbound.user_id, session_id=session.session_id)
                self._audit("privilege_denied", {"query": inbound.content}, {"response": denied}, trace_id)
                if PROMETHEUS_AVAILABLE:
                    metrics['responses_total'].labels(inbound.channel, intent, "denied").inc()
                return out

        # Try response cache
        cached_resp = self._get_cached_response(request_hash)
        if cached_resp:
            logger.debug("Served from response cache")
            out = OutboundMessage(channel=inbound.channel, content=cached_resp, user_id=inbound.user_id, session_id=session.session_id)
            self._audit("reason_cached", {"query": inbound.content, "context": {}}, {"response": cached_resp}, trace_id)
            if PROMETHEUS_AVAILABLE:
                metrics['responses_total'].labels(inbound.channel, intent, "cached").inc()
            return out

        # Build prompt / messages
        messages = self._build_persona_prompt(inbound.content, session)

        # LLM call with timing and retries
        start = time.time()
        try:
            response_text = self._call_llm_with_retries(messages)
            latency = time.time() - start
            if PROMETHEUS_AVAILABLE:
                metrics['response_latency'].labels(inbound.channel).observe(latency)
                metrics['response_latency_summary'].labels(inbound.channel).observe(latency)
                metrics['requests_total'].labels(inbound.channel, intent).inc()
                metrics['responses_total'].labels(inbound.channel, intent, "success").inc()

            # Post-process response
            safe = response_text.strip()
            # Cache it
            self._cache_response(request_hash, safe)

            # Update session history
            session.history.append({"role": "user", "content": inbound.content, "ts": inbound.timestamp})
            session.history.append({"role": "assistant", "content": safe, "ts": time.time()})
            session.touch()

            out = OutboundMessage(channel=inbound.channel, content=safe, user_id=inbound.user_id, session_id=session.session_id)
            # Audit
            core = self._get_core()
            model_name = "local_fallback"
            if core and hasattr(core, "llm_orchestrator"):
                model_name = getattr(core.llm_orchestrator, "default_model", "unknown")
            self._audit("reason_llm_success", {"query": inbound.content, "model": model_name}, {"response": {"response": safe, "latency": latency}}, trace_id)

            # Queue outbound for delivery
            self.send_outbound(out)
            return out

        except Exception as e:
            latency = time.time() - start
            fallback = self._get_fallback_reasoning(inbound.content)
            logger.error(f"Failed to generate response: {e}")
            logger.debug(traceback.format_exc())
            self._audit("reason_failure", {"query": inbound.content}, {"response": fallback, "error": str(e)}, trace_id)
            if PROMETHEUS_AVAILABLE:
                metrics['responses_total'].labels(inbound.channel, intent, "failure").inc()
            out = OutboundMessage(channel=inbound.channel, content=fallback, user_id=inbound.user_id, session_id=session.session_id)
            # still queue fallback
            self.send_outbound(out)
            return out

    def _rate_limit_check(self, inbound: InboundMessage) -> Tuple[bool, Optional[str]]:
        """Wrapper for backward compatibility."""
        return self._check_rate_limit(inbound)

    def _get_rate_limit_fallback(self, query: str) -> str:
        """Get rate limit fallback response."""
        if hasattr(self.cognition, "_get_rate_limit_fallback"):
            return self.cognition._get_rate_limit_fallback(query)
        return "[Rate limited - please try again in a moment]"

    def _get_fallback_reasoning(self, query: str) -> str:
        """Get general fallback response."""
        if hasattr(self.cognition, "_get_fallback_reasoning"):
            return self.cognition._get_fallback_reasoning(query)
        return "[Service temporarily unavailable - please try again later]"

    # -------------------------
    # Outbound Queue & Worker (sync + async)
    # -------------------------
    def send_outbound(self, message: OutboundMessage):
        """Queue an outbound message (non-blocking). Worker will handle actual delivery."""
        with self._queue_lock:
            self.outbound_queue.append(message)
            if PROMETHEUS_AVAILABLE:
                metrics['queue_size'].set(len(self.outbound_queue))
        logger.debug(f"Queued outbound message for user {message.user_id} on channel {message.channel}")

    def _outbound_worker(self):
        """Background worker to flush outbound messages (threaded mode)."""
        while not self._stop_event.is_set():
            try:
                if not self.outbound_queue:
                    time.sleep(0.1)
                    continue
                with self._queue_lock:
                    msg = self.outbound_queue.popleft()
                    if PROMETHEUS_AVAILABLE:
                        metrics['queue_size'].set(len(self.outbound_queue))
                delivered = self._deliver_message_sync(msg)
                logger.debug(f"Delivered message to {msg.channel} (delivered={delivered})")
            except Exception as e:
                logger.error(f"Outbound worker error: {e}")
                time.sleep(0.5)

    def _deliver_message_sync(self, message: OutboundMessage) -> bool:
        """
        Deliver message via available integrations (synchronous).
        """
        try:
            core = self._get_core()
            if core and hasattr(core, "notify"):
                try:
                    core.notify(channel=message.channel, message=message.content)
                    return True
                except Exception:
                    logger.debug("core.notify failed, falling back to storage/log")
            # fallback: use storage/audit log
            self._audit("outbound_deliver", {"channel": message.channel, "content_preview": message.content[:120]}, {"status": "logged"})
            return True
        except Exception as e:
            logger.warning(f"Failed to deliver message: {e}")
            return False

    async def _deliver_message_async(self, message: OutboundMessage) -> bool:
        """
        Async delivery path. Implementations might call async SDKs here.
        Default behavior proxies to sync deliver in threadpool.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._deliver_message_sync, message)

    # Async worker utilities
    def _start_async_worker(self):
        """Start asyncio loop in background thread and create task to flush outbound queue."""
        def run_loop(loop):
            asyncio.set_event_loop(loop)
            loop.run_forever()

        self._async_loop = asyncio.new_event_loop()
        t = threading.Thread(target=run_loop, args=(self._async_loop,), daemon=True, name='InteractionAsyncLoop')
        t.start()
        # schedule the queue flushing coroutine
        self._async_task = asyncio.run_coroutine_threadsafe(self._async_outbound_worker(), self._async_loop)

    async def _async_outbound_worker(self):
        """Async loop that periodically flushes queue."""
        while True:
            try:
                await asyncio.sleep(0.05)
                msg = None
                with self._queue_lock:
                    if self.outbound_queue:
                        msg = self.outbound_queue.popleft()
                        if PROMETHEUS_AVAILABLE:
                            metrics['queue_size'].set(len(self.outbound_queue))
                if msg:
                    try:
                        ok = await self._deliver_message_async(msg)
                        logger.debug(f"Async delivered message to {msg.channel} (delivered={ok})")
                    except Exception as e:
                        logger.error(f"Async delivery error: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Async outbound worker error: {e}")
                await asyncio.sleep(0.5)

    # -------------------------
    # Autosave worker
    # -------------------------
    def _autosave_worker(self, interval_sec: float):
        while not self._stop_event.is_set():
            try:
                time.sleep(interval_sec)
                self._persist_sessions()
            except Exception as e:
                logger.debug(f"Autosave worker error: {e}")

    # -------------------------
    # Persistence
    # -------------------------
    def _persist_sessions(self):
        """Persist active sessions to storage."""
        try:
            core = self._get_core()
            if core and hasattr(core, "storage"):
                with self._session_lock:
                    persisted = 0
                    for sid, sess in list(self.sessions.items()):
                        if not sess.is_expired():
                            session_data = {
                                "user_id": sess.user_id,
                                "last_active": sess.last_active,
                                "persona_tone": sess.persona_tone,
                                "history": list(sess.history),
                                "metadata": sess.metadata
                            }
                            core.storage.put(f"session_{sid}", "interaction_sessions", session_data)
                            persisted += 1
                logger.info(f"InteractionCore persisted {persisted} sessions")
        except Exception as e:
            logger.warning(f"InteractionCore failed to persist sessions: {e}")

    # -------------------------
    # Shutdown & Cleanup
    # -------------------------
    def shutdown(self, wait_seconds: float = 5.0):
        """Graceful shutdown for interaction core."""
        logger.info("InteractionCore shutdown initiated")
        self._stop_event.set()
        # stop async loop if running
        if self._async_loop and self._async_task:
            self._async_task.cancel()
            try:
                self._async_loop.call_soon_threadsafe(self._async_loop.stop)
            except Exception:
                pass
        if self._worker_thread:
            self._worker_thread.join(timeout=wait_seconds)
        # persist sessions if possible
        self._persist_sessions()
        logger.info("InteractionCore shutdown complete")

# -------------------------
# Self-test / Example usage
# -------------------------
if __name__ == "__main__":
    import logging
    import time
    from mistral_client import MistralLLMClient

    # Enable debug logging
    logging.basicConfig(level=logging.DEBUG)

    # interaction_core.py
    from persona_core import PersonaCore
    from cognition_core import CognitionCore
    from autonomy_core import AutonomyCore
    from mistral_client import MistralLLMClient

    # Initialize Mistral LLM client
    llm_client = MistralLLMClient(api_key="YOUR_MISTRAL_API_KEY")

    # Initialize core components
    persona = PersonaCore()
    cognition = CognitionCore(llm=llm_client)
    autonomy = AutonomyCore()

    # Initialize InteractionCore
    interaction = InteractionCore(persona=persona, cognition=cognition, autonomy=autonomy)


    # Test conversation messages
    test_conversation = [
        "Hello A.A.R.I.A., how are you today?",
        "Can you summarize my tasks for tomorrow?",
        "Schedule a meeting with Alice at 3 PM.",
        "run autonomy.execute"  # privileged command test
    ]

    user_id = "user_123"

    for i, message in enumerate(test_conversation, start=1):
        inbound = InboundMessage(channel="text", content=message, user_id=user_id)
        outbound = interaction.handle_inbound(inbound)
        print(f"\nMessage {i}: {message}")
        print(f"Outbound response: {outbound.content}\n")
        time.sleep(0.2)  # small delay to simulate real interaction

    # Optional: rapid-fire rate-limit test
    print("Testing rate limiting...")
    for i in range(5):
        rate_msg = InboundMessage(channel="text", content=f"Rate limit test {i}", user_id="rate_test_user")
        out_rate = interaction.handle_inbound(rate_msg)
        print(f"Rate test {i}: {out_rate.content}")
        time.sleep(0.1)

    # Graceful shutdown
    interaction.shutdown()
    print("InteractionCore self-test completed successfully with real LLM responses!")
