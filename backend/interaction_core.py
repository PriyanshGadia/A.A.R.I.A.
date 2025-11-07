# interaction_core.py
from __future__ import annotations

"""
A.A.R.I.A Interaction Layer (Async Production v3.0.0 - Agentic Router)

This core is now the "Hands" of the agent. It does not contain any
intelligence or parsing logic. It does the following:
1. Identifies the user via SecurityOrchestrator.
2. Manages the session.
3. Asks CognitionCore (the "Brain") for a plan.
4. Executes the plan (calling other cores as tools).
5. Sends the final result to the user.
"""

import asyncio
import json
import logging
import uuid
import re
import hashlib
import hmac
import base64
import time
from typing import Any, Dict, Optional, List, Tuple, Callable
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from contextlib import suppress

# --- [NEW] IMPORTS FOR AGENTIC ROUTER ---
# These are required for the new executor logic and its fallbacks
from access_control import SecurityContext, RequestSource, UserIdentity, AccessLevel
from identity_manager import IdentityProfile
from autonomy_core import Action, ActionPriority
from cognition_core import ReasoningMode
# --- [END NEW IMPORTS] ---

# Optional holo state
try:
    import hologram_state  # optional telemetry/holo integration
except Exception:
    hologram_state = None

logger = logging.getLogger("AARIA.Interaction")

# Optional prometheus
try:
    from prometheus_client import Counter as PromCounter, Histogram, Gauge, Summary
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False

if PROMETHEUS_AVAILABLE:
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

# ---------- Component type placeholders (remains the same) ----------
try:
    from persona_core import PersonaCore  # type: ignore
except Exception:
    PersonaCore = None  # type: ignore

try:
    from cognition_core import CognitionCore  # type: ignore
except Exception:
    CognitionCore = None  # type: ignore

try:
    from autonomy_core import AutonomyCore  # type: ignore
except Exception:
    AutonomyCore = None  # type: ignore

# ---------- Dataclasses (remains the same) ----------
@dataclass
class Session:
    session_id: str
    user_id: Optional[str] = None
    created_at: float = field(default_factory=lambda: time.time())
    last_active: float = field(default_factory=lambda: time.time())
    persona_tone: str = "default"
    history: deque = field(default_factory=lambda: deque(maxlen=200))
    ttl_seconds: int = 3600
    metadata: Dict[str, Any] = field(default_factory=dict)

    def touch(self):
        self.last_active = time.time()

    def is_expired(self) -> bool:
        return (time.time() - self.last_active) > self.ttl_seconds

@dataclass
class InboundMessage:
    channel: str
    content: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    timestamp: float = field(default_factory=lambda: time.time())
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class OutboundMessage:
    channel: str
    content: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    timestamp: float = field(default_factory=lambda: time.time())
    metadata: Dict[str, Any] = field(default_factory=dict)


# ---------- InteractionCore ----------
class InteractionCore:
    DEFAULT_CONFIG = {
        "session_ttl": 3600,
        "rate_limit_per_minute": 60,
        "dedup_window_seconds": 10,
        "response_cache_ttl": 300,
        "audit_event_key": "cognition_audit",
        "autosave_interval": 60,
        # "max_llm_retries": 3, # <-- Obsolete, moved to CognitionCore/LLMAdapter
        # "llm_backoff_base": 0.5, # <-- Obsolete
        "hmac_audit_key": None,
        "rate_limiter_mode": "sliding",
        "token_bucket_capacity": 60,
        "token_bucket_refill_rate_per_minute": 60,
        # "primary_provider": "groq", # <-- Obsolete, moved to LLMAdapter
        "proactive_poll_interval": 1.0,
        "proactive_persist_key": "interaction_proactive_queue_v1",
        "session_persist_key": "interaction_sessions_v1",
        "proactive_retry_base": 3.0,
        "proactive_max_attempts": 5,
        "allow_proactive_by_default": False,
    }

    def __init__(self, persona: Any = None, cognition: Any = None, autonomy: Any = None, config: Optional[Dict[str, Any]] = None):
        # components
        self.persona = persona
        self.cognition = cognition
        self.autonomy = autonomy
        self.config = {**self.DEFAULT_CONFIG, **(config or {})}

        # security orchestrator
        try:
            self.security_orchestrator = self.config.get("security_orchestrator") or (getattr(self.persona, "core", None) and getattr(self.persona.core, "security_orchestrator", None))
        except Exception:
            self.security_orchestrator = self.config.get("security_orchestrator", None)

        if self.security_orchestrator:
            logger.info("🔐 Security orchestrator integrated into InteractionCore")
        else:
            logger.warning("⚠️ No security orchestrator provided, running in unsecured mode")

        # state
        self.sessions: Dict[str, Session] = {}
        self.outbound_queue: deque = deque()
        self._session_lock = asyncio.Lock()
        self._queue_lock = asyncio.Lock()
        self._rate_lock = asyncio.Lock()

        # dedupe / cache
        self._recent_requests: Dict[str, float] = {}
        self._response_cache: Dict[str, Tuple[float, str]] = {}

        # rate-limiting structures
        self._user_rate_windows: Dict[str, deque] = {}
        self._token_buckets: Dict[str, Dict[str, float]] = {}

        # --- [DELETED] ---
        # self.intent_patterns = { ... }
        # self.privileged_actions = [ ... ]
        # --- [END DELETED] ---

        # audit logger (optional)
        self.audit_logger = None
        try:
            self.audit_logger = getattr(self.persona.core, "audit_logger", None) if self.persona and hasattr(self.persona, "core") else None
        except Exception:
            self.audit_logger = None

        # background control
        self._stop_event = asyncio.Event()
        self._background_tasks: List[asyncio.Task] = []

        # proactive persistence + in-memory inflight
        self._proactive_queue: List[Dict[str, Any]] = []
        self._proactive_inflight: deque = deque()
        self._proactive_lock = asyncio.Lock()

        # deliver hook
        self.deliver_hook: Optional[Callable[[OutboundMessage], Any]] = None

        # persistence bookkeeping
        self._last_session_persist = 0.0
        self._last_proactive_persist = 0.0

        logger.info("InteractionCore instance constructed (call await start() to run background workers)")

    # ---------------- Lifecycle (remains the same) ----------------
    async def start(self):
        """Start background workers and load persisted state. Caller must await this."""
        if self._background_tasks:
            logger.debug("InteractionCore already started")
            return
        await self._load_persisted_sessions()
        await self._load_persisted_proactive_queue()
        autosave_interval = float(self.config.get("autosave_interval", 60))
        poll_interval = float(self.config.get("proactive_poll_interval", 1.0))
        self._stop_event.clear()
        self._background_tasks.append(asyncio.create_task(self._autosave_worker(autosave_interval)))
        self._background_tasks.append(asyncio.create_task(self._outbound_worker()))
        self._background_tasks.append(asyncio.create_task(self._proactive_worker(poll_interval)))
        logger.info("Async InteractionCore started with %d background tasks", len(self._background_tasks))

    async def stop(self, wait_seconds: float = 5.0):
        """Stop background workers and persist sessions/proactive queue."""
        logger.info("InteractionCore stopping")
        self._stop_event.set()
        for t in list(self._background_tasks):
            with suppress(Exception):
                t.cancel()
        if self._background_tasks:
            try:
                await asyncio.wait(self._background_tasks, timeout=wait_seconds)
            except Exception:
                pass
        self._background_tasks.clear()
        await self._persist_sessions(force=True)
        await self._persist_proactive_queue(force=True)
        logger.info("InteractionCore stopped")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    # ---------------- Core helpers (remains the same) ----------------
    def _get_core(self) -> Optional[Any]:
        """Try to find a core-like object with .store / .notify to integrate with."""
        for comp in (self.persona, self.cognition, self.autonomy):
            try:
                if hasattr(comp, "core"):
                    return comp.core
            except Exception:
                continue
        for comp in (self.persona, self.cognition, self.autonomy):
            try:
                if comp and (hasattr(comp, "save_user_profile") or hasattr(comp, "store")):
                    return comp
            except Exception:
                continue
        return None

    # ---------------- Persistence: sessions (remains the same) ----------------
    async def _load_persisted_sessions(self):
        core = self._get_core()
        key = self.config.get("session_persist_key", "interaction_sessions_v1")
        if not core:
            logger.debug("No core available to load persisted sessions")
            return
        store = getattr(core, "store", None) or getattr(core, "secure_store", None)
        if not store:
            logger.debug("No store found on core to load sessions")
            return
        try:
            get = getattr(store, "get", None)
            maybe = get(key) if get else store.get(key) if hasattr(store, "get") else None
            data = await maybe if asyncio.iscoroutine(maybe) else maybe
            if not data or not isinstance(data, dict):
                logger.debug("No persisted session payload found")
                return
            loaded = 0
            async with self._session_lock:
                for sid, sess_data in data.items():
                    if not isinstance(sess_data, dict):
                        continue
                    s = Session(
                        session_id=sid,
                        user_id=sess_data.get("user_id"),
                        persona_tone=sess_data.get("persona_tone", "default"),
                        ttl_seconds=int(self.config.get("session_ttl", 3600)),
                        created_at=sess_data.get("created_at", time.time())
                    )
                    s.last_active = sess_data.get("last_active", s.last_active)
                    hist = sess_data.get("history", [])
                    s.history = deque(hist if isinstance(hist, list) else [], maxlen=200)
                    s.metadata = sess_data.get("metadata", {}) or {}
                    self.sessions[sid] = s
                    loaded += 1
            if PROMETHEUS_AVAILABLE:
                metrics["active_sessions"].set(len(self.sessions))
            logger.info("Loaded %d persisted sessions", loaded)
        except Exception as e:
            logger.debug("Failed loading persisted sessions: %s", e, exc_info=True)

    async def _persist_sessions(self, force: bool = False):
        core = self._get_core()
        key = self.config.get("session_persist_key", "interaction_sessions_v1")
        if not core:
            logger.debug("No core available to persist sessions")
            return
        store = getattr(core, "store", None) or getattr(core, "secure_store", None)
        if not store:
            logger.debug("No store available to persist sessions")
            return
        now = time.time()
        autosave_interval = float(self.config.get("autosave_interval", 60))
        if not force and now - self._last_session_persist < max(0.5, autosave_interval / 2):
            return
        get_put = getattr(store, "put", None)
        payload: Dict[str, Any] = {}
        async with self._session_lock:
            for sid, sess in list(self.sessions.items()):
                if sess.is_expired():
                    continue
                clean_history = []
                for h in list(sess.history):
                    try:
                        json.dumps(h)
                        clean_history.append(h)
                    except Exception:
                        clean_history.append(str(h))
                payload[sid] = {
                    "user_id": sess.user_id,
                    "created_at": sess.created_at,
                    "last_active": sess.last_active,
                    "persona_tone": sess.persona_tone,
                    "history": clean_history,
                    "metadata": self._serialize_metadata(sess.metadata),
                }
        try:
            if get_put:
                try:
                    maybe = get_put(key, "session_index", payload)
                except TypeError:
                    maybe = get_put(key, payload)
                if asyncio.iscoroutine(maybe):
                    await maybe
            elif hasattr(store, "__setitem__"):
                store[key] = payload
            self._last_session_persist = now
            logger.debug("Persisted %d sessions", len(payload))
        except Exception as e:
            logger.warning("Failed to persist sessions: %s", e, exc_info=True)

    # ---------------- Session APIs (remains the same) ----------------
    async def create_session(self, user_id: Optional[str] = None, persona_tone: str = "default", ttl_seconds: Optional[int] = None) -> Session:
        sid = f"sess_{uuid.uuid4().hex[:12]}"
        ttl = int(ttl_seconds if ttl_seconds is not None else self.config.get("session_ttl", 3600))
        s = Session(session_id=sid, user_id=user_id, persona_tone=persona_tone, ttl_seconds=ttl)
        async with self._session_lock:
            self.sessions[sid] = s
            if PROMETHEUS_AVAILABLE:
                metrics["active_sessions"].set(len(self.sessions))
        logger.debug("Created session %s (user=%s)", sid, user_id)
        return s

    async def get_session(self, session_id: Optional[str]) -> Optional[Session]:
        if not session_id:
            return None
        async with self._session_lock:
            s = self.sessions.get(session_id)
            if not s:
                return None
            if s.is_expired():
                self.sessions.pop(session_id, None)
                if PROMETHEUS_AVAILABLE:
                    metrics["active_sessions"].set(len(self.sessions))
                return None
            s.touch()
            return s

    async def end_session(self, session_id: str) -> bool:
        async with self._session_lock:
            removed = self.sessions.pop(session_id, None) is not None
            if PROMETHEUS_AVAILABLE:
                metrics["active_sessions"].set(len(self.sessions))
        logger.debug("Ended session %s (removed=%s)", session_id, removed)
        return removed

    async def cleanup_sessions(self) -> int:
        removed = 0
        async with self._session_lock:
            for sid in list(self.sessions.keys()):
                if self.sessions[sid].is_expired():
                    self.sessions.pop(sid, None)
                    removed += 1
            if PROMETHEUS_AVAILABLE:
                metrics["active_sessions"].set(len(self.sessions))
        logger.debug("Cleaned up %d expired sessions", removed)
        return removed

    # ---------------- Dedup / Rate limit (remains the same) ----------------
    def _hash_request(self, inbound: InboundMessage) -> str:
        key = f"{inbound.user_id}|{inbound.channel}|{inbound.content}"
        return hashlib.sha256(key.encode()).hexdigest()

    async def _is_duplicate(self, inbound: InboundMessage) -> bool:
        h = self._hash_request(inbound)
        now = time.time()
        last = self._recent_requests.get(h)
        if last and now - last < float(self.config.get("dedup_window_seconds", 10)):
            return True
        self._recent_requests[h] = now
        cutoff = now - (float(self.config.get("dedup_window_seconds", 10)) * 3)
        for k, v in list(self._recent_requests.items()):
            if v < cutoff:
                self._recent_requests.pop(k, None)
        return False

    async def _check_rate_limit(self, inbound: InboundMessage) -> Tuple[bool, Optional[str]]:
        mode = self.config.get("rate_limiter_mode", "sliding")
        if mode == "token_bucket":
            user = inbound.user_id or "anon"
            return await self._token_bucket_check(user)
        return await self._sliding_window_check(inbound)

    async def _sliding_window_check(self, inbound: InboundMessage) -> Tuple[bool, Optional[str]]:
        user = inbound.user_id or "anon"
        limit = int(self.config.get("rate_limit_per_minute", 60))
        window_seconds = 60
        now = time.time()
        async with self._rate_lock:
            dq = self._user_rate_windows.setdefault(user, deque())
            while dq and now - dq[0] > window_seconds:
                dq.popleft()
            if len(dq) >= limit:
                if PROMETHEUS_AVAILABLE:
                    try:
                        metrics["rate_limited"].labels(inbound.channel).inc()
                    except Exception:
                        pass
                return False, f"Rate limit exceeded: {limit}/minute"
            dq.append(now)
        return True, None

    async def _token_bucket_check(self, user: str) -> Tuple[bool, Optional[str]]:
        cfg = self.config
        cap = float(cfg.get("token_bucket_capacity", 60))
        refill_per_min = float(cfg.get("token_bucket_refill_rate_per_minute", 60))
        refill_per_sec = refill_per_min / 60.0
        now = time.time()
        async with self._rate_lock:
            bucket = self._token_buckets.setdefault(user, {"tokens": cap, "last_refill": now})
            elapsed = now - bucket["last_refill"]
            bucket["tokens"] = min(cap, bucket["tokens"] + elapsed * refill_per_sec)
            bucket["last_refill"] = now
            if bucket["tokens"] < 1.0:
                if PROMETHEUS_AVAILABLE:
                    try:
                        metrics["rate_limited"].labels("token_bucket").inc()
                    except Exception:
                        pass
                return False, "Rate limit exceeded (token bucket)"
            bucket["tokens"] -= 1.0
        return True, None

    # ---------------- Intent & Privilege Helpers (DELETED) ----------------
    # def classify_intent(self, message: str) -> str: ...
    # def _contains_privileged_action(self, content: str) -> bool: ...
    # async def is_privileged(self, ...) -> bool: ...

    # ---------------- LLM helper (DELETED) ----------------
    # async def _call_llm_with_retries(self, ...) -> str: ...

    # ---------------- Persona prompt & caching (caching remains) ----------------
    # async def _build_persona_prompt(self, ...) -> List[Dict[str, str]]: ...
    
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

    # ---------------- Audit (remains the same) ----------------
    def _sign_audit_payload(self, payload: Dict[str, Any]) -> Optional[str]:
        key_b64 = self.config.get("hmac_audit_key")
        if not key_b64:
            return None
        try:
            key = base64.b64decode(key_b64)
            serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            sig = hmac.new(key, serialized, digestmod=hashlib.sha256).digest()
            return base64.b64encode(sig).decode("utf-8")
        except Exception as e:
            logger.warning("Failed to sign audit payload: %s", e)
            return None

    async def _audit(self, event: str, request: Dict[str, Any], response: Dict[str, Any], trace_id: Optional[str] = None):
        def _make_json_safe(obj):
            if obj is None or isinstance(obj, (str, int, float, bool)):
                return obj
            if isinstance(obj, (bytes, bytearray)):
                return {"__bytes_b64": base64.b64encode(bytes(obj)).decode("ascii")}
            if isinstance(obj, datetime):
                try:
                    return obj.astimezone(timezone.utc).isoformat()
                except Exception:
                    return obj.isoformat()
            if isinstance(obj, dict):
                return {str(k): _make_json_safe(v) for k, v in obj.items()}
            if isinstance(obj, (list, tuple, set, deque)):
                return [_make_json_safe(x) for x in obj]
            if hasattr(obj, "__dict__"):
                try:
                    return _make_json_safe(vars(obj))
                except Exception:
                    return str(obj)
            try:
                return json.loads(json.dumps(obj, default=str))
            except Exception:
                return str(obj)

        try:
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": event,
                "request": request or {},
                "response": response or {},
                "trace_id": trace_id or f"trace_{uuid.uuid4().hex[:10]}",
                "app": "interaction_core",
                "runtime_ts": int(time.time())
            }
            signature = self._sign_audit_payload(payload)
            if signature:
                payload["hmac"] = signature
            safe_obj = _make_json_safe(payload)

            # try audit_logger first
            try:
                if self.audit_logger:
                    ainfo = getattr(self.audit_logger, "ainfo", None)
                    if ainfo and asyncio.iscoroutinefunction(ainfo):
                        await ainfo("interaction_audit", extra={"audit": safe_obj})
                        return safe_obj
                    if ainfo:
                        try:
                            ainfo("interaction_audit", extra={"audit": safe_obj})
                            return safe_obj
                        except Exception:
                            pass
                    info = getattr(self.audit_logger, "info", None)
                    if info:
                        if asyncio.iscoroutinefunction(info):
                            await info("interaction_audit", extra={"audit": safe_obj})
                        else:
                            info("interaction_audit", extra={"audit": safe_obj})
                        return safe_obj
            except Exception:
                logger.debug("Audit logger emission failed (continuing)", exc_info=True)

            # fallback to core.store
            core = self._get_core()
            if core:
                store = getattr(core, "store", None) or getattr(core, "secure_store", None)
                if store and hasattr(store, "put"):
                    keyname = f"{self.config.get('audit_event_key','interaction_audit')}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
                    put = getattr(store, "put")
                    try:
                        maybe = put(keyname, "interaction_audit", safe_obj)
                    except TypeError:
                        maybe = put(keyname, safe_obj)
                    if asyncio.iscoroutine(maybe):
                        await maybe
                    return safe_obj

            # final fallback: compact log
            try:
                compact = json.dumps(safe_obj, separators=(",", ":"), ensure_ascii=False)
                logger.info("AUDIT_FALLBACK: %s", compact)
                return safe_obj
            except Exception:
                logger.warning("Audit final fallback failed")
                return None
        except Exception as e:
            logger.warning("Audit failed unexpectedly: %s", e, exc_info=True)
            return None

    # ---------------- Security quick path (DELETED) ----------------
    # async def _process_security_commands_directly(self, ...) -> Optional[OutboundMessage]: ...

    # ---------------- Main inbound processing (UPGRADED) ----------------
    async def handle_inbound(self, inbound: InboundMessage) -> OutboundMessage:
        """
        [UPGRADED]
        This handler is now streamlined. All messages, whether chat or command,
        are sent to _process_normal_message, which contains the new agentic router.
        """
        # The old logic for a separate security path is GONE.
        # All messages are processed intelligently.
        return await self._process_normal_message(inbound)

    async def _process_normal_message(self, inbound: InboundMessage) -> OutboundMessage:
        """
        [UPGRADED]
        This method is now the core "Executor" for the Agentic Router.
        It gets a plan from CognitionCore and executes the specified tool calls.
        """
        trace_id = f"trace_{uuid.uuid4().hex[:10]}"
        request_hash = self._hash_request(inbound)
        node_id = f"input_{trace_id}"
        link_id = f"link_{node_id}"

        # optional hologram spawn
        if hologram_state is not None:
            try:
                spawn = getattr(hologram_state, "spawn_and_link", None)
                if callable(spawn):
                    r = spawn(node_id=node_id, node_type="input", label=f"Input: {inbound.content[:20]}...", size=5, source_id="PersonaCore", link_id=link_id)
                    if asyncio.iscoroutine(r):
                        await r
                s = getattr(hologram_state, "set_node_active", None)
                if callable(s):
                    r2 = s("PersonaCore")
                    if asyncio.iscoroutine(r2):
                        await r2
            except Exception:
                logger.debug("Hologram spawn failed (non-fatal)")

        try:
            # dedup
            if await self._is_duplicate(inbound):
                cached = self._get_cached_response(request_hash)
                if cached:
                    resp = OutboundMessage(channel=inbound.channel, content=cached, user_id=inbound.user_id, session_id=inbound.session_id)
                    await self._audit("reason_cached", {"query": inbound.content, "context": inbound.metadata}, {"response": cached}, trace_id)
                    return resp

            # rate limit
            ok, reason = await self._check_rate_limit(inbound)
            if not ok:
                fallback = await self._get_rate_limit_fallback(inbound.content)
                out = OutboundMessage(channel=inbound.channel, content=fallback, user_id=inbound.user_id, session_id=inbound.session_id)
                await self._audit("rate_limited", {"query": inbound.content, "reason": reason}, {"response": fallback}, trace_id)
                return out

            # --- [SECURITY FLOW] ---
            # Security flow is STILL ESSENTIAL. It runs *before* cognition
            # to establish the identity and context for the "Brain."
            security_context = None
            identity_profile = None
            if self.security_orchestrator:
                try:
                    request_data = {
                        "query": inbound.content,
                        "user_id": inbound.user_id,
                        "source": inbound.channel,
                        "device_id": inbound.metadata.get("device_id", "unknown"),
                        "session_token": inbound.metadata.get("session_token"),
                        "biometric_data": inbound.metadata.get("biometric_data"),
                        "behavior_profile": inbound.metadata.get("behavior_profile"),
                        "user_name": inbound.metadata.get("user_name")
                    }
                    if hasattr(self.security_orchestrator, "process_security_flow_enhanced"):
                        res = await self.security_orchestrator.process_security_flow_enhanced(request_data)
                    elif hasattr(self.security_orchestrator, "process_security_flow"):
                        res = await self.security_orchestrator.process_security_flow(request_data)
                    else:
                        res = (None, None) # Fallback

                    if isinstance(res, tuple) and len(res) >= 2:
                        security_context, identity_profile = res[0], res[1]
                    else:
                        security_context = getattr(res, "security_context", None)
                        identity_profile = getattr(res, "identity_profile", None)

                except Exception as e:
                    logger.error("Security processing error: %s", e, exc_info=True)
                    # Create a fallback identity so the system can still respond
                    identity_profile = IdentityProfile(identity_id="public_fallback", name="User", preferred_name="User", relationship="public")
                    security_context = SecurityContext(request_source=RequestSource.PUBLIC_API, user_identity=UserIdentity(user_id="public_fallback", name="User", access_level=AccessLevel.PUBLIC, privileges=set()), requested_data_categories=[])
            else:
                # Fallback if no security orchestrator at all
                identity_profile = IdentityProfile(identity_id=inbound.user_id or "public_default", name="User", preferred_name="User", relationship="public")
                security_context = SecurityContext(request_source=RequestSource.PUBLIC_API, user_identity=UserIdentity(user_id=inbound.user_id or "public_default", name="User", access_level=AccessLevel.PUBLIC, privileges=set()), requested_data_categories=[])


            # session resolution
            session = await self.get_session(inbound.session_id) if inbound.session_id else None
            if not session:
                session = await self.create_session(user_id=inbound.user_id)
                inbound.session_id = session.session_id

            # Store the identified user's context in the session
            if security_context and identity_profile:
                session.metadata["security_context"] = security_context
                session.metadata["identity"] = identity_profile
                session.metadata["preferred_name"] = getattr(identity_profile, "preferred_name", None)

            # --- [AGENTIC ROUTER: THINK -> PLAN -> ACT] ---
            start = time.time()

            # 1. THINK (Call CognitionCore to get the plan)
            if not self.cognition:
                 return OutboundMessage(channel=inbound.channel, content="CognitionCore is offline.", user_id=inbound.user_id, session_id=inbound.session_id)

            # Pass the *full* security context to the brain
            cognition_context = {"security": security_context, "identity": identity_profile}
            
            # This is the query to the "Brain"
            execution_plan = await self.cognition.reason(
                inbound.content, # Pass the original, natural language query
                context=cognition_context, 
                reasoning_mode=ReasoningMode.BALANCED 
            )

            latency = time.time() - start
            final_response_text = "Task executed." # Default response, will be overwritten
            plan_succeeded = True

            # 2. ACT (Execute the plan)
            if not execution_plan:
                 execution_plan = [{"tool_name": "chat_response", "params": {"response_text": "I'm not sure how to respond to that."}}]

            # Loop over all steps in the plan (usually just one, but supports multi-step)
            for step in execution_plan:
                tool_name = step.get("tool_name")
                params = step.get("params", {})

                if tool_name == "security_command":
                    command = params.get("command", "")
                    if not command:
                        final_response_text = "❌ Cognitive error: security_command was empty."
                        plan_succeeded = False
                    elif not self.security_orchestrator:
                        final_response_text = "❌ Security system is offline."
                        plan_succeeded = False
                    else:
                        # Execute the security command and get the string response
                        # This re-uses all your existing, robust security logic!
                        final_response_text = await self.security_orchestrator.process_security_command(
                            command, {}, identity_profile # Pass the identified identity
                        )
                
                elif tool_name == "autonomy_action":
                    action_type = params.get("action_type")
                    details = params.get("details", {})
                    if not action_type or not self.autonomy:
                        final_response_text = f"❌ Autonomy system is offline or action type was missing."
                        plan_succeeded = False
                    else:
                        # Enqueue the action for AutonomyCore to handle
                        # This correctly fixes the "Yash's birthday" reminder flow
                        await self.autonomy.enqueue_action(
                            Action(action_type=action_type, details=details, priority=ActionPriority.NORMAL.value)
                        )
                        final_response_text = f"✅ Task '{action_type}' has been scheduled."

                elif tool_name == "chat_response":
                    final_response_text = params.get("response_text", "[No response]")
                
                else:
                    final_response_text = f"❌ Unknown tool specified by Cognition: {tool_name}"
                    plan_succeeded = False

            # --- [END OF AGENTIC ROUTER] ---

            # 3. RESPOND (Finalize and send the result to the user)
            preferred_name = "User"
            if identity_profile and getattr(identity_profile, "preferred_name", None):
                preferred_name = identity_profile.preferred_name
            elif session and session.metadata.get("preferred_name"):
                preferred_name = session.metadata.get("preferred_name")

            # Personalize the *final* response string
            try:
                if preferred_name != "User":
                    # Check for owner/privileged level before replacing "Owner"
                    access_level = getattr(getattr(security_context, "user_identity", None), "access_level", AccessLevel.PUBLIC)
                    if access_level in (AccessLevel.OWNER_ROOT, AccessLevel.OWNER_REMOTE):
                        final_response_text = final_response_text.replace("Owner", preferred_name)
                    final_response_text = final_response_text.replace("User", preferred_name)
            except Exception:
                pass # Non-fatal

            if PROMETHEUS_AVAILABLE:
                try:
                    metrics["response_latency"].labels(inbound.channel).observe(latency)
                    metrics["response_latency_summary"].labels(inbound.channel).observe(latency)
                    # We can get the intent from the plan now!
                    intent_from_plan = execution_plan[0].get("tool_name", "unknown")
                    metrics["requests_total"].labels(inbound.channel, intent_from_plan).inc()
                    status_label = "success" if plan_succeeded else "failure"
                    metrics["responses_total"].labels(inbound.channel, intent_from_plan, status_label).inc()
                except Exception:
                    pass

            safe = final_response_text.strip()
            self._cache_response(request_hash, safe) # Cache the final string

            # record history
            sec_level = "unknown"
            try:
                if security_context and getattr(security_context, "user_identity", None):
                    al = security_context.user_identity.access_level
                    sec_level = getattr(al, "value", None) or getattr(al, "name", None) or str(al)
            except Exception:
                sec_level = "unknown"
                
            session.history.append({"role": "user", "content": inbound.content, "ts": inbound.timestamp, "security_level": sec_level})
            session.history.append({"role": "assistant", "content": safe, "ts": time.time(), "personalized_for": preferred_name})
            session.touch()

            try:
                asyncio.create_task(self._persist_sessions())
            except Exception:
                await self._persist_sessions()

            # The old _interpret_and_execute_actions call is no longer needed.
            # (DELETED)

            out = OutboundMessage(channel=inbound.channel, content=safe, user_id=inbound.user_id, session_id=session.session_id, metadata={
                "security_level": sec_level,
                "personalized": preferred_name != "User",
                "response_latency": latency,
                "executed_plan": execution_plan # Add plan to metadata for debugging
            })

            await self._audit("intent_executed", {"query": inbound.content, "plan": execution_plan}, {"response": safe, "latency": latency, "personalized_for": preferred_name}, trace_id)
            await self.send_outbound(out)
            return out

        except Exception as e:
            logger.exception("Unhandled error in _process_normal_message: %s", e)
            return OutboundMessage(channel=inbound.channel, content="I encountered an error while processing your message. See logs.", user_id=inbound.user_id, session_id=inbound.session_id)
        
        finally:
            # hologram cleanup
            if hologram_state is not None:
                try:
                    sfun = getattr(hologram_state, "set_node_idle", None)
                    if callable(sfun):
                        r = sfun("PersonaCore")
                        if asyncio.iscoroutine(r):
                            await r
                    despawn = getattr(hologram_state, "despawn_and_unlink", None)
                    if callable(despawn):
                        r2 = despawn(node_id, link_id)
                        if asyncio.iscoroutine(r2):
                            await r2
                except Exception:
                    logger.debug("Hologram cleanup failed (non-fatal)")

    # ---------------- Proactive enqueue / persistence (remains the same) ----------------
    async def _enqueue_proactive(self, subject_identity: str, channel: str, payload: Dict[str, Any], deliver_after: float):
        envelope = {
            "id": f"pmsg_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}",
            "subject_identity": subject_identity,
            "channel": channel,
            "payload": payload,
            "created_at": time.time(),
            "deliver_after": float(deliver_after),
            "attempts": 0,
            "max_attempts": int(self.config.get("proactive_max_attempts", 5)),
            "last_error": None
        }
        async with self._proactive_lock:
            self._proactive_queue.append(envelope)
            if envelope["deliver_after"] <= time.time():
                self._proactive_inflight.append(envelope)
        try:
            asyncio.create_task(self._persist_proactive_queue())
        except Exception:
            await self._persist_proactive_queue()

    async def _persist_proactive_queue(self, force: bool = False):
        core = self._get_core()
        key = self.config.get("proactive_persist_key", "interaction_proactive_queue_v1")
        if not core:
            return
        store = getattr(core, "store", None) or getattr(core, "secure_store", None)
        if not store:
            return
        now_ts = time.time()
        if not force and now_ts - self._last_proactive_persist < 1.0:
            return
        try:
            put = getattr(store, "put", None)
            payload = {"queue": list(self._proactive_queue), "ts": now_ts}
            if put:
                try:
                    maybe = put(key, "proactive", payload)
                except TypeError:
                    maybe = put(key, payload)
                if asyncio.iscoroutine(maybe):
                    await maybe
            elif hasattr(store, "__setitem__"):
                store[key] = payload
            self._last_proactive_persist = now_ts
            logger.debug("Persisted proactive queue (count=%d)", len(self._proactive_queue))
        except Exception as e:
            logger.exception("Failed to persist proactive queue: %s", e)

    async def _load_persisted_proactive_queue(self):
        core = self._get_core()
        key = self.config.get("proactive_persist_key", "interaction_proactive_queue_v1")
        if not core:
            logger.debug("No core to load proactive queue from")
            return
        store = getattr(core, "store", None) or getattr(core, "secure_store", None)
        if not store:
            logger.debug("No store available to load proactive queue")
            return
        try:
            get = getattr(store, "get", None)
            maybe = get(key) if get else store.get(key) if hasattr(store, "get") else None
            data = await maybe if asyncio.iscoroutine(maybe) else maybe
            if data and isinstance(data, dict) and "queue" in data:
                async with self._proactive_lock:
                    self._proactive_queue = data.get("queue", []) or []
                    now = time.time()
                    for env in list(self._proactive_queue):
                        if env.get("deliver_after", 0) <= now:
                            self._proactive_inflight.append(env)
                logger.info("Loaded persisted proactive queue (count=%d)", len(self._proactive_queue))
        except Exception as e:
            logger.exception("Failed to load persisted proactive queue: %s", e)

    # ---------------- Proactive worker (remains the same) ----------------
    async def _proactive_worker(self, poll_interval: float = 1.0):
        retry_base = float(self.config.get("proactive_retry_base", 3.0))
        max_attempts = int(self.config.get("proactive_max_attempts", 5))
        while not self._stop_event.is_set():
            try:
                now = time.time()
                async with self._proactive_lock:
                    self._proactive_queue.sort(key=lambda e: e.get("deliver_after", 0))
                    while self._proactive_queue and self._proactive_queue[0].get("deliver_after", 0) <= now:
                        env = self._proactive_queue.pop(0)
                        self._proactive_inflight.append(env)

                if self._proactive_inflight:
                    env = self._proactive_inflight.popleft()
                    try:
                        out = OutboundMessage(channel=env.get("channel", "push"), content=env.get("payload", {}).get("text", ""), user_id=env.get("subject_identity"))
                        delivered = await self._deliver_message_async(out)
                        env["attempts"] = env.get("attempts", 0) + 1
                        if delivered:
                            await self._audit("proactive_sent", {"envelope_id": env.get("id"), "payload_preview": str(env.get("payload", {}))[:120]}, {"status": "delivered"})
                            async with self._proactive_lock:
                                self._proactive_queue = [e for e in self._proactive_queue if e.get("id") != env.get("id")]
                            await self._persist_proactive_queue()
                        else:
                            env["last_error"] = "delivery_failed"
                            if env.get("attempts", 0) >= max_attempts:
                                await self._audit("proactive_failed_final", {"envelope_id": env.get("id")}, {"status": "final_failure"})
                                async with self._proactive_lock:
                                    self._proactive_queue = [e for e in self._proactive_queue if e.get("id") != env.get("id")]
                                await self._persist_proactive_queue()
                            else:
                                backoff = retry_base * (2 ** (env["attempts"] - 1))
                                env["deliver_after"] = time.time() + backoff + (0.5 * (uuid.uuid4().int & 0xff) / 255.0)
                                async with self._proactive_lock:
                                    self._proactive_queue.append(env)
                                await self._persist_proactive_queue()
                                await self._audit("proactive_retry_scheduled", {"envelope_id": env.get("id"), "next_attempt_at": env["deliver_after"]}, {"attempts": env["attempts"]})
                    except Exception as e:
                        logger.exception("Proactive delivery error: %s", e)
                        env["attempts"] = env.get("attempts", 0) + 1
                        env["last_error"] = str(e)
                        if env.get("attempts", 0) >= max_attempts:
                            await self._audit("proactive_failed_final", {"envelope_id": env.get("id")}, {"status": "final_failure", "error": str(e)})
                            async with self._proactive_lock:
                                self._proactive_queue = [e for e in self._proactive_queue if e.get("id") != env.get("id")]
                            await self._persist_proactive_queue()
                        else:
                            backoff = retry_base * (2 ** (env["attempts"] - 1))
                            env["deliver_after"] = time.time() + backoff
                            async with self._proactive_lock:
                                self._proactive_queue.append(env)
                            await self._persist_proactive_queue()
                else:
                    await asyncio.sleep(poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Proactive worker encountered exception: %s", e)
                await asyncio.sleep(1.0)
        logger.debug("Proactive worker exiting")

    # ---------------- Calendar & Contacts persistence (remains the same) ----------------
    # This logic is now DEPRECATED because the agent will call
    # autonomy.enqueue_action("calendar.add", ...), but we can leave it
    # as a fallback for the (now-deleted) _interpret_and_execute_actions
    async def _persist_calendar_event(self, event_obj: Dict[str, Any]):
        core = self._get_core()
        if not core: return
        store = getattr(core, "store", None) or getattr(core, "secure_store", None)
        if not store or not hasattr(store, "get") or not hasattr(store, "put"): return
        key = "calendar_events"
        try:
            get = getattr(store, "get")
            existing = await get(key) if asyncio.iscoroutinefunction(get) else get(key)
            if not existing: existing = []
            existing.append(event_obj)
            put = getattr(store, "put")
            try:
                maybe = put(key, "calendar", existing)
            except TypeError:
                maybe = put(key, existing)
            if asyncio.iscoroutine(maybe): await maybe
            await self._audit("calendar_event_persisted", {"event": event_obj}, {"status": "ok"})
        except Exception as e:
            logger.exception("Failed to persist calendar event: %s", e)

    async def _persist_contact(self, contact_obj: Dict[str, Any]):
        core = self._get_core()
        if not core: return
        store = getattr(core, "store", None) or getattr(core, "secure_store", None)
        if not store or not hasattr(store, "get") or not hasattr(store, "put"): return
        key = "contacts"
        try:
            get = getattr(store, "get")
            existing = await get(key) if asyncio.iscoroutinefunction(get) else get(key)
            if not existing: existing = []
            existing.append(contact_obj)
            put = getattr(store, "put")
            try:
                maybe = put(key, "contacts", existing)
            except TypeError:
                maybe = put(key, existing)
            if asyncio.iscoroutine(maybe): await maybe
            await self._audit("contact_persisted", {"contact": contact_obj}, {"status": "ok"})
        except Exception as e:
            logger.exception("Failed to persist contact: %s", e)

    # ---------------- Outbound delivery (remains the same) ----------------
    async def send_outbound(self, message: OutboundMessage):
        async with self._queue_lock:
            self.outbound_queue.append(message)
            if PROMETHEUS_AVAILABLE:
                try:
                    metrics["queue_size"].set(len(self.outbound_queue))
                except Exception:
                    pass
        logger.debug("Queued outbound message for user %s on channel %s", message.user_id, message.channel)

    async def _outbound_worker(self):
        while not self._stop_event.is_set():
            try:
                if not self.outbound_queue:
                    await asyncio.sleep(0.05)
                    continue
                async with self._queue_lock:
                    msg = self.outbound_queue.popleft()
                    if PROMETHEUS_AVAILABLE:
                        try:
                            metrics["queue_size"].set(len(self.outbound_queue))
                        except Exception:
                            pass
                delivered = await self._deliver_message_async(msg)
                logger.debug("Delivered message to %s (delivered=%s)", msg.channel, delivered)
            except asyncio.CancelledError:
                logger.info("Outbound worker cancelled; exiting")
                break
            except Exception:
                logger.exception("Outbound worker error", exc_info=True)
                await asyncio.sleep(0.5)

    async def _deliver_message_async(self, message: OutboundMessage) -> bool:
        # 1) deliver_hook
        if callable(self.deliver_hook):
            try:
                maybe = self.deliver_hook(message)
                if asyncio.iscoroutine(maybe):
                    await maybe
                return True
            except Exception:
                logger.debug("deliver_hook failed", exc_info=True)
        # 2) core.notify
        core = self._get_core()
        if core and hasattr(core, "notify"):
            try:
                notify = getattr(core, "notify")
                if asyncio.iscoroutinefunction(notify):
                    await notify(channel=message.channel, message=message.content)
                else:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, notify, message.channel, message.content)
                return True
            except Exception:
                logger.debug("core.notify failed", exc_info=True)
        # 3) persona.notify
        if self.persona and hasattr(self.persona, "notify"):
            try:
                maybe = self.persona.notify(message)
                if asyncio.iscoroutine(maybe):
                    await maybe
                return True
            except Exception:
                logger.debug("persona.notify failed", exc_info=True)
        # 4) fallback
        try:
            await self._audit("outbound_deliver", {"channel": message.channel, "content_preview": message.content[:120]}, {"status": "logged"})
            return True
        except Exception:
            logger.warning("Failed to log outbound deliver as fallback", exc_info=True)
            return False

    # ---------------- Autosave (remains the same) ----------------
    async def _autosave_worker(self, interval_sec: float):
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(interval_sec)
                await self._persist_sessions()
                await self._persist_proactive_queue()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.debug("Autosave worker error", exc_info=True)

    # ---------------- Serialization helper (remains the same) ----------------
    def _serialize_metadata(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        def _safe(v):
            if v is None or isinstance(v, (str, int, float, bool)):
                return v
            if isinstance(v, (bytes, bytearray)):
                return {"__bytes_base64": base64.b64encode(bytes(v)).decode("ascii")}
            if isinstance(v, datetime):
                return v.astimezone(timezone.utc).isoformat()
            if hasattr(v, "value") and isinstance(getattr(v, "value"), (str, int, float, bool)):
                return getattr(v, "value")
            if hasattr(v, "name") and hasattr(v, "preferred_name"):
                try:
                    return {
                        "identity_id": getattr(v, "identity_id", getattr(v, "user_id", None)),
                        "name": getattr(v, "name", None),
                        "preferred_name": getattr(v, "preferred_name", None),
                        "relationship": getattr(v, "relationship", None)
                    }
                except Exception:
                    return str(v)
            if isinstance(v, dict):
                return {str(k): _safe(val) for k, val in v.items()}
            if isinstance(v, (list, tuple, set, deque)):
                return [_safe(x) for x in v]
            try:
                return json.loads(json.dumps(v, default=str))
            except Exception:
                return str(v)
        out = {}
        for k, v in (metadata.items() if isinstance(metadata, dict) else []):
            try:
                out[str(k)] = _safe(v)
            except Exception:
                out[str(k)] = str(v)
        return out

    # ---------------- Name handling & security helpers (DELETED) ----------------
    # async def _handle_set_name_directly(self, ...) -> OutboundMessage: ...
    # async def _process_access_command(self, ...) -> OutboundMessage: ...
    # async def _process_identity_command(self, ...) -> OutboundMessage: ...
    # def _parse_security_params(self, ...) -> Dict[str, Any]: ...

    # ---------------- Fallback helpers (remains the same) ----------------
    async def _get_rate_limit_fallback(self, query: str) -> str:
        if hasattr(self.cognition, "_get_rate_limit_fallback"):
            fallback = self.cognition._get_rate_limit_fallback(query)
            if asyncio.iscoroutine(fallback):
                return await fallback
            return fallback
        return "[Rate limited - please try again in a moment]"

    async def _get_fallback_reasoning(self, query: str) -> str:
        if hasattr(self.cognition, "_get_fallback_reasoning"):
            fb = self.cognition._get_fallback_reasoning(query)
            if asyncio.iscoroutine(fb):
                return await fb
            return fb
        return "[Service temporarily unavailable - please try again later]"

    # ---------------- (DELETED) ----------------
    # async def _apply_immediate_personalization(self, ...): ...

    # ---------------- Factory wrapper (remains the same) ----------------
    async def shutdown(self, wait_seconds: float = 5.0):
        await self.stop(wait_seconds)

# ---------------- Factory (remains the same) ----------------
async def create_interaction_core(persona: Any = None, cognition: Any = None, autonomy: Any = None, config: Optional[Dict[str, Any]] = None) -> InteractionCore:
    # wire security orchestrator if available on persona.core and not provided explicitly
    cfg = dict(config or {})
    try:
        if "security_orchestrator" not in cfg and persona and hasattr(persona, "core") and hasattr(persona.core, "security_orchestrator"):
            cfg["security_orchestrator"] = persona.core.security_orchestrator
    except Exception:
        pass
    interaction_core = InteractionCore(persona=persona, cognition=cognition, autonomy=autonomy, config=cfg)
    logger.info("InteractionCore constructed successfully; call await start() to begin workers")
    return interaction_core