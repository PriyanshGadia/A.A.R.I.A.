"""
interaction_core.py - A.A.R.I.A Interaction Layer (Async Production v2.0.2)

Key Updates for Async Compatibility:
- Full async/await architecture throughout
- Integration with updated async core modules (PersonaCore, CognitionCore, AutonomyCore)
- Async session management with proper locking
- Async LLM calls using the new LLM adapter pattern
- Async storage operations for persistence
- Enhanced async rate limiting and deduplication
- Proper async context managers and background tasks
- Compatibility with async AssistantCore and SecureStorage
- Asynchronous prompt generation with real user data
- Consistent use of event loop time source

Refinements in v2.0.2:
- WITH SECURITY INTEGRATION
"""

from __future__ import annotations

import time
import json
import logging
import asyncio
import uuid
import re
import hashlib
import hmac
import base64
import hologram_state
from typing import Any, Dict, Optional, List, Tuple, Callable
from collections import deque, Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

from requests import session

# Import security types - ADD THESE IMPORTS
try:
    from access_control import AccessLevel, RequestSource, UserIdentity
    from identity_manager import IdentityProfile, IdentityState, VerificationMethod
except ImportError:
    # Fallback definitions
    class AccessLevel:
        OWNER_ROOT = "owner_root"
        OWNER_REMOTE = "owner_remote"
        PRIVILEGED = "privileged"
        PUBLIC = "public"
    
    class RequestSource:
        PRIVATE_TERMINAL = "private_terminal"
        REMOTE_TERMINAL = "remote_terminal"
        SOCIAL_APP = "social_app"
        PHONE_CALL = "phone_call"
        PUBLIC_API = "public_api"
    
    class IdentityProfile:
        def __init__(self, identity_id="", name="", preferred_name="", verification_methods=None, 
                     identity_state=None, last_verified=0, verification_expires=0, relationship="public"):
            self.identity_id = identity_id
            self.name = name
            self.preferred_name = preferred_name
            self.verification_methods = verification_methods or set()
            self.identity_state = identity_state
            self.last_verified = last_verified
            self.verification_expires = verification_expires
            self.relationship = relationship
    
    class IdentityState:
        UNVERIFIED = "unverified"
        VERIFIED_TEMPORARY = "verified_temporary"
        VERIFIED_EXTENDED = "verified_extended" 
        VERIFIED_TRUSTED = "verified_trusted"
    
    class VerificationMethod:
        BIOMETRIC_FACIAL = "biometric_facial"
        BIOMETRIC_RETINA = "biometric_retina"
        BIOMETRIC_FINGERPRINT = "biometric_fingerprint"
        TOTP_AUTHENTICATOR = "totp_authenticator"
        HARDWARE_TOKEN = "hardware_token"
        VOICE_PRINT = "voice_print"
        BEHAVIORAL_ANALYSIS = "behavioral_analysis"

    class UserIdentity:
        def __init__(self, user_id="", name="", preferred_name="", access_level=None, 
                     privileges=None, verification_required=False):
            self.user_id = user_id
            self.name = name
            self.preferred_name = preferred_name
            self.access_level = access_level or AccessLevel.PUBLIC
            self.privileges = privileges or set()
            self.verification_required = verification_required

# Optional imports (graceful fallback)
try:
    from prometheus_client import Counter as PromCounter, Histogram, Gauge, Summary
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False

# Import type-hintable dependencies with proper fallbacks
try:
    from persona_core import PersonaCore
except ImportError:
    class PersonaCore:
        pass

try:
    from cognition_core import CognitionCore  
except ImportError:
    class CognitionCore:
        pass

try:
    from autonomy_core import AutonomyCore
except ImportError:
    class AutonomyCore:
        pass

logger = logging.getLogger(__name__)

# -------------------------
# Metrics (optional) - extended
# -------------------------
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
    
# -------------------------
# Dataclasses
# -------------------------
@dataclass
class Session:
    session_id: str
    user_id: Optional[str] = None
    created_at: float = field(default_factory=lambda: asyncio.get_event_loop().time())  # FIXED: Use event loop time
    last_active: float = field(default_factory=lambda: asyncio.get_event_loop().time())  # FIXED: Use event loop time
    persona_tone: str = "default"
    history: deque = field(default_factory=lambda: deque(maxlen=200))
    ttl_seconds: int = 3600  # default 1 hour
    metadata: Dict[str, Any] = field(default_factory=dict)

    def touch(self):
        self.last_active = asyncio.get_event_loop().time()  # FIXED: Use event loop time

    def is_expired(self) -> bool:
        return asyncio.get_event_loop().time() - self.last_active > self.ttl_seconds  # FIXED: Use event loop time

@dataclass
class InboundMessage:
    channel: str  # e.g., 'text', 'voice', 'webhook'
    content: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    timestamp: float = field(default_factory=lambda: asyncio.get_event_loop().time())  # FIXED: Use event loop time
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class OutboundMessage:
    channel: str
    content: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    timestamp: float = field(default_factory=lambda: asyncio.get_event_loop().time())  # FIXED: Use event loop time
    metadata: Dict[str, Any] = field(default_factory=dict)

# -------------------------
# Async Interaction Core
# -------------------------
class InteractionCore:
    """
    Production-grade Async InteractionCore for A.A.R.I.A. (v2.0.1)
    WITH SECURITY INTEGRATION
    """

    DEFAULT_CONFIG = {
        "session_ttl": 3600,
        "rate_limit_per_minute": 60,
        "dedup_window_seconds": 10,
        "response_cache_ttl": 300,
        "audit_event_key": "cognition_audit",
        "autosave_interval": 60,  # seconds
        "max_llm_retries": 3,
        "llm_backoff_base": 0.5,  # seconds
        "hmac_audit_key": None,  # base64 key for signing audit payloads
        "rate_limiter_mode": "sliding",  # or 'token_bucket'
        "token_bucket_capacity": 60,
        "token_bucket_refill_rate_per_minute": 60
    }

    def __init__(self, persona: PersonaCore, cognition: CognitionCore, autonomy: AutonomyCore, config: Optional[Dict[str, Any]] = None):
        self.persona = persona
        self.cognition = cognition
        self.autonomy = autonomy
        self.config = {**self.DEFAULT_CONFIG, **(config or {})}

        # NEW: Security orchestrator integration
        self.security_orchestrator = self.config.get("security_orchestrator")
        if self.security_orchestrator:
            logger.info("🔐 Security orchestrator integrated into InteractionCore")
        else:
            logger.warning("⚠️  No security orchestrator provided, running in unsecured mode")

        # Async in-memory stores
        self.sessions: Dict[str, Session] = {}
        self.outbound_queue: deque = deque()
        self._session_lock = asyncio.Lock()
        self._queue_lock = asyncio.Lock()
        self._rate_lock = asyncio.Lock()

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
        self._stop_event = asyncio.Event()
        self._background_tasks: List[asyncio.Task] = []

        # Load persisted sessions on startup
        asyncio.create_task(self._load_persisted_sessions())

        # Start background autosave and outbound worker
        autosave_interval = float(self.config.get("autosave_interval", 60))
        self._background_tasks.append(asyncio.create_task(self._autosave_worker(autosave_interval)))
        self._background_tasks.append(asyncio.create_task(self._outbound_worker()))

        logger.info("Async InteractionCore v2.0.1 initialized")

    # -------------------------
    # Core Access & Utilities
    # -------------------------
    def _get_core(self) -> Optional[Any]:
        """Unified core access with proper error handling."""
        for component in [self.persona, self.cognition, self.autonomy]:
            if hasattr(component, 'core'):
                return component.core
        return None

    async def _load_persisted_sessions(self):
        """Load persisted sessions on startup if available."""
        try:
            core = self._get_core()
            if core and hasattr(core, 'store'):
                logger.debug("Session persistence available - attempting load on startup")
                # Use async storage interface
                try:
                    # Try to get session data from storage
                    session_data = await core.store.get("interaction_sessions")
                    if session_data and isinstance(session_data, dict):
                        for sid, sess_data in session_data.items():
                            if isinstance(sess_data, dict):
                                s = Session(
                                    session_id=sid, 
                                    user_id=sess_data.get('user_id'), 
                                    persona_tone=sess_data.get('persona_tone', 'default'), 
                                    ttl_seconds=self.config['session_ttl']
                                )
                                s.last_active = sess_data.get('last_active', asyncio.get_event_loop().time())  # FIXED: Use event loop time
                                s.history = deque(sess_data.get('history', []), maxlen=200)
                                s.metadata = sess_data.get('metadata', {})
                                async with self._session_lock:
                                    self.sessions[sid] = s
                        
                        if PROMETHEUS_AVAILABLE:
                            metrics['active_sessions'].set(len(self.sessions))
                        logger.info(f"Loaded {len(self.sessions)} persisted sessions")
                except Exception as e:
                    logger.debug(f"No session index found or error loading: {e}")
        except Exception as e:
            logger.debug(f"Session persistence not available or failed: {e}")

    # -------------------------
    # Session Management
    # -------------------------
    async def create_session(self, user_id: Optional[str] = None, persona_tone: str = "default", ttl_seconds: Optional[int] = None) -> Session:
        session_id = f"sess_{uuid.uuid4().hex[:12]}"
        ttl = ttl_seconds or self.config["session_ttl"]
        session = Session(session_id=session_id, user_id=user_id, persona_tone=persona_tone, ttl_seconds=ttl)
        async with self._session_lock:
            self.sessions[session_id] = session
            if PROMETHEUS_AVAILABLE:
                metrics['active_sessions'].set(len(self.sessions))
        logger.debug(f"Created session {session_id} for user {user_id}")
        return session

    async def get_session(self, session_id: Optional[str]) -> Optional[Session]:
        if not session_id:
            return None
        async with self._session_lock:
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

    async def end_session(self, session_id: str) -> bool:
        async with self._session_lock:
            removed = self.sessions.pop(session_id, None) is not None
            if PROMETHEUS_AVAILABLE:
                metrics['active_sessions'].set(len(self.sessions))
        logger.debug(f"Ended session {session_id}: {removed}")
        return removed

    async def cleanup_sessions(self) -> int:
        """Remove expired sessions and return count removed."""
        now = asyncio.get_event_loop().time()  # FIXED: Use event loop time
        removed = 0
        async with self._session_lock:
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

    async def _is_duplicate(self, inbound: InboundMessage) -> bool:
        h = self._hash_request(inbound)
        now = asyncio.get_event_loop().time()  # FIXED: Use event loop time
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

    async def _check_rate_limit(self, inbound: InboundMessage) -> Tuple[bool, Optional[str]]:
        """Support sliding window or token bucket rate limiting."""
        mode = self.config.get('rate_limiter_mode', 'sliding')
        user = inbound.user_id or 'anon'
        if mode == 'token_bucket':
            return await self._token_bucket_check(user)
        return await self._sliding_window_check(inbound)

    async def _sliding_window_check(self, inbound: InboundMessage) -> Tuple[bool, Optional[str]]:
        """Simple per-user sliding window rate limit."""
        user = inbound.user_id or "anon"
        limit = int(self.config.get("rate_limit_per_minute", 60))
        window_seconds = 60
        now = asyncio.get_event_loop().time()  # FIXED: Use event loop time
        async with self._rate_lock:
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

    async def _token_bucket_check(self, user: str) -> Tuple[bool, Optional[str]]:
        cfg = self.config
        cap = float(cfg.get('token_bucket_capacity', 60))
        refill_per_min = float(cfg.get('token_bucket_refill_rate_per_minute', 60))
        refill_per_sec = refill_per_min / 60.0
        now = asyncio.get_event_loop().time()  # FIXED: Use event loop time
        async with self._rate_lock:
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

    async def is_privileged(self, parsed_intent: str, user_id: Optional[str], session: Optional[Session]) -> bool:
        """Check whether the request can trigger privileged actions."""
        if not session or not session.user_id:
            return False
        try:
            profile = await self.persona.core.load_user_profile() or {}
            is_admin = profile.get("is_admin", False) or profile.get("user_id") == session.user_id
            return bool(is_admin)
        except Exception:
            return False

    # -------------------------
    # Async LLM helper: retries + backoff
    # -------------------------
    async def _call_llm_with_retries(self, messages: List[Dict[str, str]], model: Optional[str] = None, temperature: float = 0.2, max_tokens: int = 400) -> str:
        from llm_adapter import LLMAdapterFactory, LLMProvider, LLMRequest, LLMError
        
        max_retries = int(self.config.get('max_llm_retries', 3))
        backoff_base = float(self.config.get('llm_backoff_base', 0.5))
        attempt = 0
        last_exc = None
        
        # FIXED: Use Groq first, then Ollama as fallback
        primary_provider = self.config.get("primary_provider", "groq")
        if primary_provider == "groq":
            providers_to_try = [LLMProvider.GROQ, LLMProvider.OLLAMA]
        else:
            providers_to_try = [LLMProvider.OLLAMA, LLMProvider.GROQ]
        
        for provider in providers_to_try:
            attempt = 0
            while attempt <= max_retries:
                try:
                    async with LLMAdapterFactory.get_adapter(provider) as adapter:
                        request = LLMRequest(
                            messages=messages,
                            temperature=temperature,
                            max_tokens=max_tokens
                        )
                        response = await adapter.chat(request)
                        if response and response.content.strip():
                            return response.content.strip()
                    
                except Exception as e:
                    last_exc = e
                    attempt += 1
                    if attempt > max_retries:
                        break  # Try next provider
                    jitter = asyncio.get_event_loop().time() % 0.1
                    sleep_for = (backoff_base * (2 ** (attempt - 1))) + jitter
                    logger.warning(f"LLM call failed with {provider.value} on attempt {attempt}/{max_retries}: {e}. Backing off {sleep_for:.2f}s")
                    await asyncio.sleep(sleep_for)
        
        # Final fallback to cognition core
        if hasattr(self.cognition, 'reason'):
            try:
                plaintext_prompt = "\n".join([m.get("content", "") for m in messages if m.get("content")])
                return await self.cognition.reason(plaintext_prompt, context={})
            except Exception:
                pass
                
        return "[Service temporarily unavailable - please try again later]"

    # -------------------------
    # Synthesis & Response Generation - FIXED: Now async with real user data
    # -------------------------
    async def _build_persona_prompt(self, user_input: str, session: Optional[Session]) -> List[Dict[str, str]]:
        """
        Asynchronously create a multi-part prompt with real user data.
        """
        persona_meta = getattr(self.persona, "system_prompt", "You are A.A.R.I.A.")
        user_meta = {}
        try:
            # CORRECTED: Await the async call to get the user profile
            profile = await self.persona.core.load_user_profile() or {}
            user_meta = {
                "name": profile.get("name", "User"),
                "timezone": profile.get("timezone", "UTC")
            }
        except Exception as e:
            logger.warning(f"Failed to load user profile for prompt: {e}")
            user_meta = {"name": "User"}  # Fallback

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
        expiry = asyncio.get_event_loop().time() + int(self.config.get("response_cache_ttl", 300))  # FIXED: Use event loop time
        self._response_cache[request_hash] = (expiry, response)

    def _get_cached_response(self, request_hash: str) -> Optional[str]:
        entry = self._response_cache.get(request_hash)
        if not entry:
            return None
        expiry, response = entry
        if asyncio.get_event_loop().time() > expiry:  # FIXED: Use event loop time
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

    async def _audit(self, event: str, request: Dict[str, Any], response: Dict[str, Any], trace_id: Optional[str] = None):
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
                if hasattr(self.audit_logger, 'ainfo'):
                    await self.audit_logger.ainfo("interaction_audit", extra={"audit": payload_with_sig})
                else:
                    self.audit_logger.info("interaction_audit", extra={"audit": payload_with_sig})
            else:
                core = self._get_core()
                if core and hasattr(core, "store"):
                    key = f"{self.config['audit_event_key']}_{int(asyncio.get_event_loop().time())}_{uuid.uuid4().hex[:6]}"  # FIXED: Use event loop time
                    await core.store.put(key, "interaction_audit", payload_with_sig)
                else:
                    logger.info("AUDIT_LOG: interaction_audit %s", json.dumps(payload_with_sig))
        except Exception as e:
            logger.warning(f"Audit logging failed: {e}")

# -------------------------
# Handle Inbound
# -------------------------
    async def handle_inbound(self, inbound: InboundMessage) -> OutboundMessage:
        """Process inbound message with proper security command handling"""
        
        # INTERCEPT SECURITY COMMANDS BEFORE LLM PROCESSING
        security_response = await self._process_security_commands_directly(inbound)
        if security_response:
            return security_response
        
        # Normal message processing for non-security commands
        return await self._process_normal_message(inbound)

    async def _process_security_commands_directly(self, inbound: InboundMessage) -> Optional[OutboundMessage]:
            """
            Process security commands directly without LLM interference.
            FIXED: Correctly routes 'security status', 'access ...', and 'identity ...'
            """
            content = inbound.content.strip().lower()
            
            # Security command patterns
            security_patterns = [
                r'^security\s+',
                r'^access\s+',
                r'^identity\s+',
                r'^set_name\s+'
            ]
            
            is_security_command = any(re.match(pattern, content) for pattern in security_patterns)
            
            if not is_security_command:
                return None
            
            try:
                # Get security context
                security_context, identity_profile = await self.security_orchestrator.process_security_flow({
                    "query": inbound.content,
                    "user_id": inbound.user_id,
                    "device_id": inbound.metadata.get("device_id", "cli_terminal"),
                    "source": inbound.metadata.get("source", "private_terminal"),
                    "user_name": inbound.metadata.get("user_name")
                })
                
                # --- Direct command processing ---
                
                if content.startswith('set_name '):
                    new_name = inbound.content[9:].strip()
                    return await self._handle_set_name_directly(new_name, identity_profile, inbound)
                    
                elif content.startswith('security '):
                    command_str = content[9:].strip()
                    parts = command_str.split()
                    if not parts:
                        return OutboundMessage(
                            channel=inbound.channel,
                            content="Security commands: status, users, identity",
                            user_id=inbound.user_id,
                            session_id=inbound.session_id
                        )
                    
                    subcommand = parts[0]
                    
                    # --- FIX for 'security status' ---
                    if subcommand == "status":
                        status = await self.security_orchestrator.get_security_status()
                        response = f"🛡️ Security Status: {status['overall_status']}\n"
                        response += f"  • Access Control: {status['access_control']['privileged_users_count']} privileged users, {status['access_control']['trusted_devices_count']} trusted devices\n"
                        response += f"  • Identity: {status['identity_management']['known_identities']} known identities, {status['identity_management']['active_sessions']} active sessions"
                        return OutboundMessage(
                            channel=inbound.channel,
                            content=response,
                            user_id=inbound.user_id,
                            session_id=inbound.session_id
                        )
                    # Route other 'security' prefixed commands if needed
                    # ...
                        
                elif content.startswith('access '):
                    command = content[7:].strip()
                    return await self._process_access_command(command, identity_profile, inbound)
                    
                elif content.startswith('identity '):
                    command = content[9:].strip()
                    return await self._process_identity_command(command, identity_profile, inbound)
                
                # Fallback for unhandled security commands
                return OutboundMessage(
                    channel=inbound.channel,
                    content=f"❌ Unknown security command pattern: {content}",
                    user_id=inbound.user_id,
                    session_id=inbound.session_id
                )
                    
            except Exception as e:
                logger.error(f"Security command processing failed: {e}")
                return OutboundMessage(
                    channel=inbound.channel,
                    content=f"Security command error: {str(e)}",
                    user_id=inbound.user_id,
                    session_id=inbound.session_id
                )

    async def _process_normal_message(self, inbound: InboundMessage) -> OutboundMessage:
        """Process normal (non-security) messages through the standard flow"""
        
        # --- NEW: Hologram Task Tracking ---
        trace_id = f"trace_{uuid.uuid4().hex[:10]}"
        request_hash = self._hash_request(inbound)
        node_id = f"input_{trace_id}"
        link_id = f"link_in_{node_id}"
        # --- End New Block ---

        try:
            # --- NEW: Spawn "Input" node ---
            # This is the FIRST node in a thought-chain
            await hologram_state.spawn_and_link(
                node_id=node_id, 
                node_type="input", 
                label=f"Input: {inbound.content[:20]}...", 
                size=5,
                source_id="PersonaCore", # Inputs feed the Persona
                link_id=link_id
            )
            await hologram_state.set_node_active("PersonaCore")
            # --- End New Block ---

            # Basic deduplication
            if await self._is_duplicate(inbound):
                logger.debug("Duplicate inbound detected, returning cached acknowledgement")
                cached = self._get_cached_response(request_hash)
                if cached:
                    resp = OutboundMessage(channel=inbound.channel, content=cached, user_id=inbound.user_id, session_id=inbound.session_id)
                    await self._audit("reason_cached", {"query": inbound.content, "context": inbound.metadata}, {"response": cached}, trace_id)
                    await hologram_state.set_node_error(node_id) # Set error state
                    return resp
                logger.debug("Duplicate request but no cached response available")

            # Rate limiting
            ok, reason = await self._check_rate_limit(inbound)
            if not ok:
                fallback = await self._get_rate_limit_fallback(inbound.content)
                out = OutboundMessage(channel=inbound.channel, content=fallback, user_id=inbound.user_id, session_id=inbound.session_id)
                await self._audit("rate_limited", {"query": inbound.content, "reason": reason}, {"response": fallback}, trace_id)
                if PROMETHEUS_AVAILABLE:
                    metrics['responses_total'].labels(inbound.channel, "rate_limit", "blocked").inc()
                await hologram_state.set_node_error(node_id) # Set error state
                return out

            # SECURITY PROCESSING
            security_context = None
            identity_profile = None

            try:
                if hasattr(self, 'security_orchestrator'):
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
                    
                    if hasattr(self.security_orchestrator, 'process_security_flow_enhanced'):
                        security_context, identity_profile = await self.security_orchestrator.process_security_flow_enhanced(request_data)
                    else:
                        security_context, identity_profile = await self.security_orchestrator.process_security_flow(request_data)
                    
                    logger.info(f"🔐 Security: {identity_profile.preferred_name} -> {security_context.user_identity.access_level.value} -> Verified: {security_context.is_verified}")

                    if security_context.requested_data_categories:
                        is_authorized, authorized_categories = await self.security_orchestrator.access_control.authorize_data_access(security_context)
                        if not is_authorized:
                            denial_msg = "I'm not authorized to share that information with you."
                            out = OutboundMessage(channel=inbound.channel, content=denial_msg, user_id=inbound.user_id, session_id=inbound.session_id)
                            await self._audit("access_denied", {
                                "query": inbound.content,
                                "requested_categories": security_context.requested_data_categories,
                                "user_level": security_context.user_identity.access_level.value
                            }, {"response": denial_msg}, trace_id)
                            await hologram_state.set_node_error(node_id) # Set error state
                            return out
                            
            except Exception as e:
                logger.error(f"Security processing error: {e}")
                # Continue without security context in case of errors

            # Obtain or create session with security context
            session = await self.get_session(inbound.session_id) if inbound.session_id else None
            if not session:
                session = await self.create_session(user_id=inbound.user_id)
                inbound.session_id = session.session_id

            if security_context and identity_profile:
                session.metadata["security_context"] = security_context
                session.metadata["identity"] = identity_profile
                session.metadata["preferred_name"] = identity_profile.preferred_name

            intent = self.classify_intent(inbound.content)

            if intent == "command" and self._contains_privileged_action(inbound.content):
                if not await self.is_privileged(intent, inbound.user_id, session):
                    denied = "You are not authorized to perform that action."
                    out = OutboundMessage(channel=inbound.channel, content=denied, user_id=inbound.user_id, session_id=session.session_id)
                    await self._audit("privilege_denied", {"query": inbound.content}, {"response": denied}, trace_id)
                    if PROMETHEUS_AVAILABLE:
                        metrics['responses_total'].labels(inbound.channel, intent, "denied").inc()
                    await hologram_state.set_node_error(node_id) # Set error state
                    return out

            cached_resp = self._get_cached_response(request_hash)
            if cached_resp:
                logger.debug("Served from response cache")
                out = OutboundMessage(channel=inbound.channel, content=cached_resp, user_id=inbound.user_id, session_id=session.session_id)
                await self._audit("reason_cached", {"query": inbound.content, "context": {}}, {"response": cached_resp}, trace_id)
                if PROMETHEUS_AVAILABLE:
                    metrics['responses_total'].labels(inbound.channel, intent, "cached").inc()
                return out

            # PERSONALIZED RESPONSE GENERATION WITH SECURITY CONTEXT
            start = asyncio.get_event_loop().time()
            try:
                preferred_name = "User"
                if identity_profile and identity_profile.preferred_name:
                    preferred_name = identity_profile.preferred_name
                elif session and session.metadata.get("preferred_name"):
                    preferred_name = session.metadata.get("preferred_name")
                
                enhanced_query = inbound.content
                if security_context and security_context.user_identity.access_level != AccessLevel.PUBLIC:
                    enhanced_query = f"[{security_context.user_identity.access_level.value} access] {inbound.content}"
                
                # --- THIS IS THE HANDOFF ---
                # The "Input" node (this function) now calls "respond",
                # which will spawn the "Response" node.
                response_text = await self.persona.respond(enhanced_query)
                # --- END HANDOFF ---
                
                latency = asyncio.get_event_loop().time() - start
                
                if preferred_name != "User" and security_context and security_context.user_identity.access_level in [AccessLevel.OWNER_ROOT, AccessLevel.OWNER_REMOTE]:
                    response_text = response_text.replace("User", preferred_name)
                
                if PROMETHEUS_AVAILABLE:
                    metrics['response_latency'].labels(inbound.channel).observe(latency)
                    metrics['response_latency_summary'].labels(inbound.channel).observe(latency)
                    metrics['requests_total'].labels(inbound.channel, intent).inc()
                    metrics['responses_total'].labels(inbound.channel, intent, "success").inc()

                safe = response_text.strip()
                
                self._cache_response(request_hash, safe)

                session.history.append({
                    "role": "user", 
                    "content": inbound.content, 
                    "ts": inbound.timestamp,
                    "security_level": security_context.user_identity.access_level.value if security_context else "unknown"
                })
                session.history.append({
                    "role": "assistant", 
                    "content": safe, 
                    "ts": asyncio.get_event_loop().time(),
                    "personalized_for": preferred_name
                })
                session.touch()

                out = OutboundMessage(
                    channel=inbound.channel, 
                    content=safe, 
                    user_id=inbound.user_id, 
                    session_id=session.session_id,
                    metadata={
                        "security_level": security_context.user_identity.access_level.value if security_context else "unknown",
                        "personalized": preferred_name != "User",
                        "response_latency": latency
                    }
                )
                
                await self._audit("reason_llm_success", {
                    "query": inbound.content,
                    "security_level": security_context.user_identity.access_level.value if security_context else "unknown",
                    "user_identity": identity_profile.preferred_name if identity_profile else "unknown"
                }, {
                    "response": safe, 
                    "latency": latency,
                    "personalized_for": preferred_name
                }, trace_id)

                await self.send_outbound(out)
                return out

            except Exception as e:
                latency = asyncio.get_event_loop().time() - start
                fallback = await self._get_fallback_reasoning(inbound.content)
                logger.error(f"Failed to generate response via PersonaCore: {e}")
                await self._audit("reason_failure", {
                    "query": inbound.content,
                    "security_level": security_context.user_identity.access_level.value if security_context else "unknown"
                }, {
                    "response": fallback, 
                    "error": str(e)
                }, trace_id)
                if PROMETHEUS_AVAILABLE:
                    metrics['responses_total'].labels(inbound.channel, intent, "failure").inc()
                out = OutboundMessage(channel=inbound.channel, content=fallback, user_id=inbound.user_id, session_id=session.session_id)
                await self.send_outbound(out)
                await hologram_state.set_node_error(node_id) # Set error state
                return out
        
        finally:
            # --- NEW: Despawn "Input" node ---
            # This node fades out, while the "Respond" node (from Persona)
            # is still active. This creates the "flow" effect.
            await hologram_state.set_node_idle("PersonaCore")
            await hologram_state.despawn_and_unlink(node_id, link_id)
            # --- End New Block ---
    
    async def _handle_set_name_directly(self, new_name: str, identity: IdentityProfile, inbound: InboundMessage) -> OutboundMessage:
        """
        [MODIFIED METHOD]
        Handle set_name command directly and ensure it persists
        across all modules and sessions.
        """
        if not new_name:
            return OutboundMessage(
                channel=inbound.channel,
                content="❌ Name cannot be empty. Usage: set_name YourName",
                user_id=inbound.user_id,
                session_id=inbound.session_id
            )
        
        try:
            # --- START CRITICAL FIX ---
            
            # 1. Update the 'live' IdentityProfile object
            identity.preferred_name = new_name
            identity.name = new_name
            
            # 2. Update the master 'known_identities' list in IdentityManager
            if identity.identity_id in self.security_orchestrator.identity_manager.known_identities:
                self.security_orchestrator.identity_manager.known_identities[identity.identity_id].preferred_name = new_name
                self.security_orchestrator.identity_manager.known_identities[identity.identity_id].name = new_name
                logger.info(f"Updated in-memory IdentityManager entry for {identity.identity_id}")
            
            # 3. Persist this change to the 'known_identities' table
            await self.security_orchestrator.identity_manager.persist_identity(identity)
            
            # 4. Update the separate 'user_profile' for PersonaCore
            profile = await self.persona.core.load_user_profile() or {}
            profile["name"] = new_name
            profile["preferred_name"] = new_name
            await self.persona.core.save_user_profile(profile)
            
            # 5. Refresh the Persona's system prompt for the CURRENT session
            await self.persona.refresh_profile_context()
            
            # --- END CRITICAL FIX ---
            
            return OutboundMessage(
                channel=inbound.channel,
                content=f"✅ Name set to: {new_name}",
                user_id=inbound.user_id,
                session_id=inbound.session_id
            )

        except Exception as e:
            logger.error(f"Failed to set name: {e}")
            return OutboundMessage(
                channel=inbound.channel,
                content=f"❌ An error occurred while setting name: {e}",
                user_id=inbound.user_id,
                session_id=inbound.session_id
            )

    async def _process_access_command(self, command_str: str, identity: IdentityProfile, inbound: InboundMessage) -> OutboundMessage:
            """
            Process access control commands with aliasing.
            FIXED: Removed incorrect 'access_' prefix from internal command.
            """
            from access_control import UserIdentity, AccessLevel
            
            # Convert IdentityProfile to UserIdentity
            user_identity = UserIdentity(
                user_id=identity.identity_id,
                name=identity.name,
                preferred_name=identity.preferred_name,
                access_level=AccessLevel.OWNER_ROOT if identity.relationship == "owner" else AccessLevel.PRIVILEGED,
                privileges={"full_system_access"} if identity.relationship == "owner" else set(),
                verification_required=False
            )
            
            parts = command_str.split()
            if not parts:
                return OutboundMessage(
                    channel=inbound.channel,
                    content="Access commands: users, add_privileged_user, list_trusted_devices, etc.",
                    user_id=inbound.user_id,
                    session_id=inbound.session_id
                )
            
            subcommand = parts[0]
            params = self._parse_security_params(parts[1:])
            
            # --- COMMAND ALIASING ---
            if subcommand == "users":
                subcommand = "list_privileged_users"  # Alias 'users' to the internal command
            # Add other aliases here if needed
            
            try:
                # --- FIX: REMOVED "access_" PREFIX ---
                # The AccessControlSystem expects the command name directly
                internal_command = subcommand
                
                result = await self.security_orchestrator.access_control.process_management_command(
                    internal_command, params, user_identity
                )
                return OutboundMessage(
                    channel=inbound.channel,
                    content=result,
                    user_id=inbound.user_id,
                    session_id=inbound.session_id
                )
            except Exception as e:
                logger.error(f"Access command failed for '{command_str}': {e}")
                return OutboundMessage(
                    channel=inbound.channel,
                    content=f"❌ Access command failed: {str(e)}",
                    user_id=inbound.user_id,
                    session_id=inbound.session_id
                )
        
    async def _process_identity_command(self, command_str: str, identity: IdentityProfile, inbound: InboundMessage) -> OutboundMessage:
            """
            Process identity management commands with aliasing.
            FIXED: This method was missing proper aliasing.
            """
            parts = command_str.split()
            if not parts:
                return OutboundMessage(
                    channel=inbound.channel,
                    content="Identity commands: list, set_preferred_name",
                    user_id=inbound.user_id,
                    session_id=inbound.session_id
                )
            
            subcommand = parts[0]
            params = self._parse_security_params(parts[1:])
            
            # --- COMMAND ALIASING ---
            if subcommand == "list":
                subcommand = "list_known_identities" # Alias 'list' to the internal command
            # Add other aliases here
            
            try:
                # The orchestrator/identity_manager expects the command without a prefix
                result = await self.security_orchestrator.identity_manager.process_identity_command(
                    subcommand, params, identity
                )
                return OutboundMessage(
                    channel=inbound.channel,
                    content=result,
                    user_id=inbound.user_id,
                    session_id=inbound.session_id
                )
            except Exception as e:
                logger.error(f"Identity command failed for '{command_str}': {e}")
                return OutboundMessage(
                    channel=inbound.channel,
                    content=f"❌ Identity command failed: {str(e)}",
                    user_id=inbound.user_id,
                    session_id=inbound.session_id
                )
        
    def _parse_security_params(self, parts: List[str]) -> Dict[str, Any]:
        """Parse security command parameters"""
        params = {}
        for part in parts:
            if '=' in part:
                key, value = part.split('=', 1)
                params[key] = value
        return params
    
    async def _get_rate_limit_fallback(self, query: str) -> str:
        """Get rate limit fallback response."""
        if hasattr(self.cognition, "_get_rate_limit_fallback"):
            fallback = self.cognition._get_rate_limit_fallback(query)
            if asyncio.iscoroutine(fallback):
                return await fallback
            return fallback
        return "[Rate limited - please try again in a moment]"

    async def _get_fallback_reasoning(self, query: str) -> str:
        """Get general fallback response."""
        if hasattr(self.cognition, "_get_fallback_reasoning"):
            fallback = self.cognition._get_fallback_reasoning(query)
            if asyncio.iscoroutine(fallback):
                return await fallback
            return fallback
        return "[Service temporarily unavailable - please try again later]"

    # -------------------------
    # Async Outbound Queue & Worker
    # -------------------------
    async def send_outbound(self, message: OutboundMessage):
        """Queue an outbound message (non-blocking). Worker will handle actual delivery."""
        async with self._queue_lock:
            self.outbound_queue.append(message)
            if PROMETHEUS_AVAILABLE:
                metrics['queue_size'].set(len(self.outbound_queue))
        logger.debug(f"Queued outbound message for user {message.user_id} on channel {message.channel}")

    async def _outbound_worker(self):
        """Background worker to flush outbound messages (async mode)."""
        while not self._stop_event.is_set():
            try:
                if not self.outbound_queue:
                    await asyncio.sleep(0.1)
                    continue
                async with self._queue_lock:
                    msg = self.outbound_queue.popleft()
                    if PROMETHEUS_AVAILABLE:
                        metrics['queue_size'].set(len(self.outbound_queue))
                delivered = await self._deliver_message_async(msg)
                logger.debug(f"Delivered message to {msg.channel} (delivered={delivered})")
            except Exception as e:
                logger.error(f"Outbound worker error: {e}")
                await asyncio.sleep(0.5)

    async def _deliver_message_async(self, message: OutboundMessage) -> bool:
        """
        Deliver message via available integrations (async).
        """
        try:
            core = self._get_core()
            if core and hasattr(core, "notify"):
                try:
                    # Try async notify first
                    if hasattr(core.notify, '__await__') or asyncio.iscoroutinefunction(core.notify):
                        await core.notify(channel=message.channel, message=message.content)
                    else:
                        # Fallback to sync in executor
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, core.notify, message.channel, message.content)
                    return True
                except Exception as e:
                    logger.debug(f"core.notify failed: {e}, falling back to storage/log")
            
            # fallback: use storage/audit log
            await self._audit("outbound_deliver", {"channel": message.channel, "content_preview": message.content[:120]}, {"status": "logged"})
            return True
        except Exception as e:
            logger.warning(f"Failed to deliver message: {e}")
            return False

    # -------------------------
    # Async Autosave worker
    # -------------------------
    async def _autosave_worker(self, interval_sec: float):
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(interval_sec)
                await self._persist_sessions()
            except Exception as e:
                logger.debug(f"Autosave worker error: {e}")

    # -------------------------
    # Async Persistence
    # -------------------------
    async def _persist_sessions(self):
        """Persist active sessions to storage with proper serialization."""
        try:
            core = self._get_core()
            if core and hasattr(core, "store"):
                async with self._session_lock:
                    session_data = {}
                    for sid, sess in list(self.sessions.items()):
                        if not sess.is_expired():
                            # Ensure all data is JSON-serializable
                            session_data[sid] = {
                                "user_id": sess.user_id,
                                "last_active": sess.last_active,
                                "persona_tone": sess.persona_tone,
                                "history": list(sess.history),  # Convert deque to list
                                "metadata": self._serialize_metadata(sess.metadata)  # Custom serialization
                            }
                    
                    if session_data:
                        await core.store.put("interaction_sessions", "session_index", session_data)
                        logger.debug(f"InteractionCore persisted {len(session_data)} sessions")
        except Exception as e:
            logger.warning(f"InteractionCore failed to persist sessions: {e}")

    def _serialize_metadata(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure metadata is JSON-serializable."""
        serializable_metadata = {}
        for key, value in metadata.items():
            try:
                # Test if value is JSON-serializable
                json.dumps(value)
                serializable_metadata[key] = value
            except (TypeError, ValueError):
                # Convert non-serializable objects to strings
                serializable_metadata[key] = str(value)
        return serializable_metadata
        # -------------------------
    # Async Shutdown & Cleanup
    # -------------------------
    async def shutdown(self, wait_seconds: float = 5.0):
        """Graceful shutdown for interaction core."""
        logger.info("InteractionCore shutdown initiated")
        self._stop_event.set()
        
        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        if self._background_tasks:
            await asyncio.wait(self._background_tasks, timeout=wait_seconds)
        
        # persist sessions if possible
        await self._persist_sessions()
        logger.info("InteractionCore shutdown complete")

    # -------------------------
    # Context Manager Support
    # -------------------------
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

    async def _apply_immediate_personalization(self, identity: IdentityProfile):
        """Apply immediate personalization across the system"""
        try:
            # Update Persona Core with owner's name immediately
            if hasattr(self.persona, 'update_user_profile'):
                await self.persona.update_user_profile(
                    name=identity.preferred_name,
                    timezone="UTC"  # Could be dynamic
                )
            
            logger.info(f"👤 Applied immediate personalization for: {identity.preferred_name}")
            
        except Exception as e:
            logger.warning(f"Personalization application failed: {e}")
        
# -------------------------
# Factory Function - FIXED: Move outside class
# -------------------------
async def create_interaction_core(persona: PersonaCore, cognition: CognitionCore, autonomy: AutonomyCore, config: Optional[Dict[str, Any]] = None) -> InteractionCore:
    """Factory function to create and initialize InteractionCore with security"""
    # Ensure security orchestrator is passed if available
    if config and 'security_orchestrator' not in config:
        # Try to get security orchestrator from persona core if available
        if hasattr(persona, 'core') and hasattr(persona.core, 'security_orchestrator'):
            config = config or {}
            config['security_orchestrator'] = persona.core.security_orchestrator
    
    interaction_core = InteractionCore(persona, cognition, autonomy, config)
    logger.info("InteractionCore created successfully with security integration")
    return interaction_core