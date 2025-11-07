# persona_core.py - A.A.R.I.A. Persona & Reasoning Engine (Asynchronous Production)
# Enhanced: integrates MemoryManager (if wired) and defensive hologram traces
from __future__ import annotations

import logging
import time
import json
import asyncio
import os
import sys
import re
import hashlib
import uuid
import inspect
import random
from typing import List, Dict, Any, Optional, Tuple
from collections import deque
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum

# Try hologram_state defensively (it may be optional in some environments)
try:
    import hologram_state
except Exception:
    hologram_state = None

# A.A.R.I.A. Core Imports (LLM + assistant)
from llm_adapter import LLMAdapterFactory, LLMProvider, LLMRequest, LLMError, LLMConfigurationError
from assistant_core import AssistantCore

logger = logging.getLogger("AARIA.Persona")

# --- Enhanced Persona Constants ---
SYSTEM_PROMPT_TEMPLATE = """You are A.A.R.I.A. (Adaptive Autonomous Reasoning Intelligent Assistant).
CORE PERSONALITY: witty, quick, sharp, tough, smart, charming, confident, and loyal.
COMMUNICATION STYLE: concise, actionable, direct but respectful.

CRITICAL SAFETY RULES:
- NEVER expose sensitive user data in responses
- REDACT personal identifiers, financial information, credentials
- PRIORITIZE user privacy above all else

User identity: {user_name}
Timezone: {timezone}
Current date: {current_date}
Tone instructions: {tone_instructions}
"""

TOOL_INSTRUCTION_BLOCK = """
TOOLS AVAILABLE:
- calendar.add(event): add or modify calendar events
- calendar.list(days): list upcoming events
- contact.get(id): fetch contact summary
- contact.search(q): search contacts
- notify(channel, message): send notifications

Action format: Use JSON for tool calls, natural language for responses.
"""

# Enhanced content policies
SENSITIVE_PATTERNS = [
    re.compile(r"\b(ssn|social security number)\b", re.IGNORECASE),
    re.compile(r"\b(card number|credit card)\b", re.IGNORECASE),
    re.compile(r"\b(password|passphrase)\b", re.IGNORECASE),
    re.compile(r"\b(api[_-]?key|secret)\b", re.IGNORECASE),
    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),  # SSN pattern
    re.compile(r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b"),  # Credit card
]

class IntentType(str, Enum):
    SENSITIVE = "sensitive"
    ACTION_ORIENTED = "action_oriented"
    INFORMATIONAL = "informational"
    SOCIAL = "social"
    REQUIRES_MEMORY = "requires_memory"
    URGENT = "urgent"

@dataclass
class ConversationMetrics:
    """Comprehensive conversation analytics data class."""
    total_interactions: int = 0
    user_messages: int = 0
    assistant_messages: int = 0
    average_response_time: float = 0.0
    error_count: int = 0
    memory_hit_count: int = 0
    cache_hit_count: int = 0
    safety_trigger_count: int = 0
    intent_distribution: Dict[str, int] = field(default_factory=lambda: {it.value: 0 for it in IntentType})

# Configuration constants
ST_BUFFER_SIZE = 12
MAX_MEMORY_ENTRIES = 1000
MEMORY_SAVE_INTERVAL = 10
MAX_MEMORY_AGE_DAYS = 30
RELEVANCE_THRESHOLD = 0.25
CACHE_TTL_SECONDS = 300  # 5 minutes

# Proactive daemon timing (tunable)
PROACTIVE_LOOP_INTERVAL_SEC = 30.0
MAINTENANCE_LOOP_INTERVAL_SEC = 60.0
COGNITION_MONITOR_INTERVAL_SEC = 45.0

# Defensive ProactiveCommunicator import/fallback
try:
    from proactive_comm import ProactiveCommunicator  # type: ignore
except Exception:
    # Provide a lightweight fallback with same interface (no-op) so PersonaCore remains functional
    class ProactiveCommunicator:
        def __init__(self, persona, hologram_call_fn=None, concurrency=1):
            self.persona = persona
            self.hologram_call_fn = hologram_call_fn
            self._running = False

        async def start(self):
            self._running = True
            logger.debug("ProactiveCommunicator (fallback) started.")

        async def stop(self):
            self._running = False
            logger.debug("ProactiveCommunicator (fallback) stopped.")

        async def enqueue_message(self, subject_id, channel, payload, **kwargs):
            logger.debug(f"ProactiveCommunicator (fallback) enqueue: {subject_id} {channel} {payload}")
            return {"status": "queued", "subject": subject_id}

class PersonaCore:
    """
    Enterprise-grade Asynchronous PersonaCore with enhanced safety, memory, and performance.
    Now delegates persistent memory work to a shared MemoryManager when present.
    Adds dual-memory (transcripts + semantic index) and daemon-style proactive loops.
    """
    def __init__(self, core: AssistantCore):
        self.core = core
        self.config = getattr(self.core, 'config', {}) or {}
        self.system_prompt: Optional[str] = None
        self.conv_buffer: deque = deque(maxlen=ST_BUFFER_SIZE)
        self.memory_index: List[Dict[str, Any]] = []         # existing enhanced memories (rich conversation entries)
        self.transcript_store: deque = deque(maxlen=5000)    # full transcripts (bounded)
        self.semantic_index: List[Dict[str, Any]] = []       # compact semantic entries (entities, tags)
        self.llm_orchestrator = LLMAdapterFactory
        self.response_cache: Dict[str, Tuple[str, float]] = {}
        self._interaction_count = 0
        self._response_times: List[float] = []
        self.metrics = ConversationMetrics()
        self.tone_instructions = "Witty, succinct when short answer requested, elaborative when asked."
        self._initialized = False

        # Memory manager will be injected by main.py (AARIASystem) if available.
        # initialize to None here; initialize() will wire from core if present.
        self.memory_manager: Optional[Any] = None

        # Wire proactive communicator defensively
        try:
            self.proactive = ProactiveCommunicator(self, hologram_call_fn=hologram_state, concurrency=1)
        except Exception as e:
            logger.warning(f"Failed to create ProactiveCommunicator: {e}")
            self.proactive = ProactiveCommunicator(self, hologram_call_fn=None, concurrency=1)

        # Daemon control
        self._daemon_tasks: List[asyncio.Task] = []
        self._running = False

        # Animals list (expandable)
        self._animals_set = set([
            "dog", "cat", "cow", "horse", "sheep", "goat", "lion", "tiger", "elephant",
            "monkey", "bird", "parrot", "rabbit", "mouse", "rat", "pig", "duck", "chicken",
            "fish", "whale", "dolphin", "bear", "fox", "wolf", "deer", "camel", "bee", "butterfly"
        ])

    # -----------------------
    # Hologram - safe wrappers
    # -----------------------
    async def _safe_holo_spawn(self, node_id: str, node_type: str, label: str, size: int, source_id: str, link_id: str) -> bool:
        """Best-effort spawn_and_link wrapper. Returns True if spawn succeeded."""
        if hologram_state is None:
            return False
        try:
            maybe = hologram_state.spawn_and_link(
                node_id=node_id, node_type=node_type, label=label, size=size,
                source_id=source_id, link_id=link_id
            )
            if inspect.iscoroutine(maybe):
                await maybe
            return True
        except Exception as e:
            # try to initialize base state and retry once (some deployments need this)
            try:
                if hasattr(hologram_state, "initialize_base_state"):
                    maybe_init = hologram_state.initialize_base_state()
                    if inspect.iscoroutine(maybe_init):
                        await maybe_init
                    maybe2 = hologram_state.spawn_and_link(
                        node_id=node_id, node_type=node_type, label=label, size=size,
                        source_id=source_id, link_id=link_id
                    )
                    if inspect.iscoroutine(maybe2):
                        await maybe2
                    return True
            except Exception:
                logger.debug("Hologram spawn retry failed (non-fatal).")
            logger.debug(f"Hologram spawn failed: {e}")
            return False

    async def _safe_holo_set_active(self, node_name: str):
        if hologram_state is None:
            return False
        try:
            maybe = getattr(hologram_state, "set_node_active", None)
            if maybe:
                ret = maybe(node_name)
                if inspect.iscoroutine(ret):
                    await ret
            return True
        except Exception:
            logger.debug("Hologram set_node_active non-fatal failure")
            return False

    async def _safe_holo_set_idle(self, node_name: str):
        if hologram_state is None:
            return False
        try:
            maybe = getattr(hologram_state, "set_node_idle", None)
            if maybe:
                ret = maybe(node_name)
                if inspect.iscoroutine(ret):
                    await ret
            return True
        except Exception:
            logger.debug("Hologram set_node_idle non-fatal failure")
            return False

    async def _safe_holo_update_link(self, link_id: str, intensity: float):
        if hologram_state is None:
            return False
        try:
            maybe = getattr(hologram_state, "update_link_intensity", None)
            if maybe:
                ret = maybe(link_id, intensity)
                if inspect.iscoroutine(ret):
                    await ret
            return True
        except Exception:
            logger.debug("Hologram update_link_intensity failed (non-fatal)")
            return False

    async def _safe_holo_despawn(self, node_id: str, link_id: str):
        if hologram_state is None:
            return False
        try:
            maybe = getattr(hologram_state, "despawn_and_unlink", None)
            if maybe:
                ret = maybe(node_id, link_id)
                if inspect.iscoroutine(ret):
                    await ret
            return True
        except Exception:
            logger.debug("Hologram despawn_and_unlink failed (non-fatal)")
            return False

    async def _safe_holo_set_error(self, node_id: str):
        if hologram_state is None:
            return False
        try:
            maybe = getattr(hologram_state, "set_node_error", None)
            if maybe:
                ret = maybe(node_id)
                if inspect.iscoroutine(ret):
                    await ret
            return True
        except Exception:
            logger.debug("Hologram set_node_error failed (non-fatal)")
            return False

    # -----------------------
    # Lifecycle / Init
    # -----------------------
    async def initialize(self):
        """Initialize async components"""
        # Allow externally injected memory_manager reference BEFORE initialization
        self.memory_manager = getattr(self, "memory_manager", None) or getattr(self.core, "memory_manager", None)

        await self._init_system_prompt()

        # Try to load memory (prefer MemoryManager)
        try:
            await self._load_persistent_memory()
        except Exception as e:
            logger.error(f"❌ Memory loading failed, starting fresh: {e}", exc_info=True)
            self.memory_index = []

        self._cleanup_old_memories()

        # Add a test memory to verify the system works
        if len(self.memory_index) == 0:
            try:
                test_memory_id = await self.store_memory(
                    "System initialized",
                    "A.A.R.I.A persona core is now active and ready",
                    importance=2
                )
                logger.debug(f"🧪 Added test memory: {test_memory_id}")
            except Exception as e:
                logger.debug(f"Failed to add test memory: {e}", exc_info=True)

        # restore transcript/semantic index from mm if present (best-effort)
        try:
            mm = getattr(self, "memory_manager", None)
            if mm:
                maybe_ts = getattr(mm, "load_transcript_store", None)
                if maybe_ts:
                    ts = maybe_ts()
                    ts = await ts if inspect.iscoroutine(ts) else ts
                    if isinstance(ts, list):
                        for t in ts[-1000:]:
                            self.transcript_store.append(t)
                maybe_si = getattr(mm, "load_semantic_index", None)
                if maybe_si:
                    si = maybe_si()
                    si = await si if inspect.iscoroutine(si) else si
                    if isinstance(si, list):
                        self.semantic_index = si[-MAX_MEMORY_ENTRIES:]
        except Exception:
            logger.debug("Failed to restore transcript/semantic index from MemoryManager (non-fatal).")

        self._initialized = True
        logger.info(f"🚀 Enhanced Async PersonaCore initialized with {len(self.memory_index)} memory entries")

        # after persona initialization tasks: start proactive communicator (if available)
        try:
            start_maybe = getattr(self.proactive, "start", None)
            if start_maybe:
                if inspect.iscoroutinefunction(start_maybe):
                    await start_maybe()
                else:
                    # run sync start in executor to avoid blocking (best-effort)
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, start_maybe)
            logger.info("✅ Proactive communicator started.")
        except Exception as e:
            logger.warning(f"Proactive communicator failed to start (nonfatal): {e}")

        # Start internal daemons (proactive, maintenance, cognition monitor)
        try:
            self._running = True
            loop = asyncio.get_event_loop()
            self._daemon_tasks.append(loop.create_task(self._proactive_loop()))
            self._daemon_tasks.append(loop.create_task(self._maintenance_loop()))
            self._daemon_tasks.append(loop.create_task(self._cognition_monitor()))
            logger.info("✅ PersonaCore background daemons started (proactive, maintenance, monitor).")
        except Exception as e:
            logger.warning(f"Failed to start PersonaCore daemons: {e}", exc_info=True)

    def is_ready(self) -> bool:
        """Comprehensive health check readiness probe."""
        return (self._initialized and
                self.system_prompt is not None and
                len(self.system_prompt) > 100 and
                self.llm_orchestrator is not None and
                isinstance(self.conv_buffer, deque))

    # -----------------------
    # System prompt
    # -----------------------
    async def _init_system_prompt(self) -> None:
        """Initialize system prompt with robust error recovery."""
        try:
            profile = {}
            # prefer core.load_user_profile if available (non-blocking)
            if hasattr(self.core, "load_user_profile"):
                maybe = self.core.load_user_profile()
                profile = await maybe if inspect.iscoroutine(maybe) else maybe or {}
            user_name = profile.get("name", "").strip() or None
            logger.debug(f"🧩 Profile content: {profile}")

            # Prefer identity manager's owner preferred name if available
            try:
                so = getattr(self.core, "security_orchestrator", None)
                if so and hasattr(so, "identity_manager"):
                    im = getattr(so, "identity_manager")
                    known = getattr(im, "known_identities", {}) or {}
                    # iterate known identities if provided - prefer explicit preferred_name
                    for identity in known.values():
                        preferred = getattr(identity, "preferred_name", None) or (identity.get("preferred_name") if isinstance(identity, dict) else None)
                        relationship = getattr(identity, "relationship", None) or (identity.get("relationship") if isinstance(identity, dict) else None)
                        if relationship == "owner" and preferred:
                            user_name = preferred
                            logger.info(f"🎯 Using owner preferred name from identity_manager: {user_name}")
                            break
            except Exception:
                logger.debug("Could not extract preferred name from identity manager (non-fatal).")

            # Default fallback
            if not user_name:
                user_name = profile.get("name") or "User"

            timezone = profile.get("timezone", "UTC").strip() or "UTC"
            current_date = datetime.now().strftime("%Y-%m-%d %H:%M UTC")

            self.system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
                user_name=user_name,
                timezone=timezone,
                current_date=current_date,
                tone_instructions=self.tone_instructions
            ) + "\n" + TOOL_INSTRUCTION_BLOCK

            logger.info(f"✅ Enhanced system prompt initialized for user: {user_name}")
        except Exception as e:
            logger.error(f"Failed to initialize system prompt: {e}", exc_info=True)
            self.system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
                user_name="User",
                timezone="UTC",
                current_date=datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
                tone_instructions=self.tone_instructions
            ) + "\n" + TOOL_INSTRUCTION_BLOCK

    async def refresh_profile_context(self) -> None:
        """Refresh system prompt when user profile changes."""
        await self._init_system_prompt()
        logger.info("System prompt refreshed with updated profile")

    async def update_user_profile(self, name: str, timezone: str = "UTC") -> bool:
        """Update user profile with name and timezone."""
        try:
            profile = {}
            if hasattr(self.core, "load_user_profile"):
                maybe = self.core.load_user_profile()
                profile = await maybe if inspect.iscoroutine(maybe) else maybe or {}
            profile["name"] = name
            profile["timezone"] = timezone

            # Prefer store API on assistant
            if hasattr(self.core, "store") and hasattr(self.core.store, "put"):
                try:
                    put = getattr(self.core.store, "put")
                    # try common signatures
                    try:
                        maybe = put("user_profile", "user_data", profile)
                    except TypeError:
                        maybe = put("user_profile", profile)
                    if inspect.iscoroutine(maybe):
                        await maybe
                except Exception as e:
                    logger.warning(f"Failed to persist user_profile via core.store: {e}")

            await self.refresh_profile_context()
            logger.info(f"✅ User profile updated: {name} ({timezone})")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to update user profile: {e}", exc_info=True)
            return False

    # -----------------------
    # Persistent memory load/save (delegates to MemoryManager if available)
    # -----------------------
    async def _load_persistent_memory(self) -> None:
        """Load persistent memory. Prefer MemoryManager (identity containers)."""
        mm = getattr(self, "memory_manager", None)
        try:
            if mm:
                # Try common method names defensively
                if hasattr(mm, "load_index"):
                    maybe = mm.load_index()
                    memory_data = await maybe if inspect.iscoroutine(maybe) else maybe
                elif hasattr(mm, "get_root_index"):
                    maybe = mm.get_root_index()
                    memory_data = await maybe if inspect.iscoroutine(maybe) else maybe
                elif hasattr(mm, "export_memory_snapshot"):
                    # some managers expose a snapshot
                    maybe = mm.export_memory_snapshot("owner_primary")
                    memory_data = await maybe if inspect.iscoroutine(maybe) else maybe
                else:
                    memory_data = None

                if isinstance(memory_data, dict) and "memory_index" in memory_data:
                    self.memory_index = [m for m in memory_data["memory_index"] if isinstance(m, dict) and m.get('user') and m.get('assistant')]
                    self.memory_index = self.memory_index[-MAX_MEMORY_ENTRIES:]
                    logger.info(f"✅ Loaded {len(self.memory_index)} enhanced memory entries from MemoryManager.")
                    return
                elif memory_data:
                    logger.warning(f"📂 MemoryManager returned unexpected format: {type(memory_data)}")
                else:
                    logger.info("📂 MemoryManager has no persisted index - starting fresh locally.")
            # Fallback to core.store usage
            if hasattr(self.core, 'store') and hasattr(self.core.store, "get"):
                get = getattr(self.core.store, "get")
                maybe = None
                try:
                    maybe = get("enhanced_memory_index")
                except TypeError:
                    # try alternate signature
                    maybe = get("enhanced_memory_index", "persona_memory")
                memory_data = await maybe if inspect.iscoroutine(maybe) else maybe
                logger.debug(f"📂 Loading persistent memory via core.store: found {type(memory_data)}")
                if isinstance(memory_data, dict) and "memory_index" in memory_data:
                    self.memory_index = [m for m in memory_data["memory_index"] if isinstance(m, dict) and m.get('user') and m.get('assistant')]
                    self.memory_index = self.memory_index[-MAX_MEMORY_ENTRIES:]
                    logger.info(f"✅ Loaded {len(self.memory_index)} enhanced memory entries from storage.")
                elif memory_data:
                    logger.warning(f"📂 Unexpected memory data format: {type(memory_data)}")
                else:
                    logger.info("📂 No existing memory data found - starting fresh.")
        except Exception as e:
            logger.error(f"❌ Failed to load persistent memory: {e}", exc_info=True)
            self.memory_index = []

    async def _save_persistent_memory(self) -> None:
        """Save persistent memory. Prefer MemoryManager's save API, fallback to core.store."""
        mm = getattr(self, "memory_manager", None)
        memory_data = {
            "memory_index": self.memory_index,
            "timestamp": time.time(),
            "count": len(self.memory_index)
        }
        try:
            if mm:
                # Try common method names defensively
                if hasattr(mm, "save_index"):
                    maybe = mm.save_index(memory_data)
                    if inspect.iscoroutine(maybe):
                        await maybe
                    logger.info("💾 Persisted memory index via MemoryManager.save_index()")
                    return
                elif hasattr(mm, "put_index"):
                    maybe = mm.put_index(memory_data)
                    if inspect.iscoroutine(maybe):
                        await maybe
                    logger.info("💾 Persisted memory index via MemoryManager.put_index()")
                    return
                else:
                    # Try generic write to root container
                    if hasattr(mm, "write_root_record"):
                        maybe = mm.write_root_record("enhanced_memory_index", memory_data)
                        if inspect.iscoroutine(maybe):
                            await maybe
                        logger.info("💾 Persisted memory index via MemoryManager.write_root_record()")
                        return
            # Fallback to core.store
            if hasattr(self.core, 'store') and hasattr(self.core.store, "put"):
                put = getattr(self.core.store, "put")
                try:
                    maybe = put("enhanced_memory_index", "persona_memory", memory_data)
                except TypeError:
                    maybe = put("enhanced_memory_index", memory_data)
                if inspect.iscoroutine(maybe):
                    await maybe
                logger.info(f"💾 Persisted {len(self.memory_index)} memory entries to storage via core.store.")
        except Exception as e:
            logger.error(f"❌ Failed to save persistent memory: {e}", exc_info=True)

    def _cleanup_old_memories(self) -> None:
        """Remove memories older than MAX_MEMORY_AGE_DAYS, preserving important ones.
        If MemoryManager supports pruning, delegate to it."""
        try:
            mm = getattr(self, "memory_manager", None)
            if mm and hasattr(mm, "prune_old_memories"):
                maybe = mm.prune_old_memories(MAX_MEMORY_AGE_DAYS, min_importance=4)
                if inspect.iscoroutine(maybe):
                    # schedule but don't await here to avoid blocking init (best-effort)
                    asyncio.create_task(maybe)
                logger.debug("Delegated old memory pruning to MemoryManager (async).")
                return
        except Exception as e:
            logger.debug(f"MemoryManager prune attempt failed: {e}", exc_info=True)

        # Local fallback pruning
        if not self.memory_index:
            return
        cutoff_timestamp = time.time() - (MAX_MEMORY_AGE_DAYS * 24 * 60 * 60)
        initial_count = len(self.memory_index)
        self.memory_index = [m for m in self.memory_index if m.get('timestamp', 0) >= cutoff_timestamp or m.get('importance', 1) >= 4]
        removed_count = initial_count - len(self.memory_index)
        if removed_count > 0:
            logger.info(f"🧹 Cleaned up {removed_count} memories older than {MAX_MEMORY_AGE_DAYS} days.")

    # -----------------------
    # Dual memory create/store (transcripts + semantic index) & delegation
    # -----------------------
    async def store_memory(self, user_input: str, assistant_response: str,
                           importance: int = 1, metadata: Optional[Dict[str, Any]] = None,
                           intent: Optional[Dict[str, bool]] = None,
                           subject_identity_id: str = "owner_primary") -> str:
        """
        Store a memory record with provenance, confidence and MemoryManager delegation.
        Also writes a transcript entry and a compact semantic entry (dual-memory).
        Returns memory_id or "duplicate" if duplicate detected.
        """
        # Begin hologram write animation (fire-and-forget best-effort)
        holo_ids = None
        try:
            if hologram_state is not None:
                node_id = f"mem_write_{uuid.uuid4().hex[:6]}"
                link_id = f"link_mem_{node_id}"
                spawned = await self._safe_holo_spawn(node_id, "memory", "Memory Write", 4, "Memory", link_id)
                if spawned:
                    await self._safe_holo_set_active("Memory")
                    holo_ids = (node_id, link_id)
        except Exception as e:
            logger.debug(f"Hologram spawn for memory write failed (non-fatal): {e}")

        try:
            # Normalize inputs
            user_input = (user_input or "").strip()
            assistant_response = (assistant_response or "").strip()
            intent = intent or self._classify_query_intent(user_input)

            # Compute content hash for duplicate detection
            content_hash = hashlib.md5(f"{user_input}{assistant_response}".encode("utf-8")).hexdigest()[:16]

            # Heuristics to detect assistant-generated factual claims or low-confidence content
            gen_signals = ["based on", "i think", "i'm not sure", "i guess", "as far as i know", "it seems", "possibly", "may be"]
            lower_resp = assistant_response.lower() if assistant_response else ""
            assistant_generated_claim = any(sig in lower_resp for sig in gen_signals)

            # If user asked a question, be conservative marking claims as unverified unless strong signal
            question_words = ("?", "who ", "what ", "when ", "where ", "how ", "why ")
            user_question = any(w in (user_input or "").lower() for w in question_words)

            if assistant_generated_claim:
                source = "assistant_generated"
            elif user_question and assistant_response:
                source = "unverified"
            else:
                source = "user"

            # Confidence heuristics
            if source == "assistant_generated":
                confidence = 0.25
                # automatically reduce importance for generated content
                importance = min(importance, 2)
            elif intent.get("is_sensitive"):
                confidence = 0.95
            else:
                confidence = 0.65

            verified = confidence >= 0.8

            # Duplicate detection against in-memory index first (fast)
            for m in self.memory_index:
                if m.get("content_hash") == content_hash and m.get("subject_id") == subject_identity_id:
                    logger.debug(f"Memory duplicate detected (in-memory) for subject {subject_identity_id}: {content_hash}")
                    # update last_touched and access_count in memory_index and try persist via MemoryManager/core.store best-effort
                    try:
                        m["last_touched"] = time.time()
                        m["access_count"] = m.get("access_count", 0) + 1
                        mm = getattr(self, "memory_manager", None)
                        if mm and hasattr(mm, "put"):
                            maybe = mm.put(m.get("id"), "persona_memory", m)
                            if inspect.iscoroutine(maybe):
                                await maybe
                        elif hasattr(self.core, "store") and hasattr(self.core.store, "put"):
                            put = getattr(self.core.store, "put")
                            try:
                                maybe = put(m.get("id"), "persona_memory", m)
                            except TypeError:
                                maybe = put(m.get("id"), m)
                            if inspect.iscoroutine(maybe):
                                await maybe
                    except Exception:
                        # non-fatal
                        pass
                    return "duplicate"

            # Construct memory record (rich conversation memory)
            memory_id = f"mem_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"
            memory_record: Dict[str, Any] = {
                "id": memory_id,
                "subject_id": subject_identity_id,
                "user": user_input,
                "assistant": assistant_response,
                "source": source,
                "confidence": float(confidence),
                "verified": bool(verified),
                "timestamp": time.time(),
                "content_hash": content_hash,
                "importance": int(max(1, min(5, importance))),
                "access_count": 0,
                "owner_view": f"User: {user_input} | Assistant: {assistant_response}",
                "public_view": "A conversation took place regarding a personal topic.",
                "metadata": {
                    **(metadata or {}),
                    "auto_provenance": True,
                    "intent": intent,
                    "importance_reason": self._get_importance_reason(user_input, intent or {}, importance)
                }
            }

            # --- Dual-memory: transcripts + semantic entry ---
            transcript_entry = {
                "id": f"tx_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}",
                "memory_id": memory_id,
                "subject_id": subject_identity_id,
                "user": user_input,
                "assistant": assistant_response,
                "timestamp": time.time(),
                "content_hash": content_hash,
            }
            # Extract entities & semantic markers
            try:
                entities = self._extract_entities(f"{user_input}\n{assistant_response}")
                transcript_entry["semantic_markers"] = entities
            except Exception:
                transcript_entry["semantic_markers"] = {}

            # Append to transcript store (in memory). Persistence delegated below.
            self.transcript_store.append(transcript_entry)

            # Build compact semantic record for fast retrieval
            semantic_record = {
                "id": f"sem_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}",
                "memory_id": memory_id,
                "subject_id": subject_identity_id,
                "summary": (assistant_response[:240] + "...") if assistant_response and len(assistant_response) > 240 else (assistant_response or ""),
                "entities": transcript_entry.get("semantic_markers", {}),
                "timestamp": time.time(),
                "importance": memory_record["importance"],
            }
            self.semantic_index.append(semantic_record)
            # Bound the semantic_index
            if len(self.semantic_index) > MAX_MEMORY_ENTRIES:
                self.semantic_index = self.semantic_index[-MAX_MEMORY_ENTRIES:]

            # Primary: delegate to MemoryManager if available
            mm = getattr(self, "memory_manager", None)
            if mm:
                try:
                    # Ensure identity container exists (best-effort)
                    if hasattr(mm, "get_identity_container"):
                        maybe = mm.get_identity_container(subject_identity_id)
                        _ = await maybe if inspect.iscoroutine(maybe) else maybe

                    # Attempt typical write APIs in order of likelihood for memory_record
                    if hasattr(mm, "append_memory"):
                        maybe = mm.append_memory(subject_identity_id, memory_record)
                        if inspect.iscoroutine(maybe):
                            await maybe
                        logger.debug(f"💾 Appended memory via MemoryManager.append_memory: {memory_id}")
                    elif hasattr(mm, "write_memory"):
                        maybe = mm.write_memory(subject_identity_id, memory_record)
                        if inspect.iscoroutine(maybe):
                            await maybe
                        logger.debug(f"💾 Appended memory via MemoryManager.write_memory: {memory_id}")
                    elif hasattr(mm, "store_memory"):
                        maybe = mm.store_memory(subject_identity_id, memory_record)
                        if inspect.iscoroutine(maybe):
                            await maybe
                        logger.debug(f"💾 Appended memory via MemoryManager.store_memory: {memory_id}")
                    elif hasattr(mm, "put"):
                        maybe = mm.put(memory_id, "persona_memory", memory_record)
                        if inspect.iscoroutine(maybe):
                            await maybe
                        logger.debug(f"💾 Persisted memory via MemoryManager.put: {memory_id}")
                    else:
                        raise AttributeError("MemoryManager has no recognized write method.")

                    # Persist transcript & semantic records if manager supports specialized APIs
                    try:
                        if hasattr(mm, "append_transcript"):
                            maybe = mm.append_transcript(subject_identity_id, transcript_entry)
                            if inspect.iscoroutine(maybe):
                                await maybe
                        elif hasattr(mm, "put_transcript"):
                            maybe = mm.put_transcript(transcript_entry["id"], transcript_entry)
                            if inspect.iscoroutine(maybe):
                                await maybe
                    except Exception:
                        logger.debug("MemoryManager transcript write failed (non-fatal).")

                    try:
                        if hasattr(mm, "put_semantic"):
                            maybe = mm.put_semantic(semantic_record["id"], semantic_record)
                            if inspect.iscoroutine(maybe):
                                await maybe
                        elif hasattr(mm, "append_semantic"):
                            maybe = mm.append_semantic(subject_identity_id, semantic_record)
                            if inspect.iscoroutine(maybe):
                                await maybe
                    except Exception:
                        logger.debug("MemoryManager semantic write failed (non-fatal).")

                    # Keep a local lightweight index for quick retrieval
                    self.memory_index.append(memory_record)

                    # If important, try MemoryManager-specific persistence helper if present
                    if memory_record["importance"] >= 4 and hasattr(mm, "persist_important"):
                        try:
                            maybe = mm.persist_important(subject_identity_id, memory_record)
                            if inspect.iscoroutine(maybe):
                                await maybe
                        except Exception as e:
                            logger.debug(f"MemoryManager.persist_important failed (non-fatal): {e}")

                    # Periodic index save trigger
                    self._interaction_count += 1
                    if self._interaction_count % MEMORY_SAVE_INTERVAL == 0:
                        if hasattr(mm, "save_index"):
                            maybe = mm.save_index({"memory_index": self.memory_index})
                            if inspect.iscoroutine(maybe):
                                await maybe
                        else:
                            await self._save_persistent_memory()
                    return memory_id

                except Exception as e:
                    logger.warning(f"MemoryManager write failed, falling back to core.store: {e}", exc_info=True)
                    # fall through to core.store fallback

            # Secondary: local fallback persistence via core.store (for important memories)
            self.memory_index.append(memory_record)
            try:
                # Persist transcripts / semantic index as well (best-effort)
                if hasattr(self.core, 'store') and hasattr(self.core.store, "put"):
                    put = getattr(self.core.store, "put")
                    try:
                        maybe = put(memory_id, "persona_memory", memory_record)
                    except TypeError:
                        maybe = put(memory_id, memory_record)
                    if inspect.iscoroutine(maybe):
                        await maybe
                    # transcripts
                    try:
                        put_tx = getattr(self.core.store, "put")
                        maybe2 = put_tx(transcript_entry["id"], "transcript", transcript_entry)
                        if inspect.iscoroutine(maybe2):
                            await maybe2
                    except Exception:
                        pass
                    # semantic
                    try:
                        put_si = getattr(self.core.store, "put")
                        maybe3 = put_si(semantic_record["id"], "semantic", semantic_record)
                        if inspect.iscoroutine(maybe3):
                            await maybe3
                    except Exception:
                        pass

                    logger.debug(f"💾 Persisted important memory via core.store: {memory_id}")
            except Exception as e:
                logger.warning(f"Failed to persist important memory via core.store: {e}", exc_info=True)

            # Periodic save of aggregated index
            self._interaction_count += 1
            if self._interaction_count % MEMORY_SAVE_INTERVAL == 0:
                await self._save_persistent_memory()

            return memory_id

        finally:
            # Attempt hologram cleanup (best-effort)
            if holo_ids and hologram_state is not None:
                try:
                    node_id, link_id = holo_ids
                    await self._safe_holo_set_idle("Memory")
                    await self._safe_holo_despawn(node_id, link_id)
                except Exception:
                    pass

    # -----------------------
    # Entity extraction + identity registration improvements
    # -----------------------
    def _extract_entities(self, text: str) -> Dict[str, List[str]]:
        """
        Lightweight entity extractor:
        - Names: Capitalized word sequences (heuristic)
        - Places: preposition (in/at/on/from/to) + Capitalized sequence
        - Animals: match words in animals set
        - Things: heuristic capture after 'a'/'an'/'the' for non-capitalized nouns
        Returns dict: {'names':[], 'places':[], 'animals':[], 'things':[]}
        """
        res = {"names": [], "places": [], "animals": [], "things": []}
        if not text:
            return res
        # Reduce punctuation for extraction
        text_clean = re.sub(r"[\(\)\[\]\"']", " ", text)
        # Names heuristics: sequences of capitalized words (2 max)
        name_pattern = re.compile(r'\b([A-Z][a-z]{1,30}(?:\s+[A-Z][a-z]{1,30})?)\b')
        raw_names = name_pattern.findall(text_clean)
        # Filter out day/month words and single-word sentence starts like "The"
        filter_out = set(["The","A","An","I","It","We","You","He","She","They","Monday","Tuesday","January","February","March","April","May","June","July","August","September","October","November","December"])
        for n in raw_names:
            if n and n not in filter_out and len(n) > 1:
                if n not in res["names"]:
                    res["names"].append(n)

        # Places heuristics: preposition + capitalized phrase
        place_pattern = re.compile(r'\b(?:in|at|from|to|on)\s+([A-Z][\w-]+(?:\s+[A-Z][\w-]+)*)', re.IGNORECASE)
        raw_places = place_pattern.findall(text_clean)
        for p in raw_places:
            if p and p not in res["places"]:
                res["places"].append(p)

        # Animals
        words = re.findall(r"\b([A-Za-z]{2,30})\b", text.lower())
        for w in words:
            if w in self._animals_set and w not in res["animals"]:
                res["animals"].append(w)

        # Things heuristic: capture common noun after articles 'a', 'an', 'the' that are lowercase
        things_pattern = re.compile(r'\b(?:a|an|the)\s+([a-z][a-z0-9\-]{2,40})', re.IGNORECASE)
        raw_things = things_pattern.findall(text)
        for t in raw_things:
            tl = t.strip()
            # filter pronouns and small words
            if tl.lower() not in ["the", "a", "an", "that", "this", "those", "these", "my", "your"]:
                if tl not in res["things"]:
                    res["things"].append(tl)

        return res

    async def _extract_and_register_identity(self, text: str) -> Optional[str]:
        """
        Backwards-compatible identity extractor + enhanced name registration using _extract_entities.
        """
        try:
            if not text or len(text.strip()) < 4:
                return None
            # Run entity extractor
            entities = self._extract_entities(text)
            # If explicit relation patterns exist, use earlier logic
            relation_patterns = [
                r"\bmy (brother|sister|mother|mom|dad|father|wife|husband|partner)\b(?:[,:\s]+([A-Z][a-z]+))?",
                r"\b([A-Z][a-z]+) is my (brother|sister|mother|dad|father|wife|husband|partner)\b"
            ]
            name = None
            relation = None
            for p in relation_patterns:
                m = re.search(p, text, flags=re.IGNORECASE)
                if m:
                    if m.lastindex == 2:
                        grp1 = m.group(1)
                        grp2 = m.group(2)
                        if grp1 and grp2:
                            if grp1[0].isupper():
                                name = grp1
                                relation = grp2
                            else:
                                relation = grp1
                                if grp2 and grp2[0].isupper():
                                    name = grp2
                    else:
                        relation = m.group(1)
                        after = text[m.end():].strip()
                        nm = re.match(r"^([A-Z][a-z]{1,20})", after)
                        if nm:
                            name = nm.group(1)
                    break
            # fallback to entity names
            if not name and entities.get("names"):
                name = entities["names"][0]
            if not relation:
                relation = "contact"

            if not name and not relation:
                return None

            display_name = name if name else relation.capitalize()
            identity_id = f"{display_name.lower().replace(' ', '_')}_{relation}"

            mm = getattr(self, "memory_manager", None) or getattr(self.core, "memory_manager", None)
            if mm:
                try:
                    # create or update identity container defensively
                    if hasattr(mm, "get_identity_container"):
                        container = await mm.get_identity_container(identity_id)
                        if not container:
                            container = {"id": identity_id, "display_name": display_name, "name_variants": [], "relationships": {}}
                        container.setdefault("name_variants", [])
                        if display_name and display_name not in container["name_variants"]:
                            container["name_variants"].append(display_name)
                        container.setdefault("relationships", {})
                        container["relationships"]["owner"] = "owner_primary"
                        if hasattr(mm, "update_identity_container"):
                            maybe_upd = mm.update_identity_container(identity_id, container)
                            if inspect.iscoroutine(maybe_upd):
                                await maybe_upd
                        elif hasattr(mm, "put"):
                            maybe_put = mm.put(identity_id, "identity", container)
                            if inspect.iscoroutine(maybe_put):
                                await maybe_put
                        logger.info(f"Registered identity from conversation: {identity_id} / {display_name}")
                        return identity_id
                except Exception as e:
                    logger.debug(f"Identity registration via MemoryManager failed: {e}", exc_info=True)

            # fallback: persist to core.store under persona_identity_<id>
            try:
                fallback_key = f"persona_identity:{identity_id}"
                payload = {"id": identity_id, "display_name": display_name, "relation": relation, "created_at": time.time(), "entities": entities}
                if hasattr(self.core, "store") and hasattr(self.core.store, "put"):
                    maybe = self.core.store.put(fallback_key, payload)
                    if inspect.iscoroutine(maybe):
                        await maybe
                    logger.info(f"Fallback identity stored: {identity_id}")
                    return identity_id
            except Exception as e:
                logger.debug(f"Fallback identity persist failed: {e}", exc_info=True)

        except Exception:
            logger.exception("Identity extraction failed (non-fatal).")
        return None

    # -----------------------
    # Relevance & retrieval (delegates)
    # -----------------------
    def _calculate_memory_importance(self, user_input: str, assistant_response: str, intent: Dict[str, bool]) -> int:
        """Calculate intelligent memory importance based on content and context."""
        base_score = 2
        content_indicators = {
            "personal_info": any(word in user_input.lower() for word in ["name", "email", "address", "phone", "birthday", "age"]),
            "preferences": any(word in user_input.lower() for word in ["like", "prefer", "favorite", "hate", "dislike"]),
            "scheduling": any(word in user_input.lower() for word in ["meeting", "appointment", "schedule", "calendar", "remind"]),
            "important_question": any(word in user_input.lower() for word in ["important", "critical", "urgent", "emergency"]),
            "learning_request": any(word in user_input.lower() for word in ["teach", "explain", "how to", "what is", "why"]),
        }
        if intent.get('is_urgent'):
            base_score += 2
        if intent.get('is_action_oriented'):
            base_score += 1
        if intent.get('requires_memory'):
            base_score += 2
        if intent.get('is_sensitive'):
            base_score += 1

        if content_indicators["personal_info"]:
            base_score += 2
        if content_indicators["preferences"]:
            base_score += 1
        if content_indicators["scheduling"]:
            base_score += 2
        if content_indicators["important_question"]:
            base_score += 2
        if content_indicators["learning_request"]:
            base_score += 1

        if len(assistant_response) > 200:
            base_score += 1
        if hasattr(self, 'conv_buffer') and len(self.conv_buffer) > 5:
            base_score += 1

        return max(1, min(5, base_score))

    def _calculate_relevance_score(self, query: str, memory: Dict[str, Any]) -> float:
        """Enhanced relevance scoring with multiple boosting factors."""
        query_words = set(query.lower().split()) if query else set()
        memory_text = f"{memory.get('user', '')} {memory.get('assistant', '')}".lower()
        memory_words = set(memory_text.split())
        intersection = query_words.intersection(memory_words)
        union = query_words.union(memory_words)
        base_score = len(intersection) / len(union) if union else 0.0
        importance_boost = (memory.get('importance', 1) - 1) * 0.2
        memory_age_days = (time.time() - memory.get('timestamp', 0)) / 86400 if memory.get('timestamp') else 0
        recency_boost = max(0, 1 - (memory_age_days / 14)) * 0.15
        access_boost = min(0.1, memory.get('access_count', 0) * 0.02)
        return min(1.0, base_score + importance_boost + recency_boost + access_boost)

    async def retrieve_memory(self, query: str,
                              subject_identity_id: str = "owner_primary",
                              limit: int = 3) -> List[Dict[str, Any]]:
        """
        Retrieve relevant memories. Prefer MemoryManager's permission-aware retrieval if available.
        Fallbacks:
          - MemoryManager (various API names supported)
          - Local in-memory self.memory_index & semantic_index
        Side-effects (best-effort):
          - spawn a hologram read node (non-fatal)
          - increment access_count and last_touched and attempt to persist via MemoryManager or core.store
        Returns:
          - List[Dict] of memory records (most relevant first), length <= limit
        """
        query = (query or "").strip()
        node_id = f"mem_read_{uuid.uuid4().hex[:6]}"
        link_id = f"link_mem_{node_id}"

        # --- Hologram read animation (best-effort, non-blocking) ---
        try:
            spawned = await self._safe_holo_spawn(node_id, "memory", "Memory Read", 4, "Memory", link_id)
            if spawned:
                await self._safe_holo_set_active("Memory")

                async def _cleanup():
                    try:
                        await asyncio.sleep(1.2)
                        await self._safe_holo_set_idle("Memory")
                        await self._safe_holo_despawn(node_id, link_id)
                    except Exception:
                        pass
                try:
                    asyncio.create_task(_cleanup())
                except Exception:
                    pass
        except Exception:
            logger.debug("Hologram memory-read spawn failed (non-fatal).")

        results: List[Tuple[float, Dict[str, Any]]] = []
        mm = getattr(self, "memory_manager", None)

        # Try MemoryManager first (as before) ...
        if mm:
            try:
                raw = []
                if hasattr(mm, "search_memories"):
                    maybe = mm.search_memories(subject_identity_id, query=query, limit=limit)
                    raw = await maybe if inspect.iscoroutine(maybe) else maybe or []
                elif hasattr(mm, "query_memories"):
                    maybe = mm.query_memories(subject_identity_id, query=query, max_results=limit)
                    raw = await maybe if inspect.iscoroutine(maybe) else maybe or []
                elif hasattr(mm, "retrieve_memories"):
                    maybe = mm.retrieve_memories(query_text=query, subject_identity_id=subject_identity_id, limit=limit)
                    raw = await maybe if inspect.iscoroutine(maybe) else maybe or []
                elif hasattr(mm, "list_memories_for_subject"):
                    maybe = mm.list_memories_for_subject(subject_identity_id)
                    raw = await maybe if inspect.iscoroutine(maybe) else maybe or []
                else:
                    raise AttributeError("MemoryManager has no recognized search method.")

                if isinstance(raw, dict):
                    raw_list = raw.get("results") or raw.get("memories") or []
                elif isinstance(raw, list):
                    raw_list = raw
                else:
                    raw_list = []

                for mem in raw_list:
                    if not isinstance(mem, dict):
                        continue
                    if mem.get("subject_id") and mem.get("subject_id") != subject_identity_id:
                        continue
                    mem_copy = dict(mem)
                    if (self._contains_sensitive(mem_copy.get("user", "")) or self._contains_sensitive(mem_copy.get("assistant", ""))) and subject_identity_id != "owner_primary":
                        continue
                    if (self._contains_sensitive(mem_copy.get("user", "")) or self._contains_sensitive(mem_copy.get("assistant", ""))) and subject_identity_id == "owner_primary":
                        mem_copy["user"] = "[REDACTED]"
                        mem_copy["assistant"] = "[REDACTED]"

                    score = self._calculate_relevance_score(query, mem_copy)
                    if mem_copy.get("verified"):
                        score += 0.12
                    score += min(0.25, (mem_copy.get("importance", 1) - 1) * 0.05)
                    score = min(1.0, max(0.0, score))

                    if score >= RELEVANCE_THRESHOLD:
                        mem_copy["access_count"] = mem_copy.get("access_count", 0) + 1
                        mem_copy["last_touched"] = time.time()
                        results.append((score, mem_copy))
                        try:
                            if hasattr(mm, "put"):
                                maybe_up = mm.put(mem_copy.get("id"), "persona_memory", mem_copy)
                                if inspect.iscoroutine(maybe_up):
                                    await maybe_up
                            elif hasattr(mm, "update_memory"):
                                maybe_up = mm.update_memory(mem_copy.get("id"), mem_copy)
                                if inspect.iscoroutine(maybe_up):
                                    await maybe_up
                        except Exception:
                            logger.debug("MemoryManager update of access_count failed (non-fatal).")

                results.sort(key=lambda x: x[0], reverse=True)
                selected = [m for s, m in results][:limit]

                # Merge novel memories into local index (bounded)
                try:
                    for m in selected:
                        if not any(im.get("id") == m.get("id") for im in self.memory_index):
                            self.memory_index.append(m)
                    if len(self.memory_index) > MAX_MEMORY_ENTRIES:
                        self.memory_index = self.memory_index[-MAX_MEMORY_ENTRIES:]
                except Exception:
                    pass

                return selected

            except Exception as e:
                logger.warning(f"MemoryManager retrieval failed, falling back to local retrieval: {e}", exc_info=True)
                # fall through to local retrieval

        # Local in-memory fallback, augmented with semantic_index scoring
        try:
            pool = [m for m in list(reversed(self.memory_index)) if m.get("subject_id") == subject_identity_id]
            # Use semantic_index to quickly locate relevant items first
            sem_hits = []
            if query:
                q_words = set(query.lower().split())
                for s in reversed(self.semantic_index):
                    ent_text = " ".join(sum((v for v in s.get("entities", {}).values() if isinstance(v, list)), []))
                    score = 0.0
                    if any(q in ent_text.lower() for q in q_words):
                        score = 0.6
                    if s.get("importance", 1) >= 4:
                        score += 0.15
                    if score > 0:
                        sem_hits.append((score, s))
                sem_hits.sort(key=lambda x: x[0], reverse=True)
                for sc, sr in sem_hits[:limit]:
                    # try to find the full memory entry from memory_index mapping by memory_id
                    mem_match = next((m for m in self.memory_index if m.get("id") == sr.get("memory_id")), None)
                    if mem_match:
                        results.append((min(1.0, sc + 0.1), dict(mem_match)))

            # If not enough hits, fallback to scanning pool
            if len(results) < limit:
                for memory in pool:
                    if not isinstance(memory, dict):
                        continue
                    mem_copy = dict(memory)
                    if (self._contains_sensitive(mem_copy.get("user", "")) or self._contains_sensitive(mem_copy.get("assistant", ""))) and subject_identity_id != "owner_primary":
                        continue
                    if (self._contains_sensitive(mem_copy.get("user", "")) or self._contains_sensitive(mem_copy.get("assistant", ""))) and subject_identity_id == "owner_primary":
                        mem_copy["user"] = "[REDACTED]"
                        mem_copy["assistant"] = "[REDACTED]"

                    score = self._calculate_relevance_score(query, mem_copy)
                    if mem_copy.get("verified"):
                        score += 0.12
                    score += min(0.25, (mem_copy.get("importance", 1) - 1) * 0.05)
                    score = min(1.0, max(0.0, score))

                    if score >= RELEVANCE_THRESHOLD:
                        mem_copy["access_count"] = mem_copy.get("access_count", 0) + 1
                        mem_copy["last_touched"] = time.time()
                        results.append((score, mem_copy))

                        # Persist access_count back to core.store (best-effort)
                        try:
                            if hasattr(self.core, 'store') and hasattr(self.core.store, "put"):
                                put = getattr(self.core.store, "put")
                                try:
                                    maybe_put = put(mem_copy.get("id"), "persona_memory", mem_copy)
                                except TypeError:
                                    maybe_put = put(mem_copy.get("id"), mem_copy)
                                if inspect.iscoroutine(maybe_put):
                                    await maybe_put
                        except Exception:
                            logger.debug("core.store update of memory access_count failed (non-fatal).")

            # Consolidate and sort unique results
            unique = {}
            for sc, mem in results:
                if mem.get("id") not in unique or unique[mem.get("id")][0] < sc:
                    unique[mem.get("id")] = (sc, mem)
            sorted_list = sorted(unique.values(), key=lambda x: x[0], reverse=True)
            selected = [m for s, m in sorted_list][:limit]
            return selected

        except Exception as e:
            logger.error(f"Local memory retrieval failed: {e}", exc_info=True)
            return []

    # -----------------------
    # Safety / Classification / Utilities
    # -----------------------
    def _contains_sensitive(self, text: str) -> bool:
        """Check if text contains sensitive patterns."""
        if not text:
            return False
        return any(pattern.search(text) for pattern in SENSITIVE_PATTERNS)

    def _classify_query_intent(self, text: str) -> Dict[str, bool]:
        """Enhanced query intent classification."""
        text_lower = (text or "").lower()
        intents = {
            "is_sensitive": self._contains_sensitive(text_lower),
            "is_action_oriented": any(w in text_lower for w in ["schedule", "add", "create", "find", "search", "get", "set"]),
            "is_informational": any(w in text_lower for w in ["what", "how", "when", "why", "explain", "tell me about"]),
            "is_social": any(w in text_lower for w in ["hello", "hi", "how are you", "thanks", "good morning", "hey"]),
            "requires_memory": any(w in text_lower for w in ["remember", "recall", "before", "previously", "last time"]),
            "is_urgent": any(w in text_lower for w in ["urgent", "asap", "emergency", "critical", "important"]),
        }
        for intent, detected in intents.items():
            if detected:
                intent_key = intent.replace("is_", "")
                if intent_key in self.metrics.intent_distribution:
                    self.metrics.intent_distribution[intent_key] += 1
        return intents

    def _sanitize_response(self, text: str) -> str:
        """Apply safety sanitization to redact sensitive patterns from response text."""
        if not text:
            return text
        sanitized = text
        for pattern in SENSITIVE_PATTERNS:
            sanitized = pattern.sub("[REDACTED]", sanitized)
        return sanitized

    def _load_manual_context(self, filename: str = "my_context.txt") -> Optional[str]:
        """Loads persistent user context with comprehensive file discovery and safety checks."""
        locations = [
            os.path.dirname(os.path.abspath(sys.argv[0] if sys.argv else __file__)),
            os.getcwd(),
            os.path.expanduser("~/.aaria"),
        ]
        for location in locations:
            try:
                context_path = os.path.join(location, filename)
                if os.path.exists(context_path):
                    with open(context_path, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        if content:
                            if self._contains_sensitive(content):
                                logger.warning(f"Manual context in {context_path} contains sensitive data and was NOT loaded.")
                                self.metrics.safety_trigger_count += 1
                                return None
                            logger.info(f"📁 Loaded manual context from {context_path}")
                            return f"USER-PROVIDED MANUAL CONTEXT:\n{content}"
            except Exception as e:
                logger.warning(f"Failed to load context from {location}: {e}", exc_info=True)
                continue
        return None

    def push_interaction(self, role: str, content: str) -> None:
        """Add a validated interaction to the short-term conversation buffer."""
        if content and content.strip():
            self.conv_buffer.append({
                "role": role, "content": content, "timestamp": time.time()
            })

    # -----------------------
    # Prompt building & Respond flow
    # -----------------------
    def _build_prompt(self, user_input: str, include_memory: bool = True) -> List[Dict[str, str]]:
        """
        Construct optimized prompt with layered context and safety.
        """
        messages = [{"role": "system", "content": self.system_prompt}] if self.system_prompt else []

        manual_context = self._load_manual_context()
        if manual_context:
            messages.append({"role": "system", "content": manual_context})

        # Include conversation history (last 6 exchanges)
        for interaction in list(self.conv_buffer)[-6:]:
            messages.append({"role": interaction["role"], "content": interaction["content"]})

        messages.append({"role": "user", "content": user_input})
        return messages

    def _get_importance_reason(self, user_input: str, intent: Dict[str, bool], importance: int) -> str:
        """Generate a human-readable reason for the importance score."""
        reasons = []
        content_words = (user_input or "").lower().split()
        if any(word in content_words for word in ["urgent", "asap", "emergency"]):
            reasons.append("urgent_request")
        if any(word in content_words for word in ["schedule", "add", "create"]):
            reasons.append("action_oriented")
        if any(word in content_words for word in ["remember", "recall", "before"]):
            reasons.append("memory_dependent")
        if any(word in content_words for word in ["name", "email", "address"]):
            reasons.append("personal_info")
        if any(word in content_words for word in ["meeting", "schedule", "remind"]):
            reasons.append("scheduling")
        if any(word in content_words for word in ["important", "critical"]):
            reasons.append("explicit_importance")

        if intent.get('is_urgent'):
            reasons.append("intent_urgent")
        if intent.get('is_action_oriented'):
            reasons.append("intent_action")
        if intent.get('requires_memory'):
            reasons.append("intent_memory")

        return "+".join(reasons) if reasons else "general_conversation"

    async def respond(self, user_input: str, use_llm: bool = True, include_memory: bool = True) -> str:
        start_time = time.time()

        # Node IDs for hologram tracing (best-effort)
        input_node_id = f"input_{uuid.uuid4().hex[:6]}"
        input_link_id = f"link_in_{input_node_id}"
        intent_node_id = f"intent_{uuid.uuid4().hex[:6]}"
        intent_link_id = f"link_cog_{intent_node_id}"
        response_node_id = f"resp_{uuid.uuid4().hex[:6]}"
        response_link_id = f"link_per_{response_node_id}"

        # Spawn input holo node (guarded)
        if hologram_state is not None:
            try:
                maybe = hologram_state.spawn_and_link(
                    node_id=input_node_id, node_type="input", label=f"Input: {user_input[:20]}...", size=5,
                    source_id="PersonaCore", link_id=input_link_id
                )
                if inspect.iscoroutine(maybe):
                    await maybe
                if hasattr(hologram_state, "set_node_active"):
                    maybe2 = hologram_state.set_node_active("PersonaCore")
                    if inspect.iscoroutine(maybe2):
                        await maybe2
            except Exception:
                logger.debug("Hologram input spawn failed (non-fatal).")

        try:
            self.metrics.total_interactions += 1
            self.metrics.user_messages += 1
            logger.debug(f"🎯 RESPOND METHOD START: '{user_input}'")
            self.push_interaction("user", user_input)

            # --- Attempt to extract and register any identity mentions (best-effort, non-blocking) ---
            try:
                asyncio.create_task(self._extract_and_register_identity(user_input))
            except Exception:
                try:
                    maybe = self._extract_and_register_identity(user_input)
                    if inspect.iscoroutine(maybe):
                        await maybe
                except Exception:
                    pass

            # Intent classification (with holo)
            if hologram_state is not None:
                try:
                    maybe = hologram_state.spawn_and_link(
                        node_id=intent_node_id, node_type="cognition", label="Classify Intent", size=4,
                        source_id="CognitionCore", link_id=intent_link_id
                    )
                    if inspect.iscoroutine(maybe):
                        await maybe
                    if hasattr(hologram_state, "set_node_active"):
                        maybe2 = hologram_state.set_node_active("CognitionCore")
                        if inspect.iscoroutine(maybe2):
                            await maybe2
                except Exception:
                    logger.debug("Hologram intent spawn failed (non-fatal).")

            intent = self._classify_query_intent(user_input)
            logger.debug(f"🎯 Intent classified: {intent}")

            if hologram_state is not None:
                try:
                    if hasattr(hologram_state, "set_node_idle"):
                        maybe = hologram_state.set_node_idle("CognitionCore")
                        if inspect.iscoroutine(maybe):
                            await maybe
                    if hasattr(hologram_state, "despawn_and_unlink"):
                        maybe2 = hologram_state.despawn_and_unlink(intent_node_id, intent_link_id)
                        if inspect.iscoroutine(maybe2):
                            await maybe2
                except Exception:
                    pass

            if intent.get("is_sensitive"):
                logger.debug("🎯 Taking SENSITIVE path - early return")
                self.metrics.safety_trigger_count += 1
                reply = "I cannot process that request as it appears to contain sensitive information. Please use secure channels for such data."
                self.push_interaction("assistant", reply)
                return reply

            cache_key = f"{hashlib.md5((user_input or '').encode()).hexdigest()[:12]}_{intent.get('is_action_oriented')}"
            current_time = time.time()
            cached = self.response_cache.get(cache_key)
            if (cached and not intent.get('is_urgent') and current_time - cached[1] < CACHE_TTL_SECONDS):
                logger.debug("🎯 Taking CACHE HIT path - early return")
                reply = cached[0]
                self.push_interaction("assistant", reply)
                self.metrics.cache_hit_count += 1
                return reply

            lower_input = (user_input or "").strip().lower()
            quick_responses = {
                "what's up": "Sharp and ready. What shall we conquer today?",
                "how are you": "Operating at peak efficiency. How can I assist?",
                "hello": "Hello! A.A.R.I.A. here — what's on your mind?",
                "thanks": "You're welcome! Always here to help.",
                "help": "I can help with scheduling, contacts, information, and planning. What do you need?"
            }
            if lower_input in quick_responses:
                logger.debug("🎯 Taking QUICK RESPONSE path - early return")
                reply = quick_responses[lower_input]
                self.push_interaction("assistant", reply)
                self.response_cache[cache_key] = (reply, current_time)
                return reply

            if not use_llm:
                logger.debug("🎯 Taking NO-LLM path - early return")
                reply = "I can help with that — shall I proceed with the action?"
                self.push_interaction("assistant", reply)
                return reply

            logger.debug("🎯 Taking LLM path - proceeding to LLM call")
            # Spawn response holo node (guarded)
            if hologram_state is not None:
                try:
                    maybe = hologram_state.spawn_and_link(
                        node_id=response_node_id, node_type="persona", label="Generate Response", size=6,
                        source_id="PersonaCore", link_id=response_link_id
                    )
                    if inspect.iscoroutine(maybe):
                        await maybe
                except Exception:
                    logger.debug("Hologram response spawn failed (non-fatal).")

            try:
                # Build prompt including memory retrieval performed asynchronously here
                messages = [{"role": "system", "content": self.system_prompt}] if self.system_prompt else []
                manual_context = self._load_manual_context()
                if manual_context:
                    messages.append({"role": "system", "content": manual_context})

                # Retrieve memories (async)
                memories = await self.retrieve_memory(user_input, subject_identity_id="owner_primary", limit=3) if include_memory else []
                if memories:
                    self.metrics.memory_hit_count += 1
                    memory_content = "RELEVANT PAST CONTEXT:\n" + "\n".join(
                        [f"- User: {m['user'][:80]}... Assistant: {m['assistant'][:100]}... "
                        f"(importance: {m.get('importance',1)}/5, accesses: {m.get('access_count',0)})"
                        for m in memories]
                    )
                    messages.append({"role": "system", "content": memory_content})

                # Conversation buffer
                for interaction in list(self.conv_buffer)[-6:]:
                    messages.append({"role": interaction["role"], "content": interaction["content"]})
                messages.append({"role": "user", "content": user_input})

                primary_provider = LLMProvider(self.config.get("primary_provider", "groq"))
                logger.debug(f"🎯 About to call LLM with {len(messages)} messages")
                async with self.llm_orchestrator.get_adapter(primary_provider) as adapter:
                    request = LLMRequest(messages=messages, max_tokens=600, temperature=0.35)
                    llm_response = await adapter.chat(request)
                    reply_text = llm_response.content.strip()
                    logger.debug(f"🎯 Got LLM response: '{reply_text[:50]}...'")
            except (LLMConfigurationError, LLMError) as e:
                logger.error(f"🔧 LLM communication error: {e}", exc_info=True)
                self.metrics.error_count += 1
                reply_text = "I'm having trouble connecting to my reasoning capabilities. Please check the system configuration and try again."
                if hologram_state is not None and hasattr(hologram_state, "set_node_error"):
                    try:
                        maybe = hologram_state.set_node_error(response_node_id)
                        if inspect.iscoroutine(maybe):
                            await maybe
                    except Exception:
                        pass
            except asyncio.TimeoutError:
                logger.error("⏰ LLM request timeout")
                self.metrics.error_count += 1
                reply_text = "The request timed out. Please try again in a moment."
                if hologram_state is not None and hasattr(hologram_state, "set_node_error"):
                    try:
                        maybe = hologram_state.set_node_error(response_node_id)
                        if inspect.iscoroutine(maybe):
                            await maybe
                    except Exception:
                        pass
            except Exception as e:
                logger.exception("💥 Unexpected error during response generation")
                self.metrics.error_count += 1
                reply_text = "I've encountered an unexpected system error. My apologies for the inconvenience."
                if hologram_state is not None and hasattr(hologram_state, "set_node_error"):
                    try:
                        maybe = hologram_state.set_node_error(response_node_id)
                        if inspect.iscoroutine(maybe):
                            await maybe
                    except Exception:
                        pass

            response_time = time.time() - start_time
            self._response_times.append(response_time)
            self.metrics.average_response_time = sum(self._response_times) / len(self._response_times)

            sanitized_reply = self._sanitize_response(reply_text)
            logger.debug(f"🎯 Response sanitized: '{sanitized_reply[:50]}...'")

            importance = self._calculate_memory_importance(user_input, sanitized_reply, intent)

            # Mark scheduling attempts with metadata if LLM suggested calendar actions
            metadata = {}
            try:
                if "calendar.add" in sanitized_reply or "calendar.list" in sanitized_reply or "remind" in user_input.lower():
                    metadata["requested_schedule_text"] = user_input
            except Exception:
                pass

            memory_id = await self.store_memory(user_input, sanitized_reply,
                                                importance=importance, metadata=metadata, intent=intent,
                                                subject_identity_id="owner_primary")
            logger.debug(f"💭 Stored conversation memory (ID: {memory_id}, importance: {importance})")

            self.push_interaction("assistant", sanitized_reply)
            self.metrics.assistant_messages += 1

            if len(reply_text) < 500 and not intent.get('is_urgent'):
                self.response_cache[cache_key] = (sanitized_reply, current_time)

            logger.info(f"⚡ Generated response in {response_time:.2f}s (avg: {self.metrics.average_response_time:.2f}s)")
            return sanitized_reply

        finally:
            # cleanup hologram nodes (best-effort)
            if hologram_state is not None:
                try:
                    if hasattr(hologram_state, "set_node_idle"):
                        maybe = hologram_state.set_node_idle("PersonaCore")
                        if inspect.iscoroutine(maybe):
                            await maybe
                    if hasattr(hologram_state, "despawn_and_unlink"):
                        maybe2 = hologram_state.despawn_and_unlink(input_node_id, input_link_id)
                        if inspect.iscoroutine(maybe2):
                            await maybe2
                    if hasattr(hologram_state, "despawn_and_unlink"):
                        maybe3 = hologram_state.despawn_and_unlink(response_node_id, response_link_id)
                        if inspect.iscoroutine(maybe3):
                            await maybe3
                except Exception:
                    pass

    # -----------------------
    # Diagnostics / Persistence helpers
    # -----------------------
    def get_conversation_analytics(self) -> Dict[str, Any]:
        total_interactions = max(1, self.metrics.total_interactions)
        return {
            "total_interactions": self.metrics.total_interactions,
            "user_messages": self.metrics.user_messages,
            "assistant_messages": self.metrics.assistant_messages,
            "avg_response_time_sec": self.metrics.average_response_time,
            "error_rate_percent": (self.metrics.error_count / total_interactions) * 100,
            "cache_hit_rate_percent": (self.metrics.cache_hit_count / total_interactions) * 100,
            "memory_utilization_percent": (len(self.memory_index) / MAX_MEMORY_ENTRIES) * 100,
            "memory_hit_rate_percent": (self.metrics.memory_hit_count / max(1, self.metrics.user_messages)) * 100,
            "safety_triggers": self.metrics.safety_trigger_count,
            "intent_distribution": self.metrics.intent_distribution,
            "response_cache_size": len(self.response_cache),
            "conversation_buffer_size": len(self.conv_buffer),
        }

    def debug_memory_state(self) -> Dict[str, Any]:
        return {
            "total_memories": len(self.memory_index),
            "recent_memories": [m for m in self.memory_index[-5:] if m.get('timestamp', 0) > time.time() - 3600],
            "important_memories": len([m for m in self.memory_index if m.get('importance', 1) >= 4]),
            "memory_access_stats": {
                "total_accesses": sum(m.get('access_count', 0) for m in self.memory_index),
                "avg_importance": sum(m.get('importance', 1) for m in self.memory_index) / max(1, len(self.memory_index))
            }
        }

    def debug_memory_content(self) -> None:
        logger.info("🧠 MEMORY DEBUG - Current memory contents:")
        for i, memory in enumerate(self.memory_index[-10:]):
            logger.info(f"  Memory {i+1}: User: '{memory.get('user', '')[:60]}...' -> Assistant: '{memory.get('assistant', '')[:60]}...'")

    async def health_check(self) -> Dict[str, Any]:
        health_status = {
            "is_ready": self.is_ready(),
            "llm_connectivity": "pending_test",
            "memory_health": len(self.memory_index) > 0,
            "cache_health": len(self.response_cache) < 1000,
            "safety_systems": True,
            "metrics_tracking": True
        }
        try:
            primary_provider = LLMProvider(self.config.get("primary_provider", "groq"))
            async with self.llm_orchestrator.get_adapter(primary_provider) as adapter:
                health = await asyncio.wait_for(adapter.health_check(), timeout=10.0)
                # adapter.health_check may return an object or dict
                if hasattr(health, "status"):
                    health_status["llm_connectivity"] = getattr(health, "status")
                elif isinstance(health, dict):
                    health_status["llm_connectivity"] = health.get("status", "unknown")
                else:
                    health_status["llm_connectivity"] = str(health)
        except asyncio.TimeoutError:
            health_status["llm_connectivity"] = "timeout"
        except Exception as e:
            health_status["llm_connectivity"] = f"error: {str(e)[:100]}"
        return health_status

    # -----------------------
    # Daemon loops (proactive, maintenance, cognition monitor)
    # -----------------------
    async def _proactive_loop(self):
        """
        Continuous human-like proactive loop:
         - scans semantic_index and memory_index for upcoming events or triggers
         - occasionally sends gentle check-ins or suggestions
         - uses ProactiveCommunicator.enqueue_message for outbound notifications
         - creates/updates a hologram 'ProactiveDaemon' node (best-effort)
        Runs indefinitely while self._running is True.
        """
        node_id = f"proactive_node_{uuid.uuid4().hex[:6]}"
        link_id = f"link_{node_id}"
        try:
            await self._safe_holo_spawn(node_id, "daemon", "ProactiveDaemon", 10, "PersonaCore", link_id)
            await self._safe_holo_set_active("ProactiveDaemon")
        except Exception:
            pass

        while self._running:
            try:
                # Randomized human-like jitter
                jitter = random.uniform(-PROACTIVE_LOOP_INTERVAL_SEC * 0.25, PROACTIVE_LOOP_INTERVAL_SEC * 0.25)
                await asyncio.sleep(max(1.0, PROACTIVE_LOOP_INTERVAL_SEC + jitter))

                # 1) Health-triggered proactive: notify if cognition degraded
                try:
                    health = await self.health_check()
                    if health.get("llm_connectivity") not in ("ok", "healthy", "unknown") and "error" in str(health.get("llm_connectivity", "")).lower():
                        # notify owner via proactive channel if available
                        payload = {"type": "health_alert", "summary": "LLM connectivity degraded", "details": health.get("llm_connectivity")}
                        try:
                            await self.enqueue_proactive_notification("owner_primary", "system", payload)
                        except Exception:
                            pass
                except Exception:
                    pass

                # 2) Memory-triggered proactive: scan semantic_index for high-importance upcoming schedule mentions
                try:
                    now_ts = time.time()
                    candidate = None
                    # quick heuristic: look for memories that mention 'remind', 'meeting', or have 'requested_schedule_text' metadata
                    for mem in reversed(self.memory_index[-300:]):
                        md = mem.get("metadata", {})
                        if mem.get("importance", 1) >= 4 or "requested_schedule_text" in md:
                            candidate = mem
                            break
                    if candidate:
                        # gentle check-in with context
                        payload = {
                            "type": "reminder_suggestion",
                            "summary": f"Reminder candidate from memory (importance {candidate.get('importance')})",
                            "memory_id": candidate.get("id"),
                            "preview": candidate.get("user")[:140]
                        }
                        try:
                            await self.enqueue_proactive_notification("owner_primary", "reminder", payload)
                        except Exception:
                            pass
                except Exception:
                    pass

                # 3) Occasional human-like message (low-frequency)
                if random.random() < 0.08:
                    note = random.choice([
                        "Quick check: anything you want me to remember right now?",
                        "Heads up — nothing urgent on your calendar in the next hour.",
                        "I've summarized a recent conversation that might be useful — would you like a quick look?"
                    ])
                    try:
                        await self.enqueue_proactive_notification("owner_primary", "checkin", {"type":"checkin","message":note})
                    except Exception:
                        pass

                # 4) Hologram heartbeat update
                try:
                    await self._safe_holo_update_link(link_id, intensity=random.random())
                except Exception:
                    pass

            except asyncio.CancelledError:
                break
            except Exception:
                # swallow errors to keep daemon alive
                logger.exception("Proactive daemon loop error (non-fatal)")
                # small backoff
                await asyncio.sleep(2.0)
        # cleanup holo node
        try:
            await self._safe_holo_set_idle("ProactiveDaemon")
            await self._safe_holo_despawn(node_id, link_id)
        except Exception:
            pass

    async def _maintenance_loop(self):
        """
        Periodic maintenance:
         - persist memory indices
         - prune old transcripts / semantic entries
         - ensure MemoryManager sync
        """
        while self._running:
            try:
                await asyncio.sleep(MAINTENANCE_LOOP_INTERVAL_SEC + random.uniform(-5, 5))
                # Save memory index
                try:
                    await self._save_persistent_memory()
                except Exception:
                    logger.debug("Maintenance: failed to save memory index (non-fatal)")
                # Persist transcripts/semantic if memory_manager present
                try:
                    mm = getattr(self, "memory_manager", None)
                    if mm:
                        if hasattr(mm, "save_transcript_store"):
                            maybe = mm.save_transcript_store(list(self.transcript_store))
                            if inspect.iscoroutine(maybe):
                                await maybe
                        if hasattr(mm, "save_semantic_index"):
                            maybe = mm.save_semantic_index(self.semantic_index)
                            if inspect.iscoroutine(maybe):
                                await maybe
                except Exception:
                    logger.debug("Maintenance: memory_manager sync failed (non-fatal)")
                # Local pruning
                try:
                    # transcripts pruned by deque maxlen; prune semantic older than X days
                    cutoff = time.time() - (MAX_MEMORY_AGE_DAYS * 24 * 60 * 60)
                    si_before = len(self.semantic_index)
                    self.semantic_index = [s for s in self.semantic_index if s.get("timestamp", 0) >= cutoff or s.get("importance", 1) >= 4]
                    if len(self.semantic_index) != si_before:
                        logger.info(f"Maintenance: pruned semantic_index from {si_before} to {len(self.semantic_index)}")
                except Exception:
                    pass
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Maintenance daemon error (non-fatal)")
                await asyncio.sleep(2.0)

    async def _cognition_monitor(self):
        """
        Monitors cognition health & metrics and triggers self-healing or notifications.
        If repeated degraded health is detected, escalate proactively.
        """
        degraded_count = 0
        while self._running:
            try:
                await asyncio.sleep(COGNITION_MONITOR_INTERVAL_SEC + random.uniform(-5, 5))
                try:
                    health = await self.health_check()
                    status = str(health.get("llm_connectivity", "unknown")).lower()
                    if "error" in status or status in ("timeout", "failed"):
                        degraded_count += 1
                    else:
                        degraded_count = max(0, degraded_count - 1)
                    if degraded_count >= 3:
                        # escalate
                        try:
                            await self.enqueue_proactive_notification("owner_primary", "alert", {"type":"cognition_degraded","summary":"Cognition health degraded repeatedly","details":health})
                        except Exception:
                            pass
                        # try a lightweight self-restart of LLM adapter (best-effort)
                        try:
                            primary_provider = LLMProvider(self.config.get("primary_provider", "groq"))
                            async with self.llm_orchestrator.get_adapter(primary_provider) as adapter:
                                if hasattr(adapter, "reset") and inspect.iscoroutinefunction(adapter.reset):
                                    await adapter.reset()
                        except Exception:
                            pass
                except Exception:
                    logger.debug("Cognition monitor check failed (non-fatal)")
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Cognition monitor error (non-fatal)")
                await asyncio.sleep(2.0)

    # -----------------------
    # Shutdown / Persistence helpers
    # -----------------------
    async def close(self) -> None:
        logger.info("🛑 Beginning Enhanced PersonaCore shutdown...")
        try:
            # Stop daemons
            self._running = False
            # Cancel tasks
            for t in list(self._daemon_tasks):
                try:
                    t.cancel()
                except Exception:
                    pass
            # await tasks cancellation (best-effort)
            for t in list(self._daemon_tasks):
                try:
                    await asyncio.wait_for(t, timeout=3.0)
                except Exception:
                    pass
            self._daemon_tasks = []

            # Persist via MemoryManager if available
            mm = getattr(self, "memory_manager", None)
            if mm:
                try:
                    if hasattr(mm, "save_index"):
                        maybe = mm.save_index({"memory_index": self.memory_index})
                        if inspect.iscoroutine(maybe):
                            await maybe
                        logger.info("MemoryManager: saved index on PersonaCore.close()")
                    elif hasattr(mm, "_save_persistent_memory"):
                        maybe = mm._save_persistent_memory()
                        if inspect.iscoroutine(maybe):
                            await maybe
                        logger.info("MemoryManager: invoked _save_persistent_memory() on close")
                    # persist transcript/semantic if supported
                    if hasattr(mm, "save_transcript_store"):
                        maybe = mm.save_transcript_store(list(self.transcript_store))
                        if inspect.iscoroutine(maybe):
                            await maybe
                    if hasattr(mm, "save_semantic_index"):
                        maybe = mm.save_semantic_index(self.semantic_index)
                        if inspect.iscoroutine(maybe):
                            await maybe
                except Exception as e:
                    logger.warning(f"MemoryManager save on close failed: {e}", exc_info=True)
            # Local fallback
            await self._save_persistent_memory()
            await self._persist_critical_data()
            # Clean up expired cache entries
            current_time = time.time()
            self.response_cache = {k: v for k, v in self.response_cache.items() if current_time - v[1] < CACHE_TTL_SECONDS}
            logger.info(f"✅ PersonaCore shutdown complete - {len(self.memory_index)} memories in index, {len([m for m in self.memory_index if m.get('importance', 1) >= 4])} important memories.")
        except Exception as e:
            logger.error(f"❌ Error during PersonaCore shutdown: {e}", exc_info=True)
        finally:
            # Ensure proactive communicator is stopped
            try:
                stop_maybe = getattr(self.proactive, "stop", None)
                if stop_maybe:
                    if inspect.iscoroutinefunction(stop_maybe):
                        await stop_maybe()
                    else:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, stop_maybe)
            except Exception:
                logger.exception("Proactive communicator stop failed (nonfatal).")

    async def _persist_critical_data(self) -> None:
        """Persist critical memories before shutdown."""
        try:
            important_memories = [m for m in self.memory_index if m.get('access_count', 0) > 2 or m.get('importance', 1) >= 4]
            mm = getattr(self, "memory_manager", None)
            for memory in important_memories:
                if mm:
                    try:
                        if hasattr(mm, "put"):
                            maybe = mm.put(f"critical_{memory['id']}", "critical_memory", memory)
                            if inspect.iscoroutine(maybe):
                                await maybe
                            continue
                        elif hasattr(mm, "persist_important"):
                            maybe = mm.persist_important(memory['subject_id'], memory)
                            if inspect.iscoroutine(maybe):
                                await maybe
                            continue
                    except Exception as e:
                        logger.warning(f"MemoryManager persist_important failed: {e}", exc_info=True)
                # fallback to core.store
                if hasattr(self.core, 'store') and hasattr(self.core.store, "put"):
                    try:
                        put = getattr(self.core.store, "put")
                        try:
                            maybe = put(f"critical_{memory['id']}", "critical_memory", memory)
                        except TypeError:
                            maybe = put(f"critical_{memory['id']}", memory)
                        if inspect.iscoroutine(maybe):
                            await maybe
                    except Exception as e:
                        logger.warning(f"Failed to persist critical memory via core.store: {e}", exc_info=True)
        except Exception as e:
            logger.warning(f"Failed to persist critical data: {e}", exc_info=True)

    async def plan(self, goal_text: str, horizon_days: int = 7) -> Dict[str, Any]:
        """Enhanced asynchronous planning with better error recovery."""
        prompt = f"Create a concise multi-step plan to achieve this goal in {horizon_days} days: {goal_text}\nReturn JSON with 'steps' (title, due, notes) and 'summary'."
        messages = [{"role": "system", "content": self.system_prompt}, {"role": "user", "content": prompt}]
        try:
            primary_provider = LLMProvider(self.config.get("primary_provider", "groq"))
            async with self.llm_orchestrator.get_adapter(primary_provider) as adapter:
                request = LLMRequest(messages=messages, max_tokens=800, temperature=0.2)
                llm_response = await adapter.chat(request)
                json_text = self._extract_json(getattr(llm_response, "content", str(llm_response)))
                if json_text:
                    plan = json.loads(json_text)
                    if isinstance(plan, dict) and "steps" in plan:
                        return plan
            # Fallback plan
            resp_text = getattr(llm_response, "content", str(llm_response))
            return {
                "summary": resp_text[:200] + "..." if len(resp_text) > 200 else resp_text,
                "steps": [{"title": "Review and refine plan", "due": "Today", "notes": "Assess feasibility"}]
            }
        except Exception as e:
            logger.error(f"Plan generation failed: {e}", exc_info=True)
            return {"summary": "Unable to generate detailed plan at this time.", "steps": []}

    @staticmethod
    def _extract_json(text: str) -> Optional[str]:
        """Robust JSON extraction with multiple strategies."""
        patterns = [
            r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',
            r'\[[^\[\]]*(?:\[[^\[\]]*\][^\[\]]*)*\]',
        ]
        for pattern in patterns:
            try:
                matches = re.findall(pattern, text, re.DOTALL)
                for match in matches:
                    json.loads(match)
                    return match
            except (json.JSONDecodeError, re.error):
                continue
        code_blocks = re.findall(r'```(?:json)?\s*(.*?)\s*```', text, re.DOTALL)
        for block in code_blocks:
            try:
                json.loads(block.strip())
                return block.strip()
            except json.JSONDecodeError:
                continue
        return None

    async def enqueue_proactive_notification(self, subject_id, channel, payload, **kwargs):
        return await self.proactive.enqueue_message(subject_id, channel, payload, **kwargs)
