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
from datetime import datetime
from zoneinfo import ZoneInfo
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
        
        # --- FIX: These are now CACHES, not the source of truth ---
        self.memory_index: List[Dict[str, Any]] = []
        self.transcript_store: deque = deque(maxlen=5000)
        self.semantic_index: List[Dict[str, Any]] = []
        # --- END FIX ---

        self.llm_orchestrator = LLMAdapterFactory
        self.response_cache: Dict[str, Tuple[str, float]] = {}
        self._interaction_count = 0
        self._response_times: List[float] = []
        self.metrics = ConversationMetrics()
        self.tone_instructions = "Witty, succinct when short answer requested, elaborative when asked."
        self._initialized = False

        # Memory manager will be injected by main.py (AARIASystem) if available.
        self.memory_manager: Optional[Any] = None

        # --- FIX: Proactive communicator will be injected by main.py ---
        self.proactive: Optional[ProactiveCommunicator] = None
        # --- END FIX ---

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

        # --- FIX: Removed call to self._load_persistent_memory() ---
        # --- Memory is now loaded on-demand by retrieve_memory ---

        self._cleanup_old_memories()

        # Add a test memory to verify the system works
        if not self.memory_manager:
            logger.error("No MemoryManager present; skipping test memory creation.")
        elif len(self.memory_index) == 0: # Note: self.memory_index is just a cache
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
                if hasattr(mm, "load_transcript_store"): # This method doesn't exist, but good to check
                    maybe_ts = getattr(mm, "load_transcript_store", None)
                    if maybe_ts:
                        ts = maybe_ts()
                        ts = await ts if inspect.iscoroutine(ts) else ts
                        if isinstance(ts, list):
                            for t in ts[-1000:]:
                                self.transcript_store.append(t)
                if hasattr(mm, "load_semantic_index"): # This method doesn't exist, but good to check
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
            # --- ADD THIS LINE FOR IST ---
            IST = ZoneInfo("Asia/Kolkata")
            # --- END ADD ---
            
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
            
            # --- FIX: Set default timezone to IST ---
            timezone_str = profile.get("timezone", "IST").strip() or "IST"
            try:
                # Validate timezone, fallback to IST
                tz = ZoneInfo(timezone_str)
            except Exception:
                tz = IST
                timezone_str = "IST"
            current_date = datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")
            # --- END FIX ---

            # --- FIX: Removed TOOL_INSTRUCTION_BLOCK to prevent hallucination ---
            self.system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
                user_name=user_name,
                timezone=timezone_str,
                current_date=current_date,
                tone_instructions=self.tone_instructions
            )
            # --- END FIX ---

            logger.info(f"✅ Enhanced system prompt initialized for user: {user_name}")
        except Exception as e:
            logger.error(f"Failed to initialize system prompt: {e}", exc_info=True)
            # Fallback prompt, also without tools
            IST = ZoneInfo("Asia/Kolkata")
            self.system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
                user_name="User",
                timezone="IST",
                current_date=datetime.now(IST).strftime("%Y-%m-%d %H:%M %Z"),
                tone_instructions=self.tone_instructions
            )

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
        --- ARCHITECTURE FIX ---
        This method now implements the full dual-memory and hologram logic,
        but delegates the *persistence* of all three records (main, transcript, semantic)
        to the MemoryManager.
        """
        
        if not self.memory_manager:
            logger.error("MemoryManager not injected into PersonaCore. Cannot store memory.")
            return "error_no_manager"

        # --- HOLOGRAM LOGIC (KEPT) ---
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
            # --- DUAL-MEMORY LOGIC (KEPT) ---
            user_input = (user_input or "").strip()
            assistant_response = (assistant_response or "").strip()
            intent = intent or self._classify_query_intent(user_input)
            content_hash = hashlib.md5(f"{user_input}{assistant_response}".encode("utf-8")).hexdigest()[:16]

            # --- DELEGATION TO MEMORY MANAGER ---
            # 1. Store the main record (MemoryManager handles segmentation & duplicates)
            mem_id, segment = await self.memory_manager.store_memory(
                user_input=user_input,
                assistant_response=assistant_response,
                subject_identity_id=subject_identity_id,
                importance=importance,
                metadata=metadata,
                actor="persona"
            )

            # Check for duplicates
            if mem_id == "duplicate":
                logger.debug(f"Memory duplicate detected by MemoryManager for subject {subject_identity_id}")
                return "duplicate"
            
            # --- FIX: Manually update the full record *after* it's created ---
            # This ensures all fields (like 'source', 'confidence') are saved
            memory_record = await self.memory_manager.get(mem_id) # Get the full record back
            if not memory_record:
                 # This should not happen, but as a safeguard:
                memory_record = {}
            
            memory_record.update({
                "id": mem_id,
                "subject_id": subject_identity_id,
                "user": user_input,
                "assistant": assistant_response,
                "source": "assistant_generated", # You can restore your full source logic here
                "confidence": 0.65, # You can restore your full confidence logic here
                "verified": False,
                "timestamp": time.time(),
                "content_hash": content_hash,
                "importance": int(max(1, min(5, importance))),
                "segment": segment,
                "owner_view": f"User: {user_input} | Assistant: {assistant_response}",
                "public_view": "A conversation took place regarding a personal topic.",
                "metadata": {
                    **(metadata or {}),
                    "auto_provenance": True,
                    "intent": intent,
                    "importance_reason": self._get_importance_reason(user_input, intent or {}, importance)
                }
            })
            await self.memory_manager.put(mem_id, "persona_memory", memory_record)
            # --- END FIX ---

            # 2. Build and store the Transcript Entry
            transcript_entry = {
                "id": f"tx_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}",
                "memory_id": mem_id, # Use ID from manager
                "subject_id": subject_identity_id,
                "user": user_input,
                "assistant": assistant_response,
                "timestamp": time.time(),
                "content_hash": content_hash,
                "semantic_markers": self._extract_entities(f"{user_input}\n{assistant_response}")
            }
            if hasattr(self.memory_manager, "append_transcript"):
                await self.memory_manager.append_transcript(subject_identity_id, transcript_entry)

            # 3. Build and store the Semantic Record
            semantic_record = {
                "id": f"sem_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}",
                "memory_id": mem_id, # Use ID from manager
                "subject_id": subject_identity_id,
                "summary": (assistant_response[:240] + "...") if assistant_response and len(assistant_response) > 240 else (assistant_response or ""),
                "entities": transcript_entry.get("semantic_markers", {}),
                "timestamp": time.time(),
                "importance": memory_record["importance"],
            }
            if hasattr(self.memory_manager, "append_semantic_entry"):
                await self.memory_manager.append_semantic_entry(subject_identity_id, semantic_record)
                
            logger.info(f"💾 Dual-memory stored in container '{subject_identity_id}', segment '{segment}' (ID: {mem_id})")
            
            # Update local caches (KEPT)
            self.memory_index.append(memory_record)
            self.transcript_store.append(transcript_entry)
            self.semantic_index.append(semantic_record)
            if len(self.semantic_index) > MAX_MEMORY_ENTRIES:
                self.semantic_index = self.semantic_index[-MAX_MEMORY_ENTRIES:]
            
            self._interaction_count += 1
            return mem_id

        except Exception as e:
            logger.exception(f"PersonaCore memory storage failed: {e}")
            if holo_ids:
                await self._safe_holo_set_error(holo_ids[0]) # Set node to red
            return "error_storage_failed"

        finally:
            # --- HOLOGRAM LOGIC (KEPT) ---
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
        --- ARCHITECTURE FIX ---
        Retrieves relevant memories by delegating to the MemoryManager.
        This respects all identity and segmentation permissions.
        Hologram logic is preserved.
        """
        if not self.memory_manager:
            logger.error("MemoryManager not injected into PersonaCore. Cannot retrieve memory.")
            return []

        # --- HOLOGRAM LOGIC (KEPT) ---
        query = (query or "").strip()
        node_id = f"mem_read_{uuid.uuid4().hex[:6]}"
        link_id = f"link_mem_{node_id}"
        spawned = False
        try:
            if hologram_state is not None:
                spawned = await self._safe_holo_spawn(node_id, "memory", "Memory Read", 4, "Memory", link_id)
                if spawned:
                    await self._safe_holo_set_active("Memory")
        except Exception:
            pass # Non-fatal

        try:
            # --- DELEGATION TO MEMORY MANAGER ---
            memories = await self.memory_manager.retrieve_memories(
                query_text=query,
                subject_identity_id=subject_identity_id,
                requester_id="owner_primary", # The Owner can access all segments
                limit=limit
            )
            return memories
            
        except Exception as e:
            logger.exception(f"PersonaCore failed to delegate memory retrieval to MemoryManager: {e}")
            return []
        
        finally:
            # --- HOLOGRAM CLEANUP (KEPT) ---
            if spawned and hologram_state is not None:
                try:
                    await self._safe_holo_set_idle("Memory")
                    await self._safe_holo_despawn(node_id, link_id)
                except Exception:
                    pass

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
                    await self.save_persistent_memory()
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

            # --- FIX: Removed call to self.save_persistent_memory() ---
            # Persistence is now handled by main.py calling MemoryManager.save_index()
            
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
