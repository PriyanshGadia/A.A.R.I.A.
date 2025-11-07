# persona_core.py - A.A.R.I.A. Persona & Memory Engine (Asynchronous Production)
# [UPGRADED] - This module is now a state-of-the-art Persona and Memory
# manager. All reasoning ("Brain") functions have been removed and
# are now handled by CognitionCore.
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

# Try hologram_state defensively
try:
    import hologram_state
except Exception:
    hologram_state = None

# A.A.R.I.A. Core Imports
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

# --- [DELETED] ---
# TOOL_INSTRUCTION_BLOCK has been removed.
# Tool definition is now the responsibility of CognitionCore (the "Brain").
# --- [END DELETED] ---

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
CACHE_TTL_SECONDS = 300

# Proactive daemon timing
PROACTIVE_LOOP_INTERVAL_SEC = 30.0
MAINTENANCE_LOOP_INTERVAL_SEC = 60.0
COGNITION_MONITOR_INTERVAL_SEC = 45.0

# Defensive ProactiveCommunicator import/fallback
try:
    from proactive_comm import ProactiveCommunicator  # type: ignore
except Exception:
    class ProactiveCommunicator:
        def __init__(self, persona, hologram_call_fn=None, concurrency=1):
            self.persona = persona; self.hologram_call_fn = hologram_call_fn; self._running = False
        async def start(self): self._running = True; logger.debug("ProactiveCommunicator (fallback) started.")
        async def stop(self): self._running = False; logger.debug("ProactiveCommunicator (fallback) stopped.")
        async def enqueue_message(self, subject_id, channel, payload, **kwargs):
            logger.debug(f"ProactiveCommunicator (fallback) enqueue: {subject_id} {channel} {payload}")
            return {"status": "queued", "subject": subject_id}

class PersonaCore:
    """
    [UPGRADED] Enterprise-grade Asynchronous PersonaCore.
    This module's *sole responsibilities* are:
    1. Manage the System Prompt (Persona).
    2. Manage short-term conversation history (conv_buffer).
    3. Manage long-term memory (via the injected MemoryManager).
    4. Run background daemons for maintenance and proactive checks.
    
    All REASONING and PLANNING logic has been moved to CognitionCore.
    """
    def __init__(self, core: AssistantCore):
        self.core = core
        self.config = getattr(self.core, 'config', {}) or {}
        self.system_prompt: Optional[str] = None
        self.conv_buffer: deque = deque(maxlen=ST_BUFFER_SIZE)
        
        # Caches for local indices (not the source of truth)
        self.memory_index: List[Dict[str, Any]] = []
        self.transcript_store: deque = deque(maxlen=5000)
        self.semantic_index: List[Dict[str, Any]] = []

        self.llm_orchestrator = LLMAdapterFactory
        self.response_cache: Dict[str, Tuple[str, float]] = {}
        self._interaction_count = 0
        self._response_times: List[float] = []
        self.metrics = ConversationMetrics()
        self.tone_instructions = "Witty, succinct when short answer requested, elaborative when asked."
        self._initialized = False

        # These components are now injected by main.py
        self.memory_manager: Optional[Any] = None
        self.proactive: Optional[ProactiveCommunicator] = None

        # Daemon control
        self._daemon_tasks: List[asyncio.Task] = []
        self._running = False

        self._animals_set = set([
            "dog", "cat", "cow", "horse", "sheep", "goat", "lion", "tiger", "elephant",
            "monkey", "bird", "parrot", "rabbit", "mouse", "rat", "pig", "duck", "chicken",
            "fish", "whale", "dolphin", "bear", "fox", "wolf", "deer", "camel", "bee", "butterfly"
        ])

    # -----------------------
    # Hologram - safe wrappers (remains the same)
    # -----------------------
    async def _safe_holo_spawn(self, node_id: str, node_type: str, label: str, size: int, source_id: str, link_id: str) -> bool:
        if hologram_state is None: return False
        try:
            maybe = hologram_state.spawn_and_link(
                node_id=node_id, node_type=node_type, label=label, size=size,
                source_id=source_id, link_id=link_id
            )
            if inspect.iscoroutine(maybe): await maybe
            return True
        except Exception as e:
            try:
                if hasattr(hologram_state, "initialize_base_state"):
                    maybe_init = hologram_state.initialize_base_state()
                    if inspect.iscoroutine(maybe_init): await maybe_init
                    maybe2 = hologram_state.spawn_and_link(
                        node_id=node_id, node_type=node_type, label=label, size=size,
                        source_id=source_id, link_id=link_id
                    )
                    if inspect.iscoroutine(maybe2): await maybe2
                    return True
            except Exception:
                logger.debug("Hologram spawn retry failed (non-fatal).")
            logger.debug(f"Hologram spawn failed: {e}")
            return False

    async def _safe_holo_set_active(self, node_name: str):
        if hologram_state is None: return False
        try:
            maybe = getattr(hologram_state, "set_node_active", None)
            if maybe:
                ret = maybe(node_name)
                if inspect.iscoroutine(ret): await ret
            return True
        except Exception:
            logger.debug("Hologram set_node_active non-fatal failure")
            return False

    async def _safe_holo_set_idle(self, node_name: str):
        if hologram_state is None: return False
        try:
            maybe = getattr(hologram_state, "set_node_idle", None)
            if maybe:
                ret = maybe(node_name)
                if inspect.iscoroutine(ret): await ret
            return True
        except Exception:
            logger.debug("Hologram set_node_idle non-fatal failure")
            return False

    async def _safe_holo_update_link(self, link_id: str, intensity: float):
        if hologram_state is None: return False
        try:
            maybe = getattr(hologram_state, "update_link_intensity", None)
            if maybe:
                ret = maybe(link_id, intensity)
                if inspect.iscoroutine(ret): await ret
            return True
        except Exception:
            logger.debug("Hologram update_link_intensity failed (non-fatal)")
            return False

    async def _safe_holo_despawn(self, node_id: str, link_id: str):
        if hologram_state is None: return False
        try:
            maybe = getattr(hologram_state, "despawn_and_unlink", None)
            if maybe:
                ret = maybe(node_id, link_id)
                if inspect.iscoroutine(ret): await ret
            return True
        except Exception:
            logger.debug("Hologram despawn_and_unlink failed (non-fatal)")
            return False

    async def _safe_holo_set_error(self, node_id: str):
        if hologram_state is None: return False
        try:
            maybe = getattr(hologram_state, "set_node_error", None)
            if maybe:
                ret = maybe(node_id)
                if inspect.iscoroutine(ret): await ret
            return True
        except Exception:
            logger.debug("Hologram set_node_error failed (non-fatal)")
            return False

    # -----------------------
    # Lifecycle / Init
    # -----------------------
    async def initialize(self):
        """Initialize async components"""
        # Components are now injected by main.py *before* initialize() is called.
        self.memory_manager = getattr(self, "memory_manager", None) or getattr(self.core, "memory_manager", None)
        self.proactive = getattr(self, "proactive", None)

        if not self.memory_manager:
            logger.error("CRITICAL: PersonaCore initialized WITHOUT a MemoryManager. Memory will not function.")
        if not self.proactive:
            logger.warning("PersonaCore initialized WITHOUT a ProactiveCommunicator. Proactive daemons will be disabled.")
            self.proactive = ProactiveCommunicator(self) # Use fallback

        await self._init_system_prompt()
        self._cleanup_old_memories()

        # Add a test memory to verify the system works
        if not self.memory_manager:
            logger.error("No MemoryManager present; skipping test memory creation.")
        else:
            try:
                # We retrieve from the manager to see if it's truly empty
                existing_memories = await self.retrieve_memory("", "owner_primary", limit=1)
                if not existing_memories:
                    test_memory_id = await self.store_memory(
                        "System initialized",
                        "A.A.R.I.A persona core is now active and ready",
                        importance=2
                    )
                    logger.debug(f"🧪 Added test memory: {test_memory_id}")
            except Exception as e:
                logger.debug(f"Failed to add test memory: {e}", exc_info=True)
        
        # Load local caches from MemoryManager
        try:
            if self.memory_manager:
                if hasattr(self.memory_manager, "load_transcript_store_for_subject"):
                    ts = await self.memory_manager.load_transcript_store_for_subject("owner_primary", limit=5000)
                    if isinstance(ts, list):
                        self.transcript_store.extend(ts)
                if hasattr(self.memory_manager, "load_semantic_index_for_subject"):
                    si = await self.memory_manager.load_semantic_index_for_subject("owner_primary", limit=MAX_MEMORY_ENTRIES)
                    if isinstance(si, list):
                        self.semantic_index = si
        except Exception:
            logger.debug("Failed to restore transcript/semantic index from MemoryManager (non-fatal).")

        self._initialized = True
        logger.info(f"🚀 Enhanced Async PersonaCore initialized. Memory (cache): {len(self.semantic_index)} entries.")

        # Proactive communicator is started by main.py
        
        # Start internal daemons
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
                len(self.system_prompt) > 100)

    # -----------------------
    # System prompt
    # -----------------------
    async def _init_system_prompt(self) -> None:
        """Initialize system prompt with robust error recovery."""
        try:
            IST = ZoneInfo("Asia/Kolkata")
            
            profile = {}
            if hasattr(self.core, "load_user_profile"):
                maybe = self.core.load_user_profile()
                profile = await maybe if inspect.iscoroutine(maybe) else maybe or {}
            user_name = profile.get("name", "").strip() or None
            logger.debug(f"🧩 Profile content: {profile}")

            try:
                so = getattr(self.core, "security_orchestrator", None)
                if so and hasattr(so, "identity_manager"):
                    im = getattr(so, "identity_manager")
                    known = getattr(im, "known_identities", {}) or {}
                    for identity in known.values():
                        preferred = getattr(identity, "preferred_name", None) or (identity.get("preferred_name") if isinstance(identity, dict) else None)
                        relationship = getattr(identity, "relationship", None) or (identity.get("relationship") if isinstance(identity, dict) else None)
                        if relationship == "owner" and preferred:
                            user_name = preferred
                            logger.info(f"🎯 Using owner preferred name from identity_manager: {user_name}")
                            break
            except Exception:
                logger.debug("Could not extract preferred name from identity manager (non-fatal).")

            if not user_name:
                user_name = profile.get("name") or "User"
            
            timezone_str = profile.get("timezone", "IST").strip() or "IST"
            try:
                tz = ZoneInfo(timezone_str)
            except Exception:
                tz = IST
                timezone_str = "IST"
            current_date = datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")

            # --- [FIXED] ---
            # Removed TOOL_INSTRUCTION_BLOCK. This prompt now *only* defines persona.
            # --- [END FIX] ---
            self.system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
                user_name=user_name,
                timezone=timezone_str,
                current_date=current_date,
                tone_instructions=self.tone_instructions
            )

            logger.info(f"✅ Enhanced system prompt initialized for user: {user_name}")
        except Exception as e:
            logger.error(f"Failed to initialize system prompt: {e}", exc_info=True)
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

            if hasattr(self.core, "store") and hasattr(self.core.store, "put"):
                try:
                    put = getattr(self.core.store, "put")
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
    # Persistent memory (delegates)
    # -----------------------
    def _cleanup_old_memories(self) -> None:
        """Delegates memory pruning to the MemoryManager if possible."""
        try:
            mm = getattr(self, "memory_manager", None)
            if mm and hasattr(mm, "prune_old_memories"):
                maybe = mm.prune_old_memories(MAX_MEMORY_AGE_DAYS, min_importance=4)
                if inspect.iscoroutine(maybe):
                    asyncio.create_task(maybe)
                logger.debug("Delegated old memory pruning to MemoryManager (async).")
                return
        except Exception as e:
            logger.debug(f"MemoryManager prune attempt failed: {e}", exc_info=True)
        # No local fallback

    # -----------------------
    # Dual memory create/store (delegates)
    # -----------------------
    async def store_memory(self, user_input: str, assistant_response: str,
                           importance: int = 1, metadata: Optional[Dict[str, Any]] = None,
                           intent: Optional[Dict[str, bool]] = None,
                           subject_identity_id: str = "owner_primary") -> str:
        """
        [UPGRADED]
        Delegates persistence of all three records (main, transcript, semantic)
        to the MemoryManager. Fixes 'append_semantic_entry' typo.
        """
        
        if not self.memory_manager:
            logger.error("MemoryManager not injected into PersonaCore. Cannot store memory.")
            return "error_no_manager"

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
            user_input = (user_input or "").strip()
            assistant_response = (assistant_response or "").strip()
            intent = intent or self._classify_query_intent(user_input)
            content_hash = hashlib.md5(f"{user_input}{assistant_response}".encode("utf-8")).hexdigest()[:16]

            # 1. Store the main record (using 'append_memory' from sqlite manager)
            # We create the ID here to ensure all records are linked
            mem_id = f"mem_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"
            
            memory_record = {
                "id": mem_id,
                "subject_id": subject_identity_id,
                "user": user_input,
                "assistant": assistant_response,
                "source": "assistant_generated",
                "confidence": 0.65,
                "verified": False,
                "timestamp": time.time(),
                "content_hash": content_hash,
                "importance": int(max(1, min(5, importance))),
                "segment": "default", # memory_manager.py would calculate this, we use a default
                "owner_view": f"User: {user_input} | Assistant: {assistant_response}",
                "public_view": "A conversation took place regarding a personal topic.",
                "metadata": {
                    **(metadata or {}),
                    "auto_provenance": True,
                    "intent": intent,
                    "importance_reason": self._get_importance_reason(user_input, intent or {}, importance)
                }
            }
            
            # --- [FIXED] Call append_memory, the correct method ---
            if hasattr(self.memory_manager, "append_memory"):
                 await self.memory_manager.append_memory(subject_identity_id, memory_record)
            else:
                 logger.error("MemoryManager is missing 'append_memory' method.")
                 return "error_missing_method"

            # 2. Build and store the Transcript Entry
            transcript_entry = {
                "id": f"tx_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}",
                "memory_id": mem_id, 
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
                "memory_id": mem_id, 
                "subject_id": subject_identity_id,
                "summary": (assistant_response[:240] + "...") if assistant_response and len(assistant_response) > 240 else (assistant_response or ""),
                "entities": transcript_entry.get("semantic_markers", {}),
                "timestamp": time.time(),
                "importance": memory_record["importance"],
            }
            
            # --- [FIXED] Call the correct method name 'append_semantic' ---
            if hasattr(self.memory_manager, "append_semantic"):
                await self.memory_manager.append_semantic(subject_identity_id, semantic_record)
            elif hasattr(self.memory_manager, "append_semantic_entry"): # Fallback for old name
                await self.memory_manager.append_semantic_entry(subject_identity_id, semantic_record)
            else:
                logger.warning("MemoryManager has no 'append_semantic' or 'append_semantic_entry' method.")
                
            logger.info(f"💾 Dual-memory stored in container '{subject_identity_id}' (ID: {mem_id})")
            
            # Update local caches (harmless, but caches are stale)
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
                await self._safe_holo_set_error(holo_ids[0])
            return "error_storage_failed"

        finally:
            if holo_ids and hologram_state is not None:
                try:
                    node_id, link_id = holo_ids
                    await self._safe_holo_set_idle("Memory")
                    await self._safe_holo_despawn(node_id, link_id)
                except Exception:
                    pass

    # -----------------------
    # Entity extraction (remains the same)
    # -----------------------
    def _extract_entities(self, text: str) -> Dict[str, List[str]]:
        res = {"names": [], "places": [], "animals": [], "things": []}
        if not text: return res
        text_clean = re.sub(r"[\(\)\[\]\"']", " ", text)
        name_pattern = re.compile(r'\b([A-Z][a-z]{1,30}(?:\s+[A-Z][a-z]{1,30})?)\b')
        raw_names = name_pattern.findall(text_clean)
        filter_out = set(["The","A","An","I","It","We","You","He","She","They","Monday","Tuesday","January","February","March","April","May","June","July","August","September","October","November","December"])
        for n in raw_names:
            if n and n not in filter_out and len(n) > 1:
                if n not in res["names"]: res["names"].append(n)
        place_pattern = re.compile(r'\b(?:in|at|from|to|on)\s+([A-Z][\w-]+(?:\s+[A-Z][\w-]+)*)', re.IGNORECASE)
        raw_places = place_pattern.findall(text_clean)
        for p in raw_places:
            if p and p not in res["places"]: res["places"].append(p)
        words = re.findall(r"\b([A-Za-z]{2,30})\b", text.lower())
        for w in words:
            if w in self._animals_set and w not in res["animals"]: res["animals"].append(w)
        things_pattern = re.compile(r'\b(?:a|an|the)\s+([a-z][a-z0-9\-]{2,40})', re.IGNORECASE)
        raw_things = things_pattern.findall(text)
        for t in raw_things:
            tl = t.strip()
            if tl.lower() not in ["the", "a", "an", "that", "this", "those", "these", "my", "your"]:
                if tl not in res["things"]: res["things"].append(tl)
        return res

    async def _extract_and_register_identity(self, text: str) -> Optional[str]:
        """Extracts identity mentions and registers them with the MemoryManager."""
        try:
            if not text or len(text.strip()) < 4: return None
            entities = self._extract_entities(text)
            relation_patterns = [
                r"\bmy (brother|sister|mother|mom|dad|father|wife|husband|partner)\b(?:[,:\s]+([A-Z][a-z]+))?",
                r"\b([A-Z][a-z]+) is my (brother|sister|mother|dad|father|wife|husband|partner)\b"
            ]
            name = None; relation = None
            for p in relation_patterns:
                m = re.search(p, text, flags=re.IGNORECASE)
                if m:
                    if m.lastindex == 2:
                        grp1, grp2 = m.group(1), m.group(2)
                        if grp1 and grp2:
                            if grp1[0].isupper(): name = grp1; relation = grp2
                            else: relation = grp1;
                            if grp2 and grp2[0].isupper(): name = grp2
                    else:
                        relation = m.group(1)
                        after = text[m.end():].strip()
                        nm = re.match(r"^([A-Z][a-z]{1,20})", after)
                        if nm: name = nm.group(1)
                    break
            if not name and entities.get("names"): name = entities["names"][0]
            if not relation: relation = "contact"
            if not name and not relation: return None
            display_name = name if name else relation.capitalize()
            identity_id = f"{display_name.lower().replace(' ', '_')}_{relation}"
            mm = getattr(self, "memory_manager", None) or getattr(self.core, "memory_manager", None)
            if mm:
                try:
                    if hasattr(mm, "get_identity_container"):
                        container = await mm.get_identity_container(identity_id)
                        if not container: container = {"id": identity_id, "display_name": display_name, "name_variants": [], "relationships": {}}
                        container.setdefault("name_variants", [])
                        if display_name and display_name not in container["name_variants"]: container["name_variants"].append(display_name)
                        container.setdefault("relationships", {})
                        container["relationships"]["owner"] = "owner_primary"
                        if hasattr(mm, "update_identity_container"):
                            maybe_upd = mm.update_identity_container(identity_id, container)
                            if inspect.iscoroutine(maybe_upd): await maybe_upd
                        elif hasattr(mm, "put"):
                            maybe_put = mm.put(identity_id, "identity", container)
                            if inspect.iscoroutine(maybe_put): await maybe_put
                        logger.info(f"Registered identity from conversation: {identity_id} / {display_name}")
                        return identity_id
                except Exception as e:
                    logger.debug(f"Identity registration via MemoryManager failed: {e}", exc_info=True)
            try:
                fallback_key = f"persona_identity:{identity_id}"
                payload = {"id": identity_id, "display_name": display_name, "relation": relation, "created_at": time.time(), "entities": entities}
                if hasattr(self.core, "store") and hasattr(self.core.store, "put"):
                    maybe = self.core.store.put(fallback_key, payload)
                    if inspect.iscoroutine(maybe): await maybe
                    logger.info(f"Fallback identity stored: {identity_id}")
                    return identity_id
            except Exception as e:
                logger.debug(f"Fallback identity persist failed: {e}", exc_info=True)
        except Exception:
            logger.exception("Identity extraction failed (non-fatal).")
        return None

    # -----------------------
    # Relevance & retrieval
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
        if intent.get('is_urgent'): base_score += 2
        if intent.get('is_action_oriented'): base_score += 1
        if intent.get('requires_memory'): base_score += 2
        if intent.get('is_sensitive'): base_score += 1
        if content_indicators["personal_info"]: base_score += 2
        if content_indicators["preferences"]: base_score += 1
        if content_indicators["scheduling"]: base_score += 2
        if content_indicators["important_question"]: base_score += 2
        if content_indicators["learning_request"]: base_score += 1
        if len(assistant_response) > 200: base_score += 1
        if hasattr(self, 'conv_buffer') and len(self.conv_buffer) > 5: base_score += 1
        return max(1, min(5, base_score))

    def _get_importance_reason(self, user_input: str, intent: Dict[str, bool], importance: int) -> str:
        reasons = []
        content_words = (user_input or "").lower().split()
        if any(word in content_words for word in ["urgent", "asap", "emergency"]): reasons.append("urgent_request")
        if any(word in content_words for word in ["schedule", "add", "create"]): reasons.append("action_oriented")
        if any(word in content_words for word in ["remember", "recall", "before"]): reasons.append("memory_dependent")
        if any(word in content_words for word in ["name", "email", "address"]): reasons.append("personal_info")
        if any(word in content_words for word in ["meeting", "schedule", "remind"]): reasons.append("scheduling")
        if any(word in content_words for word in ["important", "critical"]): reasons.append("explicit_importance")
        if intent.get('is_urgent'): reasons.append("intent_urgent")
        if intent.get('is_action_oriented'): reasons.append("intent_action")
        if intent.get('requires_memory'): reasons.append("intent_memory")
        return "+".join(reasons) if reasons else "general_conversation"

    async def retrieve_memory(self, query: str,
                              subject_identity_id: str = "owner_primary",
                              limit: int = 3) -> List[Dict[str, Any]]:
        """
        [UPGRADED]
        Fixes TypeError by calling search_memories with positional arguments.
        """
        if not self.memory_manager:
            logger.error("MemoryManager not injected into PersonaCore. Cannot retrieve memory.")
            return []

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
            pass

        try:
            # --- [CRITICAL FIX] ---
            # Call with positional args (self, subject_id, query, limit)
            # as required by your memory_manager_sqlite.py
            if hasattr(self.memory_manager, "search_memories"):
                memories = await self.memory_manager.search_memories(
                    subject_identity_id,
                    query,
                    limit
                )
                return memories
            # --- [END FIX] ---
            elif hasattr(self.memory_manager, "retrieve_memories"): # Fallback
                 memories = await self.memory_manager.retrieve_memories(
                    query_text=query,
                    subject_identity_id=subject_identity_id,
                    requester_id="owner_primary",
                    limit=limit
                )
                 return memories
            else:
                logger.error("MemoryManager has no 'search_memories' or 'retrieve_memories' method.")
                return []
            
        except Exception as e:
            logger.exception(f"PersonaCore failed to delegate memory retrieval to MemoryManager: {e}")
            return []
        
        finally:
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
        if not text: return False
        return any(pattern.search(text) for pattern in SENSITIVE_PATTERNS)

    def _classify_query_intent(self, text: str) -> Dict[str, bool]:
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
        if not text: return text
        sanitized = text
        for pattern in SENSITIVE_PATTERNS:
            sanitized = pattern.sub("[REDACTED]", sanitized)
        return sanitized

    def _load_manual_context(self, filename: str = "my_context.txt") -> Optional[str]:
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

    # ---------------------------------------------------------------
    # --- [DELETED] AGENTIC METHODS ---
    # The following methods have been DELETED from PersonaCore.
    # This logic is now centralized in CognitionCore (the "Brain")
    # and InteractionCore (the "Hands").
    #
    # - respond(...)
    # - _build_prompt(...)
    # - plan(...)
    # - _extract_json(...)
    #
    # ---------------------------------------------------------------

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
            "memory_utilization_percent": (len(self.semantic_index) / max(1, MAX_MEMORY_ENTRIES)) * 100,
            "memory_hit_rate_percent": (self.metrics.memory_hit_count / max(1, self.metrics.user_messages)) * 100,
            "safety_triggers": self.metrics.safety_trigger_count,
            "intent_distribution": self.metrics.intent_distribution,
            "response_cache_size": len(self.response_cache),
            "conversation_buffer_size": len(self.conv_buffer),
        }

    def debug_memory_state(self) -> Dict[str, Any]:
        return {
            "total_semantic_memories": len(self.semantic_index),
            "recent_semantic_memories": [m for m in self.semantic_index[-5:] if m.get('timestamp', 0) > time.time() - 3600],
            "important_semantic_memories": len([m for m in self.semantic_index if m.get('importance', 1) >= 4]),
            "total_transcripts": len(self.transcript_store),
        }

    def debug_memory_content(self) -> None:
        logger.info("🧠 MEMORY DEBUG - Current semantic memory contents (cache):")
        for i, memory in enumerate(self.semantic_index[-10:]):
            logger.info(f"  Memory {i+1}: Summary: '{memory.get('summary', '')[:60]}...' (Importance: {memory.get('importance', 1)})")

    async def health_check(self) -> Dict[str, Any]:
        health_status = {
            "is_ready": self.is_ready(),
            "llm_connectivity": "pending_test",
            "memory_health": "pending_test",
            "cache_health": len(self.response_cache) < 1000,
            "safety_systems": True,
            "metrics_tracking": True
        }
        try:
            primary_provider = LLMProvider(self.config.get("primary_provider", "groq"))
            async with self.llm_orchestrator.get_adapter(primary_provider) as adapter:
                health = await asyncio.wait_for(adapter.health_check(), timeout=10.0)
                if hasattr(health, "status"):
                    health_status["llm_connectivity"] = getattr(health, "status").value
                elif isinstance(health, dict):
                    health_status["llm_connectivity"] = health.get("status", "unknown")
                else:
                    health_status["llm_connectivity"] = str(health)
        except asyncio.TimeoutError:
            health_status["llm_connectivity"] = "timeout"
        except Exception as e:
            health_status["llm_connectivity"] = f"error: {str(e)[:100]}"
            
        try:
            if self.memory_manager and hasattr(self.memory_manager, "health_check"):
                health_status["memory_health"] = await self.memory_manager.health_check()
            elif self.memory_manager:
                health_status["memory_health"] = "not_implemented"
            else:
                 health_status["memory_health"] = "not_available"
        except Exception as e:
            health_status["memory_health"] = f"error: {str(e)[:100]}"
            
        return health_status

    # -----------------------
    # Daemon loops (remains mostly the same)
    # -----------------------
    async def _proactive_loop(self):
        """Continuous human-like proactive loop."""
        node_id = f"proactive_node_{uuid.uuid4().hex[:6]}"
        link_id = f"link_{node_id}"
        try:
            await self._safe_holo_spawn(node_id, "daemon", "ProactiveDaemon", 10, "PersonaCore", link_id)
            await self._safe_holo_set_active("ProactiveDaemon")
        except Exception:
            pass

        while self._running:
            try:
                jitter = random.uniform(-PROACTIVE_LOOP_INTERVAL_SEC * 0.25, PROACTIVE_LOOP_INTERVAL_SEC * 0.25)
                await asyncio.sleep(max(1.0, PROACTIVE_LOOP_INTERVAL_SEC + jitter))

                # 1) Health-triggered proactive
                try:
                    health = await self.health_check()
                    llm_status = str(health.get("llm_connectivity", "unknown")).lower()
                    if llm_status not in ("ok", "healthy", "HEALTHY", "unknown", "pending_test"):
                        payload = {"type": "health_alert", "summary": "LLM connectivity degraded", "details": health.get("llm_connectivity")}
                        await self.enqueue_proactive_notification("owner_primary", "system", payload)
                except Exception:
                    pass

                # 2) Memory-triggered proactive
                try:
                    candidate = None
                    for mem in reversed(self.semantic_index[-100:]): # Scan local semantic cache
                        md = mem.get("metadata", {})
                        if mem.get("importance", 1) >= 4 or "requested_schedule_text" in md:
                            candidate = mem
                            break
                    if candidate:
                        payload = {
                            "type": "reminder_suggestion",
                            "summary": f"Reminder candidate from memory (importance {candidate.get('importance')})",
                            "memory_id": candidate.get("memory_id"),
                            "preview": candidate.get("summary")[:140]
                        }
                        await self.enqueue_proactive_notification("owner_primary", "reminder", payload)
                except Exception:
                    pass

                # 3) Occasional human-like message
                if random.random() < 0.08:
                    note = random.choice([
                        "Quick check: anything you want me to remember right now?",
                        "Heads up — nothing urgent on your calendar in the next hour.",
                        "I've summarized a recent conversation that might be useful — would you like a quick look?"
                    ])
                    await self.enqueue_proactive_notification("owner_primary", "checkin", {"type":"checkin","message":note})

                # 4) Hologram heartbeat
                await self._safe_holo_update_link(link_id, intensity=random.random())

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Proactive daemon loop error (non-fatal)")
                await asyncio.sleep(2.0)
        try:
            await self._safe_holo_set_idle("ProactiveDaemon")
            await self._safe_holo_despawn(node_id, link_id)
        except Exception:
            pass

    async def _maintenance_loop(self):
        """
        [UPGRADED]
        Periodic maintenance.
        This loop no longer calls save_persistent_memory().
        Persistence is handled by store_memory on every call.
        """
        while self._running:
            try:
                await asyncio.sleep(MAINTENANCE_LOOP_INTERVAL_SEC + random.uniform(-5, 5))
                
                # [DELETED] self.save_persistent_memory()
                
                # Persist transcripts/semantic if memory_manager present
                try:
                    mm = getattr(self, "memory_manager", None)
                    if mm:
                        if hasattr(mm, "save_transcript_store"):
                            maybe = mm.save_transcript_store(list(self.transcript_store))
                            if inspect.iscoroutine(maybe): await maybe
                        if hasattr(mm, "save_semantic_index"):
                            maybe = mm.save_semantic_index(self.semantic_index)
                            if inspect.iscoroutine(maybe): await maybe
                except Exception:
                    logger.debug("Maintenance: memory_manager sync failed (non-fatal)")
                
                # Local cache pruning
                try:
                    cutoff = time.time() - (MAX_MEMORY_AGE_DAYS * 24 * 60 * 60)
                    si_before = len(self.semantic_index)
                    self.semantic_index = [s for s in self.semantic_index if s.get("timestamp", 0) >= cutoff or s.get("importance", 1) >= 4]
                    if len(self.semantic_index) != si_before:
                        logger.info(f"Maintenance: pruned semantic_index cache from {si_before} to {len(self.semantic_index)}")
                except Exception:
                    pass
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Maintenance daemon error (non-fatal)")
                await asyncio.sleep(2.0)

    async def _cognition_monitor(self):
        """Monitors cognition health & metrics and triggers self-healing."""
        degraded_count = 0
        while self._running:
            try:
                await asyncio.sleep(COGNITION_MONITOR_INTERVAL_SEC + random.uniform(-5, 5))
                try:
                    health = await self.health_check()
                    status = str(health.get("llm_connectivity", "unknown")).lower()
                    if "error" in status or status in ("timeout", "failed", "unhealthy", "degraded"):
                        degraded_count += 1
                    else:
                        degraded_count = max(0, degraded_count - 1)
                    
                    if degraded_count >= 3:
                        await self.enqueue_proactive_notification("owner_primary", "alert", {"type":"cognition_degraded","summary":"Cognition health degraded repeatedly","details":health})
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
    
    # --- [NEW] ---
    # This method is ADDED to fix the shutdown crash.
    # It correctly calls the methods on the memory_manager.
    # --- [END NEW] ---
    async def save_persistent_memory(self) -> None:
        """
        [UPGRADED]
        This provides the persistence function that 'main.py' and 'maintenance_loop'
        were trying to call. It now correctly passes all required arguments.
        """
        logger.info("💾 Persisting all memory indices to database...")
        try:
            mm = getattr(self, "memory_manager", None)
            if not mm:
                logger.warning("save_persistent_memory: No MemoryManager available.")
                return

            identity_id = "owner_primary" # Define the identity for these indices

            # --- [CRITICAL FIX] ---
            # save_index expects a dict, not a list. Wrap it.
            if hasattr(mm, "save_index"):
                await mm.save_index({"data": list(self.memory_index)})
            
            # Pass the identity_id as the first argument
            if hasattr(mm, "save_transcript_store"):
                await mm.save_transcript_store(identity_id, list(self.transcript_store))
            
            if hasattr(mm, "save_semantic_index"):
                await mm.save_semantic_index(identity_id, self.semantic_index)
            # --- [END FIX] ---
                
            logger.info("💾 All memory indices successfully persisted.")
        except Exception as e:
            logger.error(f"Failed to save persistent memory: {e}", exc_info=True)
            # Do not re-raise, allow shutdown to continue


    async def close(self) -> None:
        """
        [UPGRADED]
        Graceful shutdown. Now correctly calls save_persistent_memory().
        """
        logger.info("🛑 Beginning Enhanced PersonaCore shutdown...")
        try:
            self._running = False
            for t in list(self._daemon_tasks):
                try: t.cancel()
                except Exception: pass
            for t in list(self._daemon_tasks):
                try: await asyncio.wait_for(t, timeout=3.0)
                except Exception: pass
            self._daemon_tasks = []

            # --- [FIXED] ---
            # Now correctly calls the new persistence method
            await self.save_persistent_memory()
            # --- [END FIX] ---
            
            await self._persist_critical_data()
            
            current_time = time.time()
            self.response_cache = {k: v for k, v in self.response_cache.items() if current_time - v[1] < CACHE_TTL_SECONDS}
            logger.info(f"✅ PersonaCore shutdown complete - {len(self.semantic_index)} memories in cache, {len([m for m in self.semantic_index if m.get('importance', 1) >= 4])} important.")
        except Exception as e:
            logger.error(f"❌ Error during PersonaCore shutdown: {e}", exc_info=True)
        finally:
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
            important_memories = [m for m in self.semantic_index if m.get('importance', 1) >= 4] # Use semantic cache
            mm = getattr(self, "memory_manager", None)
            if not mm: return

            for memory in important_memories:
                try:
                    # Get the *full* memory record, not just the semantic one
                    if not hasattr(mm, 'get'): continue
                    full_mem = await mm.get(memory['memory_id'])
                    if not full_mem: continue

                    if hasattr(mm, "put"):
                        maybe = mm.put(f"critical_{full_mem['id']}", "critical_memory", full_mem)
                        if inspect.iscoroutine(maybe): await maybe
                    elif hasattr(mm, "persist_important"): # Fallback to another possible name
                        maybe = mm.persist_important(full_mem['subject_id'], full_mem)
                        if inspect.iscoroutine(maybe): await maybe
                except Exception as e:
                    logger.warning(f"MemoryManager persist_important failed: {e}", exc_info=True)

        except Exception as e:
            logger.warning(f"Failed to persist critical data: {e}", exc_info=True)

    # [DELETED] plan(...)
    # [DELETED] _extract_json(...)

    async def enqueue_proactive_notification(self, subject_id, channel, payload, **kwargs):
        """Safely enqueues a message using the injected ProactiveCommunicator."""
        if not self.proactive:
            logger.warning("Proactive notification skipped: communicator not available.")
            return
        try:
            return await self.proactive.enqueue_message(subject_id, channel, payload, **kwargs)
        except Exception as e:
            logger.error(f"Failed to enqueue proactive message: {e}", exc_info=True)