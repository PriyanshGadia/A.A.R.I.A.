"""
persona_core.py - A.A.R.I.A. Persona & Reasoning Engine (Asynchronous Production)

Key Improvements:
- Asynchronous non-blocking architecture with superior performance
- Enhanced safety & privacy with comprehensive content filtering
- Advanced memory management with importance scoring and access tracking
- Production-grade conversation analytics and health monitoring
- Robust error handling with multiple fallback strategies
- Response caching with intent-aware invalidation
"""

from __future__ import annotations

import logging
import time
import json
import asyncio
import os
import sys
import re
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from collections import deque
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum

# A.A.R.I.A. Core Imports
from llm_adapter import LLMAdapterFactory, LLMProvider, LLMRequest, LLMError, LLMConfigurationError
from assistant_core import AssistantCore

logger = logging.getLogger("AARIA.Persona")

# --- Enhanced Persona Constants ---
SYSTEM_PROMPT_TEMPLATE = """You are A.A.R.I.A. (Adaptive Autonomous Reasoning Intelligence Assistant).
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

class PersonaCore:
    """
    Enterprise-grade Asynchronous PersonaCore with enhanced safety, memory, and performance.
    """
    def __init__(self, core: AssistantCore):
        self.core = core
        self.config = self.core.config if hasattr(self.core, 'config') else {}
        self.system_prompt: Optional[str] = None
        self.conv_buffer: deque = deque(maxlen=ST_BUFFER_SIZE)
        self.memory_index: List[Dict[str, Any]] = []
        self.llm_orchestrator = LLMAdapterFactory
        self.response_cache: Dict[str, Tuple[str, float]] = {}
        self._interaction_count = 0
        self._response_times: List[float] = []
        self.metrics = ConversationMetrics()
        self.tone_instructions = "Witty, succinct when short answer requested, elaborative when asked."
        self._initialized = False  # Add this flag

    async def initialize(self):
        """Initialize async components"""
        await self._init_system_prompt()
        await self._load_persistent_memory()
        self._cleanup_old_memories()
        
        # Add a test memory to verify the system works
        if len(self.memory_index) == 0:
            test_memory_id = await self.store_memory(
                "System initialized", 
                "A.A.R.I.A persona core is now active and ready", 
                importance=2
            )
            logger.debug(f"🧪 Added test memory: {test_memory_id}")
        
        self._initialized = True
        logger.info("🚀 Enhanced Async PersonaCore initialized with advanced safety and memory features")
            
    def is_ready(self) -> bool:
        """Comprehensive health check readiness probe."""
        return (self._initialized and 
                self.system_prompt is not None and 
                len(self.system_prompt) > 100 and
                self.llm_orchestrator is not None and
                isinstance(self.conv_buffer, deque))

    async def _init_system_prompt(self) -> None:
        """Initialize system prompt with robust error recovery."""
        try:
            # FIXED: Await the async call
            profile = await self.core.load_user_profile() or {}
            user_name = profile.get("name", "User").strip() or "User"
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
            self.system_prompt = ""

    async def refresh_profile_context(self) -> None:
        """Refresh system prompt when user profile changes."""
        await self._init_system_prompt()
        logger.info("System prompt refreshed with updated profile")

    async def _load_persistent_memory(self) -> None:
        """Load persistent memory with data validation."""
        try:
            if hasattr(self.core, 'store'):
                # CORRECTED: Use correct storage API - single argument
                memory_data = await self.core.store.get("enhanced_memory_index")
                logger.debug(f"📂 Loading persistent memory: found {type(memory_data)}")
                if isinstance(memory_data, dict) and "memory_index" in memory_data:
                    self.memory_index = [m for m in memory_data["memory_index"] if isinstance(m, dict) and m.get('user') and m.get('assistant')]
                    self.memory_index = self.memory_index[-MAX_MEMORY_ENTRIES:]
                    logger.info(f"✅ Loaded {len(self.memory_index)} enhanced memory entries from storage.")
                elif memory_data:
                    logger.warning(f"📂 Unexpected memory data format: {type(memory_data)}")
                else:
                    logger.info("📂 No existing memory data found - starting fresh.")
        except Exception as e:
            logger.error(f"❌ Failed to load persistent memory: {e}")
            self.memory_index = []

    async def _save_persistent_memory(self) -> None:
        """Save persistent memory with proper storage API."""
        if hasattr(self.core, 'store'):
            try:
                memory_data = {
                    "memory_index": self.memory_index,
                    "timestamp": time.time(),
                    "count": len(self.memory_index)
                }
                await self.core.store.put("enhanced_memory_index", "persona_memory", memory_data)
                logger.info(f"💾 Persisted {len(self.memory_index)} memory entries to storage.")
            except Exception as e:
                logger.error(f"❌ Failed to save persistent memory: {e}")

    def _cleanup_old_memories(self) -> None:
        """Remove memories older than MAX_MEMORY_AGE_DAYS, preserving important ones."""
        if not self.memory_index: return
        
        cutoff_timestamp = time.time() - (MAX_MEMORY_AGE_DAYS * 24 * 60 * 60)
        initial_count = len(self.memory_index)
        
        self.memory_index = [m for m in self.memory_index if m.get('timestamp', 0) >= cutoff_timestamp or m.get('importance', 1) >= 4]
        
        removed_count = initial_count - len(self.memory_index)
        if removed_count > 0:
            logger.info(f"🧹 Cleaned up {removed_count} memories older than {MAX_MEMORY_AGE_DAYS} days.")
            self._save_persistent_memory()

    async def store_memory(self, user_input: str, assistant_response: str, importance: int = 1, 
                        metadata: Optional[Dict[str, Any]] = None, intent: Optional[Dict[str, bool]] = None) -> str:
        """Enhanced memory storage with importance scoring and duplicate prevention."""
        content_hash = hashlib.md5(f"{user_input}{assistant_response}".encode()).hexdigest()[:16]
        
        if any(m.get('content_hash') == content_hash for m in self.memory_index):
            logger.debug(f"Memory duplicate detected: {content_hash}")
            return "duplicate"

        memory_id = f"mem_{int(time.time()*1000)}"
        
        # If intent is not provided, classify it
        if intent is None:
            intent = self._classify_query_intent(user_input)
        
        memory_record = {
            "id": memory_id, "user": user_input, "assistant": assistant_response,
            "timestamp": time.time(), "content_hash": content_hash,
            "importance": max(1, min(5, importance)), "access_count": 0,
            "metadata": {
                **(metadata or {}),
                "importance_reason": self._get_importance_reason(user_input, intent, importance)
            }
        }
        
        self.memory_index.append(memory_record)
        
        try:
            # Store important memories with proper storage API
            if importance >= 4 and hasattr(self.core, 'store'):
                await self.core.store.put(memory_id, "persona_memory", memory_record)
                logger.debug(f"💾 Persisted important memory: {memory_id}")
        except Exception as e:
            logger.warning(f"Failed to persist important memory {memory_id}: {e}")
        
        self._interaction_count += 1
        if self._interaction_count % MEMORY_SAVE_INTERVAL == 0:
            await self._save_persistent_memory()
            logger.debug(f"🔄 Auto-saved {len(self.memory_index)} memories")
            
        return memory_id
    
    def _calculate_memory_importance(self, user_input: str, assistant_response: str, intent: Dict[str, bool]) -> int:
        """Calculate intelligent memory importance based on content and context."""
        base_score = 2  # Start with lower base for casual conversations
        
        # Boost for specific content patterns
        content_indicators = {
            "personal_info": any(word in user_input.lower() for word in 
                            ["name", "email", "address", "phone", "birthday", "age"]),
            "preferences": any(word in user_input.lower() for word in 
                            ["like", "prefer", "favorite", "hate", "dislike"]),
            "scheduling": any(word in user_input.lower() for word in 
                            ["meeting", "appointment", "schedule", "calendar", "remind"]),
            "important_question": any(word in user_input.lower() for word in 
                                    ["important", "critical", "urgent", "emergency"]),
            "learning_request": any(word in user_input.lower() for word in 
                                ["teach", "explain", "how to", "what is", "why"]),
        }
        
        # Intent-based scoring
        if intent['is_urgent']:
            base_score += 2
        if intent['is_action_oriented']:
            base_score += 1
        if intent['requires_memory']:
            base_score += 2
        if intent['is_sensitive']:
            base_score += 1  # Sensitive conversations might be important to remember context
        
        # Content-based scoring
        if content_indicators["personal_info"]:
            base_score += 2  # Personal info is very important
        if content_indicators["preferences"]:
            base_score += 1  # User preferences help personalize future interactions
        if content_indicators["scheduling"]:
            base_score += 2  # Calendar events are important
        if content_indicators["important_question"]:
            base_score += 2  # User explicitly marked as important
        if content_indicators["learning_request"]:
            base_score += 1  # Educational content has moderate importance
        
        # Response length indicates complexity/depth
        if len(assistant_response) > 200:
            base_score += 1
        
        # Conversation history context
        if hasattr(self, 'conv_buffer') and len(self.conv_buffer) > 5:
            # Ongoing complex conversation gets higher importance
            base_score += 1
        
        return max(1, min(5, base_score))  # Clamp between 1-5
    
    def _calculate_relevance_score(self, query: str, memory: Dict[str, Any]) -> float:
        """Enhanced relevance scoring with multiple boosting factors."""
        query_words = set(query.lower().split())
        memory_text = f"{memory.get('user', '')} {memory.get('assistant', '')}".lower()
        memory_words = set(memory_text.split())
        
        intersection = query_words.intersection(memory_words)
        union = query_words.union(memory_words)
        base_score = len(intersection) / len(union) if union else 0.0
        
        importance_boost = (memory.get('importance', 1) - 1) * 0.2
        memory_age_days = (time.time() - memory.get('timestamp', 0)) / 86400
        recency_boost = max(0, 1 - (memory_age_days / 14)) * 0.15
        access_boost = min(0.1, memory.get('access_count', 0) * 0.02)
        
        return min(1.0, base_score + importance_boost + recency_boost + access_boost)

    def retrieve_memory(self, query: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Enhanced memory retrieval with relevance scoring and access tracking."""
        scored_memories = []
        for memory in self.memory_index:
            score = self._calculate_relevance_score(query, memory)
            if score >= RELEVANCE_THRESHOLD:
                memory['access_count'] = memory.get('access_count', 0) + 1
                scored_memories.append((score, memory))
        
        scored_memories.sort(key=lambda x: x[0], reverse=True)
        return [mem for score, mem in scored_memories[:limit]]

    def _contains_sensitive(self, text: str) -> bool:
        """Check if text contains sensitive patterns."""
        return any(pattern.search(text) for pattern in SENSITIVE_PATTERNS)

    def _classify_query_intent(self, text: str) -> Dict[str, bool]:
        """Enhanced query intent classification."""
        text_lower = text.lower()
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
                                return None # Do not load sensitive context
                            
                            logger.info(f"📁 Loaded manual context from {context_path}")
                            return f"USER-PROVIDED MANUAL CONTEXT:\n{content}"
            except Exception as e:
                logger.warning(f"Failed to load context from {location}: {e}")
                continue
        return None

    def push_interaction(self, role: str, content: str) -> None:
        """Add a validated interaction to the short-term conversation buffer."""
        if content and content.strip():
            self.conv_buffer.append({
                "role": role, "content": content, "timestamp": time.time()
            })

    def _build_prompt(self, user_input: str, include_memory: bool = True) -> List[Dict[str, str]]:
        """Construct optimized prompt with layered context and safety."""
        messages = [{"role": "system", "content": self.system_prompt}] if self.system_prompt else []
        
        manual_context = self._load_manual_context()
        if manual_context: 
            messages.append({"role": "system", "content": manual_context})

        # Include relevant memories
        if include_memory:
            memories = self.retrieve_memory(user_input)
            if memories:
                self.metrics.memory_hit_count += 1
                memory_content = "RELEVANT PAST CONTEXT:\n" + "\n".join(
                    [f"- User: {m['user'][:80]}... Assistant: {m['assistant'][:100]}... "
                     f"(importance: {m.get('importance',1)}/5, accesses: {m.get('access_count',0)})" 
                     for m in memories]
                )
                messages.append({"role": "system", "content": memory_content})

        # Include conversation history (last 6 exchanges)
        for interaction in list(self.conv_buffer)[-6:]:
            messages.append({"role": interaction["role"], "content": interaction["content"]})
            
        messages.append({"role": "user", "content": user_input})
        return messages

    def _get_importance_reason(self, user_input: str, importance: int) -> str:
        """Generate a human-readable reason for the importance score."""
        reasons = []
        
        # Analyze the user input directly
        content_words = user_input.lower().split()
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
        
        return "+".join(reasons) if reasons else "general_conversation"
    
    async def respond(self, user_input: str, use_llm: bool = True, include_memory: bool = True) -> str:
        """Enhanced asynchronous response generation with comprehensive safety and performance."""
        
            # TEMPORARY: Force a log message to test logging
        #print(f"🔍 FORCED DEBUG: respond method called with: '{user_input}'")
        logging.getLogger("AARIA.Persona").setLevel(logging.DEBUG)  # Force debug level
        
        start_time = time.time()
        self.metrics.total_interactions += 1
        self.metrics.user_messages += 1
        
        #logger.debug(f"🎯 RESPOND METHOD START: '{user_input}'")
        
        start_time = time.time()
        self.metrics.total_interactions += 1
        self.metrics.user_messages += 1
        
        #logger.debug(f"🎯 RESPOND METHOD START: '{user_input}'")
        
        self.push_interaction("user", user_input)

        # Enhanced intent classification for caching and safety
        intent = self._classify_query_intent(user_input)
        #logger.debug(f"🎯 Intent classified: {intent}")

        # Safety check - block sensitive queries immediately
        if intent["is_sensitive"]:
            logger.debug("🎯 Taking SENSITIVE path - early return")
            self.metrics.safety_trigger_count += 1
            reply = "I cannot process that request as it appears to contain sensitive information. Please use secure channels for such data."
            self.push_interaction("assistant", reply)
            return reply

        # Enhanced cache with intent awareness and TTL
        cache_key = f"{hashlib.md5(user_input.encode()).hexdigest()[:12]}_{intent['is_action_oriented']}"
        current_time = time.time()
        
        if (cache_key in self.response_cache and 
            not intent['is_urgent'] and
            current_time - self.response_cache[cache_key][1] < CACHE_TTL_SECONDS):
            logger.debug("🎯 Taking CACHE HIT path - early return")
            reply = self.response_cache[cache_key][0]
            self.push_interaction("assistant", reply)
            self.metrics.cache_hit_count += 1
            return reply

        # Quick responses for common queries
        lower_input = user_input.strip().lower()
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

        #logger.debug("🎯 Taking LLM path - proceeding to LLM call")

        # Enhanced async LLM invocation
        try:
            messages = self._build_prompt(user_input, include_memory=include_memory)
            primary_provider = LLMProvider(self.config.get("primary_provider", "groq"))
            
            #logger.debug(f"🎯 About to call LLM with {len(messages)} messages")
            
            # FIXED: Remove 'await' before the context manager
            async with self.llm_orchestrator.get_adapter(primary_provider) as adapter:
                request = LLMRequest(messages=messages, max_tokens=600, temperature=0.35)
                llm_response = await adapter.chat(request)
                reply_text = llm_response.content.strip()
                #logger.debug(f"🎯 Got LLM response: '{reply_text[:50]}...'")
                
        except (LLMConfigurationError, LLMError) as e:
            logger.error(f"🔧 LLM communication error: {e}")
            self.metrics.error_count += 1
            reply_text = "I'm having trouble connecting to my reasoning capabilities. Please check the system configuration and try again."
        except asyncio.TimeoutError:
            logger.error("⏰ LLM request timeout")
            self.metrics.error_count += 1
            reply_text = "The request timed out. Please try again in a moment."
        except Exception as e:
            logger.exception("💥 Unexpected error during response generation")
            self.metrics.error_count += 1
            reply_text = "I've encountered an unexpected system error. My apologies for the inconvenience."

        response_time = time.time() - start_time
        self._response_times.append(response_time)
        self.metrics.average_response_time = sum(self._response_times) / len(self._response_times)
        
        # Enhanced post-processing and safety check
        sanitized_reply = self._sanitize_response(reply_text)
        #logger.debug(f"🎯 Response sanitized: '{sanitized_reply[:50]}...'")

        #logger.debug(f"🎯 About to store memory for: '{user_input[:30]}...'")

        # In the respond method, find this section and update the store_memory call:

        # >>> ADD MEMORY STORAGE HERE <<<
        # Enhanced memory storage with importance assessment
        importance = self._calculate_memory_importance(user_input, sanitized_reply, intent)
            
        # Store the conversation in memory - NOW WITH INTENT PARAMETER
        memory_id = await self.store_memory(
            user_input, 
            sanitized_reply, 
            importance=importance, 
            intent=intent  # ← ADD THIS
        )
        logger.debug(f"💭 Stored conversation memory (ID: {memory_id}, importance: {importance})")
        # >>> END OF MEMORY STORAGE ADDITION <<<

        self.push_interaction("assistant", sanitized_reply)
        self.metrics.assistant_messages += 1

        # Enhanced caching strategy
        if len(reply_text) < 500 and not intent['is_urgent']:
            self.response_cache[cache_key] = (sanitized_reply, current_time)
            
        logger.info(f"⚡ Generated response in {response_time:.2f}s (avg: {self.metrics.average_response_time:.2f}s)")
        #logger.debug("🎯 RESPOND METHOD END - normal completion")
        return sanitized_reply

    def get_conversation_analytics(self) -> Dict[str, Any]:
        """Get comprehensive conversation analytics for monitoring."""
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

    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check with performance benchmarking."""
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
            # FIXED: Remove 'await' before the context manager
            async with self.llm_orchestrator.get_adapter(primary_provider) as adapter:
                health = await asyncio.wait_for(adapter.health_check(), timeout=10.0)
                health_status["llm_connectivity"] = health.status.value
        except asyncio.TimeoutError:
            health_status["llm_connectivity"] = "timeout"
        except Exception as e:
            health_status["llm_connectivity"] = f"error: {str(e)[:100]}"
            
        return health_status

    async def close(self) -> None:
        """Enhanced graceful shutdown with final memory persistence and cleanup."""
        logger.info("🛑 Beginning Enhanced PersonaCore shutdown...")
        try:
            # Force save all memories before shutdown
            await self._save_persistent_memory()
            await self._persist_critical_data()
            # Clean up expired cache entries
            current_time = time.time()
            self.response_cache = {
                k: v for k, v in self.response_cache.items() 
                if current_time - v[1] < CACHE_TTL_SECONDS
            }
            logger.info(f"✅ PersonaCore shutdown complete - {len(self.memory_index)} memories in index, {len([m for m in self.memory_index if m.get('importance', 1) >= 4])} important memories.")
        except Exception as e:
            logger.error(f"❌ Error during PersonaCore shutdown: {e}")

    async def _persist_critical_data(self) -> None:
        """Persist critical memories before shutdown."""
        try:
            important_memories = [m for m in self.memory_index if m.get('access_count', 0) > 2]
            for memory in important_memories:
                # FIXED: Use correct storage API and await
                if hasattr(self.core, 'store'):
                    await self.core.store.put(f"critical_{memory['id']}", "critical_memory", memory)
        except Exception as e:
            logger.warning(f"Failed to persist critical data: {e}")

    async def plan(self, goal_text: str, horizon_days: int = 7) -> Dict[str, Any]:
        """Enhanced asynchronous planning with better error recovery."""
        prompt = f"Create a concise multi-step plan to achieve this goal in {horizon_days} days: {goal_text}\nReturn JSON with 'steps' (title, due, notes) and 'summary'."
        messages = [{"role": "system", "content": self.system_prompt}, {"role": "user", "content": prompt}]
        
        try:
            primary_provider = LLMProvider(self.config.get("primary_provider", "groq"))
            # FIXED: Remove 'await' before the context manager
            async with self.llm_orchestrator.get_adapter(primary_provider) as adapter:
                request = LLMRequest(messages=messages, max_tokens=800, temperature=0.2)
                llm_response = await adapter.chat(request)
                json_text = self._extract_json(llm_response.content)
                
                if json_text:
                    plan = json.loads(json_text)
                    if isinstance(plan, dict) and "steps" in plan:
                        return plan
                        
            # Fallback plan
            return {
                "summary": llm_response.content[:200] + "..." if len(llm_response.content) > 200 else llm_response.content,
                "steps": [{"title": "Review and refine plan", "due": "Today", "notes": "Assess feasibility"}]
            }
            
        except Exception as e:
            logger.error(f"Plan generation failed: {e}")
            return {
                "summary": "Unable to generate detailed plan at this time.",
                "steps": []
            }

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