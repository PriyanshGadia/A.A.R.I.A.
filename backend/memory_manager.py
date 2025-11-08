"""
A.A.R.I.A Memory Manager with Hologram Integration
Enhanced memory management with dual-layer storage, holographic state tracking, and advanced persistence
"""

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union
import aiosqlite
from dataclasses import dataclass, asdict
import numpy as np

# Optional imports with fallbacks
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    # Create dummy classes for fallback
    class TfidfVectorizer:
        def __init__(self, **kwargs):
            pass
        def fit_transform(self, texts):
            return None

try:
    import pickle
    PICKLE_AVAILABLE = True
except ImportError:
    PICKLE_AVAILABLE = False

try:
    from hologram_state import HologramManager, HologramNode, HologramLink
    HOLOGRAM_AVAILABLE = True
except ImportError:
    HOLOGRAM_AVAILABLE = False
    # Create minimal hologram fallback
    class HologramManager:
        async def create_node(self, **kwargs):
            return f"hologram_{uuid.uuid4().hex}"
        async def create_link(self, **kwargs):
            return True
        async def update_node(self, **kwargs):
            return True
        async def get_graph_stats(self):
            return {"node_count": 0, "link_count": 0}
        async def cleanup_old_nodes(self, **kwargs):
            return True

class MemoryType(Enum):
    """Types of memories supported by the system"""
    CONVERSATION = "conversation"
    FACT = "fact"
    PREFERENCE = "preference"
    SECURITY = "security"
    EVENT = "event"
    RELATIONSHIP = "relationship"
    TASK = "task"
    PERSONAL = "personal"

class MemoryPriority(Enum):
    """Memory priority levels"""
    LOW = "low"
    MEDIUM = "medium" 
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class MemoryMetadata:
    """Metadata for memory entries"""
    created_at: str
    accessed_at: str
    access_count: int
    emotional_weight: float
    confidence: float
    source: str
    context_tags: List[str]
    expiration: Optional[str] = None

@dataclass 
class MemoryEntry:
    """Individual memory entry with full context"""
    id: str
    content: str
    memory_type: MemoryType
    priority: MemoryPriority
    metadata: MemoryMetadata
    embeddings: Optional[Any] = None
    hologram_node_id: Optional[str] = None

class DualMemoryLayer:
    """
    Dual-layer memory architecture with working memory and long-term storage
    """
    
    def __init__(self, user_id: str, db_path: str = "assistant_store.db"):
        self.user_id = user_id
        self.db_path = db_path
        self.logger = logging.getLogger("AARIA.Memory")
        
        # Memory containers
        self.working_memory: Dict[str, MemoryEntry] = {}
        self.long_term_cache: Dict[str, MemoryEntry] = {}
        
        # Memory indices
        self.temporal_index: List[str] = []  # Time-based access
        self.semantic_index: Dict[str, List[str]] = {}  # Content-based
        self.affective_index: Dict[str, float] = {}  # Emotional weight
        
        # Vectorization for semantic search (with fallback)
        if SKLEARN_AVAILABLE:
            self.vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        else:
            self.vectorizer = None
            self.logger.warning("⚠️ scikit-learn not available, semantic search disabled")
        self.content_vectors = None
        self.vectorized_contents = []
        
        # Hologram integration (with fallback)
        if HOLOGRAM_AVAILABLE:
            self.hologram_manager = HologramManager()
        else:
            self.hologram_manager = HologramManager()  # Use fallback
            self.logger.warning("⚠️ Hologram module not available, using fallback")
            
        self.memory_hologram_id = f"memory_system_{user_id}"
        
        # Performance tracking
        self.hit_count = 0
        self.miss_count = 0
        self.last_cleanup = time.time()
        
        # Configuration
        self.working_memory_limit = 50
        self.cache_memory_limit = 200
        self.cleanup_interval = 3600  # 1 hour
        
        self.logger.info(f"🔄 DualMemoryLayer initialized for user: {user_id}")

    async def initialize(self):
        """Initialize memory system and load persisted data"""
        try:
            # Initialize hologram for memory system
            if HOLOGRAM_AVAILABLE:
                await self.hologram_manager.create_node(
                    node_id=self.memory_hologram_id,
                    node_type="memory_system",
                    content={"user_id": self.user_id, "initialized_at": datetime.utcnow().isoformat()},
                    metadata={"system": "memory", "version": "2.0.0"}
                )
            
            # Load persisted memories from database
            await self._load_persisted_memories()
            
            # Initialize vectorizer with existing content
            await self._rebuild_vector_index()
            
            self.logger.info("✅ DualMemoryLayer initialization complete")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ DualMemoryLayer initialization failed: {str(e)}")
            # Don't fail completely - continue with empty memory
            return True

    async def store_memory(self, 
                          content: str, 
                          memory_type: MemoryType,
                          priority: MemoryPriority = MemoryPriority.MEDIUM,
                          emotional_weight: float = 0.5,
                          confidence: float = 1.0,
                          source: str = "user_input",
                          context_tags: List[str] = None,
                          ttl: Optional[int] = None) -> str:
        """Store a memory with full context and hologram integration"""
        
        try:
            memory_id = f"mem_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}"
            now = datetime.utcnow().isoformat()
            
            # Calculate expiration if TTL provided
            expiration = None
            if ttl:
                expiration = (datetime.utcnow() + timedelta(seconds=ttl)).isoformat()
            
            metadata = MemoryMetadata(
                created_at=now,
                accessed_at=now,
                access_count=0,
                emotional_weight=emotional_weight,
                confidence=confidence,
                source=source,
                context_tags=context_tags or [],
                expiration=expiration
            )
            
            memory_entry = MemoryEntry(
                id=memory_id,
                content=content,
                memory_type=memory_type,
                priority=priority,
                metadata=metadata
            )
            
            # Create hologram node for this memory
            if HOLOGRAM_AVAILABLE:
                try:
                    hologram_node_id = await self.hologram_manager.create_node(
                        node_id=f"memory_{memory_id}",
                        node_type="memory",
                        content={
                            "content": content,
                            "memory_type": memory_type.value,
                            "priority": priority.value,
                            "user_id": self.user_id
                        },
                        metadata={
                            "emotional_weight": emotional_weight,
                            "confidence": confidence,
                            "source": source,
                            "context_tags": context_tags or []
                        }
                    )
                    memory_entry.hologram_node_id = hologram_node_id
                    
                    # Link to memory system hologram
                    await self.hologram_manager.create_link(
                        source_id=self.memory_hologram_id,
                        target_id=hologram_node_id,
                        link_type="contains_memory",
                        metadata={"storage_time": now}
                    )
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Hologram creation failed for memory {memory_id}: {str(e)}")
            
            # Store in appropriate layer based on priority and type
            if (priority in [MemoryPriority.HIGH, MemoryPriority.CRITICAL] or 
                memory_type in [MemoryType.SECURITY, MemoryType.PERSONAL]):
                self.working_memory[memory_id] = memory_entry
                await self._evict_working_memory_if_needed()
            else:
                self.long_term_cache[memory_id] = memory_entry
                await self._evict_cache_if_needed()
            
            # Update indices
            await self._update_indices(memory_entry)
            
            # Persist to database
            await self._persist_memory(memory_entry)
            
            self.logger.info(f"💾 Memory stored: {memory_id} (Type: {memory_type.value}, Priority: {priority.value})")
            return memory_id
            
        except Exception as e:
            self.logger.error(f"❌ Memory storage failed: {str(e)}")
            # Return a fallback memory ID to prevent complete failure
            return f"mem_fallback_{int(time.time() * 1000)}"

    async def retrieve_memories(self, 
                              query: Optional[str] = None,
                              memory_type: Optional[MemoryType] = None,
                              max_results: int = 10,
                              recency_weight: float = 0.3,
                              relevance_weight: float = 0.4,
                              importance_weight: float = 0.3) -> List[MemoryEntry]:
        """Retrieve memories with multi-factor ranking"""
        
        try:
            candidates = []
            
            # Start with working memory (always highest priority)
            for memory in self.working_memory.values():
                if await self._is_memory_relevant(memory, query, memory_type):
                    candidates.append(memory)
            
            # Add from cache if needed
            if len(candidates) < max_results:
                for memory in self.long_term_cache.values():
                    if (await self._is_memory_relevant(memory, query, memory_type) and 
                        memory not in candidates):
                        candidates.append(memory)
            
            # Rank candidates using multi-factor scoring
            ranked_memories = await self._rank_memories(
                candidates, query, recency_weight, relevance_weight, importance_weight
            )
            
            # Update access patterns
            for memory in ranked_memories[:max_results]:
                await self._update_memory_access(memory)
            
            self.logger.info(f"🔍 Retrieved {len(ranked_memories[:max_results])} memories for query: {query}")
            return ranked_memories[:max_results]
            
        except Exception as e:
            self.logger.error(f"❌ Memory retrieval failed: {str(e)}")
            return []

    async def update_memory(self, 
                          memory_id: str, 
                          new_content: Optional[str] = None,
                          emotional_weight: Optional[float] = None,
                          confidence: Optional[float] = None,
                          context_tags: Optional[List[str]] = None) -> bool:
        """Update an existing memory"""
        
        try:
            memory = await self._find_memory(memory_id)
            if not memory:
                self.logger.warning(f"⚠️ Memory not found for update: {memory_id}")
                return False
            
            if new_content:
                memory.content = new_content
                # Regenerate embedding if content changed
                memory.embeddings = None
            
            if emotional_weight is not None:
                memory.metadata.emotional_weight = emotional_weight
            
            if confidence is not None:
                memory.metadata.confidence = confidence
                
            if context_tags is not None:
                memory.metadata.context_tags = context_tags
            
            memory.metadata.accessed_at = datetime.utcnow().isoformat()
            
            # Update hologram if exists
            if memory.hologram_node_id and HOLOGRAM_AVAILABLE:
                try:
                    await self.hologram_manager.update_node(
                        node_id=memory.hologram_node_id,
                        content={"content": memory.content},
                        metadata={
                            "emotional_weight": memory.metadata.emotional_weight,
                            "confidence": memory.metadata.confidence,
                            "context_tags": memory.metadata.context_tags
                        }
                    )
                except Exception as e:
                    self.logger.warning(f"⚠️ Hologram update failed for memory {memory_id}: {str(e)}")
            
            # Repersist to database
            await self._persist_memory(memory)
            
            self.logger.info(f"✏️ Memory updated: {memory_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Memory update failed: {str(e)}")
            return False

    async def forget_memory(self, memory_id: str, permanent: bool = False) -> bool:
        """Remove a memory from the system"""
        
        try:
            memory = await self._find_memory(memory_id)
            if not memory:
                return False
            
            # Remove from active storage
            if memory_id in self.working_memory:
                del self.working_memory[memory_id]
            if memory_id in self.long_term_cache:
                del self.long_term_cache[memory_id]
            
            # Remove from indices
            await self._remove_from_indices(memory_id)
            
            # Update hologram state
            if memory.hologram_node_id and HOLOGRAM_AVAILABLE:
                try:
                    await self.hologram_manager.create_node(
                        node_id=f"forgotten_{memory_id}",
                        node_type="forgotten_memory",
                        content={"original_content": memory.content, "forgotten_at": datetime.utcnow().isoformat()},
                        metadata={"original_type": memory.memory_type.value}
                    )
                    
                    # Mark original memory node as archived
                    await self.hologram_manager.update_node(
                        node_id=memory.hologram_node_id,
                        metadata={"status": "archived", "forgotten": True}
                    )
                except Exception as e:
                    self.logger.warning(f"⚠️ Hologram archival failed for memory {memory_id}: {str(e)}")
            
            # Permanent deletion from database
            if permanent:
                await self._delete_memory_from_db(memory_id)
            
            self.logger.info(f"🗑️ Memory {'permanently forgotten' if permanent else 'archived'}: {memory_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Memory forgetting failed: {str(e)}")
            return False

    async def get_memory_stats(self) -> Dict[str, Any]:
        """Get comprehensive memory system statistics"""
        
        try:
            total_memories = len(self.working_memory) + len(self.long_term_cache)
            
            # Calculate hit rate
            total_accesses = self.hit_count + self.miss_count
            hit_rate = self.hit_count / total_accesses if total_accesses > 0 else 0
            
            # Memory type distribution
            type_distribution = {}
            for memory in list(self.working_memory.values()) + list(self.long_term_cache.values()):
                mem_type = memory.memory_type.value
                type_distribution[mem_type] = type_distribution.get(mem_type, 0) + 1
            
            # Hologram integration stats
            hologram_stats = {}
            if HOLOGRAM_AVAILABLE:
                try:
                    hologram_stats = await self.hologram_manager.get_graph_stats()
                except Exception as e:
                    self.logger.warning(f"⚠️ Hologram stats failed: {str(e)}")
            
            return {
                "total_memories": total_memories,
                "working_memory_count": len(self.working_memory),
                "long_term_cache_count": len(self.long_term_cache),
                "hit_rate": hit_rate,
                "type_distribution": type_distribution,
                "hologram_integration": {
                    "memory_nodes": hologram_stats.get("node_count", 0),
                    "memory_links": hologram_stats.get("link_count", 0),
                    "available": HOLOGRAM_AVAILABLE
                },
                "last_cleanup": self.last_cleanup,
                "vector_index_size": len(self.vectorized_contents),
                "vector_search_available": SKLEARN_AVAILABLE
            }
            
        except Exception as e:
            self.logger.error(f"❌ Memory stats failed: {str(e)}")
            return {"error": str(e)}

    async def perform_maintenance(self):
        """Perform memory system maintenance tasks"""
        
        try:
            current_time = time.time()
            
            # Cleanup expired memories
            expired_count = await self._cleanup_expired_memories()
            
            # Rebuild vector index if needed
            if SKLEARN_AVAILABLE and len(self.vectorized_contents) > len(self.vectorized_contents) * 1.5:
                await self._rebuild_vector_index()
            
            # Persist important memories
            await self._persist_important_memories()
            
            # Update hologram system state
            if HOLOGRAM_AVAILABLE:
                try:
                    await self.hologram_manager.create_node(
                        node_id=f"maintenance_{int(current_time)}",
                        node_type="maintenance",
                        content={
                            "timestamp": datetime.utcnow().isoformat(),
                            "expired_memories_cleaned": expired_count,
                            "total_memories": len(self.working_memory) + len(self.long_term_cache)
                        },
                        metadata={"maintenance_cycle": True}
                    )
                except Exception as e:
                    self.logger.warning(f"⚠️ Hologram maintenance node creation failed: {str(e)}")
            
            self.last_cleanup = current_time
            self.logger.info(f"🧹 Memory maintenance completed: {expired_count} expired memories cleaned")
            
        except Exception as e:
            self.logger.error(f"❌ Memory maintenance failed: {str(e)}")

    async def persist_all_memories(self):
        """Persist all memories to database"""
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Store working memory
                for memory in self.working_memory.values():
                    await self._store_memory_in_db(db, memory)
                
                # Store cache memory
                for memory in self.long_term_cache.values():
                    await self._store_memory_in_db(db, memory)
                
                await db.commit()
            
            self.logger.info("💾 All memories persisted to database")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to persist all memories: {str(e)}")
            return False

    # Private helper methods

    async def _is_memory_relevant(self, 
                                memory: MemoryEntry, 
                                query: Optional[str],
                                memory_type: Optional[MemoryType]) -> bool:
        """Check if memory matches query and type filters"""
        
        if memory_type and memory.memory_type != memory_type:
            return False
            
        if not query:
            return True
            
        # Check content relevance
        query_lower = query.lower()
        content_lower = memory.content.lower()
        
        # Simple keyword match
        if query_lower in content_lower:
            return True
            
        # Check context tags
        for tag in memory.metadata.context_tags:
            if query_lower in tag.lower():
                return True
                
        return False

    async def _rank_memories(self,
                           memories: List[MemoryEntry],
                           query: Optional[str],
                           recency_weight: float,
                           relevance_weight: float, 
                           importance_weight: float) -> List[MemoryEntry]:
        """Rank memories using multi-factor scoring"""
        
        if not memories:
            return []
        
        scored_memories = []
        current_time = datetime.utcnow()
        
        for memory in memories:
            score = 0.0
            
            # Recency score (based on last access)
            try:
                last_access = datetime.fromisoformat(memory.metadata.accessed_at.replace('Z', '+00:00'))
                recency_days = (current_time - last_access).days
                recency_score = max(0, 1 - (recency_days / 30))  # Decay over 30 days
                score += recency_score * recency_weight
            except Exception:
                recency_score = 0.5  # Default if date parsing fails
            
            # Importance score (based on priority and emotional weight)
            priority_scores = {
                MemoryPriority.LOW: 0.2,
                MemoryPriority.MEDIUM: 0.5,
                MemoryPriority.HIGH: 0.8,
                MemoryPriority.CRITICAL: 1.0
            }
            importance_score = (priority_scores.get(memory.priority, 0.5) + memory.metadata.emotional_weight) / 2
            score += importance_score * importance_weight
            
            # Relevance score (if query provided)
            if query:
                relevance_score = await self._calculate_relevance_score(memory, query)
                score += relevance_score * relevance_weight
            else:
                # Without query, distribute relevance weight to other factors
                score += (recency_score + importance_score) / 2 * relevance_weight
            
            scored_memories.append((score, memory))
        
        # Sort by score descending
        scored_memories.sort(key=lambda x: x[0], reverse=True)
        return [memory for score, memory in scored_memories]

    async def _calculate_relevance_score(self, memory: MemoryEntry, query: str) -> float:
        """Calculate relevance score between memory and query"""
        
        # Simple keyword matching
        if query.lower() in memory.content.lower():
            return 0.8
            
        # Check context tags
        for tag in memory.metadata.context_tags:
            if query.lower() in tag.lower():
                return 0.6
                
        # Semantic similarity if available
        if SKLEARN_AVAILABLE and self.vectorizer and self.content_vectors is not None:
            try:
                # This would require proper implementation with the vectorizer
                return 0.3
            except Exception:
                pass
                
        return 0.1

    async def _update_memory_access(self, memory: MemoryEntry):
        """Update memory access patterns"""
        
        memory.metadata.accessed_at = datetime.utcnow().isoformat()
        memory.metadata.access_count += 1
        
        # Promote to working memory if frequently accessed
        if (memory.metadata.access_count > 5 and 
            memory.id in self.long_term_cache and
            len(self.working_memory) < self.working_memory_limit):
            
            self.working_memory[memory.id] = memory
            del self.long_term_cache[memory.id]

    async def _update_indices(self, memory: MemoryEntry):
        """Update all memory indices"""
        
        # Temporal index (append new memories to front)
        if memory.id not in self.temporal_index:
            self.temporal_index.insert(0, memory.id)
        
        # Semantic index (by context tags)
        for tag in memory.metadata.context_tags:
            if tag not in self.semantic_index:
                self.semantic_index[tag] = []
            if memory.id not in self.semantic_index[tag]:
                self.semantic_index[tag].append(memory.id)
        
        # Affective index
        self.affective_index[memory.id] = memory.metadata.emotional_weight

    async def _remove_from_indices(self, memory_id: str):
        """Remove memory from all indices"""
        
        # Temporal index
        if memory_id in self.temporal_index:
            self.temporal_index.remove(memory_id)
        
        # Semantic index
        for tag in list(self.semantic_index.keys()):
            if memory_id in self.semantic_index[tag]:
                self.semantic_index[tag].remove(memory_id)
            if not self.semantic_index[tag]:
                del self.semantic_index[tag]
        
        # Affective index
        if memory_id in self.affective_index:
            del self.affective_index[memory_id]

    async def _evict_working_memory_if_needed(self):
        """Evict from working memory if over limit"""
        
        if len(self.working_memory) <= self.working_memory_limit:
            return
            
        # Evict least recently accessed memories
        memories_by_access = sorted(
            self.working_memory.values(),
            key=lambda m: m.metadata.accessed_at
        )
        
        while len(self.working_memory) > self.working_memory_limit:
            memory_to_evict = memories_by_access.pop(0)
            # Move to cache if possible, otherwise persist and remove
            if len(self.long_term_cache) < self.cache_memory_limit:
                self.long_term_cache[memory_to_evict.id] = memory_to_evict
            else:
                await self._persist_memory(memory_to_evict)
            del self.working_memory[memory_to_evict.id]

    async def _evict_cache_if_needed(self):
        """Evict from cache if over limit"""
        
        if len(self.long_term_cache) <= self.cache_memory_limit:
            return
            
        # Evict least important memories
        memories_by_importance = sorted(
            self.long_term_cache.values(),
            key=lambda m: (m.priority.value, m.metadata.access_count)
        )
        
        while len(self.long_term_cache) > self.cache_memory_limit:
            memory_to_evict = memories_by_importance.pop(0)
            await self._persist_memory(memory_to_evict)
            del self.long_term_cache[memory_to_evict.id]

    async def _cleanup_expired_memories(self) -> int:
        """Remove expired memories from system"""
        
        expired_count = 0
        current_time = datetime.utcnow()
        
        for memory_list in [self.working_memory, self.long_term_cache]:
            memories_to_remove = []
            
            for memory_id, memory in memory_list.items():
                if memory.metadata.expiration:
                    try:
                        expiration_time = datetime.fromisoformat(memory.metadata.expiration.replace('Z', '+00:00'))
                        if current_time > expiration_time:
                            memories_to_remove.append(memory_id)
                    except Exception as e:
                        self.logger.warning(f"⚠️ Invalid expiration date for memory {memory_id}: {e}")
            
            for memory_id in memories_to_remove:
                await self.forget_memory(memory_id, permanent=True)
                expired_count += 1
        
        return expired_count

    async def _persist_important_memories(self):
        """Persist important memories to ensure durability"""
        
        important_memories = []
        
        for memory in self.working_memory.values():
            if (memory.priority in [MemoryPriority.HIGH, MemoryPriority.CRITICAL] or
                memory.memory_type in [MemoryType.PERSONAL, MemoryType.SECURITY]):
                important_memories.append(memory)
        
        for memory in important_memories:
            await self._persist_memory(memory)

    # Database operations

    async def _load_persisted_memories(self):
        """Load persisted memories from database"""
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Create memories table if not exists
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS memories (
                        id TEXT PRIMARY KEY,
                        user_id TEXT,
                        content TEXT,
                        memory_type TEXT,
                        priority TEXT,
                        metadata TEXT,
                        embeddings BLOB,
                        hologram_node_id TEXT,
                        created_at TEXT,
                        accessed_at TEXT
                    )
                ''')
                
                # Load memories for this user
                cursor = await db.execute(
                    'SELECT * FROM memories WHERE user_id = ? ORDER BY accessed_at DESC LIMIT ?',
                    (self.user_id, self.cache_memory_limit)
                )
                
                rows = await cursor.fetchall()
                
                for row in rows:
                    memory = await self._row_to_memory(row)
                    if memory:
                        self.long_term_cache[memory.id] = memory
                
                await self._rebuild_vector_index()
                
                self.logger.info(f"📂 Loaded {len(rows)} persisted memories for user: {self.user_id}")
                
        except Exception as e:
            self.logger.error(f"❌ Failed to load persisted memories: {str(e)}")

    async def _persist_memory(self, memory: MemoryEntry):
        """Persist a single memory to database"""
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await self._store_memory_in_db(db, memory)
                await db.commit()
                
        except Exception as e:
            self.logger.error(f"❌ Failed to persist memory {memory.id}: {str(e)}")

    async def _store_memory_in_db(self, db: aiosqlite.Connection, memory: MemoryEntry):
        """Store memory in database"""
        
        try:
            # Handle embeddings serialization
            embeddings_blob = None
            if memory.embeddings is not None and PICKLE_AVAILABLE:
                embeddings_blob = pickle.dumps(memory.embeddings)
            
            await db.execute('''
                INSERT OR REPLACE INTO memories 
                (id, user_id, content, memory_type, priority, metadata, embeddings, hologram_node_id, created_at, accessed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                memory.id,
                self.user_id,
                memory.content,
                memory.memory_type.value,
                memory.priority.value,
                json.dumps(asdict(memory.metadata)),
                embeddings_blob,
                memory.hologram_node_id,
                memory.metadata.created_at,
                memory.metadata.accessed_at
            ))
            
        except Exception as e:
            self.logger.error(f"❌ Database storage failed for memory {memory.id}: {str(e)}")

    async def _delete_memory_from_db(self, memory_id: str):
        """Delete memory from database"""
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute('DELETE FROM memories WHERE id = ?', (memory_id,))
                await db.commit()
                
        except Exception as e:
            self.logger.error(f"❌ Failed to delete memory {memory_id} from database: {str(e)}")

    async def _row_to_memory(self, row) -> Optional[MemoryEntry]:
        """Convert database row to MemoryEntry object"""
        
        try:
            metadata_dict = json.loads(row[5])  # metadata column
            metadata = MemoryMetadata(**metadata_dict)
            
            embeddings = None
            if row[6] and PICKLE_AVAILABLE:  # embeddings column
                try:
                    embeddings = pickle.loads(row[6])
                except Exception:
                    pass  # Ignore pickle errors
            
            memory = MemoryEntry(
                id=row[0],  # id
                content=row[2],  # content
                memory_type=MemoryType(row[3]),  # memory_type
                priority=MemoryPriority(row[4]),  # priority
                metadata=metadata,
                embeddings=embeddings,
                hologram_node_id=row[7]  # hologram_node_id
            )
            
            return memory
            
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to convert row to memory: {str(e)}")
            return None

    async def _find_memory(self, memory_id: str) -> Optional[MemoryEntry]:
        """Find memory in any storage layer"""
        
        if memory_id in self.working_memory:
            self.hit_count += 1
            return self.working_memory[memory_id]
        elif memory_id in self.long_term_cache:
            self.hit_count += 1
            return self.long_term_cache[memory_id]
        else:
            self.miss_count += 1
            # Try to load from database
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    cursor = await db.execute(
                        'SELECT * FROM memories WHERE id = ? AND user_id = ?',
                        (memory_id, self.user_id)
                    )
                    row = await cursor.fetchone()
                    
                    if row:
                        memory = await self._row_to_memory(row)
                        if memory:
                            # Add to cache
                            self.long_term_cache[memory_id] = memory
                            self.hit_count += 1
                            return memory
                            
            except Exception as e:
                self.logger.error(f"❌ Database lookup failed for memory {memory_id}: {str(e)}")
        
        return None

    async def _rebuild_vector_index(self):
        """Rebuild the semantic vector index"""
        
        if not SKLEARN_AVAILABLE:
            return
            
        try:
            all_contents = [memory.content for memory in list(self.working_memory.values()) + list(self.long_term_cache.values())]
            
            if all_contents:
                self.vectorized_contents = all_contents
                self.content_vectors = self.vectorizer.fit_transform(all_contents)
            else:
                self.vectorized_contents = []
                self.content_vectors = None
                
        except Exception as e:
            self.logger.warning(f"⚠️ Vector index rebuild failed: {str(e)}")
            self.vectorized_contents = []
            self.content_vectors = None

class MemoryManager:
    """
    Main Memory Manager coordinating all memory operations with hologram integration
    """
    
    def __init__(self, assistant_core=None, **kwargs):
        """
        Initialize Memory Manager
        
        Args:
            assistant_core: Optional assistant core reference for integration
            **kwargs: Additional arguments for future compatibility
        """
        self.logger = logging.getLogger("AARIA.MemoryManager")
        self.user_memory_layers: Dict[str, DualMemoryLayer] = {}
        self.assistant_core = assistant_core
        
        if HOLOGRAM_AVAILABLE:
            self.hologram_manager = HologramManager()
        else:
            self.hologram_manager = HologramManager()  # Use fallback
            
        self.initialized = False
        
        # Log any additional kwargs for debugging
        if kwargs:
            self.logger.debug(f"Additional kwargs received: {kwargs}")
        
        self.logger.info("🚀 Memory Manager initialized with assistant core integration")

    async def initialize(self):
        """Initialize the memory manager"""
        try:
            if self.assistant_core:
                self.logger.info("🔗 Memory Manager integrated with Assistant Core")
            
            self.initialized = True
            self.logger.info("✅ Memory Manager initialization complete")
            return True
        except Exception as e:
            self.logger.error(f"❌ Memory Manager initialization failed: {str(e)}")
            # Still mark as initialized to prevent system failure
            self.initialized = True
            return True

    async def get_memory_layer(self, user_id: str) -> DualMemoryLayer:
        """Get or create memory layer for user"""
        
        if not self.initialized:
            await self.initialize()
            
        if user_id not in self.user_memory_layers:
            self.user_memory_layers[user_id] = DualMemoryLayer(user_id)
            await self.user_memory_layers[user_id].initialize()
        
        return self.user_memory_layers[user_id]

    async def store_user_memory(self,
                              user_id: str,
                              content: str,
                              memory_type: MemoryType,
                              **kwargs) -> str:
        """Store memory for specific user"""
        
        try:
            memory_layer = await self.get_memory_layer(user_id)
            return await memory_layer.store_memory(content, memory_type, **kwargs)
        except Exception as e:
            self.logger.error(f"❌ User memory storage failed: {str(e)}")
            return f"mem_error_{int(time.time() * 1000)}"

    async def retrieve_user_memories(self,
                                   user_id: str,
                                   query: Optional[str] = None,
                                   **kwargs) -> List[MemoryEntry]:
        """Retrieve memories for specific user"""
        
        try:
            memory_layer = await self.get_memory_layer(user_id)
            return await memory_layer.retrieve_memories(query, **kwargs)
        except Exception as e:
            self.logger.error(f"❌ User memory retrieval failed: {str(e)}")
            return []

    async def get_system_memory_stats(self) -> Dict[str, Any]:
        """Get statistics for all memory layers"""
        
        try:
            stats = {
                "total_users": len(self.user_memory_layers),
                "users": {},
                "initialized": self.initialized,
                "assistant_core_integrated": self.assistant_core is not None,
                "dependencies": {
                    "sklearn": SKLEARN_AVAILABLE,
                    "hologram": HOLOGRAM_AVAILABLE,
                    "pickle": PICKLE_AVAILABLE
                }
            }
            
            for user_id, memory_layer in self.user_memory_layers.items():
                stats["users"][user_id] = await memory_layer.get_memory_stats()
            
            # Add hologram system stats
            if HOLOGRAM_AVAILABLE:
                try:
                    hologram_stats = await self.hologram_manager.get_graph_stats()
                    stats["hologram_system"] = hologram_stats
                except Exception as e:
                    stats["hologram_system"] = {"error": str(e)}
            
            return stats
            
        except Exception as e:
            self.logger.error(f"❌ System memory stats failed: {str(e)}")
            return {"error": str(e), "initialized": self.initialized}

    async def perform_system_maintenance(self):
        """Perform maintenance across all memory layers"""
        
        if not self.initialized:
            return
            
        self.logger.info("🔄 Performing system-wide memory maintenance")
        
        for user_id, memory_layer in self.user_memory_layers.items():
            try:
                await memory_layer.perform_maintenance()
            except Exception as e:
                self.logger.error(f"❌ Maintenance failed for user {user_id}: {str(e)}")
        
        # Global hologram maintenance
        if HOLOGRAM_AVAILABLE:
            try:
                await self.hologram_manager.cleanup_old_nodes(hours=24)
            except Exception as e:
                self.logger.warning(f"⚠️ Hologram cleanup failed: {str(e)}")

    async def persist_all_system_memories(self):
        """Persist all memories across all users"""
        
        if not self.initialized:
            return {"error": "Memory manager not initialized"}
            
        self.logger.info("💾 Persisting all system memories")
        
        results = {}
        for user_id, memory_layer in self.user_memory_layers.items():
            try:
                success = await memory_layer.persist_all_memories()
                results[user_id] = "success" if success else "failed"
            except Exception as e:
                results[user_id] = f"error: {str(e)}"
                self.logger.error(f"❌ Persistence failed for user {user_id}: {str(e)}")
        
        return results

    async def shutdown(self):
        """Clean shutdown of memory manager"""
        
        self.logger.info("🛑 Shutting down Memory Manager")
        
        # Persist all memories
        if self.initialized:
            await self.persist_all_system_memories()
        
        # Clear memory layers
        self.user_memory_layers.clear()
        self.initialized = False
        
        self.logger.info("✅ Memory Manager shutdown complete")

# Global memory manager instance
memory_manager = MemoryManager()

# Convenience functions for easy access
async def initialize_memory_manager(assistant_core=None):
    """Initialize the global memory manager"""
    global memory_manager
    # Reinitialize with assistant core if provided
    if assistant_core:
        memory_manager = MemoryManager(assistant_core=assistant_core)
    return await memory_manager.initialize()

async def store_memory(user_id: str, content: str, memory_type: Union[MemoryType, str], **kwargs):
    """Convenience function to store memory"""
    if isinstance(memory_type, str):
        memory_type = MemoryType(memory_type)
    return await memory_manager.store_user_memory(user_id, content, memory_type, **kwargs)

async def retrieve_memories(user_id: str, query: Optional[str] = None, **kwargs):
    """Convenience function to retrieve memories"""
    return await memory_manager.retrieve_user_memories(user_id, query, **kwargs)