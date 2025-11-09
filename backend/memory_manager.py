"""
A.A.R.I.A - Autonomous AI Research and Intelligence Assistant
Enhanced DualMemoryManager v7.0.0
Holographic & Semantic Memory Integration
"""

import asyncio
import json
import logging
import time
import aiosqlite
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
from uuid import uuid4
from dataclasses import dataclass, asdict, field

# --- Semantic Search & Hologram Dependencies ---
try:
    from sentence_transformers import SentenceTransformer, util
    SEMANTIC_SEARCH_AVAILABLE = True
except ImportError:
    SEMANTIC_SEARCH_AVAILABLE = False

try:
    from hologram_state import HologramState, HolographicNode, HolographicLink
    HOLOGRAM_AVAILABLE = True
except ImportError:
    HOLOGRAM_AVAILABLE = False
    # Define fallback classes if hologram_state is not available
    class HologramState:
        async def store_node(self, node): pass
        async def get_node(self, node_id): return None
        async def delete_node(self, node_id): pass
        async def create_link(self, link): pass
        async def get_node_links(self, node_id): return []
        async def set_node_active(self, node_id: str, activity_multiplier: float = 1.25): pass
        async def despawn_node(self, node_id: str): pass
    class HolographicNode:
        def __init__(self, node_id, node_type, data, **kwargs):
            self.node_id = node_id
            self.node_type = node_type
            self.data = data
    class HolographicLink:
        def __init__(self, source_id, target_id, link_type, **kwargs):
            self.source_id = source_id
            self.target_id = target_id
            self.link_type = link_type

# --- Dataclasses for Memory Structure ---

@dataclass
class MemoryEntry:
    """Represents a single, comprehensive memory entry."""
    id: str
    content: Any
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_accessed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    access_count: int = 0
    hologram_node_id: Optional[str] = None

    def to_db_tuple(self, container_id: str) -> tuple:
        """Serializes the entry for database storage."""
        return (
            self.id,
            container_id,
            json.dumps(self.content),
            json.dumps(self.metadata),
            self.created_at.isoformat(),
            self.last_accessed_at.isoformat(),
            self.access_count,
            self.hologram_node_id
        )

    @classmethod
    def from_db_row(cls, row: aiosqlite.Row) -> 'MemoryEntry':
        """Deserializes a database row into a MemoryEntry."""
        return cls(
            id=row['id'],
            content=json.loads(row['content']),
            metadata=json.loads(row['metadata']),
            created_at=datetime.fromisoformat(row['created_at']),
            last_accessed_at=datetime.fromisoformat(row['last_accessed_at']),
            access_count=row['access_count'],
            hologram_node_id=row['hologram_node_id']
        )

# --- Memory Container ---

class MemoryContainer:
    """
    Manages a discrete collection of memories for a specific user or purpose,
    with caching and database persistence.
    """
    def __init__(self, container_id: str, db_manager: 'DualMemoryManager', ttl: Optional[timedelta] = None):
        self.id = container_id
        self.db_manager = db_manager
        self.ttl = ttl
        self.cache: Dict[str, MemoryEntry] = {}
        self.logger = logging.getLogger(f"AARIA.MemoryContainer.{self.id}")
        self.is_loaded = False

    async def _load_from_db(self):
        """Loads all entries for this container from the database into the cache."""
        if self.is_loaded:
            return
        try:
            async with aiosqlite.connect(self.db_manager.db_path) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM memories WHERE container_id = ?", (self.id,)) as cursor:
                    async for row in cursor:
                        entry = MemoryEntry.from_db_row(row)
                        self.cache[entry.id] = entry
            self.is_loaded = True
            self.logger.info(f"Loaded {len(self.cache)} entries from database.")
        except Exception as e:
            self.logger.error(f"Failed to load container from DB: {e}", exc_info=True)

    async def store(self, key: str, value: Any, metadata: Optional[Dict[str, Any]] = None) -> MemoryEntry:
        """Stores or updates a key-value pair in the container."""
        await self._load_from_db()
        
        if key in self.cache:
            entry = self.cache[key]
            entry.content = value
            entry.metadata.update(metadata or {})
            entry.last_accessed_at = datetime.now(timezone.utc)
            entry.access_count += 1
        else:
            entry = MemoryEntry(
                id=key,
                content=value,
                metadata=metadata or {}
            )
            self.cache[key] = entry

        await self.db_manager._persist_entry(self.id, entry)
        return entry

    async def retrieve(self, key: str, default: Any = None) -> Optional[Any]:
        """Retrieves a value by key from the container."""
        await self._load_from_db()
        entry = self.cache.get(key)
        if entry:
            entry.last_accessed_at = datetime.now(timezone.utc)
            entry.access_count += 1
            asyncio.create_task(self.db_manager._update_entry_access(entry))
            if entry.hologram_node_id and self.db_manager.hologram:
                asyncio.create_task(self.db_manager.hologram.set_node_active(entry.hologram_node_id))
            return entry.content
        return default

    async def search_pattern(self, pattern: str) -> List[str]:
        """Searches for keys matching a simple pattern (e.g., 'user_*')."""
        await self._load_from_db()
        if pattern.endswith('*'):
            prefix = pattern[:-1]
            return [key for key in self.cache.keys() if key.startswith(prefix)]
        return [key for key in self.cache.keys() if pattern in key]

    async def delete(self, key: str) -> bool:
        """Deletes an entry by key."""
        await self._load_from_db()
        if key in self.cache:
            entry = self.cache.pop(key)
            await self.db_manager._delete_entry(entry.id)
            if entry.hologram_node_id and self.db_manager.hologram:
                asyncio.create_task(self.db_manager.hologram.despawn_node(entry.hologram_node_id))
            return True
        return False

# --- Dual Memory Manager ---

class DualMemoryManager:
    """
    The central memory orchestrator, providing access to memory containers,
    semantic search, and integrating with the holographic state system.
    """
    DB_SCHEMA = """
    CREATE TABLE IF NOT EXISTS containers (
        id TEXT PRIMARY KEY,
        created_at TEXT NOT NULL,
        ttl_seconds INTEGER
    );
    CREATE TABLE IF NOT EXISTS memories (
        id TEXT PRIMARY KEY,
        container_id TEXT NOT NULL,
        content TEXT NOT NULL,
        metadata TEXT,
        created_at TEXT NOT NULL,
        last_accessed_at TEXT NOT NULL,
        access_count INTEGER DEFAULT 0,
        hologram_node_id TEXT,
        FOREIGN KEY (container_id) REFERENCES containers (id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS memory_embeddings (
        memory_id TEXT PRIMARY KEY,
        embedding BLOB NOT NULL,
        FOREIGN KEY (memory_id) REFERENCES memories (id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_memories_container_id ON memories (container_id);
    """

    def __init__(self, assistant_core: Any = None, db_path: str = "aaria_memory_v2.db"):
        self.db_path = db_path
        self.logger = logging.getLogger("AARIA.DualMemoryManager")
        self.containers: Dict[str, MemoryContainer] = {}
        self.hologram = HologramState() if HOLOGRAM_AVAILABLE else None
        self.semantic_model = SentenceTransformer('all-MiniLM-L6-v2') if SEMANTIC_SEARCH_AVAILABLE else None
        self._db_lock = asyncio.Lock()

    async def initialize(self):
        """Initializes the database and prepares the manager."""
        async with self._db_lock:
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.executescript(self.DB_SCHEMA)
                    await db.commit()
                self.logger.info("Database initialized successfully.")
                if not SEMANTIC_SEARCH_AVAILABLE:
                    self.logger.warning("SentenceTransformers library not found. Semantic search will be disabled.")
            except Exception as e:
                self.logger.critical(f"Database initialization failed: {e}", exc_info=True)
                raise

    async def get_or_create_container(self, container_id: str, ttl: Optional[timedelta] = None) -> MemoryContainer:
        """Retrieves an existing MemoryContainer or creates a new one."""
        if container_id not in self.containers:
            async with self._db_lock:
                async with aiosqlite.connect(self.db_path) as db:
                    async with db.execute("SELECT id FROM containers WHERE id = ?", (container_id,)) as cursor:
                        if not await cursor.fetchone():
                            ttl_seconds = int(ttl.total_seconds()) if ttl else None
                            await db.execute(
                                "INSERT INTO containers (id, created_at, ttl_seconds) VALUES (?, ?, ?)",
                                (container_id, datetime.now(timezone.utc).isoformat(), ttl_seconds)
                            )
                            await db.commit()
                            self.logger.info(f"Created new persistent container: {container_id}")
            self.containers[container_id] = MemoryContainer(container_id, self, ttl)
        return self.containers[container_id]

    async def store_interaction(self, user_id: str, memory_id: str, content: Dict, metadata: Dict, persona_node_id: Optional[str] = None) -> str:
        """
        Specialized method to store interaction memories, with deep hologram integration.
        """
        container = await self.get_or_create_container(f"user_interactions_{user_id}")
        
        hologram_node_id = None
        if self.hologram:
            node = HolographicNode(
                node_id=memory_id,
                node_type="interaction_memory",
                data={**content, "user_id": user_id},
                metadata=metadata,
                encryption_level="aes256"
            )
            await self.hologram.store_node(node)
            hologram_node_id = node.node_id

            if persona_node_id:
                link = HolographicLink(
                    source_id=persona_node_id,
                    target_id=hologram_node_id,
                    link_type="memory_entanglement",
                    metadata={"strength": metadata.get("importance", 5) / 10.0}
                )
                await self.hologram.create_link(link)

        entry = MemoryEntry(
            id=memory_id,
            content=content,
            metadata=metadata,
            hologram_node_id=hologram_node_id
        )
        container.cache[memory_id] = entry
        await self._persist_entry(container.id, entry)
        self.logger.debug(f"Stored interaction {memory_id} for user {user_id}")
        return memory_id

    async def get_user_memory_count(self, user_id: str) -> int:
        """Counts all memories across all containers for a user."""
        count = 0
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(*) FROM memories WHERE container_id LIKE ?", (f"%_{user_id}",)) as cursor:
                row = await cursor.fetchone()
                if row:
                    count = row[0]
        return count

    async def get_recent_memories(self, user_id: str, limit: int = 10) -> List[MemoryEntry]:
        """Retrieves the most recently accessed memories for a user."""
        entries = []
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                query = """
                SELECT * FROM memories 
                WHERE container_id LIKE ? 
                ORDER BY last_accessed_at DESC 
                LIMIT ?
                """
                async with db.execute(query, (f"%_{user_id}", limit)) as cursor:
                    async for row in cursor:
                        entries.append(MemoryEntry.from_db_row(row))
        except Exception as e:
            self.logger.error(f"Failed to get recent memories for {user_id}: {e}")
        return entries

    async def search_semantic(self, user_id: str, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Performs semantic search over a user's memories."""
        if not self.semantic_model:
            self.logger.warning("Semantic search called but model is not available.")
            return []

        query_embedding = self.semantic_model.encode(query, convert_to_tensor=False)
        
        results = []
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                # Get all memory IDs for the user
                user_memory_ids_query = "SELECT id FROM memories WHERE container_id LIKE ?"
                async with db.execute(user_memory_ids_query, (f"%_{user_id}",)) as cursor:
                    user_memory_ids = {row['id'] for row in await cursor.fetchall()}

                if not user_memory_ids:
                    return []

                # Get all embeddings
                embeddings_query = "SELECT memory_id, embedding FROM memory_embeddings"
                async with db.execute(embeddings_query) as cursor:
                    all_embeddings = {row['memory_id']: np.frombuffer(row['embedding']) for row in await cursor.fetchall()}

            # Filter embeddings for the current user
            user_embeddings = {mid: emb for mid, emb in all_embeddings.items() if mid in user_memory_ids}
            
            if not user_embeddings:
                return []

            corpus_ids = list(user_embeddings.keys())
            corpus_embeddings = np.array(list(user_embeddings.values()))

            # Compute cosine similarities
            cosine_scores = util.cos_sim(query_embedding, corpus_embeddings)[0]
            top_results_indices = np.argsort(-cosine_scores)[:limit]

            # Fetch the full memory entries for the top results
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                for idx in top_results_indices:
                    memory_id = corpus_ids[idx]
                    score = cosine_scores[idx].item()
                    if score > 0.5: # Relevance threshold
                        async with db.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)) as cursor:
                            row = await cursor.fetchone()
                            if row:
                                entry = MemoryEntry.from_db_row(row)
                                results.append({"score": score, "memory": entry})
        except Exception as e:
            self.logger.error(f"Semantic search failed for user {user_id}: {e}", exc_info=True)

        return results

    async def store_key_value(self, user_id: str, key: str, value: Any):
        """Convenience method to store a simple key-value pair for a user."""
        container = await self.get_or_create_container(f"user_kv_store_{user_id}")
        await container.store(key, value)

    async def retrieve_key_value(self, user_id: str, key: str) -> Optional[Any]:
        """Convenience method to retrieve a value from the user's key-value store."""
        container = await self.get_or_create_container(f"user_kv_store_{user_id}")
        return await container.retrieve(key)

    async def search_pattern(self, user_id: str, pattern: str) -> List[str]:
        """Searches the user's key-value store for a pattern."""
        container = await self.get_or_create_container(f"user_kv_store_{user_id}")
        return await container.search_pattern(pattern)

    async def persist_all(self):
        """Persists all cached entries in all containers to the database."""
        self.logger.info("Persisting all memory containers to database...")
        for container in self.containers.values():
            try:
                async with self._db_lock:
                    async with aiosqlite.connect(self.db_path) as db:
                        for entry in container.cache.values():
                            await db.execute(
                                """INSERT OR REPLACE INTO memories VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                                entry.to_db_tuple(container.id)
                            )
                        await db.commit()
                self.logger.info(f"Successfully persisted container {container.id}")
            except Exception as e:
                self.logger.error(f"Failed to persist container {container.id}: {e}", exc_info=True)

    async def shutdown(self):
        """Gracefully shuts down the memory manager, persisting all data."""
        self.logger.info("Shutting down DualMemoryManager...")
        await self.persist_all()
        self.containers.clear()
        self.logger.info("DualMemoryManager shutdown complete.")

    # --- Internal DB Methods ---

    async def _persist_entry(self, container_id: str, entry: MemoryEntry):
        """Persists a single memory entry and its embedding to the database."""
        try:
            async with self._db_lock:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute(
                        """INSERT OR REPLACE INTO memories VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        entry.to_db_tuple(container_id)
                    )
                    
                    # Generate and store embedding
                    if self.semantic_model:
                        content_to_embed = ""
                        if isinstance(entry.content, dict):
                            content_to_embed = " ".join(str(v) for v in entry.content.values())
                        elif isinstance(entry.content, str):
                            content_to_embed = entry.content
                        
                        if content_to_embed:
                            embedding = self.semantic_model.encode(content_to_embed, convert_to_tensor=False)
                            await db.execute(
                                "INSERT OR REPLACE INTO memory_embeddings (memory_id, embedding) VALUES (?, ?)",
                                (entry.id, embedding.tobytes())
                            )
                    
                    await db.commit()
        except Exception as e:
            self.logger.error(f"Failed to persist entry {entry.id}: {e}", exc_info=True)

    async def _update_entry_access(self, entry: MemoryEntry):
        """Updates only the access time and count for an entry."""
        try:
            async with self._db_lock:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute(
                        "UPDATE memories SET last_accessed_at = ?, access_count = ? WHERE id = ?",
                        (entry.last_accessed_at.isoformat(), entry.access_count, entry.id)
                    )
                    await db.commit()
        except Exception as e:
            self.logger.error(f"Failed to update access for entry {entry.id}: {e}", exc_info=True)

    async def _delete_entry(self, entry_id: str):
        """Deletes a single entry and its embedding from the database."""
        try:
            async with self._db_lock:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute("DELETE FROM memories WHERE id = ?", (entry_id,))
                    await db.execute("DELETE FROM memory_embeddings WHERE memory_id = ?", (entry_id,))
                    await db.commit()
        except Exception as e:
            self.logger.error(f"Failed to delete entry {entry_id}: {e}", exc_info=True)

# Make the main class available for import, matching persona_core.py
MemoryManager = DualMemoryManager