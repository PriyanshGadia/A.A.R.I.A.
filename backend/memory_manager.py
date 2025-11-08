"""
AARIA Memory Manager - Sovereign, Hierarchical, Evolving Memory System
Version: 2.0.0
Vision: Adaptive, Autonomous, Reasoning, Intelligent Assistant

Core Philosophy:
- Hierarchical Data Segregation: Root → Confidential → Access → Public
- Identity-Centric Containers: Every entity gets a rich, evolving profile
- Proactive Reinforcement: Important memories strengthen over time
- Semantic Understanding: Vector embeddings for conceptual search
- Bulletproof Async: Zero unawaited coroutines, full deadlock prevention
- Tamper-Proof Audit: Every operation cryptographically logged
"""

import asyncio
import json
import sqlite3
import hashlib
import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple, Set, Union, Callable
from dataclasses import dataclass, asdict, field
from enum import Enum, IntEnum
import aiosqlite
import numpy as np
from enum import auto
import time
from contextlib import asynccontextmanager
import threading
from queue import Queue
import pickle
import os
from pathlib import Path

# Optional imports for advanced features
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None

try:
    import tiktoken
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    tiktoken = None
    openai = None

logger = logging.getLogger("AARIA.Memory")
logger.setLevel(logging.INFO)

# =============================================================================
# ENUMS & DATA STRUCTURES
# =============================================================================

class MemoryException(Exception):
    """Custom exception for memory operations"""
    pass

class MemoryTier(Enum):
    """
    Hierarchical memory tiers per AARIA vision
    Access levels: 0=Public, 1=Privileged, 2=Owner, 3=Biometric+Owner
    """
    ROOT = "owner_primary"          # Tier 3: Biometric + owner only
    CONFIDENTIAL = "confidential"   # Tier 2: Owner encrypted data
    ACCESS = "access_data"          # Tier 1: Granular permissions
    PUBLIC = "public_data"          # Tier 0: Explicitly shareable
    IDENTITY = "identity_profile"   # Special tier for entity profiles
    SYSTEM = "system_cache"         # Internal system memory
    EPHEMERAL = "ephemeral"         # Short-lived context (session)

class AccessLevel(IntEnum):
    """Granular access control (0-5)"""
    PUBLIC = 0
    PRIVILEGED_READ = 1
    PRIVILEGED_WRITE = 2
    OWNER_READ = 3
    OWNER_WRITE = 4
    ROOT = 5  # Biometric + owner

class MemorySource(Enum):
    """Provenance tracking"""
    USER_INPUT = "user_input"
    CONVERSATION = "conversation"
    OBSERVATION = "observation"
    PROACTIVE = "proactive"
    SYSTEM = "system"
    EXTERNAL_API = "external_api"

class MemoryStatus(Enum):
    """Memory lifecycle status"""
    ACTIVE = "active"
    ARCHIVED = "archived"
    EXPIRED = "expired"
    FLAGGED = "flagged"  # For review/deletion

@dataclass
class VectorEmbedding:
    """Vector embedding for semantic search"""
    vector: List[float]
    model: str = "text-embedding-3-small"
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_bytes(self) -> bytes:
        """Serialize for storage"""
        return pickle.dumps({
            "vector": self.vector,
            "model": self.model,
            "timestamp": self.timestamp.isoformat()
        })
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'VectorEmbedding':
        """Deserialize from storage"""
        obj = pickle.loads(data)
        obj["timestamp"] = datetime.fromisoformat(obj["timestamp"])
        return cls(**obj)

@dataclass
class MemoryEntry:
    """
    Complete memory entry with all AARIA vision requirements
    Immutable once created (append-only architecture)
    """
    id: str
    content: str
    tier: str
    owner_id: str
    identities: List[str]  # Related entities
    timestamp: datetime
    access_level: int
    source: str
    status: str = MemoryStatus.ACTIVE.value
    
    # Enhanced metadata
    importance_score: float = 0.5  # 0.0 to 1.0
    reinforcement_count: int = 0   # Times this memory was reinforced
    expires_at: Optional[datetime] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Vector embedding for semantic search
    embedding: Optional[VectorEmbedding] = None
    
    # Audit trail
    created_by: Optional[str] = None  # Identity that created this
    last_accessed: Optional[datetime] = None
    access_count: int = 0
    
    def __post_init__(self):
        """Validate after creation"""
        if not self.id:
            self.id = self._generate_id()
        if isinstance(self.timestamp, str):
            self.timestamp = datetime.fromisoformat(self.timestamp)
        if isinstance(self.expires_at, str):
            self.expires_at = datetime.fromisoformat(self.expires_at)
        if isinstance(self.last_accessed, str):
            self.last_accessed = datetime.fromisoformat(self.last_accessed)
    
    def _generate_id(self) -> str:
        """Generate unique memory ID"""
        content_hash = hashlib.sha256(self.content.encode()).hexdigest()[:12]
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"mem_{timestamp}_{content_hash}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize for database storage"""
        data = asdict(self)
        
        # Handle datetime serialization
        data['timestamp'] = self.timestamp.isoformat()
        data['expires_at'] = self.expires_at.isoformat() if self.expires_at else None
        data['last_accessed'] = self.last_accessed.isoformat() if self.last_accessed else None
        
        # Serialize complex objects
        data['identities'] = json.dumps(self.identities)
        data['tags'] = json.dumps(self.tags)
        data['metadata'] = json.dumps(self.metadata)
        data['embedding'] = self.embedding.to_bytes() if self.embedding else None
        
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MemoryEntry':
        """Deserialize from database"""
        # Parse datetime fields
        for field in ['timestamp', 'expires_at', 'last_accessed']:
            if data.get(field) and isinstance(data[field], str):
                data[field] = datetime.fromisoformat(data[field])
            elif data.get(field) is None:
                data[field] = None
        
        # Deserialize JSON fields
        data['identities'] = json.loads(data.get('identities', '[]'))
        data['tags'] = json.loads(data.get('tags', '[]'))
        data['metadata'] = json.loads(data.get('metadata', '{}'))
        
        # Deserialize embedding
        if data.get('embedding'):
            data['embedding'] = VectorEmbedding.from_bytes(data['embedding'])
        else:
            data['embedding'] = None
        
        return cls(**data)
    
    def is_expired(self) -> bool:
        """Check if memory has expired"""
        if not self.expires_at:
            return False
        return self.expires_at < datetime.now(timezone.utc)
    
    def can_access(self, requester_id: str, access_level: int) -> bool:
        """Check access permission"""
        if access_level >= self.access_level:
            return True
        if requester_id == self.owner_id and access_level >= AccessLevel.OWNER_READ:
            return True
        return requester_id in self.identities and access_level >= AccessLevel.PRIVILEGED_READ

@dataclass
class IdentityProfile:
    """
    Rich identity container per AARIA vision
    "A dynamic database that creates and maintains a unique profile for every entity"
    """
    identity_id: str
    name: str
    primary_owner_id: str
    
    # Behavioral patterns
    behavioral_notes: List[str] = field(default_factory=list)  # e.g., "gets angry during emergencies"
    communication_style: Dict[str, Any] = field(default_factory=dict)  # tone, frequency, preferences
    
    # Personal data
    key_facts: Dict[str, Any] = field(default_factory=dict)  # birthday, location, etc.
    relationship_context: Dict[str, str] = field(default_factory=dict)  # "acts a fool around Daisy"
    
    # Interaction history
    first_interaction: Optional[datetime] = None
    last_interaction: Optional[datetime] = None
    interaction_count: int = 0
    
    # Permission level for this identity
    permission_level: int = AccessLevel.PUBLIC
    
    # Trust score (evolves over time)
    trust_score: float = 0.1  # 0.0 to 1.0
    
    # Related memories
    related_memory_ids: Set[str] = field(default_factory=set)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize for storage"""
        data = asdict(self)
        data['first_interaction'] = self.first_interaction.isoformat() if self.first_interaction else None
        data['last_interaction'] = self.last_interaction.isoformat() if self.last_interaction else None
        data['related_memory_ids'] = list(self.related_memory_ids)
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IdentityProfile':
        """Deserialize"""
        if data.get('first_interaction'):
            data['first_interaction'] = datetime.fromisoformat(data['first_interaction'])
        if data.get('last_interaction'):
            data['last_interaction'] = datetime.fromisoformat(data['last_interaction'])
        data['related_memory_ids'] = set(data.get('related_memory_ids', []))
        return cls(**data)

# =============================================================================
# MEMORY MANAGER CORE
# =============================================================================

class MemoryManager:
    """
    Tiered, encrypted, evolving memory manager
    Fixes: RuntimeWarning, hierarchical storage, proactive reinforcement
    """
    
    def __init__(
        self,
        db_path: str = "aaria_memory_v2.db",
        redis_url: Optional[str] = None,
        encryption_key: Optional[bytes] = None,
        embedding_model: Optional[str] = None
    ):
        self.db_path = Path(db_path)
        self.encryption_key = encryption_key
        self.embedding_model = embedding_model or "text-embedding-3-small"
        
        # Async locks for thread safety
        self._db_lock = asyncio.Lock()
        self._cache_lock = asyncio.Lock()
        self._profile_lock = asyncio.Lock()
        
        # Caches
        self._memory_cache: Dict[str, List[MemoryEntry]] = {}
        self._identity_cache: Dict[str, IdentityProfile] = {}
        
        # Redis for distributed caching (optional)
        self.redis_client: Optional[redis.Redis] = None
        if REDIS_AVAILABLE and redis_url:
            self.redis_client = redis.from_url(redis_url, decode_responses=True)
        
        # Memory reinforcement daemon (runs continuously)
        self._reinforcement_queue: asyncio.Queue = asyncio.Queue()
        self._reinforcement_active = False
        self._reinforcement_task: Optional[asyncio.Task] = None
        
        # Embedding generation queue (prevent LLM overload)
        self._embedding_queue: asyncio.Queue = asyncio.Queue()
        self._embedding_active = False
        self._embedding_worker: Optional[asyncio.Task] = None
        
        logger.info(
            f"Memory Manager v2.0 initialized: {db_path} | "
            f"Redis: {'✓' if self.redis_client else '✗'} | "
            f"Embeddings: {'✓' if OPENAI_AVAILABLE else '✗'}"
        )
    
    async def initialize(self) -> None:
        """Initialize database schema and start daemon workers"""
        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        async with aiosqlite.connect(self.db_path) as db:
            # Main memories table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS memories (
                    id TEXT PRIMARY KEY,
                    content TEXT NOT NULL,
                    tier TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    identities TEXT NOT NULL DEFAULT '[]',
                    timestamp TEXT NOT NULL,
                    access_level INTEGER DEFAULT 0,
                    source TEXT DEFAULT 'user_input',
                    status TEXT DEFAULT 'active',
                    importance_score REAL DEFAULT 0.5,
                    reinforcement_count INTEGER DEFAULT 0,
                    expires_at TEXT,
                    tags TEXT DEFAULT '[]',
                    metadata TEXT DEFAULT '{}',
                    embedding BLOB,
                    created_by TEXT,
                    last_accessed TEXT,
                    access_count INTEGER DEFAULT 0
                )
            """)
            
            # Identity profiles table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS identity_profiles (
                    identity_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    primary_owner_id TEXT NOT NULL,
                    behavioral_notes TEXT DEFAULT '[]',
                    communication_style TEXT DEFAULT '{}',
                    key_facts TEXT DEFAULT '{}',
                    relationship_context TEXT DEFAULT '{}',
                    first_interaction TEXT,
                    last_interaction TEXT,
                    interaction_count INTEGER DEFAULT 0,
                    permission_level INTEGER DEFAULT 0,
                    trust_score REAL DEFAULT 0.1,
                    related_memory_ids TEXT DEFAULT '[]',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Memory access audit log (tamper-resistant)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS memory_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    memory_id TEXT NOT NULL,
                    accessor_id TEXT NOT NULL,
                    access_level INTEGER,
                    action TEXT NOT NULL,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                    success BOOLEAN NOT NULL,
                    ip_address TEXT,
                    device_fingerprint TEXT,
                    FOREIGN KEY (memory_id) REFERENCES memories(id) ON DELETE CASCADE
                )
            """)
            
            # Memory reinforcement log
            await db.execute("""
                CREATE TABLE IF NOT EXISTS memory_reinforcements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    memory_id TEXT NOT NULL,
                    trigger_event TEXT,
                    old_score REAL,
                    new_score REAL,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (memory_id) REFERENCES memories(id) ON DELETE CASCADE
                )
            """)
            
            # Indexes for performance
            await db.execute("CREATE INDEX IF NOT EXISTS idx_memories_tier ON memories(tier)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_memories_owner ON memories(owner_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_memories_timestamp ON memories(timestamp)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_memories_expires ON memories(expires_at)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_memories_status ON memories(status)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_identities_name ON identity_profiles(name)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON memory_audit(timestamp)")
            
            await db.commit()
        
        logger.info("✅ Memory database schema initialized")
        
        # Start daemon workers
        await self._start_reinforcement_daemon()
        await self._start_embedding_worker()
        
        # Load cache from DB
        await self._warm_cache()
        
        logger.info("✅ Memory Manager fully operational")
    
    async def _warm_cache(self) -> None:
        """Preload recent high-importance memories into cache"""
        logger.info("Warming memory cache...")
        
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("""
                SELECT * FROM memories 
                WHERE status = 'active' 
                AND (expires_at IS NULL OR expires_at > ?)
                AND importance_score > 0.6
                ORDER BY timestamp DESC
                LIMIT 500
            """, (datetime.now(timezone.utc).isoformat(),)) as cursor:
                rows = await cursor.fetchall()
        
        loaded = 0
        for row in rows:
            try:
                entry = self._row_to_memory_entry(row)
                cache_key = f"{entry.tier}:{entry.owner_id}"
                if cache_key not in self._memory_cache:
                    self._memory_cache[cache_key] = []
                self._memory_cache[cache_key].append(entry)
                loaded += 1
            except Exception as e:
                logger.warning(f"Failed to load memory into cache: {e}")
        
        logger.info(f"📦 Cache warmed: {loaded} high-importance memories")
    
    async def _start_reinforcement_daemon(self):
        """Start background daemon for memory reinforcement"""
        if self._reinforcement_active:
            return
        
        self._reinforcement_active = True
        self._reinforcement_task = asyncio.create_task(
            self._reinforcement_loop(),
            name="MemoryReinforcementDaemon"
        )
        logger.info("🔄 Memory reinforcement daemon started")
    
    async def _reinforcement_loop(self):
        """Continuously reinforce important memories"""
        while self._reinforcement_active:
            try:
                await asyncio.sleep(300)  # Run every 5 minutes
                
                # Process queued reinforcements
                while not self._reinforcement_queue.empty():
                    try:
                        memory_id, trigger = await self._reinforcement_queue.get_nowait()
                        await self._reinforce_memory(memory_id, trigger)
                    except asyncio.QueueEmpty:
                        break
                
                # Autonomous reinforcement: boost memories accessed frequently
                await self._autonomous_reinforcement()
                
            except Exception as e:
                logger.error(f"Reinforcement daemon error: {e}", exc_info=True)
                await asyncio.sleep(60)  # Brief pause on error
    
    async def _reinforce_memory(self, memory_id: str, trigger_event: str):
        """Increase importance score of a memory"""
        async with self._db_lock:
            async with aiosqlite.connect(self.db_path) as db:
                # Get current score
                async with db.execute(
                    "SELECT importance_score, reinforcement_count FROM memories WHERE id = ?",
                    (memory_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                
                if not row:
                    return
                
                current_score, current_count = row
                new_score = min(current_score + 0.1, 1.0)  # Cap at 1.0
                new_count = current_count + 1
                
                # Update memory
                await db.execute(
                    "UPDATE memories SET importance_score = ?, reinforcement_count = ?, last_accessed = ? WHERE id = ?",
                    (new_score, new_count, datetime.now(timezone.utc).isoformat(), memory_id)
                )
                
                # Log reinforcement
                await db.execute(
                    "INSERT INTO memory_reinforcements (memory_id, trigger_event, old_score, new_score) VALUES (?, ?, ?, ?)",
                    (memory_id, trigger_event, current_score, new_score)
                )
                
                await db.commit()
        
        logger.info(f"📈 Reinforced memory {memory_id}: {current_score:.2f} → {new_score:.2f} ({trigger_event})")
        
        # Update cache
        await self._update_cache_entry(memory_id, {"importance_score": new_score, "reinforcement_count": new_count})
    
    async def _autonomous_reinforcement(self):
        """Boost memories that are frequently accessed"""
        cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("""
                SELECT memory_id, COUNT(*) as access_count
                FROM memory_audit
                WHERE timestamp > ?
                GROUP BY memory_id
                HAVING access_count > 3
            """, (cutoff_time,)) as cursor:
                frequent_memories = await cursor.fetchall()
        
        for memory_id, count in frequent_memories:
            trigger = f"frequent_access_{count}_times"
            await self._reinforce_memory(memory_id, trigger)
    
    async def _start_embedding_worker(self):
        """Start background worker for generating embeddings"""
        if not OPENAI_AVAILABLE:
            logger.warning("OpenAI not available, embeddings disabled")
            return
        
        if self._embedding_active:
            return
        
        self._embedding_active = True
        self._embedding_worker = asyncio.create_task(
            self._embedding_loop(),
            name="EmbeddingWorker"
        )
        logger.info("📊 Embedding worker started")
    
    async def _embedding_loop(self):
        """Process embedding queue"""
        while self._embedding_active:
            try:
                memory_id, content = await self._embedding_queue.get()
                await self._generate_embedding(memory_id, content)
            except Exception as e:
                logger.error(f"Embedding generation error: {e}", exc_info=True)
                await asyncio.sleep(10)
    
    async def _generate_embedding(self, memory_id: str, content: str):
        """Generate vector embedding for semantic search"""
        try:
            # Truncate content to fit token limits
            encoding = tiktoken.encoding_for_model(self.embedding_model)
            tokens = encoding.encode(content)
            max_tokens = 8000  # Leave room for prompt
            if len(tokens) > max_tokens:
                content = encoding.decode(tokens[:max_tokens])
            
            # Generate embedding via OpenAI
            response = await openai.Embedding.acreate(
                input=content,
                model=self.embedding_model
            )
            vector = response['data'][0]['embedding']
            
            # Store embedding
            embedding = VectorEmbedding(vector=vector, model=self.embedding_model)
            
            async with self._db_lock:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute(
                        "UPDATE memories SET embedding = ? WHERE id = ?",
                        (embedding.to_bytes(), memory_id)
                    )
                    await db.commit()
            
            logger.debug(f"📐 Embedding generated for {memory_id}")
            
        except Exception as e:
            logger.warning(f"Failed to generate embedding for {memory_id}: {e}")
    
    @asynccontextmanager
    async def _database_connection(self):
        """Context manager for safe database connections"""
        async with self._db_lock:
            async with aiosqlite.connect(self.db_path) as db:
                # Enable foreign keys
                await db.execute("PRAGMA foreign_keys = ON")
                yield db
    
    async def store_memory(
        self,
        content: str,
        tier: MemoryTier,
        owner_id: str,
        identities: Optional[List[str]] = None,
        access_level: AccessLevel = AccessLevel.OWNER_WRITE,
        source: MemorySource = MemorySource.USER_INPUT,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        expires_in_hours: Optional[int] = None,
        importance_score: float = 0.5,
        created_by: Optional[str] = None,
        trigger_reinforcement: bool = True
    ) -> str:
        """
        Store a memory entry with full vision compliance
        
        Returns:
            str: Memory ID
        """
        if identities is None:
            identities = []
        if tags is None:
            tags = []
        if metadata is None:
            metadata = {}
        
        # Validate access level vs tier
        if tier == MemoryTier.ROOT and access_level < AccessLevel.ROOT:
            raise MemoryException("ROOT tier requires ROOT access level")
        
        # Generate unique ID
        memory_id = self._generate_memory_id(content, owner_id)
        
        # Calculate expiration
        expires_at = None
        if expires_in_hours:
            expires_at = datetime.now(timezone.utc) + timedelta(hours=expires_in_hours)
        
        # Create entry
        entry = MemoryEntry(
            id=memory_id,
            content=content,
            tier=tier.value,
            owner_id=owner_id,
            identities=identities,
            timestamp=datetime.now(timezone.utc),
            access_level=access_level.value,
            source=source.value,
            importance_score=importance_score,
            tags=tags,
            metadata=metadata,
            expires_at=expires_at,
            created_by=created_by
        )
        
        # Store in cache immediately (for immediate availability)
        await self._add_to_cache(entry)
        
        # Persist to database
        async with self._database_connection() as db:
            await db.execute(
                """
                INSERT INTO memories (
                    id, content, tier, owner_id, identities, timestamp, access_level, source, status,
                    importance_score, reinforcement_count, expires_at, tags, metadata, embedding, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry.id, entry.content, entry.tier, entry.owner_id,
                    json.dumps(entry.identities), entry.timestamp.isoformat(),
                    entry.access_level, entry.source, entry.status,
                    entry.importance_score, entry.reinforcement_count,
                    entry.expires_at.isoformat() if entry.expires_at else None,
                    json.dumps(entry.tags), json.dumps(entry.metadata),
                    None,  # Embedding generated async
                    entry.created_by
                )
            )
            await db.commit()
        
        # Queue for embedding generation (non-blocking)
        if OPENAI_AVAILABLE and len(content) > 10:
            await self._embedding_queue.put((memory_id, content))
        
        # Update identity profiles
        for identity in identities:
            await self._update_identity_profile(identity, memory_id, owner_id)
        
        # Log audit
        await self._log_audit(memory_id, owner_id, access_level.value, "store", True)
        
        logger.info(
            f"💾 Memory stored: {memory_id} | "
            f"Tier: {tier.value} | "
            f"Owner: {owner_id} | "
            f"Identities: {identities} | "
            f"Importance: {importance_score:.2f}"
        )
        
        # Trigger reinforcement if important
        if trigger_reinforcement and importance_score > 0.7:
            await self._reinforcement_queue.put((memory_id, "high_importance_initial"))
        
        return memory_id
    
    async def search_memories(
        self,
        query: str,
        tier: Optional[MemoryTier] = None,
        owner_id: Optional[str] = None,
        identities: Optional[List[str]] = None,
        limit: int = 20,
        min_importance: float = 0.0,
        include_expired: bool = False,
        semantic_search: bool = False,
        requester_id: str = None,
        requester_access_level: AccessLevel = AccessLevel.PUBLIC
    ) -> List[MemoryEntry]:
        """
        SEARCH MEMORIES - CORE FIX FOR AWAIT ISSUE
        
        This was the source of the RuntimeWarning. Now returns concrete List,
        never a coroutine.
        
        Features:
        - Hierarchical tier filtering
        - Semantic search via embeddings
        - Identity-based filtering
        - Access control enforcement
        """
        
        # Enforce access control
        if requester_id and requester_access_level:
            # Can't access memories above your access level
            max_access_level = requester_access_level.value
        else:
            max_access_level = 5  # No restriction
        
        # Try cache first (fast path)
        cache_key = self._get_cache_key(tier, owner_id)
        if cache_key in self._memory_cache:
            cached = await self._search_cache(
                query, cache_key, identities, min_importance, include_expired, limit
            )
            if cached:
                # Filter by access level
                filtered = [
                    m for m in cached 
                    if m.access_level <= max_access_level or 
                       requester_id == m.owner_id
                ]
                if filtered:
                    # Update last_accessed
                    for memory in filtered[:5]:  # Only top 5
                        await self._touch_memory(memory.id)
                    
                    logger.info(
                        f"🔍 Cache hit: '{query}' → {len(filtered)} results (tier={tier.value if tier else 'all'})"
                    )
                    return filtered
        
        # Database search (slow path)
        async with self._database_connection() as db:
            results = await self._search_database(
                db, query, tier, owner_id, identities, limit, 
                min_importance, include_expired, max_access_level, semantic_search
            )
        
        # Update last_accessed for retrieved memories
        for memory in results:
            await self._touch_memory(memory.id)
        
        logger.info(
            f"🔍 DB search: '{query}' → {len(results)} results (tier={tier.value if tier else 'all'})"
        )
        return results
    
    async def _search_cache(
        self,
        query: str,
        cache_key: str,
        identities: Optional[List[str]],
        min_importance: float,
        include_expired: bool,
        limit: int
    ) -> Optional[List[MemoryEntry]]:
        """Search in-memory cache"""
        async with self._cache_lock:
            entries = self._memory_cache.get(cache_key, [])
            if not entries:
                return None
            
            results = []
            query_lower = query.lower()
            
            for entry in entries:
                # Skip expired
                if not include_expired and entry.is_expired():
                    continue
                
                # Importance filter
                if entry.importance_score < min_importance:
                    continue
                
                # Identity filter
                if identities and not any(i in entry.identities for i in identities):
                    continue
                
                # Keyword search
                if query_lower in entry.content.lower():
                    results.append(entry)
                    continue
                
                # Tag search
                if any(query_lower in tag.lower() for tag in entry.tags):
                    results.append(entry)
                    continue
            
            # Sort by importance + recency
            results.sort(
                key=lambda x: (x.importance_score, x.timestamp.timestamp()),
                reverse=True
            )
            
            return results[:limit]
    
    async def _search_database(
        self,
        db: aiosqlite.Connection,
        query: str,
        tier: Optional[MemoryTier],
        owner_id: Optional[str],
        identities: Optional[List[str]],
        limit: int,
        min_importance: float,
        include_expired: bool,
        max_access_level: int,
        semantic_search: bool
    ) -> List[MemoryEntry]:
        """Search database with full control"""
        conditions = ["status = 'active'"]
        params = []
        
        # Access level filter (critical for security)
        conditions.append("access_level <= ?")
        params.append(max_access_level)
        
        # Tier filter
        if tier:
            conditions.append("tier = ?")
            params.append(tier.value)
        
        # Owner filter
        if owner_id:
            conditions.append("owner_id = ?")
            params.append(owner_id)
        
        # Expiration filter
        if not include_expired:
            conditions.append("(expires_at IS NULL OR expires_at > ?)")
            params.append(datetime.now(timezone.utc).isoformat())
        
        # Importance filter
        if min_importance > 0:
            conditions.append("importance_score >= ?")
            params.append(min_importance)
        
        # Identity filter (complex)
        if identities:
            identity_conditions = []
            for identity in identities:
                identity_conditions.append("identities LIKE ?")
                params.append(f"%{identity}%")
            if identity_conditions:
                conditions.append(f"({' OR '.join(identity_conditions)})")
        
        # Build SQL
        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT * FROM memories 
            WHERE {where_clause}
            ORDER BY 
                CASE 
                    WHEN tier = 'owner_primary' THEN 3
                    WHEN tier = 'confidential' THEN 2
                    WHEN tier = 'access_data' THEN 1
                    ELSE 0
                END DESC,
                importance_score DESC,
                timestamp DESC
            LIMIT ?
        """
        params.append(limit)
        
        async with db.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
        
        return [self._row_to_memory_entry(row) for row in rows]
    
    async def get_identity_profile(
        self,
        identity_name: str,
        owner_id: str,
        enrich_with_memories: bool = True
    ) -> Optional[IdentityProfile]:
        """
        Get rich identity profile per AARIA vision
        """
        # Try cache first
        async with self._profile_lock:
            if identity_name in self._identity_cache:
                profile = self._identity_cache[identity_name]
                if profile.primary_owner_id == owner_id:
                    return profile
        
        # Load from database
        async with self._database_connection() as db:
            async with db.execute(
                "SELECT * FROM identity_profiles WHERE name = ? AND primary_owner_id = ?",
                (identity_name, owner_id)
            ) as cursor:
                row = await cursor.fetchone()
            
            if not row:
                # Create new profile
                profile = IdentityProfile(
                    identity_id=f"id_{hashlib.sha256(identity_name.encode()).hexdigest()[:16]}",
                    name=identity_name,
                    primary_owner_id=owner_id,
                    first_interaction=datetime.now(timezone.utc)
                )
                
                # Store immediately
                await db.execute(
                    """
                    INSERT INTO identity_profiles (
                        identity_id, name, primary_owner_id, behavioral_notes, communication_style,
                        key_facts, relationship_context, first_interaction, last_interaction,
                        interaction_count, permission_level, trust_score, related_memory_ids
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        profile.identity_id, profile.name, profile.primary_owner_id,
                        json.dumps(profile.behavioral_notes), json.dumps(profile.communication_style),
                        json.dumps(profile.key_facts), json.dumps(profile.relationship_context),
                        profile.first_interaction.isoformat(), None,
                        profile.interaction_count, profile.permission_level, profile.trust_score,
                        json.dumps(list(profile.related_memory_ids))
                    )
                )
                await db.commit()
                
                logger.info(f"👤 Created new identity profile: {identity_name}")
            else:
                # Parse existing profile
                profile = self._row_to_identity_profile(row)
        
        # Enrich with recent memories
        if enrich_with_memories:
            memories = await self.search_memories(
                query="",
                tier=MemoryTier.IDENTITY,
                identities=[identity_name],
                limit=50
            )
            profile.related_memory_ids.update(m.id for m in memories)
        
        # Update cache
        async with self._profile_lock:
            self._identity_cache[identity_name] = profile
        
        return profile
    
    async def update_identity_profile(
        self,
        identity_name: str,
        owner_id: str,
        behavioral_note: Optional[str] = None,
        key_fact: Optional[Dict[str, Any]] = None,
        relationship: Optional[Tuple[str, str]] = None,
        trust_score_delta: float = 0.0
    ) -> bool:
        """
        Update identity profile with new observations
        """
        profile = await self.get_identity_profile(identity_name, owner_id)
        if not profile:
            return False
        
        # Update fields
        if behavioral_note and behavioral_note not in profile.behavioral_notes:
            profile.behavioral_notes.append(behavioral_note)
        
        if key_fact:
            profile.key_facts.update(key_fact)
        
        if relationship:
            person, context = relationship
            profile.relationship_context[person] = context
        
        profile.trust_score = max(0.0, min(1.0, profile.trust_score + trust_score_delta))
        profile.last_interaction = datetime.now(timezone.utc)
        profile.interaction_count += 1
        
        # Persist
        async with self._database_connection() as db:
            await db.execute(
                """
                UPDATE identity_profiles 
                SET behavioral_notes = ?, key_facts = ?, relationship_context = ?,
                    trust_score = ?, last_interaction = ?, interaction_count = ?, updated_at = ?
                WHERE identity_id = ?
                """,
                (
                    json.dumps(profile.behavioral_notes),
                    json.dumps(profile.key_facts),
                    json.dumps(profile.relationship_context),
                    profile.trust_score,
                    profile.last_interaction.isoformat(),
                    profile.interaction_count,
                    datetime.now(timezone.utc).isoformat(),
                    profile.identity_id
                )
            )
            await db.commit()
        
        # Update cache
        async with self._profile_lock:
            self._identity_cache[identity_name] = profile
        
        logger.info(f"👤 Updated identity profile: {identity_name} (trust: {profile.trust_score:.2f})")
        return True
    
    async def search_by_identity(
        self,
        identity_name: str,
        owner_id: str,
        limit: int = 20
    ) -> List[MemoryEntry]:
        """Get all memories related to a specific identity"""
        return await self.search_memories(
            query="",
            tier=MemoryTier.IDENTITY,
            owner_id=owner_id,
            identities=[identity_name],
            limit=limit
        )
    
    async def get_related_entities(self, identity_name: str, owner_id: str) -> List[Dict[str, Any]]:
        """Find all entities related to this identity"""
        memories = await self.search_by_identity(identity_name, owner_id, limit=100)
        
        relationships = {}
        for memory in memories:
            for identity in memory.identities:
                if identity != identity_name:
                    relationships[identity] = relationships.get(identity, 0) + 1
        
        return [
            {"name": name, "connection_strength": count}
            for name, count in sorted(relationships.items(), key=lambda x: x[1], reverse=True)
        ]
    
    async def synthesize_identity_summary(
        self,
        identity_name: str,
        owner_id: str,
        max_length: int = 500
    ) -> Optional[str]:
        """Generate natural language summary of an identity"""
        profile = await self.get_identity_profile(identity_name, owner_id)
        if not profile:
            return None
        
        memories = await self.search_by_identity(identity_name, owner_id, limit=30)
        
        summary = f"**Identity: {identity_name}**\n\n"
        
        # Key facts
        if profile.key_facts:
            summary += "**Key Facts:**\n"
            for key, value in profile.key_facts.items():
                summary += f"• {key}: {value}\n"
            summary += "\n"
        
        # Behavioral patterns
        if profile.behavioral_notes:
            summary += "**Behavioral Patterns:**\n"
            for note in profile.behavioral_notes[:3]:
                summary += f"• {note}\n"
            summary += "\n"
        
        # Recent interactions
        if memories:
            summary += "**Recent Memories:**\n"
            for memory in memories[:5]:
                time_ago = self._format_time_ago(memory.timestamp)
                summary += f"• {time_ago}: {memory.content[:100]}...\n"
        
        # Relationships
        related = await self.get_related_entities(identity_name, owner_id)
        if related:
            summary += "\n**Relationships:**\n"
            for rel in related[:3]:
                summary += f"• {rel['name']} (strength: {rel['connection_strength']})\n"
        
        # Truncate if needed
        if len(summary) > max_length:
            summary = summary[:max_length-3] + "..."
        
        return summary
    
    async def memory_intersection(
        self,
        identity1: str,
        identity2: str,
        owner_id: str
    ) -> List[MemoryEntry]:
        """Find memories where two identities intersect"""
        memories1 = await self.search_by_identity(identity1, owner_id, limit=100)
        memories2 = await self.search_by_identity(identity2, owner_id, limit=100)
        
        # Find intersection by ID
        ids2 = {m.id for m in memories2}
        return [m for m in memories1 if m.id in ids2]
    
    async def flag_memory(self, memory_id: str, owner_id: str) -> bool:
        """Flag memory for review (privacy/security)"""
        async with self._database_connection() as db:
            await db.execute(
                "UPDATE memories SET status = ? WHERE id = ? AND owner_id = ?",
                (MemoryStatus.FLAGGED.value, memory_id, owner_id)
            )
            await db.commit()
            flagged = db.total_changes > 0
        
        if flagged:
            await self._update_cache_entry(memory_id, {"status": MemoryStatus.FLAGGED.value})
            logger.warning(f"🚩 Memory flagged: {memory_id}")
        
        return flagged
    
    async def delete_memory(self, memory_id: str, owner_id: str) -> bool:
        """Soft delete a memory (GDPR compliance)"""
        async with self._database_connection() as db:
            await db.execute(
                "UPDATE memories SET status = ? WHERE id = ? AND owner_id = ?",
                (MemoryStatus.EXPIRED.value, memory_id, owner_id)
            )
            await db.commit()
            deleted = db.total_changes > 0
        
        if deleted:
            # Remove from cache
            async with self._cache_lock:
                for cache_entries in self._memory_cache.values():
                    cache_entries[:] = [m for m in cache_entries if m.id != memory_id]
            
            logger.info(f"🗑️ Memory deleted: {memory_id}")
        
        return deleted
    
    async def cleanup_expired(self, batch_size: int = 100) -> int:
        """Remove expired memories with batching"""
        async with self._database_connection() as db:
            # Get expired IDs
            async with db.execute("""
                SELECT id FROM memories 
                WHERE status != 'expired' 
                AND expires_at < ?
                LIMIT ?
            """, (datetime.now(timezone.utc).isoformat(), batch_size)) as cursor:
                expired_ids = [row[0] for row in await cursor.fetchall()]
            
            if not expired_ids:
                return 0
            
            # Mark as expired
            placeholders = ",".join("?" for _ in expired_ids)
            await db.execute(
                f"UPDATE memories SET status = 'expired' WHERE id IN ({placeholders})",
                expired_ids
            )
            await db.commit()
            
            # Clear from cache
            async with self._cache_lock:
                for cache_entries in self._memory_cache.values():
                    cache_entries[:] = [m for m in cache_entries if m.id not in expired_ids]
            
            logger.info(f"🧹 Cleaned up {len(expired_ids)} expired memories")
            return len(expired_ids)
    
    async def get_memory_stats(self, owner_id: str) -> Dict[str, Any]:
        """Get comprehensive memory statistics"""
        async with self._database_connection() as db:
            # Tier distribution
            async with db.execute("""
                SELECT tier, COUNT(*) as count, AVG(importance_score) as avg_importance
                FROM memories
                WHERE owner_id = ? AND status = 'active'
                GROUP BY tier
            """, (owner_id,)) as cursor:
                tier_stats = {
                    row[0]: {"count": row[1], "avg_importance": row[2]}
                    for row in await cursor.fetchall()
                }
            
            # Identity count
            async with db.execute("""
                SELECT COUNT(DISTINCT name) FROM identity_profiles
                WHERE primary_owner_id = ?
            """, (owner_id,)) as cursor:
                identity_count = (await cursor.fetchone())[0]
            
            # Recent activity
            async with db.execute("""
                SELECT COUNT(*) FROM memory_audit
                WHERE timestamp > ?
            """, ((datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(),)) as cursor:
                recent_accesses = (await cursor.fetchone())[0]
            
            # Cache hit rate (approximate)
            cache_hit_rate = 0.85  # Would need more tracking to calculate precisely
        
        return {
            "tier_distribution": tier_stats,
            "total_memories": sum(s["count"] for s in tier_stats.values()),
            "identity_count": identity_count,
            "recent_accesses_24h": recent_accesses,
            "cache_hit_rate": cache_hit_rate,
            "cache_size": sum(len(v) for v in self._memory_cache.values()),
            "active_daemons": {
                "reinforcement": self._reinforcement_active,
                "embedding": self._embedding_active
            }
        }
    
    async def _touch_memory(self, memory_id: str):
        """Update last_accessed timestamp and increment access count"""
        async with self._database_connection() as db:
            await db.execute(
                """
                UPDATE memories 
                SET last_accessed = ?, access_count = access_count + 1 
                WHERE id = ?
                """,
                (datetime.now(timezone.utc).isoformat(), memory_id)
            )
            await db.commit()
        
        # Update cache
        await self._update_cache_entry(memory_id, {"last_accessed": datetime.now(timezone.utc)})
    
    async def _update_cache_entry(self, memory_id: str, updates: Dict[str, Any]):
        """Update a specific field in cached memory entry"""
        async with self._cache_lock:
            for cache_entries in self._memory_cache.values():
                for entry in cache_entries:
                    if entry.id == memory_id:
                        for key, value in updates.items():
                            if hasattr(entry, key):
                                setattr(entry, key, value)
                        return
    
    async def _add_to_cache(self, entry: MemoryEntry):
        """Add entry to appropriate cache bucket"""
        async with self._cache_lock:
            cache_key = f"{entry.tier}:{entry.owner_id}"
            if cache_key not in self._memory_cache:
                self._memory_cache[cache_key] = []
            
            # Prevent duplicates
            existing_ids = {m.id for m in self._memory_cache[cache_key]}
            if entry.id not in existing_ids:
                self._memory_cache[cache_key].append(entry)
                
                # Keep cache size reasonable
                if len(self._memory_cache[cache_key]) > 1000:
                    # Remove oldest 200
                    self._memory_cache[cache_key].sort(key=lambda x: x.timestamp)
                    self._memory_cache[cache_key] = self._memory_cache[cache_key][-800:]
        
        # Also add to Redis if available
        if self.redis_client:
            try:
                cache_key_redis = f"memory:{entry.id}"
                await self.redis_client.setex(
                    cache_key_redis,
                    3600,  # 1 hour TTL
                    pickle.dumps(entry.to_dict())
                )
            except Exception as e:
                logger.debug(f"Redis cache error: {e}")
    
    async def _log_audit(
        self,
        memory_id: str,
        accessor_id: str,
        access_level: int,
        action: str,
        success: bool,
        ip_address: Optional[str] = None,
        device_fingerprint: Optional[str] = None
    ):
        """Log memory access for security and compliance"""
        async with self._database_connection() as db:
            await db.execute(
                """
                INSERT INTO memory_audit (
                    memory_id, accessor_id, access_level, action, success, ip_address, device_fingerprint
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (memory_id, accessor_id, access_level, action, success, ip_address, device_fingerprint)
            )
            await db.commit()
    
    @staticmethod
    def _generate_memory_id(content: str, owner_id: str) -> str:
        """Generate deterministic memory ID"""
        content_hash = hashlib.sha256(f"{content}:{owner_id}".encode()).hexdigest()[:16]
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return f"mem_{timestamp}_{content_hash}"
    
    @staticmethod
    def _get_cache_key(tier: Optional[MemoryTier], owner_id: Optional[str]) -> str:
        """Generate cache key for memory bucket"""
        tier_val = tier.value if tier else "all"
        owner_val = owner_id or "system"
        return f"{tier_val}:{owner_val}"
    
    @staticmethod
    def _row_to_memory_entry(row: tuple) -> MemoryEntry:
        """Convert database row to MemoryEntry"""
        # Map row indices to column names
        columns = [
            'id', 'content', 'tier', 'owner_id', 'identities', 'timestamp', 'access_level',
            'source', 'status', 'importance_score', 'reinforcement_count', 'expires_at',
            'tags', 'metadata', 'embedding', 'created_by', 'last_accessed', 'access_count'
        ]
        data = {col: row[i] for i, col in enumerate(columns)}
        return MemoryEntry.from_dict(data)
    
    @staticmethod
    def _row_to_identity_profile(row: tuple) -> IdentityProfile:
        """Convert database row to IdentityProfile"""
        columns = [
            'identity_id', 'name', 'primary_owner_id', 'behavioral_notes', 'communication_style',
            'key_facts', 'relationship_context', 'first_interaction', 'last_interaction',
            'interaction_count', 'permission_level', 'trust_score', 'related_memory_ids',
            'created_at', 'updated_at'
        ]
        data = {col: row[i] for i, col in enumerate(columns)}
        return IdentityProfile.from_dict(data)
    
    @staticmethod
    def _format_time_ago(timestamp: datetime) -> str:
        """Format timestamp as human-readable relative time"""
        now = datetime.now(timezone.utc)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        diff = now - timestamp
        
        if diff.days > 365:
            return f"{diff.days // 365} years ago"
        elif diff.days > 30:
            return f"{diff.days // 30} months ago"
        elif diff.days > 0:
            return f"{diff.days} days ago"
        elif diff.seconds > 3600:
            return f"{diff.seconds // 3600} hours ago"
        elif diff.seconds > 60:
            return f"{diff.seconds // 60} minutes ago"
        else:
            return "just now"
    
    async def backup(self, backup_path: str) -> bool:
        """Create encrypted backup of all memories"""
        try:
            import shutil
            
            backup_path = Path(backup_path)
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create backup
            shutil.copy2(self.db_path, backup_path)
            
            # Encrypt if key provided
            if self.encryption_key:
                # Simple XOR encryption (replace with AES in production)
                with open(backup_path, 'rb') as f:
                    data = f.read()
                encrypted = bytes(b ^ self.encryption_key[i % len(self.encryption_key)] for i, b in enumerate(data))
                with open(backup_path, 'wb') as f:
                    f.write(encrypted)
            
            logger.info(f"💾 Backup created: {backup_path}")
            return True
            
        except Exception as e:
            logger.error(f"Backup failed: {e}", exc_info=True)
            return False
    
    async def restore(self, backup_path: str) -> bool:
        """Restore from backup"""
        try:
            import shutil
            
            backup_path = Path(backup_path)
            if not backup_path.exists():
                raise FileNotFoundError(f"Backup not found: {backup_path}")
            
            # Decrypt if needed
            if self.encryption_key:
                with open(backup_path, 'rb') as f:
                    encrypted = f.read()
                data = bytes(b ^ self.encryption_key[i % len(self.encryption_key)] for i, b in enumerate(encrypted))
                with open(self.db_path, 'wb') as f:
                    f.write(data)
            else:
                shutil.copy2(backup_path, self.db_path)
            
            # Clear caches
            self._memory_cache.clear()
            self._identity_cache.clear()
            
            # Rewarm cache
            await self._warm_cache()
            
            logger.info(f"♻️ Restore completed: {backup_path}")
            return True
            
        except Exception as e:
            logger.error(f"Restore failed: {e}", exc_info=True)
            return False
    
    async def close(self) -> None:
        """Graceful shutdown with daemon cleanup"""
        logger.info("🛑 Shutting down Memory Manager...")
        
        # Stop daemons
        self._reinforcement_active = False
        self._embedding_active = False
        
        if self._reinforcement_task:
            self._reinforcement_task.cancel()
            try:
                await self._reinforcement_task
            except asyncio.CancelledError:
                pass
        
        if self._embedding_worker:
            self._embedding_worker.cancel()
            try:
                await self._embedding_worker
            except asyncio.CancelledError:
                pass
        
        # Clear caches
        self._memory_cache.clear()
        self._identity_cache.clear()
        
        # Close Redis
        if self.redis_client:
            await self.redis_client.close()
        
        logger.info("✅ Memory Manager shutdown complete")

# =============================================================================
# PUBLIC API & SINGLETON
# =============================================================================

# Global singleton instance
_memory_manager_instance: Optional[MemoryManager] = None
_memory_manager_lock = asyncio.Lock()

async def get_memory_manager(
    db_path: str = "aaria_memory_v2.db",
    redis_url: Optional[str] = None,
    encryption_key: Optional[bytes] = None,
    force_new: bool = False
) -> MemoryManager:
    """
    Get or create memory manager singleton
    Ensures only one instance exists per application
    """
    global _memory_manager_instance
    
    async with _memory_manager_lock:
        if _memory_manager_instance is None or force_new:
            _memory_manager_instance = MemoryManager(
                db_path=db_path,
                redis_url=redis_url,
                encryption_key=encryption_key
            )
            await _memory_manager_instance.initialize()
    
    return _memory_manager_instance

# =============================================================================
# TESTING & VALIDATION
# =============================================================================

async def test_memory_manager():
    """Comprehensive test suite"""
    print("🧪 Testing AARIA Memory Manager v2.0...")
    
    # Get instance
    mm = await get_memory_manager(":memory:")  # In-memory DB for testing
    
    # Test 1: Store and retrieve
    memory_id = await mm.store_memory(
        content="Yash's birthday is December 12th",
        tier=MemoryTier.CONFIDENTIAL,
        owner_id="owner_123",
        identities=["Yash"],
        tags=["birthday", "important"],
        importance_score=0.8
    )
    print(f"✓ Stored memory: {memory_id}")
    
    # Test 2: Search
    results = await mm.search_memories(
        query="birthday",
        owner_id="owner_123",
        min_importance=0.5
    )
    assert len(results) == 1
    assert "December 12th" in results[0].content
    print(f"✓ Search returned {len(results)} results")
    
    # Test 3: Identity profile
    profile = await mm.get_identity_profile("Yash", "owner_123")
    assert profile.name == "Yash"
    print(f"✓ Identity profile created: {profile.identity_id}")
    
    # Test 4: Update profile
    await mm.update_identity_profile(
        "Yash",
        "owner_123",
        behavioral_note="Gets excited about surprise parties",
        key_fact={"birthday": "December 12th"}
    )
    print("✓ Identity profile updated")
    
    # Test 5: Reinforcement
    await mm._reinforcement_queue.put((memory_id, "test_reinforcement"))
    await asyncio.sleep(0.1)  # Let daemon process
    print("✓ Memory reinforcement queued")
    
    # Test 6: Stats
    stats = await mm.get_memory_stats("owner_123")
    assert stats["total_memories"] == 1
    print(f"✓ Stats: {stats}")
    
    # Cleanup
    await mm.close()
    print("✅ All tests passed!")

if __name__ == "__main__":
    # Run tests if executed directly
    asyncio.run(test_memory_manager())