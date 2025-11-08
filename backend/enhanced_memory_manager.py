"""
Enhanced Memory Manager with Identity-Centric Architecture.
This version integrates the new hierarchical memory system while maintaining compatibility.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Any, Optional, Tuple
from .memory_hierarchy import MemoryHierarchy, MemoryLevel, MemoryCategory
from .identity_container import IdentityContainer
from .memory_adapter import MemorySystemAdapter

logger = logging.getLogger("AARIA.Memory")

class EnhancedMemoryManager:
    """
    Enhanced Memory Manager that uses the new hierarchical memory system
    while maintaining compatibility with the old interface.
    """
    
    def __init__(self, assistant_core, identity_manager=None, max_per_identity: int = 5000):
        """Initialize the enhanced memory manager with both old and new systems."""
        self.adapter = MemorySystemAdapter(
            assistant_core=assistant_core,
            identity_manager=identity_manager,
            use_new_system=True,  # Default to new system
            max_per_identity=max_per_identity
        )
        
        # For type hinting and IDE support
        self.old_manager = self.adapter.old_manager
        self.new_hierarchy = self.adapter.new_hierarchy
        
        # Set by migration
        self.migration_complete = False
    
    async def initialize(self) -> None:
        """Run initial setup tasks."""
        # Start migration in background
        self.migration_complete = await self.adapter.start_migration()
    
    # Core Memory Operations
    async def store_memory(
        self,
        user_input: str,
        assistant_response: str,
        subject_identity_id: str = "owner_primary",
        importance: int = 1,
        desired_segment: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        actor: str = "persona"
    ) -> Tuple[str, str]:
        """Store a new memory using the adapter."""
        return await self.adapter.store_memory(
            user_input=user_input,
            assistant_response=assistant_response,
            subject_identity_id=subject_identity_id,
            importance=importance,
            desired_segment=desired_segment,
            metadata=metadata,
            actor=actor
        )
    
    async def retrieve_memories(
        self,
        query_text: str,
        subject_identity_id: str = "owner_primary",
        requester_id: Optional[str] = None,
        allowed_segments: Optional[List[str]] = None,
        limit: int = 5,
        min_relevance: float = 0.05
    ) -> List[Dict[str, Any]]:
        """Retrieve memories using the adapter."""
        return await self.adapter.retrieve_memories(
            query_text=query_text,
            subject_identity_id=subject_identity_id,
            requester_id=requester_id,
            allowed_segments=allowed_segments,
            limit=limit,
            min_relevance=min_relevance
        )
    
    async def get_identity_summary(
        self,
        identity_id: str,
        requester_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get identity summary using the adapter."""
        return await self.adapter.get_identity_summary(
            identity_id=identity_id,
            requester_id=requester_id
        )
    
    # Compatibility Methods
    async def append_memory(self, identity_id: str, memory_record: Dict[str, Any]) -> Tuple[str, str]:
        """Compatibility method for old append_memory interface."""
        return await self.adapter.append_memory(identity_id, memory_record)
    
    async def write_memory(self, identity_id: str, memory_record: Dict[str, Any]) -> Tuple[str, str]:
        """Compatibility method for old write_memory interface."""
        return await self.adapter.append_memory(identity_id, memory_record)
    
    async def search_memories(self, subject_identity_id: str, query: str = "", limit: int = 5, requester_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Compatibility method for old search_memories interface.

        Added `requester_id` to allow callers (like PersonaCore) to specify
        who is requesting the memories so access control in the new
        hierarchical system works correctly.
        """
        # Use the adapter's retrieve_memories with explicit keyword args so
        # argument ordering can't accidentally be mismatched between old/new APIs.
        return await self.adapter.retrieve_memories(
            query_text=query,
            subject_identity_id=subject_identity_id,
            requester_id=requester_id,
            limit=limit
        )
    
    async def query_memories(self, subject_identity_id: str, query: str = "", max_results: int = 5) -> List[Dict[str, Any]]:
        """Compatibility method for old query_memories interface."""
        return await self.adapter.search_memories(subject_identity_id, query, max_results)
    
    async def list_memories_for_subject(self, subject_identity_id: str) -> List[str]:
        """List all memory IDs for a subject."""
        return await self.old_manager.list_memories_for_subject(subject_identity_id)
    
    async def get_identity_container(self, identity_id: str) -> Dict[str, Any]:
        """Get or create identity container."""
        if identity_id not in self.new_hierarchy.identities:
            self.new_hierarchy.identities[identity_id] = IdentityContainer(identity_id)
        
        container = self.new_hierarchy.identities[identity_id]
        
        # Convert to old format for compatibility
        return {
            "identity_id": container.identity_id,
            "created_at": container.created_at,
            "name_variants": container.name_variants,
            "behavior_patterns": container.behavioral_patterns,
            "relationships": container.relationships,
            "permissions": container.permissions,
            "memory_index": container.memory_index,
            "semantic_index": container.semantic_index,
            "transcript_index": container.transcript_index,
            "proactive_rules": container.proactive_rules
        }
    
    async def update_identity_container(self, identity_id: str, patch: Dict[str, Any]) -> bool:
        """Update identity container with new data."""
        if identity_id not in self.new_hierarchy.identities:
            self.new_hierarchy.identities[identity_id] = IdentityContainer(identity_id)
        
        container = self.new_hierarchy.identities[identity_id]
        
        # Update container fields
        if "name_variants" in patch:
            container.name_variants = patch["name_variants"]
        if "behavior_patterns" in patch:
            container.behavioral_patterns.update(patch["behavior_patterns"])
        if "relationships" in patch:
            container.relationships.update(patch["relationships"])
        if "permissions" in patch:
            container.permissions.update(patch["permissions"])
        if "proactive_rules" in patch:
            container.proactive_rules.update(patch["proactive_rules"])
        
        return True
    
    async def append_transcript(self, identity_id: str, transcript_entry: Dict[str, Any]) -> str:
        """Append transcript entry."""
        if identity_id not in self.new_hierarchy.identities:
            self.new_hierarchy.identities[identity_id] = IdentityContainer(identity_id)
        
        container = self.new_hierarchy.identities[identity_id]
        container.transcript_index.append(transcript_entry["id"])
        
        return transcript_entry["id"]
    
    async def append_semantic(self, identity_id: str, semantic_entry: Dict[str, Any]) -> str:
        """Append semantic entry."""
        if identity_id not in self.new_hierarchy.identities:
            self.new_hierarchy.identities[identity_id] = IdentityContainer(identity_id)
        
        container = self.new_hierarchy.identities[identity_id]
        container.semantic_index.append(semantic_entry["id"])
        
        return semantic_entry["id"]
    
    # Health Check
    async def health_check(self) -> Dict[str, Any]:
        """Get health status of both memory systems."""
        return await self.adapter.health_check()
    
    # Persistence
    async def save_transcript_store(self, identity_id: str, transcripts: List[Dict[str, Any]]) -> bool:
        """Save transcript store for an identity."""
        if identity_id not in self.new_hierarchy.identities:
            self.new_hierarchy.identities[identity_id] = IdentityContainer(identity_id)
        
        container = self.new_hierarchy.identities[identity_id]
        container.transcript_index = [t["id"] for t in transcripts]
        return True
    
    async def save_semantic_index(self, identity_id: str, semantic_index: List[Dict[str, Any]]) -> bool:
        """Save semantic index for an identity."""
        if identity_id not in self.new_hierarchy.identities:
            self.new_hierarchy.identities[identity_id] = IdentityContainer(identity_id)
        
        container = self.new_hierarchy.identities[identity_id]
        container.semantic_index = [e["id"] for e in semantic_index]
        return True