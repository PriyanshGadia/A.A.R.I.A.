"""Memory system adapter for transitioning between old and new implementations"""
from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Any, Optional, Tuple
from backend.memory_manager import MemoryManager
from backend.memory_hierarchy import MemoryHierarchy, MemoryLevel, MemoryCategory
from backend.identity_container import IdentityContainer

logger = logging.getLogger("AARIA.Memory.Adapter")

class MemorySystemAdapter:
    """
    Adapter class that provides compatibility between old and new memory systems.
    This allows for gradual migration and fallback capabilities.
    """
    
    def __init__(
        self,
        assistant_core,
        identity_manager=None,
        use_new_system: bool = True,
        max_per_identity: int = 5000
    ):
        self.use_new_system = use_new_system
        
        # Initialize both systems
        self.old_manager = MemoryManager(
            assistant_core=assistant_core,
            identity_manager=identity_manager,
            max_per_identity=max_per_identity
        )
        
        self.new_hierarchy = MemoryHierarchy()
        
        # Migration state
        self.migration_in_progress = False
        self.migration_results = None
    
    def _map_segment_to_level(self, segment: str) -> MemoryLevel:
        """Map old segments to new memory levels."""
        if segment == "confidential":
            return MemoryLevel.CONFIDENTIAL
        elif segment == "access_data":
            return MemoryLevel.ACCESS
        elif segment == "public_data":
            return MemoryLevel.PUBLIC
        return MemoryLevel.ACCESS  # Default
    
    def _map_level_to_segment(self, level: MemoryLevel) -> str:
        """Map new memory levels to old segments."""
        if level == MemoryLevel.CONFIDENTIAL:
            return "confidential"
        elif level == MemoryLevel.ACCESS:
            return "access_data"
        elif level == MemoryLevel.PUBLIC:
            return "public_data"
        return "access_data"  # Default
    
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
        """Store memory in the active system."""
        if not self.use_new_system:
            return await self.old_manager.store_memory(
                user_input=user_input,
                assistant_response=assistant_response,
                subject_identity_id=subject_identity_id,
                importance=importance,
                desired_segment=desired_segment,
                metadata=metadata,
                actor=actor
            )
        
        try:
            # Store in new system
            level = (
                self._map_segment_to_level(desired_segment)
                if desired_segment
                else MemoryLevel.ACCESS
            )
            
            memory_id = await self.new_hierarchy.create_memory(
                content={
                    "user": user_input,
                    "assistant": assistant_response,
                    "metadata": metadata or {}
                },
                level=level,
                category=MemoryCategory.INTERACTION,
                identity_id=subject_identity_id,
                metadata={
                    "importance": importance,
                    "actor": actor,
                    "original_segment": desired_segment
                }
            )
            
            # Update behavioral analysis
            if subject_identity_id in self.new_hierarchy.identities:
                identity = self.new_hierarchy.identities[subject_identity_id]
                await identity.analyze_behavior(
                    text=f"{user_input}\n{assistant_response}",
                    context=metadata
                )
            
            return memory_id, self._map_level_to_segment(level)
            
        except Exception as e:
            logger.error(f"Error storing memory in new system: {e}")
            # Fallback to old system
            return await self.old_manager.store_memory(
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
        """Retrieve memories from the active system."""
        if not self.use_new_system:
            return await self.old_manager.retrieve_memories(
                query_text=query_text,
                subject_identity_id=subject_identity_id,
                requester_id=requester_id,
                allowed_segments=allowed_segments,
                limit=limit,
                min_relevance=min_relevance
            )
        
        try:
            # Convert allowed segments to levels
            allowed_levels = None
            if allowed_segments:
                allowed_levels = [self._map_segment_to_level(seg) for seg in allowed_segments]
            
            # Search in new system
            results = await self.new_hierarchy.search_memories(
                requester_id=requester_id or "anonymous",
                identity_id=subject_identity_id,
                timeframe=None,  # Could add time-based filtering
                category=MemoryCategory.INTERACTION,
                limit=limit
            )
            
            # Convert to old format for compatibility
            converted_results = []
            for result in results:
                content = result["content"]
                converted = {
                    "id": result["id"],
                    "subject_id": subject_identity_id,
                    "user": content.get("user", ""),
                    "assistant": content.get("assistant", ""),
                    "timestamp": result["created_at"],
                    "importance": result.get("metadata", {}).get("importance", 1),
                    "segment": self._map_level_to_segment(MemoryLevel(result["level"])),
                    "metadata": content.get("metadata", {})
                }
                converted_results.append(converted)
            
            return converted_results[:limit]
            
        except Exception as e:
            logger.error(f"Error retrieving memories from new system: {e}")
            # Fallback to old system
            return await self.old_manager.retrieve_memories(
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
        """Get identity summary from active system."""
        if not self.use_new_system:
            container = await self.old_manager.get_identity_container(identity_id)
            return {
                "id": identity_id,
                "behavior_patterns": container.get("behavior_patterns", {}),
                "relationships": container.get("relationships", {})
            }
        
        try:
            summary = await self.new_hierarchy.get_identity_summary(
                identity_id=identity_id,
                requester_id=requester_id or "system"
            )
            
            if summary:
                return {
                    "id": identity_id,
                    "behavioral_summary": summary["identity"],
                    "recent_memories": summary["recent_memories"],
                    "relationships": summary["relationship_strength"]
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting identity summary from new system: {e}")
            # Fallback to old system
            container = await self.old_manager.get_identity_container(identity_id)
            return {
                "id": identity_id,
                "behavior_patterns": container.get("behavior_patterns", {}),
                "relationships": container.get("relationships", {})
            }
    
    async def start_migration(self) -> Dict[str, Any]:
        """Start migration from old to new system."""
        if self.migration_in_progress:
            return {"status": "already_running"}
        
        self.migration_in_progress = True
        from .memory_migration import migrate_to_hierarchical_memory, validate_migration
        
        try:
            # Perform migration
            self.migration_results = await migrate_to_hierarchical_memory(
                old_manager=self.old_manager,
                new_hierarchy=self.new_hierarchy
            )
            
            # Validate results
            validation = await validate_migration(
                old_manager=self.old_manager,
                new_hierarchy=self.new_hierarchy
            )
            
            self.migration_results["validation"] = validation
            self.migration_in_progress = False
            
            # Switch to new system if validation successful
            if validation["success"]:
                self.use_new_system = True
            
            return {
                "status": "completed",
                "results": self.migration_results
            }
            
        except Exception as e:
            self.migration_in_progress = False
            return {
                "status": "failed",
                "error": str(e)
            }
    
    async def rollback_migration(self) -> Dict[str, Any]:
        """Rollback migration if needed."""
        from .memory_migration import rollback_migration
        
        try:
            results = await rollback_migration(
                old_manager=self.old_manager,
                new_hierarchy=self.new_hierarchy
            )
            
            if results["success"]:
                self.use_new_system = False
            
            return results
            
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }
    
    async def health_check(self) -> Dict[str, Any]:
        """Check health of both memory systems."""
        results = {
            "old_system": None,
            "new_system": {
                "status": "unknown"
            },
            "active_system": "old" if not self.use_new_system else "new",
            "migration_status": "in_progress" if self.migration_in_progress else "not_started"
        }
        
        # Check old system
        try:
            old_health = await self.old_manager.health_check()
            results["old_system"] = old_health
        except Exception as e:
            results["old_system"] = {
                "status": "error",
                "error": str(e)
            }
        
        # Check new system
        try:
            identity_count = len(self.new_hierarchy.identities)
            results["new_system"] = {
                "status": "healthy",
                "identities": identity_count
            }
        except Exception as e:
            results["new_system"] = {
                "status": "error",
                "error": str(e)
            }
        
        return results

    # Compatibility methods
    async def append_memory(self, *args, **kwargs):
        """Compatibility method."""
        return await self.store_memory(*args, **kwargs)
    
    async def search_memories(self, *args, **kwargs):
        """Compatibility method."""
        return await self.retrieve_memories(*args, **kwargs)
    
    async def query_memories(self, *args, **kwargs):
        """Compatibility method."""
        return await self.retrieve_memories(*args, **kwargs)