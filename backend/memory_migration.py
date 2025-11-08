"""Migration utilities for upgrading to the new memory system"""
from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Any, Optional
from backend.memory_manager import MemoryManager
from backend.memory_hierarchy import MemoryHierarchy, MemoryLevel, MemoryCategory
from backend.identity_container import IdentityContainer

logger = logging.getLogger("AARIA.Memory.Migration")

async def migrate_to_hierarchical_memory(old_manager: MemoryManager, new_hierarchy: MemoryHierarchy) -> Dict[str, Any]:
    """Migrate from old MemoryManager to new MemoryHierarchy system."""
    results = {
        "identities_migrated": 0,
        "memories_migrated": 0,
        "errors": []
    }
    
    try:
        # Get all identities
        identities = await old_manager.list_identities()
        
        for identity_id in identities:
            try:
                # Get old container
                old_container = await old_manager.get_identity_container(identity_id)
                
                # Create new identity container
                new_container = IdentityContainer(identity_id)
                new_container.created_at = old_container.get("created_at", 0)
                new_container.name_variants = old_container.get("name_variants", [])
                new_container.permissions = old_container.get("permissions", {})
                
                # Migrate behavioral patterns
                for pattern_key, pattern_data in old_container.get("behavior_patterns", {}).items():
                    if isinstance(pattern_data, dict):
                        new_container.behavioral_patterns[pattern_key] = pattern_data
                
                # Migrate relationships
                for rel_id, rel_data in old_container.get("relationships", {}).items():
                    if isinstance(rel_data, dict):
                        new_container.relationships[rel_id] = rel_data
                
                # Store the new container
                new_hierarchy.identities[identity_id] = new_container
                results["identities_migrated"] += 1
                
                # Migrate memories
                for mem_id in old_container.get("memory_index", []):
                    try:
                        old_mem = await old_manager.get(mem_id)
                        if not old_mem:
                            continue
                            
                        # Convert segment to memory level
                        level = MemoryLevel.ACCESS  # Default
                        if old_mem.get("segment") == "confidential":
                            level = MemoryLevel.CONFIDENTIAL
                        elif old_mem.get("segment") == "public_data":
                            level = MemoryLevel.PUBLIC
                        
                        # Create memory node
                        await new_hierarchy.create_memory(
                            content={
                                "user": old_mem.get("user", ""),
                                "assistant": old_mem.get("assistant", ""),
                                "content_hash": old_mem.get("content_hash", ""),
                                "metadata": old_mem.get("metadata", {})
                            },
                            level=level,
                            category=MemoryCategory.INTERACTION,
                            identity_id=identity_id,
                            metadata={
                                "original_id": mem_id,
                                "importance": old_mem.get("importance", 1),
                                "access_count": old_mem.get("access_count", 0),
                                "timestamp": old_mem.get("timestamp", 0)
                            }
                        )
                        results["memories_migrated"] += 1
                        
                    except Exception as mem_err:
                        results["errors"].append(f"Memory migration failed for {mem_id}: {str(mem_err)}")
                        continue
                
            except Exception as id_err:
                results["errors"].append(f"Identity migration failed for {identity_id}: {str(id_err)}")
                continue
                
    except Exception as e:
        results["errors"].append(f"Global migration error: {str(e)}")
    
    return results

async def validate_migration(old_manager: MemoryManager, new_hierarchy: MemoryHierarchy) -> Dict[str, Any]:
    """Validate that migration was successful by comparing key metrics."""
    validation = {
        "success": True,
        "metrics": {},
        "discrepancies": []
    }
    
    try:
        # Compare identity counts
        old_identities = await old_manager.list_identities()
        new_identities = list(new_hierarchy.identities.keys())
        
        validation["metrics"]["old_identity_count"] = len(old_identities)
        validation["metrics"]["new_identity_count"] = len(new_identities)
        
        if len(old_identities) != len(new_identities):
            validation["discrepancies"].append(
                f"Identity count mismatch: {len(old_identities)} vs {len(new_identities)}"
            )
            validation["success"] = False
        
        # Compare memory counts and content for each identity
        total_old_memories = 0
        total_new_memories = 0
        
        for identity_id in old_identities:
            if identity_id not in new_hierarchy.identities:
                validation["discrepancies"].append(f"Missing identity in new system: {identity_id}")
                validation["success"] = False
                continue
            
            old_container = await old_manager.get_identity_container(identity_id)
            old_memories = old_container.get("memory_index", [])
            total_old_memories += len(old_memories)
            
            # Count memories in new system
            new_memories = await new_hierarchy.search_memories(
                requester_id="system",
                identity_id=identity_id,
                limit=10000  # High limit to get all
            )
            total_new_memories += len(new_memories)
            
            if len(old_memories) != len(new_memories):
                validation["discrepancies"].append(
                    f"Memory count mismatch for {identity_id}: {len(old_memories)} vs {len(new_memories)}"
                )
                validation["success"] = False
        
        validation["metrics"]["total_old_memories"] = total_old_memories
        validation["metrics"]["total_new_memories"] = total_new_memories
        
        if total_old_memories != total_new_memories:
            validation["discrepancies"].append(
                f"Total memory count mismatch: {total_old_memories} vs {total_new_memories}"
            )
            validation["success"] = False
            
    except Exception as e:
        validation["success"] = False
        validation["discrepancies"].append(f"Validation error: {str(e)}")
    
    return validation

async def rollback_migration(old_manager: MemoryManager, new_hierarchy: MemoryHierarchy) -> Dict[str, Any]:
    """Rollback migration by restoring from backup if available."""
    results = {
        "success": False,
        "restored_identities": 0,
        "restored_memories": 0,
        "errors": []
    }
    
    # This is a placeholder - implement actual rollback logic
    # For now, we'll just return the status
    results["success"] = False
    results["errors"].append("Rollback not implemented - manual intervention required")
    
    return results