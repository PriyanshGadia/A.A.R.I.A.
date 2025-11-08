"""
Hierarchical Memory Management System with Identity-Centric Architecture
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Set, Tuple
from enum import Enum
from dataclasses import dataclass, field
from .identity_container import IdentityContainer

logger = logging.getLogger("AARIA.Memory")

class MemoryLevel(str, Enum):
    ROOT = "root"               # System-critical, core identity
    OWNER = "owner"            # Owner-specific, highly confidential
    CONFIDENTIAL = "confidential"  # Sensitive information
    ACCESS = "access"          # Identity-specific accessible data
    PUBLIC = "public"          # Generally available information

class MemoryCategory(str, Enum):
    IDENTITY = "identity"      # Identity-related information
    INTERACTION = "interaction"  # Interaction history
    BEHAVIORAL = "behavioral"  # Behavioral patterns
    SEMANTIC = "semantic"      # Understanding and relationships
    EPISODIC = "episodic"     # Time-based experiences
    PROCEDURAL = "procedural"  # Skills and procedures

@dataclass
class MemoryNode:
    id: str
    level: MemoryLevel
    category: MemoryCategory
    content: Dict[str, Any]
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    related_ids: Set[str] = field(default_factory=set)
    access_control: Dict[str, List[str]] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

class MemoryHierarchy:
    """
    Hierarchical memory system with identity-centric architecture and security levels
    """
    
    def __init__(self):
        self.identities: Dict[str, IdentityContainer] = {}
        self.memory_nodes: Dict[str, MemoryNode] = {}
        self.semantic_index: Dict[str, Set[str]] = {}  # Concept -> Node IDs
        self.temporal_index: Dict[str, List[str]] = {}  # Timeframe -> Node IDs
        self.identity_index: Dict[str, Set[str]] = {}  # Identity ID -> Node IDs
        
        # Root identity for system
        self.root_identity = "system"
        if self.root_identity not in self.identities:
            self.identities[self.root_identity] = IdentityContainer(self.root_identity)
    
    async def create_memory(
        self,
        content: Dict[str, Any],
        level: MemoryLevel,
        category: MemoryCategory,
        identity_id: Optional[str] = None,
        related_ids: Optional[Set[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Create a new memory node with proper security and indexing."""
        node_id = f"mem_{time.time_ns()}"
        
        node = MemoryNode(
            id=node_id,
            level=level,
            category=category,
            content=content,
            related_ids=related_ids or set(),
            metadata=metadata or {}
        )
        
        # Set access control based on level
        if level == MemoryLevel.ROOT:
            node.access_control = {"system": ["read", "write"]}
        elif level == MemoryLevel.OWNER:
            node.access_control = {
                "system": ["read"],
                "owner": ["read", "write"]
            }
        elif level == MemoryLevel.CONFIDENTIAL:
            node.access_control = {
                "system": ["read"],
                "owner": ["read"],
                identity_id: ["read"] if identity_id else {}
            }
        elif level == MemoryLevel.ACCESS:
            node.access_control = {
                "system": ["read"],
                "owner": ["read"],
                identity_id: ["read"] if identity_id else {},
                "authenticated": ["read"]
            }
        else:  # PUBLIC
            node.access_control = {"*": ["read"]}
        
        self.memory_nodes[node_id] = node
        
        # Update indices
        if identity_id:
            if identity_id not in self.identity_index:
                self.identity_index[identity_id] = set()
            self.identity_index[identity_id].add(node_id)
        
        # Update semantic index
        if "concepts" in content:
            for concept in content["concepts"]:
                if concept not in self.semantic_index:
                    self.semantic_index[concept] = set()
                self.semantic_index[concept].add(node_id)
        
        # Update temporal index
        timeframe = self._get_timeframe(node.created_at)
        if timeframe not in self.temporal_index:
            self.temporal_index[timeframe] = []
        self.temporal_index[timeframe].append(node_id)
        
        return node_id
    
    async def update_memory(
        self,
        node_id: str,
        content: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        related_ids: Optional[Set[str]] = None
    ) -> bool:
        """Update an existing memory node."""
        if node_id not in self.memory_nodes:
            return False
        
        node = self.memory_nodes[node_id]
        current_time = time.time()
        
        if content:
            # Remove old semantic indices
            if "concepts" in node.content:
                for concept in node.content["concepts"]:
                    if concept in self.semantic_index:
                        self.semantic_index[concept].discard(node_id)
            
            node.content.update(content)
            
            # Add new semantic indices
            if "concepts" in content:
                for concept in content["concepts"]:
                    if concept not in self.semantic_index:
                        self.semantic_index[concept] = set()
                    self.semantic_index[concept].add(node_id)
        
        if metadata:
            node.metadata.update(metadata)
        
        if related_ids is not None:
            node.related_ids = related_ids
        
        node.updated_at = current_time
        return True
    
    async def get_memory(
        self,
        node_id: str,
        requester_id: str,
        include_related: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Retrieve a memory node with proper access control."""
        if node_id not in self.memory_nodes:
            return None
        
        node = self.memory_nodes[node_id]
        
        # Check access permission
        if not self._has_access(node, requester_id, "read"):
            return None
        
        result = {
            "id": node.id,
            "level": node.level.value,
            "category": node.category.value,
            "content": node.content,
            "created_at": node.created_at,
            "updated_at": node.updated_at,
            "metadata": node.metadata
        }
        
        if include_related:
            related_nodes = []
            for related_id in node.related_ids:
                if related_id in self.memory_nodes:
                    related_node = self.memory_nodes[related_id]
                    if self._has_access(related_node, requester_id, "read"):
                        related_nodes.append({
                            "id": related_node.id,
                            "level": related_node.level.value,
                            "category": related_node.category.value,
                            "content": related_node.content
                        })
            result["related_nodes"] = related_nodes
        
        return result
    
    async def search_memories(
        self,
        requester_id: str,
        identity_id: Optional[str] = None,
        concepts: Optional[List[str]] = None,
        timeframe: Optional[Tuple[float, float]] = None,
        category: Optional[MemoryCategory] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Search memories with multiple criteria and access control."""
        candidate_nodes = set()
        
        # Collect candidate nodes
        if identity_id:
            if identity_id in self.identity_index:
                candidate_nodes.update(self.identity_index[identity_id])
        
        if concepts:
            for concept in concepts:
                if concept in self.semantic_index:
                    candidate_nodes.update(self.semantic_index[concept])
        
        if timeframe:
            start_time, end_time = timeframe
            for tf in self._get_timeframes_between(start_time, end_time):
                if tf in self.temporal_index:
                    candidate_nodes.update(self.temporal_index[tf])
        
        if not candidate_nodes:
            candidate_nodes = set(self.memory_nodes.keys())
        
        # Filter and prepare results
        results = []
        for node_id in candidate_nodes:
            node = self.memory_nodes[node_id]
            
            if category and node.category != category:
                continue
            
            if not self._has_access(node, requester_id, "read"):
                continue
            
            results.append({
                "id": node.id,
                "level": node.level.value,
                "category": node.category.value,
                "content": node.content,
                "created_at": node.created_at,
                "metadata": node.metadata
            })
            
            if len(results) >= limit:
                break
        
        return sorted(results, key=lambda x: x["created_at"], reverse=True)
    
    def _has_access(self, node: MemoryNode, identity_id: str, permission: str) -> bool:
        """Check if an identity has specific permission for a node."""
        access_control = node.access_control
        
        # System has all permissions
        if identity_id == self.root_identity:
            return True
        
        # Check explicit identity permissions
        if identity_id in access_control:
            return permission in access_control[identity_id]
        
        # Check role-based permissions
        if identity_id in self.identities:
            identity = self.identities[identity_id]
            if "owner" in access_control and identity.access_level == "owner":
                return permission in access_control["owner"]
            if "authenticated" in access_control and identity.access_level != "public":
                return permission in access_control["authenticated"]
        
        # Check public permissions
        if "*" in access_control:
            return permission in access_control["*"]
        
        return False
    
    def _get_timeframe(self, timestamp: float) -> str:
        """Convert timestamp to timeframe string (e.g., '2023-Q4')."""
        # This is a simple implementation - could be more sophisticated
        from datetime import datetime
        dt = datetime.fromtimestamp(timestamp)
        quarter = (dt.month - 1) // 3 + 1
        return f"{dt.year}-Q{quarter}"
    
    def _get_timeframes_between(self, start: float, end: float) -> List[str]:
        """Get list of timeframe strings between start and end timestamps."""
        timeframes = []
        current = start
        while current <= end:
            timeframes.append(self._get_timeframe(current))
            # Advance by roughly one quarter
            current += 7776000  # 90 days in seconds
        return list(set(timeframes))  # Remove duplicates
    
    async def analyze_interaction(
        self,
        identity_id: str,
        content: Dict[str, Any],
        interaction_type: str,
        context: Dict[str, Any]
    ) -> None:
        """Analyze an interaction and update relevant memory structures."""
        if identity_id not in self.identities:
            self.identities[identity_id] = IdentityContainer(identity_id)
        
        identity = self.identities[identity_id]
        
        # Update behavioral analysis
        if "text" in content:
            await identity.analyze_behavior(content["text"], context)
        
        # Update relationship context
        if "related_to" in context:
            await identity.update_relationship(
                context["related_to"],
                interaction_type,
                context
            )
        
        # Create memory nodes for the interaction
        interaction_id = await self.create_memory(
            content=content,
            level=MemoryLevel.ACCESS,
            category=MemoryCategory.INTERACTION,
            identity_id=identity_id,
            metadata={"interaction_type": interaction_type}
        )
        
        # Update indices
        identity.interaction_history.append({
            "memory_id": interaction_id,
            "type": interaction_type,
            "timestamp": time.time()
        })
        
        if len(identity.interaction_history) > 1000:
            identity.interaction_history = identity.interaction_history[-1000:]
    
    async def get_identity_summary(self, identity_id: str, requester_id: str) -> Optional[Dict[str, Any]]:
        """Get a summary of an identity's information and behavior."""
        if identity_id not in self.identities:
            return None
        
        identity = self.identities[identity_id]
        
        # Check access permission
        if not self._has_access(
            MemoryNode(
                id="temp",
                level=MemoryLevel.ACCESS,
                category=MemoryCategory.IDENTITY,
                content={},
                access_control={"authenticated": ["read"]}
            ),
            requester_id,
            "read"
        ):
            return None
        
        return {
            "identity": identity.get_behavioral_summary(),
            "recent_memories": await self.search_memories(
                requester_id=requester_id,
                identity_id=identity_id,
                limit=5
            ),
            "relationship_strength": {
                other_id: rel.strength
                for other_id, rel in identity.relationships.items()
            }
        }