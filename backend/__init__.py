"""A.A.R.I.A Backend Package"""
from typing import Dict, Any

# Version info
__version__ = "1.0.0"
__author__ = "PriyanshGadia"

# Package metadata
metadata: Dict[str, Any] = {
    "name": "AARIA",
    "description": "Adaptive Autonomous Reasoning Intelligent Assistant",
    "version": __version__,
    "author": __author__,
}

# Expose the new memory system
from .memory_hierarchy import MemoryHierarchy, MemoryLevel, MemoryCategory
from .identity_container import IdentityContainer, RelationType, BehaviorCategory
from .memory_adapter import MemorySystemAdapter
from .enhanced_memory_manager import EnhancedMemoryManager

__all__ = [
    "MemoryHierarchy",
    "MemoryLevel",
    "MemoryCategory",
    "IdentityContainer",
    "RelationType",
    "BehaviorCategory",
    "MemorySystemAdapter",
    "EnhancedMemoryManager",
]