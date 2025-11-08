"""
Enhanced Identity Container with Behavioral Analysis and Relationship Context
"""
from __future__ import annotations

import time
import asyncio
import logging
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger("AARIA.Identity")

class RelationType(str, Enum):
    OWNER = "owner"
    FAMILY = "family"
    FRIEND = "friend"
    COLLEAGUE = "colleague"
    ACQUAINTANCE = "acquaintance"
    BUSINESS = "business"
    OTHER = "other"

class BehaviorCategory(str, Enum):
    COMMUNICATION = "communication_style"  # How they communicate
    EMOTIONAL = "emotional_patterns"       # Their emotional tendencies
    SCHEDULE = "schedule_patterns"         # Their timing/scheduling habits
    PREFERENCE = "preferences"             # Their likes/dislikes
    CONTEXT = "contextual_behavior"        # Behavior in specific contexts
    INTERACTION = "interaction_patterns"   # How they interact with others

@dataclass
class BehavioralPattern:
    category: BehaviorCategory
    pattern: str
    confidence: float
    observed_count: int
    last_observed: float
    contexts: List[str] = field(default_factory=list)
    related_identities: List[str] = field(default_factory=list)

@dataclass
class RelationshipContext:
    relation_type: RelationType
    strength: float  # 0.0 to 1.0
    contexts: List[str]
    shared_memories: List[str]
    last_interaction: float
    behavior_notes: List[str] = field(default_factory=list)
    trust_score: float = 0.5

class IdentityContainer:
    """Enhanced identity container with behavioral analysis and relationship tracking"""
    
    def __init__(self, identity_id: str):
        self.identity_id = identity_id
        self.created_at = time.time()
        self.updated_at = time.time()
        
        # Basic Identity Info
        self.name_variants: List[str] = []
        self.preferred_name: str = ""
        self.description: str = ""
        
        # Contact & Personal Info
        self.contact_info: Dict[str, str] = {}
        self.important_dates: Dict[str, str] = {}
        self.notes: List[str] = []
        
        # Behavioral Analysis
        self.behavioral_patterns: Dict[str, BehavioralPattern] = {}
        self.interaction_history: List[Dict[str, Any]] = []
        self.recent_emotions: List[Tuple[str, float]] = []
        
        # Relationship Context
        self.relationships: Dict[str, RelationshipContext] = {}
        self.primary_connections: Set[str] = set()
        
        # Security & Access
        self.permissions: Dict[str, List[str]] = {}
        self.access_level: str = "public"
        self.trust_score: float = 0.5
        self.verification_required: bool = True
        
        # Memory Indices
        self.memory_index: List[str] = []
        self.semantic_index: List[str] = []
        self.transcript_index: List[str] = []
        
        # Proactive Settings
        self.proactive_rules = {
            "notification_preferences": {},
            "quiet_hours": None,
            "urgency_patterns": [],
            "response_templates": {}
        }
    
    async def analyze_behavior(self, text: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Analyze text to extract and update behavioral patterns."""
        context = context or {}
        current_time = time.time()
        
        # Emotional Analysis
        emotional_indicators = {
            "angry": ["angry", "furious", "mad", "upset", "frustrated"],
            "happy": ["happy", "joyful", "excited", "glad", "pleased"],
            "anxious": ["worried", "anxious", "nervous", "concerned"],
            "urgent": ["urgent", "asap", "emergency", "immediate"]
        }
        
        for emotion, indicators in emotional_indicators.items():
            if any(ind in text.lower() for ind in indicators):
                self.recent_emotions.append((emotion, current_time))
                if len(self.recent_emotions) > 10:
                    self.recent_emotions = self.recent_emotions[-10:]
                
                # Update behavioral pattern
                pattern_key = f"emotional_{emotion}"
                if pattern_key not in self.behavioral_patterns:
                    self.behavioral_patterns[pattern_key] = BehavioralPattern(
                        category=BehaviorCategory.EMOTIONAL,
                        pattern=f"Shows {emotion} tendencies",
                        confidence=0.5,
                        observed_count=1,
                        last_observed=current_time
                    )
                else:
                    pattern = self.behavioral_patterns[pattern_key]
                    pattern.observed_count += 1
                    pattern.confidence = min(0.95, pattern.confidence + 0.05)
                    pattern.last_observed = current_time
                    if context.get("related_to"):
                        pattern.related_identities.append(context["related_to"])
        
        # Communication Style Analysis
        comm_patterns = {
            "formal": ["formally", "respectfully", "professionally"],
            "casual": ["casually", "informally", "friendly"],
            "direct": ["directly", "straight", "bluntly"],
            "verbose": ["extensively", "detailed", "thoroughly"]
        }
        
        for style, indicators in comm_patterns.items():
            if any(ind in text.lower() for ind in indicators):
                pattern_key = f"communication_{style}"
                if pattern_key not in self.behavioral_patterns:
                    self.behavioral_patterns[pattern_key] = BehavioralPattern(
                        category=BehaviorCategory.COMMUNICATION,
                        pattern=f"Communicates {style}",
                        confidence=0.5,
                        observed_count=1,
                        last_observed=current_time
                    )
                else:
                    pattern = self.behavioral_patterns[pattern_key]
                    pattern.observed_count += 1
                    pattern.confidence = min(0.95, pattern.confidence + 0.05)
                    pattern.last_observed = current_time
        
        self.updated_at = current_time
    
    async def update_relationship(self, other_id: str, interaction_type: str, context: Dict[str, Any]) -> None:
        """Update relationship context based on an interaction."""
        current_time = time.time()
        
        if other_id not in self.relationships:
            self.relationships[other_id] = RelationshipContext(
                relation_type=RelationType.OTHER,
                strength=0.1,
                contexts=[],
                shared_memories=[],
                last_interaction=current_time
            )
        
        rel = self.relationships[other_id]
        rel.last_interaction = current_time
        
        # Update relationship strength
        if interaction_type == "positive":
            rel.strength = min(1.0, rel.strength + 0.05)
        elif interaction_type == "negative":
            rel.strength = max(0.0, rel.strength - 0.05)
        
        # Add context if new
        if context.get("context") and context["context"] not in rel.contexts:
            rel.contexts.append(context["context"])
        
        # Add behavior note if provided
        if context.get("behavior_note"):
            rel.behavior_notes.append(context["behavior_note"])
            if len(rel.behavior_notes) > 10:
                rel.behavior_notes = rel.behavior_notes[-10:]
        
        # Update trust score based on interaction
        if context.get("trust_impact"):
            rel.trust_score = max(0.0, min(1.0, rel.trust_score + context["trust_impact"]))
        
        self.updated_at = current_time
    
    def get_behavioral_summary(self) -> Dict[str, Any]:
        """Get a summary of behavioral patterns and relationships."""
        # Filter to strong patterns
        significant_patterns = {
            k: v for k, v in self.behavioral_patterns.items()
            if v.confidence > 0.7 or v.observed_count > 3
        }
        
        # Get primary relationships
        key_relationships = {
            k: v for k, v in self.relationships.items()
            if v.strength > 0.6 or k in self.primary_connections
        }
        
        return {
            "identity_id": self.identity_id,
            "preferred_name": self.preferred_name,
            "behavioral_patterns": [
                {
                    "category": p.category.value,
                    "pattern": p.pattern,
                    "confidence": p.confidence,
                    "times_observed": p.observed_count
                }
                for p in significant_patterns.values()
            ],
            "key_relationships": [
                {
                    "other_id": k,
                    "type": v.relation_type.value,
                    "strength": v.strength,
                    "contexts": v.contexts,
                    "last_interaction": v.last_interaction
                }
                for k, v in key_relationships.items()
            ],
            "recent_emotions": self.recent_emotions[-5:],
            "trust_score": self.trust_score,
            "memory_count": len(self.memory_index)
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert container to dictionary for storage."""
        return {
            "identity_id": self.identity_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "name_variants": self.name_variants,
            "preferred_name": self.preferred_name,
            "description": self.description,
            "contact_info": self.contact_info,
            "important_dates": self.important_dates,
            "notes": self.notes,
            "behavioral_patterns": {
                k: {
                    "category": v.category.value,
                    "pattern": v.pattern,
                    "confidence": v.confidence,
                    "observed_count": v.observed_count,
                    "last_observed": v.last_observed,
                    "contexts": v.contexts,
                    "related_identities": v.related_identities
                }
                for k, v in self.behavioral_patterns.items()
            },
            "relationships": {
                k: {
                    "relation_type": v.relation_type.value,
                    "strength": v.strength,
                    "contexts": v.contexts,
                    "shared_memories": v.shared_memories,
                    "last_interaction": v.last_interaction,
                    "behavior_notes": v.behavior_notes,
                    "trust_score": v.trust_score
                }
                for k, v in self.relationships.items()
            },
            "primary_connections": list(self.primary_connections),
            "permissions": self.permissions,
            "access_level": self.access_level,
            "trust_score": self.trust_score,
            "verification_required": self.verification_required,
            "memory_index": self.memory_index,
            "semantic_index": self.semantic_index,
            "transcript_index": self.transcript_index,
            "proactive_rules": self.proactive_rules
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IdentityContainer":
        """Create container from dictionary."""
        container = cls(data["identity_id"])
        container.created_at = data.get("created_at", time.time())
        container.updated_at = data.get("updated_at", time.time())
        container.name_variants = data.get("name_variants", [])
        container.preferred_name = data.get("preferred_name", "")
        container.description = data.get("description", "")
        container.contact_info = data.get("contact_info", {})
        container.important_dates = data.get("important_dates", {})
        container.notes = data.get("notes", [])
        
        # Reconstruct behavioral patterns
        for k, v in data.get("behavioral_patterns", {}).items():
            container.behavioral_patterns[k] = BehavioralPattern(
                category=BehaviorCategory(v["category"]),
                pattern=v["pattern"],
                confidence=v["confidence"],
                observed_count=v["observed_count"],
                last_observed=v["last_observed"],
                contexts=v.get("contexts", []),
                related_identities=v.get("related_identities", [])
            )
        
        # Reconstruct relationships
        for k, v in data.get("relationships", {}).items():
            container.relationships[k] = RelationshipContext(
                relation_type=RelationType(v["relation_type"]),
                strength=v["strength"],
                contexts=v["contexts"],
                shared_memories=v["shared_memories"],
                last_interaction=v["last_interaction"],
                behavior_notes=v.get("behavior_notes", []),
                trust_score=v.get("trust_score", 0.5)
            )
        
        container.primary_connections = set(data.get("primary_connections", []))
        container.permissions = data.get("permissions", {})
        container.access_level = data.get("access_level", "public")
        container.trust_score = data.get("trust_score", 0.5)
        container.verification_required = data.get("verification_required", True)
        container.memory_index = data.get("memory_index", [])
        container.semantic_index = data.get("semantic_index", [])
        container.transcript_index = data.get("transcript_index", [])
        container.proactive_rules = data.get("proactive_rules", {
            "notification_preferences": {},
            "quiet_hours": None,
            "urgency_patterns": [],
            "response_templates": {}
        })
        
        return container