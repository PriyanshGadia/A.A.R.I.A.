"""Test suite for the enhanced memory system"""
import pytest
import asyncio
import time
from typing import Dict, Any
from ..memory_hierarchy import MemoryHierarchy, MemoryLevel, MemoryCategory
from ..identity_container import IdentityContainer, RelationType, BehaviorCategory
from ..memory_adapter import MemorySystemAdapter
from ..memory_manager import MemoryManager

class MockAssistantCore:
    """Mock assistant core for testing"""
    def __init__(self):
        self.store = {}
    
    async def put(self, key: str, type_name: str, value: Any):
        self.store[key] = value
    
    async def get(self, key: str):
        return self.store.get(key)

@pytest.fixture
async def memory_hierarchy():
    return MemoryHierarchy()

@pytest.fixture
async def memory_adapter():
    core = MockAssistantCore()
    return MemorySystemAdapter(
        assistant_core=core,
        use_new_system=True
    )

@pytest.mark.asyncio
async def test_identity_container_behavioral_analysis():
    """Test behavioral pattern analysis in identity container"""
    container = IdentityContainer("test_user")
    
    # Test emotional analysis
    await container.analyze_behavior(
        "I am very happy with the results!",
        {"context": "feedback"}
    )
    
    patterns = container.behavioral_patterns
    assert any(
        p.category == BehaviorCategory.EMOTIONAL and "happy" in p.pattern
        for p in patterns.values()
    )
    
    # Test communication style
    await container.analyze_behavior(
        "Could you please formally process this request?",
        {"context": "business"}
    )
    
    assert any(
        p.category == BehaviorCategory.COMMUNICATION and "formal" in p.pattern
        for p in patterns.values()
    )

@pytest.mark.asyncio
async def test_memory_hierarchy_storage_and_retrieval():
    """Test basic storage and retrieval in memory hierarchy"""
    hierarchy = MemoryHierarchy()
    
    # Store test memory
    memory_id = await hierarchy.create_memory(
        content={"test": "data"},
        level=MemoryLevel.ACCESS,
        category=MemoryCategory.INTERACTION,
        identity_id="test_user"
    )
    
    # Retrieve and verify
    result = await hierarchy.get_memory(
        node_id=memory_id,
        requester_id="test_user"
    )
    
    assert result is not None
    assert result["content"]["test"] == "data"
    assert result["level"] == MemoryLevel.ACCESS.value

@pytest.mark.asyncio
async def test_memory_hierarchy_access_control():
    """Test access control in memory hierarchy"""
    hierarchy = MemoryHierarchy()
    
    # Create confidential memory
    memory_id = await hierarchy.create_memory(
        content={"sensitive": "data"},
        level=MemoryLevel.CONFIDENTIAL,
        category=MemoryCategory.INTERACTION,
        identity_id="owner"
    )
    
    # Test owner access
    owner_result = await hierarchy.get_memory(
        node_id=memory_id,
        requester_id="owner"
    )
    assert owner_result is not None
    
    # Test unauthorized access
    unauth_result = await hierarchy.get_memory(
        node_id=memory_id,
        requester_id="other_user"
    )
    assert unauth_result is None

@pytest.mark.asyncio
async def test_memory_adapter_migration():
    """Test migration process in memory adapter"""
    core = MockAssistantCore()
    adapter = MemorySystemAdapter(
        assistant_core=core,
        use_new_system=False
    )
    
    # Store test data in old system
    mem_id, segment = await adapter.store_memory(
        user_input="Test input",
        assistant_response="Test response",
        subject_identity_id="test_user"
    )
    
    # Start migration
    migration_result = await adapter.start_migration()
    assert migration_result["status"] == "completed"
    
    # Verify data in new system
    adapter.use_new_system = True
    memories = await adapter.retrieve_memories(
        query_text="Test",
        subject_identity_id="test_user"
    )
    
    assert len(memories) > 0
    assert memories[0]["user"] == "Test input"
    assert memories[0]["assistant"] == "Test response"

@pytest.mark.asyncio
async def test_memory_adapter_fallback():
    """Test fallback behavior in memory adapter"""
    core = MockAssistantCore()
    adapter = MemorySystemAdapter(
        assistant_core=core,
        use_new_system=True
    )
    
    # Simulate failure in new system
    adapter.new_hierarchy = None
    
    # Should fallback to old system
    mem_id, segment = await adapter.store_memory(
        user_input="Fallback test",
        assistant_response="Testing fallback",
        subject_identity_id="test_user"
    )
    
    assert mem_id is not None
    assert segment is not None

@pytest.mark.asyncio
async def test_behavioral_pattern_persistence():
    """Test persistence of behavioral patterns across sessions"""
    hierarchy = MemoryHierarchy()
    
    # Create and analyze identity
    identity_id = "test_behavioral"
    container = IdentityContainer(identity_id)
    await container.analyze_behavior(
        "I urgently need this done ASAP!",
        {"context": "request"}
    )
    
    hierarchy.identities[identity_id] = container
    
    # Get summary and verify patterns
    summary = await hierarchy.get_identity_summary(
        identity_id=identity_id,
        requester_id="system"
    )
    
    assert summary is not None
    behavioral_summary = summary["identity"]
    patterns = behavioral_summary["behavioral_patterns"]
    
    assert any(
        p["pattern"] == "Shows urgent tendencies"
        for p in patterns
    )

@pytest.mark.asyncio
async def test_relationship_context_updates():
    """Test relationship context management"""
    container = IdentityContainer("test_relationships")
    
    # Update relationship
    await container.update_relationship(
        other_id="friend1",
        interaction_type="positive",
        context={
            "context": "social",
            "behavior_note": "Very supportive",
            "trust_impact": 0.1
        }
    )
    
    # Verify relationship data
    assert "friend1" in container.relationships
    relationship = container.relationships["friend1"]
    assert relationship.strength > 0.5  # Should increase after positive interaction
    assert "social" in relationship.contexts
    assert "Very supportive" in relationship.behavior_notes
    assert relationship.trust_score > 0.5

@pytest.mark.asyncio
async def test_memory_search_with_concepts():
    """Test semantic memory search capabilities"""
    hierarchy = MemoryHierarchy()
    
    # Store memories with concepts
    await hierarchy.create_memory(
        content={
            "text": "Discussion about AI ethics",
            "concepts": ["AI", "ethics", "technology"]
        },
        level=MemoryLevel.ACCESS,
        category=MemoryCategory.SEMANTIC,
        identity_id="test_user"
    )
    
    # Search by concept
    results = await hierarchy.search_memories(
        requester_id="test_user",
        identity_id="test_user",
        concepts=["AI", "ethics"]
    )
    
    assert len(results) > 0
    assert "AI" in results[0]["content"]["concepts"]
    assert "ethics" in results[0]["content"]["concepts"]

if __name__ == "__main__":
    pytest.main([__file__])