# verify_fixes.py
import asyncio
import os
from dotenv import load_dotenv

async def verify_all_fixes():
    """Verify all the fixes are working"""
    load_dotenv("llm.env")
    
    print("🔍 Verifying fixes...")
    
    # Test 1: Storage API
    print("1. Testing Storage API...")
    from secure_store import SecureStorageAsync
    async with SecureStorageAsync() as store:
        if not store.has_wrapped_master():
            store.create_and_store_master_key(os.getenv("AARIA_MASTER_PASSWORD"))
        else:
            store.unlock_with_password(os.getenv("AARIA_MASTER_PASSWORD"))
        
        await store.put("verify_test", "test_type", {"status": "working"})
        result = await store.get("verify_test")
        print(f"   ✅ Storage API: {result['status']}")
    
    # Test 2: Import all modules
    print("2. Testing module imports...")
    try:
        from persona_core import PersonaCore
        from cognition_core import CognitionCore
        from autonomy_core import AutonomyCore
        from interaction_core import InteractionCore
        from assistant_core import AssistantCore
        print("   ✅ All modules imported successfully")
    except ImportError as e:
        print(f"   ❌ Import failed: {e}")
        return
    
    # Test 3: Check fixed methods exist
    print("3. Checking fixed methods...")
    try:
        # Check if PersonaCore has initialize method
        if hasattr(PersonaCore, 'initialize'):
            print("   ✅ PersonaCore.initialize method exists")
        else:
            print("   ❌ PersonaCore.initialize method missing")
            
        # Check if AutonomyCore has initialize method  
        if hasattr(AutonomyCore, 'initialize'):
            print("   ✅ AutonomyCore.initialize method exists")
        else:
            print("   ❌ AutonomyCore.initialize method missing")
            
    except Exception as e:
        print(f"   ❌ Method check failed: {e}")
    
    print("🎉 Verification complete!")

if __name__ == "__main__":
    asyncio.run(verify_all_fixes())