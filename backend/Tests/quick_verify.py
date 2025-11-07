# quick_verify.py
import asyncio
import os
from dotenv import load_dotenv

async def quick_verify():
    """Quick verification that everything is working"""
    load_dotenv("llm.env")
    
    print("🔍 Quick System Verification")
    print("=" * 40)
    
    # Clear cache
    from llm_adapter import LLMAdapterFactory
    LLMAdapterFactory._instances.clear()
    
    # Test LLM adapter
    from llm_adapter import LLMProvider, LLMRequest
    
    print("1. LLM Adapter Test:")
    async with LLMAdapterFactory.get_adapter(LLMProvider.GROQ) as adapter:
        request = LLMRequest(
            messages=[{"role": "user", "content": "Say 'READY'"}],
            max_tokens=10
        )
        response = await adapter.chat(request)
        print(f"   ✅ {response.content}")
    
    # Test storage
    print("\n2. Storage Test:")
    from secure_store import SecureStorageAsync
    async with SecureStorageAsync() as store:
        if not store.has_wrapped_master():
            store.create_and_store_master_key(os.getenv("AARIA_MASTER_PASSWORD"))
        else:
            store.unlock_with_password(os.getenv("AARIA_MASTER_PASSWORD"))
        
        await store.put("test_ready", "system", {"status": "ready", "timestamp": "now"})
        result = await store.get("test_ready")
        print(f"   ✅ Storage: {result['status']}")
    
    print("\n🎉 System is ready to use with Groq!")

if __name__ == "__main__":
    asyncio.run(quick_verify())