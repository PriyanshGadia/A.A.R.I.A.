# clear_adapter_cache.py
import asyncio
from llm_adapter import LLMAdapterFactory

async def clear_cache_and_test():
    """Clear the adapter cache and test with fresh configuration"""
    print("🔄 Clearing adapter cache...")
    
    # Clear all cached adapter instances
    LLMAdapterFactory._instances.clear()
    print("✅ Adapter cache cleared")
    
    # Now test with fresh configuration
    from dotenv import load_dotenv
    load_dotenv("llm.env")
    
    from llm_adapter import LLMProvider, LLMRequest
    
    print("\n🧪 Testing with fresh configuration...")
    async with LLMAdapterFactory.get_adapter(LLMProvider.GROQ) as adapter:
        print(f"API Base: {adapter.config.api_base}")
        print(f"Chat URL: {adapter._get_chat_url()}")
        
        # Test chat
        request = LLMRequest(
            messages=[{"role": "user", "content": "Say only 'FRESH_WORKS'"}],
            max_tokens=10,
            temperature=0.1
        )
        response = await adapter.chat(request)
        print(f"Response: '{response.content}'")

if __name__ == "__main__":
    asyncio.run(clear_cache_and_test())