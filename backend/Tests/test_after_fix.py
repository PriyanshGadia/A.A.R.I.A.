# test_after_fix.py
import asyncio
from llm_adapter import LLMAdapterFactory, LLMProvider, LLMRequest

async def test_after_fix():
    """Test after applying the URL fix"""
    print("🧪 Testing after URL fix...")
    
    # Clear cache first
    LLMAdapterFactory._instances.clear()
    
    async with LLMAdapterFactory.get_adapter(LLMProvider.GROQ) as adapter:
        print(f"API Base: {adapter.config.api_base}")
        print(f"Chat URL: {adapter._get_chat_url()}")
        
        # Test health check
        health = await adapter.health_check()
        print(f"Health status: {health.status.value}")
        
        # Test chat
        request = LLMRequest(
            messages=[{"role": "user", "content": "Say only 'FIXED_WORKS'"}],
            max_tokens=10
        )
        response = await adapter.chat(request)
        print(f"✅ Response: '{response.content}'")

if __name__ == "__main__":
    asyncio.run(test_after_fix())