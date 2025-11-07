# test_final_groq.py
import asyncio
import os
from dotenv import load_dotenv

async def test_final_groq():
    """Final test with corrected configuration"""
    load_dotenv("llm.env")
    
    print("🚀 Final Groq Test")
    print("=" * 40)
    
    # Clear cache first
    from llm_adapter import LLMAdapterFactory
    LLMAdapterFactory._instances.clear()
    
    # Test with explicit configuration
    from llm_adapter import LLMProvider, LLMRequest
    
    print("1. Testing with explicit configuration...")
    async with LLMAdapterFactory.get_adapter(
        LLMProvider.GROQ,
        api_base="https://api.groq.com/openai/v1",
        model="llama-3.1-8b-instant"
    ) as adapter:
        print(f"   API Base: {adapter.config.api_base}")
        print(f"   Chat URL: {adapter._get_chat_url()}")
        
        # Test health check
        print("\n2. Testing health check...")
        health = await adapter.health_check()
        print(f"   Health: {health.status.value}")
        
        # Test chat
        print("\n3. Testing chat...")
        request = LLMRequest(
            messages=[{"role": "user", "content": "Say only 'FINAL_WORKS'"}],
            max_tokens=10,
            temperature=0.1
        )
        response = await adapter.chat(request)
        print(f"   ✅ Response: '{response.content}'")
        print(f"   Latency: {response.latency:.3f}s")

if __name__ == "__main__":
    asyncio.run(test_final_groq())