# test_groq_adapter_fixed.py
import asyncio
import os
from dotenv import load_dotenv

async def test_fixed_groq_adapter():
    """Test the fixed Groq adapter configuration"""
    load_dotenv("llm.env")
    
    print("🚀 Testing Fixed Groq Adapter")
    print("=" * 40)
    
    try:
        from llm_adapter import LLMAdapterFactory, LLMProvider, LLMRequest
        
        # Test 1: Check configuration
        print("1. Testing adapter configuration...")
        async with LLMAdapterFactory.get_adapter(LLMProvider.GROQ) as adapter:
            print(f"   ✅ Adapter created successfully")
            print(f"   API Base: {adapter.config.api_base}")
            print(f"   Model: {adapter.config.model}")
            
            # Test 2: Health check
            print("\n2. Testing health check...")
            health = await adapter.health_check()
            print(f"   Health status: {health.status.value}")
            print(f"   Model available: {health.model_available}")
            print(f"   Available models: {health.available_models[:3]}...")  # First 3 models
            
            # Test 3: Simple chat
            print("\n3. Testing chat completion...")
            request = LLMRequest(
                messages=[{"role": "user", "content": "Say only the word 'ADAPTER_WORKS'"}],
                max_tokens=10,
                temperature=0.1
            )
            response = await adapter.chat(request)
            print(f"   ✅ Chat successful!")
            print(f"   Response: '{response.content}'")
            print(f"   Provider: {response.provider.value}")
            print(f"   Model: {response.model}")
            print(f"   Latency: {response.latency:.3f}s")
            print(f"   Tokens used: {response.tokens_used}")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_fixed_groq_adapter())