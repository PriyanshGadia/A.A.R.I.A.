# test_groq.py
import asyncio
import os
import aiohttp
import json
from dotenv import load_dotenv

async def test_groq():
    """Test Groq API connectivity"""
    load_dotenv("llm.env")
    
    api_key = os.getenv("GROQ_API_KEY")
    api_base = os.getenv("GROQ_API_BASE", "https://api.groq.com/openai/v1")
    
    if not api_key:
        print("❌ GROQ_API_KEY not found in environment variables")
        print("Please add your Groq API key to llm.env:")
        print("GROQ_API_KEY=your_actual_api_key_here")
        return
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Test 1: List available models
    print("1. Testing Groq API connectivity...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{api_base}/models", headers=headers) as response:
                if response.status == 200:
                    models_data = await response.json()
                    available_models = [model["id"] for model in models_data.get("data", [])]
                    print(f"✅ Groq API connected successfully!")
                    print(f"   Available models: {available_models}")
                else:
                    print(f"❌ Groq API returned status {response.status}")
                    error_text = await response.text()
                    print(f"   Error: {error_text}")
                    return
    except Exception as e:
        print(f"❌ Cannot connect to Groq API: {e}")
        return
    
    # Test 2: Simple chat completion
    print("\n2. Testing chat completion...")
    try:
        test_payload = {
            "model": "llama-3.1-8b-instant",
            "messages": [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Say only the word 'SUCCESS'"}
            ],
            "temperature": 0.1,
            "max_tokens": 10
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{api_base}/chat/completions", 
                                  headers=headers, 
                                  json=test_payload) as response:
                if response.status == 200:
                    result = await response.json()
                    content = result["choices"][0]["message"]["content"].strip()
                    print(f"✅ Chat completion successful!")
                    print(f"   Response: '{content}'")
                    print(f"   Model: {result.get('model', 'unknown')}")
                    print(f"   Tokens used: {result.get('usage', {}).get('total_tokens', 'unknown')}")
                else:
                    print(f"❌ Chat completion failed with status {response.status}")
                    error_text = await response.text()
                    print(f"   Error: {error_text}")
                    
    except Exception as e:
        print(f"❌ Chat completion test failed: {e}")

async def test_groq_via_adapter():
    """Test Groq using the LLM adapter"""
    print("\n3. Testing via LLM Adapter...")
    try:
        from llm_adapter import LLMAdapterFactory, LLMProvider, LLMRequest
        
        async with LLMAdapterFactory.get_adapter(LLMProvider.GROQ) as adapter:
            # Test health check
            health = await adapter.health_check()
            print(f"✅ Adapter health: {health.status.value}")
            print(f"   Model available: {health.model_available}")
            print(f"   Latency: {health.latency:.3f}s")
            
            # Test simple chat
            request = LLMRequest(
                messages=[{"role": "user", "content": "Say only the word 'ADAPTER_SUCCESS'"}],
                max_tokens=10,
                temperature=0.1
            )
            response = await adapter.chat(request)
            print(f"✅ Adapter chat successful!")
            print(f"   Response: '{response.content}'")
            print(f"   Provider: {response.provider.value}")
            print(f"   Model: {response.model}")
            print(f"   Latency: {response.latency:.3f}s")
            
    except Exception as e:
        print(f"❌ Adapter test failed: {e}")
        print("   Make sure GROQ_API_KEY is set in llm.env")

if __name__ == "__main__":
    print("🚀 Testing Groq Connectivity")
    print("=" * 40)
    asyncio.run(test_groq())
    asyncio.run(test_groq_via_adapter())