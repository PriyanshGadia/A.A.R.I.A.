# verify_groq_endpoints.py
import asyncio
import aiohttp
import os
from dotenv import load_dotenv

async def verify_groq_endpoints():
    """Verify the exact Groq API endpoints"""
    load_dotenv("llm.env")
    
    api_key = os.getenv("GROQ_API_KEY")
    base_url = "https://api.groq.com"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    print("🔍 Verifying Groq API Endpoints")
    print("=" * 40)
    
    # Test different endpoint variations
    endpoints = [
        "/openai/v1/chat/completions",
        "/openai/chat/completions", 
        "/v1/chat/completions",
        "/chat/completions"
    ]
    
    for endpoint in endpoints:
        test_url = base_url + endpoint
        print(f"\nTesting: {test_url}")
        
        payload = {
            "model": "llama-3.1-8b-instant",
            "messages": [{"role": "user", "content": "test"}],
            "max_tokens": 5
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(test_url, headers=headers, json=payload, timeout=10) as response:
                    print(f"  Status: {response.status}")
                    if response.status == 200:
                        print(f"  ✅ WORKS!")
                        break
                    else:
                        print(f"  ❌ {response.status}")
        except Exception as e:
            print(f"  ❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(verify_groq_endpoints())