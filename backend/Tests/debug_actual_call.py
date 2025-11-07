# debug_actual_call.py
import asyncio
import aiohttp
import json
import os
from dotenv import load_dotenv

async def debug_actual_api_call():
    """Debug the actual API call being made"""
    load_dotenv("llm.env")
    
    api_key = os.getenv("GROQ_API_KEY")
    api_base = "https://api.groq.com/openai/v1"  # Correct base URL
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Test the exact URL the adapter is trying to use
    test_url = api_base + "/chat/completions"
    print(f"Testing URL: {test_url}")
    
    payload = {
        "model": "llama-3.1-8b-instant",
        "messages": [{"role": "user", "content": "Say only 'DEBUG_WORKS'"}],
        "temperature": 0.1,
        "max_tokens": 10
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(test_url, headers=headers, json=payload) as response:
                print(f"Status: {response.status}")
                if response.status == 200:
                    result = await response.json()
                    print(f"✅ SUCCESS! Response: {result['choices'][0]['message']['content']}")
                else:
                    error_text = await response.text()
                    print(f"❌ FAILED! Error: {error_text}")
                    
    except Exception as e:
        print(f"❌ Exception: {e}")

if __name__ == "__main__":
    asyncio.run(debug_actual_api_call())