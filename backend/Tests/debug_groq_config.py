# debug_groq_config.py
import asyncio
import os
from dotenv import load_dotenv

async def debug_groq_config():
    """Debug the Groq configuration issue"""
    load_dotenv("llm.env")
    
    print("🔍 Debugging Groq Configuration")
    print("=" * 40)
    
    # Check environment variables
    print("1. Environment Variables:")
    print(f"   GROQ_API_BASE: {os.getenv('GROQ_API_BASE')}")
    print(f"   GROQ_API_KEY: {'*' * 10 if os.getenv('GROQ_API_KEY') else 'NOT SET'}")
    print(f"   GROQ_MODEL: {os.getenv('GROQ_MODEL')}")
    
    from llm_adapter import LLMAdapterFactory, LLMProvider
    
    # Check what URL the adapter is actually using
    print("\n2. Adapter Configuration:")
    async with LLMAdapterFactory.get_adapter(LLMProvider.GROQ) as adapter:
        print(f"   Actual API Base: {adapter.config.api_base}")
        print(f"   Actual Model: {adapter.config.model}")
        
        # Check the actual URL being called
        print(f"   Chat URL: {adapter._get_chat_url()}")
        print(f"   Models URL: {adapter._get_models_url()}")

if __name__ == "__main__":
    asyncio.run(debug_groq_config())