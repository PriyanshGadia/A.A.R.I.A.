# test_aaria_groq.py
import asyncio
import os
from dotenv import load_dotenv

async def test_aaria_with_groq():
    """Test A.A.R.I.A with Groq as primary provider"""
    load_dotenv("llm.env")
    
    print("🚀 Testing A.A.R.I.A with Groq")
    print("=" * 40)
    
    # Test basic response
    try:
        from interaction_core import InboundMessage
        from llm_adapter import LLMAdapterFactory, LLMProvider
        
        # Test direct Groq adapter first
        print("1. Testing Groq adapter directly...")
        async with LLMAdapterFactory.get_adapter(LLMProvider.GROQ) as adapter:
            health = await adapter.health_check()
            if health.status.value == "healthy":
                print("   ✅ Groq adapter is healthy")
            else:
                print(f"   ⚠️  Groq adapter status: {health.status.value}")
                
        # Test simple interaction
        print("\n2. Testing simple interaction...")
        from persona_core import PersonaCore
        from assistant_core import AssistantCore
        
        assistant = AssistantCore(
            password=os.getenv("AARIA_MASTER_PASSWORD"), 
            auto_recover=True
        )
        await assistant.initialize()
        
        persona = PersonaCore(core=assistant)
        await persona.initialize()
        
        response = await persona.respond("Hello! Please respond with just 'GROQ_WORKS'")
        print(f"   Response: {response}")
        
        await assistant.close()
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_aaria_with_groq())