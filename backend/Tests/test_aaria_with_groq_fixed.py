# test_aaria_with_groq_fixed.py
import asyncio
import os
from dotenv import load_dotenv

async def test_aaria_with_groq_fixed():
    """Test A.A.R.I.A with Groq as the primary LLM provider - FIXED VERSION"""
    load_dotenv("llm.env")
    
    print("🚀 Testing A.A.R.I.A with Groq - FIXED")
    print("=" * 40)
    
    # Clear LLM adapter cache to ensure fresh configuration
    from llm_adapter import LLMAdapterFactory
    LLMAdapterFactory._instances.clear()
    
    try:
        from interaction_core import InboundMessage
        from persona_core import PersonaCore
        from assistant_core import AssistantCore
        
        # Initialize assistant core
        print("1. Initializing Assistant Core...")
        assistant = AssistantCore(
            password=os.getenv("AARIA_MASTER_PASSWORD"), 
            auto_recover=True
        )
        await assistant.initialize()
        
        # Initialize persona core
        print("2. Initializing Persona Core...")
        persona = PersonaCore(core=assistant)
        await persona.initialize()
        
        # Test simple interaction
        print("3. Testing interaction with Groq...")
        response = await persona.respond("Hello! Please respond with just 'AARIA_GROQ_WORKS'")
        print(f"   ✅ Response: {response}")
        
        # Test with interaction core - WITH GROQ CONFIG
        print("\n4. Testing with Interaction Core (configured for Groq)...")
        from interaction_core import create_interaction_core
        from cognition_core import CognitionCore
        from autonomy_core import create_autonomy_core
        
        cognition = CognitionCore(
            persona=persona,
            core=assistant,
            autonomy=None,
            config={"max_memory_entries": 100, "rate_limit": 50}
        )
        
        autonomy = await create_autonomy_core(persona, assistant)
        await autonomy.initialize()
        
        cognition.autonomy = autonomy
        
        # CONFIGURE INTERACTION CORE TO USE GROQ FIRST
        interaction = await create_interaction_core(
            persona=persona,
            cognition=cognition, 
            autonomy=autonomy,
            config={
                "session_ttl": 3600,
                "rate_limit_per_minute": 60,
                "autosave_interval": 60,
                "primary_provider": "groq"  # This ensures Groq is tried first
            }
        )
        
        # Test inbound message
        inbound_msg = InboundMessage(
            channel="test",
            content="Say only 'INTERACTION_WORKS'",
            user_id="test_user"
        )
        
        outbound_msg = await interaction.handle_inbound(inbound_msg)
        print(f"   ✅ Interaction Response: {outbound_msg.content}")
        
        # Test multiple interactions to verify memory storage
        print("\n5. Testing memory storage...")
        inbound_msg2 = InboundMessage(
            channel="test", 
            content="What was the last thing I asked you to say?",
            user_id="test_user"
        )
        
        outbound_msg2 = await interaction.handle_inbound(inbound_msg2)
        print(f"   ✅ Memory Response: {outbound_msg2.content}")
        
        # Cleanup
        await interaction.shutdown()
        await autonomy.shutdown()
        await assistant.close()
        
        print("\n🎉 SUCCESS! A.A.R.I.A is working with Groq and all fixes applied!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_aaria_with_groq_fixed())