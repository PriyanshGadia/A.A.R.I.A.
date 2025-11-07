"""
Integration test for the enhanced security system
"""

import asyncio
import logging
from main import AARIASystem

logging.basicConfig(level=logging.INFO)

async def test_integrated_system():
    print("🧪 Testing Integrated Enhanced Security System")
    print("=" * 50)
    
    try:
        # Initialize the full system
        system = AARIASystem()
        await system.initialize()
        
        print("✅ System initialized successfully")
        
        # Test enhanced security flow
        test_request = {
            "query": "What's my current location?",
            "user_id": "owner_primary", 
            "device_id": "cli_terminal",
            "ip_address": "127.0.0.1",
            "source": "private_terminal",
            "biometric_verified": True
        }
        
        security_context, identity = await system.components['security'].process_security_flow(test_request)
        
        print(f"✅ Enhanced security flow completed:")
        print(f"   User: {identity.preferred_name}")
        print(f"   Access Level: {security_context.user_identity.access_level.value}")
        print(f"   Verified: {security_context.is_verified}")
        print(f"   Data Categories: {len(security_context.requested_data_categories)}")
        
        # Test device management
        from device_commands import DeviceCommands
        device_commands = DeviceCommands(system.components['security'])
        
        # Test device registration
        result = await device_commands.process_device_command("register_device", {
            "device_id": "test_phone",
            "type": "mobile"
        }, identity)
        print(f"✅ Device registration: {result}")
        
        # Test device listing
        result = await device_commands.process_device_command("list_devices", {}, identity)
        print(f"✅ Device listing: {len(result.splitlines()) - 1} devices")
        
        await system.shutdown()
        print("🎉 Integrated system test completed successfully!")
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_integrated_system())