# test_security.py
import asyncio
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

from access_control import AccessControlSystem, AccessLevel, UserIdentity

class MockCore:
    def __init__(self):
        self._storage = {}
    
    async def get(self, key):
        return self._storage.get(key)
    
    async def put(self, key, category, value):
        self._storage[key] = value

async def test_access_control():
    """Test that all command handlers exist and work"""
    print("🧪 Testing Access Control System...")
    
    core = MockCore()
    access_control = AccessControlSystem(core)
    
    # Test initialization
    await access_control.initialize()
    print("✅ Initialization successful")
    
    # Test that all command handlers exist
    test_commands = [
        "add_privileged_user",
        "remove_privileged_user", 
        "list_privileged_users",
        "update_privileges",
        "set_public_data",
        "add_trusted_device",
        "remove_trusted_device",
        "list_trusted_devices",
        "set_owner_identifier"
    ]
    
    for command in test_commands:
        handler = access_control.management_commands.get(command)
        if handler:
            print(f"✅ Command handler found: {command}")
        else:
            print(f"❌ MISSING command handler: {command}")
            return False
    
    # Test processing a request flow
    test_request = {
        "query": "What's my current location?",
        "user_id": "test_user",
        "source": "private_terminal",
        "device_id": "test_device"
    }
    
    try:
        context = await access_control.process_request_flow(test_request)
        print(f"✅ Request flow processed: {context.user_identity.access_level.value}")
    except Exception as e:
        print(f"❌ Request flow failed: {e}")
        return False
    
    # Test command processing
    mock_owner = UserIdentity(
        user_id="test_owner",
        name="Test Owner",
        access_level=AccessLevel.OWNER_ROOT,
        privileges={"full_system_access"},
        verification_required=False
    )
    
    try:
        result = await access_control.process_management_command("list_privileged_users", {}, mock_owner)
        print(f"✅ Command processing test: {result[:50]}...")
    except Exception as e:
        print(f"❌ Command processing failed: {e}")
        return False
    
    print("🎉 All access control tests passed!")
    return True

if __name__ == "__main__":
    result = asyncio.run(test_access_control())
    sys.exit(0 if result else 1)