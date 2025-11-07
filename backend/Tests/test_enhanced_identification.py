"""
Test script for enhanced verification system
"""

import asyncio
import logging
from device_types import DeviceManager, DeviceType
from verification_manager import VerificationManager, VerificationLevel

logging.basicConfig(level=logging.INFO)

class TestCore:
    def __init__(self):
        self._store = {}
        self.store = self  # Add this for compatibility
    
    async def put(self, key, category, value):
        self._store[(key, category)] = value
        return True
    
    async def get(self, key, category=None):
        if category is None:
            for (k, c), v in self._store.items():
                if k == key:
                    return v
            return None
        return self._store.get((key, category))

async def test_device_identification():
    print("🧪 Testing Enhanced Device Identification - FIXED")
    print("=" * 50)
    
    core = TestCore()
    device_manager = DeviceManager(core)
    await device_manager.initialize()
    
    # Pre-register devices for more accurate testing
    await device_manager.register_device("iphone_123", "mobile", {
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
        "screen_resolution": "1125x2436"
    })
    
    # Add trusted networks
    await device_manager.add_trusted_network("192.168.1.0/24")
    
    # Test cases
    test_cases = [
        {
            "name": "Home PC",
            "data": {
                "device_id": "home_pc_001",
                "ip_address": "192.168.1.100",
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "biometric_verified": True
            },
            "expected": DeviceType.HOME_PC
        },
        {
            "name": "Trusted Mobile",
            "data": {
                "device_id": "iphone_123",  # Pre-registered
                "ip_address": "192.168.1.50", 
                "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
                "biometric_verified": True
            },
            "expected": DeviceType.TRUSTED_MOBILE
        },
        {
            "name": "Untrusted Mobile",
            "data": {
                "device_id": "unknown_android",
                "ip_address": "8.8.8.8",  # Public IP
                "user_agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36",
                "biometric_verified": False
            },
            "expected": DeviceType.UNTRUSTED_REMOTE
        },
        {
            "name": "Social Platform",
            "data": {
                "source": "twitter",
                "user_agent": "TwitterBot/1.0",
                "ip_address": "199.16.156.210"  # Twitter IP range
            },
            "expected": DeviceType.SOCIAL_PLATFORM
        },
        {
            "name": "Public Terminal",
            "data": {
                "user_agent": "Public Kiosk Browser 1.0",
                "ip_address": "203.0.113.45"  # Public IP
            },
            "expected": DeviceType.PUBLIC_TERMINAL
        }
    ]
    
    for test_case in test_cases:
        device_type = await device_manager.identify_device_type(test_case["data"])
        status = "✅" if device_type == test_case["expected"] else "❌"
        print(f"{status} {test_case['name']}: {device_type.value} (expected: {test_case['expected'].value})")
    
    print("\n🎉 Enhanced device identification test completed!")

class TestIdentity:
    def __init__(self, identity_id, relationship="public", preferred_name="Test User"):
        self.identity_id = identity_id
        self.relationship = relationship
        self.preferred_name = preferred_name

async def test_enhanced_verification():
    print("🧪 Testing Enhanced Verification System")
    print("=" * 50)
    
    core = TestCore()  # Using the fixed TestCore from earlier
    verification_manager = VerificationManager(core)
    await verification_manager.initialize()
    
    # Test verification levels for different scenarios
    test_scenarios = [
        {
            "name": "Home PC - Owner",
            "device_type": DeviceType.HOME_PC,
            "access_level": "owner_root",
            "sensitive": False,
            "expected": VerificationLevel.NONE
        },
        {
            "name": "Home PC - Sensitive Operation", 
            "device_type": DeviceType.HOME_PC,
            "access_level": "owner_root", 
            "sensitive": True,
            "expected": VerificationLevel.MEDIUM
        },
        {
            "name": "Trusted Mobile - Owner",
            "device_type": DeviceType.TRUSTED_MOBILE,
            "access_level": "owner_remote",
            "sensitive": False,
            "expected": VerificationLevel.MEDIUM
        },
        {
            "name": "Untrusted Remote - Public",
            "device_type": DeviceType.UNTRUSTED_REMOTE, 
            "access_level": "public",
            "sensitive": False,
            "expected": VerificationLevel.STRICT
        },
        {
            "name": "Social Platform - Public",
            "device_type": DeviceType.SOCIAL_PLATFORM,
            "access_level": "public", 
            "sensitive": False,
            "expected": VerificationLevel.STRICT
        }
    ]
    
    for scenario in test_scenarios:
        level = await verification_manager.get_required_verification_level(
            scenario["device_type"],
            scenario["access_level"], 
            scenario["sensitive"]
        )
        status = "✅" if level == scenario["expected"] else "❌"
        print(f"{status} {scenario['name']}: {level.value} (expected: {scenario['expected'].value})")
    
    print("\n🎉 Enhanced verification test completed!")

if __name__ == "__main__":
    asyncio.run(test_enhanced_verification())