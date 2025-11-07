"""
device_types.py - Enhanced Device Type Classification
"""

from enum import Enum
from typing import Dict, Any, Set, Optional
import asyncio
import logging
import hashlib
import ipaddress

logger = logging.getLogger("AARIA.Device")

class DeviceType(Enum):
    HOME_PC = "home_pc"              # Full root access - trusted environment
    TRUSTED_MOBILE = "trusted_mobile" # Limited owner access - verified mobile
    UNTRUSTED_REMOTE = "untrusted_remote" # Public access until verified
    SOCIAL_PLATFORM = "social_platform"   # Public only - external platforms
    PUBLIC_TERMINAL = "public_terminal"   # Kiosk mode - minimal access

class DeviceManager:
    """
    Manages device identification and classification without hardcoding
    """
    
    def __init__(self, core):
        self.core = core
        self.trusted_networks: Set[str] = set()
        self.device_fingerprints: Dict[str, Dict[str, Any]] = {}
        self._initialized = False
    
    async def initialize(self):
        """Initialize device manager with persisted data"""
        await self._load_trusted_networks()
        await self._load_device_fingerprints()
        self._initialized = True
        logger.info("🔧 Device Manager initialized")
    
    async def _load_trusted_networks(self):
        """Load trusted networks from security config"""
        try:
            security_config = await self.core.store.get("security_config") or {}
            networks = security_config.get("trusted_networks", [])
            self.trusted_networks = set(networks)
            logger.info(f"📡 Loaded {len(self.trusted_networks)} trusted networks")
        except Exception as e:
            logger.warning(f"Failed to load trusted networks: {e}")
            # Default trusted networks (home/office)
            self.trusted_networks = {"192.168.1.0/24", "10.0.0.0/24"}
    
    async def _load_device_fingerprints(self):
        """Load known device fingerprints"""
        try:
            fingerprints = await self.core.store.get("device_fingerprints") or {}
            self.device_fingerprints = fingerprints
            logger.info(f"📱 Loaded {len(self.device_fingerprints)} device fingerprints")
        except Exception as e:
            logger.warning(f"Failed to load device fingerprints: {e}")
            self.device_fingerprints = {}
    
    async def identify_device_type(self, request_data: Dict[str, Any]) -> DeviceType:
        """
        Identify device type using multiple factors without hardcoding
        """
        # Extract device information
        device_id = request_data.get("device_id", "unknown")
        ip_address = request_data.get("ip_address")
        user_agent = request_data.get("user_agent", "")
        source = request_data.get("source", "").lower()
        
        # Check for social platforms first
        if await self._is_social_platform(source, user_agent):
            return DeviceType.SOCIAL_PLATFORM
        
        # Check for home PC (highest trust)
        if await self._is_home_pc(device_id, ip_address, user_agent, request_data):
            return DeviceType.HOME_PC
        
        # Check for trusted mobile
        if await self._is_trusted_mobile(device_id, ip_address, user_agent, request_data):
            return DeviceType.TRUSTED_MOBILE
        
        # Check for public terminal
        if await self._is_public_terminal(ip_address, user_agent):
            return DeviceType.PUBLIC_TERMINAL
        
        # Default to untrusted remote
        return DeviceType.UNTRUSTED_REMOTE
    
    async def _is_social_platform(self, source: str, user_agent: str) -> bool:
        """Identify social media platforms"""
        social_indicators = {
            "twitter", "facebook", "instagram", "linkedin", "webhook",
            "social", "messenger", "whatsapp", "telegram"
        }
        
        source_lower = source.lower()
        user_agent_lower = user_agent.lower()
        
        return any(indicator in source_lower or indicator in user_agent_lower 
                  for indicator in social_indicators)
    
    async def _is_home_pc(self, device_id: str, ip_address: str, user_agent: str, 
                         request_data: Dict[str, Any]) -> bool:
        """Identify home PC using multiple trust factors"""
        trust_score = 0
        
        # Factor 1: Trusted device ID
        if device_id in self.device_fingerprints:
            device_info = self.device_fingerprints[device_id]
            if device_info.get("type") == "home_pc":
                trust_score += 3
        
        # Factor 2: Trusted network
        if await self._is_trusted_network(ip_address):
            trust_score += 2
        
        # Factor 3: Biometric verification
        if request_data.get("biometric_verified"):
            trust_score += 2
        
        # Factor 4: Behavioral patterns
        if await self._matches_home_behavior(request_data):
            trust_score += 1
        
        # Factor 5: Recent verification
        if await self._has_recent_verification(device_id):
            trust_score += 1
        
        return trust_score >= 5  # High threshold for home PC
    
# In device_types.py, update the _is_trusted_mobile method:

    async def _is_trusted_mobile(self, device_id: str, ip_address: str, user_agent: str,
                            request_data: Dict[str, Any]) -> bool:
        """Identify trusted mobile devices with better logic"""
        trust_score = 0
        
        # Factor 1: Known mobile device with mobile type
        if device_id in self.device_fingerprints:
            device_info = self.device_fingerprints[device_id]
            if device_info.get("type") in ["mobile", "tablet"]:
                trust_score += 2
            else:
                # If device is registered as non-mobile, don't classify as mobile
                return False
        
        # Factor 2: Strong mobile user agent indicators
        user_agent_lower = user_agent.lower()
        mobile_indicators = ["mobile", "android", "iphone", "ipad", "ios"]
        strong_mobile_indicators = ["iphone", "ipad", "android"]
        
        if any(indicator in user_agent_lower for indicator in strong_mobile_indicators):
            trust_score += 2
        elif any(indicator in user_agent_lower for indicator in mobile_indicators):
            trust_score += 1
        
        # Factor 3: Biometric verification
        if request_data.get("biometric_verified"):
            trust_score += 2
        
        # Factor 4: Recent TOTP verification
        if await self._has_recent_totp_verification(device_id):
            trust_score += 1
        
        # Factor 5: Trusted network (but less important for mobile)
        if await self._is_trusted_network(ip_address):
            trust_score += 1
        
        # Mobile should NOT have home PC characteristics
        has_home_pc_indicators = any(indicator in user_agent_lower for indicator in 
                                    ["windows", "macintosh", "linux", "desktop"])
        if has_home_pc_indicators:
            trust_score -= 2  # Penalize home PC indicators
        
        return trust_score >= 4  # Higher threshold for trusted mobile

    
    async def _is_public_terminal(self, ip_address: str, user_agent: str) -> bool:
        """Identify public terminals/kiosks"""
        # Check for public IP ranges
        if ip_address and await self._is_public_ip_range(ip_address):
            return True
        
        # Check for kiosk user agents
        kiosk_indicators = ["kiosk", "public", "terminal", "info", "display"]
        if any(indicator in user_agent.lower() for indicator in kiosk_indicators):
            return True
        
        return False
    
    async def _is_trusted_network(self, ip_address: str) -> bool:
        """Check if IP is in trusted network range"""
        if not ip_address:
            return False
        
        try:
            ip = ipaddress.ip_address(ip_address)
            for network_str in self.trusted_networks:
                network = ipaddress.ip_network(network_str, strict=False)
                if ip in network:
                    return True
        except Exception as e:
            logger.debug(f"Network check failed for {ip_address}: {e}")
        
        return False
    
    async def _is_public_ip_range(self, ip_address: str) -> bool:
        """Check if IP is in public range (not private)"""
        try:
            ip = ipaddress.ip_address(ip_address)
            return not (ip.is_private or ip.is_loopback or ip.is_link_local)
        except:
            return True  # Assume public if we can't parse
    
    async def _matches_home_behavior(self, request_data: Dict[str, Any]) -> bool:
        """Check for home environment behavioral patterns"""
        # Check for typical home usage patterns
        time_of_day = request_data.get("timestamp")
        frequency = request_data.get("request_frequency")
        
        # This would integrate with behavioral analysis system
        # For now, return True if we have consistent patterns
        return True  # Placeholder
    
    async def _has_recent_verification(self, device_id: str) -> bool:
        """Check if device has recent verification"""
        try:
            device_info = self.device_fingerprints.get(device_id, {})
            last_verified = device_info.get("last_verified", 0)
            return (asyncio.get_event_loop().time() - last_verified) < 86400  # 24 hours
        except:
            return False
    
    async def _has_recent_totp_verification(self, device_id: str) -> bool:
        """Check for recent TOTP verification"""
        try:
            verifications = await self.core.store.get("totp_verifications") or {}
            device_verification = verifications.get(device_id, {})
            last_totp = device_verification.get("last_verified", 0)
            return (asyncio.get_event_loop().time() - last_totp) < 300  # 5 minutes
        except:
            return False
    
    async def register_device(self, device_id: str, device_type: str, 
                            metadata: Dict[str, Any] = None):
        """Register a new device with fingerprint"""
        self.device_fingerprints[device_id] = {
            "type": device_type,
            "first_seen": asyncio.get_event_loop().time(),
            "last_seen": asyncio.get_event_loop().time(),
            "metadata": metadata or {},
            "verification_count": 0
        }
        await self._save_device_fingerprints()
        logger.info(f"📱 Registered device: {device_id} as {device_type}")
    
    async def update_device_verification(self, device_id: str):
        """Update device verification timestamp"""
        if device_id in self.device_fingerprints:
            self.device_fingerprints[device_id].update({
                "last_verified": asyncio.get_event_loop().time(),
                "verification_count": self.device_fingerprints[device_id].get("verification_count", 0) + 1
            })
            await self._save_device_fingerprints()
    
    async def _save_device_fingerprints(self):
        """Save device fingerprints to storage"""
        try:
            await self.core.store.put("device_fingerprints", "device_management", self.device_fingerprints)
        except Exception as e:
            logger.warning(f"Failed to save device fingerprints: {e}")
    
    async def add_trusted_network(self, network: str):
        """Add a trusted network range"""
        self.trusted_networks.add(network)
        await self._save_trusted_networks()
    
    async def _save_trusted_networks(self):
        """Save trusted networks to security config"""
        try:
            security_config = await self.core.store.get("security_config") or {}
            security_config["trusted_networks"] = list(self.trusted_networks)
            await self.core.store.put("security_config", "security", security_config)
        except Exception as e:
            logger.warning(f"Failed to save trusted networks: {e}")
    
    def generate_device_fingerprint(self, request_data: Dict[str, Any]) -> str:
        """Generate a unique device fingerprint"""
        components = [
            request_data.get("user_agent", ""),
            request_data.get("ip_address", ""),
            request_data.get("accept_language", ""),
            request_data.get("screen_resolution", ""),
            request_data.get("timezone", "")
        ]
        fingerprint_string = "|".join(str(c) for c in components)
        return hashlib.sha256(fingerprint_string.encode()).hexdigest()[:16]