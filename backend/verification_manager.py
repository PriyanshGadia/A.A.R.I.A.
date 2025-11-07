"""
verification_manager.py - Enhanced Multi-Factor Verification System
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, Tuple
from enum import Enum
from device_types import DeviceType

logger = logging.getLogger("AARIA.Verification")

class VerificationLevel(Enum):
    NONE = "none"           # No verification needed (trusted environments)
    LOW = "low"             # Single factor (TOTP or biometric)
    MEDIUM = "medium"       # Single strong factor (biometric) OR two factors
    HIGH = "high"           # Biometric + behavioral analysis
    STRICT = "strict"       # Multi-factor required (biometric + TOTP + behavioral)

class VerificationManager:
    """
    Manages multi-factor verification based on device context and risk assessment
    """
    
    def __init__(self, core):
        self.core = core
        self.verification_attempts: Dict[str, int] = {}
        self.lockout_until: Dict[str, float] = {}
        self._initialized = False
    
    async def initialize(self):
        """Initialize verification manager"""
        await self._load_verification_state()
        self._initialized = True
        logger.info("🔐 Verification Manager initialized")
    
    async def _load_verification_state(self):
        """Load verification state from storage"""
        try:
            state = await self.core.store.get("verification_state") or {}
            self.verification_attempts = state.get("attempts", {})
            self.lockout_until = state.get("lockouts", {})
        except Exception as e:
            logger.warning(f"Failed to load verification state: {e}")
    
    async def get_required_verification_level(self, device_type: DeviceType, 
                                            access_level: str,
                                            sensitive_operation: bool = False) -> VerificationLevel:
        """Determine required verification level based on context"""
        
        # Base level from device type
        base_levels = {
            DeviceType.HOME_PC: VerificationLevel.NONE,      # Trusted environment
            DeviceType.TRUSTED_MOBILE: VerificationLevel.MEDIUM,  # Biometric OR TOTP
            DeviceType.UNTRUSTED_REMOTE: VerificationLevel.STRICT, # Multi-factor
            DeviceType.SOCIAL_PLATFORM: VerificationLevel.STRICT,  # Multi-factor
            DeviceType.PUBLIC_TERMINAL: VerificationLevel.STRICT   # Multi-factor
        }
        
        level = base_levels.get(device_type, VerificationLevel.STRICT)
        
        # Adjust based on access level and sensitive operations
        if access_level == "owner_root" and sensitive_operation:
            level = VerificationLevel.STRICT  # Always strict for sensitive owner operations
        
        return level
    
    async def perform_verification(self, identity, device_type: DeviceType, 
                                 request_data: Dict[str, Any],
                                 sensitive_operation: bool = False) -> Tuple[bool, str]:
        """
        Perform context-aware multi-factor verification
        """
        # Check for lockout first
        user_id = identity.identity_id
        if await self._is_locked_out(user_id):
            return False, "Account temporarily locked due to failed verification attempts"
        
        # Get required verification level
        access_level = "owner_root" if identity.relationship == "owner" else "public"
        required_level = await self.get_required_verification_level(
            device_type, access_level, sensitive_operation
        )
        
        logger.info(f"🔐 Verification required: {required_level.value} for {identity.preferred_name}")
        
        # Perform verification based on required level
        verification_result = await self._execute_verification_flow(
            required_level, request_data, identity
        )
        
        if verification_result:
            await self._record_successful_verification(user_id, device_type)
            return True, "Verification successful"
        else:
            await self._record_failed_attempt(user_id)
            return False, f"Verification failed for level: {required_level.value}"
    
    async def _execute_verification_flow(self, level: VerificationLevel,
                                       request_data: Dict[str, Any],
                                       identity) -> bool:
        """Execute the appropriate verification flow"""
        
        if level == VerificationLevel.NONE:
            return True  # No verification needed
        
        elif level == VerificationLevel.LOW:
            # Single factor: TOTP OR biometric
            return (await self._verify_totp(request_data) or 
                    await self._verify_biometric(request_data))
        
        elif level == VerificationLevel.MEDIUM:
            # Single strong factor OR two factors
            strong_biometric = await self._verify_strong_biometric(request_data)
            if strong_biometric:
                return True
            
            # Two factors: biometric + TOTP
            return (await self._verify_biometric(request_data) and 
                    await self._verify_totp(request_data))
        
        elif level == VerificationLevel.HIGH:
            # Biometric + behavioral
            return (await self._verify_biometric(request_data) and 
                    await self._verify_behavioral(request_data, identity))
        
        elif level == VerificationLevel.STRICT:
            # Multi-factor: biometric + TOTP + behavioral
            return (await self._verify_biometric(request_data) and 
                    await self._verify_totp(request_data) and
                    await self._verify_behavioral(request_data, identity))
        
        return False
    
    async def _verify_totp(self, request_data: Dict[str, Any]) -> bool:
        """Verify Time-based One-Time Password"""
        # Integrate with existing TOTP system
        from identity_manager import IdentityManager
        return await IdentityManager._verify_totp(self, request_data)
    
    async def _verify_biometric(self, request_data: Dict[str, Any]) -> bool:
        """Verify biometric data"""
        biometric_data = request_data.get("biometric_data")
        if not biometric_data:
            return False
        
        # Integrate with biometric verification system
        # For now, simulate verification
        return bool(biometric_data.get("verified", False))
    
    async def _verify_strong_biometric(self, request_data: Dict[str, Any]) -> bool:
        """Verify strong biometric (facial/retina)"""
        biometric_data = request_data.get("biometric_data")
        if not biometric_data:
            return False
        
        # Strong biometric methods
        strong_methods = {"facial", "retina", "fingerprint"}
        method = biometric_data.get("method", "")
        
        if method in strong_methods and biometric_data.get("verified"):
            confidence = biometric_data.get("confidence", 0)
            return confidence >= 0.85  # High confidence threshold
        
        return False
    
    async def _verify_behavioral(self, request_data: Dict[str, Any], identity) -> bool:
        """Verify behavioral patterns"""
        behavior_data = request_data.get("behavior_profile")
        if not behavior_data:
            return False
        
        # Check typing patterns, interaction timing, etc.
        # This would integrate with behavioral analysis system
        typical_typing_speed = behavior_data.get("typing_speed", 0)
        interaction_pattern = behavior_data.get("interaction_pattern", "")
        
        # Simple behavioral check (would be more sophisticated)
        return (10 <= typical_typing_speed <= 100 and 
                interaction_pattern in ["familiar", "consistent"])
    
    async def _is_locked_out(self, user_id: str) -> bool:
        """Check if user is temporarily locked out"""
        lockout_time = self.lockout_until.get(user_id, 0)
        if time.time() < lockout_time:
            return True
        
        # Clear expired lockout
        if user_id in self.lockout_until:
            del self.lockout_until[user_id]
            await self._save_verification_state()
        
        return False
    
    async def _record_successful_verification(self, user_id: str, device_type: DeviceType):
        """Record successful verification"""
        # Clear failed attempts
        if user_id in self.verification_attempts:
            del self.verification_attempts[user_id]
        
        # Update device verification timestamp
        if hasattr(self.core, 'device_manager'):
            await self.core.device_manager.update_device_verification(user_id)
        
        await self._save_verification_state()
    
    async def _record_failed_attempt(self, user_id: str):
        """Record failed verification attempt"""
        self.verification_attempts[user_id] = self.verification_attempts.get(user_id, 0) + 1
        
        # Lockout after 3 failed attempts for 15 minutes
        if self.verification_attempts[user_id] >= 3:
            self.lockout_until[user_id] = time.time() + 900  # 15 minutes
            logger.warning(f"🔒 User {user_id} locked out for 15 minutes")
        
        await self._save_verification_state()
    
    async def _save_verification_state(self):
        """Save verification state to storage"""
        try:
            state = {
                "attempts": self.verification_attempts,
                "lockouts": self.lockout_until
            }
            await self.core.store.put("verification_state", "verification", state)
        except Exception as e:
            logger.warning(f"Failed to save verification state: {e}")