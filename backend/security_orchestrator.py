"""
security_orchestrator.py - Unified Security System Integration
"""

import logging
import asyncio
import uuid
import hologram_state
from typing import Dict, Any, Tuple, Optional
from access_control import AccessControlSystem, SecurityContext, AccessLevel
from identity_manager import IdentityManager, IdentityProfile

logger = logging.getLogger("AARIA.Security")

class SecurityOrchestrator:
    """
    Orchestrates the complete security flow:
    1. Identity Verification -> 2. Access Control -> 3. Data Authorization
    """
    
    def __init__(self, core):
        self.core = core
        self.access_control = AccessControlSystem(core)
        self.identity_manager = IdentityManager(core)
        self._initialized = False
    
    async def initialize(self):
        """Initialize both security systems"""
        await self.access_control.initialize()
        await self.identity_manager.initialize()
        self._initialized = True
        logger.info("🛡️ Security Orchestrator initialized")    
    
    async def _apply_immediate_personalization(self, identity: IdentityProfile):
        """Apply owner personalization immediately across system"""
        try:
            # Update Persona Core with owner's preferred name
            if hasattr(self.core, 'persona') and self.core.persona:
                await self.core.persona.update_user_profile(
                    name=identity.preferred_name,
                    timezone="UTC"  # Could get from identity metadata
                )
            
            # Update interaction core session metadata
            if hasattr(self.core, 'interaction') and self.core.interaction:
                # This ensures new sessions use the correct name immediately
                pass  # Will be handled in session creation
                
            logger.info(f"👤 Applied immediate personalization for: {identity.preferred_name}")
            
        except Exception as e:
            logger.warning(f"Personalization application failed: {e}")

    
    async def get_security_status(self) -> Dict[str, Any]:
        """Get comprehensive security status"""
        access_summary = await self.access_control.get_access_summary()
        identity_summary = await self.identity_manager.get_identity_summary()
        
        return {
            "access_control": access_summary,
            "identity_management": identity_summary,
            "overall_status": "healthy" if access_summary and identity_summary else "degraded"
        }
    
    async def process_security_command(self, command: str, params: Dict[str, Any], 
                                     requester_identity: IdentityProfile) -> str:
        """
        Process security-related commands
        Routes to appropriate subsystem
        """
        # Convert IdentityProfile to UserIdentity for access control
        from access_control import UserIdentity
        
        user_identity = UserIdentity(
            user_id=requester_identity.identity_id,
            name=requester_identity.name,
            preferred_name=requester_identity.preferred_name,
            access_level=AccessLevel.OWNER_ROOT if requester_identity.relationship == "owner" else AccessLevel.PRIVILEGED,
            privileges={"full_system_access"} if requester_identity.relationship == "owner" else set(),
            verification_required=False
        )
        
        # Route command to appropriate subsystem
        if command.startswith("access_"):
            return await self.access_control.process_management_command(command, params, user_identity)
        elif command.startswith("identity_"):
            return await self.identity_manager.process_identity_command(command.replace("identity_", ""), params, requester_identity)
        else:
            return f"❌ Unknown security command: {command}"
        
    async def process_security_flow_enhanced(self, request_data: Dict[str, Any]) -> Tuple[SecurityContext, IdentityProfile]:
        """
        [MODIFIED METHOD]
        Enhanced security processing with device awareness and hologram integration.
        """
        if not self._initialized:
            raise RuntimeError("Security orchestrator not initialized")

        # --- NEW: Hologram Task Tracking ---
        node_id = f"sec_check_{uuid.uuid4().hex[:6]}"
        link_id = f"link_sec_{node_id}"
        
        try:
            # --- NEW: Spawn "Security Check" node ---
            await hologram_state.spawn_and_link(
                node_id=node_id, 
                node_type="security", 
                label=f"Security Check: {request_data.get('source', 'unknown')}", 
                size=5,
                source_id="SecurityCore", # Linked from Security
                link_id=link_id
            )
            await hologram_state.set_node_active(node_id) # Transition to teal
            await hologram_state.set_node_active("SecurityCore")
            # --- End New Block ---
            
            # (Original method logic starts here)
            from security_flow import DynamicSecurityFlow
            security_flow = DynamicSecurityFlow(
                self.access_control, 
                self.identity_manager, 
                self.security_config
            )
            
            authorized_categories, flow_path, access_level = await security_flow.process_flow(request_data)
            
            identity, is_verified = await self.identity_manager.identify_and_verify_user(request_data)
            
            from access_control import SecurityContext, RequestSource, UserIdentity
            
            access_level_enum = {
                "root": AccessLevel.OWNER_ROOT,
                "owner": AccessLevel.OWNER_REMOTE, 
                "privileged": AccessLevel.PRIVILEGED,
                "public": AccessLevel.PUBLIC
            }.get(access_level, AccessLevel.PUBLIC)
            
            user_identity = UserIdentity(
                user_id=identity.identity_id,
                name=identity.name,
                preferred_name=identity.preferred_name,
                access_level=access_level_enum,
                privileges=set(authorized_categories),
                verification_required=False
            )
            
            security_context = SecurityContext(
                request_source=RequestSource.PRIVATE_TERMINAL if "Private Terminal" in flow_path else RequestSource.REMOTE_TERMINAL,
                user_identity=user_identity,
                requested_data_categories=list(authorized_categories),
                requires_verification=False,
                is_verified=is_verified
            )
            
            if identity.relationship == "owner" and is_verified:
                await self._apply_immediate_personalization(identity)
            
            logger.info(f"🔐 Enhanced security flow: {flow_path}")
            return security_context, identity
            # (End of original method logic)
            
        except Exception as e:
            # --- NEW: Set node to ERROR state on failure ---
            await hologram_state.set_node_error(node_id)
            logger.error(f"Enhanced security flow failed: {e}")
            # Fallback to basic flow
            return await self.process_security_flow_basic(request_data)
            # --- End New Block ---

        finally:
            # --- NEW: Despawn node ---
            await hologram_state.set_node_idle("SecurityCore")
            await hologram_state.despawn_and_unlink(node_id, link_id)
            # --- End New Block ---
    
    async def _basic_fallback_flow(self, request_data: Dict[str, Any]) -> Tuple[SecurityContext, IdentityProfile]:
        """Basic fallback security flow"""
        identity, is_verified = await self.identity_manager.identify_and_verify_user(request_data)
        security_context = await self.access_control.process_request_flow(request_data)
        security_context.is_verified = is_verified
        return security_context, identity

    # Alias for compatibility
    process_security_flow_basic = _basic_fallback_flow

    async def process_security_flow_basic(self, request_data: Dict[str, Any]) -> Tuple[SecurityContext, IdentityProfile]:
        """Basic fallback security flow when enhanced methods fail"""
        try:
            # Basic identification
            identity, is_verified = await self.identity_manager.identify_and_verify_user(request_data)
            
            # Create basic security context
            from access_control import SecurityContext, RequestSource, UserIdentity
            
            # Determine access level based on identity
            if identity.relationship == "owner":
                access_level = AccessLevel.OWNER_ROOT
            elif identity.identity_id in self.access_control.privileged_users:
                access_level = AccessLevel.PRIVILEGED
            else:
                access_level = AccessLevel.PUBLIC
            
            user_identity = UserIdentity(
                user_id=identity.identity_id,
                name=identity.name,
                preferred_name=identity.preferred_name,
                access_level=access_level,
                privileges=set(),
                verification_required=False
            )
            
            # Get basic data categories
            authorized_categories = await self.security_config.get_authorized_categories(
                access_level.value.lower() if hasattr(access_level, 'value') else "public",
                set()
            )
            
            security_context = SecurityContext(
                request_source=RequestSource.PRIVATE_TERMINAL,  # Default to private
                user_identity=user_identity,
                requested_data_categories=list(authorized_categories),
                requires_verification=False,
                is_verified=is_verified
            )
            
            return security_context, identity
            
        except Exception as e:
            logger.error(f"Basic security flow failed: {e}")
            # Ultimate fallback
            from identity_manager import IdentityProfile, IdentityState
            fallback_identity = IdentityProfile(
                identity_id="fallback",
                name="Fallback User",
                preferred_name="User",
                verification_methods=set(),
                identity_state=IdentityState.UNVERIFIED,
                last_verified=0,
                verification_expires=0,
                relationship="public"
            )
            
            from access_control import SecurityContext, RequestSource, UserIdentity
            fallback_context = SecurityContext(
                request_source=RequestSource.PUBLIC_API,
                user_identity=UserIdentity(
                    user_id="fallback",
                    name="Fallback User",
                    preferred_name="User",
                    access_level=AccessLevel.PUBLIC,
                    privileges=set(),
                    verification_required=True
                ),
                requested_data_categories=[],
                requires_verification=True,
                is_verified=False
            )
            
            return fallback_context, fallback_identity

    # Main method uses enhanced with fallbacks
    async def process_security_flow(self, request_data: Dict[str, Any]) -> Tuple[SecurityContext, IdentityProfile]:
        """Main entry point with proper fallbacks"""
        try:
            # Try enhanced flow first
            if hasattr(self, 'process_security_flow_enhanced'):
                return await self.process_security_flow_enhanced(request_data)
            else:
                # Fallback to basic flow
                return await self.process_security_flow_basic(request_data)
        except Exception as e:
            logger.error(f"Security flow failed, using basic fallback: {e}")
            return await self.process_security_flow_basic(request_data)