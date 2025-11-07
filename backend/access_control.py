"""
access_control.py - Dynamic Access Control with Command-Driven User Management
FIXED VERSION - Command handlers defined before reference
"""

from __future__ import annotations
import logging
import asyncio
import hashlib
import re
import json
import uuid
import hologram_state
from typing import Dict, List, Optional, Set, Any, Tuple
from enum import Enum
from dataclasses import dataclass
from device_types import DeviceManager, DeviceType

logger = logging.getLogger("AARIA.AccessControl")

class AccessLevel(Enum):
    OWNER_ROOT = "owner_root"
    OWNER_REMOTE = "owner_remote" 
    PRIVILEGED = "privileged"
    PUBLIC = "public"

class RequestSource(Enum):
    PRIVATE_TERMINAL = "private_terminal"
    REMOTE_TERMINAL = "remote_terminal"
    SOCIAL_APP = "social_app"
    PHONE_CALL = "phone_call"
    PUBLIC_API = "public_api"

@dataclass
class UserIdentity:
    user_id: str
    name: str
    access_level: AccessLevel
    privileges: Set[str]
    verification_required: bool = False
    relationship: str = "unknown"
    preferred_name: str = ""  # Added for personalization

@dataclass
class SecurityContext:
    request_source: RequestSource
    user_identity: UserIdentity
    requested_data_categories: List[str]
    requires_verification: bool = False
    is_verified: bool = False

class AccessControlSystem:
    """
    Dynamic access control with command-driven user management
    FIXED: Command handlers defined before being referenced
    """
    
    def __init__(self, core):
        self.core = core
        self.device_manager = DeviceManager(core)
        self.owner_identifiers: Set[str] = set()
        self.privileged_users: Dict[str, UserIdentity] = {}
        self.public_data_categories: Set[str] = set()
        self.trusted_devices: Set[str] = set()
        
        # Data classification according to flowchart
        self.confidential_categories = {
            "location", "personal_contacts", "financial_info", "health_data",
            "private_calendar", "credentials", "system_config", "family_details",
            "current_whereabouts", "emergency_contacts"
        }
        
        self.privileged_categories = {
            "current_whereabouts", "family_info", "limited_calendar",
            "emergency_contacts", "health_alerts", "location_updates"
        }
        
        self.public_categories = {
            "company_name", "public_profile", "professional_info",
            "social_media_info", "public_contact", "business_hours"
        }
        
        # Initialize command handlers AFTER they're defined
        self._initialized = False
    
    async def initialize(self):
        """Initialize access control system"""
        # Define command handlers here after all methods are defined
        await self.device_manager.initialize()
        self.management_commands = {
            "add_privileged_user": self._handle_add_privileged_user,
            "remove_privileged_user": self._handle_remove_privileged_user,
            "list_privileged_users": self._handle_list_privileged_users,
            "update_privileges": self._handle_update_privileges,
            "set_public_data": self._handle_set_public_data,
            "add_trusted_device": self._handle_add_trusted_device,
            "remove_trusted_device": self._handle_remove_trusted_device,
            "list_trusted_devices": self._handle_list_trusted_devices,
            "set_owner_identifier": self._handle_set_owner_identifier
        }
        
        await self._load_security_config()
        self._initialized = True
        logger.info("🔐 Dynamic Access Control System initialized")
    
    async def _load_security_config(self):
        """Load security configuration from storage"""
        try:
            config = await self.core.store.get("security_config") or {}
            
            self.owner_identifiers = set(config.get("owner_identifiers", ["owner_default"]))
            self.trusted_devices = set(config.get("trusted_devices", []))
            
            # Load privileged users
            privileged_users_data = config.get("privileged_users", {})
            for user_id, user_data in privileged_users_data.items():
                self.privileged_users[user_id] = UserIdentity(
                    user_id=user_id,
                    name=user_data["name"],
                    access_level=AccessLevel.PRIVILEGED,
                    privileges=set(user_data["privileges"]),
                    verification_required=user_data.get("verification_required", True),
                    relationship=user_data.get("relationship", "unknown"),
                    preferred_name=user_data.get("preferred_name", user_data["name"])
                )
            
            self.public_data_categories = set(config.get("public_data_categories", self.public_categories))
            
            logger.info(f"✅ Loaded {len(self.privileged_users)} privileged users, {len(self.trusted_devices)} trusted devices")
            
        except Exception as e:
            logger.error(f"Failed to load security config: {e}")
            # Initialize with defaults
            self.owner_identifiers = {"owner_default"}
    
    async def _save_security_config(self):
        """Save security configuration"""
        try:
            privileged_users_data = {}
            for user_id, identity in self.privileged_users.items():
                privileged_users_data[user_id] = {
                    "name": identity.name,
                    "preferred_name": identity.preferred_name,
                    "privileges": list(identity.privileges),
                    "verification_required": identity.verification_required,
                    "relationship": identity.relationship
                }
            
            config = {
                "owner_identifiers": list(self.owner_identifiers),
                "privileged_users": privileged_users_data,
                "public_data_categories": list(self.public_data_categories),
                "trusted_devices": list(self.trusted_devices)
            }
            
            await self.core.store.put("security_config", "security", config)
            logger.debug("💾 Security configuration saved")
            
        except Exception as e:
            logger.error(f"Failed to save security config: {e}")
    
    # COMMAND HANDLERS - Defined before being referenced in initialize()
    async def _handle_add_privileged_user(self, params: Dict[str, Any], requester: UserIdentity) -> str:
        """Add privileged user: add_privileged_user name=Alice relationship=mother privileges=location,calendar"""
        name = params.get("name", "").strip()
        relationship = params.get("relationship", "").strip()
        privileges_str = params.get("privileges", "").strip()
        
        if not name:
            return "❌ Name is required for adding privileged user."
        
        privileges = set(privilege.strip() for privilege in privileges_str.split(",") if privilege.strip())
        valid_privileges = privileges.intersection(self.privileged_categories)
        
        if not valid_privileges:
            return f"❌ No valid privileges specified. Available: {', '.join(self.privileged_categories)}"
        
        user_id = self._generate_user_id(f"{name}_{relationship}")
        
        self.privileged_users[user_id] = UserIdentity(
            user_id=user_id,
            name=name,
            preferred_name=name,
            access_level=AccessLevel.PRIVILEGED,
            privileges=valid_privileges,
            verification_required=params.get("verification_required", "true").lower() == "true",
            relationship=relationship
        )
        
        await self._save_security_config()
        return f"✅ Added privileged user: {name} ({relationship}) with privileges: {', '.join(valid_privileges)}"
    
    async def _handle_remove_privileged_user(self, params: Dict[str, Any], requester: UserIdentity) -> str:
        """Remove privileged user: remove_privileged_user name=Alice"""
        name = params.get("name", "").strip()
        
        if not name:
            return "❌ Name is required for removing privileged user."
        
        # Find user by name
        user_to_remove = None
        for user_id, identity in self.privileged_users.items():
            if identity.name.lower() == name.lower():
                user_to_remove = user_id
                break
        
        if not user_to_remove:
            return f"❌ No privileged user found with name: {name}"
        
        removed_name = self.privileged_users[user_to_remove].name
        del self.privileged_users[user_to_remove]
        await self._save_security_config()
        
        return f"✅ Removed privileged user: {removed_name}"
    
    async def _handle_list_privileged_users(self, params: Dict[str, Any], requester: UserIdentity) -> str:
        """List all privileged users"""
        if not self.privileged_users:
            return "📋 No privileged users configured."
        
        user_list = []
        for identity in self.privileged_users.values():
            user_info = f"• {identity.name} ({identity.relationship}) - Privileges: {', '.join(identity.privileges)}"
            if identity.verification_required:
                user_info += " [Verification Required]"
            user_list.append(user_info)
        
        return "📋 Privileged Users:\n" + "\n".join(user_list)
    
    async def _handle_update_privileges(self, params: Dict[str, Any], requester: UserIdentity) -> str:
        """Update user privileges: update_privileges name=Alice privileges=location,contacts"""
        name = params.get("name", "").strip()
        privileges_str = params.get("privileges", "").strip()
        
        if not name or not privileges_str:
            return "❌ Name and privileges are required."
        
        # Find user
        target_user = None
        for identity in self.privileged_users.values():
            if identity.name.lower() == name.lower():
                target_user = identity
                break
        
        if not target_user:
            return f"❌ No privileged user found with name: {name}"
        
        # Update privileges
        new_privileges = set(privilege.strip() for privilege in privileges_str.split(",") if privilege.strip())
        valid_privileges = new_privileges.intersection(self.privileged_categories)
        
        if not valid_privileges:
            return f"❌ No valid privileges specified. Available: {', '.join(self.privileged_categories)}"
        
        target_user.privileges = valid_privileges
        await self._save_security_config()
        
        return f"✅ Updated privileges for {target_user.name}: {', '.join(valid_privileges)}"
    
    async def _handle_set_public_data(self, params: Dict[str, Any], requester: UserIdentity) -> str:
        """Set public data categories: set_public_data categories=company_name,professional_info"""
        categories_str = params.get("categories", "").strip()
        
        if not categories_str:
            return "❌ Categories are required."
        
        new_categories = set(category.strip() for category in categories_str.split(",") if category.strip())
        valid_categories = new_categories.intersection(self.public_categories)
        
        if not valid_categories:
            return f"❌ No valid categories specified. Available: {', '.join(self.public_categories)}"
        
        self.public_data_categories = valid_categories
        await self._save_security_config()
        
        return f"✅ Public data categories updated: {', '.join(valid_categories)}"
    
    async def _handle_add_trusted_device(self, params: Dict[str, Any], requester: UserIdentity) -> str:
        """Add trusted device: add_trusted_device device_id=home_pc_123"""
        device_id = params.get("device_id", "").strip()
        if not device_id:
            return "❌ Device ID is required."
        
        self.trusted_devices.add(device_id)
        await self._save_security_config()
        return f"✅ Added trusted device: {device_id}"
    
    async def _handle_remove_trusted_device(self, params: Dict[str, Any], requester: UserIdentity) -> str:
        """Remove trusted device: remove_trusted_device device_id=home_pc_123"""
        device_id = params.get("device_id", "").strip()
        if not device_id:
            return "❌ Device ID is required."
        
        if device_id in self.trusted_devices:
            self.trusted_devices.remove(device_id)
            await self._save_security_config()
            return f"✅ Removed trusted device: {device_id}"
        else:
            return f"❌ Device {device_id} not found in trusted devices."
    
    async def _handle_list_trusted_devices(self, params: Dict[str, Any], requester: UserIdentity) -> str:
        """List all trusted devices"""
        if not self.trusted_devices:
            return "📋 No trusted devices configured."
        
        device_list = [f"• {device_id}" for device_id in self.trusted_devices]
        return "📋 Trusted Devices:\n" + "\n".join(device_list)
    
    async def _handle_set_owner_identifier(self, params: Dict[str, Any], requester: UserIdentity) -> str:
        """Set owner identifier: set_owner_identifier user_id=owner_123"""
        user_id = params.get("user_id", "").strip()
        if not user_id:
            return "❌ User ID is required."
        
        self.owner_identifiers.add(user_id)
        await self._save_security_config()
        return f"✅ Added owner identifier: {user_id}"
    
    # FLOWCHART IMPLEMENTATION - Main Decision Flow
    async def process_request_flow(self, request_data: Dict[str, Any]) -> SecurityContext:
        """
        Implement the complete flowchart logic:
        1. Request Identification -> Owner vs Public
        2. Owner: Source Identification -> Private vs Remote Terminal  
        3. Public: Secondary Identification -> Privileged vs General
        4. Route to appropriate data access
        Enhanced flowchart implementation with device awareness
        """
        logger.info("🔄 Processing enhanced security flowchart...")
        
        # STEP 1: Request Identification
        user_identity = await self._identify_user(request_data)
        device_type = await self.device_manager.identify_device_type(request_data)
        request_source = await self._identify_request_source(request_data)
        data_categories = await self._extract_data_categories(request_data)
        
        logger.info(f"📋 Enhanced Identification: {user_identity.access_level.value} -> {device_type.value}")
        
        # STEP 2: Branch based on user type with device awareness
        if user_identity.access_level in [AccessLevel.OWNER_ROOT, AccessLevel.OWNER_REMOTE]:
            # OWNER BRANCH: Enhanced with device type
            context = await self._process_owner_request_enhanced(
                user_identity, device_type, request_source, data_categories, request_data
            )
        else:
            # PUBLIC BRANCH: Enhanced with device type
            context = await self._process_public_request_enhanced(
                user_identity, device_type, request_source, data_categories, request_data
            )
        
        logger.info(f"✅ Enhanced flow completed: {user_identity.access_level.value} -> {device_type.value} -> {len(data_categories)} categories")
        return context
    
    async def _process_owner_request_enhanced(self, user_identity: UserIdentity, 
                                            device_type: DeviceType,
                                            request_source: RequestSource,
                                            data_categories: List[str],
                                            request_data: Dict[str, Any]) -> SecurityContext:
        """Enhanced owner request processing with device awareness"""
        
        if device_type == DeviceType.HOME_PC:
            # Private Terminal → Root Database (full access)
            user_identity.access_level = AccessLevel.OWNER_ROOT
            requires_verification = False  # Trusted environment
            accessible_categories = await self._get_root_data_categories()
            
            # Register/update home PC device
            device_id = request_data.get("device_id")
            if device_id:
                await self.device_manager.register_device(device_id, "home_pc")
                await self.device_manager.update_device_verification(device_id)
                
        else:
            # Remote Terminal → Owner/Confidential Data (limited access)
            user_identity.access_level = AccessLevel.OWNER_REMOTE
            requires_verification = True
            accessible_categories = await self._get_owner_remote_categories()
            
            # Update verification for trusted mobile
            if device_type == DeviceType.TRUSTED_MOBILE:
                device_id = request_data.get("device_id")
                if device_id:
                    await self.device_manager.update_device_verification(device_id)
        
        return SecurityContext(
            request_source=request_source,
            user_identity=user_identity,
            requested_data_categories=accessible_categories,
            requires_verification=requires_verification,
            is_verified=not requires_verification
        )
    
    async def _process_public_request_enhanced(self, user_identity: UserIdentity,
                                             device_type: DeviceType,
                                             request_source: RequestSource,
                                             data_categories: List[str],
                                             request_data: Dict[str, Any]) -> SecurityContext:
        """Enhanced public request processing with device awareness"""
        
        # Adjust verification requirements based on device type
        if device_type == DeviceType.SOCIAL_PLATFORM:
            # Social platforms get minimal access
            user_identity.access_level = AccessLevel.PUBLIC
            requires_verification = False
            accessible_categories = self.public_data_categories
            
        elif user_identity.access_level == AccessLevel.PRIVILEGED:
            # Privileged users on any device
            is_authorized = await self._authorize_privileged_user(user_identity, data_categories)
            requires_verification = user_identity.verification_required
            accessible_categories = data_categories if is_authorized else self.public_data_categories
            
        else:
            # General public
            is_authorized = await self._authorize_public_user(data_categories)
            requires_verification = False
            accessible_categories = data_categories if is_authorized else self.public_data_categories
        
        return SecurityContext(
            request_source=request_source,
            user_identity=user_identity,
            requested_data_categories=accessible_categories,
            requires_verification=requires_verification,
            is_verified=not requires_verification
        )
    
    async def _get_root_data_categories(self) -> List[str]:
        """Get all data categories for root access"""
        from security_config import DynamicSecurityConfig
        security_config = self.core.security_config if hasattr(self.core, 'security_config') else None
        if security_config and hasattr(security_config, 'security_policy'):
            return list(security_config.security_policy.data_categories.keys())
        return list(self.confidential_categories | self.privileged_categories | self.public_categories)
    
    async def _get_owner_remote_categories(self) -> List[str]:
        """Get limited categories for remote owner access"""
        # Exclude sensitive system categories for remote access
        excluded_categories = {"system_config", "credentials", "root_database"}
        all_categories = await self._get_root_data_categories()
        return [cat for cat in all_categories if cat not in excluded_categories]

    
    async def authorize_data_access(self, context: SecurityContext) -> Tuple[bool, List[str]]:
        """
        [MODIFIED METHOD]
        Authorize data access based on security context.
        Spawns a hologram error node on failure.
        """
        
        if not context.is_verified and context.requires_verification:
            # --- NEW: Spawn "Access Denied" node ---
            logger.warning(f"Access DENIED for {context.user_identity.name}: Verification required.")
            node_id = f"access_denied_{uuid.uuid4().hex[:6]}"
            link_id = f"link_sec_{node_id}"
            try:
                await hologram_state.spawn_and_link(
                    node_id=node_id, node_type="security", label="DENIED: Verification Required", size=8,
                    source_id="SecurityCore", link_id=link_id
                )
                await hologram_state.set_node_error(node_id) # Set to RED
                await hologram_state.despawn_and_unlink(node_id, link_id)
            except Exception as e:
                logger.error(f"Failed to spawn hologram error node: {e}")
            # --- End New Block ---
            return False, []
        
        authorized_categories = []
        
        for category in context.requested_data_categories:
            if await self._is_category_authorized(category, context):
                authorized_categories.append(category)
        
        is_authorized = len(authorized_categories) > 0
        
        # --- NEW: Spawn "Access Denied" node ---
        if not is_authorized and context.requested_data_categories:
            # This triggers if they ask for data they don't have privileges for
            logger.warning(f"Access DENIED for {context.user_identity.name}: No authorized categories.")
            node_id = f"access_denied_{uuid.uuid4().hex[:6]}"
            link_id = f"link_sec_{node_id}"
            try:
                await hologram_state.spawn_and_link(
                    node_id=node_id, node_type="security", label="DENIED: No Privileges", size=8,
                    source_id="SecurityCore", link_id=link_id
                )
                await hologram_state.set_node_error(node_id) # Set to RED
                await hologram_state.despawn_and_unlink(node_id, link_id)
            except Exception as e:
                logger.error(f"Failed to spawn hologram error node: {e}")
        # --- End New Block ---
            
        return is_authorized, authorized_categories
    
    async def _is_category_authorized(self, category: str, context: SecurityContext) -> bool:
        """Check if category is authorized for given context"""
        if context.user_identity.access_level == AccessLevel.OWNER_ROOT:
            return True  # Full access from private terminal
        
        elif context.user_identity.access_level == AccessLevel.OWNER_REMOTE:
            return category not in ["system_config", "credentials"]  # Limited remote access
        
        elif context.user_identity.access_level == AccessLevel.PRIVILEGED:
            return category in context.user_identity.privileges
        
        else:  # PUBLIC
            return category in self.public_categories
    
    # User Identification Methods
    async def _identify_user(self, request_data: Dict[str, Any]) -> UserIdentity:
        """Identify user from request data - IMPLEMENTING FLOWCHART START"""
        user_id = request_data.get("user_id", "anonymous")
        auth_token = request_data.get("auth_token")
        device_id = request_data.get("device_id")
        
        # Check if owner (multiple verification methods)
        if await self._is_owner(user_id, auth_token, device_id, request_data):
            return UserIdentity(
                user_id=user_id,
                name="Owner",
                preferred_name=await self._get_owner_preferred_name(),
                access_level=AccessLevel.OWNER_ROOT,  # Default, may change based on source
                privileges={"full_system_access"},
                verification_required=False
            )
        
        # Check if privileged user
        if user_id in self.privileged_users:
            return self.privileged_users[user_id]
        
        # Default to public access
        return UserIdentity(
            user_id=user_id,
            name="Public User",
            preferred_name="User",
            access_level=AccessLevel.PUBLIC,
            privileges=self.public_data_categories,
            verification_required=True
        )
    
    async def _identify_request_source(self, request_data: Dict[str, Any]) -> RequestSource:
        """Identify request source according to flowchart"""
        source_info = request_data.get("source", "").lower()
        device_id = request_data.get("device_id")
        
        # Check if private terminal (trusted device)
        if device_id and device_id in self.trusted_devices:
            return RequestSource.PRIVATE_TERMINAL
        
        # Identify other sources
        if any(term in source_info for term in ["web", "mobile", "remote"]):
            return RequestSource.REMOTE_TERMINAL
        elif any(term in source_info for term in ["twitter", "facebook", "social", "instagram"]):
            return RequestSource.SOCIAL_APP
        elif any(term in source_info for term in ["phone", "call", "sms"]):
            return RequestSource.PHONE_CALL
        else:
            return RequestSource.PUBLIC_API
    
    async def _is_owner(self, user_id: str, auth_token: str, device_id: str, request_data: Dict[str, Any]) -> bool:
        """Check if request is from owner using multiple methods"""
        # Method 1: Direct user ID match
        if user_id in self.owner_identifiers:
            return True
        
        # Method 2: Trusted device
        if device_id and device_id in self.trusted_devices:
            return True
        
        # Method 3: Biometric verification passed
        if request_data.get("biometric_verified"):
            return True
        
        # Method 4: Session token validation
        if await self._validate_owner_session(auth_token):
            return True
        
        return False
    
    # Data Category Extraction
    async def _extract_data_categories(self, request_data: Dict[str, Any]) -> List[str]:
        """Extract data categories from request using NLP patterns"""
        query = request_data.get("query", "").lower()
        categories = []
        
        # Location-related queries
        location_terms = ["where", "location", "address", "current place", "whereabouts"]
        if any(term in query for term in location_terms):
            categories.extend(["location", "current_whereabouts"])
        
        # Personal contact queries
        contact_terms = ["contact", "email", "phone", "number", "reach"]
        if any(term in query for term in contact_terms):
            if "emergency" in query:
                categories.append("emergency_contacts")
            else:
                categories.append("personal_contacts")
        
        # Calendar queries
        calendar_terms = ["schedule", "appointment", "meeting", "calendar", "busy"]
        if any(term in query for term in calendar_terms):
            if "private" in query or "personal" in query:
                categories.append("private_calendar")
            else:
                categories.append("limited_calendar")
        
        # Professional queries
        professional_terms = ["company", "work", "professional", "business", "job"]
        if any(term in query for term in professional_terms):
            categories.extend(["company_name", "professional_info"])
        
        return list(set(categories))
    
    # Authorization Methods
    async def _authorize_privileged_user(self, user_identity: UserIdentity, data_categories: List[str]) -> bool:
        """Authorize privileged user requests"""
        for category in data_categories:
            if category not in user_identity.privileges:
                logger.warning(f"🔒 Privileged user '{user_identity.name}' denied access to {category}")
                return False
        return True
    
    async def _authorize_public_user(self, data_categories: List[str]) -> bool:
        """Authorize public user requests"""
        for category in data_categories:
            if category not in self.public_categories:
                return False
        return True
    
    # Command Processing
    async def process_management_command(self, command: str, params: Dict[str, Any], 
                                       requester_identity: UserIdentity) -> str:
        """Process access control management commands"""
        if requester_identity.access_level != AccessLevel.OWNER_ROOT:
            return "❌ Access denied. Only system owner can manage access control."
        
        command_handler = self.management_commands.get(command)
        if not command_handler:
            return f"❌ Unknown command: {command}"
        
        try:
            return await command_handler(params, requester_identity)
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            return f"❌ Command failed: {str(e)}"
    
    # Utility Methods
    def _generate_user_id(self, identifier: str) -> str:
        return hashlib.sha256(identifier.encode()).hexdigest()[:16]
    
    async def _validate_owner_session(self, auth_token: str) -> bool:
        """Validate owner session token"""
        # Implement session validation logic
        return False  # Placeholder
    
    async def _get_owner_preferred_name(self) -> str:
        """Get owner's preferred name from storage"""
        try:
            profile = await self.core.store.get("user_profile") or {}
            return profile.get("preferred_name", "Owner")
        except:
            return "Owner"
    
    async def get_access_summary(self) -> Dict[str, Any]:
        """Get access control summary for monitoring"""
        return {
            "privileged_users_count": len(self.privileged_users),
            "trusted_devices_count": len(self.trusted_devices),
            "public_categories": list(self.public_data_categories),
            "privileged_categories": list(self.privileged_categories),
            "confidential_categories": list(self.confidential_categories),
            "owner_identifiers_count": len(self.owner_identifiers)
        }
    
# In AccessControlSystem class, add:

    async def process_request_flow_enhanced(self, request_data: Dict[str, Any]) -> SecurityContext:
        """
        Enhanced flowchart implementation
        For now, just uses the original method until fully implemented
        """
        return await self.process_request_flow(request_data)

class EnhancedAccessControlSystem(AccessControlSystem):
    async def process_management_command(self, command: str, params: Dict[str, Any], requester_identity: UserIdentity) -> str:
        """Enhanced command processing with runtime configuration"""
        if command == "add_data_category":
            return await self._handle_add_data_category(params, requester_identity)
        elif command == "update_data_category":
            return await self._handle_update_data_category(params, requester_identity)
        elif command == "list_data_categories":
            return await self._handle_list_data_categories(params, requester_identity)
        else:
            # Handle existing commands
            return await super().process_management_command(command, params, requester_identity)
    
    async def _handle_add_data_category(self, params: Dict[str, Any], requester: UserIdentity) -> str:
        """Add new data category at runtime"""
        try:
            from security_config import DataCategoryConfig
            
            category_config = DataCategoryConfig(
                name=params["name"],
                description=params.get("description", ""),
                access_level=params["access_level"],
                sensitive=params.get("sensitive", "false").lower() == "true",
                requires_verification=params.get("requires_verification", "true").lower() == "true"
            )
            
            success = await self.security_config.add_data_category(category_config)
            if success:
                return f"✅ Added data category: {params['name']} with access level: {params['access_level']}"
            else:
                return f"❌ Failed to add data category: {params['name']}"
                
        except Exception as e:
            return f"❌ Error adding data category: {str(e)}"
    
    async def _handle_update_data_category(self, params: Dict[str, Any], requester: UserIdentity) -> str:
        """Update data category at runtime"""
        try:
            category_name = params["name"]
            updates = {k: v for k, v in params.items() if k != "name"}
            
            # Convert string booleans
            for key in ['sensitive', 'requires_verification']:
                if key in updates and isinstance(updates[key], str):
                    updates[key] = updates[key].lower() == "true"
            
            success = await self.security_config.update_data_category(category_name, **updates)
            if success:
                return f"✅ Updated data category: {category_name}"
            else:
                return f"❌ Category not found: {category_name}"
                
        except Exception as e:
            return f"❌ Error updating data category: {str(e)}"
    
    async def _handle_list_data_categories(self, params: Dict[str, Any], requester: UserIdentity) -> str:
        """List all data categories"""
        try:
            categories = await self.security_config.list_data_categories()
            if not categories:
                return "📋 No data categories configured."
            
            category_list = []
            for category in categories:
                status = "🔒" if category['sensitive'] else "🔓"
                category_list.append(
                    f"• {status} {category['name']} - {category['access_level']} - {category['description']}"
                )
            
            return "📋 Data Categories:\n" + "\n".join(category_list)
            
        except Exception as e:
            return f"❌ Error listing data categories: {str(e)}"
        
    