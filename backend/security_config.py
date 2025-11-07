"""
security_config.py - Runtime Security Configuration Management
"""

import logging
import asyncio
from typing import Dict, Any, List, Set, Optional
from enum import Enum
from dataclasses import dataclass, asdict

logger = logging.getLogger("AARIA.SecurityConfig")

@dataclass
class DataCategoryConfig:
    name: str
    description: str
    access_level: str  # root, owner, privileged, public
    sensitive: bool = False
    requires_verification: bool = False

@dataclass
class PrivilegedUserConfig:
    name: str
    relationship: str
    privileges: Set[str]
    verification_required: bool = True
    trusted_devices: Set[str] = None

@dataclass
class SecurityPolicy:
    data_categories: Dict[str, DataCategoryConfig]
    privileged_users: Dict[str, PrivilegedUserConfig]
    trusted_devices: Set[str]
    public_data_categories: Set[str]
    owner_identifiers: Set[str]

class DynamicSecurityConfig:
    """
    Manages security configuration at runtime with persistence
    """
    
    def __init__(self, core):
        self.core = core
        self.security_policy: Optional[SecurityPolicy] = None
        self._config_loaded = False
    
    async def initialize(self):
        """Load or create default security configuration"""
        await self._load_security_policy()
        
        if not self.security_policy:
            await self._create_default_policy()
            
        self._config_loaded = True
        logger.info("🔧 Dynamic Security Config initialized")
    
    async def _load_security_policy(self):
        """Load security policy from storage"""
        try:
            policy_data = await self.core.store.get("security_policy")
            if policy_data:
                # Reconstruct the policy from stored data
                data_categories = {
                    name: DataCategoryConfig(**config) 
                    for name, config in policy_data.get('data_categories', {}).items()
                }
                
                privileged_users = {
                    user_id: PrivilegedUserConfig(**user_config)
                    for user_id, user_config in policy_data.get('privileged_users', {}).items()
                }
                
                self.security_policy = SecurityPolicy(
                    data_categories=data_categories,
                    privileged_users=privileged_users,
                    trusted_devices=set(policy_data.get('trusted_devices', [])),
                    public_data_categories=set(policy_data.get('public_data_categories', [])),
                    owner_identifiers=set(policy_data.get('owner_identifiers', []))
                )
                logger.info(f"✅ Loaded security policy with {len(data_categories)} data categories")
                
        except Exception as e:
            logger.error(f"❌ Failed to load security policy: {e}")
    
    async def _create_default_policy(self):
        """Create default security policy with dynamic categories"""
        # Default data categories - these can be modified at runtime
        data_categories = {
            "root_database": DataCategoryConfig(
                name="root_database",
                description="Full system configuration and root access",
                access_level="root",
                sensitive=True,
                requires_verification=True
            ),
            "owner_confidential": DataCategoryConfig(
                name="owner_confidential",
                description="Owner's confidential personal data",
                access_level="owner",
                sensitive=True,
                requires_verification=True
            ),
            "current_whereabouts": DataCategoryConfig(
                name="current_whereabouts",
                description="Current location and whereabouts",
                access_level="privileged",
                sensitive=True,
                requires_verification=True
            ),
            "personal_contacts": DataCategoryConfig(
                name="personal_contacts",
                description="Personal contact information",
                access_level="owner",
                sensitive=True,
                requires_verification=True
            ),
            "family_info": DataCategoryConfig(
                name="family_info",
                description="Family member information",
                access_level="privileged",
                sensitive=True,
                requires_verification=True
            ),
            "company_name": DataCategoryConfig(
                name="company_name",
                description="Company and professional affiliation",
                access_level="public",
                sensitive=False,
                requires_verification=False
            ),
            "professional_info": DataCategoryConfig(
                name="professional_info",
                description="Professional background and information",
                access_level="public",
                sensitive=False,
                requires_verification=False
            )
        }
        
        self.security_policy = SecurityPolicy(
            data_categories=data_categories,
            privileged_users={},
            trusted_devices=set(),
            public_data_categories={"company_name", "professional_info"},
            owner_identifiers=set()
        )
        
        await self._save_security_policy()
        logger.info("✅ Created default security policy")
    
    async def _save_security_policy(self):
        """Save security policy to storage"""
        try:
            policy_data = {
                'data_categories': {name: asdict(config) for name, config in self.security_policy.data_categories.items()},
                'privileged_users': {user_id: asdict(user_config) for user_id, user_config in self.security_policy.privileged_users.items()},
                'trusted_devices': list(self.security_policy.trusted_devices),
                'public_data_categories': list(self.security_policy.public_data_categories),
                'owner_identifiers': list(self.security_policy.owner_identifiers)
            }
            
            await self.core.store.put("security_policy", "security_config", policy_data)
            logger.debug("💾 Security policy saved")
            
        except Exception as e:
            logger.error(f"❌ Failed to save security policy: {e}")
    
    # Runtime Configuration Methods
    async def add_data_category(self, category_config: DataCategoryConfig) -> bool:
        """Add new data category at runtime"""
        try:
            self.security_policy.data_categories[category_config.name] = category_config
            
            if category_config.access_level == "public":
                self.security_policy.public_data_categories.add(category_config.name)
            
            await self._save_security_policy()
            logger.info(f"✅ Added data category: {category_config.name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to add data category: {e}")
            return False
    
    async def update_data_category(self, category_name: str, **updates) -> bool:
        """Update existing data category at runtime"""
        try:
            if category_name not in self.security_policy.data_categories:
                return False
            
            category = self.security_policy.data_categories[category_name]
            for key, value in updates.items():
                if hasattr(category, key):
                    setattr(category, key, value)
            
            # Update public categories if access level changed
            if 'access_level' in updates:
                if updates['access_level'] == 'public':
                    self.security_policy.public_data_categories.add(category_name)
                else:
                    self.security_policy.public_data_categories.discard(category_name)
            
            await self._save_security_policy()
            logger.info(f"✅ Updated data category: {category_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update data category: {e}")
            return False
    
    async def add_privileged_user(self, user_id: str, user_config: PrivilegedUserConfig) -> bool:
        """Add privileged user at runtime"""
        try:
            self.security_policy.privileged_users[user_id] = user_config
            await self._save_security_policy()
            logger.info(f"✅ Added privileged user: {user_config.name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to add privileged user: {e}")
            return False
    
    async def get_authorized_categories(self, access_level: str, user_privileges: Set[str] = None) -> Set[str]:
        """Get authorized data categories for given access level"""
        authorized = set()
        
        for category_name, config in self.security_policy.data_categories.items():
            if config.access_level == "public":
                authorized.add(category_name)
            elif config.access_level == "privileged" and user_privileges:
                if category_name in user_privileges:
                    authorized.add(category_name)
            elif config.access_level == "owner" and access_level in ["owner", "root"]:
                authorized.add(category_name)
            elif config.access_level == "root" and access_level == "root":
                authorized.add(category_name)
        
        return authorized
    
    async def get_category_config(self, category_name: str) -> Optional[DataCategoryConfig]:
        """Get configuration for specific data category"""
        return self.security_policy.data_categories.get(category_name)
    
    async def list_data_categories(self) -> List[Dict[str, Any]]:
        """List all data categories with their configurations"""
        return [
            {
                "name": config.name,
                "description": config.description,
                "access_level": config.access_level,
                "sensitive": config.sensitive,
                "requires_verification": config.requires_verification
            }
            for config in self.security_policy.data_categories.values()
        ]