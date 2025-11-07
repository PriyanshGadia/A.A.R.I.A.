"""
security_flow.py - Dynamic Security Flow Using Runtime Configuration
"""

import logging
from typing import Dict, Any, List, Tuple, Set
from security_config import DynamicSecurityConfig, DataCategoryConfig

logger = logging.getLogger("AARIA.SecurityFlow")

class DynamicSecurityFlow:
    """
    Implements security flowchart using runtime configuration
    """
    
    def __init__(self, access_control, identity_manager, security_config: DynamicSecurityConfig):
        self.access_control = access_control
        self.identity_manager = identity_manager
        self.security_config = security_config
    
    async def process_flow(self, request_data: Dict[str, Any]) -> Tuple[Set[str], str, str]:
        """
        Process request through security flowchart using runtime configuration
        
        Returns: (authorized_categories, flow_path, access_level)
        """
        logger.info("🔄 Processing through dynamic security flowchart...")
        
        # STEP 1: Request Identification
        identity, is_verified = await self.identity_manager.identify_and_verify_user(request_data)
        request_type = await self._identify_request_type(identity, is_verified)
        logger.info(f"📋 Request Identification: {request_type} - User: {identity.preferred_name}")
        
        if request_type == "owner_request":
            # STEP 2: Request Source Identification
            source_type = await self._identify_source_type(request_data)
            logger.info(f"🔍 Source Identification: {source_type}")
            
            if source_type == "private_terminal":
                # Private Terminal -> Root Database
                flow_path = "A.A.R.I.A -> Owner Request -> Private Terminal -> Root Database"
                access_level = "root"
                
            else:  # remote_terminal
                # Remote Terminal -> Owner/Confidential Data  
                flow_path = "A.A.R.I.A -> Owner Request -> Remote Terminal -> Owner/Confidential Data"
                access_level = "owner"
                
        else:  # public_request
            # STEP 3: Secondary Request Identification
            secondary_type = await self._identify_secondary_type(identity, is_verified)
            logger.info(f"🔍 Secondary Identification: {secondary_type}")
            
            if secondary_type == "privileged_request":
                # Privileged Request -> Access Data
                flow_path = "A.A.R.I.A -> Public Request -> Privileged Request -> Access Data"
                access_level = "privileged"
                
            else:  # general_request
                # General Request -> Public Data
                flow_path = "A.A.R.I.A -> Public Request -> General Request -> Public Data" 
                access_level = "public"
        
        # Get authorized categories based on access level and user privileges
        user_privileges = identity.privileges if hasattr(identity, 'privileges') else set()
        authorized_categories = await self.security_config.get_authorized_categories(
            access_level, user_privileges
        )
        
        logger.info(f"✅ Flow completed: {flow_path} - Access: {access_level} - Categories: {len(authorized_categories)}")
        return authorized_categories, flow_path, access_level
    
    async def _identify_request_type(self, identity, is_verified: bool) -> str:
        """Identify if request is from Owner or Public using runtime identity"""
        if identity.relationship == "owner" and is_verified:
            return "owner_request"
        else:
            return "public_request"
    
    async def _identify_source_type(self, request_data: Dict[str, Any]) -> str:
        """
        Identify source using runtime trusted devices.
        FIXED: Now correctly identifies 'cli_terminal' as a 'private_terminal'.
        """
        device_id = request_data.get("device_id", "")
        
        # --- FIX ---
        # The CLI is the definition of a private terminal
        if device_id == "cli_terminal":
            return "private_terminal"
        # --- END FIX ---

        # Check against runtime trusted devices
        if device_id in self.security_config.security_policy.trusted_devices:
            return "private_terminal"
        else:
            return "remote_terminal"
    
    async def _identify_secondary_type(self, identity, is_verified: bool) -> str:
        """Identify public request type using runtime privileged users"""
        # Check if this identity is in privileged users
        if (identity.identity_id in self.security_config.security_policy.privileged_users and 
            is_verified):
            return "privileged_request"
        else:
            return "general_request"
    
    async def extract_data_categories_from_query(self, query: str) -> Set[str]:
        """Extract data categories from query using NLP and runtime categories"""
        query_lower = query.lower()
        detected_categories = set()
        
        # Get all available categories from runtime config
        all_categories = await self.security_config.list_data_categories()
        category_keywords = {}
        
        # Build keyword mapping for each category
        for category in all_categories:
            keywords = self._get_keywords_for_category(category['name'])
            category_keywords[category['name']] = keywords
        
        # Match query against category keywords
        for category_name, keywords in category_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                detected_categories.add(category_name)
        
        return detected_categories
    
    def _get_keywords_for_category(self, category_name: str) -> List[str]:
        """Get relevant keywords for each data category"""
        keyword_map = {
            "current_whereabouts": ["where", "location", "address", "current place", "whereabouts", "locate"],
            "personal_contacts": ["contact", "email", "phone", "number", "reach", "call", "text"],
            "family_info": ["family", "mother", "father", "parents", "spouse", "children", "sibling"],
            "company_name": ["company", "work", "employer", "organization", "firm"],
            "professional_info": ["professional", "job", "career", "background", "experience", "qualification"],
            "private_calendar": ["schedule", "appointment", "meeting", "calendar", "busy", "available", "plan"],
            "financial_info": ["bank", "money", "financial", "account", "salary", "income", "payment"],
            "health_data": ["health", "medical", "doctor", "hospital", "ill", "sick", "condition"]
        }
        
        return keyword_map.get(category_name, [])