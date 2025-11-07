"""
main.py - A.A.R.I.A System Entry Point (Async Production)
WITH SECURITY INTEGRATION - FIXED VERSION
"""

import os
import sys
import logging
import asyncio
import traceback
import getpass
import pyotp
from dotenv import load_dotenv

# Import all the real A.A.R.I.A. core modules
try:
    from llm_adapter import LLMAdapterFactory, LLMProvider
    from secure_store import SecureStorageAsync
    from assistant_core import AssistantCore
    from persona_core import PersonaCore
    from cognition_core import CognitionCore
    from autonomy_core import AutonomyCore, create_autonomy_core
    from interaction_core import InteractionCore, InboundMessage, create_interaction_core
    # NEW SECURITY IMPORTS
    from access_control import AccessControlSystem, AccessLevel, RequestSource
    from identity_manager import IdentityManager, IdentityProfile, IdentityState, VerificationMethod
    from security_orchestrator import SecurityOrchestrator
    # NEW DYNAMIC SECURITY CONFIG
    from security_config import DynamicSecurityConfig, DataCategoryConfig, PrivilegedUserConfig
except ImportError as e:
    print(f"❌ Critical import error: {e}")
    print("Please ensure all A.A.R.I.A modules are available")
    sys.exit(1)

# >>> ADDED: import hologram_state to ensure core nodes early
try:
    import hologram_state
except Exception:
    hologram_state = None

# ADD MISSING LOGGER SETUP
logger = logging.getLogger("AARIA.System")

class AARIASystem:
    """
    Main A.A.R.I.A system orchestrator with production-grade reliability.
    NOW WITH COMPLETE SECURITY INTEGRATION - FIXED VERSION
    """
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.components = {}
        self.is_running = False
        self.config = self._load_system_config()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup comprehensive logging for the A.A.R.I.A system."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('aaria_system.log', encoding='utf-8')
            ]
        )
        return logging.getLogger("AARIA.System")
    
    def _load_system_config(self) -> dict[str, any]:
        """Load system configuration from environment with defaults."""
        return {
            "llm_provider": os.getenv("LLM_PROVIDER", "groq"),
            "llm_model": os.getenv("LLM_MODEL", "llama-3.1-8b-instant"),
            "max_concurrent_actions": int(os.getenv("MAX_CONCURRENT_ACTIONS", "3")),
            "session_timeout": int(os.getenv("SESSION_TIMEOUT", "3600")),
            "enable_autonomy": os.getenv("ENABLE_AUTONOMY", "true").lower() == "true",
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
            "master_password": os.getenv("AARIA_MASTER_PASSWORD", "default_password_change_me")
        }

    async def initialize(self) -> bool:
        """
        Initialize all A.A.R.I.A components with proper error handling.
        
        Returns:
            bool: True if system initialized successfully, False otherwise
        """
        self.logger.info("🚀 Initializing A.A.R.I.A System with Security...")
        
        try:
            # Load environment variables
            if not load_dotenv(dotenv_path="llm.env"):
                self.logger.warning("llm.env file not found, using environment variables")
            
            # Validate critical environment variables
            if not self._validate_environment():
                return False
            
            # Initialize components in dependency order WITH SECURITY
            components = await self._initialize_components_with_security()
            if not components:
                return False
                
            self.components = components
            self.is_running = True
            
            # Verify system health
            if not await self._verify_system_health():
                self.logger.error("System health check failed")
                return False
                
            self.logger.info("✅ A.A.R.I.A System with Security Initialized Successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ System initialization failed: {e}")
            self.logger.debug(traceback.format_exc())
            return False
    
    def _validate_environment(self) -> bool:
        """Validate required environment variables."""
        master_password = self.config["master_password"]
        if not master_password or master_password == "default_password_change_me":
            self.logger.error("❌ AARIA_MASTER_PASSWORD not found in environment variables or is default")
            self.logger.error("💡 Please set AARIA_MASTER_PASSWORD in llm.env file")
            return False
            
        if len(master_password) < 8:
            self.logger.warning("⚠️  AARIA_MASTER_PASSWORD is shorter than recommended 8 characters")
            
        # Validate optional LLM configuration
        llm_base = os.getenv("LLM_API_BASE")
        if not llm_base:
            self.logger.warning("⚠️  LLM_API_BASE not set, using default local Ollama")
            
        return True
    
    async def _initialize_components_with_security(self) -> dict:
        components = {}
        
        try:
            # 1. Core infrastructure
            self.logger.info("Initializing Assistant Core...")
            assistant_core = AssistantCore(
                password=self.config["master_password"], 
                auto_recover=True
            )
            await assistant_core.initialize()
            components['assistant'] = assistant_core

            # 2. Dynamic Security Configuration
            self.logger.info("Initializing Dynamic Security Configuration...")
            components['security_config'] = DynamicSecurityConfig(components['assistant'])
            await components['security_config'].initialize()

            # 3. Security System - WITH ENHANCED COMPONENTS
            self.logger.info("Initializing Enhanced Security System...")
            
            # Initialize device manager first
            from device_types import DeviceManager
            components['device_manager'] = DeviceManager(components['assistant'])
            await components['device_manager'].initialize()
            
            # Initialize security orchestrator with enhanced components
            components['security'] = SecurityOrchestrator(components['assistant'])
            
            # --- CRITICAL FIX 1: Link security_config to orchestrator ---
            components['security'].security_config = components['security_config'] 
            
            await components['security'].initialize()
            
            # --- CRITICAL FIX 2: Link orchestrator back to assistant_core ---
            # This allows PersonaCore to find the owner's name during its init
            components['assistant'].security_orchestrator = components['security']

            # >>> PATCHED: Ensure hologram base state is initialized BEFORE Persona/Core components
            if hologram_state is not None:
                try:
                    await hologram_state.initialize_base_state()
                    self.logger.info("Hologram base state initialized via initialize_base_state().")
                except Exception as e:
                    self.logger.warning(f"Failed to initialize hologram base state at startup: {e}", exc_info=True)
            else:
                self.logger.debug("hologram_state module not available; skipping hologram initialization.")
            # <<< end patch

            # 4. Core AI components
            self.logger.info("Initializing Persona Core...")
            components['persona'] = PersonaCore(core=components['assistant'])
            await components['persona'].initialize()
            
            self.logger.info("Initializing Cognition Core...")
            components['cognition'] = CognitionCore(
                persona=components['persona'],
                core=components['assistant'],
                autonomy=None,
                config={"max_memory_entries": 1000, "rate_limit": 100}
            )
            
            # Load cognitive state and start background services
            await components['cognition']._load_cognitive_state()
            await components['cognition'].start_background_services()
            
            self.logger.info("Initializing Autonomy Core...")
            components['autonomy'] = await create_autonomy_core(
                persona=components['persona'],
                core=components['assistant'],
                max_concurrent=3
            )
            await components['autonomy'].initialize()
            
            # Update cognition with autonomy reference
            components['cognition'].autonomy = components['autonomy']
            
            self.logger.info("Initializing Interaction Core with Enhanced Security...")
            components['interaction'] = await create_interaction_core(
                persona=components['persona'],
                cognition=components['cognition'],
                autonomy=components['autonomy'],
                config={
                    "session_ttl": 3600,
                    "rate_limit_per_minute": 60,
                    "autosave_interval": 60,
                    "security_orchestrator": components['security']
                }
            )
            
            # Initialize security baseline with enhanced system
            await self._initialize_enhanced_security_baseline(components['security'])
            
            return components
            
        except Exception as e:
            self.logger.error(f"❌ Component initialization failed: {e}")
            await self._cleanup_components(components)
            return {}

    async def _initialize_enhanced_security_baseline(self, security_orchestrator):
        """Initialize enhanced security system with device awareness - FIXED"""
        try:
            # Clear any existing lockouts and verification attempts
            security_orchestrator.identity_manager.failed_attempts.clear()
            security_orchestrator.identity_manager.lockout_until.clear()
            
            # Clear verification manager lockouts too
            verification_manager = security_orchestrator.identity_manager.verification_manager
            verification_manager.verification_attempts.clear()
            verification_manager.lockout_until.clear()
            await verification_manager._save_verification_state()
            
            # Ensure CLI is properly registered as home PC device
            device_manager = security_orchestrator.access_control.device_manager
            
            # Register CLI as home PC device with owner context
            await device_manager.register_device("cli_terminal", "home_pc", {
                "description": "Primary CLI terminal - Owner Access",
                "trust_level": "maximum",
                "user_agent": "AARIA_CLI_Terminal",
                "owner_device": True
            })
            
            # Add trusted networks
            await device_manager.add_trusted_network("127.0.0.0/8")  # Localhost
            await device_manager.add_trusted_network("192.168.1.0/24")  # Typical home network
            await device_manager.add_trusted_network("10.0.0.0/24")   # Alternative home network
            
            logger.info("✅ Enhanced security baseline initialized - lockouts cleared")
            
        except Exception as e:
            logger.error(f"❌ Enhanced security baseline initialization failed: {e}")

    async def _create_owner_user_identity(self):
        """Create a UserIdentity for the owner for command processing."""
        from access_control import UserIdentity, AccessLevel
        return UserIdentity(
            user_id="owner_primary",
            name="System Owner",
            preferred_name="Owner",
            access_level=AccessLevel.OWNER_ROOT,
            privileges={"full_system_access"},
            verification_required=False
        )

    async def _cleanup_components(self, components: dict):
        """Cleanup partially initialized components."""
        for name, component in components.items():
            try:
                if hasattr(component, 'shutdown'):
                    if asyncio.iscoroutinefunction(component.shutdown):
                        await component.shutdown()
                    else:
                        component.shutdown()
                elif hasattr(component, 'close'):
                    if asyncio.iscoroutinefunction(component.close):
                        await component.close()
                    else:
                        component.close()
            except Exception as e:
                self.logger.warning(f"Error cleaning up {name}: {e}")
    
    async def _verify_system_health(self) -> bool:
        """Verify all components are healthy and ready."""
        self.logger.info("🔍 Performing system health checks...")
        
        all_healthy = True
        
        try:
            # Check security health
            security = self.components.get('security')
            if security:
                try:
                    security_status = await security.get_security_status()
                    status = security_status.get('overall_status', 'unknown')
                    if status != 'healthy':
                        self.logger.warning(f"⚠️  Security health: {status}")
                        all_healthy = False
                    else:
                        self.logger.info("✅ Security health: healthy")
                except Exception as e:
                    self.logger.warning(f"⚠️  Security health check failed: {e}")
                    all_healthy = False
            
            # Check cognition health
            cognition = self.components.get('cognition')
            if cognition:
                try:
                    cog_health = await cognition.get_health()
                    status = cog_health.get('overall_status', cog_health.get('status', 'unknown'))
                    if status != 'healthy':
                        self.logger.warning(f"⚠️  Cognition health: {status}")
                        all_healthy = False
                    else:
                        self.logger.info("✅ Cognition health: healthy")
                except Exception as e:
                    self.logger.warning(f"⚠️  Cognition health check failed: {e}")
                    all_healthy = False
            
            # Check autonomy health
            autonomy = self.components.get('autonomy')
            if autonomy:
                try:
                    auto_health = await autonomy.get_system_health()
                    status = auto_health.get('status', 'unknown')
                    if status != 'healthy':
                        self.logger.warning(f"⚠️  Autonomy health: {status}")
                        all_healthy = False
                    else:
                        self.logger.info("✅ Autonomy health: healthy")
                except Exception as e:
                    self.logger.warning(f"⚠️  Autonomy health check failed: {e}")
                    all_healthy = False
            
            # Check assistant core health
            assistant = self.components.get('assistant')
            if assistant:
                try:
                    assist_health = await assistant.health_check()
                    status = assist_health.get('status', 'unknown')
                    if status != 'healthy':
                        self.logger.warning(f"⚠️  Assistant health: {status}")
                        all_healthy = False
                    else:
                        self.logger.info("✅ Assistant health: healthy")
                except Exception as e:
                    self.logger.warning(f"⚠️  Assistant health check failed: {e}")
                    all_healthy = False
            
            if all_healthy:
                self.logger.info("✅ All health checks passed")
            else:
                self.logger.warning("⚠️  System has degraded components but is operational")
                
            return True  # System can still run with degraded components
            
        except Exception as e:
            self.logger.error(f"❌ Health check failed: {e}")
            return False  # Critical failure
    
    async def run_interactive_session(self):
        """Run an interactive chat session with enhanced identity."""
        if not self.is_running:
            self.logger.error("System not initialized. Call initialize() first.")
            return
        
        self.logger.info("💬 Starting interactive session. Type 'exit' to quit.")
        self.logger.info("🔐 Security commands: 'security status', 'access users', 'identity list'")
        self.logger.info("👤 To set your identity: 'set_name YourName'")
        
        try:
            session_start = asyncio.get_event_loop().time()
            message_count = 0
            successful_responses = 0
            user_name = None
            
            while True:
                try:
                    user_input = input("\nYou: ").strip()
                    
                    if user_input.lower() in ['exit', 'quit', 'bye']:
                        break
                    elif not user_input:
                        continue
                    elif user_input.lower() in ['help', '?']:
                        self._show_help()
                        continue
                    elif user_input.lower() == 'status':
                        await self._show_system_status()
                        continue
                    elif user_input.lower() == 'stats':
                        await self._show_session_stats(message_count, successful_responses, session_start)
                        continue
                    elif user_input.lower().startswith('security '):
                        response = await self._process_security_command(user_input)
                        print(f"A.A.R.I.A: {response}")
                        continue
                    elif user_input.lower().startswith('set_name '):
                        # Simple name setting for demo + immediate persistence
                        user_name = user_input[9:].strip()
                        # attempt to update Persona/Identity if possible (defensive)
                        try:
                            persona = self.components.get('persona')
                            if persona and hasattr(persona, "set_preferred_name"):
                                maybe = persona.set_preferred_name(user_name)
                                if asyncio.iscoroutine(maybe):
                                    await maybe
                        except Exception:
                            self.logger.debug("Warning: persona.set_preferred_name not available or failed", exc_info=True)

                        # Persist persona quick so name survives a restart
                        await self._persist_persona_quick()

                        print(f"A.A.R.I.A: ✅ Name set to: {user_name}")
                        continue
                    
                    # Process user message with enhanced identity
                    message_count += 1
                    inbound_message = InboundMessage(
                        channel="cli",
                        content=user_input,
                        user_id=user_name or "user_cli",  # Use name if set
                        metadata={
                            "message_number": message_count,
                            "device_id": "cli_terminal",
                            "source": "private_terminal",  # Assume private for better access
                            "user_name": user_name  # Pass name for identity
                        }
                    )
                    
                    # Get response
                    response = await self.components['interaction'].handle_inbound(inbound_message)
                    if response and response.content:
                        successful_responses += 1
                        print(f"A.A.R.I.A: {response.content}")
                    else:
                        print("A.A.R.I.A: [No response generated]")
                        
                except KeyboardInterrupt:
                    self.logger.info("Received interrupt signal...")
                    break
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    print("A.A.R.I.A: I encountered an error. Please try again.")
                    
            session_duration = asyncio.get_event_loop().time() - session_start
            await self._show_session_stats(message_count, successful_responses, session_start)
            self.logger.info(f"Session ended: {message_count} messages in {session_duration:.1f}s")
            
        except Exception as e:
            self.logger.error(f"Interactive session failed: {e}")

    async def _process_security_command(self, user_input: str) -> str:
        """
        Process security commands entered into the CLI.
        FIXED: This method now correctly routes commands to the orchestrator.
        """
        try:
            parts = user_input.lower().split()
            if len(parts) < 2:
                return "❌ Security command format: security <subsystem> <command> [params]"
            
            subsystem = parts[1]
            command_parts = parts[2:]
            
            # Get security orchestrator
            security = self.components.get('security')
            if not security:
                return "❌ Security system not available"

            # Create owner identity for command processing
            owner_identity = await security.identity_manager._get_or_create_owner_identity()
            
            # --- Route to correct subsystem ---
            
            if subsystem == "status":
                # Handle 'security status' directly
                status = await security.get_security_status()
                response = f"🛡️ Security Status: {status['overall_status']}\n"
                response += f"  • Access Control: {status['access_control']['privileged_users_count']} privileged users, {status['access_control']['trusted_devices_count']} trusted devices\n"
                response += f"  • Identity: {status['identity_management']['known_identities']} known identities, {status['identity_management']['active_sessions']} active sessions"
                return response
            
            elif subsystem == "access":
                # Route 'security access ...' commands
                if not command_parts:
                    return "❌ Usage: security access <command> [params]"
                command = command_parts[0]
                params = self._parse_security_params(command_parts[1:])
                
                # Alias 'users'
                if command == "users":
                    command = "list_privileged_users"
                    
                internal_command = f"access_{command}"
                return await security.process_security_command(internal_command, params, owner_identity)

            elif subsystem == "identity":
                # Route 'security identity ...' commands
                if not command_parts:
                    return "❌ Usage: security identity <command> [params]"
                command = command_parts[0]
                params = self._parse_security_params(command_parts[1:])
                
                # Alias 'list'
                if command == "list":
                    command = "list_known_identities"

                internal_command = f"identity_{command}"
                return await security.process_security_command(internal_command, params, owner_identity)

            else:
                return f"❌ Unknown security subsystem: '{subsystem}'. Try 'status', 'access', or 'identity'."

        except Exception as e:
            self.logger.error(f"Security command processing failed: {e}")
            self.logger.debug(traceback.format_exc())
            return f"❌ Security command error: {str(e)}"

    def _parse_security_params(self, parts: list) -> dict:
        """Parse security command parameters"""
        params = {}
        for part in parts:
            if '=' in part:
                key, value = part.split('=', 1)
                params[key] = value
        return params

    def _show_help(self):
        """Show comprehensive help information.""" 
        help_text = """
🤖 A.A.R.I.A Help:

💬 BASIC COMMANDS:
help, ?              - Show this help message
status               - Show system status
stats                - Show session statistics
exit, quit, bye      - End the session

🔐 SECURITY COMMANDS:
security status      - Show security system status
security help        - Show security command help
set_name <name>      - Set your preferred name

👥 ACCESS CONTROL:
security users       - List privileged users  
security add_privileged_user name=<name> relationship=<relation> privileges=<priv1,priv2>
security remove_privileged_user name=<name>
security update_privileges name=<name> privileges=<new_priv1,new_priv2>

📱 DEVICE MANAGEMENT:
security trusted_devices     - List trusted devices
security add_trusted_device device_id=<id>
security remove_trusted_device device_id=<id>

🔐 IDENTITY MANAGEMENT:
security identity           - List known identities
security add_verification   - Add verification method

📊 DATA CATEGORIES:
security data_categories    - List data categories
security set_public_data categories=<cat1,cat2>

Examples:
set_name Alex
security add_privileged_user name=Mother relationship=mother privileges=current_whereabouts,family_info
security add_trusted_device device_id=my_phone
        """
        print(help_text.strip())

    def _get_security_help(self):
        """Get comprehensive security command help"""
        help_text = """
🔐 A.A.R.I.A Security Commands:

📊 STATUS & INFO:
security status                    - Show comprehensive security status
security help                      - Show this help message

👤 IDENTITY MANAGEMENT:
security identity                  - List known identities
security set_name <name>           - Set your preferred name
security add_verification          - Add verification method

👥 ACCESS CONTROL:
security users                     - List privileged users
security add_privileged_user       - Add privileged user
security remove_privileged_user    - Remove privileged user  
security update_privileges         - Update user privileges
security trusted_devices           - List trusted devices
security add_trusted_device        - Add trusted device

📱 DEVICE MANAGEMENT:
security device_register           - Register new device
security device_list               - List registered devices
security device_remove             - Remove device

🌐 NETWORK MANAGEMENT:
security networks                  - List trusted networks
security add_network               - Add trusted network
security remove_network            - Remove trusted network

🔐 VERIFICATION:
security verification_status       - Check verification status
security reset_verification        - Reset verification attempts

📁 DATA MANAGEMENT:
security data_categories           - List data categories
security set_public_data           - Set public data categories

Examples:
security set_name Alex
security device_register device_id=my_phone type=mobile
security add_network network=192.168.1.0/24
security add_privileged_user name=Alice relationship=mother privileges=location,calendar
        """
        return help_text.strip()

    def _format_timestamp(self, timestamp):
        """Format timestamp for display"""
        from datetime import datetime
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    
        # put this near the other persistence helpers (_ensure_persona_saved)
    async def _persist_persona_quick(self) -> bool:
        """
        Used for quick immediate persistence after identity changes (set_name),
        or when you want a runtime flush of persona memory. Returns True on success.
        """
        persona = self.components.get('persona')
        if not persona:
            self.logger.debug("_persist_persona_quick: no persona component")
            return False

        # Try direct API methods commonly used by PersonaCore
        candidates = ["flush_memories", "save_persistent_memory", "persist", "persist_memories", "save", "close"]
        for name in candidates:
            if hasattr(persona, name):
                method = getattr(persona, name)
                is_coro = asyncio.iscoroutinefunction(method)
                try:
                    self.logger.info(f"Attempting quick persona persist via '{name}'")
                    if is_coro:
                        await method()
                    else:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, method)
                    self.logger.info("Persona quick persist succeeded.")
                    return True
                except Exception as e:
                    self.logger.warning(f"Quick persist via '{name}' failed: {e}")
                    await asyncio.sleep(0.15)
        self.logger.warning("Persona quick persist failed (no method succeeded).")
        return False


    def _validate_contact_results(self, results: list) -> list:
        """
        Filter contact.search results against a canonical contact store from Assistant or Persona
        to avoid reporting LLM-fabricated contacts. Returns filtered results (possibly empty).
        """
        canonical = None
        assistant = self.components.get('assistant')
        persona = self.components.get('persona')

        # Attempt to obtain a canonical contact lookup function - be defensive about missing APIs
        lookup = None
        if assistant and hasattr(assistant, "contact_exists"):
            lookup = assistant.contact_exists
        elif persona and hasattr(persona, "contact_exists"):
            lookup = persona.contact_exists
        elif assistant and hasattr(assistant, "contacts"):
            # assistant.contacts assumed to be dict/list of names
            canonical = set(getattr(assistant, "contacts", []))
        elif persona and hasattr(persona, "contacts"):
            canonical = set(getattr(persona, "contacts", []))

        filtered = []
        for r in results:
            name = None
            if isinstance(r, dict):
                name = r.get("name") or r.get("display_name") or r.get("label")
            elif isinstance(r, str):
                name = r
            if not name:
                continue
            # If lookup callable exists, prefer it
            try:
                if lookup:
                    exists = lookup(name)
                    # if lookup is coroutine, await it
                    if asyncio.iscoroutine(exists):
                        exists = asyncio.get_event_loop().run_until_complete(exists)
                    if exists:
                        filtered.append(r)
                else:
                    if canonical and name in canonical:
                        filtered.append(r)
                    else:
                        # if no canonical data exists, keep only high-confidence items
                        # expect r to include an 'importance' or 'confidence' field
                        if isinstance(r, dict) and (r.get("importance", 0) >= 3 or r.get("confidence", 0) >= 0.7):
                            filtered.append(r)
            except Exception:
                # if lookup failed for some reason, be conservative: only keep if high importance
                if isinstance(r, dict) and (r.get("importance", 0) >= 3 or r.get("confidence", 0) >= 0.7):
                    filtered.append(r)
        return filtered

    async def _show_system_status(self):
        """Show current system status."""
        if not self.components:
            print("System status: Not initialized")
            return
            
        try:
            status_lines = ["🔧 A.A.R.I.A System Status:"]
            
            # Security Status
            security = self.components.get('security')
            if security:
                try:
                    security_status = await security.get_security_status()
                    status_lines.append(f"  • Security: {security_status.get('overall_status', 'unknown')}")
                except Exception as e:
                    status_lines.append(f"  • Security: error ({e})")
            
            # Assistant Status
            assistant = self.components.get('assistant')
            if assistant:
                try:
                    assist_health = await assistant.health_check()
                    status_lines.append(f"  • Assistant: {assist_health.get('status', 'unknown')}")
                except Exception as e:
                    status_lines.append(f"  • Assistant: error ({e})")
            
            # Cognition Status
            cognition = self.components.get('cognition')
            if cognition:
                try:
                    cog_health = await cognition.get_health()
                    stability = cog_health.get('emotional_stability', 0)
                    overall_status = cog_health.get('overall_status', cog_health.get('status', 'unknown'))
                    status_lines.append(f"  • Cognition: {overall_status} (Stability: {stability:.2f})")
                except Exception as e:
                    status_lines.append(f"  • Cognition: error ({e})")
            
            # Autonomy Status
            autonomy = self.components.get('autonomy')
            if autonomy:
                try:
                    auto_health = await autonomy.get_system_health()
                    queue_size = auto_health.get('queue_size', 0)
                    status_lines.append(f"  • Autonomy: {auto_health.get('status', 'unknown')} (Queue: {queue_size} actions)")
                except Exception as e:
                    status_lines.append(f"  • Autonomy: error ({e})")
            
            print("\n".join(status_lines))
            
        except Exception as e:
            print(f"Status check error: {e}")

    async def _show_session_stats(self, message_count: int, successful_responses: int, session_start: float):
        """Show session statistics.""" 
        session_duration = asyncio.get_event_loop().time() - session_start
        success_rate = (successful_responses / message_count * 100) if message_count > 0 else 0

        stats = f"""
📊 Session Statistics:
• Duration: {session_duration:.1f}s
• Messages: {message_count}
• Successful responses: {successful_responses}
• Success rate: {success_rate:.1f}%
• Avg. response time: {(session_duration / max(1, message_count)):.1f}s per message
"""
        print(stats)

    async def _ensure_persona_saved(self, timeout_seconds: float = 5.0) -> bool:
        """
        Attempt to persist PersonaCore memory safely before assistant/secure-store closes.
        Tries several likely method names (backwards compatibility) with a couple retries.
        Returns True if persisted successfully (or nothing to save), False otherwise.
        """
        persona = self.components.get('persona')
        if not persona:
            self.logger.debug("_ensure_persona_saved: no persona component present.")
            return True

        # Candidate method names PersonaCore implementations might expose
        save_candidates = [
            "save_persistent_memory",
            "persist_memories",
            "persist",
            "save_all",
            "save",
            "close"  # close often persists as a side-effect, try last
        ]

        # If there is an explicit 'is_master_key_unlocked' or similar check, we could use it.
        # Try to call available save method up to 2 times if it raises master-key errors.
        for method_name in save_candidates:
            if not hasattr(persona, method_name):
                continue
            method = getattr(persona, method_name)

            # decide whether to await or call directly
            is_coro = asyncio.iscoroutinefunction(method)

            attempts = 2
            for attempt in range(1, attempts + 1):
                try:
                    self.logger.info(f"Persisting Persona memory using '{method_name}' (attempt {attempt}/{attempts})...")
                    if is_coro:
                        await asyncio.wait_for(method(), timeout=timeout_seconds)
                    else:
                        # run sync call in threadpool to avoid blocking if it does blocking IO
                        loop = asyncio.get_event_loop()
                        await asyncio.wait_for(loop.run_in_executor(None, method), timeout=timeout_seconds)
                    self.logger.info("✅ Persona memory persisted successfully.")
                    return True
                except asyncio.TimeoutError:
                    self.logger.warning(f"Persona memory save via '{method_name}' timed out on attempt {attempt}.")
                except Exception as e:
                    # Common failure: master key locked / secure store closed. Log and retry once.
                    self.logger.warning(f"Persona memory save via '{method_name}' failed on attempt {attempt}: {e}")
                    # Small backoff
                    await asyncio.sleep(0.35)

            # if this method failed after retries, continue to next candidate
            self.logger.debug(f"Persona save method '{method_name}' exhausted attempts and failed; trying next candidate.")

        # If nothing succeeded, log error (but do not raise; we still want clean shutdown)
        self.logger.error("❌ Persona memory could not be persisted by any known method. Memories may be lost on shutdown.")
        return False


    async def shutdown(self):
        """Gracefully shutdown the entire A.A.R.I.A system."""
        self.logger.info("🛑 Shutting down A.A.R.I.A System...")
        self.is_running = False

        # 1) Ensure Persona memory persisted BEFORE we close assistant/secure store
        try:
            saved = await self._ensure_persona_saved()
            if not saved:
                # We will still proceed with shutdown, but we want this spelled out
                self.logger.error("Persona memory persistence failed during shutdown. See logs for details.")
        except Exception as e:
            self.logger.error(f"Unexpected error while attempting to persist Persona memory: {e}", exc_info=True)

        # 2) Shutdown order: interaction -> autonomy -> cognition -> persona (already persisted) -> security -> assistant
        shutdown_order = [
            'interaction',
            'autonomy',
            'cognition',
            # NOTE: persona already persisted above; still attempt graceful close if available
            'persona',
            'security',
            'assistant'
        ]

        for component_name in shutdown_order:
            component = self.components.get(component_name)
            if not component:
                continue
            try:
                self.logger.info(f"Shutting down {component_name}...")
                if hasattr(component, 'shutdown'):
                    if asyncio.iscoroutinefunction(component.shutdown):
                        await component.shutdown()
                    else:
                        # run blocking shutdown in executor so we don't block event loop if it's blocking
                        if callable(component.shutdown):
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(None, component.shutdown)
                elif hasattr(component, 'close'):
                    if asyncio.iscoroutinefunction(component.close):
                        await component.close()
                    else:
                        if callable(component.close):
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(None, component.close)
                else:
                    self.logger.debug(f"{component_name} has no 'shutdown' or 'close' method; skipping.")
            except Exception as e:
                self.logger.error(f"Error shutting down {component_name}: {e}", exc_info=True)

        # Clear components map (safe)
        self.components.clear()
        self.logger.info("✅ A.A.R.I.A System shutdown complete")


    async def __aenter__(self):
        """Async context manager entry.""" 
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()

    async def run_startup_verification(self) -> bool:
        """
        [FINAL VERSION]
        Runs a verification challenge at startup.
        If no secret is enrolled OR the secret is invalid,
        it forces a one-time enrollment flow.
        """
        if not self.components.get('security'):
            self.logger.error("Security component not loaded. Cannot verify.")
            return False
            
        identity_manager = self.components['security'].identity_manager
        owner_identity = await identity_manager._get_or_create_owner_identity()
        
        # 1. Check if a valid secret already exists
        secret = await identity_manager._get_totp_secret(owner_identity.identity_id)
        
        # --- Check if the secret is a valid Base32 string ---
        is_valid_secret = False
        if secret:
            try:
                # pyotp's constructor will check if the key is valid Base32
                pyotp.TOTP(secret) 
                is_valid_secret = True
            except Exception:
                is_valid_secret = False
        # --- End check ---

        if not is_valid_secret:
            # --- FIRST-TIME SETUP (FORCED ENROLLMENT) ---
            self.logger.warning("🔐 No valid Owner TOTP secret found. Initiating first-time setup...")
            try:
                # Call the enrollment method to generate AND SAVE a new secret
                new_secret = await identity_manager.enroll_new_totp_secret(owner_identity.identity_id)
                
                # Display enrollment instructions to the console
                print("\n" + "="*50)
                print("  FIRST-TIME A.A.R.I.A. SECURITY SETUP")
                print("  A new TOTP secret has been generated for you.")
                print("  Please add this secret to your authenticator app:")
                print(f"\n    {new_secret}\n")
                print("  This secret will not be shown again.")
                print("="*50)
                print("\n...Waiting 15 seconds for you to add the key before continuing...")
                
                await asyncio.sleep(15) 
            
            except Exception as e:
                self.logger.error(f"❌ Critical error during TOTP enrollment: {e}")
                return False

        # --- VERIFICATION LOOP ---
        self.logger.info("🔐 A.A.R.I.A. Root Access Verification Required.")
        
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # Use input() instead of getpass() if getpass() is freezing your terminal
                totp_code = input(f"  Please enter your TOTP code (Attempt {attempt+1}/{max_attempts}): ")
                
                if not totp_code:
                    continue 

                # Call the verification challenge
                is_verified = await identity_manager.challenge_owner_verification(totp_code)
                
                if is_verified:
                    self.logger.info("✅ Verification successful. Root access granted.")
                    return True
                else:
                    self.logger.warning("❌ Verification failed. Invalid code.")

            except Exception as e:
                self.logger.error(f"Verification challenge failed: {e}")

        # If loop finishes without success
        self.logger.error("❌ Too many failed verification attempts. Access denied.")
        return False


async def main():
    """
    Main async entry point for the A.A.R.I.A system.
    FIXED: Now includes startup verification.
    """
    system = AARIASystem()
    
    try:
        # Initialize the system using context manager
        async with system:
            
            # --- NEW VERIFICATION STEP ---
            if not await system.run_startup_verification():
                system.logger.error("System startup aborted due to failed verification.")
                return 1 # Exit with error code
            # --- END NEW STEP ---
            
            # Run interactive session
            await system.run_interactive_session()
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\nReceived interrupt signal. Shutting down gracefully...")
        return 0
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        if system.logger:
            system.logger.error(f"System failure: {e}")
            system.logger.debug(traceback.format_exc())
        return 1


if __name__ == "__main__":
    try:
        # Run the async main function
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nA.A.R.I.A system terminated by user.")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Fatal system error: {e}")
        sys.exit(1)
