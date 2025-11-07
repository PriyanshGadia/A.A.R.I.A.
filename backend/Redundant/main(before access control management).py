"""
main.py - A.A.R.I.A System Entry Point (Async Production)

Production Features:
- Full async/await architecture throughout
- Robust error handling and graceful degradation
- Comprehensive health checks and system validation
- Proper dependency injection and component lifecycle management
- Professional logging and monitoring
- Graceful shutdown with resource cleanup
- Async context managers for proper resource management
"""

import os
import sys
import logging
import asyncio
import traceback
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
except ImportError as e:
    print(f"❌ Critical import error: {e}")
    print("Please ensure all A.A.R.I.A modules are available")
    sys.exit(1)


class AARIASystem:
    """
    Main A.A.R.I.A system orchestrator with production-grade reliability.
    """
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.components = {}
        self.is_running = False
        
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
    
    async def initialize(self) -> bool:
        """
        Initialize all A.A.R.I.A components with proper error handling.
        
        Returns:
            bool: True if system initialized successfully, False otherwise
        """
        self.logger.info("🚀 Initializing A.A.R.I.A System...")
        
        try:
            # Load environment variables
            if not load_dotenv(dotenv_path="llm.env"):
                self.logger.warning("llm.env file not found, using environment variables")
            
            # Validate critical environment variables
            if not self._validate_environment():
                return False
            
            # Initialize components in dependency order
            components = await self._initialize_components()
            if not components:
                return False
                
            self.components = components
            self.is_running = True
            
            # Verify system health
            if not await self._verify_system_health():
                self.logger.error("System health check failed")
                return False
                
            self.logger.info("✅ A.A.R.I.A System Initialized Successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ System initialization failed: {e}")
            self.logger.debug(traceback.format_exc())
            return False
    
    def _validate_environment(self) -> bool:
        """Validate required environment variables."""
        master_password = os.getenv("AARIA_MASTER_PASSWORD")
        if not master_password:
            self.logger.error("❌ AARIA_MASTER_PASSWORD not found in environment variables")
            return False
            
        if len(master_password) < 8:
            self.logger.warning("⚠️  AARIA_MASTER_PASSWORD is shorter than recommended 8 characters")
            
        # Validate optional LLM configuration
        llm_base = os.getenv("LLM_API_BASE")
        if not llm_base:
            self.logger.warning("⚠️  LLM_API_BASE not set, using default local Ollama")
            
        return True
    
# In main.py, update the _initialize_components method:
    async def _initialize_components(self) -> dict:
        """Initialize all components with proper dependency injection."""
        components = {}
        
        try:
            # 1. Core infrastructure
            self.logger.info("Initializing Assistant Core...")
            assistant_core = AssistantCore(password=master_password, auto_recover=True)
            await assistant_core.initialize()
            components['assistant'] = assistant_core

            # 2. Security System (NEW - initialize first)
            self.logger.info("Initializing Security System...")
            components['security'] = SecurityOrchestrator(components['assistant'])
            await components['security'].initialize()

            # 3. AI components with security context
            self.logger.info("Initializing Persona Core with security...")
            components['persona'] = PersonaCore(core=components['assistant'])
            await components['persona'].initialize()

            # 2. Core AI components
            self.logger.info("Initializing Persona Core...")
            components['persona'] = PersonaCore(core=components['assistant'])
            await components['persona'].initialize()  # ADDED: Await initialization
            
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
            await components['autonomy'].initialize()  # ADDED: Await initialization
            
            # Update cognition with autonomy reference
            components['cognition'].autonomy = components['autonomy']
            
            self.logger.info("Initializing Interaction Core...")
            components['interaction'] = await create_interaction_core(
                persona=components['persona'],
                cognition=components['cognition'],
                autonomy=components['autonomy'],
                config={
                    "session_ttl": 3600,
                    "rate_limit_per_minute": 60,
                    "autosave_interval": 60
                }
            )
            
            return components
            
        except Exception as e:
            self.logger.error(f"Component initialization failed: {e}")
            await self._cleanup_components(components)
            return {}

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
        """Run an interactive chat session with the user."""
        if not self.is_running:
            self.logger.error("System not initialized. Call initialize() first.")
            return
        
        self.logger.info("💬 Starting interactive session. Type 'exit' to quit.")
        
        try:
            session_start = asyncio.get_event_loop().time()
            message_count = 0
            successful_responses = 0
            
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
                    
                    # Process user message
                    message_count += 1
                    inbound_message = InboundMessage(
                        channel="cli",
                        content=user_input,
                        user_id="user",
                        metadata={"message_number": message_count}
                    )
                    
                    # Get A.A.R.I.A's response using async handler
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
    
    def _show_help(self):
        """Show help information to the user."""
        help_text = """
A.A.R.I.A Commands:
  help, ?     - Show this help message
  status      - Show system status
  stats       - Show session statistics
  exit, quit  - End the session
  
You can ask me to:
  • Answer questions and provide information
  • Help with planning and decision making
  • Perform autonomous actions (if configured)
  • Remember your preferences and context
"""
        print(help_text)

    async def _show_system_status(self):
        """Show current system status."""
        if not self.components:
            print("System status: Not initialized")
            return
            
        try:
            status_lines = ["🔧 A.A.R.I.A System Status:"]
            
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

    async def shutdown(self):
        """Gracefully shutdown the entire A.A.R.I.A system."""
        self.logger.info("🛑 Shutting down A.A.R.I.A System...")
        self.is_running = False
        
        shutdown_order = [
            'interaction',
            'autonomy', 
            'cognition',
            'persona',
            'assistant'
        ]
        
        for component_name in shutdown_order:
            component = self.components.get(component_name)
            if component:
                try:
                    self.logger.info(f"Shutting down {component_name}...")
                    
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
                    self.logger.error(f"Error shutting down {component_name}: {e}")
        
        self.components.clear()
        self.logger.info("✅ A.A.R.I.A System shutdown complete")

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()


async def main():
    """
    Main async entry point for the A.A.R.I.A system.
    """
    system = AARIASystem()
    
    try:
        # Initialize the system using context manager
        async with system:
            # Run interactive session
            await system.run_interactive_session()
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\nReceived interrupt signal. Shutting down gracefully...")
        return 0
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
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