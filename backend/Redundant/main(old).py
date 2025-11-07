"""
main.py - A.A.R.I.A System Entry Point

Production Features:
- Robust error handling and graceful degradation
- Comprehensive health checks and system validation
- Proper dependency injection and component lifecycle management
- Professional logging and monitoring
- Graceful shutdown with resource cleanup
"""

import os
import sys
import logging
import time
import traceback
from dotenv import load_dotenv

# Import all the real A.A.R.I.A. core modules
try:
    from llm_adapter import LLMAdapter, LLMAdapterError
    from secure_store import SecureStorage
    from assistant_core import AssistantCore
    from persona_core import PersonaCore
    from cognition_core import CognitionCore
    from autonomy_core import AutonomyCore
    from interaction_core import InteractionCore, InboundMessage
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
    
    def initialize(self) -> bool:
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
            components = self._initialize_components()
            if not components:
                return False
                
            self.components = components
            self.is_running = True
            
            # Verify system health
            if not self._verify_system_health():
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
    
    def _initialize_components(self) -> dict:
        """
        Initialize all components with proper dependency injection.
        """
        components = {}
        
        try:
            # 1. Core infrastructure
            self.logger.info("Initializing LLM Adapter...")
            components['llm'] = LLMAdapter()
            
            self.logger.info("Initializing Assistant Core...")
            master_password = os.getenv("AARIA_MASTER_PASSWORD")
            components['assistant'] = AssistantCore(password=master_password)

            # Attach the LLM orchestrator to the assistant core so other modules can access it.
            components['assistant'].llm_orchestrator = components['llm']
           
            # 2. Core AI components
            self.logger.info("Initializing Persona Core...")
            components['persona'] = PersonaCore(
                core=components['assistant'], 
                llm=components['llm']
            )
            
            self.logger.info("Initializing Autonomy Core...")
            components['autonomy'] = AutonomyCore(
                persona=components['persona'], 
                core=components['assistant']
            )
            
            self.logger.info("Initializing Cognition Core...")
            components['cognition'] = CognitionCore(
                persona=components['persona'],
                core=components['assistant'],
                autonomy=components['autonomy']
            )
            
            self.logger.info("Initializing Interaction Core...")
            components['interaction'] = InteractionCore(
                persona=components['persona'],
                cognition=components['cognition'],
                autonomy=components['autonomy']
            )
            
            return components
            
        except LLMAdapterError as e:
            self.logger.error(f"❌ LLM initialization failed: {e}")
        except Exception as e:
            self.logger.error(f"❌ Component initialization failed: {e}")
            self.logger.debug(traceback.format_exc())
            
        return {}    

    def _verify_system_health(self) -> bool:
        """Verify all components are healthy and ready."""
        self.logger.info("🔍 Performing system health checks...")
        
        all_healthy = True
        
        try:
            # Check LLM health
            llm = self.components.get('llm')
            if llm and hasattr(llm, 'health_check'):
                llm_health = llm.health_check()
                if llm_health.get('status') != 'healthy':
                    self.logger.warning(f"⚠️  LLM health: {llm_health.get('status')}")
                    all_healthy = False
            
            # Check cognition health with method fallback
            cognition = self.components.get('cognition')
            if cognition:
                cog_health = {}
                if hasattr(cognition, 'deep_health_check'):
                    cog_health = cognition.deep_health_check()
                elif hasattr(cognition, 'get_health_status'):
                    cog_health = cognition.get_health_status()
                elif hasattr(cognition, 'get_health'):
                    cog_health = cognition.get_health()
                
                status = cog_health.get('overall_status') or cog_health.get('status')
                if status and status != 'healthy':
                    self.logger.warning(f"⚠️  Cognition health: {status}")
                    all_healthy = False
            
            # Check autonomy health
            autonomy = self.components.get('autonomy')
            if autonomy and hasattr(autonomy, 'get_system_health'):
                auto_health = autonomy.get_system_health()
                if auto_health.get('status') != 'healthy':
                    self.logger.warning(f"⚠️  Autonomy health: {auto_health.get('status')}")
                    all_healthy = False
            
            if all_healthy:
                self.logger.info("✅ All health checks passed")
            else:
                self.logger.warning("⚠️  System has degraded components but is operational")
                
            return True  # System can still run with degraded components
            
        except Exception as e:
            self.logger.error(f"❌ Health check failed: {e}")
            return False  # Critical failure
    
    def run_interactive_session(self):
        """Run an interactive chat session with the user."""
        if not self.is_running:
            self.logger.error("System not initialized. Call initialize() first.")
            return
        
        self.logger.info("💬 Starting interactive session. Type 'exit' to quit.")
        
        try:
            session_start = time.time()
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
                        self._show_system_status()
                        continue
                    elif user_input.lower() == 'stats':
                        self._show_session_stats(message_count, successful_responses, session_start)
                        continue
                    
                    # Process user message
                    message_count += 1
                    inbound_message = InboundMessage(
                        channel="cli",
                        content=user_input,
                        user_id="user",
                        metadata={"message_number": message_count}
                    )
                    
                    # Get A.A.R.I.A's response
                    response = self.components['interaction'].handle_inbound(inbound_message)
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
                    
            session_duration = time.time() - session_start
            self._show_session_stats(message_count, successful_responses, session_start)
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
    def _show_system_status(self):
            """Show current system status."""
            if not self.components:
                print("System status: Not initialized")
                return
                
            try:
                status_lines = ["🔧 A.A.R.I.A System Status:"]
                
                # LLM Status
                llm = self.components.get('llm')
                if llm and hasattr(llm, 'health_check'):
                    llm_health = llm.health_check()
                    status_lines.append(f"  • LLM: {llm_health.get('status', 'unknown')}")
                
                # Cognition Status (CORRECTED LOGIC)
                cognition = self.components.get('cognition')
                if cognition and hasattr(cognition, 'get_health'):
                    cog_health = cognition.get_health()
                    stability = cog_health.get('emotional_stability', 0)
                    status_lines.append(f"  • Cognition: {cog_health.get('status', 'unknown')} (Stability: {stability:.2f})")
                
                # Autonomy Status
                autonomy = self.components.get('autonomy')
                if autonomy and hasattr(autonomy, 'get_system_health'):
                    auto_health = autonomy.get_system_health()
                    status_lines.append(f"  • Autonomy: {auto_health.get('status', 'unknown')} (Queue: {auto_health.get('queue_size', 0)} actions)")
                
                print("\n".join(status_lines))
                
            except Exception as e:
                print(f"Status check error: {e}")
                    
    def shutdown(self):
        """Gracefully shutdown the entire A.A.R.I.A system."""
        self.logger.info("🛑 Shutting down A.A.R.I.A System...")
        self.is_running = False
        
        shutdown_order = [
            'interaction',
            'autonomy', 
            'cognition',
            'persona',
            'assistant',
            'llm'
        ]
        
        for component_name in shutdown_order:
            component = self.components.get(component_name)
            if component:
                try:
                    self.logger.info(f"Shutting down {component_name}...")
                    
                    if hasattr(component, 'shutdown'):
                        component.shutdown()
                    elif hasattr(component, 'close'):
                        component.close()
                    elif hasattr(component, 'stop_background_loop'):
                        component.stop_background_loop()
                        
                except Exception as e:
                    self.logger.error(f"Error shutting down {component_name}: {e}")
        
        self.components.clear()
        self.logger.info("✅ A.A.R.I.A System shutdown complete")

    def _show_session_stats(self, message_count: int, successful_responses: int, session_start: float):
        """Show session statistics."""
        session_duration = time.time() - session_start
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

def main():
    """
    Main entry point for the A.A.R.I.A system.
    """
    system = AARIASystem()
    
    try:
        # Initialize the system
        if not system.initialize():
            print("❌ Failed to initialize A.A.R.I.A system. Please check the logs.")
            return 1
        
        # Run interactive session
        system.run_interactive_session()
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\nReceived interrupt signal. Shutting down gracefully...")
        return 0
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        system.logger.error(f"System failure: {e}")
        system.logger.debug(traceback.format_exc())
        return 1
    finally:
        system.shutdown()


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)