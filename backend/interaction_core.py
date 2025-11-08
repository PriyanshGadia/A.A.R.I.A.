"""
A.A.R.I.A Interaction Core with Enhanced Security & Hologram Integration
Version: 2.1.0
Primary module for user interaction processing with security integration.
"""

import asyncio
import logging
import re
import time
import uuid
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from security_orchestrator import SecurityOrchestrator
from cognition_core import CognitionCore
from identity_manager import IdentityManager
from security_flow import SecurityFlow
from autonomy_core import AutonomyCore
from proactive_comm import ProactiveCommunicator
from assistant_core import AssistantCore

logger = logging.getLogger("AARIA.Interaction")

class InteractionState(Enum):
    INITIALIZING = "initializing"
    READY = "ready"
    PROCESSING = "processing"
    AWAITING_SECURITY = "awaiting_security"
    SECURITY_REQUIRED = "security_required"
    MAINTENANCE = "maintenance"
    SHUTDOWN = "shutdown"

class CommandType(Enum):
    NATURAL_LANGUAGE = "natural_language"
    SECURITY_COMMAND = "security_command"
    SYSTEM_COMMAND = "system_command"
    IDENTITY_COMMAND = "identity_command"
    MEMORY_COMMAND = "memory_command"
    AUTONOMY_COMMAND = "autonomy_command"
    PROACTIVE_COMMAND = "proactive_command"
    ASSISTANT_COMMAND = "assistant_command"

class SecurityLevel(Enum):
    PUBLIC = "public"
    STANDARD = "standard"
    ELEVATED = "elevated"
    HIGH = "high"
    ROOT = "root"

@dataclass
class ParsedCommand:
    command_type: CommandType
    intent: str
    parameters: Dict[str, Any]
    confidence: float
    requires_security: bool = False
    security_level: SecurityLevel = SecurityLevel.STANDARD
    user_intent: Optional[str] = None
    context_tags: List[str] = None

@dataclass
class InteractionSession:
    session_id: str
    user_id: str
    start_time: float
    device_id: str
    security_context: Dict[str, Any]
    message_count: int = 0
    last_activity: float = None
    hologram_node_id: str = None

@dataclass
class HologramState:
    node_id: str
    link_id: str
    state_type: str
    data: Dict[str, Any]
    created_at: float
    updated_at: float
    ttl: float = 3600  # 1 hour default

class EnhancedInteractionCore:
    """
    Enhanced Interaction Core with deep hologram integration and improved security command handling.
    Full-featured interaction processing with multi-modal support.
    """
    
    def __init__(self, 
                 security_orchestrator: SecurityOrchestrator, 
                 cognition_core: CognitionCore,
                 autonomy_core: Optional[AutonomyCore] = None,
                 proactive_communicator: Optional[ProactiveCommunicator] = None,
                 assistant_core: Optional[AssistantCore] = None):
        
        self.security_orchestrator = security_orchestrator
        self.cognition_core = cognition_core
        self.autonomy_core = autonomy_core
        self.proactive_communicator = proactive_communicator
        self.assistant_core = assistant_core
        
        self.identity_manager = security_orchestrator.identity_manager
        self.security_flow = SecurityFlow(security_orchestrator)
        
        self.state = InteractionState.INITIALIZING
        self.workers = []
        self.is_running = False
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        # Session management
        self.active_sessions: Dict[str, InteractionSession] = {}
        self.session_timeout = 1800  # 30 minutes
        
        # Enhanced command patterns
        self.security_patterns = self._initialize_security_patterns()
        self.command_handlers = self._initialize_command_handlers()
        
        # Hologram integration
        self.active_holograms: Dict[str, HologramState] = {}
        self.hologram_states: Dict[str, Any] = {}
        
        # Statistics and monitoring
        self.interaction_stats = {
            "total_messages": 0,
            "successful_responses": 0,
            "failed_responses": 0,
            "security_blocks": 0,
            "avg_response_time": 0.0,
            "session_count": 0
        }
        
        # Rate limiting
        self.rate_limits: Dict[str, List[float]] = {}
        self.max_requests_per_minute = 60
        
        # Context management
        self.conversation_context: Dict[str, Any] = {}
        
        logger.info("InteractionCore instance constructed (call await start() to run background workers)")
    
    def _initialize_security_patterns(self) -> Dict[str, List[str]]:
        """Initialize comprehensive security and command patterns"""
        return {
            'identity_set_name': [
                r'(?:call me|address me as|my name is|remember me as)\s+([^\.,!?]+)',
                r'(?:set.*name.*to|change.*name.*to)\s+([^\.,!?]+)',
                r'(?:i am|i\'m)\s+([^\.,!?]+)(?!\s+(?:sorry|tired|busy|happy|sad))',
                r'(?:you can call me|you may address me as)\s+([^\.,!?]+)',
                r'(?:from now on call me|please call me)\s+([^\.,!?]+)'
            ],
            'identity_preferences': [
                r'(?:my birthday is|i was born on|born on|birthdate.*is)\s+([^\.,!?]+)',
                r'(?:remember my birthday|store my birthday|my birthday)',
                r'(?:february\s+29|29 feb|feb\s+29|leap day)',
                r'(?:my favorite|i like|i prefer|i enjoy)\s+([^\.,!?]+)',
                r'(?:remember that i|note that i)\s+([^\.,!?]+)'
            ],
            'identity_query': [
                r'who am i',
                r'what is my name',
                r'what do you know about me',
                r'what is my birthday',
                r'when is my birthday',
                r'my birthdate',
                r'tell me about myself',
                r'what are my preferences'
            ],
            'security_status': [
                r'security status',
                r'system status',
                r'show security',
                r'access level',
                r'my permissions',
                r'security report'
            ],
            'autonomy_control': [
                r'(?:start|enable|activate).*autonomy',
                r'(?:stop|disable|deactivate).*autonomy',
                r'autonomy status',
                r'autonomous mode',
                r'background tasks'
            ],
            'memory_operations': [
                r'remember that',
                r'forget that',
                r'recall when',
                r'what did we talk about',
                r'clear memory',
                r'memory status'
            ],
            'proactive_requests': [
                r'remind me',
                r'schedule.*message',
                r'send.*notification',
                r'alert me when',
                r'notify me about'
            ],
            'assistant_commands': [
                r'assistant mode',
                r'enable assistant',
                r'task completion',
                r'help me with',
                r'assist with'
            ]
        }
    
    def _initialize_command_handlers(self) -> Dict[str, Callable]:
        """Initialize command handler mappings"""
        return {
            'identity_set_name': self._handle_identity_set_name,
            'identity_preferences': self._handle_identity_preferences,
            'identity_query': self._handle_identity_query,
            'security_status': self._handle_security_status,
            'autonomy_control': self._handle_autonomy_control,
            'memory_operations': self._handle_memory_operations,
            'proactive_requests': self._handle_proactive_requests,
            'assistant_commands': self._handle_assistant_commands
        }
    
    async def start(self):
        """Start the interaction core with all background workers"""
        if self.is_running:
            logger.warning("InteractionCore already running")
            return
        
        self.state = InteractionState.INITIALIZING
        
        try:
            # Initialize all subsystems
            await self._initialize_hologram_states()
            await self._initialize_session_cleanup()
            await self._initialize_context_manager()
            
            # Start background workers
            workers = [
                self._background_worker(),
                self._session_cleanup_worker(),
                self._hologram_maintenance_worker(),
                self._stats_monitoring_worker()
            ]
            
            for worker in workers:
                task = asyncio.create_task(worker)
                self.workers.append(task)
            
            self.state = InteractionState.READY
            self.is_running = True
            
            logger.info("🔐 Security orchestrator integrated into InteractionCore")
            logger.info("InteractionCore started with %d background workers", len(workers))
            
        except Exception as e:
            logger.error("Failed to start InteractionCore: %s", e)
            self.state = InteractionState.SHUTDOWN
            raise
    
    async def stop(self):
        """Stop the interaction core and all workers gracefully"""
        self.state = InteractionState.SHUTDOWN
        self.is_running = False
        
        try:
            # Cancel all workers
            for worker in self.workers:
                if not worker.done():
                    worker.cancel()
            
            # Wait for workers to complete
            if self.workers:
                await asyncio.gather(*self.workers, return_exceptions=True)
            
            # Clean up resources
            await self._cleanup_holograms()
            await self._cleanup_sessions()
            self.thread_pool.shutdown(wait=True)
            
            self.workers.clear()
            logger.info("InteractionCore stopped gracefully")
            
        except Exception as e:
            logger.error("Error during InteractionCore shutdown: %s", e)
    
    async def process_message(self, 
                            message: str, 
                            user_context: Dict[str, Any] = None,
                            session_id: str = None) -> str:
        """
        Process a user message with enhanced security and hologram integration
        
        Args:
            message: User message to process
            user_context: User identity and context information
            session_id: Optional session ID for continuous conversation
            
        Returns:
            Response message
        """
        if self.state != InteractionState.READY:
            return "System is not ready. Please wait for initialization."
        
        start_time = time.time()
        self.state = InteractionState.PROCESSING
        
        try:
            # Rate limiting check
            if not await self._check_rate_limit(user_context):
                return "Rate limit exceeded. Please wait a moment."
            
            # Get or create session
            session = await self._get_or_create_session(session_id, user_context)
            
            # Parse command with enhanced security awareness
            parsed_command = await self._parse_command(message, user_context, session)
            
            # Process through security flow
            security_context = await self.security_flow.process_request(
                request_type="user_interaction",
                user_identity=session.user_id,
                source_device=session.device_id,
                data_categories=parsed_command.parameters.get('data_categories', ['conversation'])
            )
            
            # Update session context
            session.security_context = security_context
            session.message_count += 1
            session.last_activity = time.time()
            
            # Handle command based on type
            response = await self._route_command(parsed_command, security_context, session)
            
            # Update statistics
            await self._update_interaction_stats(start_time, True)
            
            # Update hologram states
            await self._update_interaction_hologram(parsed_command, security_context, session)
            
            return response
            
        except Exception as e:
            logger.error("Error processing message: %s", e)
            await self._update_interaction_stats(start_time, False)
            return f"I encountered an error processing your request: {str(e)}"
        finally:
            self.state = InteractionState.READY
    
    async def _get_or_create_session(self, session_id: str, user_context: Dict[str, Any]) -> InteractionSession:
        """Get existing session or create new one"""
        if session_id and session_id in self.active_sessions:
            session = self.active_sessions[session_id]
            session.last_activity = time.time()
            return session
        
        # Create new session
        new_session_id = session_id or f"session_{uuid.uuid4().hex[:8]}"
        user_id = user_context.get('user_id', 'unknown')
        device_id = user_context.get('device_id', 'cli_terminal')
        
        session = InteractionSession(
            session_id=new_session_id,
            user_id=user_id,
            start_time=time.time(),
            device_id=device_id,
            security_context={},
            hologram_node_id=f"session_{new_session_id}"
        )
        
        self.active_sessions[new_session_id] = session
        self.interaction_stats["session_count"] += 1
        
        # Create session hologram
        await self._create_session_hologram(session)
        
        return session
    
    async def _parse_command(self, 
                           message: str, 
                           user_context: Dict[str, Any],
                           session: InteractionSession) -> ParsedCommand:
        """
        Enhanced command parsing with security intent recognition and context awareness
        """
        message_lower = message.lower().strip()
        
        # Check for system commands first
        system_command = await self._detect_system_command(message_lower)
        if system_command:
            return system_command
        
        # Check for identity commands
        identity_command = await self._detect_identity_command(message_lower, session)
        if identity_command:
            return identity_command
        
        # Check for other command types
        for command_type, patterns in self.security_patterns.items():
            for pattern in patterns:
                if re.search(pattern, message_lower, re.IGNORECASE):
                    handler = self.command_handlers.get(command_type)
                    if handler:
                        return await self._create_command_from_pattern(
                            command_type, message, pattern, user_context)
        
        # Default to natural language processing with context
        return await self._create_natural_language_command(message, user_context, session)
    
    async def _detect_system_command(self, message: str) -> Optional[ParsedCommand]:
        """Detect system-level commands"""
        system_commands = {
            'exit': ("system_exit", {"action": "shutdown"}),
            'quit': ("system_exit", {"action": "shutdown"}),
            'stop': ("system_exit", {"action": "shutdown"}),
            'status': ("system_status", {"action": "status_check"}),
            'health': ("system_health", {"action": "health_check"}),
            'help': ("system_help", {"action": "show_help"}),
            'stats': ("system_stats", {"action": "show_stats"}),
            'restart': ("system_restart", {"action": "restart"}),
            'clear': ("system_clear", {"action": "clear_context"})
        }
        
        for cmd, (intent, params) in system_commands.items():
            if message == cmd:
                return ParsedCommand(
                    command_type=CommandType.SYSTEM_COMMAND,
                    intent=intent,
                    parameters=params,
                    confidence=1.0
                )
        
        return None
    
    async def _detect_identity_command(self, message: str, session: InteractionSession) -> Optional[ParsedCommand]:
        """Detect identity-related commands with context awareness"""
        
        # Name setting with improved context
        name_match = await self._extract_name_from_message(message)
        if name_match:
            name, confidence = name_match
            return ParsedCommand(
                command_type=CommandType.IDENTITY_COMMAND,
                intent="set_preferred_name",
                parameters={"name": name, "action": "identity_update"},
                confidence=confidence,
                requires_security=True,
                security_level=SecurityLevel.HIGH
            )
        
        # Birthday and preferences
        birthday_match = await self._extract_birthday_from_message(message)
        if birthday_match:
            birthday, confidence = birthday_match
            return ParsedCommand(
                command_type=CommandType.IDENTITY_COMMAND,
                intent="set_birthday",
                parameters={"birthday": birthday, "action": "identity_update"},
                confidence=confidence,
                requires_security=True,
                security_level=SecurityLevel.HIGH
            )
        
        # Identity queries
        if await self._is_identity_query(message):
            return ParsedCommand(
                command_type=CommandType.IDENTITY_COMMAND,
                intent="get_identity",
                parameters={"action": "identity_query", "session_context": session.security_context},
                confidence=0.9,
                requires_security=True,
                security_level=SecurityLevel.STANDARD
            )
        
        return None
    
    async def _extract_name_from_message(self, message: str) -> Optional[Tuple[str, float]]:
        """Extract name from message with confidence scoring"""
        name_patterns = [
            (r'(?:call me|address me as)\s+([^\.,!?]+)', 0.95),
            (r'(?:my name is|i am|i\'m)\s+([^\.,!?]+)(?!\s+(?:sorry|tired))', 0.9),
            (r'(?:you can call me|you may address me as)\s+([^\.,!?]+)', 0.98),
            (r'(?:set.*name.*to|change.*name.*to)\s+([^\.,!?]+)', 0.85)
        ]
        
        for pattern, confidence in name_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                # Clean up the name
                name = re.sub(r'^(?:as|me|your)\s+', '', name, flags=re.IGNORECASE)
                if len(name) > 1 and len(name) < 50:  # Basic validation
                    return name, confidence
        
        return None
    
    async def _extract_birthday_from_message(self, message: str) -> Optional[Tuple[str, float]]:
        """Extract birthday from message with confidence scoring"""
        birthday_patterns = [
            (r'(?:my birthday is|i was born on)\s+([^\.,!?]+)', 0.9),
            (r'(?:born on|birthdate.*is)\s+([^\.,!?]+)', 0.85),
            (r'(?:february\s+29|29\s+feb|feb\s+29)', 0.8),  # Leap day
            (r'\b(\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*(?:\s+\d{4})?)\b', 0.7)
        ]
        
        for pattern, confidence in birthday_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                birthday = match.group(1).strip()
                return birthday, confidence
        
        return None
    
    async def _is_identity_query(self, message: str) -> bool:
        """Check if message is an identity query"""
        query_patterns = [
            r'who am i',
            r'what is my name',
            r'what do you know about me',
            r'what is my birthday',
            r'when is my birthday',
            r'my birthdate',
            r'tell me about myself',
            r'what are my preferences'
        ]
        
        return any(re.search(pattern, message, re.IGNORECASE) for pattern in query_patterns)
    
    async def _create_command_from_pattern(self, 
                                         command_type: str, 
                                         message: str, 
                                         pattern: str,
                                         user_context: Dict[str, Any]) -> ParsedCommand:
        """Create parsed command from detected pattern"""
        command_configs = {
            'security_status': {
                'type': CommandType.SECURITY_COMMAND,
                'intent': 'security_status',
                'confidence': 0.8,
                'security_level': SecurityLevel.STANDARD
            },
            'autonomy_control': {
                'type': CommandType.AUTONOMY_COMMAND,
                'intent': 'autonomy_control',
                'confidence': 0.7,
                'security_level': SecurityLevel.ELEVATED
            },
            'memory_operations': {
                'type': CommandType.MEMORY_COMMAND,
                'intent': 'memory_operation',
                'confidence': 0.75,
                'security_level': SecurityLevel.STANDARD
            },
            'proactive_requests': {
                'type': CommandType.PROACTIVE_COMMAND,
                'intent': 'proactive_request',
                'confidence': 0.8,
                'security_level': SecurityLevel.STANDARD
            },
            'assistant_commands': {
                'type': CommandType.ASSISTANT_COMMAND,
                'intent': 'assistant_request',
                'confidence': 0.7,
                'security_level': SecurityLevel.STANDARD
            }
        }
        
        config = command_configs.get(command_type, {
            'type': CommandType.NATURAL_LANGUAGE,
            'intent': 'conversation',
            'confidence': 0.6,
            'security_level': SecurityLevel.STANDARD
        })
        
        return ParsedCommand(
            command_type=config['type'],
            intent=config['intent'],
            parameters={
                'message': message,
                'user_context': user_context,
                'detected_pattern': pattern
            },
            confidence=config['confidence'],
            requires_security=config['security_level'] != SecurityLevel.PUBLIC,
            security_level=config['security_level']
        )
    
    async def _create_natural_language_command(self, 
                                             message: str, 
                                             user_context: Dict[str, Any],
                                             session: InteractionSession) -> ParsedCommand:
        """Create natural language command with context analysis"""
        # Analyze message for potential intents
        intent_analysis = await self._analyze_message_intent(message, session)
        
        return ParsedCommand(
            command_type=CommandType.NATURAL_LANGUAGE,
            intent=intent_analysis.get('primary_intent', 'conversation'),
            parameters={
                'message': message,
                'user_context': user_context,
                'session_context': session.security_context,
                'intent_analysis': intent_analysis
            },
            confidence=intent_analysis.get('confidence', 0.6),
            requires_security=True,
            security_level=SecurityLevel.STANDARD,
            user_intent=intent_analysis.get('user_intent'),
            context_tags=intent_analysis.get('tags', [])
        )
    
    async def _analyze_message_intent(self, message: str, session: InteractionSession) -> Dict[str, Any]:
        """Analyze message for deeper intent understanding"""
        analysis = {
            'primary_intent': 'conversation',
            'confidence': 0.6,
            'tags': [],
            'user_intent': None
        }
        
        # Basic intent detection
        message_lower = message.lower()
        
        if any(word in message_lower for word in ['how', 'what', 'when', 'where', 'why', 'can you']):
            analysis['primary_intent'] = 'information_request'
            analysis['confidence'] = 0.7
            analysis['tags'].append('question')
        
        if any(word in message_lower for word in ['thank', 'thanks', 'appreciate']):
            analysis['primary_intent'] = 'gratitude'
            analysis['confidence'] = 0.8
            analysis['tags'].append('social')
        
        if any(word in message_lower for word in ['help', 'assist', 'support']):
            analysis['primary_intent'] = 'help_request'
            analysis['confidence'] = 0.75
            analysis['tags'].append('assistance')
        
        # Contextual analysis based on session history
        if session.message_count > 0:
            analysis['tags'].append('follow_up')
        
        return analysis
    
    async def _route_command(self, 
                           command: ParsedCommand, 
                           security_context: Dict[str, Any],
                           session: InteractionSession) -> str:
        """Route command to appropriate handler"""
        try:
            if command.command_type == CommandType.IDENTITY_COMMAND:
                return await self._handle_identity_command(command, security_context, session)
            elif command.command_type == CommandType.SECURITY_COMMAND:
                return await self._handle_security_command(command, security_context, session)
            elif command.command_type == CommandType.SYSTEM_COMMAND:
                return await self._handle_system_command(command, security_context, session)
            elif command.command_type == CommandType.AUTONOMY_COMMAND:
                return await self._handle_autonomy_command(command, security_context, session)
            elif command.command_type == CommandType.MEMORY_COMMAND:
                return await self._handle_memory_command(command, security_context, session)
            elif command.command_type == CommandType.PROACTIVE_COMMAND:
                return await self._handle_proactive_command(command, security_context, session)
            elif command.command_type == CommandType.ASSISTANT_COMMAND:
                return await self._handle_assistant_command(command, security_context, session)
            else:
                return await self._process_natural_language(command, security_context, session)
                
        except Exception as e:
            logger.error("Error routing command: %s", e)
            return f"Error processing command: {str(e)}"
    
    async def _handle_identity_command(self, 
                                     command: ParsedCommand, 
                                     security_context: Dict[str, Any],
                                     session: InteractionSession) -> str:
        """Handle identity commands with proper security and hologram integration"""
        try:
            user_id = security_context.get('user_id', 'owner')
            
            if command.intent == "set_preferred_name":
                return await self._handle_identity_set_name(command, user_id, session)
            elif command.intent == "set_birthday":
                return await self._handle_identity_set_birthday(command, user_id, session)
            elif command.intent == "get_identity":
                return await self._handle_identity_query(command, user_id, session)
            else:
                return "Unknown identity command. Try using natural language like 'call me [name]' or 'my birthday is [date]'."
                
        except Exception as e:
            logger.error("Error handling identity command: %s", e)
            return f"Error processing identity command: {str(e)}"
    
    async def _handle_identity_set_name(self, command: ParsedCommand, user_id: str, session: InteractionSession) -> str:
        """Handle setting preferred name"""
        name = command.parameters.get('name')
        if not name:
            return "I didn't catch the name. Please try again like 'Call me Alex'."
        
        # Create hologram for identity update
        hologram_id = await self._create_identity_hologram(
            user_id, "name_update", {"new_name": name, "session_id": session.session_id})
        
        try:
            # Update identity through identity manager
            success = await self.identity_manager.set_preferred_name(user_id, name)
            
            if success:
                # Update session context
                session.security_context['preferred_name'] = name
                
                # Update hologram state
                await self._update_hologram_state(hologram_id, "completed", {"success": True})
                
                logger.info("Successfully updated preferred name for %s to %s", user_id, name)
                return f"I'll now address you as {name}. Your preferred name has been updated securely."
            else:
                await self._update_hologram_state(hologram_id, "failed", {"error": "identity_update_failed"})
                return "I couldn't update your name. The identity system returned an error."
                
        except Exception as e:
            await self._update_hologram_state(hologram_id, "error", {"exception": str(e)})
            logger.error("Failed to update preferred name: %s", e)
            return f"System error updating name: {str(e)}"
    
    async def _handle_identity_set_birthday(self, command: ParsedCommand, user_id: str, session: InteractionSession) -> str:
        """Handle setting birthday"""
        birthday = command.parameters.get('birthday')
        if not birthday:
            return "I didn't catch the birthday date. Please try again like 'My birthday is 29 February'."
        
        # Create hologram for birthday update
        hologram_id = await self._create_identity_hologram(
            user_id, "birthday_update", {"birthday": birthday, "session_id": session.session_id})
        
        try:
            # Store birthday in secure memory
            memory_data = {
                "type": "personal_info",
                "category": "birthday",
                "value": birthday,
                "security_level": "high",
                "user_id": user_id,
                "timestamp": time.time()
            }
            
            # Use cognition core to store this information
            storage_result = await self.cognition_core.process_request(
                request_type="memory_store",
                user_identity=user_id,
                data=memory_data
            )
            
            if storage_result.get('success', False):
                # Update session context
                session.security_context['birthday'] = birthday
                
                await self._update_hologram_state(hologram_id, "completed", {"success": True})
                
                # Add contextual response for leap day
                if 'february 29' in birthday.lower() or 'feb 29' in birthday.lower():
                    return f"I've stored your birthday ({birthday}) securely. That's a leap day! I'll remember this special date."
                else:
                    return f"I've stored your birthday ({birthday}) securely. I'll remember this important date."
            else:
                await self._update_hologram_state(hologram_id, "failed", {"error": "storage_failed"})
                return "I couldn't store your birthday. The memory system returned an error."
                
        except Exception as e:
            await self._update_hologram_state(hologram_id, "error", {"exception": str(e)})
            logger.error("Failed to store birthday: %s", e)
            return f"System error storing birthday: {str(e)}"
    
    async def _handle_identity_query(self, command: ParsedCommand, user_id: str, session: InteractionSession) -> str:
        """Handle identity queries"""
        # Create query hologram
        hologram_id = await self._create_identity_hologram(user_id, "identity_query", {"session_id": session.session_id})
        
        try:
            # Get user identity information
            user_identity = await self.identity_manager.get_identity(user_id)
            preferred_name = user_identity.get('preferred_name', 'Owner')
            
            # Build comprehensive response
            response_parts = [f"Identity Information for {preferred_name}"]
            response_parts.append(f"Security Level: {session.security_context.get('access_level', 'standard')}")
            response_parts.append(f"Current Device: {session.security_context.get('source_device', 'unknown')}")
            
            # Add birthday if available
            if session.security_context.get('birthday'):
                response_parts.append(f"Birthday: {session.security_context['birthday']}")
            
            # Add access information
            data_categories = session.security_context.get('data_categories', [])
            if data_categories:
                response_parts.append(f"Data Access: {', '.join(data_categories)}")
            
            # Add session information
            response_parts.append(f"Session Messages: {session.message_count}")
            response_parts.append(f"Session Duration: {int(time.time() - session.start_time)}s")
            
            await self._update_hologram_state(hologram_id, "completed", {"query_type": "full_identity"})
            
            return "\n".join(response_parts)
            
        except Exception as e:
            await self._update_hologram_state(hologram_id, "error", {"exception": str(e)})
            logger.error("Failed to query identity: %s", e)
            return f"Error retrieving identity information: {str(e)}"
    
    async def _handle_security_command(self, command: ParsedCommand, security_context: Dict[str, Any], session: InteractionSession) -> str:
        """Handle security-related commands"""
        try:
            if command.intent == "security_status":
                return await self._handle_security_status(command, security_context, session)
            else:
                return "Unknown security command. Try 'security status' for current security information."
                
        except Exception as e:
            logger.error("Error handling security command: %s", e)
            return f"Error processing security command: {str(e)}"
    
    async def _handle_security_status(self, command: ParsedCommand, security_context: Dict[str, Any], session: InteractionSession) -> str:
        """Handle security status command"""
        user_id = security_context.get('user_id', 'owner')
        
        # Get comprehensive security status
        status_info = {
            "user": user_id,
            "access_level": security_context.get('access_level', 'standard'),
            "device": security_context.get('source_device', 'unknown'),
            "data_categories": security_context.get('data_categories', []),
            "authentication_method": security_context.get('auth_method', 'totp'),
            "security_flow": security_context.get('flow_path', 'unknown'),
            "session_id": session.session_id,
            "message_count": session.message_count
        }
        
        response = ["Comprehensive Security Status"]
        response.append("User Identity")
        response.append(f"User ID: {status_info['user']}")
        response.append(f"Access Level: {status_info['access_level']}")
        response.append(f"Auth Method: {status_info['authentication_method']}")
        
        response.append("Device & Session")
        response.append(f"Device: {status_info['device']}")
        response.append(f"Session: {status_info['session_id']}")
        response.append(f"Messages: {status_info['message_count']}")
        
        response.append("Data Access")
        response.append(f"Categories: {', '.join(status_info['data_categories'])}")
        response.append(f"Security Flow: {status_info['security_flow']}")
        
        response.append("System Statistics")
        response.append(f"Total Messages: {self.interaction_stats['total_messages']}")
        response.append(f"Success Rate: {self._calculate_success_rate():.1f}%")
        response.append(f"Active Sessions: {len(self.active_sessions)}")
        
        return "\n".join(response)
    
    async def _handle_system_command(self, command: ParsedCommand, security_context: Dict[str, Any], session: InteractionSession) -> str:
        """Handle system commands"""
        try:
            if command.intent == "system_exit":
                return "Session ending. Type anything to start a new session."
            elif command.intent == "system_status":
                return await self._get_system_status()
            elif command.intent == "system_health":
                return await self._get_system_health()
            elif command.intent == "system_help":
                return await self._get_system_help()
            elif command.intent == "system_stats":
                return await self._get_system_stats(session)
            elif command.intent == "system_clear":
                await self._clear_conversation_context(session)
                return "Conversation context cleared."
            else:
                return "Unknown system command."
                
        except Exception as e:
            logger.error("Error handling system command: %s", e)
            return f"Error processing system command: {str(e)}"
    
    async def _handle_autonomy_command(self, command: ParsedCommand, security_context: Dict[str, Any], session: InteractionSession) -> str:
        """Handle autonomy commands"""
        if not self.autonomy_core:
            return "Autonomy core is not available."
        
        try:
            # Implement autonomy command handling
            return "Autonomy features are currently under development."
            
        except Exception as e:
            logger.error("Error handling autonomy command: %s", e)
            return f"Error processing autonomy command: {str(e)}"
    
    async def _handle_memory_command(self, command: ParsedCommand, security_context: Dict[str, Any], session: InteractionSession) -> str:
        """Handle memory commands"""
        try:
            # Implement memory command handling
            return "Memory operations are currently under development."
            
        except Exception as e:
            logger.error("Error handling memory command: %s", e)
            return f"Error processing memory command: {str(e)}"
    
    async def _handle_proactive_command(self, command: ParsedCommand, security_context: Dict[str, Any], session: InteractionSession) -> str:
        """Handle proactive commands"""
        if not self.proactive_communicator:
            return "Proactive communicator is not available."
        
        try:
            # Implement proactive command handling
            return "Proactive features are currently under development."
            
        except Exception as e:
            logger.error("Error handling proactive command: %s", e)
            return f"Error processing proactive command: {str(e)}"
    
    async def _handle_assistant_command(self, command: ParsedCommand, security_context: Dict[str, Any], session: InteractionSession) -> str:
        """Handle assistant commands"""
        if not self.assistant_core:
            return "Assistant core is not available."
        
        try:
            # Implement assistant command handling
            return "Assistant features are currently under development."
            
        except Exception as e:
            logger.error("Error handling assistant command: %s", e)
            return f"Error processing assistant command: {str(e)}"
    
    async def _process_natural_language(self, 
                                      command: ParsedCommand, 
                                      security_context: Dict[str, Any],
                                      session: InteractionSession) -> str:
        """Process natural language through cognition core with enhanced context"""
        try:
            # Enhance context with security and session information
            enhanced_context = {
                "user_identity": security_context.get('user_id'),
                "access_level": security_context.get('access_level'),
                "source_device": security_context.get('source_device'),
                "data_categories": security_context.get('data_categories'),
                "security_flow": security_context.get('flow_path'),
                "session_id": session.session_id,
                "message_count": session.message_count,
                "preferred_name": session.security_context.get('preferred_name'),
                "user_intent": command.user_intent,
                "context_tags": command.context_tags
            }
            
            # Add conversation context for continuity
            if session.session_id in self.conversation_context:
                enhanced_context["conversation_history"] = self.conversation_context[session.session_id]
            
            # Process through cognition core
            response = await self.cognition_core.process_request(
                request_type="user_message",
                user_identity=security_context.get('user_id'),
                message=command.parameters['message'],
                context=enhanced_context
            )
            
            # Update conversation context
            await self._update_conversation_context(session, command.parameters['message'], response.get('response', ''))
            
            return response.get('response', 'I encountered an error processing your message.')
            
        except Exception as e:
            logger.error("Error in natural language processing: %s", e)
            return f"Error processing your message: {str(e)}"
    
    # Hologram Integration Methods
    async def _initialize_hologram_states(self):
        """Initialize hologram states for interaction tracking"""
        try:
            # Create main interaction hologram
            main_hologram = HologramState(
                node_id='interaction_core_main',
                link_id='link_interaction_core',
                state_type='core_system',
                data={
                    'initialized_at': time.time(),
                    'version': '2.1.0',
                    'subsystems': ['security', 'cognition', 'identity', 'session']
                },
                created_at=time.time(),
                updated_at=time.time()
            )
            
            self.active_holograms['interaction_core_main'] = main_hologram
            
            # Initialize hologram states registry
            self.hologram_states = {
                'interaction_core': {
                    'node_id': 'interaction_core_main',
                    'state': 'active',
                    'created_at': time.time(),
                    'interaction_count': 0,
                    'last_command_type': None,
                    'subsystem_states': {
                        'session_manager': 'active',
                        'command_parser': 'active',
                        'security_integration': 'active'
                    }
                }
            }
            
            logger.info("Hologram base state initialized for interaction core")
            
        except Exception as e:
            logger.error("Error initializing hologram states: %s", e)
    
    async def _create_session_hologram(self, session: InteractionSession):
        """Create hologram for a new session"""
        try:
            session_hologram = HologramState(
                node_id=session.hologram_node_id,
                link_id=f"link_{session.session_id}",
                state_type='user_session',
                data={
                    'user_id': session.user_id,
                    'device_id': session.device_id,
                    'start_time': session.start_time,
                    'security_level': session.security_context.get('access_level', 'unknown')
                },
                created_at=time.time(),
                updated_at=time.time(),
                ttl=7200  # 2 hours for sessions
            )
            
            self.active_holograms[session.session_id] = session_hologram
            logger.debug("Created session hologram for %s", session.session_id)
            
        except Exception as e:
            logger.error("Error creating session hologram: %s", e)
    
    async def _create_identity_hologram(self, user_id: str, operation: str, data: Dict[str, Any]) -> str:
        """Create a hologram state for identity operations"""
        try:
            hologram_id = f"identity_{operation}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
            
            identity_hologram = HologramState(
                node_id=f"identity_{operation}",
                link_id=f"link_{hologram_id}",
                state_type='identity_operation',
                data={
                    'user_id': user_id,
                    'operation': operation,
                    **data
                },
                created_at=time.time(),
                updated_at=time.time()
            )
            
            self.active_holograms[hologram_id] = identity_hologram
            logger.info("Created identity hologram: %s for %s", hologram_id, operation)
            
            return hologram_id
            
        except Exception as e:
            logger.error("Error creating identity hologram: %s", e)
            return f"error_{int(time.time())}"
    
    async def _update_hologram_state(self, hologram_id: str, new_state: str, additional_data: Dict[str, Any] = None):
        """Update hologram state"""
        try:
            if hologram_id in self.active_holograms:
                hologram = self.active_holograms[hologram_id]
                hologram.state_type = new_state
                hologram.updated_at = time.time()
                
                if additional_data:
                    hologram.data.update(additional_data)
                
                logger.debug("Updated hologram %s to state %s", hologram_id, new_state)
                
        except Exception as e:
            logger.error("Error updating hologram state: %s", e)
    
    async def _update_interaction_hologram(self, command: ParsedCommand, security_context: Dict[str, Any], session: InteractionSession):
        """Update interaction hologram with command information"""
        try:
            if 'interaction_core' in self.hologram_states:
                core_state = self.hologram_states['interaction_core']
                core_state['interaction_count'] += 1
                core_state['last_command_type'] = command.command_type.value
                core_state['last_security_level'] = security_context.get('access_level')
                core_state['last_session'] = session.session_id
                core_state['updated_at'] = time.time()
                
                # Update session hologram
                if session.session_id in self.active_holograms:
                    session_hologram = self.active_holograms[session.session_id]
                    session_hologram.data['message_count'] = session.message_count
                    session_hologram.data['last_activity'] = session.last_activity
                    session_hologram.updated_at = time.time()
                
        except Exception as e:
            logger.error("Error updating interaction hologram: %s", e)
    
    async def _cleanup_holograms(self):
        """Clean up expired hologram states"""
        try:
            current_time = time.time()
            expired_holograms = []
            
            for hologram_id, hologram_data in self.active_holograms.items():
                if current_time - hologram_data.created_at > hologram_data.ttl:
                    expired_holograms.append(hologram_id)
            
            for hologram_id in expired_holograms:
                del self.active_holograms[hologram_id]
                
            if expired_holograms:
                logger.info("Cleaned up %d expired holograms", len(expired_holograms))
                
        except Exception as e:
            logger.error("Error cleaning up holograms: %s", e)
    
    # Session Management Methods
    async def _initialize_session_cleanup(self):
        """Initialize session cleanup system"""
        try:
            # Clean up any stale sessions on startup
            await self._cleanup_sessions()
            logger.info("Session cleanup system initialized")
            
        except Exception as e:
            logger.error("Error initializing session cleanup: %s", e)
    
    async def _cleanup_sessions(self):
        """Clean up expired sessions"""
        try:
            current_time = time.time()
            expired_sessions = []
            
            for session_id, session in self.active_sessions.items():
                if current_time - session.last_activity > self.session_timeout:
                    expired_sessions.append(session_id)
            
            for session_id in expired_sessions:
                # Clean up session hologram
                if session_id in self.active_holograms:
                    del self.active_holograms[session_id]
                
                # Clean up conversation context
                if session_id in self.conversation_context:
                    del self.conversation_context[session_id]
                
                del self.active_sessions[session_id]
                
            if expired_sessions:
                logger.info("Cleaned up %d expired sessions", len(expired_sessions))
                
        except Exception as e:
            logger.error("Error cleaning up sessions: %s", e)
    
    # Context Management Methods
    async def _initialize_context_manager(self):
        """Initialize conversation context management"""
        try:
            self.conversation_context = {}
            logger.info("Conversation context manager initialized")
            
        except Exception as e:
            logger.error("Error initializing context manager: %s", e)
    
    async def _update_conversation_context(self, session: InteractionSession, user_message: str, assistant_response: str):
        """Update conversation context for continuity"""
        try:
            if session.session_id not in self.conversation_context:
                self.conversation_context[session.session_id] = []
            
            # Keep only last 10 exchanges to manage memory
            context = self.conversation_context[session.session_id]
            context.append({"user": user_message, "assistant": assistant_response})
            
            if len(context) > 10:
                context.pop(0)
                
        except Exception as e:
            logger.error("Error updating conversation context: %s", e)
    
    async def _clear_conversation_context(self, session: InteractionSession):
        """Clear conversation context for a session"""
        try:
            if session.session_id in self.conversation_context:
                del self.conversation_context[session.session_id]
                
        except Exception as e:
            logger.error("Error clearing conversation context: %s", e)
    
    # Rate Limiting Methods
    async def _check_rate_limit(self, user_context: Dict[str, Any]) -> bool:
        """Check if user is within rate limits"""
        try:
            user_id = user_context.get('user_id', 'default')
            current_time = time.time()
            
            if user_id not in self.rate_limits:
                self.rate_limits[user_id] = []
            
            # Clean old requests (older than 1 minute)
            user_requests = self.rate_limits[user_id]
            user_requests = [req_time for req_time in user_requests if current_time - req_time < 60]
            self.rate_limits[user_id] = user_requests
            
            # Check if within limit
            if len(user_requests) >= self.max_requests_per_minute:
                return False
            
            # Add current request
            user_requests.append(current_time)
            return True
            
        except Exception as e:
            logger.error("Error checking rate limit: %s", e)
            return True  # Fail open on error
    
    # Statistics and Monitoring Methods
    async def _update_interaction_stats(self, start_time: float, success: bool):
        """Update interaction statistics"""
        try:
            response_time = time.time() - start_time
            
            self.interaction_stats["total_messages"] += 1
            
            if success:
                self.interaction_stats["successful_responses"] += 1
            else:
                self.interaction_stats["failed_responses"] += 1
            
            # Update average response time (moving average)
            current_avg = self.interaction_stats["avg_response_time"]
            total_successful = self.interaction_stats["successful_responses"]
            
            if total_successful > 0:
                self.interaction_stats["avg_response_time"] = (
                    (current_avg * (total_successful - 1) + response_time) / total_successful
                )
                
        except Exception as e:
            logger.error("Error updating interaction stats: %s", e)
    
    def _calculate_success_rate(self) -> float:
        """Calculate current success rate"""
        total = self.interaction_stats["total_messages"]
        successful = self.interaction_stats["successful_responses"]
        
        if total == 0:
            return 0.0
        
        return (successful / total) * 100
    
    async def _get_system_status(self) -> str:
        """Get comprehensive system status"""
        status_parts = ["System Status"]
        
        status_parts.append(f"Core State: {self.state.value}")
        status_parts.append(f"Active Sessions: {len(self.active_sessions)}")
        status_parts.append(f"Background Workers: {len(self.workers)}")
        status_parts.append(f"Active Holograms: {len(self.active_holograms)}")
        status_parts.append(f"Total Messages: {self.interaction_stats['total_messages']}")
        status_parts.append(f"Success Rate: {self._calculate_success_rate():.1f}%")
        
        # Subsystem status
        subsystems = {
            "Security": self.security_orchestrator is not None,
            "Cognition": self.cognition_core is not None,
            "Autonomy": self.autonomy_core is not None,
            "Proactive": self.proactive_communicator is not None,
            "Assistant": self.assistant_core is not None
        }
        
        status_parts.append("Subsystems:")
        for name, available in subsystems.items():
            status = "Available" if available else "Unavailable"
            status_parts.append(f"{name}: {status}")
        
        return "\n".join(status_parts)
    
    async def _get_system_health(self) -> str:
        """Get system health information"""
        health_parts = ["System Health"]
        
        # Memory health (simplified)
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024
        
        health_parts.append(f"Memory Usage: {memory_mb:.1f} MB")
        health_parts.append(f"Active Sessions: {len(self.active_sessions)}")
        health_parts.append(f"Session Health: {'Good' if len(self.active_sessions) < 100 else 'Warning'}")
        
        # Rate limiting health
        total_rate_limited = sum(1 for requests in self.rate_limits.values() if len(requests) > self.max_requests_per_minute * 0.8)
        health_parts.append(f"Rate Limit Health: {'Good' if total_rate_limited == 0 else 'Monitoring'}")
        
        # Hologram health
        hologram_health = "Good" if len(self.active_holograms) < 1000 else "High Load"
        health_parts.append(f"Hologram Health: {hologram_health}")
        
        return "\n".join(health_parts)
    
    async def _get_system_help(self) -> str:
        """Get system help information"""
        help_parts = ["Available Commands"]
        
        help_parts.append("Identity Commands:")
        help_parts.append("  'Call me [name]' - Set your preferred name")
        help_parts.append("  'My birthday is [date]' - Store your birthday")
        help_parts.append("  'Who am I?' - View your identity information")
        
        help_parts.append("Security Commands:")
        help_parts.append("  'Security status' - View current security configuration")
        help_parts.append("  'System status' - View system status")
        
        help_parts.append("System Commands:")
        help_parts.append("  'status' - System status")
        help_parts.append("  'health' - System health")
        help_parts.append("  'stats' - Session statistics")
        help_parts.append("  'help' - This help message")
        help_parts.append("  'exit' - End session")
        
        help_parts.append("Natural Language:")
        help_parts.append("  All other messages are processed as natural language")
        
        return "\n".join(help_parts)
    
    async def _get_system_stats(self, session: InteractionSession) -> str:
        """Get system statistics"""
        stats_parts = ["Session Statistics"]
        
        session_duration = time.time() - session.start_time
        stats_parts.append(f"Duration: {session_duration:.1f}s")
        stats_parts.append(f"Messages: {session.message_count}")
        stats_parts.append(f"Success Rate: {self._calculate_success_rate():.1f}%")
        stats_parts.append(f"Avg Response Time: {self.interaction_stats['avg_response_time']:.2f}s")
        
        stats_parts.append("Overall Statistics:")
        stats_parts.append(f"Total Messages: {self.interaction_stats['total_messages']}")
        stats_parts.append(f"Successful: {self.interaction_stats['successful_responses']}")
        stats_parts.append(f"Failed: {self.interaction_stats['failed_responses']}")
        stats_parts.append(f"Sessions: {self.interaction_stats['session_count']}")
        
        return "\n".join(stats_parts)
    
    # Background Workers
    async def _background_worker(self):
        """Main background worker for core maintenance"""
        while self.is_running:
            try:
                # Perform regular maintenance tasks
                await self._perform_maintenance()
                await asyncio.sleep(60)  # Run every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Background worker error: %s", e)
                await asyncio.sleep(30)
    
    async def _session_cleanup_worker(self):
        """Worker for cleaning up expired sessions"""
        while self.is_running:
            try:
                await self._cleanup_sessions()
                await asyncio.sleep(300)  # Run every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Session cleanup worker error: %s", e)
                await asyncio.sleep(60)
    
    async def _hologram_maintenance_worker(self):
        """Worker for hologram maintenance"""
        while self.is_running:
            try:
                await self._cleanup_holograms()
                await asyncio.sleep(600)  # Run every 10 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Hologram maintenance worker error: %s", e)
                await asyncio.sleep(60)
    
    async def _stats_monitoring_worker(self):
        """Worker for monitoring and statistics"""
        while self.is_running:
            try:
                # Log statistics periodically
                if self.interaction_stats["total_messages"] > 0:
                    success_rate = self._calculate_success_rate()
                    logger.info(
                        "Interaction stats - Total: %d, Success: %.1f%%, Avg Time: %.2fs",
                        self.interaction_stats["total_messages"],
                        success_rate,
                        self.interaction_stats["avg_response_time"]
                    )
                
                await asyncio.sleep(300)  # Run every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Stats monitoring worker error: %s", e)
                await asyncio.sleep(60)
    
    async def _perform_maintenance(self):
        """Perform regular maintenance tasks"""
        try:
            # Clean up rate limit data
            current_time = time.time()
            for user_id in list(self.rate_limits.keys()):
                self.rate_limits[user_id] = [
                    req_time for req_time in self.rate_limits[user_id] 
                    if current_time - req_time < 120  # Keep only last 2 minutes
                ]
                if not self.rate_limits[user_id]:
                    del self.rate_limits[user_id]
            
            # Update hologram states
            if 'interaction_core' in self.hologram_states:
                self.hologram_states['interaction_core']['maintenance_cycles'] = \
                    self.hologram_states['interaction_core'].get('maintenance_cycles', 0) + 1
                self.hologram_states['interaction_core']['last_maintenance'] = current_time
            
        except Exception as e:
            logger.error("Error during maintenance: %s", e)
    
    # Public API Methods
    def get_status(self) -> Dict[str, Any]:
        """Get interaction core status"""
        return {
            "state": self.state.value,
            "workers_active": len(self.workers),
            "active_sessions": len(self.active_sessions),
            "active_holograms": len(self.active_holograms),
            "is_running": self.is_running,
            "statistics": self.interaction_stats.copy()
        }
    
    async def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific session"""
        if session_id not in self.active_sessions:
            return None
        
        session = self.active_sessions[session_id]
        return {
            "session_id": session.session_id,
            "user_id": session.user_id,
            "start_time": session.start_time,
            "device_id": session.device_id,
            "message_count": session.message_count,
            "last_activity": session.last_activity,
            "security_context": session.security_context
        }
    
    async def list_active_sessions(self) -> List[Dict[str, Any]]:
        """List all active sessions"""
        sessions = []
        for session in self.active_sessions.values():
            sessions.append({
                "session_id": session.session_id,
                "user_id": session.user_id,
                "message_count": session.message_count,
                "duration": time.time() - session.start_time
            })
        return sessions
    
    async def terminate_session(self, session_id: str) -> bool:
        """Terminate a specific session"""
        if session_id not in self.active_sessions:
            return False
        
        # Clean up session resources
        if session_id in self.active_holograms:
            del self.active_holograms[session_id]
        
        if session_id in self.conversation_context:
            del self.conversation_context[session_id]
        
        del self.active_sessions[session_id]
        logger.info("Terminated session: %s", session_id)
        return True


# Factory function for creating InteractionCore instance
async def create_interaction_core(
    security_orchestrator: SecurityOrchestrator, 
    cognition_core: CognitionCore,
    autonomy_core: Optional[AutonomyCore] = None,
    proactive_communicator: Optional[ProactiveCommunicator] = None,
    assistant_core: Optional[AssistantCore] = None
) -> EnhancedInteractionCore:
    """Create and initialize an EnhancedInteractionCore instance"""
    core = EnhancedInteractionCore(
        security_orchestrator=security_orchestrator,
        cognition_core=cognition_core,
        autonomy_core=autonomy_core,
        proactive_communicator=proactive_communicator,
        assistant_core=assistant_core
    )
    await core.start()
    logger.info("InteractionCore constructed successfully")
    return core