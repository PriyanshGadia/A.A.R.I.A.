# main.py - A.A.R.I.A System Entry Point (Async Production)
# [UPGRADED] - This file is now a clean "Composition Root".
# - Implements correct Dependency Injection for the new agentic architecture.
# - Fixes the "Persona quick persist failed" shutdown bug.
# - Removes the flawed CLI parsers (set_name, security) from the
#   interactive loop, forcing all requests through the agent "Brain".
#
from __future__ import annotations

import os
import sys
import logging
import traceback
import asyncio
import inspect
import time
from zoneinfo import ZoneInfo
from datetime import datetime
from typing import Any, Dict, Optional

# Optional third-party helpers
try:
    import pyotp
except Exception:
    pyotp = None

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = lambda *a, **k: None

# Defensive imports for core modules
_missing = []
def _try_import(name: str, alias: Optional[str] = None):
    try:
        mod = __import__(name)
        return mod
    except Exception as e:
        _missing.append((name, e))
        return None

# --- [UPGRADED] ---
# We now rely on all our upgraded cores being present.
# --- [END UPGRADED] ---
try: from llm_adapter import LLMAdapterFactory, LLMProvider
except Exception: LLMAdapterFactory, LLMProvider = None, None
try: from secure_store import SecureStorageAsync
except Exception: SecureStorageAsync = None
try: from assistant_core import AssistantCore
except Exception: AssistantCore = None
try: from persona_core import EnhancedPersonaCore as PersonaCore
except Exception: PersonaCore = None
try: from cognition_core import CognitionCore
except Exception: CognitionCore = None
try: from autonomy_core import AutonomyCore, create_autonomy_core
except Exception: AutonomyCore, create_autonomy_core = None, None
try: from interaction_core import InteractionCore, InboundMessage, create_interaction_core
except Exception: InteractionCore, InboundMessage, create_interaction_core = None, None, None
try:
    from memory_manager import MemoryManager
except Exception:
    MemoryManager = None
    import logging
    logging.getLogger("AARIA.System").error("Memory Manager not available - cannot load memory subsystem")
try: from access_control import AccessControlSystem, AccessLevel, RequestSource
except Exception: AccessControlSystem = None
try: from identity_manager import IdentityManager
except Exception: IdentityManager = None
try: from security_orchestrator import SecurityOrchestrator
except Exception: SecurityOrchestrator = None
try: from security_config import DynamicSecurityConfig
except Exception: DynamicSecurityConfig = None
try: from device_types import DeviceManager
except Exception: DeviceManager = None
try:
    from proactive_comm import ProactiveCommunicator, HTTPWebhookAdapter
except Exception:
    try:
        from proactive_comm import ProactiveCommunicator  # type: ignore
        HTTPWebhookAdapter = None
    except Exception:
        ProactiveCommunicator = None
        HTTPWebhookAdapter = None
try: import hologram_state
except Exception: hologram_state = None

# Logging
LOG_FILE = "aaria_system.log"
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("AARIA.System")

# -------------------------
# Minimal time utilities (fallback)
# -------------------------
try:
    from time_utils import parse_to_timestamp, to_local_iso, now_ts
except Exception:
    def now_ts() -> float: return time.time()
    # (other time fallbacks omitted for brevity, they are unchanged)

# -------------------------
# Fallback Adapters (safe defaults)
# -------------------------
class LoggingAdapter:
    def __init__(self, name: str = "logging"):
        self.name = name; self._log = logging.getLogger(f"AARIA.Adapter.{name}")
    def send(self, envelope):
        try:
            self._log.info("[%s] send called for channel=%s subject=%s payload=%s",
                           self.name, envelope.get('channel'), envelope.get('subject_identity'), envelope.get('payload'))
            async def _sim():
                await asyncio.sleep(0.05)
                return True, f"{self.name}_receipt_{int(now_ts()*1000)}"
            return _sim()
        except Exception as e: return False, str(e)

# -------------------------
# AARIASystem
# -------------------------
class AARIASystem:
    def __init__(self):
        self.logger = logger
        self.components: Dict[str, Any] = {}
        self.is_running = False
        load_dotenv(dotenv_path="llm.env")
        self.config = self._load_system_config()

    def _load_system_config(self) -> Dict[str, Any]:
        return {
            "llm_provider": os.getenv("LLM_PROVIDER", "groq"),
            "llm_model": os.getenv("LLM_MODEL", "llama-3.1-8b-instant"),
            "max_concurrent_actions": int(os.getenv("MAX_CONCURRENT_ACTIONS", "3")),
            "session_timeout": int(os.getenv("SESSION_TIMEOUT", "3600")),
            "enable_autonomy": os.getenv("ENABLE_AUTONOMY", "true").lower() == "true",
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
            "master_password": os.getenv("AARIA_MASTER_PASSWORD", ""),
            "proactive_concurrency": int(os.getenv("PROACTIVE_CONCURRENCY", "1")),
            "proactive_rate_limit_per_minute": int(os.getenv("PROACTIVE_RATE_LIMIT", "60")),
            "allow_proactive_by_default": os.getenv("ALLOW_PROACTIVE_BY_DEFAULT", "false").lower() == "true",
            "proactive_webhook_url": os.getenv("PROACTIVE_WEBHOOK_URL", ""),
        }

    def _validate_environment(self) -> bool:
        master_password = self.config.get("master_password", "")
        if not master_password:
            self.logger.error("❌ AARIA_MASTER_PASSWORD not found. Set it in llm.env or environment.")
            return False
        if len(master_password) < 8:
            self.logger.warning("⚠️  AARIA_MASTER_PASSWORD shorter than recommended (8 chars).")
        return True

    # -------------------------
    # Hologram wrapper (remains the same)
    # -------------------------
    async def _hologram_call(self, method_name: str, *args, **kwargs):
        if hologram_state is None: return None
        try:
            method = getattr(hologram_state, method_name, None)
            if method is None:
                if hasattr(hologram_state, "initialize_base_state"):
                    maybe = hologram_state.initialize_base_state()
                    if inspect.iscoroutine(maybe): await maybe
                    method = getattr(hologram_state, method_name, None)
                    if method is None:
                        self.logger.debug("hologram_state lacks %s even after base init", method_name)
                        return None
            if inspect.iscoroutinefunction(method):
                return await method(*args, **kwargs)
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, lambda: method(*args, **kwargs))
        except Exception as e:
            self.logger.debug("hologram_call %s failed: %s", method_name, e, exc_info=True)
            return None

    # -------------------------
    # Initialize system components
    # -------------------------
    
    # --- [UPGRADED] ---
    # This method now implements the correct Dependency Injection order
    # and wires the new components together.
    # --- [END UPGRADED] ---
    async def initialize(self) -> bool:
        self.logger.info("🚀 Initializing A.A.R.I.A System with Security...")
        try:
            if not self._validate_environment(): return False
            if AssistantCore is None:
                self.logger.error("AssistantCore module missing — cannot start.")
                return False

            # 1. Assistant Core (Foundation)
            self.logger.info("Initializing Assistant Core...")
            assistant = AssistantCore(password=self.config["master_password"], auto_recover=True)
            await assistant.initialize()
            self.components["assistant"] = assistant

            # 2. Security Components
            self.logger.info("Initializing Dynamic Security Configuration...")
            security_config = DynamicSecurityConfig(assistant)
            await security_config.initialize()
            self.components["security_config"] = security_config

            self.logger.info("Initializing Device Manager...")
            device_manager = DeviceManager(assistant)
            await device_manager.initialize()
            self.components["device_manager"] = device_manager

            self.logger.info("Initializing Security Orchestrator...")
            security = SecurityOrchestrator(assistant)
            setattr(security, "security_config", security_config)
            setattr(security, "device_manager", device_manager)
            await security.initialize()
            self.components["security"] = security
            assistant.security_orchestrator = security # Inject back into core
            if hasattr(security, 'identity_manager'):
                assistant.identity_manager = security.identity_manager
            if LLMAdapterFactory:
                assistant.llm_adapter_factory = LLMAdapterFactory

            # 3. Hologram
            await self._hologram_call("initialize_base_state")
            self.logger.info("Hologram base state initialized.")

            # 4. Memory Manager (Root Database)
            self.logger.info("Initializing Memory Manager...")
            if MemoryManager:
                memory_manager = MemoryManager()
                # If MemoryManager has async initialize, call it
                if hasattr(memory_manager, "initialize") and inspect.iscoroutinefunction(memory_manager.initialize):
                    await memory_manager.initialize()
                self.components["memory_manager"] = memory_manager
                assistant.memory_manager = memory_manager  # Inject back into core
            else:
                self.logger.error("Memory Manager not available - cannot continue")
                return False

            # --- [NEW ORDER] ---
            # 5. Proactive Communicator (must be created before Persona)
            self.logger.info("Initializing Proactive Communicator...")
            proactive = ProactiveCommunicator(
                core=assistant,
                hologram_call_fn=self._hologram_call,
                concurrency=self.config.get("proactive_concurrency", 1),
                rate_limit_per_minute=self.config.get("proactive_rate_limit_per_minute", 60)
            )
            # Register adapters...
            proactive.register_adapter("sms", LoggingAdapter("sms"))
            proactive.register_adapter("push", LoggingAdapter("push"))
            proactive.register_adapter("tts", LoggingAdapter("tts"))
            proactive.register_adapter("call", LoggingAdapter("call"))
            proactive.register_adapter("logging", LoggingAdapter("proactive_logging"))
            # (Add Webhook adapter logic here if needed)
            
            self.components["proactive"] = proactive
            assistant.proactive = proactive
            # --- [END NEW ORDER] ---
            
            # 6. Persona Core (Heart & Memory)
            if PersonaCore is None:
                self.logger.error("PersonaCore missing — cannot continue safely.")
                return False
            self.logger.info("Initializing Persona Core...")
            persona = PersonaCore(core=assistant)
            
            # --- [DEPENDENCY INJECTION] ---
            persona.memory_manager = memory_manager
            persona.proactive = proactive # Inject the *instance*
            # --- [END INJECTION] ---
            
            assistant.persona = persona
            self.components["persona"] = persona

            # 7. Autonomy Core (Executor)
            autonomy = None
            if create_autonomy_core is not None and self.config.get("enable_autonomy", True):
                self.logger.info("Initializing Autonomy Core...")
                autonomy = await create_autonomy_core(
                    persona=persona, 
                    core=assistant, 
                    max_concurrent=self.config.get("max_concurrent_actions", 3)
                )
                await autonomy.initialize()
                self.components["autonomy"] = autonomy
                persona.autonomy = autonomy # Inject autonomy back into persona
            
            # 8. Cognition Core (The "Brain")
            if CognitionCore is not None:
                self.logger.info("Initializing Cognition Core (Agentic Brain)...")
                cognition = CognitionCore(
                    persona=persona, 
                    core=assistant, 
                    autonomy=autonomy, # Inject Autonomy
                    config={}
                )
                # No async initialize() method in our upgraded CognitionCore
                self.components["cognition"] = cognition
            else:
                self.logger.warning("CognitionCore not available (skipping).")

            # 9. Interaction Core (Hands & Ears)
            if create_interaction_core is None:
                self.logger.error("Interaction core factory missing — cannot start.")
                return False
            self.logger.info("Initializing Interaction Core with Enhanced Security...")
            interaction = await create_interaction_core(
                security_orchestrator=security,
                cognition_core=self.components.get("cognition"),
                autonomy_core=self.components.get("autonomy"),
                proactive_communicator=proactive,
                assistant_core=assistant
            )
            self.components["interaction"] = interaction
            
            # --- [DELETED] ---
            # Removed circular dependency:
            # proactive.integrate_with_interaction(interaction)
            # --- [END DELETED] ---
            
            # 10. Start Daemons
            # Start the ProactiveCommunicator's indefinitely-active loops
            await proactive.start()
            await self._initialize_enhanced_security_baseline()
            await self._verify_system_health()

            self.is_running = True
            self.logger.info("✅ A.A.R.I.A System with Security Initialized Successfully")
            return True
        except Exception as e:
            self.logger.error("❌ System initialization failed: %s", e, exc_info=True)
            try:
                await self._cleanup_components(self.components)
            except Exception: pass
            return False

    async def _initialize_enhanced_security_baseline(self):
        """(Unchanged) Create CLI device and trusted network entries."""
        try:
            security = self.components.get("security")
            device_manager = None
            if security:
                device_manager = getattr(security, "device_manager", None) or self.components.get("device_manager")
            if device_manager:
                try:
                    if hasattr(device_manager, "register_device"):
                        maybe = device_manager.register_device("cli_terminal", "home_pc", {
                            "description": "Primary CLI terminal - Owner Access",
                            "trust_level": "maximum", "user_agent": "AARIA_CLI_Terminal",
                            "owner_device": True
                        })
                        if inspect.iscoroutine(maybe): await maybe
                except Exception:
                    self.logger.debug("Device registration non-fatal error", exc_info=True)
                for net in ("127.0.0.0/8", "192.168.1.0/24", "10.0.0.0/24"):
                    try:
                        if hasattr(device_manager, "add_trusted_network"):
                            maybe = device_manager.add_trusted_network(net)
                            if inspect.iscoroutine(maybe): await maybe
                    except Exception: pass
            self.logger.info("✅ Enhanced security baseline initialized - lockouts cleared")
        except Exception:
            self.logger.exception("Enhanced security baseline failed (non-fatal)")

    async def _verify_system_health(self) -> bool:
        """(Unchanged) Run quick health checks on core components."""
        self.logger.info("🔍 Performing system health checks...")
        try:
            cognition = self.components.get("cognition")
            if cognition and hasattr(cognition, "get_health"):
                maybe = cognition.get_health()
                cog_health = await maybe if inspect.iscoroutine(maybe) else maybe
                overall = cog_health.get("overall_status", cog_health.get("status", "unknown"))
                if overall not in ("healthy", "normal", "ok", "warming_up"):
                    self.logger.warning("⚠️  Cognition health: %s", overall)
                else:
                    self.logger.info("✅ Cognition health: %s", overall)
            
            assistant = self.components.get("assistant")
            if assistant and hasattr(assistant, "health_check"):
                maybe = assistant.health_check()
                h = await maybe if inspect.iscoroutine(maybe) else maybe
                status = h.get("status", "unknown") if isinstance(h, dict) else str(h)
                self.logger.info("✅ Assistant health: %s", status)

            proactive = self.components.get("proactive")
            if proactive and hasattr(proactive, "health_check"):
                try:
                    probe = await proactive.health_check()
                    self.logger.info("✅ Proactive health: queued=%s dead_letter=%s", probe.get("queued"), probe.get("dead_letter"))
                except Exception:
                    self.logger.debug("Proactive health check failed (non-fatal)", exc_info=True)
        except Exception:
            self.logger.exception("Health check hit an error (non-fatal)")
        return True

    async def _cleanup_components(self, components: Dict[str, Any]):
        """(Unchanged) Shutdown in reverse order."""
        ordered_names = ["interaction", "autonomy", "cognition", "persona", "proactive", "security", "assistant", "device_manager"]
        for name in ordered_names:
            comp = components.get(name)
            if not comp: continue
            try:
                self.logger.info("Shutting down %s...", name)
                m = None
                if hasattr(comp, "shutdown"): m = getattr(comp, "shutdown")
                elif hasattr(comp, "stop"): m = getattr(comp, "stop")
                elif hasattr(comp, "close"): m = getattr(comp, "close")
                
                if m:
                    if inspect.iscoroutinefunction(m): await m()
                    else:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, m)
                self.logger.info("%s shutdown complete", name)
            except Exception:
                self.logger.exception("Error shutting down %s", name)

        if LLMAdapterFactory is not None and hasattr(LLMAdapterFactory, "cleanup"):
            try:
                maybe = LLMAdapterFactory.cleanup()
                if asyncio.iscoroutine(maybe): await maybe
                self.logger.info("LLMAdapterFactory cleaned up.")
            except Exception:
                self.logger.exception("LLMAdapterFactory cleanup failed (non-fatal)")

    # --- [UPGRADED] ---
    # This function is now fixed. It calls the one, correct persistence
    # method on PersonaCore: 'save_persistent_memory'.
    # This resolves the `Persona quick persist failed` error from your logs.
    # --- [END UPGRADED] ---
    async def _persist_persona_quick(self) -> bool:
        """Persona persistence helper"""
        persona = self.components.get("persona")
        if not persona:
            self.logger.debug("_persist_persona_quick: missing persona")
            return False
        
        # This is the *one* correct method to call on our upgraded PersonaCore
        candidates = ["save_persistent_memory"]

        for name in candidates:
            if not hasattr(persona, name):
                self.logger.error(f"PersonaCore is missing required method: {name}")
                continue
            
            method = getattr(persona, name)
            try:
                if inspect.iscoroutinefunction(method):
                    await method()
                else:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, method)
                
                self.logger.info("Persona quick persist succeeded via '%s'", name)
                return True # Success
            
            except Exception:
                self.logger.exception(f"Persona persist via {name} failed")
                
        self.logger.error("Persona quick persist failed (all candidates failed)")
        return False
        
    # ---------------------------
    # CLI Interaction
    # ---------------------------
    
    # --- [UPGRADED] ---
    # The `run_interactive_session` no longer contains any CLI parsers
    # for `set_name` or `security`. All input goes to the InteractionCore.
    # --- [END UPGRADED] ---
    async def run_interactive_session(self):
        if not self.is_running:
            self.logger.error("System not initialized. Call initialize() first.")
            return
        self.logger.info("💬 Starting interactive session. Type 'exit' to quit.")
        
        # --- [FIXED] Help text is updated ---
        self.logger.info("✨ All commands are now natural language.")
        self.logger.info("✨ Try 'call me Skye' or 'remind me about Yash's birthday on 12 Dec'")
        self.logger.info("✨ CLI-only commands: 'exit', 'help', 'status', 'stats'")
        # --- [END FIX] ---

        session_start = time.time()
        message_count = 0
        successful_responses = 0
        user_name = "owner_primary" # Default to owner for CLI

        try:
            while True:
                try:
                    user_input = input("\nYou: ").strip()
                except (EOFError, KeyboardInterrupt):
                    self.logger.info("Interactive input terminated by user")
                    break

                if not user_input: continue
                cmd = user_input.strip()
                lc = cmd.lower()

                if lc in ("exit", "quit", "bye"):
                    break
                if lc in ("help", "?"):
                    self._show_help()
                    continue
                if lc == "status":
                    await self._show_system_status()
                    continue
                if lc == "stats":
                    await self._show_session_stats(message_count, successful_responses, session_start)
                    continue
                
                # --- [DELETED] ---
                # All 'if' blocks for 'security' and 'set_name' are GONE.
                # All input now goes directly to the agent.
                # --- [END DELETED] ---

                # Build user context dictionary
                message_count += 1
                user_context = {
                    "user_id": user_name,
                    "device_id": "cli_terminal",
                    "channel": "cli",
                    "source": "private_terminal",
                    "user_name": user_name,
                    "message_number": message_count
                }

                try:
                    interaction = self.components.get("interaction")
                    if not interaction:
                        print("A.A.R.I.A: Interaction component missing.")
                        continue

                    response = await interaction.process_message(
                        message=user_input,
                        user_context=user_context
                    )

                    content = response if isinstance(response, str) else None
                    if content:
                        successful_responses += 1
                        print(f"A.A.R.I.A: {content}")
                    else:
                        print("A.A.R.I.A: [No response generated]")

                except Exception:
                    self.logger.exception("Error processing message")
                    print("A.A.R.I.A: I encountered an error while processing your message. See logs.")

            # session end
            await self._show_session_stats(message_count, successful_responses, session_start)
            self.logger.info("Session ended: %d messages in %.1fs", message_count, time.time() - session_start)

        except Exception:
            self.logger.exception("Interactive session failed unexpectedly")

    # --- [DELETED] ---
    # This method is no longer called by `run_interactive_session`
    # and is now dead code.
    # async def _process_security_command(self, user_input: str) -> str: ...
    # --- [END DELETED] ---

    def _show_help(self):
        # --- [FIXED] Help text is updated ---
        help_text = """
A.A.R.I.A Help:
help, ?              - Show this help message
status               - Show system status
stats                - Show session statistics
exit, quit, bye      - End the session

All other commands are now natural language. Try:
  'call me Master'
  'who am I?'
  'remind me to check the logs in 10 minutes'
"""
        # --- [END FIX] ---
        print(help_text.strip())

    async def _show_system_status(self):
        # (This method remains the same, no changes needed)
        try:
            lines = ["🔧 A.A.R.I.A System Status:"]
            security = self.components.get("security")
            if security and hasattr(security, "get_security_status"):
                maybe = security.get_security_status()
                s = await maybe if inspect.iscoroutine(maybe) else maybe
                lines.append(f"  • Security: {s.get('overall_status','unknown')}")
            assistant = self.components.get("assistant")
            if assistant and hasattr(assistant, "health_check"):
                maybe = assistant.health_check()
                h = await maybe if inspect.iscoroutine(maybe) else maybe
                lines.append(f"  • Assistant: {h.get('status','unknown')}")
            cognition = self.components.get("cognition")
            if cognition and hasattr(cognition, "get_health"):
                maybe = cognition.get_health()
                c = await maybe if inspect.iscoroutine(maybe) else maybe
                overall = c.get("overall_status", c.get("status", "unknown"))
                lines.append(f"  • Cognition: {overall}")
            autonomy = self.components.get("autonomy")
            if autonomy and hasattr(autonomy, "get_system_health"):
                maybe = autonomy.get_system_health()
                a = await maybe if inspect.iscoroutine(maybe) else maybe
                lines.append(f"  • Autonomy: {a.get('status', 'unknown')}")
            proactive = self.components.get("proactive")
            if proactive:
                try:
                    health = await proactive.health_check()
                    qsize = health.get("queued", "?")
                except Exception:
                    qsize = "?"
                lines.append(f"  • Proactive queue size: {qsize}")
            print("\n".join(lines))
        except Exception:
            print("Status check error (see logs).")
            self.logger.exception("Status check error")

    async def _show_session_stats(self, message_count: int, successful_responses: int, session_start: float):
        # (This method remains the same, no changes needed)
        duration = time.time() - session_start
        success_rate = (successful_responses / message_count * 100) if message_count > 0 else 0.0
        avg_resp = (duration / max(1, message_count)) if message_count > 0 else 0.0
        stats = f"""
📊 Session Statistics:
• Duration: {duration:.1f}s
• Messages: {message_count}
• Successful responses: {successful_responses}
• Success rate: {success_rate:.1f}%
• Avg. response time (approx): {avg_resp:.1f}s per message
"""
        print(stats)

    # ---------------------------
    # Startup verification (TOTP)
    # ---------------------------
    async def run_startup_verification(self) -> bool:
        # (This method remains the same, no changes needed)
        security = self.components.get("security")
        if not security:
            self.logger.warning("Security component unavailable - skipping TOTP (non-fatal).")
            return True
        im = getattr(security, "identity_manager", None)
        if not im:
            self.logger.warning("Identity manager missing - skipping TOTP (non-fatal).")
            return True
        try:
            owner_identity = im._get_or_create_owner_identity()
            owner_identity = await owner_identity if inspect.iscoroutine(owner_identity) else owner_identity
        except Exception:
            self.logger.exception("Failed to obtain owner identity (non-fatal); allowing startup.")
            return True
        try:
            secret_m = im._get_totp_secret(owner_identity.identity_id)
            secret = await secret_m if inspect.iscoroutine(secret_m) else secret_m
        except Exception:
            secret = None
        if not secret or pyotp is None:
            try:
                if hasattr(im, "enroll_new_totp_secret"):
                    new_secret = im.enroll_new_totp_secret(owner_identity.identity_id)
                    new_secret = await new_secret if inspect.iscoroutine(new_secret) else new_secret
                    print("\n" + "=" * 60)
                    print("FIRST-TIME SECURITY SETUP: A new TOTP secret has been created.")
                    print("Add it to your authenticator app (it will not be shown again):\n")
                    print(f"  {new_secret}\n")
                    print("=" * 60)
                    await asyncio.sleep(5)
                else:
                    self.logger.info("IdentityManager has no enroll_new_totp_secret; skipping enrollment.")
            except Exception:
                self.logger.exception("TOTP enrollment failed (non-fatal)")

        if not hasattr(im, "challenge_owner_verification"):
            self.logger.info("No challenge method available on IdentityManager — skipping TOTP verification.")
            return True
        attempts = 3
        for i in range(attempts):
            try:
                code = input(f"  Please enter your TOTP code (Attempt {i+1}/{attempts}): ").strip()
            except (EOFError, KeyboardInterrupt):
                self.logger.warning("Verification interrupted by user.")
                return False
            if not code: continue
            try:
                verify_m = im.challenge_owner_verification(code)
                ok = await verify_m if inspect.iscoroutine(verify_m) else verify_m
                if ok:
                    self.logger.info("✅ Verification successful. Root access granted.")
                    if hologram_state is not None and hasattr(hologram_state, "cleanup_verification_state"):
                        try:
                            c = hologram_state.cleanup_verification_state()
                            if inspect.iscoroutine(c): await c
                        except Exception: pass
                    return True
                else:
                    self.logger.warning("❌ Verification failed. Invalid code.")
            except Exception:
                self.logger.exception("Verification check failed (try again)")
        self.logger.error("❌ Too many failed verification attempts. Access denied.")
        return False

    # ---------------------------
    # Shutdown
    # ---------------------------
    async def shutdown(self):
        # (This method is upgraded to call the *fixed* _persist_persona_quick)
        self.logger.info("🛑 Shutting down A.A.R.I.A System...")
        self.is_running = False

        proactive = self.components.get("proactive")
        if proactive:
            try:
                self.logger.info("Stopping proactive communicator (persisting queue)...")
                stop_m = getattr(proactive, "stop", None)
                if inspect.iscoroutinefunction(stop_m): await stop_m()
                elif callable(stop_m):
                    maybe = stop_m()
                    if inspect.iscoroutine(maybe): await maybe
                self.logger.info("Proactive communicator stopped.")
            except Exception:
                self.logger.exception("Error stopping proactive communicator (non-fatal)")

        # Persist persona memory using our *fixed* helper
        try:
            persisted = await self._persist_persona_quick() # <-- This call now works
            if not persisted:
                self.logger.warning("Persona memory persistence returned False during shutdown.")
        except Exception:
            self.logger.exception("Persona persist failed during shutdown")

        order = ["interaction", "autonomy", "cognition", "persona", "security", "assistant", "device_manager"]
        for name in order:
            comp = self.components.get(name)
            if not comp: continue
            try:
                self.logger.info("Shutting down %s...", name)
                m = None
                if hasattr(comp, "shutdown"): m = getattr(comp, "shutdown")
                elif hasattr(comp, "close"): m = getattr(comp, "close")
                elif hasattr(comp, "stop"): m = getattr(comp, "stop")
                if m:
                    if inspect.iscoroutinefunction(m): await m()
                    else:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, m)
                self.logger.info("%s shutdown complete", name)
            except Exception:
                self.logger.exception("Error shutting down %s", name)
        
        try:
            assistant = self.components.get("assistant")
            store = getattr(assistant, "secure_store", None) or getattr(assistant, "store", None)
            if store and hasattr(store, "close"):
                close_m = getattr(store, "close")
                if inspect.iscoroutinefunction(close_m): await close_m()
                else:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, close_m)
                self.logger.info("Secure store closed")
        except Exception:
            self.logger.exception("Error closing secure store (non-fatal)")

        if LLMAdapterFactory is not None and hasattr(LLMAdapterFactory, "cleanup"):
            try:
                maybe = LLMAdapterFactory.cleanup()
                if asyncio.iscoroutine(maybe): await maybe
                self.logger.info("LLMAdapterFactory cleanup executed.")
            except Exception:
                self.logger.exception("LLMAdapterFactory cleanup failed (non-fatal)")

        self.components.clear()
        self.logger.info("✅ A.A.R.I.A System shutdown complete")

# ---------------------------
# Async entrypoint
# ---------------------------
async def main() -> int:
    system = AARIASystem()
    try:
        ok = await system.initialize()
        if not ok:
            system.logger.error("System failed to initialize - aborting startup.")
            return 1

        verified = await system.run_startup_verification()
        if not verified:
            system.logger.error("Startup verification failed - aborting.")
            await system.shutdown()
            return 1

        await system.run_interactive_session()
        await system.shutdown()
        return 0

    except KeyboardInterrupt:
        print("\n\nReceived interrupt signal. Shutting down gracefully...")
        try: await system.shutdown()
        except Exception: pass
        return 0
    except Exception as e:
        print(f"❌ Unexpected fatal error: {e}")
        logger.exception("Fatal error in main")
        try: await system.shutdown()
        except Exception: pass
        return 1

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nA.A.R.I.A system terminated by user.")
        sys.exit(0)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
