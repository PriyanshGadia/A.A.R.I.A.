# main.py - A.A.R.I.A System Entry Point (Async Production)
from __future__ import annotations

import os
import sys
import logging
import traceback
import asyncio
import inspect
import time
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

# Defensive imports for core modules — if a critical module is missing we log and exit.
_missing = []
def _try_import(name: str, alias: Optional[str] = None):
    try:
        mod = __import__(name)
        return mod
    except Exception as e:
        _missing.append((name, e))
        return None

# Prefer explicit component imports if available in your repo (try/except to preserve dev flow).
try:
    from llm_adapter import LLMAdapterFactory, LLMProvider  # optional
except Exception:
    LLMAdapterFactory = None
    LLMProvider = None

try:
    from secure_store import SecureStorageAsync
except Exception:
    SecureStorageAsync = None

try:
    from assistant_core import AssistantCore
except Exception:
    AssistantCore = None

try:
    from persona_core import PersonaCore
except Exception:
    PersonaCore = None

try:
    from cognition_core import CognitionCore
except Exception:
    CognitionCore = None

try:
    from autonomy_core import AutonomyCore, create_autonomy_core
except Exception:
    AutonomyCore = None
    create_autonomy_core = None

try:
    from interaction_core import InteractionCore, InboundMessage, create_interaction_core
except Exception:
    InteractionCore = None
    InboundMessage = None
    create_interaction_core = None

# Security related (optional)
try:
    from access_control import AccessControlSystem, AccessLevel, RequestSource
except Exception:
    AccessControlSystem = None

try:
    from identity_manager import IdentityManager
except Exception:
    IdentityManager = None

try:
    from security_orchestrator import SecurityOrchestrator
except Exception:
    SecurityOrchestrator = None

try:
    from security_config import DynamicSecurityConfig
except Exception:
    DynamicSecurityConfig = None

try:
    from device_types import DeviceManager
except Exception:
    DeviceManager = None

# Proactive communicator (your upgraded component)
try:
    from proactive_comm import ProactiveCommunicator, HTTPWebhookAdapter  # HTTPWebhookAdapter optional
except Exception:
    # fallback if proactive_comm not present
    try:
        from proactive_comm import ProactiveCommunicator  # type: ignore
        HTTPWebhookAdapter = None
    except Exception:
        ProactiveCommunicator = None
        HTTPWebhookAdapter = None

# Hologram state integration (defensive)
try:
    import hologram_state
except Exception:
    hologram_state = None

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
    # if you have a time_utils module in repo prefer it
    from time_utils import parse_to_timestamp, to_local_iso, now_ts  # type: ignore
except Exception:
    def now_ts() -> float:
        return time.time()

    def parse_to_timestamp(text: str, prefer_future: bool = True) -> Optional[float]:
        """
        Small fallback parser supporting:
          - "in N seconds|minutes|hours"
          - "now"
          - absolute 'YYYY-MM-DD HH:MM[:SS]' in local time (best-effort)
        Returns unix timestamp (float) or None if unparseable.
        """
        if not text:
            return None
        txt = str(text).strip().lower()
        if txt in ("now", "right now"):
            return now_ts()
        import re
        m = re.search(r'in\s+(\d+)\s*(s|sec|secs|seconds|m|min|mins|minutes|h|hr|hours)', txt)
        if m:
            val = int(m.group(1))
            unit = m.group(2)
            if unit.startswith('s'):
                return now_ts() + val
            if unit.startswith('m'):
                return now_ts() + val * 60
            if unit.startswith('h'):
                return now_ts() + val * 3600
        # try basic ISO parse
        try:
            import datetime
            # accept 'YYYY-MM-DD HH:MM[:SS]' (no timezone) as local time
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    dt = datetime.datetime.strptime(text, fmt)
                    return dt.timestamp()
                except Exception:
                    pass
        except Exception:
            pass
        return None

    def to_local_iso(ts: float) -> str:
        import datetime
        try:
            return datetime.datetime.fromtimestamp(ts).isoformat()
        except Exception:
            return str(ts)

# -------------------------
# Fallback Adapters (safe defaults)
# -------------------------
class LoggingAdapter:
    """Simple adapter that logs sends and reports success (useful as a default)."""
    def __init__(self, name: str = "logging"):
        self.name = name
        self._log = logging.getLogger(f"AARIA.Adapter.{name}")

    def send(self, envelope):
        """Return (True, receipt) — may be sync or async in some adapters."""
        try:
            self._log.info("[%s] send called for channel=%s subject=%s payload=%s",
                           self.name, envelope.get('channel'), envelope.get('subject_identity'), envelope.get('payload'))
            # simulate latency (do not block when used synchronously)
            # If called synchronously, we return immediately. If used in async delivery code, the worker will await if coroutine returned.
            async def _sim():
                await asyncio.sleep(0.05)
                return True, f"{self.name}_receipt_{int(now_ts()*1000)}"
            return _sim()
        except Exception as e:
            return False, str(e)

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
    # Hologram wrapper (non-blocking)
    # -------------------------
    async def _hologram_call(self, method_name: str, *args, **kwargs):
        if hologram_state is None:
            return None
        try:
            method = getattr(hologram_state, method_name, None)
            if method is None:
                # attempt base init then re-get
                if hasattr(hologram_state, "initialize_base_state"):
                    maybe = hologram_state.initialize_base_state()
                    if inspect.iscoroutine(maybe):
                        await maybe
                    else:
                        pass
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
    async def initialize(self) -> bool:
        self.logger.info("🚀 Initializing A.A.R.I.A System with Security...")
        try:
            if not self._validate_environment():
                return False

            if AssistantCore is None:
                self.logger.error("AssistantCore module missing — cannot start.")
                return False

            # Assistant core (secure store + keys)
            self.logger.info("Initializing Assistant Core...")
            assistant = AssistantCore(password=self.config["master_password"], auto_recover=True)
            # prefer coroutine initialize if present
            init_m = getattr(assistant, "initialize", None)
            if inspect.iscoroutinefunction(init_m):
                await init_m()
            elif callable(init_m):
                # still call to allow sync initialization
                init_m()
            self.components["assistant"] = assistant

            # Security config (optional)
            if DynamicSecurityConfig is not None:
                try:
                    self.logger.info("Initializing Dynamic Security Configuration...")
                    security_config = DynamicSecurityConfig(assistant)
                    if inspect.iscoroutinefunction(getattr(security_config, "initialize", None)):
                        await security_config.initialize()
                    else:
                        security_config.initialize()
                    self.components["security_config"] = security_config
                except Exception:
                    self.logger.exception("Failed to init DynamicSecurityConfig (non-fatal)")

            # Device manager (optional)
            device_manager = None
            if DeviceManager is not None:
                try:
                    self.logger.info("Initializing Device Manager...")
                    device_manager = DeviceManager(assistant)
                    if inspect.iscoroutinefunction(getattr(device_manager, "initialize", None)):
                        await device_manager.initialize()
                    else:
                        device_manager.initialize()
                    self.components["device_manager"] = device_manager
                except Exception:
                    self.logger.exception("DeviceManager init failed (non-fatal)")

            # Security orchestrator (optional)
            security = None
            if SecurityOrchestrator is not None:
                try:
                    self.logger.info("Initializing Security Orchestrator...")
                    security = SecurityOrchestrator(assistant)
                    setattr(security, "security_config", self.components.get("security_config"))
                    setattr(security, "device_manager", device_manager)
                    if inspect.iscoroutinefunction(getattr(security, "initialize", None)):
                        await security.initialize()
                    else:
                        security.initialize()
                    self.components["security"] = security
                    assistant.security_orchestrator = security
                except Exception:
                    self.logger.exception("SecurityOrchestrator init failed (non-fatal)")

            # Hologram base init (best-effort)
            if hologram_state is not None and hasattr(hologram_state, "initialize_base_state"):
                try:
                    maybe = hologram_state.initialize_base_state()
                    if inspect.iscoroutine(maybe):
                        await maybe
                    self.logger.info("Hologram base state initialized via initialize_base_state().")
                except Exception:
                    self.logger.debug("Hologram base init failed (non-fatal)", exc_info=True)

            # Persona core
            if PersonaCore is None:
                self.logger.error("PersonaCore missing — cannot continue safely.")
                return False
            self.logger.info("Initializing Persona Core...")
            persona = PersonaCore(core=assistant)
            init_m = getattr(persona, "initialize", None)
            if inspect.iscoroutinefunction(init_m):
                await init_m()
            elif callable(init_m):
                init_m()
            # attach persona reference on assistant for easier lookup by other modules
            try:
                assistant.persona = persona
            except Exception:
                pass
            self.components["persona"] = persona

        # --- FIX: Inject the (not-yet-created) proactive instance placeholder ---
            # This reference will be populated later, but the attribute must exist.
            persona.proactive = self.components.get("proactive") 
            # --- END FIX ---
            # Cognition core (optional but recommended)

            if CognitionCore is not None:
                try:
                    self.logger.info("Initializing Cognition Core...")
                    cognition = CognitionCore(persona=persona, core=assistant, autonomy=None, config={})
                    init_m = getattr(cognition, "initialize", None)
                    if inspect.iscoroutinefunction(init_m):
                        await init_m()
                    self.components["cognition"] = cognition
                except Exception:
                    self.logger.exception("CognitionCore init failed (non-fatal)")
            else:
                self.logger.warning("CognitionCore not available in repo (skipping).")

            # Autonomy core (optional)
            if create_autonomy_core is not None and self.config.get("enable_autonomy", True):
                try:
                    self.logger.info("Initializing Autonomy Core...")
                    autonomy = await create_autonomy_core(persona=persona, core=assistant, max_concurrent=self.config.get("max_concurrent_actions", 3))
                    init_m = getattr(autonomy, "initialize", None)
                    if inspect.iscoroutinefunction(init_m):
                        await init_m()
                    self.components["autonomy"] = autonomy
                    if self.components.get("cognition"):
                        self.components["cognition"].autonomy = autonomy
                except Exception:
                    self.logger.exception("Autonomy core init failed (non-fatal)")

            # Interaction core
            if create_interaction_core is None:
                self.logger.error("Interaction core factory missing — cannot start interactive session.")
                return False
            self.logger.info("Initializing Interaction Core with Enhanced Security...")
            interaction = await create_interaction_core(
                persona=persona,
                cognition=self.components.get("cognition"),
                autonomy=self.components.get("autonomy"),
                config={
                    "session_ttl": self.config.get("session_timeout", 3600),
                    "rate_limit_per_minute": 60,
                    "autosave_interval": 60,
                    "security_orchestrator": security,
                }
            )
            # wire security into interaction if supported
            if hasattr(interaction, "integrate_security") and callable(getattr(interaction, "integrate_security")) and security:
                maybe = interaction.integrate_security(security)
                if inspect.iscoroutine(maybe):
                    await maybe
            self.components["interaction"] = interaction

            # Enhanced security baseline (register CLI device + trusted networks)
            await self._initialize_enhanced_security_baseline()

            # Proactive communicator wiring and start
            if ProactiveCommunicator is not None:
                try:
                    proactive = ProactiveCommunicator(
                        core=assistant,
                        hologram_call_fn=self._hologram_call,
                        concurrency=self.config.get("proactive_concurrency", 1),
                        rate_limit_per_minute=self.config.get("proactive_rate_limit_per_minute", 60)
                    )
                    # register default adapters via API rather than direct dict writes
                    proactive.register_adapter("sms", LoggingAdapter("sms"))
                    proactive.register_adapter("push", LoggingAdapter("push"))
                    proactive.register_adapter("tts", LoggingAdapter("tts"))
                    proactive.register_adapter("call", LoggingAdapter("call"))
                    proactive.register_adapter("logging", LoggingAdapter("proactive_logging"))

                    # Optionally register webhook adapter if configured
                    webhook_url = self.config.get("proactive_webhook_url", "") or os.getenv("PROACTIVE_WEBHOOK_URL", "")
                    if webhook_url and HTTPWebhookAdapter is not None:
                        try:
                            webhook_cfg = {"url": webhook_url, "timeout": 8, "headers": {"Content-Type": "application/json"}}
                            proactive.register_adapter("webhook", HTTPWebhookAdapter(webhook_cfg))
                            self.logger.info("Registered HTTP webhook adapter for proactive channel 'webhook'.")
                        except Exception:
                            self.logger.exception("Failed to register webhook adapter (non-fatal)")

                    # Integrate proactive with InteractionCore (so it can use interaction features)
                    try:
                        proactive.integrate_with_interaction(interaction)
                    except Exception:
                        self.logger.debug("Proactive integration with InteractionCore failed (non-fatal)", exc_info=True)

                    # Make persona and assistant aware of proactive for convenience
                    try:
                        # --- FIX: Directly set the attribute ---
                        persona.proactive = proactive
                        assistant.proactive = proactive
                        # --- END FIX ---
                    except Exception:
                        pass

                    # enable proactive defaults when allowed (safe fallback)
                    if self.config.get("allow_proactive_by_default", False):
                        try:
                            await proactive.enable_proactive_defaults("owner_primary", channels=["logging", "push", "webhook"])
                            self.logger.info("Proactive defaults enabled by config flag allow_proactive_by_default.")
                        except Exception:
                            self.logger.debug("enable_proactive_defaults failed (non-fatal)")

                    # finally start proactive
                    if inspect.iscoroutinefunction(getattr(proactive, "start", None)):
                        await proactive.start()
                    else:
                        # unlikely — but if start is sync call it
                        maybe = getattr(proactive, "start", None)
                        if callable(maybe):
                            maybe()
                    self.components["proactive"] = proactive
                    self.logger.info("✅ Proactive communicator started and wired.")
                except Exception:
                    self.logger.exception("Proactive communicator failed to start (non-fatal)")
            else:
                self.logger.warning("ProactiveCommunicator class missing — proactive disabled.")

            # Final quick health checks (non-fatal)
            await self._verify_system_health()

            self.is_running = True
            self.logger.info("✅ A.A.R.I.A System with Security Initialized Successfully")
            return True
        except Exception as e:
            self.logger.error("❌ System initialization failed: %s", e, exc_info=True)
            try:
                await self._cleanup_components(self.components)
            except Exception:
                pass
            return False

    async def _initialize_enhanced_security_baseline(self):
        """Create CLI device and trusted network entries so that interactive CLI has owner privileges."""
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
                            "trust_level": "maximum",
                            "user_agent": "AARIA_CLI_Terminal",
                            "owner_device": True
                        })
                        if inspect.iscoroutine(maybe):
                            await maybe
                except Exception:
                    self.logger.debug("Device registration non-fatal error", exc_info=True)
                for net in ("127.0.0.0/8", "192.168.1.0/24", "10.0.0.0/24"):
                    try:
                        if hasattr(device_manager, "add_trusted_network"):
                            maybe = device_manager.add_trusted_network(net)
                            if inspect.iscoroutine(maybe):
                                await maybe
                    except Exception:
                        pass
            self.logger.info("✅ Enhanced security baseline initialized - lockouts cleared")
        except Exception:
            self.logger.exception("Enhanced security baseline failed (non-fatal)")

    async def _verify_system_health(self) -> bool:
        """Run quick health checks on core components. Primarily informational."""
        self.logger.info("🔍 Performing system health checks...")
        try:
            # Cognition health (if present)
            cognition = self.components.get("cognition")
            if cognition and hasattr(cognition, "get_health"):
                maybe = cognition.get_health()
                cog_health = await maybe if inspect.iscoroutine(maybe) else maybe
                overall = cog_health.get("overall_status", cog_health.get("status", "unknown"))
                if overall not in ("healthy", "normal", "ok"):
                    self.logger.warning("⚠️  Cognition health: %s", overall)
                else:
                    self.logger.info("✅ Cognition health: %s", overall)
            # Assistant health
            assistant = self.components.get("assistant")
            if assistant and hasattr(assistant, "health_check"):
                maybe = assistant.health_check()
                h = await maybe if inspect.iscoroutine(maybe) else maybe
                status = h.get("status", "unknown") if isinstance(h, dict) else str(h)
                self.logger.info("✅ Assistant health: %s", status)
            # Proactive health
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
        # shutdown in reverse order of initialization with special handling
        ordered_names = ["interaction", "autonomy", "cognition", "persona", "proactive", "security", "assistant", "device_manager"]
        for name in ordered_names:
            comp = components.get(name)
            if not comp:
                continue
            try:
                self.logger.info("Shutting down %s...", name)
                if hasattr(comp, "shutdown"):
                    m = getattr(comp, "shutdown")
                elif hasattr(comp, "stop"):
                    m = getattr(comp, "stop")
                elif hasattr(comp, "close"):
                    m = getattr(comp, "close")
                else:
                    m = None
                if m:
                    if inspect.iscoroutinefunction(m):
                        await m()
                    else:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, m)
                self.logger.info("%s shutdown complete", name)
            except Exception:
                self.logger.exception("Error shutting down %s", name)

        # finally attempt to cleanup adapters from LLMAdapterFactory if present
        if LLMAdapterFactory is not None and hasattr(LLMAdapterFactory, "cleanup"):
            try:
                maybe = LLMAdapterFactory.cleanup()
                if asyncio.iscoroutine(maybe):
                    await maybe
                self.logger.info("LLMAdapterFactory cleaned up.")
            except Exception:
                self.logger.exception("LLMAdapterFactory cleanup failed (non-fatal)")

    # Persona persistence helper (tries several candidate persist methods)
    async def _persist_persona_quick(self) -> bool:
        persona = self.components.get("persona")
        if not persona:
            self.logger.debug("_persist_persona_quick: missing persona")
            return False
        
        # --- FIX: Removed "close" from this list ---
        candidates = ["flush_memories", "save_persistent_memory", "persist_memories", "save_all", "save", "persist"]
        # --- END FIX ---

        for name in candidates:
            if not hasattr(persona, name):
                continue
            method = getattr(persona, name)
            try:
                if inspect.iscoroutinefunction(method):
                    await method()
                else:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, method)
                self.logger.info("Persona quick persist succeeded via '%s'", name)
                return True
            except Exception:
                self.logger.debug("Persona persist via %s failed (trying next)", name, exc_info=True)
                await asyncio.sleep(0.05)
        self.logger.error("Persona quick persist failed (no candidate succeeded)")
        return False
    # ---------------------------
    # CLI Interaction
    # ---------------------------
    async def run_interactive_session(self):
        if not self.is_running:
            self.logger.error("System not initialized. Call initialize() first.")
            return
        self.logger.info("💬 Starting interactive session. Type 'exit' to quit.")
        self.logger.info("🔐 Security commands: 'security status', 'access users', 'identity list'")
        self.logger.info("👤 To set your identity: 'set_name YourName'")

        session_start = time.time()
        message_count = 0
        successful_responses = 0
        user_name = None

        try:
            while True:
                try:
                    user_input = input("\nYou: ").strip()
                except (EOFError, KeyboardInterrupt):
                    self.logger.info("Interactive input terminated by user")
                    break

                if not user_input:
                    continue
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
                if lc.startswith("security "):
                    resp = await self._process_security_command(user_input)
                    print(f"A.A.R.I.A: {resp}")
                    continue
                if lc.startswith("set_name "):
                    user_name = cmd[9:].strip()
                    persona = self.components.get("persona")
                    if persona and hasattr(persona, "set_preferred_name"):
                        try:
                            maybe = persona.set_preferred_name(user_name)
                            if inspect.iscoroutine(maybe):
                                await maybe
                        except Exception:
                            self.logger.debug("persona.set_preferred_name failed (nonfatal)", exc_info=True)
                    await self._persist_persona_quick()
                    print(f"A.A.R.I.A: ✅ Name set to: {user_name}")
                    continue

                # Build inbound message
                message_count += 1
                inbound = None
                if InboundMessage is not None:
                    inbound = InboundMessage(
                        channel="cli",
                        content=user_input,
                        user_id=user_name or "owner_primary",
                        metadata={
                            "message_number": message_count,
                            "device_id": "cli_terminal",
                            "source": "private_terminal",
                            "user_name": user_name
                        }
                    )
                else:
                    # Minimal fallback inbound shape
                    inbound = {"channel": "cli", "content": user_input, "user_id": user_name or "owner_primary", "metadata": {}}

                try:
                    interaction = self.components.get("interaction")
                    if not interaction:
                        print("A.A.R.I.A: Interaction component missing.")
                        continue

                    maybe = interaction.handle_inbound(inbound)
                    response = await maybe if inspect.iscoroutine(maybe) else maybe

                    # response may be an object with .content or plain string
                    content = getattr(response, "content", None) or (response if isinstance(response, str) else None)
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

    async def _process_security_command(self, user_input: str) -> str:
        try:
            parts = user_input.strip().split()
            if len(parts) < 2:
                return "❌ Security command format: security <subsystem> <command> [params]"
            subsystem = parts[1]
            params = parts[2:]
            security = self.components.get("security")
            if not security:
                return "❌ Security subsystem not available."
            if subsystem == "status":
                maybe = security.get_security_status()
                status = await maybe if inspect.iscoroutine(maybe) else maybe
                ac = status.get("access_control", {})
                ident = status.get("identity_management", {})
                resp = f"🛡️ Security Status: {status.get('overall_status', 'unknown')}\n"
                resp += f"  • Privileged users: {ac.get('privileged_users_count', '?')}, Trusted devices: {ac.get('trusted_devices_count', '?')}\n"
                resp += f"  • Known identities: {ident.get('known_identities', '?')}, Active sessions: {ident.get('active_sessions', '?')}"
                return resp
            if hasattr(security, "process_security_command"):
                cmd = " ".join(parts[1:])
                maybe = security.process_security_command(cmd, {}, None)
                return await maybe if inspect.iscoroutine(maybe) else maybe
            return "❌ Security: unknown subsystem or missing handler."
        except Exception:
            self.logger.exception("Security command error")
            return "❌ Security command error (see logs)."

    def _show_help(self):
        help_text = """
A.A.R.I.A Help:
help, ?              - Show this help message
status               - Show system status
stats                - Show session statistics
exit, quit, bye      - End the session

Security:
security status      - Show security status
set_name <name>      - Set your preferred name
"""
        print(help_text.strip())

    async def _show_system_status(self):
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
                    qsize = len(getattr(proactive, "_persisted_queue", []))
                except Exception:
                    qsize = "?"
                lines.append(f"  • Proactive queue size: {qsize}")
            print("\n".join(lines))
        except Exception:
            print("Status check error (see logs).")
            self.logger.exception("Status check error")

    async def _show_session_stats(self, message_count: int, successful_responses: int, session_start: float):
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

        # fetch or enroll TOTP secret (best-effort)
        try:
            secret_m = im._get_totp_secret(owner_identity.identity_id)
            secret = await secret_m if inspect.iscoroutine(secret_m) else secret_m
        except Exception:
            secret = None

        if not secret or pyotp is None:
            # enroll new secret if possible
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

        # attempt to challenge for up to 3 attempts if challenge method present
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
            if not code:
                continue
            try:
                verify_m = im.challenge_owner_verification(code)
                ok = await verify_m if inspect.iscoroutine(verify_m) else verify_m
                if ok:
                    self.logger.info("✅ Verification successful. Root access granted.")
                    # best-effort cleanup of verification hologram
                    if hologram_state is not None and hasattr(hologram_state, "cleanup_verification_state"):
                        try:
                            c = hologram_state.cleanup_verification_state()
                            if inspect.iscoroutine(c):
                                await c
                        except Exception:
                            pass
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
        self.logger.info("🛑 Shutting down A.A.R.I.A System...")
        self.is_running = False

        # stop proactive communicator early
        proactive = self.components.get("proactive")
        if proactive:
            try:
                self.logger.info("Stopping proactive communicator (persisting queue)...")
                stop_m = getattr(proactive, "stop", None)
                if inspect.iscoroutinefunction(stop_m):
                    await stop_m()
                elif callable(stop_m):
                    maybe = stop_m()
                    if inspect.iscoroutine(maybe):
                        await maybe
                self.logger.info("Proactive communicator stopped.")
            except Exception:
                self.logger.exception("Error stopping proactive communicator (non-fatal)")

        # persist persona memory
        try:
            persisted = await self._persist_persona_quick()
            if not persisted:
                self.logger.warning("Persona memory persistence returned False during shutdown.")
        except Exception:
            self.logger.exception("Persona persist failed during shutdown")

        # orderly shutdown of components (interaction, autonomy, cognition, persona, security, assistant, device_manager)
        order = ["interaction", "autonomy", "cognition", "persona", "security", "assistant", "device_manager"]
        for name in order:
            comp = self.components.get(name)
            if not comp:
                continue
            try:
                self.logger.info("Shutting down %s...", name)
                if hasattr(comp, "shutdown"):
                    m = getattr(comp, "shutdown")
                elif hasattr(comp, "close"):
                    m = getattr(comp, "close")
                elif hasattr(comp, "stop"):
                    m = getattr(comp, "stop")
                else:
                    m = None
                if m:
                    if inspect.iscoroutinefunction(m):
                        await m()
                    else:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, m)
                self.logger.info("%s shutdown complete", name)
            except Exception:
                self.logger.exception("Error shutting down %s", name)

        # close secure store if assistant exposes
        try:
            assistant = self.components.get("assistant")
            store = getattr(assistant, "secure_store", None) or getattr(assistant, "store", None)
            if store and hasattr(store, "close"):
                close_m = getattr(store, "close")
                if inspect.iscoroutinefunction(close_m):
                    await close_m()
                else:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, close_m)
                self.logger.info("Secure store closed")
        except Exception:
            self.logger.exception("Error closing secure store (non-fatal)")

        # Finally attempt to cleanup LLMAdapterFactory if available
        if LLMAdapterFactory is not None and hasattr(LLMAdapterFactory, "cleanup"):
            try:
                maybe = LLMAdapterFactory.cleanup()
                if asyncio.iscoroutine(maybe):
                    await maybe
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

        # Run startup verification (TOTP). If verification fails -> abort (best-effort fallbacks exist).
        verified = await system.run_startup_verification()
        if not verified:
            system.logger.error("Startup verification failed - aborting.")
            await system.shutdown()
            return 1

        # Start interactive CLI session
        await system.run_interactive_session()

        # After session exit, shutdown gracefully
        await system.shutdown()
        return 0

    except KeyboardInterrupt:
        print("\n\nReceived interrupt signal. Shutting down gracefully...")
        try:
            await system.shutdown()
        except Exception:
            pass
        return 0
    except Exception as e:
        print(f"❌ Unexpected fatal error: {e}")
        logger.exception("Fatal error in main")
        try:
            await system.shutdown()
        except Exception:
            pass
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