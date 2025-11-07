import os
import time
from typing import Any, Dict, Optional

from .storage_sqlite_stub import SimpleStore

class AssistantCoreStub:
    def __init__(self, config: Optional[Dict[str,Any]] = None):
        self.config = config or {"primary_provider": "stub"}
        # Simple persistent store (sqlite file inside stubs/)
        self.store = SimpleStore(db_path=os.getenv("AARIA_STORE_DB","stubs/assistant_store.db"))
        self.memory_manager = None
        # security orchestrator / identity manager can be added later
        self.security_orchestrator = None

    def load_user_profile(self) -> Dict[str, Any]:
        # Return basic profile used by PersonaCore._init_system_prompt
        try:
            profile = self.store.get("user_profile") or {}
            return profile
        except Exception:
            return {"name": "User", "timezone": "UTC"}