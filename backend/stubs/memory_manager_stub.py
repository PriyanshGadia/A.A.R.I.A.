import time
import json
import os
import asyncio
from typing import Dict, Any, List, Optional

INDEX_PATH = os.getenv("AARIA_MEMORY_INDEX", "stubs/memory_index.json")
IDENTITY_PATH = os.getenv("AARIA_IDENTITY_INDEX", "stubs/identity_index.json")

class MemoryManagerStub:
    def __init__(self, index_path: str = INDEX_PATH, identity_path: str = IDENTITY_PATH):
        self.index_path = index_path
        self.identity_path = identity_path
        self._index: List[Dict[str, Any]] = []
        self._identities: Dict[str, Dict[str, Any]] = {}
        self._load_from_disk()

    # -------------------
    # Disk persistence
    # -------------------
    def _load_from_disk(self):
        try:
            if os.path.exists(self.index_path):
                with open(self.index_path, "r", encoding="utf-8") as f:
                    self._index = json.load(f) or []
        except Exception:
            self._index = []
        try:
            if os.path.exists(self.identity_path):
                with open(self.identity_path, "r", encoding="utf-8") as f:
                    self._identities = json.load(f) or {}
        except Exception:
            self._identities = {}

    async def _save_index(self):
        tmp = self.index_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._index, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.index_path)

    async def _save_identities(self):
        tmp = self.identity_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._identities, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.identity_path)

    # -------------------
    # Interface methods
    # -------------------
    async def load_index(self) -> Dict[str, Any]:
        """Return a snapshot object expected by PersonaCore"""
        return {"memory_index": self._index}

    async def save_index(self, snapshot: Dict[str, Any]) -> None:
        self._index = snapshot.get("memory_index", self._index)
        await self._save_index()

    async def append_memory(self, subject_id: str, memory_record: Dict[str, Any]) -> None:
        # ensure record has timestamp
        memory_record.setdefault("timestamp", time.time())
        self._index.append(memory_record)
        # persist asynchronously
        asyncio.create_task(self._save_index())

    async def put(self, key: str, namespace: str, payload: Dict[str, Any]) -> bool:
        # Generic put allowed for compatibility; store as critical memory entry
        payload.setdefault("id", key)
        payload.setdefault("timestamp", time.time())
        self._index.append(payload)
        asyncio.create_task(self._save_index())
        return True

    async def search_memories(self, subject_identity_id: str, query: Optional[str] = None, limit: int = 5) -> List[Dict[str, Any]]:
        q = (query or "").strip().lower()
        out = []
        for m in reversed(self._index):
            if subject_identity_id and m.get("subject_id") and m.get("subject_id") != subject_identity_id:
                continue
            text = (m.get("user","") + " " + m.get("assistant","")).lower()
            if not q or q in text:
                out.append(m)
            if len(out) >= limit:
                break
        return out

    async def query_memories(self, subject_identity_id: str, query: str, max_results: int = 10):
        return await self.search_memories(subject_identity_id, query=query, limit=max_results)

    # Identity containers --------------------------------------------------
    async def get_identity_container(self, identity_id: str) -> Dict[str, Any]:
        return self._identities.get(identity_id, {"id": identity_id, "name_variants": [], "relationships": {}, "created_at": time.time()})

    async def update_identity_container(self, identity_id: str, container: Dict[str, Any]) -> Dict[str, Any]:
        self._identities[identity_id] = container
        asyncio.create_task(self._save_identities())
        return container

    async def persist_important(self, subject_id: str, memory_record: Dict[str, Any]) -> bool:
        # Mark important copy in memory for durability
        memory_record["_persisted_as_important"] = True
        self._index.append(memory_record)
        asyncio.create_task(self._save_index())
        return True

    async def prune_old_memories(self, max_age_days: int, min_importance: int = 4):
        cutoff = time.time() - (max_age_days * 24 * 3600)
        before = len(self._index)
        self._index = [m for m in self._index if m.get("timestamp",0) >= cutoff or m.get("importance",1) >= min_importance]
        if len(self._index) != before:
            await self._save_index()
        return True