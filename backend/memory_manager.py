# memory_manager.py
"""
MemoryManager with Hologram Integration for A.A.R.I.A.

[UPGRADED] - This version is now compatible with the new Agentic architecture.
- Fixes the '_store_put' signature bug, resolving all persistence errors.
- Implements the missing 'save_transcript_store' and 'save_semantic_index' methods.
- Fixes the 'append_semantic_entry' typo.
- Implements a 'health_check' method.
- Upgrades all hologram calls to be safe and robust.
"""

from __future__ import annotations

import time
import hashlib
import asyncio
import logging
import uuid
import inspect
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("AARIA.Memory")

# Hologram: optional module
try:
    import hologram_state
except Exception:
    hologram_state = None

# Segment constants
SEG_CONFIDENTIAL = "confidential"  # Owner-only
SEG_ACCESS = "access_data"         # Privileged users (requires permission)
SEG_PUBLIC = "public_data"         # Public shareable

# Identity container prefix key in root db
ROOT_KEY_PREFIX = "rootdb"

# Auditing namespace (keeps history of accesses)
AUDIT_NAMESPACE = "audit_log"

# Default: memory retention / caps
DEFAULT_MAX_PER_IDENTITY = 5000

# Hologram animation timings (seconds)
_HOLO_SHORT = 1.2
_HOLO_MEDIUM = 1.8

# Audit retention
_MAX_AUDIT_ENTRIES = 2000

def _now_ts() -> float:
    return time.time()

def _content_hash(user_text: str, assistant_text: str) -> str:
    h = hashlib.sha256()
    h.update((user_text or "").encode("utf-8"))
    h.update(b"\x1f")
    h.update((assistant_text or "").encode("utf-8"))
    return h.hexdigest()[:24]


class MemoryManager:
    """
    [UPGRADED]
    MemoryManager: central class with hologram integration and compatibility aliases.
    Now fully implements the interface required by the new PersonaCore.
    """

    def __init__(self, assistant_core, identity_manager=None, max_per_identity:int = DEFAULT_MAX_PER_IDENTITY):
        """
        assistant_core: instance of AssistantCore (should expose .store or .secure_store)
        identity_manager: optional IdentityManager instance to link identity containers
        """
        self.assistant = assistant_core
        self.store = getattr(assistant_core, "store", None) or getattr(assistant_core, "secure_store", None)
        if self.store is None:
            logger.warning("MemoryManager: assistant has no 'store' or 'secure_store' attribute. Using in-memory fallback.")
        self.identity_manager = identity_manager
        self.max_per_identity = int(max_per_identity or DEFAULT_MAX_PER_IDENTITY)

        self._index_cache: Dict[str, List[str]] = {}
        self._inmem: Dict[str, Any] = {}
        self._locks = {
            "store": asyncio.Lock(),
            "audit": asyncio.Lock(),
            "identity": asyncio.Lock()
        }

    # -----------------------
    # Key helpers (remains the same)
    # -----------------------
    def _root_key(self, *parts) -> str:
        safe = [str(p) for p in parts if p is not None and p != ""]
        return f"{ROOT_KEY_PREFIX}:" + ":".join(safe)

    def _identity_container_key(self, identity_id: str) -> str:
        return self._root_key("identity", identity_id)

    def _memory_key(self, mem_id: str) -> str:
        return self._root_key("memory", mem_id)

    def _segment_index_key(self, segment: str) -> str:
        return self._root_key("segment_index", segment)

    def _audit_key(self) -> str:
        return self._root_key(AUDIT_NAMESPACE)
    
    def _decide_segment(self, user_input: str, assistant_response: str, metadata: Dict, subject_id: str) -> str:
        """Implements the segmentation logic from the README."""
        if metadata and metadata.get("segment") == SEG_PUBLIC: return SEG_PUBLIC
        if metadata and metadata.get("segment") == SEG_ACCESS: return SEG_ACCESS
        if subject_id == "owner_primary": return SEG_CONFIDENTIAL
        return SEG_ACCESS
    
    def _transcript_index_key(self, identity_id: str) -> str:
        return self._root_key("transcript_index", identity_id)

    def _semantic_index_key(self, identity_id: str) -> str:
        return self._root_key("semantic_index", identity_id)

    # -----------------------
    # Low-level store helpers (robust)
    # -----------------------
    
    # --- [UPGRADED] ---
    # This method is now fixed to use the correct `put(key, type, obj)`
    # signature required by `secure_store.py`. This fixes the
    # `_store_put failed... Expected dictionary object` errors.
    # --- [END UPGRADE] ---
    async def _store_put(self, key: str, obj: Any, type_name: str = "root") -> bool:
        """
        [UPGRADED]
        This method now correctly wraps lists in a dictionary
        to satisfy the encryption layer's "Expected dictionary object" requirement.
        """
        if self.store is None:
            async with self._locks["store"]:
                self._inmem[key] = obj
            return True
        try:
            put = getattr(self.store, "put", None)
            if put is None:
                if hasattr(self.store, "__setitem__"):
                    self.store[key] = obj
                    return True
                raise RuntimeError("No put method on store")

            # --- [CRITICAL FIX] ---
            # The encryption layer (crypto.py) can only encrypt dicts.
            # If the object is a list (like a segment_index), wrap it.
            payload = obj
            if not isinstance(obj, dict):
                # This wrapper makes it a dictionary that can be encrypted.
                payload = {"data": obj}
            # --- [END FIX] ---

            if not type_name or type_name == "root":
                if "segment" in key: type_name = "segment_index"
                elif "identity" in key: type_name = "identity_container"
                elif "memory" in key: type_name = "memory_record"
                elif "audit" in key: type_name = "audit_log"
                elif "transcript" in key: type_name = "transcript_index"
                elif "semantic" in key: type_name = "semantic_index"
                else: type_name = "generic_data"
            
            # Pass the (potentially wrapped) payload to the store
            maybe = put(key, type_name, payload)

            if inspect.iscoroutine(maybe):
                await maybe
            return True
        except Exception as e:
            logger.warning(f"_store_put failed for key {key} (type: {type_name}): {e}", exc_info=True)
            return False
    async def _store_get(self, key: str) -> Optional[Any]:
        if self.store is None:
            async with self._locks["store"]:
                return self._inmem.get(key)
        try:
            get = getattr(self.store, "get", None)
            if get is None:
                if hasattr(self.store, "__getitem__"):
                    try: return self.store[key]
                    except Exception: return None
                return None
            maybe = get(key)
            if inspect.iscoroutine(maybe):
                return await maybe
            return maybe
        except Exception as e:
            logger.debug(f"_store_get failed for {key}: {e}")
            async with self._locks["store"]:
                return self._inmem.get(key)

    async def _store_delete(self, key: str) -> bool:
        if self.store is None:
            async with self._locks["store"]:
                if key in self._inmem:
                    del self._inmem[key]
            return True
        try:
            delete = getattr(self.store, "delete", None)
            if delete:
                maybe = delete(key)
                if inspect.iscoroutine(maybe):
                    await maybe
                return True
            await self._store_put(key, None, "deleted") # Fallback: write None
            return True
        except Exception as e:
            logger.warning(f"_store_delete failed for {key}: {e}")
            return False

    # -----------------------
    # Hologram helpers (UPGRADED to be safe)
    # -----------------------
    async def _safe_holo_spawn(self, node_id: str, node_type: str, label: str, size: int, source_id: str, link_id: str) -> bool:
        if hologram_state is None: return False
        try:
            maybe = hologram_state.spawn_and_link(
                node_id=node_id, node_type=node_type, label=label, size=size,
                source_id=source_id, link_id=link_id
            )
            if inspect.iscoroutine(maybe): await maybe
            return True
        except Exception: return False
    async def _safe_holo_set_active(self, node_name: str):
        if hologram_state is None: return False
        try:
            fn = getattr(hologram_state, "set_node_active", None)
            if fn:
                ret = fn(node_name)
                if inspect.iscoroutine(ret): await ret
            return True
        except Exception: return False
    async def _safe_holo_set_idle(self, node_name: str):
        if hologram_state is None: return False
        try:
            fn = getattr(hologram_state, "set_node_idle", None)
            if fn:
                ret = fn(node_name)
                if inspect.iscoroutine(ret): await ret
            return True
        except Exception: return False
    async def _safe_holo_update_link(self, link_id: str, intensity: float) -> bool:
        if hologram_state is None: return False
        try:
            maybe = hologram_state.update_link_intensity(link_id, intensity)
            if inspect.iscoroutine(maybe): await maybe
            return True
        except Exception: return False
    async def _safe_holo_despawn(self, node_id: str, link_id: str) -> bool:
        if hologram_state is None: return False
        try:
            maybe = hologram_state.despawn_and_unlink(node_id, link_id)
            if inspect.iscoroutine(maybe): await maybe
            return True
        except Exception as e:
            logger.debug(f"Hologram despawn failed: {e}")
            return False

    # --- [UPGRADED] ---
    # These animation methods now use the safe helpers and fix the link_id bug.
    # --- [END UPGRADE] ---
    async def _holo_animate_write(self, node_id: str, link_id: str):
        """Background animation: spawn -> active -> pulse -> idle -> despawn"""
        try:
            ok = await self._safe_holo_spawn(node_id, "memory", "Memory Write", 4, source_id="Memory", link_id=link_id)
            if ok:
                await self._safe_holo_set_active("Memory")
                await self._safe_holo_update_link(link_id, 0.9) # <-- FIXED
                await asyncio.sleep(_HOLO_MEDIUM)
                await self._safe_holo_update_link(link_id, 0.2) # <-- FIXED
                await self._safe_holo_set_idle("Memory")
                await self._safe_holo_despawn(node_id, link_id)
        except Exception as e:
            logger.debug(f"Holo write animation exception: {e}")
            
    async def _holo_animate_read(self, node_id: str, link_id: str):
        """Background animation for reads."""
        try:
            ok = await self._safe_holo_spawn(node_id, "memory", "Memory Read", 4, source_id="Memory", link_id=link_id)
            if ok:
                await self._safe_holo_set_active("Memory")
                await self._safe_holo_update_link(link_id, 0.8) # <-- FIXED
                await asyncio.sleep(_HOLO_SHORT)
                await self._safe_holo_update_link(link_id, 0.1) # <-- FIXED
                await self._safe_holo_set_idle("Memory")
                await self._safe_holo_despawn(node_id, link_id)
        except Exception as e:
            logger.debug(f"Holo read animation exception: {e}")

    # -----------------------
    # Auditing (remains the same)
    # -----------------------
    async def _audit(self, actor: str, action: str, target: str, details: Optional[Dict] = None):
        """Append audit entry (append-only)."""
        entry = {
            "ts": _now_ts(), "actor": actor or "system", "action": action,
            "target": target, "details": details or {}
        }
        key = self._audit_key()
        async with self._locks["audit"]:
            existing = await self._store_get(key) or []
            existing.append(entry)
            if len(existing) > _MAX_AUDIT_ENTRIES:
                existing = existing[-_MAX_AUDIT_ENTRIES:]
            await self._store_put(key, existing, AUDIT_NAMESPACE) # Use correct type

    # -----------------------
    # Identity containers (remains the same)
    # -----------------------
    async def get_identity_container(self, identity_id: str) -> Dict[str, Any]:
        """Return identity container (create if missing). Container holds metadata + memory id list."""
        key = self._identity_container_key(identity_id)
        async with self._locks["identity"]:
            _c = await self._store_get(key) or {}
            container = {
                "identity_id": identity_id,
                "created_at": _c.get("created_at", _now_ts()),
                "name_variants": _c.get("name_variants", []),
                "behavior_patterns": _c.get("behavior_patterns", {}),
                "relationships": _c.get("relationships", {}),
                "permissions": _c.get("permissions", {}),
                "memory_index": _c.get("memory_index", []),
                **{k: v for k, v in _c.items() if k not in (
                    "identity_id","created_at","name_variants","behavior_patterns","relationships","permissions","memory_index"
                )}
            }
            if not _c:
                await self._store_put(key, container, "identity_container") # Use correct type
                idx_key = self._root_key("identity_index")
                idx = await self._store_get(idx_key) or []
                if identity_id not in idx:
                    idx.append(identity_id)
                    await self._store_put(idx_key, idx, "identity_index") # Use correct type
            return container

    async def update_identity_container(self, identity_id: str, patch: Dict[str, Any]) -> bool:
        key = self._identity_container_key(identity_id)
        async with self._locks["identity"]:
            container = await self.get_identity_container(identity_id)
            container.update(patch)
            await self._store_put(key, container, "identity_container") # Use correct type
        await self._audit("system", "identity_update", identity_id, {"patch": list(patch.keys())})
        return True

    # -----------------------
    # Segment index helpers (remains the same)
    # -----------------------
    async def _append_to_segment_index(self, segment: str, mem_id: str):
        key = self._segment_index_key(segment)
        idx = await self._store_get(key) or []
        if mem_id not in idx:
            idx.append(mem_id)
            if len(idx) > 20000:
                idx = idx[-20000:]
            await self._store_put(key, idx, "segment_index") # Use correct type

    async def _remove_from_segment_index(self, segment: str, mem_id: str):
        key = self._segment_index_key(segment)
        idx = await self._store_get(key) or []
        if mem_id in idx:
            idx.remove(mem_id)
            await self._store_put(key, idx, "segment_index") # Use correct type

    # -----------------------
    # Core: store a memory (public API)
    # -----------------------
    async def store_memory(
        self,
        user_input: str,
        assistant_response: str,
        subject_identity_id: str = "owner_primary",
        importance: int = 1,
        desired_segment: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        actor: str = "persona"
    ) -> Tuple[str, str]:
        """
        Store a memory and return (memory_id, segment).
        (This method is now internally hardened against `_store_put` failures)
        """
        metadata = metadata or {}
        node_id = f"mem_write_{uuid.uuid4().hex[:6]}"
        link_id = f"link_mem_{node_id}"
        try:
            asyncio.create_task(self._holo_animate_write(node_id, link_id))
        except Exception: pass

        # ... (schedule detection logic remains the same) ...
        scheduled_iso = None
        schedule_adjusted = False
        try:
            if metadata.get("schedule_time_iso"):
                scheduled_iso = metadata.get("schedule_time_iso")
            else:
                text_to_scan = metadata.get("requested_schedule_text") or user_input or assistant_response or ""
                try:
                    from dateutil import parser as _dparser
                    try:
                        dt = _dparser.parse(text_to_scan, fuzzy=True, default=None)
                        if dt: scheduled_iso = dt.astimezone().isoformat()
                    except Exception:
                        for token in text_to_scan.split(","):
                            try:
                                dt = _dparser.parse(token, fuzzy=True, default=None)
                                if dt: scheduled_iso = dt.astimezone().isoformat(); break
                            except Exception: continue
                except Exception:
                    import re
                    m = re.search(r'\b((tomorrow|today)\b.*?\b(at )?\d{1,2}(:\d{2})?\s?(am|pm)?)', (text_to_scan or "").lower(), flags=re.IGNORECASE)
                    if m:
                        try:
                            now = time.localtime()
                            tm = m.group(0)
                            hr_match = re.search(r'(\d{1,2})(?::(\d{2}))?', tm)
                            if hr_match:
                                hr = int(hr_match.group(1)); mn = int(hr_match.group(2) or 0)
                                is_pm = bool(re.search(r'\bpm\b', tm))
                                if is_pm and hr < 12: hr += 12
                                day = 1 if "tomorrow" in tm else 0
                                ts = time.time() + day * 24 * 3600
                                date_struct = time.localtime(ts)
                                scheduled_ts = time.mktime((date_struct.tm_year, date_struct.tm_mon, date_struct.tm_mday, hr, mn, 0, 0, 0, -1))
                                scheduled_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(scheduled_ts))
                        except Exception: scheduled_iso = None
            if scheduled_iso:
                try:
                    try:
                        from dateutil import parser as _dparser
                        dt = _dparser.parse(scheduled_iso); scheduled_ts = dt.timestamp()
                    except Exception:
                        try: scheduled_ts = time.mktime(time.strptime(scheduled_iso.split('.')[0], "%Y-%m-%dT%H:%M:%S"))
                        except Exception: scheduled_ts = None
                    now_ts = _now_ts()
                    if scheduled_ts:
                        if scheduled_ts + 60 < now_ts:
                            attempts = 0
                            while scheduled_ts + 60 < now_ts and attempts < 365:
                                scheduled_ts += 24 * 3600
                                attempts += 1
                            try:
                                scheduled_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(scheduled_ts))
                                schedule_adjusted = True
                            except Exception: schedule_adjusted = False
                    if scheduled_iso:
                        metadata["scheduled_for"] = scheduled_iso
                        metadata["schedule_adjusted"] = schedule_adjusted
                except Exception: pass
        except Exception:
            logger.debug("Schedule parsing/adjustment failed (non-fatal).", exc_info=True)

        content_hash = _content_hash(user_input or "", assistant_response or "")
        container = await self.get_identity_container(subject_identity_id)

        # duplicate detection
        for mid in container.get("memory_index", []):
            mem = await self._store_get(self._memory_key(mid))
            if mem and mem.get("content_hash") == content_hash:
                mem["last_touched"] = _now_ts()
                mem["access_count"] = mem.get("access_count", 0) + 1
                if metadata.get("scheduled_for"):
                    mem.setdefault("metadata", {})
                    mem["metadata"].update(metadata)
                await self._store_put(self._memory_key(mid), mem, "memory_record") # Use correct type
                await self._audit(actor, "memory_duplicate_detected", mid, {"subject": subject_identity_id})
                return mid, mem.get("segment", SEG_CONFIDENTIAL)

        segment = desired_segment or self._decide_segment(user_input or "", assistant_response or "", metadata, subject_identity_id)
        mem_id = f"mem_{int(_now_ts()*1000)}_{uuid.uuid4().hex[:6]}"

        record = {
            "id": mem_id, "subject_id": subject_identity_id, "user": user_input or "",
            "assistant": assistant_response or "", "timestamp": _now_ts(),
            "importance": int(max(1, min(5, int(importance or 1)))),
            "content_hash": content_hash, "segment": segment, "metadata": metadata or {},
            "access_count": 0, "owner_visible": True
        }
        if record["metadata"].get("scheduled_for"):
            try:
                sf = record["metadata"]["scheduled_for"]
                try:
                    from dateutil import parser as _dparser
                    dt = _dparser.parse(sf)
                    record["metadata"]["scheduled_for"] = dt.astimezone().isoformat()
                except Exception: pass
            except Exception: pass

        await self._store_put(self._memory_key(mem_id), record, "memory_record") # Use correct type
        container["memory_index"].append(mem_id)
        if len(container["memory_index"]) > self.max_per_identity:
            evicted = container["memory_index"].pop(0)
            try: await self._store_delete(self._memory_key(evicted))
            except Exception: pass
        await self._store_put(self._identity_container_key(subject_identity_id), container, "identity_container") # Use correct type
        await self._append_to_segment_index(segment, mem_id)
        await self._audit(actor, "memory_store", mem_id, {"segment": segment, "subject": subject_identity_id, "importance": record["importance"], "holo_node": node_id, "schedule_adjusted": schedule_adjusted})
        return mem_id, segment

    # -----------------------
    # Convenience aliases for compatibility
    # -----------------------
    async def append_memory(self, identity_id: str, memory_record: Dict[str, Any]) -> Tuple[str, str]:
        mem_id = memory_record.get("id") or f"mem_{int(_now_ts()*1000)}_{uuid.uuid4().hex[:6]}"
        memory_record["id"] = mem_id
        subj = memory_record.get("subject_id", "owner_primary")
        seg = memory_record.get("segment", self._decide_segment(memory_record.get("user",""), memory_record.get("assistant",""), memory_record.get("metadata"), subj))
        memory_record["segment"] = seg
        memory_record.setdefault("timestamp", _now_ts())
        memory_record.setdefault("access_count", 0)
        await self._store_put(self._memory_key(mem_id), memory_record, "memory_record") # Use correct type
        container = await self.get_identity_container(subj)
        container["memory_index"].append(mem_id)
        if len(container["memory_index"]) > self.max_per_identity:
            evicted = container["memory_index"].pop(0)
            try: await self._store_delete(self._memory_key(evicted))
            except Exception: pass
        await self._store_put(self._identity_container_key(subj), container, "identity_container") # Use correct type
        await self._append_to_segment_index(seg, mem_id)
        await self._audit("system", "append_memory", mem_id, {"subject": subj})
        return mem_id, seg

    async def write_memory(self, identity_id: str, memory_record: Dict[str, Any]) -> Tuple[str, str]:
        return await self.append_memory(identity_id, memory_record)

    # -----------------------
    # Retrieval API (permission-aware)
    # -----------------------
    async def retrieve_memories(
        self,
        query_text: str,
        subject_identity_id: str = "owner_primary",
        requester_id: Optional[str] = None,
        allowed_segments: Optional[List[str]] = None,
        limit: int = 5,
        min_relevance: float = 0.05
    ) -> List[Dict[str, Any]]:
        
        node_id = f"mem_read_{uuid.uuid4().hex[:6]}"
        link_id = f"link_mem_{node_id}"
        try:
            asyncio.create_task(self._holo_animate_read(node_id, link_id))
        except Exception: pass

        if requester_id in ("owner_primary", "owner"):
            permitted = {SEG_CONFIDENTIAL, SEG_ACCESS, SEG_PUBLIC}
        else:
            permitted = {SEG_PUBLIC}
            container = await self.get_identity_container(subject_identity_id)
            perms = container.get("permissions", {}) or {}
            if requester_id and requester_id in perms:
                permitted = set(perms.get(requester_id) or [])
                permitted.add(SEG_PUBLIC)
        if allowed_segments:
            permitted = permitted.intersection(set(allowed_segments))

        container = await self.get_identity_container(subject_identity_id)
        pool = list(reversed(container.get("memory_index", [])))
        results = []
        for mid in pool:
            mem = await self._store_get(self._memory_key(mid))
            if not mem: continue
            if mem.get("segment") not in permitted: continue
            score = self._relevance_score_simple(query_text or "", mem)
            if score < min_relevance: continue
            mem = dict(mem)
            mem["access_count"] = mem.get("access_count", 0) + 1
            mem["last_touched"] = _now_ts()
            try: await self._store_put(self._memory_key(mid), mem, "memory_record") # Use correct type
            except Exception: pass
            if requester_id not in ("owner_primary", "owner") and mem.get("segment") == SEG_CONFIDENTIAL:
                continue
            results.append((score, mem))
        results.sort(key=lambda r: (r[0], r[1].get("importance",1), r[1].get("timestamp",0)), reverse=True)
        selected = [r[1] for r in results[:limit]]
        await self._audit(requester_id or "anonymous", "memory_read", subject_identity_id, {"query": query_text, "returned": len(selected), "holo_node": node_id})
        return selected

    # Compatibility method names
    async def search_memories(self, subject_identity_id: str, query: str = "", limit: int = 5) -> List[Dict[str, Any]]:
        return await self.retrieve_memories(query_text=query, subject_identity_id=subject_identity_id, limit=limit)
    async def query_memories(self, subject_identity_id: str, query: str = "", max_results: int = 5) -> List[Dict[str, Any]]:
        return await self.retrieve_memories(query_text=query, subject_identity_id=subject_identity_id, limit=max_results)
    async def list_memories_for_subject(self, subject_identity_id: str) -> List[str]:
        container = await self.get_identity_container(subject_identity_id)
        return list(container.get("memory_index", []))

    # -----------------------
    # Simple relevance scoring
    # -----------------------
    def _relevance_score_simple(self, query: str, mem: Dict[str,Any]) -> float:
        qtokens = set(token for token in (query or "").lower().split() if token)
        mem_text = (mem.get("user","") + " " + mem.get("assistant","")).lower()
        mtokens = set(token for token in mem_text.split() if token)
        if not qtokens or not mtokens: base = 0.0
        else: base = len(qtokens.intersection(mtokens)) / max(1, len(qtokens.union(mtokens)))
        imp = (mem.get("importance",1)-1) * 0.15
        age_days = (time.time() - mem.get("timestamp", time.time())) / 86400.0
        recency = max(0.0, 1.0 - (age_days / 14.0)) * 0.15
        return min(1.0, base + imp + recency)

    # -----------------------
    # Permissions management (owner-only API)
    # -----------------------
    async def grant_privilege(self, subject_identity_id: str, privileged_user_id: str, segments: List[str]):
        container = await self.get_identity_container(subject_identity_id)
        perms = container.get("permissions", {}) or {}
        perms[privileged_user_id] = list(set(segments))
        container["permissions"] = perms
        await self._store_put(self._identity_container_key(subject_identity_id), container, "identity_container") # Use correct type
        await self._audit("owner", "grant_privilege", subject_identity_id, {"grantee": privileged_user_id, "segments": segments})
        return True

    async def revoke_privilege(self, subject_identity_id: str, privileged_user_id: str):
        container = await self.get_identity_container(subject_identity_id)
        perms = container.get("permissions", {}) or {}
        if privileged_user_id in perms:
            perms.pop(privileged_user_id)
            container["permissions"] = perms
            await self._store_put(self._identity_container_key(subject_identity_id), container, "identity_container") # Use correct type
            await self._audit("owner", "revoke_privilege", subject_identity_id, {"revoked": privileged_user_id})
        return True

    # -----------------------
    # Utilities
    # -----------------------
    async def list_identities(self) -> List[str]:
        key = self._root_key("identity_index")
        idx = await self._store_get(key)
        if isinstance(idx, list):
            return idx
        return []

    async def export_memory_snapshot(self, subject_identity_id: str) -> Dict[str,Any]:
        container = await self.get_identity_container(subject_identity_id)
        mems = []
        for mid in container.get("memory_index", []):
            m = await self._store_get(self._memory_key(mid))
            if m:
                mems.append(m)
        return {"identity": container, "memories": mems}

    # -----------------------
    # Persistence helpers & root index
    # -----------------------
    async def get_root_index(self) -> Dict[str, Any]:
        """Return a small root index suitable for quick syncing/inspection."""
        idx_key = self._root_key("root_index")
        root = await self._store_get(idx_key) or {}
        if not root:
            identities = await self.list_identities()
            root = {"identities": identities, "count": len(identities), "timestamp": _now_ts()}
            await self._store_put(idx_key, root, "root_index") # Use correct type
        return root

    async def load_index(self) -> Dict[str, Any]:
        """Compatibility: load_index alias for get_root_index"""
        return await self.get_root_index()

    async def save_index(self, index_data: Dict[str, Any]) -> bool:
        """Persist a provided index object (best-effort)."""
        idx_key = self._root_key("root_index")
        try:
            await self._store_put(idx_key, index_data, "root_index") # Use correct type
            return True
        except Exception as e:
            logger.warning(f"save_index failed: {e}")
            return False

    # -----------------------
    # Put/get by memory key (compat wrappers)
    # -----------------------
    async def put(self, key: str, category: Optional[str] = None, obj: Optional[Any] = None):
        if obj is None and category is not None:
            if isinstance(category, dict):
                # Assumes (key, obj) was passed
                return await self._store_put(self._memory_key(key), category, "memory_record")
            return False
        if obj is not None:
            if isinstance(obj, dict) and obj.get("id"):
                return await self._store_put(self._memory_key(obj["id"]), obj, category or "memory_record")
            return await self._store_put(self._memory_key(key), obj, category or "memory_record")
        return False

    async def get(self, key: str, category: Optional[str] = None):
        if key and key.startswith("mem_"):
            return await self._store_get(self._memory_key(key))
        return await self._store_get(self._root_key(key))

    # -----------------------
    # Important persistence
    # -----------------------
    async def persist_important(self, subject_identity_id: str, memory_record: Dict[str, Any]) -> bool:
        try:
            if memory_record.get("id") is None:
                memory_record["id"] = f"mem_{int(_now_ts()*1000)}_{uuid.uuid4().hex[:6]}"
            await self._store_put(self._memory_key(memory_record["id"]), memory_record, "critical_memory") # Use correct type
            container = await self.get_identity_container(subject_identity_id)
            if memory_record["id"] not in container["memory_index"]:
                container["memory_index"].append(memory_record["id"])
                await self._store_put(self._identity_container_key(subject_identity_id), container, "identity_container") # Use correct type
            await self._audit("system", "persist_important", memory_record["id"], {"subject": subject_identity_id})
            return True
        except Exception as e:
            logger.warning(f"persist_important failed: {e}")
            return False

    # -----------------------
    # Pruning / maintenance
    # -----------------------
    async def prune_old_memories(self, max_age_days: int = 30, min_importance: int = 4) -> Dict[str, int]:
        """
        Prune memories older than max_age_days unless importance >= min_importance.
        Returns counts of removed items per identity.
        """
        cutoff = _now_ts() - (max_age_days * 24 * 3600)
        removed_per_identity = {}
        identities = await self.list_identities()
        for ident in identities:
            container = await self.get_identity_container(ident)
            kept = []
            removed = 0
            for mid in container.get("memory_index", []):
                mem = await self._store_get(self._memory_key(mid))
                if not mem:
                    removed += 1
                    continue
                timestamp = mem.get("timestamp", 0)
                importance = mem.get("importance", 1)
                if timestamp < cutoff and importance < min_importance:
                    try:
                        await self._store_delete(self._memory_key(mid))
                    except Exception: pass
                    removed += 1
                else:
                    kept.append(mid)
            if removed > 0:
                container["memory_index"] = kept
                await self._store_put(self._identity_container_key(ident), container, "identity_container") # Use correct type
                removed_per_identity[ident] = removed
                await self._audit("system", "prune_old_memories", ident, {"removed": removed})
        return removed_per_identity

    # -----------------------
    # Simple search helper: naive scan across segments (not vector search)
    # -----------------------
    async def search_memories_text(self, query_text: str, segment_filter: Optional[List[str]] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Scan segment indices and return matching memories (naive textual match).
        Useful for admin/inspection.
        """
        found = []
        segments = [SEG_CONFIDENTIAL, SEG_ACCESS, SEG_PUBLIC] if segment_filter is None else segment_filter
        for seg in segments:
            idx = await self._store_get(self._segment_index_key(seg)) or []
            for mid in idx:
                mem = await self._store_get(self._memory_key(mid))
                if not mem: continue
                text = (mem.get("user","") + " " + mem.get("assistant","")).lower()
                if not query_text or all(tok in text for tok in (query_text or "").lower().split()):
                    found.append(mem)
                    if len(found) >= limit:
                        return found
        return found
    
    async def append_transcript(self, identity_id: str, transcript_entry: Dict[str, Any]) -> str:
        """
        Appends a raw transcript entry to the identity's transcript store.
        """
        key = self._transcript_index_key(identity_id)
        entry_id = transcript_entry.get("id", f"tx_{uuid.uuid4().hex[:8]}")
        transcript_entry["id"] = entry_id
        
        async with self._locks["store"]:
            transcripts = await self._store_get(key) or []
            transcripts.append(transcript_entry)
            if len(transcripts) > self.max_per_identity:
                transcripts = transcripts[-self.max_per_identity:]
            await self._store_put(key, transcripts, "transcript_index") # Use correct type
            
        await self._audit("persona", "transcript_append", entry_id, {"subject": identity_id})
        return entry_id

    # --- [UPGRADED] ---
    # Renamed from 'append_semantic_entry' to 'append_semantic'
    # to match the interface PersonaCore is calling.
    # --- [END UPGRADE] ---
    async def append_semantic(self, identity_id: str, semantic_entry: Dict[str, Any]) -> str:
        """
        Appends a compact semantic entry to the identity's semantic index.
        """
        key = self._semantic_index_key(identity_id)
        entry_id = semantic_entry.get("id", f"sem_{uuid.uuid4().hex[:8]}")
        semantic_entry["id"] = entry_id
        
        async with self._locks["store"]:
            index = await self._store_get(key) or []
            index.append(semantic_entry)
            if len(index) > self.max_per_identity:
                index = index[-self.max_per_identity:]
            await self._store_put(key, index, "semantic_index") # Use correct type
            
        await self._audit("persona", "semantic_append", entry_id, {"subject": identity_id})
        return entry_id

    # --- [NEW] ---
    # These methods are required by the upgraded PersonaCore and main.py
    # for persistence and health checks.
    # --- [END NEW] ---
    
    async def save_transcript_store(self, identity_id: str, transcripts: List[Dict[str, Any]]) -> bool:
        """
        [NEW] Overwrites the entire transcript store for a given identity.
        Used by maintenance loops.
        """
        key = self._transcript_index_key(identity_id)
        try:
            await self._store_put(key, transcripts, "transcript_index")
            return True
        except Exception as e:
            logger.warning(f"save_transcript_store failed for {identity_id}: {e}")
            return False

    async def save_semantic_index(self, identity_id: str, semantic_index: List[Dict[str, Any]]) -> bool:
        """
        [NEW] Overwrites the entire semantic index for a given identity.
        Used by maintenance loops.
        """
        key = self._semantic_index_key(identity_id)
        try:
            await self._store_put(key, semantic_index, "semantic_index")
            return True
        except Exception as e:
            logger.warning(f"save_semantic_index failed for {identity_id}: {e}")
            return False

    async def health_check(self) -> Dict[str, Any]:
        """
        [NEW] Performs a simple health check on the memory manager.
        Tries to read the root index.
        """
        try:
            index = await self.get_root_index()
            if index is not None and isinstance(index.get("identities"), list):
                return {"status": "healthy", "identities": index.get("count", 0)}
            else:
                return {"status": "degraded", "error": "Root index is malformed."}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    # -----------------------
    # Back-compat convenience wrappers
    # -----------------------
    get_root_index = get_root_index
    load_index = load_index
    save_index = save_index
    append_memory = append_memory
    write_memory = write_memory
    search_memories = search_memories
    query_memories = query_memories
    list_memories_for_subject = list_memories_for_subject
    persist_important = persist_important
    prune_old_memories = prune_old_memories
    put = put
    get = get
    delete = _store_delete
    
    # --- [NEW] ---
    # Add aliases for the new methods
    append_semantic_entry = append_semantic # Alias for old callers
    # --- [END NEW] ---

# End of file