# memory_manager.py
"""
MemoryManager with Hologram Integration for A.A.R.I.A.

- Root Database abstraction + segmented memory manager
- Hologram integration: spawn/despawn nodes, set active/idle, update link intensity
- Best-effort, non-blocking hologram operations (safe if hologram_state missing)
- Permission-aware retrieval & owner-controlled segments
- Multiple API name aliases for compatibility with existing code
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
SEG_CONFIDENTIAL = "confidential"   # Owner-only
SEG_ACCESS = "access_data"          # Privileged users (requires permission)
SEG_PUBLIC = "public_data"          # Public shareable

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
    MemoryManager: central class with hologram integration and compatibility aliases.
    Initialize with assistant_core instance (to access assistant.store or assistant.secure_store).
    """

    def __init__(self, assistant_core, identity_manager=None, max_per_identity:int = DEFAULT_MAX_PER_IDENTITY):
        """
        assistant_core: instance of AssistantCore (should expose .store or .secure_store)
        identity_manager: optional IdentityManager instance to link identity containers
        """
        self.assistant = assistant_core
        # prefer secure_store if available
        self.store = getattr(assistant_core, "store", None) or getattr(assistant_core, "secure_store", None)
        if self.store is None:
            logger.warning("MemoryManager: assistant has no 'store' or 'secure_store' attribute. Using in-memory fallback.")
        self.identity_manager = identity_manager
        self.max_per_identity = int(max_per_identity or DEFAULT_MAX_PER_IDENTITY)

        # in-memory caches and storage fallback
        self._index_cache: Dict[str, List[str]] = {}
        self._inmem: Dict[str, Any] = {}
        self._locks = {
            "store": asyncio.Lock(),
            "audit": asyncio.Lock(),
            "identity": asyncio.Lock()
        }

    # -----------------------
    # Key helpers
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

    # -----------------------
    # Low-level store helpers (robust)
    # -----------------------
    async def _store_put(self, key: str, obj: Any) -> bool:
        """Put object into underlying store; tolerant to sync/async store implementations."""
        if self.store is None:
            # in-memory fallback (protected by lock)
            async with self._locks["store"]:
                self._inmem[key] = obj
            return True
        try:
            put = getattr(self.store, "put", None)
            if put is None:
                # try setitem
                if hasattr(self.store, "__setitem__"):
                    try:
                        self.store[key] = obj
                        return True
                    except Exception:
                        pass
                raise RuntimeError("No put method on store")
            # Many store APIs accept (key, category, object) or (key, object)
            try:
                maybe = put(key, "root", obj)
            except TypeError:
                try:
                    maybe = put(key, obj)
                except TypeError:
                    maybe = put(obj)
            if inspect.iscoroutine(maybe):
                await maybe
            return True
        except Exception as e:
            logger.warning(f"_store_put failed for key {key}: {e}")
            async with self._locks["store"]:
                self._inmem[key] = obj
            return False

    async def _store_get(self, key: str) -> Optional[Any]:
        if self.store is None:
            async with self._locks["store"]:
                return self._inmem.get(key)
        try:
            get = getattr(self.store, "get", None)
            if get is None:
                # try item access
                if hasattr(self.store, "__getitem__"):
                    try:
                        return self.store[key]
                    except Exception:
                        return None
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
            # fallback: write None
            await self._store_put(key, None)
            return True
        except Exception as e:
            logger.warning(f"_store_delete failed for {key}: {e}")
            return False

    # -----------------------
    # Hologram helpers (best-effort)
    # -----------------------
    async def _holo_spawn(self, node_id: str, node_type: str, label: str, size: int, source_id: str, link_id: str) -> bool:
        if hologram_state is None:
            return False
        try:
            maybe = hologram_state.spawn_and_link(node_id=node_id, node_type=node_type, label=label, size=size, source_id=source_id, link_id=link_id)
            if inspect.iscoroutine(maybe):
                await maybe
            return True
        except Exception as e:
            logger.debug(f"Hologram spawn failed: {e}")
            return False

    async def _holo_set_active(self, node_name: str) -> bool:
        if hologram_state is None:
            return False
        try:
            maybe = hologram_state.set_node_active(node_name)
            if inspect.iscoroutine(maybe):
                await maybe
            return True
        except Exception:
            return False

    async def _holo_set_idle(self, node_name: str) -> bool:
        if hologram_state is None:
            return False
        try:
            maybe = hologram_state.set_node_idle(node_name)
            if inspect.iscoroutine(maybe):
                await maybe
            return True
        except Exception:
            return False

    async def _holo_update_link(self, link_id: str, intensity: float) -> bool:
        if hologram_state is None:
            return False
        try:
            maybe = hologram_state.update_link_intensity(link_id, intensity)
            if inspect.iscoroutine(maybe):
                await maybe
            return True
        except Exception:
            return False

    async def _holo_despawn(self, node_id: str, link_id: str) -> bool:
        if hologram_state is None:
            return False
        try:
            maybe = hologram_state.despawn_and_unlink(node_id, link_id)
            if inspect.iscoroutine(maybe):
                await maybe
            return True
        except Exception as e:
            logger.debug(f"Hologram despawn failed: {e}")
            return False

    async def _holo_animate_write(self, node_id: str, link_id: str):
        """Background animation: spawn -> active -> pulse -> idle -> despawn"""
        try:
            ok = await self._holo_spawn(node_id, "memory", "Memory Write", 4, source_id="Memory", link_id=link_id)
            if ok:
                await self._holo_set_active("Memory")
                await self._holo_update_link("link_cog_mem", 0.9)
                await asyncio.sleep(_HOLO_MEDIUM)
                await self._holo_update_link("link_cog_mem", 0.2)
                await self._holo_set_idle("Memory")
                await self._holo_despawn(node_id, link_id)
        except Exception as e:
            logger.debug(f"Holo write animation exception: {e}")

    async def _holo_animate_read(self, node_id: str, link_id: str):
        """Background animation for reads."""
        try:
            ok = await self._holo_spawn(node_id, "memory", "Memory Read", 4, source_id="Memory", link_id=link_id)
            if ok:
                await self._holo_set_active("Memory")
                await self._holo_update_link("link_cog_mem", 0.8)
                await asyncio.sleep(_HOLO_SHORT)
                await self._holo_update_link("link_cog_mem", 0.1)
                await self._holo_set_idle("Memory")
                await self._holo_despawn(node_id, link_id)
        except Exception as e:
            logger.debug(f"Holo read animation exception: {e}")

    # -----------------------
    # Auditing
    # -----------------------
    async def _audit(self, actor: str, action: str, target: str, details: Optional[Dict] = None):
        """Append audit entry (append-only)."""
        entry = {
            "ts": _now_ts(),
            "actor": actor or "system",
            "action": action,
            "target": target,
            "details": details or {}
        }
        key = self._audit_key()
        async with self._locks["audit"]:
            existing = await self._store_get(key) or []
            existing.append(entry)
            if len(existing) > _MAX_AUDIT_ENTRIES:
                existing = existing[-_MAX_AUDIT_ENTRIES:]
            await self._store_put(key, existing)

    # -----------------------
    # Identity containers
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
                "permissions": _c.get("permissions", {}),  # {privileged_user_id: [segments]}
                "memory_index": _c.get("memory_index", []),  # list of memory ids
                **{k: v for k, v in _c.items() if k not in (
                    "identity_id","created_at","name_variants","behavior_patterns","relationships","permissions","memory_index"
                )}
            }
            if not _c:
                # persist new container
                await self._store_put(key, container)
                idx_key = self._root_key("identity_index")
                idx = await self._store_get(idx_key) or []
                if identity_id not in idx:
                    idx.append(identity_id)
                    await self._store_put(idx_key, idx)
            return container

    async def update_identity_container(self, identity_id: str, patch: Dict[str, Any]) -> bool:
        key = self._identity_container_key(identity_id)
        async with self._locks["identity"]:
            container = await self.get_identity_container(identity_id)
            container.update(patch)
            await self._store_put(key, container)
        await self._audit("system", "identity_update", identity_id, {"patch": list(patch.keys())})
        return True

    # -----------------------
    # Segment index helpers
    # -----------------------
    async def _append_to_segment_index(self, segment: str, mem_id: str):
        key = self._segment_index_key(segment)
        idx = await self._store_get(key) or []
        if mem_id not in idx:
            idx.append(mem_id)
            if len(idx) > 20000:
                idx = idx[-20000:]
            await self._store_put(key, idx)

    async def _remove_from_segment_index(self, segment: str, mem_id: str):
        key = self._segment_index_key(segment)
        idx = await self._store_get(key) or []
        if mem_id in idx:
            idx.remove(mem_id)
            await self._store_put(key, idx)

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
        Enhancements:
          - Best-effort schedule parsing: detects schedule hints in metadata['requested_schedule_text']
            or metadata['schedule_time_iso'] and adjusts times already in the past by rolling them to next sensible occurrence.
          - Annotates record['metadata']['scheduled_for'] (ISO8601) and ['schedule_adjusted']=True if adjustment was made.
        """
        metadata = metadata or {}
        # start hologram write animation in background (best-effort)
        node_id = f"mem_write_{uuid.uuid4().hex[:6]}"
        link_id = f"link_mem_{node_id}"
        try:
            asyncio.create_task(self._holo_animate_write(node_id, link_id))
        except Exception:
            pass

        # schedule detection & adjustment helper
        scheduled_iso = None
        schedule_adjusted = False
        try:
            # prefer explicit ISO in metadata
            if metadata.get("schedule_time_iso"):
                scheduled_iso = metadata.get("schedule_time_iso")
            else:
                # try to parse free text hints (user_input or requested_schedule_text)
                text_to_scan = metadata.get("requested_schedule_text") or user_input or assistant_response or ""
                # try using dateutil parser if available for robust parsing
                try:
                    from dateutil import parser as _dparser  # type: ignore
                    # search for date/time-like phrases by attempting parse on slices
                    # try whole string first
                    try:
                        dt = _dparser.parse(text_to_scan, fuzzy=True, default=None)
                        if dt:
                            scheduled_iso = dt.astimezone().isoformat()
                    except Exception:
                        # fallback: token-wise attempts
                        for token in text_to_scan.split(","):
                            try:
                                dt = _dparser.parse(token, fuzzy=True, default=None)
                                if dt:
                                    scheduled_iso = dt.astimezone().isoformat()
                                    break
                            except Exception:
                                continue
                except Exception:
                    # fallback naive regex for times like '5pm', '17:00', 'tomorrow at 5'
                    import re
                    m = re.search(r'\b((tomorrow|today)\b.*?\b(at )?\d{1,2}(:\d{2})?\s?(am|pm)?)', (text_to_scan or "").lower(), flags=re.IGNORECASE)
                    if m:
                        # build a naive datetime for today/tomorrow + time
                        try:
                            now = time.localtime()
                            tm = m.group(0)
                            # crude hour extraction
                            hr_match = re.search(r'(\d{1,2})(?::(\d{2}))?', tm)
                            if hr_match:
                                hr = int(hr_match.group(1))
                                mn = int(hr_match.group(2) or 0)
                                is_pm = bool(re.search(r'\bpm\b', tm))
                                if is_pm and hr < 12:
                                    hr += 12
                                day = 0
                                if "tomorrow" in tm:
                                    day = 1
                                ts = time.time() + day * 24 * 3600
                                date_struct = time.localtime(ts)
                                scheduled_ts = time.mktime((date_struct.tm_year, date_struct.tm_mon, date_struct.tm_mday, hr, mn, 0, 0, 0, -1))
                                scheduled_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(scheduled_ts))
                        except Exception:
                            scheduled_iso = None

            # If we have a scheduled_iso, convert to timestamp and adjust if in past
            if scheduled_iso:
                try:
                    # try robust parsing first
                    try:
                        from dateutil import parser as _dparser  # type: ignore
                        dt = _dparser.parse(scheduled_iso)
                        scheduled_ts = dt.timestamp()
                    except Exception:
                        # fallback to time.strptime for common iso-like patterns
                        try:
                            scheduled_ts = time.mktime(time.strptime(scheduled_iso.split('.')[0], "%Y-%m-%dT%H:%M:%S"))
                        except Exception:
                            scheduled_ts = None

                    now_ts = _now_ts()
                    if scheduled_ts:
                        # If scheduled time is in the past (by > 60 seconds), roll forward to next reasonable occurrence
                        if scheduled_ts + 60 < now_ts:
                            # roll forward by whole days until in future (preserve time)
                            # limit to 365 attempts to avoid infinite loops
                            attempts = 0
                            while scheduled_ts + 60 < now_ts and attempts < 365:
                                scheduled_ts += 24 * 3600
                                attempts += 1
                            try:
                                # store ISO
                                scheduled_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(scheduled_ts))
                                schedule_adjusted = True
                            except Exception:
                                schedule_adjusted = False
                    # write back to metadata for downstream consumers
                    if scheduled_iso:
                        metadata["scheduled_for"] = scheduled_iso
                        metadata["schedule_adjusted"] = schedule_adjusted
                except Exception:
                    # swallow parsing errors
                    pass
        except Exception:
            # overall schedule detection must be non-fatal
            logger.debug("Schedule parsing/adjustment failed (non-fatal).", exc_info=True)

        content_hash = _content_hash(user_input or "", assistant_response or "")
        container = await self.get_identity_container(subject_identity_id)

        # duplicate detection
        for mid in container.get("memory_index", []):
            mem = await self._store_get(self._memory_key(mid))
            if mem and mem.get("content_hash") == content_hash:
                mem["last_touched"] = _now_ts()
                mem["access_count"] = mem.get("access_count", 0) + 1
                # attach schedule adjustments if we computed one (upgrade existing memory metadata)
                if metadata.get("scheduled_for"):
                    mem.setdefault("metadata", {})
                    mem["metadata"].update(metadata)
                await self._store_put(self._memory_key(mid), mem)
                await self._audit(actor, "memory_duplicate_detected", mid, {"subject": subject_identity_id})
                return mid, mem.get("segment", SEG_CONFIDENTIAL)

        # decide segment
        segment = desired_segment or self._decide_segment(user_input or "", assistant_response or "", metadata, subject_identity_id)
        mem_id = f"mem_{int(_now_ts()*1000)}_{uuid.uuid4().hex[:6]}"

        record = {
            "id": mem_id,
            "subject_id": subject_identity_id,
            "user": user_input or "",
            "assistant": assistant_response or "",
            "timestamp": _now_ts(),
            "importance": int(max(1, min(5, int(importance or 1)))),
            "content_hash": content_hash,
            "segment": segment,
            "metadata": metadata or {},
            "access_count": 0,
            "owner_visible": True
        }

        # ensure scheduled_for is always canonical ISO string if present (safeguard)
        if record["metadata"].get("scheduled_for"):
            try:
                # coerce to full ISO with timezone if possible
                sf = record["metadata"]["scheduled_for"]
                try:
                    from dateutil import parser as _dparser  # type: ignore
                    dt = _dparser.parse(sf)
                    record["metadata"]["scheduled_for"] = dt.astimezone().isoformat()
                except Exception:
                    # leave as-is if parsing fails
                    pass
            except Exception:
                pass

        # persist memory record
        await self._store_put(self._memory_key(mem_id), record)
        # append to identity container memory_index (most recent at end)
        container["memory_index"].append(mem_id)
        # trim if over cap
        if len(container["memory_index"]) > self.max_per_identity:
            evicted = container["memory_index"].pop(0)
            try:
                await self._store_delete(self._memory_key(evicted))
            except Exception:
                pass
        await self._store_put(self._identity_container_key(subject_identity_id), container)
        await self._append_to_segment_index(segment, mem_id)
        await self._audit(actor, "memory_store", mem_id, {"segment": segment, "subject": subject_identity_id, "importance": record["importance"], "holo_node": node_id, "schedule_adjusted": schedule_adjusted})
        return mem_id, segment

    # -----------------------
    # Convenience aliases for compatibility
    # -----------------------
    async def append_memory(self, identity_id: str, memory_record: Dict[str, Any]) -> Tuple[str, str]:
        """
        Accepts a full memory_record dict and appends it.
        Returns (mem_id, segment)
        """
        mem_id = memory_record.get("id") or f"mem_{int(_now_ts()*1000)}_{uuid.uuid4().hex[:6]}"
        memory_record["id"] = mem_id
        subj = memory_record.get("subject_id", "owner_primary")
        seg = memory_record.get("segment", self._decide_segment(memory_record.get("user",""), memory_record.get("assistant",""), memory_record.get("metadata"), subj))
        memory_record["segment"] = seg
        memory_record.setdefault("timestamp", _now_ts())
        memory_record.setdefault("access_count", 0)
        await self._store_put(self._memory_key(mem_id), memory_record)
        container = await self.get_identity_container(subj)
        container["memory_index"].append(mem_id)
        if len(container["memory_index"]) > self.max_per_identity:
            evicted = container["memory_index"].pop(0)
            try:
                await self._store_delete(self._memory_key(evicted))
            except Exception:
                pass
        await self._store_put(self._identity_container_key(subj), container)
        await self._append_to_segment_index(seg, mem_id)
        await self._audit("system", "append_memory", mem_id, {"subject": subj})
        return mem_id, seg

    async def write_memory(self, identity_id: str, memory_record: Dict[str, Any]) -> Tuple[str, str]:
        # alias of append_memory for compatibility
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
        """
        Return a ranked list of memory records matching query_text for subject_identity_id that requester_id is allowed to access.
        """
        # spawn holo read animation (background)
        node_id = f"mem_read_{uuid.uuid4().hex[:6]}"
        link_id = f"link_mem_{node_id}"
        try:
            asyncio.create_task(self._holo_animate_read(node_id, link_id))
        except Exception:
            pass

        # determine permitted segments
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
        pool = list(reversed(container.get("memory_index", [])))  # newest first

        results = []
        for mid in pool:
            mem = await self._store_get(self._memory_key(mid))
            if not mem:
                continue
            if mem.get("segment") not in permitted:
                # skip records not allowed
                continue
            score = self._relevance_score_simple(query_text or "", mem)
            if score < min_relevance:
                continue
            # update access metadata (best-effort)
            mem = dict(mem)  # shallow copy
            mem["access_count"] = mem.get("access_count", 0) + 1
            mem["last_touched"] = _now_ts()
            try:
                await self._store_put(self._memory_key(mid), mem)
            except Exception:
                pass
            # redact sensitive content for non-owner unless permission covers confidential
            if requester_id not in ("owner_primary", "owner") and mem.get("segment") == SEG_CONFIDENTIAL:
                # skip exposing confidential to non-owner
                continue
            results.append((score, mem))

        # sort by score, importance, recency
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
        if not qtokens or not mtokens:
            base = 0.0
        else:
            base = len(qtokens.intersection(mtokens)) / max(1, len(qtokens.union(mtokens)))
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
        await self._store_put(self._identity_container_key(subject_identity_id), container)
        await self._audit("owner", "grant_privilege", subject_identity_id, {"grantee": privileged_user_id, "segments": segments})
        return True

    async def revoke_privilege(self, subject_identity_id: str, privileged_user_id: str):
        container = await self.get_identity_container(subject_identity_id)
        perms = container.get("permissions", {}) or {}
        if privileged_user_id in perms:
            perms.pop(privileged_user_id)
            container["permissions"] = perms
            await self._store_put(self._identity_container_key(subject_identity_id), container)
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
        # derive if missing
        if not root:
            identities = await self.list_identities()
            root = {"identities": identities, "count": len(identities), "timestamp": _now_ts()}
            await self._store_put(idx_key, root)
        return root

    async def load_index(self) -> Dict[str, Any]:
        """Compatibility: load_index alias for get_root_index"""
        return await self.get_root_index()

    async def save_index(self, index_data: Dict[str, Any]) -> bool:
        """Persist a provided index object (best-effort)."""
        idx_key = self._root_key("root_index")
        try:
            await self._store_put(idx_key, index_data)
            return True
        except Exception as e:
            logger.warning(f"save_index failed: {e}")
            return False

    # -----------------------
    # Put/get by memory key (compat wrappers)
    # -----------------------
    async def put(self, key: str, category: Optional[str] = None, obj: Optional[Any] = None):
        """
        Compatibility put:
         - put(mem_id, 'persona_memory', obj)
         - put(mem_id, obj)
        """
        # If caller passed (key, obj) as two args
        if obj is None and category is not None:
            # category might actually be the object
            if isinstance(category, dict):
                return await self._store_put(self._memory_key(key), category)
            # else it might be put(mem_id, 'type', obj) with obj omitted -> treat as noop
            return False
        # If called with (key, category, obj)
        if obj is not None:
            # if category == 'persona_memory' or similar, store under memory key
            if isinstance(obj, dict) and obj.get("id"):
                return await self._store_put(self._memory_key(obj["id"]), obj)
            # fallback to putting raw
            return await self._store_put(self._memory_key(key), obj)
        return False

    async def get(self, key: str, category: Optional[str] = None):
        """Compatibility get; supports get(mem_id) and get(mem_id, 'persona_memory')"""
        # if key looks like a memory id
        if key and key.startswith("mem_"):
            return await self._store_get(self._memory_key(key))
        # otherwise try root index
        return await self._store_get(self._root_key(key))

    # -----------------------
    # Important persistence
    # -----------------------
    async def persist_important(self, subject_identity_id: str, memory_record: Dict[str, Any]) -> bool:
        """
        Best-effort helper to persist an important memory into an owner-specific container or into a prioritized store key.
        """
        try:
            if memory_record.get("id") is None:
                memory_record["id"] = f"mem_{int(_now_ts()*1000)}_{uuid.uuid4().hex[:6]}"
            # try putting under memory key
            await self._store_put(self._memory_key(memory_record["id"]), memory_record)
            # also ensure it's referenced in identity container
            container = await self.get_identity_container(subject_identity_id)
            if memory_record["id"] not in container["memory_index"]:
                container["memory_index"].append(memory_record["id"])
                await self._store_put(self._identity_container_key(subject_identity_id), container)
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
                    # consider removed already
                    removed += 1
                    continue
                timestamp = mem.get("timestamp", 0)
                importance = mem.get("importance", 1)
                if timestamp < cutoff and importance < min_importance:
                    try:
                        await self._store_delete(self._memory_key(mid))
                    except Exception:
                        pass
                    removed += 1
                else:
                    kept.append(mid)
            if removed > 0:
                container["memory_index"] = kept
                await self._store_put(self._identity_container_key(ident), container)
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
                if not mem:
                    continue
                text = (mem.get("user","") + " " + mem.get("assistant","")).lower()
                if not query_text or all(tok in text for tok in (query_text or "").lower().split()):
                    found.append(mem)
                    if len(found) >= limit:
                        return found
        return found

    # -----------------------
    # Back-compat convenience wrappers
    # -----------------------
    # alias names expected by other modules
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

# End of file
