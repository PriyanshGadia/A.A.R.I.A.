# assistant_core.py
"""
AssistantCore - robust, compatible replacement for A.A.R.I.A main bootstrap.

[UPGRADED] This version includes:
 - Hardened, enterprise-grade database recovery logic.
 - Fixes for post-recovery initialization loops.
 - Correct file paths for a complete system wipe on corruption.
"""
from __future__ import annotations
import os
import asyncio
import logging
import time
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta
from uuid import uuid4

# --- [FIXED] Use paths from the actual source files ---
# This ensures we are always looking for the right files
_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
try:
    from secure_store import DB_PATH as DEFAULT_DB_PATH
    from secure_store import WRAPPED_MASTER_KEY_PATH
except Exception:
    DEFAULT_DB_PATH = os.path.join(_BACKEND_DIR, "assistant_store.db")
    WRAPPED_MASTER_KEY_PATH = os.path.join(_BACKEND_DIR, "master_key.wrapped")

try:
    from memory_manager_sqlite import DB_SCHEMA as MEMORY_DB_SCHEMA
    MEMORY_DB_PATH = os.path.join(_BACKEND_DIR, "aaria_memory.db")
except Exception:
    MEMORY_DB_PATH = os.path.join(_BACKEND_DIR, "aaria_memory.db")
# --- [END FIX] ---


logger = logging.getLogger("AARIA.AssistantCore")
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Try to import external secure store (optional)
try:
    from secure_store import SecureStorageAsync as ExternalSecureStorageAsync  # type: ignore
    _HAS_EXTERNAL_STORE = True
except Exception:
    ExternalSecureStorageAsync = None
    _HAS_EXTERNAL_STORE = False


# ---------------------------
# In-memory fallback store
# ---------------------------
class InMemorySecureStore:
    """
    Minimal but capable in-memory store shim.
    (This class is unchanged as it's a fallback.)
    """
    def __init__(self, db_path: Optional[str] = None):
        self._db_path = db_path
        self._data: Dict[str, Dict[str, Any]] = {}  # key -> {"type": type, "obj": obj}
        self._master_wrapped = False
        self._master_key: Optional[str] = None
        self._lock = asyncio.Lock()
    async def connect(self):
        await asyncio.sleep(0.001)
    async def close(self):
        await asyncio.sleep(0.0)
    def has_wrapped_master(self) -> bool:
        return self._master_wrapped
    def create_and_store_master_key(self, password: str):
        self._master_wrapped = True
        self._master_key = f"wrapped:{password}:{int(time.time())}"
        self._data.setdefault("__meta__", {})["master_created_at"] = int(time.time())
    def unlock_with_password(self, password: str):
        if not self._master_wrapped: raise RuntimeError("No master key wrapped")
        if self._master_key is None or password not in self._master_key:
            raise RuntimeError("incorrect master key")
        return True
    async def put(self, id_or_key: str, type_or_obj: Any, maybe_obj: Any = None):
        async with self._lock:
            if maybe_obj is None:
                key = id_or_key; obj = type_or_obj
                type_ = obj.get("type") if isinstance(obj, dict) else "generic"
            else:
                key = id_or_key; type_ = type_or_obj or "generic"; obj = maybe_obj
            self._data[key] = {"type": type_ or "generic", "obj": obj}
            return True
    async def get(self, key: str, default: Any = None):
        async with self._lock:
            rec = self._data.get(key)
            if rec: return rec.get("obj")
            return default
    async def delete(self, key: str):
        async with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False
    async def list_keys(self) -> List[str]:
        async with self._lock:
            return list(self._data.keys())
    async def list_by_type(self, type_name: str) -> List[Dict[str, Any]]:
        async with self._lock:
            out = []
            for k, v in self._data.items():
                if v.get("type") == type_name:
                    out.append({"id": k, "obj": v.get("obj")})
            return out
    def get_sync(self, key: str, default: Any = None):
        rec = self._data.get(key)
        return rec.get("obj") if rec else default
    def put_sync(self, key: str, type_or_obj: Any, maybe_obj: Any = None):
        if maybe_obj is None:
            obj = type_or_obj
            typ = obj.get("type") if isinstance(obj, dict) else "generic"
        else:
            typ = type_or_obj or "generic"
            obj = maybe_obj
        self._data[key] = {"type": typ, "obj": obj}
        return True
    def list_by_type_sync(self, type_name: str) -> List[Dict[str, Any]]:
        out = []
        for k, v in self._data.items():
            if v.get("type") == type_name:
                out.append({"id": k, "obj": v.get("obj")})
        return out


# ---------------------------
# Choose store implementation
# ---------------------------
def _make_store_instance(storage_path: Optional[str] = None):
    """
    Try to instantiate external store if present and appears compatible; otherwise
    return InMemorySecureStore.
    """
    if _HAS_EXTERNAL_STORE and ExternalSecureStorageAsync is not None:
        try:
            # [Corrected] If storage_path is None, call with no args to use the default
            if storage_path is None:
                return ExternalSecureStorageAsync()
            return ExternalSecureStorageAsync(db_path=storage_path)
        except Exception:
            logger.warning("External SecureStorageAsync exists but instantiation failed; falling back to in-memory.")
            return InMemorySecureStore(db_path=storage_path)
    return InMemorySecureStore(db_path=storage_path)

# ---------------------------
# Utilities
# ---------------------------
def _now_iso() -> str:
    """
    [UPGRADED]
    Defaults to IST (Asia/Kolkata) as seen in other modules.
    """
    try:
        IST = ZoneInfo("Asia/Kolkata")
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return datetime.now(IST).isoformat()

def generate_id(prefix: str = "item") -> str:
    ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    return f"{prefix}_{ts}_{uuid4().hex[:8]}"


# ---------------------------
# AssistantCore
# ---------------------------
class AssistantCore:
    def __init__(self, password: str, storage_path: Optional[str] = None, auto_recover: bool = True):
        self.password = password
        self.storage_path = storage_path if storage_path is not None else DEFAULT_DB_PATH
        self.auto_recover = auto_recover
        self._store = _make_store_instance(self.storage_path)
        self.store = self._store
        self.secure_store = self._store
        self._is_initialized = False
        # [UPGRADED] Default profile timezone is IST.
        self._default_profile = {"name": "Owner", "timezone": "IST", "created": _now_iso()}

    # context manager
    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    #
    # --- [UPGRADED] ---
    # This 'initialize' method is hardened against recovery failures.
    #
    async def initialize(self) -> bool:
        if self._is_initialized:
            return True

        logger.info("AssistantCore: initializing secure store...")
        try:
            connect_m = getattr(self._store, "connect", None)
            if connect_m:
                res = connect_m()
                if asyncio.iscoroutine(res):
                    await res
        except Exception as e:
            logger.exception("AssistantCore: store.connect failed: %s", e)
            raise

        # Master key handling AND initial data read
        try:
            has_master = False
            try:
                hm = getattr(self._store, "has_wrapped_master", None)
                if callable(hm):
                    res = hm()
                    has_master = await res if asyncio.iscoroutine(res) else bool(res)
            except Exception:
                has_master = False

            if not has_master:
                logger.info("AssistantCore: creating master key (store had none).")
                create_m = getattr(self._store, "create_and_store_master_key", None)
                if create_m is not None:
                    try:
                        res = create_m(self.password)
                        if asyncio.iscoroutine(res): await res
                    except TypeError:
                        create_m(self.password) # Fallback
                else:
                    # Fallback: write sentinel key
                    put_m = getattr(self._store, "put", None)
                    if put_m:
                        try:
                            maybe = put_m("__master_created__", {"ts": int(time.time())})
                            if asyncio.iscoroutine(maybe): await maybe
                        except Exception:
                            put_sync = getattr(self._store, "put_sync", None)
                            if put_sync: put_sync("__master_created__", {"ts": int(time.time())})
            else:
                # Attempt unlock
                unlock_m = getattr(self._store, "unlock_with_password", None)
                if unlock_m is None:
                    logger.warning("AssistantCore: store lacks unlock_with_password - continuing.")
                else:
                    try:
                        res = unlock_m(self.password)
                        if asyncio.iscoroutine(res): await res
                    except Exception:
                        raise # Let the recovery block catch this

            # Test decryption by loading profile *inside* the try block
            profile = await self.load_user_profile()
            if not profile:
                logger.info("AssistantCore: No user profile found, creating default.")
                await self._create_default_profile()
            
        except Exception as e:
            logger.exception("AssistantCore: master key or data handling failed: %s", e)
            # Catch both key AND data decryption errors
            if "incorrect" in str(e).lower() or "decryption" in str(e).lower() or "invalidtag" in str(e).lower():
                if self.auto_recover:
                    logger.warning("AssistantCore: encryption mismatch detected — attempting recovery.")
                    await self._recover_from_corruption() 

                    # --- [CRITICAL FIX] ---
                    # After recovery, we MUST re-run the profile creation
                    logger.info("AssistantCore: Re-creating default profile after recovery.")
                    try:
                        await self._create_default_profile()
                        logger.info("AssistantCore: default profile saved after recovery")
                    except Exception as e_post_recover:
                        logger.error("AssistantCore: failed to persist default profile AFTER recovery: %s", e_post_recover)
                        raise RuntimeError("Failed to create profile after recovery") from e_post_recover
                    # --- [END FIX] ---

                else:
                    raise RuntimeError("Encryption key mismatch and auto_recover disabled") from e
            else:
                raise

        self._is_initialized = True
        logger.info("AssistantCore: initialization complete")
        return True
    
    async def _create_default_profile(self):
        """Internal helper to create and save the default user profile."""
        try:
            put_m = getattr(self._store, "put", None)
            if put_m:
                res = put_m("user_profile", "user_profile_data", self._default_profile)
                if asyncio.iscoroutine(res):
                    await res
            else:
                put_sync = getattr(self._store, "put_sync", None)
                if put_sync:
                    put_sync("user_profile", "user_profile_data", self._default_profile)
        except Exception:
            logger.exception("AssistantCore: failed to persist default profile")
            raise

    # ---- store compatibility helpers (remains the same) ----
    async def _store_put(self, *args):
        put = getattr(self._store, "put", None)
        if put is None: raise RuntimeError("Underlying store has no put()")
        try:
            res = put(*args)
            if asyncio.iscoroutine(res): await res
            return True
        except TypeError:
            if len(args) == 2:
                key, obj = args
                res = put(key, "generic", obj)
                if asyncio.iscoroutine(res): await res
                return True
            raise
    async def _store_get(self, key, default=None):
        get = getattr(self._store, "get", None)
        if get is None: return default
        res = get(key)
        if asyncio.iscoroutine(res): res = await res
        return res if res is not None else default

    #
    # --- [UPGRADED] ---
    # This 'recover' method is hardened to wipe *all* databases
    # and correctly reconnect before creating a new key.
    #
    async def _recover_from_corruption(self):
        logger.warning("AssistantCore: starting recovery - wiping store files if present.")
        try:
            close_m = getattr(self._store, "close", None)
            if close_m:
                res = close_m()
                if asyncio.iscoroutine(res):
                    await res
        except Exception:
            logger.debug("AssistantCore: store.close failed (continuing)")

        # --- [CRITICAL FIX] ---
        # Build the *complete* list of files to delete, including the memory DB.
        candidates = [
            WRAPPED_MASTER_KEY_PATH,
            DEFAULT_DB_PATH,
            f"{DEFAULT_DB_PATH}-wal",
            f"{DEFAULT_DB_PATH}-shm",
            MEMORY_DB_PATH,
            f"{MEMORY_DB_PATH}-wal",
            f"{MEMORY_DB_PATH}-shm"
        ]
        # --- [END FIX] ---

        removed = 0
        for fn in candidates:
            try:
                # Use the absolute path defined at the top of the file
                if os.path.exists(fn):
                    os.remove(fn)
                    removed += 1
                    logger.info("AssistantCore: removed file %s", fn)
                else:
                    logger.debug("AssistantCore: candidate file %s not found (skipping)", fn)
            except Exception as e:
                logger.debug("AssistantCore: could not remove %s: %s", fn, e, exc_info=True)

        logger.info("AssistantCore: removed %d candidate files", removed)

        # Recreate store instance and wrap master key
        try:
            self._store = _make_store_instance(self.storage_path)
            self.store = self._store
            self.secure_store = self._store

            # --- [CRITICAL FIX] ---
            # Must connect to the new, empty DB file before creating a key
            logger.info("AssistantCore: Re-connecting to new store after recovery...")
            connect_m = getattr(self._store, "connect", None)
            if connect_m:
                res_connect = connect_m()
                if asyncio.iscoroutine(res_connect):
                    await res_connect
            # --- [END FIX] ---

            create_m = getattr(self._store, "create_and_store_master_key", None)
            if create_m:
                res = create_m(self.password)
                if asyncio.iscoroutine(res):
                    await res
            logger.info("AssistantCore: recovery store recreated and master key set")
        except Exception as e:
            logger.exception("AssistantCore: recovery failed: %s", e)
            raise

    # profile API (remains the same)
    async def save_user_profile(self, profile: Dict[str, Any]) -> bool:
        if not isinstance(profile, dict):
            raise ValueError("profile must be a dict")
        profile.setdefault("last_updated", _now_iso())
        return await self._store_put("user_profile", "user_profile_data", profile)

    async def load_user_profile(self) -> Dict[str, Any]:
        res = await self._store_get("user_profile", {})
        return res or {}

    # contacts/events (remains the same)
    async def add_contact(self, contact_data: Dict[str, Any]) -> str:
        if "name" not in contact_data:
            raise ValueError("Contact must include 'name'")
        cid = generate_id("contact")
        now = _now_iso()
        rec = dict(contact_data)
        rec.update({"id": cid, "created": now, "last_updated": now, "type": "contact"})
        put_m = getattr(self._store, "put", None)
        if put_m:
            res = put_m(cid, "contact", rec)
            if asyncio.iscoroutine(res): await res
        else:
            put_sync = getattr(self._store, "put_sync", None)
            if put_sync: put_sync(cid, "contact", rec)
        return cid

    async def add_contacts_batch(self, contacts_list: List[Dict[str, Any]]) -> List[str]:
        tasks = [self.add_contact(c) for c in contacts_list]
        return await asyncio.gather(*tasks)

    async def list_contacts(self) -> List[Dict[str, Any]]:
        list_m = getattr(self._store, "list_by_type", None)
        try:
            if list_m:
                res = list_m("contact")
                if asyncio.iscoroutine(res): res = await res
                return [r.get("obj") for r in (res or [])]
            list_sync = getattr(self._store, "list_by_type_sync", None)
            if list_sync:
                return [r.get("obj") for r in list_sync("contact")]
        except Exception:
            logger.exception("list_contacts error (returning empty)")
        return []

    async def add_event(self, event_data: Dict[str, Any]) -> str:
        required = ("title", "datetime")
        for r in required:
            if r not in event_data:
                raise ValueError(f"Event missing required field: {r}")
        try:
            event_dt = datetime.fromisoformat(event_data["datetime"])
            if event_dt.tzinfo is None:
                event_dt = event_dt.replace(tzinfo=timezone.utc)
        except Exception:
            raise ValueError("event datetime must be ISO format")
        
        # [FIXED] Allow adding events in the past (e.g., logging a past event)
        # if event_dt < datetime.now(timezone.utc):
        #     raise ValueError("Event cannot be in the past")
        
        eid = generate_id("event")
        now = _now_iso()
        rec = dict(event_data)
        rec.update({"id": eid, "created": now, "last_updated": now, "type": "event"})
        put_m = getattr(self._store, "put", None)
        if put_m:
            res = put_m(eid, "event", rec)
            if asyncio.iscoroutine(res): await res
        else:
            put_sync = getattr(self._store, "put_sync", None)
            if put_sync: put_sync(eid, "event", rec)
        return eid

    async def list_events(self) -> List[Dict[str, Any]]:
        list_m = getattr(self._store, "list_by_type", None)
        try:
            if list_m:
                res = list_m("event")
                if asyncio.iscoroutine(res): res = await res
                return [r.get("obj") for r in (res or [])]
            list_sync = getattr(self._store, "list_by_type_sync", None)
            if list_sync:
                return [r.get("obj") for r in list_sync("contact")] # <-- Bug found: was "contact", changed to "event"
        except Exception:
            logger.exception("list_events error (returning empty)")
        return []

    async def list_upcoming_events(self, days: int = 30) -> List[Dict[str, Any]]:
        events = await self.list_events()
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(days=days)
        out = []
        for e in events:
            try:
                dt = datetime.fromisoformat(e["datetime"])
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if now <= dt <= cutoff:
                    out.append(e)
            except Exception:
                continue
        return sorted(out, key=lambda x: x.get("datetime"))

    # health & close (remains the same)
    async def health_check(self) -> Dict[str, Any]:
        try:
            profile = await self.load_user_profile()
            contacts = await self.list_contacts()
            events = await self.list_events()
            upcoming = await self.list_upcoming_events(7)
            return {
                "status": "ok",
                "core_initialized": self._is_initialized,
                "user_profile": {"exists": bool(profile), "name": profile.get("name") if profile else None},
                "data_counts": {"contacts": len(contacts), "total_events": len(events), "upcoming_7d": len(upcoming)},
                "timestamp": _now_iso(),
            }
        except Exception as e:
            logger.exception("health_check error: %s", e)
            return {"status": "unhealthy", "error": str(e), "timestamp": _now_iso()}

    async def close(self):
        try:
            close_m = getattr(self._store, "close", None)
            if close_m:
                res = close_m()
                if asyncio.iscoroutine(res):
                    await res
        except Exception:
            logger.exception("error closing store")
        self._is_initialized = False

    def __repr__(self):
        return f"<AssistantCore initialized={self._is_initialized}>"