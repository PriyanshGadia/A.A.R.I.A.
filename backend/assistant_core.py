# assistant_core.py
"""
AssistantCore - robust, compatible replacement for A.A.R.I.A main bootstrap.

Drop-in replacement that provides:
 - store/secure_store attributes expected by other modules
 - compatible fallback in-memory store when repo SecureStorageAsync not present
 - sync + async store APIs for maximum compatibility
 - master-key handling, recovery, and basic profile/contact/event APIs
"""
from __future__ import annotations
import os
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta
from uuid import uuid4

# Get the directory where this file (assistant_core.py) is located
_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))

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
    Minimal but capable in-memory store shim that:
      - supports async methods: connect(), close(), get(key), put(key[, type_, obj]),
        list_by_type(type), list_keys(), delete(key)
      - supports sync methods: get_sync, put_sync, list_by_type_sync for compatibility
      - supports master-key methods: has_wrapped_master(), create_and_store_master_key(password),
        unlock_with_password(password)
    It's intentionally simple and suitable for dev/test and as a graceful fallback.
    """
    def __init__(self, db_path: Optional[str] = None):
        self._db_path = db_path
        self._data: Dict[str, Dict[str, Any]] = {}  # key -> {"type": type, "obj": obj}
        self._master_wrapped = False
        self._master_key: Optional[str] = None
        self._lock = asyncio.Lock()

    # lifecycle
    async def connect(self):
        # tiny async no-op
        await asyncio.sleep(0.001)

    async def close(self):
        await asyncio.sleep(0.0)

    # master key
    def has_wrapped_master(self) -> bool:
        return self._master_wrapped

    def create_and_store_master_key(self, password: str):
        self._master_wrapped = True
        self._master_key = f"wrapped:{password}:{int(time.time())}"
        # also store a sentinel for components that look up a key
        self._data.setdefault("__meta__", {})["master_created_at"] = int(time.time())

    def unlock_with_password(self, password: str):
        if not self._master_wrapped:
            raise RuntimeError("No master key wrapped")
        if self._master_key is None or password not in self._master_key:
            raise RuntimeError("incorrect master key")
        return True

    # async API
    async def put(self, id_or_key: str, type_or_obj: Any, maybe_obj: Any = None):
        """
        Support both signatures:
          await put(key, payload)
          await put(key, type_, payload)
        """
        async with self._lock:
            if maybe_obj is None:
                key = id_or_key
                obj = type_or_obj
                type_ = obj.get("type") if isinstance(obj, dict) else "generic"
            else:
                key = id_or_key
                type_ = type_or_obj or "generic"
                obj = maybe_obj
            self._data[key] = {"type": type_ or "generic", "obj": obj}
            return True

    async def get(self, key: str, default: Any = None):
        async with self._lock:
            rec = self._data.get(key)
            if rec:
                return rec.get("obj")
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

    # ----- synchronous helpers for code that calls store methods without awaiting -----
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
            # try instantiate using common kwarg names; if it fails, fall back
            try:
                return ExternalSecureStorageAsync(db_path=storage_path)
            except TypeError:
                # try no-arg
                return ExternalSecureStorageAsync()
        except Exception:
            logger.warning("External SecureStorageAsync exists but instantiation failed; falling back to in-memory.")
            return InMemorySecureStore(db_path=storage_path)
    return InMemorySecureStore(db_path=storage_path)


# ---------------------------
# Utilities
# ---------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def generate_id(prefix: str = "item") -> str:
    ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    return f"{prefix}_{ts}_{uuid4().hex[:8]}"


# ---------------------------
# AssistantCore
# ---------------------------
class AssistantCore:
    def __init__(self, password: str, storage_path: Optional[str] = None, auto_recover: bool = True):
        self.password = password
        # --- THIS IS THE FIX ---
        # Ensure self.storage_path is set to the default DB path if not provided
        self.storage_path = storage_path if storage_path is not None else "assistant_store.db"
        # --- END FIX ---
        self.auto_recover = auto_recover

        # store instance (preferred attribute names used across repo: store and secure_store)
        self._store = _make_store_instance(storage_path)
        # expose both names
        self.store = self._store
        self.secure_store = self._store

        self._is_initialized = False
        # default (small) user profile
        self._default_profile = {"name": "Owner", "timezone": "UTC", "created": _now_iso()}

    # context manager
    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    #
    # Replace the entire 'initialize' function in assistant_core.py
    # with this new version.
    #
    async def initialize(self) -> bool:
        if self._is_initialized:
            return True

        logger.info("AssistantCore: initializing secure store...")
        # connect to store (handle sync or async connect)
        try:
            connect_m = getattr(self._store, "connect", None)
            if connect_m:
                res = connect_m()
                if asyncio.iscoroutine(res):
                    await res
        except Exception as e:
            logger.exception("AssistantCore: store.connect failed: %s", e)
            raise

        # master key handling AND initial data read
        try:
            has_master = False
            # prefer sync quick check
            try:
                hm = getattr(self._store, "has_wrapped_master", None)
                if callable(hm):
                    res = hm()
                    if asyncio.iscoroutine(res):
                        has_master = await res
                    else:
                        has_master = bool(res)
            except Exception:
                has_master = False

            if not has_master:
                logger.info("AssistantCore: creating master key (store had none).")
                create_m = getattr(self._store, "create_and_store_master_key", None)
                if create_m is not None:
                    # external implementations usually sync here — call directly
                    try:
                        res = create_m(self.password)
                        if asyncio.iscoroutine(res):
                            await res
                    except TypeError:
                        # fallback if signature differs
                        create_m(self.password)
                else:
                    # fallback: write sentinel key if put exists
                    put_m = getattr(self._store, "put", None)
                    if put_m:
                        try:
                            maybe = put_m("__master_created__", {"ts": int(time.time())})
                            if asyncio.iscoroutine(maybe):
                                await maybe
                        except Exception:
                            # try sync variant
                            put_sync = getattr(self._store, "put_sync", None)
                            if put_sync:
                                put_sync("__master_created__", {"ts": int(time.time())})
            else:
                # attempt unlock
                unlock_m = getattr(self._store, "unlock_with_password", None)
                if unlock_m is None:
                    logger.warning("AssistantCore: store lacks unlock_with_password - continuing.")
                else:
                    try:
                        res = unlock_m(self.password)
                        if asyncio.iscoroutine(res):
                            await res
                    except Exception:
                        # try again if sync/async mismatch
                        raise

            # --- MOVED UP & INSIDE TRY BLOCK ---
            # ensure default user profile exists (this also tests data decryption)
            profile = await self.load_user_profile()
            if not profile:
                # create but prefer store.put(key, type_, payload) when available
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

                logger.info("AssistantCore: default profile saved")
            # --- END MOVED BLOCK ---

        except Exception as e:
            logger.exception("AssistantCore: master key or data handling failed: %s", e)
            # Catch both key AND data decryption errors
            if "incorrect" in str(e).lower() or "decryption" in str(e).lower() or "invalidtag" in str(e).lower():
                if self.auto_recover:
                    logger.warning("AssistantCore: encryption mismatch detected — attempting recovery.")
                    await self._recover_from_corruption() # This function now contains the .connect() fix

                    # --- ADDED BLOCK ---
                    # After recovery, we MUST re-run the profile creation
                    logger.info("AssistantCore: Re-creating default profile after recovery.")
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
                        logger.info("AssistantCore: default profile saved after recovery")
                    except Exception as e_post_recover:
                        logger.error("AssistantCore: failed to persist default profile AFTER recovery: %s", e_post_recover)
                        raise RuntimeError("Failed to create profile after recovery") from e_post_recover
                    # --- END ADDED BLOCK ---

                else:
                    raise RuntimeError("Encryption key mismatch and auto_recover disabled") from e
            else:
                raise

        self._is_initialized = True
        logger.info("AssistantCore: initialization complete")
        return True
    
    # ---- store compatibility helpers ----
    async def _store_put(self, *args):
        """
        Normalizes calls from modules that call:
          - store.put(key, obj)
          - store.put(id, type_, obj)
        Works with sync or async store implementations.
        """
        put = getattr(self._store, "put", None)
        if put is None:
            raise RuntimeError("Underlying store has no put()")
        try:
            res = put(*args)
            if asyncio.iscoroutine(res):
                await res
            return True
        except TypeError:
            # try alternative signature: if called with 2 args, attempt (key, obj)
            if len(args) == 2:
                # try (key, type_, obj) fallback by inserting a generic type
                key, obj = args
                res = put(key, "generic", obj)
                if asyncio.iscoroutine(res):
                    await res
                return True
            raise

    async def _store_get(self, key, default=None):
        get = getattr(self._store, "get", None)
        if get is None:
            return default
        res = get(key)
        if asyncio.iscoroutine(res):
            res = await res
        return res if res is not None else default

    async def _recover_from_corruption(self):
        # attempt to close store
        logger.warning("AssistantCore: starting recovery - wiping store files if present.")
        try:
            close_m = getattr(self._store, "close", None)
            if close_m:
                res = close_m()
                if asyncio.iscoroutine(res):
                    await res
        except Exception:
            logger.debug("AssistantCore: store.close failed (continuing)")

        # --- START: CORRECTED FILE DELETION LOGIC ---
        
        # Get the real filenames from your provided code
        # From secure_store.py:
        DB_PATH = "assistant_store.db"
        WRAPPED_MASTER_KEY_PATH = "master_key.wrapped"
        # From memory_manager_sqlite.py:
        MEMORY_DB_PATH = "aaria_memory.db"

        # Build the final, correct list of files to delete
        candidates = [
            WRAPPED_MASTER_KEY_PATH,
            DB_PATH,
            f"{DB_PATH}-wal",
            f"{DB_PATH}-shm",
            MEMORY_DB_PATH,
            f"{MEMORY_DB_PATH}-wal",
            f"{MEMORY_DB_PATH}-shm"
        ]

        removed = 0
        # The files are in the Current Working Directory (CWD).
        # We will try to remove them and ignore FileNotFoundError.
        for fn in candidates: 
            try:
                # Use os.path.abspath to be 100% sure we are in the CWD
                fn_path = os.path.abspath(fn)
                if os.path.exists(fn_path):
                    os.remove(fn_path)
                    removed += 1
                    logger.info("AssistantCore: removed file %s", fn_path)
                else:
                    logger.debug("AssistantCore: candidate file %s not found (skipping)", fn_path)
            except Exception as e:
                logger.debug("AssistantCore: could not remove %s: %s", fn_path, e, exc_info=True)
        # --- END: CORRECTED FILE DELETION LOGIC ---

        logger.info("AssistantCore: removed %d candidate files", removed)

        # recreate store instance and wrap master key
        try:
            self._store = _make_store_instance(self.storage_path)
            self.store = self._store
            self.secure_store = self._store

            logger.info("AssistantCore: Re-connecting to new store after recovery...")
            connect_m = getattr(self._store, "connect", None)
            if connect_m:
                res_connect = connect_m()
                if asyncio.iscoroutine(res_connect):
                    await res_connect
            else:
                logger.warning("AssistantCore: Recovered store instance has no connect method.")

            create_m = getattr(self._store, "create_and_store_master_key", None)
            if create_m:
                res = create_m(self.password)
                if asyncio.iscoroutine(res):
                    await res
            logger.info("AssistantCore: recovery store recreated and master key set")
        except Exception as e:
            logger.exception("AssistantCore: recovery failed: %s", e)
            raise

    # profile API
    async def save_user_profile(self, profile: Dict[str, Any]) -> bool:
        if not isinstance(profile, dict):
            raise ValueError("profile must be a dict")
        profile.setdefault("last_updated", _now_iso())
        # use compatibility wrapper
        return await self._store_put("user_profile", "user_profile_data", profile)

    async def load_user_profile(self) -> Dict[str, Any]:
        res = await self._store_get("user_profile", {})
        return res or {}

    # contacts/events
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
            if asyncio.iscoroutine(res):
                await res
        else:
            put_sync = getattr(self._store, "put_sync", None)
            if put_sync:
                put_sync(cid, "contact", rec)
        return cid

    async def add_contacts_batch(self, contacts_list: List[Dict[str, Any]]) -> List[str]:
        tasks = [self.add_contact(c) for c in contacts_list]
        return await asyncio.gather(*tasks)

    async def list_contacts(self) -> List[Dict[str, Any]]:
        list_m = getattr(self._store, "list_by_type", None)
        try:
            if list_m:
                res = list_m("contact")
                if asyncio.iscoroutine(res):
                    res = await res
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
        # parse ISO-ish datetimes; require timezone or assume UTC
        try:
            event_dt = datetime.fromisoformat(event_data["datetime"])
            if event_dt.tzinfo is None:
                event_dt = event_dt.replace(tzinfo=timezone.utc)
        except Exception:
            raise ValueError("event datetime must be ISO format")
        if event_dt < datetime.now(timezone.utc):
            raise ValueError("Event cannot be in the past")
        eid = generate_id("event")
        now = _now_iso()
        rec = dict(event_data)
        rec.update({"id": eid, "created": now, "last_updated": now, "type": "event"})
        put_m = getattr(self._store, "put", None)
        if put_m:
            res = put_m(eid, "event", rec)
            if asyncio.iscoroutine(res):
                await res
        else:
            put_sync = getattr(self._store, "put_sync", None)
            if put_sync:
                put_sync(eid, "event", rec)
        return eid

    async def list_events(self) -> List[Dict[str, Any]]:
        list_m = getattr(self._store, "list_by_type", None)
        try:
            if list_m:
                res = list_m("event")
                if asyncio.iscoroutine(res):
                    res = await res
                return [r.get("obj") for r in (res or [])]
            list_sync = getattr(self._store, "list_by_type_sync", None)
            if list_sync:
                return [r.get("obj") for r in list_sync("event")]
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
        # sort by ISO-string (stable)
        return sorted(out, key=lambda x: x.get("datetime"))

    # health & close
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