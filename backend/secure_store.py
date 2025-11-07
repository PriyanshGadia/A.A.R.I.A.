# secure_store_async.py
"""
Production SecureStorage - Enterprise Grade (Asynchronous Version)

Fully compatible with aiosqlite and async/await architecture.
"""

import os
import asyncio
import logging
import ctypes
from datetime import datetime
from typing import Optional, Dict, Any, List

import aiosqlite

from crypto import (
    generate_master_key, wrap_master_key, unwrap_master_key,
    encrypt_with_master_key, decrypt_with_master_key
)

logger = logging.getLogger(__name__)

# Default Paths (adjust as needed)
DB_PATH = "assistant_store.db"
WRAPPED_MASTER_KEY_PATH = "master_key.wrapped"


class SecureStorageAsync:
    """
    Enterprise-grade, asynchronous encrypted storage with aiosqlite backend.
    """

    def __init__(self, wrapped_key_path: str = WRAPPED_MASTER_KEY_PATH, db_path: str = DB_PATH):
        self.wrapped_key_path = wrapped_key_path
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self.conn: Optional[aiosqlite.Connection] = None
        self._master_key: Optional[bytes] = None
        logger.info(f"Async SecureStorage configured for: db={db_path}, key={wrapped_key_path}")

    async def connect(self):
        """Establish the database connection and initialize the schema."""
        if self.conn is not None:
            return
            
        self.conn = await aiosqlite.connect(self.db_path, timeout=30)
        self.conn.row_factory = aiosqlite.Row
        await self._init_db()

    async def _init_db(self):
        """Initialize database schema with proper indexes."""
        async with self._lock:
            await self.conn.execute("PRAGMA journal_mode=WAL")
            await self.conn.execute("PRAGMA synchronous=NORMAL")
            await self.conn.execute("PRAGMA foreign_keys=ON")
            await self.conn.execute("PRAGMA busy_timeout=30000")

            # Main storage table
            await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS blobs (
                    id TEXT PRIMARY KEY,
                    type TEXT,
                    payload TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            
            # Metadata table
            await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            
            await self.conn.execute("INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '1')")
            await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_blobs_type ON blobs(type)")
            await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_blobs_created ON blobs(created_at)")
            
            await self.conn.commit()
            logger.debug("Async database schema initialized")

    # --- Master Key Management ---
    def has_wrapped_master(self) -> bool:
        """Check if wrapped master key exists."""
        return os.path.exists(self.wrapped_key_path)

    def create_and_store_master_key(self, password: str) -> None:
        """Create and store master key with atomic file operations."""
        if self.has_wrapped_master():
            raise RuntimeError("Wrapped master key already exists")
        
        mk = generate_master_key()
        wrapped = wrap_master_key(mk, password)
        temp_path = self.wrapped_key_path + ".tmp"
        
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                f.write(wrapped)
            os.replace(temp_path, self.wrapped_key_path)
            
            try:
                os.chmod(self.wrapped_key_path, 0o600)
            except Exception:
                logger.warning("Failed to set permissions on wrapped key file")
                
            self._master_key = mk
            logger.info("Master key created and stored")
            
        except Exception:
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception:
                    pass
            raise

    def unlock_with_password(self, password: str) -> None:
        """Unwrap master key and cache in memory."""
        if not self.has_wrapped_master():
            raise RuntimeError("No wrapped master key found")
            
        with open(self.wrapped_key_path, "r", encoding="utf-8") as f:
            wrapped = f.read()
            
        self._master_key = unwrap_master_key(wrapped, password)
        logger.info("Master key unlocked")

    def _zeroize_master_key(self):
        """Wipe master key bytes from memory."""
        if self._master_key:
            try:
                mutable_key = bytearray(self._master_key)
                ctypes.memset(ctypes.addressof(ctypes.c_char.from_buffer(mutable_key)), 0, len(mutable_key))
            except Exception:
                pass
        self._master_key = None

    async def lock(self) -> None:
        """Lock storage and wipe master key."""
        async with self._lock:
            self._zeroize_master_key()
            logger.info("SecureStorage locked")

    # --- Core CRUD Operations ---
    def _require_unlocked(self):
        """Ensure storage is unlocked before operations."""
        if self._master_key is None:
            raise RuntimeError("Master key not unlocked")

    async def put(self, id: str, type_: str, obj: Dict[str, Any]) -> None:
        """Encrypt and store object."""
        self._require_unlocked()
        async with self._lock:
            payload = encrypt_with_master_key(obj, self._master_key)
            now = datetime.utcnow().isoformat()
            
            await self.conn.execute("""
                INSERT OR REPLACE INTO blobs (id, type, payload, created_at, updated_at)
                VALUES (?, ?, ?, COALESCE((SELECT created_at FROM blobs WHERE id = ?), ?), ?)
            """, (id, type_, payload, id, now, now))
            
            await self.conn.commit()

    async def get(self, id: str) -> Optional[Dict[str, Any]]:
        """Retrieve and decrypt object by ID."""
        self._require_unlocked()
        async with self._lock:
            async with self.conn.execute("SELECT payload FROM blobs WHERE id = ?", (id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return decrypt_with_master_key(row['payload'], self._master_key)
                return None

    async def list_by_type(self, type_: str) -> List[Dict[str, Any]]:
        """List objects by type."""
        self._require_unlocked()
        async with self._lock:
            async with self.conn.execute(
                "SELECT id, payload, created_at, updated_at FROM blobs WHERE type = ? ORDER BY created_at DESC",
                (type_,)
            ) as cursor:
                rows = await cursor.fetchall()
                result = []
                for row in rows:
                    result.append({
                        "id": row['id'],
                        "obj": decrypt_with_master_key(row['payload'], self._master_key),
                        "created_at": row['created_at'],
                        "updated_at": row['updated_at']
                    })
                return result

    async def delete(self, id: str) -> bool:
        """Delete object by ID."""
        self._require_unlocked()
        async with self._lock:
            cursor = await self.conn.execute("DELETE FROM blobs WHERE id = ?", (id,))
            await self.conn.commit()
            return cursor.rowcount > 0

    # --- Compatibility Methods ---
    async def store(self, id: str, type_: str, obj: Dict[str, Any]) -> None:
        """Alias for put()."""
        await self.put(id, type_, obj)

    async def retrieve(self, id: str) -> Optional[Dict[str, Any]]:
        """Alias for get()."""
        return await self.get(id)

    async def list(self, type_: str) -> List[Dict[str, Any]]:
        """Alias for list_by_type()."""
        return await self.list_by_type(type_)

    async def remove(self, id: str) -> bool:
        """Alias for delete()."""
        return await self.delete(id)

    # --- Utility Methods ---
    async def exists(self, id: str) -> bool:
        """Check if object exists."""
        async with self._lock:
            async with self.conn.execute("SELECT 1 FROM blobs WHERE id = ?", (id,)) as cursor:
                return await cursor.fetchone() is not None

    async def get_metadata(self, type_: str = None) -> List[Dict[str, Any]]:
        """Get metadata without decrypting."""
        async with self._lock:
            if type_:
                async with self.conn.execute(
                    "SELECT id, type, created_at, updated_at FROM blobs WHERE type = ? ORDER BY created_at DESC",
                    (type_,)
                ) as cursor:
                    rows = await cursor.fetchall()
            else:
                async with self.conn.execute(
                    "SELECT id, type, created_at, updated_at FROM blobs ORDER BY created_at DESC"
                ) as cursor:
                    rows = await cursor.fetchall()
            
            return [dict(row) for row in rows]

    async def count(self, type_: str = None) -> int:
        """Count objects."""
        async with self._lock:
            if type_:
                async with self.conn.execute("SELECT COUNT(*) as count FROM blobs WHERE type = ?", (type_,)) as cursor:
                    row = await cursor.fetchone()
            else:
                async with self.conn.execute("SELECT COUNT(*) as count FROM blobs") as cursor:
                    row = await cursor.fetchone()
            return row['count']

    # --- Maintenance ---
    async def verify_integrity(self) -> bool:
        """Run SQLite integrity check."""
        async with self._lock:
            async with self.conn.execute("PRAGMA integrity_check") as cursor:
                result = await cursor.fetchone()
                return result[0].lower() == "ok"

    async def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        async with self._lock:
            async with self.conn.execute("""
                SELECT COUNT(*) as total_items,
                       COUNT(DISTINCT type) as unique_types,
                       MIN(created_at) as oldest_item,
                       MAX(updated_at) as latest_update
                FROM blobs
            """) as cursor:
                stats = dict(await cursor.fetchone())
                
            async with self.conn.execute("SELECT type, COUNT(*) as count FROM blobs GROUP BY type") as cursor:
                stats['items_by_type'] = {row['type']: row['count'] for row in await cursor.fetchall()}
                
            return stats

    # --- Context Management ---
    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Securely close storage."""
        if self.conn is not None:
            async with self._lock:
                try:
                    await self.conn.commit()
                    self._zeroize_master_key()
                    await self.conn.close()
                    self.conn = None
                    logger.info("Async SecureStorage closed")
                except Exception as e:
                    logger.warning(f"Error during close: {e}")

    # --- Health Check ---
    async def health_check(self) -> Dict[str, Any]:
        """Health check for monitoring."""
        try:
            integrity_ok = await self.verify_integrity()
            is_connected = self.conn is not None
            is_unlocked = self._master_key is not None
            
            return {
                "status": "healthy" if integrity_ok and is_connected else "unhealthy",
                "integrity_ok": integrity_ok,
                "connected": is_connected,
                "unlocked": is_unlocked
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }


# --- Example Usage ---
async def main():
    """Demonstration of async secure storage."""
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

    async with SecureStorageAsync() as store:
        # Check if we need to create master key
        if not store.has_wrapped_master():
            print("Creating new master key...")
            store.create_and_store_master_key("secure_password_123")
        else:
            print("Unlocking with password...")
            store.unlock_with_password("secure_password_123")

        # Store some data
        await store.put("user_prefs_1", "preferences", {"theme": "dark", "notifications": True})
        await store.put("doc_1", "document", {"title": "Secret Plan", "content": "Shhh..."})

        # Retrieve data
        prefs = await store.get("user_prefs_1")
        docs = await store.list_by_type("document")
        
        print(f"Preferences: {prefs}")
        print(f"Documents: {docs}")
        print(f"Integrity OK: {await store.verify_integrity()}")
        print(f"Stats: {await store.get_stats()}")

    print("Demo completed successfully.")


if __name__ == "__main__":
    asyncio.run(main())